// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;

use databend_meta_snapshot_db::DB;
use databend_meta_types::Node;
use databend_meta_types::raft_types::LogId;
use databend_meta_types::raft_types::NodeId;
use databend_meta_types::raft_types::StoredMembership;
use databend_meta_types::sys_data::SysData;
use display_more::DisplayOptionExt;
use display_more::DisplaySliceExt;
use log::info;
use seq_marked::InternalSeq;

use super::ActiveReadGuard;
use super::active_read_tracker::ActiveReadTracker;
use super::compactor::Compactor;
use crate::leveled_store::immutable::Immutable;
use crate::leveled_store::immutable_data::ImmutableData;
use crate::leveled_store::immutable_levels::ImmutableLevels;
use crate::leveled_store::level::LevelStat;
use crate::leveled_store::leveled_map::leveled_map_data::LeveledMapData;
use crate::leveled_store::state_machine::read_view::StateMachineReadView;
use crate::leveled_store::state_machine::view::StateMachineView;
use crate::sm_v003::compactor_acquirer::CompactorPermit;
use crate::sm_v003::writer_acquirer::WriterPermit;

/// Multi-level storage similar to LevelDB with single-writer concurrency control.
///
/// ## Concurrency Model
/// - **Single writer**: Only one writer allowed at a time via write_semaphore
/// - **Multiple readers**: Concurrent read access across all levels
/// - **Lock-free compaction**: Compactor clones data out before processing, no long mutex holds
/// - **At most one candidate writer**: Top level is exclusively writable
///
/// ## Performance Characteristics
/// - **Read latency**: O(log n) access across levels, newest data first
/// - **Write performance**: Single writer to top level, no contention
/// - **Memory usage**: Grows with number of levels and cached data
/// - **Compaction performance**: Non-blocking, processes cloned data independently
///
/// ## Level Organization
/// ```text
/// |                  | writer_semaphore | compactor_semaphore |
/// | :--              | :--              | :--                 |
/// | writable         | RW               |                     |
/// | immutable_levels | R                | RW                  |
/// | persisted        | R                | RW                  |
/// ```
#[derive(Debug, Clone)]
pub struct LeveledMap {
    /// The writable level and immutable levels shared by map implementations.
    pub(super) data: Arc<Mutex<LeveledMapData>>,

    /// Active sequence-bounded readers that constrain compaction.
    active_reads: Arc<ActiveReadTracker>,
}

impl Default for LeveledMap {
    fn default() -> Self {
        Self {
            data: Arc::new(Mutex::new(LeveledMapData::default())),
            active_reads: Default::default(),
        }
    }
}

impl LeveledMap {
    pub(crate) fn from_persisted(db: DB) -> Self {
        Self {
            data: Arc::new(Mutex::new(LeveledMapData {
                writable: Default::default(),
                immutable: Arc::new(ImmutableData::new(Default::default(), Some(db))),
            })),
            active_reads: Default::default(),
        }
    }

    pub(crate) fn to_view(&self) -> StateMachineView {
        StateMachineView::from_leveled_map(self)
    }

    pub(crate) fn new_active_read_guard(&self) -> ActiveReadGuard {
        // Register the guard while holding `data`: a compactor that freezes the
        // writable level cannot otherwise miss a read that obtained its old
        // sequence just before registering.
        let inner = self.data.lock().unwrap();
        let seq = InternalSeq::new(inner.writable.sys_data.curr_seq());
        self.active_reads.register(seq)
    }

    pub fn oldest_active_read_seq(&self) -> InternalSeq {
        self.active_reads.oldest_seq()
    }

    /// Wait until it is safe to compact the current immutable data.
    ///
    /// Call this while holding the compactor permit, so the immutable root that
    /// defines the boundary cannot change while waiting.
    pub async fn wait_for_active_reads_before_compaction(&self) {
        let compaction_boundary = self.immutable_data().last_seq();
        self.active_reads
            .wait_for_oldest_seq_at_least(compaction_boundary)
            .await;
    }

    pub(crate) fn with_sys_data<T>(&self, f: impl FnOnce(&mut SysData) -> T) -> T {
        self.with_inner(|inner| inner.writable.with_sys_data(f))
    }

    /// Freeze the current writable level and create a new empty writable level.
    ///
    /// Need writer permit to reset the writable level, and compactor permit to add a new immutable level.
    pub(crate) fn freeze_writable(
        &self,
        _writer_permit: &mut WriterPermit,
        _compactor_permit: &mut CompactorPermit,
    ) {
        self.do_freeze_writable()
    }

    /// For testing, requires no permit.
    #[cfg(test)]
    pub(crate) fn freeze_writable_without_permit(&self) {
        self.do_freeze_writable()
    }

    fn do_freeze_writable(&self) {
        let mut inner = self.data.lock().unwrap();

        let new_writable = inner.writable.new_level();
        let new_immutable = std::mem::replace(&mut inner.writable, new_writable);

        let mut levels = inner.immutable.levels().clone();
        levels.insert(Immutable::new_from_level(new_immutable));

        let persisted = inner.immutable.persisted().cloned();
        let new_immutable_data = ImmutableData::new(levels.clone(), persisted);
        inner.immutable = Arc::new(new_immutable_data);

        info!(
            "do_freeze_writable: after writable: {}, immutables: {}",
            inner.writable.stat(),
            levels.indexes().display_n(265)
        );
    }

    /// Return the kv count and expire count in the writable level.
    pub fn writable_stat(&self) -> LevelStat {
        self.with_inner(|inner| inner.writable.stat())
    }

    pub fn persisted(&self) -> Option<DB> {
        self.with_inner(|inner| inner.immutable.persisted().cloned())
    }

    /// Return a reference to the immutable levels.
    pub fn immutable_levels(&self) -> ImmutableLevels {
        self.with_inner(|inner| inner.immutable.levels().clone())
    }

    pub(crate) fn to_read_view(&self) -> StateMachineReadView {
        StateMachineReadView::new(self)
    }

    pub fn curr_seq(&self) -> u64 {
        self.with_sys_data(|s| s.curr_seq())
    }

    pub fn last_membership(&self) -> StoredMembership {
        self.with_sys_data(|s| s.last_membership_ref().clone())
    }

    pub fn last_applied(&self) -> Option<LogId> {
        self.with_sys_data(|s| *s.last_applied_mut())
    }

    pub fn nodes(&self) -> BTreeMap<NodeId, Node> {
        self.with_sys_data(|s| s.nodes_mut().clone())
    }

    // TODO: rename:
    pub(crate) fn with_inner<T>(&self, f: impl FnOnce(&mut LeveledMapData) -> T) -> T {
        let mut inner = self.data.lock().unwrap();
        f(&mut inner)
    }

    /// For testing only.
    /// Replace all immutable levels with the given one.
    pub(crate) fn replace_immutable_levels(&self, b: ImmutableLevels) {
        self.with_inner(|data| {
            let persisted = data.immutable.persisted().cloned();
            data.immutable = Arc::new(ImmutableData::new(b, persisted))
        });
    }

    /// Replace bottom immutable levels and persisted level with compacted data.
    ///
    /// The caller must first wait until every active read has a sequence at
    /// least as new as the compacted data.
    ///
    /// **Important**: Do not drop the compactor within this function when called
    /// under a state machine lock, as dropping may take ~250ms.
    pub fn replace_with_compacted(&self, compactor: &Compactor, db: DB) {
        let upto = compactor.immutable_data.latest_level_index();
        let compactor_indexes = compactor.immutable_data.levels().indexes();

        {
            let compacted_last_seq = compactor.immutable_data.stat().last_seq;
            let oldest_active_read_seq = self.oldest_active_read_seq();

            assert!(
                oldest_active_read_seq >= compacted_last_seq,
                "replace_with_compacted requires all active reads to reach {}; oldest active read: {}",
                compacted_last_seq,
                oldest_active_read_seq,
            );
        }

        self.with_inner(|inner| {
            let mut levels = inner.immutable.levels().clone();

            info!(
                "replace_with_compacted: compacted upto {} immutable levels; my levels: {}; compacted levels: {}",
                upto.display(),
                levels.indexes().display_n(265),
                compactor_indexes.display_n(265),
            );

            if let Some(upto) = upto {
                levels.remove_levels_upto(upto);
            }

            inner.immutable = Arc::new(ImmutableData::new(levels, Some(db)));
        });

        info!("replace_with_compacted: finished replacing the db");
    }

    pub(crate) fn immutable_data(&self) -> Arc<ImmutableData> {
        self.with_inner(|inner| inner.immutable.clone())
    }
}
