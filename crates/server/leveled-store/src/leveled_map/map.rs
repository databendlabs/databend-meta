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
use std::time::Duration;
use std::time::Instant;

use databend_meta_snapshot_db::DB;
use databend_meta_types::Node;
use databend_meta_types::raft_types::LogId;
use databend_meta_types::raft_types::NodeId;
use databend_meta_types::raft_types::StoredMembership;
use databend_meta_types::sys_data::SysData;
use display_more::DisplayOptionExt;
use display_more::DisplaySliceExt;
use log::info;
use log::warn;
use seq_marked::InternalSeq;
use tokio::sync::Semaphore;

use super::ActiveReadGuard;
use super::CompactorPermit;
use super::WriterPermit;
use super::active_read_tracker::ActiveReadTracker;
use super::compactor::Compactor;
use super::compactor_acquirer::CompactorAcquirer;
use super::writer_acquirer::WriterAcquirer;
use crate::immutable::Immutable;
use crate::immutable_data::ImmutableData;
use crate::immutable_levels::ImmutableLevels;
use crate::level::LevelStat;
use crate::leveled_map::leveled_map_data::LeveledMapData;
use crate::state_machine::read_view::StateMachineReadView;
use crate::state_machine::view::StateMachineView;

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

    /// A semaphore that permits at most one compactor to run.
    compaction_semaphore: Arc<Semaphore>,

    /// Semaphore for exclusive write access to this map.
    ///
    /// Capacity is 1, ensuring only one writer at a time. This achieves serialization for:
    /// - Applying Raft log entries to state machine
    /// - Setting up watch streams with atomic snapshot reads
    /// - Any operation requiring consistent state machine view
    ///
    /// Historical context: Inserting tombstones does not increase the seq,
    /// so MVCC isolation with seq alone cannot completely separate concurrent writers.
    /// This semaphore provides the necessary serialization.
    write_semaphore: Arc<Semaphore>,
}

impl Default for LeveledMap {
    fn default() -> Self {
        Self {
            data: Arc::new(Mutex::new(LeveledMapData::default())),
            active_reads: Default::default(),
            // Only one compactor is allowed a time.
            compaction_semaphore: Arc::new(Semaphore::new(1)),
            // Only one writer is allowed a time.
            write_semaphore: Arc::new(Semaphore::new(1)),
        }
    }
}

impl LeveledMap {
    /// Return a map holding only `db`, sharing this map's permit semaphores.
    ///
    /// Installing a snapshot swaps in a fresh map, but the semaphores must stay
    /// the same objects: a writer or compactor still holding a permit from the
    /// replaced map has to keep excluding those of the new one.
    pub fn install_persisted(&self, db: DB) -> Self {
        Self {
            data: Arc::new(Mutex::new(LeveledMapData {
                writable: Default::default(),
                immutable: Arc::new(ImmutableData::new(Default::default(), Some(db))),
            })),
            active_reads: Default::default(),
            compaction_semaphore: self.compaction_semaphore.clone(),
            write_semaphore: self.write_semaphore.clone(),
        }
    }

    pub fn to_view(&self) -> StateMachineView {
        StateMachineView::from_leveled_map(self)
    }

    pub fn new_active_read_guard(&self) -> ActiveReadGuard {
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
    ///
    /// The boundary is the newest immutable level's last sequence. Merging two
    /// older levels would be safe with an older boundary; one conservative
    /// boundary is used for every compaction shape for simplicity.
    ///
    /// The wait is unbounded: a read view held open by a slow consumer, e.g. a
    /// client streaming `get_many_kv`, delays compaction until it is dropped.
    /// A warning is logged periodically while blocked.
    pub async fn wait_for_active_reads_before_compaction(&self) {
        /// Interval at which a blocked compaction reports the read holding it up.
        const WARN_INTERVAL: Duration = Duration::from_secs(10);

        let compaction_boundary = self.immutable_data().last_seq();

        let mut wait = std::pin::pin!(
            self.active_reads
                .wait_for_oldest_seq_at_least(compaction_boundary)
        );

        let start = Instant::now();
        loop {
            match tokio::time::timeout(WARN_INTERVAL, &mut wait).await {
                Ok(()) => return,
                Err(_elapsed) => warn!(
                    "compaction blocked for {:?}, waiting for active reads to reach seq {}; oldest active read seq: {}",
                    start.elapsed(),
                    compaction_boundary,
                    self.oldest_active_read_seq(),
                ),
            }
        }
    }

    pub fn with_sys_data<T>(&self, f: impl FnOnce(&mut SysData) -> T) -> T {
        self.with_inner(|inner| inner.writable.with_sys_data(f))
    }

    /// Acquire exclusive writer permit for this map.
    ///
    /// This returns a permit that grants exclusive write access to the map.
    /// The permit is backed by a semaphore with capacity 1, ensuring only one writer
    /// can hold the permit at a time.
    ///
    /// This is used to serialize operations that require atomicity across multiple steps,
    /// such as:
    /// - Applying Raft log entries
    /// - Setting up watch streams with consistent snapshot reads
    /// - Any operation that needs to prevent concurrent state machine modifications
    ///
    /// The permit is automatically released when dropped.
    pub async fn acquire_writer_permit(&self) -> WriterPermit {
        WriterAcquirer::new(self.write_semaphore.clone())
            .acquire()
            .await
    }

    pub async fn acquire_compactor_permit(&self, name: impl ToString) -> CompactorPermit {
        CompactorAcquirer::new(self.compaction_semaphore.clone(), name)
            .acquire()
            .await
    }

    /// Acquire the compactor and writer permits in canonical order.
    pub async fn acquire_compaction_and_writer(
        &self,
        name: impl ToString,
    ) -> (CompactorPermit, WriterPermit) {
        let compactor_permit = self.acquire_compactor_permit(name).await;
        let writer_permit = self.acquire_writer_permit().await;

        (compactor_permit, writer_permit)
    }

    /// Get a singleton `Compactor` instance specific to `self`.
    pub async fn acquire_compactor(&self, name: impl ToString) -> Compactor {
        let permit = self.acquire_compactor_permit(name).await;
        self.new_compactor(permit)
    }

    /// Acquire permits in canonical order, freeze the writable level, and
    /// return a compactor that retains the compaction permit.
    pub async fn freeze_writable_for_compaction(&self, name: impl ToString) -> Compactor {
        let (mut compactor_permit, mut writer_permit) =
            self.acquire_compaction_and_writer(name).await;

        self.freeze_writable(&mut writer_permit, &mut compactor_permit);

        drop(writer_permit);

        self.new_compactor(compactor_permit)
    }

    fn new_compactor(&self, permit: CompactorPermit) -> Compactor {
        let immutable_data = self.immutable_data();

        info!(
            "new_compactor: with immutable data: {}",
            immutable_data.stat()
        );

        Compactor::new(permit, immutable_data)
    }

    /// Freeze the current writable level and create a new empty writable level.
    ///
    /// Need writer permit to reset the writable level, and compactor permit to add a new immutable level.
    pub fn freeze_writable(
        &self,
        _writer_permit: &mut WriterPermit,
        _compactor_permit: &mut CompactorPermit,
    ) {
        self.do_freeze_writable()
    }

    /// For testing, requires no permit.
    #[cfg(any(test, feature = "testing"))]
    pub fn freeze_writable_without_permit(&self) {
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

    pub fn to_read_view(&self) -> StateMachineReadView {
        StateMachineReadView::new(self)
    }

    pub fn curr_seq(&self) -> u64 {
        self.with_sys_data(|s| s.curr_seq())
    }

    pub fn last_membership(&self) -> StoredMembership {
        self.with_sys_data(|s| s.last_membership_ref().clone())
    }

    pub fn last_applied(&self) -> Option<LogId> {
        self.with_sys_data(|s| *s.last_applied_ref())
    }

    pub fn nodes(&self) -> BTreeMap<NodeId, Node> {
        self.with_sys_data(|s| s.nodes_ref().clone())
    }

    // TODO: rename:
    pub fn with_inner<T>(&self, f: impl FnOnce(&mut LeveledMapData) -> T) -> T {
        let mut inner = self.data.lock().unwrap();
        f(&mut inner)
    }

    /// Replace all immutable levels with the given one.
    ///
    /// This installs the result of in-memory compaction. The caller must hold
    /// the compactor permit, and must first wait until no active read predates
    /// the new levels; see [`Self::wait_for_active_reads_before_compaction`].
    pub fn replace_immutable_levels(&self, b: ImmutableLevels) {
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

    pub fn immutable_data(&self) -> Arc<ImmutableData> {
        self.with_inner(|inner| inner.immutable.clone())
    }
}
