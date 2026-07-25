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

use std::fmt;
use std::fmt::Formatter;
use std::io;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use databend_meta_leveled_store::leveled_map::CompactorPermit;
use databend_meta_leveled_store::leveled_map::LeveledMap;
use databend_meta_leveled_store::leveled_map::WriterPermit;
use databend_meta_leveled_store::leveled_map::compactor::Compactor;
use databend_meta_leveled_store::state_machine::read_view::StateMachineReadView;
use databend_meta_snapshot_db::DB;
use databend_meta_types::raft_types::EntryResponder;
use databend_meta_types::sys_data::SysData;
use futures::Stream;
use futures::StreamExt;
use state_machine_api::SeqV;
use state_machine_api::UserKey;

use crate::applier::Applier;
use crate::applier::applier_data::ApplierData;
use crate::applier::applier_data::OnChange;
use crate::state_machine::kv_api::StateMachineKVApi;

pub struct StateMachine {
    leveled_map: LeveledMap,

    /// Since when to start cleaning expired keys.
    cleanup_start_time: Arc<Mutex<Duration>>,

    /// Callback when a change is applied to state machine
    pub(crate) on_change_applied: Arc<Mutex<Arc<Option<OnChange>>>>,
}

impl Default for StateMachine {
    fn default() -> Self {
        Self {
            leveled_map: Default::default(),
            cleanup_start_time: Arc::new(Mutex::new(Duration::ZERO)),
            on_change_applied: Arc::new(Mutex::new(Arc::new(None))),
        }
    }
}

impl fmt::Debug for StateMachine {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("StateMachine")
            .field("levels", &self.leveled_map)
            .field(
                "on_change_applied",
                &self
                    .get_on_change_applied()
                    .as_ref()
                    .as_ref()
                    .map(|_x| "is_set"),
            )
            .finish()
    }
}

impl StateMachine {
    /// Return a mutable reference to the map that stores app data.
    pub(crate) fn map_mut(&mut self) -> &mut LeveledMap {
        &mut self.leveled_map
    }

    pub fn to_read_view(&self) -> StateMachineReadView {
        self.leveled_map.to_read_view()
    }

    pub fn kv_api(&self) -> StateMachineKVApi<'_> {
        StateMachineKVApi { sm: self }
    }

    /// Install and replace state machine with the content of a snapshot.
    pub fn install_snapshot(&self, db: DB) -> StateMachine {
        let sys_data = db.sys_data().clone();

        let new_sm = StateMachine {
            leveled_map: self.leveled_map.install_persisted(db),
            cleanup_start_time: self.cleanup_start_time.clone(),
            on_change_applied: self.on_change_applied.clone(),
        };

        new_sm.leveled_map.with_sys_data(|s| *s = sys_data);

        new_sm
    }

    pub fn get_snapshot(&self) -> Option<DB> {
        self.leveled_map.persisted()
    }

    pub fn data(&self) -> &LeveledMap {
        &self.leveled_map
    }

    pub async fn get_maybe_expired_kv(&self, key: &str) -> Result<Option<SeqV>, io::Error> {
        let view = self.data().to_read_view();
        let got = view.get(UserKey::new(key.to_string())).await?;
        let seqv = Into::<Option<SeqV>>::into(got);
        Ok(seqv)
    }

    pub(crate) async fn new_applier(&self) -> Applier<ApplierData> {
        let permit = self.acquire_writer_permit().await;

        let view = self.leveled_map.to_view();
        let applier_data = ApplierData {
            _permit: Mutex::new(permit),
            view,
            cleanup_start_time: Mutex::new(*self.cleanup_start_time.lock().unwrap()),
            cleanup_start_time_target: self.cleanup_start_time.clone(),
            on_change_applied: self.get_on_change_applied(),
            pending_changes: Mutex::new(Vec::new()),
        };

        Applier::new(applier_data)
    }

    pub fn get_on_change_applied(&self) -> Arc<Option<OnChange>> {
        self.on_change_applied.lock().unwrap().clone()
    }

    /// Acquire exclusive writer permit for state machine operations.
    ///
    /// See [`LeveledMap::acquire_writer_permit`].
    pub async fn acquire_writer_permit(&self) -> WriterPermit {
        self.leveled_map.acquire_writer_permit().await
    }

    pub(crate) async fn acquire_compaction_and_writer(
        &self,
        name: impl ToString,
    ) -> (CompactorPermit, WriterPermit) {
        self.leveled_map.acquire_compaction_and_writer(name).await
    }

    /// Apply entries from the stream to the state machine.
    ///
    /// Responses are only sent AFTER commit succeeds to ensure consistency.
    /// If we sent responses before commit, clients would consider their operations complete
    /// and might immediately read the state machine, but the data wouldn't be persisted yet.
    /// This could lead to clients observing stale or missing data.
    pub async fn apply_entries<S>(&self, mut entries: S) -> Result<(), io::Error>
    where S: Stream<Item = Result<EntryResponder, io::Error>> + Unpin {
        let mut applier = self.new_applier().await;
        let mut pending_responses = Vec::new();

        while let Some(result) = entries.next().await {
            let (entry, responder) = result?;
            let applied = applier.apply(&entry).await?;
            if let Some(responder) = responder {
                pending_responses.push((responder, applied));
            }
        }

        applier.commit().await?;

        for (responder, applied) in pending_responses {
            responder.send(applied);
        }

        Ok(())
    }

    pub fn sys_data(&self) -> SysData {
        self.leveled_map.with_sys_data(|x| x.clone())
    }

    pub fn into_leveled_map(self) -> LeveledMap {
        self.leveled_map
    }

    pub fn leveled_map(&self) -> &LeveledMap {
        &self.leveled_map
    }

    pub fn levels_mut(&mut self) -> &mut LeveledMap {
        self.map_mut()
    }

    pub fn set_on_change_applied(&self, on_change_applied: OnChange) {
        let mut g = self.on_change_applied.lock().unwrap();
        *g = Arc::new(Some(on_change_applied));
    }

    /// Get a singleton `Compactor` instance specific to `self`.
    pub async fn acquire_compactor(&self, name: impl ToString) -> Compactor {
        self.leveled_map.acquire_compactor(name).await
    }

    /// Acquire permits in canonical order, freeze the writable level, and
    /// return a compactor that retains the compaction permit.
    pub async fn freeze_writable_for_compaction(&self, name: impl ToString) -> Compactor {
        self.leveled_map.freeze_writable_for_compaction(name).await
    }
}
