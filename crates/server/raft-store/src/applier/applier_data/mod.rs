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

use std::io::Error;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use databend_meta_types::sys_data::SysData;
use log::debug;
use state_machine_api::SeqV;
use state_machine_api::StateMachineApi;

use crate::leveled_store::leveled_map::WriterPermit;
use crate::leveled_store::state_machine::view::StateMachineView;

/// A user-key change reported after a state-machine transaction commits.
pub type Change = (String, Option<SeqV>, Option<SeqV>);

pub type OnChange = Box<dyn Fn(Change) + Send + Sync>;

pub(crate) struct ApplierData {
    /// Hold a unique permit to serialize all apply operations to the state machine.
    ///
    /// Wrapping it in a Mutex to make it `Sync` while the permit itself is only `Send`.
    pub(crate) _permit: Mutex<WriterPermit>,

    pub(crate) view: StateMachineView,

    /// Transaction-local cleanup cursor, initialized from the live cursor.
    ///
    /// It is updated while applying and published only after `StateMachineView`
    /// commits, so a dropped transaction cannot advance expiration cleanup.
    pub(crate) cleanup_start_time: Mutex<Duration>,

    /// The live cleanup cursor owned by the state machine.
    ///
    /// `StateMachine::commit` replaces it with `cleanup_start_time` after committing
    /// the staged state-machine changes.
    pub(crate) cleanup_start_time_target: Arc<Mutex<Duration>>,

    pub(crate) on_change_applied: Arc<Option<OnChange>>,

    pub(crate) pending_changes: Mutex<Vec<Change>>,
}

#[async_trait::async_trait]
impl StateMachineApi<SysData> for ApplierData {
    type UserMap = StateMachineView;

    fn user_map(&self) -> &Self::UserMap {
        &self.view
    }

    fn user_map_mut(&mut self) -> &mut Self::UserMap {
        &mut self.view
    }

    type ExpireMap = StateMachineView;

    fn expire_map(&self) -> &Self::ExpireMap {
        &self.view
    }

    fn expire_map_mut(&mut self) -> &mut Self::ExpireMap {
        &mut self.view
    }

    fn on_change_applied(&mut self, change: Change) {
        self.pending_changes.lock().unwrap().push(change);
    }

    fn with_cleanup_start_timestamp<T>(&self, f: impl FnOnce(&mut Duration) -> T) -> T {
        let mut ts = self.cleanup_start_time.lock().unwrap();
        f(&mut ts)
    }

    fn with_sys_data<T>(&self, f: impl FnOnce(&mut SysData) -> T) -> T {
        self.view.with_sys_data(f)
    }

    async fn commit(self) -> Result<(), Error> {
        debug!("ApplierData::commit: start");

        let ApplierData {
            _permit,
            view,
            cleanup_start_time,
            cleanup_start_time_target,
            on_change_applied,
            pending_changes,
        } = self;

        view.commit().await?;

        *cleanup_start_time_target.lock().unwrap() = cleanup_start_time.into_inner().unwrap();
        let pending_changes = pending_changes.into_inner().unwrap();

        // Dispatch before `_permit` drops: releasing the writer permit first
        // would let the next transaction publish its events ahead of these.
        if let Some(on_change_applied) = on_change_applied.as_ref() {
            for change in pending_changes {
                on_change_applied(change);
            }
        }

        Ok(())
    }
}
