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

use seq_marked::InternalSeq;
use tokio::sync::Notify;

use super::ActiveReadGuard;

/// Tracks the sequence boundaries of live state-machine views and read views.
///
/// Compaction uses the smallest registered sequence to determine when it can
/// discard versions that are visible to an active reader.
#[derive(Debug, Default)]
pub(super) struct ActiveReadTracker {
    /// Maps each read sequence to its number of independent registrations.
    ///
    /// The count permits several readers at the same sequence; the key is
    /// removed only when their final registrations are dropped.
    active: Mutex<BTreeMap<InternalSeq, usize>>,

    /// Wakes compactions that are waiting for an older read to finish.
    changed: Notify,
}

impl ActiveReadTracker {
    pub(super) fn register(self: &Arc<Self>, seq: InternalSeq) -> ActiveReadGuard {
        let mut active = self.active.lock().unwrap();
        *active.entry(seq).or_default() += 1;
        ActiveReadGuard::new(self.clone(), seq)
    }

    pub(super) fn oldest_seq(&self) -> InternalSeq {
        self.active
            .lock()
            .unwrap()
            .first_key_value()
            .map(|(seq, _)| *seq)
            .unwrap_or_else(|| InternalSeq::new(u64::MAX))
    }

    /// Wait until no active read can observe data older than `seq`.
    pub(super) async fn wait_for_oldest_seq_at_least(&self, seq: InternalSeq) {
        loop {
            // Register before checking the condition so a concurrent guard drop
            // cannot be missed between the check and the wait.
            let notified = self.changed.notified();
            if self.oldest_seq() >= seq {
                return;
            }
            notified.await;
        }
    }

    pub(super) fn unregister(&self, seq: InternalSeq) {
        let mut active = self.active.lock().unwrap();
        let count = active.get_mut(&seq).unwrap();
        *count -= 1;
        if *count == 0 {
            active.remove(&seq);
        }
        drop(active);

        self.changed.notify_waiters();
    }
}
