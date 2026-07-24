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

use std::sync::Arc;

use seq_marked::InternalSeq;

use super::active_read_registration::ActiveReadRegistration;
use super::active_read_tracker::ActiveReadTracker;

/// Keeps an active-read sequence registered for the lifetime of its owner.
///
/// Clones share one registration; the sequence is removed after the final
/// clone is dropped.
#[derive(Clone, Debug)]
pub(crate) struct ActiveReadGuard {
    registration: Arc<ActiveReadRegistration>,
}

impl ActiveReadGuard {
    pub(super) fn new(tracker: Arc<ActiveReadTracker>, seq: InternalSeq) -> Self {
        Self {
            registration: Arc::new(ActiveReadRegistration::new(tracker, seq)),
        }
    }

    pub(crate) fn seq(&self) -> u64 {
        *self.registration.seq()
    }
}
