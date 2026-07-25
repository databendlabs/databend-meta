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

use super::active_read_tracker::ActiveReadTracker;

/// One registered active-read sequence and the tracker that owns it.
///
/// Its `Drop` implementation decrements the sequence's active-reader count.
#[derive(Debug)]
pub(super) struct ActiveReadRegistration {
    tracker: Arc<ActiveReadTracker>,
    seq: InternalSeq,
}

impl ActiveReadRegistration {
    pub(super) fn new(tracker: Arc<ActiveReadTracker>, seq: InternalSeq) -> Self {
        Self { tracker, seq }
    }

    pub(super) fn seq(&self) -> InternalSeq {
        self.seq
    }
}

impl Drop for ActiveReadRegistration {
    fn drop(&mut self) {
        self.tracker.unregister(self.seq);
    }
}
