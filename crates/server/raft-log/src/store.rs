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

use std::ops::Deref;
use std::sync::Arc;

use databend_meta_types::raft_types::NodeId;
use tokio::sync::RwLock;

use crate::RaftLog;

/// A shared [`RaftLog`] handle implementing openraft's `RaftLogStorage`.
///
/// Cloning shares one WAL: `get_log_reader` hands openraft a clone, so readers
/// and the writer contend on the same lock.
#[derive(Debug, Clone)]
pub struct RaftLogStore {
    pub(crate) id: NodeId,
    inner: Arc<RwLock<RaftLog>>,
}

impl Deref for RaftLogStore {
    type Target = Arc<RwLock<RaftLog>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl RaftLogStore {
    pub fn new(id: NodeId, inner: RaftLog) -> Self {
        Self {
            id,
            inner: Arc::new(RwLock::new(inner)),
        }
    }
}
