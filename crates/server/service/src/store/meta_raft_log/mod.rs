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

use databend_meta_raft_log::RaftLog;
use databend_meta_types::raft_types::NodeId;
use tokio::sync::RwLock;

mod impl_raft_log_storage;

/// A shared wrapper for use of RaftLog in this crate.
#[derive(Debug, Clone)]
pub struct MetaRaftLog {
    pub(crate) id: NodeId,
    inner: Arc<RwLock<RaftLog>>,
}

impl Deref for MetaRaftLog {
    type Target = Arc<RwLock<RaftLog>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl MetaRaftLog {
    pub fn new(id: NodeId, inner: RaftLog) -> Self {
        Self {
            id,
            inner: Arc::new(RwLock::new(inner)),
        }
    }
}
