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

use std::io;
use std::sync::Arc;

use databend_meta_snapshot_db::DB;
use databend_meta_types::sys_data::SysData;
use map_api::IOResultStream;
use rotbl::v001::SeqMarked;

use crate::leveled_store::immutable_data::ImmutableData;
use crate::leveled_store::immutable_levels::ImmutableLevels;
use crate::sm_v003::compactor_acquirer::CompactorPermit;

/// Compactor is responsible for compacting the immutable levels and db.
///
/// Only one Compactor can be running at a time.
#[derive(Debug)]
pub struct Compactor {
    /// Acquired permit for this compactor.
    ///
    /// This is used to ensure that only one compactor can run at a time.
    _permit: CompactorPermit,
    pub(crate) immutable_data: Arc<ImmutableData>,
}

impl Compactor {
    pub(crate) fn new(permit: CompactorPermit, immutable_data: Arc<ImmutableData>) -> Self {
        Self {
            _permit: permit,
            immutable_data,
        }
    }

    pub fn immutable_data(&self) -> Arc<ImmutableData> {
        self.immutable_data.clone()
    }

    pub fn immutable_levels(&self) -> ImmutableLevels {
        self.immutable_data.levels().clone()
    }

    pub fn db(&self) -> Option<DB> {
        self.immutable_data.persisted().cloned()
    }

    /// Compacted all data into a stream.
    ///
    /// Tombstones are removed because no more compact with lower levels.
    ///
    /// It returns a small chunk of sys data that is always copied across levels,
    /// and a stream contains `kv` and `expire` entries.
    ///
    /// The exported stream contains encoded `String` key and rotbl value [`SeqMarked`]
    pub async fn compact_into_stream(
        &mut self,
    ) -> Result<(SysData, IOResultStream<(String, SeqMarked)>), io::Error> {
        self.immutable_data.compact_into_stream().await
    }
}
