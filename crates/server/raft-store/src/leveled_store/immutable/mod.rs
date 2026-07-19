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
use std::ops::Deref;
use std::ops::RangeBounds;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use map_api::IOResultStream;
use map_api::MapKey;
use map_api::mvcc;
use seq_marked::SeqMarked;
use state_machine_api::ExpireKey;
use state_machine_api::MetaValue;
use state_machine_api::UserKey;

use crate::leveled_store::level::Level;
use crate::leveled_store::level_index::LevelIndex;

mod compact;

/// A single **immutable** level of state machine data.
#[derive(Debug, Clone)]
pub struct Immutable {
    /// An in-process unique to identify this immutable level.
    ///
    /// It is used to assert a immutable level is not replaced after compaction.
    index: LevelIndex,
    level: Arc<Level>,
}

impl Immutable {
    pub fn new(level: Arc<Level>) -> Self {
        static UNIQ: AtomicU64 = AtomicU64::new(0);

        let uniq = UNIQ.fetch_add(1, Ordering::Relaxed);
        let internal_seq = level.sys_data().curr_seq();

        let index = LevelIndex::new(internal_seq, uniq);

        Self { index, level }
    }

    pub fn new_from_level(level: Level) -> Self {
        Self::new(Arc::new(level))
    }

    pub fn new_with_index(level: Level, index: LevelIndex) -> Self {
        Self {
            index,
            level: Arc::new(level),
        }
    }

    pub fn inner(&self) -> &Arc<Level> {
        &self.level
    }

    pub fn level_index(&self) -> &LevelIndex {
        &self.index
    }

    /// Returns the total number of entries in this level, including both key-value entries and expiration entries.
    pub fn size(&self) -> u64 {
        self.level.kv.inner.len() as u64 + self.level.expire.inner.len() as u64
    }

    pub(crate) fn get_at_seq<K>(&self, key: K, snapshot_seq: u64) -> SeqMarked<K::V>
    where
        K: MapKey,
        Level: AsRef<mvcc::Table<K, K::V>>,
    {
        let table: &mvcc::Table<K, K::V> = self.level.as_ref().as_ref();
        table.get(key, snapshot_seq).cloned()
    }
}

impl AsRef<Level> for Immutable {
    fn as_ref(&self) -> &Level {
        self.level.as_ref()
    }
}

impl AsRef<mvcc::Table<UserKey, MetaValue>> for Immutable {
    fn as_ref(&self) -> &mvcc::Table<UserKey, MetaValue> {
        self.level.as_ref().as_ref()
    }
}

impl AsRef<mvcc::Table<ExpireKey, String>> for Immutable {
    fn as_ref(&self) -> &mvcc::Table<ExpireKey, String> {
        self.level.as_ref().as_ref()
    }
}

impl Deref for Immutable {
    type Target = Level;

    fn deref(&self) -> &Self::Target {
        self.level.as_ref()
    }
}

impl Immutable {
    pub(crate) async fn range_at_seq<K, R>(
        &self,
        range: R,
        snapshot_seq: u64,
    ) -> Result<IOResultStream<(K, SeqMarked<K::V>)>, Error>
    where
        K: MapKey,
        Immutable: AsRef<mvcc::Table<K, K::V>>,
        R: RangeBounds<K> + Send + Sync + Clone + 'static,
    {
        Ok(immutable_range(self.clone(), range, snapshot_seq))
    }
}

#[futures_async_stream::try_stream(boxed, ok = (K, SeqMarked<K::V>), error = Error)]
async fn immutable_range<K, R>(immutable: Immutable, range: R, snapshot_seq: u64)
where
    K: MapKey,
    Immutable: AsRef<mvcc::Table<K, K::V>>,
    R: RangeBounds<K> + Send + Sync + Clone + 'static,
{
    let table: &mvcc::Table<K, K::V> = immutable.as_ref();
    for (key, value) in table.range(range, snapshot_seq) {
        yield (key.clone(), value.cloned());
    }
}
