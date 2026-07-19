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

use std::future;
use std::io;
use std::io::Error;
use std::ops::Deref;
use std::ops::RangeBounds;

use futures_util::StreamExt;
use futures_util::TryStreamExt;
use map_api::IOResultStream;
use map_api::mvcc;
use seq_marked::SeqMarked;
use state_machine_api::ExpireKey;
use state_machine_api::MetaValue;
use state_machine_api::UserKey;

use crate::leveled_store::leveled_map::LeveledMap;
use crate::leveled_store::types::Key;

pub(crate) type MvccSnapshot = mvcc::Snapshot<Key, LeveledMap>;

/// A wrapper of mvcc::Snapshot to implement additional traits
#[derive(Clone, Debug)]
pub struct StateMachineSnapshot {
    inner: MvccSnapshot,
}

impl Deref for StateMachineSnapshot {
    type Target = MvccSnapshot;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[async_trait::async_trait]
impl mvcc::ViewGet<UserKey> for StateMachineSnapshot {
    async fn get(&self, key: UserKey) -> Result<SeqMarked<MetaValue>, io::Error> {
        StateMachineSnapshot::get(self, key).await
    }
}

#[async_trait::async_trait]
impl mvcc::ViewRange<UserKey> for StateMachineSnapshot {
    async fn range<R>(
        &self,
        range: R,
    ) -> Result<IOResultStream<(UserKey, SeqMarked<MetaValue>)>, io::Error>
    where
        R: RangeBounds<UserKey> + Send + Sync + Clone + 'static,
    {
        let start = range.start_bound().cloned();
        let end = range.end_bound().cloned();

        let start = start.map(Key::User);
        let end = end.map(Key::User);

        let strm = self.inner.range((start, end)).await?;

        Ok(strm
            .try_filter_map(|(key, value)| {
                future::ready(Ok(match (key, value) {
                    (Key::User(key), value) => Some((key, value.map(|x| x.into_user()))),
                    (Key::Expire(_), _) => None,
                }))
            })
            .boxed())
    }
}

#[async_trait::async_trait]
impl mvcc::ViewGet<ExpireKey> for StateMachineSnapshot {
    async fn get(&self, key: ExpireKey) -> Result<SeqMarked<String>, io::Error> {
        StateMachineSnapshot::get_expire(self, key).await
    }
}

#[async_trait::async_trait]
impl mvcc::ViewRange<ExpireKey> for StateMachineSnapshot {
    async fn range<R>(
        &self,
        range: R,
    ) -> Result<IOResultStream<(ExpireKey, SeqMarked<String>)>, io::Error>
    where
        R: RangeBounds<ExpireKey> + Send + Sync + Clone + 'static,
    {
        let start = range.start_bound().cloned();
        let end = range.end_bound().cloned();

        let start = start.map(Key::Expire);
        let end = end.map(Key::Expire);

        let strm = self.inner.range((start, end)).await?;

        Ok(strm
            .try_filter_map(|(key, value)| {
                future::ready(Ok(match (key, value) {
                    (Key::User(_), _) => None,
                    (Key::Expire(key), value) => Some((key, value.map(|x| x.into_expire()))),
                }))
            })
            .boxed())
    }
}

impl StateMachineSnapshot {
    pub async fn get(&self, key: UserKey) -> Result<SeqMarked<MetaValue>, Error> {
        let v = self.inner.get(Key::User(key)).await?;
        Ok(v.map(|x| x.into_user()))
    }

    pub async fn get_expire(&self, key: ExpireKey) -> Result<SeqMarked<String>, Error> {
        let v = self.inner.get(Key::Expire(key)).await?;
        Ok(v.map(|x| x.into_expire()))
    }

    pub fn new(inner: MvccSnapshot) -> Self {
        Self { inner }
    }
}
