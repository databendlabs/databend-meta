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

//! Write data staged by a state-machine view.

use std::sync::Mutex;

use databend_meta_types::sys_data::SysData;
use map_api::mvcc;
use state_machine_api::ExpireKey;
use state_machine_api::MetaValue;
use state_machine_api::UserKey;

use crate::leveled_store::level::Level;

/// State-machine data staged by a view until it commits.
///
/// The user and expiration tables share `sys_data.sequence` as their sequence
/// allocator, and all three are published together by `LeveledMap::commit`.
pub(super) struct Changes {
    pub(super) sys_data: Mutex<SysData>,
    user: mvcc::Table<UserKey, MetaValue>,
    expire: mvcc::Table<ExpireKey, String>,
}

impl Changes {
    pub(super) fn new(sys_data: SysData) -> Self {
        Self {
            sys_data: Mutex::new(sys_data),
            user: Default::default(),
            expire: Default::default(),
        }
    }

    pub(super) fn into_level(self) -> Level {
        Level {
            sys_data: self.sys_data.into_inner().unwrap(),
            kv: self.user,
            expire: self.expire,
        }
    }
}

impl AsRef<mvcc::Table<UserKey, MetaValue>> for Changes {
    fn as_ref(&self) -> &mvcc::Table<UserKey, MetaValue> {
        &self.user
    }
}

impl AsMut<mvcc::Table<UserKey, MetaValue>> for Changes {
    fn as_mut(&mut self) -> &mut mvcc::Table<UserKey, MetaValue> {
        &mut self.user
    }
}

impl AsRef<mvcc::Table<ExpireKey, String>> for Changes {
    fn as_ref(&self) -> &mvcc::Table<ExpireKey, String> {
        &self.expire
    }
}

impl AsMut<mvcc::Table<ExpireKey, String>> for Changes {
    fn as_mut(&mut self) -> &mut mvcc::Table<ExpireKey, String> {
        &mut self.expire
    }
}
