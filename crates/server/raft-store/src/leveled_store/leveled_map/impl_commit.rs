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

use log::info;

use crate::leveled_store::level::Level;
use crate::leveled_store::leveled_map::LeveledMap;

impl LeveledMap {
    pub(crate) fn commit(&self, changes: Level) {
        info!(
            "Committing changes to leveled map data: last_seq={}",
            changes.sys_data.curr_seq()
        );

        let mut inner = self.data.lock().unwrap();

        // `Table::apply` below asserts seq monotonicity, but only for non-empty
        // tables; a stale view would otherwise silently rewind sys_data.
        assert!(
            inner.writable.sys_data.curr_seq() <= changes.sys_data.curr_seq(),
            "commit must not rewind sys_data: committed seq {} > staged seq {}; the view predates another commit",
            inner.writable.sys_data.curr_seq(),
            changes.sys_data.curr_seq(),
        );

        if !changes.kv.inner.is_empty() {
            inner.writable.kv.apply(changes.kv);
        }
        if !changes.expire.inner.is_empty() {
            inner.writable.expire.apply(changes.expire);
        }
        inner.writable.sys_data = changes.sys_data;
    }
}

#[cfg(test)]
mod tests {
    use databend_meta_types::sys_data::SysData;
    use map_api::mvcc::Table;
    use seq_marked::SeqMarked;
    use state_machine_api::ExpireKey;
    use state_machine_api::UserKey;

    use super::*;

    fn user_key(key: &str) -> UserKey {
        UserKey::new(key)
    }

    fn changes(sys_data: SysData) -> Level {
        Level {
            sys_data,
            kv: Table::new(),
            expire: Table::new(),
        }
    }

    #[test]
    fn test_commit_publishes_typed_tables_and_sys_data() {
        let map = LeveledMap::default();
        let mut sys_data = SysData::default();
        sys_data.update_seq(2);

        let mut changes = changes(sys_data.clone());
        changes
            .kv
            .insert(user_key("user"), 1, (None, b"value".to_vec()))
            .unwrap();
        changes
            .expire
            .insert(ExpireKey::new(100, 1), 2, "user".to_string())
            .unwrap();

        map.commit(changes);

        let data = map.data.lock().unwrap();
        assert_eq!(data.writable.sys_data, sys_data);
        assert_eq!(
            data.writable.kv.get(user_key("user"), 2).cloned(),
            SeqMarked::new_normal(1, (None, b"value".to_vec()))
        );
        assert_eq!(
            data.writable.expire.get(ExpireKey::new(100, 1), 2).cloned(),
            SeqMarked::new_normal(2, "user".to_string())
        );
        assert_eq!(data.writable.kv.inner.len(), 1);
        assert_eq!(data.writable.expire.inner.len(), 1);
    }

    #[test]
    fn test_commit_publishes_sys_data_without_table_changes() {
        let map = LeveledMap::default();
        let mut sys_data = SysData::default();
        sys_data.update_seq(7);

        map.commit(changes(sys_data.clone()));

        let data = map.data.lock().unwrap();
        assert_eq!(data.writable.sys_data, sys_data);
        assert!(data.writable.kv.inner.is_empty());
        assert!(data.writable.expire.inner.is_empty());
    }

    #[test]
    #[should_panic(expected = "commit must not rewind sys_data")]
    fn test_commit_rejects_sys_data_from_a_stale_view() {
        let map = LeveledMap::default();
        let mut sys_data = SysData::default();
        sys_data.update_seq(7);
        map.commit(changes(sys_data));

        let mut stale = SysData::default();
        stale.update_seq(3);
        map.commit(changes(stale));
    }
}
