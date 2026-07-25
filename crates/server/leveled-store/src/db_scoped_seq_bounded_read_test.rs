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

//! Test for db_map_api_ro_impl.

use databend_meta_snapshot_db::ReadAtSeqDB;
use futures_util::TryStreamExt;
use map_api::mvcc::ViewSet;
use seq_marked::SeqMarked;
use state_machine_api::ExpireKey;
use state_machine_api::KVMeta;
use state_machine_api::UserKey;

use crate::db_builder::DBBuilder;
use crate::leveled_map::LeveledMap;

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_db_scoped_seq_bounded_read() -> anyhow::Result<()> {
    // Build two levels of kv entries and their expire index, the way an applier
    // would: the expire entry shares the seq of the kv entry it points at.
    let mut lm = {
        let lm = LeveledMap::default();

        let mut view = lm.to_view();
        view.set(user_key("a"), Some((meta(10), b("a0"))));
        view.set(ExpireKey::new(10_000, 1), Some(s("a")));
        view.set(user_key("b"), Some((meta(5), b("b0"))));
        view.set(ExpireKey::new(5_000, 2), Some(s("b")));
        view.commit().await?;

        lm.freeze_writable_without_permit();

        let mut view = lm.to_view();
        view.set(user_key("c"), Some((meta(20), b("c0"))));
        view.set(ExpireKey::new(20_000, 3), Some(s("c")));
        view.set(user_key("a"), Some((meta(15), b("a1"))));
        view.set(ExpireKey::new(10_000, 1), None);
        view.set(ExpireKey::new(15_000, 4), Some(s("a")));
        view.set(user_key("b"), None);
        view.set(ExpireKey::new(5_000, 2), None);
        view.commit().await?;

        lm
    };

    // Build a db from all data of the leveled map
    let db = {
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path();

        let db_builder = DBBuilder::new(path, "temp-db", rotbl::v001::Config::default())?;
        db_builder
            .build_from_leveled_map(&mut lm, |_| "1-1-1-1".to_string())
            .await?
    };

    // Test kv map

    let binding = ReadAtSeqDB(&db);
    let smap = binding;
    assert_eq!(
        SeqMarked::new_normal(4, (Some(KVMeta::new(Some(15), Some(0))), b("a1"))),
        smap.get_at_seq(user_key("a"), u64::MAX).await?
    );
    assert_eq!(
        SeqMarked::new_not_found(),
        smap.get_at_seq(user_key("b"), u64::MAX).await?,
        "no tombstone is stored"
    );
    assert_eq!(
        SeqMarked::new_normal(3, (Some(KVMeta::new(Some(20), Some(0))), b("c0"))),
        smap.get_at_seq(user_key("c"), u64::MAX).await?
    );
    assert_eq!(
        SeqMarked::new_not_found(),
        smap.get_at_seq(user_key("d"), u64::MAX).await?
    );

    let strm = smap.range_at_seq(UserKey::default().., u64::MAX).await?;
    let got = strm.try_collect::<Vec<_>>().await?;
    assert_eq!(
        vec![
            (
                user_key("a"),
                SeqMarked::new_normal(4, (Some(KVMeta::new(Some(15), Some(0))), b("a1"))),
            ),
            (
                user_key("c"),
                SeqMarked::new_normal(3, (Some(KVMeta::new(Some(20), Some(0))), b("c0"))),
            )
        ],
        got
    );

    // Test expire index

    let binding = ReadAtSeqDB(&db);
    let emap = binding;

    assert_eq!(
        SeqMarked::new_normal(4, s("a")),
        emap.get_at_seq(ExpireKey::new(15_000, 4), u64::MAX).await?
    );
    assert_eq!(
        SeqMarked::new_normal(3, s("c")),
        emap.get_at_seq(ExpireKey::new(20_000, 3), u64::MAX).await?
    );
    assert_eq!(
        SeqMarked::new_not_found(),
        emap.get_at_seq(ExpireKey::new(5_000, 2), u64::MAX).await?
    );

    let strm = emap.range_at_seq(ExpireKey::default().., u64::MAX).await?;
    let got = strm.try_collect::<Vec<_>>().await?;
    assert_eq!(
        vec![
            (ExpireKey::new(15_000, 4), SeqMarked::new_normal(4, s("a"))),
            (ExpireKey::new(20_000, 3), SeqMarked::new_normal(3, s("c"))),
        ],
        got
    );

    Ok(())
}

fn s(x: impl ToString) -> String {
    x.to_string()
}
fn meta(expire_at_sec: u64) -> Option<KVMeta> {
    Some(KVMeta::new(Some(expire_at_sec), Some(0)))
}
fn b(x: impl ToString) -> Vec<u8> {
    x.to_string().as_bytes().to_vec()
}
fn user_key(s: impl ToString) -> UserKey {
    UserKey::new(s)
}
