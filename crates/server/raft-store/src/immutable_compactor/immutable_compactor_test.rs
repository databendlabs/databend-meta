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
use std::time::Duration;

use databend_meta_types::UpsertKV;
use futures_util::TryStreamExt;
use map_api::mvcc::RangeAtSeq;
use map_api::mvcc::ViewRange;
use pretty_assertions::assert_eq;
use seq_marked::SeqMarked;
use state_machine_api::ExpireKey;
use state_machine_api::KVMeta;
use state_machine_api::MetaValue;
use state_machine_api::UserKey;
use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::immutable_compactor::InMemoryCompactor;
use crate::leveled_store::leveled_map::LeveledMap;
use crate::state_machine::StateMachine;

/// Failure bound for an operation that is expected to finish.
const MUST_FINISH: Duration = Duration::from_secs(5);

/// Window during which an operation expected to block must not finish.
const MUST_BLOCK: Duration = Duration::from_millis(500);

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_compact_skips_when_too_few_levels() -> anyhow::Result<()> {
    let sm = Arc::new(StateMachine::default());
    write(&sm, &[("a", "a0"), ("b", "b0")]).await?;

    let compactor = InMemoryCompactor::new(sm.clone(), "few").await;
    assert_eq!(compactor.name(), "few");
    assert_eq!(compactor.to_string(), "InMemoryCompactor(few)");
    assert_eq!(stat_str(compactor.leveled_map()), vec![
        "[writable](user=2, expire=0)"
    ]);

    let immutable_compactor = compactor.freeze();
    assert_eq!(
        immutable_compactor.to_string(),
        "ImmutableCompactor(few)",
        "the name survives the freeze"
    );

    let lm = sm.leveled_map().clone();
    assert_eq!(
        stat_str(&lm),
        vec!["[writable](user=0, expire=0)", "(user=2, expire=0)"],
        "freeze moves the writable level into the immutable levels"
    );

    let indexes = lm.immutable_levels().indexes();
    let kv = kv_entries(&lm).await?;

    immutable_compactor.compact().await;

    assert_eq!(
        lm.immutable_levels().indexes(),
        indexes,
        "a single level is below the compaction threshold"
    );
    assert_eq!(kv_entries(&lm).await?, kv);
    assert_eq!(kv, vec![
        (
            user_key("a"),
            SeqMarked::new_normal(1, (meta(None), b("a0")))
        ),
        (
            user_key("b"),
            SeqMarked::new_normal(2, (meta(None), b("b0")))
        ),
    ]);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_compact_merges_the_smallest_adjacent_levels() -> anyhow::Result<()> {
    let sm = Arc::new(StateMachine::default());
    build_compactable(&sm).await?;

    let lm = sm.leveled_map().clone();
    let kv = kv_entries(&lm).await?;
    let expire = expire_entries(&lm).await?;
    assert_eq!(kv, expected_kv());
    assert_eq!(expire, expected_expire());

    let immutable_compactor = InMemoryCompactor::new(sm.clone(), "merge").await.freeze();
    let indexes = lm.immutable_levels().indexes();
    assert_eq!(
        indexes.len(),
        6,
        "5 written levels plus the frozen writable"
    );

    immutable_compactor.compact().await;

    assert_eq!(
        lm.immutable_levels().indexes(),
        vec![indexes[0], indexes[1], indexes[2], indexes[3], indexes[5]],
        "the two smallest adjacent levels merge into the newer level index"
    );
    assert_eq!(stat_str(&lm), vec![
        "[writable](user=0, expire=0)",
        "(user=1, expire=0)",
        "(user=1, expire=0)",
        "(user=1, expire=0)",
        "(user=1, expire=0)",
        "(user=5, expire=1)",
    ]);

    assert_eq!(kv_entries(&lm).await?, kv, "compaction preserves every key");
    assert_eq!(
        expire_entries(&lm).await?,
        expire,
        "compaction preserves the expiration index"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_compact_waits_for_a_read_older_than_the_boundary() -> anyhow::Result<()> {
    let sm = Arc::new(StateMachine::default());
    build_compactable(&sm).await?;

    // Registered before the write that the compactor freezes below it.
    let old_view = sm.to_read_view();
    assert_eq!(old_view.seq(), 9);

    write(&sm, &[("j", "j0")]).await?;

    let lm = sm.leveled_map().clone();
    let immutable_compactor = InMemoryCompactor::new(sm.clone(), "wait").await.freeze();
    let indexes = lm.immutable_levels().indexes();

    let (tx, mut rx) = mpsc::channel::<()>(1);
    let compacting = tokio::spawn(async move {
        immutable_compactor.compact().await;
        tx.send(()).await.unwrap();
    });

    assert!(
        timeout(MUST_BLOCK, rx.recv()).await.is_err(),
        "compaction must not proceed while an older read is registered"
    );
    assert_eq!(
        lm.immutable_levels().indexes(),
        indexes,
        "the levels are untouched while compaction waits"
    );

    drop(old_view);

    timeout(MUST_FINISH, rx.recv()).await?.unwrap();
    compacting.await?;

    assert_eq!(
        lm.immutable_levels().indexes(),
        vec![indexes[0], indexes[1], indexes[2], indexes[3], indexes[5]],
        "releasing the old read lets compaction merge two levels"
    );

    let mut expected = expected_kv();
    expected.push((
        user_key("j"),
        SeqMarked::new_normal(10, (meta(None), b("j0"))),
    ));
    assert_eq!(kv_entries(&lm).await?, expected);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_compact_does_not_wait_for_a_read_at_the_boundary() -> anyhow::Result<()> {
    let sm = Arc::new(StateMachine::default());
    build_compactable(&sm).await?;

    let lm = sm.leveled_map().clone();
    let immutable_compactor = InMemoryCompactor::new(sm.clone(), "boundary")
        .await
        .freeze();

    // Registered at the compaction boundary: it stays alive across the
    // compaction without holding it up.
    let boundary_view = sm.to_read_view();
    assert_eq!(boundary_view.seq(), 9);

    write(&sm, &[("a", "a1"), ("j", "j0")]).await?;
    let current_view = sm.to_read_view();
    assert_eq!(current_view.seq(), 11);

    timeout(MUST_FINISH, immutable_compactor.compact()).await?;

    assert_eq!(
        lm.immutable_levels().indexes().len(),
        5,
        "a read at the boundary does not hold up compaction"
    );

    let got = boundary_view
        .range(UserKey::default()..)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(got, expected_kv(), "the boundary read is unaffected");

    let mut expected_current = expected_kv();
    expected_current[0].1 = SeqMarked::new_normal(10, (meta(None), b("a1")));
    expected_current.push((
        user_key("j"),
        SeqMarked::new_normal(11, (meta(None), b("j0"))),
    ));

    let got = current_view
        .range(UserKey::default()..)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(
        got, expected_current,
        "the current read sees the new writes"
    );

    Ok(())
}

/// Apply `kvs` to the state machine as one committed batch.
async fn write(sm: &StateMachine, kvs: &[(&str, &str)]) -> anyhow::Result<()> {
    let mut applier = sm.new_applier().await;
    for (k, v) in kvs {
        applier
            .upsert_kv(&UpsertKV::update(*k, v.as_bytes()))
            .await?;
    }
    applier.commit().await?;
    Ok(())
}

/// Build immutable levels that satisfy [`ImmutableLevels::need_compact`]: a
/// 6-entry bottom level below four single-entry levels, and an empty writable
/// level for the caller's compactor to freeze.
///
/// [`ImmutableLevels::need_compact`]: crate::leveled_store::immutable_levels::ImmutableLevels::need_compact
async fn build_compactable(sm: &Arc<StateMachine>) -> anyhow::Result<()> {
    let mut applier = sm.new_applier().await;
    applier
        .upsert_kv(&UpsertKV::update("a", b"a0").with_expire_sec(10))
        .await?;
    for (k, v) in [("b", "b0"), ("c", "c0"), ("d", "d0"), ("e", "e0")] {
        applier
            .upsert_kv(&UpsertKV::update(k, v.as_bytes()))
            .await?;
    }
    applier.commit().await?;

    for (k, v) in [("f", "f0"), ("g", "g0"), ("h", "h0"), ("i", "i0")] {
        sm.leveled_map().freeze_writable_without_permit();
        write(sm, &[(k, v)]).await?;
    }
    sm.leveled_map().freeze_writable_without_permit();

    Ok(())
}

fn expected_kv() -> Vec<(UserKey, SeqMarked<MetaValue>)> {
    vec![
        (
            user_key("a"),
            SeqMarked::new_normal(1, (meta(Some(10)), b("a0"))),
        ),
        (
            user_key("b"),
            SeqMarked::new_normal(2, (meta(None), b("b0"))),
        ),
        (
            user_key("c"),
            SeqMarked::new_normal(3, (meta(None), b("c0"))),
        ),
        (
            user_key("d"),
            SeqMarked::new_normal(4, (meta(None), b("d0"))),
        ),
        (
            user_key("e"),
            SeqMarked::new_normal(5, (meta(None), b("e0"))),
        ),
        (
            user_key("f"),
            SeqMarked::new_normal(6, (meta(None), b("f0"))),
        ),
        (
            user_key("g"),
            SeqMarked::new_normal(7, (meta(None), b("g0"))),
        ),
        (
            user_key("h"),
            SeqMarked::new_normal(8, (meta(None), b("h0"))),
        ),
        (
            user_key("i"),
            SeqMarked::new_normal(9, (meta(None), b("i0"))),
        ),
    ]
}

/// The meta an applier stamps onto a value written at log time zero.
fn meta(expire_at_sec: Option<u64>) -> Option<KVMeta> {
    Some(KVMeta::new(expire_at_sec, Some(0)))
}

fn expected_expire() -> Vec<(ExpireKey, SeqMarked<String>)> {
    vec![(
        ExpireKey::new(10_000, 1),
        SeqMarked::new_normal(1, "a".to_string()),
    )]
}

async fn kv_entries(lm: &LeveledMap) -> anyhow::Result<Vec<(UserKey, SeqMarked<MetaValue>)>> {
    let strm = lm.range_at_seq(UserKey::default().., u64::MAX).await?;
    Ok(strm.try_collect::<Vec<_>>().await?)
}

async fn expire_entries(lm: &LeveledMap) -> anyhow::Result<Vec<(ExpireKey, SeqMarked<String>)>> {
    let strm = lm.range_at_seq(ExpireKey::default().., u64::MAX).await?;
    Ok(strm.try_collect::<Vec<_>>().await?)
}

/// Render the writable level stat followed by the immutable level stats, newest
/// first. Immutable level indexes are process-unique, so they are stripped.
fn stat_str(lm: &LeveledMap) -> Vec<String> {
    let mut stats = vec![lm.writable_stat().to_string()];
    stats.extend(
        lm.immutable_levels()
            .stat()
            .iter()
            .map(|s| s.to_string().split_once(']').unwrap().1.to_string()),
    );
    stats
}

fn user_key(s: impl ToString) -> UserKey {
    UserKey::new(s)
}

fn b(x: impl ToString) -> Vec<u8> {
    x.to_string().as_bytes().to_vec()
}
