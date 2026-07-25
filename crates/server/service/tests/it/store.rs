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
use std::time::Duration;

use databend_meta::meta_node::meta_node::LogStore;
use databend_meta::meta_node::meta_node::SMStore;
use databend_meta::store::RaftStore;
use databend_meta_raft_store::db_exporter::DBExporter;
use databend_meta_raft_store::log_store::util::blocking_flush;
use databend_meta_raft_store::snapshot_store::MetaSnapshotId;
use databend_meta_runtime_api::TokioRuntime;
use databend_meta_snapshot_db::DB;
use databend_meta_test_harness::snapshot_logs;
use databend_meta_types::Cmd;
use databend_meta_types::LogEntry;
use databend_meta_types::SeqV;
use databend_meta_types::UpsertKV;
use databend_meta_types::node::Node;
use databend_meta_types::normalize_meta::NormalizeMeta;
use databend_meta_types::raft_types::Entry;
use databend_meta_types::raft_types::EntryPayload;
use databend_meta_types::raft_types::EntryResponder;
use databend_meta_types::raft_types::Membership;
use databend_meta_types::raft_types::StorageError;
use databend_meta_types::raft_types::StoredMembership;
use databend_meta_types::raft_types::TypeConfig;
use databend_meta_types::raft_types::Vote;
use databend_meta_types::raft_types::new_log_id;
use databend_meta_types::sys_data::SysData;
use futures::TryStreamExt;
use futures::stream;
use log::debug;
use log::info;
use maplit::btreemap;
use maplit::btreeset;
use openraft::RaftLogReader;
use openraft::RaftSnapshotBuilder;
use openraft::entry::RaftEntry;
use openraft::storage::RaftLogReaderExt;
use openraft::storage::RaftLogStorage;
use openraft::storage::RaftLogStorageExt;
use openraft::storage::RaftStateMachine;
use openraft::testing::log::StoreBuilder;
use openraft::testing::log_id;
use pretty_assertions::assert_eq;
use raft_log::DumpApi;
use test_harness::test;

use crate::testing::meta_service_test_harness;
use crate::tests::service::MetaSrvTestContext;

struct MetaStoreBuilder {}

impl StoreBuilder<TypeConfig, LogStore, SMStore<TokioRuntime>, MetaSrvTestContext<TokioRuntime>>
    for MetaStoreBuilder
{
    async fn build(
        &self,
    ) -> Result<
        (
            MetaSrvTestContext<TokioRuntime>,
            LogStore,
            SMStore<TokioRuntime>,
        ),
        StorageError,
    > {
        let tc = MetaSrvTestContext::<TokioRuntime>::new(555);
        let sto = RaftStore::<TokioRuntime>::open(&tc.config.raft_config)
            .await
            .expect("fail to create store");
        Ok((tc, sto.log().clone(), sto.state_machine().clone()))
    }
}

#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_impl_raft_storage() -> anyhow::Result<()> {
    openraft::testing::log::Suite::test_all(MetaStoreBuilder {}).await?;

    Ok(())
}

/// Ensure purged logs to be removed from the cache
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_meta_store_purge_cache() -> anyhow::Result<()> {
    let id = 3;
    let mut tc = MetaSrvTestContext::<TokioRuntime>::new(id);
    tc.config.raft_config.log_cache_max_items = 100;
    // Build with small chunk, because all entries in the last open chunk will be cached.
    tc.config.raft_config.log_wal_chunk_max_records = 5;

    {
        let sto = RaftStore::<TokioRuntime>::open(&tc.config.raft_config).await?;

        sto.log().clone().save_vote(&Vote::new(10, 5)).await?;

        sto.log()
            .clone()
            .blocking_append([
                Entry::new_blank(log_id::<TypeConfig>(1, 2, 1)),
                Entry::new_blank(log_id::<TypeConfig>(1, 2, 2)),
                Entry::new_blank(log_id::<TypeConfig>(1, 2, 3)),
                Entry::new_blank(log_id::<TypeConfig>(1, 2, 4)),
                Entry::new_blank(log_id::<TypeConfig>(1, 2, 5)),
            ])
            .await?;

        let stat = sto.log().read().await.stat();
        assert_eq!(stat.payload_cache_item_count, 5);

        {
            let r = sto.log().read().await;
            let got = r.dump().write_to_string()?;
            println!("dump: {}", got);
            let want_dumped = r#"RaftLog:
ChunkId(00_000_000_000_000_000_000)
  R-00000: [000_000_000, 000_000_018) Size(18): State(RaftLogState { vote: None, last: None, committed: None, purged: None, user_data: None })
  R-00001: [000_000_018, 000_000_046) Size(28): State(RaftLogState { vote: None, last: None, committed: None, purged: None, user_data: Some(LogStoreMeta { node_id: Some(3) }) })
  R-00002: [000_000_046, 000_000_096) Size(50): SaveVote(Cw(Vote { leader_id: LeaderId { term: 10, node_id: 5 }, committed: false }))
  R-00003: [000_000_096, 000_000_148) Size(52): Append(Cw(LogId { leader_id: LeaderId { term: 1, node_id: 2 }, index: 1 }), Cw(blank))
  R-00004: [000_000_148, 000_000_200) Size(52): Append(Cw(LogId { leader_id: LeaderId { term: 1, node_id: 2 }, index: 2 }), Cw(blank))
ChunkId(00_000_000_000_000_000_200)
  R-00000: [000_000_000, 000_000_100) Size(100): State(RaftLogState { vote: Some(Cw(Vote { leader_id: LeaderId { term: 10, node_id: 5 }, committed: false })), last: Some(Cw(LogId { leader_id: LeaderId { term: 1, node_id: 2 }, index: 2 })), committed: None, purged: None, user_data: Some(LogStoreMeta { node_id: Some(3) }) })
  R-00001: [000_000_100, 000_000_152) Size(52): Append(Cw(LogId { leader_id: LeaderId { term: 1, node_id: 2 }, index: 3 }), Cw(blank))
  R-00002: [000_000_152, 000_000_204) Size(52): Append(Cw(LogId { leader_id: LeaderId { term: 1, node_id: 2 }, index: 4 }), Cw(blank))
  R-00003: [000_000_204, 000_000_256) Size(52): Append(Cw(LogId { leader_id: LeaderId { term: 1, node_id: 2 }, index: 5 }), Cw(blank))
"#;
            assert_eq!(want_dumped, got);
        }

        // When purging up to index=4, all entries in the last open chunk will still be cached.
        // All previous entries are purge, although the cache is not full.

        sto.log()
            .clone()
            .purge(log_id::<TypeConfig>(1, 2, 4))
            .await?;

        let r = sto.log().read().await;
        let got = r.dump().write_to_string()?;
        println!("dump: {}", got);

        let stat = sto.log().read().await.stat();
        println!("stat: {:#}", stat);
        assert_eq!(stat.payload_cache_item_count, 3);
    }

    Ok(())
}

#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_meta_store_restart() -> anyhow::Result<()> {
    // - Create a meta store
    // - Update meta store
    // - Close and reopen it
    // - Test state is restored: hard state, log, state machine

    let id = 3;
    let tc = MetaSrvTestContext::<TokioRuntime>::new(id);

    info!("--- new meta store");
    {
        let sto = RaftStore::<TokioRuntime>::open(&tc.config.raft_config).await?;
        assert_eq!(id, sto.id);
        assert_eq!(None, sto.log().clone().read_vote().await?);

        info!("--- update metasrv");

        sto.log().clone().save_vote(&Vote::new(10, 5)).await?;

        sto.log()
            .clone()
            .blocking_append([Entry::new_blank(log_id::<TypeConfig>(1, 2, 1))])
            .await?;

        sto.log()
            .clone()
            .save_committed(Some(log_id::<TypeConfig>(1, 2, 2)))
            .await?;

        // save_committed only updates the in-memory state and does not flush
        // the WAL. Force a flush here so the committed marker is durable
        // across the close/reopen cycle below; otherwise the worker queue is
        // dropped before fsync completes and read_committed() returns None
        // after reopen.
        {
            let mut log = sto.log().write().await;
            blocking_flush(&mut log).await?;
        }

        sto.state_machine()
            .clone()
            .apply(stream::iter([Ok((
                Entry::new_blank(log_id::<TypeConfig>(1, 2, 2)),
                None,
            ))]))
            .await?;
    }

    info!("--- reopen meta store");
    {
        let sto = RaftStore::<TokioRuntime>::open(&tc.config.raft_config).await?;
        assert_eq!(id, sto.id);
        assert_eq!(Some(Vote::new(10, 5)), sto.log().clone().read_vote().await?);

        assert_eq!(
            log_id::<TypeConfig>(1, 2, 1),
            sto.log().clone().get_log_id(1).await?
        );
        assert_eq!(
            Some(log_id::<TypeConfig>(1, 2, 2)),
            sto.log().clone().read_committed().await?
        );
        assert_eq!(
            None,
            sto.state_machine().clone().applied_state().await?.0,
            "state machine is not persisted"
        );
    }
    Ok(())
}

#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_meta_store_build_snapshot() -> anyhow::Result<()> {
    // - Create a metasrv
    // - Apply logs
    // - Create a snapshot check snapshot state

    let id = 3;
    let tc = MetaSrvTestContext::<TokioRuntime>::new(id);

    let sto = RaftStore::<TokioRuntime>::open(&tc.config.raft_config).await?;

    info!("--- feed logs and state machine");

    let (logs, want) = snapshot_logs();

    sto.log().clone().blocking_append(logs.clone()).await?;
    let entry_stream = stream::iter(logs.into_iter().map(|e| Ok((e, None))));
    sto.get_sm_v003().apply_entries(entry_stream).await?;

    let curr_snap = sto.state_machine().clone().build_snapshot().await?;
    assert_eq!(Some(new_log_id(1, 0, 9)), curr_snap.meta.last_log_id);

    info!("--- check snapshot");
    {
        let data = curr_snap.snapshot;
        let res = db_to_lines(&data).await?;

        debug!("res: {:?}", res);

        assert_eq!(want, res);
    }

    info!("--- rebuild other 4 times, keeps only last 3");
    {
        sto.state_machine().clone().build_snapshot().await?;
        sto.state_machine().clone().build_snapshot().await?;
        sto.state_machine().clone().build_snapshot().await?;
        sto.state_machine().clone().build_snapshot().await?;

        let snapshot_store = sto.state_machine().snapshot_store();
        let loader = snapshot_store.new_loader();
        let (snapshot_ids, _) = loader.load_snapshot_ids().await?;
        assert_eq!(3, snapshot_ids.len());
    }

    Ok(())
}

#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_meta_store_current_snapshot() -> anyhow::Result<()> {
    // - Create a metasrv
    // - Apply logs
    // - Create a snapshot check snapshot state

    let id = 3;
    let tc = MetaSrvTestContext::<TokioRuntime>::new(id);

    let sto = RaftStore::<TokioRuntime>::open(&tc.config.raft_config).await?;

    info!("--- feed logs and state machine");

    let (logs, want) = snapshot_logs();

    sto.log().clone().blocking_append(logs.clone()).await?;
    {
        let sm = sto.get_sm_v003();
        let entry_stream = stream::iter(logs.into_iter().map(|e| Ok((e, None))));
        sm.apply_entries(entry_stream).await?;
    }

    sto.state_machine().clone().build_snapshot().await?;

    info!("--- check get_current_snapshot");

    let curr_snap = sto
        .state_machine()
        .clone()
        .get_current_snapshot()
        .await?
        .unwrap();
    assert_eq!(Some(new_log_id(1, 0, 9)), curr_snap.meta.last_log_id);

    info!("--- check snapshot");
    {
        let data = curr_snap.snapshot;
        let res = db_to_lines(&data).await?;

        debug!("res: {:?}", res);

        assert_eq!(want, res);
    }

    Ok(())
}

#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_meta_store_install_snapshot() -> anyhow::Result<()> {
    // - Create a metasrv
    // - Feed logs
    // - Create a snapshot
    // - Create a new metasrv and restore it by install the snapshot

    let (_logs, want) = snapshot_logs();

    let id = 3;
    let data = build_snapshot_db(id).await?;

    info!("--- reopen a new metasrv to install snapshot");
    {
        let tc = MetaSrvTestContext::<TokioRuntime>::new(id);

        let sto = RaftStore::<TokioRuntime>::open(&tc.config.raft_config).await?;

        info!("--- install snapshot");
        {
            let writer_permit = sto.get_sm_v003().acquire_writer_permit().await;

            // The timeout only bounds how long a correct install stays blocked;
            // it is generous so that a regressed, non-blocking install cannot
            // false-pass by merely being slow on a loaded machine.
            assert!(
                tokio::time::timeout(
                    Duration::from_millis(500),
                    sto.state_machine()
                        .clone()
                        .do_install_snapshot(data.clone()),
                )
                .await
                .is_err()
            );

            drop(writer_permit);

            sto.state_machine()
                .clone()
                .do_install_snapshot(data.clone())
                .await?;
        }

        info!("--- check installed meta");
        {
            let mem = sto.get_sm_v003().sys_data().last_membership_ref().clone();

            assert_eq!(
                StoredMembership::new(
                    Some(log_id::<TypeConfig>(1, 0, 5)),
                    Membership::new_with_defaults(vec![btreeset! {4,5,6}], [])
                ),
                mem
            );

            let last_applied = *sto.get_sm_v003().sys_data().last_applied_ref();
            assert_eq!(Some(log_id::<TypeConfig>(1, 0, 9)), last_applied);
        }

        info!("--- check snapshot");
        {
            let curr_snap = sto.state_machine().clone().build_snapshot().await?;
            let data = curr_snap.snapshot;
            let res = db_to_lines(&data).await?;

            debug!("res: {:?}", res);

            assert_eq!(want, res);
        }
    }

    Ok(())
}

/// Installing a snapshot must wait for an in-flight write and re-check
/// freshness after acquiring the writer permit: a write batch that advances
/// the state machine past the snapshot must survive the installation attempt.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_meta_store_install_snapshot_waits_for_in_flight_write() -> anyhow::Result<()> {
    let id = 3;

    // last_applied == (1, 0, 9)
    let data = build_snapshot_db(id).await?;

    let tc = MetaSrvTestContext::<TokioRuntime>::new(id);
    let sto = RaftStore::<TokioRuntime>::open(&tc.config.raft_config).await?;

    info!("--- apply an entry batch that stays open: the writer permit is held");

    let (entry_tx, entry_rx) =
        futures::channel::mpsc::unbounded::<Result<EntryResponder, io::Error>>();

    let (logs, _want) = snapshot_logs();
    for ent in logs {
        entry_tx
            .unbounded_send(Ok((ent, None)))
            .map_err(|e| anyhow::anyhow!("{e}"))?;
    }
    // One more write at index 10, past the snapshot's last_applied.
    let ent = Entry {
        log_id: new_log_id(1, 0, 10),
        payload: EntryPayload::Normal(LogEntry::new(Cmd::UpsertKV(UpsertKV::update(
            "in-flight",
            b"x",
        )))),
    };
    entry_tx
        .unbounded_send(Ok((ent, None)))
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let apply_task = {
        let sm = sto.get_sm_v003();
        tokio::spawn(async move { sm.apply_entries(entry_rx).await })
    };

    // Wait until the apply task holds the writer permit.
    loop {
        let acquired = tokio::time::timeout(
            Duration::from_millis(10),
            sto.get_sm_v003().acquire_writer_permit(),
        )
        .await;

        let Ok(permit) = acquired else {
            break;
        };
        drop(permit);
        tokio::task::yield_now().await;
    }

    info!("--- install snapshot; it must block until the batch commits");

    let install_task = {
        let mut sm_store = sto.state_machine().clone();
        let data = data.clone();
        tokio::spawn(async move { sm_store.do_install_snapshot(data).await })
    };

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !install_task.is_finished(),
        "install must wait for the in-flight write"
    );

    // Closing the entry stream commits the batch and releases the permit.
    drop(entry_tx);
    apply_task.await??;
    install_task.await??;

    info!("--- the write advanced past the snapshot: install must be skipped");
    {
        let sm = sto.get_sm_v003();

        let last_applied = *sm.sys_data().last_applied_ref();
        assert_eq!(Some(log_id::<TypeConfig>(1, 0, 10)), last_applied);

        let got = sm.get_maybe_expired_kv("in-flight").await?;
        assert_eq!(Some(SeqV::new(2, b"x".to_vec())), got.without_proposed_at());

        assert!(
            sm.get_snapshot().is_none(),
            "the stale snapshot must not be installed"
        );
    }

    Ok(())
}

/// Installing a snapshot that is not newer than the current state machine
/// must be a no-op: the state machine and its persisted level stay untouched.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_meta_store_install_snapshot_skips_stale_snapshot() -> anyhow::Result<()> {
    let id = 3;

    // last_applied == (1, 0, 9)
    let data = build_snapshot_db(id).await?;

    info!("--- state machine ahead of the snapshot: install is ignored");
    {
        let tc = MetaSrvTestContext::<TokioRuntime>::new(id);
        let sto = RaftStore::<TokioRuntime>::open(&tc.config.raft_config).await?;

        let (mut logs, _want) = snapshot_logs();
        logs.push(Entry {
            log_id: new_log_id(1, 0, 10),
            payload: EntryPayload::Normal(LogEntry::new(Cmd::UpsertKV(UpsertKV::update(
                "ahead", b"x",
            )))),
        });
        let entry_stream = stream::iter(logs.into_iter().map(|e| Ok((e, None))));
        sto.get_sm_v003().apply_entries(entry_stream).await?;

        sto.state_machine()
            .clone()
            .do_install_snapshot(data.clone())
            .await?;

        let sm = sto.get_sm_v003();

        let last_applied = *sm.sys_data().last_applied_ref();
        assert_eq!(Some(log_id::<TypeConfig>(1, 0, 10)), last_applied);

        let got = sm.get_maybe_expired_kv("ahead").await?;
        assert_eq!(Some(SeqV::new(2, b"x".to_vec())), got.without_proposed_at());

        assert!(
            sm.get_snapshot().is_none(),
            "an older snapshot must not be installed"
        );
    }

    info!("--- state machine at the same log id as the snapshot: install is ignored");
    {
        let tc = MetaSrvTestContext::<TokioRuntime>::new(id);
        let sto = RaftStore::<TokioRuntime>::open(&tc.config.raft_config).await?;

        let (logs, _want) = snapshot_logs();
        let entry_stream = stream::iter(logs.into_iter().map(|e| Ok((e, None))));
        sto.get_sm_v003().apply_entries(entry_stream).await?;

        sto.state_machine()
            .clone()
            .do_install_snapshot(data.clone())
            .await?;

        let sm = sto.get_sm_v003();

        let last_applied = *sm.sys_data().last_applied_ref();
        assert_eq!(Some(log_id::<TypeConfig>(1, 0, 9)), last_applied);

        let got = sm.get_maybe_expired_kv("a").await?;
        assert_eq!(Some(SeqV::new(1, b"A".to_vec())), got.without_proposed_at());

        assert!(
            sm.get_snapshot().is_none(),
            "a snapshot at the same log id must not be installed"
        );
    }

    Ok(())
}

/// Installing a snapshot that is still fresher than an in-flight write batch
/// must wait for the batch, then proceed: the write is legitimately
/// superseded because the snapshot contains it.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_meta_store_install_snapshot_proceeds_after_in_flight_write() -> anyhow::Result<()> {
    let id = 3;

    // last_applied == (1, 0, 9)
    let data = build_snapshot_db(id).await?;

    let tc = MetaSrvTestContext::<TokioRuntime>::new(id);
    let sto = RaftStore::<TokioRuntime>::open(&tc.config.raft_config).await?;

    info!("--- apply the first 6 entries; the batch stays open, short of the snapshot");

    let (entry_tx, entry_rx) =
        futures::channel::mpsc::unbounded::<Result<EntryResponder, io::Error>>();

    let (logs, want) = snapshot_logs();
    for ent in logs.into_iter().take(6) {
        entry_tx
            .unbounded_send(Ok((ent, None)))
            .map_err(|e| anyhow::anyhow!("{e}"))?;
    }

    let apply_task = {
        let sm = sto.get_sm_v003();
        tokio::spawn(async move { sm.apply_entries(entry_rx).await })
    };

    // Wait until the apply task holds the writer permit.
    loop {
        let acquired = tokio::time::timeout(
            Duration::from_millis(10),
            sto.get_sm_v003().acquire_writer_permit(),
        )
        .await;

        let Ok(permit) = acquired else {
            break;
        };
        drop(permit);
        tokio::task::yield_now().await;
    }

    info!("--- install snapshot; it must block until the batch commits");

    let install_task = {
        let mut sm_store = sto.state_machine().clone();
        let data = data.clone();
        tokio::spawn(async move { sm_store.do_install_snapshot(data).await })
    };

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !install_task.is_finished(),
        "install must wait for the in-flight write"
    );

    drop(entry_tx);
    apply_task.await??;
    install_task.await??;

    info!("--- the snapshot is still fresher: it must be installed");
    {
        let sm = sto.get_sm_v003();

        let last_applied = *sm.sys_data().last_applied_ref();
        assert_eq!(Some(log_id::<TypeConfig>(1, 0, 9)), last_applied);

        let mem = sm.sys_data().last_membership_ref().clone();
        assert_eq!(
            StoredMembership::new(
                Some(log_id::<TypeConfig>(1, 0, 5)),
                Membership::new_with_defaults(vec![btreeset! {4,5,6}], [])
            ),
            mem
        );

        assert!(sm.get_snapshot().is_some());

        let curr_snap = sto.state_machine().clone().build_snapshot().await?;
        let res = db_to_lines(&curr_snap.snapshot).await?;
        assert_eq!(want, res);
    }

    Ok(())
}

/// Installation is deliberately independent from compaction: a held
/// compaction permit must not delay installing a snapshot.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_meta_store_install_snapshot_not_blocked_by_compaction() -> anyhow::Result<()> {
    let id = 3;

    // last_applied == (1, 0, 9)
    let data = build_snapshot_db(id).await?;

    let tc = MetaSrvTestContext::<TokioRuntime>::new(id);
    let sto = RaftStore::<TokioRuntime>::open(&tc.config.raft_config).await?;

    let compactor = sto.get_sm_v003().acquire_compactor("test").await;

    let res = tokio::time::timeout(
        Duration::from_secs(5),
        sto.state_machine()
            .clone()
            .do_install_snapshot(data.clone()),
    )
    .await;
    res.expect("install must not wait for the compaction permit")?;

    drop(compactor);

    let sm = sto.get_sm_v003();

    let last_applied = *sm.sys_data().last_applied_ref();
    assert_eq!(Some(log_id::<TypeConfig>(1, 0, 9)), last_applied);

    let mem = sm.sys_data().last_membership_ref().clone();
    assert_eq!(
        StoredMembership::new(
            Some(log_id::<TypeConfig>(1, 0, 5)),
            Membership::new_with_defaults(vec![btreeset! {4,5,6}], [])
        ),
        mem
    );

    let got = sm.get_maybe_expired_kv("a").await?;
    assert_eq!(Some(SeqV::new(1, b"A".to_vec())), got.without_proposed_at());

    assert!(sm.get_snapshot().is_some());

    Ok(())
}

/// A snapshot without last_applied must still be installed when the state
/// machine has no last_applied either: `databend_metactl::import` produces
/// such snapshots, containing manually added nodes but no log id.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_meta_store_install_snapshot_without_last_applied() -> anyhow::Result<()> {
    let id = 3;
    let tc = MetaSrvTestContext::<TokioRuntime>::new(id);
    let sto = RaftStore::<TokioRuntime>::open(&tc.config.raft_config).await?;

    info!("--- build a snapshot with node data but no last_applied, like metactl import");

    let db = {
        let writer = sto.state_machine().snapshot_store().new_writer()?;

        let mut sys_data = SysData::default();
        sys_data.nodes_mut().insert(5, Node::default());

        writer.commit(MetaSnapshotId::new_with_epoch(None), sys_data)?
    };

    sto.state_machine().clone().do_install_snapshot(db).await?;

    let sm = sto.get_sm_v003();

    assert_eq!(None, *sm.sys_data().last_applied_ref());
    assert_eq!(
        btreemap! {5 => Node::default()},
        sm.sys_data().nodes_ref().clone()
    );
    assert!(
        sm.get_snapshot().is_some(),
        "a None/None install must not be skipped"
    );

    Ok(())
}

/// A snapshot build that captured the pre-install state machine may complete
/// after a snapshot installation; it reorganizes only the discarded map and
/// must not affect the installed state.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_meta_store_install_snapshot_during_snapshot_build() -> anyhow::Result<()> {
    let id = 3;

    // last_applied == (1, 0, 9)
    let data = build_snapshot_db(id).await?;

    let tc = MetaSrvTestContext::<TokioRuntime>::new(id);
    let sto = RaftStore::<TokioRuntime>::open(&tc.config.raft_config).await?;

    info!("--- apply the first 6 entries: last_applied == (1, 0, 6)");
    {
        let (logs, _want) = snapshot_logs();
        let entry_stream = stream::iter(logs.into_iter().take(6).map(|e| Ok((e, None))));
        sto.get_sm_v003().apply_entries(entry_stream).await?;
    }

    info!("--- start building a snapshot; it blocks on the held writer permit");

    let writer_permit = sto.get_sm_v003().acquire_writer_permit().await;

    let build_task = {
        let mut sm_store = sto.state_machine().clone();
        tokio::spawn(async move { sm_store.build_snapshot().await })
    };

    // Wait until the build task holds the compaction permit: the pre-install
    // state machine is captured by then.
    loop {
        let acquired = tokio::time::timeout(
            Duration::from_millis(10),
            sto.get_sm_v003().acquire_compactor("probe"),
        )
        .await;

        let Ok(compactor) = acquired else {
            break;
        };
        drop(compactor);
        tokio::task::yield_now().await;
    }

    drop(writer_permit);

    info!("--- install a snapshot while the build is in flight");

    sto.state_machine()
        .clone()
        .do_install_snapshot(data.clone())
        .await?;

    let built = build_task.await??;
    assert_eq!(
        Some(log_id::<TypeConfig>(1, 0, 6)),
        built.meta.last_log_id,
        "the build captured the pre-install state machine"
    );

    info!("--- the installed state machine is unaffected by the stale build");
    {
        let (_logs, want) = snapshot_logs();

        let sm = sto.get_sm_v003();

        let last_applied = *sm.sys_data().last_applied_ref();
        assert_eq!(Some(log_id::<TypeConfig>(1, 0, 9)), last_applied);

        let installed = sm.get_snapshot().unwrap();
        assert_eq!(
            Some(log_id::<TypeConfig>(1, 0, 9)),
            *installed.sys_data().last_applied_ref(),
            "the persisted level is the installed snapshot, not the stale build"
        );

        let res = db_to_lines(&installed).await?;
        assert_eq!(want, res);
    }

    Ok(())
}

/// The in-memory compactor keeps serving the current state machine after a
/// snapshot installation replaces it.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_meta_store_in_memory_compactor_survives_install_snapshot() -> anyhow::Result<()> {
    let id = 3;

    // last_applied == (1, 0, 9)
    let data = build_snapshot_db(id).await?;

    let mut tc = MetaSrvTestContext::<TokioRuntime>::new(id);
    tc.config.raft_config.compact_immutables_ms = Some(50);

    let sto = RaftStore::<TokioRuntime>::open(&tc.config.raft_config).await?;

    sto.state_machine()
        .clone()
        .do_install_snapshot(data)
        .await?;

    info!("--- write to the installed state machine; the compactor must freeze it");

    let ent = Entry {
        log_id: new_log_id(1, 0, 10),
        payload: EntryPayload::Normal(LogEntry::new(Cmd::UpsertKV(UpsertKV::update(
            "after-install",
            b"x",
        )))),
    };
    let entry_stream = stream::iter([Ok((ent, None))]);
    sto.get_sm_v003().apply_entries(entry_stream).await?;

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let n_levels = sto
            .get_sm_v003()
            .leveled_map()
            .immutable_levels()
            .stat()
            .len();
        if n_levels > 0 {
            break;
        }

        assert!(
            std::time::Instant::now() < deadline,
            "in-memory compactor must still compact the installed state machine"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    Ok(())
}

/// Feed `snapshot_logs()` to a fresh store and build a snapshot from it.
///
/// The returned snapshot DB has `last_applied == (1, 0, 9)`.
async fn build_snapshot_db(id: u64) -> anyhow::Result<DB> {
    let tc = MetaSrvTestContext::<TokioRuntime>::new(id);

    let sto = RaftStore::<TokioRuntime>::open(&tc.config.raft_config).await?;

    let (logs, _want) = snapshot_logs();

    sto.log().clone().blocking_append(logs.clone()).await?;
    let entry_stream = stream::iter(logs.into_iter().map(|e| Ok((e, None))));
    sto.get_sm_v003().apply_entries(entry_stream).await?;

    let snap = sto.state_machine().clone().build_snapshot().await?;
    Ok(snap.snapshot)
}

async fn db_to_lines(db: &DB) -> Result<Vec<String>, io::Error> {
    let strm = DBExporter::new(db).export().await?;
    let res = strm.try_collect::<Vec<_>>().await?;

    let res = res
        .into_iter()
        .map(|sm_ent| serde_json::to_string(&sm_ent).unwrap())
        .collect::<Vec<_>>();

    Ok(res)
}
