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
use databend_meta_raft_store::leveled_store::db_exporter::DBExporter;
use databend_meta_raft_store::raft_log_v004::util::blocking_flush;
use databend_meta_raft_store::state_machine::testing::snapshot_logs;
use databend_meta_runtime_api::TokioRuntime;
use databend_meta_snapshot_db::DB;
use databend_meta_types::Cmd;
use databend_meta_types::LogEntry;
use databend_meta_types::SeqV;
use databend_meta_types::UpsertKV;
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
use futures::TryStreamExt;
use futures::stream;
use log::debug;
use log::info;
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

            assert!(
                tokio::time::timeout(
                    Duration::from_millis(10),
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
