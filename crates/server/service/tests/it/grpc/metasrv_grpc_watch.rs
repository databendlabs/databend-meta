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

#![allow(clippy::collapsible_if, clippy::useless_vec)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use databend_base::string_util::prefix_to_range;
use databend_meta::meta_service::MetaNode;
use databend_meta_client::ClientHandle;
use databend_meta_client::DEFAULT_GRPC_MESSAGE_SIZE;
use databend_meta_client::MetaGrpcClient;
use databend_meta_kvapi::KvApiExt;
use databend_meta_runtime_api::SpawnApi;
use databend_meta_runtime_api::TokioRuntime;
use databend_meta_snapshot_store::MetaSnapshotId;
use databend_meta_snapshot_store::SnapshotStore;
use databend_meta_types::ConditionResult;
use databend_meta_types::MatchSeq;
use databend_meta_types::Operation;
use databend_meta_types::TxnCondition;
use databend_meta_types::TxnDeleteByPrefixRequest;
use databend_meta_types::TxnOp;
use databend_meta_types::UpsertKV;
use databend_meta_types::normalize_meta::NormalizeMeta;
use databend_meta_types::protobuf as pb;
use databend_meta_types::protobuf::Event;
use databend_meta_types::protobuf::KvMeta;
use databend_meta_types::protobuf::SeqV;
use databend_meta_types::protobuf::TxnRequest;
use databend_meta_types::protobuf::WatchRequest;
use databend_meta_types::protobuf::WatchResponse;
use databend_meta_types::protobuf::meta_service_client::MetaServiceClient;
use databend_meta_types::protobuf::watch_request::FilterType;
use databend_meta_types::raft_types::Membership;
use databend_meta_types::raft_types::StoredMembership;
use databend_meta_types::raft_types::TypeConfig;
use databend_meta_types::raft_types::Vote;
use databend_meta_types::sys_data::SysData;
use databend_meta_types::txn_condition;
use databend_meta_types::txn_op;
use futures::stream;
use log::info;
use openraft::ServerState;
use openraft::testing::log_id;
use test_harness::test;
use tokio::time::sleep;
use tonic::Code;
use tonic::Streaming;

use crate::testing::meta_service_test_harness;
use crate::tests::service::MetaSrvTestContext;
use crate::tests::service::grpc_client;

async fn test_watch_main(
    addr: String,
    watch: WatchRequest,
    mut watch_events: Vec<Event>,
    updates: Vec<UpsertKV>,
) -> anyhow::Result<()> {
    let client = make_client(&addr)?;
    let mut watch_stream = client.request(watch).await?;

    {
        let client = make_client(&addr)?;
        let _h = TokioRuntime::spawn(
            async move {
                for update in updates.iter() {
                    client.upsert_kv(update.clone()).await.unwrap();
                }
            },
            None,
        );
    }

    loop {
        if let Ok(Some(resp)) = watch_stream.message().await {
            let event = resp.event.unwrap().without_proposed_at();

            assert!(!watch_events.is_empty());

            let want = watch_events.remove(0);
            assert_eq!(want, event);

            if watch_events.is_empty() {
                break;
            }
        }
    }

    Ok(())
}

async fn test_watch_txn_main(
    addr: String,
    watch: WatchRequest,
    mut watch_events: Vec<Event>,
    txn: TxnRequest,
) -> anyhow::Result<()> {
    let client = make_client(&addr)?;
    let mut watch_stream = client.request(watch).await?;

    {
        let client = make_client(&addr)?;
        let _h = TokioRuntime::spawn(
            async move {
                client.transaction(txn).await.unwrap();
            },
            None,
        );
    }

    loop {
        if let Ok(Some(resp)) = watch_stream.message().await {
            if let Some(event) = resp.event {
                let event = event.without_proposed_at();
                assert!(!watch_events.is_empty());

                assert_eq!(watch_events.first(), Some(&event));
                watch_events.remove(0);

                if watch_events.is_empty() {
                    break;
                }
            }
        }
    }

    Ok(())
}

#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_watch_single_key() -> anyhow::Result<()> {
    let (_tc, addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;

    let seq: u64 = 1;

    let watch = WatchRequest::new(s("a"), None);

    let key_a = s("a");
    let val_a = b("a");

    let watch_events = vec![
        // set a->a
        Event {
            key: key_a.clone(),
            current: Some(SeqV::new(seq, val_a.clone())),
            prev: None,
        },
    ];

    let updates = vec![UpsertKV::new(
        "a",
        MatchSeq::GE(0),
        Operation::Update(val_a),
        None,
    )];

    test_watch_main(addr.clone(), watch, watch_events, updates).await?;

    Ok(())
}

#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_watch() -> anyhow::Result<()> {
    // - Start a metasrv server.
    // - Watch some key.
    // - Write some data.
    // - Assert watcher get all the update.

    let (_tc, addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;

    let mut seq: u64 = 1;
    // 1.update some events
    {
        let watch = WatchRequest::new("a".to_string(), Some("z".to_string()));

        let key_a = s("a");
        let key_b = s("b");

        let val_a = b("a");
        let val_b = b("b");
        let val_new = b("new");
        let val_z = b("z");

        let watch_events = vec![
            // set a->a
            Event {
                key: key_a.clone(),
                current: Some(SeqV::new(seq, val_a.clone())),
                prev: None,
            },
            // set b->b
            Event {
                key: key_b.clone(),
                current: Some(SeqV::new(seq + 2, val_b.clone())),
                prev: None,
            },
            // update b->new
            Event {
                key: key_b.clone(),
                current: Some(SeqV::new(seq + 3, val_new.clone())),
                prev: Some(SeqV::new(seq + 2, val_b.clone())),
            },
            // delete b
            Event {
                key: key_b.clone(),
                prev: Some(SeqV::new(seq + 3, val_new.clone())),
                current: None,
            },
        ];

        seq = 4;
        // update kv
        let updates = vec![
            UpsertKV::new("a", MatchSeq::GE(0), Operation::Update(val_a), None),
            // upsert key z, because z in key_end and the range is [key_start, key_end), so key z MUST not be notified in watch events.
            UpsertKV::new("z", MatchSeq::GE(0), Operation::Update(val_z), None),
            UpsertKV::new("b", MatchSeq::GE(0), Operation::Update(val_b), None),
            UpsertKV::new("b", MatchSeq::GE(0), Operation::Update(val_new), None),
            UpsertKV::new("b", MatchSeq::GE(0), Operation::Delete, None),
        ];
        test_watch_main(addr.clone(), watch, watch_events, updates).await?;
    }

    // 2. test filter
    {
        let key_str = "1";
        let watch = WatchRequest::new(s(key_str), None).with_filter(FilterType::Delete);

        let key = s(key_str);
        let val = b("old");
        let val_new = b("new");

        // has only delete events
        let watch_events = vec![
            // delete 1 first time
            Event {
                key: key.clone(),
                prev: Some(SeqV::new(seq + 1, val.clone())),
                current: None,
            },
            // delete 1 second time
            Event {
                key: key.clone(),
                prev: Some(SeqV::new(seq + 2, val_new.clone())),
                current: None,
            },
        ];

        // update and delete twice
        let updates = vec![
            UpsertKV::new(key_str, MatchSeq::GE(0), Operation::Update(val), None),
            UpsertKV::new(key_str, MatchSeq::GE(0), Operation::Delete, None),
            UpsertKV::new(key_str, MatchSeq::GE(0), Operation::Update(val_new), None),
            UpsertKV::new(key_str, MatchSeq::GE(0), Operation::Delete, None),
        ];
        test_watch_main(addr.clone(), watch, watch_events, updates).await?;
    }
    // 3. test watch transaction
    {
        // first construct test kv
        let delete_key = "watch_delete_key";
        let watch_delete_by_prefix_key = "watch_delete_by_prefix_key";

        {
            let client = make_client(&addr)?;

            let updates = vec![
                UpsertKV::update(delete_key, &b(delete_key)),
                UpsertKV::update(watch_delete_by_prefix_key, &b(watch_delete_by_prefix_key)),
            ];

            for update in updates {
                let _ = client.upsert_kv(update.clone()).await;
            }
        }

        let watch_prefix = "watch";

        let k1 = "watch_txn_key";

        let txn_key = s(k1);
        let txn_val = b("txn_val");

        let (start, end) = prefix_to_range(watch_prefix);

        let watch = WatchRequest::new(start, end);

        let conditions = vec![TxnCondition {
            key: txn_key.clone(),
            expected: ConditionResult::Eq as i32,
            target: Some(txn_condition::Target::Seq(0)),
        }];

        let if_then: Vec<TxnOp> = vec![
            TxnOp::put(txn_key.clone(), txn_val.clone()),
            TxnOp::delete(delete_key),
            TxnOp {
                request: Some(txn_op::Request::DeleteByPrefix(TxnDeleteByPrefixRequest {
                    prefix: watch_delete_by_prefix_key.to_string(),
                })),
            },
        ];

        let txn = TxnRequest::new(conditions, if_then);

        seq = 7;

        let watch_events = vec![
            Event {
                key: txn_key.clone(),
                current: Some(SeqV::new(seq + 2, txn_val)),
                prev: None,
            },
            Event {
                key: s(delete_key),
                prev: Some(SeqV::new(seq, b(delete_key))),
                current: None,
            },
            Event {
                key: s(watch_delete_by_prefix_key),
                prev: Some(SeqV::new(seq + 1, b(watch_delete_by_prefix_key))),
                current: None,
            },
        ];

        test_watch_txn_main(addr.clone(), watch, watch_events, txn).await?;
    }

    Ok(())
}

#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_watch_initialization_flush() -> anyhow::Result<()> {
    let (tc, _addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;
    let updates = vec![
        UpsertKV::update("a", b"a"),
        UpsertKV::update("b", b"b"),
        UpsertKV::update("c", b"c"),
        UpsertKV::update("d", b"d"),
        UpsertKV::update("z", b"z"),
    ];

    let client = grpc_client(&tc).await?;
    for update in updates.iter() {
        client.upsert_kv(update.clone()).await?;
    }

    let mut strm = {
        let watch = WatchRequest::new(s("a"), Some(s("e"))).with_initial_flush(true);
        client.watch_with_initialization(watch).await?
    };

    let cache = Arc::new(Mutex::new(BTreeMap::new()));
    let c = cache.clone();

    let is_initialization_completed = Arc::new(Mutex::new(false));
    let init_compl = is_initialization_completed.clone();

    let flags = Arc::new(Mutex::new(vec![]));
    let f = flags.clone();

    let cache_updater = async move {
        while let Ok(Some(resp)) = strm.message().await {
            f.lock().unwrap().push(resp.is_initialization);

            if resp.is_initialization_complete_flag() {
                *init_compl.lock().unwrap() = true;
            }

            let Some(event) = resp.event else {
                continue;
            };

            let mut cache = c.lock().unwrap();
            if let Some(value) = event.current {
                cache.insert(event.key, value);
            } else {
                cache.remove(&event.key);
            }
        }
    };

    let _h = TokioRuntime::spawn(cache_updater, None);

    tokio::time::sleep(Duration::from_secs(1)).await;

    assert_eq!(flags.lock().unwrap().clone(), vec![
        true, true, true, true,  // existent key-values
        false, // initialization complete
    ]);

    assert!(*is_initialization_completed.lock().unwrap());

    let keys = {
        let cache = cache.lock().unwrap();
        cache.keys().cloned().collect::<Vec<_>>()
    };

    assert_eq!(vec![s("a"), s("b"), s("c"), s("d")], keys);

    client.upsert_kv(UpsertKV::update("a", b"a2")).await?;
    client.upsert_kv(UpsertKV::delete("c")).await?;

    tokio::time::sleep(Duration::from_secs(1)).await;

    assert_eq!(flags.lock().unwrap().clone(), vec![
        true, true, true, true,  // existent key-values
        false, // initialization complete
        false, false, // changes
    ]);

    let values = {
        let cache = cache.lock().unwrap();
        cache
            .values()
            .map(|seqv| seqv.data.clone())
            .collect::<Vec<_>>()
    };

    assert_eq!(vec![b("a2"), b("b"), b("d")], values);

    Ok(())
}

#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_watch_expired_events() -> anyhow::Result<()> {
    // Test events emitted when cleaning expired key:
    // - Before applying, 32 expired keys will be cleaned.
    // - When applying, touched expired keys will be cleaned.

    fn sec(x: u64) -> Duration {
        Duration::from_secs(x)
    }

    let (_tc, addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;

    let watch_prefix = "w_";
    let now_sec = now();

    info!("--- prepare data that are gonna expire");
    {
        let client = make_client(&addr)?;

        let mut txn = TxnRequest::new(vec![], vec![]);

        // Every apply() will clean all expired keys.
        for i in 0..(32 + 1) {
            let k = format!("w_auto_gc_{}", i);
            txn.if_then
                .push(TxnOp::put_with_ttl(&k, b(&k), Some(sec(1))));
        }

        // Expired key won't be cleaned when they are read, although read returns None.

        txn.if_then
            .push(TxnOp::put_with_ttl("w_b1", b("w_b1"), Some(sec(6))));
        txn.if_then
            .push(TxnOp::put_with_ttl("w_b2", b("w_b2"), Some(sec(6))));
        txn.if_then
            .push(TxnOp::put_with_ttl("w_b3a", b("w_b3a"), Some(sec(6))));
        txn.if_then
            .push(TxnOp::put_with_ttl("w_b3b", b("w_b3b"), Some(sec(15))));

        client.transaction(txn).await?;
    }

    info!("--- start watching");
    let watch_client = make_client(&addr)?;
    let mut client_stream = {
        let (start, end) = prefix_to_range(watch_prefix);
        let watch = WatchRequest::new(start, end);
        watch_client.request(watch).await?
    };

    info!("--- sleep 10 for expiration");
    tokio::time::sleep(sec(10)).await;

    info!("--- apply another txn in another thread to override keys");
    {
        let txn = TxnRequest::new(vec![], vec![
            TxnOp::put("w_b1", b("w_b1_override")),
            TxnOp::delete("w_b2"),
            TxnOp {
                request: Some(txn_op::Request::DeleteByPrefix(TxnDeleteByPrefixRequest {
                    prefix: s("w_b3"),
                })),
            },
        ]);

        let client = make_client(&addr)?;
        let _h = TokioRuntime::spawn(
            async move {
                let _res = client.transaction(txn).await;
            },
            None,
        );
    }

    info!("--- check emitted events");
    {
        // 32 expired keys are auto cleaned.
        for i in 0..(32 + 1) {
            let k = format!("w_auto_gc_{}", i);
            let want = del_event(&k, 1 + i, &k, Some(KvMeta::new(Some(now_sec + 1), None)));
            let msg = client_stream.message().await?.unwrap();
            assert!(msg.event.unwrap().close_to(&want, Duration::from_secs(2)));
        }

        // Check event generated when applying the txn

        let seq = 34;
        let watch_events = vec![
            del_event(
                "w_b1",
                seq,
                "w_b1",
                Some(KvMeta::new(Some(now_sec + 6), None)),
            ), // expired
            del_event(
                "w_b2",
                seq + 1,
                "w_b2",
                Some(KvMeta::new(Some(now_sec + 6), None)),
            ), // expired
            del_event(
                "w_b3a",
                seq + 2,
                "w_b3a",
                Some(KvMeta::new(Some(now_sec + 6), None)),
            ), // expired
            add_event("w_b1", seq + 4, "w_b1_override", Some(KvMeta::default())), // override
            del_event(
                "w_b3b",
                seq + 3,
                "w_b3b",
                Some(KvMeta::new(Some(now_sec + 15), None)),
            ), // expired
        ];

        // The evaluated expire_at could not equal to the real expire_at, so we need to tidy the expire_at.
        fn tidy(mut ev: Event) -> Event {
            if let Some(ref mut prev) = ev.prev {
                if let Some(ref mut meta) = prev.meta {
                    meta.expire_at = meta.expire_at.map(tidy_timestamp);
                }
            }
            if let Some(ref mut current) = ev.current {
                if let Some(ref mut meta) = current.meta {
                    meta.expire_at = meta.expire_at.map(tidy_timestamp);
                }
            }
            ev
        }

        fn tidy_timestamp(x: u64) -> u64 {
            if x > 100_000_000_000 {
                // tidy the timestamp in millis
                x / 10_000 * 10_000
            } else {
                // tidy the timestamp in seconds
                x / 10 * 10
            }
        }

        for ev in watch_events {
            let msg = client_stream.message().await?.unwrap();
            assert!(tidy(ev).close_to(&tidy(msg.event.unwrap()), Duration::from_secs(2)));
        }
    }

    Ok(())
}

#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_watch_stream_count() -> anyhow::Result<()> {
    // When the client drops the stream, databend-meta should reclaim the resources.

    let (tc, addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;

    let watch_req = || WatchRequest::new("a".to_string(), Some("z".to_string()));

    let client1 = make_client(&addr)?;
    let _watch_stream1 = client1.request(watch_req()).await?;

    let mn: Arc<MetaNode<TokioRuntime>> = tc.grpc_srv.as_ref().unwrap().get_meta_node().await;

    info!("one watcher");
    {
        let got = mn
            .dispatcher_handle
            .request_blocking(move |d| d.watch_senders().len())
            .await?;

        assert_eq!(1, got);
    }

    info!("second watcher");
    {
        let client2 = make_client(&addr)?;
        let _watch_stream2 = client2.request(watch_req()).await?;

        let got = mn
            .dispatcher_handle
            .request_blocking(move |d| d.watch_senders().len())
            .await?;

        assert_eq!(2, got);
    }

    info!("wait a while for MetaNode to process stream cleanup");
    sleep(Duration::from_millis(2_000)).await;

    info!("second watcher is removed");
    {
        let got = mn
            .dispatcher_handle
            .request_blocking(move |d| d.watch_senders().len())
            .await?;

        assert_eq!(1, got);
    }

    Ok(())
}

/// `watch` streams every change under a key range for as long as the caller
/// keeps the stream, so an unauthenticated watcher is a standing leak rather
/// than a single theft: it never shows up as one large dump the way `export`
/// does.
///
/// The client is the generated stub rather than `MetaGrpcClient`, since that
/// one handshakes before every call and could not express this request.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_watch_refuses_a_client_that_did_not_handshake() -> anyhow::Result<()> {
    let (_tc, addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;

    let mut client = MetaServiceClient::connect(format!("http://{}", addr)).await?;

    let status = client
        .watch(WatchRequest::new(s("a"), None))
        .await
        .unwrap_err();

    assert_eq!(status.code(), Code::Unauthenticated);
    // The message names the missing token, which is what tells this refusal
    // apart from one the handshake version gate would produce.
    assert_eq!(status.message(), "Error auth-token-bin is empty");

    Ok(())
}

/// A watch stream ends when a snapshot replaces the state machine: it would receive none of the
/// changes the snapshot carries. A new watch reads the data of the snapshot.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_watch_ends_when_a_snapshot_is_installed() -> anyhow::Result<()> {
    let (tc, _addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;
    let client = grpc_client(&tc).await?;
    client.upsert_kv(UpsertKV::update("a", b"a")).await?;

    let watch = || WatchRequest::new(s("a"), Some(s("e"))).with_initial_flush(true);
    let mut strm = client.watch_with_initialization(watch()).await?;
    let first = strm.message().await?.unwrap();
    assert_eq!(first.event.unwrap().current.unwrap().data, b("a"));
    assert!(
        strm.message()
            .await?
            .unwrap()
            .is_initialization_complete_flag()
    );

    // A snapshot of a later log, in which `a` is `foo`, installed through the raft service. Its
    // index must be above the locally committed ones.
    let last_log_id = log_id::<TypeConfig>(10, 2, 100);
    let snapshot_id = MetaSnapshotId::new(Some(last_log_id), 1);
    let ss_store: SnapshotStore<TokioRuntime> = SnapshotStore::new(tc.config.raft_config.clone());
    let db = {
        let mut sys_data = SysData::default();
        *sys_data.last_applied_mut() = Some(last_log_id);
        let voters = [0, 2].into_iter().collect();
        let membership = Membership::new_with_defaults(vec![voters], []);
        let membership_id = log_id::<TypeConfig>(10, 2, 99);
        *sys_data.last_membership_mut() = StoredMembership::new(Some(membership_id), membership);
        let writer = ss_store.new_writer()?;
        writer
            .write_kv_stream(stream::iter([]), snapshot_id.clone(), sys_data)
            .await?
    };
    let mut sys_data = db.sys_data().clone();
    // The entry below has seq 1, and no entry may have a seq above `curr_seq`.
    sys_data.update_seq(1);
    let foo = databend_meta_types::SeqV::new(1, b("foo"));
    let entries = vec![
        pb::InstallEntryV004 {
            version: 1,
            key_values: vec![pb::StreamItem::new(s("kv--/a"), Some(foo.into()))],
            commit: None,
        },
        pb::InstallEntryV004 {
            version: 1,
            key_values: vec![],
            commit: Some(pb::Commit {
                snapshot_id: snapshot_id.to_string(),
                sys_data: serde_json::to_string(&sys_data)?,
                vote: Some(pb::Vote::from(Vote::new_committed(10, 2))),
            }),
        },
    ];
    let mut raft_client = tc.raft_client().await?;
    raft_client
        .install_snapshot_v004(stream::iter(entries))
        .await?;

    let ended = tokio::time::timeout(Duration::from_secs(5), async {
        while let Ok(Some(_)) = strm.message().await {}
    })
    .await;
    assert!(
        ended.is_ok(),
        "the watch stream is still open after a snapshot replaced the state machine"
    );

    let mut strm = client.watch_with_initialization(watch()).await?;
    let first = strm.message().await?.unwrap();
    assert_eq!(first.event.unwrap().current.unwrap().data, b("foo"));

    Ok(())
}

/// A leader cut off from a quorum still leads in its own view, but changes may be committed
/// without it: once its lease expires, it ends its watch streams and refuses new ones.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_watch_ends_on_a_leader_without_a_quorum() -> anyhow::Result<()> {
    let mut tcs = crate::tests::start_metasrv_cluster::<TokioRuntime>(&[0, 1, 2]).await?;
    let client = make_client(tcs[0].config.grpc.api_address().unwrap())?;
    let mut strm = open_watch(&client).await?;

    tcs[1].grpc_srv.take().unwrap().do_stop(None).await;
    tcs[2].grpc_srv.take().unwrap().do_stop(None).await;

    // The lease is 3 s by default. A 1 s check interval and a short quorum probe
    // may delay closure, but another election timeout would exceed this bound.
    assert_watch_ends(&mut strm, Duration::from_secs(6)).await;
    assert_watch_refused(&client).await
}

/// A follower that has known no leader for longer than the election timeout ends its watch
/// streams and refuses new ones.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_watch_ends_on_a_follower_without_a_leader() -> anyhow::Result<()> {
    let mut tcs = crate::tests::start_metasrv_cluster::<TokioRuntime>(&[0, 1, 2]).await?;
    let client = make_client(tcs[2].config.grpc.api_address().unwrap())?;
    let mut strm = open_watch(&client).await?;

    tcs[0].grpc_srv.take().unwrap().do_stop(None).await;
    tcs[1].grpc_srv.take().unwrap().do_stop(None).await;

    // An election timeout to give up the stopped leader, then another without a leader.
    assert_watch_ends(&mut strm, Duration::from_secs(20)).await;
    assert_watch_refused(&client).await
}

/// A follower still connected to a leader in a minority must stop serving
/// watches. The leader's quiet period lets the follower time out and campaign.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_watch_ends_on_a_follower_in_the_leaders_minority() -> anyhow::Result<()> {
    let mut tcs = crate::tests::start_metasrv_cluster::<TokioRuntime>(&[0, 1, 2, 3, 4]).await?;
    let follower = tcs[1].grpc_srv.as_ref().unwrap();
    let node = follower.get_meta_node().await;
    let wait = node.raft.wait(Some(Duration::from_secs(5)));
    wait.metrics(
        |m| {
            let voters = m.membership_config.membership().voter_ids().count();
            m.current_leader == Some(0) && voters == 5
        },
        "follower sees the leader and all five voters",
    )
    .await?;

    let address = tcs[1].config.grpc.api_address().unwrap();
    let client = make_client(address)?;
    let mut stream = open_watch(&client).await?;

    // Nodes 0 and 1 can still communicate, but cannot form a quorum.
    for tc in &mut tcs[2..] {
        let mut server = tc.grpc_srv.take().unwrap();
        server.do_stop(None).await;
    }

    assert_watch_ends(&mut stream, Duration::from_secs(15)).await;
    assert_watch_refused(&client).await
}

/// A learner does not campaign, so the old leader can remain in its vote after
/// replication stops and across a restart. Neither is evidence of fresh contact.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_watch_refuses_a_learner_without_fresh_leader_contact() -> anyhow::Result<()> {
    let (mut leader, _) = crate::tests::start_metasrv::<TokioRuntime>().await?;
    let leader_raft_address = leader
        .config
        .raft_config
        .raft_api_addr::<TokioRuntime>()
        .await?;
    let mut learner = MetaSrvTestContext::<TokioRuntime>::new(1);
    learner.config.raft_config.single = false;
    learner.config.raft_config.learner = true;
    learner.config.raft_config.join = vec![leader_raft_address.to_string()];
    crate::tests::start_metasrv_with_context(&mut learner).await?;
    let client = grpc_client(&learner).await?;
    let mut stream = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match open_watch(&client).await {
                Ok(stream) => return Ok::<_, anyhow::Error>(stream),
                Err(err) => {
                    let Some(status) = err.downcast_ref::<tonic::Status>() else {
                        return Err(err);
                    };
                    if status.code() != Code::Unavailable {
                        return Err(err);
                    }
                }
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await??;

    let mut leader_server = leader.grpc_srv.take().unwrap();
    leader_server.do_stop(None).await;
    assert_watch_ends(&mut stream, Duration::from_secs(6)).await;
    assert_watch_refused(&client).await?;

    let mut learner_server = learner.grpc_srv.take().unwrap();
    learner_server.do_stop(None).await;
    // Disable joining on restart: the learner already has the on-disk cluster state.
    learner.config.raft_config.join.clear();
    learner.config.grpc.listen_port = None;
    sleep(Duration::from_secs(1)).await;
    crate::tests::start_metasrv_with_context(&mut learner).await?;
    let server = learner.grpc_srv.as_ref().unwrap();
    let node = server.get_meta_node().await;
    let metrics = node
        .raft
        .wait(Some(Duration::from_secs(2)))
        .current_leader(0, "a committed vote survives restart")
        .await?;
    assert_eq!(metrics.state, ServerState::Learner);

    let client = grpc_client(&learner).await?;
    assert_watch_refused(&client).await
}

/// A Raft failure leaves the client-facing listener and handshake up. Both public
/// watch APIs must move off that endpoint when it refuses a stream.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_watch_reconnects_on_another_node_after_refusal() -> anyhow::Result<()> {
    let tcs = crate::tests::start_metasrv_cluster::<TokioRuntime>(&[0, 1, 2]).await?;
    let addresses: Vec<_> = tcs
        .iter()
        .map(|tc| tc.config.grpc.api_address().unwrap())
        .collect();
    let stale_addr = addresses[2].clone();
    let client = make_client_with_endpoints(addresses.clone())?;
    client.set_current_endpoint(stale_addr.clone());
    let request = || WatchRequest::new(s("a"), Some(s("e")));
    let mut stream = client.watch(request()).await?;

    let server = tcs[2].grpc_srv.as_ref().unwrap();
    let node = server.get_meta_node().await;
    node.raft.shutdown().await?;
    // A stopped Raft must end watches on the next check, not after an election timeout.
    assert_watch_ends(&mut stream, Duration::from_secs(3)).await;

    let stale_client = make_client(&stale_addr)?;
    assert_watch_refused(&stale_client).await?;

    // Exercise both APIs independently, starting from the stale endpoint each time.
    let mut stream = client.watch(request()).await?;
    let leader = make_client(&addresses[0])?;
    leader.upsert_kv(UpsertKV::update("a", b"live")).await?;
    let response = tokio::time::timeout(Duration::from_secs(5), stream.message()).await??;
    let event = response.unwrap().event.unwrap();
    assert_eq!(event.current.unwrap().data, b("live"));

    client.set_current_endpoint(stale_addr);
    let mut stream = client
        .watch_with_initialization(request().with_initial_flush(true))
        .await?;
    let response = tokio::time::timeout(Duration::from_secs(5), stream.message()).await??;
    let event = response.unwrap().event.unwrap();
    assert_eq!(event.current.unwrap().data, b("live"));

    Ok(())
}

async fn open_watch(
    client: &ClientHandle<TokioRuntime>,
) -> anyhow::Result<Streaming<WatchResponse>> {
    let mut established = client.make_established_client().await?;
    let watch = WatchRequest::new(s("a"), Some(s("e")));
    Ok(established.watch(watch).await?.into_inner())
}

async fn assert_watch_ends(strm: &mut Streaming<WatchResponse>, within: Duration) {
    let ended = tokio::time::timeout(within, async {
        while let Ok(Some(_)) = strm.message().await {}
    })
    .await;
    assert!(
        ended.is_ok(),
        "the watch stream is still open after {within:?}"
    );
}

async fn assert_watch_refused(client: &ClientHandle<TokioRuntime>) -> anyhow::Result<()> {
    let mut established = client.make_established_client().await?;
    let watch = WatchRequest::new(s("a"), Some(s("e")));
    let err = established.watch(watch).await.unwrap_err();
    assert_eq!(err.code(), Code::Unavailable, "{err}");
    Ok(())
}

fn s(x: &str) -> String {
    x.to_string()
}

fn b(x: &str) -> Vec<u8> {
    x.as_bytes().to_vec()
}

/// Build a protobuf defined `SeqV`.
fn pb_seqv(seq: u64, data: &str, meta: Option<KvMeta>) -> Option<SeqV> {
    Some(SeqV::with_meta(seq, meta, b(data)))
}

/// Build an event represent a delete operation: i.e., prev is Some, result is None
fn del_event(key: &str, prev_seq: u64, prev_val: &str, prev_meta: Option<KvMeta>) -> Event {
    Event {
        key: s(key),
        prev: pb_seqv(prev_seq, prev_val, prev_meta),
        current: None,
    }
}

/// Build an event represent an add operation: i.e., prev is None, result is Some
fn add_event(key: &str, res_seq: u64, res_val: &str, meta: Option<KvMeta>) -> Event {
    Event {
        key: s(key),
        prev: None,
        current: pb_seqv(res_seq, res_val, meta),
    }
}

fn make_client(addr: impl ToString) -> anyhow::Result<Arc<ClientHandle<TokioRuntime>>> {
    make_client_with_endpoints(vec![addr.to_string()])
}

fn make_client_with_endpoints(
    addresses: Vec<String>,
) -> anyhow::Result<Arc<ClientHandle<TokioRuntime>>> {
    let client = MetaGrpcClient::<TokioRuntime>::try_create(
        addresses,
        "root",
        "xxx",
        None,
        Some(Duration::from_secs(10)),
        None,
        DEFAULT_GRPC_MESSAGE_SIZE,
    )?;

    Ok(client)
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
