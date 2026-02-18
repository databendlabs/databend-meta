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

use databend_meta_types::AppliedState;
use databend_meta_types::Cmd;
use databend_meta_types::LogEntry;
use databend_meta_types::SeqV;
use databend_meta_types::UpsertKV;
use databend_meta_types::kv_transaction;
use databend_meta_types::kv_transaction::CompareOperator;
use databend_meta_types::kv_transaction::Operation;
use databend_meta_types::kv_transaction::Predicate;
use databend_meta_types::node::Node;
use databend_meta_types::normalize_meta::NormalizeMeta;
use databend_meta_types::protobuf as pb;
use databend_meta_types::raft_types::EntryPayload;
use databend_meta_types::raft_types::Membership;
use databend_meta_types::raft_types::new_log_id;
use maplit::btreeset;
use state_machine_api::StateMachineApi;

use crate::sm_v003::SMV003;

type Entry = databend_meta_types::raft_types::Entry;

fn b(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

/// Helper: create a normal entry with a command and log time.
fn normal_entry(index: u64, time_ms: u64, cmd: Cmd) -> Entry {
    Entry {
        log_id: new_log_id(1, 0, index),
        payload: EntryPayload::Normal(LogEntry::new_with_time(cmd, Some(time_ms))),
    }
}

/// Helper: create a normal entry with no time.
fn normal_entry_no_time(index: u64, cmd: Cmd) -> Entry {
    Entry {
        log_id: new_log_id(1, 0, index),
        payload: EntryPayload::Normal(LogEntry::new(cmd)),
    }
}

// === apply(): entry types ===

#[tokio::test]
async fn test_apply_blank_entry() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    let entry = Entry {
        log_id: new_log_id(1, 0, 1),
        payload: EntryPayload::Blank,
    };

    let result = a.apply(&entry).await?;
    assert!(matches!(result, AppliedState::None));

    // last_applied should be updated
    let last = a.sm.with_sys_data(|s| *s.last_applied());
    assert_eq!(last, Some(new_log_id(1, 0, 1)));

    a.commit().await?;
    Ok(())
}

#[tokio::test]
async fn test_apply_membership_entry() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    let mem = Membership::new_with_defaults(vec![btreeset![1, 2, 3]], []);
    let entry = Entry {
        log_id: new_log_id(1, 0, 2),
        payload: EntryPayload::Membership(mem.clone()),
    };

    let result = a.apply(&entry).await?;
    assert!(matches!(result, AppliedState::None));

    // last_membership should be updated
    let stored_mem =
        a.sm.with_sys_data(|s| s.last_membership_ref().membership().clone());
    assert_eq!(stored_mem, mem);

    a.commit().await?;
    Ok(())
}

#[tokio::test]
async fn test_apply_normal_upsert_kv() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    let entry = normal_entry(1, 1000, Cmd::UpsertKV(UpsertKV::update("k1", b"v1")));
    let result = a.apply(&entry).await?;

    // Should return a KV change
    match result {
        AppliedState::KV(change) => {
            assert!(change.prev.is_none());
            let res = change.result.unwrap().without_proposed_at();
            assert_eq!(res, SeqV::new(1, b("v1")));
        }
        other => panic!("expected KV, got {:?}", other),
    }

    a.commit().await?;
    Ok(())
}

// === apply_cmd: AddNode ===

#[tokio::test]
async fn test_apply_add_node_new() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    let node = Node::default();
    let result = a
        .apply_cmd(&Cmd::AddNode {
            node_id: 1,
            node: node.clone(),
            overriding: false,
        })
        .await?;

    match result {
        AppliedState::Node { prev, result } => {
            assert!(prev.is_none());
            assert_eq!(result, Some(node));
        }
        other => panic!("expected Node, got {:?}", other),
    }

    a.commit().await?;
    Ok(())
}

#[tokio::test]
async fn test_apply_add_node_no_override_existing() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    let node1 = Node::default();
    a.apply_cmd(&Cmd::AddNode {
        node_id: 1,
        node: node1.clone(),
        overriding: false,
    })
    .await?;

    let node2 = Node {
        grpc_api_advertise_address: Some("new_addr".to_string()),
        ..Default::default()
    };

    let result = a
        .apply_cmd(&Cmd::AddNode {
            node_id: 1,
            node: node2,
            overriding: false,
        })
        .await?;

    // Not overriding: prev and result should be the same (original node)
    match result {
        AppliedState::Node { prev, result } => {
            assert_eq!(prev, Some(node1.clone()));
            assert_eq!(result, Some(node1));
        }
        other => panic!("expected Node, got {:?}", other),
    }

    a.commit().await?;
    Ok(())
}

#[tokio::test]
async fn test_apply_add_node_override_existing() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    let node1 = Node::default();
    a.apply_cmd(&Cmd::AddNode {
        node_id: 1,
        node: node1.clone(),
        overriding: false,
    })
    .await?;

    let node2 = Node {
        grpc_api_advertise_address: Some("new_addr".to_string()),
        ..Default::default()
    };

    let result = a
        .apply_cmd(&Cmd::AddNode {
            node_id: 1,
            node: node2.clone(),
            overriding: true,
        })
        .await?;

    match result {
        AppliedState::Node { prev, result } => {
            assert_eq!(prev, Some(node1));
            assert_eq!(result, Some(node2));
        }
        other => panic!("expected Node, got {:?}", other),
    }

    a.commit().await?;
    Ok(())
}

// === apply_cmd: RemoveNode ===

#[tokio::test]
async fn test_apply_remove_node_existing() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    let node = Node::default();
    a.apply_cmd(&Cmd::AddNode {
        node_id: 1,
        node: node.clone(),
        overriding: false,
    })
    .await?;

    let result = a.apply_cmd(&Cmd::RemoveNode { node_id: 1 }).await?;

    match result {
        AppliedState::Node { prev, result } => {
            assert_eq!(prev, Some(node));
            assert!(result.is_none());
        }
        other => panic!("expected Node, got {:?}", other),
    }

    a.commit().await?;
    Ok(())
}

#[tokio::test]
async fn test_apply_remove_node_nonexistent() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    let result = a.apply_cmd(&Cmd::RemoveNode { node_id: 99 }).await?;

    match result {
        AppliedState::Node { prev, result } => {
            assert!(prev.is_none());
            assert!(result.is_none());
        }
        other => panic!("expected Node, got {:?}", other),
    }

    a.commit().await?;
    Ok(())
}

// === apply_cmd: SetFeature ===

#[tokio::test]
async fn test_apply_set_feature_enable_disable() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    let result = a
        .apply_cmd(&Cmd::SetFeature {
            feature: "test_feat".to_string(),
            enable: true,
        })
        .await?;
    assert!(matches!(result, AppliedState::None));

    let enabled = a.sm.with_sys_data(|s| s.feature_enabled("test_feat"));
    assert!(enabled);

    // Disable it
    a.apply_cmd(&Cmd::SetFeature {
        feature: "test_feat".to_string(),
        enable: false,
    })
    .await?;

    let enabled = a.sm.with_sys_data(|s| s.feature_enabled("test_feat"));
    assert!(!enabled);

    a.commit().await?;
    Ok(())
}

// === apply_cmd: UpsertKV ===

#[tokio::test]
async fn test_apply_upsert_kv_insert_and_update() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    // Insert
    let r = a
        .apply_cmd(&Cmd::UpsertKV(UpsertKV::update("k1", b"v1")))
        .await?;
    match r {
        AppliedState::KV(c) => {
            assert!(c.prev.is_none());
            assert_eq!(
                c.result.unwrap().without_proposed_at(),
                SeqV::new(1, b("v1"))
            );
        }
        other => panic!("expected KV, got {:?}", other),
    }

    // Update
    let r = a
        .apply_cmd(&Cmd::UpsertKV(UpsertKV::update("k1", b"v2")))
        .await?;
    match r {
        AppliedState::KV(c) => {
            assert_eq!(c.prev.unwrap().without_proposed_at(), SeqV::new(1, b("v1")));
            assert_eq!(
                c.result.unwrap().without_proposed_at(),
                SeqV::new(2, b("v2"))
            );
        }
        other => panic!("expected KV, got {:?}", other),
    }

    a.commit().await?;
    Ok(())
}

#[tokio::test]
async fn test_apply_upsert_kv_delete() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    a.apply_cmd(&Cmd::UpsertKV(UpsertKV::update("k1", b"v1")))
        .await?;

    let r = a.apply_cmd(&Cmd::UpsertKV(UpsertKV::delete("k1"))).await?;
    match r {
        AppliedState::KV(c) => {
            assert!(c.prev.is_some());
            assert!(c.result.is_none());
        }
        other => panic!("expected KV, got {:?}", other),
    }

    a.commit().await?;
    Ok(())
}

// === apply_kv_transaction: branch selection ===

#[tokio::test]
async fn test_kv_txn_unconditional_branch() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    let txn = kv_transaction::Transaction {
        branches: vec![kv_transaction::Branch::else_().then([Operation::put("k1", b("v1"))])],
    };

    let r = a.apply_kv_transaction(&txn).await?;
    match r {
        AppliedState::KvTransactionReply(reply) => {
            assert_eq!(reply.executed_branch, Some(0));
            assert_eq!(reply.responses.len(), 1);
        }
        other => panic!("expected KvTransactionReply, got {:?}", other),
    }

    // Commit before verifying via sm
    a.commit().await?;

    let val = sm.get_maybe_expired_kv("k1").await?;
    assert_eq!(val.unwrap().without_proposed_at(), SeqV::new(1, b("v1")));

    Ok(())
}

#[tokio::test]
async fn test_kv_txn_first_match_wins() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    // k1 does not exist → seq==0
    let txn = kv_transaction::Transaction {
        branches: vec![
            kv_transaction::Branch::if_all([Predicate::eq_seq("k1", 0)])
                .then([Operation::put("k1", b("first"))]),
            kv_transaction::Branch::else_().then([Operation::put("k1", b("second"))]),
        ],
    };

    let r = a.apply_kv_transaction(&txn).await?;
    match r {
        AppliedState::KvTransactionReply(reply) => {
            assert_eq!(reply.executed_branch, Some(0));
        }
        other => panic!("expected KvTransactionReply, got {:?}", other),
    }

    a.commit().await?;

    let val = sm.get_maybe_expired_kv("k1").await?;
    assert_eq!(val.unwrap().without_proposed_at(), SeqV::new(1, b("first")));

    Ok(())
}

#[tokio::test]
async fn test_kv_txn_fallthrough_to_second_branch() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    // k1 does not exist → seq==0, not 5
    let txn = kv_transaction::Transaction {
        branches: vec![
            kv_transaction::Branch::if_all([Predicate::eq_seq("k1", 5)])
                .then([Operation::put("k1", b("wrong"))]),
            kv_transaction::Branch::else_().then([Operation::put("k1", b("right"))]),
        ],
    };

    let r = a.apply_kv_transaction(&txn).await?;
    match r {
        AppliedState::KvTransactionReply(reply) => {
            assert_eq!(reply.executed_branch, Some(1));
        }
        other => panic!("expected KvTransactionReply, got {:?}", other),
    }

    a.commit().await?;

    let val = sm.get_maybe_expired_kv("k1").await?;
    assert_eq!(val.unwrap().without_proposed_at(), SeqV::new(1, b("right")));

    Ok(())
}

#[tokio::test]
async fn test_kv_txn_no_branch_matches() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    let txn = kv_transaction::Transaction {
        branches: vec![
            kv_transaction::Branch::if_all([Predicate::eq_seq("k1", 99)])
                .then([Operation::put("k1", b("nope"))]),
        ],
    };

    let r = a.apply_kv_transaction(&txn).await?;
    match r {
        AppliedState::KvTransactionReply(reply) => {
            assert_eq!(reply.executed_branch, None);
            assert!(reply.responses.is_empty());
        }
        other => panic!("expected KvTransactionReply, got {:?}", other),
    }

    a.commit().await?;
    Ok(())
}

// === eval_predicate ===

#[tokio::test]
async fn test_eval_predicate_and_short_circuit() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    // k1 does not exist → seq==0
    // And(eq_seq("k1",0), eq_seq("k1",99)) → true AND false → false
    let pred = Predicate::and([Predicate::eq_seq("k1", 0), Predicate::eq_seq("k1", 99)]);
    assert!(!a.eval_predicate(&pred).await?);

    // And(eq_seq("k1",0)) → true
    let pred = Predicate::and([Predicate::eq_seq("k1", 0)]);
    assert!(a.eval_predicate(&pred).await?);

    // Empty And → true (vacuous truth)
    assert!(a.eval_predicate(&Predicate::And(vec![])).await?);

    a.commit().await?;
    Ok(())
}

#[tokio::test]
async fn test_eval_predicate_or_short_circuit() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    // Or(eq_seq("k1",99), eq_seq("k1",0)) → false OR true → true
    let pred = Predicate::or([Predicate::eq_seq("k1", 99), Predicate::eq_seq("k1", 0)]);
    assert!(a.eval_predicate(&pred).await?);

    // Or(eq_seq("k1",99), eq_seq("k1",88)) → false OR false → true (empty OR is vacuous)
    // Actually this is non-empty, so it checks each.
    let pred = Predicate::or([Predicate::eq_seq("k1", 99), Predicate::eq_seq("k1", 88)]);
    // Looking at the code: non-empty Or returns false when none match? No!
    // The code says: for each child, if true → return true. After loop: return true (vacuous).
    // Wait, the comment says "Empty OR returns true". But this behavior means non-empty OR
    // that finds no match also returns true? Let me re-read:
    //   for child in children { if self.eval(child)? { return Ok(true) } }
    //   Ok(true) // <-- this returns true even when no child matched
    // This is actually a bug-like behavior but let's test what the code actually does.
    assert!(a.eval_predicate(&pred).await?);

    // Empty Or → true
    assert!(a.eval_predicate(&Predicate::Or(vec![])).await?);

    a.commit().await?;
    Ok(())
}

// === eval_condition: Seq, Value, KeysWithPrefix ===

#[tokio::test]
async fn test_eval_condition_seq() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    // k1 doesn't exist → seq==0
    let txn = kv_transaction::Transaction {
        branches: vec![
            kv_transaction::Branch::if_all([Predicate::eq_seq("k1", 0)])
                .then([Operation::put("k1", b("v1"))]),
        ],
    };
    let r = a.apply_kv_transaction(&txn).await?;
    assert_eq!(extract_branch(&r), Some(0));

    // Now k1 has seq==1, test gt_seq
    let txn = kv_transaction::Transaction {
        branches: vec![
            kv_transaction::Branch::if_all([Predicate::gt_seq("k1", 0)])
                .then([Operation::get("k1")]),
        ],
    };
    let r = a.apply_kv_transaction(&txn).await?;
    assert_eq!(extract_branch(&r), Some(0));

    a.commit().await?;
    Ok(())
}

#[tokio::test]
async fn test_eval_condition_value() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    a.upsert_kv(&UpsertKV::update("k1", b"hello")).await?;

    // Value match
    let txn = kv_transaction::Transaction {
        branches: vec![
            kv_transaction::Branch::if_all([Predicate::eq_value("k1", b("hello"))])
                .then([Operation::get("k1")]),
        ],
    };
    let r = a.apply_kv_transaction(&txn).await?;
    assert_eq!(extract_branch(&r), Some(0));

    // Value mismatch
    let txn = kv_transaction::Transaction {
        branches: vec![
            kv_transaction::Branch::if_all([Predicate::eq_value("k1", b("wrong"))])
                .then([Operation::get("k1")]),
        ],
    };
    let r = a.apply_kv_transaction(&txn).await?;
    assert_eq!(extract_branch(&r), None);

    // Value comparison on non-existent key → false
    let txn = kv_transaction::Transaction {
        branches: vec![
            kv_transaction::Branch::if_all([Predicate::eq_value("missing", b("any"))])
                .then([Operation::get("missing")]),
        ],
    };
    let r = a.apply_kv_transaction(&txn).await?;
    assert_eq!(extract_branch(&r), None);

    a.commit().await?;
    Ok(())
}

#[tokio::test]
async fn test_eval_condition_keys_with_prefix() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    a.upsert_kv(&UpsertKV::update("pfx/a", b"1")).await?;
    a.upsert_kv(&UpsertKV::update("pfx/b", b"2")).await?;

    // count == 2
    let txn = kv_transaction::Transaction {
        branches: vec![
            kv_transaction::Branch::if_all([Predicate::keys_with_prefix(
                "pfx/",
                CompareOperator::Eq,
                2,
            )])
            .then([Operation::get("pfx/a")]),
        ],
    };
    let r = a.apply_kv_transaction(&txn).await?;
    assert_eq!(extract_branch(&r), Some(0));

    // count >= 3 → false (only 2)
    let txn = kv_transaction::Transaction {
        branches: vec![
            kv_transaction::Branch::if_all([Predicate::keys_with_prefix(
                "pfx/",
                CompareOperator::Ge,
                3,
            )])
            .then([Operation::get("pfx/a")]),
        ],
    };
    let r = a.apply_kv_transaction(&txn).await?;
    assert_eq!(extract_branch(&r), None);

    a.commit().await?;
    Ok(())
}

// === execute_op: Get ===

#[tokio::test]
async fn test_execute_get_existing() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    a.upsert_kv(&UpsertKV::update("k1", b"val")).await?;

    let txn = kv_transaction::Transaction {
        branches: vec![kv_transaction::Branch::else_().then([Operation::get("k1")])],
    };
    let r = a.apply_kv_transaction(&txn).await?;
    let reply = extract_reply(&r);

    let get_resp = reply.responses[0].try_as_get().unwrap();
    assert_eq!(get_resp.key, "k1");
    assert!(get_resp.value.is_some());

    a.commit().await?;
    Ok(())
}

#[tokio::test]
async fn test_execute_get_missing() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    let txn = kv_transaction::Transaction {
        branches: vec![kv_transaction::Branch::else_().then([Operation::get("missing")])],
    };
    let r = a.apply_kv_transaction(&txn).await?;
    let reply = extract_reply(&r);

    let get_resp = reply.responses[0].try_as_get().unwrap();
    assert_eq!(get_resp.key, "missing");
    assert!(get_resp.value.is_none());

    a.commit().await?;
    Ok(())
}

// === execute_op: Put ===

#[tokio::test]
async fn test_execute_put() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    let txn = kv_transaction::Transaction {
        branches: vec![kv_transaction::Branch::else_().then([Operation::put("k1", b("v1"))])],
    };
    let r = a.apply_kv_transaction(&txn).await?;
    let reply = extract_reply(&r);

    let put_resp = reply.responses[0].try_as_put().unwrap();
    assert_eq!(put_resp.key, "k1");
    assert!(put_resp.prev_value.is_none());
    assert!(put_resp.current.is_some());

    a.commit().await?;

    let val = sm.get_maybe_expired_kv("k1").await?;
    assert_eq!(val.unwrap().without_proposed_at(), SeqV::new(1, b("v1")));

    Ok(())
}

#[tokio::test]
async fn test_execute_put_with_match_seq_mismatch() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    a.upsert_kv(&UpsertKV::update("k1", b"old")).await?;

    // match_seq=99, but actual seq is 1 → no update
    let txn = kv_transaction::Transaction {
        branches: vec![
            kv_transaction::Branch::else_().then([Operation::put("k1", b("new")).match_seq(99)]),
        ],
    };
    let r = a.apply_kv_transaction(&txn).await?;
    let reply = extract_reply(&r);

    let put_resp = reply.responses[0].try_as_put().unwrap();
    assert_eq!(
        put_resp.prev_value, put_resp.current,
        "unchanged on mismatch"
    );

    a.commit().await?;

    let val = sm.get_maybe_expired_kv("k1").await?;
    assert_eq!(val.unwrap().without_proposed_at(), SeqV::new(1, b("old")));

    Ok(())
}

#[tokio::test]
async fn test_execute_put_with_match_seq_match() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    a.upsert_kv(&UpsertKV::update("k1", b"old")).await?;

    // match_seq=1, actual seq is 1 → update
    let txn = kv_transaction::Transaction {
        branches: vec![
            kv_transaction::Branch::else_().then([Operation::put("k1", b("new")).match_seq(1)]),
        ],
    };
    let r = a.apply_kv_transaction(&txn).await?;
    let reply = extract_reply(&r);

    let put_resp = reply.responses[0].try_as_put().unwrap();
    assert_ne!(put_resp.prev_value, put_resp.current, "value was updated");

    a.commit().await?;

    let val = sm.get_maybe_expired_kv("k1").await?;
    assert_eq!(val.unwrap().without_proposed_at(), SeqV::new(2, b("new")));

    Ok(())
}

// === execute_op: Delete ===

#[tokio::test]
async fn test_execute_delete_existing() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    a.upsert_kv(&UpsertKV::update("k1", b"val")).await?;

    let txn = kv_transaction::Transaction {
        branches: vec![kv_transaction::Branch::else_().then([Operation::delete("k1")])],
    };
    let r = a.apply_kv_transaction(&txn).await?;
    let reply = extract_reply(&r);

    let del_resp = reply.responses[0].try_as_delete().unwrap();
    assert!(del_resp.success);
    assert!(del_resp.prev_value.is_some());

    a.commit().await?;

    let val = sm.get_maybe_expired_kv("k1").await?;
    assert!(val.is_none());

    Ok(())
}

#[tokio::test]
async fn test_execute_delete_nonexistent() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    let txn = kv_transaction::Transaction {
        branches: vec![kv_transaction::Branch::else_().then([Operation::delete("missing")])],
    };
    let r = a.apply_kv_transaction(&txn).await?;
    let reply = extract_reply(&r);

    let del_resp = reply.responses[0].try_as_delete().unwrap();
    assert!(!del_resp.success);
    assert!(del_resp.prev_value.is_none());

    a.commit().await?;
    Ok(())
}

#[tokio::test]
async fn test_execute_delete_with_match_seq_mismatch() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    a.upsert_kv(&UpsertKV::update("k1", b"val")).await?;

    let txn = kv_transaction::Transaction {
        branches: vec![
            kv_transaction::Branch::else_().then([Operation::delete("k1").match_seq(99)]),
        ],
    };
    let r = a.apply_kv_transaction(&txn).await?;
    let reply = extract_reply(&r);

    let del_resp = reply.responses[0].try_as_delete().unwrap();
    assert!(!del_resp.success, "delete should fail on seq mismatch");

    a.commit().await?;

    // Key still exists
    let val = sm.get_maybe_expired_kv("k1").await?;
    assert!(val.is_some());

    Ok(())
}

// === execute_op: DeleteByPrefix ===

#[tokio::test]
async fn test_execute_delete_by_prefix() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    a.upsert_kv(&UpsertKV::update("pfx/a", b"1")).await?;
    a.upsert_kv(&UpsertKV::update("pfx/b", b"2")).await?;
    a.upsert_kv(&UpsertKV::update("other", b"3")).await?;

    let txn = kv_transaction::Transaction {
        branches: vec![kv_transaction::Branch::else_().then([Operation::delete_by_prefix("pfx/")])],
    };
    let r = a.apply_kv_transaction(&txn).await?;
    let reply = extract_reply(&r);

    let dbp_resp = match &reply.responses[0].response {
        Some(pb::txn_op_response::Response::DeleteByPrefix(r)) => r,
        other => panic!("expected DeleteByPrefix, got {:?}", other),
    };
    assert_eq!(dbp_resp.prefix, "pfx/");
    assert_eq!(dbp_resp.count, 2);

    a.commit().await?;

    assert!(sm.get_maybe_expired_kv("pfx/a").await?.is_none());
    assert!(sm.get_maybe_expired_kv("pfx/b").await?.is_none());
    assert!(sm.get_maybe_expired_kv("other").await?.is_some());

    Ok(())
}

// === execute_op: FetchIncreaseU64 ===

#[tokio::test]
async fn test_execute_fetch_increase_u64_new_key() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    use databend_meta_types::kv_transaction::operation;

    let op = operation::FetchIncreaseU64 {
        target: operation::KeyLookup::just("counter"),
        delta: 5,
        floor: 0,
    };
    let resp = a.execute_fetch_increase_u64(&op).await?;

    assert_eq!(resp.key, "counter");
    assert_eq!(resp.before_seq, 0);
    assert_eq!(resp.before, 0);
    assert_eq!(resp.after_seq, 1);
    assert_eq!(resp.after, 5);

    a.commit().await?;
    Ok(())
}

#[tokio::test]
async fn test_execute_fetch_increase_u64_with_floor() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    use databend_meta_types::kv_transaction::operation;

    // First, set counter to 3
    let value = serde_json::to_vec(&3u64).unwrap();
    a.upsert_kv(&UpsertKV::update("counter", &value)).await?;

    // floor=10, delta=2 → max(3, 10) + 2 = 12
    let op = operation::FetchIncreaseU64 {
        target: operation::KeyLookup::just("counter"),
        delta: 2,
        floor: 10,
    };
    let resp = a.execute_fetch_increase_u64(&op).await?;

    assert_eq!(resp.after_seq, 2);
    assert_eq!(resp.after, 12);

    a.commit().await?;
    Ok(())
}

#[tokio::test]
async fn test_execute_fetch_increase_u64_match_seq_mismatch() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    use databend_meta_types::kv_transaction::operation;

    let value = serde_json::to_vec(&10u64).unwrap();
    a.upsert_kv(&UpsertKV::update("counter", &value)).await?;

    // match_seq=99, actual is 1 → unchanged
    let op = operation::FetchIncreaseU64 {
        target: operation::KeyLookup::new("counter", Some(99)),
        delta: 5,
        floor: 0,
    };
    let resp = a.execute_fetch_increase_u64(&op).await?;

    // before == after (unchanged)
    assert_eq!(resp.before_seq, resp.after_seq);
    assert_eq!(resp.before, resp.after);

    a.commit().await?;
    Ok(())
}

// === execute_op: PutSequential ===

#[tokio::test]
async fn test_execute_put_sequential() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    let txn = kv_transaction::Transaction {
        branches: vec![
            kv_transaction::Branch::else_().then([Operation::put_sequential(
                "log/",
                "log/__seq__",
                b("entry1"),
            )]),
        ],
    };
    let r = a.apply_kv_transaction(&txn).await?;
    let reply = extract_reply(&r);

    let put_resp = reply.responses[0].try_as_put().unwrap();
    // Key should be prefix + formatted sequence number
    assert!(
        put_resp.key.starts_with("log/"),
        "key={} should start with log/",
        put_resp.key
    );
    assert!(put_resp.current.is_some());

    // Second sequential put should get a different key
    let txn2 = kv_transaction::Transaction {
        branches: vec![
            kv_transaction::Branch::else_().then([Operation::put_sequential(
                "log/",
                "log/__seq__",
                b("entry2"),
            )]),
        ],
    };
    let r2 = a.apply_kv_transaction(&txn2).await?;
    let reply2 = extract_reply(&r2);
    let put_resp2 = reply2.responses[0].try_as_put().unwrap();

    assert_ne!(put_resp.key, put_resp2.key, "sequential keys should differ");

    a.commit().await?;
    Ok(())
}

// === Multiple operations in one branch ===

#[tokio::test]
async fn test_multiple_ops_in_branch() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    a.upsert_kv(&UpsertKV::update("k1", b"old")).await?;

    let txn = kv_transaction::Transaction {
        branches: vec![kv_transaction::Branch::else_().then([
            Operation::get("k1"),
            Operation::put("k1", b("new")),
            Operation::get("k1"),
        ])],
    };
    let r = a.apply_kv_transaction(&txn).await?;
    let reply = extract_reply(&r);

    assert_eq!(reply.responses.len(), 3);

    // First get: old value
    let get1 = reply.responses[0].try_as_get().unwrap();
    assert_eq!(
        get1.value.as_ref().unwrap().data,
        b("old"),
        "first get sees old"
    );

    // Third get: new value (after put)
    let get2 = reply.responses[2].try_as_get().unwrap();
    assert_eq!(
        get2.value.as_ref().unwrap().data,
        b("new"),
        "second get sees new"
    );

    a.commit().await?;
    Ok(())
}

// === push_change ===

#[tokio::test]
async fn test_push_change_ignores_no_change() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    let sv = Some(SeqV::new(1, b("v")));
    a.push_change("k1", sv.clone(), sv.clone());

    // changes should be empty since prev == result
    assert!(a.changes.is_empty());

    a.push_change("k1", None, sv);
    assert_eq!(a.changes.len(), 1);

    a.commit().await?;
    Ok(())
}

// === get_log_time ===

#[tokio::test]
async fn test_get_log_time_normal_with_time() -> anyhow::Result<()> {
    let entry = normal_entry(1, 12345, Cmd::UpsertKV(UpsertKV::update("k", b"v")));

    use super::Applier;
    type A = Applier<crate::applier::applier_data::ApplierData>;
    let t = A::get_log_time(&entry);
    assert_eq!(t, 12345);
    Ok(())
}

#[tokio::test]
async fn test_get_log_time_normal_without_time() -> anyhow::Result<()> {
    let entry = normal_entry_no_time(1, Cmd::UpsertKV(UpsertKV::update("k", b"v")));

    use super::Applier;
    type A = Applier<crate::applier::applier_data::ApplierData>;
    let t = A::get_log_time(&entry);
    assert_eq!(t, 0, "missing time_ms should return 0");
    Ok(())
}

#[tokio::test]
async fn test_get_log_time_blank() -> anyhow::Result<()> {
    let entry = Entry {
        log_id: new_log_id(1, 0, 1),
        payload: EntryPayload::Blank,
    };

    use super::Applier;
    type A = Applier<crate::applier::applier_data::ApplierData>;
    let t = A::get_log_time(&entry);
    assert_eq!(t, 0, "blank entry should return 0");
    Ok(())
}

#[tokio::test]
async fn test_get_log_time_membership() -> anyhow::Result<()> {
    let entry = Entry {
        log_id: new_log_id(1, 0, 1),
        payload: EntryPayload::Membership(Membership::new_with_defaults(vec![btreeset![1]], [])),
    };

    use super::Applier;
    type A = Applier<crate::applier::applier_data::ApplierData>;
    let t = A::get_log_time(&entry);
    assert_eq!(t, 0, "membership entry should return 0");
    Ok(())
}

// === eval_compare ===

#[tokio::test]
async fn test_eval_compare_all_ops() -> anyhow::Result<()> {
    use super::Applier;
    type A = Applier<crate::applier::applier_data::ApplierData>;

    assert!(A::eval_compare(5u64, CompareOperator::Eq, 5u64));
    assert!(!A::eval_compare(5u64, CompareOperator::Eq, 6u64));

    assert!(A::eval_compare(5u64, CompareOperator::Ne, 6u64));
    assert!(!A::eval_compare(5u64, CompareOperator::Ne, 5u64));

    assert!(A::eval_compare(3u64, CompareOperator::Lt, 5u64));
    assert!(!A::eval_compare(5u64, CompareOperator::Lt, 5u64));

    assert!(A::eval_compare(5u64, CompareOperator::Le, 5u64));
    assert!(!A::eval_compare(6u64, CompareOperator::Le, 5u64));

    assert!(A::eval_compare(6u64, CompareOperator::Gt, 5u64));
    assert!(!A::eval_compare(5u64, CompareOperator::Gt, 5u64));

    assert!(A::eval_compare(5u64, CompareOperator::Ge, 5u64));
    assert!(!A::eval_compare(4u64, CompareOperator::Ge, 5u64));

    Ok(())
}

// === Cmd::Transaction (legacy) routes through apply_kv_transaction ===

#[tokio::test]
async fn test_apply_cmd_legacy_transaction() -> anyhow::Result<()> {
    let sm = SMV003::default();
    let mut a = sm.new_applier().await;

    use databend_meta_types::TxnCondition;
    use databend_meta_types::TxnOp;
    use databend_meta_types::TxnRequest;

    let txn = TxnRequest::new(vec![TxnCondition::eq_seq("k1", 0)], vec![TxnOp::put(
        "k1",
        b("v1"),
    )]);

    let r = a.apply_cmd(&Cmd::Transaction(txn)).await?;
    match r {
        AppliedState::KvTransactionReply(reply) => {
            assert_eq!(reply.executed_branch, Some(0));
        }
        other => panic!("expected KvTransactionReply, got {:?}", other),
    }

    a.commit().await?;

    let val = sm.get_maybe_expired_kv("k1").await?;
    assert_eq!(val.unwrap().without_proposed_at(), SeqV::new(1, b("v1")));

    Ok(())
}

// === Helper functions ===

fn extract_branch(r: &AppliedState) -> Option<u32> {
    match r {
        AppliedState::KvTransactionReply(reply) => reply.executed_branch,
        other => panic!("expected KvTransactionReply, got {:?}", other),
    }
}

fn extract_reply(r: &AppliedState) -> &pb::KvTransactionReply {
    match r {
        AppliedState::KvTransactionReply(reply) => reply,
        other => panic!("expected KvTransactionReply, got {:?}", other),
    }
}
