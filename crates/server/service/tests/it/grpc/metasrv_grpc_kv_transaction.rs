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

//! Integration tests for the `KvTransaction` gRPC endpoint.

use databend_meta_kvapi::kvapi::KvApiExt;
use databend_meta_runtime_api::TokioRuntime;
use databend_meta_types::SeqV;
use databend_meta_types::TxnCondition;
use databend_meta_types::TxnOp;
use databend_meta_types::UpsertKV;
use databend_meta_types::protobuf as pb;
use test_harness::test;

use crate::testing::meta_service_test_harness;

fn b(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

/// Build a `KvTransactionRequest` from branches (predicate, ops).
fn kv_transaction_req(
    branches: Vec<(Option<pb::BooleanExpression>, Vec<pb::TxnOp>)>,
) -> pb::KvTransactionRequest {
    pb::KvTransactionRequest {
        branches: branches
            .into_iter()
            .map(|(pred, ops)| pb::ConditionalOperation::new(pred, ops))
            .collect(),
    }
}

/// Single unconditional branch: always executes.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_kv_transaction_unconditional() -> anyhow::Result<()> {
    let (tc, _addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;
    let client = tc.grpc_client().await?;

    let txn = kv_transaction_req(vec![(None, vec![TxnOp::put("k1", b("v1"))])]);

    let reply = client.transaction_v2(txn).await?;
    assert_eq!(reply.executed_branch, Some(0));
    assert_eq!(reply.responses.len(), 1);

    // Verify the put took effect
    let val = client.get_kv("k1").await?;
    assert_eq!(val.unwrap().data, b("v1"));

    Ok(())
}

/// Two branches: first has a failing predicate, second is unconditional.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_kv_transaction_multi_branch_fallthrough() -> anyhow::Result<()> {
    let (tc, _addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;
    let client = tc.grpc_client().await?;

    // k1 does not exist (seq=0)
    let txn = kv_transaction_req(vec![
        // Branch 0: requires k1 seq==5 → fails
        (
            Some(pb::BooleanExpression::from_conditions_and([
                TxnCondition::eq_seq("k1", 5),
            ])),
            vec![TxnOp::put("k1", b("branch0"))],
        ),
        // Branch 1: unconditional fallback
        (None, vec![TxnOp::put("k1", b("branch1"))]),
    ]);

    let reply = client.transaction_v2(txn).await?;
    assert_eq!(reply.executed_branch, Some(1));

    let val = client.get_kv("k1").await?;
    assert_eq!(val.unwrap().data, b("branch1"));

    Ok(())
}

/// First branch matches, second is skipped.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_kv_transaction_first_branch_matches() -> anyhow::Result<()> {
    let (tc, _addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;
    let client = tc.grpc_client().await?;

    // k1 does not exist → seq==0
    let txn = kv_transaction_req(vec![
        // Branch 0: requires k1 seq==0 → matches
        (
            Some(pb::BooleanExpression::from_conditions_and([
                TxnCondition::eq_seq("k1", 0),
            ])),
            vec![TxnOp::put("k1", b("first"))],
        ),
        // Branch 1: would also match, but should be skipped
        (None, vec![TxnOp::put("k1", b("second"))]),
    ]);

    let reply = client.transaction_v2(txn).await?;
    assert_eq!(reply.executed_branch, Some(0));

    let val = client.get_kv("k1").await?;
    assert_eq!(val.unwrap().data, b("first"));

    Ok(())
}

/// No branches match → executed_branch is None.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_kv_transaction_no_match() -> anyhow::Result<()> {
    let (tc, _addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;
    let client = tc.grpc_client().await?;

    let txn = kv_transaction_req(vec![
        // Only branch: requires k1 seq==99 → fails (key doesn't exist)
        (
            Some(pb::BooleanExpression::from_conditions_and([
                TxnCondition::eq_seq("k1", 99),
            ])),
            vec![TxnOp::put("k1", b("nope"))],
        ),
    ]);

    let reply = client.transaction_v2(txn).await?;
    assert_eq!(reply.executed_branch, None);
    assert!(reply.responses.is_empty());

    // Key should not exist
    let val = client.get_kv("k1").await?;
    assert!(val.is_none());

    Ok(())
}

/// Test Get operation returns the value.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_kv_transaction_get_op() -> anyhow::Result<()> {
    let (tc, _addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;
    let client = tc.grpc_client().await?;

    client.upsert_kv(UpsertKV::update("k1", b"hello")).await?;

    let txn = kv_transaction_req(vec![(None, vec![TxnOp::get("k1")])]);

    let reply = client.transaction_v2(txn).await?;
    assert_eq!(reply.executed_branch, Some(0));
    assert_eq!(reply.responses.len(), 1);

    // Verify response contains the value
    let resp = &reply.responses[0];
    let get_resp = resp.try_as_get().expect("expect Get response");
    assert_eq!(get_resp.key, "k1");
    assert!(get_resp.value.is_some());
    assert_eq!(get_resp.value.as_ref().unwrap().data, b("hello"));

    Ok(())
}

/// Test Delete operation.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_kv_transaction_delete_op() -> anyhow::Result<()> {
    let (tc, _addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;
    let client = tc.grpc_client().await?;

    client.upsert_kv(UpsertKV::update("k1", b"val")).await?;

    let txn = kv_transaction_req(vec![(None, vec![TxnOp::delete("k1")])]);

    let reply = client.transaction_v2(txn).await?;
    assert_eq!(reply.executed_branch, Some(0));

    // Key should be gone
    let val = client.get_kv("k1").await?;
    assert!(val.is_none());

    Ok(())
}

/// Test DeleteByPrefix operation.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_kv_transaction_delete_by_prefix_op() -> anyhow::Result<()> {
    let (tc, _addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;
    let client = tc.grpc_client().await?;

    client.upsert_kv(UpsertKV::update("pfx/a", b"1")).await?;
    client.upsert_kv(UpsertKV::update("pfx/b", b"2")).await?;
    client.upsert_kv(UpsertKV::update("other", b"3")).await?;

    let txn = kv_transaction_req(vec![(None, vec![TxnOp::delete_by_prefix("pfx/")])]);

    let reply = client.transaction_v2(txn).await?;
    assert_eq!(reply.executed_branch, Some(0));

    // pfx/* keys should be gone
    assert!(client.get_kv("pfx/a").await?.is_none());
    assert!(client.get_kv("pfx/b").await?.is_none());
    // "other" should still exist
    assert!(client.get_kv("other").await?.is_some());

    Ok(())
}

/// Test value-based condition.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_kv_transaction_value_condition() -> anyhow::Result<()> {
    let (tc, _addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;
    let client = tc.grpc_client().await?;

    client.upsert_kv(UpsertKV::update("k1", b"abc")).await?;

    // Branch 0: value == "xyz" → fails
    // Branch 1: value == "abc" → matches
    let txn = kv_transaction_req(vec![
        (
            Some(pb::BooleanExpression::from_conditions_and([
                TxnCondition::eq_value("k1", b("xyz")),
            ])),
            vec![TxnOp::put("result", b("wrong"))],
        ),
        (
            Some(pb::BooleanExpression::from_conditions_and([
                TxnCondition::eq_value("k1", b("abc")),
            ])),
            vec![TxnOp::put("result", b("correct"))],
        ),
    ]);

    let reply = client.transaction_v2(txn).await?;
    assert_eq!(reply.executed_branch, Some(1));

    let val = client.get_kv("result").await?;
    assert_eq!(val.unwrap().data, b("correct"));

    Ok(())
}

/// Test OR predicate: either condition can match.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_kv_transaction_or_predicate() -> anyhow::Result<()> {
    let (tc, _addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;
    let client = tc.grpc_client().await?;

    client.upsert_kv(UpsertKV::update("k1", b"v1")).await?;

    // OR: k1 seq==99 (false) OR k1 value=="v1" (true) → matches
    let txn = kv_transaction_req(vec![(
        Some(pb::BooleanExpression::from_conditions_or([
            TxnCondition::eq_seq("k1", 99),
            TxnCondition::eq_value("k1", b("v1")),
        ])),
        vec![TxnOp::put("k1", b("updated"))],
    )]);

    let reply = client.transaction_v2(txn).await?;
    assert_eq!(reply.executed_branch, Some(0));

    let val = client.get_kv("k1").await?;
    assert_eq!(val.unwrap().data, b("updated"));

    Ok(())
}

/// Test multiple operations in a single branch.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_kv_transaction_multiple_ops_in_branch() -> anyhow::Result<()> {
    let (tc, _addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;
    let client = tc.grpc_client().await?;

    client.upsert_kv(UpsertKV::update("k1", b"old")).await?;

    let txn = kv_transaction_req(vec![(None, vec![
        TxnOp::get("k1"),
        TxnOp::put("k1", b("new")),
        TxnOp::get("k1"),
    ])]);

    let reply = client.transaction_v2(txn).await?;
    assert_eq!(reply.executed_branch, Some(0));
    assert_eq!(reply.responses.len(), 3);

    // First get: "old"
    let get1 = reply.responses[0]
        .try_as_get()
        .expect("expect Get response");
    assert_eq!(get1.value.as_ref().unwrap().data, b("old"));

    // Second get (after put): "new"
    let get2 = reply.responses[2]
        .try_as_get()
        .expect("expect Get response");
    assert_eq!(get2.value.as_ref().unwrap().data, b("new"));

    Ok(())
}

/// Put without match_seq always overwrites.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_kv_transaction_put_match_seq_none() -> anyhow::Result<()> {
    let (tc, _addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;
    let client = tc.grpc_client().await?;

    client.upsert_kv(UpsertKV::update("k1", b"old")).await?;

    let txn = kv_transaction_req(vec![(None, vec![TxnOp::put("k1", b("new"))])]);

    let reply = client.transaction_v2(txn).await?;
    assert_eq!(reply.executed_branch, Some(0));

    let put_resp = reply.responses[0]
        .try_as_put()
        .expect("expect Put response");
    assert_eq!(put_resp.key, "k1");
    assert!(put_resp.current.is_some(), "put succeeded");

    let val = client.get_kv("k1").await?;
    assert_eq!(val.unwrap().data, b("new"));

    Ok(())
}

/// Put with match_seq that does NOT match: value is not updated.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_kv_transaction_put_match_seq_not_match() -> anyhow::Result<()> {
    let (tc, _addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;
    let client = tc.grpc_client().await?;

    client.upsert_kv(UpsertKV::update("k1", b"old")).await?;

    // Use match_seq=100, but actual seq is 1 → should not update
    let txn = kv_transaction_req(vec![(None, vec![
        TxnOp::put("k1", b("new")).match_seq(Some(100)),
    ])]);

    let reply = client.transaction_v2(txn).await?;
    assert_eq!(reply.executed_branch, Some(0));

    let put_resp = reply.responses[0]
        .try_as_put()
        .expect("expect Put response");
    assert_eq!(put_resp.key, "k1");

    // prev and current should be the same (not updated due to seq mismatch)
    assert_eq!(put_resp.prev_value, put_resp.current);

    let val = client.get_kv("k1").await?;
    assert_eq!(
        val.unwrap().data,
        b("old"),
        "value unchanged due to seq mismatch"
    );

    Ok(())
}

/// Put with match_seq that matches: value is updated.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_kv_transaction_put_match_seq_match() -> anyhow::Result<()> {
    let (tc, _addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;
    let client = tc.grpc_client().await?;

    client.upsert_kv(UpsertKV::update("k1", b"old")).await?;

    // Use match_seq=1 (correct) → should update
    let txn = kv_transaction_req(vec![(None, vec![
        TxnOp::put("k1", b("new")).match_seq(Some(1)),
    ])]);

    let reply = client.transaction_v2(txn).await?;
    assert_eq!(reply.executed_branch, Some(0));

    let put_resp = reply.responses[0]
        .try_as_put()
        .expect("expect Put response");
    assert_eq!(put_resp.key, "k1");
    assert_ne!(put_resp.prev_value, put_resp.current, "value was updated");

    let val = client.get_kv("k1").await?;
    assert_eq!(
        val.unwrap().data,
        b("new"),
        "value updated with matching seq"
    );

    Ok(())
}

/// Put for a brand-new key: prev_value is None, current reflects the inserted entry.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_kv_transaction_put_new_key_response() -> anyhow::Result<()> {
    let (tc, _addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;
    let client = tc.grpc_client().await?;

    let txn = kv_transaction_req(vec![(None, vec![TxnOp::put("brand_new", b("val"))])]);
    let reply = client.transaction_v2(txn).await?;

    let put_resp = reply.responses[0]
        .try_as_put()
        .expect("expect Put response");
    assert_eq!(*put_resp, pb::TxnPutResponse {
        key: "brand_new".to_string(),
        prev_value: None,
        current: Some(pb::SeqV {
            seq: 1,
            data: b("val"),
            meta: None
        }),
    });

    Ok(())
}

/// Get on a key that does not exist: response value is None.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_kv_transaction_get_nonexistent() -> anyhow::Result<()> {
    let (tc, _addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;
    let client = tc.grpc_client().await?;

    let txn = kv_transaction_req(vec![(None, vec![TxnOp::get("does_not_exist")])]);
    let reply = client.transaction_v2(txn).await?;

    let get_resp = reply.responses[0]
        .try_as_get()
        .expect("expect Get response");
    assert_eq!(get_resp.key, "does_not_exist");
    assert_eq!(get_resp.value, None);

    Ok(())
}

/// Empty branches list: no branch executes.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_kv_transaction_empty_branches() -> anyhow::Result<()> {
    let (tc, _addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;
    let client = tc.grpc_client().await?;

    let txn = kv_transaction_req(vec![]);
    let reply = client.transaction_v2(txn).await?;

    assert_eq!(reply.executed_branch, None);
    assert_eq!(reply.responses.len(), 0);

    Ok(())
}

/// Delete with match_seq that matches: key is removed, response reflects success.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_kv_transaction_delete_match_seq_matches() -> anyhow::Result<()> {
    let (tc, _addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;
    let client = tc.grpc_client().await?;

    client.upsert_kv(UpsertKV::update("k1", b"val")).await?;

    // seq=1 is correct after a single upsert
    let txn = kv_transaction_req(vec![(None, vec![TxnOp::delete_exact("k1", Some(1))])]);
    let reply = client.transaction_v2(txn).await?;
    assert_eq!(reply.executed_branch, Some(0));

    let del_resp = reply.responses[0]
        .try_as_delete()
        .expect("expect Delete response");
    assert_eq!(*del_resp, pb::TxnDeleteResponse {
        key: "k1".to_string(),
        success: true,
        prev_value: Some(pb::SeqV {
            seq: 1,
            data: b("val"),
            meta: None
        }),
    });

    assert_eq!(client.get_kv("k1").await?, None);

    Ok(())
}

/// Delete with match_seq that does not match: key is NOT removed, success=false.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_kv_transaction_delete_match_seq_mismatch() -> anyhow::Result<()> {
    let (tc, _addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;
    let client = tc.grpc_client().await?;

    client.upsert_kv(UpsertKV::update("k1", b"val")).await?;

    // seq=999 is wrong
    let txn = kv_transaction_req(vec![(None, vec![TxnOp::delete_exact("k1", Some(999))])]);
    let reply = client.transaction_v2(txn).await?;
    assert_eq!(reply.executed_branch, Some(0));

    let del_resp = reply.responses[0]
        .try_as_delete()
        .expect("expect Delete response");
    // prev_value is populated with the existing entry even when delete fails
    assert_eq!(*del_resp, pb::TxnDeleteResponse {
        key: "k1".to_string(),
        success: false,
        prev_value: Some(pb::SeqV {
            seq: 1,
            data: b("val"),
            meta: None
        }),
    });

    // Key must still exist, unchanged
    assert_eq!(
        client.get_kv("k1").await?,
        Some(SeqV {
            seq: 1,
            meta: None,
            data: b("val")
        })
    );

    Ok(())
}

/// DeleteByPrefix response contains the count of deleted keys.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_kv_transaction_delete_by_prefix_response_count() -> anyhow::Result<()> {
    let (tc, _addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;
    let client = tc.grpc_client().await?;

    client.upsert_kv(UpsertKV::update("p/a", b"1")).await?;
    client.upsert_kv(UpsertKV::update("p/b", b"2")).await?;
    client.upsert_kv(UpsertKV::update("p/c", b"3")).await?;

    let txn = kv_transaction_req(vec![(None, vec![TxnOp::delete_by_prefix("p/")])]);
    let reply = client.transaction_v2(txn).await?;
    assert_eq!(reply.executed_branch, Some(0));
    assert_eq!(reply.responses.len(), 1);

    match &reply.responses[0].response {
        Some(pb::txn_op_response::Response::DeleteByPrefix(resp)) => {
            assert_eq!(resp.count, 3, "expected 3 keys deleted");
        }
        _ => panic!("expected DeleteByPrefix response"),
    }

    Ok(())
}

/// AND predicate: both conditions must match.  Both match here → branch executes.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_kv_transaction_and_predicate_both_match() -> anyhow::Result<()> {
    let (tc, _addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;
    let client = tc.grpc_client().await?;

    client.upsert_kv(UpsertKV::update("a", b"va")).await?;
    client.upsert_kv(UpsertKV::update("b", b"vb")).await?;

    // Both: a seq==1 AND b seq==1 → true
    let txn = kv_transaction_req(vec![(
        Some(pb::BooleanExpression::from_conditions_and([
            TxnCondition::eq_seq("a", 1),
            TxnCondition::eq_seq("b", 2),
        ])),
        vec![TxnOp::put("result", b("ok"))],
    )]);

    let reply = client.transaction_v2(txn).await?;
    assert_eq!(reply.executed_branch, Some(0));
    assert_eq!(client.get_kv("result").await?.unwrap().data, b("ok"));

    Ok(())
}

/// AND predicate: one condition fails → branch does not execute.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_kv_transaction_and_predicate_one_fails() -> anyhow::Result<()> {
    let (tc, _addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;
    let client = tc.grpc_client().await?;

    client.upsert_kv(UpsertKV::update("a", b"va")).await?;
    // "b" does not exist → seq==0

    // a seq==1 (true) AND b seq==5 (false) → AND is false
    let txn = kv_transaction_req(vec![(
        Some(pb::BooleanExpression::from_conditions_and([
            TxnCondition::eq_seq("a", 1),
            TxnCondition::eq_seq("b", 5),
        ])),
        vec![TxnOp::put("result", b("ok"))],
    )]);

    let reply = client.transaction_v2(txn).await?;
    assert_eq!(
        reply.executed_branch, None,
        "AND should fail if any condition fails"
    );
    assert_eq!(client.get_kv("result").await?, None);

    Ok(())
}

/// Keys-with-prefix condition: branch executes when prefix count matches.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_kv_transaction_keys_with_prefix_condition() -> anyhow::Result<()> {
    let (tc, _addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;
    let client = tc.grpc_client().await?;

    client.upsert_kv(UpsertKV::update("ns/x", b"1")).await?;
    client.upsert_kv(UpsertKV::update("ns/y", b"2")).await?;

    // Branch 0: requires 3 keys with "ns/" → fails
    // Branch 1: requires 2 keys with "ns/" → matches
    let txn = kv_transaction_req(vec![
        (
            Some(pb::BooleanExpression::from_conditions_and([
                TxnCondition::keys_with_prefix("ns/", 3),
            ])),
            vec![TxnOp::put("r", b("wrong"))],
        ),
        (
            Some(pb::BooleanExpression::from_conditions_and([
                TxnCondition::keys_with_prefix("ns/", 2),
            ])),
            vec![TxnOp::put("r", b("correct"))],
        ),
    ]);

    let reply = client.transaction_v2(txn).await?;
    assert_eq!(reply.executed_branch, Some(1));
    assert_eq!(client.get_kv("r").await?.unwrap().data, b("correct"));

    Ok(())
}

/// FetchIncreaseU64 on a new key: counter starts at 0, result equals delta.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_kv_transaction_fetch_add_u64_from_zero() -> anyhow::Result<()> {
    let (tc, _addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;
    let client = tc.grpc_client().await?;

    // "counter" doesn't exist → starts at 0; after = 0 + 5 = 5
    let txn = kv_transaction_req(vec![(None, vec![TxnOp::fetch_add_u64("counter", 5)])]);
    let reply = client.transaction_v2(txn).await?;
    assert_eq!(reply.executed_branch, Some(0));

    let resp = reply.responses[0]
        .try_as_fetch_increase_u64()
        .expect("expect FetchIncreaseU64 response");
    assert_eq!(*resp, pb::FetchIncreaseU64Response {
        key: "counter".to_string(),
        before_seq: 0,
        before: 0,
        after_seq: 1,
        after: 5,
    });

    Ok(())
}

/// FetchIncreaseU64: multiple increments accumulate correctly.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_kv_transaction_fetch_add_u64_accumulates() -> anyhow::Result<()> {
    let (tc, _addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;
    let client = tc.grpc_client().await?;

    let inc =
        |delta: i64| kv_transaction_req(vec![(None, vec![TxnOp::fetch_add_u64("counter", delta)])]);

    let r1 = client.transaction_v2(inc(10)).await?;
    let r2 = client.transaction_v2(inc(3)).await?;
    let r3 = client.transaction_v2(inc(7)).await?;

    assert_eq!(
        *r1.responses[0].try_as_fetch_increase_u64().unwrap(),
        pb::FetchIncreaseU64Response {
            key: "counter".to_string(),
            before_seq: 0,
            before: 0,
            after_seq: 1,
            after: 10,
        }
    );
    assert_eq!(
        *r2.responses[0].try_as_fetch_increase_u64().unwrap(),
        pb::FetchIncreaseU64Response {
            key: "counter".to_string(),
            before_seq: 1,
            before: 10,
            after_seq: 2,
            after: 13,
        }
    );
    assert_eq!(
        *r3.responses[0].try_as_fetch_increase_u64().unwrap(),
        pb::FetchIncreaseU64Response {
            key: "counter".to_string(),
            before_seq: 2,
            before: 13,
            after_seq: 3,
            after: 20,
        }
    );

    Ok(())
}

/// FetchIncreaseU64 with max_value (floor): after = max(current, floor) + delta.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_kv_transaction_fetch_increase_u64_with_floor() -> anyhow::Result<()> {
    let (tc, _addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;
    let client = tc.grpc_client().await?;

    // current=0 (new key), floor=100, delta=1 → after = max(0, 100) + 1 = 101
    let txn = kv_transaction_req(vec![(None, vec![TxnOp::fetch_increase_u64(
        "counter", 100, 1,
    )])]);
    let reply = client.transaction_v2(txn).await?;

    let resp = reply.responses[0]
        .try_as_fetch_increase_u64()
        .expect("expect FetchIncreaseU64 response");
    assert_eq!(*resp, pb::FetchIncreaseU64Response {
        key: "counter".to_string(),
        before_seq: 0,
        before: 0,
        after_seq: 1,
        after: 101,
    });

    // Second call: current=101, floor=100, delta=1 → after = max(101, 100) + 1 = 102
    let txn2 = kv_transaction_req(vec![(None, vec![TxnOp::fetch_increase_u64(
        "counter", 100, 1,
    )])]);
    let reply2 = client.transaction_v2(txn2).await?;
    assert_eq!(
        *reply2.responses[0].try_as_fetch_increase_u64().unwrap(),
        pb::FetchIncreaseU64Response {
            key: "counter".to_string(),
            before_seq: 1,
            before: 101,
            after_seq: 2,
            after: 102,
        }
    );

    Ok(())
}

/// fetch_max_u64: sets counter to max(current, provided_value).
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_kv_transaction_fetch_max_u64() -> anyhow::Result<()> {
    let (tc, _addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;
    let client = tc.grpc_client().await?;

    // Set counter to 50 first
    client
        .transaction_v2(kv_transaction_req(vec![(None, vec![
            TxnOp::fetch_add_u64("counter", 50),
        ])]))
        .await?;

    // fetch_max_u64(counter, 30): max(50, 30) = 50, unchanged
    let r1 = client
        .transaction_v2(kv_transaction_req(vec![(None, vec![
            TxnOp::fetch_max_u64("counter", 30),
        ])]))
        .await?;
    assert_eq!(
        *r1.responses[0].try_as_fetch_increase_u64().unwrap(),
        pb::FetchIncreaseU64Response {
            key: "counter".to_string(),
            before_seq: 1,
            before: 50,
            after_seq: 2,
            after: 50,
        }
    );

    // fetch_max_u64(counter, 200): max(50, 200) = 200, bumped up
    let r2 = client
        .transaction_v2(kv_transaction_req(vec![(None, vec![
            TxnOp::fetch_max_u64("counter", 200),
        ])]))
        .await?;
    assert_eq!(
        *r2.responses[0].try_as_fetch_increase_u64().unwrap(),
        pb::FetchIncreaseU64Response {
            key: "counter".to_string(),
            before_seq: 2,
            before: 50,
            after_seq: 3,
            after: 200,
        }
    );

    Ok(())
}

/// FetchIncreaseU64 with match_seq that matches: update proceeds.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_kv_transaction_fetch_increase_u64_match_seq_match() -> anyhow::Result<()> {
    let (tc, _addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;
    let client = tc.grpc_client().await?;

    // First increment to establish the key (seq becomes 1)
    client
        .transaction_v2(kv_transaction_req(vec![(None, vec![
            TxnOp::fetch_add_u64("counter", 10),
        ])]))
        .await?;

    // Now increment with correct match_seq=1
    let txn = kv_transaction_req(vec![(None, vec![
        TxnOp::fetch_add_u64("counter", 5).match_seq(Some(1)),
    ])]);
    let reply = client.transaction_v2(txn).await?;

    assert_eq!(
        *reply.responses[0].try_as_fetch_increase_u64().unwrap(),
        pb::FetchIncreaseU64Response {
            key: "counter".to_string(),
            before_seq: 1,
            before: 10,
            after_seq: 2,
            after: 15,
        }
    );

    Ok(())
}

/// FetchIncreaseU64 with match_seq that does not match: counter unchanged.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_kv_transaction_fetch_increase_u64_match_seq_mismatch() -> anyhow::Result<()> {
    let (tc, _addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;
    let client = tc.grpc_client().await?;

    // Establish counter at 10 (seq=1)
    client
        .transaction_v2(kv_transaction_req(vec![(None, vec![
            TxnOp::fetch_add_u64("counter", 10),
        ])]))
        .await?;

    // Try to increment with wrong seq (999)
    let txn = kv_transaction_req(vec![(None, vec![
        TxnOp::fetch_add_u64("counter", 5).match_seq(Some(999)),
    ])]);
    let reply = client.transaction_v2(txn).await?;

    // Unchanged: before == after, seq unchanged
    assert_eq!(
        *reply.responses[0].try_as_fetch_increase_u64().unwrap(),
        pb::FetchIncreaseU64Response {
            key: "counter".to_string(),
            before_seq: 1,
            before: 10,
            after_seq: 1,
            after: 10,
        }
    );

    Ok(())
}

/// PutSequential inserts with an auto-incremented key and returns a Put response.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_kv_transaction_put_sequential_basic() -> anyhow::Result<()> {
    let (tc, _addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;
    let client = tc.grpc_client().await?;

    let txn = kv_transaction_req(vec![(None, vec![TxnOp::put_sequential(
        "log/",
        "log/__seq",
        b("e1"),
    )])]);
    let reply = client.transaction_v2(txn).await?;
    assert_eq!(reply.executed_branch, Some(0));
    assert_eq!(reply.responses.len(), 1);

    let put_resp = reply.responses[0]
        .try_as_put()
        .expect("PutSequential returns a Put response");
    // next_seq = before of FetchIncreaseU64 = 0 (counter absent), formatted as 21 zero-padded digits
    // seq=2: seq counter write (seq=1) then data key write (seq=2)
    assert_eq!(*put_resp, pb::TxnPutResponse {
        key: "log/000_000_000_000_000_000_000".to_string(),
        prev_value: None,
        current: Some(pb::SeqV {
            seq: 2,
            data: b("e1"),
            meta: None
        }),
    });

    // The actual key should exist in the store
    assert_eq!(
        client.get_kv("log/000_000_000_000_000_000_000").await?,
        Some(SeqV {
            seq: 2,
            meta: None,
            data: b("e1")
        })
    );

    Ok(())
}

/// Multiple PutSequential calls produce different, lexicographically ordered keys.
#[test(harness = meta_service_test_harness::<TokioRuntime, _, _>)]
#[fastrace::trace]
async fn test_kv_transaction_put_sequential_unique_keys() -> anyhow::Result<()> {
    let (tc, _addr) = crate::tests::start_metasrv::<TokioRuntime>().await?;
    let client = tc.grpc_client().await?;

    let mk_txn = |v: &'static str| {
        kv_transaction_req(vec![(None, vec![TxnOp::put_sequential(
            "entries/",
            "entries/__seq",
            v.as_bytes().to_vec(),
        )])])
    };

    let r1 = client.transaction_v2(mk_txn("first")).await?;
    let r2 = client.transaction_v2(mk_txn("second")).await?;
    let r3 = client.transaction_v2(mk_txn("third")).await?;

    let key1 = r1.responses[0].try_as_put().unwrap().key.clone();
    let key2 = r2.responses[0].try_as_put().unwrap().key.clone();
    let key3 = r3.responses[0].try_as_put().unwrap().key.clone();

    assert_eq!(key1, "entries/000_000_000_000_000_000_000");
    assert_eq!(key2, "entries/000_000_000_000_000_000_001");
    assert_eq!(key3, "entries/000_000_000_000_000_000_002");

    // Also verify stored values
    assert_eq!(client.get_kv(&key1).await?.unwrap().data, b("first"));
    assert_eq!(client.get_kv(&key2).await?.unwrap().data, b("second"));
    assert_eq!(client.get_kv(&key3).await?.unwrap().data, b("third"));

    Ok(())
}
