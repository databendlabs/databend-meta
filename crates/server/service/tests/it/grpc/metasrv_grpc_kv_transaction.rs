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
