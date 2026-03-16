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

use databend_meta_proto::databend_meta_base::flexible_timestamp::flexible_timestamp_to_duration;

use crate::MatchSeq;
use crate::Operation;
use crate::UpsertKV;
use crate::protobuf as pb;
use crate::protobuf::txn_condition::ConditionResult;

pub trait TxnRequestUpsertExt {
    fn from_upsert(upsert: UpsertKV) -> Self;
}

impl TxnRequestUpsertExt for pb::TxnRequest {
    /// Build a transaction request from an upsert operation.
    fn from_upsert(upsert: UpsertKV) -> Self {
        let conditions = match upsert.seq {
            MatchSeq::Any => {
                vec![]
            }
            MatchSeq::Exact(v) => {
                vec![pb::TxnCondition::match_seq(
                    &upsert.key,
                    ConditionResult::Eq,
                    v,
                )]
            }
            MatchSeq::GE(v) => {
                vec![pb::TxnCondition::match_seq(
                    &upsert.key,
                    ConditionResult::Ge,
                    v,
                )]
            }
        };

        let op = match upsert.value {
            Operation::Update(x) => {
                let mut op = pb::TxnOp::put(&upsert.key, x);

                if let Some(meta_spec) = upsert.value_meta {
                    op = op
                        .with_expires_at(meta_spec.expire_at().map(flexible_timestamp_to_duration));
                    op = op.with_ttl(meta_spec.ttl().map(|x| x.to_duration()));
                }

                op
            }
            Operation::Delete => pb::TxnOp::delete(&upsert.key),
            #[allow(deprecated)]
            Operation::AsIs => {
                unreachable!("AsIs should be never used ");
            }
        };

        let mut txn = Self::new(conditions.clone(), vec![op]);

        // Only add else_then if there are conditions
        if !conditions.is_empty() {
            txn = txn.with_else(vec![pb::TxnOp::get(&upsert.key)]);
        }

        txn
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Interval;
    use crate::MetaSpec;
    use crate::protobuf as pb;
    use crate::protobuf::txn_condition::ConditionResult;

    #[test]
    fn test_from_upsert() {
        let test_cases = vec![
            (
                "match_seq_any",
                UpsertKV {
                    key: "test_key".to_string(),
                    seq: MatchSeq::Any,
                    value: Operation::Update(b"test_value".to_vec()),
                    value_meta: None,
                },
                pb::TxnRequest {
                    operations: vec![],
                    condition: vec![],
                    if_then: vec![pb::TxnOp {
                        request: Some(pb::txn_op::Request::Put(pb::TxnPutRequest {
                            key: "test_key".to_string(),
                            value: b"test_value".to_vec(),
                            prev_value: true,
                            expire_at: None,
                            ttl_ms: None,
                            match_seq: None,
                        })),
                    }],
                    else_then: vec![],
                },
            ),
            (
                "match_seq_exact",
                UpsertKV {
                    key: "test_key".to_string(),
                    seq: MatchSeq::Exact(42),
                    value: Operation::Update(b"test_value".to_vec()),
                    value_meta: None,
                },
                pb::TxnRequest {
                    operations: vec![],
                    condition: vec![pb::TxnCondition {
                        key: "test_key".to_string(),
                        expected: ConditionResult::Eq as i32,
                        target: Some(pb::txn_condition::Target::Seq(42)),
                    }],
                    if_then: vec![pb::TxnOp {
                        request: Some(pb::txn_op::Request::Put(pb::TxnPutRequest {
                            key: "test_key".to_string(),
                            value: b"test_value".to_vec(),
                            prev_value: true,
                            expire_at: None,
                            ttl_ms: None,
                            match_seq: None,
                        })),
                    }],
                    else_then: vec![pb::TxnOp::get("test_key")],
                },
            ),
            (
                "match_seq_ge",
                UpsertKV {
                    key: "test_key".to_string(),
                    seq: MatchSeq::GE(10),
                    value: Operation::Update(b"test_value".to_vec()),
                    value_meta: None,
                },
                pb::TxnRequest {
                    operations: vec![],
                    condition: vec![pb::TxnCondition {
                        key: "test_key".to_string(),
                        expected: ConditionResult::Ge as i32,
                        target: Some(pb::txn_condition::Target::Seq(10)),
                    }],
                    if_then: vec![pb::TxnOp {
                        request: Some(pb::txn_op::Request::Put(pb::TxnPutRequest {
                            key: "test_key".to_string(),
                            value: b"test_value".to_vec(),
                            prev_value: true,
                            expire_at: None,
                            ttl_ms: None,
                            match_seq: None,
                        })),
                    }],
                    else_then: vec![pb::TxnOp::get("test_key")],
                },
            ),
            (
                "delete_operation",
                UpsertKV {
                    key: "test_key".to_string(),
                    seq: MatchSeq::Any,
                    value: Operation::Delete,
                    value_meta: None,
                },
                pb::TxnRequest {
                    operations: vec![],
                    condition: vec![],
                    if_then: vec![pb::TxnOp {
                        request: Some(pb::txn_op::Request::Delete(pb::TxnDeleteRequest {
                            key: "test_key".to_string(),
                            prev_value: true,
                            match_seq: None,
                        })),
                    }],
                    else_then: vec![],
                },
            ),
            (
                "with_ttl_and_expire_at",
                UpsertKV {
                    key: "test_key".to_string(),
                    seq: MatchSeq::Any,
                    value: Operation::Update(b"test_value".to_vec()),
                    value_meta: Some(MetaSpec::new(
                        Some(1234567890),
                        Some(Interval::from_secs(3600)),
                    )),
                },
                pb::TxnRequest {
                    operations: vec![],
                    condition: vec![],
                    if_then: vec![pb::TxnOp {
                        request: Some(pb::txn_op::Request::Put(pb::TxnPutRequest {
                            key: "test_key".to_string(),
                            value: b"test_value".to_vec(),
                            prev_value: true,
                            expire_at: Some(1_234_567_890_000),
                            ttl_ms: Some(3600 * 1000), // 3600 seconds in milliseconds
                            match_seq: None,
                        })),
                    }],
                    else_then: vec![],
                },
            ),
            (
                "with_only_ttl",
                UpsertKV {
                    key: "test_key".to_string(),
                    seq: MatchSeq::Any,
                    value: Operation::Update(b"test_value".to_vec()),
                    value_meta: Some(MetaSpec::new(None, Some(Interval::from_millis(500)))),
                },
                pb::TxnRequest {
                    operations: vec![],
                    condition: vec![],
                    if_then: vec![pb::TxnOp {
                        request: Some(pb::txn_op::Request::Put(pb::TxnPutRequest {
                            key: "test_key".to_string(),
                            value: b"test_value".to_vec(),
                            prev_value: true,
                            expire_at: None,
                            ttl_ms: Some(500),
                            match_seq: None,
                        })),
                    }],
                    else_then: vec![],
                },
            ),
            (
                "with_only_expire_at",
                UpsertKV {
                    key: "test_key".to_string(),
                    seq: MatchSeq::Any,
                    value: Operation::Update(b"test_value".to_vec()),
                    value_meta: Some(MetaSpec::new(Some(9876543210), None)),
                },
                pb::TxnRequest {
                    operations: vec![],
                    condition: vec![],
                    if_then: vec![pb::TxnOp {
                        request: Some(pb::txn_op::Request::Put(pb::TxnPutRequest {
                            key: "test_key".to_string(),
                            value: b"test_value".to_vec(),
                            prev_value: true,
                            expire_at: Some(9_876_543_210_000),
                            ttl_ms: None,
                            match_seq: None,
                        })),
                    }],
                    else_then: vec![],
                },
            ),
            (
                "complex_case_exact_seq_with_delete",
                UpsertKV {
                    key: "complex_key".to_string(),
                    seq: MatchSeq::Exact(100),
                    value: Operation::Delete,
                    value_meta: None,
                },
                pb::TxnRequest {
                    operations: vec![],
                    condition: vec![pb::TxnCondition {
                        key: "complex_key".to_string(),
                        expected: ConditionResult::Eq as i32,
                        target: Some(pb::txn_condition::Target::Seq(100)),
                    }],
                    if_then: vec![pb::TxnOp {
                        request: Some(pb::txn_op::Request::Delete(pb::TxnDeleteRequest {
                            key: "complex_key".to_string(),
                            prev_value: true,
                            match_seq: None,
                        })),
                    }],
                    else_then: vec![pb::TxnOp::get("complex_key")],
                },
            ),
        ];

        for (test_name, input_upsert, expected_output) in test_cases {
            let actual_output = pb::TxnRequest::from_upsert(input_upsert);
            assert_eq!(
                actual_output, expected_output,
                "Test case '{}' failed",
                test_name
            );
        }
    }
}
