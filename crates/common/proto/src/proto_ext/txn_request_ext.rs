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

use std::fmt;

use display_more::DisplaySliceExt;

use crate::protobuf as pb;

impl pb::TxnRequest {
    /// Push a new conditional operation branch to the transaction.
    ///
    /// A branch is just like a `if (condition1 [and|or] condition2 ... ) { then; return; }` block.
    pub fn push_branch(
        mut self,
        expr: Option<pb::BooleanExpression>,
        ops: impl IntoIterator<Item = pb::TxnOp>,
    ) -> Self {
        self.operations
            .push(pb::ConditionalOperation::new(expr, ops));
        self
    }

    /// Push the old version of `condition` and `if_then` to the transaction.
    ///
    /// It is just like a `if (condition) { then; return; }` block.
    pub fn push_if_then(
        mut self,
        conditions: impl IntoIterator<Item = pb::TxnCondition>,
        ops: impl IntoIterator<Item = pb::TxnOp>,
    ) -> Self {
        assert!(self.condition.is_empty());
        assert!(self.if_then.is_empty());
        self.condition.extend(conditions);
        self.if_then.extend(ops);
        self
    }

    pub fn new(conditions: Vec<pb::TxnCondition>, ops: Vec<pb::TxnOp>) -> Self {
        Self {
            operations: vec![],
            condition: conditions,
            if_then: ops,
            else_then: vec![],
        }
    }

    /// Adds operations to execute when the conditions are not met.
    pub fn with_else(mut self, ops: Vec<pb::TxnOp>) -> Self {
        self.else_then = ops;
        self
    }

    /// Creates a transaction request that performs the specified operations
    /// unconditionally.
    pub fn unconditional(ops: Vec<pb::TxnOp>) -> Self {
        Self {
            operations: vec![],
            condition: vec![],
            if_then: ops,
            else_then: vec![],
        }
    }
}

impl fmt::Display for pb::TxnRequest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TxnRequest{{",)?;

        for op in self.operations.iter() {
            write!(f, "{{ {} }}, ", op)?;
        }

        write!(f, "if:{} ", self.condition.display_n(10),)?;
        write!(f, "then:{} ", self.if_then.display_n(10),)?;
        write!(f, "else:{}", self.else_then.display_n(10),)?;

        write!(f, "}}",)
    }
}

pub trait TxnRequestExt {
    fn from_upsert(upsert: databend_meta_base::UpsertKV) -> pb::TxnRequest;
}

impl TxnRequestExt for pb::TxnRequest {
    fn from_upsert(upsert: databend_meta_base::UpsertKV) -> pb::TxnRequest {
        use databend_meta_base::Operation;
        use databend_meta_base::flexible_timestamp::flexible_timestamp_to_duration;
        use map_api::match_seq::MatchSeq;

        use crate::protobuf::txn_condition::ConditionResult;

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

        let mut txn = pb::TxnRequest::new(conditions.clone(), vec![op]);

        if !conditions.is_empty() {
            txn = txn.with_else(vec![pb::TxnOp::get(&upsert.key)]);
        }

        txn
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_upsert() {
        use databend_meta_base::Operation;
        use databend_meta_base::UpsertKV;
        use map_api::match_seq::MatchSeq;

        use crate::protobuf::txn_condition::ConditionResult;

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
                            match_seq: None,
                        })),
                    }],
                    else_then: vec![],
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

    #[test]
    fn test_display_txn_request() {
        let op = pb::ConditionalOperation {
            predicate: Some(pb::BooleanExpression::from_conditions_and([
                pb::TxnCondition::eq_seq("k1", 1),
                pb::TxnCondition::eq_seq("k2", 2),
            ])),
            operations: vec![
                //
                pb::TxnOp::put("k1", b"v1".to_vec()),
                pb::TxnOp::put("k2", b"v2".to_vec()),
            ],
        };

        let req = pb::TxnRequest {
            operations: vec![op],
            condition: vec![
                pb::TxnCondition::eq_seq("k1", 1),
                pb::TxnCondition::eq_seq("k2", 2),
            ],
            if_then: vec![
                pb::TxnOp::put("k1", b"v1".to_vec()),
                pb::TxnOp::put("k2", b"v2".to_vec()),
            ],
            else_then: vec![pb::TxnOp::put("k3", b"v1".to_vec())],
        };

        assert_eq!(
            format!("{}", req),
            "TxnRequest{{ if:(k1 == seq(1) AND k2 == seq(2)) then:[Put(Put key=k1),Put(Put key=k2)] }, if:[k1 == seq(1),k2 == seq(2)] then:[Put(Put key=k1),Put(Put key=k2)] else:[Put(Put key=k3)]}",
        );
    }
}
