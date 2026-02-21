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

//! Conversions between protobuf transport types and Rust-native storage types.

use log::warn;

use super::Branch;
use super::CompareOperator;
use super::Condition;
use super::Operand;
use super::Operation;
use super::Predicate;
use super::Transaction;
use super::operation;
use crate::protobuf as pb;
use crate::protobuf::boolean_expression::CombiningOperator;
use crate::txn_condition;

// === New API: KvTransactionRequest → Transaction ===

impl From<pb::KvTransactionRequest> for Transaction {
    fn from(req: pb::KvTransactionRequest) -> Self {
        Transaction {
            branches: req.branches.into_iter().map(Branch::from).collect(),
        }
    }
}

impl From<pb::ConditionalOperation> for Branch {
    fn from(co: pb::ConditionalOperation) -> Self {
        Branch {
            predicate: co
                .predicate
                .map(Predicate::from)
                .unwrap_or_else(Predicate::always_true),
            operations: co.operations.into_iter().map(Operation::from).collect(),
        }
    }
}

impl From<pb::BooleanExpression> for Predicate {
    fn from(expr: pb::BooleanExpression) -> Self {
        let op = expr.operator();
        let mut children: Vec<Predicate> = expr
            .sub_expressions
            .into_iter()
            .map(Predicate::from)
            .collect();
        children.extend(
            expr.conditions
                .into_iter()
                .map(|c| Predicate::Leaf(Condition::from(c))),
        );

        if children.len() == 1 {
            return children.pop().unwrap();
        }

        match op {
            CombiningOperator::And => Predicate::And(children),
            CombiningOperator::Or => Predicate::Or(children),
        }
    }
}

impl From<pb::TxnCondition> for Condition {
    fn from(cond: pb::TxnCondition) -> Self {
        // No target: always-false (seq < 0 is impossible for u64).
        // Matches old evaluator which returned false for missing target.
        let (target, op) = if let Some(t) = cond.target {
            let target = match t {
                txn_condition::Target::Seq(s) => Operand::Seq(s),
                txn_condition::Target::Value(v) => Operand::Value(v),
                txn_condition::Target::KeysWithPrefix(n) => Operand::KeysWithPrefix(n),
            };
            (
                target,
                CompareOperator::from_condition_result(cond.expected),
            )
        } else {
            (Operand::Seq(0), CompareOperator::Lt)
        };

        Condition {
            key: cond.key,
            target,
            op,
        }
    }
}

impl CompareOperator {
    fn from_condition_result(expected: i32) -> Self {
        use crate::ConditionResult;

        match <ConditionResult as num_traits::FromPrimitive>::from_i32(expected) {
            Some(ConditionResult::Eq) => CompareOperator::Eq,
            Some(ConditionResult::Gt) => CompareOperator::Gt,
            Some(ConditionResult::Ge) => CompareOperator::Ge,
            Some(ConditionResult::Lt) => CompareOperator::Lt,
            Some(ConditionResult::Le) => CompareOperator::Le,
            Some(ConditionResult::Ne) => CompareOperator::Ne,
            None => {
                warn!(
                    "unknown ConditionResult value {}; defaulting to Eq",
                    expected
                );
                CompareOperator::Eq
            }
        }
    }
}

impl From<pb::TxnOp> for Operation {
    fn from(op: pb::TxnOp) -> Self {
        use crate::txn_op;

        match op.request {
            Some(txn_op::Request::Get(g)) => Operation::get(g.key),
            Some(txn_op::Request::Put(p)) => Operation::Put(operation::Put {
                target: operation::KeyLookup::new(p.key, p.match_seq),
                payload: operation::Payload::new(p.value, p.expire_at, p.ttl_ms),
            }),
            Some(txn_op::Request::Delete(d)) => Operation::Delete(operation::Delete {
                target: operation::KeyLookup::new(d.key, d.match_seq),
            }),
            Some(txn_op::Request::DeleteByPrefix(d)) => Operation::delete_by_prefix(d.prefix),
            Some(txn_op::Request::FetchIncreaseU64(f)) => {
                Operation::FetchIncreaseU64(operation::FetchIncreaseU64 {
                    target: operation::KeyLookup::new(f.key, f.match_seq),
                    delta: f.delta,
                    floor: f.max_value,
                })
            }
            Some(txn_op::Request::PutSequential(p)) => {
                Operation::PutSequential(operation::PutSequential {
                    prefix: p.prefix,
                    sequence_key: p.sequence_key,
                    payload: operation::Payload::new(p.value, p.expires_at_ms, p.ttl_ms),
                })
            }

            None => {
                warn!("TxnOp has no request; defaulting to get(\"\")");
                Operation::get("")
            }
        }
    }
}

// === Old API: TxnRequest → Transaction (for raft log backward compat) ===

impl From<&pb::TxnRequest> for Transaction {
    fn from(req: &pb::TxnRequest) -> Self {
        let mut branches: Vec<Branch> = req.operations.iter().cloned().map(Branch::from).collect();

        if !req.condition.is_empty() || !req.if_then.is_empty() {
            branches.push(Branch {
                predicate: Predicate::and(
                    req.condition
                        .iter()
                        .cloned()
                        .map(|c| Predicate::Leaf(Condition::from(c))),
                ),
                operations: req.if_then.iter().cloned().map(Operation::from).collect(),
            });
        }

        if !req.else_then.is_empty() {
            branches.push(Branch {
                predicate: Predicate::always_true(),
                operations: req.else_then.iter().cloned().map(Operation::from).collect(),
            });
        }

        Transaction { branches }
    }
}

// === Response conversion: KvTransactionReply → TxnReply ===

impl pb::KvTransactionReply {
    /// Convert to `TxnReply` given the original `TxnRequest` that produced this reply.
    ///
    /// The old `TxnRequest` maps to branches as follows:
    /// - branches\[0..n\] from `operations[]`
    /// - branches\[n\] from `condition/if_then` (if present)
    /// - branches\[n + has_if\] from `else_then` (if present)
    ///
    /// `success` and `execution_path` are derived from which branch executed:
    /// - operations\[i\] → `success=true`, `execution_path="operation:i"`
    /// - if_then → `success=true`, `execution_path="then"`
    /// - else_then → `success=false`, `execution_path="else"`
    /// - no match → `success=false`, `execution_path=""`
    pub fn into_txn_reply(self, original_req: &pb::TxnRequest) -> pb::TxnReply {
        let n_operations = original_req.operations.len();
        let has_if = !original_req.condition.is_empty() || !original_req.if_then.is_empty();

        let (success, execution_path) = match self.executed_branch {
            None => {
                if has_if {
                    // Old evaluator always fell through to "else" when condition didn't match,
                    // even when else_then was empty.
                    (false, "else".to_string())
                } else {
                    (false, String::new())
                }
            }
            Some(i) => {
                let i = i as usize;
                if i < n_operations {
                    (true, format!("operation:{}", i))
                } else if has_if && i == n_operations {
                    (true, "then".to_string())
                } else {
                    (false, "else".to_string())
                }
            }
        };

        pb::TxnReply {
            success,
            responses: self.responses,
            execution_path,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TxnCondition;
    use crate::TxnOp;
    use crate::TxnRequest;

    fn b(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    #[test]
    fn test_condition_from_proto() {
        assert_eq!(Condition::from(TxnCondition::eq_seq("k", 5)), Condition {
            key: "k".into(),
            target: Operand::Seq(5),
            op: CompareOperator::Eq,
        });
        assert_eq!(
            Condition::from(pb::TxnCondition {
                key: "k".into(),
                expected: crate::ConditionResult::Gt as i32,
                target: Some(txn_condition::Target::Value(b("v"))),
            }),
            Condition {
                key: "k".into(),
                target: Operand::Value(b("v")),
                op: CompareOperator::Gt,
            }
        );
        assert_eq!(
            Condition::from(pb::TxnCondition {
                key: "p/".into(),
                expected: crate::ConditionResult::Ge as i32,
                target: Some(txn_condition::Target::KeysWithPrefix(3)),
            }),
            Condition {
                key: "p/".into(),
                target: Operand::KeysWithPrefix(3),
                op: CompareOperator::Ge,
            }
        );
        // None target → always-false condition (seq < 0 is impossible for u64)
        assert_eq!(
            Condition::from(pb::TxnCondition {
                key: "k".into(),
                expected: 0,
                target: None,
            }),
            Condition {
                key: "k".into(),
                target: Operand::Seq(0),
                op: CompareOperator::Lt,
            }
        );
    }

    #[test]
    fn test_operation_from_proto() {
        assert_eq!(Operation::from(TxnOp::get("k")), Operation::get("k"));
        assert_eq!(
            Operation::from(TxnOp::put("k", b("v"))),
            Operation::put("k", b("v"))
        );
        assert_eq!(Operation::from(TxnOp::delete("k")), Operation::delete("k"));
        assert_eq!(
            Operation::from(TxnOp::delete_by_prefix("p/")),
            Operation::delete_by_prefix("p/")
        );
        assert_eq!(
            Operation::from(pb::TxnOp { request: None }),
            Operation::get("")
        );
    }

    #[test]
    fn test_predicate_from_proto() {
        // Single condition unwraps to Leaf
        let expr = pb::BooleanExpression::from_conditions_and([TxnCondition::eq_seq("k", 5)]);
        assert_eq!(Predicate::from(expr), Predicate::eq_seq("k", 5));

        // Multiple conditions → And
        let expr = pb::BooleanExpression::from_conditions_and([
            TxnCondition::eq_seq("a", 1),
            TxnCondition::eq_seq("b", 2),
        ]);
        assert_eq!(
            Predicate::from(expr),
            Predicate::and([Predicate::eq_seq("a", 1), Predicate::eq_seq("b", 2)]),
        );

        // Or
        let mut expr = pb::BooleanExpression::from_conditions_and([
            TxnCondition::eq_seq("a", 1),
            TxnCondition::eq_seq("b", 2),
        ]);
        expr.operator = CombiningOperator::Or as i32;
        assert_eq!(
            Predicate::from(expr),
            Predicate::or([Predicate::eq_seq("a", 1), Predicate::eq_seq("b", 2)]),
        );
    }

    #[test]
    fn test_branch_from_proto() {
        let co = pb::ConditionalOperation {
            predicate: Some(pb::BooleanExpression::from_conditions_and([
                TxnCondition::eq_seq("k", 0),
            ])),
            operations: vec![TxnOp::get("k")],
        };
        let branch = Branch::from(co);
        assert_eq!(branch.predicate, Predicate::eq_seq("k", 0));
        assert_eq!(branch.operations, vec![Operation::get("k")]);

        // No predicate → always true
        let co = pb::ConditionalOperation {
            predicate: None,
            operations: vec![TxnOp::get("k")],
        };
        assert!(Branch::from(co).predicate.is_always_true());
    }

    #[test]
    fn test_transaction_from_kv_request() {
        let req = pb::KvTransactionRequest {
            branches: vec![
                pb::ConditionalOperation {
                    predicate: Some(pb::BooleanExpression::from_conditions_and([
                        TxnCondition::eq_seq("k", 0),
                    ])),
                    operations: vec![TxnOp::put("k", b("v"))],
                },
                pb::ConditionalOperation {
                    predicate: None,
                    operations: vec![TxnOp::get("k")],
                },
            ],
        };
        let txn = Transaction::from(req);
        assert_eq!(txn.branches.len(), 2);
        assert!(!txn.branches[0].predicate.is_always_true());
        assert!(txn.branches[1].predicate.is_always_true());
    }

    #[test]
    fn test_transaction_from_legacy_txn_request() {
        // if/else
        let req = TxnRequest::new(vec![TxnCondition::eq_seq("k", 5)], vec![TxnOp::put(
            "k",
            b("v"),
        )])
        .with_else(vec![TxnOp::get("k")]);
        let txn = Transaction::from(&req);
        assert_eq!(txn.branches.len(), 2);
        assert!(!txn.branches[0].predicate.is_always_true());
        assert!(txn.branches[1].predicate.is_always_true());

        // if only (no else)
        let req = TxnRequest::new(vec![TxnCondition::eq_seq("k", 0)], vec![TxnOp::put(
            "k",
            b("v"),
        )]);
        let txn = Transaction::from(&req);
        assert_eq!(txn.branches.len(), 1);
        assert!(!txn.branches[0].predicate.is_always_true());

        // operations[] style
        let req = TxnRequest::default()
            .push_branch(
                Some(pb::BooleanExpression::from_conditions_and([
                    TxnCondition::eq_seq("k", 5),
                ])),
                [TxnOp::put("k", b("v"))],
            )
            .push_branch(None, [TxnOp::get("k")]);
        let txn = Transaction::from(&req);
        assert_eq!(txn.branches.len(), 2);
        assert!(!txn.branches[0].predicate.is_always_true());
        assert!(txn.branches[1].predicate.is_always_true());
    }

    #[test]
    fn test_reply_to_txn_reply() {
        let req = TxnRequest::new(vec![TxnCondition::eq_seq("k", 0)], vec![TxnOp::put(
            "k",
            b("v"),
        )])
        .with_else(vec![TxnOp::get("k")]);

        let reply = |branch: Option<u32>| {
            pb::KvTransactionReply {
                executed_branch: branch,
                responses: vec![],
            }
            .into_txn_reply(&req)
        };

        // If branch
        let r = reply(Some(0));
        assert!(r.success);
        assert_eq!(r.execution_path, "then");

        // Else branch
        let r = reply(Some(1));
        assert!(!r.success);
        assert_eq!(r.execution_path, "else");

        // No match — with conditions, falls through to "else" for backward compat
        let r = reply(None);
        assert!(!r.success);
        assert_eq!(r.execution_path, "else");

        // Operation branch
        let req = TxnRequest::default()
            .push_branch(
                Some(pb::BooleanExpression::from_conditions_and([
                    TxnCondition::eq_seq("k", 5),
                ])),
                [TxnOp::put("k", b("v"))],
            )
            .push_branch(None, [TxnOp::get("k")]);
        let r = pb::KvTransactionReply {
            executed_branch: Some(0),
            responses: vec![],
        }
        .into_txn_reply(&req);
        assert!(r.success);
        assert_eq!(r.execution_path, "operation:0");
    }

    #[test]
    fn test_reply_mixed_operations_and_condition() {
        // operations[0] + condition/if_then + else_then
        let mut req = TxnRequest::default().push_branch(
            Some(pb::BooleanExpression::from_conditions_and([
                TxnCondition::eq_seq("k", 99),
            ])),
            [TxnOp::get("k")],
        );
        req.condition = vec![TxnCondition::eq_seq("k", 0)];
        req.if_then = vec![TxnOp::put("k", b("v"))];
        req.else_then = vec![TxnOp::get("k")];

        let reply = |branch: Option<u32>| {
            pb::KvTransactionReply {
                executed_branch: branch,
                responses: vec![],
            }
            .into_txn_reply(&req)
        };

        // Operation 0 matched
        let r = reply(Some(0));
        assert!(r.success);
        assert_eq!(r.execution_path, "operation:0");

        // "then" branch (index 1 = n_operations(1))
        let r = reply(Some(1));
        assert!(r.success);
        assert_eq!(r.execution_path, "then");

        // "else" branch (index 2 = n_operations(1) + has_if(1))
        let r = reply(Some(2));
        assert!(!r.success);
        assert_eq!(r.execution_path, "else");
    }
}
