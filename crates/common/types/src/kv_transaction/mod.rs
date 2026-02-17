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

//! Rust-native storage types for KV transactions.
//!
//! These types are stored in the raft log via `Cmd::KvTransaction(Transaction)`.
//! They are serde-serialized, decoupled from protobuf wire format.

pub mod convert;
mod display;
pub mod operation;

use serde::Deserialize;
use serde::Serialize;

/// A transaction: a list of branches evaluated in order.
/// The first branch whose predicate matches is executed.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, deepsize::DeepSizeOf)]
pub struct Transaction {
    pub branches: Vec<Branch>,
}

/// A branch: optional predicate + operations.
/// No predicate means unconditional (always matches). Use as "else" at the end.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, deepsize::DeepSizeOf)]
pub struct Branch {
    pub predicate: Predicate,
    pub operations: Vec<Operation>,
}

/// A recursive condition tree: And/Or of sub-predicates or leaf conditions.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, deepsize::DeepSizeOf)]
pub enum Predicate {
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
    Leaf(Condition),
}

/// A single condition comparing a key's attribute against a target.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, deepsize::DeepSizeOf)]
pub struct Condition {
    /// The key whose attribute is being evaluated.
    pub key: String,
    /// The right-hand side of the comparison.
    pub target: Operand,
    /// How to op the key's attribute against the target.
    pub op: CompareOperator,
}

/// The right-hand side of a condition comparison.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, deepsize::DeepSizeOf)]
pub enum Operand {
    /// Compare against the key's sequence number.
    Seq(u64),
    /// Compare against the key's stored value bytes.
    Value(Vec<u8>),
    /// Compare the count of keys sharing this prefix.
    KeysWithPrefix(u64),
}

/// Comparison operator.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, deepsize::DeepSizeOf)]
pub enum CompareOperator {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// An operation within a transaction branch.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, deepsize::DeepSizeOf)]
pub enum Operation {
    Get(operation::Get),
    Put(operation::Put),
    Delete(operation::Delete),
    DeleteByPrefix(operation::DeleteByPrefix),
    FetchIncreaseU64(operation::FetchIncreaseU64),
    PutSequential(operation::PutSequential),
}

impl Branch {
    pub fn if_(predicate: Predicate) -> Self {
        Branch {
            predicate,
            operations: vec![],
        }
    }

    pub fn if_all(predicates: impl IntoIterator<Item = Predicate>) -> Self {
        Self::if_(Predicate::and(predicates))
    }

    pub fn if_any(predicates: impl IntoIterator<Item = Predicate>) -> Self {
        Self::if_(Predicate::or(predicates))
    }

    pub fn else_() -> Self {
        Branch {
            predicate: Predicate::always_true(),
            operations: vec![],
        }
    }

    pub fn then(mut self, operations: impl IntoIterator<Item = Operation>) -> Self {
        self.operations.extend(operations);
        self
    }
}

impl Predicate {
    pub fn always_true() -> Self {
        Predicate::And(vec![])
    }

    pub fn is_always_true(&self) -> bool {
        matches!(self, Predicate::And(c) | Predicate::Or(c) if c.is_empty())
    }

    pub fn and(predicates: impl IntoIterator<Item = Predicate>) -> Self {
        Predicate::And(predicates.into_iter().collect())
    }

    pub fn or(predicates: impl IntoIterator<Item = Predicate>) -> Self {
        Predicate::Or(predicates.into_iter().collect())
    }

    /// Create a leaf predicate comparing a key's attribute.
    pub fn leaf(key: impl Into<String>, op: CompareOperator, target: Operand) -> Self {
        Predicate::Leaf(Condition {
            key: key.into(),
            target,
            op,
        })
    }

    pub fn seq(key: impl Into<String>, op: CompareOperator, seq: u64) -> Self {
        Self::leaf(key, op, Operand::Seq(seq))
    }

    pub fn value(key: impl Into<String>, op: CompareOperator, value: impl Into<Vec<u8>>) -> Self {
        Self::leaf(key, op, Operand::Value(value.into()))
    }

    pub fn keys_with_prefix(key: impl Into<String>, op: CompareOperator, count: u64) -> Self {
        Self::leaf(key, op, Operand::KeysWithPrefix(count))
    }

    pub fn eq_seq(key: impl Into<String>, seq: u64) -> Self {
        Self::seq(key, CompareOperator::Eq, seq)
    }

    pub fn ne_seq(key: impl Into<String>, seq: u64) -> Self {
        Self::seq(key, CompareOperator::Ne, seq)
    }

    pub fn lt_seq(key: impl Into<String>, seq: u64) -> Self {
        Self::seq(key, CompareOperator::Lt, seq)
    }

    pub fn le_seq(key: impl Into<String>, seq: u64) -> Self {
        Self::seq(key, CompareOperator::Le, seq)
    }

    pub fn gt_seq(key: impl Into<String>, seq: u64) -> Self {
        Self::seq(key, CompareOperator::Gt, seq)
    }

    pub fn ge_seq(key: impl Into<String>, seq: u64) -> Self {
        Self::seq(key, CompareOperator::Ge, seq)
    }

    pub fn eq_value(key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        Self::value(key, CompareOperator::Eq, value)
    }

    pub fn ne_value(key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        Self::value(key, CompareOperator::Ne, value)
    }

    pub fn lt_value(key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        Self::value(key, CompareOperator::Lt, value)
    }

    pub fn le_value(key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        Self::value(key, CompareOperator::Le, value)
    }

    pub fn gt_value(key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        Self::value(key, CompareOperator::Gt, value)
    }

    pub fn ge_value(key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        Self::value(key, CompareOperator::Ge, value)
    }
}

impl Operation {
    pub fn get(key: impl Into<String>) -> Self {
        Operation::Get(operation::Get { key: key.into() })
    }

    pub fn put(key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        Operation::Put(operation::Put {
            key: key.into(),
            value: value.into(),
            expire_at_ms: None,
            ttl_ms: None,
        })
    }

    pub fn delete(key: impl Into<String>) -> Self {
        Operation::Delete(operation::Delete {
            key: key.into(),
            match_seq: None,
        })
    }

    pub fn put_sequential(
        prefix: impl Into<String>,
        sequence_key: impl Into<String>,
        value: impl Into<Vec<u8>>,
    ) -> Self {
        Operation::PutSequential(operation::PutSequential {
            prefix: prefix.into(),
            sequence_key: sequence_key.into(),
            value: value.into(),
            expire_at_ms: None,
            ttl_ms: None,
        })
    }

    pub fn fetch_increase_u64(key: impl Into<String>, delta: i64) -> Self {
        Operation::FetchIncreaseU64(operation::FetchIncreaseU64 {
            key: key.into(),
            match_seq: None,
            delta,
            floor: 0,
        })
    }

    pub fn delete_by_prefix(prefix: impl Into<String>) -> Self {
        Operation::DeleteByPrefix(operation::DeleteByPrefix {
            prefix: prefix.into(),
        })
    }

    pub fn expire_at_ms(mut self, ms: u64) -> Self {
        match &mut self {
            Operation::Put(p) => p.expire_at_ms = Some(ms),
            Operation::PutSequential(p) => p.expire_at_ms = Some(ms),
            _ => unreachable!("expire_at_ms only applies to Put/PutSequential"),
        }
        self
    }

    pub fn ttl_ms(mut self, ms: u64) -> Self {
        match &mut self {
            Operation::Put(p) => p.ttl_ms = Some(ms),
            Operation::PutSequential(p) => p.ttl_ms = Some(ms),
            _ => unreachable!("ttl_ms only applies to Put/PutSequential"),
        }
        self
    }

    pub fn match_seq(mut self, seq: u64) -> Self {
        match &mut self {
            Operation::Delete(d) => d.match_seq = Some(seq),
            Operation::FetchIncreaseU64(f) => f.match_seq = Some(seq),
            _ => unreachable!("match_seq only applies to Delete/FetchIncreaseU64"),
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    // --- Serde round-trip ---

    #[test]
    fn test_txn_serde_round_trip() {
        let txn = Transaction {
            branches: vec![
                Branch::if_all([
                    Predicate::eq_seq("k1", 5),
                    Predicate::gt_value("k2", b("hello")),
                ])
                .then([
                    Operation::put("k1", b("v1")).expire_at_ms(1000),
                    Operation::get("k2"),
                ]),
                Branch::else_().then([Operation::delete("k1").match_seq(5)]),
            ],
        };

        let json = serde_json::to_string(&txn).unwrap();
        let decoded: Transaction = serde_json::from_str(&json).unwrap();
        assert_eq!(txn, decoded);
    }

    #[test]
    fn test_all_op_variants_serde() {
        let ops = vec![
            Operation::get("k"),
            Operation::Put(operation::Put {
                key: "k".to_string(),
                value: b("v"),
                expire_at_ms: Some(999),
                ttl_ms: Some(500),
            }),
            Operation::Delete(operation::Delete {
                key: "k".to_string(),
                match_seq: Some(3),
            }),
            Operation::delete_by_prefix("prefix/"),
            Operation::FetchIncreaseU64(operation::FetchIncreaseU64 {
                key: "counter".to_string(),
                match_seq: None,
                delta: 10,
                floor: 0,
            }),
            Operation::PutSequential(operation::PutSequential {
                prefix: "log/".to_string(),
                sequence_key: "seq".to_string(),
                value: b("entry"),
                expire_at_ms: None,
                ttl_ms: Some(60000),
            }),
        ];

        for op in &ops {
            let json = serde_json::to_string(op).unwrap();
            let decoded: Operation = serde_json::from_str(&json).unwrap();
            assert_eq!(*op, decoded, "round-trip failed for {:?}", op);
        }
    }

    #[test]
    fn test_predicate_variants_serde() {
        let predicates = vec![
            Predicate::eq_seq("k", 0),
            Predicate::and([
                Predicate::ne_value("a", b("x")),
                Predicate::keys_with_prefix("b", CompareOperator::Ge, 3),
            ]),
            Predicate::or([Predicate::and([]), Predicate::lt_seq("c", 10)]),
        ];

        for pred in &predicates {
            let json = serde_json::to_string(pred).unwrap();
            let decoded: Predicate = serde_json::from_str(&json).unwrap();
            assert_eq!(*pred, decoded, "round-trip failed for {:?}", pred);
        }
    }

    // --- Helper function tests ---

    fn cond(key: &str, op: CompareOperator, target: Operand) -> Predicate {
        Predicate::Leaf(Condition {
            key: key.to_string(),
            target,
            op,
        })
    }

    // --- Branch helpers ---

    #[test]
    fn test_branch_if_then() {
        let got = Branch::if_(Predicate::eq_seq("k", 0)).then([Operation::get("k")]);
        assert_eq!(got, Branch {
            predicate: Predicate::eq_seq("k", 0),
            operations: vec![Operation::get("k")],
        });
    }

    #[test]
    fn test_branch_if_all() {
        let got = Branch::if_all([Predicate::eq_seq("a", 1), Predicate::eq_seq("b", 2)])
            .then([Operation::get("a")]);
        assert_eq!(got, Branch {
            predicate: Predicate::And(vec![Predicate::eq_seq("a", 1), Predicate::eq_seq("b", 2),]),
            operations: vec![Operation::get("a")],
        });
    }

    #[test]
    fn test_branch_if_any() {
        let got = Branch::if_any([Predicate::eq_seq("a", 1), Predicate::eq_seq("b", 2)])
            .then([Operation::get("a")]);
        assert_eq!(got, Branch {
            predicate: Predicate::Or(vec![Predicate::eq_seq("a", 1), Predicate::eq_seq("b", 2),]),
            operations: vec![Operation::get("a")],
        });
    }

    #[test]
    fn test_branch_else_then() {
        let got = Branch::else_().then([Operation::get("k")]);
        assert_eq!(got, Branch {
            predicate: Predicate::always_true(),
            operations: vec![Operation::get("k")],
        });
    }

    // --- Predicate helpers ---

    #[test]
    fn test_predicate_and() {
        let a = Predicate::eq_seq("a", 1);
        let b = Predicate::eq_seq("b", 2);
        assert_eq!(
            Predicate::and([a.clone(), b.clone()]),
            Predicate::And(vec![a, b]),
        );
    }

    #[test]
    fn test_predicate_or() {
        let a = Predicate::eq_seq("a", 1);
        let b = Predicate::eq_seq("b", 2);
        assert_eq!(
            Predicate::or([a.clone(), b.clone()]),
            Predicate::Or(vec![a, b]),
        );
    }

    #[test]
    fn test_predicate_leaf() {
        assert_eq!(
            Predicate::leaf("k", CompareOperator::Eq, Operand::Seq(5)),
            cond("k", CompareOperator::Eq, Operand::Seq(5)),
        );
    }

    #[test]
    fn test_predicate_seq() {
        assert_eq!(
            Predicate::seq("k", CompareOperator::Gt, 3),
            cond("k", CompareOperator::Gt, Operand::Seq(3)),
        );
    }

    #[test]
    fn test_predicate_value() {
        assert_eq!(
            Predicate::value("k", CompareOperator::Ne, b("v")),
            cond("k", CompareOperator::Ne, Operand::Value(b("v"))),
        );
    }

    #[test]
    fn test_predicate_keys_with_prefix() {
        assert_eq!(
            Predicate::keys_with_prefix("p/", CompareOperator::Ge, 10),
            cond("p/", CompareOperator::Ge, Operand::KeysWithPrefix(10)),
        );
    }

    #[test]
    fn test_predicate_eq_seq() {
        assert_eq!(
            Predicate::eq_seq("k", 0),
            cond("k", CompareOperator::Eq, Operand::Seq(0))
        );
    }

    #[test]
    fn test_predicate_ne_seq() {
        assert_eq!(
            Predicate::ne_seq("k", 1),
            cond("k", CompareOperator::Ne, Operand::Seq(1))
        );
    }

    #[test]
    fn test_predicate_lt_seq() {
        assert_eq!(
            Predicate::lt_seq("k", 2),
            cond("k", CompareOperator::Lt, Operand::Seq(2))
        );
    }

    #[test]
    fn test_predicate_le_seq() {
        assert_eq!(
            Predicate::le_seq("k", 3),
            cond("k", CompareOperator::Le, Operand::Seq(3))
        );
    }

    #[test]
    fn test_predicate_gt_seq() {
        assert_eq!(
            Predicate::gt_seq("k", 4),
            cond("k", CompareOperator::Gt, Operand::Seq(4))
        );
    }

    #[test]
    fn test_predicate_ge_seq() {
        assert_eq!(
            Predicate::ge_seq("k", 5),
            cond("k", CompareOperator::Ge, Operand::Seq(5))
        );
    }

    #[test]
    fn test_predicate_eq_value() {
        assert_eq!(
            Predicate::eq_value("k", b("v")),
            cond("k", CompareOperator::Eq, Operand::Value(b("v"))),
        );
    }

    #[test]
    fn test_predicate_ne_value() {
        assert_eq!(
            Predicate::ne_value("k", b("v")),
            cond("k", CompareOperator::Ne, Operand::Value(b("v"))),
        );
    }

    #[test]
    fn test_predicate_lt_value() {
        assert_eq!(
            Predicate::lt_value("k", b("v")),
            cond("k", CompareOperator::Lt, Operand::Value(b("v"))),
        );
    }

    #[test]
    fn test_predicate_le_value() {
        assert_eq!(
            Predicate::le_value("k", b("v")),
            cond("k", CompareOperator::Le, Operand::Value(b("v"))),
        );
    }

    #[test]
    fn test_predicate_gt_value() {
        assert_eq!(
            Predicate::gt_value("k", b("v")),
            cond("k", CompareOperator::Gt, Operand::Value(b("v"))),
        );
    }

    #[test]
    fn test_predicate_ge_value() {
        assert_eq!(
            Predicate::ge_value("k", b("v")),
            cond("k", CompareOperator::Ge, Operand::Value(b("v"))),
        );
    }

    // --- Operation helpers ---

    #[test]
    fn test_operation_get() {
        assert_eq!(
            Operation::get("k"),
            Operation::Get(operation::Get {
                key: "k".to_string()
            })
        );
    }

    #[test]
    fn test_operation_put() {
        assert_eq!(
            Operation::put("k", b("v")),
            Operation::Put(operation::Put {
                key: "k".to_string(),
                value: b("v"),
                expire_at_ms: None,
                ttl_ms: None,
            }),
        );
    }

    #[test]
    fn test_operation_delete() {
        assert_eq!(
            Operation::delete("k"),
            Operation::Delete(operation::Delete {
                key: "k".to_string(),
                match_seq: None,
            }),
        );
    }

    #[test]
    fn test_operation_delete_by_prefix() {
        assert_eq!(
            Operation::delete_by_prefix("p/"),
            Operation::DeleteByPrefix(operation::DeleteByPrefix {
                prefix: "p/".to_string(),
            }),
        );
    }

    #[test]
    fn test_operation_put_sequential() {
        assert_eq!(
            Operation::put_sequential("log/", "seq", b("v")),
            Operation::PutSequential(operation::PutSequential {
                prefix: "log/".to_string(),
                sequence_key: "seq".to_string(),
                value: b("v"),
                expire_at_ms: None,
                ttl_ms: None,
            }),
        );
    }

    #[test]
    fn test_operation_fetch_increase_u64() {
        assert_eq!(
            Operation::fetch_increase_u64("c", 10),
            Operation::FetchIncreaseU64(operation::FetchIncreaseU64 {
                key: "c".to_string(),
                match_seq: None,
                delta: 10,
                floor: 0,
            }),
        );
    }

    // --- Chainable setters ---

    #[test]
    fn test_operation_expire_at_ms() {
        assert_eq!(
            Operation::put("k", b("v")).expire_at_ms(1000),
            Operation::Put(operation::Put {
                key: "k".to_string(),
                value: b("v"),
                expire_at_ms: Some(1000),
                ttl_ms: None,
            }),
        );
    }

    #[test]
    fn test_operation_ttl_ms() {
        assert_eq!(
            Operation::put("k", b("v")).ttl_ms(500),
            Operation::Put(operation::Put {
                key: "k".to_string(),
                value: b("v"),
                expire_at_ms: None,
                ttl_ms: Some(500),
            }),
        );
    }

    #[test]
    fn test_operation_match_seq() {
        assert_eq!(
            Operation::delete("k").match_seq(7),
            Operation::Delete(operation::Delete {
                key: "k".to_string(),
                match_seq: Some(7),
            }),
        );
    }

    #[test]
    fn test_operation_put_sequential_with_ttl() {
        assert_eq!(
            Operation::put_sequential("log/", "seq", b("v")).ttl_ms(60000),
            Operation::PutSequential(operation::PutSequential {
                prefix: "log/".to_string(),
                sequence_key: "seq".to_string(),
                value: b("v"),
                expire_at_ms: None,
                ttl_ms: Some(60000),
            }),
        );
    }

    #[test]
    fn test_operation_chained_expire_and_ttl() {
        assert_eq!(
            Operation::put("k", b("v")).expire_at_ms(1000).ttl_ms(500),
            Operation::Put(operation::Put {
                key: "k".to_string(),
                value: b("v"),
                expire_at_ms: Some(1000),
                ttl_ms: Some(500),
            }),
        );
    }
}
