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

//! Display implementations for kv_transaction types.

use std::fmt;
use std::time::Duration;

use display_more::DisplayOptionExt;
use display_more::DisplaySliceExt;
use display_more::DisplayUnixTimeStampExt;

use super::Branch;
use super::CompareOperator;
use super::Condition;
use super::Operand;
use super::Operation;
use super::Predicate;
use super::Transaction;
use super::operation;

const DISPLAY_LIMIT: usize = 32;

impl fmt::Display for Transaction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Transaction{}",
            self.branches.display_n(DISPLAY_LIMIT).sep(", ")
        )
    }
}

impl fmt::Display for Branch {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.predicate.is_always_true() {
            write!(f, "else ")?;
        } else {
            write!(f, "if({}) ", self.predicate)?;
        }
        write!(
            f,
            "{}",
            self.operations
                .display_n(DISPLAY_LIMIT)
                .sep(", ")
                .braces("", "")
        )
    }
}

impl fmt::Display for Predicate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Predicate::And(children) => write!(
                f,
                "{}",
                children
                    .display_n(DISPLAY_LIMIT)
                    .sep(" AND ")
                    .braces("(", ")")
            ),
            Predicate::Or(children) => write!(
                f,
                "{}",
                children
                    .display_n(DISPLAY_LIMIT)
                    .sep(" OR ")
                    .braces("(", ")")
            ),
            Predicate::Leaf(cond) => write!(f, "{cond}"),
        }
    }
}

impl fmt::Display for Condition {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}{}{}", self.key, self.op, self.target)
    }
}

impl fmt::Display for CompareOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CompareOperator::Eq => write!(f, "=="),
            CompareOperator::Ne => write!(f, "!="),
            CompareOperator::Lt => write!(f, "<"),
            CompareOperator::Le => write!(f, "<="),
            CompareOperator::Gt => write!(f, ">"),
            CompareOperator::Ge => write!(f, ">="),
        }
    }
}

impl fmt::Display for Operand {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Operand::Seq(s) => write!(f, "seq({s})"),
            Operand::Value(v) => write!(f, "value({}B)", v.len()),
            Operand::KeysWithPrefix(n) => write!(f, "keys_with_prefix({n})"),
        }
    }
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Operation::Get(op) => write!(f, "{op}"),
            Operation::Put(op) => write!(f, "{op}"),
            Operation::Delete(op) => write!(f, "{op}"),
            Operation::DeleteByPrefix(op) => write!(f, "{op}"),
            Operation::FetchIncreaseU64(op) => write!(f, "{op}"),
            Operation::PutSequential(op) => write!(f, "{op}"),
        }
    }
}

impl fmt::Display for operation::Payload {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}B, expire_at={}, ttl={:?}",
            self.value.len(),
            self.expire_at_ms
                .map(Duration::from_millis)
                .display_unix_timestamp_short(),
            self.ttl_ms.map(Duration::from_millis)
        )
    }
}

impl fmt::Display for operation::KeyLookup {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}, match_seq={}", self.key, self.match_seq.display())
    }
}

impl fmt::Display for operation::Get {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Get({})", self.key)
    }
}

impl fmt::Display for operation::Put {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Put({}, {})", self.target, self.payload)
    }
}

impl fmt::Display for operation::Delete {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Delete({})", self.target)
    }
}

impl fmt::Display for operation::DeleteByPrefix {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DeleteByPrefix({})", self.prefix)
    }
}

impl fmt::Display for operation::FetchIncreaseU64 {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "FetchIncreaseU64({}, delta={}, floor={})",
            self.target, self.delta, self.floor
        )
    }
}

impl fmt::Display for operation::PutSequential {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PutSequential({}, seq_key={}, {})",
            self.prefix, self.sequence_key, self.payload
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn b(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    #[test]
    fn test_display_condition() {
        assert_eq!(Predicate::eq_seq("k1", 5).to_string(), "k1==seq(5)");
        assert_eq!(
            Predicate::gt_value("k2", b("hello")).to_string(),
            "k2>value(5B)"
        );
        assert_eq!(
            Predicate::keys_with_prefix("prefix/", CompareOperator::Ge, 3).to_string(),
            "prefix/>=keys_with_prefix(3)"
        );
    }

    #[test]
    fn test_display_compare_operators() {
        assert_eq!(CompareOperator::Eq.to_string(), "==");
        assert_eq!(CompareOperator::Ne.to_string(), "!=");
        assert_eq!(CompareOperator::Lt.to_string(), "<");
        assert_eq!(CompareOperator::Le.to_string(), "<=");
        assert_eq!(CompareOperator::Gt.to_string(), ">");
        assert_eq!(CompareOperator::Ge.to_string(), ">=");
    }

    #[test]
    fn test_display_predicate() {
        assert_eq!(Predicate::eq_seq("k", 0).to_string(), "k==seq(0)");

        let and = Predicate::and([Predicate::eq_seq("a", 1), Predicate::ne_seq("b", 2)]);
        assert_eq!(and.to_string(), "(a==seq(1) AND b!=seq(2))");

        let or = Predicate::or([Predicate::eq_seq("x", 0), Predicate::lt_value("y", b("v"))]);
        assert_eq!(or.to_string(), "(x==seq(0) OR y<value(1B))");
    }

    #[test]
    fn test_display_operations() {
        assert_eq!(Operation::get("k").to_string(), "Get(k)");

        assert_eq!(
            Operation::put("k", b("val")).expire_at_ms(1000).to_string(),
            "Put(k, match_seq=None, 3B, expire_at=1970-01-01T00:00:01.000, ttl=None)"
        );
        assert_eq!(
            Operation::put("k", b("v")).ttl_ms(500).to_string(),
            "Put(k, match_seq=None, 1B, expire_at=None, ttl=Some(500ms))"
        );

        assert_eq!(
            Operation::delete("k").match_seq(5).to_string(),
            "Delete(k, match_seq=5)"
        );
        assert_eq!(
            Operation::delete("k").to_string(),
            "Delete(k, match_seq=None)"
        );

        assert_eq!(
            Operation::delete_by_prefix("p/").to_string(),
            "DeleteByPrefix(p/)"
        );

        assert_eq!(
            Operation::fetch_increase_u64("c", 10)
                .match_seq(3)
                .to_string(),
            "FetchIncreaseU64(c, match_seq=3, delta=10, floor=0)"
        );
        assert_eq!(
            Operation::fetch_increase_u64("c", -1).to_string(),
            "FetchIncreaseU64(c, match_seq=None, delta=-1, floor=0)"
        );

        assert_eq!(
            Operation::put_sequential("log/", "seq", b("entry"))
                .expire_at_ms(9999)
                .to_string(),
            "PutSequential(log/, seq_key=seq, 5B, expire_at=1970-01-01T00:00:09.999, ttl=None)"
        );
    }

    #[test]
    fn test_display_branch() {
        let branch = Branch::if_(Predicate::eq_seq("k", 0)).then([Operation::put("k", b("v"))]);
        assert_eq!(
            branch.to_string(),
            "if(k==seq(0)) Put(k, match_seq=None, 1B, expire_at=None, ttl=None)"
        );

        let else_branch = Branch::else_().then([Operation::get("k")]);
        assert_eq!(else_branch.to_string(), "else Get(k)");
    }

    #[test]
    fn test_display_transaction() {
        let txn = Transaction {
            branches: vec![
                Branch::if_(Predicate::eq_seq("k", 0)).then([Operation::put("k", b("v"))]),
                Branch::else_().then([Operation::get("k")]),
            ],
        };
        assert_eq!(
            txn.to_string(),
            "Transaction[if(k==seq(0)) Put(k, match_seq=None, 1B, expire_at=None, ttl=None), else Get(k)]"
        );
    }

    #[test]
    fn test_display_predicate_truncation() {
        let and = Predicate::and([
            Predicate::eq_seq("a", 1),
            Predicate::eq_seq("b", 2),
            Predicate::eq_seq("c", 3),
            Predicate::eq_seq("d", 4),
        ]);
        assert_eq!(
            and.to_string(),
            "(a==seq(1) AND b==seq(2) AND c==seq(3) AND d==seq(4))"
        );
    }

    #[test]
    fn test_display_branch_truncation() {
        let branch = Branch::else_().then([
            Operation::get("a"),
            Operation::get("b"),
            Operation::get("c"),
            Operation::get("d"),
        ]);
        assert_eq!(branch.to_string(), "else Get(a), Get(b), Get(c), Get(d)");
    }

    #[test]
    fn test_display_complex_transaction() {
        // Branch 1: nested AND(OR(...), Leaf) with two operations
        // Branch 2: nested OR(AND(...), Leaf) with three operations
        // Branch 3: unconditional else with two operations
        let txn = Transaction {
            branches: vec![
                Branch::if_all([
                    Predicate::or([Predicate::eq_seq("x", 0), Predicate::ne_value("y", b("ok"))]),
                    Predicate::keys_with_prefix("z", CompareOperator::Lt, 5),
                ])
                .then([
                    Operation::put("x", b("new")).expire_at_ms(9000),
                    Operation::delete("y").match_seq(3),
                ]),
                Branch::if_any([
                    Predicate::and([Predicate::ge_seq("a", 1), Predicate::le_seq("b", 2)]),
                    Predicate::gt_value("c", b("v")),
                ])
                .then([
                    Operation::get("a"),
                    Operation::fetch_increase_u64("counter", 1),
                    Operation::delete_by_prefix("tmp/"),
                ]),
                Branch::else_().then([
                    Operation::get("fallback"),
                    Operation::put_sequential("log/", "seq", b("err")).ttl_ms(60000),
                ]),
            ],
        };

        assert_eq!(
            txn.to_string(),
            concat!(
                "Transaction[",
                "if(((x==seq(0) OR y!=value(2B)) AND z<keys_with_prefix(5))) ",
                "Put(x, match_seq=None, 3B, expire_at=1970-01-01T00:00:09.000, ttl=None), Delete(y, match_seq=3), ",
                "if(((a>=seq(1) AND b<=seq(2)) OR c>value(1B))) ",
                "Get(a), FetchIncreaseU64(counter, match_seq=None, delta=1, floor=0), DeleteByPrefix(tmp/), ",
                "else ",
                "Get(fallback), PutSequential(log/, seq_key=seq, 3B, expire_at=None, ttl=Some(60s))",
                "]",
            )
        );
    }
}
