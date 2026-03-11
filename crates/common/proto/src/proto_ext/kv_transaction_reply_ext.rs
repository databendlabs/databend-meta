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

use display_more::DisplayOptionExt;
use display_more::DisplaySliceExt;

use crate::protobuf as pb;

impl fmt::Display for pb::KvTransactionReply {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "KvTransactionReply(branch={}, {})",
            self.executed_branch.display(),
            self.responses.display_n(32)
        )
    }
}

impl pb::KvTransactionReply {
    /// Convert to `TxnReply` given the original `TxnRequest` that produced this reply.
    ///
    /// The old `TxnRequest` maps to branches as follows:
    /// - branches\[0..n\] from `operations[]`
    /// - branches\[n\] from `condition/if_then` (always present)
    /// - branches\[n + 1\] from `else_then` (if present)
    ///
    /// `success` and `execution_path` are derived from which branch executed:
    /// - operations\[i\] → `success=true`, `execution_path="operation:i"`
    /// - if_then → `success=true`, `execution_path="then"`
    /// - else_then → `success=false`, `execution_path="else"`
    /// - no match → `success=false`, `execution_path="else"`
    pub fn into_txn_reply(self, original_req: &pb::TxnRequest) -> pb::TxnReply {
        let n_operations = original_req.operations.len();

        let (success, execution_path) = match self.executed_branch {
            None => {
                // No branch matched. This happens when the condition
                // predicate evaluates to false and there is no else_then
                // fallback. Map to "else" for backward compatibility.
                (false, "else".to_string())
            }
            Some(i) => {
                let i = i as usize;
                if i < n_operations {
                    (true, format!("operation:{}", i))
                } else if i == n_operations {
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

    #[test]
    fn test_display_no_match() {
        let reply = pb::KvTransactionReply {
            executed_branch: None,
            responses: vec![],
        };
        assert_eq!(reply.to_string(), "KvTransactionReply(branch=None, [])");
    }

    #[test]
    fn test_display_with_branch_and_responses() {
        let reply = pb::KvTransactionReply {
            executed_branch: Some(0),
            responses: vec![pb::TxnOpResponse::delete("k", true, None)],
        };
        assert_eq!(
            reply.to_string(),
            "KvTransactionReply(branch=0, [TxnOpResponse: Delete: Delete-resp: success: true, key=k, prev_seq=None])"
        );
    }
}
