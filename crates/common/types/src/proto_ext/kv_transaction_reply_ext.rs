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
            self.responses.display()
        )
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
