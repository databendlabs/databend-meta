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

use crate::protobuf as pb;

impl pb::TxnDeleteRequest {
    pub fn new(key: impl ToString, match_seq: Option<u64>) -> Self {
        Self {
            key: key.to_string(),
            match_seq,
        }
    }
}

impl fmt::Display for pb::TxnDeleteRequest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Delete key={}", self.key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let req = pb::TxnDeleteRequest::new("k1", Some(42));
        assert_eq!(req.key, "k1");
        assert_eq!(req.match_seq, Some(42));
    }

    #[test]
    fn test_new_no_match_seq() {
        let req = pb::TxnDeleteRequest::new("k2", None);
        assert_eq!(req.key, "k2");
        assert!(req.match_seq.is_none());
    }

    #[test]
    fn test_display() {
        let req = pb::TxnDeleteRequest::new("k1", None);
        assert_eq!(req.to_string(), "Delete key=k1");
    }
}
