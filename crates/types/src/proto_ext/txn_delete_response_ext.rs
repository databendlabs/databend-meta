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

use std::fmt::Display;
use std::fmt::Formatter;

use crate::TxnDeleteResponse;

impl TxnDeleteResponse {
    pub fn new(key: impl ToString, success: bool, prev_value: Option<crate::protobuf::SeqV>) -> Self {
        Self {
            key: key.to_string(),
            success,
            prev_value,
        }
    }
}

impl Display for TxnDeleteResponse {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "Delete-resp: success: {}, key={}, prev_seq={:?}",
            self.success,
            self.key,
            self.prev_value.as_ref().map(|x| x.seq)
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::protobuf as pb;

    use super::*;

    #[test]
    fn test_new_success() {
        let resp = TxnDeleteResponse::new("k1", true, Some(pb::SeqV::new(5, b"old".to_vec())));
        assert_eq!(resp.key, "k1");
        assert!(resp.success);
        assert_eq!(resp.prev_value.as_ref().unwrap().seq, 5);
    }

    #[test]
    fn test_new_no_prev() {
        let resp = TxnDeleteResponse::new("k2", false, None);
        assert_eq!(resp.key, "k2");
        assert!(!resp.success);
        assert!(resp.prev_value.is_none());
    }

    #[test]
    fn test_display() {
        let resp = TxnDeleteResponse::new("k1", true, Some(pb::SeqV::new(3, vec![])));
        assert_eq!(
            resp.to_string(),
            "Delete-resp: success: true, key=k1, prev_seq=Some(3)"
        );
    }

    #[test]
    fn test_display_no_prev() {
        let resp = TxnDeleteResponse::new("k1", false, None);
        assert_eq!(
            resp.to_string(),
            "Delete-resp: success: false, key=k1, prev_seq=None"
        );
    }
}
