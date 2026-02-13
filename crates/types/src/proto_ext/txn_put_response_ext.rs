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

use display_more::DisplayOptionExt;

use crate::SeqV;
use crate::TxnPutResponse;
use crate::protobuf as pb;

impl Display for TxnPutResponse {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "Put-resp: key={}, prev_seq={}, current_seq={}",
            self.key,
            self.prev_value.as_ref().map(|x| x.seq).display(),
            self.current.as_ref().map(|x| x.seq).display()
        )
    }
}

impl pb::TxnPutResponse {
    pub fn unpack(self) -> (String, Option<SeqV>, Option<SeqV>) {
        let pb::TxnPutResponse {
            key,
            prev_value,
            current,
        } = self;

        (key, prev_value.map(SeqV::from), current.map(SeqV::from))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display() {
        let resp = TxnPutResponse {
            key: "k1".to_string(),
            prev_value: Some(pb::SeqV::new(1, b"old".to_vec())),
            current: Some(pb::SeqV::new(2, b"new".to_vec())),
        };
        assert_eq!(
            resp.to_string(),
            "Put-resp: key=k1, prev_seq=1, current_seq=2"
        );
    }

    #[test]
    fn test_display_none() {
        let resp = TxnPutResponse {
            key: "k1".to_string(),
            prev_value: None,
            current: None,
        };
        assert_eq!(
            resp.to_string(),
            "Put-resp: key=k1, prev_seq=None, current_seq=None"
        );
    }

    #[test]
    fn test_unpack() {
        let resp = pb::TxnPutResponse {
            key: "k1".to_string(),
            prev_value: Some(pb::SeqV::new(1, b"old".to_vec())),
            current: Some(pb::SeqV::new(2, b"new".to_vec())),
        };
        let (key, prev, cur) = resp.unpack();
        assert_eq!(key, "k1");
        assert_eq!(prev.unwrap().seq, 1);
        assert_eq!(cur.unwrap().seq, 2);
    }

    #[test]
    fn test_unpack_none() {
        let resp = pb::TxnPutResponse {
            key: "k1".to_string(),
            prev_value: None,
            current: None,
        };
        let (key, prev, cur) = resp.unpack();
        assert_eq!(key, "k1");
        assert!(prev.is_none());
        assert!(cur.is_none());
    }
}
