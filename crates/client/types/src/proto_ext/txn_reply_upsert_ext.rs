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

use anyerror::AnyError;
use map_api::SeqV;

use crate::Change;
use crate::InvalidReply;
use crate::protobuf as pb;
use crate::protobuf::txn_op_response;

pub trait TxnReplyUpsertExt {
    fn into_upsert_reply(self) -> Result<Change<Vec<u8>>, InvalidReply>;
}

impl TxnReplyUpsertExt for pb::TxnReply {
    /// Convert the response to the upsert reply
    fn into_upsert_reply(self) -> Result<Change<Vec<u8>>, InvalidReply> {
        let Some(txn_op_response) = self.responses.into_iter().next() else {
            return Err(InvalidReply::new(
                "No responses in TxnReply",
                &AnyError::error(""),
            ));
        };

        let Some(res) = txn_op_response.response else {
            return Err(InvalidReply::new(
                "Empty response in TxnReply",
                &AnyError::error(""),
            ));
        };

        // An upsert operation is converted to either a Put or a Delete operation.
        // When the condition fails, a Get operation returns the current value.
        let reply = match res {
            txn_op_response::Response::Put(r) => {
                Change::new(r.prev_value.map(SeqV::from), r.current.map(SeqV::from))
            }
            txn_op_response::Response::Delete(r) => {
                let prev = r.prev_value.map(SeqV::from);
                if r.success {
                    Change::new(prev, None)
                } else {
                    Change::new(prev.clone(), prev.clone())
                }
            }
            txn_op_response::Response::Get(r) => {
                // Condition failed, return current value as both prev and result (no change)
                let current = r.value.map(SeqV::from);
                Change::new(current.clone(), current)
            }
            _ => {
                return Err(InvalidReply::new(
                    "Expect Response::Put, Response::Delete, or Response::Get",
                    &AnyError::error(""),
                ));
            }
        };

        Ok(reply)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Change;
    use crate::protobuf as pb;

    #[test]
    fn test_into_upsert_reply_success_cases() {
        let prev_seq_v = pb::SeqV {
            seq: 1,
            data: b"old_value".to_vec(),
            meta: None,
        };
        let current_seq_v = pb::SeqV {
            seq: 2,
            data: b"new_value".to_vec(),
            meta: None,
        };

        // Put with prev and current
        let mut reply = pb::TxnReply::new("operation:0");
        reply.responses = vec![pb::TxnOpResponse::put(
            "test_key",
            Some(prev_seq_v.clone()),
            Some(current_seq_v.clone()),
        )];
        let res = reply.into_upsert_reply();
        assert_eq!(
            res.unwrap(),
            Change::new(
                Some(prev_seq_v.clone().into()),
                Some(current_seq_v.clone().into())
            )
        );

        // Put new insertion (no prev)
        reply = pb::TxnReply::new("operation:0");
        reply.responses = vec![pb::TxnOpResponse::put(
            "test_key",
            None,
            Some(current_seq_v.clone()),
        )];
        let res = reply.into_upsert_reply();
        assert_eq!(res.unwrap(), Change::new(None, Some(current_seq_v.into())));

        // Put no current (expired value)
        reply = pb::TxnReply::new("operation:0");
        reply.responses = vec![pb::TxnOpResponse::put(
            "test_key",
            Some(prev_seq_v.clone()),
            None,
        )];
        let res = reply.into_upsert_reply();
        assert_eq!(
            res.unwrap(),
            Change::new(Some(prev_seq_v.clone().into()), None)
        );

        // Delete success
        reply = pb::TxnReply::new("operation:0");
        reply.responses = vec![pb::TxnOpResponse::delete(
            "test_key",
            true,
            Some(prev_seq_v.clone()),
        )];
        let res = reply.into_upsert_reply();
        assert_eq!(
            res.unwrap(),
            Change::new(Some(prev_seq_v.clone().into()), None)
        );

        // Delete failure (no change)
        reply = pb::TxnReply::new("operation:0");
        reply.responses = vec![pb::TxnOpResponse::delete(
            "test_key",
            false,
            Some(prev_seq_v.clone()),
        )];
        let res = reply.into_upsert_reply();
        assert_eq!(
            res.unwrap(),
            Change::new(
                Some(prev_seq_v.clone().into()),
                Some(prev_seq_v.clone().into())
            )
        );

        // Delete no prev
        reply = pb::TxnReply::new("operation:0");
        reply.responses = vec![pb::TxnOpResponse::delete("test_key", true, None)];
        let res = reply.into_upsert_reply();
        assert_eq!(res.unwrap(), Change::new(None, None));
    }

    #[test]
    fn test_into_upsert_reply_empty_response() {
        let mut reply = pb::TxnReply::new("operation:0");
        reply.responses = vec![pb::TxnOpResponse { response: None }];

        let res = reply.into_upsert_reply();
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("Empty response in TxnReply")
        );
    }

    #[test]
    fn test_into_upsert_reply_get_response() {
        let seq_v = pb::SeqV {
            seq: 1,
            data: b"value".to_vec(),
            meta: None,
        };

        // Get with value (condition failed, return current value)
        let mut reply = pb::TxnReply::new("else");
        reply.responses = vec![pb::TxnOpResponse::get(
            "test_key",
            Some(seq_v.clone().into()),
        )];

        let res = reply.into_upsert_reply();
        assert_eq!(
            res.unwrap(),
            Change::new(Some(seq_v.clone().into()), Some(seq_v.clone().into()))
        );

        // Get with no value
        reply = pb::TxnReply::new("else");
        reply.responses = vec![pb::TxnOpResponse::get("test_key", None)];

        let res = reply.into_upsert_reply();
        assert_eq!(res.unwrap(), Change::new(None, None));
    }

    #[test]
    fn test_into_upsert_reply_empty_responses() {
        let reply = pb::TxnReply::new("operation:0");
        // responses vector is empty by default
        let res = reply.into_upsert_reply();
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("No responses in TxnReply")
        );
    }
}
