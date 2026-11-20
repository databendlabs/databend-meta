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

use crate::protobuf::SnapshotChunkRequestV003;
use crate::protobuf::SnapshotResponseV003;
use crate::raft_types::NetworkError;
use crate::raft_types::SnapshotMeta;
use crate::raft_types::Vote;

impl SnapshotChunkRequestV003 {
    /// Build the last chunk of a snapshot stream, which contains vote and snapshot meta, without data.
    pub fn new_end_chunk(vote: Vote, snapshot_meta: SnapshotMeta, snapshot_id: String) -> Self {
        // Preserve Databend's snapshot file identity in its existing wire slot.
        let mut snapshot_meta = serde_json::to_value(snapshot_meta).unwrap();
        snapshot_meta["snapshot_id"] = snapshot_id.into();
        let meta = ("rotbl::v001".to_string(), vote, snapshot_meta);
        let rpc_meta = serde_json::to_string(&meta).unwrap();

        SnapshotChunkRequestV003 {
            rpc_meta: Some(rpc_meta),
            chunk: vec![],
        }
    }

    /// Build a chunk item with data.
    pub fn new_chunk(chunk: Vec<u8>) -> Self {
        SnapshotChunkRequestV003 {
            rpc_meta: None,
            chunk,
        }
    }
}

impl SnapshotResponseV003 {
    pub fn new(vote: Vote) -> Self {
        Self {
            vote: serde_json::to_string(&vote).unwrap(),
        }
    }

    pub fn to_vote(&self) -> Result<Vote, NetworkError> {
        serde_json::from_str(&self.vote).map_err(|e| {
            NetworkError::new(
                &AnyError::new(&e).add_context(|| "when decoding vote from SnapshotResponseV003"),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn end_chunk_preserves_pre_upgrade_snapshot_identity() {
        // Literal metadata from the alpha.29 wire layout, not serialized by 0.10.
        let old_meta = r#"{"last_log_id":{"leader_id":{"term":1,"node_id":2},"index":3},"last_membership":{"log_id":null,"membership":{"configs":[],"nodes":{}}},"snapshot_id":"1-2-3-4"}"#;
        let meta: SnapshotMeta = serde_json::from_str(old_meta).unwrap();
        let chunk =
            SnapshotChunkRequestV003::new_end_chunk(Vote::new(5, 6), meta, "1-2-3-4".to_string());
        let (format, vote, meta): (String, Vote, serde_json::Value) =
            serde_json::from_str(chunk.rpc_meta.as_ref().unwrap()).unwrap();
        assert_eq!(format, "rotbl::v001");
        assert_eq!(vote, Vote::new(5, 6));
        assert_eq!(
            meta,
            serde_json::from_str::<serde_json::Value>(old_meta).unwrap()
        );
        assert!(chunk.chunk.is_empty());
    }
}
