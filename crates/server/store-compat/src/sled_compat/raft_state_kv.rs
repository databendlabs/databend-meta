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

use databend_meta_sled_store::IVec;
use databend_meta_sled_store::SledBytesError;
use databend_meta_sled_store::SledOrderedSerde;
use databend_meta_sled_store::SledSerde;
use databend_meta_types::anyerror::AnyError;
use databend_meta_types::raft_types::LogId;
use databend_meta_types::raft_types::NodeId;
use databend_meta_types::raft_types::Vote;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RaftStateKey {
    /// The node id.
    Id,

    /// Hard state of the raft log, including `current_term` and `voted_for`.
    HardState,

    // TODO: remove this field. It is not used any more. It is kept only for compatibility.
    /// The id of the only active state machine.
    /// When installing a state machine snapshot:
    /// 1. A temp state machine is written into a new sled::Tree.
    /// 2. Update this field to point to the new state machine.
    /// 3. Cleanup old state machine.
    StateMachineId,

    /// The last committed log id
    Committed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftStateValue {
    NodeId(NodeId),

    HardState(Vote),

    /// active state machine, previous state machine
    StateMachineId((u64, u64)),

    Committed(Option<LogId>),
}

impl RaftStateValue {
    pub fn node_id(&self) -> NodeId {
        match self {
            RaftStateValue::NodeId(x) => *x,
            _ => panic!("expect NodeId"),
        }
    }

    pub fn vote(&self) -> Vote {
        match self {
            RaftStateValue::HardState(x) => *x,
            _ => panic!("expect HardState"),
        }
    }

    pub fn committed(&self) -> Option<LogId> {
        match self {
            RaftStateValue::Committed(x) => *x,
            _ => panic!("expect Committed"),
        }
    }
}

impl fmt::Display for RaftStateKey {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl SledOrderedSerde for RaftStateKey {
    fn ser(&self) -> Result<IVec, SledBytesError> {
        let i = match self {
            RaftStateKey::Id => 1,
            RaftStateKey::HardState => 2,
            RaftStateKey::StateMachineId => 3,
            RaftStateKey::Committed => 4,
        };

        Ok(IVec::from(&[i]))
    }

    fn de<V: AsRef<[u8]>>(v: V) -> Result<Self, SledBytesError>
    where Self: Sized {
        let slice = v.as_ref();
        if slice[0] == 1 {
            return Ok(RaftStateKey::Id);
        } else if slice[0] == 2 {
            return Ok(RaftStateKey::HardState);
        } else if slice[0] == 3 {
            return Ok(RaftStateKey::StateMachineId);
        } else if slice[0] == 4 {
            return Ok(RaftStateKey::Committed);
        }

        Err(SledBytesError::new(&AnyError::error("invalid key IVec")))
    }
}

impl From<RaftStateValue> for NodeId {
    fn from(v: RaftStateValue) -> Self {
        match v {
            RaftStateValue::NodeId(x) => x,
            _ => panic!("expect NodeId"),
        }
    }
}

impl From<RaftStateValue> for Vote {
    fn from(v: RaftStateValue) -> Self {
        match v {
            RaftStateValue::HardState(x) => x,
            _ => panic!("expect HardState"),
        }
    }
}

impl From<RaftStateValue> for (u64, u64) {
    fn from(v: RaftStateValue) -> Self {
        match v {
            RaftStateValue::StateMachineId(x) => x,
            _ => panic!("expect StateMachineId"),
        }
    }
}

impl From<RaftStateValue> for Option<LogId> {
    fn from(v: RaftStateValue) -> Self {
        match v {
            RaftStateValue::Committed(x) => x,
            _ => panic!("expect Committed"),
        }
    }
}

impl SledSerde for RaftStateValue {
    fn de<T: AsRef<[u8]>>(v: T) -> Result<Self, SledBytesError>
    where Self: Sized {
        let s = serde_json::from_slice(v.as_ref())?;
        Ok(s)
    }
}

#[cfg(test)]
mod tests {
    use databend_meta_types::raft_types::new_log_id;
    use pretty_assertions::assert_eq;

    use super::*;

    const ALL_KEYS: [RaftStateKey; 4] = [
        RaftStateKey::Id,
        RaftStateKey::HardState,
        RaftStateKey::StateMachineId,
        RaftStateKey::Committed,
    ];

    fn log_id() -> LogId {
        new_log_id(1, 2, 3)
    }

    #[test]
    fn test_key_round_trip_and_ordering() -> Result<(), SledBytesError> {
        let encoded = ALL_KEYS
            .iter()
            .map(|k| Ok(k.ser()?.to_vec()))
            .collect::<Result<Vec<_>, SledBytesError>>()?;
        assert_eq!(encoded, vec![vec![1], vec![2], vec![3], vec![4]]);

        for key in ALL_KEYS {
            assert_eq!(RaftStateKey::de(key.ser()?)?, key);
        }

        Ok(())
    }

    #[test]
    fn test_key_de_rejects_an_unknown_discriminant() {
        let err = RaftStateKey::de([5]).unwrap_err();
        assert_eq!(err.to_string(), "SledBytesError: invalid key IVec");
    }

    #[test]
    fn test_key_display() {
        assert_eq!(ALL_KEYS.map(|k| k.to_string()), [
            "Id",
            "HardState",
            "StateMachineId",
            "Committed"
        ]);
    }

    #[test]
    fn test_value_round_trip() -> Result<(), SledBytesError> {
        let values = [
            RaftStateValue::NodeId(7),
            RaftStateValue::HardState(Vote::new(3, 1)),
            RaftStateValue::StateMachineId((1, 2)),
            RaftStateValue::Committed(Some(log_id())),
        ];

        for value in values {
            let got = RaftStateValue::de(value.ser()?)?;
            assert_eq!(format!("{:?}", got), format!("{:?}", value));
        }

        Ok(())
    }

    #[test]
    fn test_value_de_rejects_malformed_json() {
        assert!(RaftStateValue::de(b"not-json").is_err());
    }

    #[test]
    fn test_value_accessors_and_conversions() {
        assert_eq!(RaftStateValue::NodeId(7).node_id(), 7);
        assert_eq!(NodeId::from(RaftStateValue::NodeId(7)), 7);

        let vote = Vote::new(3, 1);
        assert_eq!(RaftStateValue::HardState(vote).vote(), vote);
        assert_eq!(Vote::from(RaftStateValue::HardState(vote)), vote);

        assert_eq!(
            <(u64, u64)>::from(RaftStateValue::StateMachineId((1, 2))),
            (1, 2)
        );

        let committed = RaftStateValue::Committed(Some(log_id()));
        assert_eq!(committed.committed(), Some(log_id()));
        assert_eq!(Option::<LogId>::from(committed), Some(log_id()));
    }

    #[test]
    #[should_panic(expected = "expect NodeId")]
    fn test_node_id_of_a_non_node_id_value_panics() {
        RaftStateValue::Committed(None).node_id();
    }

    #[test]
    #[should_panic(expected = "expect HardState")]
    fn test_vote_of_a_non_hard_state_value_panics() {
        RaftStateValue::NodeId(7).vote();
    }

    #[test]
    #[should_panic(expected = "expect Committed")]
    fn test_committed_of_a_non_committed_value_panics() {
        RaftStateValue::NodeId(7).committed();
    }

    #[test]
    #[should_panic(expected = "expect StateMachineId")]
    fn test_converting_a_non_state_machine_id_value_panics() {
        let _ = <(u64, u64)>::from(RaftStateValue::NodeId(7));
    }
}
