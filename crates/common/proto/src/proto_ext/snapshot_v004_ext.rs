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

use crate::protobuf::InstallSnapshotResponseV004;

type LeaderId = openraft::impls::leader_id_adv::LeaderId<u64, u64>;
type Vote = openraft::Vote<LeaderId>;

impl InstallSnapshotResponseV004 {
    pub fn to_vote(&self) -> Result<Vote, AnyError> {
        let Some(vote) = self.vote else {
            return Err(AnyError::error(
                "missing vote in InstallSnapshotResponseV004",
            ));
        };

        Ok(vote.into())
    }
}
