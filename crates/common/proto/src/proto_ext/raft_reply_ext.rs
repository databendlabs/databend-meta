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

use serde::Serialize;

use crate::protobuf as pb;

impl pb::RaftReply {
    pub fn new<T: Serialize>(data: &T) -> Self {
        let data = serde_json::to_string(data).expect("fail to serialize");
        pb::RaftReply {
            data,
            error: Default::default(),
        }
    }

    pub fn err<E: Serialize>(e: &E) -> Self {
        let error = serde_json::to_string(e).expect("fail to serialize");
        pb::RaftReply {
            data: Default::default(),
            error,
        }
    }
}
