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

impl fmt::Display for pb::txn_op_response::Response {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Get(r) => write!(f, "Get: {}", r),
            Self::Put(r) => write!(f, "Put: {}", r),
            Self::Delete(r) => write!(f, "Delete: {}", r),
            Self::DeleteByPrefix(r) => write!(f, "DeleteByPrefix: {}", r),
            Self::FetchIncreaseU64(r) => write!(f, "FetchIncreaseU64: {}", r),
        }
    }
}
