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

use databend_meta_raft_config::header::Header;
use databend_meta_sled_store::IVec;
use databend_meta_sled_store::SledBytesError;
use databend_meta_sled_store::SledSerde;
use serde::Deserialize;
use serde::Serialize;

/// A [`Header`] as it is stored in the legacy `DataHeader` sled key space.
///
/// The header itself is live data — V004 keeps it in a plain `df_meta/VERSION`
/// file. Only the sled representation is compatibility surface, so the sled
/// encoding lives on this wrapper rather than on `Header`.
///
/// A newtype, so serde renders it as the bare `Header`: the export format is
/// frozen and must not gain a wrapper level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SledHeader(pub Header);

impl fmt::Display for SledHeader {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<Header> for SledHeader {
    fn from(header: Header) -> Self {
        Self(header)
    }
}

impl SledSerde for SledHeader {
    fn ser(&self) -> Result<IVec, SledBytesError> {
        let x = serde_json::to_vec(self)?;
        Ok(x.into())
    }

    fn de<T: AsRef<[u8]>>(v: T) -> Result<Self, SledBytesError>
    where Self: Sized {
        let x = serde_json::from_slice(v.as_ref())?;
        Ok(x)
    }
}
