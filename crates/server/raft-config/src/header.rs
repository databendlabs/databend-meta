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

use crate::data_version::DATA_VERSION;
use crate::data_version::DataVersion;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct Header {
    /// Current data version
    pub version: DataVersion,

    /// The target version to upgrade to.
    ///
    /// If it is present, the data is upgrading.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upgrading: Option<DataVersion>,

    /// The second part of the upgrading process:
    /// new version data is ready, and the old version data is cleaning up.
    #[serde(default)]
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub cleaning: bool,
}

impl fmt::Display for Header {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.version)?;

        if let Some(upgrading) = self.upgrading {
            write!(f, " -> {}", upgrading)?;
        };

        if self.cleaning {
            write!(f, " (cleaning)")?;
        }

        Ok(())
    }
}

impl Header {
    /// The key this header is stored under, in the legacy `DataHeader` key space.
    pub const KEY: &'static str = "header";

    pub fn this_version() -> Self {
        Self {
            version: DATA_VERSION,
            upgrading: None,
            cleaning: false,
        }
    }
}
