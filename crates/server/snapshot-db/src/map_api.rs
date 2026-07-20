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
use std::fmt::Write;
use std::io;

pub type MapKeyPrefix = &'static str;

pub trait MapKeyEncode {
    /// PREFIX is the prefix of the key used to define key space in the on-disk storage.
    const PREFIX: MapKeyPrefix;

    fn prefix(&self) -> MapKeyPrefix {
        Self::PREFIX
    }

    fn encode<W: Write>(&self, w: W) -> Result<(), fmt::Error>;
}

pub trait MapKeyDecode: Sized {
    fn decode(buf: &str) -> Result<Self, io::Error>;
}
