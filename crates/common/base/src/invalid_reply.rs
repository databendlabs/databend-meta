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
use std::fmt::Display;
use std::io;

use anyerror::AnyError;
use serde::Deserialize;
use serde::Serialize;

#[derive(thiserror::Error, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct InvalidReply {
    msg: String,
    #[source]
    source: AnyError,
}

impl InvalidReply {
    pub fn new(msg: impl Display, source: &(impl std::error::Error + 'static)) -> Self {
        Self {
            msg: msg.to_string(),
            source: AnyError::new(source).with_type(None::<String>),
        }
    }

    pub fn add_context(mut self, context: impl Display) -> Self {
        self.msg = format!("{}: {}", self.msg, context);
        self
    }
}

impl Display for InvalidReply {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "InvalidReply: ")?;
        if !self.msg.is_empty() {
            write!(f, "{}; ", self.msg)?;
        }
        write!(f, "source:({})", self.source)
    }
}

impl From<InvalidReply> for io::Error {
    fn from(e: InvalidReply) -> Self {
        io::Error::new(io::ErrorKind::InvalidData, e)
    }
}
