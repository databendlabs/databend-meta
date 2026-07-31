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

//! A config value that must not reach a log or a serialized config dump.

use std::fmt;

/// Printed in place of the real value.
const REDACTED: &str = "***";

/// A secret string whose [`fmt::Debug`] and [`serde::Serialize`] output is redacted.
///
/// [`RaftConfig`] derives both traits and is logged in whole at several places,
/// so a plain `String` field would write the secret to the log on every node
/// start. Wrapping it makes redaction a property of the type rather than
/// something every call site has to remember.
///
/// [`PartialEq`] compares the real value and is **not** constant time; it is
/// meant for comparing configs. Authenticating a secret received over the wire
/// must use a constant time comparison instead.
///
/// [`RaftConfig`]: crate::config::RaftConfig
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    pub fn new(secret: impl Into<String>) -> Self {
        Self(secret.into())
    }

    /// Returns the real value.
    ///
    /// Named so that every place handling the plain text can be found by
    /// grepping for it.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(REDACTED)
    }
}

impl serde::Serialize for Secret {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        serializer.serialize_str(REDACTED)
    }
}

#[cfg(test)]
mod tests {
    use crate::secret::Secret;

    #[test]
    fn test_secret_hides_the_value_but_keeps_it_reachable() -> anyhow::Result<()> {
        let s = Secret::new("hunter2");

        assert_eq!(format!("{:?}", s), "***");
        assert_eq!(serde_json::to_string(&s)?, r#""***""#);
        assert_eq!(s.expose(), "hunter2");

        Ok(())
    }

    #[test]
    fn test_secret_compares_by_value() {
        assert_eq!(Secret::new("a"), Secret::new("a"));
        assert_ne!(Secret::new("a"), Secret::new("b"));
    }
}
