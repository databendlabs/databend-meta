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

//! A value that must not reach logs or serialized config dumps.

use std::fmt;

/// Printed in place of the real value.
const REDACTED: &str = "***";

/// A secret string whose debug and serialized output is redacted.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    /// Creates a secret from a plain-text string.
    pub fn new(secret: impl Into<String>) -> Self {
        Self(secret.into())
    }

    /// Returns the plain-text value.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str(REDACTED)
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
    use super::Secret;

    #[test]
    fn test_secret_hides_the_value_but_keeps_it_reachable() -> anyhow::Result<()> {
        let secret = Secret::new("hunter2");

        let debug = format!("{secret:?}");
        assert_eq!(debug, "***");

        let serialized = serde_json::to_string(&secret)?;
        assert_eq!(serialized, r#""***""#);

        assert_eq!(secret.expose(), "hunter2");

        Ok(())
    }

    #[test]
    fn test_secret_compares_by_value() {
        let first = Secret::new("a");
        let same = Secret::new("a");
        assert_eq!(first, same);

        let first = Secret::new("a");
        let different = Secret::new("b");
        assert_ne!(first, different);
    }
}
