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

use std::fmt::Display;
use std::fmt::Formatter;

use crate::TxnDeleteByPrefixRequest;

impl TxnDeleteByPrefixRequest {
    pub fn new(prefix: impl ToString) -> Self {
        Self {
            prefix: prefix.to_string(),
        }
    }
}

impl Display for TxnDeleteByPrefixRequest {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "TxnDeleteByPrefixRequest prefix={}", self.prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let req = TxnDeleteByPrefixRequest::new("my_prefix");
        assert_eq!(req.prefix, "my_prefix");
    }

    #[test]
    fn test_display() {
        let req = TxnDeleteByPrefixRequest::new("pfx/");
        assert_eq!(req.to_string(), "TxnDeleteByPrefixRequest prefix=pfx/");
    }
}
