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
use std::time::Duration;

use display_more::DisplayUnixTimeStampExt;

use crate::protobuf as pb;

impl pb::TxnPutRequest {
    pub fn new(
        key: impl ToString,
        value: Vec<u8>,
        expire_at: Option<u64>,
        ttl_ms: Option<u64>,
    ) -> Self {
        Self {
            key: key.to_string(),
            value,
            expire_at,
            ttl_ms,
        }
    }
}

impl fmt::Display for pb::TxnPutRequest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Put key={}", self.key)?;
        if let Some(expire_at) = self.expire_at {
            write!(
                f,
                " expire_at: {}",
                Duration::from_millis(expire_at).display_unix_timestamp()
            )?;
        }
        if let Some(ttl_ms) = self.ttl_ms {
            write!(f, "  ttl: {:?}", Duration::from_millis(ttl_ms))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let req = pb::TxnPutRequest::new("k1", b"v1".to_vec(), None, None);
        assert_eq!(req.key, "k1");
        assert_eq!(req.value, b"v1");
        assert!(req.expire_at.is_none());
        assert!(req.ttl_ms.is_none());
    }

    #[test]
    fn test_display_basic() {
        let req = pb::TxnPutRequest::new("k1", b"v1".to_vec(), None, None);
        assert_eq!(req.to_string(), "Put key=k1");
    }

    #[test]
    fn test_display_with_expire_at() {
        let req = pb::TxnPutRequest::new("k1", b"v1".to_vec(), Some(1000), None);
        let s = req.to_string();
        assert!(s.starts_with("Put key=k1 expire_at: "), "got: {}", s);
    }

    #[test]
    fn test_display_with_ttl() {
        let req = pb::TxnPutRequest::new("k1", b"v1".to_vec(), None, Some(5000));
        assert_eq!(req.to_string(), "Put key=k1  ttl: 5s");
    }
}
