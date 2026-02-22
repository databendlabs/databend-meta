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
use std::fmt::Formatter;

use databend_meta_types::Change;
use databend_meta_types::SeqV;
use display_more::display_slice::DisplaySliceExt;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GetKVReq {
    pub key: String,
}

impl GetKVReq {
    pub fn new(key: impl ToString) -> Self {
        Self {
            key: key.to_string(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct MGetKVReq {
    pub keys: Vec<String>,
}

impl MGetKVReq {
    pub fn new<S: ToString>(keys: impl IntoIterator<Item = S>) -> Self {
        Self {
            keys: keys.into_iter().map(|x| x.to_string()).collect(),
        }
    }
}

impl fmt::Display for MGetKVReq {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.keys.display())
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ListKVReq {
    pub prefix: String,
}

impl ListKVReq {
    pub fn new(prefix: impl ToString) -> Self {
        Self {
            prefix: prefix.to_string(),
        }
    }
}

pub type UpsertKVReply = Change<Vec<u8>>;
pub type GetKVReply = Option<SeqV<Vec<u8>>>;
pub type MGetKVReply = Vec<Option<SeqV<Vec<u8>>>>;
pub type ListKVReply = Vec<(String, SeqV<Vec<u8>>)>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_kv_req() {
        let r = GetKVReq::new("foo");
        assert_eq!(r.key, "foo");

        let r2 = GetKVReq::new(format!("bar/{}", 42));
        assert_eq!(r2.key, "bar/42");
    }

    #[test]
    fn test_mget_kv_req() {
        let r = MGetKVReq::new(["a", "b", "c"]);
        assert_eq!(r.keys, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_mget_kv_req_display() {
        let r = MGetKVReq::new(["x", "y"]);
        let s = r.to_string();
        assert!(s.contains("x"), "{}", s);
        assert!(s.contains("y"), "{}", s);
    }

    #[test]
    fn test_list_kv_req() {
        let r = ListKVReq::new("prefix/");
        assert_eq!(r.prefix, "prefix/");
    }

    #[test]
    fn test_get_kv_req_clone_eq() {
        let r = GetKVReq::new("foo");
        let r2 = r.clone();
        assert_eq!(r, r2);
    }

    #[test]
    fn test_mget_kv_req_clone_eq() {
        let r = MGetKVReq::new(["a", "b"]);
        let r2 = r.clone();
        assert_eq!(r, r2);
    }

    #[test]
    fn test_list_kv_req_clone_eq() {
        let r = ListKVReq::new("prefix/");
        let r2 = r.clone();
        assert_eq!(r, r2);
    }
}
