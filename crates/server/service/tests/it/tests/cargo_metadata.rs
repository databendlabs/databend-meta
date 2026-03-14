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

//! Verify that `[package.metadata.compat]` in this crate's Cargo.toml
//! stays in sync with the authoritative values in `databend-meta-version`.

use databend_meta_version::Version;

fn cargo_compat_version(key: &str) -> Version {
    let v: toml::Value = toml::from_str(include_str!("../../../Cargo.toml")).unwrap();
    let s = v["package"]["metadata"]["compat"][key]
        .as_str()
        .unwrap_or_else(|| panic!("missing key `{key}` in [package.metadata.compat]"));
    Version::parse(s)
}

#[test]
fn test_min_client_version_matches_version_crate() {
    assert_eq!(
        databend_meta_version::MIN_CLIENT_VERSION,
        cargo_compat_version("min-client-version")
    );
}
