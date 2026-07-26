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

//! The layout of a node's data directory, and the version header stored in it.
//!
//! The header lives in a plain `df_meta/VERSION` file. Reading data left by an
//! older version is the upgrader's job, not this module's.

use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;

use log::info;
use raft_log::codeq::error_context_ext::ErrorContextExt;

use crate::config::RaftConfig;
use crate::data_version::DATA_VERSION;
use crate::header::Header;

/// Create the log and snapshot directories for the current data version.
pub fn ensure_dirs(raft_dir: &str) -> Result<(), io::Error> {
    let raft_dir = Path::new(raft_dir);
    let version_dir = raft_dir.join("df_meta").join(format!("{}", DATA_VERSION));

    let log_dir = version_dir.join("log");
    if !log_dir.exists() {
        fs::create_dir_all(&log_dir)
            .context(|| format!("creating dir {}", log_dir.as_path().display()))?;
        info!("Created log dir: {}", log_dir.as_path().display());
    }

    let snapshot_dir = version_dir.join("snapshot");
    if !snapshot_dir.exists() {
        fs::create_dir_all(&snapshot_dir)
            .context(|| format!("creating dir {}", snapshot_dir.as_path().display()))?;
        info!("Created snapshot dir: {}", snapshot_dir.as_path().display());
    }

    Ok(())
}

/// Path of the version file: `<raft_dir>/df_meta/VERSION`.
pub fn header_path(config: &RaftConfig) -> PathBuf {
    let raft_dir = Path::new(&config.raft_dir);
    raft_dir.join("df_meta").join("VERSION")
}

/// Read the version header, or `None` if the data directory has no version file.
pub fn load_header(config: &RaftConfig) -> Result<Option<Header>, io::Error> {
    let header_path = header_path(config);

    if !header_path.exists() {
        return Ok(None);
    }

    let state = fs::read(&header_path)
        .context(|| format!("reading version file {}", header_path.as_path().display(),))?;

    let state = serde_json::from_slice::<Header>(&state).map_err(|e| {
        io::Error::new(io::ErrorKind::InvalidData, e)
            .context(|| format!("parsing version file {}", header_path.as_path().display(),))
    })?;

    Ok(Some(state))
}

/// Write the version header, replacing any existing one.
pub fn write_header(config: &RaftConfig, header: &Header) -> Result<(), io::Error> {
    let header_path = header_path(config);
    let buf = serde_json::to_vec(header).map_err(|e| {
        io::Error::new(io::ErrorKind::InvalidData, e)
            .context(|| format!("serializing header at {}", header_path.as_path().display(),))
    })?;

    fs::write(&header_path, &buf).context(|| {
        format!(
            "writing version file at {}: {}",
            header_path.as_path().display(),
            String::from_utf8_lossy(&buf)
        )
    })?;

    info!(
        "Wrote header {:?}; at {}",
        header,
        header_path.as_path().display()
    );

    Ok(())
}

/// Panic if the on-disk data is older than this build can read.
///
/// There is no recovery from this: the operator has to run an older release
/// first, so the message names the version to download.
pub fn assert_compatible(header: &Header) {
    let min_compatible = DATA_VERSION.min_compatible_data_version();

    if header.version < min_compatible {
        let max_compatible_working_version = header.version.max_compatible_working_version();
        let version_info = min_compatible.version_info();

        eprintln!("Working data version is: {}", DATA_VERSION);
        eprintln!("On-disk data version is too old: {}", header.version);
        eprintln!(
            "The latest compatible version is {}",
            max_compatible_working_version
        );
        eprintln!(
            "Download the latest compatible version: {}",
            version_info.download_url()
        );

        panic!(
            "On-disk data version {} is too old, the latest compatible version is {}.",
            header.version, max_compatible_working_version
        );
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::data_version::DataVersion;

    fn raft_config(dir: &TempDir) -> RaftConfig {
        RaftConfig {
            raft_dir: dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn test_load_header_reports_a_malformed_version_file() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let config = raft_config(&temp_dir);

        assert_eq!(
            load_header(&config)?,
            None,
            "a missing version file is not an error"
        );

        fs::create_dir_all(temp_dir.path().join("df_meta"))?;
        fs::write(header_path(&config), b"not-json")?;

        let err = load_header(&config).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        let parse_err = serde_json::from_slice::<Header>(b"not-json").unwrap_err();
        assert_eq!(
            err.to_string(),
            format!(
                "{}; when:(parsing version file {})",
                parse_err,
                header_path(&config).display()
            )
        );

        Ok(())
    }

    #[test]
    #[should_panic(
        expected = "On-disk data version V001 is too old, the latest compatible version is V002."
    )]
    fn test_assert_compatible_rejects_data_older_than_the_minimal_compatible_version() {
        assert_compatible(&Header {
            version: DataVersion::V001,
            upgrading: None,
            cleaning: false,
        });
    }
}
