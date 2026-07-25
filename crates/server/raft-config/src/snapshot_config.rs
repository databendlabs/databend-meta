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

//! Snapshot storage path configuration and management.

use std::fs;
use std::io;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use log::info;
use openraft::SnapshotId;

use crate::config::RaftConfig;
use crate::data_version::DataVersion;

/// Path related config for Raft store.
#[derive(Debug, Clone)]
pub struct SnapshotConfig {
    data_version: DataVersion,
    raft_config: RaftConfig,
}

impl SnapshotConfig {
    const TEMP_PREFIX: &'static str = "0.snap";

    pub fn new(data_version: DataVersion, config: RaftConfig) -> Self {
        SnapshotConfig {
            data_version,
            raft_config: config,
        }
    }

    pub fn data_version(&self) -> DataVersion {
        self.data_version
    }

    pub fn raft_config(&self) -> &RaftConfig {
        &self.raft_config
    }

    pub fn version_dir(&self) -> String {
        format!(
            "{}/df_meta/{}",
            self.raft_config.raft_dir, self.data_version
        )
    }

    pub fn snapshot_dir(&self) -> String {
        format!(
            "{}/df_meta/{}/snapshot",
            self.raft_config.raft_dir, self.data_version
        )
    }

    /// Return a two element tuple of snapshot dir and fn
    pub fn snapshot_dir_fn(&self, snapshot_id: &SnapshotId) -> (String, String) {
        (self.snapshot_dir(), Self::snapshot_fn(snapshot_id))
    }

    pub fn snapshot_path(&self, snapshot_id: &SnapshotId) -> String {
        format!("{}/{}", self.snapshot_dir(), Self::snapshot_fn(snapshot_id))
    }

    pub fn snapshot_fn(snapshot_id: &SnapshotId) -> String {
        format!("{}.snap", snapshot_id)
    }

    // TODO: remove this
    /// Return a two elements tuple of snapshot dir and temp fn
    pub fn snapshot_temp_dir_fn(&self) -> (String, String) {
        let temp_snapshot_id = self.temp_snapshot_id();
        (self.snapshot_dir(), temp_snapshot_id)
    }

    // TODO: remove this
    pub fn snapshot_temp_path(&self) -> String {
        let temp_snapshot_id = self.temp_snapshot_id();
        format!("{}/{}", self.snapshot_dir(), temp_snapshot_id)
    }

    pub fn temp_snapshot_id(&self) -> String {
        // Sleep to avoid timestamp collision when this function is called twice in a short time.
        std::thread::sleep(std::time::Duration::from_millis(2));

        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        format!("{}-{}", Self::TEMP_PREFIX, ts)
    }

    /// Make directory for snapshot if it does not exist and return the snapshot directory.
    pub fn ensure_snapshot_dir(&self) -> Result<String, io::Error> {
        let dir = self.snapshot_dir();

        fs::create_dir_all(&dir).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("{}: while create_dir_all(); path: {}", e, dir),
            )
        })?;

        Ok(dir)
    }

    /// Move the snapshot to the final path.
    ///
    /// So that it is visible and can be loaded.
    ///
    /// It returns the final storage path and rel path.
    pub fn move_to_final_path(
        &self,
        temp_path: &str,
        snapshot_id: SnapshotId,
    ) -> Result<(String, String), io::Error> {
        let (storage_path, rel_path) = self.snapshot_dir_fn(&snapshot_id);
        let final_path = format!("{storage_path}/{rel_path}");

        fs::rename(temp_path, &final_path)?;

        info!(
            "snapshot {} moved to final path: {}",
            snapshot_id, final_path
        );

        Ok((storage_path, rel_path))
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io;

    use pretty_assertions::assert_eq;

    use super::SnapshotConfig;
    use crate::config::RaftConfig;
    use crate::data_version::DATA_VERSION;

    fn snapshot_config(raft_dir: &str) -> SnapshotConfig {
        let raft_config = RaftConfig {
            raft_dir: raft_dir.to_string(),
            ..Default::default()
        };

        SnapshotConfig::new(DATA_VERSION, raft_config)
    }

    #[test]
    fn test_temp_path_no_dup() -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let p = temp.path();
        let raft_config = RaftConfig {
            raft_dir: p.to_str().unwrap().to_string(),
            ..Default::default()
        };

        let store = super::SnapshotConfig::new(DATA_VERSION, raft_config);

        let mut prev = None;
        for _i in 0..10 {
            let path = store.snapshot_temp_path();
            assert_ne!(prev, Some(path.clone()), "dup: {}", path);
            prev = Some(path);
        }

        Ok(())
    }

    #[test]
    fn test_paths_are_scoped_by_raft_dir_and_data_version() {
        let c = snapshot_config("/data/meta");
        let version_dir = format!("/data/meta/df_meta/{}", DATA_VERSION);
        let snapshot_dir = format!("{}/snapshot", version_dir);
        let id = "1-2-3-4".to_string();

        assert_eq!(c.data_version(), DATA_VERSION);
        assert_eq!(c.raft_config().raft_dir, "/data/meta");
        assert_eq!(c.version_dir(), version_dir);
        assert_eq!(c.snapshot_dir(), snapshot_dir);
        assert_eq!(SnapshotConfig::snapshot_fn(&id), "1-2-3-4.snap");
        assert_eq!(
            c.snapshot_dir_fn(&id),
            (snapshot_dir.clone(), "1-2-3-4.snap".to_string())
        );
        assert_eq!(
            c.snapshot_path(&id),
            format!("{}/1-2-3-4.snap", snapshot_dir)
        );

        // The temp name ends with a timestamp, so only its shape is fixed.
        let (dir, temp_fn) = c.snapshot_temp_dir_fn();
        assert_eq!(dir, snapshot_dir);
        assert!(temp_fn.starts_with("0.snap-"), "temp fn: {}", temp_fn);

        let temp_path = c.snapshot_temp_path();
        assert!(
            temp_path.starts_with(&format!("{}/0.snap-", snapshot_dir)),
            "temp path: {}",
            temp_path
        );
    }

    #[test]
    fn test_ensure_snapshot_dir_creates_it_once() -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let c = snapshot_config(temp.path().to_str().unwrap());

        // Creating it twice must succeed: the second call finds it in place.
        assert_eq!(c.ensure_snapshot_dir()?, c.snapshot_dir());
        assert_eq!(c.ensure_snapshot_dir()?, c.snapshot_dir());
        assert!(std::path::Path::new(&c.snapshot_dir()).is_dir());

        Ok(())
    }

    #[test]
    fn test_ensure_snapshot_dir_reports_a_file_in_the_way() -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let c = snapshot_config(temp.path().to_str().unwrap());

        // A plain file where `df_meta` should be makes the whole path unusable.
        fs::write(temp.path().join("df_meta"), [])?;

        let err = c.ensure_snapshot_dir().unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::NotADirectory);
        assert!(
            err.to_string().ends_with(&format!(
                ": while create_dir_all(); path: {}",
                c.snapshot_dir()
            )),
            "{}",
            err
        );

        Ok(())
    }

    #[test]
    fn test_move_to_final_path() -> anyhow::Result<()> {
        let temp = tempfile::tempdir()?;
        let c = snapshot_config(temp.path().to_str().unwrap());
        let dir = c.ensure_snapshot_dir()?;

        let temp_path = c.snapshot_temp_path();
        fs::write(&temp_path, b"snapshot-data")?;

        let got = c.move_to_final_path(&temp_path, "1-2-3-4".to_string())?;

        assert_eq!(got, (dir.clone(), "1-2-3-4.snap".to_string()));
        assert!(!std::path::Path::new(&temp_path).exists());
        assert_eq!(fs::read(format!("{}/1-2-3-4.snap", dir))?, b"snapshot-data");

        Ok(())
    }
}
