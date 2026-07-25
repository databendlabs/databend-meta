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

//! Header handling and the V003 to V004 on-disk upgrade.

use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;

use databend_meta_runtime_api::TokioRuntime;
use databend_meta_sled_store::SledTree;
use databend_meta_sled_store::drop_sled_db;
use databend_meta_sled_store::init_get_sled_db;
use databend_meta_types::raft_types::Entry;
use databend_meta_types::raft_types::EntryPayload;
use databend_meta_types::raft_types::LogId;
use databend_meta_types::raft_types::Vote;
use databend_meta_types::raft_types::new_log_id;
use pretty_assertions::assert_eq;
use tempfile::TempDir;

use crate::config::RaftConfig;
use crate::data_version::DataVersion;
use crate::header::Header;
use crate::log_store::Cw;
use crate::log_store::LogStoreMeta;
use crate::log_store::RaftLog;
use crate::ondisk::OnDisk;
use crate::ondisk::TREE_HEADER;
use crate::sled_compat::LogMetaKey;
use crate::sled_compat::LogMetaValue;
use crate::sled_compat::RaftStateKey;
use crate::sled_compat::RaftStateValue;
use crate::sled_compat::key_spaces::DataHeader;
use crate::sled_compat::key_spaces::RaftStoreEntry;

/// The sled db is a process-wide singleton, so tests that open it must not overlap.
static SLED: Mutex<()> = Mutex::new(());

struct SledGuard(#[allow(dead_code)] MutexGuard<'static, ()>);

impl SledGuard {
    fn new() -> Self {
        let guard = SLED.lock().unwrap_or_else(|e| e.into_inner());
        drop_sled_db();
        SledGuard(guard)
    }
}

impl Drop for SledGuard {
    fn drop(&mut self) {
        drop_sled_db();
    }
}

/// The last purged log id of the V003 fixture; logs up to it must not migrate.
fn purged() -> LogId {
    new_log_id(1, 1, 2)
}

fn vote() -> Vote {
    Vote::new(3, 1)
}

fn committed() -> LogId {
    new_log_id(1, 1, 4)
}

const NODE_ID: u64 = 7;

#[tokio::test]
async fn test_open_creates_dirs_and_defaults_to_v003() -> anyhow::Result<()> {
    let _sled = SledGuard::new();
    let temp_dir = tempfile::tempdir()?;
    let config = raft_config(&temp_dir);

    let on_disk = OnDisk::open(&config).await?;

    assert_eq!(on_disk.header, Header {
        version: DataVersion::V003,
        upgrading: None,
        cleaning: false,
    });
    assert_eq!(
        on_disk.to_string(),
        format!(
            "header: {:?}, data-dir: {}",
            on_disk.header, config.raft_dir
        )
    );

    assert!(temp_dir.path().join("df_meta/V004/log").is_dir());
    assert!(temp_dir.path().join("df_meta/V004/snapshot").is_dir());

    assert_eq!(
        fs::read_to_string(header_path(&temp_dir))?,
        r#"{"version":"V003"}"#
    );
    assert_eq!(
        OnDisk::load_header_from_fs(&config)?,
        Some(on_disk.header),
        "the written header is what open() returned"
    );

    Ok(())
}

#[tokio::test]
async fn test_open_keeps_an_existing_header() -> anyhow::Result<()> {
    let _sled = SledGuard::new();
    let temp_dir = tempfile::tempdir()?;
    let config = raft_config(&temp_dir);

    let header = Header {
        version: DataVersion::V003,
        upgrading: Some(DataVersion::V004),
        cleaning: true,
    };
    write_header(&temp_dir, &header)?;

    let on_disk = OnDisk::open(&config).await?;

    assert_eq!(on_disk.header, header);
    assert_eq!(on_disk.header.to_string(), "V003 -> V004 (cleaning)");

    Ok(())
}

#[tokio::test]
async fn test_open_migrates_a_header_stored_in_sled() -> anyhow::Result<()> {
    let _sled = SledGuard::new();
    let temp_dir = tempfile::tempdir()?;
    let config = raft_config(&temp_dir);

    let header = Header {
        version: DataVersion::V003,
        upgrading: Some(DataVersion::V004),
        cleaning: true,
    };

    {
        let db = init_get_sled_db(config.raft_dir.clone(), 1024 * 1024);
        let tree = SledTree::open(&db, TREE_HEADER)?;
        let (k, v) = RaftStoreEntry::serialize(&RaftStoreEntry::new_header(header))?;
        tree.tree.insert(k, v)?;
    }

    let on_disk = OnDisk::open(&config).await?;

    assert_eq!(on_disk.header, header, "the sled header is adopted");
    assert_eq!(
        OnDisk::load_header_from_fs(&config)?,
        Some(header),
        "and copied to the version file"
    );

    let db = init_get_sled_db(config.raft_dir.clone(), 1024 * 1024);
    let tree = SledTree::open(&db, TREE_HEADER)?;
    assert_eq!(
        tree.key_space::<DataHeader>()
            .get(&Header::KEY.to_string())?,
        None,
        "the sled copy is removed after migration"
    );

    Ok(())
}

#[test]
fn test_load_header_from_fs_reports_a_malformed_version_file() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let config = raft_config(&temp_dir);

    assert_eq!(
        OnDisk::load_header_from_fs(&config)?,
        None,
        "a missing version file is not an error"
    );

    fs::create_dir_all(temp_dir.path().join("df_meta"))?;
    fs::write(header_path(&temp_dir), b"not-json")?;

    let err = OnDisk::load_header_from_fs(&config).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);

    let parse_err = serde_json::from_slice::<Header>(b"not-json").unwrap_err();
    assert_eq!(
        err.to_string(),
        format!(
            "{}; when:(parsing version file {})",
            parse_err,
            header_path(&temp_dir).display()
        )
    );

    Ok(())
}

#[test]
#[should_panic(
    expected = "On-disk data version V001 is too old, the latest compatible version is V002."
)]
fn test_new_rejects_data_older_than_the_minimal_compatible_version() {
    let temp_dir = tempfile::tempdir().unwrap();
    OnDisk::new(
        Header {
            version: DataVersion::V001,
            upgrading: None,
            cleaning: false,
        },
        &raft_config(&temp_dir),
    );
}

#[tokio::test]
async fn test_upgrade_v003_to_v004_migrates_logs_state_and_snapshot() -> anyhow::Result<()> {
    let _sled = SledGuard::new();
    let temp_dir = tempfile::tempdir()?;
    let config = raft_config(&temp_dir);

    write_v003_data(&config)?;
    write_v003_snapshot(&config)?;

    let mut on_disk = OnDisk::new(v003_header(), &config);
    on_disk.upgrade::<TokioRuntime>().await?;

    assert_upgraded_to_v004(&on_disk, &config, &temp_dir)?;

    Ok(())
}

#[tokio::test]
async fn test_upgrade_restarts_an_upgrade_interrupted_before_cleanup() -> anyhow::Result<()> {
    let _sled = SledGuard::new();
    let temp_dir = tempfile::tempdir()?;
    let config = raft_config(&temp_dir);

    write_v003_data(&config)?;
    write_v003_snapshot(&config)?;

    // A crash after `begin_upgrading`: half-written V004 data is present.
    OnDisk::ensure_dirs(&config.raft_dir)?;
    fs::write(temp_dir.path().join("df_meta/V004/log/half-written"), b"x")?;

    let mut on_disk = OnDisk::new(
        Header {
            version: DataVersion::V003,
            upgrading: Some(DataVersion::V004),
            cleaning: false,
        },
        &config,
    );
    on_disk.upgrade::<TokioRuntime>().await?;

    assert!(
        !temp_dir
            .path()
            .join("df_meta/V004/log/half-written")
            .exists(),
        "the half-written V004 data is discarded and rebuilt from V003"
    );
    assert_upgraded_to_v004(&on_disk, &config, &temp_dir)?;

    Ok(())
}

#[tokio::test]
async fn test_upgrade_finishes_an_upgrade_interrupted_during_cleanup() -> anyhow::Result<()> {
    let _sled = SledGuard::new();
    let temp_dir = tempfile::tempdir()?;
    let config = raft_config(&temp_dir);

    // A crash after `clean_upgrading`: the V004 data is complete, only the V003
    // data still has to go.
    write_v003_data(&config)?;
    write_v003_snapshot(&config)?;
    {
        let mut building = OnDisk::new(v003_header(), &config);
        building.upgrade::<TokioRuntime>().await?;
    }
    write_v003_data(&config)?;
    write_v003_snapshot(&config)?;

    let mut on_disk = OnDisk::new(
        Header {
            version: DataVersion::V003,
            upgrading: Some(DataVersion::V004),
            cleaning: true,
        },
        &config,
    );
    on_disk.upgrade::<TokioRuntime>().await?;

    assert_upgraded_to_v004(&on_disk, &config, &temp_dir)?;

    Ok(())
}

fn raft_config(dir: &TempDir) -> RaftConfig {
    RaftConfig {
        raft_dir: dir.path().to_str().unwrap().to_string(),
        ..Default::default()
    }
}

fn v003_header() -> Header {
    Header {
        version: DataVersion::V003,
        upgrading: None,
        cleaning: false,
    }
}

fn header_path(dir: &TempDir) -> PathBuf {
    dir.path().join("df_meta").join("VERSION")
}

fn write_header(dir: &TempDir, header: &Header) -> anyhow::Result<()> {
    fs::create_dir_all(dir.path().join("df_meta"))?;
    fs::write(header_path(dir), serde_json::to_vec(header)?)?;
    Ok(())
}

/// Write a V003 sled store: five logs of which the first three are purged,
/// plus the node id, vote and committed log id.
fn write_v003_data(config: &RaftConfig) -> anyhow::Result<()> {
    let db = init_get_sled_db(config.raft_dir.clone(), 1024 * 1024);

    let raft_log = SledTree::open(&db, "raft_log")?;
    insert(&raft_log, RaftStoreEntry::LogMeta {
        key: LogMetaKey::LastPurged,
        value: LogMetaValue::LogId(purged()),
    })?;
    for index in 0..=4 {
        insert(&raft_log, RaftStoreEntry::Logs {
            key: index,
            value: Entry {
                log_id: new_log_id(1, 1, index),
                payload: EntryPayload::Blank,
            },
        })?;
    }

    let raft_state = SledTree::open(&db, "raft_state")?;
    insert(&raft_state, RaftStoreEntry::RaftStateKV {
        key: RaftStateKey::Id,
        value: RaftStateValue::NodeId(NODE_ID),
    })?;
    insert(&raft_state, RaftStoreEntry::RaftStateKV {
        key: RaftStateKey::HardState,
        value: RaftStateValue::HardState(vote()),
    })?;
    insert(&raft_state, RaftStoreEntry::RaftStateKV {
        key: RaftStateKey::Committed,
        value: RaftStateValue::Committed(Some(committed())),
    })?;

    Ok(())
}

fn insert(tree: &SledTree, entry: RaftStoreEntry) -> anyhow::Result<()> {
    let (k, v) = RaftStoreEntry::serialize(&entry)?;
    tree.tree.insert(k, v)?;
    Ok(())
}

fn write_v003_snapshot(config: &RaftConfig) -> anyhow::Result<()> {
    let dir = PathBuf::from(&config.raft_dir).join("df_meta/V003/snapshot");
    fs::create_dir_all(&dir)?;
    fs::write(dir.join("1-1-1-1.snap"), b"snapshot-content")?;
    Ok(())
}

/// Assert the upgrade landed: V004 header, V004 raft log holding only the
/// unpurged entries, and the snapshot moved from V003 to V004.
fn assert_upgraded_to_v004(
    on_disk: &OnDisk,
    config: &RaftConfig,
    temp_dir: &TempDir,
) -> anyhow::Result<()> {
    let expected = Header {
        version: DataVersion::V004,
        upgrading: None,
        cleaning: false,
    };
    assert_eq!(on_disk.header, expected);
    assert_eq!(OnDisk::load_header_from_fs(config)?, Some(expected));

    let raft_log = RaftLog::open(Arc::new(config.to_raft_log_config()))?;
    let state = raft_log.log_state();

    assert_eq!(state.vote(), Some(&Cw(vote())));
    assert_eq!(state.committed(), Some(&Cw(committed())));
    assert_eq!(state.purged(), Some(&Cw(purged())));
    assert_eq!(state.last(), Some(&Cw(new_log_id(1, 1, 4))));
    assert_eq!(
        state.user_data,
        Some(LogStoreMeta {
            node_id: Some(NODE_ID)
        })
    );

    let entries = raft_log.read(0, 5).collect::<Result<Vec<_>, _>>()?;
    let log_ids = entries.into_iter().map(|(id, _)| id.0).collect::<Vec<_>>();
    assert_eq!(
        log_ids,
        vec![new_log_id(1, 1, 3), new_log_id(1, 1, 4)],
        "logs up to the purged one are not migrated"
    );

    let v004_snapshot = temp_dir.path().join("df_meta/V004/snapshot/1-1-1-1.snap");
    assert_eq!(fs::read_to_string(&v004_snapshot)?, "snapshot-content");
    assert!(
        !temp_dir.path().join("df_meta/V003/snapshot").exists(),
        "the V003 snapshot dir is removed"
    );

    assert_eq!(
        entry_names(&config.raft_dir)?,
        vec!["df_meta"],
        "the sled db files are removed"
    );
    assert_eq!(entry_names(temp_dir.path().join("df_meta"))?, vec![
        "V003", "V004", "VERSION"
    ]);

    Ok(())
}

fn entry_names(dir: impl AsRef<std::path::Path>) -> anyhow::Result<Vec<String>> {
    let mut names = fs::read_dir(dir)?
        .map(|e| Ok(e?.file_name().to_string_lossy().into_owned()))
        .collect::<anyhow::Result<Vec<_>>>()?;
    names.sort();
    Ok(names)
}
