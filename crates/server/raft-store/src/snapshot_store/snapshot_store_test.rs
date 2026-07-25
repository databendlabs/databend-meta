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

//! Round trip of a snapshot: build it with [`SnapshotWriter`], ship it in chunks to
//! [`SnapshotReceiver`], and reopen the received file.

use std::error::Error;
use std::fs;
use std::io;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use databend_meta_runtime_api::TokioRuntime;
use databend_meta_snapshot_db::DB;
use databend_meta_types::protobuf::SnapshotChunkRequestV003;
use databend_meta_types::raft_types::Membership;
use databend_meta_types::raft_types::SnapshotMeta;
use databend_meta_types::raft_types::StoredMembership;
use databend_meta_types::raft_types::TypeConfig;
use databend_meta_types::raft_types::Vote;
use databend_meta_types::sys_data::SysData;
use futures_util::TryStreamExt;
use openraft::testing::log_id;
use pretty_assertions::assert_eq;
use rotbl::v001::SeqMarked;
use tempfile::TempDir;

use crate::config::RaftConfig;
use crate::data_version::DataVersion;
use crate::snapshot_store::MetaSnapshotId;
use crate::snapshot_store::SnapshotStore;
use crate::snapshot_store::SnapshotStoreV003;
use crate::snapshot_store::open_snapshot::OpenSnapshot;
use crate::snapshot_store::receiver::SnapshotReceiver;

/// Bytes per chunk of the simulated snapshot stream.
const CHUNK_SIZE: usize = 4096;

const REMOTE_ADDR: &str = "1.2.3.4:9191";

#[test]
fn test_snapshot_store_directories_are_version_scoped() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let config = raft_config(&temp_dir);
    let raft_dir = config.raft_dir.clone();

    let mut v003 = SnapshotStoreV003::<TokioRuntime>::new(config.clone());
    assert_eq!(v003.data_version(), DataVersion::V003);
    assert_eq!(v003.config(), &config);
    assert_eq!(
        v003.snapshot_config().snapshot_dir(),
        format!("{}/df_meta/V003/snapshot", raft_dir)
    );
    // SnapshotStoreV003 is a V003-flavored view of the same store.
    let _: &mut SnapshotStore<TokioRuntime> = &mut v003;

    let v004 = SnapshotStore::<TokioRuntime>::new(config);
    assert_eq!(v004.data_version(), DataVersion::V004);
    assert_eq!(
        v004.snapshot_config().snapshot_dir(),
        format!("{}/df_meta/V004/snapshot", raft_dir)
    );

    Ok(())
}

#[tokio::test]
async fn test_write_receive_and_reopen_a_multi_chunk_snapshot() -> anyhow::Result<()> {
    let sender_dir = tempfile::tempdir()?;
    let sender = SnapshotStore::<TokioRuntime>::new(raft_config(&sender_dir));

    let entries = test_entries(64);
    let snapshot_id = MetaSnapshotId::new(Some(log_id::<TypeConfig>(1, 2, 3)), 7);

    let writer = sender.new_writer()?;
    let strm = futures::stream::iter(entries.clone().into_iter().map(Ok));
    let db = writer
        .write_kv_stream(strm, snapshot_id.clone(), test_sys_data())
        .await?;

    assert_eq!(
        db.path(),
        format!(
            "{}/{}.snap",
            sender.snapshot_config().snapshot_dir(),
            snapshot_id
        )
    );
    assert_eq!(db.sys_data(), &test_sys_data());
    assert_eq!(db.stat().key_num, entries.len() as u64);
    assert_eq!(db_entries(&db).await?, entries);

    // The temp file is gone: it was renamed to the id-derived final path.
    let mut written = fs::read_dir(sender.snapshot_config().snapshot_dir())?
        .map(|e| Ok(e?.file_name().to_string_lossy().to_string()))
        .collect::<Result<Vec<_>, io::Error>>()?;
    written.sort();
    assert_eq!(written, vec![format!("{}.snap", snapshot_id)]);

    // Ship the file to another node, chunk by chunk.

    let receiver_dir = tempfile::tempdir()?;
    let receiving = SnapshotStore::<TokioRuntime>::new(raft_config(&receiver_dir));
    let mut receiver = receiving.new_receiver(REMOTE_ADDR)?;

    let recv_bytes = Arc::new(AtomicU64::new(0));
    {
        let recv_bytes = recv_bytes.clone();
        receiver.set_on_recv_callback(move |n| {
            recv_bytes.fetch_add(n, Ordering::Relaxed);
        });
    }

    let (tx, join_handle) = receiver.spawn_receiving_thread("test round trip");

    let blob = fs::read(db.path())?;
    let n_data_chunks = blob.chunks(CHUNK_SIZE).count();
    assert!(n_data_chunks > 1, "the snapshot must span several chunks");

    for chunk in blob.chunks(CHUNK_SIZE) {
        tx.send(data_chunk(chunk)).await?;
    }
    tx.send(finish_chunk("rotbl::v001", db.snapshot_meta().clone())?)
        .await?;

    let received = join_handle.await???;

    assert_eq!(received.format, "rotbl::v001");
    assert_eq!(received.vote, test_vote());
    assert_eq!(&received.snapshot_meta, db.snapshot_meta());
    assert_eq!(received.remote_addr, REMOTE_ADDR);
    assert_eq!(
        received.n_received,
        n_data_chunks + 1,
        "the final metadata chunk is counted too"
    );
    assert_eq!(received.size_received, blob.len());
    assert_eq!(
        recv_bytes.load(Ordering::Relaxed),
        blob.len() as u64,
        "the callback observes every received byte"
    );
    assert_eq!(
        received.stat_str().to_string(),
        format!(
            "received {} chunks, {} bytes from {}",
            n_data_chunks + 1,
            blob.len(),
            REMOTE_ADDR
        )
    );

    // The received file is a complete, openable snapshot.

    let reopened = DB::open_snapshot(
        &received.storage_path,
        &received.temp_rel_path,
        snapshot_id.to_string(),
        RaftConfig::default().to_rotbl_config(),
    )?;

    assert_eq!(reopened.sys_data(), &test_sys_data());
    assert_eq!(reopened.snapshot_meta(), db.snapshot_meta());
    assert_eq!(db_entries(&reopened).await?, entries);

    Ok(())
}

#[tokio::test]
async fn test_receive_fails_when_the_input_closes_before_the_last_chunk() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let receiver = new_receiver(&temp_dir)?;

    let (tx, join_handle) = receiver.spawn_receiving_thread("ctx");
    tx.send(data_chunk(b"partial")).await?;
    drop(tx);

    let err = join_handle.await??.unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    assert_eq!(
        err.to_string(),
        format!(
            "SnapshotReceiver input channel is closed: SnapshotReceiver received 1 chunks, 7 bytes from {} while ctx",
            REMOTE_ADDR
        )
    );

    Ok(())
}

#[test]
fn test_receive_rejects_malformed_final_metadata() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let mut receiver = new_receiver(&temp_dir)?;

    let mut chunk = data_chunk(b"data");
    chunk.rpc_meta = Some("not-json".to_string());

    let err = receiver.receive(chunk).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

    let parse_err = serde_json::from_str::<(String, Vote, SnapshotMeta)>("not-json").unwrap_err();
    assert_eq!(
        err.to_string(),
        receive_context(
            &temp_dir,
            &parse_err.to_string(),
            "loading last chunk rpc_meta"
        )
    );

    Ok(())
}

#[test]
fn test_receive_rejects_an_unsupported_format() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let mut receiver = new_receiver(&temp_dir)?;

    let err = receiver
        .receive(finish_chunk("rotbl::v002", test_snapshot_meta())?)
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        err.to_string(),
        receive_context(
            &temp_dir,
            "unsupported snapshot format: rotbl::v002, expect: rotbl::v001",
            "check input format"
        )
    );

    Ok(())
}

#[test]
fn test_receive_rejects_a_chunk_after_completion() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let mut receiver = new_receiver(&temp_dir)?;

    let received = receiver
        .receive(finish_chunk("rotbl::v001", test_snapshot_meta())?)?
        .unwrap();
    assert_eq!(received.n_received, 1);
    assert_eq!(received.size_received, 0);

    let err = receiver.receive(data_chunk(b"late")).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        err.to_string(),
        receive_context(
            &temp_dir,
            "snapshot_data is already shutdown",
            "take self.temp_file"
        )
    );

    Ok(())
}

#[test]
fn test_move_to_final_path_fails_for_a_missing_source() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let store = SnapshotStore::<TokioRuntime>::new(raft_config(&temp_dir));
    let snapshot_config = store.snapshot_config();
    snapshot_config.ensure_snapshot_dir()?;

    let missing = format!("{}/absent.snap", snapshot_config.snapshot_dir());
    let err = snapshot_config
        .move_to_final_path(&missing, "1-2-3-4".to_string())
        .unwrap_err();

    assert_eq!(err.kind(), io::ErrorKind::NotFound);

    Ok(())
}

#[tokio::test]
async fn test_load_snapshot_reports_a_missing_file() -> anyhow::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let store = SnapshotStore::<TokioRuntime>::new(raft_config(&temp_dir));

    let err = store
        .new_loader()
        .load_snapshot(&"1-2-3-4".to_string())
        .await
        .unwrap_err();

    let source = err.source().unwrap().to_string();
    assert_eq!(
        err.to_string(),
        format!(
            "SnapshotStoreError(Read: {}, while: opening snapshot file: {}",
            source,
            store.snapshot_config().snapshot_dir()
        )
    );

    Ok(())
}

fn raft_config(dir: &TempDir) -> RaftConfig {
    RaftConfig {
        raft_dir: dir.path().to_str().unwrap().to_string(),
        ..Default::default()
    }
}

/// A receiver writing to a fixed temp file, so error messages are predictable.
fn new_receiver(dir: &TempDir) -> anyhow::Result<SnapshotReceiver<TokioRuntime>> {
    let f = fs::File::create(dir.path().join("temp.snap"))?;
    Ok(SnapshotReceiver::new(
        REMOTE_ADDR,
        dir.path().to_str().unwrap(),
        "temp.snap",
        f,
    ))
}

/// Rebuild the context [`SnapshotReceiver::receive`] appends to its errors.
fn receive_context(dir: &TempDir, error: &str, context: &str) -> String {
    format!(
        "{} while:(SnapshotReceiver::receive(): {}; remote_addr: {}; temp_path: {}/temp.snap)",
        error,
        context,
        REMOTE_ADDR,
        dir.path().to_str().unwrap()
    )
}

fn data_chunk(chunk: &[u8]) -> SnapshotChunkRequestV003 {
    SnapshotChunkRequestV003 {
        rpc_meta: None,
        chunk: chunk.to_vec(),
    }
}

fn finish_chunk(
    format: &str,
    snapshot_meta: SnapshotMeta,
) -> anyhow::Result<SnapshotChunkRequestV003> {
    let rpc_meta = serde_json::to_string(&(format.to_string(), test_vote(), snapshot_meta))?;
    Ok(SnapshotChunkRequestV003 {
        rpc_meta: Some(rpc_meta),
        chunk: vec![],
    })
}

fn test_vote() -> Vote {
    Vote::new(5, 6)
}

fn test_snapshot_meta() -> SnapshotMeta {
    SnapshotMeta {
        snapshot_id: "1-2-3-4".to_string(),
        last_log_id: Some(log_id::<TypeConfig>(1, 2, 3)),
        last_membership: StoredMembership::new(
            Some(log_id::<TypeConfig>(1, 2, 3)),
            Membership::new_with_defaults(vec![], []),
        ),
    }
}

fn test_sys_data() -> SysData {
    let mut sys_data = SysData::default();
    sys_data.update_seq(64);
    *sys_data.last_applied_mut() = Some(log_id::<TypeConfig>(1, 2, 3));
    *sys_data.last_membership_mut() = StoredMembership::new(
        Some(log_id::<TypeConfig>(1, 2, 3)),
        Membership::new_with_defaults(vec![], []),
    );
    sys_data
}

/// `n` sorted user-key entries, large enough that the snapshot spans several chunks.
fn test_entries(n: u64) -> Vec<(String, SeqMarked)> {
    (0..n)
        .map(|i| {
            let key = format!("kv--/key-{:04}", i);
            let value = format!("value-{:04}", i).repeat(16).into_bytes();
            (key, SeqMarked::new_normal(i + 1, value))
        })
        .collect()
}

async fn db_entries(db: &DB) -> anyhow::Result<Vec<(String, SeqMarked)>> {
    Ok(db.inner_range().try_collect::<Vec<_>>().await?)
}
