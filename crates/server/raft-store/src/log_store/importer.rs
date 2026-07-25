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

use std::io;

use databend_meta_types::raft_types::LogId;
use raft_log::api::raft_log_writer::RaftLogWriter;

use crate::log_store::RaftLog;
use crate::log_store::codec_wrapper::Cw;
use crate::log_store::log_store_meta::LogStoreMeta;
use crate::log_store::util;
use crate::sled_compat::RaftStateKey;
use crate::sled_compat::key_spaces::RaftStoreEntry;

/// Import series of [`RaftStoreEntry`] record into [`RaftLog`].
///
/// [`RaftStoreEntry`] is line-wise format for export or data backup.
pub struct Importer {
    pub raft_log: RaftLog,
    pub max_log_id: Option<LogId>,
}

impl Importer {
    pub fn new(raft_log: RaftLog) -> Self {
        Importer {
            raft_log,
            max_log_id: None,
        }
    }

    pub async fn flush(mut self) -> Result<RaftLog, io::Error> {
        util::blocking_flush(&mut self.raft_log).await?;
        Ok(self.raft_log)
    }

    pub fn import_raft_store_entry(&mut self, entry: RaftStoreEntry) -> Result<(), io::Error> {
        match entry {
            RaftStoreEntry::DataHeader { .. } => {
                // V004 RaftLog does not store DataHeader
            }

            //////////////////////////// V004 log ////////////////////////////
            RaftStoreEntry::LogEntry(log_entry) => {
                let log_id = log_entry.log_id;
                let payload = log_entry.payload;

                self.raft_log.append([(Cw(log_id), Cw(payload))])?;
                self.max_log_id = std::cmp::max(self.max_log_id, Some(log_id));
            }

            RaftStoreEntry::NodeId(node_id) => {
                self.raft_log
                    .save_user_data(Some(LogStoreMeta { node_id }))?;
            }

            RaftStoreEntry::Vote(vote) => {
                if let Some(vote) = vote {
                    self.raft_log.save_vote(Cw(vote))?;
                }
            }

            RaftStoreEntry::Committed(committed) => {
                if let Some(committed) = committed {
                    self.raft_log.commit(Cw(committed))?;
                }
            }

            RaftStoreEntry::Purged(purged) => {
                if let Some(purged) = purged {
                    self.raft_log.purge(Cw(purged))?;
                }
            }

            ///////////////////////// V003 and before Log ////////////////////
            RaftStoreEntry::Logs { .. } => {
                unreachable!("V003 Logs should be written to V004 log");
            }
            // `StateMachineId` is a V003 leftover with no V004 counterpart, and
            // `RaftStoreEntry::upgrade()` passes it through unchanged. Drop it
            // instead of aborting the upgrade of a store that still has one.
            RaftStoreEntry::RaftStateKV {
                key: RaftStateKey::StateMachineId,
                ..
            } => {}
            RaftStoreEntry::RaftStateKV { .. } => {
                unreachable!("V003 RaftStateKV should be written to V004 log");
            }
            RaftStoreEntry::LogMeta { .. } => {
                unreachable!("V003 LogMeta should be written to V004 log");
            }

            //////////////////////// State machine entries ///////////////////////
            RaftStoreEntry::StateMachineMeta { .. } => {
                unreachable!("StateMachineMeta should be written to log");
            }
            RaftStoreEntry::Nodes { .. } => {
                unreachable!("Nodes should be written to log");
            }
            RaftStoreEntry::Expire { .. } => {
                unreachable!("Expire should be written to log");
            }
            RaftStoreEntry::GenericKV { .. } => {
                unreachable!("GenericKV should be written to log");
            }
            RaftStoreEntry::Sequences { .. } => {
                unreachable!("Sequences should be written to log");
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use databend_meta_types::raft_types::Entry;
    use databend_meta_types::raft_types::EntryPayload;
    use databend_meta_types::raft_types::Vote;
    use databend_meta_types::raft_types::new_log_id;
    use pretty_assertions::assert_eq;
    use raft_log::chunked_wal::Config as WalConfig;

    use super::*;
    use crate::header::Header;
    use crate::log_store::LogStoreMeta;
    use crate::log_store::RaftLogConfig;
    use crate::sled_compat::RaftStateValue;

    fn new_raft_log(dir: &tempfile::TempDir) -> anyhow::Result<RaftLog> {
        let config = RaftLogConfig {
            wal: WalConfig {
                dir: dir.path().to_str().unwrap().to_string(),
                read_buffer_size: None,
                chunk_max_records: Some(100),
                chunk_max_size: Some(1024 * 1024),
                truncate_incomplete_record: None,
                flush_batch_wait: None,
                flush_batch_max_items: None,
            },
            log_cache_max_items: Some(1000),
            log_cache_capacity: Some(1024 * 1024),
        };
        Ok(RaftLog::open(Arc::new(config))?)
    }

    fn log_entry(index: u64) -> RaftStoreEntry {
        RaftStoreEntry::LogEntry(Entry {
            log_id: new_log_id(1, 1, index),
            payload: EntryPayload::Blank,
        })
    }

    #[tokio::test]
    async fn test_import_writes_logs_and_state() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let mut importer = Importer::new(new_raft_log(&temp_dir)?);

        // Logs first, then state: the importer must not depend on the order in
        // which the exported entries arrive.
        for index in 0..3 {
            importer.import_raft_store_entry(log_entry(index))?;
        }
        importer.import_raft_store_entry(RaftStoreEntry::NodeId(Some(7)))?;
        importer.import_raft_store_entry(RaftStoreEntry::Vote(Some(Vote::new(3, 1))))?;
        importer.import_raft_store_entry(RaftStoreEntry::Committed(Some(new_log_id(1, 1, 2))))?;
        importer.import_raft_store_entry(RaftStoreEntry::Purged(Some(new_log_id(1, 1, 0))))?;

        assert_eq!(importer.max_log_id, Some(new_log_id(1, 1, 2)));

        let raft_log = importer.flush().await?;
        let state = raft_log.log_state();

        assert_eq!(state.vote(), Some(&Cw(Vote::new(3, 1))));
        assert_eq!(state.committed(), Some(&Cw(new_log_id(1, 1, 2))));
        assert_eq!(state.purged(), Some(&Cw(new_log_id(1, 1, 0))));
        assert_eq!(state.last(), Some(&Cw(new_log_id(1, 1, 2))));
        assert_eq!(state.user_data, Some(LogStoreMeta { node_id: Some(7) }));

        let log_ids = raft_log
            .read(0, 3)
            .map(|r| r.map(|(id, _)| id.0))
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(log_ids, vec![new_log_id(1, 1, 1), new_log_id(1, 1, 2)]);

        Ok(())
    }

    #[tokio::test]
    async fn test_import_ignores_absent_state_and_repeated_writes() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let mut importer = Importer::new(new_raft_log(&temp_dir)?);

        // `None` state carries nothing to write.
        importer.import_raft_store_entry(RaftStoreEntry::Vote(None))?;
        importer.import_raft_store_entry(RaftStoreEntry::Committed(None))?;
        importer.import_raft_store_entry(RaftStoreEntry::Purged(None))?;
        // The header lives in the version file, not in the raft log.
        importer.import_raft_store_entry(RaftStoreEntry::new_header(Header::this_version()))?;
        // A V003 state-machine id has no V004 counterpart.
        importer.import_raft_store_entry(RaftStoreEntry::RaftStateKV {
            key: RaftStateKey::StateMachineId,
            value: RaftStateValue::StateMachineId((1, 2)),
        })?;

        assert_eq!(importer.max_log_id, None);

        // A later write of the same kind wins.
        importer.import_raft_store_entry(RaftStoreEntry::Vote(Some(Vote::new(1, 1))))?;
        importer.import_raft_store_entry(RaftStoreEntry::Vote(Some(Vote::new(4, 2))))?;
        importer.import_raft_store_entry(RaftStoreEntry::NodeId(Some(1)))?;
        importer.import_raft_store_entry(RaftStoreEntry::NodeId(Some(9)))?;

        let raft_log = importer.flush().await?;
        let state = raft_log.log_state();

        assert_eq!(state.vote(), Some(&Cw(Vote::new(4, 2))));
        assert_eq!(state.committed(), None);
        assert_eq!(state.purged(), None);
        assert_eq!(state.last(), None);
        assert_eq!(state.user_data, Some(LogStoreMeta { node_id: Some(9) }));

        Ok(())
    }

    #[tokio::test]
    async fn test_flushed_data_survives_a_reopen() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let mut importer = Importer::new(new_raft_log(&temp_dir)?);

        importer.import_raft_store_entry(log_entry(0))?;
        importer.import_raft_store_entry(RaftStoreEntry::Vote(Some(Vote::new(3, 1))))?;
        drop(importer.flush().await?);

        let reopened = new_raft_log(&temp_dir)?;
        let state = reopened.log_state();

        assert_eq!(state.vote(), Some(&Cw(Vote::new(3, 1))));
        assert_eq!(state.last(), Some(&Cw(new_log_id(1, 1, 0))));

        Ok(())
    }
}
