// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! RocksDB-backed Raft log store implementing openraft `RaftLogStorage`.

use crate::raft::storage::RocksDBStorage;
use crate::raft::types::{AppMetadataRaftState, MetadataRaftTypeConfig};
use openraft::storage::LogState;
use openraft::storage::RaftLogStorage;
use openraft::AnyError;
use openraft::Entry;
use openraft::LogId;
use openraft::OptionalSend;
use openraft::RaftLogId;
use openraft::RaftLogReader;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::Vote;
use parking_lot::RwLock;
use serde_json;
use std::sync::Arc;

/// Raft log storage implementation using RocksDB.
pub struct AppLogStorage {
    storage: Arc<RocksDBStorage>,
    state: Arc<RwLock<AppMetadataRaftState>>,
}

impl AppLogStorage {
    pub fn new(storage: Arc<RocksDBStorage>, state: Arc<RwLock<AppMetadataRaftState>>) -> Self {
        Self { storage, state }
    }
}

impl RaftLogReader<MetadataRaftTypeConfig> for AppLogStorage {
    async fn try_get_log_entries<RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<MetadataRaftTypeConfig>>, StorageError<u64>> {
        // Delegate to LogReader
        let mut reader = self.get_log_reader().await;
        reader.try_get_log_entries(range).await
    }
}

impl RaftLogStorage<MetadataRaftTypeConfig> for AppLogStorage {
    type LogReader = AppLogReader;

    async fn get_log_state(&mut self) -> Result<LogState<MetadataRaftTypeConfig>, StorageError<u64>> {
        let state = self.state.read();
        let last_purged_log_id = state.last_purged_log_id;

        // Prefer the actual last log entry in storage; fall back to in-memory state.
        let last_log_id = match self.storage.get_last_log_index() {
            Ok(Some(idx)) => match self.storage.get_raft_log(idx) {
                Ok(Some(bytes)) => {
                    let entry: Entry<MetadataRaftTypeConfig> = decode_log_entry(idx, &bytes)?;
                    Some(*entry.get_log_id())
                }
                Ok(None) => state.last_applied_log_id,
                Err(e) => {
                    return Err(StorageError::IO {
                        source: StorageIOError::<u64>::read_log_at_index(idx, AnyError::new(&e)),
                    })
                }
            },
            Ok(None) => state.last_applied_log_id,
            Err(e) => {
                return Err(StorageError::IO {
                    source: StorageIOError::<u64>::read_logs(AnyError::new(&e)),
                })
            }
        };

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        AppLogReader {
            storage: Arc::clone(&self.storage),
        }
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let mut state = self.state.write();
        state.vote = Some(vote.clone());
        self.storage.persist_raft_state(&state).map_err(|e| StorageError::IO {
            source: StorageIOError::<u64>::write_vote(AnyError::new(&e)),
        })?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        let state = self.state.read();
        Ok(state.vote.clone())
    }

    async fn save_committed(&mut self, committed: Option<LogId<u64>>) -> Result<(), StorageError<u64>> {
        let mut state = self.state.write();
        state.committed = committed;
        self.storage.persist_raft_state(&state).map_err(|e| StorageError::IO {
            source: StorageIOError::<u64>::write_state_machine(AnyError::new(&e)),
        })?;
        Ok(())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: openraft::storage::LogFlushed<MetadataRaftTypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<MetadataRaftTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries: Vec<_> = entries.into_iter().collect();

        // Persist each entry to RocksDB
        for entry in &entries {
            let log_id = entry.get_log_id();
            let log_index = log_id.index;
            let entry_data = serde_json::to_vec(entry).map_err(|e| StorageError::IO {
                source: StorageIOError::<u64>::write_log_entry(*log_id, AnyError::new(&e)),
            })?;

            self.storage
                .put_raft_log(log_index, &entry_data)
                .map_err(|e| StorageError::IO {
                    source: StorageIOError::<u64>::write_log_entry(*log_id, AnyError::new(&e)),
                })?;
        }

        // Update last_applied_log_id in state
        if let Some(last_entry) = entries.last() {
            let mut state = self.state.write();
            state.last_applied_log_id = Some(*last_entry.get_log_id());
            self.storage.persist_raft_state(&state).map_err(|e| StorageError::IO {
                source: StorageIOError::<u64>::write_logs(AnyError::new(&e)),
            })?;
        }

        // Call callback to notify that logs are flushed
        callback.log_io_completed(Ok(()));

        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        // Delete logs from start_index (inclusive) onwards
        self.storage
            .delete_raft_logs_from(log_id.index)
            .map_err(|e| StorageError::IO {
                source: StorageIOError::<u64>::write_logs(AnyError::new(&e)),
            })?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        // Delete logs up to and including end_index
        self.storage
            .delete_raft_logs_upto(log_id.index)
            .map_err(|e| StorageError::IO {
                source: StorageIOError::<u64>::write_logs(AnyError::new(&e)),
            })?;

        // Update last_purged_log_id
        let mut state = self.state.write();
        state.last_purged_log_id = Some(log_id);
        self.storage.persist_raft_state(&state).map_err(|e| StorageError::IO {
            source: StorageIOError::<u64>::write_logs(AnyError::new(&e)),
        })?;
        Ok(())
    }
}

/// Log reader for Raft.
pub struct AppLogReader {
    storage: Arc<RocksDBStorage>,
}

impl RaftLogReader<MetadataRaftTypeConfig> for AppLogReader {
    async fn try_get_log_entries<RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<MetadataRaftTypeConfig>>, StorageError<u64>> {
        use std::ops::Bound;

        // Determine start and end indices
        let start_index = match range.start_bound() {
            Bound::Included(&idx) => idx,
            Bound::Excluded(&idx) => idx + 1,
            Bound::Unbounded => 0,
        };

        let end_index = match range.end_bound() {
            Bound::Included(&idx) => Some(idx + 1), // Make it exclusive
            Bound::Excluded(&idx) => Some(idx),
            Bound::Unbounded => None,
        };

        let mut entries = Vec::new();
        let mut current_index = start_index;

        loop {
            // Check if we've reached the end
            if let Some(end) = end_index {
                if current_index >= end {
                    break;
                }
            }

            // Try to read log entry
            match self.storage.get_raft_log(current_index) {
                Ok(Some(entry_data)) => {
                    let entry: Entry<MetadataRaftTypeConfig> = decode_log_entry(current_index, &entry_data)?;
                    entries.push(entry);
                    current_index += 1;
                }
                Ok(None) => {
                    // No more entries
                    break;
                }
                Err(e) => {
                    return Err(StorageError::IO {
                        source: StorageIOError::<u64>::read_log_at_index(current_index, AnyError::new(&e)),
                    });
                }
            }
        }

        Ok(entries)
    }
}

// Keep log entry decoding in one place so error mapping stays consistent.
fn decode_log_entry(index: u64, data: &[u8]) -> Result<Entry<MetadataRaftTypeConfig>, StorageError<u64>> {
    serde_json::from_slice(data).map_err(|e| StorageError::IO {
        source: StorageIOError::<u64>::read_log_at_index(index, AnyError::new(&e)),
    })
}
