// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! RocksDB-backed Raft state machine store (openraft `RaftStateMachine` + snapshot I/O).

use crate::error::{MetadataError, MetadataResult};
use crate::raft::snapshot::SnapshotFile;
use crate::raft::storage::{RocksDBStorage, STATE_CFS};
use crate::raft::types::{AppDataResponse, AppMetadataRaftState, MetadataNode, MetadataRaftTypeConfig};
use crate::state::RouteEpoch;
use openraft::storage::{RaftStateMachine, SnapshotSignature};
use openraft::AnyError;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::LogId;
use openraft::RaftLogId;
use openraft::Snapshot;
use openraft::SnapshotMeta;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::StoredMembership;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json;
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tracing::info;
use uuid::Uuid;

use crate::raft::AppRaftStateMachine;
use rocksdb::{ColumnFamily, IteratorMode, ReadOptions, Snapshot as DbSnapshot, WriteBatch};

const SNAPSHOT_MAGIC: &[u8] = b"VECT";
const SNAPSHOT_VERSION_V1: u8 = 1;
const META_CF_NAME: &str = "meta";
const ROUTE_EPOCH_KEY: &[u8] = b"route_epoch";
const SNAPSHOT_BATCH_BYTES: usize = 2 * 1024 * 1024;
const TAG_END: u8 = 0;
const TAG_CF_START: u8 = 1;
const TAG_KV: u8 = 2;
const TAG_CF_END: u8 = 3;

/// Bridges openraft state machine callbacks to the application state machine and RocksDB.
pub struct StateMachineStorage {
    storage: Arc<RocksDBStorage>,
    state_machine: Arc<AppRaftStateMachine>,
    state: Arc<RwLock<AppMetadataRaftState>>,
}

impl StateMachineStorage {
    pub fn new(
        storage: Arc<RocksDBStorage>,
        state_machine: Arc<AppRaftStateMachine>,
        state: Arc<RwLock<AppMetadataRaftState>>,
    ) -> MetadataResult<Self> {
        clean_stale_snapshot_tmp(&storage)?;

        Ok(Self {
            storage,
            state_machine,
            state,
        })
    }
}

impl RaftStateMachine<MetadataRaftTypeConfig> for StateMachineStorage {
    type SnapshotBuilder = AppSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, MetadataNode>), StorageError<u64>> {
        let state = self.state.read();
        let last_applied = state.last_applied_log_id;
        let membership = StoredMembership::new(None, state.membership.clone());
        Ok((last_applied, membership))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<AppDataResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<MetadataRaftTypeConfig>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        let mut results = Vec::new();

        for entry in &entries {
            let log_id = *entry.get_log_id();

            match entry.payload {
                EntryPayload::Normal(ref cmd) => {
                    let result = self.state_machine.apply(cmd.clone()).map_err(|e| StorageError::IO {
                        source: StorageIOError::<u64>::apply(log_id, AnyError::new(&e)),
                    })?;

                    // Update last_applied_log_id
                    let mut state = self.state.write();
                    state.last_applied_log_id = Some(log_id);
                    self.storage.persist_raft_state(&state).map_err(|e| StorageError::IO {
                        source: StorageIOError::<u64>::apply(log_id, AnyError::new(&e)),
                    })?;

                    results.push(result);
                }
                EntryPayload::Membership(ref membership) => {
                    // Update membership
                    let mut state = self.state.write();
                    state.membership = membership.clone();
                    state.last_applied_log_id = Some(log_id);
                    self.storage.persist_raft_state(&state).map_err(|e| StorageError::IO {
                        source: StorageIOError::<u64>::apply(log_id, AnyError::new(&e)),
                    })?;

                    results.push(AppDataResponse::None);
                }
                EntryPayload::Blank => {
                    // Blank entry, just update last_applied_log_id
                    let mut state = self.state.write();
                    state.last_applied_log_id = Some(log_id);
                    self.storage.persist_raft_state(&state).map_err(|e| StorageError::IO {
                        source: StorageIOError::<u64>::apply(log_id, AnyError::new(&e)),
                    })?;

                    results.push(AppDataResponse::None);
                }
            }
        }

        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        AppSnapshotBuilder {
            storage: Arc::clone(&self.storage),
            _state_machine: Arc::clone(&self.state_machine),
            _state: Arc::clone(&self.state),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<<MetadataRaftTypeConfig as openraft::RaftTypeConfig>::SnapshotData>, StorageError<u64>> {
        let tmp_path = temp_snapshot_path(&self.storage, &format!("incoming-{}", Uuid::new_v4()));
        let file = SnapshotFile::create(tmp_path).await.map_err(|e| StorageError::IO {
            source: StorageIOError::<u64>::write_snapshot(None, AnyError::new(&e)),
        })?;
        Ok(Box::new(file))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, MetadataNode>,
        snapshot: Box<<MetadataRaftTypeConfig as openraft::RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<u64>> {
        let started = Instant::now();
        let snapshot_path = snapshot.path().to_path_buf();
        let final_path = snapshot_file_path(&self.storage, &meta.snapshot_id);

        let std_file = snapshot.into_std().await.map_err(|e| StorageError::IO {
            source: StorageIOError::<u64>::read_snapshot(Some(meta.signature()), AnyError::new(&e)),
        })?;
        let _header = install_snapshot_payload(&self.storage, meta, std_file)?;

        // Rename the received file into the final snapshot path.
        if final_path.exists() {
            tokio::fs::remove_file(&final_path)
                .await
                .map_err(|e| StorageError::IO {
                    source: StorageIOError::<u64>::write_snapshot(Some(meta.signature()), AnyError::new(&e)),
                })?;
        }
        tokio::fs::rename(&snapshot_path, &final_path)
            .await
            .map_err(|e| StorageError::IO {
                source: StorageIOError::<u64>::write_snapshot(Some(meta.signature()), AnyError::new(&e)),
            })?;
        {
            let mut state = self.state.write();
            state.last_applied_log_id = meta.last_log_id;
            state.membership = meta.last_membership.membership().clone();
            // Purge raft logs that are covered by the snapshot to avoid log holes
            // and keep purge progress aligned with installed snapshot.
            if let Some(last_log) = meta.last_log_id {
                self.storage
                    .delete_raft_logs_upto(last_log.index)
                    .map_err(|e| StorageError::IO {
                        source: StorageIOError::<u64>::write_snapshot(Some(meta.signature()), AnyError::new(&e)),
                    })?;
                state.last_purged_log_id = Some(last_log);
            }
            self.storage.persist_raft_state(&state).map_err(|e| StorageError::IO {
                source: StorageIOError::<u64>::write_snapshot(Some(meta.signature()), AnyError::new(&e)),
            })?;
        }

        // Persist snapshot metadata
        {
            let meta_data = serde_json::to_vec(meta).map_err(|e| StorageError::IO {
                source: StorageIOError::<u64>::write_snapshot(Some(meta.signature()), AnyError::new(&e)),
            })?;
            self.storage
                .put_snapshot_meta(&meta_data)
                .map_err(|e| StorageError::IO {
                    source: StorageIOError::<u64>::write_snapshot(Some(meta.signature()), AnyError::new(&e)),
                })?;
        }
        let size = tokio::fs::metadata(&final_path)
            .await
            .map(|m| m.len())
            .unwrap_or_default();
        info!(
            snapshot_id = %meta.snapshot_id,
            last_log = ?meta.last_log_id,
            bytes = size,
            elapsed_ms = started.elapsed().as_millis(),
            "Installed snapshot"
        );

        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<MetadataRaftTypeConfig>>, StorageError<u64>> {
        let meta_data = match self.storage.get_snapshot_meta().map_err(|e| StorageError::IO {
            source: StorageIOError::<u64>::read_snapshot(None, AnyError::new(&e)),
        })? {
            Some(m) => m,
            None => return Ok(None),
        };

        let meta: SnapshotMeta<u64, MetadataNode> =
            serde_json::from_slice(&meta_data).map_err(|e| StorageError::IO {
                source: StorageIOError::<u64>::read_snapshot(None, AnyError::new(&e)),
            })?;

        let path = snapshot_file_path(&self.storage, &meta.snapshot_id);
        if !path.exists() {
            return Ok(None);
        }

        let file = SnapshotFile::open_read(path).await.map_err(|e| StorageError::IO {
            source: StorageIOError::<u64>::read_snapshot(Some(meta.signature()), AnyError::new(&e)),
        })?;

        Ok(Some(Snapshot {
            meta: meta.clone(),
            snapshot: Box::new(file),
        }))
    }
}

/// Snapshot builder for Raft.
pub struct AppSnapshotBuilder {
    storage: Arc<RocksDBStorage>,
    _state_machine: Arc<AppRaftStateMachine>,
    _state: Arc<RwLock<AppMetadataRaftState>>,
}

impl openraft::storage::RaftSnapshotBuilder<MetadataRaftTypeConfig> for AppSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<MetadataRaftTypeConfig>, StorageError<u64>> {
        let started = Instant::now();
        let snap = self.storage.snapshot();

        let raft_state = load_raft_state_from_snapshot(&self.storage, &snap)?;
        let route_epoch = load_route_epoch_from_snapshot(&self.storage, &snap)?;

        let snapshot_id = format_snapshot_id(raft_state.last_applied_log_id);
        let tmp_path = temp_snapshot_path(&self.storage, &snapshot_id);

        let mut file = std::fs::File::create(&tmp_path).map_err(|e| StorageError::IO {
            source: StorageIOError::<u64>::write_snapshot(None, AnyError::new(&e)),
        })?;

        write_snapshot_payload(&self.storage, &snap, &mut file, route_epoch)?;

        let final_path = snapshot_file_path(&self.storage, &snapshot_id);
        if final_path.exists() {
            tokio::fs::remove_file(&final_path)
                .await
                .map_err(|e| StorageError::IO {
                    source: StorageIOError::<u64>::write_snapshot(None, AnyError::new(&e)),
                })?;
        }
        tokio::fs::rename(&tmp_path, &final_path)
            .await
            .map_err(|e| StorageError::IO {
                source: StorageIOError::<u64>::write_snapshot(None, AnyError::new(&e)),
            })?;

        let meta = SnapshotMeta {
            last_log_id: raft_state.last_applied_log_id,
            last_membership: StoredMembership::new(None, raft_state.membership.clone()),
            snapshot_id: snapshot_id.clone(),
        };

        let meta_data = serde_json::to_vec(&meta).map_err(|e| StorageError::IO {
            source: StorageIOError::<u64>::write_snapshot(None, AnyError::new(&e)),
        })?;
        self.storage
            .put_snapshot_meta(&meta_data)
            .map_err(|e| StorageError::IO {
                source: StorageIOError::<u64>::write_snapshot(None, AnyError::new(&e)),
            })?;

        let size = tokio::fs::metadata(&final_path)
            .await
            .map(|m| m.len())
            .unwrap_or_default();
        info!(
            snapshot_id = %snapshot_id,
            last_log = ?meta.last_log_id,
            bytes = size,
            elapsed_ms = started.elapsed().as_millis(),
            "Built snapshot"
        );

        let file_for_send = SnapshotFile::open_read(final_path)
            .await
            .map_err(|e| StorageError::IO {
                source: StorageIOError::<u64>::read_snapshot(Some(meta.signature()), AnyError::new(&e)),
            })?;

        Ok(Snapshot {
            meta: meta.clone(),
            snapshot: Box::new(file_for_send),
        })
    }
}

#[derive(Serialize, Deserialize)]
struct SnapshotHeaderV1 {
    route_epoch: u64,
}

fn snapshot_file_path(storage: &RocksDBStorage, snapshot_id: &str) -> PathBuf {
    storage.snapshot_dir().join(format!("snapshot-{snapshot_id}.snap"))
}

fn temp_snapshot_path(storage: &RocksDBStorage, snapshot_id: &str) -> PathBuf {
    storage.snapshot_dir().join(format!("snapshot-{snapshot_id}.snap.tmp"))
}

fn clean_stale_snapshot_tmp(storage: &RocksDBStorage) -> MetadataResult<()> {
    let dir = storage.snapshot_dir();
    if !dir.exists() {
        return Ok(());
    }

    for entry in
        fs::read_dir(dir).map_err(|e| MetadataError::Internal(format!("Failed to list snapshot dir: {}", e)))?
    {
        let entry = entry.map_err(|e| MetadataError::Internal(format!("{}", e)))?;
        let path = entry.path();
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if name.ends_with(".snap.tmp") {
                let _ = fs::remove_file(&path);
            }
        }
    }
    Ok(())
}

fn format_snapshot_id(last_log_id: Option<LogId<u64>>) -> String {
    let suffix = Uuid::new_v4();
    match last_log_id {
        Some(log_id) => format!("{}-{}-{}", log_id.leader_id.term, log_id.index, suffix),
        None => format!("bootstrap-{}", suffix),
    }
}

fn write_header<W: Write + Seek>(writer: &mut W, header: &SnapshotHeaderV1) -> std::io::Result<()> {
    writer.seek(SeekFrom::Start(0))?;
    writer.write_all(SNAPSHOT_MAGIC)?;
    writer.write_all(&[SNAPSHOT_VERSION_V1])?;
    writer.write_all(&header.route_epoch.to_le_bytes())?;
    Ok(())
}

fn read_header<R: Read + Seek>(reader: &mut R) -> std::io::Result<SnapshotHeaderV1> {
    let mut magic = [0u8; 4];
    reader.seek(SeekFrom::Start(0))?;
    reader.read_exact(&mut magic)?;
    if magic != SNAPSHOT_MAGIC {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid snapshot magic",
        ));
    }
    let mut version = [0u8; 1];
    reader.read_exact(&mut version)?;
    if version[0] != SNAPSHOT_VERSION_V1 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unsupported snapshot version {}", version[0]),
        ));
    }

    let mut buf_u64 = [0u8; 8];
    reader.read_exact(&mut buf_u64)?;
    let route_epoch = u64::from_le_bytes(buf_u64);

    Ok(SnapshotHeaderV1 { route_epoch })
}

fn write_cf_start<W: Write>(writer: &mut W, name: &str) -> std::io::Result<()> {
    writer.write_all(&[TAG_CF_START])?;
    let name_bytes = name.as_bytes();
    let len = name_bytes.len() as u16;
    writer.write_all(&len.to_le_bytes())?;
    writer.write_all(name_bytes)
}

fn write_cf_end<W: Write>(writer: &mut W) -> std::io::Result<()> {
    writer.write_all(&[TAG_CF_END])
}

fn write_kv<W: Write>(writer: &mut W, key: &[u8], value: &[u8]) -> std::io::Result<()> {
    writer.write_all(&[TAG_KV])?;
    let key_len = key.len() as u32;
    let val_len = value.len() as u64;
    writer.write_all(&key_len.to_le_bytes())?;
    writer.write_all(&val_len.to_le_bytes())?;
    writer.write_all(key)?;
    writer.write_all(value)
}

fn write_end<W: Write>(writer: &mut W) -> std::io::Result<()> {
    writer.write_all(&[TAG_END])
}

fn read_tag<R: Read>(reader: &mut R) -> std::io::Result<u8> {
    let mut tag = [0u8; 1];
    if let Err(e) = reader.read_exact(&mut tag) {
        if e.kind() == std::io::ErrorKind::UnexpectedEof {
            return Ok(TAG_END);
        }
        return Err(e);
    }
    Ok(tag[0])
}

fn read_cf_name<R: Read>(reader: &mut R) -> std::io::Result<String> {
    let mut len_buf = [0u8; 2];
    reader.read_exact(&mut len_buf)?;
    let name_len = u16::from_le_bytes(len_buf) as usize;
    let mut name_buf = vec![0u8; name_len];
    reader.read_exact(&mut name_buf)?;
    String::from_utf8(name_buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

fn read_kv<R: Read>(reader: &mut R) -> std::io::Result<(Vec<u8>, Vec<u8>)> {
    let mut key_len_buf = [0u8; 4];
    reader.read_exact(&mut key_len_buf)?;
    let key_len = u32::from_le_bytes(key_len_buf) as usize;

    let mut val_len_buf = [0u8; 8];
    reader.read_exact(&mut val_len_buf)?;
    let val_len = u64::from_le_bytes(val_len_buf) as usize;

    let mut key = vec![0u8; key_len];
    reader.read_exact(&mut key)?;
    let mut val = vec![0u8; val_len];
    reader.read_exact(&mut val)?;

    Ok((key, val))
}

// Decode a bincode-encoded u64 with consistent error mapping.
// openraft fixes this boundary to StorageError; boxing would add adapter churn.
#[allow(clippy::result_large_err)]
fn decode_u64(bytes: &[u8]) -> Result<u64, StorageError<u64>> {
    bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .map(|(v, _)| v)
        .map_err(|e| StorageError::IO {
            source: StorageIOError::<u64>::read_snapshot(None, AnyError::new(&e)),
        })
}

// openraft fixes this boundary to StorageError; boxing would add adapter churn.
#[allow(clippy::result_large_err)]
fn write_snapshot_batch(
    storage: &RocksDBStorage,
    batch: WriteBatch,
    signature: Option<SnapshotSignature<u64>>,
) -> Result<(), StorageError<u64>> {
    storage.write_batch(batch).map_err(|e| StorageError::IO {
        source: StorageIOError::<u64>::write_snapshot(signature, AnyError::new(&e)),
    })
}

// Read a u64 value from a snapshot column family, returning a default when missing.
// openraft fixes this boundary to StorageError; boxing would add adapter churn.
#[allow(clippy::result_large_err)]
fn read_u64_from_snapshot_cf(
    storage: &RocksDBStorage,
    snap: &DbSnapshot<'_>,
    cf_name: &str,
    key: &[u8],
    default: u64,
) -> Result<u64, StorageError<u64>> {
    let cf = storage.cf(cf_name).map_err(|e| StorageError::IO {
        source: StorageIOError::<u64>::read_snapshot(None, AnyError::new(&e)),
    })?;
    let data = snap
        .get_cf_opt(cf, key, ReadOptions::default())
        .map_err(|e| StorageError::IO {
            source: StorageIOError::<u64>::read_snapshot(None, AnyError::new(&e)),
        })?;

    if let Some(bytes) = data {
        decode_u64(&bytes)
    } else {
        Ok(default)
    }
}

// openraft fixes this boundary to StorageError; boxing would add adapter churn.
#[allow(clippy::result_large_err)]
fn load_raft_state_from_snapshot(
    storage: &RocksDBStorage,
    snap: &DbSnapshot<'_>,
) -> Result<AppMetadataRaftState, StorageError<u64>> {
    let cf = storage.cf("raft_state").map_err(|e| StorageError::IO {
        source: StorageIOError::<u64>::read_snapshot(None, AnyError::new(&e)),
    })?;
    let data = snap
        .get_cf_opt(cf, b"raft_state", ReadOptions::default())
        .map_err(|e| StorageError::IO {
            source: StorageIOError::<u64>::read_snapshot(None, AnyError::new(&e)),
        })?;

    match data {
        Some(bytes) => serde_json::from_slice(&bytes).map_err(|e| StorageError::IO {
            source: StorageIOError::<u64>::read_snapshot(None, AnyError::new(&e)),
        }),
        None => Ok(AppMetadataRaftState::default()),
    }
}

// openraft fixes this boundary to StorageError; boxing would add adapter churn.
#[allow(clippy::result_large_err)]
fn load_route_epoch_from_snapshot(
    storage: &RocksDBStorage,
    snap: &DbSnapshot<'_>,
) -> Result<RouteEpoch, StorageError<u64>> {
    let epoch = read_u64_from_snapshot_cf(storage, snap, META_CF_NAME, ROUTE_EPOCH_KEY, 1)?;
    Ok(RouteEpoch::new(epoch))
}

// openraft fixes this boundary to StorageError; boxing would add adapter churn.
#[allow(clippy::result_large_err)]
fn write_snapshot_payload(
    storage: &RocksDBStorage,
    snap: &DbSnapshot<'_>,
    file: &mut std::fs::File,
    route_epoch: RouteEpoch,
) -> Result<(), StorageError<u64>> {
    let header = SnapshotHeaderV1 {
        route_epoch: route_epoch.as_u64(),
    };
    write_header(file, &header).map_err(|e| StorageError::IO {
        source: StorageIOError::<u64>::write_snapshot(None, AnyError::new(&e)),
    })?;

    for cf_name in STATE_CFS {
        let cf = storage.cf(cf_name).map_err(|e| StorageError::IO {
            source: StorageIOError::<u64>::write_snapshot(None, AnyError::new(&e)),
        })?;
        write_cf_start(file, cf_name).map_err(|e| StorageError::IO {
            source: StorageIOError::<u64>::write_snapshot(None, AnyError::new(&e)),
        })?;

        let iter = snap.iterator_cf_opt(&cf, ReadOptions::default(), IteratorMode::Start);
        for item in iter {
            let (key, value): (Box<[u8]>, Box<[u8]>) = item.map_err(|e| StorageError::IO {
                source: StorageIOError::<u64>::write_snapshot(None, AnyError::new(&e)),
            })?;

            write_kv(file, &key, &value).map_err(|e| StorageError::IO {
                source: StorageIOError::<u64>::write_snapshot(None, AnyError::new(&e)),
            })?;
        }

        write_cf_end(file).map_err(|e| StorageError::IO {
            source: StorageIOError::<u64>::write_snapshot(None, AnyError::new(&e)),
        })?;
    }

    write_end(file).map_err(|e| StorageError::IO {
        source: StorageIOError::<u64>::write_snapshot(None, AnyError::new(&e)),
    })?;

    file.sync_all().map_err(|e| StorageError::IO {
        source: StorageIOError::<u64>::write_snapshot(None, AnyError::new(&e)),
    })?;
    Ok(())
}

// openraft fixes this boundary to StorageError; boxing would add adapter churn.
#[allow(clippy::result_large_err)]
fn install_snapshot_payload(
    storage: &RocksDBStorage,
    meta: &SnapshotMeta<u64, MetadataNode>,
    mut file: std::fs::File,
) -> Result<SnapshotHeaderV1, StorageError<u64>> {
    file.seek(SeekFrom::Start(0)).map_err(|e| StorageError::IO {
        source: StorageIOError::<u64>::read_snapshot(Some(meta.signature()), AnyError::new(&e)),
    })?;
    let header = read_header(&mut file).map_err(|e| StorageError::IO {
        source: StorageIOError::<u64>::read_snapshot(Some(meta.signature()), AnyError::new(&e)),
    })?;

    storage
        .clear_cfs(STATE_CFS, SNAPSHOT_BATCH_BYTES)
        .map_err(|e| StorageError::IO {
            source: StorageIOError::<u64>::write_snapshot(Some(meta.signature()), AnyError::new(&e)),
        })?;

    let mut current_cf: Option<&ColumnFamily> = None;
    let mut batch = WriteBatch::default();

    loop {
        let tag = read_tag(&mut file).map_err(|e| StorageError::IO {
            source: StorageIOError::<u64>::read_snapshot(Some(meta.signature()), AnyError::new(&e)),
        })?;

        match tag {
            TAG_END => break,
            TAG_CF_START => {
                if !batch.is_empty() {
                    write_snapshot_batch(storage, batch, Some(meta.signature()))?;
                    batch = WriteBatch::default();
                }
                let name = read_cf_name(&mut file).map_err(|e| StorageError::IO {
                    source: StorageIOError::<u64>::read_snapshot(Some(meta.signature()), AnyError::new(&e)),
                })?;
                if !STATE_CFS.contains(&name.as_str()) {
                    return Err(StorageError::IO {
                        source: StorageIOError::<u64>::read_snapshot(
                            Some(meta.signature()),
                            AnyError::new(&MetadataError::Internal(format!("Unexpected CF {} in snapshot", name))),
                        ),
                    });
                }
                current_cf = Some(storage.cf(&name).map_err(|e| StorageError::IO {
                    source: StorageIOError::<u64>::read_snapshot(Some(meta.signature()), AnyError::new(&e)),
                })?);
            }
            TAG_KV => {
                let cf = current_cf.ok_or_else(|| StorageError::IO {
                    source: StorageIOError::<u64>::read_snapshot(
                        Some(meta.signature()),
                        AnyError::new(&MetadataError::Internal(
                            "Snapshot entry without CF context".to_string(),
                        )),
                    ),
                })?;
                let (key, value) = read_kv(&mut file).map_err(|e| StorageError::IO {
                    source: StorageIOError::<u64>::read_snapshot(Some(meta.signature()), AnyError::new(&e)),
                })?;
                batch.put_cf(cf, key, value);
                if batch.size_in_bytes() >= SNAPSHOT_BATCH_BYTES {
                    write_snapshot_batch(storage, batch, Some(meta.signature()))?;
                    batch = WriteBatch::default();
                }
            }
            TAG_CF_END => {
                if !batch.is_empty() {
                    write_snapshot_batch(storage, batch, Some(meta.signature()))?;
                    batch = WriteBatch::default();
                }
                current_cf = None;
            }
            _ => {
                return Err(StorageError::IO {
                    source: StorageIOError::<u64>::read_snapshot(
                        Some(meta.signature()),
                        AnyError::new(&MetadataError::Internal(format!("Unknown snapshot tag {}", tag))),
                    ),
                });
            }
        }
    }

    if !batch.is_empty() {
        write_snapshot_batch(storage, batch, Some(meta.signature()))?;
    }

    Ok(header)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mount::MountTable;
    use crate::raft::state_machine::AppRaftStateMachine;
    use crate::state::RouteEpoch;
    use openraft::storage::RaftSnapshotBuilder;
    use openraft::LeaderId;
    use std::io::Cursor;
    use tempfile::TempDir;

    fn sample_raft_state() -> AppMetadataRaftState {
        AppMetadataRaftState {
            last_applied_log_id: Some(LogId::new(LeaderId::new(1, 1), 5)),
            last_purged_log_id: None,
            vote: None,
            committed: None,
            membership: openraft::Membership::new(vec![], None),
        }
    }

    #[test]
    fn snapshot_header_v1_is_current_format_without_extra_fields() {
        let mut cursor = Cursor::new(Vec::new());
        write_header(&mut cursor, &SnapshotHeaderV1 { route_epoch: 7 }).unwrap();

        let bytes = cursor.into_inner();
        assert_eq!(&bytes[..4], SNAPSHOT_MAGIC);
        assert_eq!(bytes[4], SNAPSHOT_VERSION_V1);
        assert_eq!(bytes.len(), SNAPSHOT_MAGIC.len() + 1 + 8);

        let mut cursor = Cursor::new(bytes);
        let header = read_header(&mut cursor).unwrap();
        assert_eq!(header.route_epoch, 7);
    }

    #[tokio::test]
    async fn snapshot_round_trip_rebuilds_state() {
        let dir_a = TempDir::new().unwrap();
        let dir_b = TempDir::new().unwrap();

        let storage_a = Arc::new(RocksDBStorage::open(dir_a.path()).unwrap());
        let storage_b = Arc::new(RocksDBStorage::open(dir_b.path()).unwrap());

        let mount_table = Arc::new(MountTable::new());
        let sm_a = Arc::new(AppRaftStateMachine::new(
            Arc::clone(&storage_a),
            Arc::clone(&mount_table),
        ));

        storage_a.put_route_epoch(RouteEpoch::new(7)).unwrap();
        storage_a.set_next_inode_id(types::fs::InodeId::new(77)).unwrap();

        // Write a simple entry into another CF to ensure multi-CF round-trip.
        let cf = storage_a.cf("block_ref_counts").unwrap();
        storage_a
            .db()
            .put_cf(
                cf,
                b"block1",
                bincode::serde::encode_to_vec(1234u64, bincode::config::standard()).unwrap(),
            )
            .unwrap();

        // Persist raft state for meta.
        let raft_state = sample_raft_state();
        storage_a
            .put_raft_state(&serde_json::to_vec(&raft_state).unwrap())
            .unwrap();
        let raft_state_lock = Arc::new(RwLock::new(raft_state));

        let mut sm_store_a =
            StateMachineStorage::new(Arc::clone(&storage_a), Arc::clone(&sm_a), raft_state_lock).unwrap();
        let mut builder = sm_store_a.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.unwrap();
        println!("built snapshot {}", snapshot.meta.snapshot_id);

        // Install into a fresh store.
        let sm_b = Arc::new(AppRaftStateMachine::new(
            Arc::clone(&storage_b),
            Arc::clone(&mount_table),
        ));
        let raft_state_b = Arc::new(RwLock::new(AppMetadataRaftState::default()));
        let mut sm_store_b =
            StateMachineStorage::new(Arc::clone(&storage_b), Arc::clone(&sm_b), Arc::clone(&raft_state_b)).unwrap();

        sm_store_b
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .unwrap();
        println!("installed snapshot");

        // Validate data restored.
        println!("checking block ref count");
        let cf_b = storage_b.cf("block_ref_counts").unwrap();
        let raw = storage_b.db().get_cf(cf_b, b"block1").unwrap().unwrap();
        let decoded: u64 = decode_u64(&raw).unwrap();
        assert_eq!(decoded, 1234);
        assert_eq!(
            storage_b.get_next_inode_id().unwrap(),
            Some(types::fs::InodeId::new(77))
        );
        assert_eq!(storage_b.allocate_inode_id().unwrap(), types::fs::InodeId::new(77));

        // get_current_snapshot should return the just-built snapshot.
        println!("loading current snapshot");
        let mut sm_store_b2 =
            StateMachineStorage::new(Arc::clone(&storage_b), Arc::clone(&sm_b), Arc::clone(&raft_state_b)).unwrap();
        let current = sm_store_b2.get_current_snapshot().await.unwrap();
        assert!(current.is_some());
        let current_meta = current.unwrap().meta;
        assert_eq!(current_meta.snapshot_id, snapshot.meta.snapshot_id);
        println!("current snapshot verified");
    }
}
