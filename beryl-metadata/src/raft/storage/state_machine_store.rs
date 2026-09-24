// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! RocksDB-backed Raft state machine store (openraft `RaftStateMachine` + snapshot I/O).

use super::{
    durable_raft_write_options, CF_META, CF_RAFT_SNAPSHOT, CF_RAFT_STATE, RAFT_STATE_KEY, STORAGE_IDENTITY_KEY,
};
use crate::error::{MetadataError, MetadataResult};
use crate::observe;
use crate::raft::response::{ApplySuccess, RaftApplyResult};
use crate::raft::storage::snapshot::{
    is_node_local_meta_key, snapshot_file_in_use, SnapshotCodecError, SnapshotFile, SnapshotIdentity, SnapshotWriter,
};
use crate::raft::storage::{RocksDBStorage, StorageIdentity, STATE_CFS};
use crate::raft::types::{from_openraft_log_id, AppMetadataRaftState, MetadataNode, MetadataRaftTypeConfig};
use crate::raft::{AppRaftStateMachine, MetadataReadView};
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, SnapshotSignature};
use openraft::{
    AnyError, Entry, EntryPayload, LogId, OptionalSend, RaftLogId, RaftTypeConfig, Snapshot, SnapshotMeta,
    StorageError, StorageIOError, StoredMembership,
};
use parking_lot::RwLock;
use rocksdb::{IteratorMode, ReadOptions, Snapshot as DbSnapshot, WriteBatch, DB};
use sha2::{Digest, Sha256};
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinError;
use tokio_util::task::task_tracker::TaskTrackerToken;
use tracing::info;
use uuid::Uuid;

const SNAPSHOT_META_KEY: &[u8] = b"snapshot_meta";

/// Bridges openraft state machine callbacks to the application state machine and RocksDB.
pub(crate) struct StateMachineStorage {
    storage: Arc<RocksDBStorage>,
    state_machine: AppRaftStateMachine,
    state: Arc<RwLock<AppMetadataRaftState>>,
    read_view: Arc<MetadataReadView>,
    // Drop after the storage owners so shutdown observes their release.
    storage_task: TaskTrackerToken,
}

impl StateMachineStorage {
    pub(crate) fn new_with_tracker(
        storage: Arc<RocksDBStorage>,
        state_machine: AppRaftStateMachine,
        state: Arc<RwLock<AppMetadataRaftState>>,
        read_view: Arc<MetadataReadView>,
        storage_task: TaskTrackerToken,
    ) -> MetadataResult<Self> {
        clean_stale_snapshot_tmp(&storage)?;
        let current_snapshot = current_snapshot_path(&storage)?;
        cleanup_obsolete_snapshot_files(&storage, current_snapshot.as_deref())?;

        Ok(Self {
            storage,
            state_machine,
            state,
            read_view,
            storage_task,
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
        let membership = state.membership.clone();
        Ok((last_applied, membership))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<RaftApplyResult>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<MetadataRaftTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut results = Vec::new();

        for entry in entries {
            let log_id = *entry.get_log_id();
            let apply_started = Instant::now();

            match entry.payload {
                EntryPayload::Normal(cmd) => {
                    let mut current = self.state.write();
                    let mut next = current.clone();
                    next.last_applied_log_id = Some(log_id);
                    let applied = match self.state_machine.apply_committed(cmd, &next) {
                        Ok(result) => result,
                        Err(e) => {
                            observe::record_raft_apply(
                                "error",
                                observe::metadata_error_kind(e.as_inner()),
                                apply_started.elapsed().as_secs_f64(),
                            );
                            return Err(StorageError::IO {
                                source: StorageIOError::<u64>::apply(log_id, AnyError::new(&e)),
                            });
                        }
                    };
                    // Publish routing before making its applied index visible.
                    if let Ok(ApplySuccess::MountUpserted(entry)) = &applied {
                        self.read_view.publish_root(entry.clone());
                    }
                    *current = next;

                    results.push(applied);
                    observe::record_raft_apply("ok", "none", apply_started.elapsed().as_secs_f64());
                }
                payload @ (EntryPayload::Membership(_) | EntryPayload::Blank) => {
                    let mut current = self.state.write();
                    let mut next = current.clone();
                    if let EntryPayload::Membership(membership) = payload {
                        next.membership = StoredMembership::new(Some(log_id), membership);
                    }
                    next.last_applied_log_id = Some(log_id);
                    if let Err(e) = self.storage.commit_applied_state(&next) {
                        observe::record_raft_apply("error", "storage", apply_started.elapsed().as_secs_f64());
                        return Err(StorageError::IO {
                            source: StorageIOError::<u64>::apply(log_id, AnyError::new(&e)),
                        });
                    }
                    *current = next;

                    results.push(Ok(ApplySuccess::RaftEntryApplied));
                    observe::record_raft_apply("ok", "none", apply_started.elapsed().as_secs_f64());
                }
            }
        }

        Ok(results)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        AppSnapshotBuilder {
            storage: Arc::clone(&self.storage),
            storage_task: self.storage_task.clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<<MetadataRaftTypeConfig as RaftTypeConfig>::SnapshotData>, StorageError<u64>> {
        Err(snapshot_write_error(
            None,
            &MetadataError::NotSupported("metadata peer snapshot transfer is unsupported".into()),
        ))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, MetadataNode>,
        _snapshot: Box<<MetadataRaftTypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<u64>> {
        Err(snapshot_write_error(
            Some(meta.signature()),
            &MetadataError::NotSupported("metadata peer snapshot transfer is unsupported".into()),
        ))
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
            let error = MetadataError::Internal(format!(
                "snapshot metadata {} references missing file {}",
                meta.snapshot_id,
                path.display()
            ));
            return Err(snapshot_read_error(Some(meta.signature()), &error));
        }

        let file = SnapshotFile::open_read(path).await.map_err(|e| StorageError::IO {
            source: StorageIOError::<u64>::read_snapshot(Some(meta.signature()), AnyError::new(&e)),
        })?;

        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(file),
        }))
    }
}

/// Snapshot builder for Raft.
pub(crate) struct AppSnapshotBuilder {
    storage: Arc<RocksDBStorage>,
    // Detached builders must release their storage owners before shutdown completes.
    storage_task: TaskTrackerToken,
}

impl RaftSnapshotBuilder<MetadataRaftTypeConfig> for AppSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<MetadataRaftTypeConfig>, StorageError<u64>> {
        let started = Instant::now();
        let storage = Arc::clone(&self.storage);
        let built = self
            .storage_task
            .task_tracker()
            .spawn_blocking(move || build_snapshot_generation(&storage))
            .await;
        let built = match built {
            Ok(Ok(built)) => built,
            Ok(Err(error)) => {
                observe::record_raft_snapshot("build", "generation", "error", 0, started.elapsed().as_secs_f64());
                return Err(snapshot_write_error(None, &error));
            }
            Err(error) => {
                observe::record_raft_snapshot("build", "join", "error", 0, started.elapsed().as_secs_f64());
                return Err(snapshot_join_error(None, error));
            }
        };
        observe::record_raft_snapshot("build", "complete", "ok", built.bytes, started.elapsed().as_secs_f64());
        info!(
            snapshot_id = %built.meta.snapshot_id,
            last_log = ?built.meta.last_log_id,
            bytes = built.bytes,
            elapsed_ms = started.elapsed().as_millis(),
            "Built snapshot"
        );

        let file_for_send = SnapshotFile::open_read(built.path)
            .await
            .map_err(|e| StorageError::IO {
                source: StorageIOError::<u64>::read_snapshot(Some(built.meta.signature()), AnyError::new(&e)),
            })?;

        Ok(Snapshot {
            meta: built.meta,
            snapshot: Box::new(file_for_send),
        })
    }
}

fn snapshot_file_path(storage: &RocksDBStorage, snapshot_id: &str) -> PathBuf {
    storage
        .snapshot_dir()
        .join(format!("snapshot-{}.snap", snapshot_id_hash(snapshot_id)))
}

fn temp_snapshot_path(storage: &RocksDBStorage, snapshot_id: &str) -> PathBuf {
    storage
        .snapshot_dir()
        .join(format!("snapshot-{}.snap.tmp", snapshot_id_hash(snapshot_id)))
}

fn snapshot_id_hash(snapshot_id: &str) -> String {
    let digest = Sha256::digest(snapshot_id.as_bytes());
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
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
                fs::remove_file(&path).map_err(|error| {
                    MetadataError::Internal(format!(
                        "failed to remove stale snapshot temp file {}: {error}",
                        path.display()
                    ))
                })?;
                observe::record_raft_storage_cleanup("stale_snapshot", 1);
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

struct SnapshotArtifact {
    meta: SnapshotMeta<u64, MetadataNode>,
    path: PathBuf,
    bytes: u64,
}

fn build_snapshot_generation(storage: &RocksDBStorage) -> MetadataResult<SnapshotArtifact> {
    storage.with_snapshot(|db, snapshot| {
        let raft_state = load_raft_state_from_snapshot(db, snapshot)?;
        let storage_identity = load_storage_identity_from_snapshot(db, snapshot)?;
        let snapshot_id = format_snapshot_id(raft_state.last_applied_log_id);
        let meta = SnapshotMeta {
            last_log_id: raft_state.last_applied_log_id,
            last_membership: raft_state.membership.clone(),
            snapshot_id,
        };
        let identity =
            SnapshotIdentity::current(storage_identity.group_name, meta.last_log_id.map(from_openraft_log_id));
        let temporary_path = temp_snapshot_path(storage, &meta.snapshot_id);
        let final_path = snapshot_file_path(storage, &meta.snapshot_id);

        let result = write_snapshot(db, snapshot, &identity, &temporary_path).and_then(|()| {
            if final_path.exists() {
                return Err(MetadataError::Internal(format!(
                    "snapshot path already exists: {}",
                    final_path.display()
                )));
            }
            fs::rename(&temporary_path, &final_path).map_err(|error| {
                MetadataError::Internal(format!("publish snapshot {}: {error}", final_path.display()))
            })?;
            sync_directory(storage.snapshot_dir())?;
            persist_snapshot_meta(db, &meta)?;
            cleanup_obsolete_snapshot_files(storage, Some(&final_path))?;
            let bytes = fs::metadata(&final_path)
                .map_err(|error| MetadataError::Internal(format!("stat snapshot {}: {error}", final_path.display())))?
                .len();
            Ok(SnapshotArtifact {
                meta,
                path: final_path,
                bytes,
            })
        });
        if result.is_err() && temporary_path.exists() {
            let _ = fs::remove_file(&temporary_path);
        }
        result
    })
}

fn write_snapshot(db: &DB, snapshot: &DbSnapshot<'_>, identity: &SnapshotIdentity, path: &Path) -> MetadataResult<()> {
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|error| MetadataError::Internal(format!("create snapshot {}: {error}", path.display())))?;
    let mut writer = SnapshotWriter::new(BufWriter::new(file), identity).map_err(local_codec_error)?;
    for cf_name in STATE_CFS {
        let cf = RocksDBStorage::cf(db, cf_name)?;
        writer.start_column_family(cf_name).map_err(local_codec_error)?;
        for item in snapshot.iterator_cf_opt(cf, ReadOptions::default(), IteratorMode::Start) {
            let (key, value) = item
                .map_err(|error| MetadataError::Internal(format!("read {cf_name} while building snapshot: {error}")))?;
            if !is_node_local_meta_key(cf_name, &key) {
                writer.write_record(&key, &value).map_err(local_codec_error)?;
            }
        }
        writer.end_column_family().map_err(local_codec_error)?;
    }
    let buffer = writer.finish().map_err(local_codec_error)?;
    let file = buffer
        .into_inner()
        .map_err(|error| MetadataError::Internal(format!("flush snapshot {}: {error}", path.display())))?;
    file.sync_all()
        .map_err(|error| MetadataError::Internal(format!("sync snapshot {}: {error}", path.display())))
}

fn load_raft_state_from_snapshot(db: &DB, snapshot: &DbSnapshot<'_>) -> MetadataResult<AppMetadataRaftState> {
    let cf = RocksDBStorage::cf(db, CF_RAFT_STATE)?;
    match snapshot
        .get_cf_opt(cf, RAFT_STATE_KEY, ReadOptions::default())
        .map_err(|error| MetadataError::Internal(format!("read raft state for snapshot: {error}")))?
    {
        Some(bytes) => serde_json::from_slice(&bytes)
            .map_err(|error| MetadataError::Internal(format!("decode raft state for snapshot: {error}"))),
        None => Ok(AppMetadataRaftState::default()),
    }
}

fn load_storage_identity_from_snapshot(db: &DB, snapshot: &DbSnapshot<'_>) -> MetadataResult<StorageIdentity> {
    let cf = RocksDBStorage::cf(db, CF_META)?;
    let bytes = snapshot
        .get_cf_opt(cf, STORAGE_IDENTITY_KEY, ReadOptions::default())
        .map_err(|error| MetadataError::Internal(format!("read storage identity for snapshot: {error}")))?
        .ok_or_else(|| MetadataError::InvalidArgument("storage identity is missing".to_string()))?;
    bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
        .map(|decoded: (StorageIdentity, usize)| decoded.0)
        .map_err(|error| MetadataError::InvalidArgument(format!("invalid storage identity: {error}")))
}

fn persist_snapshot_meta(db: &DB, meta: &SnapshotMeta<u64, MetadataNode>) -> MetadataResult<()> {
    let cf = RocksDBStorage::cf(db, CF_RAFT_SNAPSHOT)?;
    let bytes = serde_json::to_vec(meta)
        .map_err(|error| MetadataError::Internal(format!("encode snapshot metadata: {error}")))?;
    db.put_cf_opt(cf, SNAPSHOT_META_KEY, bytes, &durable_raft_write_options())
        .map_err(|error| MetadataError::Internal(format!("persist snapshot metadata: {error}")))
}

fn sync_directory(path: &Path) -> MetadataResult<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| MetadataError::Internal(format!("sync directory {}: {error}", path.display())))
}

fn current_snapshot_path(storage: &RocksDBStorage) -> MetadataResult<Option<PathBuf>> {
    let Some(bytes) = storage.get_snapshot_meta()? else {
        return Ok(None);
    };
    let meta: SnapshotMeta<u64, MetadataNode> = serde_json::from_slice(&bytes)
        .map_err(|error| MetadataError::InvalidArgument(format!("invalid current snapshot metadata: {error}")))?;
    let path = snapshot_file_path(storage, &meta.snapshot_id);
    if !path.is_file() {
        return Err(MetadataError::InvalidArgument(format!(
            "current snapshot file is missing at {}",
            path.display()
        )));
    }
    Ok(Some(path))
}

fn cleanup_obsolete_snapshot_files(storage: &RocksDBStorage, current: Option<&Path>) -> MetadataResult<()> {
    let directory = storage.snapshot_dir();
    for entry in
        fs::read_dir(directory).map_err(|error| MetadataError::Internal(format!("list snapshot directory: {error}")))?
    {
        let entry = entry.map_err(|error| MetadataError::Internal(format!("read snapshot entry: {error}")))?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let is_complete_snapshot = name
            .strip_prefix("snapshot-")
            .and_then(|name| name.strip_suffix(".snap"))
            .is_some_and(|hash| hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit()));
        if !is_complete_snapshot || current == Some(path.as_path()) || snapshot_file_in_use(&path) {
            continue;
        }
        fs::remove_file(&path).map_err(|error| {
            MetadataError::Internal(format!("remove obsolete snapshot {}: {error}", path.display()))
        })?;
        observe::record_raft_storage_cleanup("obsolete_snapshot", 1);
    }
    Ok(())
}

fn local_codec_error(error: SnapshotCodecError) -> MetadataError {
    MetadataError::Internal(format!("failed to encode local snapshot: {error}"))
}

fn snapshot_read_error(signature: Option<SnapshotSignature<u64>>, error: &MetadataError) -> StorageError<u64> {
    StorageError::IO {
        source: StorageIOError::<u64>::read_snapshot(signature, AnyError::new(error)),
    }
}

#[allow(clippy::result_large_err)]
fn snapshot_write_error(signature: Option<SnapshotSignature<u64>>, error: &MetadataError) -> StorageError<u64> {
    StorageError::IO {
        source: StorageIOError::<u64>::write_snapshot(signature, AnyError::new(error)),
    }
}

#[allow(clippy::result_large_err)]
fn snapshot_join_error(signature: Option<SnapshotSignature<u64>>, error: JoinError) -> StorageError<u64> {
    StorageError::IO {
        source: StorageIOError::<u64>::write_snapshot(signature, AnyError::new(&error)),
    }
}

impl RocksDBStorage {
    pub(crate) fn commit_applied_state(&self, raft_state: &AppMetadataRaftState) -> MetadataResult<()> {
        self.commit_authority_batch(WriteBatch::default(), raft_state)
    }

    /// Get Raft state (vote, last_purged, etc.).
    pub(super) fn get_raft_state(&self) -> MetadataResult<Option<Vec<u8>>> {
        let db = self.db();
        let cf = Self::cf(db, CF_RAFT_STATE)?;

        match db.get_cf(cf, b"raft_state") {
            Ok(Some(value)) => Ok(Some(value)),
            Ok(None) => Ok(None),
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    pub(crate) fn load_raft_state(&self) -> MetadataResult<AppMetadataRaftState> {
        match self.get_raft_state()? {
            Some(state_data) => serde_json::from_slice(&state_data)
                .map_err(|e| MetadataError::Internal(format!("Failed to deserialize Raft state: {e}"))),
            None => Ok(AppMetadataRaftState::default()),
        }
    }

    /// Persist Raft protocol state before acknowledging OpenRaft.
    pub(crate) fn persist_raft_state_durable(&self, state: &AppMetadataRaftState) -> MetadataResult<()> {
        let db = self.db();
        let cf = Self::cf(db, CF_RAFT_STATE)?;
        let state_data = serde_json::to_vec(state)
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize Raft state: {e}")))?;

        db.put_cf_opt(cf, b"raft_state", state_data, &durable_raft_write_options())
            .map_err(|e| MetadataError::Internal(format!("Failed to durably persist Raft state: {e}")))
    }

    /// Get current snapshot metadata.
    pub(crate) fn get_snapshot_meta(&self) -> MetadataResult<Option<Vec<u8>>> {
        let db = self.db();
        let cf = Self::cf(db, CF_RAFT_SNAPSHOT)?;

        db.get_cf(cf, SNAPSHOT_META_KEY)
            .map_err(|error| MetadataError::Internal(format!("read snapshot metadata: {error}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mount::MountTable;
    use crate::raft::state_machine::AppRaftStateMachine;
    use crate::raft::Command;

    use beryl_types::ids::{InodeId, MountId};
    use beryl_types::GroupName;
    use metrics::{Counter, CounterFn, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
    use openraft::storage::RaftSnapshotBuilder;
    use openraft::LeaderId;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tempfile::TempDir;
    use tokio_util::task::TaskTracker;

    fn committed_apply_test_store() -> (TempDir, Arc<RocksDBStorage>, StateMachineStorage) {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let state_machine = AppRaftStateMachine::new(Arc::clone(&storage));
        let state = Arc::new(RwLock::new(AppMetadataRaftState::default()));
        let read_view = Arc::new(MetadataReadView::new(
            Arc::new(MountTable::default()),
            Arc::clone(&state),
        ));
        let store = StateMachineStorage::new(Arc::clone(&storage), state_machine, state, read_view).unwrap();
        (dir, storage, store)
    }

    fn normal_entry(index: u64, command: Command) -> Entry<MetadataRaftTypeConfig> {
        Entry {
            log_id: LogId::new(LeaderId::new(1, 1), index),
            payload: EntryPayload::Normal(command),
        }
    }

    #[tokio::test]
    async fn shutdown_drains_snapshot_storage_before_reopen() {
        let (dir, storage, mut store) = committed_apply_test_store();
        storage
            .bind_storage_identity(&test_storage_identity("shutdown", 1))
            .unwrap();
        store.apply([normal_entry(1, bootstrap_command())]).await.unwrap();
        let tasks = store.storage_task.task_tracker().clone();
        tasks.close();
        let mut builder = store.get_snapshot_builder().await;
        drop(store);
        let released = Arc::downgrade(&storage);
        drop(storage);
        let mut drained = Box::pin(tasks.wait());
        assert!(futures::poll!(drained.as_mut()).is_pending());
        let snapshot = builder.build_snapshot().await.unwrap();
        drop(snapshot);
        drop(builder);
        tokio::time::timeout(std::time::Duration::from_secs(5), drained)
            .await
            .unwrap();
        assert!(released.upgrade().is_none());
        let reopened = RocksDBStorage::open_existing_for_start(dir.path()).unwrap();
        assert!(reopened.get_snapshot_meta().unwrap().is_some());
    }

    fn bootstrap_command() -> Command {
        Command::BootstrapNamespace {
            proposed_at_ms: 1,
            group_name: GroupName::parse("root").unwrap(),
        }
    }

    fn sample_raft_state() -> AppMetadataRaftState {
        AppMetadataRaftState {
            last_applied_log_id: Some(LogId::new(LeaderId::new(1, 1), 5)),
            last_purged_log_id: None,
            vote: None,
            committed: None,
            membership: StoredMembership::default(),
        }
    }

    fn test_storage_identity(name: &str, node_id: u64) -> StorageIdentity {
        StorageIdentity {
            storage_uuid: name.to_string(),
            cluster_id: "test-cluster".to_string(),
            group_name: GroupName::parse("root").unwrap(),
            node_id,
            bootstrap_proposed_at_ms: 1,
        }
    }

    #[tokio::test]
    async fn committed_mount_is_published_before_applied_state_is_visible() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let routing = Arc::new(MountTable::default());
        let state = Arc::new(RwLock::new(AppMetadataRaftState::default()));
        let read_view = Arc::new(MetadataReadView::new(Arc::clone(&routing), Arc::clone(&state)));
        let state_machine = AppRaftStateMachine::new(Arc::clone(&storage));
        let mut store =
            StateMachineStorage::new(storage.clone(), state_machine, Arc::clone(&state), read_view).unwrap();
        store.apply([normal_entry(1, bootstrap_command())]).await.unwrap();

        assert_eq!(
            routing
                .get_mount(MountId::new(1))
                .expect("published route")
                .root_inode_id,
            crate::mount::ROOT_INODE_ID
        );
        assert_eq!(
            state.read().last_applied_log_id.expect("published applied state").index,
            1
        );
    }

    #[tokio::test]
    async fn codec_failure_does_not_advance_applied_state() {
        let (_dir, storage, mut store) = committed_apply_test_store();
        let parent_inode_id = InodeId::new(1);
        let mut inode_key = b"inode/".to_vec();
        inode_key.extend_from_slice(&parent_inode_id.to_be_bytes());
        storage
            .with_db(|db| {
                db.put_cf(RocksDBStorage::cf(db, "inodes")?, inode_key, b"not-json")
                    .map_err(|error| MetadataError::Internal(error.to_string()))
            })
            .unwrap();
        let command = Command::CreateDirectory {
            proposed_at_ms: crate::raft::proposal_timestamp_ms(),
            root_inode_id: parent_inode_id,
            components: vec!["child".to_string()],
            recursive: false,
        };

        assert!(store.apply([normal_entry(1, command)]).await.is_err());
        assert!(storage.load_raft_state().unwrap().last_applied_log_id.is_none());
    }

    #[tokio::test]
    async fn obsolete_snapshots_wait_for_open_readers_then_are_reclaimed() {
        let directory = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(directory.path()).unwrap());
        storage
            .bind_storage_identity(&test_storage_identity("snapshot-cleanup", 1))
            .unwrap();
        let raft_state = sample_raft_state();
        storage.persist_raft_state_durable(&raft_state).unwrap();
        let state = Arc::new(RwLock::new(raft_state));
        let read_view = Arc::new(MetadataReadView::new(
            Arc::new(MountTable::default()),
            Arc::clone(&state),
        ));
        let state_machine = AppRaftStateMachine::new(Arc::clone(&storage));
        let mut store = StateMachineStorage::new(Arc::clone(&storage), state_machine, state, read_view).unwrap();
        let mut builder = store.get_snapshot_builder().await;

        let first = builder.build_snapshot().await.unwrap();
        let second = builder.build_snapshot().await.unwrap();
        assert_eq!(complete_snapshot_count(&storage), 2);

        drop(first);
        let third = builder.build_snapshot().await.unwrap();
        assert_eq!(complete_snapshot_count(&storage), 2);

        drop(second);
        drop(third);
        let current = current_snapshot_path(&storage).unwrap();
        let recorder = CleanupRecorder::default();
        metrics::with_local_recorder(&recorder, || {
            cleanup_obsolete_snapshot_files(&storage, current.as_deref()).unwrap();
        });
        assert_eq!(complete_snapshot_count(&storage), 1);
        assert_eq!(recorder.cleanups.load(Ordering::Relaxed), 1);
    }

    #[derive(Default)]
    struct CleanupRecorder {
        cleanups: Arc<AtomicU64>,
    }

    impl Recorder for CleanupRecorder {
        fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
            if key.name() == crate::observe::METADATA_RAFT_STORAGE_CLEANUP_TOTAL {
                Counter::from_arc(Arc::new(CleanupCounter {
                    cleanups: Arc::clone(&self.cleanups),
                }))
            } else {
                Counter::noop()
            }
        }

        fn register_gauge(&self, _key: &Key, _metadata: &Metadata<'_>) -> Gauge {
            Gauge::noop()
        }

        fn register_histogram(&self, _key: &Key, _metadata: &Metadata<'_>) -> Histogram {
            Histogram::noop()
        }
    }

    struct CleanupCounter {
        cleanups: Arc<AtomicU64>,
    }

    impl CounterFn for CleanupCounter {
        fn increment(&self, value: u64) {
            self.cleanups.fetch_add(value, Ordering::Relaxed);
        }

        fn absolute(&self, value: u64) {
            self.cleanups.store(value, Ordering::Relaxed);
        }
    }

    fn complete_snapshot_count(storage: &RocksDBStorage) -> usize {
        fs::read_dir(storage.snapshot_dir())
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with("snapshot-") && name.ends_with(".snap"))
            })
            .count()
    }

    impl StateMachineStorage {
        pub(crate) fn new(
            storage: Arc<RocksDBStorage>,
            state_machine: AppRaftStateMachine,
            state: Arc<RwLock<AppMetadataRaftState>>,
            read_view: Arc<MetadataReadView>,
        ) -> MetadataResult<Self> {
            Self::new_with_tracker(storage, state_machine, state, read_view, TaskTracker::new().token())
        }
    }
}
