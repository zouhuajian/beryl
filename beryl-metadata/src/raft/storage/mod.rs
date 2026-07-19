// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! RocksDB authority storage and OpenRaft storage-v2 backend.
//!
//! Keyspace schema:
//! - mounts/{mount_id} -> MountEntry (serialized)
//! - route_epoch -> u64
//! - mount_epoch -> u64
//!
//! FS schema:
//! - inodes/{inode_id_be_fixed_width} -> Inode (serialized)
//!   - key: "inode/" + 8 bytes BE (u64)
//!   - value: Inode (bincode)
//! - dentries/{parent_inode_id_be_fixed_width}/{name_bytes} -> child_inode_id_be_fixed_width
//!   - key: "dentry/" + 8 bytes BE (parent_inode_id) + name_bytes (UTF-8, no null terminator)
//!   - value: 8 bytes BE (child_inode_id)
//!   - Note: Fixed-width encoding enables efficient iteration and comparison

mod generation;
mod log_store;
mod query;
mod schema;
mod snapshot;
mod state_machine_store;
mod transaction;

pub(crate) use generation::{GenerationHandle, GenerationWriteGuard, PinnedGeneration, StagedGeneration};
pub(crate) use log_store::AppLogStorage;
pub(crate) use snapshot::{SnapshotFile, SnapshotInstallTracker};
pub(crate) use state_machine_store::StateMachineStorage;

use crate::error::{MetadataError, MetadataResult};
use crate::mount::MountEntry;
use crate::raft::AppMetadataRaftState;
use crate::state::RouteEpoch;
use crate::worker::WorkerInfo;
use beryl_types::fs::{Inode, InodeId};
use beryl_types::ids::{DataHandleId, MountId, WorkerId};
use beryl_types::layout::FileLayout;
use beryl_types::GroupName;
use bincode::config::standard;
use bincode::serde::{decode_from_slice, encode_to_vec};
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, Options, WriteBatch, WriteOptions, DB};
use serde::{Deserialize, Serialize};
use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

type DentryPage = (Vec<(String, InodeId)>, Option<Vec<u8>>, bool);

/// Column family names for RocksDB.
const CF_MOUNTS: &str = "mounts";
const CF_WORKERS: &str = "workers";
/// Raft column families
const CF_META: &str = "meta"; // route_epoch, mount_epoch, file layouts, etc.
const CF_RAFT_LOG: &str = "raft_log"; // Raft log entries
const CF_RAFT_STATE: &str = "raft_state"; // Raft state (hard_state, membership)
const CF_RAFT_SNAPSHOT: &str = "raft_snapshot"; // Raft snapshots

const ROCKSDB_SCHEMA_VERSION_KEY: &[u8] = b"rocksdb_schema_version";
const STORAGE_IDENTITY_KEY: &[u8] = b"storage_identity";
pub(crate) const ROCKSDB_SCHEMA_VERSION: u64 = 7;
const NEXT_INODE_ID_KEY: &[u8] = b"next_inode_id";
const NEXT_DATA_HANDLE_ID_KEY: &[u8] = b"next_data_handle_id";

fn durable_raft_write_options() -> WriteOptions {
    let mut options = WriteOptions::default();
    options.disable_wal(false);
    options.set_sync(true);
    options
}

fn worker_key(group_name: &GroupName, worker_id: WorkerId) -> String {
    format!("{}/{}", group_name.as_str(), worker_id.as_raw())
}

// FS column families
const CF_INODES: &str = "inodes"; // inode/{inode_id_be} -> Inode
const CF_DENTRIES: &str = "dentries"; // dentry/{parent_inode_id_be}/{name} -> child_inode_id_be

/// Column families that hold replicated state to be snapshotted/restored.
pub const STATE_CFS: &[&str] = &[CF_MOUNTS, CF_WORKERS, CF_META, CF_INODES, CF_DENTRIES];

/// Durable identity binding between the lifecycle marker and its RocksDB state.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct StorageIdentity {
    pub storage_uuid: String,
    pub cluster_id: String,
    pub group_name: GroupName,
    pub node_id: u64,
    pub bootstrap_client_id: String,
    pub bootstrap_call_id: String,
    pub bootstrap_proposed_at_ms: u64,
}

/// One authoritative state-machine commit assembled before RocksDB publication.
#[derive(Default)]
pub(crate) struct AuthorityBatch(WriteBatch);

impl Deref for AuthorityBatch {
    type Target = WriteBatch;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for AuthorityBatch {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<WriteBatch> for AuthorityBatch {
    fn from(batch: WriteBatch) -> Self {
        Self(batch)
    }
}

/// Inode identity reserved by a read-only allocator preparation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct InodeAllocation {
    pub(crate) inode_id: InodeId,
    pub(crate) next_inode_id: InodeId,
}

/// One directory insertion in an atomic recursive mkdir mutation.
pub(crate) struct RecursiveMkdirEntry {
    pub(crate) parent_inode_id: InodeId,
    pub(crate) name: String,
    pub(crate) inode: Inode,
    pub(crate) updated_parent: Inode,
}

/// File identities reserved by a read-only allocator preparation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FileAllocation {
    pub(crate) inode: InodeAllocation,
    pub(crate) data_handle_id: DataHandleId,
    next_data_handle_id: DataHandleId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BootstrapNamespaceState {
    Empty,
    Matching,
    Conflicting,
}

/// Overwritten rename target state that must be removed with the namespace move.
pub(crate) struct RenameOverwriteCleanup {
    pub inode_id: InodeId,
    pub data_handle_id: Option<DataHandleId>,
}

/// Namespace rename writes that must commit as one RocksDB batch.
pub(crate) struct RenameAtomicUpdate<'a> {
    pub src_parent_inode_id: InodeId,
    pub src_name: &'a str,
    pub dst_parent_inode_id: InodeId,
    pub dst_name: &'a str,
    pub src_inode_id: InodeId,
    pub overwritten_target: Option<RenameOverwriteCleanup>,
    pub updated_src_parent: Option<&'a Inode>,
    pub updated_dst_parent: Option<&'a Inode>,
    pub updated_src_inode: &'a Inode,
}

/// One namespace entry removed by a post-order recursive delete plan.
pub(crate) struct DeleteTreeEntry {
    pub parent_inode_id: InodeId,
    pub name: String,
    pub inode_id: InodeId,
    pub data_handle_id: Option<DataHandleId>,
    pub layout: Option<FileLayout>,
}

/// Recursive delete writes that must commit as one RocksDB batch.
pub(crate) struct DeleteTreeAtomicUpdate<'a> {
    pub entries: &'a [DeleteTreeEntry],
    pub updated_parent: &'a Inode,
}

/// RocksDB storage backend.
pub(crate) struct RocksDBStorage {
    generations: GenerationHandle,
}

impl RocksDBStorage {
    /// Encode inode key: "inode/" + 8 bytes BE (inode_id)
    fn encode_inode_key(inode_id: InodeId) -> Vec<u8> {
        let mut key = b"inode/".to_vec();
        key.extend_from_slice(&inode_id.to_be_bytes());
        key
    }

    /// Encode dentry key: "dentry/" + 8 bytes BE (parent_inode_id) + name_bytes
    fn encode_dentry_key(parent_inode_id: InodeId, name: &str) -> Vec<u8> {
        let mut key = b"dentry/".to_vec();
        key.extend_from_slice(&parent_inode_id.to_be_bytes());
        key.extend_from_slice(name.as_bytes());
        key
    }

    fn cf<'a>(db: &'a DB, name: &str) -> MetadataResult<&'a ColumnFamily> {
        db.cf_handle(name)
            .ok_or_else(|| MetadataError::Internal(format!("Column family {} not found", name)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::{format_metadata_storage, prepare_metadata_start};
    use crate::mount::{DataIoPolicy, MountKind, MountTable, ROOT_INODE_ID, ROOT_MOUNT_PREFIX};
    use crate::MetadataConfig;
    use beryl_types::ids::BlockId;
    use beryl_types::GroupName;
    use openraft::{LeaderId, LogId};
    use tempfile::TempDir;

    #[test]
    fn opening_existing_schema_v1_store_requires_reformat() {
        let dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(dir.path()).unwrap();
        storage
            .with_pinned_db(|db| {
                let meta = db.cf_handle(CF_META).unwrap();
                db.delete_cf(meta, ROCKSDB_SCHEMA_VERSION_KEY).unwrap();
                Ok(())
            })
            .unwrap();
        drop(storage);

        let error = match RocksDBStorage::open_existing_for_start(dir.path()) {
            Ok(_) => panic!("schema v1 store must not open"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("schema version is missing"));
        assert!(error.to_string().contains("reformat metadata storage"));
    }

    #[test]
    fn opening_previous_command_schema_requires_reformat() {
        let dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(dir.path()).unwrap();
        drop(storage);

        let generation_path = dir.path().join("generations/gen-000001");
        let mut db = DB::open_cf_descriptors(&Options::default(), &generation_path, schema::cf_descriptors()).unwrap();
        db.create_cf("dedup", &Options::default()).unwrap();
        let dedup = db.cf_handle("dedup").unwrap();
        db.put_cf(dedup, b"old-call", b"old-result").unwrap();
        let meta = db.cf_handle(CF_META).unwrap();
        let previous = bincode::serde::encode_to_vec(6u64, bincode::config::standard()).unwrap();
        db.put_cf(meta, ROCKSDB_SCHEMA_VERSION_KEY, previous).unwrap();
        drop(db);

        let error = match RocksDBStorage::open_existing_for_start(dir.path()) {
            Ok(_) => panic!("schema 6 store must not open after the Command format change"),
            Err(error) => error,
        };

        assert!(
            error
                .to_string()
                .contains("unsupported RocksDB schema version 6; expected 7"),
            "unexpected startup error: {error}"
        );
        assert!(
            error.to_string().contains("reformat metadata storage"),
            "unexpected startup error: {error}"
        );

        let mut descriptors = schema::cf_descriptors();
        descriptors.push(ColumnFamilyDescriptor::new("dedup", Options::default()));
        let db = DB::open_cf_descriptors(&Options::default(), generation_path, descriptors).unwrap();
        let dedup = db.cf_handle("dedup").unwrap();
        assert_eq!(
            db.get_cf(dedup, b"old-call").unwrap().as_deref(),
            Some(&b"old-result"[..])
        );
    }

    #[test]
    fn format_resume_rejects_missing_schema_even_when_generation_is_pristine() {
        let dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(dir.path()).unwrap();
        storage
            .with_pinned_db(|db| {
                let meta = db.cf_handle(CF_META).unwrap();
                db.delete_cf(meta, ROCKSDB_SCHEMA_VERSION_KEY).unwrap();
                Ok(())
            })
            .unwrap();
        drop(storage);

        let error = match RocksDBStorage::create_for_format(dir.path()) {
            Ok(_) => panic!("schema-less generation must not resume"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("schema version is missing"));
    }

    #[test]
    fn format_resume_does_not_upgrade_missing_schema_with_authority_state() {
        let dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(dir.path()).unwrap();
        storage
            .put_inode(&Inode::new_dir(
                InodeId::new(1),
                beryl_types::FileAttrs::new(),
                MountId::new(1),
            ))
            .unwrap();
        storage
            .with_pinned_db(|db| {
                let meta = db.cf_handle(CF_META).unwrap();
                db.delete_cf(meta, ROCKSDB_SCHEMA_VERSION_KEY).unwrap();
                Ok(())
            })
            .unwrap();
        drop(storage);

        let error = match RocksDBStorage::create_for_format(dir.path()) {
            Ok(_) => panic!("non-empty schema-less store must not be upgraded"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("schema version is missing"));
    }

    use beryl_types::fs::{FileAttrs, Inode, InodeData, InodeId};
    use beryl_types::ids::MountId;

    #[test]
    #[ignore = "manual durability latency baseline; run with --release and --ignored"]
    fn raft_durable_append_and_apply_latency_baseline() {
        const SAMPLES: u64 = 50;

        let temp_dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(temp_dir.path()).unwrap();
        let append_started = std::time::Instant::now();
        for index in 1..=SAMPLES {
            storage.append_raft_logs_durable(&[(index, vec![0; 256])]).unwrap();
        }
        let append_elapsed = append_started.elapsed();

        let apply_started = std::time::Instant::now();
        for index in 1..=SAMPLES {
            let raft_state = AppMetadataRaftState {
                last_applied_log_id: Some(LogId::new(LeaderId::new(1, 1), index)),
                ..AppMetadataRaftState::default()
            };
            storage
                .commit_authority_batch(AuthorityBatch::default(), &raft_state)
                .unwrap();
        }
        let apply_elapsed = apply_started.elapsed();

        eprintln!(
            "raft durability baseline: sync_append_ns_per_op={}, apply_batch_ns_per_op={}",
            append_elapsed.as_nanos() / SAMPLES as u128,
            apply_elapsed.as_nanos() / SAMPLES as u128
        );
    }

    #[test]
    fn raft_log_batch_rejects_a_hole_before_writing() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(temp_dir.path()).unwrap();
        let entries = vec![(7, vec![7]), (9, vec![9])];

        assert!(storage.append_raft_logs_durable(&entries).is_err());
        assert_eq!(None, storage.get_raft_log(7).unwrap());
        assert_eq!(None, storage.get_raft_log(9).unwrap());
    }

    #[test]
    fn raft_log_truncate_removes_the_complete_suffix() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(temp_dir.path()).unwrap();
        storage
            .append_raft_logs_durable(&[(1, vec![1]), (2, vec![2]), (3, vec![3])])
            .unwrap();

        storage.truncate_raft_logs(2).unwrap();

        assert_eq!(Some(vec![1]), storage.get_raft_log(1).unwrap());
        assert_eq!(None, storage.get_raft_log(2).unwrap());
        assert_eq!(None, storage.get_raft_log(3).unwrap());
    }

    #[test]
    fn raft_log_purge_and_last_purged_state_commit_together() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(temp_dir.path()).unwrap();
        storage
            .append_raft_logs_durable(&[(1, vec![1]), (2, vec![2]), (3, vec![3])])
            .unwrap();
        let purged = LogId::new(LeaderId::new(2, 1), 2);
        let state = AppMetadataRaftState {
            last_purged_log_id: Some(purged),
            ..AppMetadataRaftState::default()
        };

        storage.purge_raft_logs_and_state(2, &state).unwrap();

        assert_eq!(None, storage.get_raft_log(1).unwrap());
        assert_eq!(None, storage.get_raft_log(2).unwrap());
        assert_eq!(Some(vec![3]), storage.get_raft_log(3).unwrap());
        let stored: AppMetadataRaftState = serde_json::from_slice(&storage.get_raft_state().unwrap().unwrap()).unwrap();
        assert_eq!(Some(purged), stored.last_purged_log_id);
    }

    #[test]
    fn test_inode_allocator_migrates_from_existing_inodes_and_persists() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("inode_allocator_db");
        {
            let storage = RocksDBStorage::create_for_format(&db_path).unwrap();
            storage
                .put_inode(&Inode::new_dir(InodeId::new(42), FileAttrs::new(), MountId::new(1)))
                .unwrap();

            let first = storage.allocate_inode_id().unwrap();
            let second = storage.allocate_inode_id().unwrap();
            assert_eq!(first, InodeId::new(43));
            assert_eq!(second, InodeId::new(44));
        }

        let reopened = RocksDBStorage::create_for_format(&db_path).unwrap();
        let third = reopened.allocate_inode_id().unwrap();
        assert_eq!(third, InodeId::new(45));
    }

    #[test]
    fn create_file_atomic_persists_namespace_and_data_handle_owner() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(temp_dir.path()).unwrap();

        let parent_inode_id = InodeId::new(10);
        let mut parent = Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1));
        storage.put_inode(&parent).unwrap();

        let inode_id = InodeId::new(11);
        let data_handle_id = DataHandleId::new(12);
        let inode = Inode::new_file(inode_id, FileAttrs::new(), parent.mount_id, data_handle_id);
        parent.attrs.update_mtime_ctime(100);
        let layout = FileLayout::new(4096, 4096, 1);

        storage
            .put_test_file_atomic(parent_inode_id, "file", &inode, &parent, layout)
            .unwrap();

        let stored_inode = storage.get_inode(inode_id).unwrap().unwrap();
        assert_eq!(stored_inode.data_handle_id, data_handle_id);
        assert_eq!(storage.get_dentry(parent_inode_id, "file").unwrap(), Some(inode_id));
        assert_eq!(storage.get_layout(inode_id).unwrap(), layout);
        assert_eq!(
            storage.get_inode_by_data_handle(data_handle_id).unwrap(),
            Some(inode_id)
        );
        assert_eq!(storage.get_inode(parent_inode_id).unwrap().unwrap().attrs.mtime_ms, 100);
    }

    #[test]
    fn delete_file_atomic_removes_namespace_and_data_owner() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(temp_dir.path()).unwrap();

        let parent_inode_id = InodeId::new(10);
        let mut parent = Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1));
        let inode_id = InodeId::new(11);
        let data_handle_id = DataHandleId::new(12);
        let inode = Inode::new_file(inode_id, FileAttrs::new(), parent.mount_id, data_handle_id);
        let layout = FileLayout::new(4096, 4096, 1);
        storage.put_inode(&parent).unwrap();
        storage
            .put_test_file_atomic(parent_inode_id, "file", &inode, &parent, layout)
            .unwrap();

        parent.attrs.update_mtime_ctime(200);
        storage
            .delete_file_atomic(
                parent_inode_id,
                "file",
                inode_id,
                Some(data_handle_id),
                &parent,
                &AppMetadataRaftState::default(),
            )
            .unwrap();

        assert_eq!(storage.get_dentry(parent_inode_id, "file").unwrap(), None);
        assert!(storage.get_inode(inode_id).unwrap().is_none());
        assert!(storage.get_layout(inode_id).is_err());
        assert_eq!(storage.get_inode_by_data_handle(data_handle_id).unwrap(), None);
    }

    #[test]
    fn delete_empty_dir_atomic_removes_namespace() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(temp_dir.path()).unwrap();

        let parent_inode_id = InodeId::new(20);
        let mut parent = Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1));
        let inode_id = InodeId::new(21);
        let inode = Inode::new_dir(inode_id, FileAttrs::new(), parent.mount_id);
        storage.put_inode(&parent).unwrap();
        storage
            .put_test_dir_atomic(parent_inode_id, "dir", &inode, &parent)
            .unwrap();

        parent.attrs.update_mtime_ctime(300);
        storage
            .delete_empty_dir_atomic(
                parent_inode_id,
                "dir",
                inode_id,
                &parent,
                &AppMetadataRaftState::default(),
            )
            .unwrap();

        assert_eq!(storage.get_dentry(parent_inode_id, "dir").unwrap(), None);
        assert!(storage.get_inode(inode_id).unwrap().is_none());
        assert_eq!(storage.get_inode(parent_inode_id).unwrap().unwrap().attrs.mtime_ms, 300);
    }

    #[test]
    fn put_inode_atomic_persists_inode_and_applied_state() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(temp_dir.path()).unwrap();

        let inode_id = InodeId::new(12);
        let mut inode = Inode::new_file(inode_id, FileAttrs::new(), MountId::new(1), DataHandleId::new(120));
        inode.attrs.uid = 44;
        storage
            .put_inode_atomic(&inode, &AppMetadataRaftState::default())
            .unwrap();

        assert_eq!(storage.get_inode(inode_id).unwrap().unwrap().attrs.uid, 44);
    }

    #[test]
    fn publish_file_atomic_persists_inode_layout_and_applied_state() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(temp_dir.path()).unwrap();

        let inode_id = InodeId::new(13);
        let data_handle_id = DataHandleId::new(130);
        let mut inode = Inode::new_file(inode_id, FileAttrs::new(), MountId::new(1), data_handle_id);
        let layout = FileLayout::new(4096, 4096, 1);
        let block_id = BlockId::new(data_handle_id, beryl_types::ids::BlockIndex::new(0));
        if let InodeData::File {
            extents,
            content_revision,
            lease_epoch,
        } = &mut inode.data
        {
            extents.push(beryl_types::fs::Extent {
                file_offset: 0,
                block_id,
                block_offset: 0,
                len: 64,
                content_revision: None,
                block_stamp: None,
            });
            *content_revision = Some(3);
            *lease_epoch = Some(3);
        }
        inode.attrs.size = 64;
        storage.put_layout(inode_id, layout).unwrap();

        storage
            .publish_file_atomic(&inode, layout, &AppMetadataRaftState::default())
            .unwrap();

        let stored = storage.get_inode(inode_id).unwrap().unwrap();
        assert_eq!(stored.attrs.size, 64);
        assert_eq!(storage.get_layout(inode_id).unwrap(), layout);
    }

    #[test]
    fn get_layout_rejects_invalid_persisted_value() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(temp_dir.path()).unwrap();
        let inode_id = InodeId::new(14);

        storage.put_layout(inode_id, FileLayout::new(4096, 0, 1)).unwrap();

        let error = storage
            .get_layout(inode_id)
            .expect_err("invalid persisted layout must fail closed");
        assert!(error.to_string().contains("Invalid file layout"));
    }

    #[test]
    fn create_dir_atomic_persists_inode_and_dentry() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(temp_dir.path()).unwrap();

        let parent_inode_id = InodeId::new(20);
        let mut parent = Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1));
        storage.put_inode(&parent).unwrap();

        let inode_id = InodeId::new(21);
        let inode = Inode::new_dir(inode_id, FileAttrs::new(), parent.mount_id);
        parent.attrs.update_mtime_ctime(200);

        storage
            .put_test_dir_atomic(parent_inode_id, "dir", &inode, &parent)
            .unwrap();

        assert!(storage.get_inode(inode_id).unwrap().unwrap().kind.is_dir());
        assert_eq!(storage.get_dentry(parent_inode_id, "dir").unwrap(), Some(inode_id));
        assert_eq!(storage.get_inode(parent_inode_id).unwrap().unwrap().attrs.mtime_ms, 200);
    }

    #[test]
    fn rename_atomic_moves_dentry_and_preserves_inode() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(temp_dir.path()).unwrap();

        let src_parent_id = InodeId::new(30);
        let dst_parent_id = InodeId::new(31);
        let inode_id = InodeId::new(32);
        let mut src_parent = Inode::new_dir(src_parent_id, FileAttrs::new(), MountId::new(1));
        let mut dst_parent = Inode::new_dir(dst_parent_id, FileAttrs::new(), MountId::new(1));
        let mut inode = Inode::new_file(inode_id, FileAttrs::new(), MountId::new(1), DataHandleId::new(33));

        storage.put_inode(&src_parent).unwrap();
        storage.put_inode(&dst_parent).unwrap();
        storage.put_inode(&inode).unwrap();
        storage.put_dentry(src_parent_id, "old", inode_id).unwrap();

        src_parent.attrs.update_mtime_ctime(300);
        dst_parent.attrs.update_mtime_ctime(300);
        inode.attrs.update_ctime(300);

        storage
            .rename_test_atomic(crate::raft::storage::RenameAtomicUpdate {
                src_parent_inode_id: src_parent_id,
                src_name: "old",
                dst_parent_inode_id: dst_parent_id,
                dst_name: "new",
                src_inode_id: inode_id,
                overwritten_target: None,
                updated_src_parent: Some(&src_parent),
                updated_dst_parent: Some(&dst_parent),
                updated_src_inode: &inode,
            })
            .unwrap();

        assert_eq!(storage.get_dentry(src_parent_id, "old").unwrap(), None);
        assert_eq!(storage.get_dentry(dst_parent_id, "new").unwrap(), Some(inode_id));
        assert!(storage.get_inode(inode_id).unwrap().is_some());
    }

    fn setup_dir_with_entries(parent_inode_id: InodeId, entries: &[(&str, InodeId)]) -> (TempDir, RocksDBStorage) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db_dentries");
        let storage = RocksDBStorage::create_for_format(&db_path).unwrap();

        // Create parent dir and some child nodes.
        let parent_inode = Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1));
        storage.put_inode(&parent_inode).unwrap();

        for (name, child) in entries {
            storage.put_dentry(parent_inode_id, name, *child).unwrap();
        }

        (temp_dir, storage)
    }

    #[test]
    fn test_list_dentries_with_cursor_pagination() {
        let entries = [("a", InodeId::new(2)), ("b", InodeId::new(3)), ("c", InodeId::new(4))];
        let (_tmp_dir, storage) = setup_dir_with_entries(InodeId::new(1), &entries);

        let (page1, cursor1, eof1) = storage
            .list_dentries_with_cursor(InodeId::new(1), None, Some(2))
            .unwrap();
        assert_eq!(
            page1,
            vec![("a".to_string(), InodeId::new(2)), ("b".to_string(), InodeId::new(3))]
        );
        assert!(cursor1.is_some());
        assert!(!eof1);

        // When continuing iteration using the returned cursor,
        // you should skip the last record that has already been returned.
        let (page2, cursor2, eof2) = storage
            .list_dentries_with_cursor(InodeId::new(1), cursor1.as_deref(), Some(10))
            .unwrap();
        assert_eq!(page2, vec![("c".to_string(), InodeId::new(4))]);
        assert!(eof2);
        assert!(cursor2.is_none());
    }

    #[test]
    fn test_list_dentries_with_cursor_ignores_off_prefix_cursor() {
        let entries = [("x", InodeId::new(11)), ("y", InodeId::new(12))];
        let (_tmp_dir, storage) = setup_dir_with_entries(InodeId::new(10), &entries);

        // The cursor lands under another directory prefix,
        // the expectation is to start from that directory prefix without skipping the first entry.
        let mut other_cursor = b"dentry/".to_vec();
        other_cursor.extend_from_slice(&InodeId::new(99).to_be_bytes());
        other_cursor.extend_from_slice(b"zzz");

        let (page, _cursor, eof) = storage
            .list_dentries_with_cursor(InodeId::new(10), Some(&other_cursor), Some(10))
            .unwrap();
        assert_eq!(
            page,
            vec![("x".to_string(), InodeId::new(11)), ("y".to_string(), InodeId::new(12))]
        );
        assert!(eof);
    }

    #[test]
    fn test_obsolete_cf_detection() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");

        // Create a RocksDB with obsolete "files" CF.
        {
            let mut opts = Options::default();
            opts.create_if_missing(true);
            opts.create_missing_column_families(true);

            let cfs = vec![
                ColumnFamilyDescriptor::new("files", Options::default()),
                ColumnFamilyDescriptor::new("blocks", Options::default()),
            ];

            let db = DB::open_cf_descriptors(&opts, &db_path, cfs).unwrap();
            // Write something to files CF to ensure it exists
            let files_cf = db.cf_handle("files").unwrap();
            db.put_cf(files_cf, b"test_key", b"test_value").unwrap();
        }

        // Try to open with new code; obsolete CF layouts must fail fast.
        let result = RocksDBStorage::create_for_format(&db_path);
        assert!(result.is_err(), "Opening DB with obsolete 'files' CF should fail");
        match result {
            Err(e) => {
                let error_msg = format!("{}", e);
                assert!(
                    error_msg.contains("invalid CURRENT")
                        || error_msg.contains("obsolete column family")
                        || error_msg.contains("files"),
                    "Error message should mention obsolete column family 'files', got: {}",
                    error_msg
                );
            }
            Ok(_) => panic!("Expected error but got Ok"),
        }
    }

    fn lifecycle_config(dir: &TempDir) -> MetadataConfig {
        MetadataConfig {
            storage_dir: dir.path().join("metadata"),
            ..MetadataConfig::default()
        }
    }

    #[tokio::test]
    async fn metadata_format_creates_root_namespace_through_raft_path() {
        let dir = TempDir::new().unwrap();
        let config = lifecycle_config(&dir);
        format_metadata_storage(&config).await.unwrap();

        prepare_metadata_start(&config).await.unwrap();

        let storage = RocksDBStorage::create_for_format(&config.storage_dir).unwrap();
        let mount_table = MountTable::load_from_storage(&storage).unwrap();
        assert!(storage.get_inode(ROOT_INODE_ID).unwrap().is_some());
        let mounts = mount_table.list_mounts();
        assert_eq!(mounts.len(), 1);
        let root = &mounts[0];
        assert_eq!(root.mount_id, MountId::new(1));
        assert_eq!(root.mount_prefix, ROOT_MOUNT_PREFIX);
        assert_eq!(root.mount_kind, MountKind::Internal);
        assert_eq!(root.data_io_policy, DataIoPolicy::Allow);
        assert_eq!(root.namespace_owner_group_name, GroupName::parse("root").unwrap());
    }

    #[tokio::test]
    async fn metadata_start_rejects_root_without_data_io() {
        let dir = TempDir::new().unwrap();
        let config = lifecycle_config(&dir);
        format_metadata_storage(&config).await.unwrap();
        let storage = RocksDBStorage::create_for_format(&config.storage_dir).unwrap();
        let mut root = MountTable::load_from_storage(&storage)
            .unwrap()
            .list_mounts()
            .into_iter()
            .find(|mount| mount.mount_prefix == ROOT_MOUNT_PREFIX)
            .expect("root mount after format");
        root.data_io_policy = DataIoPolicy::Forbid;
        storage.put_mount(&root).unwrap();
        drop(storage);

        let err = prepare_metadata_start(&config)
            .await
            .expect_err("root must be writable for data IO on start");
        let message = err.to_string();

        assert!(message.contains("root mount exists"), "{message}");
        assert!(message.contains("violates root invariants"), "{message}");
    }

    #[tokio::test]
    async fn metadata_start_accepts_root_attributes_changed_by_normal_namespace_mutation() {
        let dir = TempDir::new().unwrap();
        let config = lifecycle_config(&dir);
        format_metadata_storage(&config).await.unwrap();
        let storage = RocksDBStorage::create_for_format(&config.storage_dir).unwrap();
        let mut root = storage.get_inode(ROOT_INODE_ID).unwrap().unwrap();
        root.attrs.mtime_ms = root.attrs.mtime_ms.saturating_add(1);
        root.attrs.ctime_ms = root.attrs.ctime_ms.saturating_add(1);
        root.attrs.size = 4096;
        storage.put_inode(&root).unwrap();
        drop(storage);

        prepare_metadata_start(&config).await.unwrap();
    }
}
