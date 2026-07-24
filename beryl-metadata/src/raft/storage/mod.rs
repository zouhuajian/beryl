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
pub(crate) const ROCKSDB_SCHEMA_VERSION: u64 = 8;
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
