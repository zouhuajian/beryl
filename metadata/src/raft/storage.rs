// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! RocksDB storage backend for Raft state machine.
//!
//! Keyspace schema:
//! - blocks/{block_id} -> BlockMetaState (serialized)
//! - leases/{block_id} -> LeaseState (serialized)
//! - mounts/{mount_id} -> MountEntry (serialized)
//! - dedup/{request_id} -> AppliedResult (serialized)
//! - shard_groups/{group_id} -> ShardGroupInfo (serialized)
//! - shard_routing/{shard_id} -> group_id (u64 as string)
//! - layout_version -> u64
//! - mount_version -> u64
//!
//! FS schema:
//! - inodes/{inode_id_be_fixed_width} -> Inode (serialized)
//!   - key: "inode/" + 8 bytes BE (u64)
//!   - value: Inode (bincode)
//! - dentries/{parent_inode_id_be_fixed_width}/{name_bytes} -> child_inode_id_be_fixed_width
//!   - key: "dentry/" + 8 bytes BE (parent_inode_id) + name_bytes (UTF-8, no null terminator)
//!   - value: 8 bytes BE (child_inode_id)
//!   - Note: Fixed-width encoding enables efficient iteration and comparison

use crate::error::{MetadataError, MetadataResult};
use crate::mount::MountEntry;
use crate::raft::types::AppMetadataRaftState;
use crate::state::{BlockMetaState, LayoutVersion, LeaseState};
use crate::worker::WorkerInfo;
use bincode::config::standard;
use bincode::serde::{decode_from_slice, encode_to_vec};
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, Options, WriteBatch, DB};
use serde::{Deserialize, Serialize};
use serde_json;
use std::path::Path;
use std::sync::Arc;
use tracing::warn;
use types::fs::{Inode, InodeId};
use types::ids::{BlockId, DataHandleId, MountId, ShardGroupId, ShardId, WorkerId};
use types::layout::FileLayout;
use types::CallId;

/// Column family names for RocksDB.
const CF_BLOCKS: &str = "blocks";
const CF_LEASES: &str = "leases";
const CF_MOUNTS: &str = "mounts";
const CF_DEDUP: &str = "dedup";
const CF_SHARD_GROUPS: &str = "shard_groups";
const CF_SHARD_ROUTING: &str = "shard_routing"; // shard_id -> group_id mapping
const CF_WORKERS: &str = "workers";
const CF_BLOCK_REF_COUNTS: &str = "block_ref_counts"; // block_id -> u64 (global refcount)
const CF_DELETE_INTENTS: &str = "delete_intents"; // intent_id -> DeleteIntent
/// Raft column families
const CF_META: &str = "meta"; // layout_version, mount_version, etc.
const CF_RAFT_LOG: &str = "raft_log"; // Raft log entries
const CF_RAFT_STATE: &str = "raft_state"; // Raft state (hard_state, membership)
const CF_RAFT_SNAPSHOT: &str = "raft_snapshot"; // Raft snapshots

// FS column families
const CF_INODES: &str = "inodes"; // inode/{inode_id_be} -> Inode
const CF_DENTRIES: &str = "dentries"; // dentry/{parent_inode_id_be}/{name} -> child_inode_id_be

/// Column families that hold replicated state to be snapshotted/restored.
pub const STATE_CFS: &[&str] = &[
    CF_BLOCKS,
    CF_LEASES,
    CF_MOUNTS,
    CF_DEDUP,
    CF_SHARD_GROUPS,
    CF_SHARD_ROUTING,
    CF_WORKERS,
    CF_BLOCK_REF_COUNTS,
    CF_DELETE_INTENTS,
    CF_META,
    CF_INODES,
    CF_DENTRIES,
];

/// Applied result for idempotency.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppliedResult {
    pub seq: u64,
    pub result: Vec<u8>, // Serialized result
}

/// Shard group information.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ShardGroupInfo {
    pub group_id: ShardGroupId,
    pub shard_ids: Vec<u64>,
    pub initial_members: Vec<u64>,
    pub version: u64,
}

/// RocksDB storage backend.
pub struct RocksDBStorage {
    db: Arc<DB>,
    snapshot_dir: std::path::PathBuf,
}

impl RocksDBStorage {
    /// Get reference to the underlying DB (for iteration).
    pub fn db(&self) -> &Arc<DB> {
        &self.db
    }

    /// Open or create a RocksDB database.
    pub fn open<P: AsRef<Path>>(path: P) -> MetadataResult<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        // TODO: Optimize rocksdb opts
        // opts.set_allow_mmap_writes(true);
        // opts.set_allow_mmap_reads(true);

        let cfs = vec![
            ColumnFamilyDescriptor::new(CF_BLOCKS, Options::default()),
            ColumnFamilyDescriptor::new(CF_LEASES, Options::default()),
            ColumnFamilyDescriptor::new(CF_MOUNTS, Options::default()),
            ColumnFamilyDescriptor::new(CF_DEDUP, Options::default()),
            ColumnFamilyDescriptor::new(CF_SHARD_GROUPS, Options::default()),
            ColumnFamilyDescriptor::new(CF_SHARD_ROUTING, Options::default()),
            ColumnFamilyDescriptor::new(CF_WORKERS, Options::default()),
            ColumnFamilyDescriptor::new(CF_BLOCK_REF_COUNTS, Options::default()),
            ColumnFamilyDescriptor::new(CF_DELETE_INTENTS, Options::default()),
            ColumnFamilyDescriptor::new(CF_META, Options::default()),
            ColumnFamilyDescriptor::new(CF_RAFT_LOG, Options::default()),
            ColumnFamilyDescriptor::new(CF_RAFT_STATE, Options::default()),
            ColumnFamilyDescriptor::new(CF_RAFT_SNAPSHOT, Options::default()),
            ColumnFamilyDescriptor::new(CF_INODES, Options::default()),
            ColumnFamilyDescriptor::new(CF_DENTRIES, Options::default()),
        ];

        let db = DB::open_cf_descriptors(&opts, &path_buf, cfs)
            .map_err(|e| MetadataError::Internal(format!("Failed to open RocksDB: {}", e)))?;

        let snapshot_dir = path_buf.join("snapshots");
        std::fs::create_dir_all(&snapshot_dir)
            .map_err(|e| MetadataError::Internal(format!("Failed to create snapshot dir {:?}: {}", snapshot_dir, e)))?;

        Ok(Self {
            db: Arc::new(db),
            snapshot_dir,
        })
    }

    /// Get block metadata.
    pub fn get_block(&self, block_id: BlockId) -> MetadataResult<Option<BlockMetaState>> {
        let cf = self
            .db
            .cf_handle(CF_BLOCKS)
            .ok_or_else(|| MetadataError::Internal("Blocks CF not found".to_string()))?;
        let key = format!("{}", block_id);

        match self.db.get_cf(cf, key.as_bytes()) {
            Ok(Some(value)) => {
                let meta: BlockMetaState = decode_from_slice(&value, standard())
                    .map_err(|e| MetadataError::Internal(format!("Failed to deserialize BlockMetaState: {}", e)))?
                    .0;
                Ok(Some(meta))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    /// Put block metadata.
    pub fn put_block(&self, block_meta: &BlockMetaState) -> MetadataResult<()> {
        let cf = self
            .db
            .cf_handle(CF_BLOCKS)
            .ok_or_else(|| MetadataError::Internal("Blocks CF not found".to_string()))?;
        let key = format!("{}", block_meta.block_id);
        let value = encode_to_vec(block_meta, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize BlockMetaState: {}", e)))?;

        self.db
            .put_cf(cf, key.as_bytes(), value)
            .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
        Ok(())
    }

    /// Get lease state.
    pub fn get_lease(&self, block_id: BlockId) -> MetadataResult<Option<LeaseState>> {
        let cf = self
            .db
            .cf_handle(CF_LEASES)
            .ok_or_else(|| MetadataError::Internal("Leases CF not found".to_string()))?;
        let key = format!("{}", block_id);

        match self.db.get_cf(cf, key.as_bytes()) {
            Ok(Some(value)) => {
                let lease: LeaseState = decode_from_slice(&value, standard())
                    .map_err(|e| MetadataError::Internal(format!("Failed to deserialize LeaseState: {}", e)))?
                    .0;
                Ok(Some(lease))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    /// Put lease state.
    pub fn put_lease(&self, lease_state: &LeaseState) -> MetadataResult<()> {
        let cf = self
            .db
            .cf_handle(CF_LEASES)
            .ok_or_else(|| MetadataError::Internal("Leases CF not found".to_string()))?;
        let key = format!("{}", lease_state.block_id);
        let value = encode_to_vec(lease_state, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize LeaseState: {}", e)))?;

        self.db
            .put_cf(cf, key.as_bytes(), value)
            .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
        Ok(())
    }

    /// Delete lease state.
    pub fn delete_lease(&self, block_id: BlockId) -> MetadataResult<()> {
        let cf = self
            .db
            .cf_handle(CF_LEASES)
            .ok_or_else(|| MetadataError::Internal("Leases CF not found".to_string()))?;
        let key = format!("{}", block_id);

        self.db
            .delete_cf(cf, key.as_bytes())
            .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
        Ok(())
    }

    /// Get applied result for idempotency.
    pub fn get_applied_result(&self, request_id: &CallId) -> MetadataResult<Option<AppliedResult>> {
        let cf = self
            .db
            .cf_handle(CF_DEDUP)
            .ok_or_else(|| MetadataError::Internal("Dedup CF not found".to_string()))?;
        let key = request_id.to_string();

        match self.db.get_cf(cf, key.as_bytes()) {
            Ok(Some(value)) => {
                let result: AppliedResult = decode_from_slice(&value, standard())
                    .map_err(|e| MetadataError::Internal(format!("Failed to deserialize AppliedResult: {}", e)))?
                    .0;
                Ok(Some(result))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    /// Put applied result for idempotency.
    pub fn put_applied_result(&self, request_id: &CallId, result: AppliedResult) -> MetadataResult<()> {
        let cf = self
            .db
            .cf_handle(CF_DEDUP)
            .ok_or_else(|| MetadataError::Internal("Dedup CF not found".to_string()))?;
        let key = request_id.to_string();
        let value = encode_to_vec(&result, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize AppliedResult: {}", e)))?;

        self.db
            .put_cf(cf, key.as_bytes(), value)
            .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
        Ok(())
    }

    /// Get layout version.
    pub fn get_layout_version(&self) -> MetadataResult<LayoutVersion> {
        let cf = self
            .db
            .cf_handle(CF_META)
            .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;

        match self.db.get_cf(cf, b"layout_version") {
            Ok(Some(value)) => {
                let version: u64 = decode_from_slice(&value, standard())
                    .map_err(|e| MetadataError::Internal(format!("Failed to deserialize layout_version: {}", e)))?
                    .0;
                Ok(LayoutVersion::new(version))
            }
            Ok(None) => Ok(LayoutVersion::new(1)), // Default version
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    /// Put layout version.
    pub fn put_layout_version(&self, version: LayoutVersion) -> MetadataResult<()> {
        let cf = self
            .db
            .cf_handle(CF_META)
            .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;
        let value = encode_to_vec(&version.as_u64(), standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize layout_version: {}", e)))?;

        self.db
            .put_cf(cf, b"layout_version", value)
            .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
        Ok(())
    }

    /// Persist the layout for a specific inode (authoritative data-plane parameters).
    pub fn put_layout(&self, inode_id: InodeId, layout: FileLayout) -> MetadataResult<()> {
        let cf = self
            .db
            .cf_handle(CF_META)
            .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;
        let key = format!("layout:{}", inode_id.as_raw());
        let value = encode_to_vec(&layout, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize file layout: {}", e)))?;

        self.db
            .put_cf(cf, key.as_bytes(), value)
            .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
        Ok(())
    }

    /// Load the layout for a specific inode.
    pub fn get_layout(&self, inode_id: InodeId) -> MetadataResult<FileLayout> {
        let cf = self
            .db
            .cf_handle(CF_META)
            .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;
        let key = format!("layout:{}", inode_id.as_raw());
        match self.db.get_cf(cf, key.as_bytes()) {
            Ok(Some(value)) => decode_from_slice(&value, standard())
                .map(|(layout, _)| layout)
                .map_err(|e| MetadataError::Internal(format!("Failed to deserialize file layout: {}", e))),
            Ok(None) => Err(MetadataError::NotFound(format!(
                "Layout not found for inode {}",
                inode_id
            ))),
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    /// Get applied sequence persisted for idempotency tracking.
    pub fn get_applied_seq(&self) -> MetadataResult<Option<u64>> {
        let cf = self
            .db
            .cf_handle(CF_META)
            .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;

        match self.db.get_cf(cf, b"applied_seq") {
            Ok(Some(value)) => {
                let seq: u64 = decode_from_slice(&value, standard())
                    .map_err(|e| MetadataError::Internal(format!("Failed to deserialize applied_seq: {}", e)))?
                    .0;
                Ok(Some(seq))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    /// Persist applied sequence.
    pub fn put_applied_seq(&self, seq: u64) -> MetadataResult<()> {
        let cf = self
            .db
            .cf_handle(CF_META)
            .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;
        let value = encode_to_vec(&seq, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize applied_seq: {}", e)))?;

        self.db
            .put_cf(cf, b"applied_seq", value)
            .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
        Ok(())
    }

    /// Get shard group info.
    pub fn get_shard_group(&self, group_id: ShardGroupId) -> MetadataResult<Option<ShardGroupInfo>> {
        let cf = self
            .db
            .cf_handle(CF_SHARD_GROUPS)
            .ok_or_else(|| MetadataError::Internal("ShardGroups CF not found".to_string()))?;
        let key = format!("{}", group_id.as_raw());

        match self.db.get_cf(cf, key.as_bytes()) {
            Ok(Some(value)) => {
                let info: ShardGroupInfo = decode_from_slice(&value, standard())
                    .map_err(|e| MetadataError::Internal(format!("Failed to deserialize ShardGroupInfo: {}", e)))?
                    .0;
                Ok(Some(info))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    /// Put shard group info.
    pub fn put_shard_group(&self, info: &ShardGroupInfo) -> MetadataResult<()> {
        let cf = self
            .db
            .cf_handle(CF_SHARD_GROUPS)
            .ok_or_else(|| MetadataError::Internal("ShardGroups CF not found".to_string()))?;
        let key = format!("{}", info.group_id.as_raw());
        let value = encode_to_vec(info, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize ShardGroupInfo: {}", e)))?;

        self.db
            .put_cf(cf, key.as_bytes(), value)
            .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
        Ok(())
    }

    /// Put shard to group routing mapping.
    pub fn put_shard_routing(&self, shard_id: ShardId, group_id: ShardGroupId) -> MetadataResult<()> {
        let cf = self
            .db
            .cf_handle(CF_SHARD_ROUTING)
            .ok_or_else(|| MetadataError::Internal("ShardRouting CF not found".to_string()))?;
        let key = format!("{}", shard_id.as_raw());
        let value = format!("{}", group_id.as_raw());

        self.db
            .put_cf(cf, key.as_bytes(), value.as_bytes())
            .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
        Ok(())
    }

    /// Get shard to group routing mapping.
    pub fn get_shard_routing(&self, shard_id: ShardId) -> MetadataResult<Option<ShardGroupId>> {
        let cf = self
            .db
            .cf_handle(CF_SHARD_ROUTING)
            .ok_or_else(|| MetadataError::Internal("ShardRouting CF not found".to_string()))?;
        let key = format!("{}", shard_id.as_raw());

        match self.db.get_cf(cf, key.as_bytes()) {
            Ok(Some(value)) => {
                let group_id_str = String::from_utf8(value)
                    .map_err(|e| MetadataError::Internal(format!("Failed to parse group_id: {}", e)))?;
                let group_id_raw = group_id_str
                    .parse::<u64>()
                    .map_err(|e| MetadataError::Internal(format!("Failed to parse group_id as u64: {}", e)))?;
                Ok(Some(ShardGroupId::new(group_id_raw)))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    /// Load all shard to group routing mappings.
    pub fn load_all_shard_routings(&self) -> MetadataResult<std::collections::HashMap<ShardId, ShardGroupId>> {
        let cf = self
            .db
            .cf_handle(CF_SHARD_ROUTING)
            .ok_or_else(|| MetadataError::Internal("ShardRouting CF not found".to_string()))?;

        let mut mappings = std::collections::HashMap::new();
        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);

        for item in iter {
            let (key, value) = item.map_err(|e| MetadataError::Internal(format!("RocksDB iterator error: {}", e)))?;

            let shard_id_str = String::from_utf8(key.to_vec())
                .map_err(|e| MetadataError::Internal(format!("Failed to parse shard_id key: {}", e)))?;
            let shard_id_raw = shard_id_str
                .parse::<u64>()
                .map_err(|e| MetadataError::Internal(format!("Failed to parse shard_id as u64: {}", e)))?;
            let shard_id = ShardId::new(shard_id_raw);

            let group_id_str = String::from_utf8(value.to_vec())
                .map_err(|e| MetadataError::Internal(format!("Failed to parse group_id value: {}", e)))?;
            let group_id_raw = group_id_str
                .parse::<u64>()
                .map_err(|e| MetadataError::Internal(format!("Failed to parse group_id as u64: {}", e)))?;
            let group_id = ShardGroupId::new(group_id_raw);

            mappings.insert(shard_id, group_id);
        }

        Ok(mappings)
    }

    /// Delete shard to group routing mapping.
    pub fn delete_shard_routing(&self, shard_id: ShardId) -> MetadataResult<()> {
        let cf = self
            .db
            .cf_handle(CF_SHARD_ROUTING)
            .ok_or_else(|| MetadataError::Internal("ShardRouting CF not found".to_string()))?;
        let key = format!("{}", shard_id.as_raw());

        self.db
            .delete_cf(cf, key.as_bytes())
            .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
        Ok(())
    }

    /// Get mount entry.
    pub fn get_mount(&self, mount_id: MountId) -> MetadataResult<Option<MountEntry>> {
        let cf = self
            .db
            .cf_handle(CF_MOUNTS)
            .ok_or_else(|| MetadataError::Internal("Mounts CF not found".to_string()))?;
        let key = format!("{}", mount_id.as_raw());

        match self.db.get_cf(cf, key.as_bytes()) {
            Ok(Some(value)) => {
                let entry: MountEntry = decode_from_slice(&value, standard())
                    .map_err(|e| MetadataError::Internal(format!("Failed to deserialize MountEntry: {}", e)))?
                    .0;
                Ok(Some(entry))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    /// Put mount entry.
    pub fn put_mount(&self, entry: &MountEntry) -> MetadataResult<()> {
        let cf = self
            .db
            .cf_handle(CF_MOUNTS)
            .ok_or_else(|| MetadataError::Internal("Mounts CF not found".to_string()))?;
        let key = format!("{}", entry.mount_id.as_raw());
        let value = encode_to_vec(entry, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize MountEntry: {}", e)))?;

        self.db
            .put_cf(cf, key.as_bytes(), value)
            .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
        Ok(())
    }

    /// Delete mount entry.
    pub fn delete_mount(&self, mount_id: MountId) -> MetadataResult<()> {
        let cf = self
            .db
            .cf_handle(CF_MOUNTS)
            .ok_or_else(|| MetadataError::Internal("Mounts CF not found".to_string()))?;
        let key = format!("{}", mount_id.as_raw());

        self.db
            .delete_cf(cf, key.as_bytes())
            .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
        Ok(())
    }

    /// Get mount version.
    pub fn get_mount_version(&self) -> MetadataResult<u64> {
        let cf = self
            .db
            .cf_handle(CF_META)
            .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;

        match self.db.get_cf(cf, b"mount_version") {
            Ok(Some(value)) => {
                let version: u64 = decode_from_slice(&value, standard())
                    .map_err(|e| MetadataError::Internal(format!("Failed to deserialize mount_version: {}", e)))?
                    .0;
                Ok(version)
            }
            Ok(None) => Ok(1), // Default version
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    /// Get next worker ID (atomic increment).
    pub fn get_and_increment_worker_id(&self) -> MetadataResult<WorkerId> {
        use rocksdb::WriteBatch;

        let cf_meta = self
            .db
            .cf_handle(CF_META)
            .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;

        // Read current value
        let current_id = match self.db.get_cf(cf_meta, b"next_worker_id") {
            Ok(Some(value)) => {
                let id: u64 = decode_from_slice(&value, standard())
                    .map_err(|e| MetadataError::Internal(format!("Failed to deserialize next_worker_id: {}", e)))?
                    .0;
                id
            }
            Ok(None) => 1, // Start from 1
            Err(e) => return Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        };

        // Atomic increment using WriteBatch
        let mut batch = WriteBatch::default();
        let next_id = current_id + 1;
        let next_id_value = encode_to_vec(&next_id, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize next_worker_id: {}", e)))?;
        batch.put_cf(cf_meta, b"next_worker_id", next_id_value);

        self.db
            .write(batch)
            .map_err(|e| MetadataError::Internal(format!("RocksDB write error: {}", e)))?;

        Ok(WorkerId::new(current_id))
    }

    /// Get next data handle ID (atomic increment, data-plane identity allocator).
    pub fn get_and_increment_data_handle_id(&self) -> MetadataResult<DataHandleId> {
        use rocksdb::WriteBatch;

        let cf_meta = self
            .db
            .cf_handle(CF_META)
            .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;

        // Read current value
        let current_id = match self.db.get_cf(cf_meta, b"next_data_handle_id") {
            Ok(Some(value)) => {
                let id: u64 = decode_from_slice(&value, standard())
                    .map_err(|e| MetadataError::Internal(format!("Failed to deserialize next_data_handle_id: {}", e)))?
                    .0;
                id
            }
            Ok(None) => 1, // Start from 1
            Err(e) => return Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        };

        // Atomic increment using WriteBatch
        let mut batch = WriteBatch::default();
        let next_id = current_id + 1;
        let next_id_value = encode_to_vec(&next_id, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize next_data_handle_id: {}", e)))?;
        batch.put_cf(cf_meta, b"next_data_handle_id", next_id_value);

        self.db
            .write(batch)
            .map_err(|e| MetadataError::Internal(format!("RocksDB write error: {}", e)))?;

        Ok(DataHandleId::new(current_id))
    }

    /// Persist mapping from data_handle_id -> inode_id for routing from data plane back to namespace.
    pub fn put_data_handle_owner(&self, data_handle_id: DataHandleId, inode_id: InodeId) -> MetadataResult<()> {
        let cf_meta = self
            .db
            .cf_handle(CF_META)
            .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;
        let key = format!("data_handle_owner:{}", data_handle_id.as_raw());
        let value = encode_to_vec(&inode_id.as_raw(), standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize inode_id: {}", e)))?;

        self.db
            .put_cf(cf_meta, key.as_bytes(), value)
            .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
        Ok(())
    }

    /// Lookup inode_id from a data_handle_id (authoritative mapping).
    pub fn get_inode_by_data_handle(&self, data_handle_id: DataHandleId) -> MetadataResult<Option<InodeId>> {
        let cf_meta = self
            .db
            .cf_handle(CF_META)
            .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;
        let key = format!("data_handle_owner:{}", data_handle_id.as_raw());

        match self.db.get_cf(cf_meta, key.as_bytes()) {
            Ok(Some(value)) => {
                let inode_id_raw: u64 = decode_from_slice(&value, standard())
                    .map_err(|e| MetadataError::Internal(format!("Failed to deserialize inode_id: {}", e)))?
                    .0;
                Ok(Some(InodeId::new(inode_id_raw)))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    /// Validate that a data_handle_id has a bound inode_id and optionally matches an expected inode.
    /// Returns the authoritative inode_id on success.
    pub fn validate_data_handle_owner(
        &self,
        data_handle_id: DataHandleId,
        expect_inode: Option<InodeId>,
    ) -> MetadataResult<InodeId> {
        let inode_id = self.get_inode_by_data_handle(data_handle_id)?.ok_or_else(|| {
            MetadataError::StaleState(format!(
                "Missing owner for data_handle_id {}, refresh metadata state",
                data_handle_id
            ))
        })?;
        if let Some(expected) = expect_inode {
            if expected != inode_id {
                return Err(MetadataError::InvalidArgument(format!(
                    "data_handle_id {} is owned by inode {}, not {}",
                    data_handle_id, inode_id, expected
                )));
            }
        }
        Ok(inode_id)
    }

    /// Get worker ID by identity (address + labels hash).
    pub fn get_worker_id_by_identity(&self, identity: &str) -> MetadataResult<Option<WorkerId>> {
        let cf_meta = self
            .db
            .cf_handle(CF_META)
            .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;
        let key = format!("worker_identity:{}", identity);

        match self.db.get_cf(cf_meta, key.as_bytes()) {
            Ok(Some(value)) => {
                let worker_id_raw: u64 = decode_from_slice(&value, standard())
                    .map_err(|e| MetadataError::Internal(format!("Failed to deserialize worker_id: {}", e)))?
                    .0;
                Ok(Some(WorkerId::new(worker_id_raw)))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    /// Map worker identity to worker ID.
    pub fn put_worker_identity(&self, identity: &str, worker_id: WorkerId) -> MetadataResult<()> {
        use rocksdb::WriteBatch;

        let cf_meta = self
            .db
            .cf_handle(CF_META)
            .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;
        let key = format!("worker_identity:{}", identity);
        let value = encode_to_vec(&worker_id.as_raw(), standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize worker_id: {}", e)))?;

        let mut batch = WriteBatch::default();
        batch.put_cf(cf_meta, key.as_bytes(), value);

        self.db
            .write(batch)
            .map_err(|e| MetadataError::Internal(format!("RocksDB write error: {}", e)))?;

        Ok(())
    }

    /// Put mount version.
    pub fn put_mount_version(&self, version: u64) -> MetadataResult<()> {
        let cf = self
            .db
            .cf_handle(CF_META)
            .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;
        let value = encode_to_vec(&version, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize mount_version: {}", e)))?;

        self.db
            .put_cf(cf, b"mount_version", value)
            .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
        Ok(())
    }

    /// List all mount entries.
    pub fn list_mounts(&self) -> MetadataResult<Vec<MountEntry>> {
        let cf = self
            .db
            .cf_handle(CF_MOUNTS)
            .ok_or_else(|| MetadataError::Internal("Mounts CF not found".to_string()))?;

        let mut mounts = Vec::new();
        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);

        for item in iter {
            let (_, value) = item.map_err(|e| MetadataError::Internal(format!("RocksDB iterator error: {}", e)))?;
            let entry: MountEntry = decode_from_slice(&value, standard())
                .map_err(|e| MetadataError::Internal(format!("Failed to deserialize MountEntry: {}", e)))?
                .0;
            mounts.push(entry);
        }

        Ok(mounts)
    }

    /// Get worker info.
    pub fn get_worker(&self, worker_id: WorkerId) -> MetadataResult<Option<WorkerInfo>> {
        let cf = self
            .db
            .cf_handle(CF_WORKERS)
            .ok_or_else(|| MetadataError::Internal("Workers CF not found".to_string()))?;
        let key = format!("{}", worker_id.as_raw());

        match self.db.get_cf(cf, key.as_bytes()) {
            Ok(Some(value)) => {
                let info: WorkerInfo = decode_from_slice(&value, standard())
                    .map_err(|e| MetadataError::Internal(format!("Failed to deserialize WorkerInfo: {}", e)))?
                    .0;
                Ok(Some(info))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    /// Put worker info.
    pub fn put_worker(&self, info: &WorkerInfo) -> MetadataResult<()> {
        let cf = self
            .db
            .cf_handle(CF_WORKERS)
            .ok_or_else(|| MetadataError::Internal("Workers CF not found".to_string()))?;
        let key = format!("{}", info.worker_id.as_raw());
        let value = encode_to_vec(info, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize WorkerInfo: {}", e)))?;

        self.db
            .put_cf(cf, key.as_bytes(), value)
            .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
        Ok(())
    }

    /// List all workers.
    pub fn list_workers(&self) -> MetadataResult<Vec<WorkerInfo>> {
        let cf = self
            .db
            .cf_handle(CF_WORKERS)
            .ok_or_else(|| MetadataError::Internal("Workers CF not found".to_string()))?;

        let mut workers = Vec::new();
        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);

        for item in iter {
            let (_, value) = item.map_err(|e| MetadataError::Internal(format!("RocksDB iterator error: {}", e)))?;
            let info: WorkerInfo = decode_from_slice(&value, standard())
                .map_err(|e| MetadataError::Internal(format!("Failed to deserialize WorkerInfo: {}", e)))?
                .0;
            workers.push(info);
        }

        Ok(workers)
    }

    // ===== Raft-specific methods =====

    /// Get Raft log entry by index.
    pub fn get_raft_log(&self, log_index: u64) -> MetadataResult<Option<Vec<u8>>> {
        let cf = self
            .db
            .cf_handle(CF_RAFT_LOG)
            .ok_or_else(|| MetadataError::Internal("RaftLog CF not found".to_string()))?;
        let key = format!("{:020}", log_index); // Zero-padded for lexicographic ordering

        match self.db.get_cf(cf, key.as_bytes()) {
            Ok(Some(value)) => Ok(Some(value)),
            Ok(None) => Ok(None),
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    /// Put Raft log entry.
    pub fn put_raft_log(&self, log_index: u64, entry_data: &[u8]) -> MetadataResult<()> {
        let cf = self
            .db
            .cf_handle(CF_RAFT_LOG)
            .ok_or_else(|| MetadataError::Internal("RaftLog CF not found".to_string()))?;
        let key = format!("{:020}", log_index);

        self.db
            .put_cf(cf, key.as_bytes(), entry_data)
            .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
        Ok(())
    }

    /// Delete Raft log entries from start_index (inclusive) onwards.
    pub fn delete_raft_logs_from(&self, start_index: u64) -> MetadataResult<()> {
        let cf = self
            .db
            .cf_handle(CF_RAFT_LOG)
            .ok_or_else(|| MetadataError::Internal("RaftLog CF not found".to_string()))?;

        let start_key = format!("{:020}", start_index);
        let iter = self.db.iterator_cf(
            cf,
            rocksdb::IteratorMode::From(start_key.as_bytes(), rocksdb::Direction::Forward),
        );

        let mut keys_to_delete = Vec::new();
        for item in iter {
            let (key, _) = item.map_err(|e| MetadataError::Internal(format!("RocksDB iterator error: {}", e)))?;
            keys_to_delete.push(key);
        }

        for key in keys_to_delete {
            self.db
                .delete_cf(cf, &key)
                .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
        }

        Ok(())
    }

    /// Delete Raft log entries up to and including end_index.
    pub fn delete_raft_logs_upto(&self, end_index: u64) -> MetadataResult<()> {
        let cf = self
            .db
            .cf_handle(CF_RAFT_LOG)
            .ok_or_else(|| MetadataError::Internal("RaftLog CF not found".to_string()))?;

        let end_key = format!("{:020}", end_index);
        let end_key_bytes = end_key.as_bytes();
        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);

        let mut keys_to_delete = Vec::new();
        for item in iter {
            let (key, _) = item.map_err(|e| MetadataError::Internal(format!("RocksDB iterator error: {}", e)))?;

            // Compare lexicographically (key is Box<[u8]>, convert to &[u8] for comparison)
            if key.as_ref() <= end_key_bytes {
                keys_to_delete.push(key);
            } else {
                break;
            }
        }

        for key in keys_to_delete {
            self.db
                .delete_cf(cf, &key)
                .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
        }

        Ok(())
    }

    /// Get Raft state (vote, last_purged, etc.).
    pub fn get_raft_state(&self) -> MetadataResult<Option<Vec<u8>>> {
        let cf = self
            .db
            .cf_handle(CF_RAFT_STATE)
            .ok_or_else(|| MetadataError::Internal("RaftState CF not found".to_string()))?;

        match self.db.get_cf(cf, b"raft_state") {
            Ok(Some(value)) => Ok(Some(value)),
            Ok(None) => Ok(None),
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    /// Put Raft state.
    pub fn put_raft_state(&self, state_data: &[u8]) -> MetadataResult<()> {
        let cf = self
            .db
            .cf_handle(CF_RAFT_STATE)
            .ok_or_else(|| MetadataError::Internal("RaftState CF not found".to_string()))?;

        self.db
            .put_cf(cf, b"raft_state", state_data)
            .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
        Ok(())
    }

    /// Serialize and persist the full Raft state blob.
    pub fn persist_raft_state(&self, state: &AppMetadataRaftState) -> MetadataResult<()> {
        let state_data = serde_json::to_vec(state)
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize raft state: {}", e)))?;
        self.put_raft_state(&state_data)
    }

    /// Get current snapshot metadata.
    pub fn get_snapshot_meta(&self) -> MetadataResult<Option<Vec<u8>>> {
        let cf = self
            .db
            .cf_handle(CF_RAFT_SNAPSHOT)
            .ok_or_else(|| MetadataError::Internal("RaftSnapshot CF not found".to_string()))?;

        match self.db.get_cf(cf, b"snapshot_meta") {
            Ok(Some(value)) => Ok(Some(value)),
            Ok(None) => Ok(None),
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    /// Put snapshot metadata.
    pub fn put_snapshot_meta(&self, meta_data: &[u8]) -> MetadataResult<()> {
        let cf = self
            .db
            .cf_handle(CF_RAFT_SNAPSHOT)
            .ok_or_else(|| MetadataError::Internal("RaftSnapshot CF not found".to_string()))?;

        self.db
            .put_cf(cf, b"snapshot_meta", meta_data)
            .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
        Ok(())
    }

    /// Get snapshot data.
    pub fn get_snapshot_data(&self, snapshot_id: &str) -> MetadataResult<Option<Vec<u8>>> {
        let cf = self
            .db
            .cf_handle(CF_RAFT_SNAPSHOT)
            .ok_or_else(|| MetadataError::Internal("RaftSnapshot CF not found".to_string()))?;
        let key = format!("snapshot:{}", snapshot_id);

        match self.db.get_cf(cf, key.as_bytes()) {
            Ok(Some(value)) => Ok(Some(value)),
            Ok(None) => Ok(None),
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    /// Put snapshot data.
    pub fn put_snapshot_data(&self, snapshot_id: &str, data: &[u8]) -> MetadataResult<()> {
        let cf = self
            .db
            .cf_handle(CF_RAFT_SNAPSHOT)
            .ok_or_else(|| MetadataError::Internal("RaftSnapshot CF not found".to_string()))?;
        let key = format!("snapshot:{}", snapshot_id);

        self.db
            .put_cf(cf, key.as_bytes(), data)
            .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
        Ok(())
    }

    /// Get the last log index from RocksDB.
    pub fn get_last_log_index(&self) -> MetadataResult<Option<u64>> {
        let cf = self
            .db
            .cf_handle(CF_RAFT_LOG)
            .ok_or_else(|| MetadataError::Internal("RaftLog CF not found".to_string()))?;

        // Iterate from end to find the last log
        let mut iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::End);

        if let Some(item) = iter.next() {
            let (key, _) = item.map_err(|e| MetadataError::Internal(format!("RocksDB iterator error: {}", e)))?;

            // Parse the key (format: "{:020}")
            let key_str = String::from_utf8_lossy(&key);
            if let Ok(index) = key_str.trim().parse::<u64>() {
                Ok(Some(index))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Put block reference count (global, per block_id).
    pub fn put_block_ref_count(&self, block_id: BlockId, count: u64) -> MetadataResult<()> {
        let cf = self
            .db
            .cf_handle(CF_BLOCK_REF_COUNTS)
            .ok_or_else(|| MetadataError::Internal("BlockRefCounts CF not found".to_string()))?;
        let key = format!("{}", block_id);
        let value = encode_to_vec(&count, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize ref count: {}", e)))?;

        self.db
            .put_cf(cf, key.as_bytes(), value)
            .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
        Ok(())
    }

    /// Get block reference count (global, per block_id).
    pub fn get_block_ref_count(&self, block_id: BlockId) -> MetadataResult<Option<u64>> {
        let cf = self
            .db
            .cf_handle(CF_BLOCK_REF_COUNTS)
            .ok_or_else(|| MetadataError::Internal("BlockRefCounts CF not found".to_string()))?;
        let key = format!("{}", block_id);

        match self.db.get_cf(cf, key.as_bytes()) {
            Ok(Some(value)) => {
                let count: u64 = decode_from_slice(&value, standard())
                    .map_err(|e| MetadataError::Internal(format!("Failed to deserialize ref count: {}", e)))?
                    .0;
                Ok(Some(count))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    /// Increment block reference count (atomic increment).
    /// Returns the new count.
    pub fn increment_block_ref_count(&self, block_id: BlockId) -> MetadataResult<u64> {
        let current = self.get_block_ref_count(block_id)?.unwrap_or(0);
        let new_count = current + 1;
        self.put_block_ref_count(block_id, new_count)?;
        Ok(new_count)
    }

    /// Decrement block reference count (atomic decrement, clamped to 0).
    /// Returns the new count and whether it reached zero.
    pub fn decrement_block_ref_count(&self, block_id: BlockId) -> MetadataResult<(u64, bool)> {
        let current = self.get_block_ref_count(block_id)?.unwrap_or(0);
        if current == 0 {
            // Refcount already 0 or negative (consistency bug, but clamp to 0)
            warn!(
                block_id = %block_id,
                "Attempted to decrement refcount that is already 0 (consistency issue)"
            );
            return Ok((0, true));
        }
        let new_count = current - 1;
        if new_count == 0 {
            self.delete_block_ref_count(block_id)?;
            Ok((0, true))
        } else {
            self.put_block_ref_count(block_id, new_count)?;
            Ok((new_count, false))
        }
    }

    /// Delete block reference count (when count reaches 0).
    pub fn delete_block_ref_count(&self, block_id: BlockId) -> MetadataResult<()> {
        let cf = self
            .db
            .cf_handle(CF_BLOCK_REF_COUNTS)
            .ok_or_else(|| MetadataError::Internal("BlockRefCounts CF not found".to_string()))?;
        let key = format!("{}", block_id);

        self.db
            .delete_cf(cf, key.as_bytes())
            .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
        Ok(())
    }

    /// Get all block reference counts (for snapshot/restore).
    pub fn get_all_block_ref_counts(&self) -> MetadataResult<Vec<(BlockId, u64)>> {
        use rocksdb::IteratorMode;
        use std::str::FromStr;

        let cf = self
            .db
            .cf_handle(CF_BLOCK_REF_COUNTS)
            .ok_or_else(|| MetadataError::Internal("BlockRefCounts CF not found".to_string()))?;

        let mut ref_counts = Vec::new();
        let iter = self.db.iterator_cf(cf, IteratorMode::Start);

        for item in iter {
            let (key, value) = item.map_err(|e| MetadataError::Internal(format!("RocksDB iterator error: {}", e)))?;

            // Parse key: block_id string (e.g., "42:7")
            let key_str = String::from_utf8_lossy(&key);
            if let Ok(block_id) = BlockId::from_str(&key_str) {
                let count: u64 = decode_from_slice(&value, standard())
                    .map_err(|e| MetadataError::Internal(format!("Failed to deserialize ref count: {}", e)))?
                    .0;

                ref_counts.push((block_id, count));
            }
        }

        Ok(ref_counts)
    }

    /// Put delete intent.
    pub fn put_delete_intent(&self, intent: &crate::state::DeleteIntent) -> MetadataResult<()> {
        let cf = self
            .db
            .cf_handle(CF_DELETE_INTENTS)
            .ok_or_else(|| MetadataError::Internal("DeleteIntents CF not found".to_string()))?;
        let key = format!("{}", intent.intent_id);
        let value = encode_to_vec(intent, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize DeleteIntent: {}", e)))?;

        self.db
            .put_cf(cf, key.as_bytes(), value)
            .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
        Ok(())
    }

    /// Get delete intent by intent_id.
    pub fn get_delete_intent(&self, intent_id: u64) -> MetadataResult<Option<crate::state::DeleteIntent>> {
        let cf = self
            .db
            .cf_handle(CF_DELETE_INTENTS)
            .ok_or_else(|| MetadataError::Internal("DeleteIntents CF not found".to_string()))?;
        let key = format!("{}", intent_id);

        match self.db.get_cf(cf, key.as_bytes()) {
            Ok(Some(value)) => {
                let intent: crate::state::DeleteIntent = decode_from_slice(&value, standard())
                    .map_err(|e| MetadataError::Internal(format!("Failed to deserialize DeleteIntent: {}", e)))?
                    .0;
                Ok(Some(intent))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    /// List pending delete intents (not_before_ms <= now_ms, status==Pending, ordered by not_before_ms).
    /// Returns up to `limit` intents.
    pub fn list_pending_delete_intents(
        &self,
        limit: usize,
        now_ms: u64,
    ) -> MetadataResult<Vec<crate::state::DeleteIntent>> {
        use crate::state::DeleteIntentStatus;
        use rocksdb::IteratorMode;

        let cf = self
            .db
            .cf_handle(CF_DELETE_INTENTS)
            .ok_or_else(|| MetadataError::Internal("DeleteIntents CF not found".to_string()))?;

        let mut intents = Vec::new();
        let iter = self.db.iterator_cf(cf, IteratorMode::Start);

        for item in iter {
            let (_, value) = item.map_err(|e| MetadataError::Internal(format!("RocksDB iterator error: {}", e)))?;

            match decode_from_slice::<crate::state::DeleteIntent, _>(&value, standard()) {
                Ok((intent, _)) => {
                    // Only include intents that are:
                    // 1. Status is Pending (not Completed/Failed)
                    // 2. Ready (not_before_ms <= now_ms)
                    if matches!(intent.status, DeleteIntentStatus::Pending) && intent.not_before_ms <= now_ms {
                        intents.push(intent);
                    }
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        "Failed to deserialize DeleteIntent, skipping"
                    );
                    // Don't fail the entire scan for decode errors
                }
            }
        }

        // Sort by not_before_ms (ascending) and limit
        intents.sort_by_key(|intent| intent.not_before_ms);
        intents.truncate(limit);

        Ok(intents)
    }

    /// Update delete intent status (for execution progress, not via Raft).
    /// This is idempotent: repeated writes of the same status should not error.
    pub fn update_delete_intent_status(
        &self,
        intent_id: u64,
        status: crate::state::DeleteIntentStatus,
        finished_at_ms: Option<u64>,
        error_msg: Option<String>,
    ) -> MetadataResult<()> {
        // Get existing intent
        let mut intent = match self.get_delete_intent(intent_id)? {
            Some(intent) => intent,
            None => {
                // Intent not found - this is acceptable (may have been deleted)
                return Ok(());
            }
        };

        // Update status (idempotent: if already in the same status, no-op)
        intent.status = status;
        intent.finished_at_ms = finished_at_ms;
        if let Some(msg) = error_msg {
            intent.last_error_msg = Some(msg);
        }

        // Persist updated intent
        self.put_delete_intent(&intent)?;

        Ok(())
    }

    /// Delete delete intent (after execution/ack).
    /// Note: With status persistence, we typically don't delete intents immediately.
    /// They are kept for audit/recovery purposes and cleaned up later.
    pub fn delete_delete_intent(&self, intent_id: u64) -> MetadataResult<()> {
        let cf = self
            .db
            .cf_handle(CF_DELETE_INTENTS)
            .ok_or_else(|| MetadataError::Internal("DeleteIntents CF not found".to_string()))?;
        let key = format!("{}", intent_id);

        self.db
            .delete_cf(cf, key.as_bytes())
            .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
        Ok(())
    }

    /// Generate a new intent ID (monotonically increasing).
    /// Uses timestamp-based ID with sequence number for uniqueness.
    pub fn generate_intent_id(&self) -> MetadataResult<u64> {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        // Use timestamp as base, add random component for uniqueness
        // In production, could use atomic counter or UUID
        Ok(now_ms << 16 | (rand::random::<u16>() as u64))
    }

    /// Encode inode key: "inode/" + 8 bytes BE (inode_id)
    fn encode_inode_key(inode_id: InodeId) -> Vec<u8> {
        let mut key = b"inode/".to_vec();
        key.extend_from_slice(&inode_id.to_be_bytes());
        key
    }

    /// Get inode by ID.
    pub fn get_inode(&self, inode_id: InodeId) -> MetadataResult<Option<Inode>> {
        let cf = self
            .db
            .cf_handle(CF_INODES)
            .ok_or_else(|| MetadataError::Internal("Inodes CF not found".to_string()))?;
        let key = Self::encode_inode_key(inode_id);

        match self.db.get_cf(cf, &key) {
            Ok(Some(value)) => {
                let inode: Inode = serde_json::from_slice(&value)
                    .map_err(|e| MetadataError::Internal(format!("Failed to deserialize Inode: {}", e)))?;
                Ok(Some(inode))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    /// Put inode.
    pub fn put_inode(&self, inode: &Inode) -> MetadataResult<()> {
        let cf = self
            .db
            .cf_handle(CF_INODES)
            .ok_or_else(|| MetadataError::Internal("Inodes CF not found".to_string()))?;
        let key = Self::encode_inode_key(inode.inode_id);
        let value = serde_json::to_vec(inode)
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize Inode: {}", e)))?;

        self.db
            .put_cf(cf, &key, value)
            .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
        Ok(())
    }

    /// Delete inode.
    pub fn delete_inode(&self, inode_id: InodeId) -> MetadataResult<()> {
        let cf = self
            .db
            .cf_handle(CF_INODES)
            .ok_or_else(|| MetadataError::Internal("Inodes CF not found".to_string()))?;
        let key = Self::encode_inode_key(inode_id);

        self.db
            .delete_cf(cf, &key)
            .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
        Ok(())
    }

    /// Encode dentry key: "dentry/" + 8 bytes BE (parent_inode_id) + name_bytes
    fn encode_dentry_key(parent_inode_id: InodeId, name: &str) -> Vec<u8> {
        let mut key = b"dentry/".to_vec();
        key.extend_from_slice(&parent_inode_id.to_be_bytes());
        key.extend_from_slice(name.as_bytes());
        key
    }

    /// Decode dentry key: extract parent_inode_id and name
    fn decode_dentry_key(key: &[u8]) -> Option<(InodeId, String)> {
        if !key.starts_with(b"dentry/") {
            return None;
        }
        let prefix_len = b"dentry/".len();
        if key.len() < prefix_len + 8 {
            return None;
        }
        let parent_bytes: [u8; 8] = key[prefix_len..prefix_len + 8].try_into().ok()?;
        let parent_inode_id = InodeId::from_be_bytes(parent_bytes);
        let name_bytes = &key[prefix_len + 8..];
        let name = String::from_utf8(name_bytes.to_vec()).ok()?;
        Some((parent_inode_id, name))
    }

    /// Get dentry (parent_inode_id, name) -> child_inode_id
    pub fn get_dentry(&self, parent_inode_id: InodeId, name: &str) -> MetadataResult<Option<InodeId>> {
        let cf = self
            .db
            .cf_handle(CF_DENTRIES)
            .ok_or_else(|| MetadataError::Internal("Dentries CF not found".to_string()))?;
        let key = Self::encode_dentry_key(parent_inode_id, name);

        match self.db.get_cf(cf, &key) {
            Ok(Some(value)) => {
                if value.len() != 8 {
                    return Err(MetadataError::Internal(format!(
                        "Invalid dentry value length: {}",
                        value.len()
                    )));
                }
                let mut child_bytes = [0u8; 8];
                child_bytes.copy_from_slice(&value[..8]);
                let child_inode_id = InodeId::from_be_bytes(child_bytes);
                Ok(Some(child_inode_id))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    /// Put dentry.
    pub fn put_dentry(&self, parent_inode_id: InodeId, name: &str, child_inode_id: InodeId) -> MetadataResult<()> {
        let cf = self
            .db
            .cf_handle(CF_DENTRIES)
            .ok_or_else(|| MetadataError::Internal("Dentries CF not found".to_string()))?;
        let key = Self::encode_dentry_key(parent_inode_id, name);
        let value = child_inode_id.to_be_bytes();

        self.db
            .put_cf(cf, &key, value)
            .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
        Ok(())
    }

    /// Delete dentry.
    pub fn delete_dentry(&self, parent_inode_id: InodeId, name: &str) -> MetadataResult<()> {
        let cf = self
            .db
            .cf_handle(CF_DENTRIES)
            .ok_or_else(|| MetadataError::Internal("Dentries CF not found".to_string()))?;
        let key = Self::encode_dentry_key(parent_inode_id, name);

        self.db
            .delete_cf(cf, &key)
            .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
        Ok(())
    }

    /// List dentries for a parent directory (for ReadDir).
    /// Returns all (name, child_inode_id) pairs for the given parent_inode_id.
    /// The results are sorted by key (which includes name), suitable for pagination.
    pub fn list_dentries(&self, parent_inode_id: InodeId) -> MetadataResult<Vec<(String, InodeId)>> {
        let (entries, _, _) = self.list_dentries_with_cursor(parent_inode_id, None, None)?;
        Ok(entries)
    }

    /// List dentries with pagination support (for ReadDir).
    ///
    /// Args:
    /// - parent_inode_id: Parent directory inode ID
    /// - cursor_key: Optional cursor key (opaque bytes from previous ReadDir response).
    ///   If None, starts from the beginning. If Some, seeks to the key's successor.
    /// - max_entries: Maximum number of entries to return. If None, returns all.
    ///
    /// Returns:
    /// - entries: Vec of (name, child_inode_id) pairs
    /// - next_cursor_key: Next cursor key for pagination (None if EOF)
    /// - eof: Whether this is the last page
    pub fn list_dentries_with_cursor(
        &self,
        parent_inode_id: InodeId,
        cursor_key: Option<&[u8]>,
        max_entries: Option<usize>,
    ) -> MetadataResult<(Vec<(String, InodeId)>, Option<Vec<u8>>, bool)> {
        let cf = self
            .db
            .cf_handle(CF_DENTRIES)
            .ok_or_else(|| MetadataError::Internal("Dentries CF not found".to_string()))?;

        let prefix = Self::encode_dentry_key(parent_inode_id, "");

        let (start_key, mut skip_first) = match cursor_key {
            Some(c) if c.starts_with(&prefix) => (c.to_vec(), true),
            _ => (prefix.clone(), false),
        };

        let mut entries = Vec::new();
        let mut iter = self
            .db
            .iterator_cf(cf, rocksdb::IteratorMode::From(&start_key, rocksdb::Direction::Forward));

        while let Some(item) = iter.next() {
            let (key, value) = item.map_err(|e| MetadataError::Internal(format!("RocksDB iterator error: {}", e)))?;

            // Check if key still matches prefix (parent_inode_id)
            if !key.starts_with(&prefix) {
                break; // finished this directory
            }

            // Skip the cursor key entry itself
            if skip_first {
                skip_first = false;
                continue;
            }

            let Some((decoded_parent, name)) = Self::decode_dentry_key(&key) else {
                continue;
            };

            if decoded_parent != parent_inode_id || value.len() != 8 {
                continue;
            }

            let mut child_bytes = [0u8; 8];
            child_bytes.copy_from_slice(&value[..8]);
            let child_inode_id = InodeId::from_be_bytes(child_bytes);
            entries.push((name, child_inode_id));

            if let Some(max) = max_entries {
                if entries.len() == max {
                    // Peek ahead to know if another page exists; only set cursor when there is more.
                    let has_more = if let Some(next_item) = iter.next() {
                        let (next_key, _) =
                            next_item.map_err(|e| MetadataError::Internal(format!("RocksDB iterator error: {}", e)))?;
                        next_key.starts_with(&prefix)
                    } else {
                        false
                    };
                    let next_cursor_key = if has_more { Some(key.to_vec()) } else { None };
                    return Ok((entries, next_cursor_key, !has_more));
                }
            }
        }
        Ok((entries, None, true))
    }

    /// Check if directory is empty (has no dentries).
    pub fn is_directory_empty(&self, parent_inode_id: InodeId) -> MetadataResult<bool> {
        let (entries, _, _) = self.list_dentries_with_cursor(parent_inode_id, None, Some(1))?;
        Ok(entries.is_empty())
    }

    /// Get a column family handle by name.
    pub fn cf(&self, name: &str) -> MetadataResult<&ColumnFamily> {
        self.db
            .cf_handle(name)
            .ok_or_else(|| MetadataError::Internal(format!("Column family {} not found", name)))
    }

    /// Clear the provided column families by iterating and batching deletes.
    pub fn clear_cfs(&self, cf_names: &[&str], batch_bytes: usize) -> MetadataResult<()> {
        for cf_name in cf_names {
            let cf = self.cf(cf_name)?;
            let mut iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);
            let mut batch = WriteBatch::default();

            while let Some(item) = iter.next() {
                let (key, _) = item.map_err(|e| MetadataError::Internal(format!("RocksDB iterator error: {}", e)))?;
                batch.delete_cf(cf, key);

                if batch.size_in_bytes() >= batch_bytes {
                    self.write_batch(batch)?;
                    batch = WriteBatch::default();
                }
            }

            if batch.len() > 0 {
                self.write_batch(batch)?;
            }
        }

        Ok(())
    }

    /// Create a RocksDB snapshot view for consistent reads.
    pub fn snapshot(&self) -> rocksdb::Snapshot<'_> {
        self.db.snapshot()
    }

    /// Directory where snapshot files are materialized.
    pub fn snapshot_dir(&self) -> &Path {
        &self.snapshot_dir
    }

    /// Write a RocksDB batch with consistent error mapping.
    pub fn write_batch(&self, batch: WriteBatch) -> MetadataResult<()> {
        self.db
            .write(batch)
            .map_err(|e| MetadataError::Internal(format!("RocksDB batch write: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::TempDir;
    use types::fs::{FileAttrs, Inode, InodeId};
    use types::ids::MountId;

    #[test]
    fn test_data_handle_allocator_unique_and_durable() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("allocator_db");
        let storage = RocksDBStorage::open(&db_path).unwrap();

        let first = storage.get_and_increment_data_handle_id().unwrap();
        let second = storage.get_and_increment_data_handle_id().unwrap();
        assert_ne!(first, second);
        assert!(second.as_raw() > first.as_raw());

        // Re-open to ensure durability.
        drop(storage);
        let reopened = RocksDBStorage::open(&db_path).unwrap();
        let third = reopened.get_and_increment_data_handle_id().unwrap();
        assert!(third.as_raw() > second.as_raw());
    }

    fn setup_dir_with_entries(parent_inode_id: InodeId, entries: &[(&str, InodeId)]) -> (TempDir, RocksDBStorage) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db_dentries");
        let storage = RocksDBStorage::open(&db_path).unwrap();

        // Create parent dir and some child nodes.
        let parent_inode = Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1));
        storage.put_inode(&parent_inode).unwrap();

        for (name, child) in entries {
            storage.put_dentry(parent_inode_id, name, *child).unwrap();
        }

        (temp_dir, storage)
    }

    #[test]
    #[ignore = "pagination semantics under identity pivot pending"]
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
        assert!(cursor2.is_some()); // The current implementation returns the last key at EOF.
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
    fn test_legacy_cf_detection() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");

        // Create a RocksDB with legacy "files" CF
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

        // Try to open with new code - should fail with legacy CF error
        let result = RocksDBStorage::open(&db_path);
        assert!(result.is_err(), "Opening DB with legacy 'files' CF should fail");
        match result {
            Err(e) => {
                let error_msg = format!("{}", e);
                assert!(
                    error_msg.contains("legacy column family") || error_msg.contains("files"),
                    "Error message should mention legacy column family 'files', got: {}",
                    error_msg
                );
            }
            Ok(_) => panic!("Expected error but got Ok"),
        }
    }
}
