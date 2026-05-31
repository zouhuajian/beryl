// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! RocksDB storage backend for Raft state machine.
//!
//! Keyspace schema:
//! - blocks/{block_id} -> BlockMetaState (serialized)
//! - leases/{block_id} -> LeaseState (serialized)
//! - mounts/{mount_id} -> MountEntry (serialized)
//! - dedup/{client_id}:{call_id} -> AppliedResult (serialized)
//! - shard_groups/{group_name} -> ShardGroupInfo (serialized)
//! - shard_routing/{shard_id} -> group_name (validated string)
//! - route_epoch -> u64
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
use crate::metrics::{
    DEDUP_EVICTED_SIZE_TOTAL, DEDUP_EVICTED_TTL_TOTAL, DEDUP_LOOKUP_HIT_TOTAL, DEDUP_LOOKUP_MISS_TOTAL,
    DEDUP_STORE_ENTRIES_GAUGE,
};
use crate::mount::MountEntry;
use crate::raft::types::{AppDataResponse, AppMetadataRaftState, CommandFingerprint, DedupKey, ShardGroupInfo};
use crate::state::{BlockMetaState, DeleteIntentStatus, LeaseState, RouteEpoch};
use crate::worker::WorkerInfo;
use bincode::config::standard;
use bincode::serde::{decode_from_slice, encode_to_vec};
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, Options, WriteBatch, DB};
use serde::{Deserialize, Serialize};
use serde_json;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::warn;
use types::fs::{Inode, InodeId};
use types::ids::{BlockId, DataHandleId, MountId, ShardId, WorkerId};
use types::layout::FileLayout;
use types::GroupName;

type DentryPage = (Vec<(String, InodeId)>, Option<Vec<u8>>, bool);

/// Column family names for RocksDB.
const CF_BLOCKS: &str = "blocks";
const CF_LEASES: &str = "leases";
const CF_MOUNTS: &str = "mounts";
const CF_DEDUP: &str = "dedup";
const CF_SHARD_GROUPS: &str = "shard_groups";
const CF_SHARD_ROUTING: &str = "shard_routing"; // shard_id -> group_name mapping
const CF_WORKERS: &str = "workers";
const CF_BLOCK_REF_COUNTS: &str = "block_ref_counts"; // block_id -> u64 (global refcount)
const CF_DELETE_INTENTS: &str = "delete_intents"; // intent_id -> DeleteIntent
/// Raft column families
const CF_META: &str = "meta"; // route_epoch, mount_version, file layouts, etc.
const CF_RAFT_LOG: &str = "raft_log"; // Raft log entries
const CF_RAFT_STATE: &str = "raft_state"; // Raft state (hard_state, membership)
const CF_RAFT_SNAPSHOT: &str = "raft_snapshot"; // Raft snapshots

const DEDUP_VERSION_KEY: &str = "dedup_version";
const DEDUP_FORMAT_VERSION: u64 = 3;
const DEDUP_TTL_MS: u64 = 10 * 60 * 1000; // 10 minutes; TODO: make configurable
const DEDUP_MAX_ENTRIES: usize = if cfg!(debug_assertions) { 128 } else { 10_000 };
const NEXT_INODE_ID_KEY: &[u8] = b"next_inode_id";
const NEXT_DELETE_INTENT_ID_KEY: &[u8] = b"next_delete_intent_id";

fn worker_key(group_name: &GroupName, worker_id: WorkerId) -> String {
    format!("{}/{}", group_name.as_str(), worker_id.as_raw())
}

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

/// Persisted replay record for an applied mutation command.
///
/// AppliedResult stores the minimal deterministic result of an applied mutation
/// command. It is used for retry/replay, not as a general RPC response cache.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AppliedResult {
    pub fingerprint: CommandFingerprint,
    pub result: AppDataResponse,
    pub created_at_ms: u64,
    pub size_bytes: u32,
}

/// Overwritten rename target state that must be removed with the namespace move.
pub struct RenameOverwriteCleanup<'a> {
    pub inode_id: InodeId,
    pub data_handle_id: Option<DataHandleId>,
    pub released_block_ids: &'a [BlockId],
    pub now_ms: u64,
}

/// Namespace rename writes that must commit as one RocksDB batch.
pub struct RenameAtomicUpdate<'a> {
    pub src_parent_inode_id: InodeId,
    pub src_name: &'a str,
    pub dst_parent_inode_id: InodeId,
    pub dst_name: &'a str,
    pub src_inode_id: InodeId,
    pub overwritten_target: Option<RenameOverwriteCleanup<'a>>,
    pub updated_src_parent: Option<&'a Inode>,
    pub updated_dst_parent: Option<&'a Inode>,
    pub updated_src_inode: &'a Inode,
}

/// One namespace entry removed by a post-order recursive delete plan.
pub struct DeleteTreeEntry {
    pub parent_inode_id: InodeId,
    pub name: String,
    pub inode_id: InodeId,
    pub data_handle_id: Option<DataHandleId>,
    pub layout: Option<FileLayout>,
}

/// Recursive delete writes that must commit as one RocksDB batch.
pub struct DeleteTreeAtomicUpdate<'a> {
    pub entries: &'a [DeleteTreeEntry],
    pub updated_parent: &'a Inode,
    pub block_ref_decrements: &'a [(BlockId, u64)],
    pub now_ms: u64,
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

    /// Create RocksDB state for `metadata format`.
    pub fn create_for_format<P: AsRef<Path>>(path: P) -> MetadataResult<Self> {
        Self::open_with_create_policy(path, true)
    }

    /// Open already formatted RocksDB state for `metadata start`.
    pub fn open_existing_for_start<P: AsRef<Path>>(path: P) -> MetadataResult<Self> {
        Self::open_with_create_policy(path, false)
    }

    fn open_with_create_policy<P: AsRef<Path>>(path: P, create_missing: bool) -> MetadataResult<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let snapshot_dir = path_buf.join("snapshots");
        if !create_missing {
            validate_existing_rocksdb_state(&path_buf, &snapshot_dir)?;
        }

        let mut opts = Options::default();
        opts.create_if_missing(create_missing);
        opts.create_missing_column_families(create_missing);
        // TODO: Optimize rocksdb opts
        // opts.set_allow_mmap_writes(true);
        // opts.set_allow_mmap_reads(true);

        let db = DB::open_cf_descriptors(&opts, &path_buf, cf_descriptors()).map_err(|e| {
            if create_missing {
                MetadataError::Internal(format!("Failed to create RocksDB state at {}: {e}", path_buf.display()))
            } else {
                missing_rocksdb_state_error(&path_buf, &format!("RocksDB open failed: {e}"))
            }
        })?;

        if create_missing {
            // Format is the create-capable path; start must not rewrite existing state.
            reset_dedup_if_stale(&db)?;
            std::fs::create_dir_all(&snapshot_dir).map_err(|e| {
                MetadataError::Internal(format!("Failed to create snapshot dir {:?}: {}", snapshot_dir, e))
            })?;
        }

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

    fn batch_put_block(
        batch: &mut WriteBatch,
        cf_blocks: &rocksdb::ColumnFamily,
        block_meta: &BlockMetaState,
    ) -> MetadataResult<()> {
        let key = format!("{}", block_meta.block_id);
        let value = encode_to_vec(block_meta, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize BlockMetaState: {}", e)))?;
        batch.put_cf(cf_blocks, key.as_bytes(), value);
        Ok(())
    }

    pub fn put_block_with_apply_result_atomic(
        &self,
        block_meta: &BlockMetaState,
        dedup_key: &DedupKey,
        applied_result: AppliedResult,
    ) -> MetadataResult<()> {
        let cf_blocks = self.cf(CF_BLOCKS)?;
        let mut batch = WriteBatch::default();
        Self::batch_put_block(&mut batch, cf_blocks, block_meta)?;
        self.commit_apply_batch(batch, dedup_key, applied_result)
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

    fn batch_put_lease(batch: &mut WriteBatch, cf: &ColumnFamily, lease_state: &LeaseState) -> MetadataResult<()> {
        let key = format!("{}", lease_state.block_id);
        let value = encode_to_vec(lease_state, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize LeaseState: {}", e)))?;
        batch.put_cf(cf, key.as_bytes(), value);
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

    /// Atomically persist a lease upsert with apply tracking.
    pub fn acquire_lease_with_apply_result_atomic(
        &self,
        lease_state: &LeaseState,
        dedup_key: &DedupKey,
        applied_result: AppliedResult,
    ) -> MetadataResult<()> {
        let cf_leases = self.cf(CF_LEASES)?;
        let mut batch = WriteBatch::default();
        Self::batch_put_lease(&mut batch, cf_leases, lease_state)?;
        self.commit_apply_batch(batch, dedup_key, applied_result)
    }

    /// Atomically persist a lease release with apply tracking.
    pub fn release_lease_with_apply_result_atomic(
        &self,
        block_id: BlockId,
        dedup_key: &DedupKey,
        applied_result: AppliedResult,
    ) -> MetadataResult<()> {
        let cf_leases = self.cf(CF_LEASES)?;
        let mut batch = WriteBatch::default();
        let key = format!("{}", block_id);
        batch.delete_cf(cf_leases, key.as_bytes());
        self.commit_apply_batch(batch, dedup_key, applied_result)
    }

    /// Get applied result for idempotency.
    pub fn get_applied_result(&self, request: &DedupKey) -> MetadataResult<Option<AppliedResult>> {
        let now_ms = now_millis();
        let evicted = self.evict_expired(now_ms)?;
        if evicted > 0 {
            DEDUP_EVICTED_TTL_TOTAL.fetch_add(evicted as u64, Ordering::Relaxed);
        }

        self.get_applied_result_without_ttl_eviction(request)
    }

    pub fn get_applied_result_without_ttl_eviction(&self, request: &DedupKey) -> MetadataResult<Option<AppliedResult>> {
        let cf = self
            .db
            .cf_handle(CF_DEDUP)
            .ok_or_else(|| MetadataError::Internal("Dedup CF not found".to_string()))?;
        let key = format!("{}:{}", request.client_id.as_raw(), request.call_id);

        match self.db.get_cf(cf, key.as_bytes()) {
            Ok(Some(value)) => {
                let result: AppliedResult = decode_from_slice(&value, standard())
                    .map_err(|e| MetadataError::Internal(format!("Failed to deserialize AppliedResult: {}", e)))?
                    .0;
                DEDUP_LOOKUP_HIT_TOTAL.fetch_add(1, Ordering::Relaxed);
                Ok(Some(result))
            }
            Ok(None) => {
                DEDUP_LOOKUP_MISS_TOTAL.fetch_add(1, Ordering::Relaxed);
                Ok(None)
            }
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    /// Put applied result for idempotency.
    pub fn put_applied_result(&self, request: &DedupKey, result: AppliedResult) -> MetadataResult<()> {
        // Evict stale entries before inserting
        let now_ms = now_millis();
        let evicted_ttl = self.evict_expired(now_ms)?;
        if evicted_ttl > 0 {
            DEDUP_EVICTED_TTL_TOTAL.fetch_add(evicted_ttl as u64, Ordering::Relaxed);
        }

        let cf = self
            .db
            .cf_handle(CF_DEDUP)
            .ok_or_else(|| MetadataError::Internal("Dedup CF not found".to_string()))?;
        let key = format!("{}:{}", request.client_id.as_raw(), request.call_id);
        let mut result = result;
        let value = encode_to_vec(&result, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize AppliedResult: {}", e)))?;
        result.size_bytes = value.len() as u32;
        let value = encode_to_vec(&result, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize AppliedResult: {}", e)))?;

        self.db
            .put_cf(cf, key.as_bytes(), value)
            .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;

        // Enforce size bound after insertion to account for the new record.
        let evicted_size = self.enforce_size_bound()?;
        if evicted_size > 0 {
            DEDUP_EVICTED_SIZE_TOTAL.fetch_add(evicted_size as u64, Ordering::Relaxed);
        }

        // Update gauge
        let entries = self.count_dedup_entries()?;
        DEDUP_STORE_ENTRIES_GAUGE.store(entries as u64, Ordering::Relaxed);
        Ok(())
    }

    fn dedup_key_bytes(request: &DedupKey) -> Vec<u8> {
        format!("{}:{}", request.client_id.as_raw(), request.call_id).into_bytes()
    }

    fn batch_put_applied_result(
        batch: &mut WriteBatch,
        cf: &ColumnFamily,
        request: &DedupKey,
        result: AppliedResult,
    ) -> MetadataResult<()> {
        let mut result = result;
        let value = encode_to_vec(&result, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize AppliedResult: {}", e)))?;
        result.size_bytes = value.len() as u32;
        let value = encode_to_vec(&result, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize AppliedResult: {}", e)))?;
        batch.put_cf(cf, Self::dedup_key_bytes(request), value);
        Ok(())
    }

    fn batch_enforce_dedup_size_after_insert(
        &self,
        batch: &mut WriteBatch,
        cf: &ColumnFamily,
        inserted_key: &[u8],
        inserted_created_at_ms: u64,
    ) -> MetadataResult<usize> {
        let mut entries = Vec::new();
        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);
        for item in iter {
            let (key, value) =
                item.map_err(|e| MetadataError::Internal(format!("RocksDB iterator error (dedup size): {}", e)))?;
            if key.as_ref() == inserted_key {
                continue;
            }
            let applied: AppliedResult = match decode_from_slice(&value, standard()) {
                Ok((ar, _)) => ar,
                Err(_) => {
                    entries.push((0u64, key));
                    continue;
                }
            };
            entries.push((applied.created_at_ms, key));
        }

        entries.push((inserted_created_at_ms, inserted_key.to_vec().into_boxed_slice()));
        if entries.len() <= DEDUP_MAX_ENTRIES {
            return Ok(0);
        }

        entries.sort_by_key(|(ts, _)| *ts);
        let remove_count = entries.len() - DEDUP_MAX_ENTRIES;
        let mut evicted = 0usize;
        for (_, key) in entries.into_iter().take(remove_count) {
            if key.as_ref() == inserted_key {
                continue;
            }
            batch.delete_cf(cf, key);
            evicted += 1;
        }
        Ok(evicted)
    }

    fn refresh_dedup_gauge(&self) {
        match self.count_dedup_entries() {
            Ok(entries) => DEDUP_STORE_ENTRIES_GAUGE.store(entries as u64, Ordering::Relaxed),
            Err(err) => warn!(error = %err, "failed to refresh dedup entry gauge"),
        }
    }

    /// Atomically append dedup tracking to an existing RocksDB batch.
    pub fn commit_apply_batch(
        &self,
        mut batch: WriteBatch,
        request: &DedupKey,
        result: AppliedResult,
    ) -> MetadataResult<()> {
        let cf_dedup = self.cf(CF_DEDUP)?;
        let dedup_key = Self::dedup_key_bytes(request);
        let inserted_created_at_ms = result.created_at_ms;

        Self::batch_put_applied_result(&mut batch, cf_dedup, request, result)?;
        let evicted_size =
            self.batch_enforce_dedup_size_after_insert(&mut batch, cf_dedup, &dedup_key, inserted_created_at_ms)?;

        // Namespace apply paths use this boundary so mutation and dedup result
        // either all survive replay or all remain absent.
        self.write_batch(batch)?;

        if evicted_size > 0 {
            DEDUP_EVICTED_SIZE_TOTAL.fetch_add(evicted_size as u64, Ordering::Relaxed);
        }
        self.refresh_dedup_gauge();
        Ok(())
    }

    /// Atomically persist only dedup tracking.
    pub fn put_apply_result_atomic(&self, request: &DedupKey, result: AppliedResult) -> MetadataResult<()> {
        self.commit_apply_batch(WriteBatch::default(), request, result)
    }

    /// Get the authoritative route epoch used for stale-route validation.
    pub fn get_route_epoch(&self) -> MetadataResult<RouteEpoch> {
        let cf = self
            .db
            .cf_handle(CF_META)
            .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;

        match self.db.get_cf(cf, b"route_epoch") {
            Ok(Some(value)) => {
                let version: u64 = decode_from_slice(&value, standard())
                    .map_err(|e| MetadataError::Internal(format!("Failed to deserialize route_epoch: {}", e)))?
                    .0;
                Ok(RouteEpoch::new(version))
            }
            Ok(None) => Ok(RouteEpoch::new(1)), // Default epoch
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    /// Persist the authoritative route epoch used for stale-route validation.
    pub fn put_route_epoch(&self, epoch: RouteEpoch) -> MetadataResult<()> {
        let cf = self
            .db
            .cf_handle(CF_META)
            .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;
        let value = encode_to_vec(epoch.as_u64(), standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize route_epoch: {}", e)))?;

        self.db
            .put_cf(cf, b"route_epoch", value)
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
        let value = encode_to_vec(layout, standard())
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
            Ok(Some(value)) => {
                let (layout, _): (FileLayout, usize) = decode_from_slice(&value, standard())
                    .map_err(|e| MetadataError::Internal(format!("Failed to deserialize file layout: {}", e)))?;
                layout
                    .validate()
                    .map_err(|e| MetadataError::Internal(format!("Invalid file layout: {}", e)))?;
                Ok(layout)
            }
            Ok(None) => Err(MetadataError::NotFound(format!(
                "Layout not found for inode {}",
                inode_id
            ))),
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    fn batch_put_layout(
        batch: &mut WriteBatch,
        cf: &ColumnFamily,
        inode_id: InodeId,
        layout: FileLayout,
    ) -> MetadataResult<()> {
        let key = format!("layout:{}", inode_id.as_raw());
        let value = encode_to_vec(layout, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize file layout: {}", e)))?;
        batch.put_cf(cf, key.as_bytes(), value);
        Ok(())
    }

    /// Get shard group info.
    pub fn get_shard_group(&self, group_name: &GroupName) -> MetadataResult<Option<ShardGroupInfo>> {
        let cf = self
            .db
            .cf_handle(CF_SHARD_GROUPS)
            .ok_or_else(|| MetadataError::Internal("ShardGroups CF not found".to_string()))?;
        let key = group_name.as_str();

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
        let key = info.group_name.as_str();
        let value = encode_to_vec(info, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize ShardGroupInfo: {}", e)))?;

        self.db
            .put_cf(cf, key.as_bytes(), value)
            .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
        Ok(())
    }

    fn batch_put_shard_group(batch: &mut WriteBatch, cf: &ColumnFamily, info: &ShardGroupInfo) -> MetadataResult<()> {
        let key = info.group_name.as_str();
        let value = encode_to_vec(info, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize ShardGroupInfo: {}", e)))?;
        batch.put_cf(cf, key.as_bytes(), value);
        Ok(())
    }

    /// Put shard to group routing mapping.
    pub fn put_shard_routing(&self, shard_id: ShardId, group_name: &GroupName) -> MetadataResult<()> {
        let cf = self
            .db
            .cf_handle(CF_SHARD_ROUTING)
            .ok_or_else(|| MetadataError::Internal("ShardRouting CF not found".to_string()))?;
        let key = format!("{}", shard_id.as_raw());
        let value = group_name.as_str();

        self.db
            .put_cf(cf, key.as_bytes(), value.as_bytes())
            .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
        Ok(())
    }

    fn batch_put_shard_routing(batch: &mut WriteBatch, cf: &ColumnFamily, shard_id: ShardId, group_name: &GroupName) {
        let key = format!("{}", shard_id.as_raw());
        let value = group_name.as_str();
        batch.put_cf(cf, key.as_bytes(), value.as_bytes());
    }

    /// Get shard to group routing mapping.
    pub fn get_shard_routing(&self, shard_id: ShardId) -> MetadataResult<Option<GroupName>> {
        let cf = self
            .db
            .cf_handle(CF_SHARD_ROUTING)
            .ok_or_else(|| MetadataError::Internal("ShardRouting CF not found".to_string()))?;
        let key = format!("{}", shard_id.as_raw());

        match self.db.get_cf(cf, key.as_bytes()) {
            Ok(Some(value)) => {
                let group_name = String::from_utf8(value)
                    .map_err(|e| MetadataError::Internal(format!("Failed to parse group_name: {}", e)))?;
                let group_name = GroupName::parse(group_name)
                    .map_err(|e| MetadataError::Internal(format!("Failed to parse group_name value: {}", e)))?;
                Ok(Some(group_name))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    /// Load all shard to group routing mappings.
    pub fn load_all_shard_routings(&self) -> MetadataResult<std::collections::HashMap<ShardId, GroupName>> {
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

            let group_name = String::from_utf8(value.to_vec())
                .map_err(|e| MetadataError::Internal(format!("Failed to parse group_name value: {}", e)))?;
            let group_name = GroupName::parse(group_name)
                .map_err(|e| MetadataError::Internal(format!("Failed to parse group_name value: {}", e)))?;

            mappings.insert(shard_id, group_name);
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

    fn batch_put_mount(batch: &mut WriteBatch, cf: &ColumnFamily, entry: &MountEntry) -> MetadataResult<()> {
        let key = format!("{}", entry.mount_id.as_raw());
        let value = encode_to_vec(entry, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize MountEntry: {}", e)))?;
        batch.put_cf(cf, key.as_bytes(), value);
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

    fn batch_put_route_epoch(batch: &mut WriteBatch, cf: &ColumnFamily, epoch: RouteEpoch) -> MetadataResult<()> {
        let value = encode_to_vec(epoch.as_u64(), standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize route_epoch: {}", e)))?;
        batch.put_cf(cf, b"route_epoch", value);
        Ok(())
    }

    fn batch_put_mount_version(batch: &mut WriteBatch, cf: &ColumnFamily, version: u64) -> MetadataResult<()> {
        let value = encode_to_vec(version, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize mount_version: {}", e)))?;
        batch.put_cf(cf, b"mount_version", value);
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
        let next_id_value = encode_to_vec(next_id, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize next_data_handle_id: {}", e)))?;
        batch.put_cf(cf_meta, b"next_data_handle_id", next_id_value);

        self.db
            .write(batch)
            .map_err(|e| MetadataError::Internal(format!("RocksDB write error: {}", e)))?;

        Ok(DataHandleId::new(current_id))
    }

    /// Read the durable next inode ID allocator value.
    pub fn get_next_inode_id(&self) -> MetadataResult<Option<InodeId>> {
        let cf_meta = self
            .db
            .cf_handle(CF_META)
            .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;

        match self.db.get_cf(cf_meta, NEXT_INODE_ID_KEY) {
            Ok(Some(value)) => {
                let id: u64 = decode_from_slice(&value, standard())
                    .map_err(|e| MetadataError::Internal(format!("Failed to deserialize next_inode_id: {}", e)))?
                    .0;
                Ok(Some(InodeId::new(id)))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    /// Persist the durable next inode ID allocator value.
    pub fn set_next_inode_id(&self, next_inode_id: InodeId) -> MetadataResult<()> {
        let cf_meta = self
            .db
            .cf_handle(CF_META)
            .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;
        let value = encode_to_vec(next_inode_id.as_raw(), standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize next_inode_id: {}", e)))?;

        self.db
            .put_cf(cf_meta, NEXT_INODE_ID_KEY, value)
            .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
        Ok(())
    }

    /// Allocate an inode ID from replicated RocksDB state.
    pub fn allocate_inode_id(&self) -> MetadataResult<InodeId> {
        let cf_meta = self
            .db
            .cf_handle(CF_META)
            .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;

        let current_id = match self.get_next_inode_id()? {
            Some(id) => id.as_raw(),
            None => {
                // Migration fallback for stores created before the allocator was replicated:
                // derive the next value from existing inode keys once, then persist the allocator.
                self.max_inode_id()?.map(|id| id.as_raw() + 1).unwrap_or(2)
            }
        };

        let next_id = current_id + 1;
        let next_id_value = encode_to_vec(next_id, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize next_inode_id: {}", e)))?;
        let mut batch = WriteBatch::default();
        batch.put_cf(cf_meta, NEXT_INODE_ID_KEY, next_id_value);

        self.db
            .write(batch)
            .map_err(|e| MetadataError::Internal(format!("RocksDB write error: {}", e)))?;

        Ok(InodeId::new(current_id))
    }

    /// Persist mapping from data_handle_id -> inode_id for routing from data plane back to namespace.
    pub fn put_data_handle_owner(&self, data_handle_id: DataHandleId, inode_id: InodeId) -> MetadataResult<()> {
        let cf_meta = self
            .db
            .cf_handle(CF_META)
            .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;
        let key = format!("data_handle_owner:{}", data_handle_id.as_raw());
        let value = encode_to_vec(inode_id.as_raw(), standard())
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

    /// Put mount version.
    pub fn put_mount_version(&self, version: u64) -> MetadataResult<()> {
        let cf = self
            .db
            .cf_handle(CF_META)
            .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;
        let value = encode_to_vec(version, standard())
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

    /// Get worker info accepted by a metadata group.
    pub fn get_worker_in_group(
        &self,
        group_name: &GroupName,
        worker_id: WorkerId,
    ) -> MetadataResult<Option<WorkerInfo>> {
        let cf = self
            .db
            .cf_handle(CF_WORKERS)
            .ok_or_else(|| MetadataError::Internal("Workers CF not found".to_string()))?;
        let key = worker_key(group_name, worker_id);

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
        let key = worker_key(&info.group_name, info.worker_id);
        let value = encode_to_vec(info, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize WorkerInfo: {}", e)))?;

        self.db
            .put_cf(cf, key.as_bytes(), value)
            .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
        Ok(())
    }

    fn batch_put_worker(batch: &mut WriteBatch, cf: &ColumnFamily, info: &WorkerInfo) -> MetadataResult<()> {
        let key = worker_key(&info.group_name, info.worker_id);
        let value = encode_to_vec(info, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize WorkerInfo: {}", e)))?;
        batch.put_cf(cf, key.as_bytes(), value);
        Ok(())
    }

    pub fn prepare_worker_registration(
        &self,
        group_name: GroupName,
        worker_id: WorkerId,
        address: String,
        worker_net_protocol: i32,
        fault_domain: Option<String>,
    ) -> MetadataResult<WorkerInfo> {
        if worker_id.as_raw() == 0 {
            return Err(MetadataError::InvalidArgument(
                "worker_id must be non-zero for registration".to_string(),
            ));
        }
        Ok(WorkerInfo {
            group_name,
            worker_id,
            address,
            worker_net_protocol,
            capacity_total: 0,
            capacity_used: 0,
            capacity_available: 0,
            active_reads: 0,
            active_writes: 0,
            health: crate::worker::HealthStatus::Healthy,
            last_heartbeat: 0,
            fault_domain,
        })
    }

    pub fn register_worker_with_apply_result_atomic(
        &self,
        info: &WorkerInfo,
        dedup_key: &DedupKey,
        applied_result: AppliedResult,
    ) -> MetadataResult<()> {
        let cf_workers = self.cf(CF_WORKERS)?;
        if info.worker_id.as_raw() == 0 {
            return Err(MetadataError::InvalidArgument(
                "worker_id must be non-zero for registration".to_string(),
            ));
        }

        let mut batch = WriteBatch::default();
        Self::batch_put_worker(&mut batch, cf_workers, info)?;
        self.commit_apply_batch(batch, dedup_key, applied_result)
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
        let value = encode_to_vec(count, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize ref count: {}", e)))?;

        self.db
            .put_cf(cf, key.as_bytes(), value)
            .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
        Ok(())
    }

    fn batch_put_block_ref_count(
        batch: &mut WriteBatch,
        cf: &ColumnFamily,
        block_id: BlockId,
        count: u64,
    ) -> MetadataResult<()> {
        let key = format!("{}", block_id);
        let value = encode_to_vec(count, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize ref count: {}", e)))?;
        batch.put_cf(cf, key.as_bytes(), value);
        Ok(())
    }

    fn batch_delete_block_ref_count(batch: &mut WriteBatch, cf: &ColumnFamily, block_id: BlockId) {
        let key = format!("{}", block_id);
        batch.delete_cf(cf, key.as_bytes());
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

    fn batch_put_delete_intent(
        batch: &mut WriteBatch,
        cf: &ColumnFamily,
        intent: &crate::state::DeleteIntent,
    ) -> MetadataResult<()> {
        let key = format!("{}", intent.intent_id);
        let value = encode_to_vec(intent, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize DeleteIntent: {}", e)))?;
        batch.put_cf(cf, key.as_bytes(), value);
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

    pub fn update_delete_intent_status_with_apply_result_atomic(
        &self,
        intent_id: u64,
        status: DeleteIntentStatus,
        finished_at_ms: Option<u64>,
        error_msg: Option<String>,
        dedup_key: &DedupKey,
        applied_result: AppliedResult,
    ) -> MetadataResult<()> {
        let cf_delete_intents = self.cf(CF_DELETE_INTENTS)?;
        let mut intent = self
            .get_delete_intent(intent_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Delete intent not found: {}", intent_id)))?;

        if !Self::delete_intent_status_transition_allowed(intent.status, status) {
            return Err(MetadataError::InvalidArgument(format!(
                "invalid delete intent status transition for {}: {:?} -> {:?}",
                intent_id, intent.status, status
            )));
        }

        intent.status = status;
        intent.finished_at_ms = finished_at_ms;
        intent.last_error_msg = error_msg;

        let mut batch = WriteBatch::default();
        Self::batch_put_delete_intent(&mut batch, cf_delete_intents, &intent)?;
        self.commit_apply_batch(batch, dedup_key, applied_result)
    }

    fn delete_intent_status_transition_allowed(from: DeleteIntentStatus, to: DeleteIntentStatus) -> bool {
        matches!(
            (from, to),
            (DeleteIntentStatus::Pending, DeleteIntentStatus::Pending)
                | (DeleteIntentStatus::Pending, DeleteIntentStatus::Completed)
                | (DeleteIntentStatus::Pending, DeleteIntentStatus::Failed)
                | (DeleteIntentStatus::InFlight, DeleteIntentStatus::InFlight)
                | (DeleteIntentStatus::InFlight, DeleteIntentStatus::Completed)
                | (DeleteIntentStatus::InFlight, DeleteIntentStatus::Failed)
                | (DeleteIntentStatus::Completed, DeleteIntentStatus::Completed)
                | (DeleteIntentStatus::Failed, DeleteIntentStatus::Failed)
        )
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

    fn stored_next_delete_intent_id(&self) -> MetadataResult<Option<u64>> {
        let cf_meta = self.cf(CF_META)?;
        match self.db.get_cf(cf_meta, NEXT_DELETE_INTENT_ID_KEY) {
            Ok(Some(value)) => {
                let id: u64 = decode_from_slice(&value, standard())
                    .map_err(|e| {
                        MetadataError::Internal(format!("Failed to deserialize next_delete_intent_id: {}", e))
                    })?
                    .0;
                Ok(Some(id))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    fn max_delete_intent_id(&self) -> MetadataResult<Option<u64>> {
        use rocksdb::IteratorMode;

        let cf = self.cf(CF_DELETE_INTENTS)?;
        let iter = self.db.iterator_cf(cf, IteratorMode::Start);
        let mut max_id = None;
        for item in iter {
            let (key, value) = item.map_err(|e| MetadataError::Internal(format!("RocksDB iterator error: {}", e)))?;
            let key_str = std::str::from_utf8(&key)
                .map_err(|e| MetadataError::Internal(format!("Malformed delete intent key: {}", e)))?;
            let key_id = key_str
                .parse::<u64>()
                .map_err(|e| MetadataError::Internal(format!("Malformed delete intent id key: {}", e)))?;
            let intent: crate::state::DeleteIntent = decode_from_slice(&value, standard())
                .map_err(|e| MetadataError::Internal(format!("Failed to deserialize DeleteIntent: {}", e)))?
                .0;
            if intent.intent_id != key_id {
                return Err(MetadataError::Internal(format!(
                    "Delete intent key/value id mismatch: key={}, value={}",
                    key_id, intent.intent_id
                )));
            }
            max_id = Some(max_id.map_or(key_id, |current: u64| current.max(key_id)));
        }
        Ok(max_id)
    }

    fn reserve_delete_intent_ids_in_batch(
        &self,
        batch: &mut WriteBatch,
        cf_meta: &ColumnFamily,
        count: u64,
    ) -> MetadataResult<u64> {
        let current = self.stored_next_delete_intent_id()?.unwrap_or(1);
        let max_existing_next = self
            .max_delete_intent_id()?
            .map(|id| {
                id.checked_add(1)
                    .ok_or_else(|| MetadataError::Internal("delete intent id allocator overflow".to_string()))
            })
            .transpose()?
            .unwrap_or(1);
        let first_id = current.max(max_existing_next);
        let next_id = first_id
            .checked_add(count)
            .ok_or_else(|| MetadataError::Internal("delete intent id allocator overflow".to_string()))?;
        for intent_id in first_id..next_id {
            if self.get_delete_intent(intent_id)?.is_some() {
                return Err(MetadataError::Internal(format!(
                    "Reserved delete intent id already exists: {}",
                    intent_id
                )));
            }
        }
        let value = encode_to_vec(next_id, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize next_delete_intent_id: {}", e)))?;
        batch.put_cf(cf_meta, NEXT_DELETE_INTENT_ID_KEY, value);
        Ok(first_id)
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

    fn batch_put_inode(batch: &mut WriteBatch, cf: &ColumnFamily, inode: &Inode) -> MetadataResult<()> {
        let key = Self::encode_inode_key(inode.inode_id);
        let value = serde_json::to_vec(inode)
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize Inode: {}", e)))?;
        batch.put_cf(cf, key, value);
        Ok(())
    }

    /// Atomically persist a single inode update with apply tracking.
    pub fn put_inode_with_apply_result_atomic(
        &self,
        inode: &Inode,
        dedup_key: &DedupKey,
        applied_result: AppliedResult,
    ) -> MetadataResult<()> {
        let cf_inodes = self.cf(CF_INODES)?;
        let mut batch = WriteBatch::default();
        Self::batch_put_inode(&mut batch, cf_inodes, inode)?;
        self.commit_apply_batch(batch, dedup_key, applied_result)
    }

    /// Atomically persist a CloseWrite commit with replay tracking.
    // Atomic storage helpers keep every column-family mutation visible at the call boundary.
    #[allow(clippy::too_many_arguments)]
    pub fn close_write_with_apply_result_atomic(
        &self,
        inode: &Inode,
        layout: FileLayout,
        block_ref_increments: &[BlockId],
        block_ref_decrements: &[BlockId],
        now_ms: u64,
        dedup_key: &DedupKey,
        applied_result: AppliedResult,
    ) -> MetadataResult<()> {
        let cf_inodes = self.cf(CF_INODES)?;
        let cf_meta = self.cf(CF_META)?;
        let cf_block_ref_counts = self.cf(CF_BLOCK_REF_COUNTS)?;
        let cf_delete_intents = self.cf(CF_DELETE_INTENTS)?;
        let mut batch = WriteBatch::default();

        Self::batch_put_inode(&mut batch, cf_inodes, inode)?;
        Self::batch_put_layout(&mut batch, cf_meta, inode.inode_id, layout)?;

        let mut seen = std::collections::HashSet::with_capacity(block_ref_increments.len());
        for block_id in block_ref_increments {
            if seen.insert(*block_id) {
                let new_count = self.get_block_ref_count(*block_id)?.unwrap_or(0) + 1;
                Self::batch_put_block_ref_count(&mut batch, cf_block_ref_counts, *block_id, new_count)?;
            }
        }
        self.append_block_ref_decrements_to_batch(
            &mut batch,
            cf_block_ref_counts,
            cf_delete_intents,
            block_ref_decrements,
            now_ms,
        )?;

        self.commit_apply_batch(batch, dedup_key, applied_result)
    }

    /// Atomically persist a mount creation/update with apply tracking.
    pub fn create_mount_with_apply_result_atomic(
        &self,
        entry: &MountEntry,
        root_inode_to_create: Option<&Inode>,
        mount_version: u64,
        route_epoch: RouteEpoch,
        dedup_key: &DedupKey,
        applied_result: AppliedResult,
    ) -> MetadataResult<()> {
        let cf_mounts = self.cf(CF_MOUNTS)?;
        let cf_meta = self.cf(CF_META)?;
        let cf_inodes = self.cf(CF_INODES)?;
        let mut batch = WriteBatch::default();
        if let Some(inode) = root_inode_to_create {
            Self::batch_put_inode(&mut batch, cf_inodes, inode)?;
        }
        Self::batch_put_mount(&mut batch, cf_mounts, entry)?;
        Self::batch_put_mount_version(&mut batch, cf_meta, mount_version)?;
        Self::batch_put_route_epoch(&mut batch, cf_meta, route_epoch)?;
        self.commit_apply_batch(batch, dedup_key, applied_result)
    }

    /// Atomically persist a mount deletion with apply tracking.
    pub fn delete_mount_with_apply_result_atomic(
        &self,
        mount_id: MountId,
        mount_version: u64,
        route_epoch: RouteEpoch,
        dedup_key: &DedupKey,
        applied_result: AppliedResult,
    ) -> MetadataResult<()> {
        let cf_mounts = self.cf(CF_MOUNTS)?;
        let cf_meta = self.cf(CF_META)?;
        let mut batch = WriteBatch::default();
        let key = format!("{}", mount_id.as_raw());
        batch.delete_cf(cf_mounts, key.as_bytes());
        Self::batch_put_mount_version(&mut batch, cf_meta, mount_version)?;
        Self::batch_put_route_epoch(&mut batch, cf_meta, route_epoch)?;
        self.commit_apply_batch(batch, dedup_key, applied_result)
    }

    /// Atomically persist shard group registration and shard routing with apply tracking.
    pub fn add_shard_group_with_apply_result_atomic(
        &self,
        info: &ShardGroupInfo,
        shard_ids: &[ShardId],
        dedup_key: &DedupKey,
        applied_result: AppliedResult,
    ) -> MetadataResult<()> {
        let cf_groups = self.cf(CF_SHARD_GROUPS)?;
        let cf_routing = self.cf(CF_SHARD_ROUTING)?;
        let mut batch = WriteBatch::default();
        Self::batch_put_shard_group(&mut batch, cf_groups, info)?;
        for shard_id in shard_ids {
            Self::batch_put_shard_routing(&mut batch, cf_routing, *shard_id, &info.group_name);
        }
        self.commit_apply_batch(batch, dedup_key, applied_result)
    }

    /// Atomically persist a worker descriptor with apply tracking.
    pub fn upsert_worker_descriptor_with_apply_result_atomic(
        &self,
        info: &WorkerInfo,
        dedup_key: &DedupKey,
        applied_result: AppliedResult,
    ) -> MetadataResult<()> {
        let cf_workers = self.cf(CF_WORKERS)?;
        let mut batch = WriteBatch::default();
        Self::batch_put_worker(&mut batch, cf_workers, info)?;
        self.commit_apply_batch(batch, dedup_key, applied_result)
    }

    /// Atomically persist a create-file namespace mutation.
    pub fn create_file_atomic(
        &self,
        parent_inode_id: InodeId,
        name: &str,
        inode: &Inode,
        updated_parent: &Inode,
        layout: FileLayout,
    ) -> MetadataResult<()> {
        self.write_batch(self.create_file_batch(parent_inode_id, name, inode, updated_parent, layout)?)
    }

    fn create_file_batch(
        &self,
        parent_inode_id: InodeId,
        name: &str,
        inode: &Inode,
        updated_parent: &Inode,
        layout: FileLayout,
    ) -> MetadataResult<WriteBatch> {
        let cf_inodes = self.cf(CF_INODES)?;
        let cf_dentries = self.cf(CF_DENTRIES)?;
        let cf_meta = self.cf(CF_META)?;

        let mut batch = WriteBatch::default();
        Self::batch_put_inode(&mut batch, cf_inodes, inode)?;
        Self::batch_put_inode(&mut batch, cf_inodes, updated_parent)?;
        batch.put_cf(
            cf_dentries,
            Self::encode_dentry_key(parent_inode_id, name),
            inode.inode_id.to_be_bytes(),
        );

        let layout_key = format!("layout:{}", inode.inode_id.as_raw());
        let layout_value = encode_to_vec(layout, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize file layout: {}", e)))?;
        batch.put_cf(cf_meta, layout_key.as_bytes(), layout_value);

        let data_handle_id = inode.current_data_handle_id;
        let owner_key = format!("data_handle_owner:{}", data_handle_id.as_raw());
        let owner_value = encode_to_vec(inode.inode_id.as_raw(), standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize inode_id: {}", e)))?;
        batch.put_cf(cf_meta, owner_key.as_bytes(), owner_value);

        Ok(batch)
    }

    /// Atomically persist create-file mutation with apply tracking.
    // Atomic storage helpers keep every column-family mutation visible at the call boundary.
    #[allow(clippy::too_many_arguments)]
    pub fn create_file_with_apply_result_atomic(
        &self,
        parent_inode_id: InodeId,
        name: &str,
        inode: &Inode,
        updated_parent: &Inode,
        layout: FileLayout,
        dedup_key: &DedupKey,
        applied_result: AppliedResult,
    ) -> MetadataResult<()> {
        let batch = self.create_file_batch(parent_inode_id, name, inode, updated_parent, layout)?;
        self.commit_apply_batch(batch, dedup_key, applied_result)
    }

    /// Atomically persist a mkdir namespace mutation.
    pub fn create_dir_atomic(
        &self,
        parent_inode_id: InodeId,
        name: &str,
        inode: &Inode,
        updated_parent: &Inode,
    ) -> MetadataResult<()> {
        self.write_batch(self.create_dir_batch(parent_inode_id, name, inode, updated_parent)?)
    }

    fn create_dir_batch(
        &self,
        parent_inode_id: InodeId,
        name: &str,
        inode: &Inode,
        updated_parent: &Inode,
    ) -> MetadataResult<WriteBatch> {
        let cf_inodes = self.cf(CF_INODES)?;
        let cf_dentries = self.cf(CF_DENTRIES)?;

        let mut batch = WriteBatch::default();
        Self::batch_put_inode(&mut batch, cf_inodes, inode)?;
        Self::batch_put_inode(&mut batch, cf_inodes, updated_parent)?;
        batch.put_cf(
            cf_dentries,
            Self::encode_dentry_key(parent_inode_id, name),
            inode.inode_id.to_be_bytes(),
        );

        Ok(batch)
    }

    /// Atomically persist mkdir mutation with apply tracking.
    pub fn create_dir_with_apply_result_atomic(
        &self,
        parent_inode_id: InodeId,
        name: &str,
        inode: &Inode,
        updated_parent: &Inode,
        dedup_key: &DedupKey,
        applied_result: AppliedResult,
    ) -> MetadataResult<()> {
        let batch = self.create_dir_batch(parent_inode_id, name, inode, updated_parent)?;
        self.commit_apply_batch(batch, dedup_key, applied_result)
    }

    fn delete_dentry_inode_batch(
        &self,
        parent_inode_id: InodeId,
        name: &str,
        inode_id: InodeId,
        updated_parent: &Inode,
    ) -> MetadataResult<WriteBatch> {
        let cf_inodes = self.cf(CF_INODES)?;
        let cf_dentries = self.cf(CF_DENTRIES)?;

        let mut batch = WriteBatch::default();
        batch.delete_cf(cf_dentries, Self::encode_dentry_key(parent_inode_id, name));
        batch.delete_cf(cf_inodes, Self::encode_inode_key(inode_id));
        Self::batch_put_inode(&mut batch, cf_inodes, updated_parent)?;
        Ok(batch)
    }

    /// Atomically persist empty-directory deletion with apply tracking.
    pub fn delete_empty_dir_with_apply_result_atomic(
        &self,
        parent_inode_id: InodeId,
        name: &str,
        inode_id: InodeId,
        updated_parent: &Inode,
        dedup_key: &DedupKey,
        applied_result: AppliedResult,
    ) -> MetadataResult<()> {
        let batch = self.delete_dentry_inode_batch(parent_inode_id, name, inode_id, updated_parent)?;
        self.commit_apply_batch(batch, dedup_key, applied_result)
    }

    /// Atomically persist non-directory deletion with namespace and optional data-handle cleanup.
    // Atomic storage helpers keep every column-family mutation visible at the call boundary.
    #[allow(clippy::too_many_arguments)]
    pub fn delete_empty_file_with_apply_result_atomic(
        &self,
        parent_inode_id: InodeId,
        name: &str,
        inode_id: InodeId,
        data_handle_id: Option<DataHandleId>,
        updated_parent: &Inode,
        dedup_key: &DedupKey,
        applied_result: AppliedResult,
    ) -> MetadataResult<()> {
        let cf_meta = self.cf(CF_META)?;
        let mut batch = self.delete_dentry_inode_batch(parent_inode_id, name, inode_id, updated_parent)?;
        let layout_key = format!("layout:{}", inode_id.as_raw());
        batch.delete_cf(cf_meta, layout_key.as_bytes());
        if let Some(data_handle_id) = data_handle_id {
            let owner_key = format!("data_handle_owner:{}", data_handle_id.as_raw());
            batch.delete_cf(cf_meta, owner_key.as_bytes());
        }
        self.commit_apply_batch(batch, dedup_key, applied_result)
    }

    fn append_block_ref_decrements_to_batch(
        &self,
        batch: &mut WriteBatch,
        cf_block_ref_counts: &ColumnFamily,
        cf_delete_intents: &ColumnFamily,
        released_block_ids: &[BlockId],
        now_ms: u64,
    ) -> MetadataResult<()> {
        let mut seen = std::collections::HashSet::with_capacity(released_block_ids.len());
        let mut unique_blocks = Vec::with_capacity(released_block_ids.len());
        for block_id in released_block_ids {
            if seen.insert(*block_id) {
                unique_blocks.push(*block_id);
            }
        }
        unique_blocks.sort_by_key(|block_id| (block_id.data_handle_id.as_raw(), block_id.index.as_raw()));
        let decrement_counts = unique_blocks
            .into_iter()
            .map(|block_id| (block_id, 1))
            .collect::<Vec<_>>();

        self.append_block_ref_decrement_counts_to_batch(
            batch,
            cf_block_ref_counts,
            cf_delete_intents,
            &decrement_counts,
            now_ms,
        )
    }

    fn append_block_ref_decrement_counts_to_batch(
        &self,
        batch: &mut WriteBatch,
        cf_block_ref_counts: &ColumnFamily,
        cf_delete_intents: &ColumnFamily,
        block_ref_decrements: &[(BlockId, u64)],
        now_ms: u64,
    ) -> MetadataResult<()> {
        let cf_meta = self.cf(CF_META)?;
        let mut zero_ref_blocks = Vec::new();
        for (block_id, decrement) in block_ref_decrements {
            let current = self.get_block_ref_count(*block_id)?.ok_or_else(|| {
                MetadataError::InvalidArgument(format!("Missing block refcount for released block {}", block_id))
            })?;
            if *decrement == 0 || current < *decrement {
                return Err(MetadataError::InvalidArgument(format!(
                    "Block refcount underflow for released block {}",
                    block_id
                )));
            }

            if current == *decrement {
                Self::batch_delete_block_ref_count(batch, cf_block_ref_counts, *block_id);
                zero_ref_blocks.push(*block_id);
            } else {
                Self::batch_put_block_ref_count(batch, cf_block_ref_counts, *block_id, current - *decrement)?;
            }
        }

        if !zero_ref_blocks.is_empty() {
            let first_intent_id =
                self.reserve_delete_intent_ids_in_batch(batch, cf_meta, zero_ref_blocks.len() as u64)?;
            for (offset, block_id) in zero_ref_blocks.into_iter().enumerate() {
                let intent = crate::state::DeleteIntent {
                    intent_id: first_intent_id + offset as u64,
                    block_id,
                    reason: crate::state::DeleteIntentReason::Gc,
                    created_at_ms: now_ms,
                    not_before_ms: now_ms,
                    group_name: None,
                    guard_watermark: None,
                    mount_epoch: None,
                    guard_state_id: types::RaftLogId {
                        term: 0,
                        leader_node_id: 0,
                        index: 0,
                    },
                    target_workers: Vec::new(),
                    status: crate::state::DeleteIntentStatus::Pending,
                    finished_at_ms: None,
                    last_error_msg: None,
                };
                Self::batch_put_delete_intent(batch, cf_delete_intents, &intent)?;
            }
        }
        Ok(())
    }

    /// Atomically persist a recursive tree delete with block lifecycle updates and apply tracking.
    pub fn delete_tree_with_apply_result_atomic(
        &self,
        update: DeleteTreeAtomicUpdate<'_>,
        dedup_key: &DedupKey,
        applied_result: AppliedResult,
    ) -> MetadataResult<()> {
        let cf_inodes = self.cf(CF_INODES)?;
        let cf_dentries = self.cf(CF_DENTRIES)?;
        let cf_meta = self.cf(CF_META)?;
        let cf_block_ref_counts = self.cf(CF_BLOCK_REF_COUNTS)?;
        let cf_delete_intents = self.cf(CF_DELETE_INTENTS)?;
        let mut batch = WriteBatch::default();

        for entry in update.entries {
            batch.delete_cf(cf_dentries, Self::encode_dentry_key(entry.parent_inode_id, &entry.name));
            batch.delete_cf(cf_inodes, Self::encode_inode_key(entry.inode_id));
            if entry.layout.is_some() {
                let layout_key = format!("layout:{}", entry.inode_id.as_raw());
                batch.delete_cf(cf_meta, layout_key.as_bytes());
            }
            if let Some(data_handle_id) = entry.data_handle_id {
                let owner_key = format!("data_handle_owner:{}", data_handle_id.as_raw());
                batch.delete_cf(cf_meta, owner_key.as_bytes());
            }
        }
        Self::batch_put_inode(&mut batch, cf_inodes, update.updated_parent)?;
        self.append_block_ref_decrement_counts_to_batch(
            &mut batch,
            cf_block_ref_counts,
            cf_delete_intents,
            update.block_ref_decrements,
            update.now_ms,
        )?;

        self.commit_apply_batch(batch, dedup_key, applied_result)
    }

    /// Atomically persist delete intents with apply tracking.
    pub fn create_delete_intents_with_apply_result_atomic(
        &self,
        intents: Vec<crate::state::DeleteIntent>,
        dedup_key: &DedupKey,
        applied_result: AppliedResult,
    ) -> MetadataResult<()> {
        let cf_delete_intents = self.cf(CF_DELETE_INTENTS)?;
        let mut seen = std::collections::HashSet::with_capacity(intents.len());
        for intent in &intents {
            if !seen.insert(intent.intent_id) {
                return Err(MetadataError::InvalidArgument(format!(
                    "Duplicate delete intent id in command: {}",
                    intent.intent_id
                )));
            }
        }
        for intent in &intents {
            if self.get_delete_intent(intent.intent_id)?.is_some() {
                return Err(MetadataError::InvalidArgument(format!(
                    "Delete intent id already exists: {}",
                    intent.intent_id
                )));
            }
        }

        let mut batch = WriteBatch::default();
        for mut intent in intents {
            intent.status = crate::state::DeleteIntentStatus::Pending;
            intent.finished_at_ms = None;
            intent.last_error_msg = None;
            Self::batch_put_delete_intent(&mut batch, cf_delete_intents, &intent)?;
        }
        self.commit_apply_batch(batch, dedup_key, applied_result)
    }

    pub fn allocate_delete_intents_with_apply_result_atomic(
        &self,
        intents: Vec<crate::state::DeleteIntent>,
        dedup_key: &DedupKey,
        applied_result: AppliedResult,
    ) -> MetadataResult<()> {
        let cf_meta = self.cf(CF_META)?;
        let cf_delete_intents = self.cf(CF_DELETE_INTENTS)?;
        for intent in &intents {
            if intent.intent_id != 0 {
                return Err(MetadataError::InvalidArgument(format!(
                    "Allocated delete intent command must use intent_id=0, got {}",
                    intent.intent_id
                )));
            }
        }

        let mut batch = WriteBatch::default();
        if !intents.is_empty() {
            let first_intent_id = self.reserve_delete_intent_ids_in_batch(&mut batch, cf_meta, intents.len() as u64)?;
            for (offset, mut intent) in intents.into_iter().enumerate() {
                intent.intent_id = first_intent_id + offset as u64;
                intent.status = crate::state::DeleteIntentStatus::Pending;
                intent.finished_at_ms = None;
                intent.last_error_msg = None;
                Self::batch_put_delete_intent(&mut batch, cf_delete_intents, &intent)?;
            }
        }
        self.commit_apply_batch(batch, dedup_key, applied_result)
    }

    /// Atomically persist truncate shrink effects with block lifecycle updates and apply tracking.
    pub fn truncate_file_with_apply_result_atomic(
        &self,
        inode: &Inode,
        layout: FileLayout,
        released_block_ids: &[BlockId],
        now_ms: u64,
        dedup_key: &DedupKey,
        applied_result: AppliedResult,
    ) -> MetadataResult<()> {
        let cf_inodes = self.cf(CF_INODES)?;
        let cf_meta = self.cf(CF_META)?;
        let cf_block_ref_counts = self.cf(CF_BLOCK_REF_COUNTS)?;
        let cf_delete_intents = self.cf(CF_DELETE_INTENTS)?;
        let mut batch = WriteBatch::default();

        Self::batch_put_inode(&mut batch, cf_inodes, inode)?;
        Self::batch_put_layout(&mut batch, cf_meta, inode.inode_id, layout)?;
        self.append_block_ref_decrements_to_batch(
            &mut batch,
            cf_block_ref_counts,
            cf_delete_intents,
            released_block_ids,
            now_ms,
        )?;

        self.commit_apply_batch(batch, dedup_key, applied_result)
    }

    /// Atomically persist extent-bearing file deletion with block lifecycle updates and apply tracking.
    // Atomic storage helpers keep every column-family mutation visible at the call boundary.
    #[allow(clippy::too_many_arguments)]
    pub fn delete_file_with_extents_and_apply_result_atomic(
        &self,
        parent_inode_id: InodeId,
        name: &str,
        inode_id: InodeId,
        data_handle_id: DataHandleId,
        updated_parent: &Inode,
        released_block_ids: &[BlockId],
        now_ms: u64,
        dedup_key: &DedupKey,
        applied_result: AppliedResult,
    ) -> MetadataResult<()> {
        let cf_meta = self.cf(CF_META)?;
        let cf_block_ref_counts = self.cf(CF_BLOCK_REF_COUNTS)?;
        let cf_delete_intents = self.cf(CF_DELETE_INTENTS)?;
        let mut batch = self.delete_dentry_inode_batch(parent_inode_id, name, inode_id, updated_parent)?;

        let layout_key = format!("layout:{}", inode_id.as_raw());
        batch.delete_cf(cf_meta, layout_key.as_bytes());
        let owner_key = format!("data_handle_owner:{}", data_handle_id.as_raw());
        batch.delete_cf(cf_meta, owner_key.as_bytes());
        self.append_block_ref_decrements_to_batch(
            &mut batch,
            cf_block_ref_counts,
            cf_delete_intents,
            released_block_ids,
            now_ms,
        )?;

        self.commit_apply_batch(batch, dedup_key, applied_result)
    }

    /// Atomically persist a rename namespace mutation.
    pub fn rename_atomic(&self, update: RenameAtomicUpdate<'_>) -> MetadataResult<()> {
        self.write_batch(self.rename_batch(update)?)
    }

    fn rename_batch(&self, update: RenameAtomicUpdate<'_>) -> MetadataResult<WriteBatch> {
        let cf_inodes = self.cf(CF_INODES)?;
        let cf_dentries = self.cf(CF_DENTRIES)?;
        let cf_meta = self.cf(CF_META)?;
        let cf_block_ref_counts = self.cf(CF_BLOCK_REF_COUNTS)?;
        let cf_delete_intents = self.cf(CF_DELETE_INTENTS)?;

        let mut batch = WriteBatch::default();

        if let Some(cleanup) = update.overwritten_target {
            batch.delete_cf(cf_inodes, Self::encode_inode_key(cleanup.inode_id));
            batch.delete_cf(
                cf_dentries,
                Self::encode_dentry_key(update.dst_parent_inode_id, update.dst_name),
            );
            let layout_key = format!("layout:{}", cleanup.inode_id.as_raw());
            batch.delete_cf(cf_meta, layout_key.as_bytes());
            if let Some(data_handle_id) = cleanup.data_handle_id {
                let owner_key = format!("data_handle_owner:{}", data_handle_id.as_raw());
                batch.delete_cf(cf_meta, owner_key.as_bytes());
            }
            self.append_block_ref_decrements_to_batch(
                &mut batch,
                cf_block_ref_counts,
                cf_delete_intents,
                cleanup.released_block_ids,
                cleanup.now_ms,
            )?;
        }

        batch.delete_cf(
            cf_dentries,
            Self::encode_dentry_key(update.src_parent_inode_id, update.src_name),
        );
        batch.put_cf(
            cf_dentries,
            Self::encode_dentry_key(update.dst_parent_inode_id, update.dst_name),
            update.src_inode_id.to_be_bytes(),
        );

        if let Some(parent) = update.updated_src_parent {
            Self::batch_put_inode(&mut batch, cf_inodes, parent)?;
        }
        if let Some(parent) = update.updated_dst_parent {
            Self::batch_put_inode(&mut batch, cf_inodes, parent)?;
        }
        Self::batch_put_inode(&mut batch, cf_inodes, update.updated_src_inode)?;

        Ok(batch)
    }

    /// Atomically persist rename mutation with apply tracking.
    pub fn rename_with_apply_result_atomic(
        &self,
        update: RenameAtomicUpdate<'_>,
        dedup_key: &DedupKey,
        applied_result: AppliedResult,
    ) -> MetadataResult<()> {
        let batch = self.rename_batch(update)?;
        self.commit_apply_batch(batch, dedup_key, applied_result)
    }

    /// Return the largest inode ID currently present in storage.
    pub fn max_inode_id(&self) -> MetadataResult<Option<InodeId>> {
        let cf = self
            .db
            .cf_handle(CF_INODES)
            .ok_or_else(|| MetadataError::Internal("Inodes CF not found".to_string()))?;

        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);
        let mut max_inode_id = None;
        for item in iter {
            let (key, _) =
                item.map_err(|e| MetadataError::Internal(format!("RocksDB iterator error (inodes): {}", e)))?;
            let key = key.as_ref();
            if !key.starts_with(b"inode/") || key.len() != b"inode/".len() + 8 {
                continue;
            }
            let mut raw = [0u8; 8];
            raw.copy_from_slice(&key[b"inode/".len()..]);
            let inode_id = InodeId::new(u64::from_be_bytes(raw));
            max_inode_id = Some(max_inode_id.map_or(inode_id, |current: InodeId| {
                if inode_id.as_raw() > current.as_raw() {
                    inode_id
                } else {
                    current
                }
            }));
        }

        Ok(max_inode_id)
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
    ) -> MetadataResult<DentryPage> {
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
            let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);
            let mut batch = WriteBatch::default();

            for item in iter {
                let (key, _) = item.map_err(|e| MetadataError::Internal(format!("RocksDB iterator error: {}", e)))?;
                batch.delete_cf(cf, key);

                if batch.size_in_bytes() >= batch_bytes {
                    self.write_batch(batch)?;
                    batch = WriteBatch::default();
                }
            }

            if !batch.is_empty() {
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

    fn evict_expired(&self, now_ms: u64) -> MetadataResult<usize> {
        let cf = self
            .db
            .cf_handle(CF_DEDUP)
            .ok_or_else(|| MetadataError::Internal("Dedup CF not found".to_string()))?;
        let mut evicted = 0usize;
        let mut batch = WriteBatch::default();
        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);
        for item in iter {
            let (key, value) =
                item.map_err(|e| MetadataError::Internal(format!("RocksDB iterator error (dedup ttl): {}", e)))?;
            let applied: AppliedResult = match decode_from_slice(&value, standard()) {
                Ok((ar, _)) => ar,
                Err(_) => {
                    // If corrupt, evict defensively
                    batch.delete_cf(cf, key);
                    evicted += 1;
                    continue;
                }
            };
            if now_ms.saturating_sub(applied.created_at_ms) > DEDUP_TTL_MS {
                batch.delete_cf(cf, key);
                evicted += 1;
            }
        }
        if evicted > 0 {
            self.write_batch(batch)?;
        }
        Ok(evicted)
    }

    fn enforce_size_bound(&self) -> MetadataResult<usize> {
        let cf = self
            .db
            .cf_handle(CF_DEDUP)
            .ok_or_else(|| MetadataError::Internal("Dedup CF not found".to_string()))?;
        let mut entries = Vec::new();
        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);
        for item in iter {
            let (key, value) =
                item.map_err(|e| MetadataError::Internal(format!("RocksDB iterator error (dedup size): {}", e)))?;
            let applied: AppliedResult = match decode_from_slice(&value, standard()) {
                Ok((ar, _)) => ar,
                Err(_) => {
                    // Corrupt entry: schedule removal
                    entries.push((0u64, key));
                    continue;
                }
            };
            entries.push((applied.created_at_ms, key));
        }
        if entries.len() <= DEDUP_MAX_ENTRIES {
            return Ok(0);
        }
        // Evict oldest
        entries.sort_by_key(|(ts, _)| *ts);
        let remove_count = entries.len() - DEDUP_MAX_ENTRIES;
        let mut batch = WriteBatch::default();
        for (_, key) in entries.into_iter().take(remove_count) {
            batch.delete_cf(cf, key);
        }
        if remove_count > 0 {
            self.write_batch(batch)?;
        }
        Ok(remove_count)
    }

    fn count_dedup_entries(&self) -> MetadataResult<usize> {
        let cf = self
            .db
            .cf_handle(CF_DEDUP)
            .ok_or_else(|| MetadataError::Internal("Dedup CF not found".to_string()))?;
        let iter = self.db.iterator_cf(cf, rocksdb::IteratorMode::Start);
        let mut count = 0usize;
        for item in iter {
            item.map_err(|e| MetadataError::Internal(format!("RocksDB iterator error (dedup count): {}", e)))?;
            count += 1;
        }
        Ok(count)
    }
}

fn cf_descriptors() -> Vec<ColumnFamilyDescriptor> {
    vec![
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
    ]
}

fn validate_existing_rocksdb_state(path: &Path, snapshot_dir: &Path) -> MetadataResult<()> {
    if !path.is_dir() {
        return Err(missing_rocksdb_state_error(path, "storage directory is missing"));
    }
    if !snapshot_dir.is_dir() {
        return Err(missing_rocksdb_state_error(path, "snapshot directory is missing"));
    }
    Ok(())
}

fn missing_rocksdb_state_error(path: &Path, detail: &str) -> MetadataError {
    MetadataError::InvalidArgument(format!(
        "metadata storage is formatted but RocksDB state is missing or corrupt at {}; {detail}; run `metadata format --config <path>` only on empty storage, or clean/reset manually",
        path.display()
    ))
}

fn reset_dedup_if_stale(db: &DB) -> MetadataResult<()> {
    let meta_cf = db
        .cf_handle(CF_META)
        .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;
    let dedup_cf = db
        .cf_handle(CF_DEDUP)
        .ok_or_else(|| MetadataError::Internal("Dedup CF not found".to_string()))?;

    let stored_version = match db.get_cf(meta_cf, DEDUP_VERSION_KEY) {
        Ok(Some(bytes)) => {
            let (v, _): (u64, _) = decode_from_slice(&bytes, standard())
                .map_err(|e| MetadataError::Internal(format!("Failed to decode dedup_version: {}", e)))?;
            v
        }
        Ok(None) => 0,
        Err(e) => return Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
    };

    if stored_version == DEDUP_FORMAT_VERSION {
        return Ok(());
    }

    let mut batch = WriteBatch::default();
    let iter = db.iterator_cf(dedup_cf, rocksdb::IteratorMode::Start);
    for item in iter {
        let (key, _) =
            item.map_err(|e| MetadataError::Internal(format!("RocksDB iterator error clearing dedup: {}", e)))?;
        batch.delete_cf(dedup_cf, key);
    }

    let version_bytes = encode_to_vec(DEDUP_FORMAT_VERSION, standard())
        .map_err(|e| MetadataError::Internal(format!("Failed to encode dedup_version: {}", e)))?;
    batch.put_cf(meta_cf, DEDUP_VERSION_KEY, version_bytes);
    db.write(batch)
        .map_err(|e| MetadataError::Internal(format!("RocksDB error clearing dedup: {}", e)))?;

    Ok(())
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use types::fs::{FileAttrs, Inode, InodeData, InodeId};
    use types::ids::MountId;
    use types::{CallId, ClientId};

    #[test]
    fn test_data_handle_allocator_unique_and_durable() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("allocator_db");
        let storage = RocksDBStorage::create_for_format(&db_path).unwrap();

        let first = storage.get_and_increment_data_handle_id().unwrap();
        let second = storage.get_and_increment_data_handle_id().unwrap();
        assert_ne!(first, second);
        assert!(second.as_raw() > first.as_raw());

        // Re-open to ensure durability.
        drop(storage);
        let reopened = RocksDBStorage::create_for_format(&db_path).unwrap();
        let third = reopened.get_and_increment_data_handle_id().unwrap();
        assert!(third.as_raw() > second.as_raw());
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
            .create_file_atomic(parent_inode_id, "file", &inode, &parent, layout)
            .unwrap();

        let stored_inode = storage.get_inode(inode_id).unwrap().unwrap();
        assert_eq!(stored_inode.current_data_handle_id, data_handle_id);
        assert_eq!(storage.get_dentry(parent_inode_id, "file").unwrap(), Some(inode_id));
        assert_eq!(storage.get_layout(inode_id).unwrap(), layout);
        assert_eq!(
            storage.get_inode_by_data_handle(data_handle_id).unwrap(),
            Some(inode_id)
        );
        assert_eq!(storage.get_inode(parent_inode_id).unwrap().unwrap().attrs.mtime_ms, 100);
    }

    #[test]
    fn create_file_with_apply_result_atomic_persists_namespace_dedup() {
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
        let dedup = DedupKey::new(ClientId::new(101), types::CallId::new());
        let applied = AppliedResult {
            fingerprint: CommandFingerprint(77),
            result: AppDataResponse::Fs(crate::raft::types::FsCommandResult::Ok(
                crate::raft::types::FsOkResult {
                    inode_id: Some(inode_id),
                    data_handle_id: Some(data_handle_id),
                    file_version: None,
                },
            )),
            created_at_ms: now_millis(),
            size_bytes: 0,
        };

        storage
            .create_file_with_apply_result_atomic(parent_inode_id, "file", &inode, &parent, layout, &dedup, applied)
            .unwrap();

        assert_eq!(storage.get_dentry(parent_inode_id, "file").unwrap(), Some(inode_id));
        assert_eq!(storage.get_layout(inode_id).unwrap(), layout);
        assert!(storage.get_applied_result(&dedup).unwrap().is_some());
    }

    #[test]
    fn delete_empty_file_with_apply_result_atomic_removes_namespace_data_owner_dedup() {
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
            .create_file_atomic(parent_inode_id, "file", &inode, &parent, layout)
            .unwrap();

        parent.attrs.update_mtime_ctime(200);
        let dedup = DedupKey::new(ClientId::new(103), types::CallId::new());
        let applied = AppliedResult {
            fingerprint: CommandFingerprint(99),
            result: AppDataResponse::Fs(crate::raft::types::FsCommandResult::ok()),
            created_at_ms: now_millis(),
            size_bytes: 0,
        };

        storage
            .delete_empty_file_with_apply_result_atomic(
                parent_inode_id,
                "file",
                inode_id,
                Some(data_handle_id),
                &parent,
                &dedup,
                applied,
            )
            .unwrap();

        assert_eq!(storage.get_dentry(parent_inode_id, "file").unwrap(), None);
        assert!(storage.get_inode(inode_id).unwrap().is_none());
        assert!(storage.get_layout(inode_id).is_err());
        assert_eq!(storage.get_inode_by_data_handle(data_handle_id).unwrap(), None);
        assert!(storage.get_applied_result(&dedup).unwrap().is_some());
    }

    #[test]
    fn delete_empty_dir_with_apply_result_atomic_removes_namespace_dedup() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(temp_dir.path()).unwrap();

        let parent_inode_id = InodeId::new(20);
        let mut parent = Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1));
        let inode_id = InodeId::new(21);
        let inode = Inode::new_dir(inode_id, FileAttrs::new(), parent.mount_id);
        storage.put_inode(&parent).unwrap();
        storage
            .create_dir_atomic(parent_inode_id, "dir", &inode, &parent)
            .unwrap();

        parent.attrs.update_mtime_ctime(300);
        let dedup = DedupKey::new(ClientId::new(104), types::CallId::new());
        let applied = AppliedResult {
            fingerprint: CommandFingerprint(100),
            result: AppDataResponse::Fs(crate::raft::types::FsCommandResult::ok()),
            created_at_ms: now_millis(),
            size_bytes: 0,
        };

        storage
            .delete_empty_dir_with_apply_result_atomic(parent_inode_id, "dir", inode_id, &parent, &dedup, applied)
            .unwrap();

        assert_eq!(storage.get_dentry(parent_inode_id, "dir").unwrap(), None);
        assert!(storage.get_inode(inode_id).unwrap().is_none());
        assert_eq!(storage.get_inode(parent_inode_id).unwrap().unwrap().attrs.mtime_ms, 300);
        assert!(storage.get_applied_result(&dedup).unwrap().is_some());
    }

    #[test]
    fn put_inode_with_apply_result_atomic_persists_inode_dedup() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(temp_dir.path()).unwrap();

        let inode_id = InodeId::new(12);
        let mut inode = Inode::new_file(inode_id, FileAttrs::new(), MountId::new(1), DataHandleId::new(120));
        inode.attrs.uid = 44;
        let dedup = DedupKey::new(ClientId::new(102), types::CallId::new());
        let applied = AppliedResult {
            fingerprint: CommandFingerprint(88),
            result: AppDataResponse::Fs(crate::raft::types::FsCommandResult::ok()),
            created_at_ms: now_millis(),
            size_bytes: 0,
        };

        storage
            .put_inode_with_apply_result_atomic(&inode, &dedup, applied)
            .unwrap();

        assert_eq!(storage.get_inode(inode_id).unwrap().unwrap().attrs.uid, 44);
        assert!(storage.get_applied_result(&dedup).unwrap().is_some());
    }

    #[test]
    fn close_write_with_apply_result_atomic_persists_inode_block_refs_dedup() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(temp_dir.path()).unwrap();

        let inode_id = InodeId::new(13);
        let data_handle_id = DataHandleId::new(130);
        let mut inode = Inode::new_file(inode_id, FileAttrs::new(), MountId::new(1), data_handle_id);
        let layout = FileLayout::new(4096, 4096, 1);
        let block_id = BlockId::new(data_handle_id, types::ids::BlockIndex::new(0));
        if let InodeData::File {
            extents,
            file_version,
            lease_epoch,
        } = &mut inode.data
        {
            extents.push(types::fs::Extent {
                file_offset: 0,
                block_id,
                block_offset: 0,
                len: 64,
                file_version: None,
                block_stamp: None,
            });
            *file_version = Some(3);
            *lease_epoch = Some(3);
        }
        inode.attrs.size = 64;
        storage.put_layout(inode_id, layout).unwrap();

        let dedup = DedupKey::new(ClientId::new(105), types::CallId::new());
        let applied = AppliedResult {
            fingerprint: CommandFingerprint(101),
            result: AppDataResponse::Fs(crate::raft::types::FsCommandResult::ok()),
            created_at_ms: now_millis(),
            size_bytes: 0,
        };

        storage
            .close_write_with_apply_result_atomic(&inode, layout, &[block_id], &[], 1, &dedup, applied)
            .unwrap();

        let stored = storage.get_inode(inode_id).unwrap().unwrap();
        assert_eq!(stored.attrs.size, 64);
        assert_eq!(storage.get_layout(inode_id).unwrap(), layout);
        assert_eq!(storage.get_block_ref_count(block_id).unwrap(), Some(1));
        assert!(storage.get_applied_result(&dedup).unwrap().is_some());
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
            .create_dir_atomic(parent_inode_id, "dir", &inode, &parent)
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
            .rename_atomic(crate::raft::storage::RenameAtomicUpdate {
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
                    error_msg.contains("obsolete column family") || error_msg.contains("files"),
                    "Error message should mention obsolete column family 'files', got: {}",
                    error_msg
                );
            }
            Ok(_) => panic!("Expected error but got Ok"),
        }
    }

    #[test]
    fn dedup_ttl_evicts_and_returns_miss() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(temp_dir.path()).unwrap();
        let key = DedupKey::new(ClientId::new(42), CallId::new());
        let result = AppliedResult {
            fingerprint: CommandFingerprint(1),
            result: AppDataResponse::None,
            created_at_ms: now_millis().saturating_sub(DEDUP_TTL_MS + 1_000),
            size_bytes: 0,
        };
        storage.put_applied_result(&key, result).unwrap();

        let fetched = storage.get_applied_result(&key).unwrap();
        assert!(fetched.is_none(), "expired dedup entry should be evicted");
    }

    #[test]
    fn dedup_size_bound_evicts_oldest() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(temp_dir.path()).unwrap();

        for _ in 0..(DEDUP_MAX_ENTRIES + 2) {
            let key = DedupKey::new(ClientId::new(7), CallId::new());
            let result = AppliedResult {
                fingerprint: CommandFingerprint(2),
                result: AppDataResponse::None,
                created_at_ms: now_millis(),
                size_bytes: 0,
            };
            storage.put_applied_result(&key, result).unwrap();
        }

        let count = storage.count_dedup_entries().unwrap();
        assert!(
            count <= DEDUP_MAX_ENTRIES,
            "size bound should cap dedup entries (count={count}, max={DEDUP_MAX_ENTRIES})"
        );
    }
}
