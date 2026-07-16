// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use super::*;

impl RocksDBStorage {
    fn dedup_key_bytes(request: &DedupKey) -> Vec<u8> {
        format!("{}:{}", request.client_id.as_raw(), request.call_id).into_bytes()
    }

    fn batch_put_applied_result(
        batch: &mut WriteBatch,
        cf: &ColumnFamily,
        request: &DedupKey,
        result: AppliedResult,
    ) -> MetadataResult<usize> {
        let mut result = result;
        let value = encode_to_vec(&result, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize AppliedResult: {}", e)))?;
        result.size_bytes = value.len() as u32;
        let value = encode_to_vec(&result, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize AppliedResult: {}", e)))?;
        let bytes = value.len();
        batch.put_cf(cf, Self::dedup_key_bytes(request), value);
        Ok(bytes)
    }

    /// Atomically append dedup tracking to an existing RocksDB batch.
    pub(crate) fn commit_apply_batch(
        &self,
        mut batch: AuthorityBatch,
        request: &DedupKey,
        result: AppliedResult,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf_dedup = Self::cf(db, CF_DEDUP)?;
        let dedup_bytes = Self::batch_put_applied_result(&mut batch, cf_dedup, request, result)?;
        self.commit_authority_batch(batch, raft_state)?;
        DEDUP_STORE_ENTRIES_GAUGE.fetch_add(1, Ordering::Relaxed);
        crate::observe::record_raft_dedup_insert(dedup_bytes);
        Ok(())
    }

    /// Atomically persist only dedup tracking.
    pub fn put_apply_result_atomic(
        &self,
        request: &DedupKey,
        result: AppliedResult,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let _generation = self.pin_generation()?;
        self.commit_apply_batch(AuthorityBatch::default(), request, result, raft_state)
    }

    pub(super) fn commit_authority_batch(
        &self,
        mut batch: AuthorityBatch,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf_raft_state = Self::cf(db, CF_RAFT_STATE)?;
        let state_data = serde_json::to_vec(raft_state)
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize Raft state: {e}")))?;
        batch.put_cf(cf_raft_state, b"raft_state", state_data);
        let started = Instant::now();
        let result = db
            .write(batch.0)
            .map_err(|e| MetadataError::Internal(format!("Failed to commit authority batch: {e}")));
        crate::observe::record_raft_authority_commit(
            if result.is_ok() { "ok" } else { "error" },
            started.elapsed().as_secs_f64(),
        );
        result
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

    fn batch_put_mount(batch: &mut WriteBatch, cf: &ColumnFamily, entry: &MountEntry) -> MetadataResult<()> {
        let key = format!("{}", entry.mount_id.as_raw());
        let value = encode_to_vec(entry, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize MountEntry: {}", e)))?;
        batch.put_cf(cf, key.as_bytes(), value);
        Ok(())
    }

    fn batch_put_route_epoch(batch: &mut WriteBatch, cf: &ColumnFamily, epoch: RouteEpoch) -> MetadataResult<()> {
        let value = encode_to_vec(epoch.as_u64(), standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize route_epoch: {}", e)))?;
        batch.put_cf(cf, b"route_epoch", value);
        Ok(())
    }

    fn batch_put_mount_epoch(batch: &mut WriteBatch, cf: &ColumnFamily, epoch: u64) -> MetadataResult<()> {
        let value = encode_to_vec(epoch, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize mount_epoch: {}", e)))?;
        batch.put_cf(cf, b"mount_epoch", value);
        Ok(())
    }

    fn batch_put_inode_allocation(
        batch: &mut WriteBatch,
        cf_meta: &ColumnFamily,
        allocation: InodeAllocation,
    ) -> MetadataResult<()> {
        let value = encode_to_vec(allocation.next_inode_id.as_raw(), standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize next_inode_id: {}", e)))?;
        batch.put_cf(cf_meta, NEXT_INODE_ID_KEY, value);
        Ok(())
    }

    fn batch_put_file_allocation(
        batch: &mut WriteBatch,
        cf_meta: &ColumnFamily,
        allocation: FileAllocation,
    ) -> MetadataResult<()> {
        Self::batch_put_inode_allocation(batch, cf_meta, allocation.inode)?;
        let value = encode_to_vec(allocation.next_data_handle_id.as_raw(), standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize next_data_handle_id: {}", e)))?;
        batch.put_cf(cf_meta, NEXT_DATA_HANDLE_ID_KEY, value);
        Ok(())
    }

    fn batch_put_worker(batch: &mut WriteBatch, cf: &ColumnFamily, info: &WorkerInfo) -> MetadataResult<()> {
        let key = worker_key(&info.group_name, info.worker_id);
        let value = encode_to_vec(info, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize WorkerInfo: {}", e)))?;
        batch.put_cf(cf, key.as_bytes(), value);
        Ok(())
    }

    pub fn register_worker_with_apply_result_atomic(
        &self,
        info: &WorkerInfo,
        dedup_key: &DedupKey,
        applied_result: AppliedResult,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf_workers = Self::cf(db, CF_WORKERS)?;
        if info.worker_id.as_raw() == 0 {
            return Err(MetadataError::InvalidArgument(
                "worker_id must be non-zero for registration".to_string(),
            ));
        }

        let mut batch = WriteBatch::default();
        Self::batch_put_worker(&mut batch, cf_workers, info)?;
        self.commit_apply_batch(batch.into(), dedup_key, applied_result, raft_state)
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
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf_inodes = Self::cf(db, CF_INODES)?;
        let mut batch = WriteBatch::default();
        Self::batch_put_inode(&mut batch, cf_inodes, inode)?;
        self.commit_apply_batch(batch.into(), dedup_key, applied_result, raft_state)
    }

    /// Atomically persist a CloseWrite commit with replay tracking.
    // Atomic storage helpers keep every column-family mutation visible at the call boundary.
    #[allow(clippy::too_many_arguments)]
    pub fn close_write_with_apply_result_atomic(
        &self,
        inode: &Inode,
        layout: FileLayout,
        dedup_key: &DedupKey,
        applied_result: AppliedResult,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf_inodes = Self::cf(db, CF_INODES)?;
        let cf_meta = Self::cf(db, CF_META)?;
        let mut batch = WriteBatch::default();

        Self::batch_put_inode(&mut batch, cf_inodes, inode)?;
        Self::batch_put_layout(&mut batch, cf_meta, inode.inode_id, layout)?;

        self.commit_apply_batch(batch.into(), dedup_key, applied_result, raft_state)
    }

    pub(crate) fn bootstrap_namespace_with_apply_result_atomic(
        &self,
        root_inode: &Inode,
        root_mount: &MountEntry,
        dedup_key: &DedupKey,
        applied_result: AppliedResult,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf_inodes = Self::cf(db, CF_INODES)?;
        let cf_mounts = Self::cf(db, CF_MOUNTS)?;
        let cf_meta = Self::cf(db, CF_META)?;
        let mut batch = WriteBatch::default();
        Self::batch_put_inode(&mut batch, cf_inodes, root_inode)?;
        Self::batch_put_mount(&mut batch, cf_mounts, root_mount)?;
        Self::batch_put_route_epoch(&mut batch, cf_meta, RouteEpoch::new(1))?;
        Self::batch_put_mount_epoch(&mut batch, cf_meta, 1)?;
        batch.put_cf(
            cf_meta,
            NEXT_INODE_ID_KEY,
            encode_to_vec(2u64, standard())
                .map_err(|error| MetadataError::Internal(format!("Failed to serialize next_inode_id: {error}")))?,
        );
        batch.put_cf(
            cf_meta,
            NEXT_DATA_HANDLE_ID_KEY,
            encode_to_vec(1u64, standard()).map_err(|error| {
                MetadataError::Internal(format!("Failed to serialize next_data_handle_id: {error}"))
            })?,
        );
        self.commit_apply_batch(batch.into(), dedup_key, applied_result, raft_state)
    }

    fn create_file_batch(
        &self,
        parent_inode_id: InodeId,
        name: &str,
        inode: &Inode,
        updated_parent: &Inode,
        layout: FileLayout,
    ) -> MetadataResult<WriteBatch> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf_inodes = Self::cf(db, CF_INODES)?;
        let cf_dentries = Self::cf(db, CF_DENTRIES)?;
        let cf_meta = Self::cf(db, CF_META)?;

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
    pub(crate) fn create_file_with_apply_result_atomic(
        &self,
        allocation: FileAllocation,
        parent_inode_id: InodeId,
        name: &str,
        inode: &Inode,
        updated_parent: &Inode,
        layout: FileLayout,
        dedup_key: &DedupKey,
        applied_result: AppliedResult,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        if inode.inode_id != allocation.inode.inode_id || inode.current_data_handle_id != allocation.data_handle_id {
            return Err(MetadataError::Internal(
                "file allocation does not match prepared inode".to_string(),
            ));
        }
        let mut batch = self.create_file_batch(parent_inode_id, name, inode, updated_parent, layout)?;
        let cf_meta = Self::cf(db, CF_META)?;
        Self::batch_put_file_allocation(&mut batch, cf_meta, allocation)?;
        self.commit_apply_batch(batch.into(), dedup_key, applied_result, raft_state)
    }

    fn create_dir_batch(
        &self,
        parent_inode_id: InodeId,
        name: &str,
        inode: &Inode,
        updated_parent: &Inode,
    ) -> MetadataResult<WriteBatch> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf_inodes = Self::cf(db, CF_INODES)?;
        let cf_dentries = Self::cf(db, CF_DENTRIES)?;

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
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create_dir_with_apply_result_atomic(
        &self,
        allocation: InodeAllocation,
        parent_inode_id: InodeId,
        name: &str,
        inode: &Inode,
        updated_parent: &Inode,
        dedup_key: &DedupKey,
        applied_result: AppliedResult,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        if inode.inode_id != allocation.inode_id {
            return Err(MetadataError::Internal(
                "directory allocation does not match prepared inode".to_string(),
            ));
        }
        let mut batch = self.create_dir_batch(parent_inode_id, name, inode, updated_parent)?;
        let cf_meta = Self::cf(db, CF_META)?;
        Self::batch_put_inode_allocation(&mut batch, cf_meta, allocation)?;
        self.commit_apply_batch(batch.into(), dedup_key, applied_result, raft_state)
    }

    fn delete_dentry_inode_batch(
        &self,
        parent_inode_id: InodeId,
        name: &str,
        inode_id: InodeId,
        updated_parent: &Inode,
    ) -> MetadataResult<WriteBatch> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf_inodes = Self::cf(db, CF_INODES)?;
        let cf_dentries = Self::cf(db, CF_DENTRIES)?;

        let mut batch = WriteBatch::default();
        batch.delete_cf(cf_dentries, Self::encode_dentry_key(parent_inode_id, name));
        batch.delete_cf(cf_inodes, Self::encode_inode_key(inode_id));
        Self::batch_put_inode(&mut batch, cf_inodes, updated_parent)?;
        Ok(batch)
    }

    /// Atomically persist empty-directory deletion with apply tracking.
    #[allow(clippy::too_many_arguments)]
    pub fn delete_empty_dir_with_apply_result_atomic(
        &self,
        parent_inode_id: InodeId,
        name: &str,
        inode_id: InodeId,
        updated_parent: &Inode,
        dedup_key: &DedupKey,
        applied_result: AppliedResult,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let _generation = self.pin_generation()?;
        let batch = self.delete_dentry_inode_batch(parent_inode_id, name, inode_id, updated_parent)?;
        self.commit_apply_batch(batch.into(), dedup_key, applied_result, raft_state)
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
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf_meta = Self::cf(db, CF_META)?;
        let mut batch = self.delete_dentry_inode_batch(parent_inode_id, name, inode_id, updated_parent)?;
        let layout_key = format!("layout:{}", inode_id.as_raw());
        batch.delete_cf(cf_meta, layout_key.as_bytes());
        if let Some(data_handle_id) = data_handle_id {
            let owner_key = format!("data_handle_owner:{}", data_handle_id.as_raw());
            batch.delete_cf(cf_meta, owner_key.as_bytes());
        }
        self.commit_apply_batch(batch.into(), dedup_key, applied_result, raft_state)
    }

    /// Atomically persist a recursive tree delete with apply tracking.
    pub fn delete_tree_with_apply_result_atomic(
        &self,
        update: DeleteTreeAtomicUpdate<'_>,
        dedup_key: &DedupKey,
        applied_result: AppliedResult,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf_inodes = Self::cf(db, CF_INODES)?;
        let cf_dentries = Self::cf(db, CF_DENTRIES)?;
        let cf_meta = Self::cf(db, CF_META)?;
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

        self.commit_apply_batch(batch.into(), dedup_key, applied_result, raft_state)
    }

    /// Atomically persist truncate shrink effects with apply tracking.
    pub fn truncate_file_with_apply_result_atomic(
        &self,
        inode: &Inode,
        layout: FileLayout,
        dedup_key: &DedupKey,
        applied_result: AppliedResult,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf_inodes = Self::cf(db, CF_INODES)?;
        let cf_meta = Self::cf(db, CF_META)?;
        let mut batch = WriteBatch::default();

        Self::batch_put_inode(&mut batch, cf_inodes, inode)?;
        Self::batch_put_layout(&mut batch, cf_meta, inode.inode_id, layout)?;
        self.commit_apply_batch(batch.into(), dedup_key, applied_result, raft_state)
    }

    fn rename_batch(&self, update: RenameAtomicUpdate<'_>) -> MetadataResult<WriteBatch> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf_inodes = Self::cf(db, CF_INODES)?;
        let cf_dentries = Self::cf(db, CF_DENTRIES)?;
        let cf_meta = Self::cf(db, CF_META)?;

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
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let _generation = self.pin_generation()?;
        let batch = self.rename_batch(update)?;
        self.commit_apply_batch(batch.into(), dedup_key, applied_result, raft_state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl RocksDBStorage {
        /// Put applied result for idempotency.
        pub fn put_applied_result(&self, request: &DedupKey, result: AppliedResult) -> MetadataResult<()> {
            let generation = self.pin_generation()?;
            let db = generation.db();
            let cf = db
                .cf_handle(CF_DEDUP)
                .ok_or_else(|| MetadataError::Internal("Dedup CF not found".to_string()))?;
            let key = format!("{}:{}", request.client_id.as_raw(), request.call_id);
            let mut result = result;
            let value = encode_to_vec(&result, standard())
                .map_err(|e| MetadataError::Internal(format!("Failed to serialize AppliedResult: {}", e)))?;
            result.size_bytes = value.len() as u32;
            let value = encode_to_vec(&result, standard())
                .map_err(|e| MetadataError::Internal(format!("Failed to serialize AppliedResult: {}", e)))?;

            db.put_cf(cf, key.as_bytes(), value)
                .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
            DEDUP_STORE_ENTRIES_GAUGE.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        /// Persist the authoritative route epoch used for stale-route validation.
        pub fn put_route_epoch(&self, epoch: RouteEpoch) -> MetadataResult<()> {
            let generation = self.pin_generation()?;
            let db = generation.db();
            let cf = db
                .cf_handle(CF_META)
                .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;
            let value = encode_to_vec(epoch.as_u64(), standard())
                .map_err(|e| MetadataError::Internal(format!("Failed to serialize route_epoch: {}", e)))?;

            db.put_cf(cf, b"route_epoch", value)
                .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
            Ok(())
        }

        /// Persist the layout for a specific inode (authoritative data-plane parameters).
        pub fn put_layout(&self, inode_id: InodeId, layout: FileLayout) -> MetadataResult<()> {
            let generation = self.pin_generation()?;
            let db = generation.db();
            let cf = db
                .cf_handle(CF_META)
                .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;
            let key = format!("layout:{}", inode_id.as_raw());
            let value = encode_to_vec(layout, standard())
                .map_err(|e| MetadataError::Internal(format!("Failed to serialize file layout: {}", e)))?;

            db.put_cf(cf, key.as_bytes(), value)
                .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
            Ok(())
        }

        /// Put mount entry.
        pub fn put_mount(&self, entry: &MountEntry) -> MetadataResult<()> {
            let generation = self.pin_generation()?;
            let db = generation.db();
            let cf = db
                .cf_handle(CF_MOUNTS)
                .ok_or_else(|| MetadataError::Internal("Mounts CF not found".to_string()))?;
            let key = format!("{}", entry.mount_id.as_raw());
            let value = encode_to_vec(entry, standard())
                .map_err(|e| MetadataError::Internal(format!("Failed to serialize MountEntry: {}", e)))?;

            db.put_cf(cf, key.as_bytes(), value)
                .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
            Ok(())
        }

        pub(crate) fn delete_mount(&self, mount_id: MountId) -> MetadataResult<()> {
            let generation = self.pin_generation()?;
            let db = generation.db();
            let cf = Self::cf(db, CF_MOUNTS)?;
            db.delete_cf(cf, format!("{}", mount_id.as_raw()).as_bytes())
                .map_err(|error| MetadataError::Internal(format!("delete test mount: {error}")))
        }

        /// Persist the durable next inode ID allocator value.
        pub fn set_next_inode_id(&self, next_inode_id: InodeId) -> MetadataResult<()> {
            let generation = self.pin_generation()?;
            let db = generation.db();
            let cf_meta = db
                .cf_handle(CF_META)
                .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;
            let value = encode_to_vec(next_inode_id.as_raw(), standard())
                .map_err(|e| MetadataError::Internal(format!("Failed to serialize next_inode_id: {}", e)))?;

            db.put_cf(cf_meta, NEXT_INODE_ID_KEY, value)
                .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
            Ok(())
        }

        /// Allocate an inode ID from replicated RocksDB state.
        pub fn allocate_inode_id(&self) -> MetadataResult<InodeId> {
            let generation = self.pin_generation()?;
            let db = generation.db();
            let cf_meta = db
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

            let next_id = current_id
                .checked_add(1)
                .ok_or_else(|| MetadataError::Internal("inode ID allocator overflow".to_string()))?;
            let next_id_value = encode_to_vec(next_id, standard())
                .map_err(|e| MetadataError::Internal(format!("Failed to serialize next_inode_id: {}", e)))?;
            let mut batch = WriteBatch::default();
            batch.put_cf(cf_meta, NEXT_INODE_ID_KEY, next_id_value);

            db.write(batch)
                .map_err(|e| MetadataError::Internal(format!("RocksDB write error: {}", e)))?;

            Ok(InodeId::new(current_id))
        }

        /// Persist mapping from data_handle_id -> inode_id for routing from data plane back to namespace.
        pub fn put_data_handle_owner(&self, data_handle_id: DataHandleId, inode_id: InodeId) -> MetadataResult<()> {
            let generation = self.pin_generation()?;
            let db = generation.db();
            let cf_meta = db
                .cf_handle(CF_META)
                .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;
            let key = format!("data_handle_owner:{}", data_handle_id.as_raw());
            let value = encode_to_vec(inode_id.as_raw(), standard())
                .map_err(|e| MetadataError::Internal(format!("Failed to serialize inode_id: {}", e)))?;

            db.put_cf(cf_meta, key.as_bytes(), value)
                .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
            Ok(())
        }

        /// Put mount epoch.
        pub fn put_mount_epoch(&self, epoch: u64) -> MetadataResult<()> {
            let generation = self.pin_generation()?;
            let db = generation.db();
            let cf = db
                .cf_handle(CF_META)
                .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;
            let value = encode_to_vec(epoch, standard())
                .map_err(|e| MetadataError::Internal(format!("Failed to serialize mount_epoch: {}", e)))?;

            db.put_cf(cf, b"mount_epoch", value)
                .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
            Ok(())
        }

        /// Put worker info.
        pub fn put_worker(&self, info: &WorkerInfo) -> MetadataResult<()> {
            let generation = self.pin_generation()?;
            let db = generation.db();
            let cf = db
                .cf_handle(CF_WORKERS)
                .ok_or_else(|| MetadataError::Internal("Workers CF not found".to_string()))?;
            let key = worker_key(&info.group_name, info.worker_id);
            let value = encode_to_vec(info, standard())
                .map_err(|e| MetadataError::Internal(format!("Failed to serialize WorkerInfo: {}", e)))?;

            db.put_cf(cf, key.as_bytes(), value)
                .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
            Ok(())
        }

        /// Put inode.
        pub fn put_inode(&self, inode: &Inode) -> MetadataResult<()> {
            let generation = self.pin_generation()?;
            let db = generation.db();
            let cf = db
                .cf_handle(CF_INODES)
                .ok_or_else(|| MetadataError::Internal("Inodes CF not found".to_string()))?;
            let key = Self::encode_inode_key(inode.inode_id);
            let value = serde_json::to_vec(inode)
                .map_err(|e| MetadataError::Internal(format!("Failed to serialize Inode: {}", e)))?;

            db.put_cf(cf, &key, value)
                .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
            Ok(())
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
            let _generation = self.pin_generation()?;
            self.write_batch(self.create_file_batch(parent_inode_id, name, inode, updated_parent, layout)?)
        }

        /// Atomically persist a mkdir namespace mutation.
        pub fn create_dir_atomic(
            &self,
            parent_inode_id: InodeId,
            name: &str,
            inode: &Inode,
            updated_parent: &Inode,
        ) -> MetadataResult<()> {
            let _generation = self.pin_generation()?;
            self.write_batch(self.create_dir_batch(parent_inode_id, name, inode, updated_parent)?)
        }

        /// Atomically persist a rename namespace mutation.
        pub fn rename_atomic(&self, update: RenameAtomicUpdate<'_>) -> MetadataResult<()> {
            let _generation = self.pin_generation()?;
            self.write_batch(self.rename_batch(update)?)
        }

        /// Put dentry.
        pub fn put_dentry(&self, parent_inode_id: InodeId, name: &str, child_inode_id: InodeId) -> MetadataResult<()> {
            let generation = self.pin_generation()?;
            let db = generation.db();
            let cf = db
                .cf_handle(CF_DENTRIES)
                .ok_or_else(|| MetadataError::Internal("Dentries CF not found".to_string()))?;
            let key = Self::encode_dentry_key(parent_inode_id, name);
            let value = child_inode_id.to_be_bytes();

            db.put_cf(cf, &key, value)
                .map_err(|e| MetadataError::Internal(format!("RocksDB error: {}", e)))?;
            Ok(())
        }

        /// Write a RocksDB batch with consistent error mapping.
        pub fn write_batch(&self, batch: WriteBatch) -> MetadataResult<()> {
            let generation = self.pin_generation()?;
            let db = generation.db();
            db.write(batch)
                .map_err(|e| MetadataError::Internal(format!("RocksDB batch write: {}", e)))
        }
    }
}
