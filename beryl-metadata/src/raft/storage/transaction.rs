// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use super::*;

impl RocksDBStorage {
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
        batch.put_cf(cf_raft_state, RAFT_STATE_KEY, state_data);
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

    fn validate_inode_allocation_targets(
        db: &DB,
        allocation: InodeAllocation,
        target_inode_ids: &[InodeId],
    ) -> MetadataResult<()> {
        if target_inode_ids.is_empty() || allocation.inode_id.as_raw() < 2 {
            return Err(MetadataError::Internal(
                "inode allocation has no valid target".to_string(),
            ));
        }
        let cf_meta = Self::cf(db, CF_META)?;
        let persisted = db
            .get_cf(cf_meta, NEXT_INODE_ID_KEY)
            .map_err(|error| MetadataError::Internal(format!("Failed to read next_inode_id: {error}")))?
            .ok_or_else(|| MetadataError::Internal("next_inode_id allocator authority is missing".to_string()))?;
        let persisted: u64 = decode_from_slice(&persisted, standard())
            .map_err(|error| MetadataError::Internal(format!("Failed to deserialize next_inode_id: {error}")))?
            .0;
        if persisted != allocation.inode_id.as_raw() {
            return Err(MetadataError::Internal(format!(
                "inode allocation {} does not match durable next_inode_id {persisted}",
                allocation.inode_id
            )));
        }

        let cf_inodes = Self::cf(db, CF_INODES)?;
        let mut expected_raw = allocation.inode_id.as_raw();
        for inode_id in target_inode_ids {
            if inode_id.as_raw() != expected_raw {
                return Err(MetadataError::Internal(format!(
                    "inode allocation target {inode_id} is not the expected inode {expected_raw}"
                )));
            }
            if db
                .get_cf(cf_inodes, Self::encode_inode_key(*inode_id))
                .map_err(|error| MetadataError::Internal(format!("Failed to read inode {inode_id}: {error}")))?
                .is_some()
            {
                return Err(MetadataError::Internal(format!(
                    "inode allocation target already exists: {inode_id}"
                )));
            }
            expected_raw = expected_raw
                .checked_add(1)
                .ok_or_else(|| MetadataError::Internal("inode ID allocator overflow".to_string()))?;
        }
        if allocation.next_inode_id.as_raw() != expected_raw {
            return Err(MetadataError::Internal(format!(
                "inode allocation next value {} does not match expected {expected_raw}",
                allocation.next_inode_id
            )));
        }
        Ok(())
    }

    fn batch_put_worker(batch: &mut WriteBatch, cf: &ColumnFamily, info: &WorkerInfo) -> MetadataResult<()> {
        let key = worker_key(&info.group_name, info.worker_id);
        let value = encode_to_vec(info, standard())
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize WorkerInfo: {}", e)))?;
        batch.put_cf(cf, key.as_bytes(), value);
        Ok(())
    }

    pub fn register_worker_atomic(&self, info: &WorkerInfo, raft_state: &AppMetadataRaftState) -> MetadataResult<()> {
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
        self.commit_authority_batch(batch.into(), raft_state)
    }

    fn batch_put_inode(batch: &mut WriteBatch, cf: &ColumnFamily, inode: &Inode) -> MetadataResult<()> {
        let key = Self::encode_inode_key(inode.inode_id);
        let value = serde_json::to_vec(inode)
            .map_err(|e| MetadataError::Internal(format!("Failed to serialize Inode: {}", e)))?;
        batch.put_cf(cf, key, value);
        Ok(())
    }

    /// Atomically persist a single inode update with apply tracking.
    pub fn put_inode_atomic(&self, inode: &Inode, raft_state: &AppMetadataRaftState) -> MetadataResult<()> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf_inodes = Self::cf(db, CF_INODES)?;
        let mut batch = WriteBatch::default();
        Self::batch_put_inode(&mut batch, cf_inodes, inode)?;
        self.commit_authority_batch(batch.into(), raft_state)
    }

    /// Atomically persist visible file authority and the applied Raft state.
    pub fn publish_file_atomic(
        &self,
        inode: &Inode,
        layout: FileLayout,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf_inodes = Self::cf(db, CF_INODES)?;
        let cf_meta = Self::cf(db, CF_META)?;
        let mut batch = WriteBatch::default();

        Self::batch_put_inode(&mut batch, cf_inodes, inode)?;
        Self::batch_put_layout(&mut batch, cf_meta, inode.inode_id, layout)?;

        self.commit_authority_batch(batch.into(), raft_state)
    }

    pub(crate) fn bootstrap_namespace_atomic(
        &self,
        root_inode: &Inode,
        root_mount: &MountEntry,
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
        self.commit_authority_batch(batch.into(), raft_state)
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

        Ok(batch)
    }

    /// Atomically persist create-file mutation with apply tracking.
    // Atomic storage helpers keep every column-family mutation visible at the call boundary.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create_file_atomic(
        &self,
        allocation: InodeAllocation,
        parent_inode_id: InodeId,
        name: &str,
        inode: &Inode,
        updated_parent: &Inode,
        layout: FileLayout,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        Self::validate_inode_allocation_targets(db, allocation, std::slice::from_ref(&inode.inode_id))?;
        let mut batch = self.create_file_batch(parent_inode_id, name, inode, updated_parent, layout)?;
        let cf_meta = Self::cf(db, CF_META)?;
        Self::batch_put_inode_allocation(&mut batch, cf_meta, allocation)?;
        self.commit_authority_batch(batch.into(), raft_state)
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
    pub(crate) fn create_dir_atomic(
        &self,
        allocation: InodeAllocation,
        parent_inode_id: InodeId,
        name: &str,
        inode: &Inode,
        updated_parent: &Inode,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        Self::validate_inode_allocation_targets(db, allocation, std::slice::from_ref(&inode.inode_id))?;
        let mut batch = self.create_dir_batch(parent_inode_id, name, inode, updated_parent)?;
        let cf_meta = Self::cf(db, CF_META)?;
        Self::batch_put_inode_allocation(&mut batch, cf_meta, allocation)?;
        self.commit_authority_batch(batch.into(), raft_state)
    }

    /// Atomically persist all missing components of one recursive mkdir command.
    pub(crate) fn create_directories_atomic(
        &self,
        allocation: InodeAllocation,
        entries: &[RecursiveMkdirEntry],
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf_inodes = Self::cf(db, CF_INODES)?;
        let cf_dentries = Self::cf(db, CF_DENTRIES)?;
        let cf_meta = Self::cf(db, CF_META)?;
        let target_inode_ids: Vec<_> = entries.iter().map(|entry| entry.inode.inode_id).collect();
        Self::validate_inode_allocation_targets(db, allocation, &target_inode_ids)?;
        let mut batch = WriteBatch::default();
        for entry in entries {
            Self::batch_put_inode(&mut batch, cf_inodes, &entry.inode)?;
            Self::batch_put_inode(&mut batch, cf_inodes, &entry.updated_parent)?;
            batch.put_cf(
                cf_dentries,
                Self::encode_dentry_key(entry.parent_inode_id, &entry.name),
                entry.inode.inode_id.to_be_bytes(),
            );
        }
        Self::batch_put_inode_allocation(&mut batch, cf_meta, allocation)?;
        self.commit_authority_batch(batch.into(), raft_state)
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
    pub fn delete_empty_dir_atomic(
        &self,
        parent_inode_id: InodeId,
        name: &str,
        inode_id: InodeId,
        updated_parent: &Inode,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let _generation = self.pin_generation()?;
        let batch = self.delete_dentry_inode_batch(parent_inode_id, name, inode_id, updated_parent)?;
        self.commit_authority_batch(batch.into(), raft_state)
    }

    /// Atomically persist non-directory deletion with its file layout cleanup.
    // Atomic storage helpers keep every column-family mutation visible at the call boundary.
    #[allow(clippy::too_many_arguments)]
    pub fn delete_file_atomic(
        &self,
        parent_inode_id: InodeId,
        name: &str,
        inode_id: InodeId,
        updated_parent: &Inode,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf_meta = Self::cf(db, CF_META)?;
        let mut batch = self.delete_dentry_inode_batch(parent_inode_id, name, inode_id, updated_parent)?;
        let layout_key = format!("layout:{}", inode_id.as_raw());
        batch.delete_cf(cf_meta, layout_key.as_bytes());
        self.commit_authority_batch(batch.into(), raft_state)
    }

    /// Atomically hide a recursive-delete root and publish its durable marker.
    ///
    /// The root inode and all descendants remain intact for bounded background
    /// reclamation. Namespace visibility and reclaim authority therefore
    /// change in the same RocksDB commit as Raft apply tracking.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn detach_directory_atomic(
        &self,
        parent_inode_id: InodeId,
        name: &str,
        root_inode_id: InodeId,
        updated_parent: &Inode,
        detached_root: DetachedRoot,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf_inodes = Self::cf(db, CF_INODES)?;
        let cf_dentries = Self::cf(db, CF_DENTRIES)?;
        let cf_detached_roots = Self::cf(db, CF_DETACHED_ROOTS)?;
        let mut batch = WriteBatch::default();

        batch.delete_cf(cf_dentries, Self::encode_dentry_key(parent_inode_id, name));
        Self::batch_put_inode(&mut batch, cf_inodes, updated_parent)?;
        batch.put_cf(
            cf_detached_roots,
            Self::encode_detached_root_key(root_inode_id),
            Self::encode_detached_root(&detached_root)?,
        );

        self.commit_authority_batch(batch.into(), raft_state)
    }

    /// Atomically publish one validated, bounded detached-root reclamation.
    pub(crate) fn reclaim_detached_roots_atomic(
        &self,
        update: DetachedRootReclaimUpdate,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf_inodes = Self::cf(db, CF_INODES)?;
        let cf_dentries = Self::cf(db, CF_DENTRIES)?;
        let cf_detached_roots = Self::cf(db, CF_DETACHED_ROOTS)?;
        let cf_meta = Self::cf(db, CF_META)?;
        let mut batch = WriteBatch::default();

        for entry in update.entries {
            batch.delete_cf(cf_dentries, Self::encode_dentry_key(entry.parent_inode_id, &entry.name));
            if let Some(detached_root) = entry.child_detached_root {
                batch.put_cf(
                    cf_detached_roots,
                    Self::encode_detached_root_key(entry.inode_id),
                    Self::encode_detached_root(&detached_root)?,
                );
                continue;
            }

            batch.delete_cf(cf_inodes, Self::encode_inode_key(entry.inode_id));
            if entry.remove_file_layout {
                batch.delete_cf(cf_meta, Self::encode_layout_key(entry.inode_id));
            }
        }
        for root_inode_id in update.completed_root_inode_ids {
            batch.delete_cf(cf_inodes, Self::encode_inode_key(root_inode_id));
            batch.delete_cf(cf_detached_roots, Self::encode_detached_root_key(root_inode_id));
        }

        self.commit_authority_batch(batch.into(), raft_state)
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
    pub fn rename_atomic(
        &self,
        update: RenameAtomicUpdate<'_>,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let _generation = self.pin_generation()?;
        let batch = self.rename_batch(update)?;
        self.commit_authority_batch(batch.into(), raft_state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use beryl_types::fs::{FileAttrs, InodeData};
    use beryl_types::ids::BlockId;
    use tempfile::TempDir;

    impl RocksDBStorage {
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

        /// Seed detached-root authority for state-machine and snapshot tests.
        pub(crate) fn put_detached_root(&self, inode_id: InodeId, detached_root: DetachedRoot) -> MetadataResult<()> {
            let generation = self.pin_generation()?;
            let db = generation.db();
            let cf = Self::cf(db, CF_DETACHED_ROOTS)?;
            db.put_cf(
                cf,
                Self::encode_detached_root_key(inode_id),
                Self::encode_detached_root(&detached_root)?,
            )
            .map_err(|error| MetadataError::Internal(format!("RocksDB error: {error}")))
        }

        /// Seed many empty roots in one test-only RocksDB batch.
        pub(crate) fn put_empty_detached_roots(
            &self,
            first_inode_id: u64,
            count: usize,
            detached_root: DetachedRoot,
        ) -> MetadataResult<()> {
            let generation = self.pin_generation()?;
            let db = generation.db();
            let cf_inodes = Self::cf(db, CF_INODES)?;
            let cf_detached_roots = Self::cf(db, CF_DETACHED_ROOTS)?;
            let encoded_marker = Self::encode_detached_root(&detached_root)?;
            let mut batch = WriteBatch::default();
            for offset in 0..count {
                let raw = first_inode_id
                    .checked_add(offset as u64)
                    .ok_or_else(|| MetadataError::Internal("test inode range overflow".to_string()))?;
                let inode = Inode::new_dir(InodeId::new(raw), FileAttrs::new(), detached_root.mount_id);
                Self::batch_put_inode(&mut batch, cf_inodes, &inode)?;
                batch.put_cf(
                    cf_detached_roots,
                    Self::encode_detached_root_key(inode.inode_id),
                    &encoded_marker,
                );
            }
            db.write(batch)
                .map_err(|error| MetadataError::Internal(format!("RocksDB error: {error}")))
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

        /// Seed a semantically corrupt key/value identity pair for state-machine tests.
        pub(crate) fn put_inode_at_storage_key(
            &self,
            storage_key_inode_id: InodeId,
            inode: &Inode,
        ) -> MetadataResult<()> {
            let generation = self.pin_generation()?;
            let db = generation.db();
            let cf = db
                .cf_handle(CF_INODES)
                .ok_or_else(|| MetadataError::Internal("Inodes CF not found".to_string()))?;
            let key = Self::encode_inode_key(storage_key_inode_id);
            let value = serde_json::to_vec(inode)
                .map_err(|error| MetadataError::Internal(format!("Failed to serialize Inode: {error}")))?;

            db.put_cf(cf, key, value)
                .map_err(|error| MetadataError::Internal(format!("RocksDB error: {error}")))
        }

        /// Atomically persist a create-file namespace mutation.
        pub fn put_test_file_atomic(
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
        pub fn put_test_dir_atomic(
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
        pub fn rename_test_atomic(&self, update: RenameAtomicUpdate<'_>) -> MetadataResult<()> {
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

    #[test]
    fn create_file_atomic_persists_namespace_inode_and_layout() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(temp_dir.path()).unwrap();

        let parent_inode_id = InodeId::new(10);
        let mut parent = Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1));
        storage.put_inode(&parent).unwrap();

        let inode_id = InodeId::new(12);
        let inode = Inode::new_file(inode_id, FileAttrs::new(), parent.mount_id);
        parent.attrs.update_mtime_ctime(100);
        let layout = FileLayout::new(4096, 4096, 1);

        storage
            .put_test_file_atomic(parent_inode_id, "file", &inode, &parent, layout)
            .unwrap();

        let stored_inode = storage.get_inode(inode_id).unwrap().unwrap();
        assert_eq!(stored_inode.inode_id, inode_id);
        assert_eq!(storage.get_dentry(parent_inode_id, "file").unwrap(), Some(inode_id));
        assert_eq!(storage.get_layout(inode_id).unwrap(), layout);
        assert_eq!(storage.get_inode(parent_inode_id).unwrap().unwrap().attrs.mtime_ms, 100);
    }

    #[test]
    fn create_file_atomic_rejects_a_target_installed_after_allocation_preparation() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(temp_dir.path()).unwrap();
        let parent_inode_id = InodeId::new(10);
        let parent = Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1));
        storage.put_inode(&parent).unwrap();
        storage.set_next_inode_id(InodeId::new(11)).unwrap();
        let allocation = storage.prepare_inode_allocation().unwrap();
        let existing = Inode::new_file(allocation.inode_id, FileAttrs::new(), MountId::new(1));
        storage.put_inode(&existing).unwrap();
        let applied_before = storage.load_raft_state().unwrap();
        let rejected_applied_state = AppMetadataRaftState {
            last_applied_log_id: Some(openraft::LogId::new(openraft::LeaderId::new(9, 1), 901)),
            ..AppMetadataRaftState::default()
        };

        let error = storage
            .create_file_atomic(
                allocation,
                parent_inode_id,
                "file",
                &Inode::new_file(allocation.inode_id, FileAttrs::new(), MountId::new(1)),
                &parent,
                FileLayout::new(4096, 4096, 1),
                &rejected_applied_state,
            )
            .unwrap_err();

        assert!(error.to_string().contains("already exists"));
        assert_eq!(storage.load_raft_state().unwrap(), applied_before);
        assert_eq!(storage.get_inode(allocation.inode_id).unwrap(), Some(existing));
        assert_eq!(storage.get_dentry(parent_inode_id, "file").unwrap(), None);
        assert_eq!(storage.get_layout_optional(allocation.inode_id).unwrap(), None);
        assert_eq!(storage.get_next_inode_id().unwrap(), Some(allocation.inode_id));
    }

    #[test]
    fn delete_file_atomic_removes_namespace_inode_and_layout() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(temp_dir.path()).unwrap();

        let parent_inode_id = InodeId::new(10);
        let mut parent = Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1));
        let inode_id = InodeId::new(12);
        let inode = Inode::new_file(inode_id, FileAttrs::new(), parent.mount_id);
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
                &parent,
                &AppMetadataRaftState::default(),
            )
            .unwrap();

        assert_eq!(storage.get_dentry(parent_inode_id, "file").unwrap(), None);
        assert!(storage.get_inode(inode_id).unwrap().is_none());
        assert!(storage.get_layout(inode_id).is_err());
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
        let mut inode = Inode::new_file(inode_id, FileAttrs::new(), MountId::new(1));
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
        let mut inode = Inode::new_file(inode_id, FileAttrs::new(), MountId::new(1));
        let layout = FileLayout::new(4096, 4096, 1);
        let block_id = BlockId::new(inode_id, beryl_types::ids::BlockIndex::new(0));
        if let InodeData::File {
            extents,
            content_revision,
            lease_epoch,
            next_block_index,
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
            *next_block_index = 1;
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
        let mut inode = Inode::new_file(inode_id, FileAttrs::new(), MountId::new(1));

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
}
