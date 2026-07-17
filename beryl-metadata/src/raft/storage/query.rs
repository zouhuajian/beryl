// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use super::*;

impl RocksDBStorage {
    /// Get the authoritative route epoch used for stale-route validation.
    pub fn get_route_epoch(&self) -> MetadataResult<RouteEpoch> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf = db
            .cf_handle(CF_META)
            .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;

        match db.get_cf(cf, b"route_epoch") {
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

    /// Load the layout for a specific inode.
    pub fn get_layout(&self, inode_id: InodeId) -> MetadataResult<FileLayout> {
        let _generation = self.pin_generation()?;
        self.get_layout_optional(inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Layout not found for inode {}", inode_id)))
    }

    pub(crate) fn get_layout_optional(&self, inode_id: InodeId) -> MetadataResult<Option<FileLayout>> {
        crate::observe::record_rocksdb_read("layout");
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf = db
            .cf_handle(CF_META)
            .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;
        let key = format!("layout:{}", inode_id.as_raw());
        match db.get_cf(cf, key.as_bytes()) {
            Ok(Some(value)) => {
                let (layout, _): (FileLayout, usize) = decode_from_slice(&value, standard())
                    .map_err(|e| MetadataError::Internal(format!("Failed to deserialize file layout: {}", e)))?;
                layout
                    .validate()
                    .map_err(|e| MetadataError::Internal(format!("Invalid file layout: {}", e)))?;
                Ok(Some(layout))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    fn get_meta_u64_optional(&self, key: &[u8], label: &str) -> MetadataResult<Option<u64>> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf = Self::cf(db, CF_META)?;
        match db.get_cf(cf, key) {
            Ok(Some(value)) => decode_from_slice(&value, standard())
                .map(|decoded: (u64, usize)| Some(decoded.0))
                .map_err(|error| MetadataError::Internal(format!("Failed to deserialize {label}: {error}"))),
            Ok(None) => Ok(None),
            Err(error) => Err(MetadataError::Internal(format!(
                "RocksDB error reading {label}: {error}"
            ))),
        }
    }

    pub(crate) fn bootstrap_namespace_state(
        &self,
        expected_group_name: &GroupName,
    ) -> MetadataResult<BootstrapNamespaceState> {
        let _generation = self.pin_generation()?;
        let root_inode = self.get_inode(crate::mount::ROOT_INODE_ID)?;
        let mounts = self.list_mounts()?;
        let route_epoch = self.get_meta_u64_optional(b"route_epoch", "route_epoch")?;
        let mount_epoch = self.get_meta_u64_optional(b"mount_epoch", "mount_epoch")?;
        let next_inode = self.get_next_inode_id()?;
        let next_data_handle = self.get_next_data_handle_id()?;
        let namespace_has_any_state = root_inode.is_some()
            || !mounts.is_empty()
            || route_epoch.is_some()
            || mount_epoch.is_some()
            || next_inode.is_some()
            || next_data_handle.is_some()
            || self.max_inode_id()?.is_some();
        if !namespace_has_any_state {
            return Ok(BootstrapNamespaceState::Empty);
        }

        let matching_inode = root_inode.as_ref().is_some_and(|inode| {
            inode.inode_id == crate::mount::ROOT_INODE_ID
                && inode.kind.is_dir()
                && matches!(inode.data, beryl_types::fs::InodeData::Dir)
                && inode.mount_id == MountId::new(1)
                && inode.current_data_handle_id == DataHandleId::new(0)
        });
        let matching_mount = mounts.len() == 1
            && mounts.first().is_some_and(|mount| {
                mount.mount_id == MountId::new(1)
                    && mount.mount_prefix == crate::mount::ROOT_MOUNT_PREFIX
                    && mount.mount_kind == crate::mount::MountKind::Internal
                    && mount.ufs_uri.is_none()
                    && mount.data_io_policy == crate::mount::DataIoPolicy::Allow
                    && mount.mount_epoch == 1
                    && mount.namespace_owner_group_name == *expected_group_name
                    && mount.root_inode_id == crate::mount::ROOT_INODE_ID
            });
        if matching_inode
            && matching_mount
            && self.max_inode_id()? == Some(crate::mount::ROOT_INODE_ID)
            && route_epoch == Some(1)
            && mount_epoch == Some(1)
            && next_inode == Some(InodeId::new(2))
            && next_data_handle == Some(DataHandleId::new(1))
        {
            Ok(BootstrapNamespaceState::Matching)
        } else {
            Ok(BootstrapNamespaceState::Conflicting)
        }
    }

    fn get_next_data_handle_id(&self) -> MetadataResult<Option<DataHandleId>> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf_meta = Self::cf(db, CF_META)?;
        match db.get_cf(cf_meta, NEXT_DATA_HANDLE_ID_KEY) {
            Ok(Some(value)) => {
                let id: u64 = decode_from_slice(&value, standard())
                    .map_err(|e| MetadataError::Internal(format!("Failed to deserialize next_data_handle_id: {}", e)))?
                    .0;
                Ok(Some(DataHandleId::new(id)))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    /// Read allocator state without consuming an inode ID.
    pub(crate) fn prepare_inode_allocation(&self) -> MetadataResult<InodeAllocation> {
        let _generation = self.pin_generation()?;
        let inode_id = self.get_next_inode_id()?.unwrap_or_else(|| InodeId::new(2));
        let next_raw = inode_id
            .as_raw()
            .checked_add(1)
            .ok_or_else(|| MetadataError::Internal("inode ID allocator overflow".to_string()))?;
        Ok(InodeAllocation {
            inode_id,
            next_inode_id: InodeId::new(next_raw),
        })
    }

    /// Read allocator state without consuming file identities.
    pub(crate) fn prepare_file_allocation(&self) -> MetadataResult<FileAllocation> {
        let _generation = self.pin_generation()?;
        let inode = self.prepare_inode_allocation()?;
        let data_handle_id = self.get_next_data_handle_id()?.unwrap_or_else(|| DataHandleId::new(1));
        let next_raw = data_handle_id
            .as_raw()
            .checked_add(1)
            .ok_or_else(|| MetadataError::Internal("data handle ID allocator overflow".to_string()))?;
        Ok(FileAllocation {
            inode,
            data_handle_id,
            next_data_handle_id: DataHandleId::new(next_raw),
        })
    }

    /// Read the durable next inode ID allocator value.
    pub fn get_next_inode_id(&self) -> MetadataResult<Option<InodeId>> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf_meta = db
            .cf_handle(CF_META)
            .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;

        match db.get_cf(cf_meta, NEXT_INODE_ID_KEY) {
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

    /// Lookup inode_id from a data_handle_id (authoritative mapping).
    pub fn get_inode_by_data_handle(&self, data_handle_id: DataHandleId) -> MetadataResult<Option<InodeId>> {
        crate::observe::record_rocksdb_read("data_handle_owner");
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf_meta = db
            .cf_handle(CF_META)
            .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;
        let key = format!("data_handle_owner:{}", data_handle_id.as_raw());

        match db.get_cf(cf_meta, key.as_bytes()) {
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
        let _generation = self.pin_generation()?;
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

    /// List all mount entries.
    pub fn list_mounts(&self) -> MetadataResult<Vec<MountEntry>> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf = db
            .cf_handle(CF_MOUNTS)
            .ok_or_else(|| MetadataError::Internal("Mounts CF not found".to_string()))?;

        let mut mounts = Vec::new();
        let iter = db.iterator_cf(cf, rocksdb::IteratorMode::Start);

        for item in iter {
            let (_, value) = item.map_err(|e| MetadataError::Internal(format!("RocksDB iterator error: {}", e)))?;
            let entry: MountEntry = decode_from_slice(&value, standard())
                .map_err(|e| MetadataError::Internal(format!("Failed to deserialize MountEntry: {}", e)))?
                .0;
            mounts.push(entry);
        }

        Ok(mounts)
    }

    pub fn prepare_worker_registration(
        &self,
        group_name: GroupName,
        worker_id: WorkerId,
        address: String,
        worker_net_protocol: i32,
        fault_domain: Option<String>,
    ) -> MetadataResult<WorkerInfo> {
        let _generation = self.pin_generation()?;
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

    /// List all workers.
    pub fn list_workers(&self) -> MetadataResult<Vec<WorkerInfo>> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf = db
            .cf_handle(CF_WORKERS)
            .ok_or_else(|| MetadataError::Internal("Workers CF not found".to_string()))?;

        let mut workers = Vec::new();
        let iter = db.iterator_cf(cf, rocksdb::IteratorMode::Start);

        for item in iter {
            let (_, value) = item.map_err(|e| MetadataError::Internal(format!("RocksDB iterator error: {}", e)))?;
            let info: WorkerInfo = decode_from_slice(&value, standard())
                .map_err(|e| MetadataError::Internal(format!("Failed to deserialize WorkerInfo: {}", e)))?
                .0;
            workers.push(info);
        }

        Ok(workers)
    }

    /// Get inode by ID.
    pub fn get_inode(&self, inode_id: InodeId) -> MetadataResult<Option<Inode>> {
        crate::observe::record_rocksdb_read("inode");
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf = db
            .cf_handle(CF_INODES)
            .ok_or_else(|| MetadataError::Internal("Inodes CF not found".to_string()))?;
        let key = Self::encode_inode_key(inode_id);

        match db.get_cf(cf, &key) {
            Ok(Some(value)) => {
                let inode: Inode = serde_json::from_slice(&value)
                    .map_err(|e| MetadataError::Internal(format!("Failed to deserialize Inode: {}", e)))?;
                Ok(Some(inode))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(MetadataError::Internal(format!("RocksDB error: {}", e))),
        }
    }

    /// Return the largest inode ID currently present in storage.
    pub fn max_inode_id(&self) -> MetadataResult<Option<InodeId>> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf = db
            .cf_handle(CF_INODES)
            .ok_or_else(|| MetadataError::Internal("Inodes CF not found".to_string()))?;

        let iter = db.iterator_cf(cf, rocksdb::IteratorMode::Start);
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
        crate::observe::record_rocksdb_read("dentry");
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf = db
            .cf_handle(CF_DENTRIES)
            .ok_or_else(|| MetadataError::Internal("Dentries CF not found".to_string()))?;
        let key = Self::encode_dentry_key(parent_inode_id, name);

        match db.get_cf(cf, &key) {
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

    /// List dentries for a parent directory (for ReadDir).
    /// Returns all (name, child_inode_id) pairs for the given parent_inode_id.
    /// The results are sorted by key (which includes name), suitable for pagination.
    pub fn list_dentries(&self, parent_inode_id: InodeId) -> MetadataResult<Vec<(String, InodeId)>> {
        let _generation = self.pin_generation()?;
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
        crate::observe::record_rocksdb_read("dentry_scan");
        let generation = self.pin_generation()?;
        let db = generation.db();
        let cf = db
            .cf_handle(CF_DENTRIES)
            .ok_or_else(|| MetadataError::Internal("Dentries CF not found".to_string()))?;

        let prefix = Self::encode_dentry_key(parent_inode_id, "");

        let (start_key, mut skip_first) = match cursor_key {
            Some(c) if c.starts_with(&prefix) => (c.to_vec(), true),
            _ => (prefix.clone(), false),
        };

        let mut entries = Vec::new();
        let mut iter = db.iterator_cf(cf, rocksdb::IteratorMode::From(&start_key, rocksdb::Direction::Forward));

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
        let _generation = self.pin_generation()?;
        let (entries, _, _) = self.list_dentries_with_cursor(parent_inode_id, None, Some(1))?;
        Ok(entries.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl RocksDBStorage {
        /// Get mount entry.
        pub fn get_mount(&self, mount_id: MountId) -> MetadataResult<Option<MountEntry>> {
            let generation = self.pin_generation()?;
            let db = generation.db();
            let cf = db
                .cf_handle(CF_MOUNTS)
                .ok_or_else(|| MetadataError::Internal("Mounts CF not found".to_string()))?;
            let key = format!("{}", mount_id.as_raw());

            match db.get_cf(cf, key.as_bytes()) {
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

        /// Get worker info accepted by a metadata group.
        pub fn get_worker_in_group(
            &self,
            group_name: &GroupName,
            worker_id: WorkerId,
        ) -> MetadataResult<Option<WorkerInfo>> {
            let generation = self.pin_generation()?;
            let db = generation.db();
            let cf = db
                .cf_handle(CF_WORKERS)
                .ok_or_else(|| MetadataError::Internal("Workers CF not found".to_string()))?;
            let key = worker_key(group_name, worker_id);

            match db.get_cf(cf, key.as_bytes()) {
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
    }
}
