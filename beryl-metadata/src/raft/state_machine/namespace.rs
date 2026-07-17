// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use super::*;

impl AppRaftStateMachine {
    pub(super) fn apply_bootstrap_namespace(
        &self,
        group_name: GroupName,
        proposed_at_ms: u64,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<MountCommandResult> {
        let state = self.storage.bootstrap_namespace_state(&group_name, proposed_at_ms)?;
        if state == BootstrapNamespaceState::Conflicting {
            return Err(MetadataError::InvalidArgument(
                "metadata namespace is partially initialized or conflicts with writable root bootstrap; reformat metadata storage"
                    .to_string(),
            ));
        }

        let root_mount = crate::mount::MountEntry {
            mount_id: MountId::new(1),
            mount_prefix: crate::mount::ROOT_MOUNT_PREFIX.to_string(),
            mount_kind: crate::mount::MountKind::Internal,
            ufs_uri: None,
            data_io_policy: crate::mount::DataIoPolicy::Allow,
            mount_epoch: 1,
            namespace_owner_group_name: group_name,
            root_inode_id: crate::mount::ROOT_INODE_ID,
        };
        let result = MountCommandResult::Upserted(root_mount.clone());
        let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::Mount(result.clone()));
        if state == BootstrapNamespaceState::Matching {
            self.storage
                .put_apply_result_atomic(dedup_key, applied_result, raft_state)?;
            return Ok(result);
        }

        let mut attrs = FileAttrs::new();
        attrs.update_timestamps(proposed_at_ms);
        attrs.nlink = 1;
        let root_inode = Inode::new_dir(crate::mount::ROOT_INODE_ID, attrs, MountId::new(1));
        self.storage.bootstrap_namespace_with_apply_result_atomic(
            &root_inode,
            &root_mount,
            dedup_key,
            applied_result,
            raft_state,
        )?;
        Ok(result)
    }

    /// Apply Mkdir command.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_mkdir(
        &self,
        parent_inode_id: InodeId,
        name: String,
        mut attrs: FileAttrs,
        proposed_at_ms: u64,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<FsCommandResult> {
        let prepared: MetadataResult<(InodeAllocation, Inode, Inode, FsOkResult)> = (|| {
            // Check parent exists and is a directory
            let parent_inode = self
                .storage
                .get_inode(parent_inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Parent inode not found: {}", parent_inode_id)))?;
            if !parent_inode.kind.is_dir() {
                return Err(MetadataError::NotDir(format!(
                    "Parent is not a directory: {}",
                    parent_inode_id
                )));
            }

            // Check if name already exists
            if self.storage.get_dentry(parent_inode_id, &name)?.is_some() {
                return Err(MetadataError::AlreadyExists(format!(
                    "Directory already exists: {}",
                    name
                )));
            }

            // Generate inode ID
            let allocation = self.storage.prepare_inode_allocation()?;
            let inode_id = allocation.inode_id;
            let now_ms = proposed_at_ms;

            // Initialize attrs
            attrs.update_timestamps(now_ms);
            attrs.nlink = 1; // Directory starts with 1 link (self)

            // Create directory inode (inherit mount_id from parent)
            let inode = Inode::new_dir(inode_id, attrs, parent_inode.mount_id);

            // Update parent directory mtime/ctime
            let mut parent_attrs = parent_inode.attrs.clone();
            parent_attrs.update_mtime_ctime(Self::mutation_timestamp(&parent_inode, proposed_at_ms));
            let mut updated_parent = parent_inode.clone();
            updated_parent.attrs = parent_attrs;

            Ok(FsOkResult {
                inode_id: Some(inode_id),
                data_handle_id: None,
                file_version: None,
                attrs: Some(inode.attrs.clone()),
                layout: None,
            })
            .map(|ok| (allocation, inode, updated_parent, ok))
        })();

        let (allocation, inode, updated_parent, ok) = match prepared {
            Ok(prepared) => prepared,
            Err(err) => return self.persist_fs_error(err, dedup_key, fingerprint, raft_state),
        };
        let result = FsCommandResult::Ok(ok);
        let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::Fs(result.clone()));
        self.storage.create_dir_with_apply_result_atomic(
            allocation,
            parent_inode_id,
            &name,
            &inode,
            &updated_parent,
            dedup_key,
            applied_result,
            raft_state,
        )?;
        Ok(result)
    }

    /// Apply one recursive CreateDirectory command as a single authority batch.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_create_directory(
        &self,
        root_inode_id: InodeId,
        components: Vec<String>,
        attrs: FileAttrs,
        proposed_at_ms: u64,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<FsCommandResult> {
        if components.is_empty() || components.iter().any(|component| component.is_empty()) {
            return self.persist_fs_error(
                MetadataError::InvalidArgument("CreateDirectory requires non-empty path components".to_string()),
                dedup_key,
                fingerprint,
                raft_state,
            );
        }
        let mut parent = match self.storage.get_inode(root_inode_id)? {
            Some(inode) if inode.kind.is_dir() => inode,
            Some(_) => {
                return self.persist_fs_error(
                    MetadataError::NotDir(format!("Root is not a directory: {root_inode_id}")),
                    dedup_key,
                    fingerprint,
                    raft_state,
                );
            }
            None => {
                return self.persist_fs_error(
                    MetadataError::NotFound(format!("Root inode not found: {root_inode_id}")),
                    dedup_key,
                    fingerprint,
                    raft_state,
                );
            }
        };
        let mut allocation = self.storage.prepare_inode_allocation()?;
        let mut next_raw = allocation.inode_id.as_raw();
        let mut entries = Vec::new();

        for name in components {
            if let Some(child_inode_id) = self.storage.get_dentry(parent.inode_id, &name)? {
                let child = match self.storage.get_inode(child_inode_id)? {
                    Some(inode) if inode.kind.is_dir() => inode,
                    Some(_) => {
                        return self.persist_fs_error(
                            MetadataError::NotDir(format!("Path component is not a directory: {name}")),
                            dedup_key,
                            fingerprint,
                            raft_state,
                        );
                    }
                    None => {
                        return self.persist_fs_error(
                            MetadataError::NotFound(format!("Target inode not found: {child_inode_id}")),
                            dedup_key,
                            fingerprint,
                            raft_state,
                        );
                    }
                };
                parent = child;
                continue;
            }

            let inode_id = InodeId::new(next_raw);
            next_raw = next_raw
                .checked_add(1)
                .ok_or_else(|| MetadataError::Internal("inode ID allocator overflow".to_string()))?;
            let mut child_attrs = attrs.clone();
            child_attrs.update_timestamps(proposed_at_ms);
            child_attrs.nlink = 1;
            let child = Inode::new_dir(inode_id, child_attrs, parent.mount_id);
            let mut updated_parent = parent.clone();
            updated_parent
                .attrs
                .update_mtime_ctime(Self::mutation_timestamp(&parent, proposed_at_ms));
            entries.push(RecursiveMkdirEntry {
                parent_inode_id: parent.inode_id,
                name,
                inode: child.clone(),
                updated_parent,
            });
            parent = child;
        }

        let result = FsCommandResult::Ok(FsOkResult {
            inode_id: Some(parent.inode_id),
            data_handle_id: None,
            file_version: None,
            attrs: Some(parent.attrs.clone()),
            layout: None,
        });
        let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::Fs(result.clone()));
        if entries.is_empty() {
            self.storage
                .put_apply_result_atomic(dedup_key, applied_result, raft_state)?;
        } else {
            allocation.next_inode_id = InodeId::new(next_raw);
            self.storage.create_directories_with_apply_result_atomic(
                allocation,
                &entries,
                dedup_key,
                applied_result,
                raft_state,
            )?;
        }
        Ok(result)
    }

    /// Apply Create command.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_create(
        &self,
        parent_inode_id: InodeId,
        name: String,
        mut attrs: FileAttrs,
        layout: FileLayout,
        mode: crate::raft::CreateFileMode,
        proposed_at_ms: u64,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<FsCommandResult> {
        let existing: MetadataResult<Option<FsCommandResult>> = (|| {
            let Some(existing_inode_id) = self.storage.get_dentry(parent_inode_id, &name)? else {
                return Ok(None);
            };
            if mode == crate::raft::CreateFileMode::CreateNew {
                return Err(MetadataError::AlreadyExists(format!("File already exists: {name}")));
            }
            let existing = self
                .storage
                .get_inode(existing_inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Existing inode not found: {existing_inode_id}")))?;
            if !existing.kind.is_file() {
                return Err(MetadataError::IsDir(format!("Existing entry is not a file: {name}")));
            }
            let data_handle_id = existing.current_data_handle_id;
            self.storage
                .validate_data_handle_owner(data_handle_id, Some(existing_inode_id))?;
            let existing_layout = self.storage.get_layout(existing_inode_id)?;
            Ok(Some(FsCommandResult::Ok(FsOkResult {
                inode_id: Some(existing_inode_id),
                data_handle_id: Some(data_handle_id),
                file_version: match &existing.data {
                    InodeData::File { file_version, .. } => *file_version,
                    _ => None,
                },
                attrs: Some(existing.attrs.clone()),
                layout: Some(existing_layout),
            })))
        })();
        match existing {
            Ok(Some(result)) => {
                let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::Fs(result.clone()));
                self.storage
                    .put_apply_result_atomic(dedup_key, applied_result, raft_state)?;
                return Ok(result);
            }
            Ok(None) => {}
            Err(err) => return self.persist_fs_error(err, dedup_key, fingerprint, raft_state),
        }

        let prepared: MetadataResult<(FileAllocation, Inode, Inode, FsOkResult)> = (|| {
            // Check parent exists and is a directory
            let parent_inode = self
                .storage
                .get_inode(parent_inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Parent inode not found: {}", parent_inode_id)))?;
            if !parent_inode.kind.is_dir() {
                return Err(MetadataError::NotDir(format!(
                    "Parent is not a directory: {}",
                    parent_inode_id
                )));
            }

            // Generate inode ID
            let allocation = self.storage.prepare_file_allocation()?;
            let inode_id = allocation.inode.inode_id;
            let data_handle_id = allocation.data_handle_id;
            let now_ms = proposed_at_ms;

            // Initialize attrs
            attrs.update_timestamps(now_ms);
            attrs.nlink = 1;

            // Create file inode (inherit mount_id from parent) with a freshly allocated data handle.
            let inode = Inode::new_file(inode_id, attrs, parent_inode.mount_id, data_handle_id);

            // Update parent directory mtime/ctime
            let mut parent_attrs = parent_inode.attrs.clone();
            parent_attrs.update_mtime_ctime(Self::mutation_timestamp(&parent_inode, proposed_at_ms));
            let mut updated_parent = parent_inode.clone();
            updated_parent.attrs = parent_attrs;

            Ok(FsOkResult {
                inode_id: Some(inode_id),
                data_handle_id: Some(data_handle_id),
                file_version: None,
                attrs: Some(inode.attrs.clone()),
                layout: Some(layout),
            })
            .map(|ok| (allocation, inode, updated_parent, ok))
        })();

        let (allocation, inode, updated_parent, ok) = match prepared {
            Ok(prepared) => prepared,
            Err(err) => return self.persist_fs_error(err, dedup_key, fingerprint, raft_state),
        };
        let result = FsCommandResult::Ok(ok);
        let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::Fs(result.clone()));
        self.storage.create_file_with_apply_result_atomic(
            allocation,
            parent_inode_id,
            &name,
            &inode,
            &updated_parent,
            layout,
            dedup_key,
            applied_result,
            raft_state,
        )?;
        Ok(result)
    }

    /// Apply the stable Delete command by deciding the target kind atomically.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_delete(
        &self,
        parent_inode_id: InodeId,
        name: String,
        recursive: bool,
        proposed_at_ms: u64,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<FsCommandResult> {
        let child_inode_id = match self.storage.get_dentry(parent_inode_id, &name)? {
            Some(inode_id) => inode_id,
            None => {
                return self.persist_fs_error(
                    MetadataError::NotFound(format!("Entry not found: {name}")),
                    dedup_key,
                    fingerprint,
                    raft_state,
                );
            }
        };
        let child_inode = match self.storage.get_inode(child_inode_id)? {
            Some(inode) => inode,
            None => {
                return self.persist_fs_error(
                    MetadataError::NotFound(format!("Child inode not found: {child_inode_id}")),
                    dedup_key,
                    fingerprint,
                    raft_state,
                );
            }
        };

        if child_inode.kind.is_dir() {
            if recursive {
                self.apply_delete_tree(
                    parent_inode_id,
                    name,
                    proposed_at_ms,
                    dedup_key,
                    fingerprint,
                    raft_state,
                )
            } else {
                self.apply_delete_empty_dir(
                    parent_inode_id,
                    name,
                    proposed_at_ms,
                    dedup_key,
                    fingerprint,
                    raft_state,
                )
            }
        } else {
            self.apply_unlink(
                parent_inode_id,
                name,
                proposed_at_ms,
                dedup_key,
                fingerprint,
                raft_state,
            )
        }
    }

    /// Apply Unlink command.
    pub(super) fn apply_unlink(
        &self,
        parent_inode_id: InodeId,
        name: String,
        proposed_at_ms: u64,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<FsCommandResult> {
        let prepared: MetadataResult<PreparedUnlink> = (|| {
            // Get dentry
            let child_inode_id = self
                .storage
                .get_dentry(parent_inode_id, &name)?
                .ok_or_else(|| MetadataError::NotFound(format!("Entry not found: {}", name)))?;

            // Get child inode
            let child_inode = self
                .storage
                .get_inode(child_inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Child inode not found: {}", child_inode_id)))?;

            // Check it's not a directory
            if child_inode.kind.is_dir() {
                return Err(MetadataError::IsDir(format!("Cannot unlink directory: {}", name)));
            }

            // Update parent directory mtime/ctime
            let parent_inode = self
                .storage
                .get_inode(parent_inode_id)?
                .ok_or_else(|| MetadataError::Internal("Parent inode disappeared".to_string()))?;
            let mut parent_attrs = parent_inode.attrs.clone();
            parent_attrs.update_mtime_ctime(Self::mutation_timestamp(&parent_inode, proposed_at_ms));
            let mut updated_parent = parent_inode.clone();
            updated_parent.attrs = parent_attrs;

            let data_handle_id = match &child_inode.data {
                InodeData::File { .. } => {
                    let data_handle_id = child_inode.current_data_handle_id;
                    if data_handle_id.as_raw() == 0 {
                        return Err(MetadataError::Internal(format!(
                            "File inode {} is missing current_data_handle_id",
                            child_inode_id
                        )));
                    }
                    self.storage
                        .validate_data_handle_owner(data_handle_id, Some(child_inode_id))?;
                    Some(data_handle_id)
                }
                InodeData::Symlink { .. } => None,
                InodeData::Dir => return Err(MetadataError::IsDir(format!("Cannot unlink directory: {}", name))),
            };

            Ok(FsOkResult::default()).map(|ok| (child_inode_id, data_handle_id, updated_parent, ok))
        })();

        let (child_inode_id, data_handle_id, updated_parent, ok) = match prepared {
            Ok(prepared) => prepared,
            Err(err) => return self.persist_fs_error(err, dedup_key, fingerprint, raft_state),
        };
        let result = FsCommandResult::Ok(ok);
        let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::Fs(result.clone()));
        self.storage.delete_empty_file_with_apply_result_atomic(
            parent_inode_id,
            &name,
            child_inode_id,
            data_handle_id,
            &updated_parent,
            dedup_key,
            applied_result,
            raft_state,
        )?;
        Ok(result)
    }

    /// Apply empty-directory delete command.
    pub(super) fn apply_delete_empty_dir(
        &self,
        parent_inode_id: InodeId,
        name: String,
        proposed_at_ms: u64,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<FsCommandResult> {
        let prepared: MetadataResult<(InodeId, Inode, FsOkResult)> = (|| {
            // Get dentry
            let child_inode_id = self
                .storage
                .get_dentry(parent_inode_id, &name)?
                .ok_or_else(|| MetadataError::NotFound(format!("Directory not found: {}", name)))?;

            // Get child inode
            let child_inode = self
                .storage
                .get_inode(child_inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Child inode not found: {}", child_inode_id)))?;

            // Check it's a directory
            if !child_inode.kind.is_dir() {
                return Err(MetadataError::NotDir(format!("Not a directory: {}", name)));
            }

            // Check directory is empty
            if !self.storage.is_directory_empty(child_inode_id)? {
                return Err(MetadataError::DirectoryNotEmpty(format!(
                    "Directory not empty: {}",
                    name
                )));
            }

            // Update parent directory mtime/ctime
            let parent_inode = self
                .storage
                .get_inode(parent_inode_id)?
                .ok_or_else(|| MetadataError::Internal("Parent inode disappeared".to_string()))?;
            let mut parent_attrs = parent_inode.attrs.clone();
            parent_attrs.update_mtime_ctime(Self::mutation_timestamp(&parent_inode, proposed_at_ms));
            let mut updated_parent = parent_inode.clone();
            updated_parent.attrs = parent_attrs;

            Ok(FsOkResult::default()).map(|ok| (child_inode_id, updated_parent, ok))
        })();

        let (child_inode_id, updated_parent, ok) = match prepared {
            Ok(prepared) => prepared,
            Err(err) => return self.persist_fs_error(err, dedup_key, fingerprint, raft_state),
        };
        let result = FsCommandResult::Ok(ok);
        let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::Fs(result.clone()));
        self.storage.delete_empty_dir_with_apply_result_atomic(
            parent_inode_id,
            &name,
            child_inode_id,
            &updated_parent,
            dedup_key,
            applied_result,
            raft_state,
        )?;
        Ok(result)
    }

    pub(super) fn apply_delete_tree(
        &self,
        parent_inode_id: InodeId,
        name: String,
        proposed_at_ms: u64,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<FsCommandResult> {
        let prepared: MetadataResult<PreparedDeleteTree> = (|| {
            let root_inode_id = self
                .storage
                .get_dentry(parent_inode_id, &name)?
                .ok_or_else(|| MetadataError::NotFound(format!("Directory not found: {}", name)))?;
            let root_inode = self
                .storage
                .get_inode(root_inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Root inode not found: {}", root_inode_id)))?;
            if !root_inode.kind.is_dir() {
                return Err(MetadataError::NotDir(format!("Not a directory: {}", name)));
            }
            self.reject_mount_root_delete(root_inode_id)?;

            let parent_inode = self
                .storage
                .get_inode(parent_inode_id)?
                .ok_or_else(|| MetadataError::Internal("Parent inode disappeared".to_string()))?;
            if !parent_inode.kind.is_dir() {
                return Err(MetadataError::NotDir(format!(
                    "Parent is not a directory: {}",
                    parent_inode_id
                )));
            }
            if parent_inode.mount_id != root_inode.mount_id {
                return Err(MetadataError::CrossMountRename(
                    "recursive delete cannot cross mount boundary".to_string(),
                ));
            }

            let mut parent_attrs = parent_inode.attrs.clone();
            parent_attrs.update_mtime_ctime(Self::mutation_timestamp(&parent_inode, proposed_at_ms));
            let mut updated_parent = parent_inode;
            updated_parent.attrs = parent_attrs;

            let mut plan = DeleteTreePlan {
                root_mount_id: updated_parent.mount_id,
                entries: Vec::new(),
            };
            self.prepare_delete_tree_node(parent_inode_id, name, root_inode_id, root_inode, &mut plan)?;

            Ok(PreparedDeleteTree {
                updated_parent,
                entries: plan.entries,
            })
        })();

        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(err) => return self.persist_fs_error(err, dedup_key, fingerprint, raft_state),
        };
        let result = FsCommandResult::Ok(FsOkResult::default());
        let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::Fs(result.clone()));
        self.storage.delete_tree_with_apply_result_atomic(
            DeleteTreeAtomicUpdate {
                entries: &prepared.entries,
                updated_parent: &prepared.updated_parent,
            },
            dedup_key,
            applied_result,
            raft_state,
        )?;
        Ok(result)
    }

    fn prepare_delete_tree_node(
        &self,
        parent_inode_id: InodeId,
        name: String,
        inode_id: InodeId,
        inode: Inode,
        plan: &mut DeleteTreePlan,
    ) -> MetadataResult<()> {
        if inode.mount_id != plan.root_mount_id || self.is_mount_root_inode(inode_id)? {
            return Err(MetadataError::CrossMountRename(
                "recursive delete cannot cross mount boundary".to_string(),
            ));
        }

        let (data_handle_id, layout) = match &inode.data {
            InodeData::Dir => {
                let mut children = self.storage.list_dentries(inode_id)?;
                children.sort_by(|left, right| left.0.cmp(&right.0));
                for (child_name, child_inode_id) in children {
                    let child_inode = self
                        .storage
                        .get_inode(child_inode_id)?
                        .ok_or_else(|| MetadataError::NotFound(format!("Child inode not found: {}", child_inode_id)))?;
                    self.prepare_delete_tree_node(inode_id, child_name, child_inode_id, child_inode, plan)?;
                }
                (None, None)
            }
            InodeData::File { .. } => {
                let data_handle_id = inode.current_data_handle_id;
                if data_handle_id.as_raw() == 0 {
                    return Err(MetadataError::Internal(format!(
                        "File inode {} is missing current_data_handle_id",
                        inode_id
                    )));
                }
                self.storage
                    .validate_data_handle_owner(data_handle_id, Some(inode_id))?;
                let layout = self.storage.get_layout(inode_id).map_err(|err| match err {
                    MetadataError::NotFound(_) => {
                        MetadataError::InvalidArgument(format!("Missing layout for file inode {}", inode_id))
                    }
                    err => err,
                })?;
                (Some(data_handle_id), Some(layout))
            }
            InodeData::Symlink { .. } => (None, None),
        };

        plan.entries.push(DeleteTreeEntry {
            parent_inode_id,
            name,
            inode_id,
            data_handle_id,
            layout,
        });
        Ok(())
    }

    fn reject_mount_root_delete(&self, inode_id: InodeId) -> MetadataResult<()> {
        if inode_id == crate::mount::ROOT_INODE_ID || self.is_mount_root_inode(inode_id)? {
            return Err(MetadataError::InvalidArgument(format!(
                "Cannot delete mount root inode {}",
                inode_id
            )));
        }
        Ok(())
    }

    fn is_mount_root_inode(&self, inode_id: InodeId) -> MetadataResult<bool> {
        Ok(self
            .storage
            .list_mounts()?
            .iter()
            .any(|entry| entry.root_inode_id == inode_id))
    }

    /// Apply Rename command (atomic within mount).
    // Raft apply helpers mirror command payload fields for replay clarity.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_rename(
        &self,
        src_parent_inode_id: InodeId,
        src_name: String,
        dst_parent_inode_id: InodeId,
        dst_name: String,
        flags: u32,
        proposed_at_ms: u64,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<FsCommandResult> {
        let prepared: MetadataResult<PreparedRename> = (|| {
            // Get source dentry
            let src_inode_id = self
                .storage
                .get_dentry(src_parent_inode_id, &src_name)?
                .ok_or_else(|| MetadataError::NotFound(format!("Source not found: {}", src_name)))?;

            // Get source inode
            let src_inode = self
                .storage
                .get_inode(src_inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Source inode not found: {}", src_inode_id)))?;

            let mut overwritten_target = None;

            // Check if destination exists
            if let Some(dst_inode_id) = self.storage.get_dentry(dst_parent_inode_id, &dst_name)? {
                // NOREPLACE flag set -> fail when destination exists
                if flags & 0x1 != 0 {
                    return Err(MetadataError::AlreadyExists(format!(
                        "Destination exists and RENAME_NOREPLACE set: {}",
                        dst_name
                    )));
                }
                if src_inode_id == dst_inode_id {
                    return Ok(PreparedRename {
                        src_inode_id,
                        overwritten_target: None,
                        updated_src_parent: None,
                        updated_dst_parent: None,
                        updated_src_inode: src_inode,
                    });
                }
                // Destination exists - check if it's a directory and empty (if source is directory)
                let dst_inode = self
                    .storage
                    .get_inode(dst_inode_id)?
                    .ok_or_else(|| MetadataError::Internal("Destination inode disappeared".to_string()))?;

                if src_inode.kind.is_dir() {
                    if !dst_inode.kind.is_dir() {
                        return Err(MetadataError::NotDir(
                            "Cannot overwrite non-directory with directory".to_string(),
                        ));
                    }
                    if !self.storage.is_directory_empty(dst_inode_id)? {
                        return Err(MetadataError::DirectoryNotEmpty(
                            "Cannot overwrite non-empty directory".to_string(),
                        ));
                    }
                } else {
                    if dst_inode.kind.is_dir() {
                        return Err(MetadataError::IsDir("Cannot overwrite directory with file".to_string()));
                    }
                }
                overwritten_target = Some(self.prepare_rename_overwrite_target_cleanup(dst_inode_id, &dst_inode)?);
            }

            // Update parent directories mtime/ctime
            let (updated_src_parent, updated_dst_parent) = if src_parent_inode_id != dst_parent_inode_id {
                // Different parents - update both
                let src_parent = self
                    .storage
                    .get_inode(src_parent_inode_id)?
                    .ok_or_else(|| MetadataError::Internal("Source parent disappeared".to_string()))?;
                let mut src_attrs = src_parent.attrs.clone();
                src_attrs.update_mtime_ctime(Self::mutation_timestamp(&src_parent, proposed_at_ms));
                let mut src_parent = src_parent.clone();
                src_parent.attrs = src_attrs;
                let dst_parent = self
                    .storage
                    .get_inode(dst_parent_inode_id)?
                    .ok_or_else(|| MetadataError::Internal("Destination parent disappeared".to_string()))?;
                let mut dst_attrs = dst_parent.attrs.clone();
                dst_attrs.update_mtime_ctime(Self::mutation_timestamp(&dst_parent, proposed_at_ms));
                let mut dst_parent = dst_parent.clone();
                dst_parent.attrs = dst_attrs;
                (Some(src_parent), Some(dst_parent))
            } else {
                let parent = self
                    .storage
                    .get_inode(src_parent_inode_id)?
                    .ok_or_else(|| MetadataError::Internal("Parent disappeared".to_string()))?;
                let mut attrs = parent.attrs.clone();
                attrs.update_mtime_ctime(Self::mutation_timestamp(&parent, proposed_at_ms));
                let mut parent = parent.clone();
                parent.attrs = attrs;
                (Some(parent), None)
            };

            // Update source inode ctime
            let mut src_attrs = src_inode.attrs.clone();
            src_attrs.update_ctime(Self::mutation_timestamp(&src_inode, proposed_at_ms));
            let mut updated_src_inode = src_inode.clone();
            updated_src_inode.attrs = src_attrs;

            Ok(PreparedRename {
                src_inode_id,
                overwritten_target,
                updated_src_parent,
                updated_dst_parent,
                updated_src_inode,
            })
        })();

        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(err) => return self.persist_fs_error(err, dedup_key, fingerprint, raft_state),
        };
        let result = FsCommandResult::Ok(FsOkResult::default());
        let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::Fs(result.clone()));
        self.storage.rename_with_apply_result_atomic(
            RenameAtomicUpdate {
                src_parent_inode_id,
                src_name: &src_name,
                dst_parent_inode_id,
                dst_name: &dst_name,
                src_inode_id: prepared.src_inode_id,
                overwritten_target: prepared
                    .overwritten_target
                    .as_ref()
                    .map(|target| RenameOverwriteCleanup {
                        inode_id: target.inode_id,
                        data_handle_id: target.data_handle_id,
                    }),
                updated_src_parent: prepared.updated_src_parent.as_ref(),
                updated_dst_parent: prepared.updated_dst_parent.as_ref(),
                updated_src_inode: &prepared.updated_src_inode,
            },
            dedup_key,
            applied_result,
            raft_state,
        )?;

        Ok(result)
    }

    fn prepare_rename_overwrite_target_cleanup(
        &self,
        dst_inode_id: InodeId,
        dst_inode: &Inode,
    ) -> MetadataResult<PreparedRenameOverwrite> {
        match &dst_inode.data {
            InodeData::File { .. } => {
                let data_handle_id = dst_inode.current_data_handle_id;
                if data_handle_id.as_raw() == 0 {
                    return Err(MetadataError::Internal(format!(
                        "File inode {} is missing current_data_handle_id",
                        dst_inode_id
                    )));
                }
                self.storage
                    .validate_data_handle_owner(data_handle_id, Some(dst_inode_id))?;
                Ok(PreparedRenameOverwrite {
                    inode_id: dst_inode_id,
                    data_handle_id: Some(data_handle_id),
                })
            }
            InodeData::Dir => {
                if !self.storage.is_directory_empty(dst_inode_id)? {
                    return Err(MetadataError::DirectoryNotEmpty(
                        "Cannot overwrite non-empty directory".to_string(),
                    ));
                }
                Ok(PreparedRenameOverwrite {
                    inode_id: dst_inode_id,
                    data_handle_id: None,
                })
            }
            InodeData::Symlink { .. } => Ok(PreparedRenameOverwrite {
                inode_id: dst_inode_id,
                data_handle_id: None,
            }),
        }
    }

    /// Apply SetAttr command.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_set_attr(
        &self,
        inode_id: InodeId,
        mask: u32,
        new_attrs: FileAttrs,
        proposed_at_ms: u64,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<FsCommandResult> {
        let prepared: MetadataResult<(Inode, FsOkResult)> = (|| {
            let mut inode = self
                .storage
                .get_inode(inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {}", inode_id)))?;

            let now_ms = Self::mutation_timestamp(&inode, proposed_at_ms);
            let size_changes_visible_file_state =
                inode.kind.is_file() && mask & 1 != 0 && new_attrs.size != inode.attrs.size;

            // Apply mask: only update fields specified by mask
            // Bit flags: 1=size, 2=mode, 4=uid, 8=gid, 16=atime, 32=mtime
            if mask & 1 != 0 {
                inode.attrs.size = new_attrs.size;
            }
            if mask & 2 != 0 {
                inode.attrs.mode = new_attrs.mode;
            }
            if mask & 4 != 0 {
                inode.attrs.uid = new_attrs.uid;
            }
            if mask & 8 != 0 {
                inode.attrs.gid = new_attrs.gid;
            }
            if mask & 16 != 0 {
                inode.attrs.atime_ms = new_attrs.atime_ms;
            }
            if mask & 32 != 0 {
                inode.attrs.mtime_ms = new_attrs.mtime_ms;
            }

            // Always update ctime
            inode.attrs.ctime_ms = now_ms;

            let file_version = if size_changes_visible_file_state {
                match &mut inode.data {
                    InodeData::File {
                        extents, file_version, ..
                    } => {
                        let next = Self::next_file_version(inode_id, *file_version)?;
                        for extent in extents.iter_mut() {
                            extent.file_version = Some(next);
                        }
                        *file_version = Some(next);
                        Some(next)
                    }
                    _ => None,
                }
            } else {
                None
            };

            Ok((
                inode,
                FsOkResult {
                    inode_id: Some(inode_id),
                    file_version,
                    ..FsOkResult::default()
                },
            ))
        })();

        let (inode, ok) = match prepared {
            Ok(prepared) => prepared,
            Err(err) => return self.persist_fs_error(err, dedup_key, fingerprint, raft_state),
        };
        let result = FsCommandResult::Ok(ok);
        let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::Fs(result.clone()));
        self.storage
            .put_inode_with_apply_result_atomic(&inode, dedup_key, applied_result, raft_state)?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::state_machine::tests::*;

    #[test]
    fn create_file_persists_data_handle_mapping() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let parent_inode_id = InodeId::new(10);
        let mount_id = MountId::new(1);
        let parent = Inode::new_dir(parent_inode_id, FileAttrs::new(), mount_id);
        storage.put_inode(&parent).unwrap();

        let cmd = Command::new(
            crate::raft::DedupKey::new(ClientId::new(10), CallId::new()),
            crate::raft::proposal_timestamp_ms(),
            crate::raft::Mutation::CreateFile {
                mode: crate::raft::CreateFileMode::CreateNew,
                parent_inode_id,
                name: "file".to_string(),
                attrs: FileAttrs::new(),
                layout: FileLayout::new(4096, 4096, 1),
            },
        );

        let raw = sm.apply(cmd).unwrap();
        let inode_id = match raw {
            AppDataResponse::Fs(FsCommandResult::Ok(ok)) => ok.inode_id.expect("inode id should be returned"),
            other => panic!("unexpected apply response: {:?}", other),
        };

        let inode = storage.get_inode(inode_id).unwrap().expect("inode should exist");
        let handle = inode.current_data_handle_id;
        assert_ne!(handle.as_raw(), 0, "create must allocate a data handle");

        let mapped = storage
            .get_inode_by_data_handle(handle)
            .unwrap()
            .expect("mapping should exist");
        assert_eq!(mapped, inode_id, "data handle owner mapping must match created inode");
        assert_eq!(storage.get_dentry(parent_inode_id, "file").unwrap(), Some(inode_id));
        assert_eq!(storage.get_layout(inode_id).unwrap(), FileLayout::new(4096, 4096, 1));
    }

    #[test]
    fn create_reapply_returns_original_success_result_and_replay_result() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let parent_inode_id = InodeId::new(10);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1)))
            .unwrap();

        let dedup = dedup_for_test(41);
        let cmd = Command::new(
            dedup.clone(),
            crate::raft::proposal_timestamp_ms(),
            crate::raft::Mutation::CreateFile {
                mode: crate::raft::CreateFileMode::CreateNew,
                parent_inode_id,
                name: "file".to_string(),
                attrs: FileAttrs::new(),
                layout: FileLayout::new(4096, 4096, 1),
            },
        );

        let first = expect_fs_ok(sm.apply(cmd.clone()).unwrap());

        let second = expect_fs_ok(sm.apply(cmd).unwrap());
        assert_eq!(second, first);

        let inode_id = first.inode_id.expect("inode id should be returned");
        assert_eq!(storage.get_dentry(parent_inode_id, "file").unwrap(), Some(inode_id));
        assert!(storage.get_applied_result(&dedup).unwrap().is_some());
    }

    #[test]
    fn create_or_overwrite_reuses_existing_file_authority_atomically() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));
        let parent_inode_id = InodeId::new(10);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1)))
            .unwrap();
        let first = expect_fs_ok(
            sm.apply(Command::new(
                dedup_for_test(411),
                crate::raft::proposal_timestamp_ms(),
                crate::raft::Mutation::CreateFile {
                    mode: crate::raft::CreateFileMode::CreateNew,
                    parent_inode_id,
                    name: "file".to_string(),
                    attrs: FileAttrs::new(),
                    layout: FileLayout::new(4096, 4096, 1),
                },
            ))
            .unwrap(),
        );
        let overwrite = expect_fs_ok(
            sm.apply(Command::new(
                dedup_for_test(412),
                crate::raft::proposal_timestamp_ms(),
                crate::raft::Mutation::CreateFile {
                    mode: crate::raft::CreateFileMode::CreateOrOverwrite,
                    parent_inode_id,
                    name: "file".to_string(),
                    attrs: FileAttrs::new(),
                    layout: FileLayout::new(8192, 8192, 1),
                },
            ))
            .unwrap(),
        );

        assert_eq!(overwrite.inode_id, first.inode_id);
        assert_eq!(overwrite.data_handle_id, first.data_handle_id);
        let inode_id = first.inode_id.unwrap();
        assert_eq!(storage.get_dentry(parent_inode_id, "file").unwrap(), Some(inode_id));
        assert_eq!(storage.get_layout(inode_id).unwrap(), FileLayout::new(4096, 4096, 1));
    }

    #[test]
    fn dedup_rejects_same_call_id_for_different_mutation_method() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));
        let parent_inode_id = InodeId::new(10);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1)))
            .unwrap();
        let dedup = dedup_for_test(413);
        sm.apply(Command::new(
            dedup.clone(),
            crate::raft::proposal_timestamp_ms(),
            crate::raft::Mutation::CreateFile {
                mode: crate::raft::CreateFileMode::CreateNew,
                parent_inode_id,
                name: "file".to_string(),
                attrs: FileAttrs::new(),
                layout: FileLayout::new(4096, 4096, 1),
            },
        ))
        .unwrap();
        let err = sm
            .apply(Command::new(
                dedup,
                crate::raft::proposal_timestamp_ms(),
                crate::raft::Mutation::Mkdir {
                    parent_inode_id,
                    name: "dir".to_string(),
                    attrs: FileAttrs::new(),
                },
            ))
            .unwrap_err();

        assert!(matches!(err, MetadataError::InvalidArgument(_)));
        assert_eq!(storage.get_dentry(parent_inode_id, "dir").unwrap(), None);
    }

    #[test]
    fn mkdir_persists_inode_and_dentry() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let parent_inode_id = InodeId::new(10);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1)))
            .unwrap();

        let raw = sm
            .apply(Command::new(
                dedup_for_test(29),
                crate::raft::proposal_timestamp_ms(),
                crate::raft::Mutation::Mkdir {
                    parent_inode_id,
                    name: "dir".to_string(),
                    attrs: FileAttrs::new(),
                },
            ))
            .unwrap();
        let inode_id = match raw {
            AppDataResponse::Fs(FsCommandResult::Ok(ok)) => ok.inode_id.expect("inode id should be returned"),
            other => panic!("unexpected apply response: {:?}", other),
        };

        assert!(storage.get_inode(inode_id).unwrap().unwrap().kind.is_dir());
        assert_eq!(storage.get_dentry(parent_inode_id, "dir").unwrap(), Some(inode_id));
    }

    #[test]
    fn mkdir_reapply_returns_original_success_result_and_replay_result() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let parent_inode_id = InodeId::new(10);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1)))
            .unwrap();

        let dedup = dedup_for_test(42);
        let cmd = Command::new(
            dedup.clone(),
            crate::raft::proposal_timestamp_ms(),
            crate::raft::Mutation::Mkdir {
                parent_inode_id,
                name: "dir".to_string(),
                attrs: FileAttrs::new(),
            },
        );

        let first = expect_fs_ok(sm.apply(cmd.clone()).unwrap());

        let second = expect_fs_ok(sm.apply(cmd).unwrap());
        assert_eq!(second, first);

        let inode_id = first.inode_id.expect("inode id should be returned");
        assert_eq!(storage.get_dentry(parent_inode_id, "dir").unwrap(), Some(inode_id));
        assert!(storage.get_applied_result(&dedup).unwrap().is_some());
    }

    #[test]
    fn rename_moves_dentry_and_preserves_inode() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let parent_inode_id = InodeId::new(10);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1)))
            .unwrap();

        let created = sm
            .apply(Command::new(
                dedup_for_test(36),
                crate::raft::proposal_timestamp_ms(),
                crate::raft::Mutation::CreateFile {
                    mode: crate::raft::CreateFileMode::CreateNew,
                    parent_inode_id,
                    name: "old".to_string(),
                    attrs: FileAttrs::new(),
                    layout: FileLayout::new(4096, 4096, 1),
                },
            ))
            .unwrap();
        let inode_id = match created {
            AppDataResponse::Fs(FsCommandResult::Ok(ok)) => ok.inode_id.unwrap(),
            other => panic!("unexpected apply response: {:?}", other),
        };

        sm.apply(Command::new(
            dedup_for_test(37),
            crate::raft::proposal_timestamp_ms(),
            crate::raft::Mutation::Rename {
                src_parent_inode_id: parent_inode_id,
                src_name: "old".to_string(),
                dst_parent_inode_id: parent_inode_id,
                dst_name: "new".to_string(),
                flags: 0,
            },
        ))
        .unwrap();

        assert_eq!(storage.get_dentry(parent_inode_id, "old").unwrap(), None);
        assert_eq!(storage.get_dentry(parent_inode_id, "new").unwrap(), Some(inode_id));
        assert!(storage.get_inode(inode_id).unwrap().is_some());
    }

    #[test]
    fn rename_reapply_returns_original_success_result_and_replay_result() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let parent_inode_id = InodeId::new(10);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1)))
            .unwrap();

        let created = expect_fs_ok(
            sm.apply(Command::new(
                dedup_for_test(43),
                crate::raft::proposal_timestamp_ms(),
                crate::raft::Mutation::CreateFile {
                    mode: crate::raft::CreateFileMode::CreateNew,
                    parent_inode_id,
                    name: "old".to_string(),
                    attrs: FileAttrs::new(),
                    layout: FileLayout::new(4096, 4096, 1),
                },
            ))
            .unwrap(),
        );
        let inode_id = created.inode_id.unwrap();

        let dedup = dedup_for_test(44);
        let cmd = Command::new(
            dedup.clone(),
            crate::raft::proposal_timestamp_ms(),
            crate::raft::Mutation::Rename {
                src_parent_inode_id: parent_inode_id,
                src_name: "old".to_string(),
                dst_parent_inode_id: parent_inode_id,
                dst_name: "new".to_string(),
                flags: 0,
            },
        );

        let first = expect_fs_ok(sm.apply(cmd.clone()).unwrap());
        assert_eq!(first, FsOkResult::default());

        let second = expect_fs_ok(sm.apply(cmd).unwrap());
        assert_eq!(second, first);
        assert_eq!(storage.get_dentry(parent_inode_id, "old").unwrap(), None);
        assert_eq!(storage.get_dentry(parent_inode_id, "new").unwrap(), Some(inode_id));
        assert!(storage.get_applied_result(&dedup).unwrap().is_some());
    }

    #[test]
    fn set_attr_reapply_returns_original_result_and_replay_result() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let inode_id = InodeId::new(70);
        storage
            .put_inode(&Inode::new_file(
                inode_id,
                FileAttrs::new(),
                MountId::new(1),
                DataHandleId::new(700),
            ))
            .unwrap();

        let mut attrs = FileAttrs::new();
        attrs.uid = 123;
        let set_attr = Command::new(
            dedup_for_test(70),
            crate::raft::proposal_timestamp_ms(),
            crate::raft::Mutation::SetAttr {
                inode_id,
                mask: 4,
                attrs,
            },
        );
        let first = expect_fs_ok(sm.apply(set_attr.clone()).unwrap());
        let ctime_after_first = storage.get_inode(inode_id).unwrap().unwrap().attrs.ctime_ms;
        let second = expect_fs_ok(sm.apply(set_attr).unwrap());
        assert_eq!(second, first);
        let stored = storage.get_inode(inode_id).unwrap().unwrap();
        assert_eq!(stored.attrs.uid, 123);
        assert_eq!(stored.attrs.ctime_ms, ctime_after_first);
    }

    #[test]
    fn rename_overwrites_empty_file() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let parent_inode_id = InodeId::new(10);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1)))
            .unwrap();

        let source = sm
            .apply(Command::new(
                dedup_for_test(38),
                crate::raft::proposal_timestamp_ms(),
                crate::raft::Mutation::CreateFile {
                    mode: crate::raft::CreateFileMode::CreateNew,
                    parent_inode_id,
                    name: "source".to_string(),
                    attrs: FileAttrs::new(),
                    layout: FileLayout::new(4096, 4096, 1),
                },
            ))
            .unwrap();
        let source_inode_id = match source {
            AppDataResponse::Fs(FsCommandResult::Ok(ok)) => ok.inode_id.unwrap(),
            other => panic!("unexpected apply response: {:?}", other),
        };

        let target = sm
            .apply(Command::new(
                dedup_for_test(39),
                crate::raft::proposal_timestamp_ms(),
                crate::raft::Mutation::CreateFile {
                    mode: crate::raft::CreateFileMode::CreateNew,
                    parent_inode_id,
                    name: "target".to_string(),
                    attrs: FileAttrs::new(),
                    layout: FileLayout::new(8192, 8192, 1),
                },
            ))
            .unwrap();
        let target_inode_id = match target {
            AppDataResponse::Fs(FsCommandResult::Ok(ok)) => ok.inode_id.unwrap(),
            other => panic!("unexpected apply response: {:?}", other),
        };
        let target_inode = storage.get_inode(target_inode_id).unwrap().unwrap();
        let target_handle = target_inode.current_data_handle_id;
        let source_handle = storage
            .get_inode(source_inode_id)
            .unwrap()
            .unwrap()
            .current_data_handle_id;

        expect_fs_ok(
            sm.apply(Command::new(
                dedup_for_test(40),
                crate::raft::proposal_timestamp_ms(),
                crate::raft::Mutation::Rename {
                    src_parent_inode_id: parent_inode_id,
                    src_name: "source".to_string(),
                    dst_parent_inode_id: parent_inode_id,
                    dst_name: "target".to_string(),
                    flags: 0,
                },
            ))
            .unwrap(),
        );

        assert_eq!(storage.get_dentry(parent_inode_id, "source").unwrap(), None);
        assert_eq!(
            storage.get_dentry(parent_inode_id, "target").unwrap(),
            Some(source_inode_id)
        );
        assert!(storage.get_inode(source_inode_id).unwrap().is_some());
        assert!(storage.get_inode(target_inode_id).unwrap().is_none());
        assert_eq!(
            storage.get_inode_by_data_handle(source_handle).unwrap(),
            Some(source_inode_id)
        );
        assert_eq!(storage.get_inode_by_data_handle(target_handle).unwrap(), None);
        assert!(storage.get_layout(target_inode_id).is_err());
    }

    #[test]
    fn rename_overwrites_file_with_extents() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let parent_inode_id = InodeId::new(110);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1)))
            .unwrap();
        let source = expect_fs_ok(
            sm.apply(Command::new(
                dedup_for_test(110),
                crate::raft::proposal_timestamp_ms(),
                crate::raft::Mutation::CreateFile {
                    mode: crate::raft::CreateFileMode::CreateNew,
                    parent_inode_id,
                    name: "source".to_string(),
                    attrs: FileAttrs::new(),
                    layout: FileLayout::new(4096, 4096, 1),
                },
            ))
            .unwrap(),
        );
        let source_inode_id = source.inode_id.unwrap();
        let target_inode_id = InodeId::new(1111);
        let target_handle = DataHandleId::new(112);
        let block_id = BlockId::new(target_handle, BlockIndex::new(0));
        install_file_with_extents(
            &storage,
            parent_inode_id,
            "target",
            target_inode_id,
            target_handle,
            vec![extent(block_id, 0, 128)],
            128,
        );

        expect_fs_ok(
            sm.apply(Command::new(
                dedup_for_test(111),
                crate::raft::proposal_timestamp_ms(),
                crate::raft::Mutation::Rename {
                    src_parent_inode_id: parent_inode_id,
                    src_name: "source".to_string(),
                    dst_parent_inode_id: parent_inode_id,
                    dst_name: "target".to_string(),
                    flags: 0,
                },
            ))
            .unwrap(),
        );

        assert_eq!(
            storage.get_dentry(parent_inode_id, "target").unwrap(),
            Some(source_inode_id)
        );
        assert!(storage.get_inode(target_inode_id).unwrap().is_none());
        assert!(storage.get_layout(target_inode_id).is_err());
        assert_eq!(storage.get_inode_by_data_handle(target_handle).unwrap(), None);
    }

    #[test]
    fn rename_overwrite_removes_target_authority() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let parent_inode_id = InodeId::new(120);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1)))
            .unwrap();
        expect_fs_ok(
            sm.apply(Command::new(
                dedup_for_test(120),
                crate::raft::proposal_timestamp_ms(),
                crate::raft::Mutation::CreateFile {
                    mode: crate::raft::CreateFileMode::CreateNew,
                    parent_inode_id,
                    name: "source".to_string(),
                    attrs: FileAttrs::new(),
                    layout: FileLayout::new(4096, 4096, 1),
                },
            ))
            .unwrap(),
        );
        let target_inode_id = InodeId::new(1221);
        let target_handle = DataHandleId::new(122);
        let block_id = BlockId::new(target_handle, BlockIndex::new(0));
        install_file_with_extents(
            &storage,
            parent_inode_id,
            "target",
            target_inode_id,
            target_handle,
            vec![extent(block_id, 0, 128)],
            128,
        );

        expect_fs_ok(
            sm.apply(Command::new(
                dedup_for_test(121),
                crate::raft::proposal_timestamp_ms(),
                crate::raft::Mutation::Rename {
                    src_parent_inode_id: parent_inode_id,
                    src_name: "source".to_string(),
                    dst_parent_inode_id: parent_inode_id,
                    dst_name: "target".to_string(),
                    flags: 0,
                },
            ))
            .unwrap(),
        );
    }

    #[test]
    fn rename_rejects_non_empty_directory_target() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let parent_inode_id = InodeId::new(140);
        let source_dir_id = InodeId::new(141);
        let target_dir_id = InodeId::new(142);
        let child_inode_id = InodeId::new(143);
        let mount_id = MountId::new(1);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), mount_id))
            .unwrap();
        storage
            .put_inode(&Inode::new_dir(source_dir_id, FileAttrs::new(), mount_id))
            .unwrap();
        storage
            .put_inode(&Inode::new_dir(target_dir_id, FileAttrs::new(), mount_id))
            .unwrap();
        storage
            .put_inode(&Inode::new_file(
                child_inode_id,
                FileAttrs::new(),
                mount_id,
                DataHandleId::new(143),
            ))
            .unwrap();
        storage.put_dentry(parent_inode_id, "source", source_dir_id).unwrap();
        storage.put_dentry(parent_inode_id, "target", target_dir_id).unwrap();
        storage.put_dentry(target_dir_id, "child", child_inode_id).unwrap();

        expect_fs_errno(
            sm.apply(Command::new(
                dedup_for_test(140),
                crate::raft::proposal_timestamp_ms(),
                crate::raft::Mutation::Rename {
                    src_parent_inode_id: parent_inode_id,
                    src_name: "source".to_string(),
                    dst_parent_inode_id: parent_inode_id,
                    dst_name: "target".to_string(),
                    flags: 0,
                },
            ))
            .unwrap(),
            FsErrorCode::ENotEmpty,
        );

        assert_eq!(
            storage.get_dentry(parent_inode_id, "source").unwrap(),
            Some(source_dir_id)
        );
        assert_eq!(
            storage.get_dentry(parent_inode_id, "target").unwrap(),
            Some(target_dir_id)
        );
        assert_eq!(
            storage.get_dentry(target_dir_id, "child").unwrap(),
            Some(child_inode_id)
        );
    }

    #[test]
    fn rename_overwrite_is_dedup_safe() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let parent_inode_id = InodeId::new(150);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1)))
            .unwrap();
        let source = expect_fs_ok(
            sm.apply(Command::new(
                dedup_for_test(150),
                crate::raft::proposal_timestamp_ms(),
                crate::raft::Mutation::CreateFile {
                    mode: crate::raft::CreateFileMode::CreateNew,
                    parent_inode_id,
                    name: "source".to_string(),
                    attrs: FileAttrs::new(),
                    layout: FileLayout::new(4096, 4096, 1),
                },
            ))
            .unwrap(),
        );
        let source_inode_id = source.inode_id.unwrap();
        let target = expect_fs_ok(
            sm.apply(Command::new(
                dedup_for_test(151),
                crate::raft::proposal_timestamp_ms(),
                crate::raft::Mutation::CreateFile {
                    mode: crate::raft::CreateFileMode::CreateNew,
                    parent_inode_id,
                    name: "target".to_string(),
                    attrs: FileAttrs::new(),
                    layout: FileLayout::new(8192, 8192, 1),
                },
            ))
            .unwrap(),
        );
        let target_inode_id = target.inode_id.unwrap();

        let dedup = dedup_for_test(152);
        let command = Command::new(
            dedup.clone(),
            crate::raft::proposal_timestamp_ms(),
            crate::raft::Mutation::Rename {
                src_parent_inode_id: parent_inode_id,
                src_name: "source".to_string(),
                dst_parent_inode_id: parent_inode_id,
                dst_name: "target".to_string(),
                flags: 0,
            },
        );

        let first = expect_fs_ok(sm.apply(command.clone()).unwrap());
        let second = expect_fs_ok(sm.apply(command).unwrap());

        assert_eq!(second, first);
        assert_eq!(
            storage.get_dentry(parent_inode_id, "target").unwrap(),
            Some(source_inode_id)
        );
        assert!(storage.get_inode(target_inode_id).unwrap().is_none());
        assert!(storage.get_applied_result(&dedup).unwrap().is_some());
    }

    #[test]
    fn rename_overwrite_replay_is_stable() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let parent_inode_id = InodeId::new(160);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1)))
            .unwrap();
        expect_fs_ok(
            sm.apply(Command::new(
                dedup_for_test(160),
                crate::raft::proposal_timestamp_ms(),
                crate::raft::Mutation::CreateFile {
                    mode: crate::raft::CreateFileMode::CreateNew,
                    parent_inode_id,
                    name: "source".to_string(),
                    attrs: FileAttrs::new(),
                    layout: FileLayout::new(4096, 4096, 1),
                },
            ))
            .unwrap(),
        );
        let target_inode_id = InodeId::new(1661);
        let target_handle = DataHandleId::new(162);
        let block_id = BlockId::new(target_handle, BlockIndex::new(0));
        install_file_with_extents(
            &storage,
            parent_inode_id,
            "target",
            target_inode_id,
            target_handle,
            vec![extent(block_id, 0, 128)],
            128,
        );

        let command = Command::new(
            dedup_for_test(161),
            crate::raft::proposal_timestamp_ms(),
            crate::raft::Mutation::Rename {
                src_parent_inode_id: parent_inode_id,
                src_name: "source".to_string(),
                dst_parent_inode_id: parent_inode_id,
                dst_name: "target".to_string(),
                flags: 0,
            },
        );

        expect_fs_ok(sm.apply(command.clone()).unwrap());

        expect_fs_ok(sm.apply(command).unwrap());
    }

    #[test]
    fn rename_reusing_call_id_for_different_target_is_rejected() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let parent_inode_id = InodeId::new(170);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1)))
            .unwrap();
        let source = expect_fs_ok(
            sm.apply(Command::new(
                dedup_for_test(170),
                crate::raft::proposal_timestamp_ms(),
                crate::raft::Mutation::CreateFile {
                    mode: crate::raft::CreateFileMode::CreateNew,
                    parent_inode_id,
                    name: "source".to_string(),
                    attrs: FileAttrs::new(),
                    layout: FileLayout::new(4096, 4096, 1),
                },
            ))
            .unwrap(),
        );
        let source_inode_id = source.inode_id.unwrap();

        let dedup = dedup_for_test(171);
        expect_fs_ok(
            sm.apply(Command::new(
                dedup.clone(),
                crate::raft::proposal_timestamp_ms(),
                crate::raft::Mutation::Rename {
                    src_parent_inode_id: parent_inode_id,
                    src_name: "source".to_string(),
                    dst_parent_inode_id: parent_inode_id,
                    dst_name: "first".to_string(),
                    flags: 0,
                },
            ))
            .unwrap(),
        );

        let mismatch = sm
            .apply(Command::new(
                dedup,
                crate::raft::proposal_timestamp_ms(),
                crate::raft::Mutation::Rename {
                    src_parent_inode_id: parent_inode_id,
                    src_name: "first".to_string(),
                    dst_parent_inode_id: parent_inode_id,
                    dst_name: "second".to_string(),
                    flags: 0,
                },
            ))
            .unwrap_err();

        assert!(matches!(mismatch, MetadataError::InvalidArgument(_)));
        assert_eq!(
            storage.get_dentry(parent_inode_id, "first").unwrap(),
            Some(source_inode_id)
        );
        assert_eq!(storage.get_dentry(parent_inode_id, "second").unwrap(), None);
    }

    #[test]
    fn create_allocates_distinct_inode_ids() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let parent_inode_id = InodeId::new(10);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1)))
            .unwrap();

        let first = sm
            .apply(Command::new(
                dedup_for_test(30),
                crate::raft::proposal_timestamp_ms(),
                crate::raft::Mutation::CreateFile {
                    mode: crate::raft::CreateFileMode::CreateNew,
                    parent_inode_id,
                    name: "first".to_string(),
                    attrs: FileAttrs::new(),
                    layout: FileLayout::new(4096, 4096, 1),
                },
            ))
            .unwrap();
        let second = sm
            .apply(Command::new(
                dedup_for_test(31),
                crate::raft::proposal_timestamp_ms(),
                crate::raft::Mutation::CreateFile {
                    mode: crate::raft::CreateFileMode::CreateNew,
                    parent_inode_id,
                    name: "second".to_string(),
                    attrs: FileAttrs::new(),
                    layout: FileLayout::new(4096, 4096, 1),
                },
            ))
            .unwrap();

        let first_inode_id = match first {
            AppDataResponse::Fs(FsCommandResult::Ok(ok)) => ok.inode_id.unwrap(),
            other => panic!("unexpected response: {:?}", other),
        };
        let second_inode_id = match second {
            AppDataResponse::Fs(FsCommandResult::Ok(ok)) => ok.inode_id.unwrap(),
            other => panic!("unexpected response: {:?}", other),
        };
        assert_ne!(first_inode_id, second_inode_id);
    }

    #[test]
    fn create_continues_inode_allocator_after_reopen() {
        let dir = TempDir::new().unwrap();
        let db_path = dir.path().join("state_machine_inode_allocator");
        let parent_inode_id = InodeId::new(100);
        let first_inode_id = {
            let storage = Arc::new(RocksDBStorage::create_for_format(&db_path).unwrap());
            storage
                .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1)))
                .unwrap();
            let sm = AppRaftStateMachine::new(Arc::clone(&storage));
            let response = sm
                .apply(Command::new(
                    dedup_for_test(32),
                    crate::raft::proposal_timestamp_ms(),
                    crate::raft::Mutation::CreateFile {
                        mode: crate::raft::CreateFileMode::CreateNew,
                        parent_inode_id,
                        name: "before-reopen".to_string(),
                        attrs: FileAttrs::new(),
                        layout: FileLayout::new(4096, 4096, 1),
                    },
                ))
                .unwrap();
            match response {
                AppDataResponse::Fs(FsCommandResult::Ok(ok)) => ok.inode_id.unwrap(),
                other => panic!("unexpected response: {:?}", other),
            }
        };

        let storage = Arc::new(RocksDBStorage::create_for_format(&db_path).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));
        let response = sm
            .apply(Command::new(
                dedup_for_test(33),
                crate::raft::proposal_timestamp_ms(),
                crate::raft::Mutation::CreateFile {
                    mode: crate::raft::CreateFileMode::CreateNew,
                    parent_inode_id,
                    name: "after-reopen".to_string(),
                    attrs: FileAttrs::new(),
                    layout: FileLayout::new(4096, 4096, 1),
                },
            ))
            .unwrap();
        let second_inode_id = match response {
            AppDataResponse::Fs(FsCommandResult::Ok(ok)) => ok.inode_id.unwrap(),
            other => panic!("unexpected response: {:?}", other),
        };

        assert_ne!(first_inode_id, second_inode_id);
        assert!(second_inode_id.as_raw() > first_inode_id.as_raw());
    }

    #[test]
    fn unlink_empty_file_deletes_namespace_data_owner_and_replays_without_mutating_again() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let parent_inode_id = InodeId::new(10);
        let mut parent = Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1));
        let inode_id = InodeId::new(11);
        let data_handle_id = DataHandleId::new(12);
        let inode = Inode::new_file(inode_id, FileAttrs::new(), parent.mount_id, data_handle_id);
        storage.put_inode(&parent).unwrap();
        parent.attrs.update_mtime_ctime(1);
        storage
            .create_file_atomic(parent_inode_id, "file", &inode, &parent, FileLayout::new(4096, 4096, 1))
            .unwrap();

        let dedup = dedup_for_test(80);
        let command = Command::new(
            dedup.clone(),
            crate::raft::proposal_timestamp_ms(),
            crate::raft::Mutation::Delete {
                parent_inode_id,
                name: "file".to_string(),
                recursive: false,
            },
        );

        expect_fs_ok(sm.apply(command.clone()).unwrap());
        assert_eq!(storage.get_dentry(parent_inode_id, "file").unwrap(), None);
        assert!(storage.get_inode(inode_id).unwrap().is_none());
        assert_eq!(storage.get_inode_by_data_handle(data_handle_id).unwrap(), None);
        assert!(storage.get_applied_result(&dedup).unwrap().is_some());

        expect_fs_ok(sm.apply(command).unwrap());
        assert_eq!(storage.get_dentry(parent_inode_id, "file").unwrap(), None);
    }

    #[test]
    fn unlink_file_with_extents_deletes_namespace_layout_and_owner_once() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let parent_inode_id = InodeId::new(20);
        let inode_id = InodeId::new(21);
        let data_handle_id = DataHandleId::new(22);
        let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        install_file_with_extents(
            &storage,
            parent_inode_id,
            "file",
            inode_id,
            data_handle_id,
            vec![extent(block_id, 0, 128)],
            128,
        );

        let command = Command::new(
            dedup_for_test(81),
            crate::raft::proposal_timestamp_ms(),
            crate::raft::Mutation::Delete {
                parent_inode_id,
                name: "file".to_string(),
                recursive: false,
            },
        );

        expect_fs_ok(sm.apply(command.clone()).unwrap());
        assert_eq!(storage.get_dentry(parent_inode_id, "file").unwrap(), None);
        assert!(storage.get_inode(inode_id).unwrap().is_none());
        assert!(storage.get_layout(inode_id).is_err());
        assert_eq!(storage.get_inode_by_data_handle(data_handle_id).unwrap(), None);

        expect_fs_ok(sm.apply(command).unwrap());
    }

    #[test]
    fn delete_empty_dir_deletes_namespace_and_replays_without_mutating_again() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let parent_inode_id = InodeId::new(30);
        let parent = Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1));
        let inode_id = InodeId::new(31);
        let inode = Inode::new_dir(inode_id, FileAttrs::new(), parent.mount_id);
        storage.put_inode(&parent).unwrap();
        storage.put_inode(&inode).unwrap();
        storage.put_dentry(parent_inode_id, "dir", inode_id).unwrap();

        let dedup = dedup_for_test(82);
        let command = Command::new(
            dedup.clone(),
            crate::raft::proposal_timestamp_ms(),
            crate::raft::Mutation::Delete {
                parent_inode_id,
                name: "dir".to_string(),
                recursive: false,
            },
        );

        expect_fs_ok(sm.apply(command.clone()).unwrap());
        assert_eq!(storage.get_dentry(parent_inode_id, "dir").unwrap(), None);
        assert!(storage.get_inode(inode_id).unwrap().is_none());
        assert!(storage.get_applied_result(&dedup).unwrap().is_some());

        expect_fs_ok(sm.apply(command).unwrap());
        assert_eq!(storage.get_dentry(parent_inode_id, "dir").unwrap(), None);
    }

    #[test]
    fn delete_empty_dir_non_empty_dir_returns_directory_not_empty_and_preserves_namespace() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let parent_inode_id = InodeId::new(40);
        let dir_inode_id = InodeId::new(41);
        let child_inode_id = InodeId::new(42);
        let parent = Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1));
        let dir_inode = Inode::new_dir(dir_inode_id, FileAttrs::new(), parent.mount_id);
        let child_inode = Inode::new_file(child_inode_id, FileAttrs::new(), parent.mount_id, DataHandleId::new(42));
        storage.put_inode(&parent).unwrap();
        storage.put_inode(&dir_inode).unwrap();
        storage.put_inode(&child_inode).unwrap();
        storage.put_dentry(parent_inode_id, "dir", dir_inode_id).unwrap();
        storage.put_dentry(dir_inode_id, "child", child_inode_id).unwrap();

        let dedup = dedup_for_test(83);
        let command = Command::new(
            dedup.clone(),
            crate::raft::proposal_timestamp_ms(),
            crate::raft::Mutation::Delete {
                parent_inode_id,
                name: "dir".to_string(),
                recursive: false,
            },
        );

        expect_fs_errno(sm.apply(command.clone()).unwrap(), FsErrorCode::ENotEmpty);
        assert_eq!(storage.get_dentry(parent_inode_id, "dir").unwrap(), Some(dir_inode_id));
        assert!(storage.get_inode(dir_inode_id).unwrap().is_some());
        assert!(storage.get_applied_result(&dedup).unwrap().is_some());

        expect_fs_errno(sm.apply(command).unwrap(), FsErrorCode::ENotEmpty);
        assert_eq!(storage.get_dentry(parent_inode_id, "dir").unwrap(), Some(dir_inode_id));
    }

    #[test]
    fn delete_tree_nested_extent_files_deletes_namespace_layouts_and_owners_once() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let parent_inode_id = InodeId::new(70);
        let dir_inode_id = InodeId::new(71);
        let subdir_inode_id = InodeId::new(72);
        let first_inode_id = InodeId::new(73);
        let second_inode_id = InodeId::new(74);
        let first_handle = DataHandleId::new(73);
        let second_handle = DataHandleId::new(74);
        let first_block = BlockId::new(first_handle, BlockIndex::new(0));
        let second_block = BlockId::new(second_handle, BlockIndex::new(0));
        let parent = Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1));
        let dir_inode = Inode::new_dir(dir_inode_id, FileAttrs::new(), parent.mount_id);
        let subdir_inode = Inode::new_dir(subdir_inode_id, FileAttrs::new(), parent.mount_id);
        storage.put_inode(&parent).unwrap();
        storage.put_inode(&dir_inode).unwrap();
        storage.put_inode(&subdir_inode).unwrap();
        storage.put_dentry(parent_inode_id, "dir", dir_inode_id).unwrap();
        storage.put_dentry(dir_inode_id, "sub", subdir_inode_id).unwrap();
        install_file_with_extents(
            &storage,
            dir_inode_id,
            "first",
            first_inode_id,
            first_handle,
            vec![extent(first_block, 0, 64)],
            64,
        );
        install_file_with_extents(
            &storage,
            subdir_inode_id,
            "second",
            second_inode_id,
            second_handle,
            vec![extent(second_block, 0, 128)],
            128,
        );

        let command = Command::new(
            dedup_for_test(99),
            crate::raft::proposal_timestamp_ms(),
            crate::raft::Mutation::Delete {
                parent_inode_id,
                name: "dir".to_string(),
                recursive: true,
            },
        );

        expect_fs_ok(sm.apply(command.clone()).unwrap());
        assert_eq!(storage.get_dentry(parent_inode_id, "dir").unwrap(), None);
        for inode_id in [dir_inode_id, subdir_inode_id, first_inode_id, second_inode_id] {
            assert!(storage.get_inode(inode_id).unwrap().is_none());
        }
        assert!(storage.get_layout(first_inode_id).is_err());
        assert!(storage.get_layout(second_inode_id).is_err());
        assert_eq!(storage.get_inode_by_data_handle(first_handle).unwrap(), None);
        assert_eq!(storage.get_inode_by_data_handle(second_handle).unwrap(), None);

        expect_fs_ok(sm.apply(command).unwrap());
    }

    #[test]
    fn delete_tree_missing_layout_returns_error_without_half_delete_and_replay_is_stable() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let parent_inode_id = InodeId::new(81);
        let dir_inode_id = InodeId::new(82);
        let file_inode_id = InodeId::new(83);
        let data_handle_id = DataHandleId::new(83);
        let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        let parent = Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1));
        let dir_inode = Inode::new_dir(dir_inode_id, FileAttrs::new(), parent.mount_id);
        let mut file_inode = Inode::new_file(file_inode_id, FileAttrs::new(), parent.mount_id, data_handle_id);
        file_inode.attrs.size = 64;
        if let InodeData::File {
            extents,
            file_version,
            lease_epoch,
        } = &mut file_inode.data
        {
            *extents = vec![extent(block_id, 0, 64)];
            *file_version = Some(1);
            *lease_epoch = Some(1);
        }
        storage.put_inode(&parent).unwrap();
        storage.put_inode(&dir_inode).unwrap();
        storage.put_inode(&file_inode).unwrap();
        storage.put_dentry(parent_inode_id, "dir", dir_inode_id).unwrap();
        storage.put_dentry(dir_inode_id, "file", file_inode_id).unwrap();
        storage.put_data_handle_owner(data_handle_id, file_inode_id).unwrap();

        let dedup = dedup_for_test(102);
        let command = Command::new(
            dedup.clone(),
            crate::raft::proposal_timestamp_ms(),
            crate::raft::Mutation::Delete {
                parent_inode_id,
                name: "dir".to_string(),
                recursive: true,
            },
        );

        expect_fs_errno(sm.apply(command.clone()).unwrap(), FsErrorCode::EInval);
        assert_eq!(storage.get_dentry(parent_inode_id, "dir").unwrap(), Some(dir_inode_id));
        assert_eq!(storage.get_dentry(dir_inode_id, "file").unwrap(), Some(file_inode_id));
        assert!(storage.get_inode(dir_inode_id).unwrap().is_some());
        assert!(storage.get_inode(file_inode_id).unwrap().is_some());
        assert_eq!(
            storage.get_inode_by_data_handle(data_handle_id).unwrap(),
            Some(file_inode_id)
        );
        assert!(storage.get_applied_result(&dedup).unwrap().is_some());

        expect_fs_errno(sm.apply(command).unwrap(), FsErrorCode::EInval);
        assert_eq!(storage.get_dentry(parent_inode_id, "dir").unwrap(), Some(dir_inode_id));
        assert_eq!(storage.get_dentry(dir_inode_id, "file").unwrap(), Some(file_inode_id));
    }

    #[test]
    fn delete_tree_fingerprint_mismatch_preserves_second_tree() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let parent_inode_id = InodeId::new(78);
        let first_dir = InodeId::new(79);
        let second_dir = InodeId::new(80);
        let parent = Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1));
        storage.put_inode(&parent).unwrap();
        storage
            .put_inode(&Inode::new_dir(first_dir, FileAttrs::new(), parent.mount_id))
            .unwrap();
        storage
            .put_inode(&Inode::new_dir(second_dir, FileAttrs::new(), parent.mount_id))
            .unwrap();
        storage.put_dentry(parent_inode_id, "first", first_dir).unwrap();
        storage.put_dentry(parent_inode_id, "second", second_dir).unwrap();
        let dedup = dedup_for_test(101);

        expect_fs_ok(
            sm.apply(Command::new(
                dedup.clone(),
                crate::raft::proposal_timestamp_ms(),
                crate::raft::Mutation::Delete {
                    parent_inode_id,
                    name: "first".to_string(),
                    recursive: true,
                },
            ))
            .unwrap(),
        );
        let mismatch = sm.apply(Command::new(
            dedup,
            crate::raft::proposal_timestamp_ms(),
            crate::raft::Mutation::Delete {
                parent_inode_id,
                name: "second".to_string(),
                recursive: true,
            },
        ));

        assert!(matches!(mismatch, Err(MetadataError::InvalidArgument(_))));
        assert_eq!(storage.get_dentry(parent_inode_id, "first").unwrap(), None);
        assert_eq!(storage.get_dentry(parent_inode_id, "second").unwrap(), Some(second_dir));
        assert!(storage.get_inode(second_dir).unwrap().is_some());
    }
}
