// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use super::*;

impl AppRaftStateMachine {
    pub(super) fn apply_bootstrap_namespace(
        &self,
        group_name: GroupName,
        proposed_at_ms: u64,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<crate::mount::MountEntry> {
        let state = self.storage.bootstrap_namespace_state(&group_name)?;
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
        if state == BootstrapNamespaceState::Matching {
            self.storage.commit_applied_state(raft_state)?;
            return Ok(root_mount);
        }

        let mut attrs = FileAttrs::new();
        attrs.update_timestamps(proposed_at_ms);
        attrs.nlink = 1;
        let root_inode = Inode::new_dir(crate::mount::ROOT_INODE_ID, attrs, MountId::new(1));
        self.storage
            .bootstrap_namespace_atomic(&root_inode, &root_mount, raft_state)?;
        Ok(root_mount)
    }

    /// Apply Mkdir command.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_mkdir(
        &self,
        parent_inode_id: InodeId,
        name: String,
        mut attrs: FileAttrs,
        proposed_at_ms: u64,
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
                content_revision: None,
                attrs: Some(inode.attrs.clone()),
                layout: None,
                lease_epoch: None,
            })
            .map(|ok| (allocation, inode, updated_parent, ok))
        })();

        let (allocation, inode, updated_parent, ok) = match prepared {
            Ok(prepared) => prepared,
            Err(err) => return self.persist_fs_error(err, raft_state),
        };
        let result = FsCommandResult::Ok(ok);
        self.storage
            .create_dir_atomic(allocation, parent_inode_id, &name, &inode, &updated_parent, raft_state)?;
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
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<FsCommandResult> {
        if components.is_empty() || components.iter().any(|component| component.is_empty()) {
            return self.persist_fs_error(
                MetadataError::InvalidArgument("CreateDirectory requires non-empty path components".to_string()),
                raft_state,
            );
        }
        let mut parent = match self.storage.get_inode(root_inode_id)? {
            Some(inode) if inode.kind.is_dir() => inode,
            Some(_) => {
                return self.persist_fs_error(
                    MetadataError::NotDir(format!("Root is not a directory: {root_inode_id}")),
                    raft_state,
                );
            }
            None => {
                return self.persist_fs_error(
                    MetadataError::NotFound(format!("Root inode not found: {root_inode_id}")),
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
                            raft_state,
                        );
                    }
                    None => {
                        return self.persist_fs_error(
                            MetadataError::NotFound(format!("Target inode not found: {child_inode_id}")),
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
            content_revision: None,
            attrs: Some(parent.attrs.clone()),
            layout: None,
            lease_epoch: None,
        });
        if entries.is_empty() {
            self.storage.commit_applied_state(raft_state)?;
        } else {
            allocation.next_inode_id = InodeId::new(next_raw);
            self.storage
                .create_directories_atomic(allocation, &entries, raft_state)?;
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
        proposed_at_ms: u64,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<FsCommandResult> {
        if self.storage.get_dentry(parent_inode_id, &name)?.is_some() {
            return self.persist_fs_error(
                MetadataError::AlreadyExists(format!("File already exists: {name}")),
                raft_state,
            );
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
                content_revision: None,
                attrs: Some(inode.attrs.clone()),
                layout: Some(layout),
                lease_epoch: None,
            })
            .map(|ok| (allocation, inode, updated_parent, ok))
        })();

        let (allocation, inode, updated_parent, ok) = match prepared {
            Ok(prepared) => prepared,
            Err(err) => return self.persist_fs_error(err, raft_state),
        };
        let result = FsCommandResult::Ok(ok);
        self.storage.create_file_atomic(
            allocation,
            parent_inode_id,
            &name,
            &inode,
            &updated_parent,
            layout,
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
        expected_inode_id: InodeId,
        expected_file_lease_epochs: Vec<(InodeId, u64)>,
        recursive: bool,
        proposed_at_ms: u64,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<FsCommandResult> {
        let child_inode_id = match self.storage.get_dentry(parent_inode_id, &name)? {
            Some(inode_id) => inode_id,
            None => {
                return self.persist_fs_error(MetadataError::NotFound(format!("Entry not found: {name}")), raft_state);
            }
        };
        if child_inode_id != expected_inode_id {
            return self.persist_fs_error(
                MetadataError::Again(format!(
                    "delete target changed for {name}: expected {expected_inode_id}, current {child_inode_id}"
                )),
                raft_state,
            );
        }
        let child_inode = match self.storage.get_inode(child_inode_id)? {
            Some(inode) => inode,
            None => {
                return self.persist_fs_error(
                    MetadataError::NotFound(format!("Child inode not found: {child_inode_id}")),
                    raft_state,
                );
            }
        };

        if child_inode.kind.is_dir() {
            if recursive {
                self.apply_delete_tree(
                    parent_inode_id,
                    name,
                    expected_file_lease_epochs,
                    proposed_at_ms,
                    raft_state,
                )
            } else {
                if !expected_file_lease_epochs.is_empty() {
                    return self.persist_fs_error(
                        MetadataError::Again("delete target lease preconditions changed".to_string()),
                        raft_state,
                    );
                }
                self.apply_delete_empty_dir(parent_inode_id, name, proposed_at_ms, raft_state)
            }
        } else {
            let current_file_lease_epochs = match &child_inode.data {
                InodeData::File { lease_epoch, .. } => {
                    vec![(child_inode_id, lease_epoch.unwrap_or(0))]
                }
                _ => Vec::new(),
            };
            if current_file_lease_epochs != expected_file_lease_epochs {
                return self.persist_fs_error(
                    MetadataError::Again(format!(
                        "delete target lease preconditions changed: expected {expected_file_lease_epochs:?}, current {current_file_lease_epochs:?}"
                    )),
                    raft_state,
                );
            }
            self.apply_unlink(parent_inode_id, name, proposed_at_ms, raft_state)
        }
    }

    /// Apply Unlink command.
    pub(super) fn apply_unlink(
        &self,
        parent_inode_id: InodeId,
        name: String,
        proposed_at_ms: u64,
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
                    let data_handle_id = child_inode.data_handle_id;
                    if data_handle_id.as_raw() == 0 {
                        return Err(MetadataError::Internal(format!(
                            "File inode {} is missing data_handle_id",
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
            Err(err) => return self.persist_fs_error(err, raft_state),
        };
        let result = FsCommandResult::Ok(ok);
        self.storage.delete_file_atomic(
            parent_inode_id,
            &name,
            child_inode_id,
            data_handle_id,
            &updated_parent,
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
            Err(err) => return self.persist_fs_error(err, raft_state),
        };
        let result = FsCommandResult::Ok(ok);
        self.storage
            .delete_empty_dir_atomic(parent_inode_id, &name, child_inode_id, &updated_parent, raft_state)?;
        Ok(result)
    }

    pub(super) fn apply_delete_tree(
        &self,
        parent_inode_id: InodeId,
        name: String,
        expected_file_lease_epochs: Vec<(InodeId, u64)>,
        proposed_at_ms: u64,
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
                file_lease_epochs: Vec::new(),
            };
            self.prepare_delete_tree_node(parent_inode_id, name, root_inode_id, root_inode, &mut plan)?;

            Ok(PreparedDeleteTree {
                updated_parent,
                entries: plan.entries,
                file_lease_epochs: plan.file_lease_epochs,
            })
        })();

        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(err) => return self.persist_fs_error(err, raft_state),
        };
        let mut current_file_lease_epochs = prepared.file_lease_epochs.clone();
        current_file_lease_epochs.sort_by_key(|(inode_id, _)| inode_id.as_raw());
        if current_file_lease_epochs != expected_file_lease_epochs {
            return self.persist_fs_error(
                MetadataError::Again(format!(
                    "recursive delete file lease preconditions changed: expected {expected_file_lease_epochs:?}, current {current_file_lease_epochs:?}"
                )),
                raft_state,
            );
        }
        let result = FsCommandResult::Ok(FsOkResult::default());
        self.storage.delete_tree_atomic(
            DeleteTreeAtomicUpdate {
                entries: &prepared.entries,
                updated_parent: &prepared.updated_parent,
            },
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
            InodeData::File { lease_epoch, .. } => {
                plan.file_lease_epochs.push((inode_id, lease_epoch.unwrap_or(0)));
                let data_handle_id = inode.data_handle_id;
                if data_handle_id.as_raw() == 0 {
                    return Err(MetadataError::Internal(format!(
                        "File inode {} is missing data_handle_id",
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
    // Keep the state transition inputs explicit at the apply boundary.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_rename(
        &self,
        src_parent_inode_id: InodeId,
        src_name: String,
        expected_src_inode_id: InodeId,
        dst_parent_inode_id: InodeId,
        dst_name: String,
        expected_dst_inode_id: Option<InodeId>,
        expected_dst_lease_epoch: Option<u64>,
        flags: u32,
        proposed_at_ms: u64,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<FsCommandResult> {
        let prepared: MetadataResult<PreparedRename> = (|| {
            // Get source dentry
            let src_inode_id = self
                .storage
                .get_dentry(src_parent_inode_id, &src_name)?
                .ok_or_else(|| MetadataError::NotFound(format!("Source not found: {}", src_name)))?;
            if src_inode_id != expected_src_inode_id {
                return Err(MetadataError::Again(format!(
                    "rename source changed for {src_name}: expected {expected_src_inode_id}, current {src_inode_id}"
                )));
            }

            let current_dst_inode_id = self.storage.get_dentry(dst_parent_inode_id, &dst_name)?;
            if current_dst_inode_id != expected_dst_inode_id {
                return Err(MetadataError::Again(format!(
                    "rename destination changed for {dst_name}: expected {expected_dst_inode_id:?}, current {current_dst_inode_id:?}"
                )));
            }

            // Get source inode
            let src_inode = self
                .storage
                .get_inode(src_inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Source inode not found: {}", src_inode_id)))?;

            let mut overwritten_target = None;

            // Check if destination exists
            if let Some(dst_inode_id) = current_dst_inode_id {
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
                let current_dst_lease_epoch = match &dst_inode.data {
                    InodeData::File { lease_epoch, .. } => Some(lease_epoch.unwrap_or(0)),
                    _ => None,
                };
                if current_dst_lease_epoch != expected_dst_lease_epoch {
                    return Err(MetadataError::Again(format!(
                        "rename destination lease epoch changed for {dst_name}: expected {expected_dst_lease_epoch:?}, current {current_dst_lease_epoch:?}"
                    )));
                }

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
            Err(err) => return self.persist_fs_error(err, raft_state),
        };
        let result = FsCommandResult::Ok(FsOkResult::default());
        self.storage.rename_atomic(
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
                let data_handle_id = dst_inode.data_handle_id;
                if data_handle_id.as_raw() == 0 {
                    return Err(MetadataError::Internal(format!(
                        "File inode {} is missing data_handle_id",
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

            let content_revision = if size_changes_visible_file_state {
                match &mut inode.data {
                    InodeData::File {
                        extents,
                        content_revision,
                        ..
                    } => {
                        let next = Self::next_content_revision(inode_id, *content_revision)?;
                        for extent in extents.iter_mut() {
                            extent.content_revision = Some(next);
                        }
                        *content_revision = Some(next);
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
                    content_revision,
                    ..FsOkResult::default()
                },
            ))
        })();

        let (inode, ok) = match prepared {
            Ok(prepared) => prepared,
            Err(err) => return self.persist_fs_error(err, raft_state),
        };
        let result = FsCommandResult::Ok(ok);
        self.storage.put_inode_atomic(&inode, raft_state)?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::state_machine::test_support::*;

    fn test_state() -> (TempDir, Arc<RocksDBStorage>, AppRaftStateMachine, InodeId) {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let parent_inode_id = InodeId::new(10);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1)))
            .unwrap();
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));
        (dir, storage, sm, parent_inode_id)
    }

    fn create_file(sm: &AppRaftStateMachine, parent_inode_id: InodeId, name: &str) -> FsOkResult {
        expect_fs_ok(
            sm.apply(Command::CreateFile {
                proposed_at_ms: 1,
                parent_inode_id,
                name: name.to_string(),
                attrs: FileAttrs::new(),
                layout: FileLayout::new(4096, 4096, 1),
            })
            .unwrap(),
        )
    }

    #[test]
    fn create_file_persists_namespace_layout_and_data_handle_owner() {
        let (_dir, storage, sm, parent_inode_id) = test_state();

        let created = create_file(&sm, parent_inode_id, "file");
        let inode_id = created.inode_id.unwrap();
        let data_handle_id = created.data_handle_id.unwrap();

        assert_eq!(storage.get_dentry(parent_inode_id, "file").unwrap(), Some(inode_id));
        assert_eq!(
            storage.get_inode_by_data_handle(data_handle_id).unwrap(),
            Some(inode_id)
        );
        assert_eq!(storage.get_layout(inode_id).unwrap(), FileLayout::new(4096, 4096, 1));

        expect_fs_errno(
            sm.apply(Command::CreateFile {
                proposed_at_ms: 2,
                parent_inode_id,
                name: "file".to_string(),
                attrs: FileAttrs::new(),
                layout: FileLayout::new(8192, 8192, 1),
            })
            .unwrap(),
            FsErrorCode::EExist,
        );
    }

    #[test]
    fn recursive_create_directory_is_a_convergent_ensure_operation() {
        let (_dir, storage, sm, parent_inode_id) = test_state();
        let command = Command::CreateDirectory {
            proposed_at_ms: 1,
            root_inode_id: parent_inode_id,
            components: vec!["a".to_string(), "b".to_string()],
            attrs: FileAttrs::new(),
            recursive: true,
        };

        let first = expect_fs_ok(sm.apply(command.clone()).unwrap()).inode_id.unwrap();
        let second = expect_fs_ok(sm.apply(command).unwrap()).inode_id.unwrap();

        assert_eq!(first, second);
        let a = storage.get_dentry(parent_inode_id, "a").unwrap().unwrap();
        assert_eq!(storage.get_dentry(a, "b").unwrap(), Some(first));
    }

    #[test]
    fn delete_rejects_a_same_name_replacement_before_mutating_it() {
        let (_dir, storage, sm, parent_inode_id) = test_state();
        let original = create_file(&sm, parent_inode_id, "target").inode_id.unwrap();
        let replacement = create_file(&sm, parent_inode_id, "replacement").inode_id.unwrap();
        storage.put_dentry(parent_inode_id, "target", replacement).unwrap();

        expect_fs_errno(
            sm.apply(Command::Delete {
                proposed_at_ms: 2,
                parent_inode_id,
                name: "target".to_string(),
                expected_inode_id: original,
                expected_file_lease_epochs: vec![(original, 0)],
                recursive: false,
            })
            .unwrap(),
            FsErrorCode::EAgain,
        );

        assert_eq!(
            storage.get_dentry(parent_inode_id, "target").unwrap(),
            Some(replacement)
        );
        assert!(storage.get_inode(replacement).unwrap().is_some());
    }

    #[test]
    fn rename_rejects_destination_changes_before_mutating_namespace() {
        let (_dir, storage, sm, parent_inode_id) = test_state();
        let source = create_file(&sm, parent_inode_id, "source").inode_id.unwrap();
        let replacement = create_file(&sm, parent_inode_id, "destination").inode_id.unwrap();

        expect_fs_errno(
            sm.apply(Command::Rename {
                proposed_at_ms: 2,
                src_parent_inode_id: parent_inode_id,
                src_name: "source".to_string(),
                expected_src_inode_id: source,
                dst_parent_inode_id: parent_inode_id,
                dst_name: "destination".to_string(),
                expected_dst_inode_id: None,
                expected_dst_lease_epoch: None,
                flags: 0,
            })
            .unwrap(),
            FsErrorCode::EAgain,
        );

        assert_eq!(storage.get_dentry(parent_inode_id, "source").unwrap(), Some(source));
        assert_eq!(
            storage.get_dentry(parent_inode_id, "destination").unwrap(),
            Some(replacement)
        );
    }

    #[test]
    fn rename_noreplace_is_decided_atomically_in_apply() {
        let (_dir, storage, sm, parent_inode_id) = test_state();
        let source = create_file(&sm, parent_inode_id, "source").inode_id.unwrap();
        let destination = create_file(&sm, parent_inode_id, "destination").inode_id.unwrap();

        expect_fs_errno(
            sm.apply(Command::Rename {
                proposed_at_ms: 2,
                src_parent_inode_id: parent_inode_id,
                src_name: "source".to_string(),
                expected_src_inode_id: source,
                dst_parent_inode_id: parent_inode_id,
                dst_name: "destination".to_string(),
                expected_dst_inode_id: Some(destination),
                expected_dst_lease_epoch: Some(0),
                flags: 0x1,
            })
            .unwrap(),
            FsErrorCode::EExist,
        );
        assert_eq!(storage.get_dentry(parent_inode_id, "source").unwrap(), Some(source));
    }

    #[test]
    fn rename_overwrite_removes_replaced_file_authority() {
        let (_dir, storage, sm, parent_inode_id) = test_state();
        let source = create_file(&sm, parent_inode_id, "source");
        let destination = create_file(&sm, parent_inode_id, "destination");

        expect_fs_ok(
            sm.apply(Command::Rename {
                proposed_at_ms: 2,
                src_parent_inode_id: parent_inode_id,
                src_name: "source".to_string(),
                expected_src_inode_id: source.inode_id.unwrap(),
                dst_parent_inode_id: parent_inode_id,
                dst_name: "destination".to_string(),
                expected_dst_inode_id: destination.inode_id,
                expected_dst_lease_epoch: Some(0),
                flags: 0,
            })
            .unwrap(),
        );

        assert_eq!(storage.get_dentry(parent_inode_id, "source").unwrap(), None);
        assert_eq!(
            storage.get_dentry(parent_inode_id, "destination").unwrap(),
            source.inode_id
        );
        assert_eq!(storage.get_inode(destination.inode_id.unwrap()).unwrap(), None);
        assert_eq!(
            storage
                .get_inode_by_data_handle(destination.data_handle_id.unwrap())
                .unwrap(),
            None
        );
    }

    #[test]
    fn recursive_delete_removes_nested_file_authority_atomically() {
        let (_dir, storage, sm, parent_inode_id) = test_state();
        let directory = expect_fs_ok(
            sm.apply(Command::CreateDirectory {
                proposed_at_ms: 1,
                root_inode_id: parent_inode_id,
                components: vec!["dir".to_string()],
                attrs: FileAttrs::new(),
                recursive: false,
            })
            .unwrap(),
        )
        .inode_id
        .unwrap();
        let file = create_file(&sm, directory, "file");

        expect_fs_ok(
            sm.apply(Command::Delete {
                proposed_at_ms: 2,
                parent_inode_id,
                name: "dir".to_string(),
                expected_inode_id: directory,
                expected_file_lease_epochs: vec![(file.inode_id.unwrap(), 0)],
                recursive: true,
            })
            .unwrap(),
        );

        assert_eq!(storage.get_dentry(parent_inode_id, "dir").unwrap(), None);
        assert_eq!(storage.get_inode(directory).unwrap(), None);
        assert_eq!(storage.get_inode(file.inode_id.unwrap()).unwrap(), None);
        assert_eq!(
            storage.get_inode_by_data_handle(file.data_handle_id.unwrap()).unwrap(),
            None
        );
    }

    #[test]
    fn delete_rejects_a_lease_acquired_after_preflight() {
        let (_dir, storage, sm, parent_inode_id) = test_state();
        let file = create_file(&sm, parent_inode_id, "target");
        let inode_id = file.inode_id.unwrap();

        expect_fs_ok(
            sm.apply(Command::AcquireWriteLease {
                proposed_at_ms: 2,
                inode_id,
                expected_lease_epoch: 0,
            })
            .unwrap(),
        );
        expect_fs_errno(
            sm.apply(Command::Delete {
                proposed_at_ms: 3,
                parent_inode_id,
                name: "target".to_string(),
                expected_inode_id: inode_id,
                expected_file_lease_epochs: vec![(inode_id, 0)],
                recursive: false,
            })
            .unwrap(),
            FsErrorCode::EAgain,
        );

        assert_eq!(storage.get_dentry(parent_inode_id, "target").unwrap(), Some(inode_id));
    }

    #[test]
    fn delete_that_linearizes_first_prevents_later_lease_acquisition() {
        let (_dir, storage, sm, parent_inode_id) = test_state();
        let file = create_file(&sm, parent_inode_id, "target");
        let inode_id = file.inode_id.unwrap();

        expect_fs_ok(
            sm.apply(Command::Delete {
                proposed_at_ms: 2,
                parent_inode_id,
                name: "target".to_string(),
                expected_inode_id: inode_id,
                expected_file_lease_epochs: vec![(inode_id, 0)],
                recursive: false,
            })
            .unwrap(),
        );
        expect_fs_errno(
            sm.apply(Command::AcquireWriteLease {
                proposed_at_ms: 3,
                inode_id,
                expected_lease_epoch: 0,
            })
            .unwrap(),
            FsErrorCode::ENoEnt,
        );

        assert_eq!(storage.get_dentry(parent_inode_id, "target").unwrap(), None);
        assert_eq!(storage.get_inode(inode_id).unwrap(), None);
    }

    #[test]
    fn recursive_delete_rejects_a_descendant_lease_acquired_after_preflight() {
        let (_dir, storage, sm, parent_inode_id) = test_state();
        let directory = expect_fs_ok(
            sm.apply(Command::CreateDirectory {
                proposed_at_ms: 1,
                root_inode_id: parent_inode_id,
                components: vec!["dir".to_string()],
                attrs: FileAttrs::new(),
                recursive: false,
            })
            .unwrap(),
        )
        .inode_id
        .unwrap();
        let file_id = create_file(&sm, directory, "file").inode_id.unwrap();

        expect_fs_ok(
            sm.apply(Command::AcquireWriteLease {
                proposed_at_ms: 2,
                inode_id: file_id,
                expected_lease_epoch: 0,
            })
            .unwrap(),
        );
        expect_fs_errno(
            sm.apply(Command::Delete {
                proposed_at_ms: 3,
                parent_inode_id,
                name: "dir".to_string(),
                expected_inode_id: directory,
                expected_file_lease_epochs: vec![(file_id, 0)],
                recursive: true,
            })
            .unwrap(),
            FsErrorCode::EAgain,
        );

        assert_eq!(storage.get_dentry(parent_inode_id, "dir").unwrap(), Some(directory));
        assert_eq!(storage.get_dentry(directory, "file").unwrap(), Some(file_id));
    }

    #[test]
    fn overwrite_rename_rejects_a_destination_lease_acquired_after_preflight() {
        let (_dir, storage, sm, parent_inode_id) = test_state();
        let source = create_file(&sm, parent_inode_id, "source").inode_id.unwrap();
        let destination = create_file(&sm, parent_inode_id, "destination").inode_id.unwrap();

        expect_fs_ok(
            sm.apply(Command::AcquireWriteLease {
                proposed_at_ms: 2,
                inode_id: destination,
                expected_lease_epoch: 0,
            })
            .unwrap(),
        );
        expect_fs_errno(
            sm.apply(Command::Rename {
                proposed_at_ms: 3,
                src_parent_inode_id: parent_inode_id,
                src_name: "source".to_string(),
                expected_src_inode_id: source,
                dst_parent_inode_id: parent_inode_id,
                dst_name: "destination".to_string(),
                expected_dst_inode_id: Some(destination),
                expected_dst_lease_epoch: Some(0),
                flags: 0,
            })
            .unwrap(),
            FsErrorCode::EAgain,
        );

        assert_eq!(storage.get_dentry(parent_inode_id, "source").unwrap(), Some(source));
        assert_eq!(
            storage.get_dentry(parent_inode_id, "destination").unwrap(),
            Some(destination)
        );
    }

    #[test]
    fn overwrite_rename_that_linearizes_first_prevents_a_lease_on_the_replaced_inode() {
        let (_dir, storage, sm, parent_inode_id) = test_state();
        let source = create_file(&sm, parent_inode_id, "source").inode_id.unwrap();
        let destination = create_file(&sm, parent_inode_id, "destination").inode_id.unwrap();

        expect_fs_ok(
            sm.apply(Command::Rename {
                proposed_at_ms: 2,
                src_parent_inode_id: parent_inode_id,
                src_name: "source".to_string(),
                expected_src_inode_id: source,
                dst_parent_inode_id: parent_inode_id,
                dst_name: "destination".to_string(),
                expected_dst_inode_id: Some(destination),
                expected_dst_lease_epoch: Some(0),
                flags: 0,
            })
            .unwrap(),
        );
        expect_fs_errno(
            sm.apply(Command::AcquireWriteLease {
                proposed_at_ms: 3,
                inode_id: destination,
                expected_lease_epoch: 0,
            })
            .unwrap(),
            FsErrorCode::ENoEnt,
        );

        assert_eq!(
            storage.get_dentry(parent_inode_id, "destination").unwrap(),
            Some(source)
        );
        assert_eq!(storage.get_inode(destination).unwrap(), None);
    }
}
