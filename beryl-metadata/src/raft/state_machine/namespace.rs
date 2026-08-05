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

            // Generate inode ID
            let allocation = self.storage.prepare_inode_allocation()?;
            let inode_id = allocation.inode_id;
            let now_ms = proposed_at_ms;

            // Initialize attrs
            attrs.update_timestamps(now_ms);
            attrs.nlink = 1;

            // Create the file under its single canonical inode identity.
            let inode = Inode::new_file(inode_id, attrs, parent_inode.mount_id);

            // Update parent directory mtime/ctime
            let mut parent_attrs = parent_inode.attrs.clone();
            parent_attrs.update_mtime_ctime(Self::mutation_timestamp(&parent_inode, proposed_at_ms));
            let mut updated_parent = parent_inode.clone();
            updated_parent.attrs = parent_attrs;

            Ok(FsOkResult {
                inode_id: Some(inode_id),
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

    /// Revalidate one bounded mount-relative Delete command and apply its target-specific mutation.
    ///
    /// Path resolution happens inside Raft apply so a stale leader admission
    /// cannot mutate a parent that has since become unreachable. Work is
    /// bounded by the fixed path limits and the number of mount records, never
    /// by the size of a recursive-delete subtree.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_delete(
        &self,
        mount_id: MountId,
        expected_mount_epoch: u64,
        mount_root_inode_id: InodeId,
        relative_components: Vec<String>,
        expected_inode_id: InodeId,
        expected_file_lease_epoch: Option<u64>,
        recursive: bool,
        proposed_at_ms: u64,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<FsCommandResult> {
        let (parent_inode_id, name, child_inode) = match self.resolve_delete_target(
            mount_id,
            expected_mount_epoch,
            mount_root_inode_id,
            &relative_components,
        ) {
            Ok(target) => target,
            Err(error) => return self.persist_fs_error(error, raft_state),
        };
        if child_inode.inode_id != expected_inode_id {
            return self.persist_fs_error(
                MetadataError::Again(format!(
                    "delete target changed for {name}: expected {expected_inode_id}, current {}",
                    child_inode.inode_id
                )),
                raft_state,
            );
        }

        if child_inode.kind.is_dir() {
            if expected_file_lease_epoch.is_some() {
                return self.persist_fs_error(
                    MetadataError::Again("delete target lease precondition changed".to_string()),
                    raft_state,
                );
            }
            if recursive {
                self.apply_detach_directory(parent_inode_id, name, child_inode.inode_id, proposed_at_ms, raft_state)
            } else {
                self.apply_delete_empty_dir(parent_inode_id, name, proposed_at_ms, raft_state)
            }
        } else {
            let current_file_lease_epoch = match &child_inode.data {
                InodeData::File { lease_epoch, .. } => Some(lease_epoch.unwrap_or(0)),
                _ => None,
            };
            if current_file_lease_epoch != expected_file_lease_epoch {
                return self.persist_fs_error(
                    MetadataError::Again(format!(
                        "delete target lease precondition changed: expected {expected_file_lease_epoch:?}, current {current_file_lease_epoch:?}"
                    )),
                    raft_state,
                );
            }
            self.apply_unlink(parent_inode_id, name, proposed_at_ms, raft_state)
        }
    }

    /// Resolve and validate the exact target named by a replicated Delete command.
    fn resolve_delete_target(
        &self,
        mount_id: MountId,
        expected_mount_epoch: u64,
        mount_root_inode_id: InodeId,
        relative_components: &[String],
    ) -> MetadataResult<(InodeId, String, Inode)> {
        Self::validate_delete_components(relative_components)?;
        let mounts = self.storage.list_mounts()?;
        let mount = mounts
            .iter()
            .find(|entry| entry.mount_id == mount_id)
            .ok_or_else(|| MetadataError::NotFound(format!("Mount not found: {mount_id:?}")))?;
        if mount.mount_epoch != expected_mount_epoch || mount.root_inode_id != mount_root_inode_id {
            return Err(MetadataError::Again(format!(
                "delete mount precondition changed for {mount_id:?}"
            )));
        }

        let relative_path_bytes = relative_components
            .iter()
            .try_fold(relative_components.len().saturating_sub(1), |bytes, component| {
                bytes.checked_add(component.len())
            })
            .ok_or_else(|| MetadataError::InvalidArgument("Delete path length overflow".to_string()))?;
        let target_path_bytes = if mount.mount_prefix == crate::mount::ROOT_MOUNT_PREFIX {
            1usize.checked_add(relative_path_bytes)
        } else {
            mount
                .mount_prefix
                .len()
                .checked_add(1)
                .and_then(|bytes| bytes.checked_add(relative_path_bytes))
        }
        .ok_or_else(|| MetadataError::InvalidArgument("Delete path length overflow".to_string()))?;
        if target_path_bytes > crate::path_resolver::MAX_PATH_BYTES {
            return Err(MetadataError::InvalidArgument(format!(
                "Delete path exceeds {} bytes",
                crate::path_resolver::MAX_PATH_BYTES
            )));
        }
        let relative_path = relative_components.join("/");
        let target_path = if mount.mount_prefix == crate::mount::ROOT_MOUNT_PREFIX {
            format!("/{relative_path}")
        } else {
            format!("{}/{relative_path}", mount.mount_prefix)
        };
        if mounts.iter().any(|entry| {
            entry.mount_id != mount_id && crate::mount::mount_prefix_matches_path(&target_path, &entry.mount_prefix)
        }) {
            return Err(MetadataError::CrossMountRename(
                "delete target is a mount root or contains a nested mount".to_string(),
            ));
        }

        let mut parent = self
            .storage
            .get_inode(mount_root_inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Mount root inode not found: {mount_root_inode_id}")))?;
        if parent.inode_id != mount_root_inode_id {
            return Err(MetadataError::Internal(format!(
                "mount root inode key {mount_root_inode_id} contains inode {}",
                parent.inode_id
            )));
        }
        if parent.kind != parent.data.kind() {
            return Err(MetadataError::Internal(format!(
                "mount root inode {mount_root_inode_id} kind and payload disagree"
            )));
        }
        if !parent.kind.is_dir() || !matches!(&parent.data, InodeData::Dir) {
            return Err(MetadataError::NotDir(format!(
                "Mount root is not a directory: {mount_root_inode_id}"
            )));
        }
        if parent.mount_id != mount_id {
            return Err(MetadataError::CrossMountRename(
                "mount root inode belongs to a different mount".to_string(),
            ));
        }

        for (index, component) in relative_components.iter().enumerate() {
            let child_inode_id = self.storage.get_dentry(parent.inode_id, component)?.ok_or_else(|| {
                MetadataError::NotFound(format!(
                    "Entry not found: {component} (parent inode: {})",
                    parent.inode_id
                ))
            })?;
            let child = self
                .storage
                .get_inode(child_inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Child inode not found: {child_inode_id}")))?;
            if child.inode_id != child_inode_id {
                return Err(MetadataError::Internal(format!(
                    "inode key {child_inode_id} contains inode {}",
                    child.inode_id
                )));
            }
            if child.kind != child.data.kind() {
                return Err(MetadataError::Internal(format!(
                    "inode {child_inode_id} kind and payload disagree"
                )));
            }
            if child.mount_id != mount_id {
                return Err(MetadataError::CrossMountRename(
                    "delete path crosses mount authority".to_string(),
                ));
            }
            if index + 1 == relative_components.len() {
                if mounts.iter().any(|entry| entry.root_inode_id == child_inode_id) {
                    return Err(MetadataError::InvalidArgument(format!(
                        "Cannot delete mount root inode {child_inode_id}"
                    )));
                }
                return Ok((parent.inode_id, component.clone(), child));
            }
            if !child.kind.is_dir() || !matches!(&child.data, InodeData::Dir) {
                return Err(MetadataError::NotDir(format!(
                    "Path component is not a directory: {component}"
                )));
            }
            parent = child;
        }

        unreachable!("Delete components are checked as non-empty")
    }

    fn validate_delete_components(relative_components: &[String]) -> MetadataResult<()> {
        if relative_components.is_empty() {
            return Err(MetadataError::InvalidArgument("Cannot delete mount root".to_string()));
        }
        if relative_components.len() > crate::path_resolver::MAX_PATH_COMPONENTS {
            return Err(MetadataError::InvalidArgument(format!(
                "Delete path exceeds {} components",
                crate::path_resolver::MAX_PATH_COMPONENTS
            )));
        }
        for component in relative_components {
            if component.is_empty() || component.contains('/') || component.contains('\0') {
                return Err(MetadataError::InvalidArgument(
                    "Delete path contains an invalid component".to_string(),
                ));
            }
            if component.len() > crate::path_resolver::MAX_PATH_COMPONENT_BYTES {
                return Err(MetadataError::InvalidArgument(format!(
                    "Delete path component exceeds {} bytes",
                    crate::path_resolver::MAX_PATH_COMPONENT_BYTES
                )));
            }
        }
        Ok(())
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

            match &child_inode.data {
                InodeData::File { .. } => {
                    if child_inode.inode_id != child_inode_id
                        || self.storage.get_layout_optional(child_inode_id)?.is_none()
                    {
                        return Err(MetadataError::Internal(format!(
                            "file inode {child_inode_id} has corrupt identity or missing layout: value_id={}",
                            child_inode.inode_id
                        )));
                    }
                }
                InodeData::Symlink { .. } => {
                    if child_inode.inode_id != child_inode_id
                        || self.storage.get_layout_optional(child_inode_id)?.is_some()
                    {
                        return Err(MetadataError::Internal(format!(
                            "symlink inode {child_inode_id} carries invalid file authority: value_id={}",
                            child_inode_id
                        )));
                    }
                }
                InodeData::Dir => return Err(MetadataError::IsDir(format!("Cannot unlink directory: {}", name))),
            }

            Ok(FsOkResult::default()).map(|ok| (child_inode_id, updated_parent, ok))
        })();

        let (child_inode_id, updated_parent, ok) = match prepared {
            Ok(prepared) => prepared,
            Err(err) => return self.persist_fs_error(err, raft_state),
        };
        let result = FsCommandResult::Ok(ok);
        self.storage
            .delete_file_atomic(parent_inode_id, &name, child_inode_id, &updated_parent, raft_state)?;
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

    /// Atomically hide a recursive-delete root and make it reclaimable.
    pub(super) fn apply_detach_directory(
        &self,
        parent_inode_id: InodeId,
        name: String,
        root_inode_id: InodeId,
        proposed_at_ms: u64,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<FsCommandResult> {
        let prepared: MetadataResult<(Inode, DetachedRoot)> = (|| {
            let current_root_inode_id = self
                .storage
                .get_dentry(parent_inode_id, &name)?
                .ok_or_else(|| MetadataError::NotFound(format!("Directory not found: {name}")))?;
            if current_root_inode_id != root_inode_id {
                return Err(MetadataError::Again(format!(
                    "delete target changed for {name}: expected {root_inode_id}, current {current_root_inode_id}"
                )));
            }
            let root_inode = self
                .storage
                .get_inode(root_inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Root inode not found: {root_inode_id}")))?;
            if !root_inode.kind.is_dir() || !matches!(&root_inode.data, InodeData::Dir) {
                return Err(MetadataError::NotDir(format!("Not a directory: {name}")));
            }
            if root_inode.inode_id != root_inode_id || self.storage.get_layout_optional(root_inode_id)?.is_some() {
                return Err(MetadataError::Internal(format!(
                    "directory inode {root_inode_id} carries file authority"
                )));
            }
            if self.storage.get_detached_root(root_inode_id)?.is_some() {
                return Err(MetadataError::Internal(format!(
                    "inode {root_inode_id} is both reachable and already detached"
                )));
            }

            let parent_inode = self
                .storage
                .get_inode(parent_inode_id)?
                .ok_or_else(|| MetadataError::Internal("Parent inode disappeared".to_string()))?;
            if !parent_inode.kind.is_dir() || !matches!(&parent_inode.data, InodeData::Dir) {
                return Err(MetadataError::NotDir(format!(
                    "Parent is not a directory: {parent_inode_id}"
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

            Ok((
                updated_parent,
                DetachedRoot {
                    mount_id: root_inode.mount_id,
                    detached_at_ms: proposed_at_ms,
                },
            ))
        })();

        let (updated_parent, detached_root) = match prepared {
            Ok(prepared) => prepared,
            Err(err) => return self.persist_fs_error(err, raft_state),
        };
        let result = FsCommandResult::Ok(FsOkResult::default());
        self.storage.detach_directory_atomic(
            parent_inode_id,
            &name,
            root_inode_id,
            &updated_parent,
            detached_root,
            raft_state,
        )?;
        Ok(result)
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
                if dst_inode.inode_id != dst_inode_id || self.storage.get_layout_optional(dst_inode_id)?.is_none() {
                    return Err(MetadataError::Internal(format!(
                        "file inode {dst_inode_id} has corrupt identity or missing layout: value_id={}",
                        dst_inode.inode_id
                    )));
                }
                Ok(PreparedRenameOverwrite { inode_id: dst_inode_id })
            }
            InodeData::Dir => {
                if !self.storage.is_directory_empty(dst_inode_id)? {
                    return Err(MetadataError::DirectoryNotEmpty(
                        "Cannot overwrite non-empty directory".to_string(),
                    ));
                }
                if dst_inode.inode_id != dst_inode_id || self.storage.get_layout_optional(dst_inode_id)?.is_some() {
                    return Err(MetadataError::Internal(format!(
                        "directory inode {dst_inode_id} carries invalid file authority: value_id={}",
                        dst_inode.inode_id
                    )));
                }
                Ok(PreparedRenameOverwrite { inode_id: dst_inode_id })
            }
            InodeData::Symlink { .. } => {
                if dst_inode.inode_id != dst_inode_id || self.storage.get_layout_optional(dst_inode_id)?.is_some() {
                    return Err(MetadataError::Internal(format!(
                        "symlink inode {dst_inode_id} carries invalid file authority: value_id={}",
                        dst_inode.inode_id
                    )));
                }
                Ok(PreparedRenameOverwrite { inode_id: dst_inode_id })
            }
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
    use crate::raft::response::ApplyRejectionKind;
    use crate::raft::state_machine::tests::*;

    fn test_state() -> (TempDir, Arc<RocksDBStorage>, AppRaftStateMachine, InodeId) {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let parent_inode_id = InodeId::new(10);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1)))
            .unwrap();
        storage.set_next_inode_id(InodeId::new(11)).unwrap();
        storage
            .put_mount(&crate::mount::MountEntry {
                mount_id: MountId::new(1),
                mount_prefix: crate::mount::ROOT_MOUNT_PREFIX.to_string(),
                mount_kind: crate::mount::MountKind::Internal,
                ufs_uri: None,
                data_io_policy: crate::mount::DataIoPolicy::Allow,
                mount_epoch: 1,
                namespace_owner_group_name: group_name("root"),
                root_inode_id: parent_inode_id,
            })
            .unwrap();
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));
        (dir, storage, sm, parent_inode_id)
    }

    fn delete_command(name: &str, expected_inode_id: InodeId, lease_epoch: Option<u64>, recursive: bool) -> Command {
        delete_path_command(vec![name.to_string()], expected_inode_id, lease_epoch, recursive)
    }

    fn delete_path_command(
        relative_components: Vec<String>,
        expected_inode_id: InodeId,
        lease_epoch: Option<u64>,
        recursive: bool,
    ) -> Command {
        Command::Delete {
            proposed_at_ms: 2,
            mount_id: MountId::new(1),
            expected_mount_epoch: 1,
            mount_root_inode_id: InodeId::new(10),
            relative_components,
            expected_inode_id,
            expected_file_lease_epoch: lease_epoch,
            recursive,
        }
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

    fn assert_delete_rejection_preserves_directory(
        storage: &RocksDBStorage,
        sm: &AppRaftStateMachine,
        parent_inode_id: InodeId,
        directory_inode_id: InodeId,
        command: Command,
        expected_rejection: ApplyRejectionKind,
    ) {
        expect_fs_rejection(sm.apply(command).unwrap(), expected_rejection);
        assert_eq!(
            storage.get_dentry(parent_inode_id, "target").unwrap(),
            Some(directory_inode_id)
        );
        assert!(storage.get_inode(directory_inode_id).unwrap().is_some());
        assert!(storage.get_detached_root(directory_inode_id).unwrap().is_none());
    }

    #[test]
    fn create_file_persists_namespace_inode_and_layout() {
        let (_dir, storage, sm, parent_inode_id) = test_state();

        let created = create_file(&sm, parent_inode_id, "file");
        let inode_id = created.inode_id.unwrap();

        assert_eq!(storage.get_dentry(parent_inode_id, "file").unwrap(), Some(inode_id));
        assert_eq!(storage.get_inode(inode_id).unwrap().unwrap().inode_id, inode_id);
        assert_eq!(storage.get_layout(inode_id).unwrap(), FileLayout::new(4096, 4096, 1));

        expect_fs_rejection(
            sm.apply(Command::CreateFile {
                proposed_at_ms: 2,
                parent_inode_id,
                name: "file".to_string(),
                attrs: FileAttrs::new(),
                layout: FileLayout::new(8192, 8192, 1),
            })
            .unwrap(),
            ApplyRejectionKind::AlreadyExists,
        );
    }

    fn assert_corrupt_inode_allocator_rejects_create(next_inode_id: Option<u64>, existing_inode_ids: &[u64]) {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let parent_inode_id = InodeId::new(10);
        let parent = Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1));
        storage.put_inode(&parent).unwrap();
        let existing: Vec<_> = existing_inode_ids
            .iter()
            .map(|raw| Inode::new_dir(InodeId::new(*raw), FileAttrs::new(), MountId::new(1)))
            .collect();
        for inode in &existing {
            storage.put_inode(inode).unwrap();
        }
        if let Some(next_inode_id) = next_inode_id {
            storage.set_next_inode_id(InodeId::new(next_inode_id)).unwrap();
        }
        let applied_before = storage.load_raft_state().unwrap();
        let rejected_applied_state = AppMetadataRaftState {
            last_applied_log_id: Some(openraft::LogId::new(openraft::LeaderId::new(8, 1), 801)),
            ..AppMetadataRaftState::default()
        };
        assert_ne!(rejected_applied_state, applied_before);
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let error = sm
            .apply_with_raft_state(
                Command::CreateFile {
                    proposed_at_ms: 1,
                    parent_inode_id,
                    name: "file".to_string(),
                    attrs: FileAttrs::new(),
                    layout: FileLayout::new(4096, 4096, 1),
                },
                &rejected_applied_state,
            )
            .unwrap_err();

        assert!(error.to_string().contains("next_inode_id allocator"));
        assert_eq!(storage.load_raft_state().unwrap(), applied_before);
        assert_eq!(storage.get_dentry(parent_inode_id, "file").unwrap(), None);
        assert_eq!(storage.get_inode(parent_inode_id).unwrap(), Some(parent));
        for inode in existing {
            assert_eq!(storage.get_inode(inode.inode_id).unwrap(), Some(inode));
        }
        if let Some(next_inode_id) = next_inode_id {
            assert_eq!(storage.get_layout_optional(InodeId::new(next_inode_id)).unwrap(), None);
        }
    }

    #[test]
    fn create_file_rejects_missing_invalid_and_reused_inode_allocator_authority() {
        assert_corrupt_inode_allocator_rejects_create(None, &[]);
        assert_corrupt_inode_allocator_rejects_create(Some(0), &[]);
        assert_corrupt_inode_allocator_rejects_create(Some(1), &[]);
        assert_corrupt_inode_allocator_rejects_create(Some(11), &[11]);
        assert_corrupt_inode_allocator_rejects_create(Some(11), &[12]);
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

        expect_fs_rejection(
            sm.apply(delete_command("target", original, Some(0), false)).unwrap(),
            ApplyRejectionKind::Again,
        );

        assert_eq!(
            storage.get_dentry(parent_inode_id, "target").unwrap(),
            Some(replacement)
        );
        assert!(storage.get_inode(replacement).unwrap().is_some());
    }

    #[test]
    fn delete_rejects_mount_root_key_identity_mismatch_without_path_deviation() {
        let (_dir, storage, sm, mount_root_inode_id) = test_state();
        let visible = create_file(&sm, mount_root_inode_id, "target");
        let diverted = create_file(&sm, mount_root_inode_id, "diverted");
        let visible_inode_id = visible.inode_id.unwrap();
        let diverted_inode_id = diverted.inode_id.unwrap();
        let diverted_parent_inode_id = InodeId::new(20);
        storage
            .put_dentry(diverted_parent_inode_id, "target", diverted_inode_id)
            .unwrap();
        storage
            .put_inode_at_storage_key(
                mount_root_inode_id,
                &Inode::new_dir(diverted_parent_inode_id, FileAttrs::new(), MountId::new(1)),
            )
            .unwrap();
        let applied_before = storage.load_raft_state().unwrap();

        let error = sm
            .apply(delete_command("target", diverted_inode_id, Some(0), false))
            .unwrap_err();

        assert!(error.to_string().contains("mount root inode key"));
        assert_eq!(
            storage.get_dentry(mount_root_inode_id, "target").unwrap(),
            Some(visible_inode_id)
        );
        assert_eq!(
            storage.get_dentry(diverted_parent_inode_id, "target").unwrap(),
            Some(diverted_inode_id)
        );
        assert!(storage.get_inode(visible_inode_id).unwrap().is_some());
        assert!(storage.get_inode(diverted_inode_id).unwrap().is_some());
        assert!(storage.get_layout_optional(visible_inode_id).unwrap().is_some());
        assert!(storage.get_layout_optional(diverted_inode_id).unwrap().is_some());
        assert!(storage.get_detached_root(diverted_inode_id).unwrap().is_none());
        assert_eq!(storage.load_raft_state().unwrap(), applied_before);
    }

    #[test]
    fn delete_rejects_intermediate_key_identity_mismatch_without_path_deviation() {
        let (_dir, storage, sm, mount_root_inode_id) = test_state();
        let outer_inode_id = expect_fs_ok(
            sm.apply(Command::CreateDirectory {
                proposed_at_ms: 1,
                root_inode_id: mount_root_inode_id,
                components: vec!["outer".to_string()],
                attrs: FileAttrs::new(),
                recursive: false,
            })
            .unwrap(),
        )
        .inode_id
        .unwrap();
        let visible = create_file(&sm, outer_inode_id, "target");
        let diverted = create_file(&sm, outer_inode_id, "diverted");
        let visible_inode_id = visible.inode_id.unwrap();
        let diverted_inode_id = diverted.inode_id.unwrap();
        let diverted_parent_inode_id = InodeId::new(200);
        storage
            .put_dentry(diverted_parent_inode_id, "target", diverted_inode_id)
            .unwrap();
        storage
            .put_inode_at_storage_key(
                outer_inode_id,
                &Inode::new_dir(diverted_parent_inode_id, FileAttrs::new(), MountId::new(1)),
            )
            .unwrap();
        let applied_before = storage.load_raft_state().unwrap();

        let error = sm
            .apply(delete_path_command(
                vec!["outer".to_string(), "target".to_string()],
                diverted_inode_id,
                Some(0),
                false,
            ))
            .unwrap_err();

        assert!(error.to_string().contains("inode key"));
        assert_eq!(
            storage.get_dentry(mount_root_inode_id, "outer").unwrap(),
            Some(outer_inode_id)
        );
        assert_eq!(
            storage.get_dentry(outer_inode_id, "target").unwrap(),
            Some(visible_inode_id)
        );
        assert_eq!(
            storage.get_dentry(diverted_parent_inode_id, "target").unwrap(),
            Some(diverted_inode_id)
        );
        assert!(storage.get_inode(visible_inode_id).unwrap().is_some());
        assert!(storage.get_inode(diverted_inode_id).unwrap().is_some());
        assert!(storage.get_layout_optional(visible_inode_id).unwrap().is_some());
        assert!(storage.get_layout_optional(diverted_inode_id).unwrap().is_some());
        assert!(storage.get_detached_root(diverted_inode_id).unwrap().is_none());
        assert_eq!(storage.load_raft_state().unwrap(), applied_before);
    }

    #[test]
    fn delete_rejects_stale_mount_and_target_fencing_without_mutation() {
        let (_dir, storage, sm, parent_inode_id) = test_state();
        let directory = expect_fs_ok(
            sm.apply(Command::CreateDirectory {
                proposed_at_ms: 1,
                root_inode_id: parent_inode_id,
                components: vec!["target".to_string()],
                attrs: FileAttrs::new(),
                recursive: false,
            })
            .unwrap(),
        )
        .inode_id
        .unwrap();
        let stale_commands = [
            Command::Delete {
                proposed_at_ms: 2,
                mount_id: MountId::new(1),
                expected_mount_epoch: 2,
                mount_root_inode_id: parent_inode_id,
                relative_components: vec!["target".to_string()],
                expected_inode_id: directory,
                expected_file_lease_epoch: None,
                recursive: true,
            },
            Command::Delete {
                proposed_at_ms: 2,
                mount_id: MountId::new(1),
                expected_mount_epoch: 1,
                mount_root_inode_id: InodeId::new(11),
                relative_components: vec!["target".to_string()],
                expected_inode_id: directory,
                expected_file_lease_epoch: None,
                recursive: true,
            },
            delete_command("target", InodeId::new(999), None, true),
        ];

        for command in stale_commands {
            assert_delete_rejection_preserves_directory(
                &storage,
                &sm,
                parent_inode_id,
                directory,
                command,
                ApplyRejectionKind::Again,
            );
        }
    }

    #[test]
    fn delete_rejects_invalid_components_without_mutation() {
        let (_dir, storage, sm, parent_inode_id) = test_state();
        let directory = expect_fs_ok(
            sm.apply(Command::CreateDirectory {
                proposed_at_ms: 1,
                root_inode_id: parent_inode_id,
                components: vec!["target".to_string()],
                attrs: FileAttrs::new(),
                recursive: false,
            })
            .unwrap(),
        )
        .inode_id
        .unwrap();
        let invalid_components = [
            Vec::new(),
            vec![String::new()],
            vec!["/".to_string()],
            vec!["nul\0component".to_string()],
            vec!["x".repeat(crate::path_resolver::MAX_PATH_COMPONENT_BYTES + 1)],
            vec!["x".to_string(); crate::path_resolver::MAX_PATH_COMPONENTS + 1],
        ];

        for relative_components in invalid_components {
            let command = Command::Delete {
                proposed_at_ms: 2,
                mount_id: MountId::new(1),
                expected_mount_epoch: 1,
                mount_root_inode_id: parent_inode_id,
                relative_components,
                expected_inode_id: directory,
                expected_file_lease_epoch: None,
                recursive: true,
            };
            assert_delete_rejection_preserves_directory(
                &storage,
                &sm,
                parent_inode_id,
                directory,
                command,
                ApplyRejectionKind::InvalidArgument,
            );
        }
    }

    #[test]
    fn rename_rejects_destination_changes_before_mutating_namespace() {
        let (_dir, storage, sm, parent_inode_id) = test_state();
        let source = create_file(&sm, parent_inode_id, "source").inode_id.unwrap();
        let replacement = create_file(&sm, parent_inode_id, "destination").inode_id.unwrap();

        expect_fs_rejection(
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
            ApplyRejectionKind::Again,
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

        expect_fs_rejection(
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
            ApplyRejectionKind::AlreadyExists,
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
        assert!(storage
            .get_layout_optional(destination.inode_id.unwrap())
            .unwrap()
            .is_none());
    }

    #[test]
    fn recursive_delete_atomically_detaches_root_without_removing_descendants() {
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

        expect_fs_ok(sm.apply(delete_command("dir", directory, None, true)).unwrap());

        assert_eq!(storage.get_dentry(parent_inode_id, "dir").unwrap(), None);
        assert!(storage.get_inode(directory).unwrap().is_some());
        assert!(storage.get_inode(file.inode_id.unwrap()).unwrap().is_some());
        assert!(storage.get_layout(file.inode_id.unwrap()).is_ok());
        assert_eq!(
            storage.get_detached_root(directory).unwrap(),
            Some(DetachedRoot {
                mount_id: MountId::new(1),
                detached_at_ms: 2,
            })
        );
    }

    #[test]
    fn recursive_delete_rejects_directory_layout_before_detach() {
        let (_dir, storage, sm, parent_inode_id) = test_state();
        let directory = expect_fs_ok(
            sm.apply(Command::CreateDirectory {
                proposed_at_ms: 1,
                root_inode_id: parent_inode_id,
                components: vec!["target".to_string()],
                attrs: FileAttrs::new(),
                recursive: false,
            })
            .unwrap(),
        )
        .inode_id
        .unwrap();
        let parent_before = storage.get_inode(parent_inode_id).unwrap().unwrap();
        let directory_before = storage.get_inode(directory).unwrap().unwrap();
        let layout = FileLayout::new(4096, 4096, 1);
        storage.put_layout(directory, layout).unwrap();
        let applied_before = storage.load_raft_state().unwrap();
        let rejected_applied_state = AppMetadataRaftState {
            last_applied_log_id: Some(openraft::LogId::new(openraft::LeaderId::new(7, 1), 701)),
            ..AppMetadataRaftState::default()
        };
        assert_ne!(rejected_applied_state, applied_before);

        let error = sm
            .apply_with_raft_state(delete_command("target", directory, None, true), &rejected_applied_state)
            .unwrap_err();

        assert!(error.to_string().contains("carries file authority"));
        assert_eq!(storage.get_dentry(parent_inode_id, "target").unwrap(), Some(directory));
        assert_eq!(storage.get_inode(parent_inode_id).unwrap(), Some(parent_before));
        assert_eq!(storage.get_inode(directory).unwrap(), Some(directory_before));
        assert_eq!(storage.get_layout(directory).unwrap(), layout);
        assert!(storage.get_detached_root(directory).unwrap().is_none());
        assert_eq!(storage.load_raft_state().unwrap(), applied_before);
    }

    #[test]
    fn stale_delete_cannot_mutate_a_parent_after_it_is_detached() {
        let (_dir, storage, sm, parent_inode_id) = test_state();
        let inner = expect_fs_ok(
            sm.apply(Command::CreateDirectory {
                proposed_at_ms: 1,
                root_inode_id: parent_inode_id,
                components: vec!["outer".to_string(), "inner".to_string()],
                attrs: FileAttrs::new(),
                recursive: true,
            })
            .unwrap(),
        )
        .inode_id
        .unwrap();
        let outer = storage.get_dentry(parent_inode_id, "outer").unwrap().unwrap();
        let stale_inner_delete = delete_path_command(vec!["outer".to_string(), "inner".to_string()], inner, None, true);

        expect_fs_ok(sm.apply(delete_command("outer", outer, None, true)).unwrap());
        expect_fs_rejection(sm.apply(stale_inner_delete).unwrap(), ApplyRejectionKind::NotFound);

        assert_eq!(storage.get_dentry(outer, "inner").unwrap(), Some(inner));
        assert!(storage.get_detached_root(outer).unwrap().is_some());
        assert!(storage.get_detached_root(inner).unwrap().is_none());
    }

    #[test]
    fn recursive_delete_rejects_nested_mount_before_detach() {
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
        storage
            .put_mount(&crate::mount::MountEntry {
                mount_id: MountId::new(2),
                mount_prefix: "/dir/nested".to_string(),
                mount_kind: crate::mount::MountKind::Internal,
                ufs_uri: None,
                data_io_policy: crate::mount::DataIoPolicy::Allow,
                mount_epoch: 2,
                namespace_owner_group_name: group_name("root"),
                root_inode_id: InodeId::new(200),
            })
            .unwrap();

        expect_fs_rejection(
            sm.apply(delete_command("dir", directory, None, true)).unwrap(),
            ApplyRejectionKind::CrossMountRename,
        );

        assert_eq!(storage.get_dentry(parent_inode_id, "dir").unwrap(), Some(directory));
        assert!(storage.get_detached_root(directory).unwrap().is_none());
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
        expect_fs_rejection(
            sm.apply(delete_command("target", inode_id, Some(0), false)).unwrap(),
            ApplyRejectionKind::Again,
        );

        assert_eq!(storage.get_dentry(parent_inode_id, "target").unwrap(), Some(inode_id));
    }

    #[test]
    fn delete_that_linearizes_first_prevents_later_lease_acquisition() {
        let (_dir, storage, sm, parent_inode_id) = test_state();
        let file = create_file(&sm, parent_inode_id, "target");
        let inode_id = file.inode_id.unwrap();

        expect_fs_ok(sm.apply(delete_command("target", inode_id, Some(0), false)).unwrap());
        expect_fs_rejection(
            sm.apply(Command::AcquireWriteLease {
                proposed_at_ms: 3,
                inode_id,
                expected_lease_epoch: 0,
            })
            .unwrap(),
            ApplyRejectionKind::NotFound,
        );

        assert_eq!(storage.get_dentry(parent_inode_id, "target").unwrap(), None);
        assert_eq!(storage.get_inode(inode_id).unwrap(), None);
    }

    #[test]
    fn recursive_delete_does_not_scan_descendant_lease_epochs() {
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
        expect_fs_ok(sm.apply(delete_command("dir", directory, None, true)).unwrap());

        assert_eq!(storage.get_dentry(parent_inode_id, "dir").unwrap(), None);
        assert_eq!(storage.get_dentry(directory, "file").unwrap(), Some(file_id));
        assert!(storage.get_detached_root(directory).unwrap().is_some());
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
        expect_fs_rejection(
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
            ApplyRejectionKind::Again,
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
        expect_fs_rejection(
            sm.apply(Command::AcquireWriteLease {
                proposed_at_ms: 3,
                inode_id: destination,
                expected_lease_epoch: 0,
            })
            .unwrap(),
            ApplyRejectionKind::NotFound,
        );

        assert_eq!(
            storage.get_dentry(parent_inode_id, "destination").unwrap(),
            Some(source)
        );
        assert_eq!(storage.get_inode(destination).unwrap(), None);
    }
}
