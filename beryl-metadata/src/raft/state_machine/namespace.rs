// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use super::{
    AppMetadataRaftState, AppRaftStateMachine, BootstrapNamespaceState, CreateFileOperationId, CreateFileReplayRecord,
    DetachedRoot, GroupName, Inode, InodeAttrs, InodeId, InodeKind, MetadataError, MetadataResult, MountId,
    RecursiveMkdirEntry, RenameAtomicUpdate,
};
use crate::mount::MountEntry;
use beryl_types::{ContentGeneration, LeaseEpoch};

impl AppRaftStateMachine {
    pub(super) fn apply_bootstrap_namespace(
        &self,
        group_name: GroupName,
        proposed_at_ms: u64,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<MountEntry> {
        let state = self.storage.bootstrap_namespace_state(&group_name)?;
        if state == BootstrapNamespaceState::Conflicting {
            return Err(MetadataError::InvalidArgument(
                "metadata namespace is partially initialized or conflicts with writable root bootstrap; reformat metadata storage"
                    .to_string(),
            ));
        }

        let root_mount = MountEntry {
            mount_id: MountId::new(1),
            namespace_owner_group_name: group_name,
            root_inode_id: crate::mount::ROOT_INODE_ID,
        };
        if state == BootstrapNamespaceState::Matching {
            self.storage.commit_applied_state(raft_state)?;
            return Ok(root_mount);
        }

        let mut attrs = InodeAttrs::new();
        attrs.initialize(proposed_at_ms);

        let root_inode = Inode::new_dir(crate::mount::ROOT_INODE_ID, attrs, MountId::new(1));
        self.storage
            .bootstrap_namespace_atomic(&root_inode, &root_mount, raft_state)?;
        Ok(root_mount)
    }

    /// Apply Mkdir command.
    pub(super) fn apply_mkdir(
        &self,
        parent_inode_id: InodeId,
        name: String,
        proposed_at_ms: u64,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<(InodeId, InodeAttrs)> {
        // Check parent exists and is a directory
        let parent_inode = self
            .storage
            .get_inode(parent_inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Parent inode not found: {}", parent_inode_id)))?;
        if !parent_inode.file_type().is_dir() {
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
        let mut attrs = InodeAttrs::new();
        attrs.initialize(proposed_at_ms);

        // Create directory inode (inherit mount_id from parent)
        let inode = Inode::new_dir(inode_id, attrs, parent_inode.mount_id);

        // Update parent directory modification time
        let mut updated_parent = parent_inode;
        updated_parent.attrs.set_modify_time(proposed_at_ms);

        let result = (inode.inode_id, inode.attrs.clone());
        self.storage
            .create_dir_atomic(allocation, parent_inode_id, &name, &inode, &updated_parent, raft_state)?;
        Ok(result)
    }

    /// Apply one recursive CreateDirectory command as a single authority batch.
    pub(super) fn apply_create_directory(
        &self,
        root_inode_id: InodeId,
        components: Vec<String>,
        proposed_at_ms: u64,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<(InodeId, InodeAttrs)> {
        if components.is_empty() || components.iter().any(|component| component.is_empty()) {
            return Err(MetadataError::InvalidArgument(
                "CreateDirectory requires non-empty path components".to_string(),
            ));
        }
        let mut parent = match self.storage.get_inode(root_inode_id)? {
            Some(inode) if inode.file_type().is_dir() => inode,
            Some(_) => {
                return Err(MetadataError::NotDir(format!(
                    "Root is not a directory: {root_inode_id}"
                )));
            }
            None => {
                return Err(MetadataError::NotFound(format!(
                    "Root inode not found: {root_inode_id}"
                )));
            }
        };
        let mut allocation = self.storage.prepare_inode_allocation()?;
        let mut next_raw = allocation.inode_id.as_raw();
        let mut entries = Vec::new();

        for name in components {
            if let Some(child_inode_id) = self.storage.get_dentry(parent.inode_id, &name)? {
                let child = match self.storage.get_inode(child_inode_id)? {
                    Some(inode) if inode.file_type().is_dir() => inode,
                    Some(_) => {
                        return Err(MetadataError::NotDir(format!(
                            "Path component is not a directory: {name}"
                        )));
                    }
                    None => {
                        return Err(MetadataError::NotFound(format!(
                            "Target inode not found: {child_inode_id}"
                        )));
                    }
                };
                parent = child;
                continue;
            }

            let inode_id = InodeId::new(next_raw);
            next_raw = next_raw
                .checked_add(1)
                .ok_or_else(|| MetadataError::Internal("inode ID allocator overflow".to_string()))?;
            let mut child_attrs = InodeAttrs::new();
            child_attrs.initialize(proposed_at_ms);

            let child = Inode::new_dir(inode_id, child_attrs, parent.mount_id);
            let parent_inode_id = parent.inode_id;
            let mut updated_parent = parent;
            updated_parent.attrs.set_modify_time(proposed_at_ms);
            entries.push(RecursiveMkdirEntry {
                parent_inode_id,
                name,
                inode: child.clone(),
                updated_parent,
            });
            parent = child;
        }

        let result = (parent.inode_id, parent.attrs.clone());
        if entries.is_empty() {
            self.storage.commit_applied_state(raft_state)?;
        } else {
            allocation.next_inode_id = InodeId::new(next_raw);
            self.storage
                .create_directories_atomic(allocation, &entries, raft_state)?;
        }
        Ok(result)
    }

    /// Create a file, its initial write lease, and its replay record in one commit.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_create(
        &self,
        operation_id: CreateFileOperationId,
        request_deadline_ms: u64,
        session_expires_at_ms: u64,
        mount_id: MountId,
        mount_root_inode_id: InodeId,
        relative_components: Vec<String>,
        block_size: u32,
        proposed_at_ms: u64,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<CreateFileReplayRecord> {
        if let Some(record) = self.storage.get_create_file_replay(operation_id)? {
            if record.request_deadline_ms != request_deadline_ms
                || record.mount_id != mount_id
                || record.mount_root_inode_id != mount_root_inode_id
                || record.relative_components != relative_components
            {
                return Err(MetadataError::InvalidArgument(
                    "CreateFile operation identity was reused with a different request".to_string(),
                ));
            }
            self.validate_create_file_replay(&record, proposed_at_ms)?;
            self.storage.commit_applied_state(raft_state)?;
            return Ok(record);
        }
        if request_deadline_ms < proposed_at_ms {
            return Err(MetadataError::InvalidArgument(
                "CreateFile request deadline expired before proposal".to_string(),
            ));
        }
        if session_expires_at_ms <= proposed_at_ms {
            return Err(MetadataError::Again(
                "CreateFile write session expired before proposal".to_string(),
            ));
        }
        beryl_types::validate_block_size(u64::from(block_size))
            .map_err(|error| MetadataError::InvalidArgument(format!("invalid CreateFile block_size: {error}")))?;
        let (parent_inode_id, name, parent_inode) =
            self.resolve_create_parent(mount_id, mount_root_inode_id, &relative_components)?;
        if self.storage.get_dentry(parent_inode_id, &name)?.is_some() {
            return Err(MetadataError::AlreadyExists(format!("File already exists: {name}")));
        }

        // Generate inode ID
        let allocation = self.storage.prepare_inode_allocation()?;
        let inode_id = allocation.inode_id;
        let mut attrs = InodeAttrs::new();
        attrs.initialize(proposed_at_ms);

        // Create the file under its single canonical inode identity.
        let mut inode = Inode::new_file(inode_id, attrs, parent_inode.mount_id, block_size);
        let InodeKind::File(crate::inode::FileData { lease_epoch, .. }) = &mut inode.kind else {
            unreachable!("new file constructor must produce file authority")
        };
        *lease_epoch = LeaseEpoch::new(1);

        // Update parent directory modification time
        let mut updated_parent = parent_inode;
        updated_parent.attrs.set_modify_time(proposed_at_ms);

        let record = CreateFileReplayRecord {
            operation_id,
            request_deadline_ms,
            parent_inode_id,
            inode_id: inode.inode_id,
            mount_id,
            mount_root_inode_id,
            relative_components,
            block_size,
            expires_at_ms: session_expires_at_ms,
        };
        self.storage.create_file_atomic(
            allocation,
            parent_inode_id,
            &name,
            &inode,
            &updated_parent,
            &record,
            proposed_at_ms,
            raft_state,
        )?;
        Ok(record)
    }

    /// Confirm that a durable CreateFile result still names its initial writable state.
    fn validate_create_file_replay(&self, record: &CreateFileReplayRecord, proposed_at_ms: u64) -> MetadataResult<()> {
        if record.expires_at_ms <= proposed_at_ms {
            return Err(MetadataError::Again(
                "replayed CreateFile write session has expired".to_string(),
            ));
        }
        let (parent_inode_id, name, _) =
            self.resolve_create_parent(record.mount_id, record.mount_root_inode_id, &record.relative_components)?;
        if parent_inode_id != record.parent_inode_id {
            return Err(MetadataError::Again(
                "replayed CreateFile path authority changed".to_string(),
            ));
        }
        if self.storage.get_dentry(record.parent_inode_id, &name)? != Some(record.inode_id) {
            return Err(MetadataError::AlreadyExists(
                "replayed CreateFile target no longer names its original inode".to_string(),
            ));
        }
        let inode = self
            .storage
            .get_inode(record.inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("CreateFile inode not found: {}", record.inode_id)))?;
        if inode.mount_id != record.mount_id {
            return Err(MetadataError::Internal(
                "replayed CreateFile inode authority is corrupt".to_string(),
            ));
        }
        let InodeKind::File(crate::inode::FileData {
            blocks,
            generation,
            lease_epoch,
            next_index,
            block_size,
            ..
        }) = &inode.kind
        else {
            return Err(MetadataError::Internal(
                "replayed CreateFile inode payload is not a file".to_string(),
            ));
        };
        if *lease_epoch != LeaseEpoch::new(1) {
            return Err(MetadataError::LeaseFenced {
                expected: *lease_epoch,
                got: LeaseEpoch::new(1),
            });
        }
        if !blocks.is_empty() || *generation != ContentGeneration::new(0) || *next_index != 0 || inode.len() != 0 {
            return Err(MetadataError::AlreadyExists(
                "replayed CreateFile result no longer owns the initial file state".to_string(),
            ));
        }
        if *block_size != record.block_size {
            return Err(MetadataError::Internal(
                "replayed CreateFile block_size authority changed".to_string(),
            ));
        }
        Ok(())
    }

    /// Revalidate the mount-relative parent path carried by CreateFile apply.
    fn resolve_create_parent(
        &self,
        mount_id: MountId,
        mount_root_inode_id: InodeId,
        relative_components: &[String],
    ) -> MetadataResult<(InodeId, String, Inode)> {
        Self::validate_relative_components("CreateFile", relative_components)?;
        let mount = self
            .storage
            .get_mount(mount_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Mount not found: {mount_id:?}")))?;
        if mount.root_inode_id != mount_root_inode_id {
            return Err(MetadataError::Again(format!(
                "CreateFile mount precondition changed for {mount_id:?}"
            )));
        }
        let mut parent = self
            .storage
            .get_inode(mount_root_inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Mount root inode not found: {mount_root_inode_id}")))?;
        if parent.mount_id != mount_id || !parent.file_type().is_dir() {
            return Err(MetadataError::Internal(
                "CreateFile mount root authority is corrupt".to_string(),
            ));
        }
        let (name, parent_components) = relative_components
            .split_last()
            .expect("validated CreateFile path has a terminal component");
        for component in parent_components {
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
            if child.mount_id != mount_id {
                return Err(MetadataError::Internal(
                    "CreateFile parent path authority is corrupt".to_string(),
                ));
            }
            if !child.file_type().is_dir() {
                return Err(MetadataError::NotDir(format!(
                    "Path component is not a directory: {component}"
                )));
            }
            parent = child;
        }
        Ok((parent.inode_id, name.clone(), parent))
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
        mount_root_inode_id: InodeId,
        relative_components: Vec<String>,
        expected_inode_id: InodeId,
        expected_file_lease_epoch: Option<LeaseEpoch>,
        recursive: bool,
        proposed_at_ms: u64,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
        let (mut parent_inode, name, child_inode_id, child_inode) =
            self.resolve_delete_target(mount_id, mount_root_inode_id, &relative_components)?;
        if child_inode.inode_id != expected_inode_id {
            return Err(MetadataError::Again(format!(
                "delete target changed for {name}: expected {expected_inode_id}, current {}",
                child_inode.inode_id
            )));
        }

        let current_file_lease_epoch = match &child_inode.kind {
            InodeKind::File(crate::inode::FileData { lease_epoch, .. }) => Some(*lease_epoch),
            InodeKind::Dir => None,
        };
        if current_file_lease_epoch != expected_file_lease_epoch {
            return Err(MetadataError::Again(format!(
                "delete target lease precondition changed: expected {expected_file_lease_epoch:?}, current {current_file_lease_epoch:?}"
            )));
        }
        parent_inode.attrs.set_modify_time(proposed_at_ms);
        if child_inode.file_type().is_dir() && recursive {
            if child_inode_id != child_inode.inode_id {
                return Err(MetadataError::Again(format!(
                    "delete target changed for {name}: expected {}, current {child_inode_id}",
                    child_inode.inode_id
                )));
            }
            if self.storage.get_detached_root(child_inode_id)?.is_some() {
                return Err(MetadataError::Internal(format!(
                    "inode {child_inode_id} is both reachable and already detached"
                )));
            }
            self.storage.detach_directory_atomic(
                parent_inode.inode_id,
                &name,
                child_inode_id,
                &parent_inode,
                DetachedRoot {
                    mount_id: child_inode.mount_id,
                    detached_at_ms: proposed_at_ms,
                },
                raft_state,
            )?;
        } else {
            if child_inode.file_type().is_dir() && !self.storage.is_directory_empty(child_inode_id)? {
                return Err(MetadataError::DirectoryNotEmpty(format!("Directory not empty: {name}")));
            }
            self.storage.unlink_inode_atomic(
                parent_inode.inode_id,
                &name,
                child_inode_id,
                &parent_inode,
                raft_state,
            )?;
        }
        Ok(())
    }

    /// Resolve and validate the exact target named by a replicated Delete command.
    fn resolve_delete_target(
        &self,
        mount_id: MountId,
        mount_root_inode_id: InodeId,
        relative_components: &[String],
    ) -> MetadataResult<(Inode, String, InodeId, Inode)> {
        Self::validate_relative_components("Delete", relative_components)?;
        let mount = self
            .storage
            .get_mount(mount_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Mount not found: {mount_id:?}")))?;
        if mount.root_inode_id != mount_root_inode_id {
            return Err(MetadataError::Again(format!(
                "delete mount precondition changed for {mount_id:?}"
            )));
        }

        let target_path_bytes = relative_components.len() + relative_components.iter().map(String::len).sum::<usize>();
        if target_path_bytes > crate::path_resolver::MAX_PATH_BYTES {
            return Err(MetadataError::InvalidArgument(format!(
                "Delete path exceeds {} bytes",
                crate::path_resolver::MAX_PATH_BYTES
            )));
        }
        let mut parent = self
            .storage
            .get_inode(mount_root_inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Mount root inode not found: {mount_root_inode_id}")))?;
        if !parent.file_type().is_dir() {
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
            if child.mount_id != mount_id {
                return Err(MetadataError::CrossMountRename(
                    "delete path crosses mount authority".to_string(),
                ));
            }
            if index + 1 == relative_components.len() {
                if mount_root_inode_id == child_inode_id {
                    return Err(MetadataError::InvalidArgument(format!(
                        "Cannot delete mount root inode {child_inode_id}"
                    )));
                }
                return Ok((parent, component.clone(), child_inode_id, child));
            }
            if !child.file_type().is_dir() {
                return Err(MetadataError::NotDir(format!(
                    "Path component is not a directory: {component}"
                )));
            }
            parent = child;
        }

        unreachable!("Delete components are checked as non-empty")
    }

    /// Enforce the fixed replicated bound for one mount-relative namespace path.
    fn validate_relative_components(operation: &str, relative_components: &[String]) -> MetadataResult<()> {
        if relative_components.is_empty() {
            return Err(MetadataError::InvalidArgument(format!(
                "{operation} cannot target a mount root"
            )));
        }
        if relative_components.len() > crate::path_resolver::MAX_PATH_COMPONENTS {
            return Err(MetadataError::InvalidArgument(format!(
                "{operation} path exceeds {} components",
                crate::path_resolver::MAX_PATH_COMPONENTS
            )));
        }
        for component in relative_components {
            if component.is_empty() || component.contains('/') || component.contains('\0') {
                return Err(MetadataError::InvalidArgument(format!(
                    "{operation} path contains an invalid component"
                )));
            }
            if component.len() > crate::path_resolver::MAX_PATH_COMPONENT_BYTES {
                return Err(MetadataError::InvalidArgument(format!(
                    "{operation} path component exceeds {} bytes",
                    crate::path_resolver::MAX_PATH_COMPONENT_BYTES
                )));
            }
        }
        Ok(())
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
        expected_dst_lease_epoch: Option<LeaseEpoch>,
        flags: u32,
        proposed_at_ms: u64,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<()> {
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
            if src_inode_id != dst_inode_id {
                // Destination exists - check if it's a directory and empty (if source is directory)
                let dst_inode = self
                    .storage
                    .get_inode(dst_inode_id)?
                    .ok_or_else(|| MetadataError::Internal("Destination inode disappeared".to_string()))?;
                let current_dst_lease_epoch = match &dst_inode.kind {
                    InodeKind::File(crate::inode::FileData { lease_epoch, .. }) => Some(*lease_epoch),
                    _ => None,
                };
                if current_dst_lease_epoch != expected_dst_lease_epoch {
                    return Err(MetadataError::Again(format!(
                        "rename destination lease epoch changed for {dst_name}: expected {expected_dst_lease_epoch:?}, current {current_dst_lease_epoch:?}"
                    )));
                }

                if src_inode.file_type().is_dir() {
                    if !dst_inode.file_type().is_dir() {
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
                    if dst_inode.file_type().is_dir() {
                        return Err(MetadataError::IsDir("Cannot overwrite directory with file".to_string()));
                    }
                }
                overwritten_target = Some(dst_inode_id);
            }
        }

        // Update parent directories modification time
        let (updated_src_parent, updated_dst_parent) = if current_dst_inode_id == Some(src_inode_id) {
            (None, None)
        } else if src_parent_inode_id != dst_parent_inode_id {
            // Different parents - update both
            let mut src_parent = self
                .storage
                .get_inode(src_parent_inode_id)?
                .ok_or_else(|| MetadataError::Internal("Source parent disappeared".to_string()))?;
            src_parent.attrs.set_modify_time(proposed_at_ms);
            let mut dst_parent = self
                .storage
                .get_inode(dst_parent_inode_id)?
                .ok_or_else(|| MetadataError::Internal("Destination parent disappeared".to_string()))?;
            dst_parent.attrs.set_modify_time(proposed_at_ms);
            (Some(src_parent), Some(dst_parent))
        } else {
            let mut parent = self
                .storage
                .get_inode(src_parent_inode_id)?
                .ok_or_else(|| MetadataError::Internal("Parent disappeared".to_string()))?;
            parent.attrs.set_modify_time(proposed_at_ms);
            (Some(parent), None)
        };

        self.storage.rename_atomic(
            RenameAtomicUpdate {
                src_parent_inode_id,
                src_name: &src_name,
                dst_parent_inode_id,
                dst_name: &dst_name,
                src_inode_id,
                overwritten_target,
                updated_src_parent: updated_src_parent.as_ref(),
                updated_dst_parent: updated_dst_parent.as_ref(),
            },
            raft_state,
        )?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::response::ApplyRejectionKind;
    use crate::raft::state_machine::tests::*;
    use beryl_types::{CallId, ClientId};

    fn test_state() -> (TempDir, Arc<RocksDBStorage>, AppRaftStateMachine, InodeId) {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let parent_inode_id = crate::mount::ROOT_INODE_ID;
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, InodeAttrs::new(), MountId::new(1)))
            .unwrap();
        storage.set_next_inode_id(InodeId::new(11)).unwrap();
        storage
            .put_mount(&MountEntry {
                mount_id: MountId::new(1),
                namespace_owner_group_name: group_name("root"),
                root_inode_id: parent_inode_id,
            })
            .unwrap();
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));
        (dir, storage, sm, parent_inode_id)
    }

    fn delete_command(
        name: &str,
        expected_inode_id: InodeId,
        lease_epoch: Option<LeaseEpoch>,
        recursive: bool,
    ) -> Command {
        delete_path_command(vec![name.to_string()], expected_inode_id, lease_epoch, recursive)
    }

    fn delete_path_command(
        relative_components: Vec<String>,
        expected_inode_id: InodeId,
        lease_epoch: Option<LeaseEpoch>,
        recursive: bool,
    ) -> Command {
        Command::Delete {
            proposed_at_ms: 2,
            mount_id: MountId::new(1),
            mount_root_inode_id: crate::mount::ROOT_INODE_ID,
            relative_components,
            expected_inode_id,
            expected_file_lease_epoch: lease_epoch,
            recursive,
        }
    }

    fn create_file_command(
        operation_id: CreateFileOperationId,
        mount_root_inode_id: InodeId,
        components: &[&str],
    ) -> Command {
        let relative_components: Vec<_> = components.iter().map(|component| (*component).to_string()).collect();
        Command::CreateFile {
            proposed_at_ms: 1,
            operation_id,
            request_deadline_ms: 100,
            session_expires_at_ms: 100,
            mount_id: MountId::new(1),
            mount_root_inode_id,
            relative_components,
            block_size: 4096,
        }
    }

    fn create_file(sm: &AppRaftStateMachine, mount_root_inode_id: InodeId, components: &[&str]) -> InodeId {
        let command = create_file_command(
            CreateFileOperationId {
                client_id: ClientId::new(1),
                call_id: CallId::new(),
            },
            mount_root_inode_id,
            components,
        );
        expect_file_created(sm.apply(command).unwrap()).0
    }

    fn assert_delete_rejection_preserves_directory(
        storage: &RocksDBStorage,
        sm: &AppRaftStateMachine,
        parent_inode_id: InodeId,
        directory_inode_id: InodeId,
        command: Command,
        expected_rejection: ApplyRejectionKind,
    ) {
        expect_apply_rejection(sm.apply(command), expected_rejection);
        assert_eq!(
            storage.get_dentry(parent_inode_id, "target").unwrap(),
            Some(directory_inode_id)
        );
        assert!(storage.get_inode(directory_inode_id).unwrap().is_some());
        assert!(storage.get_detached_root(directory_inode_id).unwrap().is_none());
    }

    #[test]
    fn create_file_replays_one_durable_result_without_allocating_again() {
        let (_dir, storage, sm, parent_inode_id) = test_state();
        let operation_id = CreateFileOperationId {
            client_id: ClientId::new(7),
            call_id: CallId::new(),
        };
        let command = create_file_command(operation_id, parent_inode_id, &["target"]);

        let first = expect_file_created(sm.apply(command.clone()).unwrap());
        let next_inode_id = storage.get_next_inode_id().unwrap();
        expect_apply_rejection(
            sm.apply(Command::AcquireWriteLease {
                proposed_at_ms: 2,
                inode_id: first.0,
                expected_lease_epoch: LeaseEpoch::new(1),
            }),
            ApplyRejectionKind::Again,
        );

        let mut replay_command = command;
        let Command::CreateFile {
            proposed_at_ms,
            request_deadline_ms,
            session_expires_at_ms,
            ..
        } = &mut replay_command
        else {
            unreachable!("test helper must build CreateFile")
        };
        *proposed_at_ms = 2;
        *request_deadline_ms = 200;
        *session_expires_at_ms = 200;
        expect_apply_rejection(sm.apply(replay_command.clone()), ApplyRejectionKind::InvalidArgument);
        let Command::CreateFile {
            request_deadline_ms,
            block_size,
            ..
        } = &mut replay_command
        else {
            unreachable!("test helper must build CreateFile")
        };
        *request_deadline_ms = 100;
        *block_size = 8192; // A changed new-file default cannot change a replayed result.
        let replay = expect_file_created(sm.apply(replay_command.clone()).unwrap());

        assert_eq!(replay, first);
        assert_eq!(storage.get_next_inode_id().unwrap(), next_inode_id);
        assert_eq!(storage.get_dentry(parent_inode_id, "target").unwrap(), Some(first.0));
        assert_eq!(
            storage.get_create_file_replay(operation_id).unwrap().unwrap().inode_id,
            first.0
        );
        assert_eq!(
            storage
                .get_create_file_replay_for_inode(first.0)
                .unwrap()
                .unwrap()
                .operation_id,
            operation_id
        );

        expect_apply_rejection(
            sm.apply(create_file_command(operation_id, parent_inode_id, &["other"])),
            ApplyRejectionKind::InvalidArgument,
        );
        assert_eq!(storage.get_dentry(parent_inode_id, "other").unwrap(), None);

        let Command::CreateFile { proposed_at_ms, .. } = &mut replay_command else {
            unreachable!("test helper must build CreateFile")
        };
        *proposed_at_ms = 100;
        expect_apply_rejection(sm.apply(replay_command), ApplyRejectionKind::Again);
        expect_write_lease_acquired(
            sm.apply(Command::AcquireWriteLease {
                proposed_at_ms: 100,
                inode_id: first.0,
                expected_lease_epoch: LeaseEpoch::new(1),
            })
            .unwrap(),
        );
    }

    #[test]
    fn delete_rejects_stale_mount_and_target_fencing_without_mutation() {
        let (_dir, storage, sm, parent_inode_id) = test_state();
        let directory = expect_directory_ensured(
            sm.apply(Command::CreateDirectory {
                proposed_at_ms: 1,
                root_inode_id: parent_inode_id,
                components: vec!["target".to_string()],
                recursive: false,
            })
            .unwrap(),
        )
        .0;
        let stale_commands = [
            Command::Delete {
                proposed_at_ms: 2,
                mount_id: MountId::new(1),
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
    fn recursive_delete_atomically_detaches_root_without_removing_descendants() {
        let (_dir, storage, sm, parent_inode_id) = test_state();
        let directory = expect_directory_ensured(
            sm.apply(Command::CreateDirectory {
                proposed_at_ms: 1,
                root_inode_id: parent_inode_id,
                components: vec!["dir".to_string()],
                recursive: false,
            })
            .unwrap(),
        )
        .0;
        let file = create_file(&sm, parent_inode_id, &["dir", "file"]);

        expect_delete_applied(sm.apply(delete_command("dir", directory, None, true)).unwrap());

        assert_eq!(storage.get_dentry(parent_inode_id, "dir").unwrap(), None);
        assert!(storage.get_inode(directory).unwrap().is_some());
        assert!(storage.get_inode(file).unwrap().is_some());
        assert_eq!(
            storage.get_detached_root(directory).unwrap(),
            Some(DetachedRoot {
                mount_id: MountId::new(1),
                detached_at_ms: 2,
            })
        );
    }

    #[test]
    fn stale_delete_cannot_mutate_a_parent_after_it_is_detached() {
        let (_dir, storage, sm, parent_inode_id) = test_state();
        let inner = expect_directory_ensured(
            sm.apply(Command::CreateDirectory {
                proposed_at_ms: 1,
                root_inode_id: parent_inode_id,
                components: vec!["outer".to_string(), "inner".to_string()],
                recursive: true,
            })
            .unwrap(),
        )
        .0;
        let outer = storage.get_dentry(parent_inode_id, "outer").unwrap().unwrap();
        let stale_inner_delete = delete_path_command(vec!["outer".to_string(), "inner".to_string()], inner, None, true);

        expect_delete_applied(sm.apply(delete_command("outer", outer, None, true)).unwrap());
        expect_apply_rejection(sm.apply(stale_inner_delete), ApplyRejectionKind::NotFound);

        assert_eq!(storage.get_dentry(outer, "inner").unwrap(), Some(inner));
        assert!(storage.get_detached_root(outer).unwrap().is_some());
        assert!(storage.get_detached_root(inner).unwrap().is_none());
    }

    #[test]
    fn delete_rejects_a_lease_acquired_after_preflight() {
        let (_dir, storage, sm, parent_inode_id) = test_state();
        let inode_id = create_file(&sm, parent_inode_id, &["target"]);

        expect_write_lease_acquired(
            sm.apply(Command::AcquireWriteLease {
                proposed_at_ms: 100,
                inode_id,
                expected_lease_epoch: LeaseEpoch::new(1),
            })
            .unwrap(),
        );
        expect_apply_rejection(
            sm.apply(delete_command("target", inode_id, Some(LeaseEpoch::new(1)), false)),
            ApplyRejectionKind::Again,
        );

        assert_eq!(storage.get_dentry(parent_inode_id, "target").unwrap(), Some(inode_id));
    }

    #[test]
    fn delete_that_linearizes_first_prevents_later_lease_acquisition() {
        let (_dir, storage, sm, parent_inode_id) = test_state();
        let inode_id = create_file(&sm, parent_inode_id, &["target"]);

        expect_delete_applied(
            sm.apply(delete_command("target", inode_id, Some(LeaseEpoch::new(1)), false))
                .unwrap(),
        );
        expect_apply_rejection(
            sm.apply(Command::AcquireWriteLease {
                proposed_at_ms: 3,
                inode_id,
                expected_lease_epoch: LeaseEpoch::new(1),
            }),
            ApplyRejectionKind::NotFound,
        );

        assert_eq!(storage.get_dentry(parent_inode_id, "target").unwrap(), None);
        assert_eq!(storage.get_inode(inode_id).unwrap(), None);
    }
}
