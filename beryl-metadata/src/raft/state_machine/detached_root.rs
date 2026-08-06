// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Deterministic, bounded reclamation of unreachable namespace roots.

use super::*;
use crate::path_resolver::MAX_PATH_COMPONENT_BYTES;
use crate::raft::{
    MAX_RECLAIM_DETACHED_ROOT_BATCH_BYTES, MAX_RECLAIM_DETACHED_ROOT_CANDIDATES, MAX_RECLAIM_DETACHED_ROOT_ENTRIES,
    MIN_RECLAIM_DETACHED_ROOT_BATCH_BYTES,
};
use std::collections::BTreeSet;

impl AppRaftStateMachine {
    /// Apply one bounded reclamation batch from a leader-selected root set.
    ///
    /// Marker absence is an idempotent no-op. Every marker that is present is
    /// validated with its inode, mount, descendants, and layouts before
    /// one authority batch is published.
    pub(super) fn apply_reclaim_detached_roots(
        &self,
        candidate_root_inode_ids: Vec<InodeId>,
        max_entries: u32,
        max_batch_bytes: u32,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<DetachedRootReclaimResult> {
        Self::validate_detached_root_reclaim_command(&candidate_root_inode_ids, max_entries, max_batch_bytes)?;

        let max_entries = max_entries as usize;
        let max_batch_bytes = max_batch_bytes as usize;
        let mut update = DetachedRootReclaimUpdate::default();
        let mut logical_batch_bytes = update.logical_batch_bytes(raft_state)?;
        if logical_batch_bytes > max_batch_bytes {
            return Err(MetadataError::Internal(format!(
                "Raft apply state requires {logical_batch_bytes} logical bytes, exceeding detached-root batch budget {max_batch_bytes}"
            )));
        }

        let mut seen_candidates = BTreeSet::new();
        let mut planned_entry_inode_ids = BTreeSet::new();
        let mut processed_entries = 0usize;
        let mut created_roots = 0usize;

        'candidates: for root_inode_id in candidate_root_inode_ids {
            if !seen_candidates.insert(root_inode_id) {
                continue;
            }
            let Some(detached_root) = self.storage.get_detached_root(root_inode_id)? else {
                continue;
            };
            let mount_root_inode_id = self.validate_detached_root(root_inode_id, detached_root)?;
            let scan_limit = max_entries.saturating_sub(processed_entries).max(1);
            let (entries, eof) = self.storage.list_dentries_for_reclaim(root_inode_id, scan_limit)?;
            let mut consumed_page = true;

            for (name, child_inode_id) in entries {
                if processed_entries == max_entries {
                    consumed_page = false;
                    break;
                }
                if !planned_entry_inode_ids.insert(child_inode_id) {
                    return Err(MetadataError::Internal(format!(
                        "detached-root forest contains duplicate inode {child_inode_id}"
                    )));
                }
                let entry = self.prepare_detached_root_entry(
                    root_inode_id,
                    name,
                    child_inode_id,
                    detached_root,
                    mount_root_inode_id,
                )?;
                let entry_logical_bytes = entry.logical_bytes()?;
                let next_logical_bytes = logical_batch_bytes
                    .checked_add(entry_logical_bytes)
                    .ok_or_else(|| MetadataError::Internal("detached-root logical byte count overflow".to_string()))?;
                if next_logical_bytes > max_batch_bytes {
                    if update.entries.is_empty() && update.completed_root_inode_ids.is_empty() {
                        return Err(MetadataError::Internal(format!(
                            "detached-root entry {child_inode_id} requires {next_logical_bytes} logical bytes, exceeding batch budget {max_batch_bytes}"
                        )));
                    }
                    break 'candidates;
                }

                created_roots += usize::from(entry.child_detached_root.is_some());
                update.entries.push(entry);
                processed_entries += 1;
                logical_batch_bytes = next_logical_bytes;
            }

            if consumed_page && eof {
                let completion_bytes = DetachedRootReclaimUpdate::completed_root_logical_bytes(root_inode_id)?;
                let next_logical_bytes = logical_batch_bytes
                    .checked_add(completion_bytes)
                    .ok_or_else(|| MetadataError::Internal("detached-root logical byte count overflow".to_string()))?;
                if next_logical_bytes > max_batch_bytes {
                    if update.entries.is_empty() && update.completed_root_inode_ids.is_empty() {
                        return Err(MetadataError::Internal(format!(
                            "detached-root completion for inode {root_inode_id} requires {next_logical_bytes} logical bytes, exceeding batch budget {max_batch_bytes}"
                        )));
                    }
                    break;
                }
                update.completed_root_inode_ids.push(root_inode_id);
                logical_batch_bytes = next_logical_bytes;
            }
        }

        let verified_logical_bytes = update.logical_batch_bytes(raft_state)?;
        if verified_logical_bytes != logical_batch_bytes {
            return Err(MetadataError::Internal(format!(
                "detached-root logical byte accounting diverged: prepared={logical_batch_bytes}, verified={verified_logical_bytes}"
            )));
        }
        let completed_roots = update.completed_root_inode_ids.len();
        self.storage.reclaim_detached_roots_atomic(update, raft_state)?;

        Ok(DetachedRootReclaimResult {
            processed_entries: u32::try_from(processed_entries)
                .expect("processed entries are bounded by a u32 protocol limit"),
            completed_roots: u32::try_from(completed_roots)
                .expect("candidate roots are bounded by a u32 protocol limit"),
            created_roots: u32::try_from(created_roots).expect("created roots are bounded by a u32 protocol limit"),
            logical_batch_bytes: u32::try_from(logical_batch_bytes)
                .expect("logical bytes are bounded by a u32 protocol limit"),
        })
    }

    fn validate_detached_root_reclaim_command(
        candidate_root_inode_ids: &[InodeId],
        max_entries: u32,
        max_batch_bytes: u32,
    ) -> MetadataResult<()> {
        if candidate_root_inode_ids.is_empty()
            || candidate_root_inode_ids.len() > MAX_RECLAIM_DETACHED_ROOT_CANDIDATES as usize
            || candidate_root_inode_ids.iter().any(|inode_id| inode_id.as_raw() == 0)
        {
            return Err(MetadataError::Internal(format!(
                "invalid detached-root candidate count or identity: count={}, maximum={MAX_RECLAIM_DETACHED_ROOT_CANDIDATES}",
                candidate_root_inode_ids.len()
            )));
        }
        if max_entries == 0 || max_entries > MAX_RECLAIM_DETACHED_ROOT_ENTRIES {
            return Err(MetadataError::Internal(format!(
                "invalid detached-root entry budget {max_entries}; maximum={MAX_RECLAIM_DETACHED_ROOT_ENTRIES}"
            )));
        }
        if !(MIN_RECLAIM_DETACHED_ROOT_BATCH_BYTES..=MAX_RECLAIM_DETACHED_ROOT_BATCH_BYTES).contains(&max_batch_bytes) {
            return Err(MetadataError::Internal(format!(
                "invalid detached-root byte budget {max_batch_bytes}; range={MIN_RECLAIM_DETACHED_ROOT_BATCH_BYTES}..={MAX_RECLAIM_DETACHED_ROOT_BATCH_BYTES}"
            )));
        }
        Ok(())
    }

    /// Validate the marker-to-root relationship and return its active mount root.
    fn validate_detached_root(&self, root_inode_id: InodeId, detached_root: DetachedRoot) -> MetadataResult<InodeId> {
        let root_inode = self.storage.get_inode(root_inode_id)?.ok_or_else(|| {
            MetadataError::Internal(format!("DetachedRoot for inode {root_inode_id} has no directory inode"))
        })?;
        if root_inode.inode_id != root_inode_id {
            return Err(MetadataError::Internal(format!(
                "DetachedRoot inode key {root_inode_id} contains inode {}",
                root_inode.inode_id
            )));
        }
        if root_inode.kind != root_inode.data.kind() {
            return Err(MetadataError::Internal(format!(
                "DetachedRoot inode {root_inode_id} kind and payload disagree"
            )));
        }
        if !root_inode.kind.is_dir() {
            return Err(MetadataError::Internal(format!(
                "DetachedRoot inode {root_inode_id} is not a directory"
            )));
        }
        if self.storage.get_layout_optional(root_inode_id)?.is_some() {
            return Err(MetadataError::Internal(format!(
                "DetachedRoot directory inode {root_inode_id} carries file authority"
            )));
        }
        if root_inode.mount_id != detached_root.mount_id {
            return Err(MetadataError::Internal(format!(
                "DetachedRoot inode {root_inode_id} belongs to mount {}, marker names mount {}",
                root_inode.mount_id, detached_root.mount_id
            )));
        }
        let mount = self.storage.get_mount(detached_root.mount_id)?.ok_or_else(|| {
            MetadataError::Internal(format!(
                "DetachedRoot inode {root_inode_id} references missing mount {}",
                detached_root.mount_id
            ))
        })?;
        if mount.mount_id != detached_root.mount_id {
            return Err(MetadataError::Internal(format!(
                "mount key {} contains mount {} while reclaiming inode {root_inode_id}",
                detached_root.mount_id, mount.mount_id
            )));
        }
        if mount.root_inode_id == root_inode_id {
            return Err(MetadataError::Internal(format!(
                "mount root inode {root_inode_id} cannot be a DetachedRoot"
            )));
        }
        Ok(mount.root_inode_id)
    }

    fn prepare_detached_root_entry(
        &self,
        parent_inode_id: InodeId,
        name: String,
        child_inode_id: InodeId,
        parent_detached_root: DetachedRoot,
        mount_root_inode_id: InodeId,
    ) -> MetadataResult<DetachedRootReclaimEntry> {
        if name.is_empty() || name.len() > MAX_PATH_COMPONENT_BYTES || name.contains('/') || name.contains('\0') {
            return Err(MetadataError::Internal(format!(
                "invalid dentry name under DetachedRoot inode {parent_inode_id}"
            )));
        }
        if child_inode_id.as_raw() == 0 {
            return Err(MetadataError::Internal(format!(
                "DetachedRoot inode {parent_inode_id} references zero child inode"
            )));
        }
        if self.storage.get_detached_root(child_inode_id)?.is_some() {
            return Err(MetadataError::Internal(format!(
                "inode {child_inode_id} is both reachable from DetachedRoot {parent_inode_id} and independently detached"
            )));
        }
        let child_inode = self.storage.get_inode(child_inode_id)?.ok_or_else(|| {
            MetadataError::Internal(format!(
                "DetachedRoot inode {parent_inode_id} references missing child inode {child_inode_id}"
            ))
        })?;
        if child_inode.inode_id != child_inode_id {
            return Err(MetadataError::Internal(format!(
                "inode key {child_inode_id} under DetachedRoot {parent_inode_id} contains inode {}",
                child_inode.inode_id
            )));
        }
        if child_inode.kind != child_inode.data.kind() {
            return Err(MetadataError::Internal(format!(
                "inode {child_inode_id} under DetachedRoot {parent_inode_id} has kind/payload mismatch"
            )));
        }
        if child_inode.mount_id != parent_detached_root.mount_id {
            return Err(MetadataError::Internal(format!(
                "DetachedRoot inode {parent_inode_id} crosses from mount {} to child inode {child_inode_id} in mount {}",
                parent_detached_root.mount_id, child_inode.mount_id
            )));
        }

        let (remove_file_layout, child_detached_root) = match child_inode.data {
            InodeData::Dir => {
                if self.storage.get_layout_optional(child_inode_id)?.is_some() {
                    return Err(MetadataError::Internal(format!(
                        "directory inode {child_inode_id} under DetachedRoot {parent_inode_id} carries file authority"
                    )));
                }
                if child_inode_id == mount_root_inode_id {
                    return Err(MetadataError::Internal(format!(
                        "DetachedRoot inode {parent_inode_id} reaches mount root inode {child_inode_id}"
                    )));
                }
                (false, Some(parent_detached_root))
            }
            InodeData::File { .. } => {
                if self.storage.get_layout_optional(child_inode_id)?.is_none() {
                    return Err(MetadataError::Internal(format!(
                        "detached file inode {child_inode_id} has no file layout"
                    )));
                }
                (true, None)
            }
            InodeData::Symlink { .. } => {
                if self.storage.get_layout_optional(child_inode_id)?.is_some() {
                    return Err(MetadataError::Internal(format!(
                        "symlink inode {child_inode_id} under DetachedRoot {parent_inode_id} carries file authority"
                    )));
                }
                (false, None)
            }
        };

        Ok(DetachedRootReclaimEntry {
            parent_inode_id,
            name,
            inode_id: child_inode_id,
            remove_file_layout,
            child_detached_root,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::state_machine::tests::*;
    use beryl_types::fs::{InodeData, InodeKind};

    fn new_state_machine() -> (TempDir, Arc<RocksDBStorage>, AppRaftStateMachine) {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let state_machine = AppRaftStateMachine::new(Arc::clone(&storage));
        expect_mount_upserted(state_machine.apply(bootstrap_command("root", 1)).unwrap());
        (dir, storage, state_machine)
    }

    fn detached_root(mount_id: MountId, detached_at_ms: u64) -> DetachedRoot {
        DetachedRoot {
            mount_id,
            detached_at_ms,
        }
    }

    fn seed_directory(storage: &RocksDBStorage, inode_id: InodeId, mount_id: MountId) {
        storage
            .put_inode(&Inode::new_dir(inode_id, FileAttrs::new(), mount_id))
            .unwrap();
    }

    fn seed_file(storage: &RocksDBStorage, parent_inode_id: InodeId, name: &str, inode_id: InodeId, mount_id: MountId) {
        storage
            .put_inode(&Inode::new_file(inode_id, FileAttrs::new(), mount_id))
            .unwrap();
        storage.put_dentry(parent_inode_id, name, inode_id).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
    }

    fn reclaim(
        state_machine: &AppRaftStateMachine,
        candidates: Vec<InodeId>,
        max_entries: u32,
    ) -> MetadataResult<DetachedRootReclaimResult> {
        reclaim_with_budget(
            state_machine,
            candidates,
            max_entries,
            MAX_RECLAIM_DETACHED_ROOT_BATCH_BYTES,
        )
    }

    fn reclaim_with_budget(
        state_machine: &AppRaftStateMachine,
        candidates: Vec<InodeId>,
        max_entries: u32,
        max_batch_bytes: u32,
    ) -> MetadataResult<DetachedRootReclaimResult> {
        match state_machine.apply(Command::ReclaimDetachedRoots {
            candidate_root_inode_ids: candidates,
            max_entries,
            max_batch_bytes,
        })? {
            ApplySuccess::DetachedRootsReclaimed(result) => Ok(result),
            other => panic!("unexpected reclaim response: {other:?}"),
        }
    }

    #[test]
    fn mixed_tree_reclaims_in_bounded_batches_and_inherits_detach_age() {
        let (_dir, storage, state_machine) = new_state_machine();
        let root_id = InodeId::new(10);
        let child_dir_id = InodeId::new(11);
        let file_id = InodeId::new(12);
        let symlink_id = InodeId::new(13);
        let marker = detached_root(MountId::new(1), 77);
        seed_directory(&storage, root_id, marker.mount_id);
        seed_directory(&storage, child_dir_id, marker.mount_id);
        storage.put_dentry(root_id, "a", child_dir_id).unwrap();
        seed_file(&storage, root_id, "b", file_id, marker.mount_id);
        storage
            .put_inode(&Inode::new_symlink(
                symlink_id,
                FileAttrs::new(),
                "target".to_string(),
                marker.mount_id,
            ))
            .unwrap();
        storage.put_dentry(root_id, "c", symlink_id).unwrap();
        storage.put_detached_root(root_id, marker).unwrap();

        let first = reclaim(&state_machine, vec![root_id], 2).unwrap();
        assert_eq!(first.processed_entries, 2);
        assert_eq!(first.created_roots, 1);
        assert_eq!(first.completed_roots, 0);
        assert!(first.logical_batch_bytes <= MAX_RECLAIM_DETACHED_ROOT_BATCH_BYTES);
        assert_eq!(storage.get_detached_root(child_dir_id).unwrap(), Some(marker));
        assert!(storage.get_inode(file_id).unwrap().is_none());
        assert!(storage.get_layout_optional(file_id).unwrap().is_none());
        assert!(storage.get_inode(root_id).unwrap().is_some());

        let second = reclaim(&state_machine, vec![root_id], 2).unwrap();
        assert_eq!(second.processed_entries, 1);
        assert_eq!(second.completed_roots, 1);
        assert!(storage.get_inode(root_id).unwrap().is_none());
        assert!(storage.get_detached_root(root_id).unwrap().is_none());
        assert!(storage.get_inode(symlink_id).unwrap().is_none());

        let third = reclaim(&state_machine, vec![child_dir_id], 2).unwrap();
        assert_eq!(third.processed_entries, 0);
        assert_eq!(third.completed_roots, 1);
        assert!(storage.get_inode(child_dir_id).unwrap().is_none());

        let replay = reclaim(&state_machine, vec![root_id, child_dir_id, root_id], 2).unwrap();
        assert_eq!(replay.processed_entries, 0);
        assert_eq!(replay.completed_roots, 0);
    }

    #[test]
    fn later_cross_mount_error_keeps_the_whole_batch_unmodified() {
        let (_dir, storage, state_machine) = new_state_machine();
        let root_id = InodeId::new(20);
        let valid_file_id = InodeId::new(21);
        let cross_mount_dir_id = InodeId::new(22);
        let marker = detached_root(MountId::new(1), 88);
        seed_directory(&storage, root_id, marker.mount_id);
        seed_file(&storage, root_id, "a", valid_file_id, marker.mount_id);
        seed_directory(&storage, cross_mount_dir_id, MountId::new(2));
        storage.put_dentry(root_id, "b", cross_mount_dir_id).unwrap();
        storage.put_detached_root(root_id, marker).unwrap();

        let error = reclaim(&state_machine, vec![root_id], 10).unwrap_err();

        assert!(error.to_string().contains("crosses from mount"));
        assert!(storage.get_inode(valid_file_id).unwrap().is_some());
        assert_eq!(storage.get_dentry(root_id, "a").unwrap(), Some(valid_file_id));
        assert_eq!(storage.get_detached_root(root_id).unwrap(), Some(marker));
    }

    #[test]
    fn missing_child_and_missing_file_layout_fail_closed_without_partial_delete() {
        let (_dir, storage, state_machine) = new_state_machine();
        let missing_root_id = InodeId::new(30);
        let missing_child_id = InodeId::new(31);
        let marker = detached_root(MountId::new(1), 99);
        seed_directory(&storage, missing_root_id, marker.mount_id);
        storage
            .put_dentry(missing_root_id, "missing", missing_child_id)
            .unwrap();
        storage.put_detached_root(missing_root_id, marker).unwrap();

        let missing_error = reclaim(&state_machine, vec![missing_root_id], 10).unwrap_err();
        assert!(missing_error.to_string().contains("missing child inode"));
        assert_eq!(
            storage.get_dentry(missing_root_id, "missing").unwrap(),
            Some(missing_child_id)
        );

        let layout_root_id = InodeId::new(40);
        let file_id = InodeId::new(41);
        seed_directory(&storage, layout_root_id, marker.mount_id);
        storage
            .put_inode(&Inode::new_file(file_id, FileAttrs::new(), marker.mount_id))
            .unwrap();
        storage.put_dentry(layout_root_id, "file", file_id).unwrap();
        storage.put_detached_root(layout_root_id, marker).unwrap();

        let layout_error = reclaim(&state_machine, vec![layout_root_id], 10).unwrap_err();
        assert!(layout_error.to_string().contains("has no file layout"));
        assert!(storage.get_inode(file_id).unwrap().is_some());
        assert_eq!(storage.get_dentry(layout_root_id, "file").unwrap(), Some(file_id));
    }

    #[test]
    fn kind_payload_mismatch_keeps_entire_reclaim_batch_unmodified() {
        let (_dir, storage, state_machine) = new_state_machine();
        let root_inode_id = InodeId::new(43);
        let valid_file_inode_id = InodeId::new(44);
        let corrupt_inode_id = InodeId::new(45);
        let grandchild_inode_id = InodeId::new(46);
        let marker = detached_root(MountId::new(1), 101);
        let layout = FileLayout::new(4096, 4096, 1);
        seed_directory(&storage, root_inode_id, marker.mount_id);
        seed_file(&storage, root_inode_id, "a-valid", valid_file_inode_id, marker.mount_id);
        let mut corrupt_inode = Inode::new_file(corrupt_inode_id, FileAttrs::new(), marker.mount_id);
        corrupt_inode.kind = InodeKind::Dir;
        storage.put_inode(&corrupt_inode).unwrap();
        storage
            .put_dentry(root_inode_id, "b-corrupt", corrupt_inode_id)
            .unwrap();
        storage.put_layout(corrupt_inode_id, layout).unwrap();
        seed_directory(&storage, grandchild_inode_id, marker.mount_id);
        storage
            .put_dentry(corrupt_inode_id, "grandchild", grandchild_inode_id)
            .unwrap();
        storage.put_detached_root(root_inode_id, marker).unwrap();
        let applied_before = storage.load_raft_state().unwrap();
        let rejected_applied_state = AppMetadataRaftState {
            last_applied_log_id: Some(openraft::LogId::new(openraft::LeaderId::new(7, 1), 702)),
            ..AppMetadataRaftState::default()
        };
        assert_ne!(rejected_applied_state, applied_before);

        let error = state_machine
            .apply_with_raft_state(
                Command::ReclaimDetachedRoots {
                    candidate_root_inode_ids: vec![root_inode_id],
                    max_entries: 10,
                    max_batch_bytes: MAX_RECLAIM_DETACHED_ROOT_BATCH_BYTES,
                },
                &rejected_applied_state,
            )
            .unwrap_err();

        assert!(error.to_string().contains("kind/payload mismatch"));
        assert_eq!(
            storage.get_dentry(root_inode_id, "a-valid").unwrap(),
            Some(valid_file_inode_id)
        );
        assert_eq!(
            storage.get_dentry(root_inode_id, "b-corrupt").unwrap(),
            Some(corrupt_inode_id)
        );
        assert_eq!(
            storage.get_dentry(corrupt_inode_id, "grandchild").unwrap(),
            Some(grandchild_inode_id)
        );
        assert!(storage.get_inode(root_inode_id).unwrap().is_some());
        assert!(storage.get_inode(valid_file_inode_id).unwrap().is_some());
        assert_eq!(storage.get_inode(corrupt_inode_id).unwrap(), Some(corrupt_inode));
        assert!(storage.get_inode(grandchild_inode_id).unwrap().is_some());
        assert_eq!(storage.get_layout(valid_file_inode_id).unwrap(), layout);
        assert_eq!(storage.get_layout(corrupt_inode_id).unwrap(), layout);
        assert_eq!(storage.get_detached_root(root_inode_id).unwrap(), Some(marker));
        assert!(storage.get_detached_root(corrupt_inode_id).unwrap().is_none());
        assert_eq!(storage.load_raft_state().unwrap(), applied_before);
    }

    #[test]
    fn root_and_child_identity_or_kind_mismatches_fail_closed() {
        let (_dir, storage, state_machine) = new_state_machine();
        let marker = detached_root(MountId::new(1), 102);

        let root_key_inode_id = InodeId::new(47);
        let embedded_root_inode_id = InodeId::new(48);
        let corrupt_root = Inode::new_dir(embedded_root_inode_id, FileAttrs::new(), marker.mount_id);
        storage
            .put_inode_at_storage_key(root_key_inode_id, &corrupt_root)
            .unwrap();
        storage.put_detached_root(root_key_inode_id, marker).unwrap();
        let identity_applied_before = storage.load_raft_state().unwrap();

        let root_identity_error = reclaim(&state_machine, vec![root_key_inode_id], 10).unwrap_err();

        assert!(root_identity_error.to_string().contains("inode key"));
        assert_eq!(storage.get_inode(root_key_inode_id).unwrap(), Some(corrupt_root));
        assert_eq!(storage.get_detached_root(root_key_inode_id).unwrap(), Some(marker));
        assert_eq!(storage.load_raft_state().unwrap(), identity_applied_before);

        let kind_mismatch_root_inode_id = InodeId::new(49);
        let mut kind_mismatch_root = Inode::new_dir(kind_mismatch_root_inode_id, FileAttrs::new(), marker.mount_id);
        kind_mismatch_root.kind = InodeKind::File;
        storage.put_inode(&kind_mismatch_root).unwrap();
        storage.put_detached_root(kind_mismatch_root_inode_id, marker).unwrap();
        let kind_applied_before = storage.load_raft_state().unwrap();

        let root_kind_error = reclaim(&state_machine, vec![kind_mismatch_root_inode_id], 10).unwrap_err();

        assert!(root_kind_error.to_string().contains("kind and payload disagree"));
        assert_eq!(
            storage.get_inode(kind_mismatch_root_inode_id).unwrap(),
            Some(kind_mismatch_root)
        );
        assert_eq!(
            storage.get_detached_root(kind_mismatch_root_inode_id).unwrap(),
            Some(marker)
        );
        assert_eq!(storage.load_raft_state().unwrap(), kind_applied_before);

        let child_root_inode_id = InodeId::new(50);
        let child_key_inode_id = InodeId::new(51);
        let embedded_child_inode_id = InodeId::new(52);
        let corrupt_child = Inode::new_symlink(
            embedded_child_inode_id,
            FileAttrs::new(),
            "target".to_string(),
            marker.mount_id,
        );
        seed_directory(&storage, child_root_inode_id, marker.mount_id);
        storage
            .put_inode_at_storage_key(child_key_inode_id, &corrupt_child)
            .unwrap();
        storage
            .put_dentry(child_root_inode_id, "child", child_key_inode_id)
            .unwrap();
        storage.put_detached_root(child_root_inode_id, marker).unwrap();
        let child_applied_before = storage.load_raft_state().unwrap();

        let child_identity_error = reclaim(&state_machine, vec![child_root_inode_id], 10).unwrap_err();

        assert!(child_identity_error.to_string().contains("inode key"));
        assert_eq!(
            storage.get_dentry(child_root_inode_id, "child").unwrap(),
            Some(child_key_inode_id)
        );
        assert_eq!(storage.get_inode(child_key_inode_id).unwrap(), Some(corrupt_child));
        assert_eq!(storage.get_detached_root(child_root_inode_id).unwrap(), Some(marker));
        assert!(storage.get_detached_root(child_key_inode_id).unwrap().is_none());
        assert_eq!(storage.load_raft_state().unwrap(), child_applied_before);
    }

    #[test]
    fn reopen_resumes_from_deleted_dentries_without_a_cursor() {
        let dir = TempDir::new().unwrap();
        let root_id = InodeId::new(50);
        {
            let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
            let state_machine = AppRaftStateMachine::new(Arc::clone(&storage));
            expect_mount_upserted(state_machine.apply(bootstrap_command("root", 1)).unwrap());
            let marker = detached_root(MountId::new(1), 111);
            seed_directory(&storage, root_id, marker.mount_id);
            seed_file(&storage, root_id, "a", InodeId::new(51), marker.mount_id);
            seed_file(&storage, root_id, "b", InodeId::new(52), marker.mount_id);
            storage.put_detached_root(root_id, marker).unwrap();
            let first = reclaim(&state_machine, vec![root_id], 1).unwrap();
            assert_eq!(first.processed_entries, 1);
        }

        let storage = Arc::new(RocksDBStorage::open_existing_for_start(dir.path()).unwrap());
        let state_machine = AppRaftStateMachine::new(Arc::clone(&storage));
        let second = reclaim(&state_machine, vec![root_id], 1).unwrap();
        assert_eq!(second.processed_entries, 1);
        assert_eq!(second.completed_roots, 1);
        assert!(storage.get_detached_root(root_id).unwrap().is_none());
        assert!(storage.get_inode(root_id).unwrap().is_none());
    }

    #[test]
    fn protocol_limits_are_rejected_before_authority_changes() {
        let (_dir, storage, state_machine) = new_state_machine();
        let root_id = InodeId::new(60);
        let marker = detached_root(MountId::new(1), 123);
        seed_directory(&storage, root_id, marker.mount_id);
        storage.put_detached_root(root_id, marker).unwrap();

        let error = state_machine
            .apply(Command::ReclaimDetachedRoots {
                candidate_root_inode_ids: vec![root_id],
                max_entries: MAX_RECLAIM_DETACHED_ROOT_ENTRIES + 1,
                max_batch_bytes: MAX_RECLAIM_DETACHED_ROOT_BATCH_BYTES,
            })
            .unwrap_err();

        assert!(error.to_string().contains("invalid detached-root entry budget"));
        assert_eq!(storage.get_detached_root(root_id).unwrap(), Some(marker));
        assert!(matches!(
            storage.get_inode(root_id).unwrap().unwrap().data,
            InodeData::Dir
        ));
    }

    #[test]
    fn missing_or_non_directory_root_marker_fails_closed() {
        let (_dir, storage, state_machine) = new_state_machine();
        let marker = detached_root(MountId::new(1), 234);
        let missing_root_id = InodeId::new(70);
        storage.put_detached_root(missing_root_id, marker).unwrap();

        let missing_error = reclaim(&state_machine, vec![missing_root_id], 1).unwrap_err();
        assert!(missing_error.to_string().contains("has no directory inode"));
        assert_eq!(storage.get_detached_root(missing_root_id).unwrap(), Some(marker));

        let file_root_id = InodeId::new(71);
        storage
            .put_inode(&Inode::new_file(file_root_id, FileAttrs::new(), marker.mount_id))
            .unwrap();
        storage
            .put_layout(file_root_id, FileLayout::new(4096, 4096, 1))
            .unwrap();
        storage.put_detached_root(file_root_id, marker).unwrap();

        let type_error = reclaim(&state_machine, vec![file_root_id], 1).unwrap_err();
        assert!(type_error.to_string().contains("is not a directory"));
        assert!(storage.get_inode(file_root_id).unwrap().is_some());
        assert_eq!(storage.get_detached_root(file_root_id).unwrap(), Some(marker));
    }

    #[test]
    fn byte_budget_stops_before_the_entry_budget_without_partial_overrun() {
        let (_dir, storage, state_machine) = new_state_machine();
        let root_id = InodeId::new(80);
        let marker = detached_root(MountId::new(1), 345);
        seed_directory(&storage, root_id, marker.mount_id);
        for index in 0..64u64 {
            let inode_id = InodeId::new(81 + index);
            let name = format!("{index:03}-{}", "x".repeat(96));
            storage
                .put_inode(&Inode::new_symlink(
                    inode_id,
                    FileAttrs::new(),
                    "target".to_string(),
                    marker.mount_id,
                ))
                .unwrap();
            storage.put_dentry(root_id, &name, inode_id).unwrap();
        }
        storage.put_detached_root(root_id, marker).unwrap();

        let result = reclaim_with_budget(
            &state_machine,
            vec![root_id],
            MAX_RECLAIM_DETACHED_ROOT_ENTRIES,
            MIN_RECLAIM_DETACHED_ROOT_BATCH_BYTES,
        )
        .unwrap();

        assert!(result.processed_entries > 0);
        assert!(result.processed_entries < 64);
        assert!(result.logical_batch_bytes <= MIN_RECLAIM_DETACHED_ROOT_BATCH_BYTES);
        assert!(storage.get_detached_root(root_id).unwrap().is_some());
        let (remaining, _, eof) = storage.list_dentries_with_cursor(root_id, None, 64).unwrap();
        assert!(eof);
        assert_eq!(remaining.len(), 64 - result.processed_entries as usize);
    }

    #[test]
    fn hundred_thousand_empty_roots_are_selected_and_reclaimed_by_bounded_candidate_batch() {
        let (_dir, storage, state_machine) = new_state_machine();
        let marker = detached_root(MountId::new(1), 456);
        storage.put_empty_detached_roots(10_000, 100_000, marker).unwrap();
        let (candidates, has_more) = storage
            .list_detached_roots(MAX_RECLAIM_DETACHED_ROOT_CANDIDATES as usize)
            .unwrap();
        assert_eq!(candidates.len(), MAX_RECLAIM_DETACHED_ROOT_CANDIDATES as usize);
        assert!(has_more);

        let result = reclaim(
            &state_machine,
            candidates.into_iter().map(|(inode_id, _)| inode_id).collect(),
            MAX_RECLAIM_DETACHED_ROOT_ENTRIES,
        )
        .unwrap();

        assert_eq!(result.processed_entries, 0);
        assert_eq!(result.completed_roots, MAX_RECLAIM_DETACHED_ROOT_CANDIDATES);
        assert!(result.logical_batch_bytes <= MAX_RECLAIM_DETACHED_ROOT_BATCH_BYTES);
        let (next_candidates, still_has_more) = storage
            .list_detached_roots(MAX_RECLAIM_DETACHED_ROOT_CANDIDATES as usize)
            .unwrap();
        assert_eq!(next_candidates.len(), MAX_RECLAIM_DETACHED_ROOT_CANDIDATES as usize);
        assert!(still_has_more);
    }
}
