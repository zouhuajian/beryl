// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use super::*;

impl AppRaftStateMachine {
    /// Apply CloseWrite command.
    // Raft apply helpers mirror command payload fields for replay clarity.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_close_write(
        &self,
        inode_id: InodeId,
        extents: Vec<types::fs::Extent>,
        final_size: u64,
        lease_id: types::ids::LeaseId,
        open_epoch: u64,
        lease_epoch: u64,
        commit_mode: FileCommitMode,
        proposed_at_ms: u64,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<FsCommandResult> {
        let prepared: MetadataResult<PreparedCloseWrite> = (|| {
            let mut inode = self
                .storage
                .get_inode(inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {}", inode_id)))?;

            if !inode.kind.is_file() {
                return Err(MetadataError::InvalidArgument(format!(
                    "Inode is not a file: {}",
                    inode_id
                )));
            }

            let expected_data_handle_id = inode.current_data_handle_id;
            if expected_data_handle_id.as_raw() == 0 {
                return Err(MetadataError::Internal(format!(
                    "File inode {} is missing current_data_handle_id",
                    inode_id
                )));
            }

            // lease_id/open_epoch are part of the command fingerprint and replay
            // identity, but the Raft apply layer has no authoritative runtime
            // write-session table after restart. FsCore validates the live session
            // before proposing; apply can only persist the lease_epoch carried here.
            let _ = (lease_id, open_epoch);

            let layout = self.storage.get_layout(inode_id)?;
            let now_ms = Self::mutation_timestamp(&inode, proposed_at_ms);

            let old_size = inode.attrs.size;
            let (existing_extents_snapshot, current_file_version) = match &inode.data {
                InodeData::File {
                    extents, file_version, ..
                } => (extents.clone(), *file_version),
                _ => {
                    return Err(MetadataError::InvalidArgument(format!(
                        "Inode data is not File: {}",
                        inode_id
                    )));
                }
            };

            let mut committed_block_ids = std::collections::HashSet::with_capacity(extents.len());
            let file_version = Self::next_file_version(inode_id, current_file_version)?;
            let mut ordered_extents = extents;
            ordered_extents.sort_by_key(|extent| (extent.file_offset, extent.block_id.index.as_raw()));
            let mut previous_end = None;
            let mut max_committed_end = 0;

            for extent in &ordered_extents {
                if extent.len == 0 {
                    return Err(MetadataError::InvalidArgument(
                        "Committed extent len must be greater than 0".to_string(),
                    ));
                }
                if extent.block_id.data_handle_id != expected_data_handle_id {
                    return Err(MetadataError::InvalidArgument(format!(
                        "Extent block data_handle_id {} does not match inode {} current_data_handle_id {}",
                        extent.block_id.data_handle_id, inode_id, expected_data_handle_id
                    )));
                }
                if !committed_block_ids.insert(extent.block_id) {
                    return Err(MetadataError::InvalidArgument(format!(
                        "Committed block {} was submitted more than once",
                        extent.block_id
                    )));
                }
                let extent_end = extent.file_offset.checked_add(extent.len).ok_or_else(|| {
                    MetadataError::InvalidArgument(format!(
                        "Extent end overflows: file_offset={}, len={}",
                        extent.file_offset, extent.len
                    ))
                })?;
                if previous_end.map(|prev| extent.file_offset < prev).unwrap_or(false) {
                    return Err(MetadataError::InvalidArgument(
                        "Committed extents must not overlap".to_string(),
                    ));
                }
                if extent_end > final_size {
                    return Err(MetadataError::InvalidArgument(format!(
                        "Extent extends beyond final_size: extent_end={}, final_size={}",
                        extent_end, final_size
                    )));
                }
                previous_end = Some(extent_end);
                max_committed_end = max_committed_end.max(extent_end);
            }

            let mut extents_to_publish = ordered_extents.clone();
            match commit_mode {
                FileCommitMode::Replace => {
                    if ordered_extents.is_empty() && final_size != 0 {
                        return Err(MetadataError::InvalidArgument(format!(
                            "Empty replace commit cannot publish nonzero final_size={}",
                            final_size
                        )));
                    }
                    if final_size < max_committed_end {
                        return Err(MetadataError::InvalidArgument(format!(
                            "Replace final_size {} is smaller than committed end {}",
                            final_size, max_committed_end
                        )));
                    }
                }
                FileCommitMode::Append => {
                    extents_to_publish = Self::append_extents_not_already_visible(
                        &existing_extents_snapshot,
                        &ordered_extents,
                        old_size,
                        final_size,
                        "Append",
                    )?;
                }
            }

            // Update inode: publish extents and update size/mtime/ctime/file_version/lease_epoch.
            Self::stamp_extents(&mut extents_to_publish, &existing_extents_snapshot, file_version);
            match &mut inode.data {
                types::fs::InodeData::File {
                    extents: existing_extents,
                    file_version: stored_file_version,
                    lease_epoch: stored_lease_epoch,
                    ..
                } => {
                    match commit_mode {
                        FileCommitMode::Replace => {
                            *existing_extents = extents_to_publish.clone();
                        }
                        FileCommitMode::Append => {
                            existing_extents.extend(extents_to_publish.clone());
                        }
                    }
                    for extent in existing_extents.iter_mut() {
                        extent.file_version = Some(file_version);
                    }
                    *stored_file_version = Some(file_version);
                    // Update lease_epoch (persisted for fencing after restart)
                    *stored_lease_epoch = Some(lease_epoch);
                }
                _ => {
                    return Err(MetadataError::InvalidArgument(format!(
                        "Inode data is not File: {}",
                        inode_id
                    )));
                }
            }

            // Update file size and timestamps
            inode.attrs.size = final_size;
            inode.attrs.update_mtime_ctime(now_ms);

            Ok((
                inode,
                layout,
                FsOkResult {
                    inode_id: Some(inode_id),
                    data_handle_id: Some(expected_data_handle_id),
                    file_version: Some(file_version),
                },
            ))
        })();

        let (inode, layout, ok) = match prepared {
            Ok(prepared) => prepared,
            Err(err) => return self.persist_fs_error(err, dedup_key, fingerprint, raft_state),
        };
        let result = FsCommandResult::Ok(ok);
        let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::Fs(result.clone()));
        self.storage
            .close_write_with_apply_result_atomic(&inode, layout, dedup_key, applied_result, raft_state)?;
        Ok(result)
    }

    #[allow(clippy::too_many_arguments)]
    /// Apply a SyncWrite command by publishing a prefix while leaving the write session open.
    pub(super) fn apply_sync_write(
        &self,
        inode_id: InodeId,
        extents: Vec<Extent>,
        target_size: u64,
        lease_id: types::ids::LeaseId,
        open_epoch: u64,
        lease_epoch: u64,
        commit_mode: FileCommitMode,
        proposed_at_ms: u64,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<(FsCommandResult, bool)> {
        let prepared: MetadataResult<PreparedSyncWrite> = (|| {
            let mut inode = self
                .storage
                .get_inode(inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {}", inode_id)))?;

            if !inode.kind.is_file() {
                return Err(MetadataError::InvalidArgument(format!(
                    "Inode is not a file: {}",
                    inode_id
                )));
            }

            let expected_data_handle_id = inode.current_data_handle_id;
            if expected_data_handle_id.as_raw() == 0 {
                return Err(MetadataError::Internal(format!(
                    "File inode {} is missing current_data_handle_id",
                    inode_id
                )));
            }

            // FsCore validates the live runtime session before proposing. The
            // Raft command carries lease identity so dedup replay still rejects
            // calls that reuse a call_id for a different write barrier.
            let _ = (lease_id, open_epoch);

            let layout = self.storage.get_layout(inode_id)?;
            let now_ms = Self::mutation_timestamp(&inode, proposed_at_ms);
            let current_size = inode.attrs.size;
            let (existing_extents_snapshot, current_file_version) = match &inode.data {
                InodeData::File {
                    extents, file_version, ..
                } => (extents.clone(), *file_version),
                _ => {
                    return Err(MetadataError::InvalidArgument(format!(
                        "Inode data is not File: {}",
                        inode_id
                    )));
                }
            };

            let mut committed_block_ids = std::collections::HashSet::with_capacity(extents.len());
            let mut ordered_extents = extents;
            ordered_extents.sort_by_key(|extent| (extent.file_offset, extent.block_id.index.as_raw()));
            let mut previous_end = None;
            for extent in &ordered_extents {
                if extent.len == 0 {
                    return Err(MetadataError::InvalidArgument(
                        "Committed extent len must be greater than 0".to_string(),
                    ));
                }
                if extent.block_id.data_handle_id != expected_data_handle_id {
                    return Err(MetadataError::InvalidArgument(format!(
                        "Extent block data_handle_id {} does not match inode {} current_data_handle_id {}",
                        extent.block_id.data_handle_id, inode_id, expected_data_handle_id
                    )));
                }
                if !committed_block_ids.insert(extent.block_id) {
                    return Err(MetadataError::InvalidArgument(format!(
                        "Committed block {} was submitted more than once",
                        extent.block_id
                    )));
                }
                let extent_end = extent.file_offset.checked_add(extent.len).ok_or_else(|| {
                    MetadataError::InvalidArgument(format!(
                        "Extent end overflows: file_offset={}, len={}",
                        extent.file_offset, extent.len
                    ))
                })?;
                if previous_end.map(|prev| extent.file_offset < prev).unwrap_or(false) {
                    return Err(MetadataError::InvalidArgument(
                        "Committed extents must not overlap".to_string(),
                    ));
                }
                if extent_end > target_size {
                    return Err(MetadataError::InvalidArgument(format!(
                        "Extent extends beyond target_size: extent_end={}, target_size={}",
                        extent_end, target_size
                    )));
                }
                previous_end = Some(extent_end);
            }

            if target_size <= current_size {
                Self::validate_noop_sync_prefix(&existing_extents_snapshot, &ordered_extents, target_size)?;
                return Ok((
                    inode,
                    layout,
                    FsOkResult {
                        inode_id: Some(inode_id),
                        data_handle_id: Some(expected_data_handle_id),
                        file_version: current_file_version,
                    },
                    false,
                ));
            }

            let extents_to_publish = match commit_mode {
                FileCommitMode::Replace => {
                    Self::validate_contiguous_extents(&ordered_extents, 0, target_size, "SyncWrite replace")?;
                    ordered_extents.clone()
                }
                FileCommitMode::Append => Self::append_extents_not_already_visible(
                    &existing_extents_snapshot,
                    &ordered_extents,
                    current_size,
                    target_size,
                    "SyncWrite append",
                )?,
            };

            let file_version = Self::next_file_version(inode_id, current_file_version)?;
            let mut stamped_extents = extents_to_publish;
            Self::stamp_extents(&mut stamped_extents, &existing_extents_snapshot, file_version);

            match &mut inode.data {
                InodeData::File {
                    extents: existing_extents,
                    file_version: stored_file_version,
                    lease_epoch: stored_lease_epoch,
                    ..
                } => {
                    match commit_mode {
                        FileCommitMode::Replace => {
                            *existing_extents = stamped_extents.clone();
                        }
                        FileCommitMode::Append => {
                            existing_extents.extend(stamped_extents.clone());
                        }
                    }
                    for extent in existing_extents.iter_mut() {
                        extent.file_version = Some(file_version);
                    }
                    *stored_file_version = Some(file_version);
                    *stored_lease_epoch = Some(lease_epoch);
                }
                _ => {
                    return Err(MetadataError::InvalidArgument(format!(
                        "Inode data is not File: {}",
                        inode_id
                    )));
                }
            }
            inode.attrs.size = target_size;
            inode.attrs.update_mtime_ctime(now_ms);

            Ok((
                inode,
                layout,
                FsOkResult {
                    inode_id: Some(inode_id),
                    data_handle_id: Some(expected_data_handle_id),
                    file_version: Some(file_version),
                },
                true,
            ))
        })();

        let (inode, layout, ok, mutates_metadata) = match prepared {
            Ok(prepared) => prepared,
            Err(err) => {
                return self
                    .persist_fs_error(err, dedup_key, fingerprint, raft_state)
                    .map(|result| (result, false));
            }
        };
        let result = FsCommandResult::Ok(ok);
        let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::Fs(result.clone()));
        if !mutates_metadata {
            self.storage
                .put_apply_result_atomic(dedup_key, applied_result, raft_state)?;
        } else {
            self.storage
                .close_write_with_apply_result_atomic(&inode, layout, dedup_key, applied_result, raft_state)?;
        }
        Ok((result, mutates_metadata))
    }

    /// Apply Truncate command.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_truncate(
        &self,
        inode_id: InodeId,
        new_size: u64,
        lease_id: types::ids::LeaseId,
        lease_epoch: u64,
        proposed_at_ms: u64,
        dedup_key: &DedupKey,
        fingerprint: CommandFingerprint,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<FsCommandResult> {
        let prepared: MetadataResult<(Inode, FileLayout, FsOkResult)> = (|| {
            // Get inode
            let mut inode = self
                .storage
                .get_inode(inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {}", inode_id)))?;

            if !inode.kind.is_file() {
                return Err(MetadataError::InvalidArgument(format!(
                    "Inode is not a file: {}",
                    inode_id
                )));
            }

            let (stored_lease_epoch, current_file_version) = match &inode.data {
                types::fs::InodeData::File {
                    lease_epoch,
                    file_version,
                    ..
                } => (*lease_epoch, *file_version),
                _ => (None, None),
            };
            Self::validate_truncate_lease(inode_id, stored_lease_epoch, lease_id, lease_epoch)?;

            let current_size = inode.attrs.size;
            if new_size > current_size {
                return Err(MetadataError::NotSupported(format!(
                    "Truncate grow not supported: current_size={}, new_size={}",
                    current_size, new_size
                )));
            }

            if new_size == current_size {
                return Ok((inode, self.storage.get_layout(inode_id)?, FsOkResult::default()));
            }

            let now_ms = Self::mutation_timestamp(&inode, proposed_at_ms);
            let layout = self.storage.get_layout(inode_id)?;
            let data_handle_id = inode.current_data_handle_id;
            if data_handle_id.as_raw() == 0 {
                return Err(MetadataError::Internal(format!(
                    "File inode {} is missing current_data_handle_id",
                    inode_id
                )));
            }
            self.storage
                .validate_data_handle_owner(data_handle_id, Some(inode_id))?;

            let next_file_version = Self::next_file_version(inode_id, current_file_version)?;
            match &mut inode.data {
                types::fs::InodeData::File {
                    extents,
                    file_version: stored_file_version,
                    lease_epoch: stored_lease_epoch,
                    ..
                } => {
                    let new_extents = Self::truncate_layout_to_size(inode_id, data_handle_id, extents, new_size)?;
                    *extents = new_extents;
                    for extent in extents.iter_mut() {
                        extent.file_version = Some(next_file_version);
                    }
                    *stored_file_version = Some(next_file_version);
                    *stored_lease_epoch = Some(lease_epoch);
                }
                _ => {
                    return Err(MetadataError::InvalidArgument(format!(
                        "Inode data is not File: {}",
                        inode_id
                    )));
                }
            }

            // Update file size and timestamps
            inode.attrs.size = new_size;
            inode.attrs.update_mtime_ctime(now_ms);

            Ok((
                inode,
                layout,
                FsOkResult {
                    inode_id: Some(inode_id),
                    data_handle_id: Some(data_handle_id),
                    file_version: Some(next_file_version),
                },
            ))
        })();

        let (inode, layout, ok) = match prepared {
            Ok(prepared) => prepared,
            Err(err) => return self.persist_fs_error(err, dedup_key, fingerprint, raft_state),
        };
        let result = FsCommandResult::Ok(ok);
        let applied_result = Self::make_applied_result(fingerprint, AppDataResponse::Fs(result.clone()));
        self.storage
            .truncate_file_with_apply_result_atomic(&inode, layout, dedup_key, applied_result, raft_state)?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::state_machine::tests::*;

    #[test]
    fn truncate_shrink_within_extent_updates_inode_layout_applied_result_and_replay_result() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let data_handle_id = DataHandleId::new(91);
        let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        let inode_id = InodeId::new(910);
        install_file_with_extents(
            &storage,
            InodeId::new(909),
            "file",
            inode_id,
            data_handle_id,
            vec![extent(block_id, 0, 1024)],
            1024,
        );

        let dedup = dedup_for_test(91);
        expect_fs_ok(
            sm.apply(Command::new(
                dedup.clone(),
                crate::raft::proposal_timestamp_ms(),
                crate::raft::Mutation::Truncate {
                    inode_id,
                    new_size: 512,
                    lease_id: lease_id_for_inode_epoch(inode_id, 2),
                    lease_epoch: 2,
                },
            ))
            .unwrap(),
        );

        let inode = storage.get_inode(inode_id).unwrap().unwrap();
        assert_eq!(inode.attrs.size, 512);
        match inode.data {
            InodeData::File {
                extents,
                lease_epoch,
                file_version,
            } => {
                let mut expected_extent = extent(block_id, 0, 512);
                expected_extent.file_version = Some(1);
                assert_eq!(extents, vec![expected_extent]);
                assert_eq!(file_version, Some(1));
                assert_eq!(lease_epoch, Some(2));
            }
            other => panic!("unexpected inode data: {:?}", other),
        }
        assert_eq!(storage.get_layout(inode_id).unwrap(), FileLayout::new(4096, 4096, 1));
        assert!(storage.get_applied_result(&dedup).unwrap().is_some());
    }

    #[test]
    fn truncate_drops_full_blocks_and_replay_is_stable() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let data_handle_id = DataHandleId::new(92);
        let kept_block = BlockId::new(data_handle_id, BlockIndex::new(0));
        let dropped_block = BlockId::new(data_handle_id, BlockIndex::new(1));
        let inode_id = InodeId::new(920);
        install_file_with_extents(
            &storage,
            InodeId::new(919),
            "file",
            inode_id,
            data_handle_id,
            vec![extent(kept_block, 0, 4096), extent(dropped_block, 4096, 4096)],
            8192,
        );

        let dedup = dedup_for_test(92);
        let command = Command::new(
            dedup.clone(),
            crate::raft::proposal_timestamp_ms(),
            crate::raft::Mutation::Truncate {
                inode_id,
                new_size: 4096,
                lease_id: lease_id_for_inode_epoch(inode_id, 2),
                lease_epoch: 2,
            },
        );

        expect_fs_ok(sm.apply(command.clone()).unwrap());

        expect_fs_ok(sm.apply(command).unwrap());
    }

    #[test]
    fn truncate_same_block_keeps_remaining_extent() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let data_handle_id = DataHandleId::new(925);
        let shared_block = BlockId::new(data_handle_id, BlockIndex::new(0));
        let inode_id = InodeId::new(9250);
        install_file_with_extents(
            &storage,
            InodeId::new(9249),
            "file",
            inode_id,
            data_handle_id,
            vec![extent(shared_block, 0, 4096), extent(shared_block, 4096, 4096)],
            8192,
        );

        expect_fs_ok(
            sm.apply(Command::new(
                dedup_for_test(925),
                crate::raft::proposal_timestamp_ms(),
                crate::raft::Mutation::Truncate {
                    inode_id,
                    new_size: 4096,
                    lease_id: lease_id_for_inode_epoch(inode_id, 2),
                    lease_epoch: 2,
                },
            ))
            .unwrap(),
        );

        let inode = storage.get_inode(inode_id).unwrap().unwrap();
        assert_eq!(inode.attrs.size, 4096);
        match inode.data {
            InodeData::File { extents, .. } => {
                let mut expected_extent = extent(shared_block, 0, 4096);
                expected_extent.file_version = Some(1);
                assert_eq!(extents, vec![expected_extent]);
            }
            other => panic!("unexpected inode data: {:?}", other),
        }
        assert_eq!(storage.get_layout(inode_id).unwrap(), FileLayout::new(4096, 4096, 1));
    }

    #[test]
    fn truncate_rejects_data_handle_mismatch_without_half_commit() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let inode_data_handle = DataHandleId::new(93);
        let wrong_data_handle = DataHandleId::new(94);
        let mismatched_block = BlockId::new(wrong_data_handle, BlockIndex::new(0));
        let inode_id = InodeId::new(930);
        install_file_with_extents(
            &storage,
            InodeId::new(929),
            "file",
            inode_id,
            inode_data_handle,
            vec![extent(mismatched_block, 0, 4096)],
            4096,
        );

        expect_fs_errno(
            sm.apply(Command::new(
                dedup_for_test(93),
                crate::raft::proposal_timestamp_ms(),
                crate::raft::Mutation::Truncate {
                    inode_id,
                    new_size: 0,
                    lease_id: lease_id_for_inode_epoch(inode_id, 2),
                    lease_epoch: 2,
                },
            ))
            .unwrap(),
            FsErrorCode::EInval,
        );
        assert_eq!(storage.get_inode(inode_id).unwrap().unwrap().attrs.size, 4096);
    }

    #[test]
    fn truncate_grow_remains_not_supported_and_same_size_is_stable_noop() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let data_handle_id = DataHandleId::new(95);
        let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        let inode_id = InodeId::new(950);
        install_file_with_extents(
            &storage,
            InodeId::new(949),
            "file",
            inode_id,
            data_handle_id,
            vec![extent(block_id, 0, 1024)],
            1024,
        );

        expect_fs_errno(
            sm.apply(Command::new(
                dedup_for_test(95),
                crate::raft::proposal_timestamp_ms(),
                crate::raft::Mutation::Truncate {
                    inode_id,
                    new_size: 2048,
                    lease_id: lease_id_for_inode_epoch(inode_id, 2),
                    lease_epoch: 2,
                },
            ))
            .unwrap(),
            FsErrorCode::ENotsup,
        );
        assert_eq!(storage.get_inode(inode_id).unwrap().unwrap().attrs.size, 1024);

        let dedup = dedup_for_test(96);
        expect_fs_ok(
            sm.apply(Command::new(
                dedup.clone(),
                crate::raft::proposal_timestamp_ms(),
                crate::raft::Mutation::Truncate {
                    inode_id,
                    new_size: 1024,
                    lease_id: lease_id_for_inode_epoch(inode_id, 2),
                    lease_epoch: 2,
                },
            ))
            .unwrap(),
        );
        assert!(storage.get_applied_result(&dedup).unwrap().is_some());

        let mismatch = sm.apply(Command::new(
            dedup,
            crate::raft::proposal_timestamp_ms(),
            crate::raft::Mutation::Truncate {
                inode_id,
                new_size: 512,
                lease_id: lease_id_for_inode_epoch(inode_id, 2),
                lease_epoch: 2,
            },
        ));
        assert!(matches!(mismatch, Err(MetadataError::InvalidArgument(_))));
    }

    #[test]
    fn truncate_rejects_invalid_lease_identity_and_epoch_without_half_commit() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let data_handle_id = DataHandleId::new(958);
        let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        let inode_id = InodeId::new(9580);
        install_file_with_extents(
            &storage,
            InodeId::new(9579),
            "file",
            inode_id,
            data_handle_id,
            vec![extent(block_id, 0, 4096)],
            4096,
        );

        expect_fs_errno(
            sm.apply(Command::new(
                dedup_for_test(958),
                crate::raft::proposal_timestamp_ms(),
                crate::raft::Mutation::Truncate {
                    inode_id,
                    new_size: 0,
                    lease_id: types::ids::LeaseId::new(1),
                    lease_epoch: 2,
                },
            ))
            .unwrap(),
            FsErrorCode::EAcces,
        );
        assert_eq!(storage.get_inode(inode_id).unwrap().unwrap().attrs.size, 4096);

        expect_fs_errno(
            sm.apply(Command::new(
                dedup_for_test(959),
                crate::raft::proposal_timestamp_ms(),
                crate::raft::Mutation::Truncate {
                    inode_id,
                    new_size: 0,
                    lease_id: lease_id_for_inode_epoch(inode_id, 1),
                    lease_epoch: 1,
                },
            ))
            .unwrap(),
            FsErrorCode::EAcces,
        );
        assert_eq!(storage.get_inode(inode_id).unwrap().unwrap().attrs.size, 4096);
    }

    #[test]
    fn close_write_extents_must_use_inode_data_handle() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let inode_id = InodeId::new(7);
        let data_handle_id = DataHandleId::new(99);
        let inode = Inode::new_file(inode_id, FileAttrs::new(), MountId::new(1), data_handle_id);
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        sm.apply(Command::new(
            dedup_for_test(34),
            crate::raft::proposal_timestamp_ms(),
            crate::raft::Mutation::CloseWrite {
                inode_id,
                extents: vec![types::fs::Extent {
                    file_offset: 0,
                    block_id,
                    block_offset: 0,
                    len: 11,
                    file_version: None,
                    block_stamp: None,
                }],
                final_size: 11,
                lease_id: types::ids::LeaseId::new(1),
                open_epoch: 1,
                lease_epoch: 1,
                commit_mode: FileCommitMode::Append,
            },
        ))
        .unwrap();
        let updated = storage.get_inode(inode_id).unwrap().unwrap();
        match updated.data {
            types::fs::InodeData::File { extents, .. } => {
                assert_eq!(extents[0].block_id.data_handle_id, data_handle_id)
            }
            other => panic!("unexpected inode data: {:?}", other),
        }

        let mismatch = sm.apply(Command::new(
            dedup_for_test(35),
            crate::raft::proposal_timestamp_ms(),
            crate::raft::Mutation::CloseWrite {
                inode_id,
                extents: vec![types::fs::Extent {
                    file_offset: 11,
                    // Intentional invalid fixture: extents must use inode.current_data_handle_id.
                    block_id: BlockId::new(DataHandleId::new(inode_id.as_raw()), BlockIndex::new(1)),
                    block_offset: 0,
                    len: 1,
                    file_version: None,
                    block_stamp: None,
                }],
                final_size: 12,
                lease_id: types::ids::LeaseId::new(1),
                open_epoch: 1,
                lease_epoch: 2,
                commit_mode: FileCommitMode::Append,
            },
        ));
        assert!(matches!(
            mismatch,
            Ok(AppDataResponse::Fs(FsCommandResult::Err(FsErrnoResult {
                errno: FsErrorCode::EInval,
                ..
            })))
        ));
    }

    #[test]
    fn close_write_preserves_visible_block_stamp_for_already_published_extent() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let parent_inode_id = InodeId::new(910);
        let inode_id = InodeId::new(911);
        let data_handle_id = DataHandleId::new(1911);
        let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        let mut visible_extent = extent(block_id, 0, 24);
        visible_extent.file_version = Some(1);
        visible_extent.block_stamp = Some(1);
        let mut inode = install_file_with_extents(
            &storage,
            parent_inode_id,
            "file",
            inode_id,
            data_handle_id,
            vec![visible_extent],
            24,
        );
        if let InodeData::File { file_version, .. } = &mut inode.data {
            *file_version = Some(1);
        }
        storage.put_inode(&inode).unwrap();

        expect_fs_ok(
            sm.apply(Command::new(
                dedup_for_test(191),
                crate::raft::proposal_timestamp_ms(),
                crate::raft::Mutation::CloseWrite {
                    inode_id,
                    extents: vec![extent(block_id, 0, 24)],
                    final_size: 24,
                    lease_id: lease_id_for_inode_epoch(inode_id, 1),
                    open_epoch: 1,
                    lease_epoch: 1,
                    commit_mode: FileCommitMode::Replace,
                },
            ))
            .unwrap(),
        );

        let updated = storage.get_inode(inode_id).unwrap().unwrap();
        match updated.data {
            InodeData::File {
                extents, file_version, ..
            } => {
                assert_eq!(file_version, Some(2));
                assert_eq!(extents.len(), 1);
                assert_eq!(extents[0].file_version, Some(2));
                assert_eq!(extents[0].block_stamp, Some(1));
            }
            other => panic!("unexpected inode data: {:?}", other),
        }
    }

    #[test]
    fn apply_rejects_duplicate_blocks() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let inode_id = InodeId::new(73);
        let data_handle_id = DataHandleId::new(173);
        let inode = Inode::new_file(inode_id, FileAttrs::new(), MountId::new(1), data_handle_id);
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        expect_fs_errno(
            sm.apply(close_write_command(
                dedup_for_test(93),
                inode_id,
                vec![
                    close_extent(data_handle_id, 0, 0, 64),
                    close_extent(data_handle_id, 0, 64, 64),
                ],
                128,
                FileCommitMode::Append,
            ))
            .unwrap(),
            FsErrorCode::EInval,
        );
        assert_eq!(storage.get_inode(inode_id).unwrap().unwrap(), inode);
    }

    #[test]
    fn apply_rejects_overlapping_ranges() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let inode_id = InodeId::new(74);
        let data_handle_id = DataHandleId::new(174);
        let inode = Inode::new_file(inode_id, FileAttrs::new(), MountId::new(1), data_handle_id);
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        expect_fs_errno(
            sm.apply(close_write_command(
                dedup_for_test(94),
                inode_id,
                vec![
                    close_extent(data_handle_id, 0, 0, 64),
                    close_extent(data_handle_id, 1, 32, 64),
                ],
                96,
                FileCommitMode::Append,
            ))
            .unwrap(),
            FsErrorCode::EInval,
        );
        assert_eq!(storage.get_inode(inode_id).unwrap().unwrap(), inode);
    }

    #[test]
    fn apply_rejects_zero_length_block() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let inode_id = InodeId::new(75);
        let data_handle_id = DataHandleId::new(175);
        let inode = Inode::new_file(inode_id, FileAttrs::new(), MountId::new(1), data_handle_id);
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        expect_fs_errno(
            sm.apply(close_write_command(
                dedup_for_test(95),
                inode_id,
                vec![close_extent(data_handle_id, 0, 0, 0)],
                0,
                FileCommitMode::Append,
            ))
            .unwrap(),
            FsErrorCode::EInval,
        );
        assert_eq!(storage.get_inode(inode_id).unwrap().unwrap(), inode);
    }

    #[test]
    fn apply_rejects_bad_final_size() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let inode_id = InodeId::new(76);
        let data_handle_id = DataHandleId::new(176);
        let old_extent = close_extent(data_handle_id, 0, 0, 64);
        let mut attrs = FileAttrs::new();
        attrs.size = 64;
        let mut inode = Inode::new_file(inode_id, attrs, MountId::new(1), data_handle_id);
        if let InodeData::File { extents, .. } = &mut inode.data {
            extents.push(old_extent);
        }
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        expect_fs_errno(
            sm.apply(close_write_command(
                dedup_for_test(96),
                inode_id,
                vec![close_extent(data_handle_id, 1, 64, 64)],
                200,
                FileCommitMode::Append,
            ))
            .unwrap(),
            FsErrorCode::EInval,
        );
        assert_eq!(storage.get_inode(inode_id).unwrap().unwrap(), inode);
    }

    #[test]
    fn apply_replace_removes_old_layout() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let inode_id = InodeId::new(77);
        let data_handle_id = DataHandleId::new(177);
        let new_block_id = BlockId::new(data_handle_id, BlockIndex::new(1));
        let mut attrs = FileAttrs::new();
        attrs.size = 64;
        let mut inode = Inode::new_file(inode_id, attrs, MountId::new(1), data_handle_id);
        if let InodeData::File { extents, .. } = &mut inode.data {
            extents.push(close_extent(data_handle_id, 0, 0, 64));
        }
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        expect_fs_ok(
            sm.apply(close_write_command(
                dedup_for_test(97),
                inode_id,
                vec![close_extent(data_handle_id, 1, 0, 32)],
                32,
                FileCommitMode::Replace,
            ))
            .unwrap(),
        );

        let updated = storage.get_inode(inode_id).unwrap().unwrap();
        match updated.data {
            InodeData::File { extents, .. } => {
                assert_eq!(extents.len(), 1);
                assert_eq!(extents[0].block_id, new_block_id);
            }
            other => panic!("unexpected inode data: {:?}", other),
        }
    }

    #[test]
    fn apply_append_keeps_old_layout() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let inode_id = InodeId::new(78);
        let data_handle_id = DataHandleId::new(178);
        let old_block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        let new_block_id = BlockId::new(data_handle_id, BlockIndex::new(1));
        let mut attrs = FileAttrs::new();
        attrs.size = 64;
        let mut inode = Inode::new_file(inode_id, attrs, MountId::new(1), data_handle_id);
        if let InodeData::File { extents, .. } = &mut inode.data {
            extents.push(close_extent(data_handle_id, 0, 0, 64));
        }
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        expect_fs_ok(
            sm.apply(close_write_command(
                dedup_for_test(98),
                inode_id,
                vec![close_extent(data_handle_id, 1, 64, 32)],
                96,
                FileCommitMode::Append,
            ))
            .unwrap(),
        );

        let updated = storage.get_inode(inode_id).unwrap().unwrap();
        match updated.data {
            InodeData::File { extents, .. } => {
                assert_eq!(extents.len(), 2);
                assert_eq!(extents[0].block_id, old_block_id);
                assert_eq!(extents[1].block_id, new_block_id);
            }
            other => panic!("unexpected inode data: {:?}", other),
        }
    }

    #[test]
    fn apply_rejects_append_offset_not_current_size() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let inode_id = InodeId::new(79);
        let data_handle_id = DataHandleId::new(179);
        let old_extent = close_extent(data_handle_id, 0, 0, 64);
        let mut attrs = FileAttrs::new();
        attrs.size = 64;
        let mut inode = Inode::new_file(inode_id, attrs, MountId::new(1), data_handle_id);
        if let InodeData::File { extents, .. } = &mut inode.data {
            extents.push(old_extent);
        }
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        expect_fs_errno(
            sm.apply(close_write_command(
                dedup_for_test(99),
                inode_id,
                vec![close_extent(data_handle_id, 1, 32, 32)],
                64,
                FileCommitMode::Append,
            ))
            .unwrap(),
            FsErrorCode::EInval,
        );
        assert_eq!(storage.get_inode(inode_id).unwrap().unwrap(), inode);
    }

    #[test]
    fn dedup_rejects_commit_mode_mismatch() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let inode_id = InodeId::new(80);
        let data_handle_id = DataHandleId::new(180);
        let inode = Inode::new_file(inode_id, FileAttrs::new(), MountId::new(1), data_handle_id);
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        let dedup = dedup_for_test(100);
        let extents = vec![close_extent(data_handle_id, 0, 0, 64)];
        expect_fs_ok(
            sm.apply(close_write_command(
                dedup.clone(),
                inode_id,
                extents.clone(),
                64,
                FileCommitMode::Replace,
            ))
            .unwrap(),
        );
        let mismatch = sm
            .apply(close_write_command(
                dedup,
                inode_id,
                extents,
                64,
                FileCommitMode::Append,
            ))
            .expect_err("same call_id with different commit mode must be rejected");
        assert!(matches!(mismatch, MetadataError::InvalidArgument(_)));
    }

    #[test]
    fn apply_replace_replaces_extents() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let inode_id = InodeId::new(70);
        let data_handle_id = DataHandleId::new(1700);
        let old_block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        let new_block_id = BlockId::new(data_handle_id, BlockIndex::new(1));
        let mut inode = Inode::new_file(inode_id, FileAttrs::new(), MountId::new(1), data_handle_id);
        inode.attrs.size = 64;
        if let types::fs::InodeData::File { extents, .. } = &mut inode.data {
            extents.push(types::fs::Extent {
                file_offset: 0,
                block_id: old_block_id,
                block_offset: 0,
                len: 64,
                file_version: None,
                block_stamp: None,
            });
        }
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        expect_fs_ok(
            sm.apply(Command::new(
                dedup_for_test(36),
                crate::raft::proposal_timestamp_ms(),
                crate::raft::Mutation::CloseWrite {
                    inode_id,
                    extents: vec![types::fs::Extent {
                        file_offset: 0,
                        block_id: new_block_id,
                        block_offset: 0,
                        len: 32,
                        file_version: None,
                        block_stamp: None,
                    }],
                    final_size: 32,
                    lease_id: types::ids::LeaseId::new(1),
                    open_epoch: 1,
                    lease_epoch: 2,
                    commit_mode: FileCommitMode::Replace,
                },
            ))
            .unwrap(),
        );

        let updated = storage.get_inode(inode_id).unwrap().unwrap();
        match updated.data {
            types::fs::InodeData::File { extents, .. } => {
                assert_eq!(extents.len(), 1);
                assert_eq!(extents[0].block_id, new_block_id);
            }
            other => panic!("unexpected inode data: {:?}", other),
        }
    }

    #[test]
    fn close_write_success_replay_returns_original_result_without_reapplying_mutation() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let inode_id = InodeId::new(70);
        let data_handle_id = DataHandleId::new(170);
        let inode = Inode::new_file(inode_id, FileAttrs::new(), MountId::new(1), data_handle_id);
        let layout = FileLayout::new(4096, 4096, 1);
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, layout).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        let dedup = dedup_for_test(90);
        let command = Command::new(
            dedup.clone(),
            crate::raft::proposal_timestamp_ms(),
            crate::raft::Mutation::CloseWrite {
                inode_id,
                extents: vec![types::fs::Extent {
                    file_offset: 0,
                    block_id,
                    block_offset: 0,
                    len: 64,
                    file_version: None,
                    block_stamp: None,
                }],
                final_size: 64,
                lease_id: types::ids::LeaseId::new(1),
                open_epoch: 1,
                lease_epoch: 3,
                commit_mode: FileCommitMode::Append,
            },
        );

        expect_fs_ok(sm.apply(command.clone()).unwrap());
        let first_inode = storage.get_inode(inode_id).unwrap().unwrap();
        let first_result = match storage.get_applied_result(&dedup).unwrap().unwrap().result {
            AppDataResponse::Fs(result) => result,
            other => panic!("unexpected applied result: {:?}", other),
        };

        expect_fs_ok(sm.apply(command).unwrap());
        let replayed_inode = storage.get_inode(inode_id).unwrap().unwrap();
        let replayed_result = match storage.get_applied_result(&dedup).unwrap().unwrap().result {
            AppDataResponse::Fs(result) => result,
            other => panic!("unexpected applied result: {:?}", other),
        };

        assert_eq!(first_result, replayed_result);
        assert_eq!(replayed_inode, first_inode);
        assert_eq!(storage.get_layout(inode_id).unwrap(), layout);
    }

    #[test]
    fn close_write_extent_data_handle_mismatch_persists_error_without_half_commit() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let inode_id = InodeId::new(71);
        let data_handle_id = DataHandleId::new(171);
        let inode = Inode::new_file(inode_id, FileAttrs::new(), MountId::new(1), data_handle_id);
        let layout = FileLayout::new(4096, 4096, 1);
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, layout).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        let bad_block_id = BlockId::new(DataHandleId::new(999), BlockIndex::new(0));
        let command = Command::new(
            dedup_for_test(91),
            crate::raft::proposal_timestamp_ms(),
            crate::raft::Mutation::CloseWrite {
                inode_id,
                extents: vec![types::fs::Extent {
                    file_offset: 0,
                    block_id: bad_block_id,
                    block_offset: 0,
                    len: 64,
                    file_version: None,
                    block_stamp: None,
                }],
                final_size: 64,
                lease_id: types::ids::LeaseId::new(1),
                open_epoch: 1,
                lease_epoch: 3,
                commit_mode: FileCommitMode::Append,
            },
        );

        expect_fs_errno(sm.apply(command.clone()).unwrap(), FsErrorCode::EInval);
        expect_fs_errno(sm.apply(command).unwrap(), FsErrorCode::EInval);

        let stored = storage.get_inode(inode_id).unwrap().unwrap();
        assert_eq!(stored, inode);
        assert_eq!(storage.get_layout(inode_id).unwrap(), layout);
    }

    #[test]
    fn close_write_fingerprint_mismatch_does_not_reapply_mutation() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let inode_id = InodeId::new(72);
        let data_handle_id = DataHandleId::new(172);
        let inode = Inode::new_file(inode_id, FileAttrs::new(), MountId::new(1), data_handle_id);
        let layout = FileLayout::new(4096, 4096, 1);
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, layout).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        let dedup = dedup_for_test(92);
        let first_block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        let first = Command::new(
            dedup.clone(),
            crate::raft::proposal_timestamp_ms(),
            crate::raft::Mutation::CloseWrite {
                inode_id,
                extents: vec![types::fs::Extent {
                    file_offset: 0,
                    block_id: first_block_id,
                    block_offset: 0,
                    len: 64,
                    file_version: None,
                    block_stamp: None,
                }],
                final_size: 64,
                lease_id: types::ids::LeaseId::new(1),
                open_epoch: 1,
                lease_epoch: 3,
                commit_mode: FileCommitMode::Append,
            },
        );
        let second_block_id = BlockId::new(data_handle_id, BlockIndex::new(1));
        let mismatch = Command::new(
            dedup,
            crate::raft::proposal_timestamp_ms(),
            crate::raft::Mutation::CloseWrite {
                inode_id,
                extents: vec![types::fs::Extent {
                    file_offset: 64,
                    block_id: second_block_id,
                    block_offset: 0,
                    len: 64,
                    file_version: None,
                    block_stamp: None,
                }],
                final_size: 128,
                lease_id: types::ids::LeaseId::new(1),
                open_epoch: 1,
                lease_epoch: 3,
                commit_mode: FileCommitMode::Append,
            },
        );

        expect_fs_ok(sm.apply(first).unwrap());
        let first_inode = storage.get_inode(inode_id).unwrap().unwrap();
        let err = sm.apply(mismatch).unwrap_err();

        assert!(matches!(err, MetadataError::InvalidArgument(_)));
        assert_eq!(storage.get_inode(inode_id).unwrap().unwrap(), first_inode);
    }

    #[test]
    fn dedup_fingerprint_mismatch_does_not_apply_mutation_or_reapply_mutation() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));

        let parent_inode_id = InodeId::new(10);
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), MountId::new(1)))
            .unwrap();

        let dedup = dedup_for_test(45);
        let first = Command::new(
            dedup.clone(),
            crate::raft::proposal_timestamp_ms(),
            crate::raft::Mutation::CreateFile {
                parent_inode_id,
                name: "first".to_string(),
                attrs: FileAttrs::new(),
                layout: FileLayout::new(4096, 4096, 1),
            },
        );
        let mismatch = Command::new(
            dedup,
            crate::raft::proposal_timestamp_ms(),
            crate::raft::Mutation::CreateFile {
                parent_inode_id,
                name: "second".to_string(),
                attrs: FileAttrs::new(),
                layout: FileLayout::new(4096, 4096, 1),
            },
        );

        sm.apply(first).unwrap();
        let err = sm.apply(mismatch).unwrap_err();

        assert!(matches!(err, MetadataError::InvalidArgument(_)));
        assert_eq!(storage.get_dentry(parent_inode_id, "second").unwrap(), None);
    }
}
