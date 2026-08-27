// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use super::*;

impl AppRaftStateMachine {
    fn ensure_file_inode_authority(inode_id: InodeId, inode: &Inode) -> MetadataResult<()> {
        if inode.inode_id != inode_id || !inode.kind.is_file() || !matches!(&inode.data, InodeData::File { .. }) {
            return Err(MetadataError::Internal(format!(
                "inode authority is corrupt for file mutation: key={inode_id}, value_id={}, kind={:?}, payload={:?}",
                inode.inode_id,
                inode.kind,
                inode.data.kind()
            )));
        }
        Ok(())
    }

    pub(super) fn apply_allocate_block(
        &self,
        inode_id: InodeId,
        lease_epoch: u64,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<BlockId> {
        let mut inode = self
            .storage
            .get_inode(inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {inode_id}")))?;
        Self::ensure_file_inode_authority(inode_id, &inode)?;
        let next_block_index = match &mut inode.data {
            InodeData::File {
                lease_epoch: stored_lease_epoch,
                next_block_index,
                ..
            } => {
                let current_lease_epoch = stored_lease_epoch.unwrap_or(0);
                if current_lease_epoch != lease_epoch {
                    return Err(MetadataError::LeaseFenced {
                        expected: current_lease_epoch,
                        got: lease_epoch,
                    });
                }
                let allocated = *next_block_index;
                *next_block_index = allocated.checked_add(1).ok_or_else(|| {
                    MetadataError::InvalidArgument(format!("block index overflow for inode {inode_id}"))
                })?;
                allocated
            }
            _ => {
                return Err(MetadataError::InvalidArgument(format!(
                    "Inode is not a file: {inode_id}"
                )))
            }
        };
        let block_index = u32::try_from(next_block_index)
            .map_err(|_| MetadataError::InvalidArgument(format!("block index exhausted for inode {inode_id}")))?;
        let block_id = BlockId::new(inode_id, BlockIndex::new(block_index));
        self.storage.put_inode_atomic(&inode, raft_state)?;
        Ok(block_id)
    }

    /// Advance durable write-lease authority from the caller's exact expected epoch.
    pub(super) fn apply_acquire_write_lease(
        &self,
        inode_id: InodeId,
        expected_lease_epoch: u64,
        proposed_at_ms: u64,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<u64> {
        let prepared: MetadataResult<(Inode, u64)> = (|| {
            let mut inode = self
                .storage
                .get_inode(inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {inode_id}")))?;
            Self::ensure_file_inode_authority(inode_id, &inode)?;
            if let Some(record) = self.storage.get_create_file_replay_for_inode(inode_id)? {
                let InodeData::File {
                    extents,
                    content_revision,
                    lease_epoch,
                    next_block_index,
                } = &inode.data
                else {
                    unreachable!("file authority checked above")
                };
                let still_initial = extents.is_empty()
                    && content_revision.unwrap_or_default() == record.content_revision
                    && *lease_epoch == Some(record.lease_epoch)
                    && *next_block_index == 0
                    && inode.attrs.size == 0;
                if record.expires_at_ms > proposed_at_ms && still_initial {
                    return Err(MetadataError::Again(format!(
                        "CreateFile replay still owns the initial write session for inode {inode_id}"
                    )));
                }
            }
            let lease_epoch = match &mut inode.data {
                InodeData::File { lease_epoch, .. } => {
                    let current = lease_epoch.unwrap_or(0);
                    if current != expected_lease_epoch {
                        return Err(MetadataError::Again(format!(
                            "write lease epoch changed for inode {inode_id}: expected {expected_lease_epoch}, current {current}"
                        )));
                    }
                    let next = current.checked_add(1).ok_or_else(|| {
                        MetadataError::InvalidArgument(format!("write lease epoch overflow for inode {inode_id}"))
                    })?;
                    *lease_epoch = Some(next);
                    next
                }
                _ => {
                    return Err(MetadataError::InvalidArgument(format!(
                        "Inode is not a file: {inode_id}"
                    )))
                }
            };
            Ok((inode, lease_epoch))
        })();

        let (inode, lease_epoch) = prepared?;
        self.storage.put_inode_atomic(&inode, raft_state)?;
        Ok(lease_epoch)
    }

    /// End one exact lease epoch, accepting replay only at its immediate successor.
    pub(super) fn apply_end_write_lease(
        &self,
        inode_id: InodeId,
        lease_epoch: u64,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<u64> {
        let prepared: MetadataResult<(Option<Inode>, u64)> = (|| {
            let mut inode = self
                .storage
                .get_inode(inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {inode_id}")))?;
            Self::ensure_file_inode_authority(inode_id, &inode)?;
            let next = lease_epoch.checked_add(1).ok_or_else(|| {
                MetadataError::InvalidArgument(format!("write lease epoch overflow for inode {inode_id}"))
            })?;
            let current = match &mut inode.data {
                InodeData::File { lease_epoch, .. } => lease_epoch.unwrap_or(0),
                _ => {
                    return Err(MetadataError::InvalidArgument(format!(
                        "Inode is not a file: {inode_id}"
                    )))
                }
            };
            if current == next {
                return Ok((None, current));
            }
            if current != lease_epoch {
                return Err(MetadataError::LeaseFenced {
                    expected: current,
                    got: lease_epoch,
                });
            }
            let InodeData::File {
                lease_epoch: stored_epoch,
                ..
            } = &mut inode.data
            else {
                unreachable!("file checked above")
            };
            *stored_epoch = Some(next);
            Ok((Some(inode), next))
        })();

        let (inode, ended_epoch) = prepared?;
        if let Some(inode) = inode {
            self.storage.put_inode_atomic(&inode, raft_state)?;
        } else {
            self.storage.commit_applied_state(raft_state)?;
        }
        Ok(ended_epoch)
    }

    /// Publish one fenced file version or confirm an exact idempotent replay.
    ///
    /// The returned revision is always the durable revision visible after the
    /// command. Both mutation and replay paths commit the supplied applied index.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_publish_file(
        &self,
        inode_id: InodeId,
        mut requested_extents: Vec<Extent>,
        target_size: u64,
        expected_content_revision: u64,
        expected_file_size: u64,
        lease_epoch: u64,
        mode: PublishMode,
        proposed_at_ms: u64,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<u64> {
        let prepared: MetadataResult<(Inode, FileLayout, u64, bool)> = (|| {
            let mut inode = self
                .storage
                .get_inode(inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {inode_id}")))?;
            Self::ensure_file_inode_authority(inode_id, &inode)?;
            let layout = self.storage.get_layout(inode_id)?;
            requested_extents.sort_by_key(|extent| (extent.file_offset, extent.block_id.index.as_raw()));

            let (existing_extents, current_content_revision, stored_lease_epoch) = match &inode.data {
                InodeData::File {
                    extents,
                    content_revision,
                    lease_epoch,
                    ..
                } => {
                    if extents.len() > MAX_FILE_EXTENTS {
                        return Err(MetadataError::ResourceExhausted(format!(
                            "stored file extent count {} exceeds maximum {} for inode {}",
                            extents.len(),
                            MAX_FILE_EXTENTS,
                            inode_id
                        )));
                    }
                    (extents.clone(), content_revision.unwrap_or(0), lease_epoch.unwrap_or(0))
                }
                _ => unreachable!("file inode must carry file data"),
            };
            if stored_lease_epoch != lease_epoch {
                return Err(MetadataError::LeaseFenced {
                    expected: stored_lease_epoch,
                    got: lease_epoch,
                });
            }
            if current_content_revision == expected_content_revision && inode.attrs.size != expected_file_size {
                return Err(MetadataError::Again(format!(
                    "file size changed for inode {inode_id}: expected {expected_file_size}, current {}",
                    inode.attrs.size
                )));
            }

            let mut seen = std::collections::HashSet::with_capacity(requested_extents.len());
            let block_capacity = u64::from(layout.block_size);
            for extent in &requested_extents {
                if extent.len == 0 {
                    return Err(MetadataError::InvalidArgument(
                        "Committed extent len must be greater than 0".to_string(),
                    ));
                }
                if extent.block_id.inode_id != inode_id {
                    return Err(MetadataError::InvalidArgument(format!(
                        "Extent block inode_id {} does not match inode {inode_id}",
                        extent.block_id.inode_id
                    )));
                }
                if !seen.insert(extent.block_id) {
                    return Err(MetadataError::InvalidArgument(format!(
                        "Committed block {} was submitted more than once",
                        extent.block_id
                    )));
                }
                if extent.block_offset != 0 {
                    return Err(MetadataError::InvalidArgument(format!(
                        "Committed block {} must start at block offset 0",
                        extent.block_id
                    )));
                }
                if extent.len > block_capacity {
                    return Err(MetadataError::InvalidArgument(format!(
                        "Committed block {} length {} exceeds layout block capacity {}",
                        extent.block_id, extent.len, block_capacity
                    )));
                }
                if Self::extent_end(extent)? > target_size {
                    return Err(MetadataError::InvalidArgument(format!(
                        "Extent extends beyond target_size {target_size}: {}",
                        extent.block_id
                    )));
                }
            }

            // Only the last block introduced by one publication may be partial.
            // Exact visible extents are excluded so a later SyncWrite can append
            // after a partial block already made durable by an earlier command.
            let newly_visible = requested_extents
                .iter()
                .filter(|extent| !Self::extent_matches_visible(&existing_extents, extent))
                .collect::<Vec<_>>();
            for extent in newly_visible.iter().take(newly_visible.len().saturating_sub(1)) {
                if extent.len != block_capacity {
                    return Err(MetadataError::InvalidArgument(format!(
                        "non-tail committed block {} must use full layout capacity {}",
                        extent.block_id, block_capacity
                    )));
                }
            }

            let state_matches = inode.attrs.size == target_size
                && match mode {
                    PublishMode::ReplaceIfUnchanged => {
                        existing_extents.len() == requested_extents.len()
                            && requested_extents
                                .iter()
                                .all(|extent| Self::extent_matches_visible(&existing_extents, extent))
                    }
                    PublishMode::AppendIfUnchanged => {
                        requested_extents
                            .iter()
                            .all(|extent| Self::extent_matches_visible(&existing_extents, extent))
                            && Self::visible_suffix_matches(
                                &existing_extents,
                                &requested_extents,
                                expected_file_size,
                                target_size,
                            )
                    }
                };
            if expected_content_revision.checked_add(1) == Some(current_content_revision) && state_matches {
                return Ok((inode, layout, current_content_revision, false));
            }
            if current_content_revision != expected_content_revision {
                return Err(MetadataError::Again(format!(
                    "content revision changed for inode {inode_id}: expected {expected_content_revision}, current {current_content_revision}"
                )));
            }
            if state_matches {
                return Ok((inode, layout, current_content_revision, false));
            }

            let mut extents_to_publish = match mode {
                PublishMode::ReplaceIfUnchanged => {
                    Self::validate_contiguous_extents(&requested_extents, 0, target_size, "ReplaceIfUnchanged")?;
                    requested_extents
                }
                PublishMode::AppendIfUnchanged => {
                    if target_size < inode.attrs.size {
                        return Err(MetadataError::InvalidArgument(format!(
                            "AppendIfUnchanged target_size {target_size} is smaller than current size {}",
                            inode.attrs.size
                        )));
                    }
                    Self::append_extents_not_already_visible(
                        &existing_extents,
                        &requested_extents,
                        inode.attrs.size,
                        target_size,
                        "AppendIfUnchanged",
                    )?
                }
            };
            let final_extent_count = match mode {
                PublishMode::ReplaceIfUnchanged => extents_to_publish.len(),
                PublishMode::AppendIfUnchanged => existing_extents
                    .len()
                    .checked_add(extents_to_publish.len())
                    .ok_or_else(|| {
                        MetadataError::ResourceExhausted("final file extent count overflowed".to_string())
                    })?,
            };
            if final_extent_count > MAX_FILE_EXTENTS {
                return Err(MetadataError::ResourceExhausted(format!(
                    "final file extent count {final_extent_count} exceeds maximum {MAX_FILE_EXTENTS} for inode {inode_id}"
                )));
            }
            let content_revision = Self::next_content_revision(inode_id, Some(current_content_revision))?;
            Self::stamp_extents(&mut extents_to_publish, &existing_extents, content_revision);
            match &mut inode.data {
                InodeData::File {
                    extents,
                    content_revision: stored_content_revision,
                    ..
                } => {
                    match mode {
                        PublishMode::ReplaceIfUnchanged => *extents = extents_to_publish,
                        PublishMode::AppendIfUnchanged => extents.extend(extents_to_publish),
                    }
                    for extent in extents.iter_mut() {
                        extent.content_revision = Some(content_revision);
                    }
                    *stored_content_revision = Some(content_revision);
                }
                _ => unreachable!("file inode must carry file data"),
            }
            inode.attrs.size = target_size;
            inode
                .attrs
                .update_mtime_ctime(Self::mutation_timestamp(&inode, proposed_at_ms));
            Ok((inode, layout, content_revision, true))
        })();

        let (inode, layout, content_revision, changed) = prepared?;
        if changed {
            self.storage.publish_file_atomic(&inode, layout, raft_state)?;
        } else {
            self.storage.commit_applied_state(raft_state)?;
        }
        Ok(content_revision)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::response::ApplyRejectionKind;
    use crate::raft::state_machine::tests::*;
    use beryl_types::InodeKind;

    fn expect_block_allocated(result: ApplySuccess) -> BlockId {
        match result {
            ApplySuccess::BlockAllocated(block_id) => block_id,
            other => panic!("unexpected apply response: {other:?}"),
        }
    }

    fn assert_file_mutations_reject_corrupt_inode(
        storage: Arc<RocksDBStorage>,
        inode_id: InodeId,
        expected_inode: &Inode,
    ) {
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));
        let applied_before = storage.load_raft_state().unwrap();
        let rejected_applied_state = AppMetadataRaftState {
            last_applied_log_id: Some(openraft::LogId::new(openraft::LeaderId::new(7, 1), 703)),
            ..AppMetadataRaftState::default()
        };
        assert_ne!(rejected_applied_state, applied_before);
        let commands = [
            Command::AllocateBlock {
                inode_id,
                lease_epoch: 1,
            },
            Command::AcquireWriteLease {
                proposed_at_ms: 1,
                inode_id,
                expected_lease_epoch: 1,
            },
            Command::EndWriteLease {
                proposed_at_ms: 1,
                inode_id,
                lease_epoch: 1,
            },
            Command::PublishFile {
                proposed_at_ms: 1,
                inode_id,
                extents: Vec::new(),
                target_size: 0,
                expected_content_revision: 0,
                expected_file_size: 0,
                lease_epoch: 1,
                mode: PublishMode::ReplaceIfUnchanged,
            },
        ];

        for command in commands {
            let error = sm.apply_with_raft_state(command, &rejected_applied_state).unwrap_err();
            assert!(error.to_string().contains("inode authority is corrupt"));
            assert_eq!(storage.load_raft_state().unwrap(), applied_before);
            assert_eq!(storage.get_inode(inode_id).unwrap().as_ref(), Some(expected_inode));
        }
    }

    #[test]
    fn file_mutations_reject_corrupt_inode_authority_without_advancing_apply_state() {
        let kind_dir = TempDir::new().unwrap();
        let kind_storage = Arc::new(RocksDBStorage::create_for_format(kind_dir.path()).unwrap());
        let kind_inode_id = InodeId::new(108);
        let mut kind_mismatch =
            install_file_with_extents(&kind_storage, InodeId::new(100), "file", kind_inode_id, Vec::new(), 0);
        kind_mismatch.kind = InodeKind::Dir;
        kind_storage.put_inode(&kind_mismatch).unwrap();
        assert_file_mutations_reject_corrupt_inode(kind_storage, kind_inode_id, &kind_mismatch);

        let key_dir = TempDir::new().unwrap();
        let key_storage = Arc::new(RocksDBStorage::create_for_format(key_dir.path()).unwrap());
        let key_inode_id = InodeId::new(109);
        let key_mismatch = Inode::new_file(InodeId::new(110), FileAttrs::new(), MountId::new(1));
        key_storage
            .put_inode_at_storage_key(key_inode_id, &key_mismatch)
            .unwrap();
        assert_file_mutations_reject_corrupt_inode(key_storage, key_inode_id, &key_mismatch);
    }

    #[test]
    fn block_allocation_is_durable_and_lease_fencing_does_not_consume_an_index() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let inode_id = InodeId::new(106);
        install_file_with_extents(&storage, InodeId::new(100), "file", inode_id, Vec::new(), 0);
        let allocate = || Command::AllocateBlock {
            inode_id,
            lease_epoch: 1,
        };

        let first = expect_block_allocated(
            AppRaftStateMachine::new(Arc::clone(&storage))
                .apply(allocate())
                .unwrap(),
        );
        assert_eq!(first, BlockId::new(inode_id, BlockIndex::new(0)));
        expect_file_published(
            AppRaftStateMachine::new(Arc::clone(&storage))
                .apply(Command::PublishFile {
                    proposed_at_ms: 1,
                    inode_id,
                    extents: vec![extent(first, 0, 1024)],
                    target_size: 1024,
                    expected_content_revision: 0,
                    expected_file_size: 0,
                    lease_epoch: 1,
                    mode: PublishMode::ReplaceIfUnchanged,
                })
                .unwrap(),
        );

        let restarted = AppRaftStateMachine::new(Arc::clone(&storage));
        let second = expect_block_allocated(restarted.apply(allocate()).unwrap());
        assert_eq!(second, BlockId::new(inode_id, BlockIndex::new(1)));
        assert!(matches!(
            restarted.apply(Command::AllocateBlock {
                inode_id,
                lease_epoch: 0,
            }),
            Err(MetadataError::LeaseFenced { expected: 1, got: 0 })
        ));

        let inode = storage.get_inode(inode_id).unwrap().unwrap();
        let InodeData::File { next_block_index, .. } = inode.data else {
            panic!("expected file inode")
        };
        assert_eq!(next_block_index, 2);
    }

    #[test]
    fn block_allocation_exhaustion_fails_closed_without_wrapping() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let inode_id = InodeId::new(107);
        let mut inode = install_file_with_extents(&storage, InodeId::new(100), "file", inode_id, Vec::new(), 0);
        let InodeData::File { next_block_index, .. } = &mut inode.data else {
            panic!("expected file inode")
        };
        *next_block_index = u64::from(u32::MAX);
        storage.put_inode(&inode).unwrap();
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));
        let command = || Command::AllocateBlock {
            inode_id,
            lease_epoch: 1,
        };

        let last = expect_block_allocated(sm.apply(command()).unwrap());
        assert_eq!(last.index, BlockIndex::new(u32::MAX));
        assert!(matches!(
            sm.apply(command()),
            Err(MetadataError::InvalidArgument(message)) if message.contains("exhausted")
        ));
        let inode = storage.get_inode(inode_id).unwrap().unwrap();
        let InodeData::File { next_block_index, .. } = inode.data else {
            panic!("expected file inode")
        };
        assert_eq!(next_block_index, u64::from(u32::MAX) + 1);
    }

    #[test]
    fn ending_a_write_lease_fences_a_publish_that_has_not_linearized() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));
        let inode_id = InodeId::new(104);
        install_file_with_extents(&storage, InodeId::new(100), "file", inode_id, Vec::new(), 0);
        let publish = Command::PublishFile {
            proposed_at_ms: 2,
            inode_id,
            extents: vec![extent(BlockId::new(inode_id, BlockIndex::new(0)), 0, 1024)],
            target_size: 1024,
            expected_content_revision: 0,
            expected_file_size: 0,
            lease_epoch: 1,
            mode: PublishMode::ReplaceIfUnchanged,
        };

        let ended = expect_write_lease_ended(
            sm.apply(Command::EndWriteLease {
                proposed_at_ms: 1,
                inode_id,
                lease_epoch: 1,
            })
            .unwrap(),
        );
        assert_eq!(ended, (inode_id, 2));
        let replayed_end = expect_write_lease_ended(
            sm.apply(Command::EndWriteLease {
                proposed_at_ms: 3,
                inode_id,
                lease_epoch: 1,
            })
            .unwrap(),
        );
        assert_eq!(replayed_end, (inode_id, 2));
        expect_apply_rejection(
            sm.apply(publish),
            ApplyRejectionKind::LeaseFenced { expected: 2, got: 1 },
        );
        assert_eq!(storage.get_inode(inode_id).unwrap().unwrap().attrs.size, 0);
    }

    #[test]
    fn replace_publish_is_idempotent_only_for_the_exact_visible_state() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));
        let inode_id = InodeId::new(102);
        install_file_with_extents(&storage, InodeId::new(100), "file", inode_id, Vec::new(), 0);
        let command = Command::PublishFile {
            proposed_at_ms: 10,
            inode_id,
            extents: vec![extent(BlockId::new(inode_id, BlockIndex::new(0)), 0, 1024)],
            target_size: 1024,
            expected_content_revision: 0,
            expected_file_size: 0,
            lease_epoch: 1,
            mode: PublishMode::ReplaceIfUnchanged,
        };

        let first = expect_file_published(sm.apply(command.clone()).unwrap());
        let replay = expect_file_published(sm.apply(command).unwrap());
        assert_eq!(first, (inode_id, 1));
        assert_eq!(replay, first);

        expect_apply_rejection(
            sm.apply(Command::PublishFile {
                proposed_at_ms: 11,
                inode_id,
                extents: Vec::new(),
                target_size: 0,
                expected_content_revision: 0,
                expected_file_size: 0,
                lease_epoch: 1,
                mode: PublishMode::ReplaceIfUnchanged,
            }),
            ApplyRejectionKind::Again,
        );
        assert_eq!(storage.get_inode(inode_id).unwrap().unwrap().attrs.size, 1024);
    }

    #[test]
    fn append_publish_requires_the_current_content_revision_and_contiguous_offset() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));
        let inode_id = InodeId::new(103);
        install_file_with_extents(
            &storage,
            InodeId::new(100),
            "file",
            inode_id,
            vec![extent(BlockId::new(inode_id, BlockIndex::new(0)), 0, 1024)],
            1024,
        );

        let result = expect_file_published(
            sm.apply(Command::PublishFile {
                proposed_at_ms: 10,
                inode_id,
                extents: vec![extent(BlockId::new(inode_id, BlockIndex::new(1)), 1024, 512)],
                target_size: 1536,
                expected_content_revision: 0,
                expected_file_size: 1024,
                lease_epoch: 1,
                mode: PublishMode::AppendIfUnchanged,
            })
            .unwrap(),
        );
        assert_eq!(result, (inode_id, 1));

        expect_apply_rejection(
            sm.apply(Command::PublishFile {
                proposed_at_ms: 11,
                inode_id,
                extents: vec![extent(BlockId::new(inode_id, BlockIndex::new(2)), 1024, 512)],
                target_size: 1536,
                expected_content_revision: 0,
                expected_file_size: 1024,
                lease_epoch: 1,
                mode: PublishMode::AppendIfUnchanged,
            }),
            ApplyRejectionKind::Again,
        );

        let second_append = Command::PublishFile {
            proposed_at_ms: 12,
            inode_id,
            extents: vec![extent(BlockId::new(inode_id, BlockIndex::new(2)), 1536, 512)],
            target_size: 2048,
            expected_content_revision: 1,
            expected_file_size: 1536,
            lease_epoch: 1,
            mode: PublishMode::AppendIfUnchanged,
        };
        let second = expect_file_published(sm.apply(second_append.clone()).unwrap());
        let replay = expect_file_published(sm.apply(second_append).unwrap());
        assert_eq!(second, (inode_id, 2));
        assert_eq!(replay, second);
    }
}
