// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use super::*;

impl AppRaftStateMachine {
    pub(super) fn apply_acquire_write_lease(
        &self,
        inode_id: InodeId,
        expected_lease_epoch: u64,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<FsCommandResult> {
        let prepared: MetadataResult<(Inode, FsOkResult)> = (|| {
            let mut inode = self
                .storage
                .get_inode(inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {inode_id}")))?;
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
            Ok((
                inode,
                FsOkResult {
                    inode_id: Some(inode_id),
                    lease_epoch: Some(lease_epoch),
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

    pub(super) fn apply_end_write_lease(
        &self,
        inode_id: InodeId,
        lease_epoch: u64,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<FsCommandResult> {
        let prepared: MetadataResult<(Option<Inode>, u64)> = (|| {
            let mut inode = self
                .storage
                .get_inode(inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {inode_id}")))?;
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

        let (inode, ended_epoch) = match prepared {
            Ok(prepared) => prepared,
            Err(err) => return self.persist_fs_error(err, raft_state),
        };
        let result = FsCommandResult::Ok(FsOkResult {
            inode_id: Some(inode_id),
            lease_epoch: Some(ended_epoch),
            ..FsOkResult::default()
        });
        if let Some(inode) = inode {
            self.storage.put_inode_atomic(&inode, raft_state)?;
        } else {
            self.storage.commit_applied_state(raft_state)?;
        }
        Ok(result)
    }

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
    ) -> MetadataResult<FsCommandResult> {
        let prepared: MetadataResult<(Inode, FileLayout, FsOkResult, bool)> = (|| {
            let mut inode = self
                .storage
                .get_inode(inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {inode_id}")))?;
            if !inode.kind.is_file() {
                return Err(MetadataError::InvalidArgument(format!(
                    "Inode is not a file: {inode_id}"
                )));
            }
            let data_handle_id = inode.current_data_handle_id;
            if data_handle_id.as_raw() == 0 {
                return Err(MetadataError::Internal(format!(
                    "File inode {inode_id} is missing current_data_handle_id"
                )));
            }
            let layout = self.storage.get_layout(inode_id)?;
            requested_extents.sort_by_key(|extent| (extent.file_offset, extent.block_id.index.as_raw()));

            let (existing_extents, current_content_revision, stored_lease_epoch) = match &inode.data {
                InodeData::File {
                    extents,
                    content_revision,
                    lease_epoch,
                } => (extents.clone(), content_revision.unwrap_or(0), lease_epoch.unwrap_or(0)),
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
            for extent in &requested_extents {
                if extent.len == 0 {
                    return Err(MetadataError::InvalidArgument(
                        "Committed extent len must be greater than 0".to_string(),
                    ));
                }
                if extent.block_id.data_handle_id != data_handle_id {
                    return Err(MetadataError::InvalidArgument(format!(
                        "Extent block data_handle_id {} does not match inode {inode_id} data_handle_id {data_handle_id}",
                        extent.block_id.data_handle_id
                    )));
                }
                if !seen.insert(extent.block_id) {
                    return Err(MetadataError::InvalidArgument(format!(
                        "Committed block {} was submitted more than once",
                        extent.block_id
                    )));
                }
                if Self::extent_end(extent)? > target_size {
                    return Err(MetadataError::InvalidArgument(format!(
                        "Extent extends beyond target_size {target_size}: {}",
                        extent.block_id
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
            if current_content_revision == expected_content_revision.saturating_add(1) && state_matches {
                return Ok((
                    inode,
                    layout,
                    FsOkResult {
                        inode_id: Some(inode_id),
                        data_handle_id: Some(data_handle_id),
                        content_revision: Some(current_content_revision),
                        ..FsOkResult::default()
                    },
                    false,
                ));
            }
            if current_content_revision != expected_content_revision {
                return Err(MetadataError::Again(format!(
                    "content revision changed for inode {inode_id}: expected {expected_content_revision}, current {current_content_revision}"
                )));
            }
            if state_matches {
                return Ok((
                    inode,
                    layout,
                    FsOkResult {
                        inode_id: Some(inode_id),
                        data_handle_id: Some(data_handle_id),
                        content_revision: Some(current_content_revision),
                        ..FsOkResult::default()
                    },
                    false,
                ));
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
            Ok((
                inode,
                layout,
                FsOkResult {
                    inode_id: Some(inode_id),
                    data_handle_id: Some(data_handle_id),
                    content_revision: Some(content_revision),
                    ..FsOkResult::default()
                },
                true,
            ))
        })();

        let (inode, layout, ok, changed) = match prepared {
            Ok(prepared) => prepared,
            Err(err) => return self.persist_fs_error(err, raft_state),
        };
        let result = FsCommandResult::Ok(ok);
        if changed {
            self.storage.publish_file_atomic(&inode, layout, raft_state)?;
        } else {
            self.storage.commit_applied_state(raft_state)?;
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::state_machine::tests::*;

    #[test]
    fn acquire_write_lease_uses_durable_compare_and_increment() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));
        let inode_id = InodeId::new(101);
        install_file_with_extents(
            &storage,
            InodeId::new(100),
            "file",
            inode_id,
            DataHandleId::new(201),
            Vec::new(),
            0,
        );

        let first = expect_fs_ok(
            sm.apply(Command::AcquireWriteLease {
                proposed_at_ms: 1,
                inode_id,
                expected_lease_epoch: 1,
            })
            .unwrap(),
        );
        assert_eq!(first.lease_epoch, Some(2));

        expect_fs_errno(
            sm.apply(Command::AcquireWriteLease {
                proposed_at_ms: 2,
                inode_id,
                expected_lease_epoch: 1,
            })
            .unwrap(),
            FsErrorCode::EAgain,
        );
        let inode = storage.get_inode(inode_id).unwrap().unwrap();
        let InodeData::File { lease_epoch, .. } = inode.data else {
            panic!("expected file inode");
        };
        assert_eq!(lease_epoch, Some(2));
    }

    #[test]
    fn ending_a_write_lease_fences_a_publish_that_has_not_linearized() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));
        let inode_id = InodeId::new(104);
        let data_handle_id = DataHandleId::new(204);
        install_file_with_extents(
            &storage,
            InodeId::new(100),
            "file",
            inode_id,
            data_handle_id,
            Vec::new(),
            0,
        );
        let publish = Command::PublishFile {
            proposed_at_ms: 2,
            inode_id,
            extents: vec![extent(BlockId::new(data_handle_id, BlockIndex::new(0)), 0, 1024)],
            target_size: 1024,
            expected_content_revision: 0,
            expected_file_size: 0,
            lease_epoch: 1,
            mode: PublishMode::ReplaceIfUnchanged,
        };

        let ended = expect_fs_ok(
            sm.apply(Command::EndWriteLease {
                proposed_at_ms: 1,
                inode_id,
                lease_epoch: 1,
            })
            .unwrap(),
        );
        assert_eq!(ended.lease_epoch, Some(2));
        let replayed_end = expect_fs_ok(
            sm.apply(Command::EndWriteLease {
                proposed_at_ms: 3,
                inode_id,
                lease_epoch: 1,
            })
            .unwrap(),
        );
        assert_eq!(replayed_end.lease_epoch, Some(2));
        expect_fs_errno(sm.apply(publish).unwrap(), FsErrorCode::EInval);
        assert_eq!(storage.get_inode(inode_id).unwrap().unwrap().attrs.size, 0);
    }

    #[test]
    fn ending_a_write_lease_after_publish_preserves_visible_content() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));
        let inode_id = InodeId::new(105);
        let data_handle_id = DataHandleId::new(205);
        install_file_with_extents(
            &storage,
            InodeId::new(100),
            "file",
            inode_id,
            data_handle_id,
            Vec::new(),
            0,
        );

        expect_fs_ok(
            sm.apply(Command::PublishFile {
                proposed_at_ms: 1,
                inode_id,
                extents: vec![extent(BlockId::new(data_handle_id, BlockIndex::new(0)), 0, 1024)],
                target_size: 1024,
                expected_content_revision: 0,
                expected_file_size: 0,
                lease_epoch: 1,
                mode: PublishMode::ReplaceIfUnchanged,
            })
            .unwrap(),
        );
        let ended = expect_fs_ok(
            sm.apply(Command::EndWriteLease {
                proposed_at_ms: 2,
                inode_id,
                lease_epoch: 1,
            })
            .unwrap(),
        );

        assert_eq!(ended.lease_epoch, Some(2));
        let inode = storage.get_inode(inode_id).unwrap().unwrap();
        assert_eq!(inode.attrs.size, 1024);
        let InodeData::File {
            content_revision,
            lease_epoch,
            ..
        } = inode.data
        else {
            panic!("expected file inode");
        };
        assert_eq!(content_revision, Some(1));
        assert_eq!(lease_epoch, Some(2));
    }

    #[test]
    fn replace_publish_is_idempotent_only_for_the_exact_visible_state() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));
        let inode_id = InodeId::new(102);
        let data_handle_id = DataHandleId::new(202);
        install_file_with_extents(
            &storage,
            InodeId::new(100),
            "file",
            inode_id,
            data_handle_id,
            Vec::new(),
            0,
        );
        let command = Command::PublishFile {
            proposed_at_ms: 10,
            inode_id,
            extents: vec![extent(BlockId::new(data_handle_id, BlockIndex::new(0)), 0, 1024)],
            target_size: 1024,
            expected_content_revision: 0,
            expected_file_size: 0,
            lease_epoch: 1,
            mode: PublishMode::ReplaceIfUnchanged,
        };

        let first = expect_fs_ok(sm.apply(command.clone()).unwrap());
        let replay = expect_fs_ok(sm.apply(command).unwrap());
        assert_eq!(first.content_revision, Some(1));
        assert_eq!(replay.content_revision, first.content_revision);

        expect_fs_errno(
            sm.apply(Command::PublishFile {
                proposed_at_ms: 11,
                inode_id,
                extents: Vec::new(),
                target_size: 0,
                expected_content_revision: 0,
                expected_file_size: 0,
                lease_epoch: 1,
                mode: PublishMode::ReplaceIfUnchanged,
            })
            .unwrap(),
            FsErrorCode::EAgain,
        );
        assert_eq!(storage.get_inode(inode_id).unwrap().unwrap().attrs.size, 1024);
    }

    #[test]
    fn append_publish_requires_the_current_content_revision_and_contiguous_offset() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let sm = AppRaftStateMachine::new(Arc::clone(&storage));
        let inode_id = InodeId::new(103);
        let data_handle_id = DataHandleId::new(203);
        install_file_with_extents(
            &storage,
            InodeId::new(100),
            "file",
            inode_id,
            data_handle_id,
            vec![extent(BlockId::new(data_handle_id, BlockIndex::new(0)), 0, 1024)],
            1024,
        );

        let result = expect_fs_ok(
            sm.apply(Command::PublishFile {
                proposed_at_ms: 10,
                inode_id,
                extents: vec![extent(BlockId::new(data_handle_id, BlockIndex::new(1)), 1024, 512)],
                target_size: 1536,
                expected_content_revision: 0,
                expected_file_size: 1024,
                lease_epoch: 1,
                mode: PublishMode::AppendIfUnchanged,
            })
            .unwrap(),
        );
        assert_eq!(result.content_revision, Some(1));

        expect_fs_errno(
            sm.apply(Command::PublishFile {
                proposed_at_ms: 11,
                inode_id,
                extents: vec![extent(BlockId::new(data_handle_id, BlockIndex::new(2)), 1024, 512)],
                target_size: 1536,
                expected_content_revision: 0,
                expected_file_size: 1024,
                lease_epoch: 1,
                mode: PublishMode::AppendIfUnchanged,
            })
            .unwrap(),
            FsErrorCode::EAgain,
        );

        let second_append = Command::PublishFile {
            proposed_at_ms: 12,
            inode_id,
            extents: vec![extent(BlockId::new(data_handle_id, BlockIndex::new(2)), 1536, 512)],
            target_size: 2048,
            expected_content_revision: 1,
            expected_file_size: 1536,
            lease_epoch: 1,
            mode: PublishMode::AppendIfUnchanged,
        };
        let second = expect_fs_ok(sm.apply(second_append.clone()).unwrap());
        let replay = expect_fs_ok(sm.apply(second_append).unwrap());
        assert_eq!(second.content_revision, Some(2));
        assert_eq!(replay.content_revision, second.content_revision);
    }
}
