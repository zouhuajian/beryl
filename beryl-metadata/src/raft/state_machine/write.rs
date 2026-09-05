// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use super::{
    AppMetadataRaftState, AppRaftStateMachine, BlockId, BlockIndex, Extent, FileLayout, Inode, InodeData, InodeId,
    MetadataError, MetadataResult, PublishMode, MAX_FILE_EXTENTS,
};
use crate::inode::{FileCommit, FilePublication};
use beryl_types::{CallId, ClientId, ContentGeneration, LeaseEpoch};
use std::collections::HashSet;

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

    /// Reserve a never-reused sequence under the current durable writer epoch.
    pub(super) fn apply_allocate_block(
        &self,
        inode_id: InodeId,
        lease_epoch: LeaseEpoch,
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
                let epoch = stored_lease_epoch.unwrap_or_default();
                if epoch != lease_epoch {
                    return Err(MetadataError::LeaseFenced {
                        expected: epoch,
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
        expected_lease_epoch: LeaseEpoch,
        proposed_at_ms: u64,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<LeaseEpoch> {
        let prepared: MetadataResult<(Inode, LeaseEpoch)> = (|| {
            let mut inode = self
                .storage
                .get_inode(inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {inode_id}")))?;
            Self::ensure_file_inode_authority(inode_id, &inode)?;
            if let Some(record) = self.storage.get_create_file_replay_for_inode(inode_id)? {
                let InodeData::File {
                    extents,
                    generation,
                    lease_epoch,
                    next_block_index,
                    ..
                } = &inode.data
                else {
                    unreachable!("file authority checked above")
                };
                let still_initial = extents.is_empty()
                    && generation.unwrap_or_default() == record.generation
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
                    let current = lease_epoch.unwrap_or_default();
                    if current != expected_lease_epoch {
                        return Err(MetadataError::Again(format!(
                            "write lease epoch changed for inode {inode_id}: expected {expected_lease_epoch}, current {current}"
                        )));
                    }
                    let next = current.checked_next().ok_or_else(|| {
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
        lease_epoch: LeaseEpoch,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<LeaseEpoch> {
        let prepared: MetadataResult<(Option<Inode>, LeaseEpoch)> = (|| {
            let mut inode = self
                .storage
                .get_inode(inode_id)?
                .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {inode_id}")))?;
            Self::ensure_file_inode_authority(inode_id, &inode)?;
            let next = lease_epoch.checked_next().ok_or_else(|| {
                MetadataError::InvalidArgument(format!("write lease epoch overflow for inode {inode_id}"))
            })?;
            let current = match &mut inode.data {
                InodeData::File { lease_epoch, .. } => lease_epoch.unwrap_or_default(),
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

    /// Publish one fenced content generation or confirm an exact idempotent replay.
    ///
    /// The returned generation is always the durable generation visible after the
    /// command. Both mutation and replay paths commit the supplied applied index.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn apply_publish_file(
        &self,
        inode_id: InodeId,
        requested_extents: Vec<Extent>,
        target_size: u64,
        expected_generation: ContentGeneration,
        expected_file_size: u64,
        lease_epoch: LeaseEpoch,
        mode: PublishMode,
        proposed_at_ms: u64,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<ContentGeneration> {
        let inode = self
            .storage
            .get_inode(inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {inode_id}")))?;
        Self::ensure_file_inode_authority(inode_id, &inode)?;
        let (inode, layout, generation, changed) = self.prepare_file_publication(
            inode,
            FilePublication {
                extents: requested_extents,
                target_size,
                expected_generation,
                expected_file_size,
                lease_epoch,
                mode,
            },
            proposed_at_ms,
        )?;
        if changed {
            self.storage.publish_file_atomic(&inode, layout, raft_state)?;
        } else {
            self.storage.commit_applied_state(raft_state)?;
        }
        Ok(generation)
    }

    /// Validate a publication and prepare its inode without performing any write.
    /// Sync and Commit share content rules but select separate atomic persistence.
    fn prepare_file_publication(
        &self,
        mut inode: Inode,
        publication: FilePublication,
        proposed_at_ms: u64,
    ) -> MetadataResult<(Inode, FileLayout, ContentGeneration, bool)> {
        let inode_id = inode.inode_id;
        let FilePublication {
            extents: mut requested_extents,
            target_size,
            expected_generation,
            expected_file_size,
            lease_epoch,
            mode,
        } = publication;
        if requested_extents.len() > MAX_FILE_EXTENTS {
            return Err(MetadataError::ResourceExhausted(
                "file publication exceeds extent limit".into(),
            ));
        }
        Self::ensure_file_inode_authority(inode_id, &inode)?;
        let layout = self.storage.get_layout(inode_id)?;
        requested_extents.sort_by_key(|extent| (extent.file_offset, extent.block_id.index.as_raw()));

        let (existing_extents, generation, stored_lease_epoch) = match &inode.data {
            InodeData::File {
                extents,
                generation,
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
                (
                    extents.clone(),
                    generation.unwrap_or_default(),
                    lease_epoch.unwrap_or_default(),
                )
            }
            _ => unreachable!("file inode must carry file data"),
        };
        if stored_lease_epoch != lease_epoch {
            return Err(MetadataError::LeaseFenced {
                expected: stored_lease_epoch,
                got: lease_epoch,
            });
        }
        if generation == expected_generation && inode.attrs.size != expected_file_size {
            return Err(MetadataError::Again(format!(
                "file size changed for inode {inode_id}: expected {expected_file_size}, current {}",
                inode.attrs.size
            )));
        }

        let mut seen = HashSet::with_capacity(requested_extents.len());
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
        if expected_generation.checked_next() == Some(generation) && state_matches {
            return Ok((inode, layout, generation, false));
        }
        if generation != expected_generation {
            return Err(MetadataError::Again(format!(
                "content generation changed for inode {inode_id}: expected {expected_generation}, current {generation}"
            )));
        }
        if state_matches {
            return Ok((inode, layout, generation, false));
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
                .ok_or_else(|| MetadataError::ResourceExhausted("final file extent count overflowed".to_string()))?,
        };
        if final_extent_count > MAX_FILE_EXTENTS {
            return Err(MetadataError::ResourceExhausted(format!(
                "final file extent count {final_extent_count} exceeds maximum {MAX_FILE_EXTENTS} for inode {inode_id}"
            )));
        }
        let generation = Self::next_generation(inode_id, Some(generation))?;
        Self::stamp_extents(&mut extents_to_publish, &existing_extents, generation);
        match &mut inode.data {
            InodeData::File {
                extents,
                generation: stored_generation,
                last_commit,
                ..
            } => {
                match mode {
                    PublishMode::ReplaceIfUnchanged => *extents = extents_to_publish,
                    PublishMode::AppendIfUnchanged => extents.extend(extents_to_publish),
                }
                for extent in extents.iter_mut() {
                    extent.generation = Some(generation);
                }
                *stored_generation = Some(generation);
                *last_commit = None;
            }
            _ => unreachable!("file inode must carry file data"),
        }
        inode.attrs.size = target_size;
        inode
            .attrs
            .update_mtime_ctime(Self::mutation_timestamp(&inode, proposed_at_ms));
        Ok((inode, layout, generation, true))
    }

    /// Atomically publish and revoke one writer, including empty and no-op closes.
    /// Only an exact persisted receipt may bypass the original generation/lease CAS.
    pub(super) fn apply_commit_file(
        &self,
        inode_id: InodeId,
        operation: (ClientId, CallId),
        publication: FilePublication,
        proposed_at_ms: u64,
        raft_state: &AppMetadataRaftState,
    ) -> MetadataResult<ContentGeneration> {
        if publication.extents.len() > MAX_FILE_EXTENTS {
            return Err(MetadataError::ResourceExhausted(
                "CommitFile exceeds extent limit".into(),
            ));
        }
        let inode = self
            .storage
            .get_inode(inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {inode_id}")))?;
        Self::ensure_file_inode_authority(inode_id, &inode)?;
        if let Some(generation) = publication.resolve_commit(&inode, operation.0, operation.1)? {
            self.storage.commit_applied_state(raft_state)?;
            return Ok(generation);
        }
        let InodeData::File { generation, .. } = &inode.data else {
            unreachable!("file checked above")
        };
        if generation.unwrap_or_default() != publication.expected_generation {
            return Err(MetadataError::Again(
                "CommitFile content generation changed without matching completion evidence".into(),
            ));
        }
        let ended_epoch = publication
            .lease_epoch
            .checked_next()
            .ok_or_else(|| MetadataError::InvalidArgument("write lease epoch overflow".into()))?;
        let mut commit = FileCommit {
            client_id: operation.0,
            call_id: operation.1,
            lease_epoch: publication.lease_epoch,
            expected_generation: publication.expected_generation,
            expected_file_size: publication.expected_file_size,
            mode: publication.mode,
            committed_size: publication.target_size,
            generation: publication.expected_generation,
        };
        let (mut inode, _, generation, _) = self.prepare_file_publication(inode, publication, proposed_at_ms)?;
        commit.generation = generation;
        let InodeData::File {
            lease_epoch,
            last_commit,
            ..
        } = &mut inode.data
        else {
            unreachable!("file checked above")
        };
        *lease_epoch = Some(ended_epoch);
        *last_commit = Some(commit);
        // The layout is unchanged by publication; one inode put also stores the
        // completion evidence, with last_applied in the same authority batch.
        self.storage.put_inode_atomic(&inode, raft_state)?;
        Ok(generation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::response::ApplyRejectionKind;
    use crate::raft::state_machine::tests::*;
    use beryl_types::FileType;
    use openraft::{LeaderId, LogId};

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
            last_applied_log_id: Some(LogId::new(LeaderId::new(7, 1), 703)),
            ..AppMetadataRaftState::default()
        };
        assert_ne!(rejected_applied_state, applied_before);
        let commands = [
            Command::AllocateBlock {
                inode_id,
                lease_epoch: LeaseEpoch::new(1),
            },
            Command::AcquireWriteLease {
                proposed_at_ms: 1,
                inode_id,
                expected_lease_epoch: LeaseEpoch::new(1),
            },
            Command::EndWriteLease {
                proposed_at_ms: 1,
                inode_id,
                lease_epoch: LeaseEpoch::new(1),
            },
            Command::PublishFile {
                proposed_at_ms: 1,
                inode_id,
                extents: Vec::new(),
                target_size: 0,
                expected_generation: ContentGeneration::new(0),
                expected_file_size: 0,
                lease_epoch: LeaseEpoch::new(1),
                mode: PublishMode::ReplaceIfUnchanged,
            },
            commit_command(
                inode_id,
                FilePublication {
                    extents: Vec::new(),
                    target_size: 0,
                    expected_generation: ContentGeneration::new(0),
                    expected_file_size: 0,
                    lease_epoch: LeaseEpoch::new(1),
                    mode: PublishMode::ReplaceIfUnchanged,
                },
            ),
        ];

        for command in commands {
            let error = sm.apply_with_raft_state(command, &rejected_applied_state).unwrap_err();
            assert!(error.to_string().contains("inode authority is corrupt"));
            assert_eq!(storage.load_raft_state().unwrap(), applied_before);
            assert_eq!(storage.get_inode(inode_id).unwrap().as_ref(), Some(expected_inode));
        }
    }

    fn commit_command(inode_id: InodeId, publication: FilePublication) -> Command {
        Command::CommitFile {
            proposed_at_ms: 10,
            inode_id,
            client_id: ClientId::new(9),
            call_id: CallId::new(),
            publication,
        }
    }

    #[test]
    fn commit_atomically_closes_empty_new_and_synced_content_and_bounds_replay() {
        for (length, synced) in [(0, false), (64, false), (64, true)] {
            let dir = TempDir::new().unwrap();
            let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
            let sm = AppRaftStateMachine::new(Arc::clone(&storage));
            let inode_id = InodeId::new(102);
            install_file_with_extents(&storage, InodeId::new(100), "file", inode_id, Vec::new(), 0);
            let extents = if length == 0 {
                Vec::new()
            } else {
                vec![extent(BlockId::new(inode_id, BlockIndex::new(0)), 0, length)]
            };
            if synced {
                sm.apply(Command::PublishFile {
                    proposed_at_ms: 1,
                    inode_id,
                    extents: extents.clone(),
                    target_size: length,
                    expected_generation: ContentGeneration::new(0),
                    expected_file_size: 0,
                    lease_epoch: LeaseEpoch::new(1),
                    mode: PublishMode::ReplaceIfUnchanged,
                })
                .unwrap();
            }
            let command = commit_command(
                inode_id,
                FilePublication {
                    extents,
                    target_size: length,
                    expected_generation: ContentGeneration::new(u64::from(synced)),
                    expected_file_size: if synced { length } else { 0 },
                    lease_epoch: LeaseEpoch::new(1),
                    mode: PublishMode::ReplaceIfUnchanged,
                },
            );
            sm.apply(command.clone()).unwrap();
            let committed = storage.get_inode(inode_id).unwrap().unwrap();
            let InodeData::File {
                generation,
                lease_epoch,
                last_commit,
                ..
            } = &committed.data
            else {
                panic!("file")
            };
            assert_eq!(
                *generation,
                if length == 0 {
                    None
                } else {
                    Some(ContentGeneration::new(1))
                }
            );
            assert_eq!(*lease_epoch, Some(LeaseEpoch::new(2)));
            assert!(last_commit.is_some());
            sm.apply(command.clone()).unwrap();
            sm.apply(Command::EndWriteLease {
                proposed_at_ms: 11,
                inode_id,
                lease_epoch: LeaseEpoch::new(1),
            })
            .unwrap();
            assert_eq!(storage.get_inode(inode_id).unwrap().unwrap(), committed);
            sm.apply(Command::AcquireWriteLease {
                proposed_at_ms: 12,
                inode_id,
                expected_lease_epoch: LeaseEpoch::new(2),
            })
            .unwrap();
            sm.apply(command.clone()).unwrap();
            let reopened = storage.get_inode(inode_id).unwrap().unwrap();
            assert!(
                matches!(reopened.data, InodeData::File { lease_epoch: Some(epoch), .. } if epoch == LeaseEpoch::new(3))
            );
            let next = commit_command(
                inode_id,
                FilePublication {
                    extents: Vec::new(),
                    target_size: length,
                    expected_generation: ContentGeneration::new(u64::from(length > 0)),
                    expected_file_size: length,
                    lease_epoch: LeaseEpoch::new(3),
                    mode: PublishMode::AppendIfUnchanged,
                },
            );
            sm.apply(next.clone()).unwrap();
            sm.apply(next.clone()).unwrap();
            assert!(
                sm.apply(command).is_err(),
                "a newer no-op close retires the old receipt"
            );
            sm.apply(Command::AcquireWriteLease {
                proposed_at_ms: 13,
                inode_id,
                expected_lease_epoch: LeaseEpoch::new(4),
            })
            .unwrap();
            for changed in [false, true] {
                sm.apply(Command::PublishFile {
                    proposed_at_ms: 14,
                    inode_id,
                    extents: if changed {
                        vec![extent(BlockId::new(inode_id, BlockIndex::new(1)), length, 1)]
                    } else {
                        Vec::new()
                    },
                    target_size: length + u64::from(changed),
                    expected_generation: ContentGeneration::new(u64::from(length > 0)),
                    expected_file_size: length,
                    lease_epoch: LeaseEpoch::new(5),
                    mode: PublishMode::AppendIfUnchanged,
                })
                .unwrap();
                assert_eq!(sm.apply(next.clone()).is_ok(), !changed);
                let current = storage.get_inode(inode_id).unwrap().unwrap();
                assert!(
                    matches!(current.data, InodeData::File { last_commit, .. } if last_commit.is_none() == changed)
                );
            }
        }
    }

    #[test]
    fn commit_counter_exhaustion_leaves_content_and_lease_unchanged() {
        for exhausted_lease in [false, true] {
            let dir = TempDir::new().unwrap();
            let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
            let sm = AppRaftStateMachine::new(Arc::clone(&storage));
            let inode_id = InodeId::new(102);
            let mut inode = install_file_with_extents(&storage, InodeId::new(100), "file", inode_id, Vec::new(), 0);
            let generation = ContentGeneration::new(if exhausted_lease { 0 } else { u64::MAX });
            let epoch = LeaseEpoch::new(if exhausted_lease { u64::MAX } else { 1 });
            let InodeData::File {
                generation: stored_generation,
                lease_epoch,
                ..
            } = &mut inode.data
            else {
                unreachable!()
            };
            *stored_generation = Some(generation);
            *lease_epoch = Some(epoch);
            storage.put_inode(&inode).unwrap();
            assert!(sm
                .apply(commit_command(
                    inode_id,
                    FilePublication {
                        extents: vec![extent(BlockId::new(inode_id, BlockIndex::new(0)), 0, 1)],
                        target_size: 1,
                        expected_generation: generation,
                        expected_file_size: 0,
                        lease_epoch: epoch,
                        mode: PublishMode::ReplaceIfUnchanged,
                    }
                ))
                .is_err());
            assert_eq!(storage.get_inode(inode_id).unwrap().unwrap(), inode);
        }
    }

    #[test]
    fn sync_abort_and_new_lease_never_supply_commit_evidence() {
        for acquire in [false, true] {
            let dir = TempDir::new().unwrap();
            let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
            let sm = AppRaftStateMachine::new(Arc::clone(&storage));
            let inode_id = InodeId::new(102);
            install_file_with_extents(&storage, InodeId::new(100), "file", inode_id, Vec::new(), 0);
            let publication = FilePublication {
                extents: vec![extent(BlockId::new(inode_id, BlockIndex::new(0)), 0, 64)],
                target_size: 64,
                expected_generation: ContentGeneration::new(0),
                expected_file_size: 0,
                lease_epoch: LeaseEpoch::new(1),
                mode: PublishMode::ReplaceIfUnchanged,
            };
            sm.apply(Command::PublishFile {
                proposed_at_ms: 1,
                inode_id,
                extents: publication.extents.clone(),
                target_size: 64,
                expected_generation: publication.expected_generation,
                expected_file_size: 0,
                lease_epoch: publication.lease_epoch,
                mode: publication.mode,
            })
            .unwrap();
            let transition = if acquire {
                Command::AcquireWriteLease {
                    proposed_at_ms: 2,
                    inode_id,
                    expected_lease_epoch: LeaseEpoch::new(1),
                }
            } else {
                Command::EndWriteLease {
                    proposed_at_ms: 2,
                    inode_id,
                    lease_epoch: LeaseEpoch::new(1),
                }
            };
            sm.apply(transition).unwrap();
            let before = storage.get_inode(inode_id).unwrap();
            assert!(sm.apply(commit_command(inode_id, publication.clone())).is_err());
            let noop = FilePublication {
                expected_generation: ContentGeneration::new(1),
                expected_file_size: 64,
                ..publication
            };
            assert!(sm.apply(commit_command(inode_id, noop)).is_err());
            assert_eq!(storage.get_inode(inode_id).unwrap(), before);
        }
    }

    #[test]
    fn file_mutations_reject_corrupt_inode_authority_without_advancing_apply_state() {
        let kind_dir = TempDir::new().unwrap();
        let kind_storage = Arc::new(RocksDBStorage::create_for_format(kind_dir.path()).unwrap());
        let kind_inode_id = InodeId::new(108);
        let mut kind_mismatch =
            install_file_with_extents(&kind_storage, InodeId::new(100), "file", kind_inode_id, Vec::new(), 0);
        kind_mismatch.kind = FileType::Dir;
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
            lease_epoch: LeaseEpoch::new(1),
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
                    expected_generation: ContentGeneration::new(0),
                    expected_file_size: 0,
                    lease_epoch: LeaseEpoch::new(1),
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
                lease_epoch: LeaseEpoch::new(0),
            }),
            Err(MetadataError::LeaseFenced { expected, got }) if expected == LeaseEpoch::new(1) && got == LeaseEpoch::new(0)
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
            lease_epoch: LeaseEpoch::new(1),
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
            expected_generation: ContentGeneration::new(0),
            expected_file_size: 0,
            lease_epoch: LeaseEpoch::new(1),
            mode: PublishMode::ReplaceIfUnchanged,
        };

        let ended = expect_write_lease_ended(
            sm.apply(Command::EndWriteLease {
                proposed_at_ms: 1,
                inode_id,
                lease_epoch: LeaseEpoch::new(1),
            })
            .unwrap(),
        );
        assert_eq!(ended, (inode_id, 2));
        let replayed_end = expect_write_lease_ended(
            sm.apply(Command::EndWriteLease {
                proposed_at_ms: 3,
                inode_id,
                lease_epoch: LeaseEpoch::new(1),
            })
            .unwrap(),
        );
        assert_eq!(replayed_end, (inode_id, 2));
        expect_apply_rejection(
            sm.apply(publish),
            ApplyRejectionKind::LeaseFenced {
                expected: LeaseEpoch::new(2),
                got: LeaseEpoch::new(1),
            },
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
            expected_generation: ContentGeneration::new(0),
            expected_file_size: 0,
            lease_epoch: LeaseEpoch::new(1),
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
                expected_generation: ContentGeneration::new(0),
                expected_file_size: 0,
                lease_epoch: LeaseEpoch::new(1),
                mode: PublishMode::ReplaceIfUnchanged,
            }),
            ApplyRejectionKind::Again,
        );
        assert_eq!(storage.get_inode(inode_id).unwrap().unwrap().attrs.size, 1024);
    }

    #[test]
    fn append_publish_requires_the_generation_and_contiguous_offset() {
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
                expected_generation: ContentGeneration::new(0),
                expected_file_size: 1024,
                lease_epoch: LeaseEpoch::new(1),
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
                expected_generation: ContentGeneration::new(0),
                expected_file_size: 1024,
                lease_epoch: LeaseEpoch::new(1),
                mode: PublishMode::AppendIfUnchanged,
            }),
            ApplyRejectionKind::Again,
        );

        let second_append = Command::PublishFile {
            proposed_at_ms: 12,
            inode_id,
            extents: vec![extent(BlockId::new(inode_id, BlockIndex::new(2)), 1536, 512)],
            target_size: 2048,
            expected_generation: ContentGeneration::new(1),
            expected_file_size: 1536,
            lease_epoch: LeaseEpoch::new(1),
            mode: PublishMode::AppendIfUnchanged,
        };
        let second = expect_file_published(sm.apply(second_append.clone()).unwrap());
        let replay = expect_file_published(sm.apply(second_append).unwrap());
        assert_eq!(second, (inode_id, 2));
        assert_eq!(replay, second);
    }
}
