// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Durable file visibility publication for sync and commit.

use super::command::unexpected_raft_apply_success;
use super::{
    fs_failure_from_metadata_error, Freshness, FsFailure, FsResult, MetadataFileSystem, RequestContext, WriteHandle,
};
use crate::error::{MetadataError, MetadataResult};
use crate::observe;
use crate::raft::{ApplySuccess, Command, PublishMode};
use crate::session_registry::{BeginWritePublicationError, WritePublication, WriteSession};
use crate::worker::{PublishReadyConflict, PublishReadyStatus, PublishReadyTarget};
use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, RefreshHint, WorkerErrorKind};
use beryl_types::fs::{Extent, InodeData};
use beryl_types::ids::{InodeId, MountId};
use beryl_types::{CommittedBlock, ContentGeneration, GroupName, LeaseEpoch, WriteMode, MAX_FILE_EXTENTS};
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug)]
pub(super) struct CloseWriteIntent {
    pub(super) committed_blocks: Vec<CommittedBlock>,
    pub(super) final_size: u64,
    pub(super) expected_file_size: u64,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SyncWriteOutput {
    pub(crate) synced_size: u64,
    pub(crate) generation: Option<ContentGeneration>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct CloseWriteOutput {
    pub(crate) committed_size: u64,
}

pub(crate) struct CommitFileArgs {
    pub(crate) handle: WriteHandle,
    pub(crate) committed_blocks: Vec<CommittedBlock>,
    pub(crate) final_size: u64,
    pub(crate) freshness: Freshness,
    pub(crate) expected_generation: ContentGeneration,
    pub(crate) expected_file_size: u64,
    pub(crate) publish_mode: PublishMode,
}

pub(crate) struct SyncWriteArgs {
    pub(crate) handle: WriteHandle,
    pub(crate) committed_blocks: Vec<CommittedBlock>,
    pub(crate) target_size: u64,
    pub(crate) freshness: Freshness,
    pub(crate) expected_generation: ContentGeneration,
    pub(crate) expected_file_size: u64,
    pub(crate) publish_mode: PublishMode,
}

impl MetadataFileSystem {
    pub(crate) async fn commit_file(&self, ctx: &RequestContext, args: CommitFileArgs) -> FsResult<CloseWriteOutput> {
        if let Some(failure) = self.session_write_admission_failure(ctx, args.handle.inode_id) {
            return self.failure_from_admission(failure);
        }
        let inode_id = args.handle.inode_id;
        if args
            .committed_blocks
            .iter()
            .any(|block| block.block_id.inode_id != inode_id)
        {
            return self.failure_from_error(
                ctx,
                MetadataError::InvalidArgument("committed block inode_id does not match request".to_string()),
                None,
                None,
            );
        }

        let handle = args.handle;
        let committed_block_count = args.committed_blocks.len();
        let committed_bytes: u64 = args.committed_blocks.iter().map(|block| block.len).sum();
        let result = self
            .close_write_session(
                ctx,
                handle,
                CloseWriteIntent {
                    committed_blocks: args.committed_blocks,
                    final_size: args.final_size,
                    expected_file_size: args.expected_file_size,
                },
                args.freshness,
                args.expected_generation,
                args.publish_mode,
            )
            .await;
        match &result {
            Ok(success) => tracing::info!(
                target: "metadata.state",
                op = "CommitFile",
                result = "committed",
                error_code = "none",
                client_id = %ctx.caller.client.client_id,
                call_id = %ctx.caller.client.call_id,
                inode_id = inode_id.as_raw(),
                final_size = args.final_size,
                committed_block_count,
                committed_bytes,
                lease_epoch = handle.lease_epoch.as_raw(),
                mount_epoch = success.mount_epoch,
                route_epoch = success.route_epoch,
                "CommitFile committed"
            ),
            Err(failure) => tracing::warn!(
                target: "metadata.state",
                op = "CommitFile",
                result = "rejected",
                error_code = observe::rpc_error_kind(&failure.error),
                client_id = %ctx.caller.client.client_id,
                call_id = %ctx.caller.client.call_id,
                inode_id = inode_id.as_raw(),
                final_size = args.final_size,
                committed_block_count,
                committed_bytes,
                lease_epoch = handle.lease_epoch.as_raw(),
                mount_epoch = failure.mount_epoch,
                route_epoch = failure.route_epoch,
                "CommitFile rejected"
            ),
        }
        result
    }

    pub(crate) async fn sync_write(&self, ctx: &RequestContext, args: SyncWriteArgs) -> FsResult<SyncWriteOutput> {
        if let Some(failure) = self.session_write_admission_failure(ctx, args.handle.inode_id) {
            return self.failure_from_admission(failure);
        }
        let inode_id = args.handle.inode_id;
        if args
            .committed_blocks
            .iter()
            .any(|block| block.block_id.inode_id != inode_id)
        {
            return self.failure_from_error(
                ctx,
                MetadataError::InvalidArgument("committed block inode_id does not match request".to_string()),
                None,
                None,
            );
        }

        let handle = args.handle;
        self.sync_write_session(
            ctx,
            handle,
            CloseWriteIntent {
                committed_blocks: args.committed_blocks,
                final_size: args.target_size,
                expected_file_size: args.expected_file_size,
            },
            args.freshness,
            args.expected_generation,
            args.publish_mode,
        )
        .await
    }

    fn publish_mode_for_session(session: &WriteSession) -> PublishMode {
        match session.mode {
            WriteMode::Overwrite => PublishMode::ReplaceIfUnchanged,
            WriteMode::Append => PublishMode::AppendIfUnchanged,
        }
    }

    fn active_publish_session(
        &self,
        ctx: &RequestContext,
        inode_id: InodeId,
        lease_epoch: LeaseEpoch,
        publish_mode: PublishMode,
        operation: &'static str,
    ) -> Result<Option<WriteSession>, FsFailure> {
        let Some(session) = self.session_registry.get_session(inode_id) else {
            return Ok(None);
        };
        let invalid = |message| match self.session_terminal_failure::<()>(
            ctx,
            ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
            message,
            None,
            None,
        ) {
            Err(failure) => failure,
            Ok(_) => unreachable!("session_terminal_failure always returns Err"),
        };
        if session.open_client_id != ctx.caller.client.client_id {
            return Err(invalid(format!("{operation} client does not own inode_id={inode_id}")));
        }
        if session.lease_epoch != lease_epoch || Self::publish_mode_for_session(&session) != publish_mode {
            return Err(invalid(format!(
                "{operation} publish precondition does not match the active session"
            )));
        }
        Ok(Some(session))
    }

    /// Freeze the current issued-target sequence before validating publication.
    fn begin_write_publication<'a>(
        &'a self,
        ctx: &RequestContext,
        inode_id: InodeId,
        lease_epoch: LeaseEpoch,
        publish_mode: PublishMode,
        operation: &'static str,
    ) -> Result<WritePublication<'a>, FsFailure> {
        let publication = match self.session_registry.begin_publication(inode_id, lease_epoch) {
            Ok(publication) => publication,
            Err(BeginWritePublicationError::Session(message)) => {
                return Err(self
                    .session_terminal_failure::<()>(
                        ctx,
                        ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                        format!("{operation} write session is no longer current: {message}"),
                        None,
                        None,
                    )
                    .expect_err("session_terminal_failure always returns Err"));
            }
            Err(BeginWritePublicationError::AddBlockPending) => {
                return Err(self
                    .failure_from_error::<()>(
                        ctx,
                        MetadataError::Again(format!(
                            "{operation} cannot freeze inode_id={inode_id} while AddBlock is pending"
                        )),
                        None,
                        None,
                    )
                    .expect_err("failure_from_error always returns Err"));
            }
            Err(BeginWritePublicationError::PublicationInProgress) => {
                return Err(self
                    .failure_from_error::<()>(
                        ctx,
                        MetadataError::Again(format!(
                            "another file publication is already in progress for inode_id={inode_id}"
                        )),
                        None,
                        None,
                    )
                    .expect_err("failure_from_error always returns Err"));
            }
            Err(BeginWritePublicationError::PublicationIdExhausted) => {
                return Err(self
                    .failure_from_error::<()>(
                        ctx,
                        MetadataError::Internal("write publication identity exhausted".to_string()),
                        None,
                        None,
                    )
                    .expect_err("failure_from_error always returns Err"));
            }
        };
        let session = publication.session();
        if session.open_client_id != ctx.caller.client.client_id
            || Self::publish_mode_for_session(session) != publish_mode
        {
            return Err(self
                .session_terminal_failure::<()>(
                    ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                    format!("{operation} publish precondition does not match the active session"),
                    None,
                    None,
                )
                .expect_err("session_terminal_failure always returns Err"));
        }
        Ok(publication)
    }

    /// Resolve an ambiguous publish from the durable file state.
    ///
    /// This is state-equivalence recovery, not historical request replay. Once
    /// the requested postcondition is visible at the next content generation,
    /// preconditions such as the original publish mode are no longer
    /// distinguishable without persisting request history.
    fn resolve_published_state(
        &self,
        inode_id: InodeId,
        lease_epoch: LeaseEpoch,
        intent: &CloseWriteIntent,
        expected_generation: ContentGeneration,
        mode: PublishMode,
    ) -> MetadataResult<Option<(InodeId, MountId, ContentGeneration, LeaseEpoch)>> {
        let inode = self
            .read_inode(inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {inode_id}")))?;
        if inode.inode_id != inode_id {
            return Err(MetadataError::Internal(format!(
                "inode key {inode_id} contains inode {}",
                inode.inode_id
            )));
        }
        let (visible_extents, generation, stored_lease_epoch) = match &inode.data {
            InodeData::File {
                extents,
                generation,
                lease_epoch,
                ..
            } => (extents, generation.unwrap_or_default(), lease_epoch.unwrap_or_default()),
            _ => {
                return Err(MetadataError::InvalidArgument(format!(
                    "Inode is not a file: {inode_id}"
                )))
            }
        };
        if stored_lease_epoch != lease_epoch && lease_epoch.checked_next() != Some(stored_lease_epoch) {
            return Err(MetadataError::LeaseFenced {
                expected: stored_lease_epoch,
                got: lease_epoch,
            });
        }

        let mut blocks = intent.committed_blocks.iter().collect::<Vec<_>>();
        blocks.sort_by_key(|block| (block.file_offset, block.block_id.index.as_raw()));
        let mut seen = HashSet::with_capacity(blocks.len());
        let start_offset = match mode {
            PublishMode::ReplaceIfUnchanged => 0,
            PublishMode::AppendIfUnchanged => intent.expected_file_size,
        };
        let mut expected_offset = start_offset;
        for block in &blocks {
            if block.len == 0
                || block.block_id.inode_id != inode_id
                || !seen.insert(block.block_id)
                || block.file_offset != expected_offset
            {
                return Err(MetadataError::InvalidArgument(
                    "completed publish payload is not a contiguous set of unique blocks".to_string(),
                ));
            }
            expected_offset = block.file_offset.checked_add(block.len).ok_or_else(|| {
                MetadataError::InvalidArgument("completed publish block range overflows u64".to_string())
            })?;
        }
        if expected_offset != intent.final_size {
            return Err(MetadataError::InvalidArgument(format!(
                "completed publish payload ends at {expected_offset}, expected {}",
                intent.final_size
            )));
        }
        let mut visible = visible_extents
            .iter()
            .filter(|extent| extent.file_offset >= start_offset)
            .collect::<Vec<_>>();
        visible.sort_by_key(|extent| (extent.file_offset, extent.block_id.index.as_raw()));
        let state_matches = inode.attrs.size == intent.final_size
            && visible.len() == blocks.len()
            && visible.iter().zip(blocks.iter()).all(|(extent, block)| {
                extent.block_id == block.block_id
                    && extent.file_offset == block.file_offset
                    && extent.block_offset == 0
                    && extent.len == block.len
            });
        if expected_generation.checked_next() == Some(generation) && state_matches {
            return Ok(Some((inode_id, inode.mount_id, generation, stored_lease_epoch)));
        }
        if generation == expected_generation && intent.committed_blocks.is_empty() && state_matches {
            return Ok(Some((inode_id, inode.mount_id, generation, stored_lease_epoch)));
        }
        if generation != expected_generation {
            return Err(MetadataError::Again(format!(
                "content generation changed for inode {inode_id}: expected {expected_generation}, current {generation}"
            )));
        }
        Ok(None)
    }

    async fn completed_publish_hints(
        &self,
        ctx: &RequestContext,
        freshness: Freshness,
        mount_id: MountId,
        operation: &'static str,
    ) -> Result<(Option<GroupName>, Option<u64>, Option<u64>), FsFailure> {
        let (group_name, mount_epoch) = self
            .freshness_validator
            .validate_mount_epoch(ctx, freshness, mount_id)?;
        let route_epoch = self
            .freshness_validator
            .validate_route_epoch(ctx, freshness, group_name.clone(), mount_epoch, operation)
            .await?;
        Ok((group_name, mount_epoch, route_epoch))
    }

    /// Select blocks that require Ready evidence for the next publication.
    ///
    /// A target is historical only when the durable inode already contains the
    /// exact extent and stamp. An older, uncommitted target must fail closed:
    /// publishing it under a later generation would make Metadata and Worker
    /// disagree about the block stamp.
    fn publication_ready_targets(
        &self,
        session: &WriteSession,
        committed_blocks: &[CommittedBlock],
        expected_generation: ContentGeneration,
    ) -> MetadataResult<Vec<PublishReadyTarget>> {
        let new_block_stamp = expected_generation
            .checked_next()
            .ok_or_else(|| MetadataError::InvalidArgument("content generation overflow".to_string()))?;
        let inode = self
            .read_inode(session.inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {}", session.inode_id)))?;
        let (visible_extents, durable_generation) = match &inode.data {
            InodeData::File {
                extents, generation, ..
            } => (extents, generation.unwrap_or_default()),
            _ => {
                return Err(MetadataError::InvalidArgument(format!(
                    "Inode is not a file: {}",
                    session.inode_id
                )))
            }
        };
        if durable_generation != expected_generation {
            return Err(MetadataError::StaleState(format!(
                "content generation changed for inode {}: expected {expected_generation}, current {durable_generation}",
                session.inode_id
            )));
        }

        let issued = session
            .issued_targets
            .iter()
            .map(|target| (target.block_id, target))
            .collect::<HashMap<_, _>>();
        let mut ready_targets = Vec::new();
        for block in committed_blocks {
            let target = issued.get(&block.block_id).ok_or_else(|| {
                MetadataError::InvalidArgument(format!("Committed block {} was not issued by AddBlock", block.block_id))
            })?;
            let already_visible = visible_extents.iter().any(|extent| {
                extent.block_id == block.block_id
                    && extent.file_offset == block.file_offset
                    && extent.block_offset == 0
                    && extent.len == block.len
                    && extent.block_stamp == Some(target.block_stamp)
            });
            if already_visible {
                continue;
            }
            if target.block_stamp != new_block_stamp.as_raw() {
                return Err(MetadataError::InvalidArgument(format!(
                    "Committed block {} was issued for block stamp {} but is not visible at content generation {expected_generation}",
                    block.block_id, target.block_stamp
                )));
            }
            ready_targets.push(PublishReadyTarget {
                target: (*target).clone(),
                effective_len: block.len,
            });
        }
        Ok(ready_targets)
    }

    fn publish_ready_refresh_failure(
        &self,
        ctx: &RequestContext,
        kind: ErrorKind,
        message: impl Into<String>,
        group_name: &GroupName,
        epochs: (Option<u64>, Option<u64>),
        worker_resolve_required: bool,
    ) -> FsFailure {
        match self.refresh_metadata_failure_with_hint::<()>(
            ctx,
            kind,
            message,
            Some(group_name.clone()),
            epochs.0,
            epochs.1,
            Some(RefreshHint {
                worker_resolve_required,
                ..Default::default()
            }),
        ) {
            Err(failure) => failure,
            Ok(_) => unreachable!("refresh_metadata_failure_with_hint always returns Err"),
        }
    }

    fn publish_ready_conflict_failure(
        &self,
        ctx: &RequestContext,
        conflict: PublishReadyConflict,
        group_name: &GroupName,
        mount_epoch: Option<u64>,
        route_epoch: Option<u64>,
    ) -> FsFailure {
        match conflict {
            PublishReadyConflict::MissingWriteEndpoint { block_id } => self.publish_ready_refresh_failure(
                ctx,
                ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
                format!("block {block_id} has no metadata-authorized write endpoint"),
                group_name,
                (mount_epoch, route_epoch),
                true,
            ),
            PublishReadyConflict::WorkerRunMismatch {
                block_id,
                worker_id,
                expected,
                current,
            } => self.publish_ready_refresh_failure(
                ctx,
                ErrorKind::Worker(WorkerErrorKind::RunMismatch),
                format!(
                    "worker run changed before publishing block {block_id}: worker_id={}, expected={expected}, current={current:?}",
                    worker_id.as_raw()
                ),
                group_name,
                (mount_epoch, route_epoch),
                true,
            ),
            PublishReadyConflict::EndpointMismatch { block_id, worker_id } => self.publish_ready_refresh_failure(
                ctx,
                ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
                format!(
                    "worker endpoint changed before publishing block {block_id}: worker_id={}",
                    worker_id.as_raw()
                ),
                group_name,
                (mount_epoch, route_epoch),
                true,
            ),
            PublishReadyConflict::BlockStampMismatch {
                block_id,
                worker_id,
                expected,
                reported,
            } => self.publish_ready_refresh_failure(
                ctx,
                ErrorKind::Worker(WorkerErrorKind::BlockStampMismatch),
                format!(
                    "worker reported the wrong stamp for block {block_id}: worker_id={}, expected={expected}, reported={reported}",
                    worker_id.as_raw()
                ),
                group_name,
                (mount_epoch, route_epoch),
                false,
            ),
            PublishReadyConflict::EffectiveLenMismatch {
                block_id,
                worker_id,
                expected,
                reported,
            } => self
                .failure_from_error_with_route_epoch::<()>(
                    ctx,
                    MetadataError::InvalidArgument(format!(
                        "worker effective length does not match the committed block: block_id={block_id}, worker_id={}, expected={expected}, reported={reported}",
                        worker_id.as_raw()
                    )),
                    Some(group_name.clone()),
                    mount_epoch,
                    route_epoch,
                )
                .expect_err("failure_from_error_with_route_epoch always returns Err"),
            PublishReadyConflict::UnreadableBlock {
                block_id,
                worker_id,
                state,
            } => self.publish_ready_refresh_failure(
                ctx,
                ErrorKind::Worker(WorkerErrorKind::Corrupt),
                format!(
                    "worker reported an unreadable block before publication: block_id={block_id}, worker_id={}, state={state:?}",
                    worker_id.as_raw()
                ),
                group_name,
                (mount_epoch, route_epoch),
                false,
            ),
        }
    }

    /// Wait until every new target has current Ready evidence or the request
    /// deadline expires.
    ///
    /// The watch receiver is created before the first snapshot check, so a
    /// report applied between checking and awaiting remains observable. No
    /// WorkerManager lock is held across the await.
    async fn wait_for_publish_ready(
        &self,
        ctx: &RequestContext,
        group_name: &GroupName,
        mount_epoch: Option<u64>,
        route_epoch: Option<u64>,
        targets: &[PublishReadyTarget],
    ) -> Result<(), FsFailure> {
        if targets.is_empty() {
            return Ok(());
        }
        let Some(worker_manager) = self.worker_manager.as_ref() else {
            return Err(self.publish_ready_refresh_failure(
                ctx,
                ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
                "worker observations are unavailable for file publication",
                group_name,
                (mount_epoch, route_epoch),
                false,
            ));
        };

        let mut observations = worker_manager.subscribe_publication_observations();
        loop {
            let pending_block_id = match worker_manager.check_publish_ready(group_name, targets) {
                PublishReadyStatus::Ready => return Ok(()),
                PublishReadyStatus::Pending { block_id } => block_id,
                PublishReadyStatus::Conflict(conflict) => {
                    return Err(self.publish_ready_conflict_failure(
                        ctx,
                        conflict,
                        group_name,
                        mount_epoch,
                        route_epoch,
                    ));
                }
            };

            let remaining = ctx.caller.deadline.remaining();
            if remaining.is_zero() {
                return Err(self.publish_ready_refresh_failure(
                    ctx,
                    ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
                    format!("deadline expired while waiting for Ready report for block {pending_block_id}"),
                    group_name,
                    (mount_epoch, route_epoch),
                    false,
                ));
            }
            match tokio::time::timeout(remaining, observations.changed()).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => {
                    return Err(self
                        .failure_from_error_with_route_epoch::<()>(
                            ctx,
                            MetadataError::Internal("worker publication observation channel closed".to_string()),
                            Some(group_name.clone()),
                            mount_epoch,
                            route_epoch,
                        )
                        .expect_err("failure_from_error_with_route_epoch always returns Err"));
                }
                Err(_) => {
                    return Err(self.publish_ready_refresh_failure(
                        ctx,
                        ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
                        format!("deadline expired while waiting for Ready report for block {pending_block_id}"),
                        group_name,
                        (mount_epoch, route_epoch),
                        false,
                    ));
                }
            }
        }
    }

    /// Perform the non-waiting Ready recheck immediately before proposal.
    fn require_publish_ready(
        &self,
        ctx: &RequestContext,
        group_name: &GroupName,
        mount_epoch: Option<u64>,
        route_epoch: Option<u64>,
        targets: &[PublishReadyTarget],
    ) -> Result<(), FsFailure> {
        if targets.is_empty() {
            return Ok(());
        }
        let Some(worker_manager) = self.worker_manager.as_ref() else {
            return Err(self.publish_ready_refresh_failure(
                ctx,
                ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
                "worker observations are unavailable for file publication",
                group_name,
                (mount_epoch, route_epoch),
                false,
            ));
        };
        match worker_manager.check_publish_ready(group_name, targets) {
            PublishReadyStatus::Ready => Ok(()),
            PublishReadyStatus::Pending { block_id } => Err(self.publish_ready_refresh_failure(
                ctx,
                ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
                format!("Ready evidence changed before publishing block {block_id}"),
                group_name,
                (mount_epoch, route_epoch),
                false,
            )),
            PublishReadyStatus::Conflict(conflict) => {
                Err(self.publish_ready_conflict_failure(ctx, conflict, group_name, mount_epoch, route_epoch))
            }
        }
    }

    /// Reject a new publication after the caller's deadline has expired.
    ///
    /// Durable replay is resolved before this guard. This check protects the
    /// final proposal boundary after all asynchronous authority and Ready
    /// revalidation has completed.
    fn require_publish_deadline(
        &self,
        ctx: &RequestContext,
        group_name: Option<&GroupName>,
        mount_epoch: Option<u64>,
        route_epoch: Option<u64>,
    ) -> Result<(), FsFailure> {
        if !ctx.caller.deadline.has_passed() {
            return Ok(());
        }
        match group_name {
            Some(group_name) => Err(self.publish_ready_refresh_failure(
                ctx,
                ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
                "deadline expired before file publication",
                group_name,
                (mount_epoch, route_epoch),
                false,
            )),
            None => Err(self
                .failure_from_error_with_route_epoch::<()>(
                    ctx,
                    MetadataError::Again("deadline expired before file publication".to_string()),
                    None,
                    mount_epoch,
                    route_epoch,
                )
                .expect_err("expired publication deadline must fail")),
        }
    }

    /// Revalidate leader-local session and lease state after an asynchronous
    /// Ready wait. A caller may proceed only with the same publication
    /// preconditions that were used to select the target set.
    async fn revalidate_publish_session(
        &self,
        ctx: &RequestContext,
        publication: &WritePublication<'_>,
        publish_mode: PublishMode,
        operation: &'static str,
    ) -> Result<WriteSession, FsFailure> {
        let expected = publication.session();
        if let Some(failure) = self.session_write_admission_failure(ctx, expected.inode_id) {
            return Err(self
                .failure_from_admission::<()>(failure)
                .expect_err("failure_from_admission always returns Err"));
        }
        let current = publication.revalidate().map_err(|message| {
            self.session_terminal_failure::<()>(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!("{operation} write publication changed while waiting for Ready block reports: {message}"),
                None,
                None,
            )
            .expect_err("session_terminal_failure always returns Err")
        })?;
        if current.open_client_id != ctx.caller.client.client_id
            || Self::publish_mode_for_session(&current) != publish_mode
        {
            return Err(self
                .session_terminal_failure::<()>(
                    ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                    format!("{operation} publish precondition does not match the active session"),
                    None,
                    None,
                )
                .expect_err("session_terminal_failure always returns Err"));
        }
        if current.inode_id != expected.inode_id
            || current.mount_id != expected.mount_id
            || current.base_size != expected.base_size
            || current.generation != expected.generation
        {
            return Err(self
                .session_terminal_failure::<()>(
                    ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                    format!("{operation} write session changed while waiting for Ready block reports"),
                    None,
                    None,
                )
                .expect_err("session_terminal_failure always returns Err"));
        }
        if self
            .session_registry
            .validate_session(current.inode_id, current.lease_epoch)
            .is_err()
        {
            return Err(self
                .session_terminal_failure::<()>(
                    ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionExpired),
                    format!("{operation} lease expired while waiting for Ready block reports"),
                    None,
                    None,
                )
                .expect_err("session_terminal_failure always returns Err"));
        }
        Ok(current)
    }

    async fn sync_write_session(
        &self,
        ctx: &RequestContext,
        handle: WriteHandle,
        intent: CloseWriteIntent,
        freshness: Freshness,
        expected_generation: ContentGeneration,
        publish_mode: PublishMode,
    ) -> FsResult<SyncWriteOutput> {
        let inode_id = handle.inode_id;
        let lease_epoch = handle.lease_epoch;
        let active_session = match self.active_publish_session(ctx, inode_id, lease_epoch, publish_mode, "SyncWrite") {
            Ok(session) => session,
            Err(failure) => return Err(failure),
        };
        match self.resolve_published_state(inode_id, lease_epoch, &intent, expected_generation, publish_mode) {
            Ok(Some((_inode_id, mount_id, generation, _stored_lease_epoch))) => {
                let publication = if active_session.is_some() {
                    Some(self.begin_write_publication(ctx, inode_id, lease_epoch, publish_mode, "SyncWrite")?)
                } else {
                    None
                };
                if publication.as_ref().is_some_and(|publication| {
                    let session = publication.session();
                    session.generation != expected_generation && session.generation != generation
                }) {
                    return self.session_terminal_failure(
                        ctx,
                        ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                        "SyncWrite content generation does not match the active session".to_string(),
                        None,
                        None,
                    );
                }
                let (group_name, mount_epoch, route_epoch) = self
                    .completed_publish_hints(ctx, freshness, mount_id, "SyncWrite")
                    .await?;
                if let Some(publication) = publication {
                    if let Err(message) = publication.complete_sync(generation, intent.final_size) {
                        return self.failure_from_error(ctx, MetadataError::Internal(message), group_name, mount_epoch);
                    }
                }
                return self.success_with_route_epoch(
                    SyncWriteOutput {
                        synced_size: intent.final_size,
                        generation: Some(generation),
                    },
                    group_name,
                    mount_epoch,
                    route_epoch,
                );
            }
            Ok(None) => {}
            Err(err) => return self.failure_from_error(ctx, err, None, None),
        }
        let publication = match active_session {
            Some(_) => match self.begin_write_publication(ctx, inode_id, lease_epoch, publish_mode, "SyncWrite") {
                Ok(publication) => publication,
                Err(failure) => return Err(failure),
            },
            None => {
                return self.session_terminal_failure(
                    ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                    format!("write session not found for inode_id={}", inode_id),
                    None,
                    None,
                );
            }
        };
        let session = publication.session().clone();
        if session.generation != expected_generation {
            return self.session_terminal_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                "SyncWrite publish precondition does not match the active session".to_string(),
                None,
                None,
            );
        }
        let (group_name, mount_epoch) =
            match self
                .freshness_validator
                .validate_mount_epoch(ctx, freshness, session.mount_id)
            {
                Ok(hints) => hints,
                Err(err) => return Err(err),
            };

        let route_epoch = match self
            .freshness_validator
            .validate_route_epoch(ctx, freshness, group_name.clone(), mount_epoch, "SyncWrite")
            .await
        {
            Ok(route_epoch) => route_epoch,
            Err(err) => return Err(err),
        };

        for block in &intent.committed_blocks {
            if block.block_id.inode_id != session.inode_id {
                return self.failure_from_error_with_route_epoch(
                    ctx,
                    MetadataError::InvalidArgument(format!(
                        "SyncWrite committed block inode_id {} does not match write handle inode_id {}",
                        block.block_id.inode_id, session.inode_id
                    )),
                    group_name,
                    mount_epoch,
                    route_epoch,
                );
            }
        }

        if lease_epoch != session.lease_epoch {
            return self.session_terminal_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!(
                    "write handle epoch mismatch for inode_id={}: expected {}, got {}",
                    inode_id, session.lease_epoch, lease_epoch
                ),
                group_name,
                mount_epoch,
            );
        }
        if self
            .session_registry
            .validate_session(session.inode_id, lease_epoch)
            .is_err()
        {
            return self.session_terminal_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionExpired),
                format!("lease validation rejected for inode_id={}", inode_id,),
                group_name,
                mount_epoch,
            );
        }

        let intent = CloseWriteIntent {
            committed_blocks: intent.committed_blocks.clone(),
            final_size: intent.final_size,
            expected_file_size: intent.expected_file_size,
        };
        let extents = match Self::validate_committed_blocks(&intent, &session) {
            Ok(extents) => extents,
            Err(err) => {
                return Err(self.invalid_publication_failure(ctx, err.to_string(), group_name, mount_epoch));
            }
        };
        if let Err(error) = self.validate_final_extent_count(session.inode_id, &extents, publish_mode) {
            return self.failure_from_error_with_route_epoch(ctx, error, group_name, mount_epoch, route_epoch);
        }
        let worker_lookup_group_name =
            self.require_worker_lookup_group(ctx, group_name.clone(), mount_epoch, route_epoch, "SyncWrite")?;
        let new_targets = match self.publication_ready_targets(&session, &intent.committed_blocks, expected_generation)
        {
            Ok(targets) => targets,
            Err(err) => {
                return Err(self.invalid_publication_failure(ctx, err.to_string(), group_name, mount_epoch));
            }
        };
        self.wait_for_publish_ready(ctx, &worker_lookup_group_name, mount_epoch, route_epoch, &new_targets)
            .await?;

        let revalidated_freshness = Freshness {
            mount_epoch,
            route_epoch,
        };
        let (revalidated_group_name, revalidated_mount_epoch) =
            self.freshness_validator
                .validate_mount_epoch(ctx, revalidated_freshness, session.mount_id)?;
        if revalidated_group_name.as_ref() != Some(&worker_lookup_group_name) || revalidated_mount_epoch != mount_epoch
        {
            return self.failure_from_error_with_route_epoch(
                ctx,
                MetadataError::StaleState("SyncWrite mount authority changed during Ready wait".to_string()),
                revalidated_group_name,
                revalidated_mount_epoch,
                route_epoch,
            );
        }
        let route_epoch = self
            .freshness_validator
            .validate_route_epoch(ctx, revalidated_freshness, group_name.clone(), mount_epoch, "SyncWrite")
            .await?;
        let session = self
            .revalidate_publish_session(ctx, &publication, publish_mode, "SyncWrite")
            .await?;
        self.require_publish_ready(ctx, &worker_lookup_group_name, mount_epoch, route_epoch, &new_targets)?;

        let routed = match self.route_ctx_for_write_with_error_hints(
            ctx,
            &[session.inode_id],
            revalidated_freshness,
            group_name.clone(),
            mount_epoch,
        ) {
            Ok(ctx) => ctx,
            Err(failure) => return Err(failure),
        };
        self.require_publish_deadline(
            ctx,
            Some(&worker_lookup_group_name),
            Some(routed.mount_epoch),
            route_epoch,
        )?;

        let command = Command::PublishFile {
            proposed_at_ms: crate::raft::proposal_timestamp_ms(),
            inode_id: session.inode_id,
            extents,
            target_size: intent.final_size,
            expected_generation,
            expected_file_size: intent.expected_file_size,
            lease_epoch,
            mode: publish_mode,
        };
        let inode_id = session.inode_id;
        let generation = match self
            .propose_fs_write_command(command, move |success| match success {
                ApplySuccess::FilePublished {
                    inode_id: returned_inode_id,
                    generation,
                } if returned_inode_id == inode_id => Ok(generation),
                unexpected => Err(unexpected_raft_apply_success("PublishFile", unexpected)),
            })
            .await
        {
            Ok(generation) => generation,
            Err(err) => {
                return self.failure_from_error(ctx, err, Some(routed.group_name.clone()), Some(routed.mount_epoch));
            }
        };
        if let Err(message) = publication.complete_sync(generation, intent.final_size) {
            return self.failure_from_error(
                ctx,
                MetadataError::Internal(message),
                Some(routed.group_name.clone()),
                Some(routed.mount_epoch),
            );
        }

        self.success_with_route_epoch(
            SyncWriteOutput {
                synced_size: intent.final_size,
                generation: Some(generation),
            },
            Some(routed.group_name.clone()),
            Some(routed.mount_epoch),
            route_epoch,
        )
    }

    fn invalid_publication_failure(
        &self,
        ctx: &RequestContext,
        message: impl Into<String>,
        group_name: Option<GroupName>,
        mount_epoch: Option<u64>,
    ) -> FsFailure {
        fs_failure_from_metadata_error(
            ctx,
            MetadataError::InvalidArgument(message.into()),
            group_name,
            mount_epoch,
            None,
        )
    }

    fn block_end(block: &CommittedBlock) -> Option<u64> {
        block.file_offset.checked_add(block.len)
    }

    /// Validate the post-publication inline extent count before proposing Raft.
    ///
    /// Append retries may include extents that are already visible, so this
    /// counts only the requested suffix that apply would add. Apply repeats the
    /// check against authoritative state to close the read/proposal race.
    fn validate_final_extent_count(
        &self,
        inode_id: InodeId,
        requested_extents: &[Extent],
        mode: PublishMode,
    ) -> MetadataResult<()> {
        let final_extent_count = match mode {
            PublishMode::ReplaceIfUnchanged => requested_extents.len(),
            PublishMode::AppendIfUnchanged => {
                let inode = self
                    .read_inode(inode_id)?
                    .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {inode_id}")))?;
                let visible_extents = match &inode.data {
                    InodeData::File { extents, .. } => extents,
                    _ => {
                        return Err(MetadataError::InvalidArgument(format!(
                            "Inode is not a file: {inode_id}"
                        )))
                    }
                };
                if visible_extents.len() > MAX_FILE_EXTENTS {
                    return Err(MetadataError::ResourceExhausted(format!(
                        "persisted file extent count {} exceeds maximum {} for inode {inode_id}",
                        visible_extents.len(),
                        MAX_FILE_EXTENTS
                    )));
                }
                let added = requested_extents
                    .iter()
                    .filter(|candidate| {
                        !visible_extents.iter().any(|visible| {
                            visible.block_id == candidate.block_id
                                && visible.file_offset == candidate.file_offset
                                && visible.block_offset == candidate.block_offset
                                && visible.len == candidate.len
                        })
                    })
                    .count();
                visible_extents
                    .len()
                    .checked_add(added)
                    .ok_or_else(|| MetadataError::ResourceExhausted("final file extent count overflowed".to_string()))?
            }
        };
        if final_extent_count > MAX_FILE_EXTENTS {
            return Err(MetadataError::ResourceExhausted(format!(
                "final file extent count {final_extent_count} exceeds maximum {MAX_FILE_EXTENTS} for inode {inode_id}"
            )));
        }
        Ok(())
    }

    /// Convert client-reported actual lengths into extents under issued target bounds.
    ///
    /// A newly published partial block must be the session's last issued target;
    /// otherwise the next capacity-aligned target would leave a file gap.
    fn validate_committed_blocks(intent: &CloseWriteIntent, session: &WriteSession) -> MetadataResult<Vec<Extent>> {
        if intent.expected_file_size != session.base_size {
            return Err(MetadataError::InvalidArgument(format!(
                "Expected file size mismatch: session={}, request={}",
                session.base_size, intent.expected_file_size
            )));
        }
        let mut issued = HashMap::with_capacity(session.issued_targets.len());
        for target in &session.issued_targets {
            issued.insert(target.block_id, target);
        }
        let last_issued_block_id = session.issued_targets.last().map(|target| target.block_id);

        let mut seen = HashSet::with_capacity(intent.committed_blocks.len());
        let mut sorted = intent.committed_blocks.iter().collect::<Vec<_>>();
        sorted.sort_by_key(|block| (block.file_offset, block.block_id.index.as_raw()));
        let mut extents = Vec::with_capacity(sorted.len());
        let mut previous_end = None;

        for block in &sorted {
            if block.len == 0 {
                return Err(MetadataError::InvalidArgument(
                    "committed block len must be greater than 0".to_string(),
                ));
            }
            if block.block_id.inode_id != session.inode_id {
                return Err(MetadataError::InvalidArgument(format!(
                    "Committed block inode_id {} does not match write handle inode_id {}",
                    block.block_id.inode_id, session.inode_id
                )));
            }
            if !seen.insert(block.block_id) {
                return Err(MetadataError::InvalidArgument(format!(
                    "Committed block {} was submitted more than once",
                    block.block_id
                )));
            }
            let Some(target) = issued.get(&block.block_id).copied() else {
                return Err(MetadataError::InvalidArgument(format!(
                    "Committed block {} was not issued by AddBlock",
                    block.block_id
                )));
            };
            if block.file_offset != target.file_offset {
                return Err(MetadataError::InvalidArgument(format!(
                    "Committed block {} does not match its issued target offset: expected {}, got {}",
                    block.block_id, target.file_offset, block.file_offset
                )));
            }
            if block.len > target.block_size {
                return Err(MetadataError::InvalidArgument(format!(
                    "Committed block {} length {} exceeds its issued capacity {}",
                    block.block_id, block.len, target.block_size
                )));
            }
            let is_new_partial = target.file_offset >= session.base_size && block.len < target.block_size;
            if is_new_partial && last_issued_block_id != Some(block.block_id) {
                return Err(MetadataError::InvalidArgument(format!(
                    "partial committed block {} must be the last target issued by AddBlock",
                    block.block_id
                )));
            }
            let Some(end) = Self::block_end(block) else {
                return Err(MetadataError::InvalidArgument(
                    "committed block range overflows u64".to_string(),
                ));
            };
            if previous_end.map(|prev| block.file_offset < prev).unwrap_or(false) {
                return Err(MetadataError::InvalidArgument(
                    "committed blocks must not overlap".to_string(),
                ));
            }
            previous_end = Some(end);
            extents.push(Extent {
                file_offset: block.file_offset,
                block_id: block.block_id,
                block_offset: 0,
                len: block.len,
                generation: None,
                block_stamp: None,
            });
        }

        if sorted.is_empty() {
            let expected_final_size = match session.mode {
                WriteMode::Append => session.base_size,
                WriteMode::Overwrite => 0,
            };
            if intent.final_size != expected_final_size {
                return Err(MetadataError::InvalidArgument(format!(
                    "Final size mismatch: expected {}, got {}",
                    expected_final_size, intent.final_size
                )));
            }
            return Ok(extents);
        }

        match session.mode {
            WriteMode::Append => {
                let mut expected_offset = session.base_size;
                for block in &sorted {
                    if block.file_offset != expected_offset {
                        return Err(MetadataError::InvalidArgument(format!(
                            "Extent file_offset mismatch: expected {}, got {}",
                            expected_offset, block.file_offset
                        )));
                    }
                    expected_offset = Self::block_end(block).expect("checked above");
                }
                if intent.final_size != expected_offset {
                    return Err(MetadataError::InvalidArgument(format!(
                        "Final size mismatch: expected {}, got {} (append mode)",
                        expected_offset, intent.final_size
                    )));
                }
            }
            WriteMode::Overwrite => {
                let mut expected_offset = 0;
                for block in &sorted {
                    if block.file_offset != expected_offset {
                        return Err(MetadataError::InvalidArgument(format!(
                            "Extent file_offset mismatch: expected {}, got {}",
                            expected_offset, block.file_offset
                        )));
                    }
                    expected_offset = Self::block_end(block).expect("checked above");
                }
                if intent.final_size != expected_offset {
                    return Err(MetadataError::InvalidArgument(format!(
                        "Final size mismatch: expected {}, got {}",
                        expected_offset, intent.final_size
                    )));
                }
            }
        }

        Ok(extents)
    }

    pub(super) async fn close_write_session(
        &self,
        ctx: &RequestContext,
        handle: WriteHandle,
        intent: CloseWriteIntent,
        freshness: Freshness,
        expected_generation: ContentGeneration,
        publish_mode: PublishMode,
    ) -> FsResult<CloseWriteOutput> {
        let inode_id = handle.inode_id;
        let lease_epoch = handle.lease_epoch;
        let active_session = match self.active_publish_session(ctx, inode_id, lease_epoch, publish_mode, "CommitFile") {
            Ok(session) => session,
            Err(failure) => return Err(failure),
        };
        match self.resolve_published_state(inode_id, lease_epoch, &intent, expected_generation, publish_mode) {
            Ok(Some((_inode_id, mount_id, generation, stored_lease_epoch))) => {
                let publication = if active_session.is_some() {
                    Some(self.begin_write_publication(ctx, inode_id, lease_epoch, publish_mode, "CommitFile")?)
                } else {
                    None
                };
                if publication.as_ref().is_some_and(|publication| {
                    let session = publication.session();
                    session.generation != expected_generation && session.generation != generation
                }) {
                    return self.session_terminal_failure(
                        ctx,
                        ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                        "CommitFile content generation does not match the active session".to_string(),
                        None,
                        None,
                    );
                }
                let (group_name, mount_epoch, route_epoch) = self
                    .completed_publish_hints(ctx, freshness, mount_id, "CommitFile")
                    .await?;
                // A no-op close has no content mutation to prove that the initial
                // CreateFile write right ended, so advance its durable fence explicitly.
                if generation == expected_generation && stored_lease_epoch == lease_epoch {
                    let proposed_at_ms = crate::raft::proposal_timestamp_ms();
                    if active_session.is_none() {
                        let owner_matches = match self.storage.get_create_file_replay_for_inode(inode_id) {
                            Ok(Some(replay)) => {
                                replay.operation_id.client_id == ctx.caller.client.client_id
                                    && replay.inode_id == inode_id
                                    && replay.mount_id == mount_id
                                    && replay.lease_epoch == lease_epoch
                                    && replay.generation == expected_generation
                                    && replay.expires_at_ms > proposed_at_ms
                            }
                            Ok(None) => false,
                            Err(error) => return self.failure_from_error(ctx, error, group_name, mount_epoch),
                        };
                        if !owner_matches {
                            return self.session_terminal_failure(
                                ctx,
                                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                                format!("CommitFile cannot authenticate the durable owner for inode_id={inode_id}"),
                                group_name,
                                mount_epoch,
                            );
                        }
                    }
                    self.require_publish_deadline(ctx, group_name.as_ref(), mount_epoch, route_epoch)?;
                    let next_epoch = lease_epoch.checked_next();
                    if let Err(error) = self
                        .propose_fs_write_command(
                            Command::EndWriteLease {
                                proposed_at_ms,
                                inode_id,
                                lease_epoch,
                            },
                            move |success| match success {
                                ApplySuccess::WriteLeaseEnded {
                                    inode_id: returned_inode_id,
                                    lease_epoch: ended_epoch,
                                } if returned_inode_id == inode_id && Some(ended_epoch) == next_epoch => Ok(()),
                                unexpected => Err(unexpected_raft_apply_success("EndWriteLease", unexpected)),
                            },
                        )
                        .await
                    {
                        return self.failure_from_error(ctx, error, group_name, mount_epoch);
                    }
                }
                if let Some(publication) = publication {
                    if let Err(message) = publication.complete_commit() {
                        return self.failure_from_error(ctx, MetadataError::Internal(message), group_name, mount_epoch);
                    }
                }
                return self.success_with_route_epoch(
                    CloseWriteOutput {
                        committed_size: intent.final_size,
                    },
                    group_name,
                    mount_epoch,
                    route_epoch,
                );
            }
            Ok(None) => {}
            Err(err) => return self.failure_from_error(ctx, err, None, None),
        }
        let publication = match active_session {
            Some(_) => match self.begin_write_publication(ctx, inode_id, lease_epoch, publish_mode, "CommitFile") {
                Ok(publication) => publication,
                Err(failure) => return Err(failure),
            },
            None => {
                return self.session_terminal_failure(
                    ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                    format!("write session not found for inode_id={}", inode_id),
                    None,
                    None,
                );
            }
        };
        let session = publication.session().clone();
        if session.generation != expected_generation {
            return self.session_terminal_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                "CommitFile publish precondition does not match the active session".to_string(),
                None,
                None,
            );
        }
        let (group_name, mount_epoch) =
            match self
                .freshness_validator
                .validate_mount_epoch(ctx, freshness, session.mount_id)
            {
                Ok(hints) => hints,
                Err(err) => return Err(err),
            };

        let route_epoch = match self
            .freshness_validator
            .validate_route_epoch(ctx, freshness, group_name.clone(), mount_epoch, "CommitFile")
            .await
        {
            Ok(route_epoch) => route_epoch,
            Err(err) => return Err(err),
        };

        if lease_epoch != session.lease_epoch {
            return self.session_terminal_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!(
                    "write handle epoch mismatch for inode_id={}: expected {}, got {}",
                    inode_id, session.lease_epoch, lease_epoch,
                ),
                group_name,
                mount_epoch,
            );
        }
        if self
            .session_registry
            .validate_session(session.inode_id, lease_epoch)
            .is_err()
        {
            return self.session_terminal_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionExpired),
                format!("lease validation rejected for inode_id={}", inode_id),
                group_name,
                mount_epoch,
            );
        }

        let extents = match Self::validate_committed_blocks(&intent, &session) {
            Ok(extents) => extents,
            Err(err) => {
                return Err(self.invalid_publication_failure(ctx, err.to_string(), group_name.clone(), mount_epoch))
            }
        };
        if let Err(error) = self.validate_final_extent_count(session.inode_id, &extents, publish_mode) {
            return self.failure_from_error_with_route_epoch(ctx, error, group_name, mount_epoch, route_epoch);
        }
        let worker_lookup_group_name =
            self.require_worker_lookup_group(ctx, group_name.clone(), mount_epoch, route_epoch, "CommitFile")?;
        let new_targets = match self.publication_ready_targets(&session, &intent.committed_blocks, expected_generation)
        {
            Ok(targets) => targets,
            Err(err) => {
                return Err(self.invalid_publication_failure(ctx, err.to_string(), group_name.clone(), mount_epoch));
            }
        };
        self.wait_for_publish_ready(ctx, &worker_lookup_group_name, mount_epoch, route_epoch, &new_targets)
            .await?;

        let revalidated_freshness = Freshness {
            mount_epoch,
            route_epoch,
        };
        let (revalidated_group_name, revalidated_mount_epoch) =
            self.freshness_validator
                .validate_mount_epoch(ctx, revalidated_freshness, session.mount_id)?;
        if revalidated_group_name.as_ref() != Some(&worker_lookup_group_name) || revalidated_mount_epoch != mount_epoch
        {
            return self.failure_from_error_with_route_epoch(
                ctx,
                MetadataError::StaleState("CommitFile mount authority changed during Ready wait".to_string()),
                revalidated_group_name,
                revalidated_mount_epoch,
                route_epoch,
            );
        }
        let route_epoch = self
            .freshness_validator
            .validate_route_epoch(
                ctx,
                revalidated_freshness,
                group_name.clone(),
                mount_epoch,
                "CommitFile",
            )
            .await?;
        let session = self
            .revalidate_publish_session(ctx, &publication, publish_mode, "CommitFile")
            .await?;
        self.require_publish_ready(ctx, &worker_lookup_group_name, mount_epoch, route_epoch, &new_targets)?;

        let routed = match self.route_ctx_for_write_with_error_hints(
            ctx,
            &[session.inode_id],
            revalidated_freshness,
            group_name.clone(),
            mount_epoch,
        ) {
            Ok(ctx) => ctx,
            Err(failure) => return Err(failure),
        };
        self.require_publish_deadline(
            ctx,
            Some(&worker_lookup_group_name),
            Some(routed.mount_epoch),
            route_epoch,
        )?;

        let command = Command::PublishFile {
            proposed_at_ms: crate::raft::proposal_timestamp_ms(),
            inode_id: session.inode_id,
            extents,
            target_size: intent.final_size,
            expected_generation,
            expected_file_size: intent.expected_file_size,
            lease_epoch,
            mode: publish_mode,
        };
        let inode_id = session.inode_id;
        match self
            .propose_fs_write_command(command, move |success| match success {
                ApplySuccess::FilePublished {
                    inode_id: returned_inode_id,
                    ..
                } if returned_inode_id == inode_id => Ok(()),
                unexpected => Err(unexpected_raft_apply_success("PublishFile", unexpected)),
            })
            .await
        {
            Ok(()) => {}
            Err(err) => {
                return self.failure_from_error(ctx, err, Some(routed.group_name.clone()), Some(routed.mount_epoch));
            }
        }

        if let Err(message) = publication.complete_commit() {
            return self.failure_from_error(
                ctx,
                MetadataError::Internal(message),
                Some(routed.group_name.clone()),
                Some(routed.mount_epoch),
            );
        }

        self.success_with_route_epoch(
            CloseWriteOutput {
                committed_size: intent.final_size,
            },
            Some(routed.group_name.clone()),
            Some(routed.mount_epoch),
            route_epoch,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::filesystem::tests::*;
    use beryl_common::error::rpc::MetadataErrorKind;
    use beryl_common::Deadline;

    async fn open_write_with_target(env: &WriteFlowEnv) -> (OpenWriteOutput, WriteTarget) {
        let open = env
            .filesystem
            .open_write_inode(
                &request_context(),
                "/file".to_string(),
                env.inode_id,
                vec![env.inode_id],
                WriteMode::Overwrite,
                Freshness::default(),
            )
            .await
            .expect("open write")
            .payload;
        let target = add_block_for_key(&env.filesystem, &open).await;
        (open, target)
    }

    fn target_intent(target: &WriteTarget, expected_file_size: u64) -> CloseWriteIntent {
        CloseWriteIntent {
            committed_blocks: vec![committed_block(target.block_id, target.file_offset, 64)],
            final_size: target.file_offset + 64,
            expected_file_size,
        }
    }

    #[tokio::test]
    async fn commit_waits_for_ready_observation_then_publishes() {
        let env = write_flow_env(0).await;
        let open = env
            .filesystem
            .open_write_inode(
                &request_context(),
                "/file".to_string(),
                env.inode_id,
                vec![env.inode_id],
                WriteMode::Overwrite,
                Freshness::default(),
            )
            .await
            .expect("open write")
            .payload;
        let target = add_block_for_key(&env.filesystem, &open).await;
        let committed = vec![committed_block(target.block_id, target.file_offset, 64)];
        let commit = commit_for_key(&env.filesystem, &open, committed, 64);
        tokio::pin!(commit);

        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut commit)
                .await
                .is_err(),
            "publication must remain pending before the Ready report"
        );
        assert_eq!(stored_generation(&env.storage, env.inode_id), None);

        publish_env_write_target(&env, &target, 1);
        tokio::time::timeout(Duration::from_secs(2), &mut commit)
            .await
            .expect("Ready observation should wake publication")
            .expect("commit should succeed");
        assert_eq!(
            stored_generation(&env.storage, env.inode_id),
            Some(ContentGeneration::new(target.block_stamp))
        );
    }

    #[tokio::test]
    async fn deadline_expiring_after_ready_wait_does_not_publish() {
        let env = write_flow_env(0).await;
        let open = env
            .filesystem
            .open_write_inode(
                &request_context(),
                "/file".to_string(),
                env.inode_id,
                vec![env.inode_id],
                WriteMode::Overwrite,
                Freshness::default(),
            )
            .await
            .expect("open write")
            .payload;
        let target = add_block_for_key(&env.filesystem, &open).await;
        let mut ctx = request_context();
        ctx.caller.deadline = Deadline::from_now(Duration::from_millis(40));
        let commit = env.filesystem.close_write_session(
            &ctx,
            WriteHandle {
                inode_id: open.inode_id,
                lease_epoch: open.lease_epoch,
            },
            CloseWriteIntent {
                committed_blocks: vec![committed_block(target.block_id, target.file_offset, 64)],
                final_size: 64,
                expected_file_size: open.base_size,
            },
            Freshness::default(),
            open.generation,
            PublishMode::ReplaceIfUnchanged,
        );
        tokio::pin!(commit);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut commit)
                .await
                .is_err(),
            "publication must first wait for Ready"
        );
        publish_env_write_target(&env, &target, 1);
        std::thread::sleep(Duration::from_millis(50));

        commit
            .await
            .expect_err("deadline expiring after the wait must still prevent publication");
        assert_eq!(stored_generation(&env.storage, env.inode_id), None);
        assert!(env.filesystem.write_session_for_inode(open.inode_id).is_some());
    }

    #[tokio::test]
    async fn noop_close_requires_a_live_owner_and_deadline_before_ending_the_lease() {
        let env = write_flow_env(0).await;
        let open = env
            .filesystem
            .open_write_inode(
                &request_context(),
                "/file".to_string(),
                env.inode_id,
                vec![env.inode_id],
                WriteMode::Overwrite,
                Freshness::default(),
            )
            .await
            .expect("open write")
            .payload;
        let mut ctx = request_context();
        ctx.caller.deadline = Deadline::from_unix_ms(0);

        env.filesystem
            .close_write_session(
                &ctx,
                WriteHandle {
                    inode_id: open.inode_id,
                    lease_epoch: open.lease_epoch,
                },
                CloseWriteIntent {
                    committed_blocks: Vec::new(),
                    final_size: 0,
                    expected_file_size: 0,
                },
                Freshness::default(),
                open.generation,
                PublishMode::ReplaceIfUnchanged,
            )
            .await
            .expect_err("an expired no-op close must fail before ending the lease");

        let stored_epoch = env
            .storage
            .get_inode(open.inode_id)
            .unwrap()
            .and_then(|inode| match inode.data {
                InodeData::File { lease_epoch, .. } => lease_epoch,
                _ => None,
            });
        assert_eq!(stored_epoch, Some(open.lease_epoch));
        assert!(env.filesystem.write_session_for_inode(open.inode_id).is_some());

        let env = write_flow_env(0).await;
        let open = env
            .filesystem
            .open_write_inode(
                &request_context(),
                "/file".to_string(),
                env.inode_id,
                vec![env.inode_id],
                WriteMode::Overwrite,
                Freshness::default(),
            )
            .await
            .expect("open write")
            .payload;
        env.filesystem
            .session_registry()
            .remove_session_if_epoch(open.inode_id, open.lease_epoch)
            .expect("remove leader-local session");
        env.filesystem
            .close_write_session(
                &request_context(),
                WriteHandle {
                    inode_id: open.inode_id,
                    lease_epoch: open.lease_epoch,
                },
                CloseWriteIntent {
                    committed_blocks: Vec::new(),
                    final_size: 0,
                    expected_file_size: 0,
                },
                Freshness::default(),
                open.generation,
                PublishMode::ReplaceIfUnchanged,
            )
            .await
            .expect_err("sessionless OpenWrite close must not advance its durable fence");
        let stored_epoch = env
            .storage
            .get_inode(open.inode_id)
            .unwrap()
            .and_then(|inode| match inode.data {
                InodeData::File { lease_epoch, .. } => lease_epoch,
                _ => None,
            });
        assert_eq!(stored_epoch, Some(open.lease_epoch));
    }

    #[tokio::test]
    async fn session_removed_during_ready_wait_prevents_publication() {
        let env = write_flow_env(0).await;
        let (open, target) = open_write_with_target(&env).await;
        let ctx = request_context();
        let commit = env.filesystem.close_write_session(
            &ctx,
            WriteHandle {
                inode_id: open.inode_id,
                lease_epoch: open.lease_epoch,
            },
            target_intent(&target, open.base_size),
            Freshness::default(),
            open.generation,
            PublishMode::ReplaceIfUnchanged,
        );
        tokio::pin!(commit);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut commit)
                .await
                .is_err(),
            "CommitFile must wait for Ready"
        );
        env.filesystem
            .session_registry()
            .remove_session_if_epoch(open.inode_id, open.lease_epoch)
            .expect("remove active session");
        publish_env_write_target(&env, &target, 1);

        let failure = tokio::time::timeout(Duration::from_secs(2), &mut commit)
            .await
            .expect("session change must wake and finish publication")
            .expect_err("a removed session must fail closed");
        assert_eq!(
            failure.error.kind,
            ErrorKind::Metadata(MetadataErrorKind::SessionInvalid)
        );
        assert!(matches!(
            failure.error.recovery,
            RecoveryAction::ReopenWriteSession { .. }
        ));
        assert_eq!(stored_generation(&env.storage, env.inode_id), None);
    }

    #[tokio::test]
    async fn mount_change_during_ready_wait_prevents_publication() {
        let env = write_flow_env(0).await;
        let (open, target) = open_write_with_target(&env).await;
        let session = env
            .filesystem
            .write_session_for_inode(open.inode_id)
            .expect("active session");
        let ctx = request_context();
        let commit = env.filesystem.close_write_session(
            &ctx,
            WriteHandle {
                inode_id: open.inode_id,
                lease_epoch: open.lease_epoch,
            },
            target_intent(&target, open.base_size),
            Freshness::default(),
            open.generation,
            PublishMode::ReplaceIfUnchanged,
        );
        tokio::pin!(commit);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut commit)
                .await
                .is_err(),
            "CommitFile must wait for Ready"
        );
        let mount_table = env.filesystem.mount_table();
        let mut mount = mount_table
            .get_mount(session.mount_id)
            .expect("read mount")
            .expect("active mount");
        mount.mount_epoch += 1;
        mount_table.upsert(mount).expect("replace mount");
        publish_env_write_target(&env, &target, 1);

        let failure = tokio::time::timeout(Duration::from_secs(2), &mut commit)
            .await
            .expect("mount change must wake and finish publication")
            .expect_err("a changed mount must fail closed");
        assert_refresh_metadata(
            &failure.error,
            ErrorKind::Metadata(MetadataErrorKind::MountEpochMismatch),
        );
        assert_eq!(stored_generation(&env.storage, env.inode_id), None);
    }

    #[tokio::test]
    async fn leadership_loss_during_ready_wait_prevents_publication() {
        let env = write_flow_env(0).await;
        let (open, target) = open_write_with_target(&env).await;
        let ctx = request_context();
        let commit = env.filesystem.close_write_session(
            &ctx,
            WriteHandle {
                inode_id: open.inode_id,
                lease_epoch: open.lease_epoch,
            },
            target_intent(&target, open.base_size),
            Freshness::default(),
            open.generation,
            PublishMode::ReplaceIfUnchanged,
        );
        tokio::pin!(commit);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut commit)
                .await
                .is_err(),
            "CommitFile must wait for Ready"
        );
        env.filesystem
            .raft_node()
            .shutdown()
            .await
            .expect("stop Raft leadership");
        publish_env_write_target(&env, &target, 1);

        let failure = tokio::time::timeout(Duration::from_secs(2), &mut commit)
            .await
            .expect("leadership loss must wake and finish publication")
            .expect_err("a nonleader must fail closed");
        assert_refresh_metadata(&failure.error, ErrorKind::Metadata(MetadataErrorKind::NotLeader));
        assert_eq!(stored_generation(&env.storage, env.inode_id), None);
    }
}
