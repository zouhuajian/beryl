// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Durable file visibility publication for sync and commit.

use super::{Freshness, FsFailure, FsResult, MetadataFileSystem, PresentedWriteHandle, RequestContext};
use crate::error::{MetadataError, MetadataResult};
use crate::observe;
use crate::raft::{Command, FsCommandResult, PublishMode};
use crate::worker::{PublishReadyConflict, PublishReadyStatus};
use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, RefreshHint, WorkerErrorKind};
use beryl_types::fs::{Extent, FsErrorCode, InodeId};
use beryl_types::ids::MountId;
use beryl_types::{CommittedBlock, GroupName, WriteTarget, MAX_FILE_EXTENTS};
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
    pub(crate) content_revision: Option<u64>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct CloseWriteOutput {
    pub(crate) committed_size: u64,
}

pub(crate) struct CommitFileArgs {
    pub(crate) handle: PresentedWriteHandle,
    pub(crate) committed_blocks: Vec<CommittedBlock>,
    pub(crate) final_size: u64,
    pub(crate) freshness: Freshness,
    pub(crate) expected_content_revision: u64,
    pub(crate) expected_file_size: u64,
    pub(crate) publish_mode: PublishMode,
}

pub(crate) struct SyncWriteArgs {
    pub(crate) handle: PresentedWriteHandle,
    pub(crate) committed_blocks: Vec<CommittedBlock>,
    pub(crate) target_size: u64,
    pub(crate) freshness: Freshness,
    pub(crate) expected_content_revision: u64,
    pub(crate) expected_file_size: u64,
    pub(crate) publish_mode: PublishMode,
}

impl MetadataFileSystem {
    pub(crate) async fn commit_file(&self, ctx: &RequestContext, args: CommitFileArgs) -> FsResult<CloseWriteOutput> {
        if let Some(failure) = self.session_write_admission_failure(ctx, args.handle.inode_id).await {
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
                args.expected_content_revision,
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
                lease_epoch = handle.lease_epoch,
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
                lease_epoch = handle.lease_epoch,
                mount_epoch = failure.mount_epoch,
                route_epoch = failure.route_epoch,
                "CommitFile rejected"
            ),
        }
        result
    }

    pub(crate) async fn sync_write(&self, ctx: &RequestContext, args: SyncWriteArgs) -> FsResult<SyncWriteOutput> {
        if let Some(failure) = self.session_write_admission_failure(ctx, args.handle.inode_id).await {
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
            args.expected_content_revision,
            args.publish_mode,
        )
        .await
    }

    fn publish_mode_for_session(session: &crate::session_registry::WriteSession) -> PublishMode {
        match session.mode {
            crate::inode_lease::WriteMode::Write => PublishMode::ReplaceIfUnchanged,
            crate::inode_lease::WriteMode::Append => PublishMode::AppendIfUnchanged,
        }
    }

    fn active_publish_session(
        &self,
        ctx: &RequestContext,
        inode_id: InodeId,
        lease_epoch: u64,
        publish_mode: PublishMode,
        operation: &'static str,
    ) -> Result<Option<crate::session_registry::WriteSession>, FsFailure> {
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

    /// Resolve an ambiguous publish from the durable file state.
    ///
    /// This is state-equivalence recovery, not historical request replay. Once
    /// the requested postcondition is visible at the next content revision,
    /// preconditions such as the original publish mode are no longer
    /// distinguishable without persisting request history.
    fn resolve_published_state(
        &self,
        inode_id: InodeId,
        lease_epoch: u64,
        intent: &CloseWriteIntent,
        expected_content_revision: u64,
        mode: PublishMode,
    ) -> MetadataResult<Option<(InodeId, MountId, u64)>> {
        let inode = self
            .read_inode(inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {inode_id}")))?;
        if inode.inode_id != inode_id {
            return Err(MetadataError::Internal(format!(
                "inode key {inode_id} contains inode {}",
                inode.inode_id
            )));
        }
        let (visible_extents, content_revision, stored_lease_epoch) = match &inode.data {
            beryl_types::fs::InodeData::File {
                extents,
                content_revision,
                lease_epoch,
                ..
            } => (extents, content_revision.unwrap_or(0), lease_epoch.unwrap_or(0)),
            _ => {
                return Err(MetadataError::InvalidArgument(format!(
                    "Inode is not a file: {inode_id}"
                )))
            }
        };
        if stored_lease_epoch != lease_epoch && lease_epoch.checked_add(1) != Some(stored_lease_epoch) {
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
        if expected_content_revision.checked_add(1) == Some(content_revision) && state_matches {
            return Ok(Some((inode_id, inode.mount_id, content_revision)));
        }
        if content_revision == expected_content_revision && intent.committed_blocks.is_empty() && state_matches {
            return Ok(Some((inode_id, inode.mount_id, content_revision)));
        }
        if content_revision != expected_content_revision {
            return Err(MetadataError::Again(format!(
                "content revision changed for inode {inode_id}: expected {expected_content_revision}, current {content_revision}"
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
    /// publishing it under a later revision would make Metadata and Worker
    /// disagree about the block stamp.
    fn publication_ready_targets(
        &self,
        session: &crate::session_registry::WriteSession,
        committed_blocks: &[CommittedBlock],
        expected_content_revision: u64,
    ) -> MetadataResult<Vec<WriteTarget>> {
        let new_block_stamp = expected_content_revision
            .checked_add(1)
            .ok_or_else(|| MetadataError::InvalidArgument("content revision overflow".to_string()))?;
        let inode = self
            .read_inode(session.inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {}", session.inode_id)))?;
        let (visible_extents, durable_content_revision) = match &inode.data {
            beryl_types::fs::InodeData::File {
                extents,
                content_revision,
                ..
            } => (extents, content_revision.unwrap_or(0)),
            _ => {
                return Err(MetadataError::InvalidArgument(format!(
                    "Inode is not a file: {}",
                    session.inode_id
                )))
            }
        };
        if durable_content_revision != expected_content_revision {
            return Err(MetadataError::StaleState(format!(
                "content revision changed for inode {}: expected {expected_content_revision}, current {durable_content_revision}",
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
            if target.block_stamp != new_block_stamp {
                return Err(MetadataError::InvalidArgument(format!(
                    "Committed block {} was issued for block stamp {} but is not visible at content revision {expected_content_revision}",
                    block.block_id, target.block_stamp
                )));
            }
            ready_targets.push((*target).clone());
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
        targets: &[WriteTarget],
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
        targets: &[WriteTarget],
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
        group_name: &GroupName,
        mount_epoch: Option<u64>,
        route_epoch: Option<u64>,
    ) -> Result<(), FsFailure> {
        if !ctx.caller.deadline.has_passed() {
            return Ok(());
        }
        Err(self.publish_ready_refresh_failure(
            ctx,
            ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
            "deadline expired before file publication",
            group_name,
            (mount_epoch, route_epoch),
            false,
        ))
    }

    /// Revalidate leader-local session and lease state after an asynchronous
    /// Ready wait. A caller may proceed only with the same publication
    /// preconditions that were used to select the target set.
    async fn revalidate_publish_session(
        &self,
        ctx: &RequestContext,
        expected: &crate::session_registry::WriteSession,
        publish_mode: PublishMode,
        operation: &'static str,
    ) -> Result<crate::session_registry::WriteSession, FsFailure> {
        if let Some(failure) = self.session_write_admission_failure(ctx, expected.inode_id).await {
            return Err(self
                .failure_from_admission::<()>(failure)
                .expect_err("failure_from_admission always returns Err"));
        }
        let current =
            match self.active_publish_session(ctx, expected.inode_id, expected.lease_epoch, publish_mode, operation)? {
                Some(session) => session,
                None => {
                    return Err(self
                        .session_terminal_failure::<()>(
                            ctx,
                            ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                            format!("{operation} write session disappeared while waiting for Ready block reports"),
                            None,
                            None,
                        )
                        .expect_err("session_terminal_failure always returns Err"));
                }
            };
        if current.inode_id != expected.inode_id
            || current.mount_id != expected.mount_id
            || current.base_size != expected.base_size
            || current.content_revision != expected.content_revision
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
            .lease_manager
            .validate_lease(current.inode_id, current.lease_epoch)
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
        handle: PresentedWriteHandle,
        intent: CloseWriteIntent,
        freshness: Freshness,
        expected_content_revision: u64,
        publish_mode: PublishMode,
    ) -> FsResult<SyncWriteOutput> {
        let inode_id = handle.inode_id;
        let lease_epoch = handle.lease_epoch;
        let active_session = match self.active_publish_session(ctx, inode_id, lease_epoch, publish_mode, "SyncWrite") {
            Ok(session) => session,
            Err(failure) => return Err(failure),
        };
        match self.resolve_published_state(inode_id, lease_epoch, &intent, expected_content_revision, publish_mode) {
            Ok(Some((_inode_id, mount_id, content_revision))) => {
                if active_session.as_ref().is_some_and(|session| {
                    session.content_revision != expected_content_revision
                        && session.content_revision != content_revision
                }) {
                    return self.session_terminal_failure(
                        ctx,
                        ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                        "SyncWrite content revision does not match the active session".to_string(),
                        None,
                        None,
                    );
                }
                let (group_name, mount_epoch, route_epoch) = self
                    .completed_publish_hints(ctx, freshness, mount_id, "SyncWrite")
                    .await?;
                if active_session.is_some() {
                    let _ = self.session_registry.update_published_state(
                        inode_id,
                        lease_epoch,
                        content_revision,
                        intent.final_size,
                    );
                }
                return self.success_with_route_epoch(
                    ctx,
                    SyncWriteOutput {
                        synced_size: intent.final_size,
                        content_revision: Some(content_revision),
                    },
                    group_name,
                    mount_epoch,
                    route_epoch,
                );
            }
            Ok(None) => {}
            Err(err) => return self.failure_from_error(ctx, err, None, None),
        }
        let session = match active_session {
            Some(session) => session,
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
        if session.content_revision != expected_content_revision {
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
            .lease_manager
            .validate_lease(session.inode_id, lease_epoch)
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
                return Err(self.invalid_sync_write_failure(ctx, err.to_string(), group_name, mount_epoch));
            }
        };
        if let Err(error) = self.validate_final_extent_count(session.inode_id, &extents, publish_mode) {
            return self.failure_from_error_with_route_epoch(ctx, error, group_name, mount_epoch, route_epoch);
        }
        let worker_lookup_group_name =
            self.require_worker_lookup_group(ctx, group_name.clone(), mount_epoch, route_epoch, "SyncWrite")?;
        let new_targets =
            match self.publication_ready_targets(&session, &intent.committed_blocks, expected_content_revision) {
                Ok(targets) => targets,
                Err(err) => {
                    return Err(self.invalid_sync_write_failure(ctx, err.to_string(), group_name, mount_epoch));
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
            .revalidate_publish_session(ctx, &session, publish_mode, "SyncWrite")
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
        self.require_publish_deadline(ctx, &worker_lookup_group_name, Some(routed.mount_epoch), route_epoch)?;

        let command = Command::PublishFile {
            proposed_at_ms: crate::raft::proposal_timestamp_ms(),
            inode_id: session.inode_id,
            extents,
            target_size: intent.final_size,
            expected_content_revision,
            expected_file_size: intent.expected_file_size,
            lease_epoch,
            mode: publish_mode,
        };
        let content_revision = match self.propose_fs_write_command(command).await {
            Ok(FsCommandResult::Ok(ok)) => ok.content_revision,
            Ok(FsCommandResult::Err(err)) => {
                return self.fatal_fs_failure(
                    ctx,
                    err.errno,
                    err.message,
                    Some(routed.group_name.clone()),
                    Some(routed.mount_epoch),
                );
            }
            Err(err) => {
                return self.failure_from_error(ctx, err, Some(routed.group_name.clone()), Some(routed.mount_epoch));
            }
        };
        let content_revision = match content_revision {
            Some(content_revision) => content_revision,
            None => {
                return self.failure_from_error(
                    ctx,
                    MetadataError::Internal("PublishFile returned no content revision".to_string()),
                    Some(routed.group_name.clone()),
                    Some(routed.mount_epoch),
                )
            }
        };
        if let Err(message) =
            self.session_registry
                .update_published_state(inode_id, lease_epoch, content_revision, intent.final_size)
        {
            return self.failure_from_error(
                ctx,
                MetadataError::Internal(message),
                Some(routed.group_name.clone()),
                Some(routed.mount_epoch),
            );
        }

        self.success_with_route_epoch(
            ctx,
            SyncWriteOutput {
                synced_size: intent.final_size,
                content_revision: Some(content_revision),
            },
            Some(routed.group_name.clone()),
            Some(routed.mount_epoch),
            route_epoch,
        )
    }

    fn invalid_commit_failure(
        &self,
        ctx: &RequestContext,
        message: impl Into<String>,
        group_name: Option<GroupName>,
        mount_epoch: Option<u64>,
    ) -> FsFailure {
        match self.fatal_fs_failure::<()>(ctx, FsErrorCode::EInval, message, group_name, mount_epoch) {
            Err(failure) => failure,
            Ok(_) => unreachable!("fatal_fs_failure always returns Err"),
        }
    }

    fn invalid_sync_write_failure(
        &self,
        ctx: &RequestContext,
        message: impl Into<String>,
        group_name: Option<GroupName>,
        mount_epoch: Option<u64>,
    ) -> FsFailure {
        match self.fatal_fs_failure::<()>(ctx, FsErrorCode::EInval, message, group_name, mount_epoch) {
            Err(failure) => failure,
            Ok(_) => unreachable!("fatal_fs_failure always returns Err"),
        }
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
                    beryl_types::fs::InodeData::File { extents, .. } => extents,
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

    fn validate_committed_blocks(
        intent: &CloseWriteIntent,
        session: &crate::session_registry::WriteSession,
    ) -> MetadataResult<Vec<Extent>> {
        if intent.expected_file_size != session.base_size {
            return Err(MetadataError::InvalidArgument(format!(
                "Expected file size mismatch: session={}, request={}",
                session.base_size, intent.expected_file_size
            )));
        }
        let mut issued = HashMap::with_capacity(session.issued_targets.len());
        for target in &session.issued_targets {
            issued.insert(target.block_id, (target.file_offset, target.effective_len));
        }

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
            let Some((issued_offset, issued_len)) = issued.get(&block.block_id).copied() else {
                return Err(MetadataError::InvalidArgument(format!(
                    "Committed block {} was not issued by AddBlock",
                    block.block_id
                )));
            };
            if block.file_offset != issued_offset || block.len != issued_len {
                return Err(MetadataError::InvalidArgument(format!(
                    "Committed block {} does not match issued target: expected offset={} len={}, got offset={} len={}",
                    block.block_id, issued_offset, issued_len, block.file_offset, block.len
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
                content_revision: None,
                block_stamp: None,
            });
        }

        if sorted.is_empty() {
            let expected_final_size = match session.mode {
                crate::inode_lease::WriteMode::Append => session.base_size,
                crate::inode_lease::WriteMode::Write => 0,
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
            crate::inode_lease::WriteMode::Append => {
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
            crate::inode_lease::WriteMode::Write => {
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
        handle: PresentedWriteHandle,
        intent: CloseWriteIntent,
        freshness: Freshness,
        expected_content_revision: u64,
        publish_mode: PublishMode,
    ) -> FsResult<CloseWriteOutput> {
        let inode_id = handle.inode_id;
        let lease_epoch = handle.lease_epoch;
        let active_session = match self.active_publish_session(ctx, inode_id, lease_epoch, publish_mode, "CommitFile") {
            Ok(session) => session,
            Err(failure) => return Err(failure),
        };
        match self.resolve_published_state(inode_id, lease_epoch, &intent, expected_content_revision, publish_mode) {
            Ok(Some((_inode_id, mount_id, content_revision))) => {
                if active_session.as_ref().is_some_and(|session| {
                    session.content_revision != expected_content_revision
                        && session.content_revision != content_revision
                }) {
                    return self.session_terminal_failure(
                        ctx,
                        ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                        "CommitFile content revision does not match the active session".to_string(),
                        None,
                        None,
                    );
                }
                let (group_name, mount_epoch, route_epoch) = self
                    .completed_publish_hints(ctx, freshness, mount_id, "CommitFile")
                    .await?;
                if let Some(session) = self.session_registry.remove_session_if_epoch(inode_id, lease_epoch) {
                    self.lease_manager.release(session.inode_id, session.lease_epoch);
                }
                return self.success_with_route_epoch(
                    ctx,
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
        let session = match active_session {
            Some(session) => session,
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
        if session.content_revision != expected_content_revision {
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
            .lease_manager
            .validate_lease(session.inode_id, lease_epoch)
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
            Err(err) => return Err(self.invalid_commit_failure(ctx, err.to_string(), group_name.clone(), mount_epoch)),
        };
        if let Err(error) = self.validate_final_extent_count(session.inode_id, &extents, publish_mode) {
            return self.failure_from_error_with_route_epoch(ctx, error, group_name, mount_epoch, route_epoch);
        }
        let worker_lookup_group_name =
            self.require_worker_lookup_group(ctx, group_name.clone(), mount_epoch, route_epoch, "CommitFile")?;
        let new_targets =
            match self.publication_ready_targets(&session, &intent.committed_blocks, expected_content_revision) {
                Ok(targets) => targets,
                Err(err) => {
                    return Err(self.invalid_commit_failure(ctx, err.to_string(), group_name.clone(), mount_epoch));
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
            .revalidate_publish_session(ctx, &session, publish_mode, "CommitFile")
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
        self.require_publish_deadline(ctx, &worker_lookup_group_name, Some(routed.mount_epoch), route_epoch)?;

        let command = Command::PublishFile {
            proposed_at_ms: crate::raft::proposal_timestamp_ms(),
            inode_id: session.inode_id,
            extents,
            target_size: intent.final_size,
            expected_content_revision,
            expected_file_size: intent.expected_file_size,
            lease_epoch,
            mode: publish_mode,
        };
        let content_revision = match self.propose_fs_write_command(command).await {
            Ok(FsCommandResult::Ok(ok)) => ok.content_revision,
            Ok(FsCommandResult::Err(err)) => {
                return self.fatal_fs_failure(
                    ctx,
                    err.errno,
                    err.message,
                    Some(routed.group_name.clone()),
                    Some(routed.mount_epoch),
                );
            }
            Err(err) => {
                return self.failure_from_error(ctx, err, Some(routed.group_name.clone()), Some(routed.mount_epoch));
            }
        };
        if content_revision.is_none() {
            return self.failure_from_error(
                ctx,
                MetadataError::Internal("PublishFile returned no content revision".to_string()),
                Some(routed.group_name.clone()),
                Some(routed.mount_epoch),
            );
        }

        self.lease_manager.release(session.inode_id, session.lease_epoch);
        self.session_registry.remove_session_if_epoch(inode_id, lease_epoch);

        self.success_with_route_epoch(
            ctx,
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
    use crate::service::filesystem::test_support::*;
    use beryl_common::error::rpc::MetadataErrorKind;
    use beryl_common::Deadline;

    async fn open_write_with_target(env: &WriteFlowEnv) -> (OpenWriteOutput, WriteTarget) {
        let open = env
            .filesystem
            .open_write_inode(
                &request_context(),
                env.inode_id,
                vec![env.inode_id],
                crate::inode_lease::WriteMode::Write,
                Freshness::default(),
            )
            .await
            .expect("open write")
            .payload;
        let target = add_block_for_key(&env.filesystem, &open, 64).await;
        (open, target)
    }

    fn target_intent(target: &WriteTarget, expected_file_size: u64) -> CloseWriteIntent {
        CloseWriteIntent {
            committed_blocks: vec![committed_block(
                target.block_id,
                target.file_offset,
                target.effective_len,
            )],
            final_size: target.file_offset + target.effective_len,
            expected_file_size,
        }
    }

    #[tokio::test]
    async fn final_extent_limit_counts_only_new_append_extents() {
        let env = write_flow_env(0).await;
        let visible = (0..MAX_FILE_EXTENTS)
            .map(|index| Extent {
                file_offset: index as u64,
                block_id: beryl_types::BlockId::new(env.inode_id, beryl_types::BlockIndex::new(index as u32)),
                block_offset: 0,
                len: 1,
                content_revision: Some(1),
                block_stamp: Some(1),
            })
            .collect::<Vec<_>>();
        let mut inode = env.storage.get_inode(env.inode_id).unwrap().expect("file inode");
        let beryl_types::fs::InodeData::File { extents, .. } = &mut inode.data else {
            panic!("file inode must carry file data");
        };
        *extents = visible.clone();
        env.storage.put_inode(&inode).unwrap();

        env.filesystem
            .validate_final_extent_count(env.inode_id, &visible, PublishMode::AppendIfUnchanged)
            .expect("already-visible append extents must not be counted twice");

        let new_extent = Extent {
            file_offset: MAX_FILE_EXTENTS as u64,
            block_id: beryl_types::BlockId::new(env.inode_id, beryl_types::BlockIndex::new(MAX_FILE_EXTENTS as u32)),
            block_offset: 0,
            len: 1,
            content_revision: None,
            block_stamp: None,
        };
        let error = env
            .filesystem
            .validate_final_extent_count(env.inode_id, &[new_extent], PublishMode::AppendIfUnchanged)
            .expect_err("new append extent beyond the hard maximum must fail before proposal");
        assert!(matches!(error, MetadataError::ResourceExhausted(_)));
    }

    #[tokio::test]
    async fn final_extent_limit_rejects_oversized_persisted_state_before_append_scan() {
        let env = write_flow_env(0).await;
        let visible = (0..=MAX_FILE_EXTENTS)
            .map(|index| Extent {
                file_offset: index as u64,
                block_id: beryl_types::BlockId::new(env.inode_id, beryl_types::BlockIndex::new(index as u32)),
                block_offset: 0,
                len: 1,
                content_revision: Some(1),
                block_stamp: Some(1),
            })
            .collect::<Vec<_>>();
        let mut inode = env.storage.get_inode(env.inode_id).unwrap().expect("file inode");
        let beryl_types::fs::InodeData::File { extents, .. } = &mut inode.data else {
            panic!("file inode must carry file data");
        };
        *extents = visible.clone();
        env.storage.put_inode(&inode).unwrap();

        let error = env
            .filesystem
            .validate_final_extent_count(env.inode_id, &visible[..1], PublishMode::AppendIfUnchanged)
            .expect_err("oversized persisted extent state must fail before append matching");
        assert!(
            matches!(error, MetadataError::ResourceExhausted(message) if message.contains("persisted file extent count"))
        );

        let stored = env.storage.get_inode(env.inode_id).unwrap().expect("file inode");
        let beryl_types::fs::InodeData::File { extents, .. } = stored.data else {
            panic!("file inode must carry file data");
        };
        assert_eq!(extents.len(), MAX_FILE_EXTENTS + 1);
    }

    #[tokio::test]
    async fn commit_waits_for_ready_observation_then_publishes() {
        let env = write_flow_env(0).await;
        let open = env
            .filesystem
            .open_write_inode(
                &request_context(),
                env.inode_id,
                vec![env.inode_id],
                crate::inode_lease::WriteMode::Write,
                Freshness::default(),
            )
            .await
            .expect("open write")
            .payload;
        let target = add_block_for_key(&env.filesystem, &open, 64).await;
        let committed = vec![committed_block(
            target.block_id,
            target.file_offset,
            target.effective_len,
        )];
        let commit = commit_for_key(&env.filesystem, &open, committed, 64);
        tokio::pin!(commit);

        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut commit)
                .await
                .is_err(),
            "publication must remain pending before the Ready report"
        );
        assert_eq!(stored_content_revision(&env.storage, env.inode_id), None);

        publish_env_write_target(&env, &target, 1);
        tokio::time::timeout(Duration::from_secs(2), &mut commit)
            .await
            .expect("Ready observation should wake publication")
            .expect("commit should succeed");
        assert_eq!(
            stored_content_revision(&env.storage, env.inode_id),
            Some(target.block_stamp)
        );
    }

    #[tokio::test]
    async fn ready_deadline_preserves_the_previous_visible_revision() {
        let env = write_flow_env(0).await;
        let open = env
            .filesystem
            .open_write_inode(
                &request_context(),
                env.inode_id,
                vec![env.inode_id],
                crate::inode_lease::WriteMode::Write,
                Freshness::default(),
            )
            .await
            .expect("open write")
            .payload;
        let target = add_block_for_key(&env.filesystem, &open, 64).await;
        let mut ctx = request_context();
        ctx.caller.deadline = Deadline::from_now(Duration::from_millis(20));

        let failure = env
            .filesystem
            .close_write_session(
                &ctx,
                PresentedWriteHandle {
                    inode_id: open.inode_id,
                    lease_epoch: open.lease_epoch,
                },
                CloseWriteIntent {
                    committed_blocks: vec![committed_block(
                        target.block_id,
                        target.file_offset,
                        target.effective_len,
                    )],
                    final_size: 64,
                    expected_file_size: open.base_size,
                },
                Freshness::default(),
                open.content_revision,
                PublishMode::ReplaceIfUnchanged,
            )
            .await
            .expect_err("missing Ready evidence must not publish");

        assert_block_location_unavailable(&failure, target.block_id);
        assert_eq!(stored_content_revision(&env.storage, env.inode_id), None);
        assert!(env.filesystem.write_session_for_inode(open.inode_id).is_some());
    }

    #[tokio::test]
    async fn ready_commit_with_expired_deadline_does_not_publish() {
        let env = write_flow_env(0).await;
        let open = env
            .filesystem
            .open_write_inode(
                &request_context(),
                env.inode_id,
                vec![env.inode_id],
                crate::inode_lease::WriteMode::Write,
                Freshness::default(),
            )
            .await
            .expect("open write")
            .payload;
        let target = add_block_for_key(&env.filesystem, &open, 64).await;
        publish_env_write_target(&env, &target, 1);
        let mut ctx = request_context();
        ctx.caller.deadline = Deadline::from_unix_ms(0);

        let failure = env
            .filesystem
            .close_write_session(
                &ctx,
                PresentedWriteHandle {
                    inode_id: open.inode_id,
                    lease_epoch: open.lease_epoch,
                },
                CloseWriteIntent {
                    committed_blocks: vec![committed_block(
                        target.block_id,
                        target.file_offset,
                        target.effective_len,
                    )],
                    final_size: 64,
                    expected_file_size: open.base_size,
                },
                Freshness::default(),
                open.content_revision,
                PublishMode::ReplaceIfUnchanged,
            )
            .await
            .expect_err("expired CommitFile must not publish existing Ready evidence");

        assert_refresh_metadata(
            &failure.error,
            ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
        );
        assert!(failure
            .error
            .message
            .contains("deadline expired before file publication"));
        assert_eq!(stored_content_revision(&env.storage, env.inode_id), None);
        assert!(env.filesystem.write_session_for_inode(open.inode_id).is_some());
    }

    #[tokio::test]
    async fn ready_sync_with_expired_deadline_does_not_publish() {
        let env = write_flow_env(0).await;
        let open = env
            .filesystem
            .open_write_inode(
                &request_context(),
                env.inode_id,
                vec![env.inode_id],
                crate::inode_lease::WriteMode::Write,
                Freshness::default(),
            )
            .await
            .expect("open write")
            .payload;
        let target = add_block_for_key(&env.filesystem, &open, 64).await;
        publish_env_write_target(&env, &target, 1);
        let mut ctx = request_context();
        ctx.caller.deadline = Deadline::from_unix_ms(0);

        let failure = env
            .filesystem
            .sync_write_session(
                &ctx,
                PresentedWriteHandle {
                    inode_id: open.inode_id,
                    lease_epoch: open.lease_epoch,
                },
                CloseWriteIntent {
                    committed_blocks: vec![committed_block(
                        target.block_id,
                        target.file_offset,
                        target.effective_len,
                    )],
                    final_size: 64,
                    expected_file_size: open.base_size,
                },
                Freshness::default(),
                open.content_revision,
                PublishMode::ReplaceIfUnchanged,
            )
            .await
            .expect_err("expired SyncWrite must not publish existing Ready evidence");

        assert_refresh_metadata(
            &failure.error,
            ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
        );
        assert!(failure
            .error
            .message
            .contains("deadline expired before file publication"));
        assert_eq!(stored_content_revision(&env.storage, env.inode_id), None);
        assert!(env.filesystem.write_session_for_inode(open.inode_id).is_some());
    }

    #[tokio::test]
    async fn deadline_expiring_after_ready_wait_does_not_publish() {
        let env = write_flow_env(0).await;
        let open = env
            .filesystem
            .open_write_inode(
                &request_context(),
                env.inode_id,
                vec![env.inode_id],
                crate::inode_lease::WriteMode::Write,
                Freshness::default(),
            )
            .await
            .expect("open write")
            .payload;
        let target = add_block_for_key(&env.filesystem, &open, 64).await;
        let mut ctx = request_context();
        ctx.caller.deadline = Deadline::from_now(Duration::from_millis(40));
        let commit = env.filesystem.close_write_session(
            &ctx,
            PresentedWriteHandle {
                inode_id: open.inode_id,
                lease_epoch: open.lease_epoch,
            },
            CloseWriteIntent {
                committed_blocks: vec![committed_block(
                    target.block_id,
                    target.file_offset,
                    target.effective_len,
                )],
                final_size: 64,
                expected_file_size: open.base_size,
            },
            Freshness::default(),
            open.content_revision,
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
        assert_eq!(stored_content_revision(&env.storage, env.inode_id), None);
        assert!(env.filesystem.write_session_for_inode(open.inode_id).is_some());
    }

    #[tokio::test]
    async fn unpublished_old_stamp_target_cannot_enter_a_later_revision() {
        let env = write_flow_env(0).await;
        let open = env
            .filesystem
            .open_write_inode(
                &request_context(),
                env.inode_id,
                vec![env.inode_id],
                crate::inode_lease::WriteMode::Write,
                Freshness::default(),
            )
            .await
            .expect("open write")
            .payload;
        let first = add_block_for_key(&env.filesystem, &open, 64).await;
        let second = add_block_for_key(&env.filesystem, &open, 64).await;
        assert_eq!(first.block_stamp, 1);
        assert_eq!(second.block_stamp, 1);
        publish_env_write_target(&env, &first, 1);
        let first_committed = committed_block(first.block_id, first.file_offset, first.effective_len);
        env.filesystem
            .sync_write_session(
                &request_context(),
                PresentedWriteHandle {
                    inode_id: open.inode_id,
                    lease_epoch: open.lease_epoch,
                },
                CloseWriteIntent {
                    committed_blocks: vec![first_committed.clone()],
                    final_size: 64,
                    expected_file_size: 0,
                },
                Freshness::default(),
                0,
                PublishMode::ReplaceIfUnchanged,
            )
            .await
            .expect("publish the first target only");

        let synced = env
            .filesystem
            .write_session_for_inode(open.inode_id)
            .expect("session remains open");
        let failure = env
            .filesystem
            .sync_write_session(
                &request_context(),
                PresentedWriteHandle {
                    inode_id: open.inode_id,
                    lease_epoch: open.lease_epoch,
                },
                CloseWriteIntent {
                    committed_blocks: vec![
                        first_committed,
                        committed_block(second.block_id, second.file_offset, second.effective_len),
                    ],
                    final_size: 128,
                    expected_file_size: synced.base_size,
                },
                Freshness::default(),
                synced.content_revision,
                PublishMode::ReplaceIfUnchanged,
            )
            .await
            .expect_err("an uncommitted old-stamp target must fail closed");

        assert!(failure.error.message.contains("is not visible at content revision 1"));
        let inode = env.storage.get_inode(env.inode_id).unwrap().expect("stored inode");
        assert_eq!(inode.attrs.size, 64);
        let beryl_types::fs::InodeData::File {
            extents,
            content_revision,
            ..
        } = inode.data
        else {
            panic!("stored inode must be a file");
        };
        assert_eq!(content_revision, Some(1));
        assert_eq!(extents.len(), 1);
        assert_eq!(extents[0].block_id, first.block_id);
        assert_eq!(extents[0].block_stamp, Some(1));
    }

    #[tokio::test]
    async fn session_removed_during_ready_wait_prevents_publication() {
        let env = write_flow_env(0).await;
        let (open, target) = open_write_with_target(&env).await;
        let ctx = request_context();
        let commit = env.filesystem.close_write_session(
            &ctx,
            PresentedWriteHandle {
                inode_id: open.inode_id,
                lease_epoch: open.lease_epoch,
            },
            target_intent(&target, open.base_size),
            Freshness::default(),
            open.content_revision,
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
        assert_eq!(stored_content_revision(&env.storage, env.inode_id), None);
    }

    #[tokio::test]
    async fn lease_released_during_ready_wait_prevents_publication() {
        let env = write_flow_env(0).await;
        let (open, target) = open_write_with_target(&env).await;
        let ctx = request_context();
        let commit = env.filesystem.close_write_session(
            &ctx,
            PresentedWriteHandle {
                inode_id: open.inode_id,
                lease_epoch: open.lease_epoch,
            },
            target_intent(&target, open.base_size),
            Freshness::default(),
            open.content_revision,
            PublishMode::ReplaceIfUnchanged,
        );
        tokio::pin!(commit);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut commit)
                .await
                .is_err(),
            "CommitFile must wait for Ready"
        );
        env.filesystem.lease_manager().release(env.inode_id, open.lease_epoch);
        publish_env_write_target(&env, &target, 1);

        let failure = tokio::time::timeout(Duration::from_secs(2), &mut commit)
            .await
            .expect("lease change must wake and finish publication")
            .expect_err("a released lease must fail closed");
        assert_eq!(
            failure.error.kind,
            ErrorKind::Metadata(MetadataErrorKind::SessionExpired)
        );
        assert!(matches!(
            failure.error.recovery,
            RecoveryAction::ReopenWriteSession { .. }
        ));
        assert_eq!(stored_content_revision(&env.storage, env.inode_id), None);
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
            PresentedWriteHandle {
                inode_id: open.inode_id,
                lease_epoch: open.lease_epoch,
            },
            target_intent(&target, open.base_size),
            Freshness::default(),
            open.content_revision,
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
        assert_eq!(stored_content_revision(&env.storage, env.inode_id), None);
    }

    #[tokio::test]
    async fn route_change_during_ready_wait_prevents_publication() {
        let env = write_flow_env(0).await;
        let (open, target) = open_write_with_target(&env).await;
        let ctx = request_context();
        let commit = env.filesystem.close_write_session(
            &ctx,
            PresentedWriteHandle {
                inode_id: open.inode_id,
                lease_epoch: open.lease_epoch,
            },
            target_intent(&target, open.base_size),
            Freshness::default(),
            open.content_revision,
            PublishMode::ReplaceIfUnchanged,
        );
        tokio::pin!(commit);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut commit)
                .await
                .is_err(),
            "CommitFile must wait for Ready"
        );
        env.state_store.set_route_epoch(2);
        publish_env_write_target(&env, &target, 1);

        let failure = tokio::time::timeout(Duration::from_secs(2), &mut commit)
            .await
            .expect("route change must wake and finish publication")
            .expect_err("a changed route must fail closed");
        assert_refresh_metadata(
            &failure.error,
            ErrorKind::Metadata(MetadataErrorKind::RouteEpochMismatch),
        );
        assert_eq!(stored_content_revision(&env.storage, env.inode_id), None);
    }

    #[tokio::test]
    async fn leadership_loss_during_ready_wait_prevents_publication() {
        let env = write_flow_env(0).await;
        let (open, target) = open_write_with_target(&env).await;
        let ctx = request_context();
        let commit = env.filesystem.close_write_session(
            &ctx,
            PresentedWriteHandle {
                inode_id: open.inode_id,
                lease_epoch: open.lease_epoch,
            },
            target_intent(&target, open.base_size),
            Freshness::default(),
            open.content_revision,
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
        assert_eq!(stored_content_revision(&env.storage, env.inode_id), None);
    }

    #[tokio::test]
    async fn multiple_workers_require_every_target_ready_and_reject_deleting() {
        let env = write_flow_env(0).await;
        let open = env
            .filesystem
            .open_write_inode(
                &request_context(),
                env.inode_id,
                vec![env.inode_id],
                crate::inode_lease::WriteMode::Write,
                Freshness::default(),
            )
            .await
            .expect("open write")
            .payload;
        let first = add_block_for_key(&env.filesystem, &open, 64).await;
        let second = add_block_for_key(&env.filesystem, &open, 64).await;
        let first_worker = first.worker_endpoints.first().expect("first target worker");
        let second_worker = second.worker_endpoints.first().expect("second target worker");
        assert_ne!(
            first_worker.worker_id, second_worker.worker_id,
            "test requires targets on distinct workers"
        );
        let committed = vec![
            committed_block(first.block_id, first.file_offset, first.effective_len),
            committed_block(second.block_id, second.file_offset, second.effective_len),
        ];
        publish_env_write_target(&env, &first, 1);
        let ctx = request_context();
        let commit = env.filesystem.close_write_session(
            &ctx,
            PresentedWriteHandle {
                inode_id: open.inode_id,
                lease_epoch: open.lease_epoch,
            },
            CloseWriteIntent {
                committed_blocks: committed,
                final_size: 128,
                expected_file_size: 0,
            },
            Freshness::default(),
            0,
            PublishMode::ReplaceIfUnchanged,
        );
        tokio::pin!(commit);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut commit)
                .await
                .is_err(),
            "one Ready worker must not publish both targets"
        );
        publish_env_write_target(&env, &second, 1);
        tokio::time::timeout(Duration::from_secs(2), &mut commit)
            .await
            .expect("the second Ready report must wake publication")
            .expect("all targets are Ready");
        assert_eq!(stored_content_revision(&env.storage, env.inode_id), Some(1));

        let deleting_env = write_flow_env(0).await;
        let deleting_open = deleting_env
            .filesystem
            .open_write_inode(
                &request_context(),
                deleting_env.inode_id,
                vec![deleting_env.inode_id],
                crate::inode_lease::WriteMode::Write,
                Freshness::default(),
            )
            .await
            .expect("open deleting case")
            .payload;
        let ready = add_block_for_key(&deleting_env.filesystem, &deleting_open, 64).await;
        let deleting = add_block_for_key(&deleting_env.filesystem, &deleting_open, 64).await;
        publish_env_write_target(&deleting_env, &ready, 1);
        let deleting_worker = deleting.worker_endpoints.first().expect("deleting target worker");
        publish_report_block(
            deleting_env.filesystem.worker_manager.as_ref().expect("worker manager"),
            &deleting_env.group_name,
            deleting_worker.worker_id,
            1,
            report_block_with_stamp_and_state(deleting.block_id, deleting.block_stamp, BlockReportBlockState::Deleting),
        );
        let failure = deleting_env
            .filesystem
            .close_write_session(
                &request_context(),
                PresentedWriteHandle {
                    inode_id: deleting_open.inode_id,
                    lease_epoch: deleting_open.lease_epoch,
                },
                CloseWriteIntent {
                    committed_blocks: vec![
                        committed_block(ready.block_id, ready.file_offset, ready.effective_len),
                        committed_block(deleting.block_id, deleting.file_offset, deleting.effective_len),
                    ],
                    final_size: 128,
                    expected_file_size: 0,
                },
                Freshness::default(),
                0,
                PublishMode::ReplaceIfUnchanged,
            )
            .await
            .expect_err("Deleting evidence must fail the complete target set");
        assert_refresh_metadata(&failure.error, ErrorKind::Worker(WorkerErrorKind::Corrupt));
        assert_eq!(
            stored_content_revision(&deleting_env.storage, deleting_env.inode_id),
            None
        );
    }

    #[tokio::test]
    async fn close_after_sync_does_not_recheck_historical_ready_blocks() {
        let env = write_flow_env(0).await;
        let open = env
            .filesystem
            .open_write_inode(
                &request_context(),
                env.inode_id,
                vec![env.inode_id],
                crate::inode_lease::WriteMode::Write,
                Freshness::default(),
            )
            .await
            .expect("open write")
            .payload;
        let target = add_block_for_key(&env.filesystem, &open, 64).await;
        let committed = vec![committed_block(
            target.block_id,
            target.file_offset,
            target.effective_len,
        )];
        publish_env_write_target(&env, &target, 1);
        env.filesystem
            .sync_write_session(
                &request_context(),
                PresentedWriteHandle {
                    inode_id: open.inode_id,
                    lease_epoch: open.lease_epoch,
                },
                CloseWriteIntent {
                    committed_blocks: committed.clone(),
                    final_size: 64,
                    expected_file_size: 0,
                },
                Freshness::default(),
                open.content_revision,
                PublishMode::ReplaceIfUnchanged,
            )
            .await
            .expect("visibility sync");

        env.filesystem
            .worker_manager
            .as_ref()
            .expect("worker manager")
            .reset_worker_soft_state();
        let synced_session = env
            .filesystem
            .write_session_for_inode(open.inode_id)
            .expect("session remains open");
        env.filesystem
            .close_write_session(
                &request_context(),
                PresentedWriteHandle {
                    inode_id: open.inode_id,
                    lease_epoch: open.lease_epoch,
                },
                CloseWriteIntent {
                    committed_blocks: committed,
                    final_size: 64,
                    expected_file_size: 64,
                },
                Freshness::default(),
                synced_session.content_revision,
                PublishMode::ReplaceIfUnchanged,
            )
            .await
            .expect("close without new blocks must not require old Ready evidence");
    }
}
