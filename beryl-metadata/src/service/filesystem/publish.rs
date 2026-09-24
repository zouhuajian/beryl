// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Durable file visibility publication for sync and commit.

use super::{fs_failure_from_metadata_error, FsFailure, FsResult, MetadataFileSystem, RequestHeader};
use crate::error::{MetadataError, MetadataResult};
use crate::inode::FilePublication;
use crate::observe;
use crate::raft::{Command, PublishMode};
use crate::session_registry::{BeginWritePublicationError, WritePublication, WriteSession, WriteSessionIdentity};
use crate::worker::{PublishReadyConflict, PublishReadyStatus, PublishReadyTarget};
use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, WorkerErrorKind};
use beryl_types::ids::{InodeId, MountId};
use beryl_types::{ContentGeneration, GroupName, LeaseEpoch, WriteMode};
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub(crate) struct SyncWriteOutput {
    pub(crate) synced_len: u64,
    pub(crate) generation: ContentGeneration,
}

impl MetadataFileSystem {
    pub(crate) async fn commit_file(
        &self,
        ctx: &RequestHeader,
        inode_id: InodeId,
        publication: FilePublication,
    ) -> FsResult<u64> {
        self.check_publication_admission(ctx, inode_id, &publication)?;
        let lease_epoch = publication.lease_epoch;
        let final_len = publication.target_len;
        let committed_block_count = publication.blocks.len();
        let committed_bytes: u64 = publication
            .blocks
            .iter()
            .fold(0u64, |sum, block| sum.saturating_add(block.len));
        let result = self.close_write_session(ctx, inode_id, publication).await;
        match &result {
            Ok(_) => tracing::info!(
                target: "metadata.state",
                op = "CommitFile",
                result = "committed",
                error_code = "none",
                client_id = %ctx.client.client_id,
                call_id = %ctx.client.call_id,
                inode_id = inode_id.as_raw(),
                final_len,
                committed_block_count,
                committed_bytes,
                lease_epoch = lease_epoch.as_raw(),
                "CommitFile committed"
            ),
            Err(failure) => tracing::warn!(
                target: "metadata.state",
                op = "CommitFile",
                result = "rejected",
                error_code = observe::rpc_error_kind(&failure.error),
                client_id = %ctx.client.client_id,
                call_id = %ctx.client.call_id,
                inode_id = inode_id.as_raw(),
                final_len,
                committed_block_count,
                committed_bytes,
                lease_epoch = lease_epoch.as_raw(),
                "CommitFile rejected"
            ),
        }
        result
    }

    pub(crate) async fn sync_write(
        &self,
        ctx: &RequestHeader,
        inode_id: InodeId,
        publication: FilePublication,
    ) -> FsResult<SyncWriteOutput> {
        self.check_publication_admission(ctx, inode_id, &publication)?;
        self.sync_write_session(ctx, inode_id, publication).await
    }

    fn check_publication_admission(
        &self,
        ctx: &RequestHeader,
        inode_id: InodeId,
        publication: &FilePublication,
    ) -> Result<(), FsFailure> {
        self.check_session_write_admission(ctx, inode_id)?;
        if publication
            .blocks
            .iter()
            .any(|block| block.block_id.inode_id != inode_id)
        {
            return Err(self.failure_from_error(
                ctx,
                MetadataError::InvalidArgument("committed block inode_id does not match request".to_string()),
                None,
            ));
        }

        Ok(())
    }

    fn publish_mode_for_session(mode: WriteMode) -> PublishMode {
        match mode {
            WriteMode::Overwrite => PublishMode::ReplaceIfUnchanged,
            WriteMode::Append => PublishMode::AppendIfUnchanged,
        }
    }

    fn active_publish_session(
        &self,
        ctx: &RequestHeader,
        inode_id: InodeId,
        lease_epoch: LeaseEpoch,
        operation: &'static str,
    ) -> Result<Option<WriteSessionIdentity>, FsFailure> {
        let Some(session) = self.session_registry.get_session_identity(inode_id) else {
            return Ok(None);
        };
        let invalid = |message| {
            self.session_terminal_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                message,
                None,
            )
        };
        if session.open_client_id != ctx.client.client_id {
            return Err(invalid(format!("{operation} client does not own inode_id={inode_id}")));
        }
        if session.lease_epoch != lease_epoch {
            return Err(invalid(format!(
                "{operation} publish precondition does not match the active session"
            )));
        }
        Ok(Some(session))
    }

    /// Freeze the current issued-target sequence before validating publication.
    fn begin_write_publication(
        &self,
        ctx: &RequestHeader,
        inode_id: InodeId,
        lease_epoch: LeaseEpoch,
        publish_mode: PublishMode,
        operation: &'static str,
    ) -> Result<WritePublication, FsFailure> {
        let publication = match self.session_registry.begin_publication(inode_id, lease_epoch) {
            Ok(publication) => publication,
            Err(BeginWritePublicationError::Session(message)) => {
                return Err(self.session_terminal_failure(
                    ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                    format!("{operation} write session is no longer current: {message}"),
                    None,
                ));
            }
            Err(BeginWritePublicationError::AllocateBlockPending) => {
                return Err(self.failure_from_error(
                    ctx,
                    MetadataError::Again(format!(
                        "{operation} cannot freeze inode_id={inode_id} while AllocateBlock is pending"
                    )),
                    None,
                ));
            }
            Err(BeginWritePublicationError::PublicationInProgress) => {
                return Err(self.failure_from_error(
                    ctx,
                    MetadataError::Again(format!(
                        "another file publication is already in progress for inode_id={inode_id}"
                    )),
                    None,
                ));
            }
            Err(BeginWritePublicationError::PublicationIdExhausted) => {
                return Err(self.failure_from_error(
                    ctx,
                    MetadataError::Internal("write publication identity exhausted".to_string()),
                    None,
                ));
            }
        };
        let session = publication.session();
        if session.open_client_id != ctx.client.client_id
            || Self::publish_mode_for_session(session.mode) != publish_mode
        {
            return Err(self.session_terminal_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!("{operation} publish precondition does not match the active session"),
                None,
            ));
        }
        Ok(publication)
    }

    /// Resolve a SyncWrite postcondition; this never proves that CommitFile ran.
    ///
    /// This is state-equivalence recovery, not historical request replay. Once
    /// the requested postcondition is visible at the next content generation,
    /// preconditions such as the original publish mode are no longer
    /// distinguishable without persisting request history.
    fn resolve_synced_state(
        &self,
        inode_id: InodeId,
        payload: &FilePublication,
    ) -> MetadataResult<Option<(MountId, ContentGeneration)>> {
        let inode = self
            .storage
            .get_inode(inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("inode {inode_id}")))?;
        let file = inode.file()?;
        file.validate(inode_id)?;
        if file.lease_epoch != payload.lease_epoch && payload.lease_epoch.checked_next() != Some(file.lease_epoch) {
            return Err(MetadataError::LeaseFenced {
                expected: file.lease_epoch,
                got: payload.lease_epoch,
            });
        }
        let start = payload.start_index(file.block_size)?;
        let matches = payload.matches_visible(file, start);
        if matches
            && (payload.expected_generation.checked_next() == Some(file.generation)
                || (file.generation == payload.expected_generation && payload.blocks.is_empty()))
        {
            return Ok(Some((inode.mount_id, file.generation)));
        }
        if file.generation != payload.expected_generation {
            return Err(MetadataError::Again("content generation changed".into()));
        }
        Ok(None)
    }

    async fn completed_publish_hints(
        &self,
        ctx: &RequestHeader,
        mount_id: MountId,
    ) -> Result<Option<GroupName>, FsFailure> {
        let group_name = self.mount_owner(mount_id);
        self.check_leader(ctx, group_name.clone()).await?;
        Ok(group_name)
    }

    fn publish_ready_conflict_failure(
        &self,
        ctx: &RequestHeader,
        conflict: PublishReadyConflict,
        group_name: &GroupName,
    ) -> FsFailure {
        match conflict {
            PublishReadyConflict::WorkerRunMismatch {
                block_id,
                worker_id,
                expected,
                current,
            } => self.refresh_metadata_failure(
                ctx,
                ErrorKind::Worker(WorkerErrorKind::RunMismatch),
                format!(
                    "worker run changed before publishing block {block_id}: worker_id={}, expected={expected}, current={current:?}",
                    worker_id.as_raw()
                ),
                Some(group_name.clone())),
            PublishReadyConflict::EndpointMismatch { block_id, worker_id } => self.refresh_metadata_failure(
                ctx,
                ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
                format!(
                    "worker endpoint changed before publishing block {block_id}: worker_id={}",
                    worker_id.as_raw()
                ),
                Some(group_name.clone())),
            PublishReadyConflict::LeaseEpochMismatch {
                block_id,
                worker_id,
                expected,
                reported,
            } => self.refresh_metadata_failure(
                ctx,
                ErrorKind::Worker(WorkerErrorKind::Fencing),
                format!(
                    "worker reported the wrong writer epoch for block {block_id}: worker_id={}, expected={expected}, reported={reported}",
                    worker_id.as_raw()
                ),
                Some(group_name.clone())),
            PublishReadyConflict::UnreadableBlock {
                block_id,
                worker_id,
                state,
            } => self.refresh_metadata_failure(
                ctx,
                ErrorKind::Worker(WorkerErrorKind::Corrupt),
                format!(
                    "worker reported an unreadable block before publication: block_id={block_id}, worker_id={}, state={state:?}",
                    worker_id.as_raw()
                ),
                Some(group_name.clone())),
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
        ctx: &RequestHeader,
        group_name: &GroupName,
        targets: &[PublishReadyTarget],
    ) -> Result<(), FsFailure> {
        if targets.is_empty() {
            return Ok(());
        }
        let worker_manager = &self.worker_manager;

        let mut observations = worker_manager.subscribe_publication_observations();
        loop {
            let pending_block_id = match worker_manager.check_publish_ready(group_name, targets) {
                PublishReadyStatus::Ready => return Ok(()),
                PublishReadyStatus::Pending { block_id } => block_id,
                PublishReadyStatus::Conflict(conflict) => {
                    return Err(self.publish_ready_conflict_failure(ctx, conflict, group_name));
                }
            };

            let remaining = ctx.deadline.remaining();
            if remaining.is_zero() {
                return Err(self.refresh_metadata_failure(
                    ctx,
                    ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
                    format!("deadline expired while waiting for Ready report for block {pending_block_id}"),
                    Some(group_name.clone()),
                ));
            }
            match tokio::time::timeout(remaining, observations.changed()).await {
                Ok(result) => result.expect("WorkerManager retains the publication observation sender"),
                Err(_) => {
                    return Err(self.refresh_metadata_failure(
                        ctx,
                        ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
                        format!("deadline expired while waiting for Ready report for block {pending_block_id}"),
                        Some(group_name.clone()),
                    ));
                }
            }
        }
    }

    /// Perform the non-waiting Ready recheck immediately before proposal.
    fn require_publish_ready(
        &self,
        ctx: &RequestHeader,
        group_name: &GroupName,
        targets: &[PublishReadyTarget],
    ) -> Result<(), FsFailure> {
        if targets.is_empty() {
            return Ok(());
        }
        let worker_manager = &self.worker_manager;
        match worker_manager.check_publish_ready(group_name, targets) {
            PublishReadyStatus::Ready => Ok(()),
            PublishReadyStatus::Pending { block_id } => Err(self.refresh_metadata_failure(
                ctx,
                ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
                format!("Ready evidence changed before publishing block {block_id}"),
                Some(group_name.clone()),
            )),
            PublishReadyStatus::Conflict(conflict) => {
                Err(self.publish_ready_conflict_failure(ctx, conflict, group_name))
            }
        }
    }

    /// Prepare new publication targets and revalidate authority after Worker Ready.
    /// Sync recovery and exact Commit replay are resolved by their callers first.
    async fn prepare_file_publication(
        &self,
        ctx: &RequestHeader,
        payload: &FilePublication,
        publication: &WritePublication,
        operation: &'static str,
    ) -> Result<crate::mount::MountEntry, FsFailure> {
        let session = publication.session();
        if session.generation != payload.expected_generation {
            return Err(self.session_terminal_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!("{operation} publish precondition does not match the active session"),
                None,
            ));
        }
        let group_name = self.mount_owner(session.mount_id);

        self.check_leader(ctx, group_name.clone()).await?;

        if self
            .session_registry
            .validate_session(session.inode_id, payload.lease_epoch)
            .is_err()
        {
            return Err(self.session_terminal_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionExpired),
                format!("lease validation rejected for inode_id={}", session.inode_id),
                group_name,
            ));
        }

        let worker_lookup_group_name = self.require_worker_lookup_group(ctx, group_name.clone(), operation)?;
        let new_targets = match self.prepare_publication_targets(payload, session) {
            Ok(targets) => targets,
            Err(err) => {
                return Err(self.invalid_publication_failure(ctx, err.to_string(), group_name));
            }
        };
        self.wait_for_publish_ready(ctx, &worker_lookup_group_name, &new_targets)
            .await?;

        // Fence against the authority observed before waiting for Worker Ready.
        let revalidated_group_name = self.mount_owner(session.mount_id);
        if revalidated_group_name.as_ref() != Some(&worker_lookup_group_name) {
            return Err(self.failure_from_error(
                ctx,
                MetadataError::StaleState(format!("{operation} mount authority changed during Ready wait")),
                revalidated_group_name,
            ));
        }
        self.check_leader(ctx, group_name.clone()).await?;
        self.revalidate_publish_session(ctx, publication, operation)?;
        self.require_publish_ready(ctx, &worker_lookup_group_name, &new_targets)?;

        let routed = self.route_ctx_for_write_with_error_hints(ctx, session.inode_id, group_name)?;
        // Durable replay is resolved before this final proposal deadline check.
        if ctx.deadline.has_passed() {
            return Err(self.refresh_metadata_failure(
                ctx,
                ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
                "deadline expired before file publication",
                Some(worker_lookup_group_name),
            ));
        }
        Ok(routed)
    }

    /// Revalidate leader-local session and lease state after an asynchronous
    /// Ready wait. A caller may proceed only with the same publication
    /// preconditions that were used to select the target set.
    fn revalidate_publish_session(
        &self,
        ctx: &RequestHeader,
        publication: &WritePublication,
        operation: &'static str,
    ) -> Result<(), FsFailure> {
        let expected = publication.session();
        self.check_session_write_admission(ctx, expected.inode_id)?;
        publication.revalidate().map_err(|message| {
            self.session_terminal_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!("{operation} write publication changed while waiting for Ready block reports: {message}"),
                None,
            )
        })?;
        if self
            .session_registry
            .validate_session(expected.inode_id, expected.lease_epoch)
            .is_err()
        {
            return Err(self.session_terminal_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionExpired),
                format!("{operation} lease expired while waiting for Ready block reports"),
                None,
            ));
        }
        Ok(())
    }

    async fn sync_write_session(
        &self,
        ctx: &RequestHeader,
        inode_id: InodeId,
        payload: FilePublication,
    ) -> FsResult<SyncWriteOutput> {
        let lease_epoch = payload.lease_epoch;
        let target_len = payload.target_len;
        let active_session = match self.active_publish_session(ctx, inode_id, lease_epoch, "SyncWrite") {
            Ok(session) => session,
            Err(failure) => return Err(failure),
        };
        match self.resolve_synced_state(inode_id, &payload) {
            Ok(Some((mount_id, generation))) => {
                let publication = if let Some(session) = &active_session {
                    Some(self.begin_write_publication(
                        ctx,
                        inode_id,
                        lease_epoch,
                        Self::publish_mode_for_session(session.mode),
                        "SyncWrite",
                    )?)
                } else {
                    None
                };
                if publication.as_ref().is_some_and(|publication| {
                    let session = publication.session();
                    session.generation != payload.expected_generation && session.generation != generation
                }) {
                    return Err(self.session_terminal_failure(
                        ctx,
                        ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                        "SyncWrite content generation does not match the active session".to_string(),
                        None,
                    ));
                }
                let group_name = self.completed_publish_hints(ctx, mount_id).await?;
                if let Some(publication) = publication {
                    if let Err(message) = publication.complete_sync(generation, target_len) {
                        return Err(self.failure_from_error(ctx, MetadataError::Internal(message), group_name));
                    }
                }
                return self.success(
                    SyncWriteOutput {
                        synced_len: target_len,
                        generation,
                    },
                    group_name,
                );
            }
            Ok(None) => {}
            Err(err) => return Err(self.failure_from_error(ctx, err, None)),
        }
        let publication = match active_session {
            Some(_) => match self.begin_write_publication(ctx, inode_id, lease_epoch, payload.mode, "SyncWrite") {
                Ok(publication) => publication,
                Err(failure) => return Err(failure),
            },
            None => {
                return Err(self.session_terminal_failure(
                    ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                    format!("write session not found for inode_id={}", inode_id),
                    None,
                ));
            }
        };
        let routed = self
            .prepare_file_publication(ctx, &payload, &publication, "SyncWrite")
            .await?;

        let command = Command::PublishFile {
            proposed_at_ms: crate::raft::proposal_timestamp_ms(),
            inode_id,
            publication: payload,
        };
        let generation = match self.propose_file_publication(command, publication).await {
            Ok(generation) => generation,
            Err(error) => {
                return Err(self.failure_from_error(ctx, error, Some(routed.namespace_owner_group_name.clone())))
            }
        };

        self.success(
            SyncWriteOutput {
                synced_len: target_len,
                generation,
            },
            Some(routed.namespace_owner_group_name.clone()),
        )
    }

    fn invalid_publication_failure(
        &self,
        ctx: &RequestHeader,
        message: impl Into<String>,
        group_name: Option<GroupName>,
    ) -> FsFailure {
        fs_failure_from_metadata_error(ctx, MetadataError::InvalidArgument(message.into()), group_name)
    }

    /// Validate the ordered changed tail and new blocks against this session's
    /// issued targets. Raft repeats complete-layout and prefix validation at apply.
    fn prepare_publication_targets(
        &self,
        payload: &FilePublication,
        session: &WriteSession,
    ) -> MetadataResult<Vec<PublishReadyTarget>> {
        if payload.expected_file_len != session.base_len {
            return Err(MetadataError::InvalidArgument(
                "expected file size does not match session".into(),
            ));
        }
        let start = payload.start_index(session.block_size)?;
        let capacity = u64::from(session.block_size);
        let issued: HashMap<_, _> = session
            .issued_targets
            .iter()
            .map(|target| (target.block_id, target))
            .collect();
        let mut targets = Vec::with_capacity(payload.blocks.len());
        for (index, block) in payload.blocks.iter().enumerate() {
            let target = issued
                .get(&block.block_id)
                .ok_or_else(|| MetadataError::InvalidArgument("block was not issued to this writer".into()))?;
            let offset = (start + index) as u64 * capacity;
            if target.file_offset != offset {
                return Err(MetadataError::InvalidArgument(
                    "invalid ordered publication block".into(),
                ));
            }
            targets.push(PublishReadyTarget {
                target: (*target).clone(),
                effective_len: block.len,
            });
        }
        let inode = self
            .storage
            .get_inode(session.inode_id)?
            .ok_or_else(|| MetadataError::NotFound("inode missing".into()))?;
        if inode.file()?.generation != payload.expected_generation {
            return Err(MetadataError::StaleState("content generation changed".into()));
        }
        Ok(targets)
    }

    pub(super) async fn close_write_session(
        &self,
        ctx: &RequestHeader,
        inode_id: InodeId,
        payload: FilePublication,
    ) -> FsResult<u64> {
        let lease_epoch = payload.lease_epoch;
        let target_len = payload.target_len;
        // Read the receipt and its layout together before checking any soft
        // session state: a completed commit has already ended that session.
        let resolved = self
            .raft_node
            .read(true, || {
                let inode = self
                    .storage
                    .get_inode(inode_id)?
                    .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {inode_id}")))?;
                payload
                    .resolve_commit(&inode, ctx.client.client_id, ctx.client.call_id)
                    .map(|generation| generation.map(|_| inode.mount_id))
            })
            .await;
        match resolved {
            Ok(Some(mount_id)) => {
                let group_name = self.completed_publish_hints(ctx, mount_id).await?;
                self.session_registry.remove_session_if_epoch(inode_id, lease_epoch);
                return self.success(target_len, group_name);
            }
            Ok(None) => {}
            Err(error) => return Err(self.failure_from_error(ctx, error, None)),
        }
        let active_session = self.active_publish_session(ctx, inode_id, lease_epoch, "CommitFile")?;
        let publication = match active_session {
            Some(_) => match self.begin_write_publication(ctx, inode_id, lease_epoch, payload.mode, "CommitFile") {
                Ok(publication) => publication,
                Err(failure) => return Err(failure),
            },
            None => {
                return Err(self.session_terminal_failure(
                    ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                    format!("write session not found for inode_id={}", inode_id),
                    None,
                ));
            }
        };
        let routed = self
            .prepare_file_publication(ctx, &payload, &publication, "CommitFile")
            .await?;

        let command = Command::CommitFile {
            proposed_at_ms: crate::raft::proposal_timestamp_ms(),
            inode_id,
            client_id: ctx.client.client_id,
            call_id: ctx.client.call_id,
            publication: payload,
        };
        if let Err(error) = self.propose_file_publication(command, publication).await {
            return Err(self.failure_from_error(ctx, error, Some(routed.namespace_owner_group_name.clone())));
        }

        self.success(target_len, Some(routed.namespace_owner_group_name.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inode::InodeKind;
    use crate::service::filesystem::tests::*;
    use beryl_common::error::rpc::MetadataErrorKind;
    use beryl_common::Deadline;

    async fn open_write_with_target(env: &WriteFlowEnv) -> (OpenWriteOutput, LocatedBlock) {
        let open = env
            .filesystem
            .open_write_inode(
                &request_context(),
                "/file".to_string(),
                env.inode_id,
                vec![env.inode_id],
                WriteMode::Overwrite,
            )
            .await
            .expect("open write")
            .payload;
        let target = allocate_block_for_key(&env.filesystem, &open).await;
        (open, target)
    }

    fn target_publication(open: &OpenWriteOutput, target: &LocatedBlock) -> FilePublication {
        FilePublication {
            blocks: vec![committed_block(target.block_id, 64)],
            target_len: target.file_offset + 64,
            expected_file_len: open.base_len,
            expected_generation: open.generation,
            lease_epoch: open.lease_epoch,
            mode: PublishMode::ReplaceIfUnchanged,
        }
    }

    #[tokio::test]
    async fn commit_waits_for_ready_observation_then_publishes() {
        let env = write_flow_env(0).await;
        let (open, target) = open_write_with_target(&env).await;
        let committed = vec![committed_block(target.block_id, 64)];
        let commit = commit_for_key(&env.filesystem, &open, committed, 64);
        tokio::pin!(commit);

        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut commit)
                .await
                .is_err(),
            "publication must remain pending before the Ready report"
        );
        assert_eq!(
            stored_generation(&env.storage, env.inode_id),
            ContentGeneration::default()
        );

        publish_env_write_target(&env, &target, 1);
        tokio::time::timeout(Duration::from_secs(2), &mut commit)
            .await
            .expect("Ready observation should wake publication")
            .expect("commit should succeed");
        assert_eq!(stored_generation(&env.storage, env.inode_id), ContentGeneration::new(1));
    }

    #[tokio::test]
    async fn deadline_expiring_after_ready_wait_does_not_publish() {
        let env = write_flow_env(0).await;
        let (open, target) = open_write_with_target(&env).await;
        let mut ctx = request_context();
        ctx.deadline = Deadline::from_now(Duration::from_millis(40));
        let commit = env
            .filesystem
            .close_write_session(&ctx, open.inode_id, target_publication(&open, &target));
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
        assert_eq!(
            stored_generation(&env.storage, env.inode_id),
            ContentGeneration::default()
        );
        assert!(env.filesystem.write_session_for_inode(open.inode_id).is_some());
    }

    #[tokio::test]
    async fn noop_close_checks_authority_without_advancing_exhausted_generation() {
        for case in 0..3 {
            let env = write_flow_env(0).await;
            if case == 2 {
                let mut inode = env.storage.get_inode(env.inode_id).unwrap().unwrap();
                let InodeKind::File(crate::inode::FileData { generation, .. }) = &mut inode.kind else {
                    unreachable!()
                };
                *generation = ContentGeneration::new(u64::MAX);
                env.storage.put_inode(&inode).unwrap();
            }
            let open = env
                .filesystem
                .open_write_inode(
                    &request_context(),
                    "/file".into(),
                    env.inode_id,
                    vec![env.inode_id],
                    WriteMode::Overwrite,
                )
                .await
                .unwrap()
                .payload;
            let mut ctx = request_context();
            if case == 0 {
                ctx.deadline = Deadline::from_unix_ms(0);
            } else if case == 1 {
                env.filesystem
                    .session_registry()
                    .remove_session_if_epoch(open.inode_id, open.lease_epoch);
            }
            let result = env
                .filesystem
                .close_write_session(
                    &ctx,
                    open.inode_id,
                    FilePublication {
                        blocks: Vec::new(),
                        target_len: 0,
                        expected_file_len: 0,
                        expected_generation: open.generation,
                        lease_epoch: open.lease_epoch,
                        mode: PublishMode::ReplaceIfUnchanged,
                    },
                )
                .await;
            assert_eq!(result.is_ok(), case == 2);
            let inode = env.storage.get_inode(open.inode_id).unwrap().unwrap();
            assert_eq!(stored_generation(&env.storage, env.inode_id), open.generation);
            assert!(
                matches!(inode.kind, InodeKind::File(crate::inode::FileData { lease_epoch: epoch, last_commit, .. })
                if epoch.as_raw() == open.lease_epoch.as_raw() + u64::from(case == 2)
                    && last_commit.is_some() == (case == 2))
            );
        }
    }

    #[tokio::test]
    async fn submitted_publication_survives_cancelled_waiter() {
        use std::future::Future;
        use std::task::Poll;
        for closes in [false, true] {
            let env = write_flow_env(0).await;
            let (open, target) = open_write_with_target(&env).await;
            publish_env_write_target(&env, &target, 1);
            let registry = env.filesystem.session_registry();
            let publication = registry.begin_publication(open.inode_id, open.lease_epoch).unwrap();
            let payload = target_publication(&open, &target);
            let command = if closes {
                Command::CommitFile {
                    proposed_at_ms: 1,
                    inode_id: open.inode_id,
                    client_id: publication.session().open_client_id,
                    call_id: beryl_types::CallId::new(),
                    publication: payload,
                }
            } else {
                Command::PublishFile {
                    proposed_at_ms: 1,
                    inode_id: open.inode_id,
                    publication: payload,
                }
            };
            {
                let mut waiter = Box::pin(env.filesystem.propose_file_publication(command, publication));
                // The current-thread executor cannot run the completion task until
                // this yields, so the waiter is cancelled strictly before apply.
                std::future::poll_fn(|cx| {
                    assert!(waiter.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
            }
            assert!(registry.get_session_identity(open.inode_id).is_some());
            assert_eq!(
                stored_generation(&env.storage, open.inode_id),
                ContentGeneration::default()
            );
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    match registry.get_session(open.inode_id) {
                        None if closes => break,
                        Some(session) if !closes && session.generation == ContentGeneration::new(1) => break,
                        _ => tokio::task::yield_now().await,
                    }
                }
            })
            .await
            .unwrap();
            let inode = env.storage.get_inode(open.inode_id).unwrap().unwrap();
            assert_eq!(inode.len(), 64);
            assert!(matches!(inode.kind,
                InodeKind::File(crate::inode::FileData { last_commit, lease_epoch: epoch, .. })
                if last_commit.is_some() == closes
                    && epoch.as_raw() == open.lease_epoch.as_raw() + u64::from(closes)));
        }
    }

    #[tokio::test]
    async fn authority_changes_during_ready_wait_prevent_commit() {
        for change in 0..2 {
            let env = write_flow_env(0).await;
            let (open, target) = open_write_with_target(&env).await;
            let ctx = request_context();
            let commit = env
                .filesystem
                .close_write_session(&ctx, open.inode_id, target_publication(&open, &target));
            tokio::pin!(commit);
            assert!(
                tokio::time::timeout(Duration::from_millis(20), &mut commit)
                    .await
                    .is_err(),
                "Ready has not been reported"
            );
            let expected = match change {
                0 => {
                    env.filesystem
                        .session_registry()
                        .remove_session_if_epoch(open.inode_id, open.lease_epoch);
                    MetadataErrorKind::SessionInvalid
                }
                _ => {
                    env.filesystem.raft_node().shutdown().await.unwrap();
                    MetadataErrorKind::NotLeader
                }
            };
            publish_env_write_target(&env, &target, 1);
            let failure = tokio::time::timeout(Duration::from_secs(2), &mut commit)
                .await
                .unwrap()
                .expect_err("changed authority must fail closed");
            assert_eq!(failure.error.kind, ErrorKind::Metadata(expected));
            assert_eq!(
                stored_generation(&env.storage, env.inode_id),
                ContentGeneration::default()
            );
        }
    }
}
