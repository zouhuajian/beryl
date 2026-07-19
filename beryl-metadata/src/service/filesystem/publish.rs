// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Durable file visibility publication for sync and commit.

use super::{Freshness, FsFailure, FsResult, MetadataFileSystem, PresentedWriteHandle, RequestContext};
use crate::error::{MetadataError, MetadataResult};
use crate::observe;
use crate::raft::{Command, FsCommandResult, PublishMode};
use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, RefreshHint, WorkerErrorKind};
use beryl_types::fs::{Extent, FsErrorCode, InodeId};
use beryl_types::ids::{DataHandleId, MountId};
use beryl_types::{CommittedBlock, GroupName};
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
        if let Some(failure) = self
            .session_write_admission_failure(ctx, args.handle.data_handle_id)
            .await
        {
            return self.failure_from_admission(failure);
        }
        let data_handle_id = args.handle.data_handle_id;
        if args
            .committed_blocks
            .iter()
            .any(|block| block.block_id.data_handle_id != data_handle_id)
        {
            return self.failure_from_error(
                ctx,
                MetadataError::InvalidArgument("committed block data_handle_id does not match request".to_string()),
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
                data_handle_id = data_handle_id.as_raw(),
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
                data_handle_id = data_handle_id.as_raw(),
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
        if let Some(failure) = self
            .session_write_admission_failure(ctx, args.handle.data_handle_id)
            .await
        {
            return self.failure_from_admission(failure);
        }
        let data_handle_id = args.handle.data_handle_id;
        if args
            .committed_blocks
            .iter()
            .any(|block| block.block_id.data_handle_id != data_handle_id)
        {
            return self.failure_from_error(
                ctx,
                MetadataError::InvalidArgument("committed block data_handle_id does not match request".to_string()),
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
        data_handle_id: DataHandleId,
        lease_epoch: u64,
        publish_mode: PublishMode,
        operation: &'static str,
    ) -> Result<Option<crate::session_registry::WriteSession>, FsFailure> {
        let Some(session) = self.session_registry.get_session(data_handle_id) else {
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
            return Err(invalid(format!(
                "{operation} client does not own data_handle_id={data_handle_id}"
            )));
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
        data_handle_id: DataHandleId,
        lease_epoch: u64,
        intent: &CloseWriteIntent,
        expected_content_revision: u64,
        mode: PublishMode,
    ) -> MetadataResult<Option<(InodeId, MountId, u64)>> {
        let Some(inode_id) = self.storage.get_inode_by_data_handle(data_handle_id)? else {
            return Err(MetadataError::StaleState(format!(
                "data handle owner not found: {data_handle_id}"
            )));
        };
        let inode = self
            .read_inode(inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {inode_id}")))?;
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
                || block.block_id.data_handle_id != data_handle_id
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

    async fn sync_write_session(
        &self,
        ctx: &RequestContext,
        handle: PresentedWriteHandle,
        intent: CloseWriteIntent,
        freshness: Freshness,
        expected_content_revision: u64,
        publish_mode: PublishMode,
    ) -> FsResult<SyncWriteOutput> {
        let data_handle_id = handle.data_handle_id;
        let lease_epoch = handle.lease_epoch;
        let active_session =
            match self.active_publish_session(ctx, data_handle_id, lease_epoch, publish_mode, "SyncWrite") {
                Ok(session) => session,
                Err(failure) => return Err(failure),
            };
        match self.resolve_published_state(
            data_handle_id,
            lease_epoch,
            &intent,
            expected_content_revision,
            publish_mode,
        ) {
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
                        data_handle_id,
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
                    format!("write session not found for data_handle_id={}", data_handle_id),
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
            if block.block_id.data_handle_id != session.data_handle_id {
                return self.failure_from_error_with_route_epoch(
                    ctx,
                    MetadataError::InvalidArgument(format!(
                        "SyncWrite committed block data_handle_id {} does not match write handle data_handle_id {}",
                        block.block_id.data_handle_id, session.data_handle_id
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
                    "write handle epoch mismatch for data_handle_id={}: expected {}, got {}",
                    data_handle_id, session.lease_epoch, lease_epoch
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
                format!("lease validation rejected for data_handle_id={}", data_handle_id,),
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

        let routed = match self.route_ctx_for_write_with_error_hints(
            ctx,
            &[session.inode_id],
            freshness,
            group_name.clone(),
            mount_epoch,
        ) {
            Ok(ctx) => ctx,
            Err(failure) => return Err(failure),
        };

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
        if let Err(message) = self.session_registry.update_published_state(
            data_handle_id,
            lease_epoch,
            content_revision,
            intent.final_size,
        ) {
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
            if block.block_id.data_handle_id != session.data_handle_id {
                return Err(MetadataError::InvalidArgument(format!(
                    "Committed block data_handle_id {} does not match write handle data_handle_id {}",
                    block.block_id.data_handle_id, session.data_handle_id
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
        let data_handle_id = handle.data_handle_id;
        let lease_epoch = handle.lease_epoch;
        let active_session =
            match self.active_publish_session(ctx, data_handle_id, lease_epoch, publish_mode, "CommitFile") {
                Ok(session) => session,
                Err(failure) => return Err(failure),
            };
        match self.resolve_published_state(
            data_handle_id,
            lease_epoch,
            &intent,
            expected_content_revision,
            publish_mode,
        ) {
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
                if let Some(session) = self
                    .session_registry
                    .remove_session_if_epoch(data_handle_id, lease_epoch)
                {
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
                    format!("write session not found for data_handle_id={}", data_handle_id),
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

        if let Some(worker_manager) = self.worker_manager.as_ref() {
            let worker_lookup_group_name =
                self.require_worker_lookup_group(ctx, group_name.clone(), mount_epoch, route_epoch, "CommitFile")?;
            for target in &session.issued_targets {
                for endpoint in &target.worker_endpoints {
                    let worker_id = endpoint.worker_id;
                    let current_run_id = worker_manager
                        .get_registration(&worker_lookup_group_name, worker_id)
                        .map(|registration| registration.worker_run_id);
                    if !current_run_id.is_some_and(|run_id| run_id.matches(endpoint.worker_run_id)) {
                        let hint = worker_refresh_hint_from_session(&session, true);
                        return self.refresh_metadata_failure_with_hint(
                            ctx,
                            ErrorKind::Worker(WorkerErrorKind::RunMismatch),
                            format!(
                                "worker_run_id mismatch for worker_id={}: client/session={}, server={:?}; {}",
                                endpoint.worker_id,
                                endpoint.worker_run_id,
                                current_run_id,
                                MetadataFileSystem::replay_hint("CommitFile")
                            ),
                            group_name,
                            mount_epoch,
                            route_epoch,
                            Some(hint),
                        );
                    }
                }
            }
        }

        if lease_epoch != session.lease_epoch {
            return self.session_terminal_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!(
                    "write handle epoch mismatch for data_handle_id={}: expected {}, got {}",
                    data_handle_id, session.lease_epoch, lease_epoch,
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
                format!("lease validation rejected for data_handle_id={}", data_handle_id),
                group_name,
                mount_epoch,
            );
        }

        let extents = match Self::validate_committed_blocks(&intent, &session) {
            Ok(extents) => extents,
            Err(err) => return Err(self.invalid_commit_failure(ctx, err.to_string(), group_name.clone(), mount_epoch)),
        };

        let routed = match self.route_ctx_for_write_with_error_hints(
            ctx,
            &[session.inode_id],
            freshness,
            group_name.clone(),
            mount_epoch,
        ) {
            Ok(ctx) => ctx,
            Err(failure) => return Err(failure),
        };

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
        self.session_registry
            .remove_session_if_epoch(data_handle_id, lease_epoch);

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

fn worker_refresh_hint_from_session(
    _session: &crate::session_registry::WriteSession,
    resolve_required: bool,
) -> RefreshHint {
    RefreshHint {
        worker_resolve_required: resolve_required,
        ..Default::default()
    }
}
