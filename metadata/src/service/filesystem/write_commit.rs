// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use super::{
    Freshness, FsFailure, FsResult, MetadataFileSystem, PresentedFencingToken, PresentedWriteHandle, RequestContext,
    WriteCommandKind,
};
use crate::error::{MetadataError, MetadataResult};
use crate::observe;
use crate::raft::{
    AppDataResponse, Command, CommandFingerprint, DedupKey, FileCommitMode, FsCommandResult, RocksDBStorage,
};
use common::error::rpc::{ErrorKind, MetadataErrorKind, RefreshHint, WorkerErrorKind};
use std::collections::{HashMap, HashSet};
use types::fs::{Extent, FsErrorCode};
use types::ids::{DataHandleId, LeaseId};
use types::{CommittedBlock, GroupName};

#[derive(Clone, Debug)]
pub(super) struct CloseWriteIntent {
    pub(super) committed_blocks: Vec<CommittedBlock>,
    pub(super) final_size: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SyncWriteMode {
    Visibility,
    Durability,
}

#[derive(Clone, Debug)]
struct SyncWriteInput {
    ctx: RequestContext,
    file_handle: u64,
    lease_id: Option<LeaseId>,
    lease_epoch: u64,
    open_epoch: u64,
    fencing_token: Option<PresentedFencingToken>,
    data_handle_id: DataHandleId,
    committed_blocks: Vec<CommittedBlock>,
    target_size: u64,
    _flags: u32,
    mode: SyncWriteMode,
    freshness: Freshness,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SyncWriteOutput {
    pub(crate) synced_size: u64,
    pub(crate) file_version: Option<u64>,
}

#[derive(Clone, Debug)]
pub(super) struct CloseWriteInput {
    pub(super) ctx: RequestContext,
    pub(super) file_handle: u64,
    pub(super) lease_id: Option<LeaseId>,
    pub(super) lease_epoch: u64,
    pub(super) open_epoch: u64,
    pub(super) fencing_token: Option<PresentedFencingToken>,
    pub(super) intent: CloseWriteIntent,
    pub(super) freshness: Freshness,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct CloseWriteOutput {
    pub(crate) committed_size: u64,
    pub(crate) file_version: Option<u64>,
}

pub(crate) struct CommitFileArgs {
    pub(crate) handle: PresentedWriteHandle,
    pub(crate) data_handle_id: DataHandleId,
    pub(crate) committed_blocks: Vec<CommittedBlock>,
    pub(crate) final_size: u64,
    pub(crate) freshness: Freshness,
}

pub(crate) struct SyncWriteArgs {
    pub(crate) handle: PresentedWriteHandle,
    pub(crate) data_handle_id: DataHandleId,
    pub(crate) committed_blocks: Vec<CommittedBlock>,
    pub(crate) target_size: u64,
    pub(crate) flags: u32,
    pub(crate) mode: SyncWriteMode,
    pub(crate) freshness: Freshness,
}

impl MetadataFileSystem {
    pub(crate) async fn commit_file(&self, ctx: &RequestContext, args: CommitFileArgs) -> FsResult<CloseWriteOutput> {
        if let Some(failure) = self.session_write_admission_failure(ctx, args.handle.file_handle).await {
            return self.failure_from_admission(failure);
        }
        if args
            .committed_blocks
            .iter()
            .any(|block| block.block_id.data_handle_id != args.data_handle_id)
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
            .close_write_resolved(CloseWriteInput {
                ctx: ctx.clone(),
                file_handle: handle.file_handle,
                lease_id: handle.lease_id,
                lease_epoch: handle.lease_epoch,
                open_epoch: handle.open_epoch,
                fencing_token: handle.fencing_token,
                intent: CloseWriteIntent {
                    committed_blocks: args.committed_blocks,
                    final_size: args.final_size,
                },
                freshness: args.freshness,
            })
            .await;
        let lease_id = handle.lease_id.map(|lease_id| lease_id.as_raw());
        match &result {
            Ok(success) => tracing::info!(
                target: "metadata.state",
                op = "CommitFile",
                result = "committed",
                error_code = "none",
                client_id = %ctx.caller.client.client_id,
                call_id = %ctx.caller.client.call_id,
                data_handle_id = args.data_handle_id.as_raw(),
                file_handle = handle.file_handle,
                final_size = args.final_size,
                committed_block_count,
                committed_bytes,
                lease_id,
                lease_epoch = handle.lease_epoch,
                file_version = success.payload.file_version,
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
                data_handle_id = args.data_handle_id.as_raw(),
                file_handle = handle.file_handle,
                final_size = args.final_size,
                committed_block_count,
                committed_bytes,
                lease_id,
                lease_epoch = handle.lease_epoch,
                mount_epoch = failure.mount_epoch,
                route_epoch = failure.route_epoch,
                "CommitFile rejected"
            ),
        }
        result
    }

    pub(crate) async fn sync_write(&self, ctx: &RequestContext, args: SyncWriteArgs) -> FsResult<SyncWriteOutput> {
        if let Some(failure) = self.session_write_admission_failure(ctx, args.handle.file_handle).await {
            return self.failure_from_admission(failure);
        }
        if args
            .committed_blocks
            .iter()
            .any(|block| block.block_id.data_handle_id != args.data_handle_id)
        {
            return self.failure_from_error(
                ctx,
                MetadataError::InvalidArgument("committed block data_handle_id does not match request".to_string()),
                None,
                None,
            );
        }

        let handle = args.handle;
        self.sync_write_resolved(SyncWriteInput {
            ctx: ctx.clone(),
            file_handle: handle.file_handle,
            lease_id: handle.lease_id,
            lease_epoch: handle.lease_epoch,
            open_epoch: handle.open_epoch,
            fencing_token: handle.fencing_token,
            data_handle_id: args.data_handle_id,
            committed_blocks: args.committed_blocks,
            target_size: args.target_size,
            _flags: args.flags,
            mode: args.mode,
            freshness: args.freshness,
        })
        .await
    }

    fn commit_mode_for_session(session: &crate::session_registry::WriteSession) -> FileCommitMode {
        match session.mode {
            crate::inode_lease::WriteMode::Write => FileCommitMode::Replace,
            crate::inode_lease::WriteMode::Append => FileCommitMode::Append,
        }
    }

    async fn sync_write_resolved(&self, req: SyncWriteInput) -> FsResult<SyncWriteOutput> {
        let file_handle = req.file_handle;

        let session = match self.session_registry.get_session(file_handle) {
            Some(session) => session,
            None => {
                return self.session_terminal_failure(
                    &req.ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                    format!(
                        "write handle not found for handle={}; SyncWrite cannot be replayed automatically",
                        file_handle,
                    ),
                    None,
                    None,
                );
            }
        };

        let (group_name, mount_epoch) =
            match self
                .freshness_validator
                .validate_mount_epoch(&req.ctx, req.freshness, session.mount_id)
            {
                Ok(hints) => hints,
                Err(err) => return Err(err),
            };

        let route_epoch = match self
            .freshness_validator
            .validate_route_epoch(&req.ctx, req.freshness, group_name.clone(), mount_epoch, "SyncWrite")
            .await
        {
            Ok(route_epoch) => route_epoch,
            Err(err) => return Err(err),
        };

        if req.data_handle_id != session.data_handle_id {
            return self.failure_from_error_with_route_epoch(
                &req.ctx,
                MetadataError::InvalidArgument(format!(
                    "SyncWrite data_handle_id {} does not match write handle data_handle_id {}",
                    req.data_handle_id, session.data_handle_id
                )),
                group_name,
                mount_epoch,
                route_epoch,
            );
        }
        for block in &req.committed_blocks {
            if block.block_id.data_handle_id != session.data_handle_id {
                return self.failure_from_error_with_route_epoch(
                    &req.ctx,
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

        let lease_id_typed = match req.lease_id {
            Some(lease_id) => lease_id,
            None => {
                return self.failure_from_error_with_route_epoch(
                    &req.ctx,
                    MetadataError::InvalidArgument("Missing lease_id".to_string()),
                    group_name,
                    mount_epoch,
                    route_epoch,
                );
            }
        };
        if lease_id_typed != session.lease_id || req.lease_epoch != session.lease_epoch {
            return self.session_terminal_failure(
                &req.ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!(
                    "lease/write handle mismatch: expected lease_id={:?} lease_epoch={}, got lease_id={:?} lease_epoch={}; SyncWrite cannot be replayed automatically",
                    session.lease_id,
                    session.lease_epoch,
                    lease_id_typed,
                    req.lease_epoch,
                ),
                group_name,
                mount_epoch,
            );
        }
        if req.open_epoch != session.open_epoch {
            return self.session_terminal_failure(
                &req.ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!(
                    "open_epoch mismatch: expected {}, got {}; SyncWrite cannot be replayed automatically",
                    session.open_epoch, req.open_epoch,
                ),
                group_name,
                mount_epoch,
            );
        }
        let req_token = match req.fencing_token.as_ref() {
            Some(token) => token,
            None => {
                return self.session_terminal_failure(
                    &req.ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                    format!(
                        "missing fencing_token for handle={}; SyncWrite cannot be replayed automatically",
                        file_handle,
                    ),
                    group_name,
                    mount_epoch,
                );
            }
        };
        if !MetadataFileSystem::fencing_token_matches_session(&session, req_token) {
            return self.session_terminal_failure(
                &req.ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!(
                    "fencing_token mismatch for handle={}; SyncWrite cannot be replayed automatically",
                    file_handle,
                ),
                group_name,
                mount_epoch,
            );
        }
        if self
            .lease_manager
            .validate_lease(session.inode_id, lease_id_typed, req.lease_epoch)
            .is_err()
        {
            return self.session_terminal_failure(
                &req.ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionExpired),
                format!(
                    "lease validation rejected for handle={}; SyncWrite cannot be replayed automatically",
                    file_handle,
                ),
                group_name,
                mount_epoch,
            );
        }

        // Durability is a client/worker precondition: metadata publishes the
        // same visible prefix after the client has completed worker durable sync.
        match req.mode {
            SyncWriteMode::Visibility | SyncWriteMode::Durability => {}
        }

        let intent = CloseWriteIntent {
            committed_blocks: req.committed_blocks.clone(),
            final_size: req.target_size,
        };
        let extents = match Self::validate_committed_blocks(&intent, &session) {
            Ok(extents) => extents,
            Err(err) => {
                return Err(self.invalid_sync_write_failure(&req, err.to_string(), group_name, mount_epoch));
            }
        };

        let ctx = match self.route_ctx_for_write_with_error_hints(
            &req.ctx,
            WriteCommandKind::SetAttr,
            &[session.inode_id],
            req.freshness,
            group_name.clone(),
            mount_epoch,
        ) {
            Ok(ctx) => ctx,
            Err(failure) => return Err(failure),
        };

        let dedup = match self.dedup_key(&req.ctx.caller) {
            Ok(k) => k,
            Err(err) => return self.failure_from_error(&req.ctx, err, group_name.clone(), mount_epoch),
        };
        let command = Command::new(
            dedup,
            crate::raft::proposal_timestamp_ms(),
            crate::raft::Mutation::SyncWrite {
                inode_id: session.inode_id,
                extents,
                target_size: req.target_size,
                lease_id: session.lease_id,
                open_epoch: session.open_epoch,
                lease_epoch: req.lease_epoch,
                commit_mode: Self::commit_mode_for_session(&session),
            },
        );
        let file_version = match self.propose_fs_write_command(WriteCommandKind::SetAttr, command).await {
            Ok(FsCommandResult::Ok(ok)) => ok.file_version,
            Ok(FsCommandResult::Err(err)) => {
                return self.fatal_fs_failure(
                    &req.ctx,
                    err.errno,
                    err.message,
                    Some(ctx.namespace_owner_group_name.clone()),
                    Some(ctx.mount_epoch),
                );
            }
            Err(err) => {
                return self.failure_from_error(
                    &req.ctx,
                    err,
                    Some(ctx.namespace_owner_group_name.clone()),
                    Some(ctx.mount_epoch),
                );
            }
        };

        self.success_with_route_epoch(
            &req.ctx,
            SyncWriteOutput {
                synced_size: req.target_size,
                file_version,
            },
            Some(ctx.namespace_owner_group_name.clone()),
            Some(ctx.mount_epoch),
            route_epoch,
        )
    }

    fn invalid_commit_failure(
        &self,
        req: &CloseWriteInput,
        message: impl Into<String>,
        group_name: Option<GroupName>,
        mount_epoch: Option<u64>,
    ) -> FsFailure {
        match self.fatal_fs_failure::<()>(&req.ctx, FsErrorCode::EInval, message, group_name, mount_epoch) {
            Err(failure) => failure,
            Ok(_) => unreachable!("fatal_fs_failure always returns Err"),
        }
    }

    fn invalid_sync_write_failure(
        &self,
        req: &SyncWriteInput,
        message: impl Into<String>,
        group_name: Option<GroupName>,
        mount_epoch: Option<u64>,
    ) -> FsFailure {
        match self.fatal_fs_failure::<()>(&req.ctx, FsErrorCode::EInval, message, group_name, mount_epoch) {
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
                file_version: None,
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

    pub(super) async fn close_write_resolved(&self, req: CloseWriteInput) -> FsResult<CloseWriteOutput> {
        let caller_ctx = &req.ctx.caller;
        let file_handle = req.file_handle;
        let dedup = match self.dedup_key(caller_ctx) {
            Ok(k) => k,
            Err(err) => return self.failure_from_error(&req.ctx, err, None, None),
        };

        let session = match self.session_registry.get_session(file_handle) {
            Some(session) => session,
            None => {
                if let Some(replay) = self.replay_close_write_if_applied(&req, &dedup).await {
                    return replay;
                }
                return self.session_terminal_failure(
                    &req.ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                    format!(
                        "write handle not found for handle={}; CommitFile cannot be replayed automatically",
                        file_handle,
                    ),
                    None,
                    None,
                );
            }
        };

        let (group_name, mount_epoch) =
            match self
                .freshness_validator
                .validate_mount_epoch(&req.ctx, req.freshness, session.mount_id)
            {
                Ok(hints) => hints,
                Err(err) => return Err(err),
            };

        let route_epoch = match self
            .freshness_validator
            .validate_route_epoch(&req.ctx, req.freshness, group_name.clone(), mount_epoch, "CommitFile")
            .await
        {
            Ok(route_epoch) => route_epoch,
            Err(err) => return Err(err),
        };

        if let Some(worker_manager) = self.worker_manager.as_ref() {
            let worker_lookup_group_name =
                self.require_worker_lookup_group(&req.ctx, group_name.clone(), mount_epoch, route_epoch, "CommitFile")?;
            for target in &session.write_targets {
                for endpoint in &target.worker_endpoints {
                    let worker_id = endpoint.worker_id;
                    let current_run_id = worker_manager
                        .get_registration(&worker_lookup_group_name, worker_id)
                        .map(|registration| registration.worker_run_id);
                    if !current_run_id.is_some_and(|run_id| run_id.matches(endpoint.worker_run_id)) {
                        let hint = worker_refresh_hint_from_session(&session, true);
                        return self.refresh_metadata_failure_with_hint(
                            &req.ctx,
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

        let lease_id_typed = match req.lease_id {
            Some(lease_id) => lease_id,
            None => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::InvalidArgument("Missing lease_id".to_string()),
                    group_name,
                    mount_epoch,
                );
            }
        };
        let request_lease_epoch = req.lease_epoch;

        if lease_id_typed != session.lease_id || request_lease_epoch != session.lease_epoch {
            return self.session_terminal_failure(
                &req.ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!(
                    "lease/write handle mismatch: expected lease_id={:?} lease_epoch={}, got lease_id={:?} lease_epoch={}; CommitFile cannot be replayed automatically",
                    session.lease_id,
                    session.lease_epoch,
                    lease_id_typed,
                    request_lease_epoch,
                ),
                group_name,
                mount_epoch,
            );
        }

        let req_token = match req.fencing_token.as_ref() {
            Some(token) => token,
            None => {
                return self.session_terminal_failure(
                    &req.ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                    format!(
                        "missing fencing_token for handle={}; CommitFile cannot be replayed automatically",
                        file_handle,
                    ),
                    group_name,
                    mount_epoch,
                );
            }
        };
        if !MetadataFileSystem::fencing_token_matches_session(&session, req_token) {
            return self.session_terminal_failure(
                &req.ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!(
                    "fencing_token mismatch for handle={}; CommitFile cannot be replayed automatically",
                    file_handle,
                ),
                group_name,
                mount_epoch,
            );
        }

        if req.open_epoch != session.open_epoch {
            return self.session_terminal_failure(
                &req.ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!(
                    "open_epoch mismatch: expected {}, got {}; CommitFile cannot be replayed automatically",
                    session.open_epoch, req.open_epoch,
                ),
                group_name,
                mount_epoch,
            );
        }

        if self
            .lease_manager
            .validate_lease(session.inode_id, lease_id_typed, request_lease_epoch)
            .is_err()
        {
            return self.session_terminal_failure(
                &req.ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionExpired),
                format!(
                    "lease validation rejected for handle={}; write lease expired and CommitFile cannot be replayed automatically",
                    file_handle,
                ),
                group_name,
                mount_epoch,
            );
        }

        let extents = match Self::validate_committed_blocks(&req.intent, &session) {
            Ok(extents) => extents,
            Err(err) => {
                return Err(self.invalid_commit_failure(&req, err.to_string(), group_name.clone(), mount_epoch))
            }
        };

        let ctx = match self.route_ctx_for_write_with_error_hints(
            &req.ctx,
            WriteCommandKind::SetAttr,
            &[session.inode_id],
            req.freshness,
            group_name.clone(),
            mount_epoch,
        ) {
            Ok(ctx) => ctx,
            Err(failure) => return Err(failure),
        };

        let command = Command::new(
            dedup,
            crate::raft::proposal_timestamp_ms(),
            crate::raft::Mutation::CloseWrite {
                inode_id: session.inode_id,
                extents,
                final_size: req.intent.final_size,
                lease_id: session.lease_id,
                open_epoch: session.open_epoch,
                lease_epoch: request_lease_epoch,
                commit_mode: Self::commit_mode_for_session(&session),
            },
        );
        let file_version = match self.propose_fs_write_command(WriteCommandKind::SetAttr, command).await {
            Ok(FsCommandResult::Ok(ok)) => ok.file_version,
            Ok(FsCommandResult::Err(err)) => {
                return self.fatal_fs_failure(
                    &req.ctx,
                    err.errno,
                    err.message,
                    Some(ctx.namespace_owner_group_name.clone()),
                    Some(ctx.mount_epoch),
                );
            }
            Err(err) => {
                return self.failure_from_error(
                    &req.ctx,
                    err,
                    Some(ctx.namespace_owner_group_name.clone()),
                    Some(ctx.mount_epoch),
                );
            }
        };

        self.lease_manager
            .release(session.inode_id, lease_id_typed, session.lease_epoch);
        self.session_registry.remove_session(file_handle);

        self.success_with_route_epoch(
            &req.ctx,
            CloseWriteOutput {
                committed_size: req.intent.final_size,
                file_version,
            },
            Some(ctx.namespace_owner_group_name.clone()),
            Some(ctx.mount_epoch),
            route_epoch,
        )
    }

    async fn replay_close_write_if_applied(
        &self,
        req: &CloseWriteInput,
        dedup: &DedupKey,
    ) -> Option<FsResult<CloseWriteOutput>> {
        let storage = &self.storage;
        let applied = match storage.get_applied_result(dedup) {
            Ok(Some(applied)) => applied,
            Ok(None) => return None,
            Err(err) => return Some(self.failure_from_error(&req.ctx, err, None, None)),
        };

        let (command, group_name, mount_epoch) =
            match self.close_write_replay_command(req, dedup, storage, applied.fingerprint) {
                Ok(replay) => replay,
                Err(err) => return Some(self.failure_from_error(&req.ctx, err, None, None)),
            };
        let fingerprint = command.fingerprint();
        if applied.fingerprint != fingerprint {
            return Some(self.failure_from_error(
                &req.ctx,
                MetadataError::InvalidArgument(format!(
                    "call_id {} reused with different command payload",
                    dedup.call_id
                )),
                group_name,
                mount_epoch,
            ));
        }

        let route_epoch = match self.authoritative_route_epoch().await {
            Ok(route_epoch) => Some(route_epoch),
            Err(error) => return Some(self.failure_from_error(&req.ctx, error, group_name, mount_epoch)),
        };
        match applied.result {
            AppDataResponse::Fs(FsCommandResult::Ok(ok)) => Some(self.success_with_route_epoch(
                &req.ctx,
                CloseWriteOutput {
                    committed_size: req.intent.final_size,
                    file_version: ok.file_version,
                },
                group_name,
                mount_epoch,
                route_epoch,
            )),
            AppDataResponse::Fs(FsCommandResult::Err(err)) => {
                Some(self.fatal_fs_failure(&req.ctx, err.errno, err.message, group_name, mount_epoch))
            }
            _ => Some(self.failure_from_error(
                &req.ctx,
                MetadataError::InvalidArgument(format!(
                    "applied result for call_id {} is not a CloseWrite filesystem result",
                    dedup.call_id
                )),
                group_name,
                mount_epoch,
            )),
        }
    }

    fn close_write_replay_command(
        &self,
        req: &CloseWriteInput,
        dedup: &DedupKey,
        storage: &RocksDBStorage,
        applied_fingerprint: CommandFingerprint,
    ) -> MetadataResult<(Command, Option<GroupName>, Option<u64>)> {
        let lease_id = req
            .lease_id
            .ok_or_else(|| MetadataError::InvalidArgument("Missing lease_id".to_string()))?;
        let token = req
            .fencing_token
            .as_ref()
            .ok_or_else(|| MetadataError::InvalidArgument("Missing fencing_token".to_string()))?;
        let token_block = token
            .block_id
            .ok_or_else(|| MetadataError::InvalidArgument("Missing fencing_token block_id".to_string()))?;
        let inode_id = storage
            .get_inode_by_data_handle(token_block.data_handle_id)?
            .ok_or_else(|| {
                MetadataError::StaleState(format!(
                    "Missing owner for data_handle_id {}, refresh metadata state",
                    token_block.data_handle_id
                ))
            })?;
        let inode = self
            .read_inode(inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {}", inode_id)))?;
        let (group_name, mount_epoch) = self.freshness_validator.mount_hints_for_mount(inode.mount_id);

        let extents: Vec<_> = req
            .intent
            .committed_blocks
            .iter()
            .map(|block| Extent {
                file_offset: block.file_offset,
                block_id: block.block_id,
                block_offset: 0,
                len: block.len,
                file_version: None,
                block_stamp: None,
            })
            .collect();

        let build_command = |commit_mode| {
            Command::new(
                dedup.clone(),
                crate::raft::proposal_timestamp_ms(),
                crate::raft::Mutation::CloseWrite {
                    inode_id,
                    extents: extents.clone(),
                    final_size: req.intent.final_size,
                    lease_id,
                    open_epoch: req.open_epoch,
                    lease_epoch: req.lease_epoch,
                    commit_mode,
                },
            )
        };

        let replace = build_command(FileCommitMode::Replace);
        let append = build_command(FileCommitMode::Append);
        let command = if replace.fingerprint() == applied_fingerprint {
            replace
        } else if append.fingerprint() == applied_fingerprint {
            append
        } else {
            return Err(MetadataError::InvalidArgument(format!(
                "call_id {} reused with different command payload",
                dedup.call_id
            )));
        };

        Ok((command, group_name, mount_epoch))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::filesystem::test_support::*;

    async fn sync_for_key(
        filesystem: &MetadataFileSystem,
        key: &SessionKey,
        committed_blocks: Vec<CommittedBlock>,
        target_size: u64,
        mode: SyncWriteMode,
    ) -> FsResult<SyncWriteOutput> {
        filesystem
            .sync_write_resolved(SyncWriteInput {
                ctx: request_context(),
                file_handle: key.file_handle,
                lease_id: Some(key.lease_id),
                lease_epoch: key.lease_epoch,
                open_epoch: key.open_epoch,
                fencing_token: Some(presented_key_token(key)),
                data_handle_id: key.fencing_token.block_id.data_handle_id,
                committed_blocks,
                target_size,
                _flags: 0,
                mode,
                freshness: Freshness::default(),
            })
            .await
    }

    #[tokio::test]
    async fn commit_worker_run_check_rejects_missing_authoritative_group() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(53);
        let inode_id = InodeId::new(530);
        let data_handle_id = DataHandleId::new(9530);
        let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        let worker_id = WorkerId::new(1);
        let filesystem = filesystem_builder_without_mount()
            .with_storage(Arc::clone(&storage))
            .with_worker_manager(worker_manager_for_write_targets(&group_name("root")))
            .build();

        storage
            .put_inode(&Inode::new_file(inode_id, FileAttrs::new(), mount_id, data_handle_id))
            .unwrap();
        let writer = ClientId::new(7);
        let (lease_id, lease_epoch, _) = filesystem
            .lease_manager()
            .try_acquire(
                inode_id,
                writer,
                Some(types::CallId::new()),
                crate::inode_lease::WriteMode::Write,
                None,
            )
            .expect("lease acquired");
        let file_handle = filesystem
            .session_registry()
            .create_session(crate::session_registry::CreateSessionInput {
                inode_id,
                mount_id,
                data_handle_id,
                lease_id,
                lease_epoch,
                fencing_token: FencingToken {
                    block_id,
                    owner: writer,
                    epoch: lease_epoch,
                },
                open_epoch: 777,
                base_size: 0,
                mode: crate::inode_lease::WriteMode::Write,
                write_targets: vec![WriteTarget {
                    block_id,
                    file_offset: 0,
                    block_size: 64,
                    effective_len: 64,
                    worker_endpoints: vec![WorkerEndpointInfo {
                        worker_id,
                        endpoint: "127.0.0.1:9001".to_string(),
                        worker_net_protocol: WorkerNetProtocol::Grpc,
                        worker_run_id: worker_run_id(&group_name("root"), worker_id),
                    }],
                    fencing_token: FencingToken {
                        block_id,
                        owner: writer,
                        epoch: lease_epoch,
                    },
                    block_stamp: 1,
                    chunk_size: 64,
                    block_format_id: types::BlockFormatId::CURRENT_FOR_NEW_FILE,
                    tier: types::Tier::Hdd,
                }],
            });
        let session = filesystem
            .write_session_for_handle(file_handle)
            .expect("session should be installed");
        filesystem
            .add_block_resolved(AddBlockInput {
                ctx: request_context(),
                file_handle,
                lease_id: Some(session.lease_id),
                lease_epoch: session.lease_epoch,
                open_epoch: session.open_epoch,
                fencing_token: Some(presented_session_token(&session)),
                desired_len: Some(64),
                freshness: Freshness::default(),
            })
            .await
            .expect("preallocated target should be issued");
        let session = filesystem
            .write_session_for_handle(file_handle)
            .expect("session should remain installed");

        let failure = filesystem
            .close_write_resolved(CloseWriteInput {
                ctx: request_context(),
                file_handle,
                lease_id: Some(session.lease_id),
                lease_epoch: session.lease_epoch,
                open_epoch: session.open_epoch,
                fencing_token: Some(presented_session_token(&session)),
                intent: CloseWriteIntent {
                    committed_blocks: vec![committed_block(block_id, 0, 64)],
                    final_size: 64,
                },
                freshness: Freshness::default(),
            })
            .await
            .expect_err("missing authoritative group must reject commit worker lookup");

        assert!(
            failure
                .error
                .message
                .contains("CommitFile worker lookup requires authoritative metadata group"),
            "unexpected error: {}",
            failure.error.message
        );
    }

    #[tokio::test]
    async fn commit_advances_file_version() {
        let env = write_flow_env(64).await;
        seed_committed_file_version(&env, 41, 900);

        let open = env
            .filesystem
            .open_write_resolved(OpenWriteInput {
                ctx: request_context(),
                inode_id: env.inode_id,
                desired_len: Some(64),
                mode: crate::inode_lease::WriteMode::Write,
                freshness: Freshness::default(),
            })
            .await
            .expect("replace open should succeed");
        let key = open.payload.session_key;
        assert!(key.lease_epoch > 41);
        let target = add_block_for_key(&env.filesystem, &key, 64).await;

        let close = commit_for_key(
            &env.filesystem,
            &key,
            vec![committed_block(
                target.block_id,
                target.file_offset,
                target.effective_len,
            )],
            64,
        )
        .await
        .expect("replace commit should succeed");

        assert_eq!(close.payload.file_version, Some(42));
        assert_eq!(stored_file_version(&env.storage, env.inode_id), Some(42));
    }

    #[tokio::test]
    async fn commit_worker_run_check_uses_session_group() {
        let env = write_flow_env(0).await;
        let open = env
            .filesystem
            .open_write_resolved(OpenWriteInput {
                ctx: request_context(),
                inode_id: env.inode_id,
                desired_len: Some(64),
                mode: crate::inode_lease::WriteMode::Write,
                freshness: Freshness::default(),
            })
            .await
            .expect("open write should succeed");
        let key = open.payload.session_key;
        let target = add_block_for_key(&env.filesystem, &key, 64).await;
        let endpoint = target.worker_endpoints.first().expect("worker endpoint").clone();
        let worker_manager = env.filesystem.worker_manager.as_ref().expect("worker manager");
        let other_group = group_name("other");

        worker_manager
            .load_registered_workers(vec![WorkerInfo {
                group_name: other_group,
                worker_id: endpoint.worker_id,
                address: "127.0.0.1:9999".to_string(),
                worker_net_protocol: 1,
                capacity_total: 0,
                capacity_used: 0,
                capacity_available: 0,
                active_reads: 0,
                active_writes: 0,
                health: HealthStatus::Healthy,
                last_heartbeat: 0,
                fault_domain: None,
            }])
            .expect("replace manager descriptors with another group");

        let failure = commit_for_key(
            &env.filesystem,
            &key,
            vec![committed_block(
                target.block_id,
                target.file_offset,
                target.effective_len,
            )],
            64,
        )
        .await
        .expect_err("commit must not validate worker_run_id against another group's registration");

        assert_refresh_metadata(&failure.error, ErrorKind::Worker(WorkerErrorKind::RunMismatch));
        assert!(failure.error.message.contains("worker_run_id mismatch"));
    }

    #[tokio::test]
    async fn commit_worker_run_check_rejects_stale_live_registration() {
        let env = write_flow_env(0).await;
        let open = env
            .filesystem
            .open_write_resolved(OpenWriteInput {
                ctx: request_context(),
                inode_id: env.inode_id,
                desired_len: Some(64),
                mode: crate::inode_lease::WriteMode::Write,
                freshness: Freshness::default(),
            })
            .await
            .expect("open write should succeed");
        let key = open.payload.session_key;
        let target = add_block_for_key(&env.filesystem, &key, 64).await;
        let endpoint = target.worker_endpoints.first().expect("worker endpoint").clone();
        let worker_manager = env.filesystem.worker_manager.as_ref().expect("worker manager");
        let replacement_run_id: WorkerRunId = "550e8400-e29b-41d4-a716-44665544f00d"
            .parse()
            .expect("valid replacement WorkerRunId");

        worker_manager
            .load_registered_workers(vec![WorkerInfo {
                group_name: env.group_name.clone(),
                worker_id: endpoint.worker_id,
                address: endpoint.endpoint.clone(),
                worker_net_protocol: 1,
                capacity_total: 0,
                capacity_used: 0,
                capacity_available: 0,
                active_reads: 0,
                active_writes: 0,
                health: HealthStatus::Healthy,
                last_heartbeat: 0,
                fault_domain: None,
            }])
            .expect("replace manager descriptors after metadata reload");
        worker_manager
            .register_worker_run(
                &env.group_name,
                endpoint.worker_id,
                endpoint.endpoint.clone(),
                1,
                replacement_run_id,
                None,
            )
            .expect("replacement worker run should register after reload");

        let failure = commit_for_key(
            &env.filesystem,
            &key,
            vec![committed_block(
                target.block_id,
                target.file_offset,
                target.effective_len,
            )],
            64,
        )
        .await
        .expect_err("commit must reject stale worker process-run identity");

        assert_refresh_metadata(&failure.error, ErrorKind::Worker(WorkerErrorKind::RunMismatch));
        assert!(failure.error.message.contains("worker_run_id mismatch"));
    }

    #[tokio::test]
    async fn create_new_commit_returns_initial_file_version() {
        let env = write_flow_env(0).await;
        let open = env
            .filesystem
            .open_write_resolved(OpenWriteInput {
                ctx: request_context(),
                inode_id: env.inode_id,
                desired_len: Some(64),
                mode: crate::inode_lease::WriteMode::Write,
                freshness: Freshness::default(),
            })
            .await
            .expect("open write should succeed");
        let key = open.payload.session_key;
        let target = add_block_for_key(&env.filesystem, &key, 64).await;

        let close = commit_for_key(
            &env.filesystem,
            &key,
            vec![committed_block(
                target.block_id,
                target.file_offset,
                target.effective_len,
            )],
            64,
        )
        .await
        .expect("commit should succeed");

        assert_eq!(close.payload.file_version, Some(1));
    }

    #[tokio::test]
    async fn append_advances_file_version() {
        let env = write_flow_env(0).await;
        let first_open = env
            .filesystem
            .open_write_resolved(OpenWriteInput {
                ctx: request_context(),
                inode_id: env.inode_id,
                desired_len: Some(64),
                mode: crate::inode_lease::WriteMode::Write,
                freshness: Freshness::default(),
            })
            .await
            .expect("first open should succeed");
        let first_key = first_open.payload.session_key;
        let first_target = add_block_for_key(&env.filesystem, &first_key, 64).await;
        let first_close = commit_for_key(
            &env.filesystem,
            &first_key,
            vec![committed_block(
                first_target.block_id,
                first_target.file_offset,
                first_target.effective_len,
            )],
            64,
        )
        .await
        .expect("first commit should succeed");

        let second_open = env
            .filesystem
            .open_write_resolved(OpenWriteInput {
                ctx: request_context(),
                inode_id: env.inode_id,
                desired_len: Some(64),
                mode: crate::inode_lease::WriteMode::Append,
                freshness: Freshness::default(),
            })
            .await
            .expect("append open should succeed");
        let second_key = second_open.payload.session_key;
        let second_target = add_block_for_key(&env.filesystem, &second_key, 64).await;
        let second_close = commit_for_key(
            &env.filesystem,
            &second_key,
            vec![committed_block(
                second_target.block_id,
                second_target.file_offset,
                second_target.effective_len,
            )],
            128,
        )
        .await
        .expect("second commit should succeed");

        assert_eq!(first_close.payload.file_version, Some(1));
        assert_eq!(second_close.payload.file_version, Some(2));
        assert_eq!(stored_file_version(&env.storage, env.inode_id), Some(2));
    }

    #[tokio::test]
    async fn close_write_invalid_lease_or_fencing_does_not_clear_runtime_session() {
        let mount_id = MountId::new(53);
        let group_name_value = group_name("g11");
        let inode_id = InodeId::new(530);
        let filesystem = filesystem_with_mount(mount_id, 9, &group_name_value);
        let file_handle = install_write_session(&filesystem, inode_id, mount_id);
        let session = filesystem
            .write_session_for_handle(file_handle)
            .expect("session should be installed");

        let wrong_lease = filesystem
            .close_write_resolved(CloseWriteInput {
                ctx: request_context(),
                file_handle,
                lease_id: Some(types::ids::LeaseId::new(session.lease_id.as_raw() + 1)),
                lease_epoch: session.lease_epoch,
                open_epoch: session.open_epoch,
                fencing_token: Some(PresentedFencingToken {
                    block_id: Some(session.fencing_token.block_id),
                    owner: session.fencing_token.owner,
                    epoch: session.fencing_token.epoch,
                }),
                intent: CloseWriteIntent {
                    committed_blocks: Vec::new(),
                    final_size: 0,
                },
                freshness: Freshness::default(),
            })
            .await
            .unwrap_err();

        assert_reopen_write_session(
            &wrong_lease.error,
            ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
        );
        assert!(filesystem.write_session_for_handle(file_handle).is_some());
        assert!(filesystem.lease_manager().get_active_lease(inode_id).is_some());

        let wrong_fencing = filesystem
            .close_write_resolved(CloseWriteInput {
                ctx: request_context(),
                file_handle,
                lease_id: Some(session.lease_id),
                lease_epoch: session.lease_epoch,
                open_epoch: session.open_epoch,
                fencing_token: Some(PresentedFencingToken {
                    block_id: Some(BlockId::new(DataHandleId::new(999_999), BlockIndex::new(0))),
                    owner: session.fencing_token.owner,
                    epoch: session.fencing_token.epoch,
                }),
                intent: CloseWriteIntent {
                    committed_blocks: Vec::new(),
                    final_size: 0,
                },
                freshness: Freshness::default(),
            })
            .await
            .unwrap_err();

        assert_reopen_write_session(
            &wrong_fencing.error,
            ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
        );
        assert!(filesystem.write_session_for_handle(file_handle).is_some());
        assert!(filesystem.lease_manager().get_active_lease(inode_id).is_some());
    }

    #[tokio::test]
    async fn commit_rejects_unissued_block() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(56);
        let group_name_value = group_name("g14");
        let inode_id = InodeId::new(560);
        let data_handle_id = DataHandleId::new(424_242);
        let builder = filesystem_builder_with_mount(mount_id, 9, &group_name_value);
        let mount_table = builder.mount_table();
        let (raft_node, _state_machine) = single_node_raft(Arc::clone(&storage), mount_table).await;
        let filesystem = builder
            .with_storage(Arc::clone(&storage))
            .with_raft_node(raft_node)
            .build();
        storage
            .put_inode(&Inode::new_file(inode_id, FileAttrs::new(), mount_id, data_handle_id))
            .unwrap();
        storage
            .put_layout(inode_id, FileLayout::try_new(4096, 4096, 1).unwrap())
            .unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        let file_handle = install_write_session(&filesystem, inode_id, mount_id);
        let session = filesystem
            .write_session_for_handle(file_handle)
            .expect("session should be installed");
        let failure = filesystem
            .close_write_resolved(CloseWriteInput {
                ctx: request_context(),
                file_handle,
                lease_id: Some(session.lease_id),
                lease_epoch: session.lease_epoch,
                open_epoch: session.open_epoch,
                fencing_token: Some(PresentedFencingToken {
                    block_id: Some(session.fencing_token.block_id),
                    owner: session.fencing_token.owner,
                    epoch: session.fencing_token.epoch,
                }),
                intent: CloseWriteIntent {
                    committed_blocks: vec![committed_block(BlockId::new(data_handle_id, BlockIndex::new(0)), 0, 64)],
                    final_size: 64,
                },
                freshness: Freshness::default(),
            })
            .await
            .expect_err("unissued block must be rejected");

        assert_fail(&failure.error, ErrorKind::Fs(FsErrorCode::EInval));
        assert!(filesystem.write_session_for_handle(file_handle).is_some());
    }

    #[tokio::test]
    async fn commit_rejects_invalid_block_sequence() {
        let env = write_flow_env(0).await;
        let open = env
            .filesystem
            .open_write_resolved(OpenWriteInput {
                ctx: request_context(),
                inode_id: env.inode_id,
                desired_len: Some(256),
                mode: crate::inode_lease::WriteMode::Write,
                freshness: Freshness::default(),
            })
            .await
            .expect("open write should succeed");
        let key = open.payload.session_key;
        let target = add_block_for_key(&env.filesystem, &key, 256).await;
        let block = committed_block(target.block_id, target.file_offset, target.effective_len);

        let failure = commit_for_key(&env.filesystem, &key, vec![block.clone(), block], 256)
            .await
            .expect_err("duplicate committed block must be rejected");

        assert_fail(&failure.error, ErrorKind::Fs(FsErrorCode::EInval));
        assert!(env.filesystem.write_session_for_handle(key.file_handle).is_some());
        let committed = committed_block(target.block_id, target.file_offset + 1, target.effective_len);

        let failure = commit_for_key(&env.filesystem, &key, vec![committed], 257)
            .await
            .expect_err("offset mismatch must be rejected");

        assert_fail(&failure.error, ErrorKind::Fs(FsErrorCode::EInval));
        assert!(env.filesystem.write_session_for_handle(key.file_handle).is_some());
    }

    #[tokio::test]
    async fn sync_write_visibility_publishes_prefix_and_keeps_session_open() {
        let env = write_flow_env(0).await;
        let open = env
            .filesystem
            .open_write_resolved(OpenWriteInput {
                ctx: request_context(),
                inode_id: env.inode_id,
                desired_len: Some(8192),
                mode: crate::inode_lease::WriteMode::Write,
                freshness: Freshness::default(),
            })
            .await
            .expect("open write should succeed");
        let key = open.payload.session_key;
        let first = add_block_for_key(&env.filesystem, &key, 64).await;
        let second = add_block_for_key(&env.filesystem, &key, 64).await;

        let synced = sync_for_key(
            &env.filesystem,
            &key,
            vec![committed_block(first.block_id, first.file_offset, first.effective_len)],
            64,
            SyncWriteMode::Visibility,
        )
        .await
        .expect("visibility sync should succeed");

        assert_eq!(synced.payload.synced_size, 64);
        assert_eq!(env.storage.get_inode(env.inode_id).unwrap().unwrap().attrs.size, 64);
        assert!(synced.payload.file_version.is_some());
        assert!(env.filesystem.write_session_for_handle(key.file_handle).is_some());
        publish_env_block_location(&env, first.block_id, first.block_stamp, 1);

        let layout = env
            .filesystem
            .get_file_layout_resolved(GetFileLayoutInput {
                ctx: request_context(),
                inode_id: env.inode_id,
                range: None,
                requested_data_handle_id: Some(env.data_handle_id),
                freshness: Freshness::default(),
            })
            .await
            .expect("synced prefix should be readable");
        assert_eq!(layout.payload.file_size, 64);
        assert_eq!(layout.payload.locations.len(), 1);
        assert_eq!(layout.payload.locations[0].block_id, first.block_id);

        commit_for_key(
            &env.filesystem,
            &key,
            vec![
                committed_block(first.block_id, first.file_offset, first.effective_len),
                committed_block(second.block_id, second.file_offset, second.effective_len),
            ],
            128,
        )
        .await
        .expect("CommitFile should still close after SyncWrite");
        assert!(env.filesystem.write_session_for_handle(key.file_handle).is_none());
        assert_eq!(env.storage.get_inode(env.inode_id).unwrap().unwrap().attrs.size, 128);
    }

    #[tokio::test]
    async fn sync_write_rejects_target_beyond_committed_block_coverage() {
        let env = write_flow_env(0).await;
        let open = env
            .filesystem
            .open_write_resolved(OpenWriteInput {
                ctx: request_context(),
                inode_id: env.inode_id,
                desired_len: Some(64),
                mode: crate::inode_lease::WriteMode::Write,
                freshness: Freshness::default(),
            })
            .await
            .expect("open write should succeed");
        let key = open.payload.session_key;
        let target = add_block_for_key(&env.filesystem, &key, 64).await;

        let failure = sync_for_key(
            &env.filesystem,
            &key,
            vec![committed_block(
                target.block_id,
                target.file_offset,
                target.effective_len,
            )],
            128,
            SyncWriteMode::Visibility,
        )
        .await
        .expect_err("target beyond committed coverage must be rejected");

        assert_fail(&failure.error, ErrorKind::Fs(FsErrorCode::EInval));
        assert!(env.filesystem.write_session_for_handle(key.file_handle).is_some());
        assert_eq!(env.storage.get_inode(env.inode_id).unwrap().unwrap().attrs.size, 0);
    }

    #[tokio::test]
    async fn repeated_identical_sync_write_is_idempotent_without_file_version_advance() {
        let env = write_flow_env(0).await;
        let open = env
            .filesystem
            .open_write_resolved(OpenWriteInput {
                ctx: request_context(),
                inode_id: env.inode_id,
                desired_len: Some(64),
                mode: crate::inode_lease::WriteMode::Write,
                freshness: Freshness::default(),
            })
            .await
            .expect("open write should succeed");
        let key = open.payload.session_key;
        let target = add_block_for_key(&env.filesystem, &key, 64).await;
        let blocks = vec![committed_block(
            target.block_id,
            target.file_offset,
            target.effective_len,
        )];

        let first = sync_for_key(&env.filesystem, &key, blocks.clone(), 64, SyncWriteMode::Visibility)
            .await
            .expect("first SyncWrite should publish");
        let first_version = stored_file_version(&env.storage, env.inode_id).expect("file version");
        let second = sync_for_key(&env.filesystem, &key, blocks, 64, SyncWriteMode::Visibility)
            .await
            .expect("repeated SyncWrite should be a no-op");

        assert_eq!(second.payload.file_version, first.payload.file_version);
        assert_eq!(stored_file_version(&env.storage, env.inode_id), Some(first_version));
    }

    #[tokio::test]
    async fn append_uses_base_size() {
        let env = write_flow_env(128).await;
        let open = env
            .filesystem
            .open_write_resolved(OpenWriteInput {
                ctx: request_context(),
                inode_id: env.inode_id,
                desired_len: Some(64),
                mode: crate::inode_lease::WriteMode::Append,
                freshness: Freshness::default(),
            })
            .await
            .expect("append open should succeed");
        let key = open.payload.session_key;
        assert_eq!(open.payload.base_size, 128);
        let target = add_block_for_key(&env.filesystem, &key, 64).await;
        assert_eq!(target.file_offset, 128);

        let wrong_offset = committed_block(target.block_id, 0, target.effective_len);
        let failure = commit_for_key(&env.filesystem, &key, vec![wrong_offset], 64)
            .await
            .expect_err("append commit must start at base_size");

        assert_fail(&failure.error, ErrorKind::Fs(FsErrorCode::EInval));
        assert!(env.filesystem.write_session_for_handle(key.file_handle).is_some());
    }

    #[tokio::test]
    async fn replay_keeps_file_version() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(54);
        let group_name_value = group_name("g12");
        let inode_id = InodeId::new(540);
        let data_handle_id = DataHandleId::new(424_242);
        let builder = filesystem_builder_with_mount(mount_id, 9, &group_name_value);
        let mount_table = builder.mount_table();
        let (raft_node, _state_machine) = single_node_raft(Arc::clone(&storage), mount_table).await;
        let filesystem = builder
            .with_storage(Arc::clone(&storage))
            .with_raft_node(raft_node)
            .build();
        storage
            .put_inode(&Inode::new_file(inode_id, FileAttrs::new(), mount_id, data_handle_id))
            .unwrap();
        storage
            .put_layout(inode_id, FileLayout::try_new(4096, 4096, 1).unwrap())
            .unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        let file_handle = install_write_session(&filesystem, inode_id, mount_id);
        let session = filesystem
            .write_session_for_handle(file_handle)
            .expect("session should be installed");
        filesystem
            .session_registry()
            .allocate_target(file_handle, None)
            .expect("target should be issued");
        let ctx = request_context();
        let request = CloseWriteInput {
            ctx,
            file_handle,
            lease_id: Some(session.lease_id),
            lease_epoch: session.lease_epoch,
            open_epoch: session.open_epoch,
            fencing_token: Some(PresentedFencingToken {
                block_id: Some(session.fencing_token.block_id),
                owner: session.fencing_token.owner,
                epoch: session.fencing_token.epoch,
            }),
            intent: CloseWriteIntent {
                committed_blocks: vec![committed_block(BlockId::new(data_handle_id, BlockIndex::new(0)), 0, 64)],
                final_size: 64,
            },
            freshness: Freshness::default(),
        };

        let first = filesystem
            .close_write_resolved(request.clone())
            .await
            .expect("first close should succeed");
        assert_eq!(first.payload.committed_size, 64);
        assert!(filesystem.write_session_for_handle(file_handle).is_none());

        let inode_after_first = storage.get_inode(inode_id).unwrap().unwrap();
        let replay = filesystem
            .close_write_resolved(request.clone())
            .await
            .expect("same close replay should return persisted result");

        assert_eq!(replay.payload.committed_size, first.payload.committed_size);
        assert_eq!(replay.payload.file_version, first.payload.file_version);
        assert!(filesystem.write_session_for_handle(file_handle).is_none());
        assert_eq!(storage.get_inode(inode_id).unwrap().unwrap(), inode_after_first);

        let mut mismatch = request;
        mismatch.intent.final_size = 65;
        let mismatch_failure = filesystem
            .close_write_resolved(mismatch)
            .await
            .expect_err("same call_id with different close payload should fail");
        assert_fail(&mismatch_failure.error, ErrorKind::Fs(FsErrorCode::EInval));
        assert!(filesystem.write_session_for_handle(file_handle).is_none());
        assert_eq!(storage.get_inode(inode_id).unwrap().unwrap(), inode_after_first);
    }

    #[tokio::test]
    async fn replay_keeps_append_commit_mode() {
        let env = write_flow_env(64).await;
        let open = env
            .filesystem
            .open_write_resolved(OpenWriteInput {
                ctx: request_context(),
                inode_id: env.inode_id,
                desired_len: Some(64),
                mode: crate::inode_lease::WriteMode::Append,
                freshness: Freshness::default(),
            })
            .await
            .expect("append open should succeed");
        let key = open.payload.session_key;
        let target = add_block_for_key(&env.filesystem, &key, 64).await;
        assert_eq!(target.file_offset, 64);
        let request = CloseWriteInput {
            ctx: request_context(),
            file_handle: key.file_handle,
            lease_id: Some(key.lease_id),
            lease_epoch: key.lease_epoch,
            open_epoch: key.open_epoch,
            fencing_token: Some(presented_key_token(&key)),
            intent: CloseWriteIntent {
                committed_blocks: vec![committed_block(
                    target.block_id,
                    target.file_offset,
                    target.effective_len,
                )],
                final_size: 128,
            },
            freshness: Freshness::default(),
        };

        let first = env
            .filesystem
            .close_write_resolved(request.clone())
            .await
            .expect("append close should succeed");
        assert_eq!(first.payload.committed_size, 128);
        assert!(env.filesystem.write_session_for_handle(key.file_handle).is_none());

        let replay = env
            .filesystem
            .close_write_resolved(request)
            .await
            .expect("append close replay should recover original commit mode");
        assert_eq!(replay.payload.committed_size, first.payload.committed_size);
        assert!(env.filesystem.write_session_for_handle(key.file_handle).is_none());
    }

    #[tokio::test]
    async fn close_write_session_missing_without_applied_result_stays_session_invalid() {
        let group_name_value = group_name("g13");
        let filesystem = filesystem_with_mount(MountId::new(55), 9, &group_name_value);
        let mut ctx = request_context();
        ctx.caller = ctx.caller.with_group_name(group_name_value.clone());

        let failure = filesystem
            .close_write_resolved(CloseWriteInput {
                ctx,
                file_handle: 999_999,
                lease_id: Some(types::ids::LeaseId::new(1)),
                lease_epoch: 1,
                open_epoch: 1,
                fencing_token: Some(PresentedFencingToken {
                    block_id: Some(BlockId::new(DataHandleId::new(1), BlockIndex::new(0))),
                    owner: ClientId::new(7),
                    epoch: 1,
                }),
                intent: CloseWriteIntent {
                    committed_blocks: Vec::new(),
                    final_size: 0,
                },
                freshness: Freshness::default(),
            })
            .await
            .unwrap_err();

        assert_reopen_write_session(&failure.error, ErrorKind::Metadata(MetadataErrorKind::SessionInvalid));
        assert_eq!(failure.group_name, Some(group_name_value));
    }
}
