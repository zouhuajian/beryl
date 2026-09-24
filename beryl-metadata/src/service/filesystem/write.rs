// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Leader-local write lease, placement, and session lifecycle.

use super::command::unexpected_raft_apply_success;
use super::{
    missing_resolved_target_error, FsFailure, FsResult, FsSuccess, MetadataFileSystem, RequestHeader, WriteHandle,
};
use crate::error::MetadataError;
use crate::observe;
use crate::path_resolver::{PathResolver, ResolvedPath};
use crate::placement::{plan_placement, PlacementOp, PlacementRequest, PlacementStatus};
use crate::raft::{ApplySuccess, Command};
use crate::session_registry::{
    BeginAllocateBlock, BeginAllocateBlockError, BeginSessionError, BeginSessionInput, CompleteWriteTargetError,
    WriteOpeningError, WriteSession, WriteSessionError, WriteTargetLimit,
};
use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind};
use beryl_common::header::CallerContextFields;
use beryl_types::ids::{BlockId, InodeId};
use beryl_types::lease::FencingToken;
use beryl_types::{
    ContentGeneration, LeaseEpoch, LocatedBlock, Tier, WorkerEndpointInfo, WorkerId, WorkerRunId, WriteMode,
};

/// Exact stream facts checked against the active session and durable inode authority.
pub(crate) struct AuthorizeBlockWriteArgs {
    pub block_id: BlockId,
    pub worker_id: WorkerId,
    pub worker_run_id: WorkerRunId,
    pub fencing_token: FencingToken,
    pub write_offset: u64,
    pub block_size: u64,
    pub tier: Tier,
}

/// Acquired writer authority and visible base returned to the client.
#[derive(Clone, Debug)]
pub(crate) struct OpenWriteOutput {
    pub(crate) inode_id: InodeId,
    pub(crate) lease_epoch: LeaseEpoch,
    pub(crate) block_size: u32,
    pub(crate) base_len: u64,
    pub(crate) expires_at_ms: u64,
    pub(crate) generation: ContentGeneration,
    pub(crate) tail_block: Option<LocatedBlock>,
}

/// Caller-supplied allocation predecessor under a write handle and freshness fence.
pub(crate) struct AllocateBlockArgs {
    pub(crate) handle: WriteHandle,
    pub(crate) previous_block_id: Option<BlockId>,
}

impl MetadataFileSystem {
    /// Authorizes one stream from the same session registry used by publication and cleanup.
    /// A linearizable read prevents an old leader from issuing a new write authorization.
    pub(crate) async fn authorize_block_write(
        &self,
        ctx: &RequestHeader,
        args: AuthorizeBlockWriteArgs,
    ) -> FsResult<u64> {
        let inode_id = args.block_id.inode_id;
        self.check_session_write_admission(ctx, inode_id)?;
        let result = async {
            let raft = &self.raft_node;
            raft.read(true, || {
                let invalid = || MetadataError::PermissionDenied("block writer is no longer authorized".into());
                let session = self.session_registry.get_session(inode_id).ok_or_else(invalid)?;
                self.session_registry
                    .validate_session(inode_id, args.fencing_token.epoch)
                    .map_err(|_| invalid())?;
                if session.open_client_id != args.fencing_token.owner || session.lease_epoch != args.fencing_token.epoch
                {
                    return Err(invalid());
                }
                let inode = self.storage.get_inode(inode_id)?.ok_or_else(invalid)?;
                let mount = self.mount_table.get_mount(inode.mount_id).ok_or_else(invalid)?;
                if ctx.group_name.as_ref() != Some(&mount.namespace_owner_group_name) {
                    return Err(invalid());
                }
                let file = inode.file()?;
                if file.lease_epoch != session.lease_epoch {
                    return Err(invalid());
                }
                let target = session
                    .issued_targets
                    .iter()
                    .find(|target| target.block_id == args.block_id)
                    .ok_or_else(invalid)?;
                if target.fencing_token != args.fencing_token
                    || target.block_size != args.block_size
                    || target.tier != args.tier
                    || args.write_offset < target.write_offset
                    || args.write_offset >= target.block_size
                {
                    return Err(invalid());
                }
                if !target.workers.iter().any(|endpoint| {
                    endpoint.worker_id == args.worker_id && endpoint.worker_run_id == args.worker_run_id
                }) {
                    return Err(invalid());
                }
                let manager = &self.worker_manager;
                if !manager
                    .collect_worker_placement_views(&mount.namespace_owner_group_name)
                    .iter()
                    .any(|worker| {
                        worker.worker_id == args.worker_id
                            && worker.worker_run_id == Some(args.worker_run_id)
                            && worker.lease_valid
                    })
                {
                    return Err(invalid());
                }
                let visible_len = match file.blocks.iter().position(|block| *block == args.block_id) {
                    Some(ordinal) => file.block_len(ordinal),
                    None => 0,
                };
                if visible_len > args.write_offset {
                    return Err(invalid());
                }
                Ok(visible_len)
            })
            .await
        }
        .await;
        match result {
            Ok(visible_len) => self.success(visible_len, ctx.group_name.clone()),
            Err(error) => Err(self.failure_from_error(ctx, error, ctx.group_name.clone())),
        }
    }

    /// Admit one allocation or replay without changing file visibility.
    pub(crate) async fn allocate_block(&self, ctx: &RequestHeader, args: AllocateBlockArgs) -> FsResult<LocatedBlock> {
        self.check_session_write_admission(ctx, args.handle.inode_id)?;
        let handle = args.handle;
        let result = self
            .allocate_block_session(ctx, handle.inode_id, handle.lease_epoch, args.previous_block_id)
            .await;
        match &result {
            Ok(success) => {
                let target = &success.payload;
                tracing::info!(
                    target: "metadata.block",
                    op = "AllocateBlock",
                    result = "ok",
                    error_code = "none",
                    client_id = %ctx.client.client_id,
                    call_id = %ctx.client.call_id,
                    block_id = %target.block_id,
                    block_index = target.block_id.index.as_raw(),
                    group_id = success.group_name.as_ref().map(|group| group.as_str()),
                    target_count = target.workers.len(),
                    targets_sample = ?target.workers.iter().take(3).map(|endpoint| endpoint.worker_id.as_raw()).collect::<Vec<_>>(),
                    inode_id = target.block_id.inode_id.as_raw(),
                    handle_inode_id = handle.inode_id.as_raw(),
                    "AllocateBlock succeeded"
                );
            }
            Err(failure) => tracing::warn!(
                target: "metadata.block",
                op = "AllocateBlock",
                result = "rejected",
                error_code = crate::observe::rpc_error_kind(&failure.error),
                client_id = %ctx.client.client_id,
                call_id = %ctx.client.call_id,
                handle_inode_id = handle.inode_id.as_raw(),
                lease_epoch = handle.lease_epoch.as_raw(),
                "AllocateBlock rejected"
            ),
        }
        result
    }

    pub(crate) async fn abort_file_write(&self, ctx: &RequestHeader, handle: WriteHandle) -> FsResult<()> {
        self.check_session_write_admission(ctx, handle.inode_id)?;
        let result = self.abort_session(ctx, handle.inode_id, handle.lease_epoch).await;
        match &result {
            Ok(_) => tracing::info!(
                target: "metadata.state",
                op = "AbortFileWrite",
                result = "completed",
                error_code = "none",
                client_id = %ctx.client.client_id,
                call_id = %ctx.client.call_id,
                inode_id = handle.inode_id.as_raw(),
                lease_epoch = handle.lease_epoch.as_raw(),
                "AbortFileWrite completed"
            ),
            Err(failure) => tracing::warn!(
                target: "metadata.state",
                op = "AbortFileWrite",
                result = "rejected",
                error_code = crate::observe::rpc_error_kind(&failure.error),
                client_id = %ctx.client.client_id,
                call_id = %ctx.client.call_id,
                inode_id = handle.inode_id.as_raw(),
                lease_epoch = handle.lease_epoch.as_raw(),
                "AbortFileWrite rejected"
            ),
        }
        result
    }

    /// Renew an active write session while excluding topology-changing operations.
    ///
    /// The shared topology guard keeps ownership validation and every session
    /// expiry index update within one namespace admission interval.
    pub(crate) async fn renew_lease(&self, ctx: &RequestHeader, handle: WriteHandle) -> FsResult<u64> {
        self.check_session_write_admission(ctx, handle.inode_id)?;
        let _topology_guard = self.namespace_topology.read().await;
        let result = self.renew_session(ctx, handle.inode_id, handle.lease_epoch).await;
        match &result {
            Ok(_) => tracing::info!(
                target: "metadata.state",
                op = "RenewLease",
                result = "completed",
                error_code = "none",
                client_id = %ctx.client.client_id,
                call_id = %ctx.client.call_id,
                inode_id = handle.inode_id.as_raw(),
                lease_epoch = handle.lease_epoch.as_raw(),
                "RenewLease completed"
            ),
            Err(failure) => tracing::warn!(
                target: "metadata.state",
                op = "RenewLease",
                result = "rejected",
                error_code = crate::observe::rpc_error_kind(&failure.error),
                client_id = %ctx.client.client_id,
                call_id = %ctx.client.call_id,
                inode_id = handle.inode_id.as_raw(),
                lease_epoch = handle.lease_epoch.as_raw(),
                "RenewLease rejected"
            ),
        }
        result
    }

    /// Resolve data-write admission from lightweight session identity only.
    pub(super) fn check_session_write_admission(
        &self,
        ctx: &RequestHeader,
        inode_id: InodeId,
    ) -> Result<(), FsFailure> {
        if let Some(session) = self.session_registry.get_session_identity(inode_id) {
            self.check_data_write(ctx, session.mount_id)
        } else {
            self.check_meta_write(ctx)
        }
    }
}

impl MetadataFileSystem {
    async fn abort_session(&self, ctx: &RequestHeader, inode_id: InodeId, lease_epoch: LeaseEpoch) -> FsResult<()> {
        let mount_id = match self.session_registry.get_session_identity(inode_id) {
            Some(session) => {
                if session.open_client_id != ctx.client.client_id {
                    return Err(self.session_terminal_failure(
                        ctx,
                        ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                        format!("AbortFileWrite client does not own inode_id={inode_id}"),
                        None,
                    ));
                }
                if lease_epoch != session.lease_epoch {
                    return Err(self.session_terminal_failure(
                        ctx,
                        ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                        format!(
                            "write handle epoch mismatch for inode_id={inode_id}: expected {}, got {lease_epoch}",
                            session.lease_epoch
                        ),
                        None,
                    ));
                }
                session.mount_id
            }
            None => {
                // A restarted leader can authenticate the initial CreateFile owner
                // from its durable replay record even though local session state is gone.
                let replay = match self.storage.get_create_file_replay_for_inode(inode_id) {
                    Ok(Some(replay))
                        if replay.operation_id.client_id == ctx.client.client_id
                            && lease_epoch == LeaseEpoch::new(1) =>
                    {
                        replay
                    }
                    Ok(Some(_)) | Ok(None) => return self.success((), None),
                    Err(error) => return Err(self.failure_from_error(ctx, error, None)),
                };
                replay.mount_id
            }
        };

        let group_name = self.mount_owner(mount_id);
        self.check_leader(ctx, group_name.clone()).await?;

        match self
            .propose_fs_write_command(
                Command::EndWriteLease { inode_id, lease_epoch },
                move |success| match success {
                    ApplySuccess::WriteLeaseEnded => Ok(()),
                    unexpected => Err(unexpected_raft_apply_success("EndWriteLease", unexpected)),
                },
            )
            .await
        {
            Ok(()) => {}
            Err(err) => return Err(self.failure_from_error(ctx, err, group_name)),
        }
        self.session_registry.remove_session_if_epoch(inode_id, lease_epoch);

        self.success((), group_name)
    }

    async fn renew_session(&self, ctx: &RequestHeader, inode_id: InodeId, lease_epoch: LeaseEpoch) -> FsResult<u64> {
        let session = match self.session_registry.get_session_identity(inode_id) {
            Some(session) => session,
            None => {
                return Err(self.session_terminal_failure(
                    ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                    format!("write session not found for inode_id={inode_id}",),
                    None,
                ));
            }
        };
        if session.open_client_id != ctx.client.client_id {
            return Err(self.session_terminal_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!("RenewLease client does not own inode_id={inode_id}"),
                None,
            ));
        }

        let group_name = self.mount_owner(session.mount_id);

        if lease_epoch != session.lease_epoch {
            return Err(self.session_terminal_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!(
                    "write handle epoch mismatch for inode_id={inode_id}: expected {}, got {}",
                    session.lease_epoch, lease_epoch
                ),
                group_name,
            ));
        }

        let expires_at_ms = match self
            .session_registry
            .renew_session(inode_id, lease_epoch, ctx.client.client_id)
        {
            Ok(expires_at_ms) => expires_at_ms,
            Err(WriteSessionError::Expired) => {
                return Err(self.session_terminal_failure(
                    ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionExpired),
                    format!("lease renewal rejected for inode_id={inode_id}; write lease expired",),
                    group_name,
                ));
            }
            Err(error) => {
                return Err(self.session_terminal_failure(
                    ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                    format!("write session renewal rejected for inode_id={inode_id}: {error:?}"),
                    group_name,
                ));
            }
        };

        self.check_leader(ctx, group_name.clone()).await?;
        self.success(expires_at_ms, group_name)
    }

    /// Install an opening, persist its fencing epoch, and atomically activate it.
    ///
    /// `ancestor_inode_ids` must be the bounded mount-root-to-file chain
    /// captured while namespace topology is stable.
    pub(super) async fn open_write_inode(
        &self,
        ctx: &RequestHeader,
        normalized_path: String,
        inode_id: InodeId,
        ancestor_inode_ids: Vec<InodeId>,
        mode: WriteMode,
    ) -> FsResult<OpenWriteOutput> {
        let inode = match self.storage.get_inode(inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                return Err(self.failure_from_error(
                    ctx,
                    MetadataError::NotFound(format!("Inode not found: {}", inode_id)),
                    None,
                ));
            }
            Err(err) => {
                return Err(self.failure_from_error(ctx, err, None));
            }
        };

        if !inode.file_type().is_file() {
            return Err(self.failure_from_error(
                ctx,
                MetadataError::IsDir(format!("Inode is not a file: {}", inode_id)),
                None,
            ));
        }

        let group_name = self.mount_owner(inode.mount_id);

        self.check_leader(ctx, group_name.clone()).await?;

        let file = inode.file().expect("file kind checked above");
        let block_size = file.block_size;
        let base_epoch = file.lease_epoch;

        let opening = match self.session_registry.begin_session(BeginSessionInput {
            normalized_path,
            inode_id,
            mount_id: inode.mount_id,
            current_lease_epoch: base_epoch,
            mode,
            open_client_id: ctx.client.client_id,
            block_size,
            ancestor_inode_ids,
        }) {
            Ok(opening) => opening,
            Err(BeginSessionError::Busy) => {
                return Err(self.failure_from_error(
                    ctx,
                    MetadataError::Busy(format!(
                        "File already has an opening or active write session: {inode_id}"
                    )),
                    group_name,
                ));
            }
            Err(BeginSessionError::LimitExceeded(rejection)) => {
                return Err(self.failure_from_error(
                    ctx,
                    MetadataError::WriteSessionLimitExceeded(format!(
                        "{} limit {} reached",
                        rejection.limit.label(),
                        rejection.maximum
                    )),
                    group_name,
                ));
            }
            Err(BeginSessionError::LeaseEpochExhausted) => {
                return Err(self.failure_from_error(
                    ctx,
                    MetadataError::ResourceExhausted(format!("write lease epoch exhausted for inode {inode_id}")),
                    group_name,
                ));
            }
            Err(BeginSessionError::OpeningIdExhausted) => {
                return Err(self.failure_from_error(
                    ctx,
                    MetadataError::ResourceExhausted("leader-local write opening identity exhausted".to_string()),
                    group_name,
                ));
            }
            Err(BeginSessionError::InvalidAncestorChain) => {
                return Err(self.failure_from_error(
                    ctx,
                    MetadataError::Internal("validated write session ancestor chain was rejected".to_string()),
                    group_name,
                ));
            }
        };
        let lease_epoch = opening.proposed_lease_epoch();

        let lease_result = self
            .propose_fs_write_command(
                Command::AcquireWriteLease {
                    proposed_at_ms: crate::raft::proposal_timestamp_ms(),
                    inode_id,
                    expected_lease_epoch: base_epoch,
                },
                move |success| match success {
                    ApplySuccess::WriteLeaseAcquired => Ok(()),
                    unexpected => Err(unexpected_raft_apply_success("AcquireWriteLease", unexpected)),
                },
            )
            .await;
        match lease_result {
            Ok(()) => {}
            Err(err) => {
                return Err(self.failure_from_error(ctx, err, group_name));
            }
        }

        // AcquireWriteLease may have ordered behind an earlier publication. Capture
        // the visible file only after its new durable epoch has been installed.
        let snapshot = match self
            .storage
            .get_inode(inode_id)
            .and_then(|inode| inode.ok_or_else(|| MetadataError::NotFound("opened file disappeared".into())))
        {
            Ok(inode) => inode,
            Err(error) => return Err(self.failure_from_error(ctx, error, group_name)),
        };
        let file = match snapshot.file().and_then(|file| {
            file.validate(inode_id)?;
            Ok(file)
        }) {
            Ok(file) if file.lease_epoch == lease_epoch => file,
            Ok(_) => {
                return Err(self.session_terminal_failure(
                    ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                    "opened writer was fenced",
                    group_name,
                ))
            }
            Err(error) => return Err(self.failure_from_error(ctx, error, group_name)),
        };
        let tail_block = if mode == WriteMode::Append && file.len % u64::from(file.block_size) != 0 {
            let group = self.require_worker_lookup_group(ctx, group_name.clone(), "OpenWrite")?;
            match self.locate_append_tail(&group, file, ctx.client.client_id, lease_epoch) {
                Ok(tail) => Some(tail),
                Err(error) => return Err(self.failure_from_error(ctx, error, group_name)),
            }
        } else {
            None
        };
        let session = match opening.activate(file, tail_block) {
            Err(WriteOpeningError::TargetLimit) => {
                return Err(self.failure_from_error(
                    ctx,
                    MetadataError::GlobalWriteTargetLimitExceeded("opening a tail exceeds the target limit".into()),
                    group_name,
                ))
            }

            Ok(result) => result,
            Err(WriteOpeningError::Expired | WriteOpeningError::NotCurrent) => {
                return Err(self.session_terminal_failure(
                    ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionExpired),
                    format!("write opening expired before activation for inode_id={inode_id}"),
                    group_name,
                ));
            }
        };

        self.success(open_write_output(&session), group_name)
    }

    /// Resolves the existing tail on a live replica without allocating a new block identity.
    fn locate_append_tail(
        &self,
        group_name: &beryl_types::GroupName,
        file: &crate::inode::FileData,
        owner: beryl_types::ClientId,
        epoch: LeaseEpoch,
    ) -> Result<LocatedBlock, MetadataError> {
        let ordinal = file
            .blocks
            .len()
            .checked_sub(1)
            .expect("validated partial file must have a tail");
        let block_id = file.blocks[ordinal];
        let len = file.block_len(ordinal);
        let manager = &self.worker_manager;
        let request = PlacementRequest {
            group_name: group_name.clone(),
            op: PlacementOp::Read,
            block_id,
            visible_len: len,
            block_size: file.block_size,
            caller: None,
            existing: &manager.reported_block_locations(group_name, block_id),
        };
        let placement = plan_placement(&request, &manager.collect_worker_placement_views(group_name));
        if placement.status != PlacementStatus::Ok {
            return Err(MetadataError::ServiceUnavailable(placement.failure_message(&request)));
        }
        let tier = placement.workers[0].tier;
        let workers = placement
            .workers
            .into_iter()
            .map(|worker| WorkerEndpointInfo {
                worker_id: worker.worker_id,
                endpoint: worker.endpoint,
                worker_run_id: worker.worker_run_id,
            })
            .collect();
        Ok(LocatedBlock {
            block_id,
            file_offset: ordinal as u64 * u64::from(file.block_size),
            block_size: u64::from(file.block_size),
            workers,
            fencing_token: FencingToken::new(owner, epoch),
            write_offset: len,

            tier,
        })
    }

    /// Replay an issued target or reserve leader-local capacity before Raft allocation.
    ///
    /// The durable allocator never reuses an index. Placement failure or cancellation
    /// after Raft may leave a gap; only a completed reservation becomes replayable.
    /// No Worker data is created and no file contents are published by this method.
    pub(super) async fn allocate_block_session(
        &self,
        ctx: &RequestHeader,
        inode_id: InodeId,
        lease_epoch: LeaseEpoch,
        previous_block_id: Option<BlockId>,
    ) -> FsResult<LocatedBlock> {
        let session = match self.session_registry.get_session_identity(inode_id) {
            Some(session) => session,
            None => {
                return Err(self.session_terminal_failure(
                    ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                    format!("write session not found for inode_id={inode_id}"),
                    None,
                ));
            }
        };
        if session.open_client_id != ctx.client.client_id {
            return Err(self.session_terminal_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!("AllocateBlock client does not own inode_id={inode_id}"),
                None,
            ));
        }

        let group_name = self.mount_owner(session.mount_id);
        self.check_leader(ctx, group_name.clone()).await?;

        if lease_epoch != session.lease_epoch {
            return Err(self.session_terminal_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                format!(
                    "write handle epoch mismatch for inode_id={inode_id}: expected {}, got {}",
                    session.lease_epoch, lease_epoch
                ),
                group_name,
            ));
        }
        if self.session_registry.validate_session(inode_id, lease_epoch).is_err() {
            return Err(self.session_terminal_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::SessionExpired),
                format!("lease validation rejected for inode_id={inode_id}; reopen before AllocateBlock"),
                group_name,
            ));
        }

        let reservation = match self
            .session_registry
            .begin_allocate_block(inode_id, lease_epoch, previous_block_id)
        {
            Ok(BeginAllocateBlock::Replay(target)) => return self.success(target, group_name),
            Ok(BeginAllocateBlock::Reserved(reservation)) => reservation,
            Err(BeginAllocateBlockError::Session(message)) => {
                return Err(self.session_terminal_failure(
                    ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionExpired),
                    format!("AllocateBlock session is no longer current for inode_id={inode_id}: {message}"),
                    group_name,
                ))
            }
            Err(BeginAllocateBlockError::InvalidArgument(message)) => {
                return Err(self.failure_from_error(
                    ctx,
                    MetadataError::InvalidArgument(format!(
                        "AllocateBlock rejected for inode_id={inode_id}: {message}"
                    )),
                    group_name,
                ))
            }
            Err(BeginAllocateBlockError::Pending) => {
                return Err(self.failure_from_error(
                    ctx,
                    MetadataError::Again(format!(
                        "AllocateBlock is already pending for inode_id={inode_id} and predecessor={previous_block_id:?}"
                    )),
                    group_name,
                ))
            }
            Err(BeginAllocateBlockError::PublicationInProgress) => {
                return Err(self.failure_from_error(
                    ctx,
                    MetadataError::Again(format!(
                        "AllocateBlock cannot allocate inode_id={inode_id} while file publication is in progress"
                    )),
                    group_name,
                ))
            }
            Err(BeginAllocateBlockError::LimitExceeded(exceeded)) => {
                let error = match exceeded.limit {
                    WriteTargetLimit::Global => MetadataError::GlobalWriteTargetLimitExceeded(format!(
                        "global limit {} reached before allocating inode_id={inode_id}",
                        exceeded.maximum
                    )),
                    WriteTargetLimit::PerSession => MetadataError::ResourceExhausted(format!(
                        "write session target limit {} reached for inode_id={inode_id}",
                        exceeded.maximum
                    )),
                };
                return Err(self.failure_from_error(ctx, error, group_name));
            }
        };
        let block_size = reservation.block_size();
        let file_offset = reservation.file_offset();
        let open_client_id = reservation.open_client_id();
        let block_id = match self.propose_block_allocation(inode_id, lease_epoch).await {
            Ok(block_id) => block_id,
            Err(error) => return Err(self.failure_from_error(ctx, error, group_name)),
        };

        let worker_manager = &self.worker_manager;
        let placement_group_name = self.require_worker_lookup_group(ctx, group_name.clone(), "AllocateBlock")?;
        let placement_views = worker_manager.collect_worker_placement_views(&placement_group_name);
        let placement_request = PlacementRequest {
            group_name: placement_group_name,
            op: PlacementOp::Write,
            block_id,
            visible_len: 0,
            block_size,
            caller: ctx
                .caller_context
                .as_ref()
                .map(CallerContextFields::from_caller_context),
            existing: &[],
        };
        let placement = plan_placement(&placement_request, &placement_views);
        if placement.status != PlacementStatus::Ok {
            return Err(self.failure_from_error(
                ctx,
                MetadataError::ServiceUnavailable(format!(
                    "Failed to select write placement: {}",
                    placement.failure_message(&placement_request)
                )),
                group_name,
            ));
        }
        let tier = placement.workers[0].tier;
        let workers = placement
            .workers
            .into_iter()
            .map(|worker| WorkerEndpointInfo {
                worker_id: worker.worker_id,
                endpoint: worker.endpoint,
                worker_run_id: worker.worker_run_id,
            })
            .collect();
        let target = LocatedBlock {
            block_id,
            file_offset,
            block_size: u64::from(block_size),
            workers,
            fencing_token: FencingToken {
                owner: open_client_id,
                epoch: lease_epoch,
            },
            write_offset: 0,

            tier,
        };
        let target = match reservation.complete(target) {
            Ok(target) => target,
            Err(CompleteWriteTargetError::NotCurrent) => {
                return Err(self.session_terminal_failure(
                    ctx,
                    ErrorKind::Metadata(MetadataErrorKind::SessionExpired),
                    format!("write session expired before AllocateBlock completed for inode_id={inode_id}"),
                    group_name,
                ))
            }
        };
        self.success(target, group_name)
    }
}

fn open_write_output(session: &WriteSession) -> OpenWriteOutput {
    OpenWriteOutput {
        inode_id: session.inode_id,
        lease_epoch: session.lease_epoch,
        block_size: session.block_size,
        base_len: session.base_len,
        expires_at_ms: session.expires_at_ms,
        generation: session.generation,
        tail_block: session
            .issued_targets
            .first()
            .filter(|target| target.write_offset > 0)
            .cloned(),
    }
}

pub(crate) struct OpenWriteArgs {
    pub(crate) path: String,
    pub(crate) mode: WriteMode,
}

impl MetadataFileSystem {
    /// Open a path for writing under shared namespace-topology admission.
    pub(crate) async fn open_write(&self, ctx: &RequestHeader, args: OpenWriteArgs) -> FsResult<OpenWriteOutput> {
        let path = args.path.clone();
        let result = self.open_write_inner(ctx, args).await;
        match &result {
            Ok(success) => {
                let payload = &success.payload;
                tracing::info!(
                    target: "metadata.state",
                    op = "OpenWrite",
                    result = "opened",
                    error_code = "none",
                    client_id = %ctx.client.client_id,
                    call_id = %ctx.client.call_id,
                    path = %path,
                    inode_id = payload.inode_id.as_raw(),
                    lease_epoch = payload.lease_epoch.as_raw(),
                    "OpenWrite opened"
                );
            }
            Err(failure) => tracing::warn!(
                target: "metadata.state",
                op = "OpenWrite",
                result = "rejected",
                error_code = observe::rpc_error_kind(&failure.error),
                client_id = %ctx.client.client_id,
                call_id = %ctx.client.call_id,
                path = %path,
                "OpenWrite rejected"
            ),
        }
        result
    }

    /// Resolve the path, install an opening, persist fencing, and index its ancestors.
    ///
    /// The shared guard spans resolution, Raft fencing-epoch acquisition, session
    /// creation, and the final topology safety predicate.
    async fn open_write_inner(&self, ctx: &RequestHeader, args: OpenWriteArgs) -> FsResult<OpenWriteOutput> {
        self.check_meta_write(ctx)?;
        let _topology_guard = self.namespace_topology.read().await;
        let open_path = match PathResolver::normalize(&args.path) {
            Ok(path) => path,
            Err(err) => return Err(self.failure_from_path_error(ctx, &args.path, err)),
        };
        let resolved = match self.path_resolver.resolve_normalized_path(&open_path) {
            Ok(resolved) => resolved,
            Err(err) => return Err(self.failure_from_path_error(ctx, &args.path, err)),
        };
        let Some(inode_id) = resolved.inode_id else {
            return Err(self.failure_from_resolved_path_error(
                ctx,
                missing_resolved_target_error(&resolved),
                Some(&resolved.mount_ctx),
            ));
        };
        self.check_data_write(ctx, resolved.mount_ctx.mount_id)?;
        let opened = self
            .open_write_inode(
                ctx,
                open_path.clone(),
                inode_id,
                resolved.ancestor_inode_ids.clone(),
                args.mode,
            )
            .await?;

        self.finish_open_write(ctx, &open_path, &resolved, opened)
    }

    /// Revalidate the complete path identity before exposing a new write session.
    ///
    /// A previously submitted topology mutation may still apply after its RPC
    /// task is canceled and its guard is dropped. Any mismatch removes only
    /// the matching active session epoch and returns `EAGAIN`.
    fn finish_open_write(
        &self,
        ctx: &RequestHeader,
        open_path: &str,
        resolved: &ResolvedPath,
        opened: FsSuccess<OpenWriteOutput>,
    ) -> FsResult<OpenWriteOutput> {
        let inode_id = opened.payload.inode_id;
        let topology_unchanged = self
            .path_resolver
            .resolve_normalized_path(open_path)
            .is_ok_and(|current| {
                current.mount_ctx.mount_id == resolved.mount_ctx.mount_id
                    && current.mount_ctx.namespace_owner_group_name == resolved.mount_ctx.namespace_owner_group_name
                    && current.mount_ctx.root_inode_id == resolved.mount_ctx.root_inode_id
                    && current.inode_id == Some(inode_id)
                    && current.ancestor_inode_ids == resolved.ancestor_inode_ids
            });
        if !topology_unchanged {
            self.session_registry
                .remove_session_if_epoch(opened.payload.inode_id, opened.payload.lease_epoch);
            return Err(self.failure_from_error(
                ctx,
                MetadataError::Again("namespace topology changed during OpenWrite".to_string()),
                opened.group_name,
            ));
        }

        Ok(opened)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inode::InodeKind;
    use crate::raft::Command;
    use crate::service::filesystem::tests::*;
    use crate::session_registry::SessionRegistry;
    use beryl_common::header::RequestHeader;
    use beryl_types::ClientId;

    #[tokio::test]
    async fn session_limit_plus_one_rejects_before_raft_proposal() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(1);
        let first_inode_id = InodeId::new(490);
        let second_inode_id = InodeId::new(491);
        let third_inode_id = InodeId::new(492);
        for inode_id in [first_inode_id, second_inode_id, third_inode_id] {
            storage
                .put_inode(&Inode::new_file(inode_id, InodeAttrs::new(), mount_id, 4096))
                .unwrap();
        }

        let builder = filesystem_builder_with_mount(mount_id, &group_name("g6"));
        let mount_table = builder.mount_table();
        let (raft_node, _state_machine) = single_node_raft(Arc::clone(&storage), mount_table).await;
        let session_registry = Arc::new(SessionRegistry::new(2, 1, 100, 100, 60_000));
        let filesystem = builder
            .with_storage(Arc::clone(&storage))
            .with_raft_node(raft_node)
            .with_session_registry(session_registry)
            .build()
            .await;
        let first_client = ClientId::new(7);
        let other_client = ClientId::new(8);

        filesystem
            .open_write_inode(
                &RequestHeader::new(first_client),
                "/first".to_string(),
                first_inode_id,
                vec![first_inode_id],
                WriteMode::Overwrite,
            )
            .await
            .expect("first client session");

        let log_before_per_client_rejection = storage.get_last_log_index().unwrap();
        let applied_before_per_client_rejection = filesystem.raft_node().get_last_applied_state_id();
        let per_client_rejection = filesystem
            .open_write_inode(
                &RequestHeader::new(first_client),
                "/second".to_string(),
                second_inode_id,
                vec![second_inode_id],
                WriteMode::Overwrite,
            )
            .await
            .expect_err("per-client limit plus one must fail");
        assert_retry(
            &per_client_rejection.error,
            ErrorKind::Metadata(MetadataErrorKind::ResourceExhausted),
        );
        assert!(per_client_rejection.error.message.contains("per_client limit 1"));
        assert_eq!(storage.get_last_log_index().unwrap(), log_before_per_client_rejection);
        assert_eq!(
            filesystem.raft_node().get_last_applied_state_id(),
            applied_before_per_client_rejection
        );
        filesystem
            .open_write_inode(
                &RequestHeader::new(other_client),
                "/second".to_string(),
                second_inode_id,
                vec![second_inode_id],
                WriteMode::Overwrite,
            )
            .await
            .expect("another client uses remaining global capacity");

        let log_before_global_rejection = storage.get_last_log_index().unwrap();
        let applied_before_global_rejection = filesystem.raft_node().get_last_applied_state_id();
        let global_rejection = filesystem
            .open_write_inode(
                &RequestHeader::new(ClientId::new(9)),
                "/third".to_string(),
                third_inode_id,
                vec![third_inode_id],
                WriteMode::Overwrite,
            )
            .await
            .expect_err("global limit plus one must fail");
        assert_retry(
            &global_rejection.error,
            ErrorKind::Metadata(MetadataErrorKind::ResourceExhausted),
        );
        assert!(global_rejection.error.message.contains("global limit 2"));
        assert_eq!(storage.get_last_log_index().unwrap(), log_before_global_rejection);
        assert_eq!(
            filesystem.raft_node().get_last_applied_state_id(),
            applied_before_global_rejection
        );
    }

    #[tokio::test]
    async fn open_write_rejects_a_path_moved_by_an_already_admitted_rename() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(1);
        let old_parent_inode_id = InodeId::new(680);
        let new_parent_inode_id = InodeId::new(681);
        let file_inode_id = InodeId::new(682);
        let builder = filesystem_builder_with_mount(mount_id, &group_name("g20"));
        let mount_table = builder.mount_table();
        let (raft_node, _state_machine) = single_node_raft(Arc::clone(&storage), mount_table).await;
        let filesystem = builder
            .with_storage(Arc::clone(&storage))
            .with_raft_node(raft_node)
            .build()
            .await;

        for inode_id in [ROOT_INODE_ID, old_parent_inode_id, new_parent_inode_id] {
            storage
                .put_inode(&Inode::new_dir(inode_id, InodeAttrs::new(), mount_id))
                .unwrap();
        }
        storage
            .put_inode(&Inode::new_file(file_inode_id, InodeAttrs::new(), mount_id, 64))
            .unwrap();
        storage.put_dentry(ROOT_INODE_ID, "old", old_parent_inode_id).unwrap();
        storage.put_dentry(ROOT_INODE_ID, "new", new_parent_inode_id).unwrap();
        storage.put_dentry(old_parent_inode_id, "file", file_inode_id).unwrap();

        let open_path = "/old/file";
        let resolved = filesystem.path_resolver.resolve_path(open_path).unwrap();
        let opened = filesystem
            .open_write_inode(
                &request_context(),
                open_path.to_string(),
                file_inode_id,
                resolved.ancestor_inode_ids.clone(),
                WriteMode::Overwrite,
            )
            .await
            .expect("AcquireWriteLease");
        let rename_result = filesystem
            .raft_node()
            .propose(Command::Rename {
                proposed_at_ms: crate::raft::proposal_timestamp_ms(),
                src_parent_inode_id: old_parent_inode_id,
                src_name: "file".to_string(),
                expected_src_inode_id: file_inode_id,
                dst_parent_inode_id: new_parent_inode_id,
                dst_name: "file".to_string(),
                expected_dst_inode_id: None,
                expected_dst_lease_epoch: None,
                flags: 0,
            })
            .await
            .expect("already admitted Rename must apply");
        assert!(matches!(rename_result, ApplySuccess::RenameApplied));

        let failure = filesystem
            .finish_open_write(&request_context(), open_path, &resolved, opened)
            .expect_err("OpenWrite must not publish a stale ancestor chain");

        assert_retry(&failure.error, ErrorKind::Metadata(MetadataErrorKind::Conflict));
        assert!(filesystem.write_session_for_inode(file_inode_id).is_none());
        let moved = filesystem.path_resolver.resolve_path("/new/file").unwrap();
        assert_eq!(moved.inode_id, Some(file_inode_id));
        assert_eq!(
            moved.ancestor_inode_ids,
            vec![ROOT_INODE_ID, new_parent_inode_id, file_inode_id]
        );
    }

    #[tokio::test]
    async fn open_write_uses_inode_identity_and_duplicate_fails_without_advancing_epoch() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(1);
        let group_name_value = group_name("g9");
        let inode_id = InodeId::new(510);
        storage
            .put_inode(&Inode::new_file(inode_id, InodeAttrs::new(), mount_id, 4096))
            .unwrap();

        let builder = filesystem_builder_with_mount(mount_id, &group_name_value);
        let mount_table = builder.mount_table();
        let (raft_node, _state_machine) = single_node_raft(Arc::clone(&storage), mount_table).await;
        let filesystem = builder
            .with_storage(Arc::clone(&storage))
            .with_raft_node(raft_node)
            .with_worker_manager(worker_manager_for_write_targets(&group_name_value))
            .build()
            .await;

        let success = filesystem
            .open_write_inode(
                &request_context(),
                "/file".to_string(),
                inode_id,
                vec![inode_id],
                WriteMode::Overwrite,
            )
            .await
            .expect("open_write should succeed");

        let session = filesystem
            .write_session_for_inode(success.payload.inode_id)
            .expect("session should be stored");
        assert!(session.issued_targets.is_empty());
        assert_eq!(success.payload.inode_id, inode_id);
        assert_eq!(session.inode_id, inode_id);
        assert_ne!(filesystem.file_block_size, 4096);
        assert_eq!(success.payload.block_size, 4096);
        assert_eq!(session.block_size, 4096);

        let persisted_epoch = storage
            .get_inode(inode_id)
            .unwrap()
            .and_then(|inode| match inode.kind {
                InodeKind::File(crate::inode::FileData { lease_epoch, .. }) => Some(lease_epoch),
                _ => None,
            })
            .expect("OpenWrite must persist the acquired lease epoch");
        let duplicate = filesystem
            .open_write_inode(
                &request_context(),
                "/file".to_string(),
                inode_id,
                vec![inode_id],
                WriteMode::Overwrite,
            )
            .await
            .expect_err("a duplicate OpenWrite must fail closed while the lease is active");
        assert_fail(&duplicate.error, ErrorKind::Metadata(MetadataErrorKind::Busy));
        let epoch_after_duplicate = storage.get_inode(inode_id).unwrap().and_then(|inode| match inode.kind {
            InodeKind::File(crate::inode::FileData { lease_epoch, .. }) => Some(lease_epoch),
            _ => None,
        });
        assert_eq!(epoch_after_duplicate, Some(persisted_epoch));
    }

    #[tokio::test]
    async fn allocation_failures_release_capacity_without_reusing_durable_indices() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(1);
        let group_name_value = group_name("g10");
        let inode_id = InodeId::new(550);
        storage
            .put_inode(&Inode::new_file(inode_id, InodeAttrs::new(), mount_id, 4096))
            .unwrap();

        let worker_manager = Arc::new(WorkerManager::new(60_000));
        let builder = filesystem_builder_with_mount(mount_id, &group_name_value);
        let mount_table = builder.mount_table();
        let (raft_node, _state_machine) = single_node_raft(Arc::clone(&storage), mount_table).await;
        let filesystem = builder
            .with_storage(Arc::clone(&storage))
            .with_raft_node(raft_node)
            .with_worker_manager(Arc::clone(&worker_manager))
            .build()
            .await;
        let opened = filesystem
            .open_write_inode(
                &request_context(),
                "/file".to_string(),
                inode_id,
                vec![inode_id],
                WriteMode::Overwrite,
            )
            .await
            .expect("OpenWrite");

        let lease_epoch = opened.payload.lease_epoch;
        let next_index = || match storage.get_inode(inode_id).unwrap().unwrap().kind {
            InodeKind::File(crate::inode::FileData { next_index, .. }) => next_index,
            _ => panic!("expected file"),
        };
        let registry = filesystem.session_registry();
        let reservation = match registry.begin_allocate_block(inode_id, lease_epoch, None).unwrap() {
            BeginAllocateBlock::Reserved(reservation) => reservation,
            BeginAllocateBlock::Replay(_) => panic!("first allocation must reserve"),
        };
        let duplicate = filesystem
            .allocate_block_session(&request_context(), inode_id, lease_epoch, None)
            .await
            .expect_err("pending duplicate must fail before Raft");
        assert_retry(&duplicate.error, ErrorKind::Metadata(MetadataErrorKind::Conflict));
        assert_eq!(next_index(), 0);
        drop(reservation);

        filesystem
            .allocate_block_session(&request_context(), inode_id, lease_epoch, None)
            .await
            .expect_err("placement without a live worker fails after durable allocation");
        assert_eq!(next_index(), 1);

        let worker_id = WorkerId::new(1);
        register_worker_descriptor(
            &worker_manager,
            &group_name_value,
            worker_id,
            "127.0.0.1:9001".to_string(),
        );
        record_worker_heartbeat(&worker_manager, &group_name_value, worker_id, 1024 * 1024);
        let block = filesystem
            .allocate_block_session(&request_context(), inode_id, lease_epoch, None)
            .await
            .expect("placement recovery releases the failed reservation")
            .payload;
        assert_eq!(block.block_id.index, BlockIndex::new(1));
        let replay = filesystem
            .allocate_block_session(&request_context(), inode_id, lease_epoch, None)
            .await
            .unwrap()
            .payload;
        assert_eq!(replay, block);
        assert_eq!(next_index(), 2);
        assert_eq!(
            filesystem.write_session_for_inode(inode_id).unwrap().issued_targets,
            vec![block]
        );
    }
}
