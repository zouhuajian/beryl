// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use super::{CoreWriteOp, FsCore};
use crate::error::{MetadataError, MetadataResult};
use crate::raft::{
    AppDataResponse, Command, CommandFingerprint, DedupKey, FileCommitMode, FsCommandResult, RocksDBStorage,
};
use crate::service::domain::{
    AbortWriteInput, AbortWriteOutput, AddBlockInput, AddBlockOutput, CloseWriteInput, CloseWriteIntent,
    CloseWriteOutput, CommittedBlock, CoreResult, OpenWriteInput, OpenWriteOutput, RenewLeaseInput, RenewLeaseOutput,
    RequestContext, SessionKey, WorkerHint, WriteTarget,
};
use common::error::canonical::{RefreshHint, RefreshReason, WorkerEndpointHint};
use common::header::RpcErrorCode;
use proto::metadata::WriteTargetProto;
use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};
use types::fs::{Extent, FsErrorCode};
use types::ids::{BlockId, BlockIndex, DataHandleId, WorkerId};
use types::layout::FileLayout;
use types::lease::FencingToken;

impl FsCore {
    pub(crate) fn write_session_for_handle(&self, file_handle: u64) -> Option<crate::write_session::WriteSession> {
        self.write_session_manager.get_session(file_handle)
    }

    pub(crate) async fn execute_abort_write(&self, req: AbortWriteInput) -> CoreResult<AbortWriteOutput> {
        WriteSessionCoordinator::new(self).execute_abort_write(req).await
    }

    pub(crate) async fn execute_renew_inode_lease(&self, req: RenewLeaseInput) -> CoreResult<RenewLeaseOutput> {
        WriteSessionCoordinator::new(self).execute_renew_inode_lease(req).await
    }

    pub(crate) async fn execute_open_write(&self, req: OpenWriteInput) -> CoreResult<OpenWriteOutput> {
        WriteSessionCoordinator::new(self).execute_open_write(req).await
    }

    pub(crate) fn preflight_open_write_runtime(
        &self,
        ctx: &RequestContext,
        desired_len: Option<u64>,
        layout: FileLayout,
        group_id: Option<u64>,
        mount_epoch: Option<u64>,
    ) -> Option<crate::service::domain::CoreFailure> {
        WriteSessionCoordinator::new(self).preflight_open_write_runtime(ctx, desired_len, layout, group_id, mount_epoch)
    }

    pub(crate) async fn execute_add_block(&self, req: AddBlockInput) -> CoreResult<AddBlockOutput> {
        WriteSessionCoordinator::new(self).execute_add_block(req).await
    }

    pub(crate) async fn execute_close_write(&self, req: CloseWriteInput) -> CoreResult<CloseWriteOutput> {
        WriteSessionCoordinator::new(self).execute_close_write(req).await
    }
}

fn worker_refresh_hint_from_session(
    session: &crate::write_session::WriteSession,
    worker_epoch: Option<u64>,
    resolve_required: bool,
) -> RefreshHint {
    let capacity = session
        .write_targets
        .iter()
        .map(|target| target.worker_endpoints.len())
        .sum();
    let mut worker_endpoints = Vec::with_capacity(capacity);
    for target in &session.write_targets {
        for endpoint in &target.worker_endpoints {
            worker_endpoints.push(WorkerEndpointHint {
                worker_id: endpoint.worker_id,
                endpoint: endpoint.endpoint.clone(),
                net_transport_kind: endpoint.net_transport_kind,
                worker_epoch: endpoint.worker_epoch,
            });
        }
    }

    RefreshHint {
        worker_epoch,
        worker_endpoints,
        worker_resolve_required: resolve_required,
        ..Default::default()
    }
}

struct PlannedWriteTarget {
    block_id: BlockId,
    file_offset: u64,
    len: u64,
    worker_endpoints: Vec<WorkerHint>,
    worker_endpoints_proto: Vec<proto::common::WorkerEndpointInfoProto>,
}

struct WriteSessionCoordinator<'a> {
    core: &'a FsCore,
}

impl<'a> WriteSessionCoordinator<'a> {
    fn new(core: &'a FsCore) -> Self {
        Self { core }
    }

    fn commit_mode_for_session(session: &crate::write_session::WriteSession) -> FileCommitMode {
        match session.mode {
            crate::inode_lease::WriteMode::Write => FileCommitMode::Replace,
            crate::inode_lease::WriteMode::Append => FileCommitMode::Append,
        }
    }

    async fn execute_abort_write(&self, req: AbortWriteInput) -> CoreResult<AbortWriteOutput> {
        let file_handle = req.file_handle;
        let session = match self.core.write_session_manager.get_session(file_handle) {
            Some(session) => session,
            None => {
                return self.core.session_terminal_failure(
                    &req.ctx,
                    RefreshReason::SessionInvalid,
                    RpcErrorCode::Fencing,
                    format!(
                        "write handle not found for handle={}; AbortFileWrite cannot be replayed automatically",
                        file_handle,
                    ),
                    None,
                    None,
                );
            }
        };

        let (group_id, mount_epoch) =
            match self
                .core
                .validate_mount_epoch_for_mount(&req.ctx, req.freshness, session.mount_id)
            {
                Ok(hints) => hints,
                Err(err) => return Err(err),
            };
        let route_epoch = match self
            .core
            .validate_route_epoch(&req.ctx, req.freshness, group_id, mount_epoch, "AbortFileWrite")
            .await
        {
            Ok(route_epoch) => route_epoch,
            Err(err) => return Err(err),
        };

        let lease_id = match req.lease_id {
            Some(lease_id) => lease_id,
            None => {
                return self.core.failure_from_error(
                    &req.ctx,
                    MetadataError::InvalidArgument("Missing lease_id".to_string()),
                    group_id,
                    mount_epoch,
                );
            }
        };
        if lease_id != session.lease_id || req.lease_epoch != session.lease_epoch {
            return self.core.session_terminal_failure(
                &req.ctx,
                RefreshReason::SessionInvalid,
                RpcErrorCode::Fencing,
                format!(
                    "lease/write handle mismatch for handle={}; AbortFileWrite cannot be replayed automatically",
                    file_handle
                ),
                group_id,
                mount_epoch,
            );
        }
        if req.open_epoch != session.open_epoch {
            return self.core.session_terminal_failure(
                &req.ctx,
                RefreshReason::SessionInvalid,
                RpcErrorCode::EpochMismatch,
                format!(
                    "open_epoch mismatch: expected {}, got {}; AbortFileWrite cannot be replayed automatically",
                    session.open_epoch, req.open_epoch
                ),
                group_id,
                mount_epoch,
            );
        }
        let token = match req.fencing_token.as_ref() {
            Some(token) => token,
            None => {
                return self.core.session_terminal_failure(
                    &req.ctx,
                    RefreshReason::SessionInvalid,
                    RpcErrorCode::Fencing,
                    format!(
                        "missing fencing_token for handle={}; AbortFileWrite cannot be replayed automatically",
                        file_handle
                    ),
                    group_id,
                    mount_epoch,
                );
            }
        };
        if !FsCore::fencing_token_matches_session(&session, token) {
            return self.core.session_terminal_failure(
                &req.ctx,
                RefreshReason::SessionInvalid,
                RpcErrorCode::Fencing,
                format!(
                    "fencing_token mismatch for handle={}; AbortFileWrite cannot be replayed automatically",
                    file_handle
                ),
                group_id,
                mount_epoch,
            );
        }
        if self
            .core
            .inode_lease_manager
            .validate_lease(session.inode_id, lease_id, req.lease_epoch)
            .is_err()
        {
            return self.core.session_terminal_failure(
                &req.ctx,
                RefreshReason::SessionExpired,
                RpcErrorCode::Fencing,
                format!(
                    "lease validation rejected for handle={}; write lease expired and AbortFileWrite cannot be replayed automatically",
                    file_handle,
                ),
                group_id,
                mount_epoch,
            );
        }

        self.core
            .inode_lease_manager
            .release(session.inode_id, lease_id, session.lease_epoch);
        self.core.write_session_manager.remove_session(file_handle);

        self.core
            .success_with_route_epoch(&req.ctx, AbortWriteOutput, group_id, mount_epoch, route_epoch)
    }

    async fn execute_renew_inode_lease(&self, req: RenewLeaseInput) -> CoreResult<RenewLeaseOutput> {
        let file_handle = req.file_handle;

        let session = match self.core.write_session_manager.get_session(file_handle) {
            Some(session) => session,
            None => {
                return self.core.session_terminal_failure(
                    &req.ctx,
                    RefreshReason::SessionInvalid,
                    RpcErrorCode::Fencing,
                    format!(
                        "write handle not found for handle={}; RenewLease cannot be replayed automatically",
                        file_handle,
                    ),
                    None,
                    None,
                );
            }
        };

        let (group_id, mount_epoch) =
            match self
                .core
                .validate_mount_epoch_for_mount(&req.ctx, req.freshness, session.mount_id)
            {
                Ok(hints) => hints,
                Err(err) => return Err(err),
            };

        let lease_id_typed = match req.lease_id {
            Some(lease_id) => lease_id,
            None => {
                return self.core.failure_from_error(
                    &req.ctx,
                    MetadataError::InvalidArgument("Missing lease_id".to_string()),
                    group_id,
                    mount_epoch,
                );
            }
        };

        if lease_id_typed != session.lease_id || req.lease_epoch != session.lease_epoch {
            return self.core.session_terminal_failure(
                &req.ctx,
                RefreshReason::SessionInvalid,
                RpcErrorCode::Fencing,
                format!(
                    "lease/write handle mismatch: expected lease_id={:?} lease_epoch={}, got lease_id={:?} lease_epoch={}; RenewLease cannot be replayed automatically",
                    session.lease_id,
                    session.lease_epoch,
                    lease_id_typed,
                    req.lease_epoch,
                ),
                group_id,
                mount_epoch,
            );
        }

        if req.open_epoch != session.open_epoch {
            return self.core.session_terminal_failure(
                &req.ctx,
                RefreshReason::SessionInvalid,
                RpcErrorCode::EpochMismatch,
                format!(
                    "open_epoch mismatch: expected {}, got {}; RenewLease cannot be replayed automatically",
                    session.open_epoch, req.open_epoch,
                ),
                group_id,
                mount_epoch,
            );
        }

        let req_token = match req.fencing_token.as_ref() {
            Some(token) => token,
            None => {
                return self.core.session_terminal_failure(
                    &req.ctx,
                    RefreshReason::SessionInvalid,
                    RpcErrorCode::Fencing,
                    format!(
                        "missing fencing_token for handle={}; RenewLease cannot be replayed automatically",
                        file_handle,
                    ),
                    group_id,
                    mount_epoch,
                );
            }
        };
        if !FsCore::fencing_token_matches_session(&session, req_token) {
            return self.core.session_terminal_failure(
                &req.ctx,
                RefreshReason::SessionInvalid,
                RpcErrorCode::Fencing,
                format!(
                    "fencing_token mismatch for handle={}; RenewLease cannot be replayed automatically",
                    file_handle,
                ),
                group_id,
                mount_epoch,
            );
        }

        let expires_at_ms = match self
            .core
            .inode_lease_manager
            .renew(session.inode_id, lease_id_typed, req.lease_epoch)
        {
            Ok(expires) => expires,
            Err(_) => {
                return self.core.session_terminal_failure(
                    &req.ctx,
                    RefreshReason::SessionExpired,
                    RpcErrorCode::Fencing,
                    format!(
                        "lease renewal rejected for handle={}; write lease expired and RenewLease cannot be replayed automatically",
                        file_handle,
                    ),
                    group_id,
                    mount_epoch,
                );
            }
        };

        let route_epoch = self.core.authoritative_route_epoch().await;
        self.core.success_with_route_epoch(
            &req.ctx,
            RenewLeaseOutput { expires_at_ms },
            group_id,
            mount_epoch,
            route_epoch,
        )
    }

    async fn execute_open_write(&self, req: OpenWriteInput) -> CoreResult<OpenWriteOutput> {
        let caller_ctx = &req.ctx.caller;
        let inode_id = req.inode_id;

        let storage = match self.core.storage.as_ref() {
            Some(storage) => storage,
            None => {
                return self.core.failure_from_error(
                    &req.ctx,
                    MetadataError::Internal("Storage not available".to_string()),
                    None,
                    None,
                );
            }
        };

        let inode = match storage.get_inode(inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                return self.core.failure_from_error(
                    &req.ctx,
                    MetadataError::NotFound(format!("Inode not found: {}", inode_id)),
                    None,
                    None,
                );
            }
            Err(err) => {
                return self.core.failure_from_error(&req.ctx, err, None, None);
            }
        };

        if !inode.kind.is_file() {
            return self.core.failure_from_error(
                &req.ctx,
                MetadataError::IsDir(format!("Inode is not a file: {}", inode_id)),
                None,
                None,
            );
        }

        let data_handle_id = inode.current_data_handle_id;
        if data_handle_id.as_raw() == 0 {
            return self.core.failure_from_error(
                &req.ctx,
                MetadataError::Internal(format!("File inode {} is missing current_data_handle_id", inode_id)),
                None,
                None,
            );
        }
        if let Err(err) = storage.validate_data_handle_owner(data_handle_id, Some(inode_id)) {
            return self.core.failure_from_error(&req.ctx, err, None, None);
        }

        let (group_id, mount_epoch) =
            match self
                .core
                .validate_mount_epoch_for_mount(&req.ctx, req.freshness, inode.mount_id)
            {
                Ok(hints) => hints,
                Err(err) => return Err(err),
            };

        let route_epoch = match self
            .core
            .validate_route_epoch(&req.ctx, req.freshness, group_id, mount_epoch, "OpenWrite")
            .await
        {
            Ok(route_epoch) => route_epoch,
            Err(err) => return Err(err),
        };

        let mode = req.mode;
        let base_size = match mode {
            crate::inode_lease::WriteMode::Append => inode.attrs.size,
            crate::inode_lease::WriteMode::Write => 0,
        };

        let desired_len = req.desired_len.unwrap_or(4 * 1024 * 1024);
        let block_size = storage
            .get_layout(inode_id)
            .map(|layout| layout.block_size as u64)
            .unwrap_or(4 * 1024 * 1024)
            .max(1);
        let num_blocks = desired_len.div_ceil(block_size).clamp(1, 10);
        let start_index = match &inode.data {
            types::fs::InodeData::File { extents, .. } => extents
                .iter()
                .map(|extent| extent.block_id.index.as_raw())
                .max()
                .map(|index| index + 1)
                .unwrap_or(0),
            _ => 0,
        };

        let worker_manager = match self.core.worker_manager.as_ref() {
            Some(worker_manager) => worker_manager,
            None => {
                return self.core.failure_from_error(
                    &req.ctx,
                    MetadataError::ServiceUnavailable("Worker manager not available".to_string()),
                    group_id,
                    mount_epoch,
                );
            }
        };

        let mut planned_targets = Vec::with_capacity(num_blocks as usize);
        for i in 0..num_blocks {
            let block_index = BlockIndex::new(start_index + i as u32);
            let block_id = BlockId::new(data_handle_id, block_index);
            let file_offset = base_size + i * block_size;
            let len = desired_len.saturating_sub(i * block_size).min(block_size).max(1);
            let placement = match worker_manager.select_workers_for_placement(3, None) {
                Ok(placement) => placement,
                Err(e) => {
                    return self.core.failure_from_error(
                        &req.ctx,
                        MetadataError::Internal(format!("Failed to select workers: {}", e)),
                        group_id,
                        mount_epoch,
                    );
                }
            };

            let mut worker_endpoints = Vec::with_capacity(3);
            let mut worker_endpoints_proto = Vec::with_capacity(3);
            for worker_id in placement.all_workers() {
                if let Some(worker_info) = worker_manager.get_worker(worker_id) {
                    let endpoint = worker_info.address.clone();
                    worker_endpoints.push(WorkerHint {
                        worker_id,
                        endpoint: endpoint.clone(),
                        net_transport_kind: worker_info.net_transport_kind,
                        worker_epoch: worker_info.worker_epoch,
                    });
                    worker_endpoints_proto.push(proto::common::WorkerEndpointInfoProto {
                        worker_id: worker_id.as_raw(),
                        endpoint,
                        net_transport_kind: worker_info.net_transport_kind,
                        worker_epoch: worker_info.worker_epoch,
                    });
                }
            }

            if worker_endpoints.is_empty() {
                return self.core.failure_from_error(
                    &req.ctx,
                    MetadataError::ServiceUnavailable("selected placement has no live worker endpoints".to_string()),
                    group_id,
                    mount_epoch,
                );
            }

            planned_targets.push(PlannedWriteTarget {
                block_id,
                file_offset,
                len,
                worker_endpoints,
                worker_endpoints_proto,
            });
        }

        let current_lease_epoch = match &inode.data {
            types::fs::InodeData::File { lease_epoch, .. } => *lease_epoch,
            _ => None,
        };

        let (lease_id, lease_epoch, expires_at_ms) = match self.core.inode_lease_manager.try_acquire(
            inode_id,
            caller_ctx.client.client_id,
            Some(caller_ctx.client.call_id),
            mode,
            current_lease_epoch,
        ) {
            Ok(result) => result,
            Err(FsErrorCode::EBusy) => {
                return self.core.fatal_fs_failure(
                    &req.ctx,
                    FsErrorCode::EBusy,
                    format!("File already has an active write lease: {}", inode_id),
                    group_id,
                    mount_epoch,
                );
            }
            Err(e) => {
                return self.core.fatal_fs_failure(
                    &req.ctx,
                    e,
                    format!("Failed to acquire lease: {}", inode_id),
                    group_id,
                    mount_epoch,
                );
            }
        };

        let open_epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let mut write_targets = Vec::with_capacity(planned_targets.len());
        let mut write_targets_proto = Vec::with_capacity(planned_targets.len());
        for planned in planned_targets {
            let block_id = planned.block_id;
            let target_token = FencingToken {
                block_id,
                owner: caller_ctx.client.client_id,
                epoch: lease_epoch,
            };
            write_targets.push(WriteTarget {
                block_id,
                file_offset: planned.file_offset,
                len: planned.len,
                worker_endpoints: planned.worker_endpoints,
                fencing_token: target_token,
            });
            write_targets_proto.push(proto::metadata::WriteTargetProto {
                block_id: Some(proto::common::BlockIdProto {
                    data_handle_id: block_id.data_handle_id.as_raw(),
                    block_index: block_id.index.as_raw(),
                }),
                file_offset: planned.file_offset,
                len: planned.len,
                worker_endpoints: planned.worker_endpoints_proto,
                fencing_token: Some(proto::common::FencingTokenProto {
                    block_id: Some(proto::common::BlockIdProto {
                        data_handle_id: block_id.data_handle_id.as_raw(),
                        block_index: block_id.index.as_raw(),
                    }),
                    owner: caller_ctx.client.client_id.as_raw(),
                    epoch: lease_epoch,
                }),
            });
        }

        let session_token = FencingToken {
            block_id: BlockId::new(data_handle_id, BlockIndex::new(0)),
            owner: caller_ctx.client.client_id,
            epoch: lease_epoch,
        };
        let file_handle = self
            .core
            .write_session_manager
            .create_session(crate::write_session::CreateSessionInput {
                inode_id,
                mount_id: inode.mount_id,
                data_handle_id,
                lease_id,
                lease_epoch,
                fencing_token: session_token,
                open_epoch,
                base_size,
                mode,
                write_targets: write_targets_proto,
                writer_identity: crate::write_session::WriterIdentity {
                    client_id: caller_ctx.client.client_id,
                    call_id: caller_ctx.client.call_id,
                },
            });

        self.core.success_with_route_epoch(
            &req.ctx,
            OpenWriteOutput {
                inode_id,
                data_handle_id,
                session_key: SessionKey {
                    file_handle,
                    lease_id,
                    lease_epoch,
                    open_epoch,
                    fencing_token: session_token,
                },
                write_targets,
                base_size,
                expires_at_ms,
            },
            group_id,
            mount_epoch,
            route_epoch,
        )
    }

    async fn execute_add_block(&self, req: AddBlockInput) -> CoreResult<AddBlockOutput> {
        let file_handle = req.file_handle;
        let session = match self.core.write_session_manager.get_session(file_handle) {
            Some(session) => session,
            None => {
                return self.core.session_terminal_failure(
                    &req.ctx,
                    RefreshReason::SessionInvalid,
                    RpcErrorCode::Fencing,
                    format!(
                        "write handle not found for handle={}; reopen before AddBlock",
                        file_handle
                    ),
                    None,
                    None,
                );
            }
        };

        let (group_id, mount_epoch) =
            match self
                .core
                .validate_mount_epoch_for_mount(&req.ctx, req.freshness, session.mount_id)
            {
                Ok(hints) => hints,
                Err(err) => return Err(err),
            };
        let route_epoch = match self
            .core
            .validate_route_epoch(&req.ctx, req.freshness, group_id, mount_epoch, "AddBlock")
            .await
        {
            Ok(route_epoch) => route_epoch,
            Err(err) => return Err(err),
        };

        let lease_id = match req.lease_id {
            Some(lease_id) => lease_id,
            None => {
                return self.core.failure_from_error(
                    &req.ctx,
                    MetadataError::InvalidArgument("Missing lease_id".to_string()),
                    group_id,
                    mount_epoch,
                );
            }
        };
        if lease_id != session.lease_id || req.lease_epoch != session.lease_epoch {
            return self.core.session_terminal_failure(
                &req.ctx,
                RefreshReason::SessionInvalid,
                RpcErrorCode::Fencing,
                format!("lease mismatch for handle={}; reopen before AddBlock", file_handle),
                group_id,
                mount_epoch,
            );
        }
        if req.open_epoch != session.open_epoch {
            return self.core.session_terminal_failure(
                &req.ctx,
                RefreshReason::EpochMismatch,
                RpcErrorCode::EpochMismatch,
                format!(
                    "open_epoch mismatch: expected {}, got {}; reopen before AddBlock",
                    session.open_epoch, req.open_epoch
                ),
                group_id,
                mount_epoch,
            );
        }
        let token = match req.fencing_token.as_ref() {
            Some(token) => token,
            None => {
                return self.core.session_terminal_failure(
                    &req.ctx,
                    RefreshReason::SessionInvalid,
                    RpcErrorCode::Fencing,
                    format!(
                        "missing fencing_token for handle={}; reopen before AddBlock",
                        file_handle
                    ),
                    group_id,
                    mount_epoch,
                );
            }
        };
        if !FsCore::fencing_token_matches_session(&session, token) {
            return self.core.session_terminal_failure(
                &req.ctx,
                RefreshReason::SessionInvalid,
                RpcErrorCode::Fencing,
                format!(
                    "fencing_token mismatch for handle={}; reopen before AddBlock",
                    file_handle
                ),
                group_id,
                mount_epoch,
            );
        }
        if self
            .core
            .inode_lease_manager
            .validate_lease(session.inode_id, lease_id, req.lease_epoch)
            .is_err()
        {
            return self.core.session_terminal_failure(
                &req.ctx,
                RefreshReason::SessionExpired,
                RpcErrorCode::Fencing,
                format!(
                    "lease validation rejected for handle={}; reopen before AddBlock",
                    file_handle
                ),
                group_id,
                mount_epoch,
            );
        }

        let target = match self
            .core
            .write_session_manager
            .allocate_target(file_handle, req.desired_len)
        {
            Some(target) => target,
            None => {
                return self.core.fatal_fs_failure(
                    &req.ctx,
                    FsErrorCode::EAgain,
                    "no preallocated write target available; reopen with a larger desired_len",
                    group_id,
                    mount_epoch,
                );
            }
        };
        let block_id = match target.block_id {
            Some(block_id) => BlockId::new(
                types::ids::DataHandleId::new(block_id.data_handle_id),
                BlockIndex::new(block_id.block_index),
            ),
            None => {
                return self.core.failure_from_error(
                    &req.ctx,
                    MetadataError::Internal("preallocated write target missing block_id".to_string()),
                    group_id,
                    mount_epoch,
                );
            }
        };
        let fencing_token = target
            .fencing_token
            .map(|token| FencingToken {
                block_id: token
                    .block_id
                    .map(|block| {
                        BlockId::new(
                            types::ids::DataHandleId::new(block.data_handle_id),
                            BlockIndex::new(block.block_index),
                        )
                    })
                    .unwrap_or(block_id),
                owner: types::ids::ClientId::new(token.owner),
                epoch: token.epoch,
            })
            .unwrap_or(session.fencing_token);
        let worker_endpoints = target
            .worker_endpoints
            .into_iter()
            .map(|endpoint| WorkerHint {
                worker_id: WorkerId::new(endpoint.worker_id),
                endpoint: endpoint.endpoint,
                net_transport_kind: endpoint.net_transport_kind,
                worker_epoch: endpoint.worker_epoch,
            })
            .collect();

        self.core.success_with_route_epoch(
            &req.ctx,
            AddBlockOutput {
                target: WriteTarget {
                    block_id,
                    file_offset: target.file_offset,
                    len: target.len,
                    worker_endpoints,
                    fencing_token,
                },
            },
            group_id,
            mount_epoch,
            route_epoch,
        )
    }

    fn invalid_commit_failure(
        &self,
        req: &CloseWriteInput,
        message: impl Into<String>,
        group_id: Option<u64>,
        mount_epoch: Option<u64>,
    ) -> crate::service::domain::CoreFailure {
        match self
            .core
            .fatal_fs_failure::<()>(&req.ctx, FsErrorCode::EInval, message, group_id, mount_epoch)
        {
            Err(failure) => failure,
            Ok(_) => unreachable!("fatal_fs_failure always returns Err"),
        }
    }

    fn target_block_id(target: &WriteTargetProto) -> MetadataResult<BlockId> {
        let block_id = target
            .block_id
            .as_ref()
            .ok_or_else(|| MetadataError::InvalidArgument("issued write target missing block_id".to_string()))?;
        Ok(BlockId::new(
            DataHandleId::new(block_id.data_handle_id),
            BlockIndex::new(block_id.block_index),
        ))
    }

    fn block_end(block: &CommittedBlock) -> Option<u64> {
        block.file_offset.checked_add(block.len)
    }

    fn validate_committed_blocks(
        intent: &CloseWriteIntent,
        session: &crate::write_session::WriteSession,
    ) -> MetadataResult<Vec<Extent>> {
        let mut issued = HashMap::with_capacity(session.issued_targets.len());
        for target in &session.issued_targets {
            let block_id = Self::target_block_id(target)?;
            issued.insert(block_id, (target.file_offset, target.len));
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

    async fn execute_close_write(&self, req: CloseWriteInput) -> CoreResult<CloseWriteOutput> {
        let caller_ctx = &req.ctx.caller;
        let file_handle = req.file_handle;
        let dedup = match self.core.dedup_key(caller_ctx) {
            Ok(k) => k,
            Err(err) => return self.core.failure_from_error(&req.ctx, err, None, None),
        };

        if let Some(replay) = self.replay_close_write_if_applied(&req, &dedup).await {
            return replay;
        }

        let session = match self.core.write_session_manager.get_session(file_handle) {
            Some(session) => session,
            None => {
                return self.core.session_terminal_failure(
                    &req.ctx,
                    RefreshReason::SessionInvalid,
                    RpcErrorCode::Fencing,
                    format!(
                        "write handle not found for handle={}; CommitFile cannot be replayed automatically",
                        file_handle,
                    ),
                    None,
                    None,
                );
            }
        };

        let (group_id, mount_epoch) =
            match self
                .core
                .validate_mount_epoch_for_mount(&req.ctx, req.freshness, session.mount_id)
            {
                Ok(hints) => hints,
                Err(err) => return Err(err),
            };

        let route_epoch = match self
            .core
            .validate_route_epoch(&req.ctx, req.freshness, group_id, mount_epoch, "CommitFile")
            .await
        {
            Ok(route_epoch) => route_epoch,
            Err(err) => return Err(err),
        };

        if let Some(worker_manager) = self.core.worker_manager.as_ref() {
            for target in &session.write_targets {
                for endpoint in &target.worker_endpoints {
                    let worker_id = WorkerId::new(endpoint.worker_id);
                    let current_epoch = worker_manager.get_descriptor(worker_id).map(|d| d.worker_epoch);
                    if current_epoch != Some(endpoint.worker_epoch) {
                        let hint = worker_refresh_hint_from_session(&session, current_epoch, true);
                        return self.core.need_refresh_failure_with_hint(
                            &req.ctx,
                            RpcErrorCode::WorkerEpochMismatch,
                            RefreshReason::WorkerEpochMismatch,
                            format!(
                                "worker_epoch mismatch for worker_id={}: client/session={}, server={:?}; {}",
                                endpoint.worker_id,
                                endpoint.worker_epoch,
                                current_epoch,
                                FsCore::replay_hint("CommitFile")
                            ),
                            group_id,
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
                return self.core.failure_from_error(
                    &req.ctx,
                    MetadataError::InvalidArgument("Missing lease_id".to_string()),
                    group_id,
                    mount_epoch,
                );
            }
        };
        let request_lease_epoch = req.lease_epoch;

        if lease_id_typed != session.lease_id || request_lease_epoch != session.lease_epoch {
            return self.core.session_terminal_failure(
                &req.ctx,
                RefreshReason::SessionInvalid,
                RpcErrorCode::Fencing,
                format!(
                    "lease/write handle mismatch: expected lease_id={:?} lease_epoch={}, got lease_id={:?} lease_epoch={}; CommitFile cannot be replayed automatically",
                    session.lease_id,
                    session.lease_epoch,
                    lease_id_typed,
                    request_lease_epoch,
                ),
                group_id,
                mount_epoch,
            );
        }

        let req_token = match req.fencing_token.as_ref() {
            Some(token) => token,
            None => {
                return self.core.session_terminal_failure(
                    &req.ctx,
                    RefreshReason::SessionInvalid,
                    RpcErrorCode::Fencing,
                    format!(
                        "missing fencing_token for handle={}; CommitFile cannot be replayed automatically",
                        file_handle,
                    ),
                    group_id,
                    mount_epoch,
                );
            }
        };
        if !FsCore::fencing_token_matches_session(&session, req_token) {
            return self.core.session_terminal_failure(
                &req.ctx,
                RefreshReason::SessionInvalid,
                RpcErrorCode::Fencing,
                format!(
                    "fencing_token mismatch for handle={}; CommitFile cannot be replayed automatically",
                    file_handle,
                ),
                group_id,
                mount_epoch,
            );
        }

        if req.open_epoch != session.open_epoch {
            return self.core.session_terminal_failure(
                &req.ctx,
                RefreshReason::SessionInvalid,
                RpcErrorCode::EpochMismatch,
                format!(
                    "open_epoch mismatch: expected {}, got {}; CommitFile cannot be replayed automatically",
                    session.open_epoch, req.open_epoch,
                ),
                group_id,
                mount_epoch,
            );
        }

        if self
            .core
            .inode_lease_manager
            .validate_lease(session.inode_id, lease_id_typed, request_lease_epoch)
            .is_err()
        {
            return self.core.session_terminal_failure(
                &req.ctx,
                RefreshReason::SessionExpired,
                RpcErrorCode::Fencing,
                format!(
                    "lease validation rejected for handle={}; write lease expired and CommitFile cannot be replayed automatically",
                    file_handle,
                ),
                group_id,
                mount_epoch,
            );
        }

        let extents = match Self::validate_committed_blocks(&req.intent, &session) {
            Ok(extents) => extents,
            Err(err) => return Err(self.invalid_commit_failure(&req, err.to_string(), group_id, mount_epoch)),
        };

        let ctx = match self.core.route_ctx_for_write_with_error_hints(
            &req.ctx,
            CoreWriteOp::SetAttr,
            &[session.inode_id],
            req.freshness,
            group_id,
            mount_epoch,
        ) {
            Ok(ctx) => ctx,
            Err(failure) => return Err(failure),
        };

        let command = Command::CloseWrite {
            dedup,
            inode_id: session.inode_id,
            extents,
            final_size: req.intent.final_size,
            lease_id: session.lease_id,
            open_epoch: session.open_epoch,
            lease_epoch: request_lease_epoch,
            commit_mode: Self::commit_mode_for_session(&session),
        };
        let file_version = match self.core.propose_fs_write_command(CoreWriteOp::SetAttr, command).await {
            Ok(FsCommandResult::Ok(ok)) => ok.file_version,
            Ok(FsCommandResult::Err(err)) => {
                return self.core.fatal_fs_failure(
                    &req.ctx,
                    err.errno,
                    err.message,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
            }
            Err(err) => {
                return self.core.failure_from_error(
                    &req.ctx,
                    err,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
            }
        };

        self.core
            .inode_lease_manager
            .release(session.inode_id, lease_id_typed, session.lease_epoch);
        self.core.write_session_manager.remove_session(file_handle);

        self.core.success_with_route_epoch(
            &req.ctx,
            CloseWriteOutput {
                committed_size: req.intent.final_size,
                file_version,
            },
            Some(ctx.namespace_owner_group_id.as_raw()),
            Some(ctx.mount_epoch),
            route_epoch,
        )
    }

    async fn replay_close_write_if_applied(
        &self,
        req: &CloseWriteInput,
        dedup: &DedupKey,
    ) -> Option<CoreResult<CloseWriteOutput>> {
        let storage = self.core.storage.as_ref()?;
        let applied = match storage.get_applied_result(dedup) {
            Ok(Some(applied)) => applied,
            Ok(None) => return None,
            Err(err) => return Some(self.core.failure_from_error(&req.ctx, err, None, None)),
        };

        let (command, group_id, mount_epoch) =
            match self.close_write_replay_command(req, dedup, storage, applied.fingerprint) {
                Ok(replay) => replay,
                Err(err) => return Some(self.core.failure_from_error(&req.ctx, err, None, None)),
            };
        let fingerprint = command.fingerprint();
        if applied.fingerprint != fingerprint {
            return Some(self.core.failure_from_error(
                &req.ctx,
                MetadataError::InvalidArgument(format!(
                    "call_id {} reused with different command payload",
                    dedup.call_id
                )),
                group_id,
                mount_epoch,
            ));
        }

        let route_epoch = self.core.authoritative_route_epoch().await;
        match applied.result {
            AppDataResponse::Fs(FsCommandResult::Ok(ok)) => Some(self.core.success_with_route_epoch(
                &req.ctx,
                CloseWriteOutput {
                    committed_size: req.intent.final_size,
                    file_version: ok.file_version,
                },
                group_id,
                mount_epoch,
                route_epoch,
            )),
            AppDataResponse::Fs(FsCommandResult::Err(err)) => {
                Some(
                    self.core
                        .fatal_fs_failure(&req.ctx, err.errno, err.message, group_id, mount_epoch),
                )
            }
            _ => Some(self.core.failure_from_error(
                &req.ctx,
                MetadataError::InvalidArgument(format!(
                    "applied result for call_id {} is not a CloseWrite filesystem result",
                    dedup.call_id
                )),
                group_id,
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
    ) -> MetadataResult<(Command, Option<u64>, Option<u64>)> {
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
        let inode = storage
            .get_inode(inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {}", inode_id)))?;
        let (group_id, mount_epoch) = self.core.mount_hints_for_mount(inode.mount_id);

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

        let build_command = |commit_mode| Command::CloseWrite {
            dedup: dedup.clone(),
            inode_id,
            extents: extents.clone(),
            final_size: req.intent.final_size,
            lease_id,
            open_epoch: req.open_epoch,
            lease_epoch: req.lease_epoch,
            commit_mode,
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

        Ok((command, group_id, mount_epoch))
    }

    pub(crate) fn preflight_open_write_runtime(
        &self,
        req_ctx: &RequestContext,
        desired_len: Option<u64>,
        layout: FileLayout,
        group_id: Option<u64>,
        mount_epoch: Option<u64>,
    ) -> Option<crate::service::domain::CoreFailure> {
        let worker_manager = match self.core.worker_manager.as_ref() {
            Some(worker_manager) => worker_manager,
            None => {
                return self
                    .core
                    .failure_from_error::<()>(
                        req_ctx,
                        MetadataError::ServiceUnavailable("Worker manager not available".to_string()),
                        group_id,
                        mount_epoch,
                    )
                    .err();
            }
        };

        let desired_len = desired_len.unwrap_or(4 * 1024 * 1024);
        let block_size = (layout.block_size as u64).max(1);
        let num_blocks = desired_len.div_ceil(block_size).clamp(1, 10);
        for _ in 0..num_blocks {
            let placement = match worker_manager.select_workers_for_placement(3, None) {
                Ok(placement) => placement,
                Err(err) => {
                    return self
                        .core
                        .failure_from_error::<()>(req_ctx, err, group_id, mount_epoch)
                        .err()
                }
            };
            let has_live_endpoint = placement
                .all_workers()
                .any(|worker_id| worker_manager.get_worker(worker_id).is_some());
            if !has_live_endpoint {
                return self
                    .core
                    .failure_from_error::<()>(
                        req_ctx,
                        MetadataError::ServiceUnavailable(
                            "selected placement has no live worker endpoints".to_string(),
                        ),
                        group_id,
                        mount_epoch,
                    )
                    .err();
            }
        }

        None
    }
}
