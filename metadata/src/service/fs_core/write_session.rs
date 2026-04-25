// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use super::{CoreWriteOp, FsCore};
use crate::error::MetadataError;
use crate::raft::{Command, DedupKey};
use crate::service::domain::{
    CloseWriteInput, CloseWriteOutput, CoreResult, FsyncBarrierInput, FsyncBarrierOutput, OpenWriteInput,
    OpenWriteOutput, ReleaseSessionInput, ReleaseSessionOutput, RenewLeaseInput, RenewLeaseOutput, RequestContext,
    SessionGuardInputs, SessionKey, WorkerHint, WriteTarget,
};
use common::error::canonical::{RefreshHint, RefreshReason, WorkerEndpointHint};
use common::header::RpcErrorCode;
use proto::worker::worker_data_service_client::WorkerDataServiceClient;
use proto::worker::CommitWriteRequestProto;
use std::time::{SystemTime, UNIX_EPOCH};
use types::fs::FsErrorCode;
use types::ids::{BlockId, BlockIndex, DataHandleId, WorkerId};
use types::lease::FencingToken;

impl FsCore {
    pub(crate) async fn plan_session(
        &self,
        req_ctx: &RequestContext,
        file_handle: u64,
    ) -> CoreResult<SessionGuardInputs> {
        let session = self.write_session_manager.get_session(file_handle);
        self.success(
            req_ctx,
            SessionGuardInputs {
                file_handle,
                inode_id: session.as_ref().map(|s| s.inode_id),
                mount_id: session.as_ref().map(|s| s.mount_id),
            },
            None,
            None,
        )
    }

    pub(crate) fn write_session_for_handle(&self, file_handle: u64) -> Option<crate::write_session::WriteSession> {
        self.write_session_manager.get_session(file_handle)
    }

    pub(crate) async fn execute_release(&self, req: ReleaseSessionInput) -> CoreResult<ReleaseSessionOutput> {
        WriteSessionCoordinator::new(self).execute_release(req).await
    }

    pub(crate) async fn execute_renew_inode_lease(&self, req: RenewLeaseInput) -> CoreResult<RenewLeaseOutput> {
        WriteSessionCoordinator::new(self).execute_renew_inode_lease(req).await
    }

    pub(crate) async fn execute_open_write(&self, req: OpenWriteInput) -> CoreResult<OpenWriteOutput> {
        WriteSessionCoordinator::new(self).execute_open_write(req).await
    }

    pub(crate) async fn execute_close_write(&self, req: CloseWriteInput) -> CoreResult<CloseWriteOutput> {
        WriteSessionCoordinator::new(self).execute_close_write(req).await
    }

    pub(crate) async fn execute_fsync(&self, req: FsyncBarrierInput) -> CoreResult<FsyncBarrierOutput> {
        WriteSessionCoordinator::new(self).execute_fsync(req).await
    }

    pub(crate) async fn execute_hsync(&self, req: FsyncBarrierInput) -> CoreResult<FsyncBarrierOutput> {
        self.execute_fsync(req).await
    }

    pub(crate) async fn execute_hflush(&self, req: FsyncBarrierInput) -> CoreResult<FsyncBarrierOutput> {
        self.execute_fsync(req).await
    }
}

fn canonical_from_error_detail(detail: proto::common::ErrorDetailProto) -> common::error::canonical::CanonicalError {
    proto::convert::error_detail_to_canonical(&detail)
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

struct WriteSessionCoordinator<'a> {
    core: &'a FsCore,
}

impl<'a> WriteSessionCoordinator<'a> {
    fn new(core: &'a FsCore) -> Self {
        Self { core }
    }

    async fn execute_release(&self, req: ReleaseSessionInput) -> CoreResult<ReleaseSessionOutput> {
        let session = match self.core.write_session_manager.get_session(req.file_handle) {
            Some(session) => session,
            None => {
                let route_epoch = self.core.authoritative_route_epoch().await;
                return self
                    .core
                    .success_with_route_epoch(&req.ctx, ReleaseSessionOutput, None, None, route_epoch);
            }
        };

        self.core
            .inode_lease_manager
            .release(session.inode_id, session.lease_id, session.lease_epoch);
        self.core.write_session_manager.remove_session(req.file_handle);

        let (group_id, mount_epoch) = self.core.mount_hints_for_mount(session.mount_id);
        let route_epoch = self.core.authoritative_route_epoch().await;
        self.core
            .success_with_route_epoch(&req.ctx, ReleaseSessionOutput, group_id, mount_epoch, route_epoch)
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
                        "write session not found for handle={}; reopen and replay RenewWriteSessionLease",
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
                    "lease/session mismatch: expected lease_id={:?} lease_epoch={}, got lease_id={:?} lease_epoch={}; reopen and replay RenewWriteSessionLease",
                    session.lease_id,
                    session.lease_epoch,
                    lease_id_typed,
                    req.lease_epoch,
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
                        "lease renewal rejected for handle={}; session expired, reopen and replay RenewWriteSessionLease",
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
            .validate_route_epoch(&req.ctx, req.freshness, group_id, mount_epoch, "OpenWriteByPath")
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
        let data_handle_id = DataHandleId::new(inode_id.as_raw());
        let desired_len = req.desired_len.unwrap_or(4 * 1024 * 1024);
        let block_size = 4 * 1024 * 1024;
        let num_blocks = ((desired_len + block_size - 1) / block_size).max(1).min(10);

        let worker_manager = match self.core.worker_manager.as_ref() {
            Some(worker_manager) => worker_manager,
            None => {
                return self.core.failure_from_error(
                    &req.ctx,
                    MetadataError::Internal("Worker manager not available".to_string()),
                    group_id,
                    mount_epoch,
                );
            }
        };

        let mut write_targets = Vec::with_capacity(num_blocks as usize);
        let mut write_targets_proto = Vec::with_capacity(num_blocks as usize);
        for i in 0..num_blocks {
            let block_index = BlockIndex::new(i as u32);
            let block_id = BlockId::new(data_handle_id, block_index);
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
                    let endpoint = format!("{}:{}", worker_info.address, 0);
                    worker_endpoints.push(WorkerHint {
                        worker_id,
                        endpoint: endpoint.clone(),
                        net_transport_kind: worker_info.net_transport_kind as i32,
                        worker_epoch: worker_info.worker_epoch,
                    });
                    worker_endpoints_proto.push(proto::common::WorkerEndpointInfoProto {
                        worker_id: worker_id.as_raw(),
                        endpoint,
                        net_transport_kind: worker_info.net_transport_kind as i32,
                        worker_epoch: worker_info.worker_epoch,
                    });
                }
            }

            let target_token = FencingToken {
                block_id,
                owner: caller_ctx.client.client_id,
                epoch: lease_epoch,
            };
            write_targets.push(WriteTarget {
                block_id,
                worker_endpoints,
                fencing_token: target_token,
            });
            write_targets_proto.push(proto::metadata::WriteTargetProto {
                block_id: Some(proto::common::BlockIdProto {
                    data_handle_id: block_id.data_handle_id.as_raw(),
                    block_index: block_id.index.as_raw(),
                }),
                worker_endpoints: worker_endpoints_proto,
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
        let file_handle = self.core.write_session_manager.create_session(
            inode_id,
            inode.mount_id,
            lease_id,
            lease_epoch,
            session_token,
            open_epoch,
            base_size,
            mode,
            write_targets_proto,
            crate::write_session::WriterIdentity {
                client_id: caller_ctx.client.client_id,
                call_id: caller_ctx.client.call_id,
            },
        );

        self.core.success_with_route_epoch(
            &req.ctx,
            OpenWriteOutput {
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

    async fn execute_close_write(&self, req: CloseWriteInput) -> CoreResult<CloseWriteOutput> {
        let caller_ctx = &req.ctx.caller;
        let file_handle = req.file_handle;

        let session = match self.core.write_session_manager.get_session(file_handle) {
            Some(session) => session,
            None => {
                return self.core.session_terminal_failure(
                    &req.ctx,
                    RefreshReason::SessionInvalid,
                    RpcErrorCode::Fencing,
                    format!(
                        "write session not found for handle={}; reopen and replay CloseWriteSession",
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
            .validate_route_epoch(&req.ctx, req.freshness, group_id, mount_epoch, "CloseWriteSession")
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
                                FsCore::replay_hint("CloseWriteSession")
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
                    "lease/session mismatch: expected lease_id={:?} lease_epoch={}, got lease_id={:?} lease_epoch={}; reopen and replay CloseWriteSession",
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
                        "missing fencing_token for handle={}; reopen and replay CloseWriteSession",
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
                    "fencing_token mismatch for handle={}; reopen and replay CloseWriteSession",
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
                    "open_epoch mismatch: expected {}, got {}; reopen and replay CloseWriteSession",
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
                    "lease validation rejected for handle={}; session expired, reopen and replay CloseWriteSession",
                    file_handle,
                ),
                group_id,
                mount_epoch,
            );
        }

        let extents = req.intent.extents;
        if session.mode == crate::inode_lease::WriteMode::Append {
            let mut expected_offset = session.base_size;
            for extent in &extents {
                if extent.file_offset != expected_offset {
                    return self.core.fatal_fs_failure(
                        &req.ctx,
                        FsErrorCode::EInval,
                        format!(
                            "Extent file_offset mismatch: expected {}, got {}",
                            expected_offset, extent.file_offset
                        ),
                        group_id,
                        mount_epoch,
                    );
                }
                expected_offset += extent.len;
            }
            let expected_final_size = session.base_size + extents.iter().map(|e| e.len).sum::<u64>();
            if req.intent.final_size != expected_final_size {
                return self.core.fatal_fs_failure(
                    &req.ctx,
                    FsErrorCode::EInval,
                    format!(
                        "Final size mismatch: expected {}, got {} (append mode)",
                        expected_final_size, req.intent.final_size
                    ),
                    group_id,
                    mount_epoch,
                );
            }
        } else {
            for extent in &extents {
                if extent.file_offset + extent.len > req.intent.final_size {
                    return self.core.fatal_fs_failure(
                        &req.ctx,
                        FsErrorCode::EInval,
                        format!(
                            "Extent extends beyond final_size: extent_end={}, final_size={}",
                            extent.file_offset + extent.len,
                            req.intent.final_size
                        ),
                        group_id,
                        mount_epoch,
                    );
                }
            }
        }

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

        let dedup = match self.core.dedup_key(caller_ctx) {
            Ok(k) => k,
            Err(err) => {
                return self.core.failure_from_error(
                    &req.ctx,
                    err,
                    Some(ctx.namespace_owner_group_id.as_raw()),
                    Some(ctx.mount_epoch),
                );
            }
        };

        let command = Command::CloseWrite {
            dedup,
            inode_id: session.inode_id,
            extents,
            final_size: req.intent.final_size,
            lease_id: session.lease_id,
            open_epoch: session.open_epoch,
            lease_epoch: request_lease_epoch,
        };
        if let Err(err) = self.core.propose_fs_write_command(CoreWriteOp::SetAttr, command).await {
            return self.core.failure_from_error(
                &req.ctx,
                err,
                Some(ctx.namespace_owner_group_id.as_raw()),
                Some(ctx.mount_epoch),
            );
        }

        self.core
            .inode_lease_manager
            .release(session.inode_id, lease_id_typed, session.lease_epoch);
        self.core.write_session_manager.remove_session(file_handle);

        self.core.success_with_route_epoch(
            &req.ctx,
            CloseWriteOutput {
                committed_size: req.intent.final_size,
                file_version: None,
            },
            Some(ctx.namespace_owner_group_id.as_raw()),
            Some(ctx.mount_epoch),
            route_epoch,
        )
    }

    async fn execute_fsync(&self, req: FsyncBarrierInput) -> CoreResult<FsyncBarrierOutput> {
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
            Err(err) => return self.core.failure_from_error(&req.ctx, err, None, None),
        };

        if !inode.kind.is_file() {
            return self.core.failure_from_error(
                &req.ctx,
                MetadataError::IsDir(format!("Inode is not a file: {}", inode_id)),
                None,
                None,
            );
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
            .validate_route_epoch(&req.ctx, req.freshness, group_id, mount_epoch, "FsyncSession")
            .await
        {
            Ok(route_epoch) => route_epoch,
            Err(err) => return Err(err),
        };

        let mut lease_id = req.lease_id;
        let mut lease_epoch = req.lease_epoch.unwrap_or(0);
        let presented_token = req.fencing_token;
        let mut fencing_token = presented_token.as_ref().and_then(|token| {
            token.block_id.map(|block_id| FencingToken {
                block_id,
                owner: types::ids::ClientId::new(token.owner),
                epoch: token.epoch,
            })
        });
        let mut commit_workers: Vec<proto::common::WorkerEndpointInfoProto> = Vec::new();
        let mut target_size = req.target_size.unwrap_or(inode.attrs.size);

        if let Some(handle) = req.file_handle {
            let session = match self.core.write_session_manager.get_session(handle) {
                Some(session) => session,
                None => {
                    return self.core.session_terminal_failure(
                        &req.ctx,
                        RefreshReason::SessionInvalid,
                        RpcErrorCode::Fencing,
                        format!(
                            "write session not found for handle={}; reopen and replay FsyncSession",
                            handle,
                        ),
                        group_id,
                        mount_epoch,
                    );
                }
            };

            if session.inode_id != inode_id {
                return self.core.session_terminal_failure(
                    &req.ctx,
                    RefreshReason::SessionInvalid,
                    RpcErrorCode::EpochMismatch,
                    format!(
                        "file_handle={} does not match inode={}; reopen and replay FsyncSession",
                        handle, inode_id,
                    ),
                    group_id,
                    mount_epoch,
                );
            }

            if lease_id.is_none() {
                lease_id = Some(session.lease_id);
            }
            if lease_epoch == 0 {
                lease_epoch = session.lease_epoch;
            }
            if presented_token.is_none() {
                fencing_token = Some(session.fencing_token);
            } else if let Some(token) = presented_token.as_ref() {
                if !FsCore::fencing_token_matches_session(&session, token) {
                    return self.core.session_terminal_failure(
                        &req.ctx,
                        RefreshReason::SessionInvalid,
                        RpcErrorCode::Fencing,
                        format!(
                            "fencing_token mismatch for handle={}; reopen and replay FsyncSession",
                            handle,
                        ),
                        group_id,
                        mount_epoch,
                    );
                }
            }

            for wt in &session.write_targets {
                commit_workers.extend(wt.worker_endpoints.clone());
            }
            target_size = target_size.max(session.base_size).max(session.last_written);

            if let Some(client_worker_epoch) = req.freshness.worker_epoch {
                let mismatch = session
                    .write_targets
                    .iter()
                    .flat_map(|t| t.worker_endpoints.iter())
                    .any(|ep| ep.worker_epoch != client_worker_epoch);
                if mismatch {
                    let server_worker_epoch = session
                        .write_targets
                        .iter()
                        .flat_map(|t| t.worker_endpoints.iter())
                        .map(|ep| ep.worker_epoch)
                        .max();
                    let hint = worker_refresh_hint_from_session(&session, server_worker_epoch, false);
                    return self.core.need_refresh_failure_with_hint(
                        &req.ctx,
                        RpcErrorCode::WorkerEpochMismatch,
                        RefreshReason::WorkerEpochMismatch,
                        format!(
                            "worker_epoch mismatch: client={}, session_targets differ; {}",
                            client_worker_epoch,
                            FsCore::replay_hint("FsyncSession")
                        ),
                        group_id,
                        mount_epoch,
                        route_epoch,
                        Some(hint),
                    );
                }
            }
        }

        let lease_id_typed = match lease_id {
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

        if self
            .core
            .inode_lease_manager
            .validate_lease(inode_id, lease_id_typed, lease_epoch)
            .is_err()
        {
            return self.core.session_terminal_failure(
                &req.ctx,
                RefreshReason::SessionExpired,
                RpcErrorCode::Fencing,
                format!(
                    "lease validation rejected for inode={}; session expired, reopen and replay FsyncSession",
                    inode_id,
                ),
                group_id,
                mount_epoch,
            );
        }

        if commit_workers.is_empty() {
            target_size = target_size.max(inode.attrs.size);
        } else {
            let mut tasks = Vec::with_capacity(commit_workers.len());
            let effective_route_epoch = req.freshness.route_epoch.or(req.ctx.route_epoch).unwrap_or(0);
            for ep in commit_workers {
                let endpoint = format!("http://{}", ep.endpoint);
                let header_client = proto::common::ClientInfoProto {
                    call_id: caller_ctx.client.call_id.to_string(),
                    client_id: caller_ctx.client.client_id.as_raw(),
                    client_name: caller_ctx.client.client_name.clone().unwrap_or_default(),
                };
                let commit_req = CommitWriteRequestProto {
                    header: Some(proto::worker::DataRequestHeaderProto {
                        client: Some(header_client.clone()),
                        traceparent: req.ctx.traceparent.clone().unwrap_or_default(),
                    }),
                    block_id: fencing_token
                        .as_ref()
                        .map(|t| proto::common::BlockIdProto {
                            data_handle_id: t.block_id.data_handle_id.as_raw(),
                            block_index: t.block_id.index.as_raw(),
                        })
                        .or_else(|| {
                            Some(proto::common::BlockIdProto {
                                data_handle_id: inode.current_data_handle_id.as_raw(),
                                block_index: 0,
                            })
                        }),
                    token: fencing_token.as_ref().map(|t| proto::common::FencingTokenProto {
                        block_id: Some(proto::common::BlockIdProto {
                            data_handle_id: t.block_id.data_handle_id.as_raw(),
                            block_index: t.block_id.index.as_raw(),
                        }),
                        owner: t.owner.as_raw(),
                        epoch: t.epoch,
                    }),
                    lease_epoch,
                    route_epoch: effective_route_epoch,
                    worker_epoch: ep.worker_epoch,
                    file_version: 0,
                    committed_length: target_size,
                };
                if let Some(hook) = self.core.worker_commit_hook.lock().unwrap().clone() {
                    let req_clone = commit_req.clone();
                    tasks.push(tokio::spawn(async move {
                        Ok::<proto::worker::CommitWriteResponseProto, MetadataError>(hook(req_clone))
                    }));
                    continue;
                }
                let mut client = match WorkerDataServiceClient::connect(endpoint.clone()).await {
                    Ok(client) => client,
                    Err(e) => {
                        return self.core.failure_from_error(
                            &req.ctx,
                            MetadataError::ServiceUnavailable(format!("Failed to connect worker {}: {}", endpoint, e)),
                            group_id,
                            mount_epoch,
                        );
                    }
                };
                tasks.push(tokio::spawn(async move {
                    client
                        .commit_write(commit_req)
                        .await
                        .map(|resp| resp.into_inner())
                        .map_err(|status| {
                            MetadataError::ServiceUnavailable(format!("Worker commit failed: {}", status))
                        })
                }));
            }

            for t in tasks {
                let inner = match t.await {
                    Ok(Ok(resp)) => resp,
                    Ok(Err(err)) => {
                        return self.core.failure_from_error(&req.ctx, err, group_id, mount_epoch);
                    }
                    Err(e) => {
                        return self.core.failure_from_error(
                            &req.ctx,
                            MetadataError::ServiceUnavailable(format!("Join error: {}", e)),
                            group_id,
                            mount_epoch,
                        );
                    }
                };
                if let Some(err) = inner.header.and_then(|h| h.error) {
                    let cerr = canonical_from_error_detail(err);
                    return self.core.failure_from_canonical_with_route_epoch(
                        &req.ctx,
                        cerr,
                        group_id,
                        mount_epoch,
                        route_epoch,
                    );
                }
            }
        }

        let mut attrs = inode.attrs.clone();
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        attrs.size = attrs.size.max(target_size);
        attrs.update_mtime_ctime(now_ms);

        let ctx = match self.core.route_ctx_for_write_with_error_hints(
            &req.ctx,
            CoreWriteOp::SetAttr,
            &[inode_id],
            req.freshness,
            group_id,
            mount_epoch,
        ) {
            Ok(ctx) => ctx,
            Err(failure) => return Err(failure),
        };

        let dedup = if caller_ctx.client.client_id.as_raw() == 0 {
            DedupKey::system()
        } else {
            match self.core.dedup_key(caller_ctx) {
                Ok(k) => k,
                Err(err) => {
                    return self.core.failure_from_error(
                        &req.ctx,
                        err,
                        Some(ctx.namespace_owner_group_id.as_raw()),
                        Some(ctx.mount_epoch),
                    );
                }
            }
        };
        let command = Command::SetAttr {
            dedup,
            inode_id,
            mask: 1 | 32,
            attrs,
        };
        if let Err(err) = self.core.propose_fs_write_command(CoreWriteOp::SetAttr, command).await {
            return self.core.failure_from_error(
                &req.ctx,
                err,
                Some(ctx.namespace_owner_group_id.as_raw()),
                Some(ctx.mount_epoch),
            );
        }

        self.core.success_with_route_epoch(
            &req.ctx,
            FsyncBarrierOutput,
            Some(ctx.namespace_owner_group_id.as_raw()),
            Some(ctx.mount_epoch),
            route_epoch,
        )
    }
}
