// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! FileSystemServiceProto implementation.
//!
//! This module keeps the complete path-first RPC API view. It orchestrates
//! request context extraction, guard checks, path resolution, permission
//! checks, FsCore calls, and response/header construction.
//!
//! Provider-specific permission rules live behind GuardChain/PermissionChecker.
//! Domain freshness, session, lease, and fencing semantics remain in FsCore.

use super::domain::{
    AbortWriteInput, AddBlockInput, CloseWriteInput, CloseWriteIntent, CreateInput, DeleteEmptyDirInput,
    DeleteTreeInput, FileRange, Freshness, GetAttrInput, GetFileLayoutInput, MkdirInput, OpenWriteInput, ReadDirInput,
    RenameInput, RenewLeaseInput, RequestContext, SyncWriteInput, SyncWriteMode, UnlinkInput,
};
use super::guard::{GuardChain, GuardFailure, LeadershipChecker};
use super::MsyncHandler;
use super::{
    fencing_to_proto, file_attrs_from_proto, file_attrs_to_proto, file_layout_from_proto, header_from_canonical_error,
    header_from_core_failure, lease_id_from_proto, lease_id_to_proto, location_to_proto, ok_header_from_core_success,
    presented_fencing_from_proto, request_context_from_proto, validate_active_write_layout, write_target_to_proto,
};
use super::{FsCore, PermissionBits, PermissionChecker, SharedWorkerCommitHook};
use crate::error::{to_canonical_fs, MetadataError};
use crate::mount::MountTable;
use crate::observe;
use crate::path_resolver::{MountContext, PathResolver, ResolvedPath};
use crate::raft::RocksDBStorage;
use proto::metadata::file_system_service_proto_server::FileSystemServiceProto;
use proto::metadata::*;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::instrument;
use types::ids::DataHandleId;
use types::CommittedBlock;

trait HeaderResponse {
    fn with_header(self, header: proto::common::ResponseHeaderProto) -> Self;
}

macro_rules! impl_header_response {
    ($($resp_ty:ty),+ $(,)?) => {
        $(
            impl HeaderResponse for $resp_ty {
                fn with_header(mut self, header: proto::common::ResponseHeaderProto) -> Self {
                    self.header = Some(header);
                    self
                }
            }
        )+
    };
}

impl_header_response!(
    GetStatusResponseProto,
    ListStatusResponseProto,
    CreateDirectoryResponseProto,
    DeleteResponseProto,
    RenameResponseProto,
    OpenFileResponseProto,
    GetBlockLocationsResponseProto,
    CreateFileResponseProto,
    AppendFileResponseProto,
    AddBlockResponseProto,
    CommitFileResponseProto,
    AbortFileWriteResponseProto,
    RenewLeaseResponseProto,
    SyncWriteResponseProto,
    MsyncResponseProto,
);

/// FileSystemServiceProto implementation.
pub struct MetadataFileSystemServiceImpl {
    path_resolver: PathResolver,
    fs_core: Arc<FsCore>,
    guard_chain: GuardChain,
    msync: Option<MsyncHandler>,
    _metrics: Option<Arc<crate::metrics::MetadataMetrics>>,
}

macro_rules! response_with_header {
    ($resp:expr, $header:expr) => {{
        Ok(Response::new(HeaderResponse::with_header($resp, $header)))
    }};
}

macro_rules! error_response {
    ($resp_ty:ty, $header:expr) => {{
        response_with_header!(<$resp_ty>::default(), $header)
    }};
}

macro_rules! request_context_or_error {
    ($req:expr, $resp_ty:ty) => {{
        match request_context_from_proto(&$req.header) {
            Ok(ctx) => ctx,
            Err(err) => {
                return error_response!($resp_ty, header_from_canonical_error(&$req.header, None, None, &err));
            }
        }
    }};
}

macro_rules! guard_or_error {
    ($svc:expr, $req:expr, $resp_ty:ty, $check:expr) => {{
        if let Err(failure) = $check.await {
            let resp_header = $svc.header_from_guard_failure(&$req.header, failure);
            return error_response!($resp_ty, resp_header);
        }
    }};
}

pub struct MetadataFileSystemServiceDeps {
    pub authority: FileSystemAuthorityDeps,
    pub runtime: FileSystemRuntimeDeps,
    pub policy: FileSystemPolicyDeps,
}

pub struct FileSystemAuthorityDeps {
    pub state_store: Arc<dyn crate::state::StateStore>,
    pub mount_table: Arc<MountTable>,
    pub storage: Arc<RocksDBStorage>,
    pub raft_node: Option<Arc<crate::raft::AppRaftNode>>,
    pub group_name: types::GroupName,
}

pub struct FileSystemRuntimeDeps {
    pub write_session_manager: Arc<crate::write_session::WriteSessionManager>,
    pub inode_lease_manager: Arc<crate::inode_lease::InodeLeaseManager>,
    pub worker_commit_hook: SharedWorkerCommitHook,
    pub worker_manager: Option<Arc<crate::worker::WorkerManager>>,
    pub metrics: Option<Arc<crate::metrics::MetadataMetrics>>,
    pub readiness_gate: Option<Arc<crate::readiness::RootReadinessGate>>,
}

pub struct FileSystemPolicyDeps {
    pub leadership_checker: Option<Arc<dyn LeadershipChecker>>,
    pub permission_checker: Arc<dyn PermissionChecker>,
}

impl MetadataFileSystemServiceImpl {
    pub fn new(deps: MetadataFileSystemServiceDeps) -> Self {
        let MetadataFileSystemServiceDeps {
            authority,
            runtime,
            policy,
        } = deps;
        let FileSystemAuthorityDeps {
            state_store,
            mount_table,
            storage,
            raft_node,
            group_name,
        } = authority;
        let FileSystemRuntimeDeps {
            write_session_manager,
            inode_lease_manager,
            worker_commit_hook,
            worker_manager,
            metrics,
            readiness_gate,
        } = runtime;
        let FileSystemPolicyDeps {
            leadership_checker,
            permission_checker,
        } = policy;

        let path_resolver = PathResolver::new(Arc::clone(&mount_table), Arc::clone(&storage));
        let mut fs_core = FsCore::new(
            state_store,
            Arc::clone(&mount_table),
            write_session_manager,
            inode_lease_manager,
            worker_commit_hook,
        );
        fs_core.set_storage(Arc::clone(&storage));
        if let Some(raft_node) = raft_node.as_ref() {
            fs_core.set_raft_node(Arc::clone(raft_node));
        }
        if let Some(worker_manager) = worker_manager.as_ref() {
            fs_core.set_worker_manager(Arc::clone(worker_manager));
        }
        if let Some(metrics) = metrics.as_ref() {
            fs_core.set_metrics(Arc::clone(metrics));
        }
        let fs_core = Arc::new(fs_core);

        let leadership_checker = leadership_checker.or_else(|| {
            raft_node
                .as_ref()
                .map(|raft_node| Arc::clone(raft_node) as Arc<dyn LeadershipChecker>)
        });
        let guard_chain = GuardChain::new(mount_table)
            .with_readiness_gate(readiness_gate)
            .with_leadership_checker(leadership_checker)
            .with_permission_checker(permission_checker);
        let msync = raft_node
            .as_ref()
            .map(|raft_node| MsyncHandler::new(Arc::clone(raft_node), group_name));

        Self {
            path_resolver,
            fs_core,
            guard_chain,
            msync,
            _metrics: metrics,
        }
    }

    fn header_from_path_error(
        &self,
        req_header: &Option<proto::common::RequestHeaderProto>,
        err: MetadataError,
        mount_ctx: Option<&MountContext>,
    ) -> proto::common::ResponseHeaderProto {
        let (group_name, mount_epoch) = mount_ctx
            .map(|ctx| (Some(ctx.owner_group_name.clone()), Some(ctx.mount_epoch)))
            .unwrap_or((None, None));

        let canonical = to_canonical_fs(err);
        header_from_canonical_error(req_header, group_name, mount_epoch, &canonical)
    }

    fn header_from_resolution_error(
        &self,
        req_header: &Option<proto::common::RequestHeaderProto>,
        path: &str,
        err: MetadataError,
    ) -> proto::common::ResponseHeaderProto {
        let mount_ctx = self
            .path_resolver
            .resolve_mount_components(path)
            .ok()
            .map(|(mount_ctx, _)| mount_ctx);
        self.header_from_path_error(req_header, err, mount_ctx.as_ref())
    }

    fn header_from_guard_failure(
        &self,
        req_header: &Option<proto::common::RequestHeaderProto>,
        failure: GuardFailure,
    ) -> proto::common::ResponseHeaderProto {
        header_from_canonical_error(req_header, failure.group_name, failure.mount_epoch, &failure.err)
    }

    fn freshness_from_header(header: &Option<proto::common::RequestHeaderProto>) -> Freshness {
        Freshness {
            mount_epoch: header.as_ref().and_then(|h| h.mount_epoch),
            route_epoch: header.as_ref().and_then(|h| h.route_epoch),
        }
    }

    fn write_handle_from_key(key: &super::domain::SessionKey) -> WriteHandleProto {
        WriteHandleProto {
            handle_id: key.file_handle,
            lease_id: Some(lease_id_to_proto(key.lease_id)),
            lease_epoch: key.lease_epoch,
            open_epoch: key.open_epoch,
            fencing_token: Some(fencing_to_proto(key.fencing_token)),
        }
    }

    fn write_handle_or_error(
        header: &Option<proto::common::RequestHeaderProto>,
        handle: Option<WriteHandleProto>,
    ) -> Result<WriteHandleProto, Box<proto::common::ResponseHeaderProto>> {
        handle.ok_or_else(|| {
            Box::new(header_from_canonical_error(
                header,
                None,
                None,
                &to_canonical_fs(MetadataError::InvalidArgument("missing write_handle".to_string())),
            ))
        })
    }

    fn committed_block_from_proto(block: CommittedBlockProto) -> Result<CommittedBlock, MetadataError> {
        CommittedBlock::try_from(block).map_err(MetadataError::InvalidArgument)
    }

    fn data_handle_proto(data_handle_id: DataHandleId) -> proto::common::DataHandleIdProto {
        data_handle_id.into()
    }

    async fn create_directory_recursive(
        &self,
        req: CreateDirectoryRequestProto,
        req_ctx: RequestContext,
    ) -> Result<Response<CreateDirectoryResponseProto>, Status> {
        let (mount_ctx, components) = match self.path_resolver.resolve_mount_components(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let header = self.header_from_path_error(&req.header, err, None);
                return error_response!(CreateDirectoryResponseProto, header);
            }
        };
        if components.is_empty() {
            let header = self.header_from_path_error(
                &req.header,
                MetadataError::InvalidArgument("Cannot operate on mount root".to_string()),
                Some(&mount_ctx),
            );
            return error_response!(CreateDirectoryResponseProto, header);
        }
        let attrs = match file_attrs_from_proto(req.attrs) {
            Ok(attrs) => attrs,
            Err(err) => {
                let header = self.header_from_path_error(&req.header, err, Some(&mount_ctx));
                return error_response!(CreateDirectoryResponseProto, header);
            }
        };

        let mut parent_inode_id = mount_ctx.root_inode_id;
        let mut traverse_dir_inode_ids = Vec::with_capacity(components.len());
        let mut last_create_success = None;
        let mut last_created_parent_inode_id = None;
        let freshness = Self::freshness_from_header(&req.header);

        for name in components {
            traverse_dir_inode_ids.push(parent_inode_id);
            match self.path_resolver.get_dentry(parent_inode_id, &name) {
                Ok(Some(child_inode_id)) => {
                    let inode = match self.path_resolver.get_inode(child_inode_id) {
                        Ok(Some(inode)) => inode,
                        Ok(None) => {
                            let header = self.header_from_path_error(
                                &req.header,
                                MetadataError::NotFound(format!("Target inode not found: {}", child_inode_id)),
                                Some(&mount_ctx),
                            );
                            return error_response!(CreateDirectoryResponseProto, header);
                        }
                        Err(err) => {
                            let header = self.header_from_path_error(&req.header, err, Some(&mount_ctx));
                            return error_response!(CreateDirectoryResponseProto, header);
                        }
                    };
                    if !inode.kind.is_dir() {
                        let header = self.header_from_path_error(
                            &req.header,
                            MetadataError::NotDir(format!("Path component is not a directory: {}", name)),
                            Some(&mount_ctx),
                        );
                        return error_response!(CreateDirectoryResponseProto, header);
                    }
                    parent_inode_id = child_inode_id;
                }
                Ok(None) => {
                    let create_parent_inode_id = parent_inode_id;
                    let resolved_parent = ResolvedPath {
                        mount_ctx: mount_ctx.clone(),
                        parent_inode_id: Some(create_parent_inode_id),
                        name: Some(name.clone()),
                        inode_id: None,
                        traverse_dir_inode_ids: traverse_dir_inode_ids.clone(),
                    };
                    guard_or_error!(
                        self,
                        req,
                        CreateDirectoryResponseProto,
                        self.guard_chain.check_parent_perm(
                            &req_ctx,
                            PermissionBits::WRITE,
                            &req.path,
                            &resolved_parent,
                        )
                    );

                    let mut child_ctx = req_ctx.clone();
                    child_ctx.caller = req_ctx.caller.child();
                    match self
                        .fs_core
                        .execute_mkdir(MkdirInput {
                            ctx: child_ctx,
                            parent_inode_id: create_parent_inode_id,
                            name,
                            attrs: attrs.clone(),
                            freshness,
                        })
                        .await
                    {
                        Ok(success) => {
                            let Some(created_inode_id) = success.payload.inode_id else {
                                let header = self.header_from_path_error(
                                    &req.header,
                                    MetadataError::Internal("CreateDirectory succeeded without inode_id".to_string()),
                                    Some(&mount_ctx),
                                );
                                return error_response!(CreateDirectoryResponseProto, header);
                            };
                            parent_inode_id = created_inode_id;
                            last_created_parent_inode_id = Some(create_parent_inode_id);
                            last_create_success = Some(success);
                        }
                        Err(failure) => {
                            tracing::warn!(
                                target: "metadata.state",
                                op = "CreateDirectory",
                                result = "rejected",
                                error_code = observe::canonical_error_kind(&failure.error),
                                client_id = %req_ctx.caller.client.client_id,
                                call_id = %req_ctx.caller.client.call_id,
                                path = %req.path,
                                parent_inode_id = create_parent_inode_id.as_raw(),
                                "CreateDirectory rejected"
                            );
                            return error_response!(
                                CreateDirectoryResponseProto,
                                header_from_core_failure(&req_ctx, &failure)
                            );
                        }
                    }
                }
                Err(err) => {
                    let header = self.header_from_path_error(&req.header, err, Some(&mount_ctx));
                    return error_response!(CreateDirectoryResponseProto, header);
                }
            }
        }

        if let Some(success) = last_create_success {
            let header = ok_header_from_core_success(&req_ctx, &success);
            let attrs = success.payload.attrs.as_ref().map(file_attrs_to_proto);
            tracing::info!(
                target: "metadata.state",
                op = "CreateDirectory",
                result = "committed",
                error_code = "none",
                client_id = %req_ctx.caller.client.client_id,
                call_id = %req_ctx.caller.client.call_id,
                path = %req.path,
                inode_id = success.payload.inode_id.map(|id| id.as_raw()),
                parent_inode_id = last_created_parent_inode_id.map(|id| id.as_raw()),
                mount_epoch = success.mount_epoch,
                route_epoch = success.route_epoch,
                "CreateDirectory committed"
            );
            return response_with_header!(
                CreateDirectoryResponseProto {
                    attrs,
                    ..Default::default()
                },
                header
            );
        }

        match self
            .fs_core
            .execute_get_attr(GetAttrInput {
                ctx: req_ctx.clone(),
                inode_id: parent_inode_id,
                freshness,
            })
            .await
        {
            Ok(success) => response_with_header!(
                CreateDirectoryResponseProto {
                    attrs: Some(file_attrs_to_proto(&success.payload.attrs)),
                    ..Default::default()
                },
                ok_header_from_core_success(&req_ctx, &success)
            ),
            Err(failure) => error_response!(
                CreateDirectoryResponseProto,
                header_from_core_failure(&req_ctx, &failure)
            ),
        }
    }
}

#[tonic::async_trait]
impl FileSystemServiceProto for MetadataFileSystemServiceImpl {
    async fn msync(&self, request: Request<MsyncRequestProto>) -> Result<Response<MsyncResponseProto>, Status> {
        let req = request.into_inner();
        let response = match self.msync.as_ref() {
            Some(msync) => msync.handle(req),
            None => MsyncHandler::unavailable(req),
        };
        Ok(Response::new(response))
    }

    #[instrument(skip_all)]
    async fn get_status(
        &self,
        request: Request<GetStatusRequestProto>,
    ) -> Result<Response<GetStatusResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_or_error!(req, GetStatusResponseProto);
        guard_or_error!(
            self,
            req,
            GetStatusResponseProto,
            self.guard_chain.check_meta_read(&req_ctx)
        );

        let resolved = match self.path_resolver.resolve_inode(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let header = self.header_from_resolution_error(&req.header, &req.path, err);
                return error_response!(GetStatusResponseProto, header);
            }
        };
        guard_or_error!(
            self,
            req,
            GetStatusResponseProto,
            self.guard_chain
                .check_perm(&req_ctx, PermissionBits::READ, &req.path, &resolved)
        );
        let inode_id = match resolved.expect_inode() {
            Ok(inode_id) => inode_id,
            Err(err) => {
                let header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
                return error_response!(GetStatusResponseProto, header);
            }
        };

        match self
            .fs_core
            .execute_get_attr(GetAttrInput {
                ctx: req_ctx.clone(),
                inode_id,
                freshness: Self::freshness_from_header(&req.header),
            })
            .await
        {
            Ok(success) => response_with_header!(
                GetStatusResponseProto {
                    attrs: Some(file_attrs_to_proto(&success.payload.attrs)),
                    ..Default::default()
                },
                ok_header_from_core_success(&req_ctx, &success)
            ),
            Err(failure) => error_response!(GetStatusResponseProto, header_from_core_failure(&req_ctx, &failure)),
        }
    }

    #[instrument(skip_all)]
    async fn create_directory(
        &self,
        request: Request<CreateDirectoryRequestProto>,
    ) -> Result<Response<CreateDirectoryResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_or_error!(req, CreateDirectoryResponseProto);
        guard_or_error!(
            self,
            req,
            CreateDirectoryResponseProto,
            self.guard_chain.check_meta_write(&req_ctx)
        );
        if req.recursive {
            return self.create_directory_recursive(req, req_ctx).await;
        }

        let resolved = match self.path_resolver.resolve_path(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let header = self.header_from_path_error(&req.header, err, None);
                return error_response!(CreateDirectoryResponseProto, header);
            }
        };
        guard_or_error!(
            self,
            req,
            CreateDirectoryResponseProto,
            self.guard_chain
                .check_parent_perm(&req_ctx, PermissionBits::WRITE, &req.path, &resolved)
        );
        let parent_inode_id = match resolved.expect_parent() {
            Ok(parent_inode_id) => parent_inode_id,
            Err(err) => {
                let header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
                return error_response!(CreateDirectoryResponseProto, header);
            }
        };
        let name = match resolved.expect_name() {
            Ok(name) => name.to_string(),
            Err(err) => {
                let header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
                return error_response!(CreateDirectoryResponseProto, header);
            }
        };
        let attrs = match file_attrs_from_proto(req.attrs) {
            Ok(attrs) => attrs,
            Err(err) => {
                let header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
                return error_response!(CreateDirectoryResponseProto, header);
            }
        };

        match self
            .fs_core
            .execute_mkdir(MkdirInput {
                ctx: req_ctx.clone(),
                parent_inode_id,
                name,
                attrs,
                freshness: Self::freshness_from_header(&req.header),
            })
            .await
        {
            Ok(success) => {
                let header = ok_header_from_core_success(&req_ctx, &success);
                let payload = success.payload;
                let attrs = payload.attrs.as_ref().map(file_attrs_to_proto);
                tracing::info!(
                    target: "metadata.state",
                    op = "CreateDirectory",
                    result = "committed",
                    error_code = "none",
                    client_id = %req_ctx.caller.client.client_id,
                    call_id = %req_ctx.caller.client.call_id,
                    path = %req.path,
                    inode_id = payload.inode_id.map(|id| id.as_raw()),
                    parent_inode_id = parent_inode_id.as_raw(),
                    mount_epoch = success.mount_epoch,
                    route_epoch = success.route_epoch,
                    "CreateDirectory committed"
                );
                response_with_header!(
                    CreateDirectoryResponseProto {
                        attrs,
                        ..Default::default()
                    },
                    header
                )
            }
            Err(failure) => {
                tracing::warn!(
                    target: "metadata.state",
                    op = "CreateDirectory",
                    result = "rejected",
                    error_code = observe::canonical_error_kind(&failure.error),
                    client_id = %req_ctx.caller.client.client_id,
                    call_id = %req_ctx.caller.client.call_id,
                    path = %req.path,
                    parent_inode_id = parent_inode_id.as_raw(),
                    "CreateDirectory rejected"
                );
                error_response!(
                    CreateDirectoryResponseProto,
                    header_from_core_failure(&req_ctx, &failure)
                )
            }
        }
    }

    #[instrument(skip_all)]
    async fn delete(&self, request: Request<DeleteRequestProto>) -> Result<Response<DeleteResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_or_error!(req, DeleteResponseProto);
        guard_or_error!(
            self,
            req,
            DeleteResponseProto,
            self.guard_chain.check_meta_write(&req_ctx)
        );

        let resolved = match self.path_resolver.resolve_path(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                return error_response!(DeleteResponseProto, self.header_from_path_error(&req.header, err, None))
            }
        };
        guard_or_error!(
            self,
            req,
            DeleteResponseProto,
            self.guard_chain
                .check_parent_perm(&req_ctx, PermissionBits::WRITE, &req.path, &resolved)
        );
        let parent_inode_id = match resolved.expect_parent() {
            Ok(parent_inode_id) => parent_inode_id,
            Err(err) => {
                return error_response!(
                    DeleteResponseProto,
                    self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx))
                )
            }
        };
        let name = match resolved.expect_name() {
            Ok(name) => name.to_string(),
            Err(err) => {
                return error_response!(
                    DeleteResponseProto,
                    self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx))
                )
            }
        };
        let target_inode = match resolved.inode_id {
            Some(target_inode_id) => match self.path_resolver.get_inode(target_inode_id) {
                Ok(Some(inode)) => Some(inode),
                Ok(None) => {
                    return error_response!(
                        DeleteResponseProto,
                        self.header_from_path_error(
                            &req.header,
                            MetadataError::NotFound(format!("Target inode not found: {}", target_inode_id)),
                            Some(&resolved.mount_ctx),
                        )
                    )
                }
                Err(err) => {
                    return error_response!(
                        DeleteResponseProto,
                        self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx))
                    )
                }
            },
            None => None,
        };

        let freshness = Self::freshness_from_header(&req.header);
        let result = if req.recursive && target_inode.as_ref().map(|inode| inode.kind.is_dir()).unwrap_or(true) {
            self.fs_core
                .execute_delete_tree(DeleteTreeInput {
                    ctx: req_ctx.clone(),
                    parent_inode_id,
                    name,
                    freshness,
                })
                .await
                .map(|success| ok_header_from_core_success(&req_ctx, &success))
        } else if target_inode.as_ref().is_some_and(|inode| inode.kind.is_dir()) {
            self.fs_core
                .execute_delete_empty_dir(DeleteEmptyDirInput {
                    ctx: req_ctx.clone(),
                    parent_inode_id,
                    name,
                    freshness,
                })
                .await
                .map(|success| ok_header_from_core_success(&req_ctx, &success))
        } else {
            if target_inode.is_none() {
                return error_response!(
                    DeleteResponseProto,
                    self.header_from_path_error(
                        &req.header,
                        MetadataError::NotFound(format!("Entry not found: {}", name)),
                        Some(&resolved.mount_ctx),
                    )
                );
            }
            self.fs_core
                .execute_unlink(UnlinkInput {
                    ctx: req_ctx.clone(),
                    parent_inode_id,
                    name,
                    freshness,
                })
                .await
                .map(|success| ok_header_from_core_success(&req_ctx, &success))
        };
        match result {
            Ok(header) => {
                tracing::info!(
                    target: "metadata.state",
                    op = "Delete",
                    result = "committed",
                    error_code = "none",
                    client_id = %req_ctx.caller.client.client_id,
                    call_id = %req_ctx.caller.client.call_id,
                    path = %req.path,
                    inode_id = target_inode.as_ref().map(|inode| inode.inode_id.as_raw()),
                    parent_inode_id = parent_inode_id.as_raw(),
                    recursive = req.recursive,
                    "Delete committed"
                );
                response_with_header!(DeleteResponseProto::default(), header)
            }
            Err(failure) => {
                tracing::warn!(
                    target: "metadata.state",
                    op = "Delete",
                    result = "rejected",
                    error_code = observe::canonical_error_kind(&failure.error),
                    client_id = %req_ctx.caller.client.client_id,
                    call_id = %req_ctx.caller.client.call_id,
                    path = %req.path,
                    parent_inode_id = parent_inode_id.as_raw(),
                    recursive = req.recursive,
                    "Delete rejected"
                );
                error_response!(DeleteResponseProto, header_from_core_failure(&req_ctx, &failure))
            }
        }
    }

    #[instrument(skip_all)]
    async fn rename(&self, request: Request<RenameRequestProto>) -> Result<Response<RenameResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_or_error!(req, RenameResponseProto);
        guard_or_error!(
            self,
            req,
            RenameResponseProto,
            self.guard_chain.check_meta_write(&req_ctx)
        );

        let (src_resolved, dst_resolved) = match self.path_resolver.resolve_rename(&req.src_path, &req.dst_path) {
            Ok(resolved) => resolved,
            Err(err) => {
                return error_response!(RenameResponseProto, self.header_from_path_error(&req.header, err, None))
            }
        };
        guard_or_error!(
            self,
            req,
            RenameResponseProto,
            self.guard_chain
                .check_parent_perm(&req_ctx, PermissionBits::WRITE, &req.src_path, &src_resolved)
        );
        guard_or_error!(
            self,
            req,
            RenameResponseProto,
            self.guard_chain
                .check_parent_perm(&req_ctx, PermissionBits::WRITE, &req.dst_path, &dst_resolved)
        );
        let src_parent_inode_id = match src_resolved.expect_parent() {
            Ok(parent_inode_id) => parent_inode_id,
            Err(err) => {
                return error_response!(
                    RenameResponseProto,
                    self.header_from_path_error(&req.header, err, Some(&src_resolved.mount_ctx))
                )
            }
        };
        let dst_parent_inode_id = match dst_resolved.expect_parent() {
            Ok(parent_inode_id) => parent_inode_id,
            Err(err) => {
                return error_response!(
                    RenameResponseProto,
                    self.header_from_path_error(&req.header, err, Some(&dst_resolved.mount_ctx))
                )
            }
        };
        let src_name = match src_resolved.expect_name() {
            Ok(name) => name.to_string(),
            Err(err) => {
                return error_response!(
                    RenameResponseProto,
                    self.header_from_path_error(&req.header, err, Some(&src_resolved.mount_ctx))
                )
            }
        };
        let dst_name = match dst_resolved.expect_name() {
            Ok(name) => name.to_string(),
            Err(err) => {
                return error_response!(
                    RenameResponseProto,
                    self.header_from_path_error(&req.header, err, Some(&dst_resolved.mount_ctx))
                )
            }
        };

        match self
            .fs_core
            .execute_rename(RenameInput {
                ctx: req_ctx.clone(),
                src_parent_inode_id,
                src_name,
                dst_parent_inode_id,
                dst_name,
                flags: req.flags,
                freshness: Self::freshness_from_header(&req.header),
            })
            .await
        {
            Ok(success) => {
                tracing::info!(
                    target: "metadata.state",
                    op = "Rename",
                    result = "committed",
                    error_code = "none",
                    client_id = %req_ctx.caller.client.client_id,
                    call_id = %req_ctx.caller.client.call_id,
                    src = %req.src_path,
                    dst = %req.dst_path,
                    parent_inode_id = src_parent_inode_id.as_raw(),
                    mount_epoch = success.mount_epoch,
                    route_epoch = success.route_epoch,
                    "Rename committed"
                );
                response_with_header!(
                    RenameResponseProto::default(),
                    ok_header_from_core_success(&req_ctx, &success)
                )
            }
            Err(failure) => {
                tracing::warn!(
                    target: "metadata.state",
                    op = "Rename",
                    result = "rejected",
                    error_code = observe::canonical_error_kind(&failure.error),
                    client_id = %req_ctx.caller.client.client_id,
                    call_id = %req_ctx.caller.client.call_id,
                    src = %req.src_path,
                    dst = %req.dst_path,
                    parent_inode_id = src_parent_inode_id.as_raw(),
                    "Rename rejected"
                );
                error_response!(RenameResponseProto, header_from_core_failure(&req_ctx, &failure))
            }
        }
    }

    #[instrument(skip_all)]
    async fn list_status(
        &self,
        request: Request<ListStatusRequestProto>,
    ) -> Result<Response<ListStatusResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_or_error!(req, ListStatusResponseProto);
        guard_or_error!(
            self,
            req,
            ListStatusResponseProto,
            self.guard_chain.check_meta_read(&req_ctx)
        );
        let resolved = match self.path_resolver.resolve_inode(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                return error_response!(
                    ListStatusResponseProto,
                    self.header_from_path_error(&req.header, err, None)
                )
            }
        };
        if req.recursive {
            return error_response!(
                ListStatusResponseProto,
                self.header_from_path_error(
                    &req.header,
                    MetadataError::NotSupported("Recursive listing not yet implemented".to_string()),
                    Some(&resolved.mount_ctx),
                )
            );
        }
        guard_or_error!(
            self,
            req,
            ListStatusResponseProto,
            self.guard_chain
                .check_perm(&req_ctx, PermissionBits::READ, &req.path, &resolved)
        );
        let inode_id = match resolved.expect_inode() {
            Ok(inode_id) => inode_id,
            Err(err) => {
                return error_response!(
                    ListStatusResponseProto,
                    self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx))
                )
            }
        };
        let cursor_key = (!req.cursor.is_empty()).then_some(req.cursor.clone());
        let max_entries = (req.limit != 0).then_some(req.limit as usize);
        match self
            .fs_core
            .execute_read_dir(ReadDirInput {
                ctx: req_ctx.clone(),
                parent_inode_id: inode_id,
                cursor_key,
                max_entries,
                freshness: Self::freshness_from_header(&req.header),
            })
            .await
        {
            Ok(success) => {
                let header = ok_header_from_core_success(&req_ctx, &success);
                let payload = success.payload;
                let entries = payload
                    .entries
                    .into_iter()
                    .map(|entry| proto::fs::DirEntryProto {
                        name: entry.name,
                        kind: match entry.kind {
                            Some(types::fs::InodeKind::File) => proto::fs::InodeKindProto::InodeKindFile as i32,
                            Some(types::fs::InodeKind::Dir) => proto::fs::InodeKindProto::InodeKindDir as i32,
                            Some(types::fs::InodeKind::Symlink) => proto::fs::InodeKindProto::InodeKindSymlink as i32,
                            None => proto::fs::InodeKindProto::InodeKindUnspecified as i32,
                        },
                        attrs: entry.attrs.as_ref().map(file_attrs_to_proto),
                    })
                    .collect();
                response_with_header!(
                    ListStatusResponseProto {
                        entries,
                        next_cursor: payload.next_cursor_key,
                        eof: payload.eof,
                        ..Default::default()
                    },
                    header
                )
            }
            Err(failure) => error_response!(ListStatusResponseProto, header_from_core_failure(&req_ctx, &failure)),
        }
    }

    #[instrument(skip_all)]
    async fn open_file(
        &self,
        request: Request<OpenFileRequestProto>,
    ) -> Result<Response<OpenFileResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_or_error!(req, OpenFileResponseProto);
        guard_or_error!(
            self,
            req,
            OpenFileResponseProto,
            self.guard_chain.check_meta_read(&req_ctx)
        );

        let resolved = match self.path_resolver.resolve_inode(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                return error_response!(
                    OpenFileResponseProto,
                    self.header_from_resolution_error(&req.header, &req.path, err)
                )
            }
        };
        guard_or_error!(
            self,
            req,
            OpenFileResponseProto,
            self.guard_chain
                .check_perm(&req_ctx, PermissionBits::READ, &req.path, &resolved)
        );
        guard_or_error!(
            self,
            req,
            OpenFileResponseProto,
            self.guard_chain.check_data_read(&req_ctx, resolved.mount_ctx.mount_id)
        );
        let inode_id = match resolved.expect_inode() {
            Ok(inode_id) => inode_id,
            Err(err) => {
                return error_response!(
                    OpenFileResponseProto,
                    self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx))
                )
            }
        };
        let inode = match self.path_resolver.get_inode(inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                return error_response!(
                    OpenFileResponseProto,
                    self.header_from_path_error(
                        &req.header,
                        MetadataError::NotFound(format!("Inode not found: {}", inode_id)),
                        Some(&resolved.mount_ctx),
                    )
                )
            }
            Err(err) => {
                return error_response!(
                    OpenFileResponseProto,
                    self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx))
                )
            }
        };
        if !inode.kind.is_file() {
            return error_response!(
                OpenFileResponseProto,
                self.header_from_path_error(
                    &req.header,
                    MetadataError::IsDir(format!("Inode is not a file: {}", inode_id)),
                    Some(&resolved.mount_ctx),
                )
            );
        }
        match self
            .fs_core
            .execute_get_file_layout(GetFileLayoutInput {
                ctx: req_ctx.clone(),
                inode_id,
                range: None,
                requested_data_handle_id: None,
                freshness: Self::freshness_from_header(&req.header),
            })
            .await
        {
            Ok(success) => {
                let header = ok_header_from_core_success(&req_ctx, &success);
                let payload = success.payload;
                response_with_header!(
                    OpenFileResponseProto {
                        data_handle_id: Some(Self::data_handle_proto(inode.current_data_handle_id)),
                        file_size: payload.file_size,
                        file_version: payload.file_version,
                        ..Default::default()
                    },
                    header
                )
            }
            Err(failure) => error_response!(OpenFileResponseProto, header_from_core_failure(&req_ctx, &failure)),
        }
    }

    #[instrument(skip_all)]
    async fn get_block_locations(
        &self,
        request: Request<GetBlockLocationsRequestProto>,
    ) -> Result<Response<GetBlockLocationsResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_or_error!(req, GetBlockLocationsResponseProto);
        guard_or_error!(
            self,
            req,
            GetBlockLocationsResponseProto,
            self.guard_chain.check_meta_read(&req_ctx)
        );

        let mut requested_data_handle_id = None;
        let inode_id = match req.target.clone() {
            Some(get_block_locations_request_proto::Target::Path(path)) => {
                let resolved = match self.path_resolver.resolve_inode(&path) {
                    Ok(resolved) => resolved,
                    Err(err) => {
                        return error_response!(
                            GetBlockLocationsResponseProto,
                            self.header_from_resolution_error(&req.header, &path, err)
                        )
                    }
                };
                guard_or_error!(
                    self,
                    req,
                    GetBlockLocationsResponseProto,
                    self.guard_chain
                        .check_perm(&req_ctx, PermissionBits::READ, &path, &resolved)
                );
                guard_or_error!(
                    self,
                    req,
                    GetBlockLocationsResponseProto,
                    self.guard_chain.check_data_read(&req_ctx, resolved.mount_ctx.mount_id)
                );
                match resolved.expect_inode() {
                    Ok(inode_id) => inode_id,
                    Err(err) => {
                        return error_response!(
                            GetBlockLocationsResponseProto,
                            self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx))
                        )
                    }
                }
            }
            Some(get_block_locations_request_proto::Target::DataHandleId(data_handle)) => {
                let data_handle_id = DataHandleId::try_from(data_handle)
                    .unwrap_or_else(|()| unreachable!("DataHandleIdProto conversion is infallible"));
                requested_data_handle_id = Some(data_handle_id);
                let inode_id = match self.fs_core.inode_for_data_handle(&req_ctx, data_handle_id).await {
                    Ok(success) => success.payload,
                    Err(failure) => {
                        return error_response!(
                            GetBlockLocationsResponseProto,
                            header_from_core_failure(&req_ctx, &failure)
                        )
                    }
                };
                match self.fs_core.plan_inode_mount(&req_ctx, inode_id).await {
                    Ok(success) => {
                        guard_or_error!(
                            self,
                            req,
                            GetBlockLocationsResponseProto,
                            self.guard_chain.check_data_read(&req_ctx, success.payload.mount_id)
                        );
                    }
                    Err(failure) => {
                        return error_response!(
                            GetBlockLocationsResponseProto,
                            header_from_core_failure(&req_ctx, &failure)
                        )
                    }
                }
                inode_id
            }
            None => {
                return error_response!(
                    GetBlockLocationsResponseProto,
                    self.header_from_path_error(
                        &req.header,
                        MetadataError::InvalidArgument("missing block location target".to_string()),
                        None,
                    )
                )
            }
        };
        let inode = match self.path_resolver.get_inode(inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                return error_response!(
                    GetBlockLocationsResponseProto,
                    self.header_from_path_error(
                        &req.header,
                        MetadataError::NotFound(format!("Inode not found: {}", inode_id)),
                        None,
                    )
                )
            }
            Err(err) => {
                return error_response!(
                    GetBlockLocationsResponseProto,
                    self.header_from_path_error(&req.header, err, None)
                )
            }
        };
        let range = req.range.map(|r| FileRange {
            offset: r.offset,
            len: r.len as u64,
        });
        match self
            .fs_core
            .execute_get_file_layout(GetFileLayoutInput {
                ctx: req_ctx.clone(),
                inode_id,
                range,
                requested_data_handle_id,
                freshness: Self::freshness_from_header(&req.header),
            })
            .await
        {
            Ok(success) => {
                let header = ok_header_from_core_success(&req_ctx, &success);
                let payload = success.payload;
                response_with_header!(
                    GetBlockLocationsResponseProto {
                        data_handle_id: Some(Self::data_handle_proto(inode.current_data_handle_id)),
                        file_size: payload.file_size,
                        file_version: payload.file_version,
                        locations: payload.locations.iter().map(location_to_proto).collect(),
                        ..Default::default()
                    },
                    header
                )
            }
            Err(failure) => error_response!(
                GetBlockLocationsResponseProto,
                header_from_core_failure(&req_ctx, &failure)
            ),
        }
    }

    #[instrument(skip_all)]
    async fn create_file(
        &self,
        request: Request<CreateFileRequestProto>,
    ) -> Result<Response<CreateFileResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_or_error!(req, CreateFileResponseProto);
        guard_or_error!(
            self,
            req,
            CreateFileResponseProto,
            self.guard_chain.check_meta_write(&req_ctx)
        );
        let create_mode = CreateModeProto::try_from(req.create_mode).unwrap_or(CreateModeProto::CreateModeUnspecified);
        if create_mode == CreateModeProto::CreateModeUnspecified {
            return error_response!(
                CreateFileResponseProto,
                self.header_from_path_error(
                    &req.header,
                    MetadataError::InvalidArgument("create mode is required".to_string()),
                    None,
                )
            );
        }

        let inode_id = if create_mode == CreateModeProto::CreateOrOverwrite {
            match self.path_resolver.resolve_inode(&req.path) {
                Ok(resolved) => {
                    guard_or_error!(
                        self,
                        req,
                        CreateFileResponseProto,
                        self.guard_chain
                            .check_perm(&req_ctx, PermissionBits::WRITE, &req.path, &resolved)
                    );
                    guard_or_error!(
                        self,
                        req,
                        CreateFileResponseProto,
                        self.guard_chain.check_data_write(&req_ctx, resolved.mount_ctx.mount_id)
                    );
                    match resolved.expect_inode() {
                        Ok(inode_id) => inode_id,
                        Err(err) => {
                            return error_response!(
                                CreateFileResponseProto,
                                self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx))
                            )
                        }
                    }
                }
                Err(_) => {
                    let resolved = match self.path_resolver.resolve_path(&req.path) {
                        Ok(resolved) => resolved,
                        Err(err) => {
                            return error_response!(
                                CreateFileResponseProto,
                                self.header_from_path_error(&req.header, err, None)
                            )
                        }
                    };
                    guard_or_error!(
                        self,
                        req,
                        CreateFileResponseProto,
                        self.guard_chain
                            .check_parent_perm(&req_ctx, PermissionBits::WRITE, &req.path, &resolved)
                    );
                    let attrs = match file_attrs_from_proto(req.attrs) {
                        Ok(attrs) => attrs,
                        Err(err) => {
                            return error_response!(
                                CreateFileResponseProto,
                                self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx))
                            )
                        }
                    };
                    let layout = match file_layout_from_proto(req.layout) {
                        Ok(layout) => layout,
                        Err(err) => {
                            return error_response!(
                                CreateFileResponseProto,
                                self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx))
                            )
                        }
                    };
                    if let Err(err) = validate_active_write_layout(&layout) {
                        return error_response!(
                            CreateFileResponseProto,
                            self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx))
                        );
                    }
                    let parent_inode_id = match resolved.expect_parent() {
                        Ok(parent_inode_id) => parent_inode_id,
                        Err(err) => {
                            return error_response!(
                                CreateFileResponseProto,
                                self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx))
                            )
                        }
                    };
                    let name = match resolved.expect_name() {
                        Ok(name) => name.to_string(),
                        Err(err) => {
                            return error_response!(
                                CreateFileResponseProto,
                                self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx))
                            )
                        }
                    };
                    if let Some(failure) = self.fs_core.preflight_open_write_runtime(
                        &req_ctx,
                        req.desired_len,
                        layout,
                        Some(resolved.mount_ctx.owner_group_name.clone()),
                        Some(resolved.mount_ctx.mount_epoch),
                    ) {
                        tracing::warn!(
                            target: "metadata.state",
                            op = "CreateFile",
                            result = "rejected",
                            error_code = observe::canonical_error_kind(&failure.error),
                            client_id = %req_ctx.caller.client.client_id,
                            call_id = %req_ctx.caller.client.call_id,
                            path = %req.path,
                            parent_inode_id = parent_inode_id.as_raw(),
                            "CreateFile rejected"
                        );
                        return error_response!(CreateFileResponseProto, header_from_core_failure(&req_ctx, &failure));
                    }
                    let create = self
                        .fs_core
                        .execute_create(CreateInput {
                            ctx: req_ctx.clone(),
                            parent_inode_id,
                            name,
                            attrs,
                            layout,
                            freshness: Self::freshness_from_header(&req.header),
                        })
                        .await;
                    match create {
                        Ok(success) => match success.payload.inode_id {
                            Some(inode_id) => inode_id,
                            None => {
                                return error_response!(
                                    CreateFileResponseProto,
                                    self.header_from_path_error(
                                        &req.header,
                                        MetadataError::Internal("create did not return inode_id".to_string()),
                                        Some(&resolved.mount_ctx),
                                    )
                                )
                            }
                        },
                        Err(failure) => {
                            tracing::warn!(
                                target: "metadata.state",
                                op = "CreateFile",
                                result = "rejected",
                                error_code = observe::canonical_error_kind(&failure.error),
                                client_id = %req_ctx.caller.client.client_id,
                                call_id = %req_ctx.caller.client.call_id,
                                path = %req.path,
                                parent_inode_id = parent_inode_id.as_raw(),
                                "CreateFile rejected"
                            );
                            return error_response!(
                                CreateFileResponseProto,
                                header_from_core_failure(&req_ctx, &failure)
                            );
                        }
                    }
                }
            }
        } else {
            let resolved = match self.path_resolver.resolve_path(&req.path) {
                Ok(resolved) => resolved,
                Err(err) => {
                    return error_response!(
                        CreateFileResponseProto,
                        self.header_from_path_error(&req.header, err, None)
                    )
                }
            };
            guard_or_error!(
                self,
                req,
                CreateFileResponseProto,
                self.guard_chain
                    .check_parent_perm(&req_ctx, PermissionBits::WRITE, &req.path, &resolved)
            );
            let attrs = match file_attrs_from_proto(req.attrs) {
                Ok(attrs) => attrs,
                Err(err) => {
                    return error_response!(
                        CreateFileResponseProto,
                        self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx))
                    )
                }
            };
            let layout = match file_layout_from_proto(req.layout) {
                Ok(layout) => layout,
                Err(err) => {
                    return error_response!(
                        CreateFileResponseProto,
                        self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx))
                    )
                }
            };
            if let Err(err) = validate_active_write_layout(&layout) {
                return error_response!(
                    CreateFileResponseProto,
                    self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx))
                );
            }
            let parent_inode_id = match resolved.expect_parent() {
                Ok(parent_inode_id) => parent_inode_id,
                Err(err) => {
                    return error_response!(
                        CreateFileResponseProto,
                        self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx))
                    )
                }
            };
            let name = match resolved.expect_name() {
                Ok(name) => name.to_string(),
                Err(err) => {
                    return error_response!(
                        CreateFileResponseProto,
                        self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx))
                    )
                }
            };
            if let Some(failure) = self.fs_core.preflight_open_write_runtime(
                &req_ctx,
                req.desired_len,
                layout,
                Some(resolved.mount_ctx.owner_group_name.clone()),
                Some(resolved.mount_ctx.mount_epoch),
            ) {
                tracing::warn!(
                    target: "metadata.state",
                    op = "CreateFile",
                    result = "rejected",
                    error_code = observe::canonical_error_kind(&failure.error),
                    client_id = %req_ctx.caller.client.client_id,
                    call_id = %req_ctx.caller.client.call_id,
                    path = %req.path,
                    parent_inode_id = parent_inode_id.as_raw(),
                    "CreateFile rejected"
                );
                return error_response!(CreateFileResponseProto, header_from_core_failure(&req_ctx, &failure));
            }
            match self
                .fs_core
                .execute_create(CreateInput {
                    ctx: req_ctx.clone(),
                    parent_inode_id,
                    name,
                    attrs,
                    layout,
                    freshness: Self::freshness_from_header(&req.header),
                })
                .await
            {
                Ok(success) => match success.payload.inode_id {
                    Some(inode_id) => inode_id,
                    None => {
                        return error_response!(
                            CreateFileResponseProto,
                            self.header_from_path_error(
                                &req.header,
                                MetadataError::Internal("create did not return inode_id".to_string()),
                                Some(&resolved.mount_ctx),
                            )
                        )
                    }
                },
                Err(failure) => {
                    tracing::warn!(
                        target: "metadata.state",
                        op = "CreateFile",
                        result = "rejected",
                        error_code = observe::canonical_error_kind(&failure.error),
                        client_id = %req_ctx.caller.client.client_id,
                        call_id = %req_ctx.caller.client.call_id,
                        path = %req.path,
                        parent_inode_id = parent_inode_id.as_raw(),
                        "CreateFile rejected"
                    );
                    return error_response!(CreateFileResponseProto, header_from_core_failure(&req_ctx, &failure));
                }
            }
        };

        match self
            .fs_core
            .execute_open_write(OpenWriteInput {
                ctx: req_ctx.clone(),
                inode_id,
                desired_len: req.desired_len,
                mode: crate::inode_lease::WriteMode::Write,
                freshness: Self::freshness_from_header(&req.header),
            })
            .await
        {
            Ok(success) => {
                let header = ok_header_from_core_success(&req_ctx, &success);
                let payload = success.payload;
                tracing::info!(
                    target: "metadata.state",
                    op = "CreateFile",
                    result = "committed",
                    error_code = "none",
                    client_id = %req_ctx.caller.client.client_id,
                    call_id = %req_ctx.caller.client.call_id,
                    path = %req.path,
                    inode_id = payload.inode_id.as_raw(),
                    data_handle_id = payload.data_handle_id.as_raw(),
                    file_handle = payload.session_key.file_handle,
                    lease_id = payload.session_key.lease_id.as_raw(),
                    lease_epoch = payload.session_key.lease_epoch,
                    layout_block_size = payload.layout.block_size,
                    layout_chunk_size = payload.layout.chunk_size,
                    replication = payload.layout.replication,
                    desired_len = req.desired_len,
                    mount_epoch = success.mount_epoch,
                    route_epoch = success.route_epoch,
                    "CreateFile committed"
                );
                response_with_header!(
                    CreateFileResponseProto {
                        write_handle: Some(Self::write_handle_from_key(&payload.session_key)),
                        data_handle_id: Some(Self::data_handle_proto(payload.data_handle_id)),
                        base_size: payload.base_size,
                        initial_targets: Vec::new(),
                        expires_at_ms: payload.expires_at_ms,
                        layout: Some((&payload.layout).into()),
                        ..Default::default()
                    },
                    header
                )
            }
            Err(failure) => {
                tracing::warn!(
                    target: "metadata.state",
                    op = "CreateFile",
                    result = "rejected",
                    error_code = observe::canonical_error_kind(&failure.error),
                    client_id = %req_ctx.caller.client.client_id,
                    call_id = %req_ctx.caller.client.call_id,
                    path = %req.path,
                    inode_id = inode_id.as_raw(),
                    "CreateFile rejected"
                );
                error_response!(CreateFileResponseProto, header_from_core_failure(&req_ctx, &failure))
            }
        }
    }

    #[instrument(skip_all)]
    async fn append_file(
        &self,
        request: Request<AppendFileRequestProto>,
    ) -> Result<Response<AppendFileResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_or_error!(req, AppendFileResponseProto);
        guard_or_error!(
            self,
            req,
            AppendFileResponseProto,
            self.guard_chain.check_meta_write(&req_ctx)
        );
        let resolved = match self.path_resolver.resolve_inode(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                return error_response!(
                    AppendFileResponseProto,
                    self.header_from_resolution_error(&req.header, &req.path, err)
                )
            }
        };
        guard_or_error!(
            self,
            req,
            AppendFileResponseProto,
            self.guard_chain
                .check_perm(&req_ctx, PermissionBits::WRITE, &req.path, &resolved)
        );
        guard_or_error!(
            self,
            req,
            AppendFileResponseProto,
            self.guard_chain.check_data_write(&req_ctx, resolved.mount_ctx.mount_id)
        );
        let inode_id = match resolved.expect_inode() {
            Ok(inode_id) => inode_id,
            Err(err) => {
                return error_response!(
                    AppendFileResponseProto,
                    self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx))
                )
            }
        };
        match self
            .fs_core
            .execute_open_write(OpenWriteInput {
                ctx: req_ctx.clone(),
                inode_id,
                desired_len: req.desired_len,
                mode: crate::inode_lease::WriteMode::Append,
                freshness: Self::freshness_from_header(&req.header),
            })
            .await
        {
            Ok(success) => {
                let header = ok_header_from_core_success(&req_ctx, &success);
                let payload = success.payload;
                tracing::info!(
                    target: "metadata.state",
                    op = "AppendFile",
                    result = "opened",
                    error_code = "none",
                    client_id = %req_ctx.caller.client.client_id,
                    call_id = %req_ctx.caller.client.call_id,
                    path = %req.path,
                    inode_id = payload.inode_id.as_raw(),
                    data_handle_id = payload.data_handle_id.as_raw(),
                    file_handle = payload.session_key.file_handle,
                    lease_id = payload.session_key.lease_id.as_raw(),
                    lease_epoch = payload.session_key.lease_epoch,
                    mount_epoch = success.mount_epoch,
                    route_epoch = success.route_epoch,
                    "AppendFile opened"
                );
                response_with_header!(
                    AppendFileResponseProto {
                        write_handle: Some(Self::write_handle_from_key(&payload.session_key)),
                        data_handle_id: Some(Self::data_handle_proto(payload.data_handle_id)),
                        base_size: payload.base_size,
                        initial_targets: Vec::new(),
                        expires_at_ms: payload.expires_at_ms,
                        layout: Some((&payload.layout).into()),
                        ..Default::default()
                    },
                    header
                )
            }
            Err(failure) => {
                tracing::warn!(
                    target: "metadata.state",
                    op = "AppendFile",
                    result = "rejected",
                    error_code = observe::canonical_error_kind(&failure.error),
                    client_id = %req_ctx.caller.client.client_id,
                    call_id = %req_ctx.caller.client.call_id,
                    path = %req.path,
                    inode_id = inode_id.as_raw(),
                    "AppendFile rejected"
                );
                error_response!(AppendFileResponseProto, header_from_core_failure(&req_ctx, &failure))
            }
        }
    }

    #[instrument(skip_all)]
    async fn add_block(
        &self,
        request: Request<AddBlockRequestProto>,
    ) -> Result<Response<AddBlockResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_or_error!(req, AddBlockResponseProto);
        let handle = match Self::write_handle_or_error(&req.header, req.write_handle) {
            Ok(handle) => handle,
            Err(header) => return response_with_header!(AddBlockResponseProto::default(), *header),
        };
        if let Some(session) = self.fs_core.write_session_for_handle(handle.handle_id) {
            guard_or_error!(
                self,
                req,
                AddBlockResponseProto,
                self.guard_chain.check_data_write(&req_ctx, session.mount_id)
            );
        } else {
            guard_or_error!(
                self,
                req,
                AddBlockResponseProto,
                self.guard_chain.check_meta_write(&req_ctx)
            );
        }
        match self
            .fs_core
            .execute_add_block(AddBlockInput {
                ctx: req_ctx.clone(),
                file_handle: handle.handle_id,
                lease_id: lease_id_from_proto(handle.lease_id),
                lease_epoch: handle.lease_epoch,
                open_epoch: handle.open_epoch,
                fencing_token: presented_fencing_from_proto(handle.fencing_token),
                desired_len: req.desired_len,
                freshness: Self::freshness_from_header(&req.header),
            })
            .await
        {
            Ok(success) => {
                let target = &success.payload.target;
                tracing::info!(
                    target: "metadata.block",
                    op = "AddBlock",
                    result = "allocated",
                    error_code = "none",
                    client_id = %req_ctx.caller.client.client_id,
                    call_id = %req_ctx.caller.client.call_id,
                    block_id = %target.block_id,
                    block_index = target.block_id.index.as_raw(),
                    group_id = success.group_name.as_ref().map(|group| group.as_str()),
                    desired_len = req.desired_len,
                    target_count = target.worker_endpoints.len(),
                    targets_sample = ?target.worker_endpoints.iter().take(3).map(|endpoint| endpoint.worker_id.as_raw()).collect::<Vec<_>>(),
                    data_handle_id = target.block_id.data_handle_id.as_raw(),
                    file_handle = handle.handle_id,
                    mount_epoch = success.mount_epoch,
                    route_epoch = success.route_epoch,
                    "AddBlock allocated"
                );
                response_with_header!(
                    AddBlockResponseProto {
                        target: Some(write_target_to_proto(&success.payload.target)),
                        ..Default::default()
                    },
                    ok_header_from_core_success(&req_ctx, &success)
                )
            }
            Err(failure) => {
                tracing::warn!(
                    target: "metadata.block",
                    op = "AddBlock",
                    result = "rejected",
                    error_code = observe::canonical_error_kind(&failure.error),
                    client_id = %req_ctx.caller.client.client_id,
                    call_id = %req_ctx.caller.client.call_id,
                    desired_len = req.desired_len,
                    file_handle = handle.handle_id,
                    lease_epoch = handle.lease_epoch,
                    mount_epoch = failure.mount_epoch,
                    route_epoch = failure.route_epoch,
                    "AddBlock rejected"
                );
                error_response!(AddBlockResponseProto, header_from_core_failure(&req_ctx, &failure))
            }
        }
    }

    #[instrument(skip_all)]
    async fn commit_file(
        &self,
        request: Request<CommitFileRequestProto>,
    ) -> Result<Response<CommitFileResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_or_error!(req, CommitFileResponseProto);
        let handle = match Self::write_handle_or_error(&req.header, req.write_handle) {
            Ok(handle) => handle,
            Err(header) => return response_with_header!(CommitFileResponseProto::default(), *header),
        };
        if let Some(session) = self.fs_core.write_session_for_handle(handle.handle_id) {
            guard_or_error!(
                self,
                req,
                CommitFileResponseProto,
                self.guard_chain.check_data_write(&req_ctx, session.mount_id)
            );
        } else {
            guard_or_error!(
                self,
                req,
                CommitFileResponseProto,
                self.guard_chain.check_meta_write(&req_ctx)
            );
        }
        let data_handle_id = match req.data_handle_id.as_ref() {
            Some(data_handle_id) => data_handle_id.value,
            None => {
                return error_response!(
                    CommitFileResponseProto,
                    self.header_from_path_error(
                        &req.header,
                        MetadataError::InvalidArgument("missing data_handle_id".to_string()),
                        None,
                    )
                )
            }
        };
        let mut committed_blocks = Vec::with_capacity(req.committed_blocks.len());
        for block in req.committed_blocks {
            if block.block_id.as_ref().map(|id| id.data_handle_id) != Some(data_handle_id) {
                return error_response!(
                    CommitFileResponseProto,
                    self.header_from_path_error(
                        &req.header,
                        MetadataError::InvalidArgument(
                            "committed block data_handle_id does not match request".to_string()
                        ),
                        None,
                    )
                );
            }
            match Self::committed_block_from_proto(block) {
                Ok(committed_block) => committed_blocks.push(committed_block),
                Err(err) => {
                    return error_response!(
                        CommitFileResponseProto,
                        self.header_from_path_error(&req.header, err, None)
                    )
                }
            }
        }
        let final_size = req.final_size;
        let committed_block_count = committed_blocks.len();
        let committed_bytes: u64 = committed_blocks.iter().map(|block| block.len).sum();
        let lease_id = lease_id_from_proto(handle.lease_id);
        let lease_id_value = lease_id.map(|lease_id| lease_id.as_raw());
        match self
            .fs_core
            .execute_close_write(CloseWriteInput {
                ctx: req_ctx.clone(),
                file_handle: handle.handle_id,
                lease_id,
                lease_epoch: handle.lease_epoch,
                open_epoch: handle.open_epoch,
                fencing_token: presented_fencing_from_proto(handle.fencing_token),
                intent: CloseWriteIntent {
                    committed_blocks,
                    final_size,
                },
                freshness: Self::freshness_from_header(&req.header),
            })
            .await
        {
            Ok(success) => {
                tracing::info!(
                    target: "metadata.state",
                    op = "CommitFile",
                    result = "committed",
                    error_code = "none",
                    client_id = %req_ctx.caller.client.client_id,
                    call_id = %req_ctx.caller.client.call_id,
                    data_handle_id,
                    file_handle = handle.handle_id,
                    final_size,
                    committed_block_count,
                    committed_bytes,
                    lease_id = lease_id_value,
                    lease_epoch = handle.lease_epoch,
                    file_version = success.payload.file_version,
                    mount_epoch = success.mount_epoch,
                    route_epoch = success.route_epoch,
                    "CommitFile committed"
                );
                response_with_header!(
                    CommitFileResponseProto {
                        committed_size: success.payload.committed_size,
                        file_version: success.payload.file_version,
                        ..Default::default()
                    },
                    ok_header_from_core_success(&req_ctx, &success)
                )
            }
            Err(failure) => {
                tracing::warn!(
                    target: "metadata.state",
                    op = "CommitFile",
                    result = "rejected",
                    error_code = observe::canonical_error_kind(&failure.error),
                    client_id = %req_ctx.caller.client.client_id,
                    call_id = %req_ctx.caller.client.call_id,
                    data_handle_id,
                    file_handle = handle.handle_id,
                    final_size,
                    committed_block_count,
                    committed_bytes,
                    lease_id = lease_id_value,
                    lease_epoch = handle.lease_epoch,
                    mount_epoch = failure.mount_epoch,
                    route_epoch = failure.route_epoch,
                    "CommitFile rejected"
                );
                error_response!(CommitFileResponseProto, header_from_core_failure(&req_ctx, &failure))
            }
        }
    }

    #[instrument(skip_all)]
    async fn abort_file_write(
        &self,
        request: Request<AbortFileWriteRequestProto>,
    ) -> Result<Response<AbortFileWriteResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_or_error!(req, AbortFileWriteResponseProto);
        let handle = match Self::write_handle_or_error(&req.header, req.write_handle) {
            Ok(handle) => handle,
            Err(header) => return response_with_header!(AbortFileWriteResponseProto::default(), *header),
        };
        if let Some(session) = self.fs_core.write_session_for_handle(handle.handle_id) {
            guard_or_error!(
                self,
                req,
                AbortFileWriteResponseProto,
                self.guard_chain.check_data_write(&req_ctx, session.mount_id)
            );
        } else {
            guard_or_error!(
                self,
                req,
                AbortFileWriteResponseProto,
                self.guard_chain.check_meta_write(&req_ctx)
            );
        }
        let lease_id = lease_id_from_proto(handle.lease_id);
        let lease_id_value = lease_id.map(|lease_id| lease_id.as_raw());
        match self
            .fs_core
            .execute_abort_write(AbortWriteInput {
                ctx: req_ctx.clone(),
                file_handle: handle.handle_id,
                lease_id,
                lease_epoch: handle.lease_epoch,
                open_epoch: handle.open_epoch,
                fencing_token: presented_fencing_from_proto(handle.fencing_token),
                freshness: Self::freshness_from_header(&req.header),
            })
            .await
        {
            Ok(success) => {
                tracing::info!(
                    target: "metadata.state",
                    op = "AbortFileWrite",
                    result = "completed",
                    error_code = "none",
                    client_id = %req_ctx.caller.client.client_id,
                    call_id = %req_ctx.caller.client.call_id,
                    file_handle = handle.handle_id,
                    lease_id = lease_id_value,
                    lease_epoch = handle.lease_epoch,
                    mount_epoch = success.mount_epoch,
                    route_epoch = success.route_epoch,
                    "AbortFileWrite completed"
                );
                response_with_header!(
                    AbortFileWriteResponseProto::default(),
                    ok_header_from_core_success(&req_ctx, &success)
                )
            }
            Err(failure) => {
                tracing::warn!(
                    target: "metadata.state",
                    op = "AbortFileWrite",
                    result = "rejected",
                    error_code = observe::canonical_error_kind(&failure.error),
                    client_id = %req_ctx.caller.client.client_id,
                    call_id = %req_ctx.caller.client.call_id,
                    file_handle = handle.handle_id,
                    lease_id = lease_id_value,
                    lease_epoch = handle.lease_epoch,
                    mount_epoch = failure.mount_epoch,
                    route_epoch = failure.route_epoch,
                    "AbortFileWrite rejected"
                );
                response_with_header!(
                    AbortFileWriteResponseProto::default(),
                    header_from_core_failure(&req_ctx, &failure)
                )
            }
        }
    }

    #[instrument(skip_all)]
    async fn renew_lease(
        &self,
        request: Request<RenewLeaseRequestProto>,
    ) -> Result<Response<RenewLeaseResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_or_error!(req, RenewLeaseResponseProto);
        let handle = match Self::write_handle_or_error(&req.header, req.write_handle) {
            Ok(handle) => handle,
            Err(header) => return response_with_header!(RenewLeaseResponseProto::default(), *header),
        };
        if let Some(session) = self.fs_core.write_session_for_handle(handle.handle_id) {
            guard_or_error!(
                self,
                req,
                RenewLeaseResponseProto,
                self.guard_chain.check_data_write(&req_ctx, session.mount_id)
            );
        } else {
            guard_or_error!(
                self,
                req,
                RenewLeaseResponseProto,
                self.guard_chain.check_meta_write(&req_ctx)
            );
        }
        let lease_id = lease_id_from_proto(handle.lease_id);
        let lease_id_value = lease_id.map(|lease_id| lease_id.as_raw());
        match self
            .fs_core
            .execute_renew_inode_lease(RenewLeaseInput {
                ctx: req_ctx.clone(),
                file_handle: handle.handle_id,
                lease_id,
                lease_epoch: handle.lease_epoch,
                open_epoch: handle.open_epoch,
                fencing_token: presented_fencing_from_proto(handle.fencing_token),
                freshness: Self::freshness_from_header(&req.header),
            })
            .await
        {
            Ok(success) => {
                tracing::info!(
                    target: "metadata.state",
                    op = "RenewLease",
                    result = "completed",
                    error_code = "none",
                    client_id = %req_ctx.caller.client.client_id,
                    call_id = %req_ctx.caller.client.call_id,
                    file_handle = handle.handle_id,
                    lease_id = lease_id_value,
                    lease_epoch = handle.lease_epoch,
                    mount_epoch = success.mount_epoch,
                    route_epoch = success.route_epoch,
                    "RenewLease completed"
                );
                response_with_header!(
                    RenewLeaseResponseProto {
                        expires_at_ms: success.payload.expires_at_ms,
                        ..Default::default()
                    },
                    ok_header_from_core_success(&req_ctx, &success)
                )
            }
            Err(failure) => {
                tracing::warn!(
                    target: "metadata.state",
                    op = "RenewLease",
                    result = "rejected",
                    error_code = observe::canonical_error_kind(&failure.error),
                    client_id = %req_ctx.caller.client.client_id,
                    call_id = %req_ctx.caller.client.call_id,
                    file_handle = handle.handle_id,
                    lease_id = lease_id_value,
                    lease_epoch = handle.lease_epoch,
                    mount_epoch = failure.mount_epoch,
                    route_epoch = failure.route_epoch,
                    "RenewLease rejected"
                );
                error_response!(RenewLeaseResponseProto, header_from_core_failure(&req_ctx, &failure))
            }
        }
    }

    #[instrument(skip_all)]
    async fn sync_write(
        &self,
        request: Request<SyncWriteRequestProto>,
    ) -> Result<Response<SyncWriteResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_or_error!(req, SyncWriteResponseProto);
        let handle = match Self::write_handle_or_error(&req.header, req.write_handle) {
            Ok(handle) => handle,
            Err(header) => return response_with_header!(SyncWriteResponseProto::default(), *header),
        };
        if let Some(session) = self.fs_core.write_session_for_handle(handle.handle_id) {
            guard_or_error!(
                self,
                req,
                SyncWriteResponseProto,
                self.guard_chain.check_data_write(&req_ctx, session.mount_id)
            );
        } else {
            guard_or_error!(
                self,
                req,
                SyncWriteResponseProto,
                self.guard_chain.check_meta_write(&req_ctx)
            );
        }
        let data_handle_id = match req.data_handle_id.as_ref() {
            Some(data_handle_id) => DataHandleId::new(data_handle_id.value),
            None => {
                return error_response!(
                    SyncWriteResponseProto,
                    self.header_from_path_error(
                        &req.header,
                        MetadataError::InvalidArgument("missing data_handle_id".to_string()),
                        None,
                    )
                )
            }
        };
        let mode = match WriteSyncModeProto::try_from(req.mode) {
            Ok(WriteSyncModeProto::WriteSyncModeVisibility) => SyncWriteMode::Visibility,
            Ok(WriteSyncModeProto::WriteSyncModeDurability) => SyncWriteMode::Durability,
            Ok(WriteSyncModeProto::WriteSyncModeUnspecified) | Err(_) => {
                return error_response!(
                    SyncWriteResponseProto,
                    self.header_from_path_error(
                        &req.header,
                        MetadataError::InvalidArgument("SyncWrite mode must be visibility or durability".to_string()),
                        None,
                    )
                )
            }
        };
        let mut committed_blocks = Vec::with_capacity(req.committed_blocks.len());
        for block in req.committed_blocks {
            if block.block_id.as_ref().map(|id| id.data_handle_id) != Some(data_handle_id.as_raw()) {
                return error_response!(
                    SyncWriteResponseProto,
                    self.header_from_path_error(
                        &req.header,
                        MetadataError::InvalidArgument(
                            "committed block data_handle_id does not match request".to_string()
                        ),
                        None,
                    )
                );
            }
            match Self::committed_block_from_proto(block) {
                Ok(committed_block) => committed_blocks.push(committed_block),
                Err(err) => {
                    return error_response!(
                        SyncWriteResponseProto,
                        self.header_from_path_error(&req.header, err, None)
                    )
                }
            }
        }
        match self
            .fs_core
            .execute_sync_write(SyncWriteInput {
                ctx: req_ctx.clone(),
                file_handle: handle.handle_id,
                lease_id: lease_id_from_proto(handle.lease_id),
                lease_epoch: handle.lease_epoch,
                open_epoch: handle.open_epoch,
                fencing_token: presented_fencing_from_proto(handle.fencing_token),
                data_handle_id,
                committed_blocks,
                target_size: req.target_size,
                flags: req.flags,
                mode,
                freshness: Self::freshness_from_header(&req.header),
            })
            .await
        {
            Ok(success) => response_with_header!(
                SyncWriteResponseProto {
                    synced_size: success.payload.synced_size,
                    file_version: success.payload.file_version,
                    ..Default::default()
                },
                ok_header_from_core_success(&req_ctx, &success)
            ),
            Err(failure) => error_response!(SyncWriteResponseProto, header_from_core_failure(&req_ctx, &failure)),
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn path_service_does_not_branch_on_authz_scheme() {
        let source = include_str!("path_service.rs");

        assert!(!source.contains(concat!("Authz", "Scheme")));
        assert!(!source.contains(concat!("authz", "_targets_for_")));
        assert!(!source.contains(concat!("traverse", "_pre_checks")));
        assert!(!source.contains(concat!("sticky", "_pre_checks")));
    }
}
