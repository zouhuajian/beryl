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
    CloseWriteInput, CloseWriteIntent, CreateInput, FileRange, Freshness, FsyncBarrierInput, GetAttrInput,
    GetFileLayoutInput, GetXattrInput, ListXattrInput, MkdirInput, OpenInput, OpenWriteInput, ReadDirInput,
    ReleaseSessionInput, RemoveXattrInput, RenameInput, RenewLeaseInput, RmdirInput, SetXattrInput, TruncateInput,
    UnlinkInput,
};
use super::guard::{GuardChain, GuardFailure, LeadershipChecker};
use super::{
    extent_from_proto, extent_to_proto, fencing_to_proto, file_attrs_from_proto, file_attrs_to_proto,
    file_layout_from_proto, header_from_canonical_error, header_from_core_failure, lease_id_from_proto,
    lease_id_to_proto, location_to_proto, need_refresh_header, ok_header_from_core_success, ok_header_from_request,
    presented_fencing_from_proto, request_context_from_proto, write_target_to_proto,
};
use super::{FsCore, PermissionBits, PermissionChecker, SharedWorkerCommitHook};
use crate::error::{to_canonical_fs, MetadataError};
use crate::mount::MountTable;
use crate::path_resolver::{MountContext, PathResolver};
use crate::raft::RocksDBStorage;
use proto::metadata::file_system_service_proto_server::FileSystemServiceProto;
use proto::metadata::*;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::instrument;

/// FileSystemServiceProto implementation.
pub struct MetadataFileSystemServiceImpl {
    path_resolver: PathResolver,
    fs_core: Arc<FsCore>,
    guard_chain: GuardChain,
    _metrics: Option<Arc<crate::metrics::MetadataMetrics>>,
}

macro_rules! response_with_header {
    ($resp:expr, $header:expr) => {{
        let mut resp = $resp;
        resp.header = Some($header);
        Ok(Response::new(resp))
    }};
}

macro_rules! error_response {
    ($resp_ty:ty, $header:expr) => {{
        response_with_header!(<$resp_ty>::default(), $header)
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

        Self {
            path_resolver,
            fs_core,
            guard_chain,
            _metrics: metrics,
        }
    }

    fn header_from_path_error(
        &self,
        req_header: &Option<proto::common::RequestHeaderProto>,
        err: MetadataError,
        mount_ctx: Option<&MountContext>,
    ) -> proto::common::ResponseHeaderProto {
        let (group_id, mount_epoch) = mount_ctx
            .map(|ctx| (Some(ctx.owner_group_id.as_raw()), Some(ctx.mount_epoch)))
            .unwrap_or((None, None));

        let canonical = to_canonical_fs(err);
        header_from_canonical_error(req_header, group_id, mount_epoch, &canonical)
    }

    fn header_from_guard_failure(
        &self,
        req_header: &Option<proto::common::RequestHeaderProto>,
        failure: GuardFailure,
    ) -> proto::common::ResponseHeaderProto {
        header_from_canonical_error(req_header, failure.group_id, failure.mount_epoch, &failure.err)
    }
}

#[tonic::async_trait]
impl FileSystemServiceProto for MetadataFileSystemServiceImpl {
    #[instrument(skip(self), fields(call_id, client_id))]
    async fn get_file_status(
        &self,
        request: Request<GetFileStatusRequestProto>,
    ) -> Result<Response<GetFileStatusResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);
        guard_or_error!(
            self,
            req,
            GetFileStatusResponseProto,
            self.guard_chain.check_meta_read(&req_ctx)
        );

        // Resolve path to inode.
        let resolved = match self.path_resolver.resolve_inode(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(GetFileStatusResponseProto, resp_header);
            }
        };
        guard_or_error!(
            self,
            req,
            GetFileStatusResponseProto,
            self.guard_chain
                .check_perm(&req_ctx, PermissionBits::READ, &req.path, &resolved)
        );
        let inode_id = match resolved.expect_inode() {
            Ok(inode_id) => inode_id,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
                return error_response!(GetFileStatusResponseProto, resp_header);
            }
        };

        match self
            .fs_core
            .execute_get_attr(GetAttrInput {
                ctx: req_ctx.clone(),
                inode_id,
            })
            .await
        {
            Ok(success) => {
                let header = ok_header_from_core_success(&req_ctx, &success);
                response_with_header!(
                    GetFileStatusResponseProto {
                        inode_id: Some(proto::fs::InodeIdProto {
                            value: inode_id.as_raw(),
                        }),
                        attrs: Some(file_attrs_to_proto(&success.payload.attrs)),
                        inode: None,
                        ..Default::default()
                    },
                    header
                )
            }
            Err(failure) => {
                let header = header_from_core_failure(&req_ctx, &failure);
                error_response!(GetFileStatusResponseProto, header)
            }
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn mkdir(&self, request: Request<MkdirPathRequestProto>) -> Result<Response<MkdirPathResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);
        guard_or_error!(
            self,
            req,
            MkdirPathResponseProto,
            self.guard_chain.check_meta_write(&req_ctx)
        );

        // Resolve path
        let resolved = match self.path_resolver.resolve_path(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(MkdirPathResponseProto, resp_header);
            }
        };
        guard_or_error!(
            self,
            req,
            MkdirPathResponseProto,
            self.guard_chain
                .check_parent_perm(&req_ctx, PermissionBits::WRITE, &req.path, &resolved)
        );
        let parent_inode_id = match resolved.expect_parent() {
            Ok(parent_inode_id) => parent_inode_id,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
                return error_response!(MkdirPathResponseProto, resp_header);
            }
        };
        let name = match resolved.expect_name() {
            Ok(name) => name.to_string(),
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
                return error_response!(MkdirPathResponseProto, resp_header);
            }
        };

        // Convert attrs
        let attrs = match file_attrs_from_proto(req.attrs) {
            Ok(attrs) => attrs,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
                return error_response!(MkdirPathResponseProto, resp_header);
            }
        };

        let freshness = Freshness {
            mount_epoch: req.header.as_ref().and_then(|h| h.mount_epoch),
            route_epoch: req.header.as_ref().and_then(|h| h.route_epoch),
            worker_epoch: None,
        };
        match self
            .fs_core
            .execute_mkdir(MkdirInput {
                ctx: req_ctx.clone(),
                parent_inode_id,
                name,
                attrs,
                freshness,
            })
            .await
        {
            Ok(success) => {
                let header = ok_header_from_core_success(&req_ctx, &success);
                let payload = success.payload;
                let attrs_proto = payload.attrs.as_ref().map(file_attrs_to_proto);
                let inode = payload.inode_id.map(|inode_id| proto::fs::InodeProto {
                    inode_id: Some(proto::fs::InodeIdProto {
                        value: inode_id.as_raw(),
                    }),
                    kind: proto::fs::InodeKindProto::InodeKindDir as i32,
                    attrs: attrs_proto.clone(),
                    mount_id: Some(proto::common::MountIdProto {
                        value: resolved.mount_ctx.mount_id.as_raw(),
                    }),
                    ..Default::default()
                });
                response_with_header!(
                    MkdirPathResponseProto {
                        inode,
                        attrs: attrs_proto,
                        ..Default::default()
                    },
                    header
                )
            }
            Err(failure) => {
                let header = header_from_core_failure(&req_ctx, &failure);
                error_response!(MkdirPathResponseProto, header)
            }
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn create(
        &self,
        request: Request<CreatePathRequestProto>,
    ) -> Result<Response<CreatePathResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);
        guard_or_error!(
            self,
            req,
            CreatePathResponseProto,
            self.guard_chain.check_meta_write(&req_ctx)
        );

        // Resolve path
        let resolved = match self.path_resolver.resolve_path(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(CreatePathResponseProto, resp_header);
            }
        };
        guard_or_error!(
            self,
            req,
            CreatePathResponseProto,
            self.guard_chain
                .check_parent_perm(&req_ctx, PermissionBits::WRITE, &req.path, &resolved)
        );
        let parent_inode_id = match resolved.expect_parent() {
            Ok(parent_inode_id) => parent_inode_id,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
                return error_response!(CreatePathResponseProto, resp_header);
            }
        };
        let name = match resolved.expect_name() {
            Ok(name) => name.to_string(),
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
                return error_response!(CreatePathResponseProto, resp_header);
            }
        };

        // Convert attrs and layout
        let attrs = match file_attrs_from_proto(req.attrs) {
            Ok(attrs) => attrs,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
                return error_response!(CreatePathResponseProto, resp_header);
            }
        };
        let layout = match file_layout_from_proto(req.layout) {
            Ok(layout) => layout,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
                return error_response!(CreatePathResponseProto, resp_header);
            }
        };

        let freshness = Freshness {
            mount_epoch: req.header.as_ref().and_then(|h| h.mount_epoch),
            route_epoch: req.header.as_ref().and_then(|h| h.route_epoch),
            worker_epoch: None,
        };
        match self
            .fs_core
            .execute_create(CreateInput {
                ctx: req_ctx.clone(),
                parent_inode_id,
                name,
                attrs,
                layout,
                freshness,
            })
            .await
        {
            Ok(success) => {
                let header = ok_header_from_core_success(&req_ctx, &success);
                let payload = success.payload;
                let attrs_proto = payload.attrs.as_ref().map(file_attrs_to_proto);
                let inode_id = payload.inode_id.map(|inode_id| proto::fs::InodeIdProto {
                    value: inode_id.as_raw(),
                });
                let inode = payload.inode_id.map(|inode_id| proto::fs::InodeProto {
                    inode_id: Some(proto::fs::InodeIdProto {
                        value: inode_id.as_raw(),
                    }),
                    kind: proto::fs::InodeKindProto::InodeKindFile as i32,
                    attrs: attrs_proto.clone(),
                    mount_id: Some(proto::common::MountIdProto {
                        value: resolved.mount_ctx.mount_id.as_raw(),
                    }),
                    ..Default::default()
                });
                response_with_header!(
                    CreatePathResponseProto {
                        inode_id,
                        inode,
                        attrs: attrs_proto,
                        ..Default::default()
                    },
                    header
                )
            }
            Err(failure) => {
                let header = header_from_core_failure(&req_ctx, &failure);
                error_response!(CreatePathResponseProto, header)
            }
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn unlink(
        &self,
        request: Request<UnlinkPathRequestProto>,
    ) -> Result<Response<UnlinkPathResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);
        guard_or_error!(
            self,
            req,
            UnlinkPathResponseProto,
            self.guard_chain.check_meta_write(&req_ctx)
        );

        // Resolve path
        let resolved = match self.path_resolver.resolve_path(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(UnlinkPathResponseProto, resp_header);
            }
        };
        guard_or_error!(
            self,
            req,
            UnlinkPathResponseProto,
            self.guard_chain
                .check_parent_perm(&req_ctx, PermissionBits::WRITE, &req.path, &resolved)
        );
        let parent_inode_id = match resolved.expect_parent() {
            Ok(parent_inode_id) => parent_inode_id,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
                return error_response!(UnlinkPathResponseProto, resp_header);
            }
        };
        let name = match resolved.expect_name() {
            Ok(name) => name.to_string(),
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
                return error_response!(UnlinkPathResponseProto, resp_header);
            }
        };

        let freshness = Freshness {
            mount_epoch: req.header.as_ref().and_then(|h| h.mount_epoch),
            route_epoch: req.header.as_ref().and_then(|h| h.route_epoch),
            worker_epoch: None,
        };
        match self
            .fs_core
            .execute_unlink(UnlinkInput {
                ctx: req_ctx.clone(),
                parent_inode_id,
                name,
                freshness,
            })
            .await
        {
            Ok(success) => {
                response_with_header!(
                    UnlinkPathResponseProto::default(),
                    ok_header_from_core_success(&req_ctx, &success)
                )
            }
            Err(failure) => {
                let header = header_from_core_failure(&req_ctx, &failure);
                error_response!(UnlinkPathResponseProto, header)
            }
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn rmdir(&self, request: Request<RmdirPathRequestProto>) -> Result<Response<RmdirPathResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);
        guard_or_error!(
            self,
            req,
            RmdirPathResponseProto,
            self.guard_chain.check_meta_write(&req_ctx)
        );

        // Resolve path
        let resolved = match self.path_resolver.resolve_path(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(RmdirPathResponseProto, resp_header);
            }
        };
        guard_or_error!(
            self,
            req,
            RmdirPathResponseProto,
            self.guard_chain
                .check_parent_perm(&req_ctx, PermissionBits::WRITE, &req.path, &resolved)
        );
        let parent_inode_id = match resolved.expect_parent() {
            Ok(parent_inode_id) => parent_inode_id,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
                return error_response!(RmdirPathResponseProto, resp_header);
            }
        };
        let name = match resolved.expect_name() {
            Ok(name) => name.to_string(),
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
                return error_response!(RmdirPathResponseProto, resp_header);
            }
        };

        let freshness = Freshness {
            mount_epoch: req.header.as_ref().and_then(|h| h.mount_epoch),
            route_epoch: req.header.as_ref().and_then(|h| h.route_epoch),
            worker_epoch: None,
        };
        match self
            .fs_core
            .execute_rmdir(RmdirInput {
                ctx: req_ctx.clone(),
                parent_inode_id,
                name,
                freshness,
            })
            .await
        {
            Ok(success) => {
                response_with_header!(
                    RmdirPathResponseProto::default(),
                    ok_header_from_core_success(&req_ctx, &success)
                )
            }
            Err(failure) => {
                let header = header_from_core_failure(&req_ctx, &failure);
                error_response!(RmdirPathResponseProto, header)
            }
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn rename(
        &self,
        request: Request<RenamePathRequestProto>,
    ) -> Result<Response<RenamePathResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);
        guard_or_error!(
            self,
            req,
            RenamePathResponseProto,
            self.guard_chain.check_meta_write(&req_ctx)
        );

        // Resolve both paths
        let (src_resolved, dst_resolved) = match self.path_resolver.resolve_rename(&req.src_path, &req.dst_path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(RenamePathResponseProto, resp_header);
            }
        };
        guard_or_error!(
            self,
            req,
            RenamePathResponseProto,
            self.guard_chain
                .check_parent_perm(&req_ctx, PermissionBits::WRITE, &req.src_path, &src_resolved)
        );
        guard_or_error!(
            self,
            req,
            RenamePathResponseProto,
            self.guard_chain
                .check_parent_perm(&req_ctx, PermissionBits::WRITE, &req.dst_path, &dst_resolved)
        );
        let src_parent_inode_id = match src_resolved.expect_parent() {
            Ok(parent_inode_id) => parent_inode_id,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, Some(&src_resolved.mount_ctx));
                return error_response!(RenamePathResponseProto, resp_header);
            }
        };
        let src_name = match src_resolved.expect_name() {
            Ok(name) => name.to_string(),
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, Some(&src_resolved.mount_ctx));
                return error_response!(RenamePathResponseProto, resp_header);
            }
        };
        let dst_parent_inode_id = match dst_resolved.expect_parent() {
            Ok(parent_inode_id) => parent_inode_id,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, Some(&dst_resolved.mount_ctx));
                return error_response!(RenamePathResponseProto, resp_header);
            }
        };
        let dst_name = match dst_resolved.expect_name() {
            Ok(name) => name.to_string(),
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, Some(&dst_resolved.mount_ctx));
                return error_response!(RenamePathResponseProto, resp_header);
            }
        };

        let freshness = Freshness {
            mount_epoch: req.header.as_ref().and_then(|h| h.mount_epoch),
            route_epoch: req.header.as_ref().and_then(|h| h.route_epoch),
            worker_epoch: None,
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
                freshness,
            })
            .await
        {
            Ok(success) => {
                response_with_header!(
                    RenamePathResponseProto::default(),
                    ok_header_from_core_success(&req_ctx, &success)
                )
            }
            Err(failure) => {
                let header = header_from_core_failure(&req_ctx, &failure);
                error_response!(RenamePathResponseProto, header)
            }
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn list_status(
        &self,
        request: Request<ListStatusPathRequestProto>,
    ) -> Result<Response<ListStatusPathResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);
        guard_or_error!(
            self,
            req,
            ListStatusPathResponseProto,
            self.guard_chain.check_meta_read(&req_ctx)
        );

        // Resolve path to inode
        let resolved = match self.path_resolver.resolve_inode(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(ListStatusPathResponseProto, resp_header);
            }
        };

        if !req.recursive {
            guard_or_error!(
                self,
                req,
                ListStatusPathResponseProto,
                self.guard_chain
                    .check_perm(&req_ctx, PermissionBits::READ, &req.path, &resolved)
            );
            let inode_id = match resolved.expect_inode() {
                Ok(inode_id) => inode_id,
                Err(err) => {
                    let resp_header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
                    return error_response!(ListStatusPathResponseProto, resp_header);
                }
            };

            let cursor_key = if req.cursor.is_empty() {
                None
            } else {
                Some(req.cursor.clone())
            };
            let max_entries = if req.limit == 0 { None } else { Some(req.limit as usize) };
            match self
                .fs_core
                .execute_read_dir(ReadDirInput {
                    ctx: req_ctx.clone(),
                    parent_inode_id: inode_id,
                    cursor_key,
                    max_entries,
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
                            inode_id: Some(proto::fs::InodeIdProto {
                                value: entry.inode_id.as_raw(),
                            }),
                            kind: match entry.kind {
                                Some(types::fs::InodeKind::File) => proto::fs::InodeKindProto::InodeKindFile as i32,
                                Some(types::fs::InodeKind::Dir) => proto::fs::InodeKindProto::InodeKindDir as i32,
                                Some(types::fs::InodeKind::Symlink) => {
                                    proto::fs::InodeKindProto::InodeKindSymlink as i32
                                }
                                None => proto::fs::InodeKindProto::InodeKindUnspecified as i32,
                            },
                            attrs: entry.attrs.as_ref().map(file_attrs_to_proto),
                        })
                        .collect();
                    response_with_header!(
                        ListStatusPathResponseProto {
                            entries,
                            next_cursor: payload.next_cursor_key,
                            eof: payload.eof,
                            ..Default::default()
                        },
                        header
                    )
                }
                Err(failure) => {
                    let header = header_from_core_failure(&req_ctx, &failure);
                    error_response!(ListStatusPathResponseProto, header)
                }
            }
        } else {
            // Recursive listing: TODO implement BFS/DFS with hard limits
            let resp_header = self.header_from_path_error(
                &req.header,
                MetadataError::NotSupported("Recursive listing not yet implemented".to_string()),
                None,
            );
            error_response!(ListStatusPathResponseProto, resp_header)
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn open(&self, request: Request<OpenPathRequestProto>) -> Result<Response<OpenPathResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);
        guard_or_error!(
            self,
            req,
            OpenPathResponseProto,
            self.guard_chain.check_meta_read(&req_ctx)
        );

        // Resolve path to inode
        let resolved = match self.path_resolver.resolve_inode(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(OpenPathResponseProto, resp_header);
            }
        };
        guard_or_error!(
            self,
            req,
            OpenPathResponseProto,
            self.guard_chain
                .check_perm(&req_ctx, PermissionBits::READ, &req.path, &resolved)
        );
        let inode_id = match resolved.expect_inode() {
            Ok(inode_id) => inode_id,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
                return error_response!(OpenPathResponseProto, resp_header);
            }
        };

        match self
            .fs_core
            .execute_open(OpenInput {
                ctx: req_ctx.clone(),
                inode_id,
                flags: req.flags as i32,
            })
            .await
        {
            Ok(success) => {
                let header = ok_header_from_core_success(&req_ctx, &success);
                response_with_header!(
                    OpenPathResponseProto {
                        file_handle: success.payload.file_handle,
                        ..Default::default()
                    },
                    header
                )
            }
            Err(failure) => {
                let header = header_from_core_failure(&req_ctx, &failure);
                error_response!(OpenPathResponseProto, header)
            }
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn release(
        &self,
        request: Request<ReleasePathRequestProto>,
    ) -> Result<Response<ReleasePathResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);
        let session = self.fs_core.write_session_for_handle(req.file_handle);
        if let Some(session) = session.as_ref() {
            guard_or_error!(
                self,
                req,
                ReleasePathResponseProto,
                self.guard_chain.check_data_write(&req_ctx, session.mount_id)
            );
        } else if let Err(failure) = self.guard_chain.check_meta_read(&req_ctx).await {
            let resp_header = self.header_from_guard_failure(&req.header, failure);
            return response_with_header!(ReleasePathResponseProto::default(), resp_header);
        }

        match self
            .fs_core
            .execute_release(ReleaseSessionInput {
                ctx: req_ctx.clone(),
                file_handle: req.file_handle,
            })
            .await
        {
            Ok(success) => {
                response_with_header!(
                    ReleasePathResponseProto::default(),
                    ok_header_from_core_success(&req_ctx, &success)
                )
            }
            Err(failure) => {
                let header = header_from_core_failure(&req_ctx, &failure);
                response_with_header!(ReleasePathResponseProto::default(), header)
            }
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn fsync(&self, request: Request<FsyncPathRequestProto>) -> Result<Response<FsyncPathResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);
        guard_or_error!(
            self,
            req,
            FsyncPathResponseProto,
            self.guard_chain.check_meta_write(&req_ctx)
        );

        // Handle path-based or handle-based fsync
        match req.target {
            Some(proto::metadata::fsync_path_request_proto::Target::Path(path)) => {
                // Resolve path to inode
                let resolved = match self.path_resolver.resolve_inode(&path) {
                    Ok(resolved) => resolved,
                    Err(err) => {
                        let resp_header = self.header_from_path_error(&req.header, err, None);
                        return error_response!(FsyncPathResponseProto, resp_header);
                    }
                };
                guard_or_error!(
                    self,
                    req,
                    FsyncPathResponseProto,
                    self.guard_chain
                        .check_perm(&req_ctx, PermissionBits::WRITE, &path, &resolved)
                );
                guard_or_error!(
                    self,
                    req,
                    FsyncPathResponseProto,
                    self.guard_chain.check_data_write(&req_ctx, resolved.mount_ctx.mount_id)
                );
                let inode_id = match resolved.expect_inode() {
                    Ok(inode_id) => inode_id,
                    Err(err) => {
                        let resp_header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
                        return error_response!(FsyncPathResponseProto, resp_header);
                    }
                };

                let freshness = Freshness {
                    mount_epoch: req.header.as_ref().and_then(|h| h.mount_epoch),
                    route_epoch: req.header.as_ref().and_then(|h| h.route_epoch),
                    worker_epoch: None,
                };
                match self
                    .fs_core
                    .execute_fsync(FsyncBarrierInput {
                        ctx: req_ctx.clone(),
                        inode_id,
                        file_handle: None,
                        lease_id: None,
                        lease_epoch: None,
                        fencing_token: None,
                        target_size: None,
                        flags: req.flags as i32,
                        freshness,
                    })
                    .await
                {
                    Ok(success) => {
                        return response_with_header!(
                            FsyncPathResponseProto::default(),
                            ok_header_from_core_success(&req_ctx, &success)
                        );
                    }
                    Err(failure) => {
                        let header = header_from_core_failure(&req_ctx, &failure);
                        return response_with_header!(FsyncPathResponseProto::default(), header);
                    }
                }
            }
            Some(proto::metadata::fsync_path_request_proto::Target::FileHandle(_handle)) => {
                // Handle-based fsync: TODO implement when FS service supports it
                let resp_header = self.header_from_path_error(
                    &req.header,
                    MetadataError::NotSupported("Handle-based fsync not yet implemented".to_string()),
                    None,
                );
                return error_response!(FsyncPathResponseProto, resp_header);
            }
            None => {
                let resp_header = self.header_from_path_error(
                    &req.header,
                    MetadataError::InvalidArgument("Either path or file_handle must be provided".to_string()),
                    None,
                );
                return error_response!(FsyncPathResponseProto, resp_header);
            }
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn hsync(&self, request: Request<HsyncPathRequestProto>) -> Result<Response<HsyncPathResponseProto>, Status> {
        let req = request.into_inner();
        let inner = match req.fsync {
            Some(inner) => inner,
            None => {
                let resp_header = self.header_from_path_error(
                    &None,
                    MetadataError::InvalidArgument("missing fsync".to_string()),
                    None,
                );
                return error_response!(HsyncPathResponseProto, resp_header);
            }
        };
        let fallback_header = inner.header.clone();
        let resp = self.fsync(Request::new(inner)).await?;
        response_with_header!(
            HsyncPathResponseProto::default(),
            resp.into_inner()
                .header
                .unwrap_or_else(|| ok_header_from_request(&fallback_header, None, None))
        )
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn hflush(
        &self,
        request: Request<HflushPathRequestProto>,
    ) -> Result<Response<HflushPathResponseProto>, Status> {
        let req = request.into_inner();
        let inner = match req.fsync {
            Some(inner) => inner,
            None => {
                let resp_header = self.header_from_path_error(
                    &None,
                    MetadataError::InvalidArgument("missing fsync".to_string()),
                    None,
                );
                return error_response!(HflushPathResponseProto, resp_header);
            }
        };
        let fallback_header = inner.header.clone();
        let resp = self.fsync(Request::new(inner)).await?;
        response_with_header!(
            HflushPathResponseProto::default(),
            resp.into_inner()
                .header
                .unwrap_or_else(|| ok_header_from_request(&fallback_header, None, None))
        )
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn truncate(
        &self,
        request: Request<TruncatePathRequestProto>,
    ) -> Result<Response<TruncatePathResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);
        guard_or_error!(
            self,
            req,
            TruncatePathResponseProto,
            self.guard_chain.check_meta_write(&req_ctx)
        );

        // Resolve path to inode
        let resolved = match self.path_resolver.resolve_inode(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(TruncatePathResponseProto, resp_header);
            }
        };
        guard_or_error!(
            self,
            req,
            TruncatePathResponseProto,
            self.guard_chain
                .check_perm(&req_ctx, PermissionBits::WRITE, &req.path, &resolved)
        );
        guard_or_error!(
            self,
            req,
            TruncatePathResponseProto,
            self.guard_chain.check_data_write(&req_ctx, resolved.mount_ctx.mount_id)
        );
        let inode_id = match resolved.expect_inode() {
            Ok(inode_id) => inode_id,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
                return error_response!(TruncatePathResponseProto, resp_header);
            }
        };

        let freshness = Freshness {
            mount_epoch: req.header.as_ref().and_then(|h| h.mount_epoch),
            route_epoch: req.header.as_ref().and_then(|h| h.route_epoch),
            worker_epoch: None,
        };
        match self
            .fs_core
            .execute_truncate(TruncateInput {
                ctx: req_ctx.clone(),
                inode_id,
                new_size: req.new_size,
                lease_id: lease_id_from_proto(req.lease_id),
                lease_epoch: req.lease_epoch,
                freshness,
            })
            .await
        {
            Ok(success) => {
                let header = ok_header_from_core_success(&req_ctx, &success);
                response_with_header!(
                    TruncatePathResponseProto {
                        new_size: success.payload.new_size,
                        ..Default::default()
                    },
                    header
                )
            }
            Err(failure) => {
                let header = header_from_core_failure(&req_ctx, &failure);
                error_response!(TruncatePathResponseProto, header)
            }
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn set_xattr(
        &self,
        request: Request<SetXattrPathRequestProto>,
    ) -> Result<Response<SetXattrPathResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);
        guard_or_error!(
            self,
            req,
            SetXattrPathResponseProto,
            self.guard_chain.check_meta_write(&req_ctx)
        );
        let resolved = match self.path_resolver.resolve_inode(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(SetXattrPathResponseProto, resp_header);
            }
        };
        guard_or_error!(
            self,
            req,
            SetXattrPathResponseProto,
            self.guard_chain
                .check_perm(&req_ctx, PermissionBits::WRITE, &req.path, &resolved)
        );
        let inode_id = match resolved.expect_inode() {
            Ok(inode_id) => inode_id,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
                return error_response!(SetXattrPathResponseProto, resp_header);
            }
        };

        let freshness = Freshness {
            mount_epoch: req.header.as_ref().and_then(|h| h.mount_epoch),
            route_epoch: req.header.as_ref().and_then(|h| h.route_epoch),
            worker_epoch: None,
        };
        match self
            .fs_core
            .execute_set_xattr(SetXattrInput {
                ctx: req_ctx.clone(),
                inode_id,
                name: req.name,
                value: req.value,
                create: req.create,
                replace: req.replace,
                freshness,
            })
            .await
        {
            Ok(success) => {
                response_with_header!(
                    SetXattrPathResponseProto::default(),
                    ok_header_from_core_success(&req_ctx, &success)
                )
            }
            Err(failure) => {
                let header = header_from_core_failure(&req_ctx, &failure);
                error_response!(SetXattrPathResponseProto, header)
            }
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn get_xattr(
        &self,
        request: Request<GetXattrPathRequestProto>,
    ) -> Result<Response<GetXattrPathResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);
        guard_or_error!(
            self,
            req,
            GetXattrPathResponseProto,
            self.guard_chain.check_meta_read(&req_ctx)
        );
        let resolved = match self.path_resolver.resolve_inode(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(GetXattrPathResponseProto, resp_header);
            }
        };
        guard_or_error!(
            self,
            req,
            GetXattrPathResponseProto,
            self.guard_chain
                .check_perm(&req_ctx, PermissionBits::READ, &req.path, &resolved)
        );
        let inode_id = match resolved.expect_inode() {
            Ok(inode_id) => inode_id,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
                return error_response!(GetXattrPathResponseProto, resp_header);
            }
        };

        match self
            .fs_core
            .execute_get_xattr(GetXattrInput {
                ctx: req_ctx.clone(),
                inode_id,
                name: req.name,
            })
            .await
        {
            Ok(success) => {
                let header = ok_header_from_core_success(&req_ctx, &success);
                response_with_header!(
                    GetXattrPathResponseProto {
                        value: success.payload.value,
                        ..Default::default()
                    },
                    header
                )
            }
            Err(failure) => {
                let header = header_from_core_failure(&req_ctx, &failure);
                error_response!(GetXattrPathResponseProto, header)
            }
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn list_xattr(
        &self,
        request: Request<ListXattrPathRequestProto>,
    ) -> Result<Response<ListXattrPathResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);
        guard_or_error!(
            self,
            req,
            ListXattrPathResponseProto,
            self.guard_chain.check_meta_read(&req_ctx)
        );
        let resolved = match self.path_resolver.resolve_inode(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(ListXattrPathResponseProto, resp_header);
            }
        };
        guard_or_error!(
            self,
            req,
            ListXattrPathResponseProto,
            self.guard_chain
                .check_perm(&req_ctx, PermissionBits::READ, &req.path, &resolved)
        );
        let inode_id = match resolved.expect_inode() {
            Ok(inode_id) => inode_id,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
                return error_response!(ListXattrPathResponseProto, resp_header);
            }
        };

        match self
            .fs_core
            .execute_list_xattr(ListXattrInput {
                ctx: req_ctx.clone(),
                inode_id,
            })
            .await
        {
            Ok(success) => {
                let header = ok_header_from_core_success(&req_ctx, &success);
                response_with_header!(
                    ListXattrPathResponseProto {
                        names: success.payload.names,
                        ..Default::default()
                    },
                    header
                )
            }
            Err(failure) => {
                let header = header_from_core_failure(&req_ctx, &failure);
                error_response!(ListXattrPathResponseProto, header)
            }
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn remove_xattr(
        &self,
        request: Request<RemoveXattrPathRequestProto>,
    ) -> Result<Response<RemoveXattrPathResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);
        guard_or_error!(
            self,
            req,
            RemoveXattrPathResponseProto,
            self.guard_chain.check_meta_write(&req_ctx)
        );
        let resolved = match self.path_resolver.resolve_inode(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(RemoveXattrPathResponseProto, resp_header);
            }
        };
        guard_or_error!(
            self,
            req,
            RemoveXattrPathResponseProto,
            self.guard_chain
                .check_perm(&req_ctx, PermissionBits::WRITE, &req.path, &resolved)
        );
        let inode_id = match resolved.expect_inode() {
            Ok(inode_id) => inode_id,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
                return error_response!(RemoveXattrPathResponseProto, resp_header);
            }
        };

        let freshness = Freshness {
            mount_epoch: req.header.as_ref().and_then(|h| h.mount_epoch),
            route_epoch: req.header.as_ref().and_then(|h| h.route_epoch),
            worker_epoch: None,
        };
        match self
            .fs_core
            .execute_remove_xattr(RemoveXattrInput {
                ctx: req_ctx.clone(),
                inode_id,
                name: req.name,
                freshness,
            })
            .await
        {
            Ok(success) => {
                response_with_header!(
                    RemoveXattrPathResponseProto::default(),
                    ok_header_from_core_success(&req_ctx, &success)
                )
            }
            Err(failure) => {
                let header = header_from_core_failure(&req_ctx, &failure);
                error_response!(RemoveXattrPathResponseProto, header)
            }
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn get_file_layout_by_path(
        &self,
        request: Request<GetFileLayoutByPathRequestProto>,
    ) -> Result<Response<GetFileLayoutByPathResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);
        guard_or_error!(
            self,
            req,
            GetFileLayoutByPathResponseProto,
            self.guard_chain.check_meta_read(&req_ctx)
        );
        let resolved = match self.path_resolver.resolve_inode(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(GetFileLayoutByPathResponseProto, resp_header);
            }
        };
        guard_or_error!(
            self,
            req,
            GetFileLayoutByPathResponseProto,
            self.guard_chain
                .check_perm(&req_ctx, PermissionBits::READ, &req.path, &resolved)
        );
        guard_or_error!(
            self,
            req,
            GetFileLayoutByPathResponseProto,
            self.guard_chain.check_data_read(&req_ctx, resolved.mount_ctx.mount_id)
        );
        let inode_id = match resolved.expect_inode() {
            Ok(inode_id) => inode_id,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
                return error_response!(GetFileLayoutByPathResponseProto, resp_header);
            }
        };
        let freshness = Freshness {
            mount_epoch: req.header.as_ref().and_then(|h| h.mount_epoch),
            route_epoch: req.header.as_ref().and_then(|h| h.route_epoch),
            worker_epoch: None,
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
                freshness,
            })
            .await
        {
            Ok(success) => {
                let header = ok_header_from_core_success(&req_ctx, &success);
                let payload = success.payload;
                response_with_header!(
                    GetFileLayoutByPathResponseProto {
                        extents: payload.extents.iter().map(extent_to_proto).collect(),
                        file_size: payload.file_size,
                        locations: payload.locations.iter().map(location_to_proto).collect(),
                        ..Default::default()
                    },
                    header
                )
            }
            Err(failure) => {
                let header = header_from_core_failure(&req_ctx, &failure);
                error_response!(GetFileLayoutByPathResponseProto, header)
            }
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn open_write_by_path(
        &self,
        request: Request<OpenWriteByPathRequestProto>,
    ) -> Result<Response<OpenWriteByPathResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);
        guard_or_error!(
            self,
            req,
            OpenWriteByPathResponseProto,
            self.guard_chain.check_meta_write(&req_ctx)
        );

        let resolved = match self.path_resolver.resolve_inode(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(OpenWriteByPathResponseProto, resp_header);
            }
        };
        guard_or_error!(
            self,
            req,
            OpenWriteByPathResponseProto,
            self.guard_chain
                .check_perm(&req_ctx, PermissionBits::WRITE, &req.path, &resolved)
        );
        let inode_id = match resolved.expect_inode() {
            Ok(inode_id) => inode_id,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
                return error_response!(OpenWriteByPathResponseProto, resp_header);
            }
        };
        let freshness = Freshness {
            mount_epoch: req.header.as_ref().and_then(|h| h.mount_epoch),
            route_epoch: req.header.as_ref().and_then(|h| h.route_epoch),
            worker_epoch: None,
        };
        if let Err(failure) = self
            .fs_core
            .validate_mount_freshness(&req_ctx, freshness, resolved.mount_ctx.mount_id)
        {
            let header = header_from_core_failure(&req_ctx, &failure);
            return error_response!(OpenWriteByPathResponseProto, header);
        }
        guard_or_error!(
            self,
            req,
            OpenWriteByPathResponseProto,
            self.guard_chain.check_data_write(&req_ctx, resolved.mount_ctx.mount_id)
        );

        let mode = match req.mode {
            x if x == WriteModeProto::WriteModeAppend as i32 => crate::inode_lease::WriteMode::Append,
            _ => crate::inode_lease::WriteMode::Write,
        };
        match self
            .fs_core
            .execute_open_write(OpenWriteInput {
                ctx: req_ctx.clone(),
                inode_id,
                desired_len: req.desired_len,
                mode,
                freshness,
            })
            .await
        {
            Ok(success) => {
                let header = ok_header_from_core_success(&req_ctx, &success);
                let payload = success.payload;
                response_with_header!(
                    OpenWriteByPathResponseProto {
                        file_handle: payload.session_key.file_handle,
                        lease_id: Some(lease_id_to_proto(payload.session_key.lease_id)),
                        fencing_token: Some(fencing_to_proto(payload.session_key.fencing_token)),
                        write_targets: payload.write_targets.iter().map(write_target_to_proto).collect(),
                        base_size: payload.base_size,
                        open_epoch: payload.session_key.open_epoch,
                        lease_epoch: payload.session_key.lease_epoch,
                        expires_at_ms: payload.expires_at_ms,
                        ..Default::default()
                    },
                    header
                )
            }
            Err(failure) => {
                let resp_header = header_from_core_failure(&req_ctx, &failure);
                error_response!(OpenWriteByPathResponseProto, resp_header)
            }
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn close_write_session(
        &self,
        request: Request<CloseWriteSessionRequestProto>,
    ) -> Result<Response<CloseWriteSessionResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);
        if let Some(session) = self.fs_core.write_session_for_handle(req.file_handle) {
            guard_or_error!(
                self,
                req,
                CloseWriteSessionResponseProto,
                self.guard_chain.check_data_write(&req_ctx, session.mount_id)
            );
        } else {
            guard_or_error!(
                self,
                req,
                CloseWriteSessionResponseProto,
                self.guard_chain.check_meta_write(&req_ctx)
            );
        }

        let extents = match req
            .extents
            .into_iter()
            .map(extent_from_proto)
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(extents) => extents,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(CloseWriteSessionResponseProto, resp_header);
            }
        };

        let freshness = Freshness {
            mount_epoch: req.header.as_ref().and_then(|h| h.mount_epoch),
            route_epoch: req.header.as_ref().and_then(|h| h.route_epoch),
            worker_epoch: None,
        };
        match self
            .fs_core
            .execute_close_write(CloseWriteInput {
                ctx: req_ctx.clone(),
                file_handle: req.file_handle,
                lease_id: lease_id_from_proto(req.lease_id),
                lease_epoch: req.lease_epoch,
                open_epoch: req.open_epoch,
                fencing_token: presented_fencing_from_proto(req.fencing_token),
                intent: CloseWriteIntent {
                    extents,
                    final_size: req.final_size,
                },
                freshness,
            })
            .await
        {
            Ok(success) => {
                let header = ok_header_from_core_success(&req_ctx, &success);
                let payload = success.payload;
                response_with_header!(
                    CloseWriteSessionResponseProto {
                        committed_size: payload.committed_size,
                        file_version: payload.file_version,
                        ..Default::default()
                    },
                    header
                )
            }
            Err(failure) => {
                let resp_header = header_from_core_failure(&req_ctx, &failure);
                error_response!(CloseWriteSessionResponseProto, resp_header)
            }
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn renew_write_session_lease(
        &self,
        request: Request<RenewWriteSessionLeaseRequestProto>,
    ) -> Result<Response<RenewWriteSessionLeaseResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);
        if let Some(session) = self.fs_core.write_session_for_handle(req.file_handle) {
            guard_or_error!(
                self,
                req,
                RenewWriteSessionLeaseResponseProto,
                self.guard_chain.check_data_write(&req_ctx, session.mount_id)
            );
        } else {
            guard_or_error!(
                self,
                req,
                RenewWriteSessionLeaseResponseProto,
                self.guard_chain.check_meta_write(&req_ctx)
            );
        }

        let freshness = Freshness {
            mount_epoch: req.header.as_ref().and_then(|h| h.mount_epoch),
            route_epoch: req.header.as_ref().and_then(|h| h.route_epoch),
            worker_epoch: None,
        };
        match self
            .fs_core
            .execute_renew_inode_lease(RenewLeaseInput {
                ctx: req_ctx.clone(),
                file_handle: req.file_handle,
                lease_id: lease_id_from_proto(req.lease_id),
                lease_epoch: req.lease_epoch,
                freshness,
            })
            .await
        {
            Ok(success) => response_with_header!(
                RenewWriteSessionLeaseResponseProto {
                    expires_at_ms: success.payload.expires_at_ms,
                    ..Default::default()
                },
                ok_header_from_core_success(&req_ctx, &success)
            ),
            Err(failure) => {
                let resp_header = header_from_core_failure(&req_ctx, &failure);
                error_response!(RenewWriteSessionLeaseResponseProto, resp_header)
            }
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn fsync_session(
        &self,
        request: Request<FsyncSessionRequestProto>,
    ) -> Result<Response<FsyncSessionResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);
        if let Err(failure) = self.guard_chain.check_meta_write(&req_ctx).await {
            let resp_header = self.header_from_guard_failure(&req.header, failure);
            return response_with_header!(FsyncSessionResponseProto::default(), resp_header);
        }
        let session = match self.fs_core.write_session_for_handle(req.file_handle) {
            Some(session) => session,
            None => {
                let resp_header = need_refresh_header(
                    &req.header,
                    common::header::RpcErrorCode::Fencing,
                    common::error::canonical::RefreshReason::SessionInvalid,
                    "write session not found; refresh and re-open session before replaying fsync",
                    None,
                    None,
                );
                return response_with_header!(FsyncSessionResponseProto::default(), resp_header);
            }
        };
        if let Err(failure) = self.guard_chain.check_data_write(&req_ctx, session.mount_id).await {
            let resp_header = self.header_from_guard_failure(&req.header, failure);
            return response_with_header!(FsyncSessionResponseProto::default(), resp_header);
        }

        let freshness = Freshness {
            mount_epoch: req.header.as_ref().and_then(|h| h.mount_epoch),
            route_epoch: req.header.as_ref().and_then(|h| h.route_epoch),
            worker_epoch: req.worker_epoch,
        };
        match self
            .fs_core
            .execute_fsync(FsyncBarrierInput {
                ctx: req_ctx.clone(),
                inode_id: session.inode_id,
                file_handle: Some(req.file_handle),
                lease_id: lease_id_from_proto(req.lease_id),
                lease_epoch: req.lease_epoch,
                fencing_token: presented_fencing_from_proto(req.fencing_token),
                target_size: req.target_size,
                flags: req.flags as i32,
                freshness,
            })
            .await
        {
            Ok(success) => response_with_header!(
                FsyncSessionResponseProto::default(),
                ok_header_from_core_success(&req_ctx, &success)
            ),
            Err(failure) => {
                let resp_header = header_from_core_failure(&req_ctx, &failure);
                response_with_header!(FsyncSessionResponseProto::default(), resp_header)
            }
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn hsync_session(
        &self,
        request: Request<HsyncSessionRequestProto>,
    ) -> Result<Response<HsyncSessionResponseProto>, Status> {
        let req = request.into_inner();
        let inner = match req.fsync {
            Some(inner) => inner,
            None => {
                let resp_header = self.header_from_path_error(
                    &None,
                    MetadataError::InvalidArgument("missing fsync payload".to_string()),
                    None,
                );
                return response_with_header!(HsyncSessionResponseProto::default(), resp_header);
            }
        };
        let fallback_header = inner.header.clone();
        let resp = self.fsync_session(Request::new(inner)).await?;
        let resp_header = resp
            .into_inner()
            .header
            .unwrap_or_else(|| ok_header_from_request(&fallback_header, None, None));
        response_with_header!(HsyncSessionResponseProto::default(), resp_header)
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn hflush_session(
        &self,
        request: Request<HflushSessionRequestProto>,
    ) -> Result<Response<HflushSessionResponseProto>, Status> {
        let req = request.into_inner();
        let inner = match req.fsync {
            Some(inner) => inner,
            None => {
                let resp_header = self.header_from_path_error(
                    &None,
                    MetadataError::InvalidArgument("missing fsync payload".to_string()),
                    None,
                );
                return response_with_header!(HflushSessionResponseProto::default(), resp_header);
            }
        };
        let fallback_header = inner.header.clone();
        let resp = self.fsync_session(Request::new(inner)).await?;
        let resp_header = resp
            .into_inner()
            .header
            .unwrap_or_else(|| ok_header_from_request(&fallback_header, None, None));
        response_with_header!(HflushSessionResponseProto::default(), resp_header)
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn release_session(
        &self,
        request: Request<ReleaseSessionRequestProto>,
    ) -> Result<Response<ReleaseSessionResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);
        let session = self.fs_core.write_session_for_handle(req.file_handle);
        if let Some(session) = session.as_ref() {
            if let Err(failure) = self.guard_chain.check_data_write(&req_ctx, session.mount_id).await {
                let resp_header = self.header_from_guard_failure(&req.header, failure);
                return response_with_header!(ReleaseSessionResponseProto::default(), resp_header);
            }
        } else if let Err(failure) = self.guard_chain.check_meta_read(&req_ctx).await {
            let resp_header = self.header_from_guard_failure(&req.header, failure);
            return response_with_header!(ReleaseSessionResponseProto::default(), resp_header);
        }

        match self
            .fs_core
            .execute_release(ReleaseSessionInput {
                ctx: req_ctx.clone(),
                file_handle: req.file_handle,
            })
            .await
        {
            Ok(success) => response_with_header!(
                ReleaseSessionResponseProto::default(),
                ok_header_from_core_success(&req_ctx, &success)
            ),
            Err(failure) => {
                let resp_header = header_from_core_failure(&req_ctx, &failure);
                response_with_header!(ReleaseSessionResponseProto::default(), resp_header)
            }
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
