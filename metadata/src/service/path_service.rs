// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! FileSystemServiceProto implementation.
//!
//! This is a thin adapter layer that converts path-based requests to inode-based operations.
//! All FS semantics are delegated to FsCore domain APIs.

use super::domain::{
    CloseWriteInput, CloseWriteIntent, CreateInput, FileRange, Freshness, FsyncBarrierInput, GetAttrInput,
    GetFileLayoutInput, GetXattrInput, ListXattrInput, MkdirInput, OpenInput, OpenWriteInput, ReadDirInput,
    ReleaseSessionInput, RemoveXattrInput, RenameInput, RenewLeaseInput, RequestContext, RmdirInput, SetXattrInput,
    TruncateInput, UnlinkInput,
};
use super::extract_and_inject_context;
use super::guard::{AuthzContext, GuardChain, GuardSpec, LeadershipChecker};
use super::{
    extent_from_proto, extent_to_proto, fencing_to_proto, header_from_canonical_error, header_from_core_failure,
    lease_id_from_proto, lease_id_to_proto, location_to_proto, need_refresh_header, ok_header_from_core_success,
    ok_header_from_request, presented_fencing_from_proto, request_context_from_proto, write_target_to_proto,
};
use super::{AllowAllAuthz, AuthzOp, AuthzProvider, AuthzTarget, FsCore};
use crate::data_io::DataIoOp;
use crate::error::{to_canonical_fs, MetadataError, MetadataResult};
use crate::mount::MountTable;
use crate::path_resolver::{MountContext, PathResolver};
use crate::raft::RocksDBStorage;
use proto::metadata::file_system_service_proto_server::FileSystemServiceProto;
use proto::metadata::*;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::instrument;
use types::fs::FileAttrs;
use types::layout::FileLayout;

/// FileSystemServiceProto implementation.
pub struct MetadataFileSystemServiceImpl {
    path_resolver: PathResolver,
    fs_core: Arc<FsCore>,
    guard_chain: GuardChain,
    authz_provider: Arc<dyn AuthzProvider>,
    metrics: Option<Arc<crate::metrics::MetadataMetrics>>,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PathRpcAuthz {
    GetFileStatus,
    Mkdir,
    Create,
    Unlink,
    Rmdir,
    Rename,
    ListStatus,
    Open,
    Release,
    Fsync,
    Truncate,
    SetXattr,
    GetXattr,
    ListXattr,
    RemoveXattr,
    GetFileLayoutByPath,
    OpenWriteByPath,
    CloseWriteSession,
    RenewWriteSessionLease,
    FsyncSession,
    ReleaseSession,
}

impl MetadataFileSystemServiceImpl {
    pub fn new(mount_table: Arc<MountTable>, storage: Arc<RocksDBStorage>, fs_core: Arc<FsCore>) -> Self {
        let path_resolver = PathResolver::new(mount_table.clone(), storage.clone());
        let mut guard_chain = GuardChain::new(mount_table);
        if let Some(raft_node) = fs_core.raft_node() {
            guard_chain.set_leadership_checker(raft_node);
        }
        let authz_provider: Arc<dyn AuthzProvider> = Arc::new(AllowAllAuthz);
        guard_chain.set_authz_provider(authz_provider.clone());
        Self {
            path_resolver,
            fs_core,
            guard_chain,
            authz_provider,
            metrics: None,
        }
    }

    /// Set metrics for tracking (optional).
    pub fn with_metrics(mut self, metrics: Arc<crate::metrics::MetadataMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn with_readiness_gate(mut self, readiness_gate: Arc<crate::readiness::RootReadinessGate>) -> Self {
        self.guard_chain.set_readiness_gate(readiness_gate);
        self
    }

    pub fn with_leadership_checker<T>(mut self, checker: Arc<T>) -> Self
    where
        T: LeadershipChecker + 'static,
    {
        self.guard_chain.set_leadership_checker(checker);
        self
    }

    pub fn with_authz_provider(mut self, provider: Arc<dyn AuthzProvider>) -> Self {
        self.authz_provider = provider.clone();
        self.guard_chain.set_authz_provider(provider);
        self
    }

    fn path_parent_target(path: &str) -> MetadataResult<AuthzTarget> {
        let normalized = PathResolver::normalize(path)?;
        if normalized == "/" {
            return Err(MetadataError::InvalidArgument(
                "Cannot derive parent target from root path".to_string(),
            ));
        }
        let (parent_path, name) = normalized
            .rsplit_once('/')
            .ok_or_else(|| MetadataError::InvalidArgument(format!("Invalid path: {}", normalized)))?;
        let parent_path = if parent_path.is_empty() { "/" } else { parent_path };
        Ok(AuthzTarget::for_path_parent(parent_path.to_string(), name.to_string()))
    }

    async fn authorize_path(
        &self,
        req_header: &Option<proto::common::RequestHeaderProto>,
        caller_ctx: &common::header::RequestHeader,
        target: AuthzTarget,
        op: AuthzOp,
    ) -> Result<(), common::error::canonical::CanonicalError> {
        let req_ctx = RequestContext {
            caller: caller_ctx.clone(),
            traceparent: caller_ctx.traceparent.clone(),
            route_epoch: req_header.as_ref().and_then(|h| h.route_epoch),
        };
        self.authz_provider.authorize(&req_ctx, target, op).await
    }

    fn authz_for_rpc(rpc: PathRpcAuthz, target: AuthzTarget) -> AuthzContext {
        let op = match rpc {
            PathRpcAuthz::GetFileStatus | PathRpcAuthz::ListStatus | PathRpcAuthz::Open => AuthzOp::Read,
            PathRpcAuthz::Mkdir | PathRpcAuthz::Create => AuthzOp::Write,
            PathRpcAuthz::Unlink | PathRpcAuthz::Rmdir => AuthzOp::Delete,
            PathRpcAuthz::Rename => AuthzOp::Rename,
            PathRpcAuthz::GetXattr | PathRpcAuthz::ListXattr | PathRpcAuthz::SetXattr | PathRpcAuthz::RemoveXattr => {
                AuthzOp::Xattr
            }
            PathRpcAuthz::Release
            | PathRpcAuthz::Fsync
            | PathRpcAuthz::Truncate
            | PathRpcAuthz::GetFileLayoutByPath
            | PathRpcAuthz::OpenWriteByPath
            | PathRpcAuthz::CloseWriteSession
            | PathRpcAuthz::RenewWriteSessionLease
            | PathRpcAuthz::FsyncSession
            | PathRpcAuthz::ReleaseSession => AuthzOp::Write,
        };
        AuthzContext { op, target }
    }

    /// Validate mount_epoch for write operations.
    /// Returns error if mount_epoch mismatch (should be converted to NEED_REFRESH).
    fn validate_mount_epoch(
        &self,
        req_header: &Option<proto::common::RequestHeaderProto>,
        mount_ctx: &crate::path_resolver::MountContext,
    ) -> MetadataResult<()> {
        if let Some(header) = req_header {
            if let Some(client_mount_epoch) = header.mount_epoch {
                if client_mount_epoch != mount_ctx.mount_epoch {
                    return Err(MetadataError::MountEpochMismatch {
                        expected: mount_ctx.mount_epoch,
                        got: client_mount_epoch,
                        mount_id: Some(mount_ctx.mount_id),
                    });
                }
            }
        }
        Ok(())
    }

    fn header_or_ok(
        &self,
        req_header: &Option<proto::common::RequestHeaderProto>,
        fs_header: Option<proto::common::ResponseHeaderProto>,
        group_id: Option<u64>,
        mount_epoch: Option<u64>,
    ) -> proto::common::ResponseHeaderProto {
        fs_header.unwrap_or_else(|| ok_header_from_request(req_header, group_id, mount_epoch))
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

    async fn guard_request(
        &self,
        req_header: &Option<proto::common::RequestHeaderProto>,
        caller_ctx: &common::header::RequestHeader,
        mut spec: GuardSpec,
        mount_id: Option<types::ids::MountId>,
        authz: Option<AuthzContext>,
        fallback_group_id: Option<u64>,
        fallback_mount_epoch: Option<u64>,
    ) -> Option<proto::common::ResponseHeaderProto> {
        if authz.is_some() {
            spec = spec.with_authz();
        }
        match self
            .guard_chain
            .check_request(req_header, caller_ctx, spec, mount_id, authz)
            .await
        {
            Ok(()) => None,
            Err(failure) => Some(header_from_canonical_error(
                req_header,
                failure.group_id.or(fallback_group_id),
                failure.mount_epoch.or(fallback_mount_epoch),
                &failure.err,
            )),
        }
    }

    /// Convert types FileAttrs to proto FileAttrsProto.
    fn file_attrs_to_proto(attrs: &FileAttrs) -> proto::fs::FileAttrsProto {
        proto::fs::FileAttrsProto {
            mode: attrs.mode,
            uid: attrs.uid,
            gid: attrs.gid,
            size: attrs.size,
            atime_ms: attrs.atime_ms,
            mtime_ms: attrs.mtime_ms,
            ctime_ms: attrs.ctime_ms,
            nlink: attrs.nlink,
        }
    }

    /// Convert proto FileAttrsProto to types FileAttrs.
    fn proto_to_file_attrs(attrs: Option<proto::fs::FileAttrsProto>) -> MetadataResult<FileAttrs> {
        let attrs = attrs.ok_or_else(|| MetadataError::InvalidArgument("Missing FileAttrs".to_string()))?;
        Ok(FileAttrs {
            mode: attrs.mode,
            uid: attrs.uid,
            gid: attrs.gid,
            size: attrs.size,
            atime_ms: attrs.atime_ms,
            mtime_ms: attrs.mtime_ms,
            ctime_ms: attrs.ctime_ms,
            nlink: attrs.nlink,
        })
    }

    /// Convert proto FileLayoutProto to types FileLayout.
    fn proto_to_file_layout(layout: Option<proto::common::FileLayoutProto>) -> MetadataResult<FileLayout> {
        let layout = layout.ok_or_else(|| MetadataError::InvalidArgument("Missing FileLayout".to_string()))?;
        Ok(FileLayout::new(
            layout.block_size,
            layout.chunk_size,
            layout.replication as u8,
        ))
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
        let caller_ctx = extract_and_inject_context(&req.header);

        if let Err(err) = self
            .authorize_path(
                &req.header,
                &caller_ctx,
                AuthzTarget::for_path(req.path.clone()),
                AuthzOp::Read,
            )
            .await
        {
            let resp_header = header_from_canonical_error(&req.header, None, None, &err);
            return error_response!(GetFileStatusResponseProto, resp_header);
        }

        // Resolve path to inode.
        let resolved = match self.path_resolver.resolve_inode(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(GetFileStatusResponseProto, resp_header);
            }
        };

        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::metadata_read(),
                Some(resolved.mount_ctx.mount_id),
                Some(Self::authz_for_rpc(
                    PathRpcAuthz::GetFileStatus,
                    AuthzTarget::for_inode(resolved.inode_id),
                )),
                Some(resolved.mount_ctx.owner_group_id.as_raw()),
                Some(resolved.mount_ctx.mount_epoch),
            )
            .await
        {
            return error_response!(GetFileStatusResponseProto, resp_header);
        }

        let req_ctx = request_context_from_proto(&req.header);
        match self
            .fs_core
            .get_attr(GetAttrInput {
                ctx: req_ctx.clone(),
                inode_id: resolved.inode_id,
            })
            .await
        {
            Ok(success) => {
                let header = ok_header_from_core_success(&req_ctx, &success);
                response_with_header!(
                    GetFileStatusResponseProto {
                        inode_id: Some(proto::fs::InodeIdProto {
                            value: resolved.inode_id.as_raw(),
                        }),
                        attrs: Some(Self::file_attrs_to_proto(&success.payload.attrs)),
                        inode: None,
                        ..Default::default()
                    },
                    header
                )
            }
            Err(failure) => {
                let header = self.header_or_ok(
                    &req.header,
                    Some(header_from_core_failure(&req_ctx, &failure)),
                    Some(resolved.mount_ctx.owner_group_id.as_raw()),
                    Some(resolved.mount_ctx.mount_epoch),
                );
                error_response!(GetFileStatusResponseProto, header)
            }
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn mkdir(&self, request: Request<MkdirPathRequestProto>) -> Result<Response<MkdirPathResponseProto>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);

        let authz_target = match Self::path_parent_target(&req.path) {
            Ok(target) => target,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(MkdirPathResponseProto, resp_header);
            }
        };
        if let Err(err) = self
            .authorize_path(&req.header, &caller_ctx, authz_target, AuthzOp::Write)
            .await
        {
            let resp_header = header_from_canonical_error(&req.header, None, None, &err);
            return error_response!(MkdirPathResponseProto, resp_header);
        }

        // Resolve path
        let resolved = match self.path_resolver.resolve_path(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(MkdirPathResponseProto, resp_header);
            }
        };

        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::metadata_write(),
                Some(resolved.mount_ctx.mount_id),
                Some(Self::authz_for_rpc(
                    PathRpcAuthz::Mkdir,
                    AuthzTarget::for_inode(resolved.parent_inode_id),
                )),
                Some(resolved.mount_ctx.owner_group_id.as_raw()),
                Some(resolved.mount_ctx.mount_epoch),
            )
            .await
        {
            return error_response!(MkdirPathResponseProto, resp_header);
        }

        // Convert attrs
        let attrs = match Self::proto_to_file_attrs(req.attrs) {
            Ok(attrs) => attrs,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
                return error_response!(MkdirPathResponseProto, resp_header);
            }
        };

        let req_ctx = request_context_from_proto(&req.header);
        let freshness = Freshness {
            mount_epoch: req.header.as_ref().and_then(|h| h.mount_epoch),
            route_epoch: req.header.as_ref().and_then(|h| h.route_epoch),
            worker_epoch: None,
        };
        match self
            .fs_core
            .mkdir(MkdirInput {
                ctx: req_ctx.clone(),
                parent_inode_id: resolved.parent_inode_id,
                name: resolved.name,
                attrs,
                freshness,
            })
            .await
        {
            Ok(success) => {
                let header = ok_header_from_core_success(&req_ctx, &success);
                let payload = success.payload;
                let attrs_proto = payload.attrs.as_ref().map(Self::file_attrs_to_proto);
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
                let header = self.header_or_ok(
                    &req.header,
                    Some(header_from_core_failure(&req_ctx, &failure)),
                    Some(resolved.mount_ctx.owner_group_id.as_raw()),
                    Some(resolved.mount_ctx.mount_epoch),
                );
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
        let caller_ctx = extract_and_inject_context(&req.header);

        let authz_target = match Self::path_parent_target(&req.path) {
            Ok(target) => target,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(CreatePathResponseProto, resp_header);
            }
        };
        if let Err(err) = self
            .authorize_path(&req.header, &caller_ctx, authz_target, AuthzOp::Write)
            .await
        {
            let resp_header = header_from_canonical_error(&req.header, None, None, &err);
            return error_response!(CreatePathResponseProto, resp_header);
        }

        // Resolve path
        let resolved = match self.path_resolver.resolve_path(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(CreatePathResponseProto, resp_header);
            }
        };

        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::metadata_write(),
                Some(resolved.mount_ctx.mount_id),
                Some(Self::authz_for_rpc(
                    PathRpcAuthz::Create,
                    AuthzTarget::for_inode(resolved.parent_inode_id),
                )),
                Some(resolved.mount_ctx.owner_group_id.as_raw()),
                Some(resolved.mount_ctx.mount_epoch),
            )
            .await
        {
            return error_response!(CreatePathResponseProto, resp_header);
        }

        // Convert attrs and layout
        let attrs = match Self::proto_to_file_attrs(req.attrs) {
            Ok(attrs) => attrs,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
                return error_response!(CreatePathResponseProto, resp_header);
            }
        };
        let layout = match Self::proto_to_file_layout(req.layout) {
            Ok(layout) => layout,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
                return error_response!(CreatePathResponseProto, resp_header);
            }
        };

        let req_ctx = request_context_from_proto(&req.header);
        let freshness = Freshness {
            mount_epoch: req.header.as_ref().and_then(|h| h.mount_epoch),
            route_epoch: req.header.as_ref().and_then(|h| h.route_epoch),
            worker_epoch: None,
        };
        match self
            .fs_core
            .create(CreateInput {
                ctx: req_ctx.clone(),
                parent_inode_id: resolved.parent_inode_id,
                name: resolved.name,
                attrs,
                layout,
                freshness,
            })
            .await
        {
            Ok(success) => {
                let header = ok_header_from_core_success(&req_ctx, &success);
                let payload = success.payload;
                let attrs_proto = payload.attrs.as_ref().map(Self::file_attrs_to_proto);
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
                let header = self.header_or_ok(
                    &req.header,
                    Some(header_from_core_failure(&req_ctx, &failure)),
                    Some(resolved.mount_ctx.owner_group_id.as_raw()),
                    Some(resolved.mount_ctx.mount_epoch),
                );
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
        let caller_ctx = extract_and_inject_context(&req.header);

        let authz_target = match Self::path_parent_target(&req.path) {
            Ok(target) => target,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(UnlinkPathResponseProto, resp_header);
            }
        };
        if let Err(err) = self
            .authorize_path(&req.header, &caller_ctx, authz_target, AuthzOp::Delete)
            .await
        {
            let resp_header = header_from_canonical_error(&req.header, None, None, &err);
            return error_response!(UnlinkPathResponseProto, resp_header);
        }

        // Resolve path
        let resolved = match self.path_resolver.resolve_path(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(UnlinkPathResponseProto, resp_header);
            }
        };

        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::metadata_write(),
                Some(resolved.mount_ctx.mount_id),
                Some(Self::authz_for_rpc(
                    PathRpcAuthz::Unlink,
                    AuthzTarget::for_inode(resolved.parent_inode_id),
                )),
                Some(resolved.mount_ctx.owner_group_id.as_raw()),
                Some(resolved.mount_ctx.mount_epoch),
            )
            .await
        {
            return error_response!(UnlinkPathResponseProto, resp_header);
        }

        let req_ctx = request_context_from_proto(&req.header);
        let freshness = Freshness {
            mount_epoch: req.header.as_ref().and_then(|h| h.mount_epoch),
            route_epoch: req.header.as_ref().and_then(|h| h.route_epoch),
            worker_epoch: None,
        };
        match self
            .fs_core
            .unlink(UnlinkInput {
                ctx: req_ctx.clone(),
                parent_inode_id: resolved.parent_inode_id,
                name: resolved.name,
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
                let header = self.header_or_ok(
                    &req.header,
                    Some(header_from_core_failure(&req_ctx, &failure)),
                    Some(resolved.mount_ctx.owner_group_id.as_raw()),
                    Some(resolved.mount_ctx.mount_epoch),
                );
                error_response!(UnlinkPathResponseProto, header)
            }
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn rmdir(&self, request: Request<RmdirPathRequestProto>) -> Result<Response<RmdirPathResponseProto>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);

        let authz_target = match Self::path_parent_target(&req.path) {
            Ok(target) => target,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(RmdirPathResponseProto, resp_header);
            }
        };
        if let Err(err) = self
            .authorize_path(&req.header, &caller_ctx, authz_target, AuthzOp::Delete)
            .await
        {
            let resp_header = header_from_canonical_error(&req.header, None, None, &err);
            return error_response!(RmdirPathResponseProto, resp_header);
        }

        // Resolve path
        let resolved = match self.path_resolver.resolve_path(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(RmdirPathResponseProto, resp_header);
            }
        };

        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::metadata_write(),
                Some(resolved.mount_ctx.mount_id),
                Some(Self::authz_for_rpc(
                    PathRpcAuthz::Rmdir,
                    AuthzTarget::for_inode(resolved.parent_inode_id),
                )),
                Some(resolved.mount_ctx.owner_group_id.as_raw()),
                Some(resolved.mount_ctx.mount_epoch),
            )
            .await
        {
            return error_response!(RmdirPathResponseProto, resp_header);
        }

        let req_ctx = request_context_from_proto(&req.header);
        let freshness = Freshness {
            mount_epoch: req.header.as_ref().and_then(|h| h.mount_epoch),
            route_epoch: req.header.as_ref().and_then(|h| h.route_epoch),
            worker_epoch: None,
        };
        match self
            .fs_core
            .rmdir(RmdirInput {
                ctx: req_ctx.clone(),
                parent_inode_id: resolved.parent_inode_id,
                name: resolved.name,
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
                let header = self.header_or_ok(
                    &req.header,
                    Some(header_from_core_failure(&req_ctx, &failure)),
                    Some(resolved.mount_ctx.owner_group_id.as_raw()),
                    Some(resolved.mount_ctx.mount_epoch),
                );
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
        let caller_ctx = extract_and_inject_context(&req.header);

        if let Err(err) = self
            .authorize_path(
                &req.header,
                &caller_ctx,
                AuthzTarget::for_path(req.src_path.clone()),
                AuthzOp::Rename,
            )
            .await
        {
            let resp_header = header_from_canonical_error(&req.header, None, None, &err);
            return error_response!(RenamePathResponseProto, resp_header);
        }

        let dst_parent_target = match Self::path_parent_target(&req.dst_path) {
            Ok(target) => target,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(RenamePathResponseProto, resp_header);
            }
        };
        if let Err(err) = self
            .authorize_path(&req.header, &caller_ctx, dst_parent_target, AuthzOp::Rename)
            .await
        {
            let resp_header = header_from_canonical_error(&req.header, None, None, &err);
            return error_response!(RenamePathResponseProto, resp_header);
        }

        // Resolve both paths
        let (src_resolved, dst_resolved) = match self.path_resolver.resolve_rename(&req.src_path, &req.dst_path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(RenamePathResponseProto, resp_header);
            }
        };

        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::metadata_write(),
                Some(src_resolved.mount_ctx.mount_id),
                Some(Self::authz_for_rpc(
                    PathRpcAuthz::Rename,
                    AuthzTarget::for_inode(src_resolved.parent_inode_id).with_parent(dst_resolved.parent_inode_id),
                )),
                Some(src_resolved.mount_ctx.owner_group_id.as_raw()),
                Some(src_resolved.mount_ctx.mount_epoch),
            )
            .await
        {
            return error_response!(RenamePathResponseProto, resp_header);
        }

        let req_ctx = request_context_from_proto(&req.header);
        let freshness = Freshness {
            mount_epoch: req.header.as_ref().and_then(|h| h.mount_epoch),
            route_epoch: req.header.as_ref().and_then(|h| h.route_epoch),
            worker_epoch: None,
        };
        match self
            .fs_core
            .rename(RenameInput {
                ctx: req_ctx.clone(),
                src_parent_inode_id: src_resolved.parent_inode_id,
                src_name: src_resolved.name,
                dst_parent_inode_id: dst_resolved.parent_inode_id,
                dst_name: dst_resolved.name,
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
                let header = self.header_or_ok(
                    &req.header,
                    Some(header_from_core_failure(&req_ctx, &failure)),
                    Some(src_resolved.mount_ctx.owner_group_id.as_raw()),
                    Some(src_resolved.mount_ctx.mount_epoch),
                );
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
        let caller_ctx = extract_and_inject_context(&req.header);

        if let Err(err) = self
            .authorize_path(
                &req.header,
                &caller_ctx,
                AuthzTarget::for_path(req.path.clone()),
                AuthzOp::Read,
            )
            .await
        {
            let resp_header = header_from_canonical_error(&req.header, None, None, &err);
            return error_response!(ListStatusPathResponseProto, resp_header);
        }

        // Resolve path to inode
        let resolved = match self.path_resolver.resolve_inode(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(ListStatusPathResponseProto, resp_header);
            }
        };

        if !req.recursive {
            if let Some(resp_header) = self
                .guard_request(
                    &req.header,
                    &caller_ctx,
                    GuardSpec::metadata_read(),
                    Some(resolved.mount_ctx.mount_id),
                    Some(Self::authz_for_rpc(
                        PathRpcAuthz::ListStatus,
                        AuthzTarget::for_inode(resolved.inode_id),
                    )),
                    Some(resolved.mount_ctx.owner_group_id.as_raw()),
                    Some(resolved.mount_ctx.mount_epoch),
                )
                .await
            {
                return error_response!(ListStatusPathResponseProto, resp_header);
            }

            let req_ctx = request_context_from_proto(&req.header);
            let cursor_key = if req.cursor.is_empty() {
                None
            } else {
                Some(req.cursor.clone())
            };
            let max_entries = if req.limit == 0 { None } else { Some(req.limit as usize) };
            match self
                .fs_core
                .read_dir(ReadDirInput {
                    ctx: req_ctx.clone(),
                    parent_inode_id: resolved.inode_id,
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
                            attrs: entry.attrs.as_ref().map(Self::file_attrs_to_proto),
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
                    let header = self.header_or_ok(
                        &req.header,
                        Some(header_from_core_failure(&req_ctx, &failure)),
                        Some(resolved.mount_ctx.owner_group_id.as_raw()),
                        Some(resolved.mount_ctx.mount_epoch),
                    );
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
        let caller_ctx = extract_and_inject_context(&req.header);

        if let Err(err) = self
            .authorize_path(
                &req.header,
                &caller_ctx,
                AuthzTarget::for_path(req.path.clone()),
                AuthzOp::Read,
            )
            .await
        {
            let resp_header = header_from_canonical_error(&req.header, None, None, &err);
            return error_response!(OpenPathResponseProto, resp_header);
        }

        // Resolve path to inode
        let resolved = match self.path_resolver.resolve_inode(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(OpenPathResponseProto, resp_header);
            }
        };

        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::metadata_read(),
                Some(resolved.mount_ctx.mount_id),
                Some(Self::authz_for_rpc(
                    PathRpcAuthz::Open,
                    AuthzTarget::for_inode(resolved.inode_id),
                )),
                Some(resolved.mount_ctx.owner_group_id.as_raw()),
                Some(resolved.mount_ctx.mount_epoch),
            )
            .await
        {
            return error_response!(OpenPathResponseProto, resp_header);
        }

        let req_ctx = request_context_from_proto(&req.header);
        match self
            .fs_core
            .open(OpenInput {
                ctx: req_ctx.clone(),
                inode_id: resolved.inode_id,
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
                let header = self.header_or_ok(
                    &req.header,
                    Some(header_from_core_failure(&req_ctx, &failure)),
                    Some(resolved.mount_ctx.owner_group_id.as_raw()),
                    Some(resolved.mount_ctx.mount_epoch),
                );
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
        let caller_ctx = extract_and_inject_context(&req.header);
        if let Some(session) = self.fs_core.write_session_for_handle(req.file_handle) {
            let (group_id, mount_epoch) = self.fs_core.mount_hints_for_mount(session.mount_id);
            if let Some(resp_header) = self
                .guard_request(
                    &req.header,
                    &caller_ctx,
                    GuardSpec::data_io(DataIoOp::CloseWrite).with_leader(),
                    Some(session.mount_id),
                    Some(Self::authz_for_rpc(
                        PathRpcAuthz::Release,
                        AuthzTarget::for_session(req.file_handle, Some(session.inode_id)),
                    )),
                    group_id,
                    mount_epoch,
                )
                .await
            {
                return response_with_header!(ReleasePathResponseProto::default(), resp_header);
            }
        } else if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::metadata_read(),
                None,
                Some(Self::authz_for_rpc(
                    PathRpcAuthz::Release,
                    AuthzTarget::for_file_handle(req.file_handle),
                )),
                None,
                None,
            )
            .await
        {
            return response_with_header!(ReleasePathResponseProto::default(), resp_header);
        }

        let req_ctx = request_context_from_proto(&req.header);
        match self
            .fs_core
            .release_session(ReleaseSessionInput {
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
                let header = self.header_or_ok(
                    &req.header,
                    Some(header_from_core_failure(&req_ctx, &failure)),
                    None,
                    None,
                );
                response_with_header!(ReleasePathResponseProto::default(), header)
            }
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn fsync(&self, request: Request<FsyncPathRequestProto>) -> Result<Response<FsyncPathResponseProto>, Status> {
        let req = request.into_inner();
        let caller_ctx = extract_and_inject_context(&req.header);

        // Handle path-based or handle-based fsync
        match req.target {
            Some(proto::metadata::fsync_path_request_proto::Target::Path(path)) => {
                if let Err(err) = self
                    .authorize_path(
                        &req.header,
                        &caller_ctx,
                        AuthzTarget::for_path(path.clone()),
                        AuthzOp::Write,
                    )
                    .await
                {
                    let resp_header = header_from_canonical_error(&req.header, None, None, &err);
                    return error_response!(FsyncPathResponseProto, resp_header);
                }

                // Resolve path to inode
                let resolved = match self.path_resolver.resolve_inode(&path) {
                    Ok(resolved) => resolved,
                    Err(err) => {
                        let resp_header = self.header_from_path_error(&req.header, err, None);
                        return error_response!(FsyncPathResponseProto, resp_header);
                    }
                };

                if let Some(resp_header) = self
                    .guard_request(
                        &req.header,
                        &caller_ctx,
                        GuardSpec::data_io(DataIoOp::Fsync).with_leader(),
                        Some(resolved.mount_ctx.mount_id),
                        Some(Self::authz_for_rpc(
                            PathRpcAuthz::Fsync,
                            AuthzTarget::for_inode(resolved.inode_id),
                        )),
                        Some(resolved.mount_ctx.owner_group_id.as_raw()),
                        Some(resolved.mount_ctx.mount_epoch),
                    )
                    .await
                {
                    return error_response!(FsyncPathResponseProto, resp_header);
                }

                let req_ctx = request_context_from_proto(&req.header);
                let freshness = Freshness {
                    mount_epoch: req.header.as_ref().and_then(|h| h.mount_epoch),
                    route_epoch: req.header.as_ref().and_then(|h| h.route_epoch),
                    worker_epoch: None,
                };
                match self
                    .fs_core
                    .fsync_barrier(FsyncBarrierInput {
                        ctx: req_ctx.clone(),
                        inode_id: resolved.inode_id,
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
                        let header = self.header_or_ok(
                            &req.header,
                            Some(header_from_core_failure(&req_ctx, &failure)),
                            Some(resolved.mount_ctx.owner_group_id.as_raw()),
                            Some(resolved.mount_ctx.mount_epoch),
                        );
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
        let caller_ctx = extract_and_inject_context(&req.header);

        if let Err(err) = self
            .authorize_path(
                &req.header,
                &caller_ctx,
                AuthzTarget::for_path(req.path.clone()),
                AuthzOp::Write,
            )
            .await
        {
            let resp_header = header_from_canonical_error(&req.header, None, None, &err);
            return error_response!(TruncatePathResponseProto, resp_header);
        }

        // Resolve path to inode
        let resolved = match self.path_resolver.resolve_inode(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(TruncatePathResponseProto, resp_header);
            }
        };

        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::data_io(DataIoOp::Truncate).with_leader(),
                Some(resolved.mount_ctx.mount_id),
                Some(Self::authz_for_rpc(
                    PathRpcAuthz::Truncate,
                    AuthzTarget::for_inode(resolved.inode_id),
                )),
                Some(resolved.mount_ctx.owner_group_id.as_raw()),
                Some(resolved.mount_ctx.mount_epoch),
            )
            .await
        {
            return error_response!(TruncatePathResponseProto, resp_header);
        }

        let req_ctx = request_context_from_proto(&req.header);
        let freshness = Freshness {
            mount_epoch: req.header.as_ref().and_then(|h| h.mount_epoch),
            route_epoch: req.header.as_ref().and_then(|h| h.route_epoch),
            worker_epoch: None,
        };
        match self
            .fs_core
            .truncate(TruncateInput {
                ctx: req_ctx.clone(),
                inode_id: resolved.inode_id,
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
                let header = self.header_or_ok(
                    &req.header,
                    Some(header_from_core_failure(&req_ctx, &failure)),
                    Some(resolved.mount_ctx.owner_group_id.as_raw()),
                    Some(resolved.mount_ctx.mount_epoch),
                );
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
        let caller_ctx = extract_and_inject_context(&req.header);
        if let Err(err) = self
            .authorize_path(
                &req.header,
                &caller_ctx,
                AuthzTarget::for_path(req.path.clone()),
                AuthzOp::Xattr,
            )
            .await
        {
            let resp_header = header_from_canonical_error(&req.header, None, None, &err);
            return error_response!(SetXattrPathResponseProto, resp_header);
        }
        let resolved = match self.path_resolver.resolve_inode(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(SetXattrPathResponseProto, resp_header);
            }
        };

        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::metadata_write(),
                Some(resolved.mount_ctx.mount_id),
                Some(Self::authz_for_rpc(
                    PathRpcAuthz::SetXattr,
                    AuthzTarget::for_inode(resolved.inode_id),
                )),
                Some(resolved.mount_ctx.owner_group_id.as_raw()),
                Some(resolved.mount_ctx.mount_epoch),
            )
            .await
        {
            return error_response!(SetXattrPathResponseProto, resp_header);
        }

        let req_ctx = request_context_from_proto(&req.header);
        let freshness = Freshness {
            mount_epoch: req.header.as_ref().and_then(|h| h.mount_epoch),
            route_epoch: req.header.as_ref().and_then(|h| h.route_epoch),
            worker_epoch: None,
        };
        match self
            .fs_core
            .set_xattr(SetXattrInput {
                ctx: req_ctx.clone(),
                inode_id: resolved.inode_id,
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
                let header = self.header_or_ok(
                    &req.header,
                    Some(header_from_core_failure(&req_ctx, &failure)),
                    Some(resolved.mount_ctx.owner_group_id.as_raw()),
                    Some(resolved.mount_ctx.mount_epoch),
                );
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
        let caller_ctx = extract_and_inject_context(&req.header);
        if let Err(err) = self
            .authorize_path(
                &req.header,
                &caller_ctx,
                AuthzTarget::for_path(req.path.clone()),
                AuthzOp::Xattr,
            )
            .await
        {
            let resp_header = header_from_canonical_error(&req.header, None, None, &err);
            return error_response!(GetXattrPathResponseProto, resp_header);
        }
        let resolved = match self.path_resolver.resolve_inode(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(GetXattrPathResponseProto, resp_header);
            }
        };

        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::metadata_read(),
                Some(resolved.mount_ctx.mount_id),
                Some(Self::authz_for_rpc(
                    PathRpcAuthz::GetXattr,
                    AuthzTarget::for_inode(resolved.inode_id),
                )),
                Some(resolved.mount_ctx.owner_group_id.as_raw()),
                Some(resolved.mount_ctx.mount_epoch),
            )
            .await
        {
            return error_response!(GetXattrPathResponseProto, resp_header);
        }

        let req_ctx = request_context_from_proto(&req.header);
        match self
            .fs_core
            .get_xattr(GetXattrInput {
                ctx: req_ctx.clone(),
                inode_id: resolved.inode_id,
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
                let header = self.header_or_ok(
                    &req.header,
                    Some(header_from_core_failure(&req_ctx, &failure)),
                    Some(resolved.mount_ctx.owner_group_id.as_raw()),
                    Some(resolved.mount_ctx.mount_epoch),
                );
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
        let caller_ctx = extract_and_inject_context(&req.header);
        if let Err(err) = self
            .authorize_path(
                &req.header,
                &caller_ctx,
                AuthzTarget::for_path(req.path.clone()),
                AuthzOp::Xattr,
            )
            .await
        {
            let resp_header = header_from_canonical_error(&req.header, None, None, &err);
            return error_response!(ListXattrPathResponseProto, resp_header);
        }
        let resolved = match self.path_resolver.resolve_inode(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(ListXattrPathResponseProto, resp_header);
            }
        };

        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::metadata_read(),
                Some(resolved.mount_ctx.mount_id),
                Some(Self::authz_for_rpc(
                    PathRpcAuthz::ListXattr,
                    AuthzTarget::for_inode(resolved.inode_id),
                )),
                Some(resolved.mount_ctx.owner_group_id.as_raw()),
                Some(resolved.mount_ctx.mount_epoch),
            )
            .await
        {
            return error_response!(ListXattrPathResponseProto, resp_header);
        }

        let req_ctx = request_context_from_proto(&req.header);
        match self
            .fs_core
            .list_xattr(ListXattrInput {
                ctx: req_ctx.clone(),
                inode_id: resolved.inode_id,
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
                let header = self.header_or_ok(
                    &req.header,
                    Some(header_from_core_failure(&req_ctx, &failure)),
                    Some(resolved.mount_ctx.owner_group_id.as_raw()),
                    Some(resolved.mount_ctx.mount_epoch),
                );
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
        let caller_ctx = extract_and_inject_context(&req.header);
        if let Err(err) = self
            .authorize_path(
                &req.header,
                &caller_ctx,
                AuthzTarget::for_path(req.path.clone()),
                AuthzOp::Xattr,
            )
            .await
        {
            let resp_header = header_from_canonical_error(&req.header, None, None, &err);
            return error_response!(RemoveXattrPathResponseProto, resp_header);
        }
        let resolved = match self.path_resolver.resolve_inode(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(RemoveXattrPathResponseProto, resp_header);
            }
        };

        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::metadata_write(),
                Some(resolved.mount_ctx.mount_id),
                Some(Self::authz_for_rpc(
                    PathRpcAuthz::RemoveXattr,
                    AuthzTarget::for_inode(resolved.inode_id),
                )),
                Some(resolved.mount_ctx.owner_group_id.as_raw()),
                Some(resolved.mount_ctx.mount_epoch),
            )
            .await
        {
            return error_response!(RemoveXattrPathResponseProto, resp_header);
        }

        let req_ctx = request_context_from_proto(&req.header);
        let freshness = Freshness {
            mount_epoch: req.header.as_ref().and_then(|h| h.mount_epoch),
            route_epoch: req.header.as_ref().and_then(|h| h.route_epoch),
            worker_epoch: None,
        };
        match self
            .fs_core
            .remove_xattr(RemoveXattrInput {
                ctx: req_ctx.clone(),
                inode_id: resolved.inode_id,
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
                let header = self.header_or_ok(
                    &req.header,
                    Some(header_from_core_failure(&req_ctx, &failure)),
                    Some(resolved.mount_ctx.owner_group_id.as_raw()),
                    Some(resolved.mount_ctx.mount_epoch),
                );
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
        let caller_ctx = extract_and_inject_context(&req.header);
        if let Err(err) = self
            .authorize_path(
                &req.header,
                &caller_ctx,
                AuthzTarget::for_path(req.path.clone()),
                AuthzOp::Write,
            )
            .await
        {
            let resp_header = header_from_canonical_error(&req.header, None, None, &err);
            return error_response!(GetFileLayoutByPathResponseProto, resp_header);
        }
        let resolved = match self.path_resolver.resolve_inode(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(GetFileLayoutByPathResponseProto, resp_header);
            }
        };
        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::data_io(DataIoOp::Read),
                Some(resolved.mount_ctx.mount_id),
                Some(Self::authz_for_rpc(
                    PathRpcAuthz::GetFileLayoutByPath,
                    AuthzTarget::for_inode(resolved.inode_id),
                )),
                Some(resolved.mount_ctx.owner_group_id.as_raw()),
                Some(resolved.mount_ctx.mount_epoch),
            )
            .await
        {
            return error_response!(GetFileLayoutByPathResponseProto, resp_header);
        }
        let req_ctx = request_context_from_proto(&req.header);
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
            .get_file_layout(GetFileLayoutInput {
                ctx: req_ctx.clone(),
                inode_id: resolved.inode_id,
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
                let header = self.header_or_ok(
                    &req.header,
                    Some(header_from_core_failure(&req_ctx, &failure)),
                    Some(resolved.mount_ctx.owner_group_id.as_raw()),
                    Some(resolved.mount_ctx.mount_epoch),
                );
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
        let caller_ctx = extract_and_inject_context(&req.header);

        if let Err(err) = self
            .authorize_path(
                &req.header,
                &caller_ctx,
                AuthzTarget::for_path(req.path.clone()),
                AuthzOp::Write,
            )
            .await
        {
            let resp_header = header_from_canonical_error(&req.header, None, None, &err);
            return error_response!(OpenWriteByPathResponseProto, resp_header);
        }

        let resolved = match self.path_resolver.resolve_inode(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(OpenWriteByPathResponseProto, resp_header);
            }
        };

        if let Err(err) = self.validate_mount_epoch(&req.header, &resolved.mount_ctx) {
            let resp_header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
            return error_response!(OpenWriteByPathResponseProto, resp_header);
        }
        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::data_io(DataIoOp::OpenWrite).with_leader(),
                Some(resolved.mount_ctx.mount_id),
                Some(Self::authz_for_rpc(
                    PathRpcAuthz::OpenWriteByPath,
                    AuthzTarget::for_inode(resolved.inode_id),
                )),
                Some(resolved.mount_ctx.owner_group_id.as_raw()),
                Some(resolved.mount_ctx.mount_epoch),
            )
            .await
        {
            return error_response!(OpenWriteByPathResponseProto, resp_header);
        }

        let req_ctx = request_context_from_proto(&req.header);
        let mode = match req.mode {
            x if x == WriteModeProto::WriteModeAppend as i32 => crate::inode_lease::WriteMode::Append,
            _ => crate::inode_lease::WriteMode::Write,
        };
        let freshness = Freshness {
            mount_epoch: req.header.as_ref().and_then(|h| h.mount_epoch),
            route_epoch: req.header.as_ref().and_then(|h| h.route_epoch),
            worker_epoch: None,
        };
        match self
            .fs_core
            .open_write(OpenWriteInput {
                ctx: req_ctx.clone(),
                inode_id: resolved.inode_id,
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
                let resp_header = self.header_or_ok(
                    &req.header,
                    Some(header_from_core_failure(&req_ctx, &failure)),
                    Some(resolved.mount_ctx.owner_group_id.as_raw()),
                    Some(resolved.mount_ctx.mount_epoch),
                );
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
        let caller_ctx = extract_and_inject_context(&req.header);
        if let Some(session) = self.fs_core.write_session_for_handle(req.file_handle) {
            let (group_id, mount_epoch) = self.fs_core.mount_hints_for_mount(session.mount_id);
            if let Some(resp_header) = self
                .guard_request(
                    &req.header,
                    &caller_ctx,
                    GuardSpec::data_io(DataIoOp::CloseWrite).with_leader(),
                    Some(session.mount_id),
                    Some(Self::authz_for_rpc(
                        PathRpcAuthz::CloseWriteSession,
                        AuthzTarget::for_session(req.file_handle, Some(session.inode_id)),
                    )),
                    group_id,
                    mount_epoch,
                )
                .await
            {
                return error_response!(CloseWriteSessionResponseProto, resp_header);
            }
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

        let req_ctx = request_context_from_proto(&req.header);
        let freshness = Freshness {
            mount_epoch: req.header.as_ref().and_then(|h| h.mount_epoch),
            route_epoch: req.header.as_ref().and_then(|h| h.route_epoch),
            worker_epoch: None,
        };
        match self
            .fs_core
            .close_write(CloseWriteInput {
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
                let resp_header = self.header_or_ok(
                    &req.header,
                    Some(header_from_core_failure(&req_ctx, &failure)),
                    None,
                    None,
                );
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
        let caller_ctx = extract_and_inject_context(&req.header);
        if let Some(session) = self.fs_core.write_session_for_handle(req.file_handle) {
            let (group_id, mount_epoch) = self.fs_core.mount_hints_for_mount(session.mount_id);
            if let Some(resp_header) = self
                .guard_request(
                    &req.header,
                    &caller_ctx,
                    GuardSpec::data_io(DataIoOp::RenewLease).with_leader(),
                    Some(session.mount_id),
                    Some(Self::authz_for_rpc(
                        PathRpcAuthz::RenewWriteSessionLease,
                        AuthzTarget::for_session(req.file_handle, Some(session.inode_id)),
                    )),
                    group_id,
                    mount_epoch,
                )
                .await
            {
                return error_response!(RenewWriteSessionLeaseResponseProto, resp_header);
            }
        }

        let req_ctx = request_context_from_proto(&req.header);
        let freshness = Freshness {
            mount_epoch: req.header.as_ref().and_then(|h| h.mount_epoch),
            route_epoch: req.header.as_ref().and_then(|h| h.route_epoch),
            worker_epoch: None,
        };
        match self
            .fs_core
            .renew_lease(RenewLeaseInput {
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
                let resp_header = self.header_or_ok(
                    &req.header,
                    Some(header_from_core_failure(&req_ctx, &failure)),
                    None,
                    None,
                );
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
        let caller_ctx = extract_and_inject_context(&req.header);
        let session = match self.fs_core.write_session_for_handle(req.file_handle) {
            Some(session) => session,
            None => {
                let resp_header = need_refresh_header(
                    &req.header,
                    common::header::RpcErrorCode::Fencing,
                    common::error::canonical::RefreshReason::Fencing,
                    "write session not found; refresh and re-open session before replaying fsync",
                    None,
                    None,
                );
                return response_with_header!(FsyncSessionResponseProto::default(), resp_header);
            }
        };
        let (group_id, mount_epoch) = self.fs_core.mount_hints_for_mount(session.mount_id);
        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::data_io(DataIoOp::Fsync).with_leader(),
                Some(session.mount_id),
                Some(Self::authz_for_rpc(
                    PathRpcAuthz::FsyncSession,
                    AuthzTarget::for_session(req.file_handle, Some(session.inode_id)),
                )),
                group_id,
                mount_epoch,
            )
            .await
        {
            return response_with_header!(FsyncSessionResponseProto::default(), resp_header);
        }

        let req_ctx = request_context_from_proto(&req.header);
        let freshness = Freshness {
            mount_epoch: req.header.as_ref().and_then(|h| h.mount_epoch),
            route_epoch: req.header.as_ref().and_then(|h| h.route_epoch),
            worker_epoch: req.worker_epoch,
        };
        match self
            .fs_core
            .fsync_barrier(FsyncBarrierInput {
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
                let resp_header = self.header_or_ok(
                    &req.header,
                    Some(header_from_core_failure(&req_ctx, &failure)),
                    None,
                    None,
                );
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
        let caller_ctx = extract_and_inject_context(&req.header);
        if let Some(session) = self.fs_core.write_session_for_handle(req.file_handle) {
            let (group_id, mount_epoch) = self.fs_core.mount_hints_for_mount(session.mount_id);
            if let Some(resp_header) = self
                .guard_request(
                    &req.header,
                    &caller_ctx,
                    GuardSpec::data_io(DataIoOp::CloseWrite).with_leader(),
                    Some(session.mount_id),
                    Some(Self::authz_for_rpc(
                        PathRpcAuthz::ReleaseSession,
                        AuthzTarget::for_session(req.file_handle, Some(session.inode_id)),
                    )),
                    group_id,
                    mount_epoch,
                )
                .await
            {
                return response_with_header!(ReleaseSessionResponseProto::default(), resp_header);
            }
        } else if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &caller_ctx,
                GuardSpec::metadata_read(),
                None,
                Some(Self::authz_for_rpc(
                    PathRpcAuthz::ReleaseSession,
                    AuthzTarget::for_file_handle(req.file_handle),
                )),
                None,
                None,
            )
            .await
        {
            return response_with_header!(ReleaseSessionResponseProto::default(), resp_header);
        }

        let req_ctx = request_context_from_proto(&req.header);
        match self
            .fs_core
            .release_session(ReleaseSessionInput {
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
                let resp_header = self.header_or_ok(
                    &req.header,
                    Some(header_from_core_failure(&req_ctx, &failure)),
                    None,
                    None,
                );
                response_with_header!(ReleaseSessionResponseProto::default(), resp_header)
            }
        }
    }
}
