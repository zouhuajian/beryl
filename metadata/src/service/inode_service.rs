// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! MetadataInodeServiceProto implementation as a thin RPC adapter.

use super::domain::{
    AccessInput, CloseWriteInput, CloseWriteIntent, CreateInput, FileRange, Freshness, FsyncBarrierInput, GetAttrInput,
    GetFileLayoutInput, GetXattrInput, LinkInput, ListXattrInput, LookupInput, MkdirInput, OpenInput, OpenWriteInput,
    ReadDirInput, ReadlinkInput, ReleaseSessionInput, RemoveXattrInput, RenameInput, RenewLeaseInput, RmdirInput,
    SetAttrInput, SetXattrInput, StatFsInput, SymlinkInput, TruncateInput, UnlinkInput,
};
use super::fs_core::FsCore;
use super::guard::{AuthzCheck, AuthzContext, GuardChain, GuardSpec, LeadershipChecker};
use super::{
    extent_from_proto, extent_to_proto, fencing_to_proto, header_from_canonical_error, header_from_core_failure,
    lease_id_from_proto, lease_id_to_proto, location_to_proto, ok_header_from_core_success,
    presented_fencing_from_proto, request_context_from_proto, write_target_to_proto,
};
use super::{AllowAllAuthz, AuthzOp, AuthzProvider, AuthzScheme, AuthzTarget, InodePermReader};
use crate::data_io::DataIoOp;
use crate::error::{to_canonical_fs, MetadataError, MetadataResult};
use crate::mount::MountTable;
use crate::raft::{AppRaftNode, RocksDBStorage};
use crate::readiness::RootReadinessGate;
use crate::state::StateStore;
use common::header::RequestHeader;
use proto::metadata::metadata_inode_service_proto_server::MetadataInodeServiceProto;
use proto::metadata::*;
use proto::worker::CommitWriteRequestProto;
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status};
use tracing::instrument;
use types::fs::{FileAttrs, InodeData, InodeId, InodeKind};

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

trait HeaderCarrier: Default {
    fn set_header(&mut self, header: proto::common::ResponseHeaderProto);
}

macro_rules! impl_header_carrier {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl HeaderCarrier for $ty {
                fn set_header(&mut self, header: proto::common::ResponseHeaderProto) {
                    self.header = Some(header);
                }
            }
        )+
    };
}

impl_header_carrier!(
    LookupResponseProto,
    GetAttrResponseProto,
    SetAttrResponseProto,
    MkdirResponseProto,
    CreateResponseProto,
    ReadDirResponseProto,
    UnlinkResponseProto,
    RmdirResponseProto,
    FsRenameResponseProto,
    OpenResponseProto,
    ReleaseResponseProto,
    FsyncResponseProto,
    HsyncResponseProto,
    HflushResponseProto,
    OpenWriteResponseProto,
    CloseWriteResponseProto,
    GetFileLayoutResponseProto,
    RenewInodeLeaseResponseProto,
    TruncateResponseProto,
    StatFsResponseProto,
    AccessResponseProto,
    SymlinkResponseProto,
    ReadlinkResponseProto,
    LinkResponseProto,
    SetXattrResponseProto,
    GetXattrResponseProto,
    ListXattrResponseProto,
    RemoveXattrResponseProto
);

type CommitHook = Arc<dyn Fn(CommitWriteRequestProto) -> proto::worker::CommitWriteResponseProto + Send + Sync>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InodeRpcAuthz {
    Lookup,
    GetAttr,
    SetAttr,
    Mkdir,
    Create,
    ReadDir,
    Unlink,
    Rmdir,
    Rename,
    Open,
    Release,
    Fsync,
    Hsync,
    Hflush,
    OpenWrite,
    CloseWrite,
    GetFileLayout,
    RenewInodeLease,
    Truncate,
    StatFs,
    Access,
    Symlink,
    Readlink,
    Link,
    SetXattr,
    GetXattr,
    ListXattr,
    RemoveXattr,
}

#[derive(Clone, Copy)]
enum FsyncFlavor {
    Fsync,
    Hsync,
    Hflush,
}

/// MetadataInodeServiceProto implementation.
pub struct MetadataInodeServiceImpl {
    fs_core: Arc<FsCore>,
    guard_chain: GuardChain,
    inode_perm_reader: Option<Arc<dyn InodePermReader>>,
}

impl MetadataInodeServiceImpl {
    pub fn new(state_store: Arc<dyn StateStore>, mount_table: Arc<MountTable>) -> Self {
        let write_session_manager = Arc::new(crate::write_session::WriteSessionManager::default());
        let inode_lease_manager = Arc::new(crate::inode_lease::InodeLeaseManager::default());
        let worker_commit_hook: Arc<Mutex<Option<CommitHook>>> = Arc::new(Mutex::new(None));
        let fs_core = Arc::new(FsCore::new(
            Arc::clone(&state_store),
            Arc::clone(&mount_table),
            Arc::clone(&write_session_manager),
            Arc::clone(&inode_lease_manager),
            Arc::clone(&worker_commit_hook),
        ));

        let mut guard_chain = GuardChain::new(mount_table);
        guard_chain.set_authz_provider(Arc::new(AllowAllAuthz));

        Self {
            fs_core,
            guard_chain,
            inode_perm_reader: None,
        }
    }

    pub fn with_storage(mut self, db: Arc<RocksDBStorage>) -> Self {
        Arc::get_mut(&mut self.fs_core)
            .expect("fs_core should be uniquely owned during builder configuration")
            .set_storage(db);
        self
    }

    pub fn with_raft_node(mut self, node: Arc<AppRaftNode>) -> Self {
        self.guard_chain.set_leadership_checker(Arc::clone(&node));
        Arc::get_mut(&mut self.fs_core)
            .expect("fs_core should be uniquely owned during builder configuration")
            .set_raft_node(node);
        self
    }

    pub fn with_leadership_checker<T>(mut self, checker: Arc<T>) -> Self
    where
        T: LeadershipChecker + 'static,
    {
        self.guard_chain.set_leadership_checker(checker);
        self
    }

    pub fn with_metrics(mut self, metrics: Arc<crate::metrics::MetadataMetrics>) -> Self {
        Arc::get_mut(&mut self.fs_core)
            .expect("fs_core should be uniquely owned during builder configuration")
            .set_metrics(metrics);
        self
    }

    pub fn with_readiness_gate(mut self, readiness_gate: Arc<RootReadinessGate>) -> Self {
        self.guard_chain.set_readiness_gate(readiness_gate);
        self
    }

    pub fn with_authz_provider(mut self, provider: Arc<dyn AuthzProvider>) -> Self {
        assert!(
            !matches!(provider.scheme(), AuthzScheme::RangerPath),
            "InodeService does not support RangerPath; use FileSystemService"
        );
        self.guard_chain.set_authz_provider(provider);
        self
    }

    pub fn with_inode_perm_reader(mut self, inode_perm_reader: Arc<dyn InodePermReader>) -> Self {
        self.inode_perm_reader = Some(inode_perm_reader);
        self
    }

    #[cfg(test)]
    pub fn set_worker_commit_hook_for_test(&self, hook: CommitHook) {
        self.fs_core.set_worker_commit_hook_for_test(hook);
    }

    #[cfg(debug_assertions)]
    pub fn set_worker_commit_hook_debug(&self, hook: CommitHook) {
        self.fs_core.set_worker_commit_hook_debug(hook);
    }

    #[cfg(test)]
    pub fn clear_worker_commit_hook_for_test(&self) {
        self.fs_core.clear_worker_commit_hook_for_test();
    }

    #[cfg(debug_assertions)]
    pub fn clear_worker_commit_hook_debug(&self) {
        self.fs_core.clear_worker_commit_hook_debug();
    }

    #[cfg(test)]
    pub fn write_session_manager_for_test(&self) -> Arc<crate::write_session::WriteSessionManager> {
        self.fs_core.write_session_manager_for_test()
    }

    #[cfg(debug_assertions)]
    pub fn debug_write_session_manager(&self) -> Arc<crate::write_session::WriteSessionManager> {
        self.fs_core.debug_write_session_manager()
    }

    #[cfg(test)]
    pub fn inode_lease_manager_for_test(&self) -> Arc<crate::inode_lease::InodeLeaseManager> {
        self.fs_core.inode_lease_manager_for_test()
    }

    #[cfg(debug_assertions)]
    pub fn debug_inode_lease_manager(&self) -> Arc<crate::inode_lease::InodeLeaseManager> {
        self.fs_core.debug_inode_lease_manager()
    }

    pub fn with_worker_manager(mut self, worker_manager: Arc<crate::worker::WorkerManager>) -> Self {
        Arc::get_mut(&mut self.fs_core)
            .expect("fs_core should be uniquely owned during builder configuration")
            .set_worker_manager(worker_manager);
        self
    }

    pub fn fs_core(&self) -> Arc<FsCore> {
        Arc::clone(&self.fs_core)
    }

    async fn guard_request(
        &self,
        req_header: &Option<proto::common::RequestHeaderProto>,
        caller_ctx: &RequestHeader,
        mut spec: GuardSpec,
        mount_id: Option<types::ids::MountId>,
        authz: Option<AuthzContext>,
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
                failure.group_id,
                failure.mount_epoch,
                &failure.err,
            )),
        }
    }

    async fn guard_pre_read(
        &self,
        req_header: &Option<proto::common::RequestHeaderProto>,
        caller_ctx: &RequestHeader,
    ) -> Option<proto::common::ResponseHeaderProto> {
        self.guard_request(req_header, caller_ctx, GuardSpec::readiness_only(), None, None)
            .await
    }

    async fn guard_pre_write(
        &self,
        req_header: &Option<proto::common::RequestHeaderProto>,
        caller_ctx: &RequestHeader,
    ) -> Option<proto::common::ResponseHeaderProto> {
        self.guard_request(
            req_header,
            caller_ctx,
            GuardSpec::readiness_only().with_leader(),
            None,
            None,
        )
        .await
    }

    fn authz_for_rpc(rpc: InodeRpcAuthz, targets: Vec<AuthzTarget>) -> AuthzContext {
        let op = match rpc {
            InodeRpcAuthz::Lookup
            | InodeRpcAuthz::GetAttr
            | InodeRpcAuthz::ReadDir
            | InodeRpcAuthz::Open
            | InodeRpcAuthz::GetFileLayout
            | InodeRpcAuthz::StatFs
            | InodeRpcAuthz::Access
            | InodeRpcAuthz::Readlink => AuthzOp::Read,
            InodeRpcAuthz::SetAttr
            | InodeRpcAuthz::Mkdir
            | InodeRpcAuthz::Create
            | InodeRpcAuthz::Release
            | InodeRpcAuthz::Fsync
            | InodeRpcAuthz::Hsync
            | InodeRpcAuthz::Hflush
            | InodeRpcAuthz::OpenWrite
            | InodeRpcAuthz::CloseWrite
            | InodeRpcAuthz::RenewInodeLease
            | InodeRpcAuthz::Truncate
            | InodeRpcAuthz::Symlink
            | InodeRpcAuthz::Link => AuthzOp::Write,
            InodeRpcAuthz::Unlink | InodeRpcAuthz::Rmdir => AuthzOp::Delete,
            InodeRpcAuthz::Rename => AuthzOp::Rename,
            InodeRpcAuthz::SetXattr
            | InodeRpcAuthz::GetXattr
            | InodeRpcAuthz::ListXattr
            | InodeRpcAuthz::RemoveXattr => AuthzOp::Xattr,
        };
        AuthzContext::new(op, targets)
    }

    fn sticky_pre_checks_for_delete(parent_inode_id: InodeId, target_inode_id: Option<InodeId>) -> Vec<AuthzCheck> {
        target_inode_id
            .into_iter()
            .map(|target_inode_id| AuthzCheck {
                op: AuthzOp::Sticky,
                target: AuthzTarget::for_inode(target_inode_id).with_parent(parent_inode_id),
            })
            .collect()
    }

    fn sticky_pre_checks_for_rename(
        src_parent_inode_id: InodeId,
        src_inode_id: Option<InodeId>,
        dst_parent_inode_id: InodeId,
        dst_inode_id: Option<InodeId>,
    ) -> Vec<AuthzCheck> {
        let mut checks = Vec::new();
        if let Some(src_inode_id) = src_inode_id {
            checks.push(AuthzCheck {
                op: AuthzOp::Sticky,
                target: AuthzTarget::for_inode(src_inode_id).with_parent(src_parent_inode_id),
            });
        }
        if let Some(dst_inode_id) = dst_inode_id {
            checks.push(AuthzCheck {
                op: AuthzOp::Sticky,
                target: AuthzTarget::for_inode(dst_inode_id).with_parent(dst_parent_inode_id),
            });
        }
        checks
    }

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

    fn proto_to_file_layout(
        layout: Option<proto::common::FileLayoutProto>,
    ) -> MetadataResult<types::layout::FileLayout> {
        let layout = layout.ok_or_else(|| MetadataError::InvalidArgument("Missing FileLayout".to_string()))?;
        Ok(types::layout::FileLayout::new(
            layout.block_size,
            layout.chunk_size,
            layout.replication as u8,
        ))
    }

    fn inode_id_from_proto(field: Option<proto::fs::InodeIdProto>, field_name: &str) -> MetadataResult<InodeId> {
        let inode_id = field.ok_or_else(|| MetadataError::InvalidArgument(format!("Missing {}", field_name)))?;
        Ok(InodeId::new(inode_id.value))
    }

    fn freshness_from_header(req_header: &Option<proto::common::RequestHeaderProto>) -> Freshness {
        Freshness {
            mount_epoch: req_header.as_ref().and_then(|h| h.mount_epoch),
            route_epoch: req_header.as_ref().and_then(|h| h.route_epoch),
            worker_epoch: None,
        }
    }

    fn response_with_metadata_error<T: HeaderCarrier>(
        req_header: &Option<proto::common::RequestHeaderProto>,
        err: MetadataError,
    ) -> Result<Response<T>, Status> {
        let canonical = to_canonical_fs(err);
        let mut response = T::default();
        response.set_header(header_from_canonical_error(req_header, None, None, &canonical));
        Ok(Response::new(response))
    }

    fn inode_to_proto(inode: &types::fs::Inode) -> proto::fs::InodeProto {
        let data = match &inode.data {
            InodeData::File { extents, lease_epoch } => {
                Some(proto::fs::inode_proto::Data::File(proto::fs::InodeFileProto {
                    extents: extents.iter().map(extent_to_proto).collect(),
                    lease_epoch: *lease_epoch,
                    lease_id: None,
                }))
            }
            InodeData::Dir => Some(proto::fs::inode_proto::Data::Dir(proto::fs::InodeDirectoryProto {})),
            InodeData::Symlink { target } => {
                Some(proto::fs::inode_proto::Data::Symlink(proto::fs::InodeSymlinkProto {
                    target: target.clone(),
                }))
            }
        };

        proto::fs::InodeProto {
            inode_id: Some(proto::fs::InodeIdProto {
                value: inode.inode_id.as_raw(),
            }),
            kind: match inode.kind {
                InodeKind::File => proto::fs::InodeKindProto::InodeKindFile as i32,
                InodeKind::Dir => proto::fs::InodeKindProto::InodeKindDir as i32,
                InodeKind::Symlink => proto::fs::InodeKindProto::InodeKindSymlink as i32,
            },
            attrs: Some(Self::file_attrs_to_proto(&inode.attrs)),
            data,
            mount_id: Some(proto::common::MountIdProto {
                value: inode.mount_id.as_raw(),
            }),
            xattrs: inode
                .xattrs
                .iter()
                .map(|(name, value)| proto::fs::XattrProto {
                    name: name.clone(),
                    value: value.clone(),
                })
                .collect(),
        }
    }

    async fn fsync_like(
        &self,
        req: FsyncRequestProto,
        flavor: FsyncFlavor,
    ) -> Result<proto::common::ResponseHeaderProto, Status> {
        let req_ctx = request_context_from_proto(&req.header);
        if let Some(resp_header) = self.guard_pre_write(&req.header, &req_ctx.caller).await {
            return Ok(resp_header);
        }

        let inode_id = match Self::inode_id_from_proto(req.inode_id.clone(), "inode_id") {
            Ok(inode_id) => inode_id,
            Err(err) => {
                return Ok(header_from_canonical_error(
                    &req.header,
                    None,
                    None,
                    &to_canonical_fs(err),
                ));
            }
        };

        let session_plan = match req.file_handle {
            Some(file_handle) => match self.fs_core.plan_session(&req_ctx, file_handle).await {
                Ok(plan) => Some(plan),
                Err(failure) => return Ok(header_from_core_failure(&req_ctx, &failure)),
            },
            None => None,
        };
        let mount_plan = match self.fs_core.plan_inode_mount(&req_ctx, inode_id).await {
            Ok(plan) => plan,
            Err(failure) => return Ok(header_from_core_failure(&req_ctx, &failure)),
        };

        let rpc = match flavor {
            FsyncFlavor::Fsync => InodeRpcAuthz::Fsync,
            FsyncFlavor::Hsync => InodeRpcAuthz::Hsync,
            FsyncFlavor::Hflush => InodeRpcAuthz::Hflush,
        };
        let authz_inode_id = session_plan
            .as_ref()
            .and_then(|plan| plan.payload.inode_id)
            .unwrap_or(inode_id);
        let authz = Self::authz_for_rpc(rpc, vec![AuthzTarget::for_inode(authz_inode_id)]);
        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &req_ctx.caller,
                GuardSpec::data_io(DataIoOp::Fsync).with_leader(),
                Some(mount_plan.payload.mount_id),
                Some(authz),
            )
            .await
        {
            return Ok(resp_header);
        }

        let freshness = Freshness {
            mount_epoch: req.header.as_ref().and_then(|h| h.mount_epoch),
            route_epoch: req
                .route_epoch
                .or_else(|| req.header.as_ref().and_then(|h| h.route_epoch)),
            worker_epoch: req.worker_epoch,
        };
        let result = match flavor {
            FsyncFlavor::Fsync => {
                self.fs_core
                    .execute_fsync(FsyncBarrierInput {
                        ctx: req_ctx.clone(),
                        inode_id,
                        file_handle: req.file_handle,
                        lease_id: lease_id_from_proto(req.lease_id),
                        lease_epoch: req.lease_epoch,
                        fencing_token: presented_fencing_from_proto(req.fencing_token),
                        target_size: req.target_size,
                        flags: req.flags as i32,
                        freshness,
                    })
                    .await
            }
            FsyncFlavor::Hsync => {
                self.fs_core
                    .execute_hsync(FsyncBarrierInput {
                        ctx: req_ctx.clone(),
                        inode_id,
                        file_handle: req.file_handle,
                        lease_id: lease_id_from_proto(req.lease_id),
                        lease_epoch: req.lease_epoch,
                        fencing_token: presented_fencing_from_proto(req.fencing_token),
                        target_size: req.target_size,
                        flags: req.flags as i32,
                        freshness,
                    })
                    .await
            }
            FsyncFlavor::Hflush => {
                self.fs_core
                    .execute_hflush(FsyncBarrierInput {
                        ctx: req_ctx.clone(),
                        inode_id,
                        file_handle: req.file_handle,
                        lease_id: lease_id_from_proto(req.lease_id),
                        lease_epoch: req.lease_epoch,
                        fencing_token: presented_fencing_from_proto(req.fencing_token),
                        target_size: req.target_size,
                        flags: req.flags as i32,
                        freshness,
                    })
                    .await
            }
        };

        Ok(match result {
            Ok(success) => ok_header_from_core_success(&req_ctx, &success),
            Err(failure) => header_from_core_failure(&req_ctx, &failure),
        })
    }
}

#[tonic::async_trait]
impl MetadataInodeServiceProto for MetadataInodeServiceImpl {
    #[instrument(skip(self), fields(call_id, client_id))]
    async fn lookup(&self, request: Request<LookupRequestProto>) -> Result<Response<LookupResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);

        let parent_inode_id = match Self::inode_id_from_proto(req.parent_inode_id.clone(), "parent_inode_id") {
            Ok(parent_inode_id) => parent_inode_id,
            Err(err) => return Self::response_with_metadata_error(&req.header, err),
        };
        if let Some(resp_header) = self.guard_pre_read(&req.header, &req_ctx.caller).await {
            return error_response!(LookupResponseProto, resp_header);
        }

        let authz = Self::authz_for_rpc(InodeRpcAuthz::Lookup, vec![AuthzTarget::for_inode(parent_inode_id)]);
        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &req_ctx.caller,
                GuardSpec::metadata_read(),
                None,
                Some(authz),
            )
            .await
        {
            return error_response!(LookupResponseProto, resp_header);
        }

        match self
            .fs_core
            .execute_lookup(LookupInput {
                ctx: req_ctx.clone(),
                parent_inode_id,
                name: req.name,
            })
            .await
        {
            Ok(success) => {
                let header = ok_header_from_core_success(&req_ctx, &success);
                let inode = success.payload.inode;
                response_with_header!(
                    LookupResponseProto {
                        inode: Some(Self::inode_to_proto(&inode)),
                        attrs: Some(Self::file_attrs_to_proto(&inode.attrs)),
                        ..Default::default()
                    },
                    header
                )
            }
            Err(failure) => error_response!(LookupResponseProto, header_from_core_failure(&req_ctx, &failure)),
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn get_attr(&self, request: Request<GetAttrRequestProto>) -> Result<Response<GetAttrResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);

        let inode_id = match Self::inode_id_from_proto(req.inode_id.clone(), "inode_id") {
            Ok(inode_id) => inode_id,
            Err(err) => return Self::response_with_metadata_error(&req.header, err),
        };
        if let Some(resp_header) = self.guard_pre_read(&req.header, &req_ctx.caller).await {
            return error_response!(GetAttrResponseProto, resp_header);
        }

        let authz = Self::authz_for_rpc(InodeRpcAuthz::GetAttr, vec![AuthzTarget::for_inode(inode_id)]);
        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &req_ctx.caller,
                GuardSpec::metadata_read(),
                None,
                Some(authz),
            )
            .await
        {
            return error_response!(GetAttrResponseProto, resp_header);
        }

        match self
            .fs_core
            .execute_get_attr(GetAttrInput {
                ctx: req_ctx.clone(),
                inode_id,
            })
            .await
        {
            Ok(success) => response_with_header!(
                GetAttrResponseProto {
                    attrs: Some(Self::file_attrs_to_proto(&success.payload.attrs)),
                    ..Default::default()
                },
                ok_header_from_core_success(&req_ctx, &success)
            ),
            Err(failure) => error_response!(GetAttrResponseProto, header_from_core_failure(&req_ctx, &failure)),
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn set_attr(&self, request: Request<SetAttrRequestProto>) -> Result<Response<SetAttrResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);

        let inode_id = match Self::inode_id_from_proto(req.inode_id.clone(), "inode_id") {
            Ok(inode_id) => inode_id,
            Err(err) => return Self::response_with_metadata_error(&req.header, err),
        };
        let attrs = match Self::proto_to_file_attrs(req.attrs) {
            Ok(attrs) => attrs,
            Err(err) => return Self::response_with_metadata_error(&req.header, err),
        };

        if let Some(resp_header) = self.guard_pre_write(&req.header, &req_ctx.caller).await {
            return error_response!(SetAttrResponseProto, resp_header);
        }

        let authz = Self::authz_for_rpc(InodeRpcAuthz::SetAttr, vec![AuthzTarget::for_inode(inode_id)]);
        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &req_ctx.caller,
                GuardSpec::metadata_write(),
                None,
                Some(authz),
            )
            .await
        {
            return error_response!(SetAttrResponseProto, resp_header);
        }

        let freshness = Self::freshness_from_header(&req.header);
        match self
            .fs_core
            .execute_set_attr(SetAttrInput {
                ctx: req_ctx.clone(),
                inode_id,
                mask: req.mask,
                attrs,
                freshness,
            })
            .await
        {
            Ok(success) => {
                if let Some(reader) = self.inode_perm_reader.as_ref() {
                    reader.invalidate(inode_id);
                }
                response_with_header!(
                    SetAttrResponseProto {
                        attrs: Some(Self::file_attrs_to_proto(&success.payload.attrs)),
                        ..Default::default()
                    },
                    ok_header_from_core_success(&req_ctx, &success)
                )
            }
            Err(failure) => error_response!(SetAttrResponseProto, header_from_core_failure(&req_ctx, &failure)),
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn mkdir(&self, request: Request<MkdirRequestProto>) -> Result<Response<MkdirResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);

        let parent_inode_id = match Self::inode_id_from_proto(req.parent_inode_id.clone(), "parent_inode_id") {
            Ok(parent_inode_id) => parent_inode_id,
            Err(err) => return Self::response_with_metadata_error(&req.header, err),
        };
        let attrs = match Self::proto_to_file_attrs(req.attrs) {
            Ok(attrs) => attrs,
            Err(err) => return Self::response_with_metadata_error(&req.header, err),
        };

        if let Some(resp_header) = self.guard_pre_write(&req.header, &req_ctx.caller).await {
            return error_response!(MkdirResponseProto, resp_header);
        }

        let authz = Self::authz_for_rpc(InodeRpcAuthz::Mkdir, vec![AuthzTarget::for_inode(parent_inode_id)]);
        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &req_ctx.caller,
                GuardSpec::metadata_write(),
                None,
                Some(authz),
            )
            .await
        {
            return error_response!(MkdirResponseProto, resp_header);
        }

        let freshness = Self::freshness_from_header(&req.header);
        match self
            .fs_core
            .execute_mkdir(MkdirInput {
                ctx: req_ctx.clone(),
                parent_inode_id,
                name: req.name,
                attrs,
                freshness,
            })
            .await
        {
            Ok(success) => {
                let attrs_proto = success.payload.attrs.as_ref().map(Self::file_attrs_to_proto);
                let inode_proto = success.payload.inode_id.map(|inode_id| proto::fs::InodeProto {
                    inode_id: Some(proto::fs::InodeIdProto {
                        value: inode_id.as_raw(),
                    }),
                    kind: proto::fs::InodeKindProto::InodeKindDir as i32,
                    attrs: attrs_proto.clone(),
                    data: Some(proto::fs::inode_proto::Data::Dir(proto::fs::InodeDirectoryProto {})),
                    mount_id: None,
                    xattrs: vec![],
                });
                response_with_header!(
                    MkdirResponseProto {
                        inode: inode_proto,
                        attrs: attrs_proto,
                        ..Default::default()
                    },
                    ok_header_from_core_success(&req_ctx, &success)
                )
            }
            Err(failure) => error_response!(MkdirResponseProto, header_from_core_failure(&req_ctx, &failure)),
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn create(&self, request: Request<CreateRequestProto>) -> Result<Response<CreateResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);

        let parent_inode_id = match Self::inode_id_from_proto(req.parent_inode_id.clone(), "parent_inode_id") {
            Ok(parent_inode_id) => parent_inode_id,
            Err(err) => return Self::response_with_metadata_error(&req.header, err),
        };
        let attrs = match Self::proto_to_file_attrs(req.attrs) {
            Ok(attrs) => attrs,
            Err(err) => return Self::response_with_metadata_error(&req.header, err),
        };
        let layout = match Self::proto_to_file_layout(req.layout) {
            Ok(layout) => layout,
            Err(err) => return Self::response_with_metadata_error(&req.header, err),
        };

        if let Some(resp_header) = self.guard_pre_write(&req.header, &req_ctx.caller).await {
            return error_response!(CreateResponseProto, resp_header);
        }

        let authz = Self::authz_for_rpc(InodeRpcAuthz::Create, vec![AuthzTarget::for_inode(parent_inode_id)]);
        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &req_ctx.caller,
                GuardSpec::metadata_write(),
                None,
                Some(authz),
            )
            .await
        {
            return error_response!(CreateResponseProto, resp_header);
        }

        let freshness = Self::freshness_from_header(&req.header);
        match self
            .fs_core
            .execute_create(CreateInput {
                ctx: req_ctx.clone(),
                parent_inode_id,
                name: req.name,
                attrs,
                layout,
                freshness,
            })
            .await
        {
            Ok(success) => {
                let attrs_proto = success.payload.attrs.as_ref().map(Self::file_attrs_to_proto);
                let inode_proto = success.payload.inode_id.map(|inode_id| proto::fs::InodeProto {
                    inode_id: Some(proto::fs::InodeIdProto {
                        value: inode_id.as_raw(),
                    }),
                    kind: proto::fs::InodeKindProto::InodeKindFile as i32,
                    attrs: attrs_proto.clone(),
                    data: None,
                    mount_id: None,
                    xattrs: vec![],
                });
                response_with_header!(
                    CreateResponseProto {
                        inode: inode_proto,
                        attrs: attrs_proto,
                        data_handle_id: success
                            .payload
                            .data_handle_id
                            .map(types::ids::DataHandleId::as_raw)
                            .unwrap_or_default(),
                        ..Default::default()
                    },
                    ok_header_from_core_success(&req_ctx, &success)
                )
            }
            Err(failure) => error_response!(CreateResponseProto, header_from_core_failure(&req_ctx, &failure)),
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn read_dir(&self, request: Request<ReadDirRequestProto>) -> Result<Response<ReadDirResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);

        let parent_inode_id = match Self::inode_id_from_proto(req.parent_inode_id.clone(), "parent_inode_id") {
            Ok(parent_inode_id) => parent_inode_id,
            Err(err) => return Self::response_with_metadata_error(&req.header, err),
        };

        if let Some(resp_header) = self.guard_pre_read(&req.header, &req_ctx.caller).await {
            return error_response!(ReadDirResponseProto, resp_header);
        }

        let authz = Self::authz_for_rpc(InodeRpcAuthz::ReadDir, vec![AuthzTarget::for_inode(parent_inode_id)]);
        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &req_ctx.caller,
                GuardSpec::metadata_read(),
                None,
                Some(authz),
            )
            .await
        {
            return error_response!(ReadDirResponseProto, resp_header);
        }

        let cursor_key = if req.cursor_key.is_empty() {
            None
        } else {
            Some(req.cursor_key)
        };
        let max_entries = if req.max_entries == 0 {
            None
        } else {
            Some(req.max_entries as usize)
        };

        match self
            .fs_core
            .execute_read_dir(ReadDirInput {
                ctx: req_ctx.clone(),
                parent_inode_id,
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
                    .iter()
                    .map(|entry| proto::fs::DirEntryProto {
                        name: entry.name.clone(),
                        inode_id: Some(proto::fs::InodeIdProto {
                            value: entry.inode_id.as_raw(),
                        }),
                        kind: entry
                            .kind
                            .map(|kind| match kind {
                                InodeKind::File => proto::fs::InodeKindProto::InodeKindFile as i32,
                                InodeKind::Dir => proto::fs::InodeKindProto::InodeKindDir as i32,
                                InodeKind::Symlink => proto::fs::InodeKindProto::InodeKindSymlink as i32,
                            })
                            .unwrap_or(proto::fs::InodeKindProto::InodeKindUnspecified as i32),
                        attrs: entry.attrs.as_ref().map(Self::file_attrs_to_proto),
                    })
                    .collect();

                response_with_header!(
                    ReadDirResponseProto {
                        entries,
                        next_cursor_key: payload.next_cursor_key,
                        eof: payload.eof,
                        ..Default::default()
                    },
                    header
                )
            }
            Err(failure) => error_response!(ReadDirResponseProto, header_from_core_failure(&req_ctx, &failure)),
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn unlink(&self, request: Request<UnlinkRequestProto>) -> Result<Response<UnlinkResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);

        let parent_inode_id = match Self::inode_id_from_proto(req.parent_inode_id.clone(), "parent_inode_id") {
            Ok(parent_inode_id) => parent_inode_id,
            Err(err) => return Self::response_with_metadata_error(&req.header, err),
        };

        if let Some(resp_header) = self.guard_pre_write(&req.header, &req_ctx.caller).await {
            return error_response!(UnlinkResponseProto, resp_header);
        }

        let plan = match self.fs_core.plan_unlink(&req_ctx, parent_inode_id, &req.name).await {
            Ok(plan) => plan,
            Err(failure) => return error_response!(UnlinkResponseProto, header_from_core_failure(&req_ctx, &failure)),
        };

        let mut authz = Self::authz_for_rpc(
            InodeRpcAuthz::Unlink,
            vec![AuthzTarget::for_inode(plan.payload.parent_inode_id)],
        );
        authz.pre_checks =
            Self::sticky_pre_checks_for_delete(plan.payload.parent_inode_id, plan.payload.target_inode_id);
        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &req_ctx.caller,
                GuardSpec::metadata_write(),
                None,
                Some(authz),
            )
            .await
        {
            return error_response!(UnlinkResponseProto, resp_header);
        }

        let freshness = Self::freshness_from_header(&req.header);
        match self
            .fs_core
            .execute_unlink(UnlinkInput {
                ctx: req_ctx.clone(),
                parent_inode_id,
                name: req.name,
                freshness,
            })
            .await
        {
            Ok(success) => response_with_header!(
                UnlinkResponseProto::default(),
                ok_header_from_core_success(&req_ctx, &success)
            ),
            Err(failure) => error_response!(UnlinkResponseProto, header_from_core_failure(&req_ctx, &failure)),
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn rmdir(&self, request: Request<RmdirRequestProto>) -> Result<Response<RmdirResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);

        let parent_inode_id = match Self::inode_id_from_proto(req.parent_inode_id.clone(), "parent_inode_id") {
            Ok(parent_inode_id) => parent_inode_id,
            Err(err) => return Self::response_with_metadata_error(&req.header, err),
        };

        if let Some(resp_header) = self.guard_pre_write(&req.header, &req_ctx.caller).await {
            return error_response!(RmdirResponseProto, resp_header);
        }

        let plan = match self.fs_core.plan_rmdir(&req_ctx, parent_inode_id, &req.name).await {
            Ok(plan) => plan,
            Err(failure) => return error_response!(RmdirResponseProto, header_from_core_failure(&req_ctx, &failure)),
        };

        let mut authz = Self::authz_for_rpc(
            InodeRpcAuthz::Rmdir,
            vec![AuthzTarget::for_inode(plan.payload.parent_inode_id)],
        );
        authz.pre_checks =
            Self::sticky_pre_checks_for_delete(plan.payload.parent_inode_id, plan.payload.target_inode_id);
        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &req_ctx.caller,
                GuardSpec::metadata_write(),
                None,
                Some(authz),
            )
            .await
        {
            return error_response!(RmdirResponseProto, resp_header);
        }

        let freshness = Self::freshness_from_header(&req.header);
        match self
            .fs_core
            .execute_rmdir(RmdirInput {
                ctx: req_ctx.clone(),
                parent_inode_id,
                name: req.name,
                freshness,
            })
            .await
        {
            Ok(success) => response_with_header!(
                RmdirResponseProto::default(),
                ok_header_from_core_success(&req_ctx, &success)
            ),
            Err(failure) => error_response!(RmdirResponseProto, header_from_core_failure(&req_ctx, &failure)),
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn rename(&self, request: Request<FsRenameRequestProto>) -> Result<Response<FsRenameResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);

        let src_parent_inode_id =
            match Self::inode_id_from_proto(req.src_parent_inode_id.clone(), "src_parent_inode_id") {
                Ok(inode_id) => inode_id,
                Err(err) => return Self::response_with_metadata_error(&req.header, err),
            };
        let dst_parent_inode_id =
            match Self::inode_id_from_proto(req.dst_parent_inode_id.clone(), "dst_parent_inode_id") {
                Ok(inode_id) => inode_id,
                Err(err) => return Self::response_with_metadata_error(&req.header, err),
            };

        if let Some(resp_header) = self.guard_pre_write(&req.header, &req_ctx.caller).await {
            return error_response!(FsRenameResponseProto, resp_header);
        }

        let plan = match self
            .fs_core
            .plan_rename(
                &req_ctx,
                src_parent_inode_id,
                &req.src_name,
                dst_parent_inode_id,
                &req.dst_name,
            )
            .await
        {
            Ok(plan) => plan,
            Err(failure) => {
                return error_response!(FsRenameResponseProto, header_from_core_failure(&req_ctx, &failure));
            }
        };

        let mut authz = Self::authz_for_rpc(
            InodeRpcAuthz::Rename,
            vec![
                AuthzTarget::for_inode(plan.payload.src_parent_inode_id),
                AuthzTarget::for_inode(plan.payload.dst_parent_inode_id),
            ],
        );
        authz.pre_checks = Self::sticky_pre_checks_for_rename(
            plan.payload.src_parent_inode_id,
            plan.payload.src_inode_id,
            plan.payload.dst_parent_inode_id,
            plan.payload.dst_inode_id,
        );
        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &req_ctx.caller,
                GuardSpec::metadata_write(),
                None,
                Some(authz),
            )
            .await
        {
            return error_response!(FsRenameResponseProto, resp_header);
        }

        let freshness = Self::freshness_from_header(&req.header);
        match self
            .fs_core
            .execute_rename(RenameInput {
                ctx: req_ctx.clone(),
                src_parent_inode_id,
                src_name: req.src_name,
                dst_parent_inode_id,
                dst_name: req.dst_name,
                flags: req.flags,
                freshness,
            })
            .await
        {
            Ok(success) => {
                response_with_header!(
                    FsRenameResponseProto::default(),
                    ok_header_from_core_success(&req_ctx, &success)
                )
            }
            Err(failure) => error_response!(FsRenameResponseProto, header_from_core_failure(&req_ctx, &failure)),
        }
    }

    async fn open(&self, request: Request<OpenRequestProto>) -> Result<Response<OpenResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);

        let inode_id = match Self::inode_id_from_proto(req.inode_id.clone(), "inode_id") {
            Ok(inode_id) => inode_id,
            Err(err) => return Self::response_with_metadata_error(&req.header, err),
        };

        if let Some(resp_header) = self.guard_pre_read(&req.header, &req_ctx.caller).await {
            return error_response!(OpenResponseProto, resp_header);
        }

        let authz = Self::authz_for_rpc(InodeRpcAuthz::Open, vec![AuthzTarget::for_inode(inode_id)]);
        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &req_ctx.caller,
                GuardSpec::metadata_read(),
                None,
                Some(authz),
            )
            .await
        {
            return error_response!(OpenResponseProto, resp_header);
        }

        match self
            .fs_core
            .execute_open(OpenInput {
                ctx: req_ctx.clone(),
                inode_id,
                flags: req.flags as i32,
            })
            .await
        {
            Ok(success) => response_with_header!(
                OpenResponseProto {
                    file_handle: success.payload.file_handle,
                    ..Default::default()
                },
                ok_header_from_core_success(&req_ctx, &success)
            ),
            Err(failure) => error_response!(OpenResponseProto, header_from_core_failure(&req_ctx, &failure)),
        }
    }

    async fn release(&self, request: Request<ReleaseRequestProto>) -> Result<Response<ReleaseResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);

        if let Some(resp_header) = self.guard_pre_write(&req.header, &req_ctx.caller).await {
            return error_response!(ReleaseResponseProto, resp_header);
        }

        let plan = match self.fs_core.plan_session(&req_ctx, req.file_handle).await {
            Ok(plan) => plan,
            Err(failure) => return error_response!(ReleaseResponseProto, header_from_core_failure(&req_ctx, &failure)),
        };

        let authz_targets = plan
            .payload
            .inode_id
            .into_iter()
            .map(AuthzTarget::for_inode)
            .collect::<Vec<_>>();
        let authz = Self::authz_for_rpc(InodeRpcAuthz::Release, authz_targets);
        let spec = if plan.payload.mount_id.is_some() {
            GuardSpec::data_io(DataIoOp::CloseWrite).with_leader()
        } else {
            GuardSpec::metadata_write()
        };
        if let Some(resp_header) = self
            .guard_request(&req.header, &req_ctx.caller, spec, plan.payload.mount_id, Some(authz))
            .await
        {
            return error_response!(ReleaseResponseProto, resp_header);
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
                    ReleaseResponseProto::default(),
                    ok_header_from_core_success(&req_ctx, &success)
                )
            }
            Err(failure) => error_response!(ReleaseResponseProto, header_from_core_failure(&req_ctx, &failure)),
        }
    }

    async fn fsync(&self, request: Request<FsyncRequestProto>) -> Result<Response<FsyncResponseProto>, Status> {
        let resp_header = self.fsync_like(request.into_inner(), FsyncFlavor::Fsync).await?;
        response_with_header!(FsyncResponseProto::default(), resp_header)
    }

    async fn hsync(&self, request: Request<HsyncRequestProto>) -> Result<Response<HsyncResponseProto>, Status> {
        let req = request.into_inner();
        let fsync_req = match req.fsync {
            Some(fsync_req) => fsync_req,
            None => {
                let canonical = to_canonical_fs(MetadataError::InvalidArgument("missing fsync body".to_string()));
                let resp_header = header_from_canonical_error(&None, None, None, &canonical);
                return error_response!(HsyncResponseProto, resp_header);
            }
        };
        let resp_header = self.fsync_like(fsync_req, FsyncFlavor::Hsync).await?;
        response_with_header!(HsyncResponseProto::default(), resp_header)
    }

    async fn hflush(&self, request: Request<HflushRequestProto>) -> Result<Response<HflushResponseProto>, Status> {
        let req = request.into_inner();
        let fsync_req = match req.fsync {
            Some(fsync_req) => fsync_req,
            None => {
                let canonical = to_canonical_fs(MetadataError::InvalidArgument("missing fsync body".to_string()));
                let resp_header = header_from_canonical_error(&None, None, None, &canonical);
                return error_response!(HflushResponseProto, resp_header);
            }
        };
        let resp_header = self.fsync_like(fsync_req, FsyncFlavor::Hflush).await?;
        response_with_header!(HflushResponseProto::default(), resp_header)
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn open_write(
        &self,
        request: Request<OpenWriteRequestProto>,
    ) -> Result<Response<OpenWriteResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);

        let inode_id = match Self::inode_id_from_proto(req.inode_id.clone(), "inode_id") {
            Ok(inode_id) => inode_id,
            Err(err) => return Self::response_with_metadata_error(&req.header, err),
        };

        if let Some(resp_header) = self.guard_pre_write(&req.header, &req_ctx.caller).await {
            return error_response!(OpenWriteResponseProto, resp_header);
        }

        let plan = match self.fs_core.plan_inode_mount(&req_ctx, inode_id).await {
            Ok(plan) => plan,
            Err(failure) => {
                return error_response!(OpenWriteResponseProto, header_from_core_failure(&req_ctx, &failure));
            }
        };
        let authz = Self::authz_for_rpc(InodeRpcAuthz::OpenWrite, vec![AuthzTarget::for_inode(inode_id)]);
        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &req_ctx.caller,
                GuardSpec::data_io(DataIoOp::OpenWrite).with_leader(),
                Some(plan.payload.mount_id),
                Some(authz),
            )
            .await
        {
            return error_response!(OpenWriteResponseProto, resp_header);
        }

        let mode = match req.mode {
            x if x == WriteModeProto::WriteModeAppend as i32 => crate::inode_lease::WriteMode::Append,
            _ => crate::inode_lease::WriteMode::Write,
        };
        let freshness = Self::freshness_from_header(&req.header);
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
            Ok(success) => response_with_header!(
                OpenWriteResponseProto {
                    file_handle: success.payload.session_key.file_handle,
                    lease_id: Some(lease_id_to_proto(success.payload.session_key.lease_id)),
                    fencing_token: Some(fencing_to_proto(success.payload.session_key.fencing_token)),
                    write_targets: success
                        .payload
                        .write_targets
                        .iter()
                        .map(write_target_to_proto)
                        .collect(),
                    base_size: success.payload.base_size,
                    open_epoch: success.payload.session_key.open_epoch,
                    lease_epoch: success.payload.session_key.lease_epoch,
                    expires_at_ms: success.payload.expires_at_ms,
                    ..Default::default()
                },
                ok_header_from_core_success(&req_ctx, &success)
            ),
            Err(failure) => error_response!(OpenWriteResponseProto, header_from_core_failure(&req_ctx, &failure)),
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn close_write(
        &self,
        request: Request<CloseWriteRequestProto>,
    ) -> Result<Response<CloseWriteResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);

        let extents = match req
            .extents
            .clone()
            .into_iter()
            .map(extent_from_proto)
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(extents) => extents,
            Err(err) => return Self::response_with_metadata_error(&req.header, err),
        };

        if let Some(resp_header) = self.guard_pre_write(&req.header, &req_ctx.caller).await {
            return error_response!(CloseWriteResponseProto, resp_header);
        }

        let plan = match self.fs_core.plan_session(&req_ctx, req.file_handle).await {
            Ok(plan) => plan,
            Err(failure) => {
                return error_response!(CloseWriteResponseProto, header_from_core_failure(&req_ctx, &failure));
            }
        };

        let authz_targets = plan
            .payload
            .inode_id
            .into_iter()
            .map(AuthzTarget::for_inode)
            .collect::<Vec<_>>();
        let authz = Self::authz_for_rpc(InodeRpcAuthz::CloseWrite, authz_targets);
        let spec = if plan.payload.mount_id.is_some() {
            GuardSpec::data_io(DataIoOp::CloseWrite).with_leader()
        } else {
            GuardSpec::metadata_write()
        };
        if let Some(resp_header) = self
            .guard_request(&req.header, &req_ctx.caller, spec, plan.payload.mount_id, Some(authz))
            .await
        {
            return error_response!(CloseWriteResponseProto, resp_header);
        }

        let freshness = Self::freshness_from_header(&req.header);
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
            Ok(success) => response_with_header!(
                CloseWriteResponseProto {
                    committed_size: success.payload.committed_size,
                    file_version: success.payload.file_version,
                    ..Default::default()
                },
                ok_header_from_core_success(&req_ctx, &success)
            ),
            Err(failure) => error_response!(CloseWriteResponseProto, header_from_core_failure(&req_ctx, &failure)),
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn get_file_layout(
        &self,
        request: Request<GetFileLayoutRequestProto>,
    ) -> Result<Response<GetFileLayoutResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);

        let inode_id = match Self::inode_id_from_proto(req.inode_id.clone(), "inode_id") {
            Ok(inode_id) => inode_id,
            Err(err) => return Self::response_with_metadata_error(&req.header, err),
        };

        if let Some(resp_header) = self.guard_pre_read(&req.header, &req_ctx.caller).await {
            return error_response!(GetFileLayoutResponseProto, resp_header);
        }

        let plan = match self.fs_core.plan_inode_mount(&req_ctx, inode_id).await {
            Ok(plan) => plan,
            Err(failure) => {
                return error_response!(GetFileLayoutResponseProto, header_from_core_failure(&req_ctx, &failure));
            }
        };
        let authz = Self::authz_for_rpc(InodeRpcAuthz::GetFileLayout, vec![AuthzTarget::for_inode(inode_id)]);
        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &req_ctx.caller,
                GuardSpec::data_io(DataIoOp::Read),
                Some(plan.payload.mount_id),
                Some(authz),
            )
            .await
        {
            return error_response!(GetFileLayoutResponseProto, resp_header);
        }

        let range = req.range.map(|range| FileRange {
            offset: range.offset,
            len: range.len as u64,
        });
        let freshness = Self::freshness_from_header(&req.header);
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
            Ok(success) => response_with_header!(
                GetFileLayoutResponseProto {
                    extents: success.payload.extents.iter().map(extent_to_proto).collect(),
                    file_size: success.payload.file_size,
                    locations: success.payload.locations.iter().map(location_to_proto).collect(),
                    ..Default::default()
                },
                ok_header_from_core_success(&req_ctx, &success)
            ),
            Err(failure) => error_response!(GetFileLayoutResponseProto, header_from_core_failure(&req_ctx, &failure)),
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn renew_inode_lease(
        &self,
        request: Request<RenewInodeLeaseRequestProto>,
    ) -> Result<Response<RenewInodeLeaseResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);

        if let Some(resp_header) = self.guard_pre_write(&req.header, &req_ctx.caller).await {
            return error_response!(RenewInodeLeaseResponseProto, resp_header);
        }

        let plan = match self.fs_core.plan_session(&req_ctx, req.file_handle).await {
            Ok(plan) => plan,
            Err(failure) => {
                return error_response!(
                    RenewInodeLeaseResponseProto,
                    header_from_core_failure(&req_ctx, &failure)
                );
            }
        };
        let authz_targets = plan
            .payload
            .inode_id
            .into_iter()
            .map(AuthzTarget::for_inode)
            .collect::<Vec<_>>();
        let authz = Self::authz_for_rpc(InodeRpcAuthz::RenewInodeLease, authz_targets);
        let spec = if plan.payload.mount_id.is_some() {
            GuardSpec::data_io(DataIoOp::RenewLease).with_leader()
        } else {
            GuardSpec::metadata_write()
        };
        if let Some(resp_header) = self
            .guard_request(&req.header, &req_ctx.caller, spec, plan.payload.mount_id, Some(authz))
            .await
        {
            return error_response!(RenewInodeLeaseResponseProto, resp_header);
        }

        let freshness = Self::freshness_from_header(&req.header);
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
                RenewInodeLeaseResponseProto {
                    expires_at_ms: success.payload.expires_at_ms,
                    ..Default::default()
                },
                ok_header_from_core_success(&req_ctx, &success)
            ),
            Err(failure) => {
                error_response!(
                    RenewInodeLeaseResponseProto,
                    header_from_core_failure(&req_ctx, &failure)
                )
            }
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn truncate(
        &self,
        request: Request<TruncateRequestProto>,
    ) -> Result<Response<TruncateResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);

        let inode_id = match Self::inode_id_from_proto(req.inode_id.clone(), "inode_id") {
            Ok(inode_id) => inode_id,
            Err(err) => return Self::response_with_metadata_error(&req.header, err),
        };

        if let Some(resp_header) = self.guard_pre_write(&req.header, &req_ctx.caller).await {
            return error_response!(TruncateResponseProto, resp_header);
        }

        let plan = match self.fs_core.plan_inode_mount(&req_ctx, inode_id).await {
            Ok(plan) => plan,
            Err(failure) => {
                return error_response!(TruncateResponseProto, header_from_core_failure(&req_ctx, &failure))
            }
        };

        let authz = Self::authz_for_rpc(InodeRpcAuthz::Truncate, vec![AuthzTarget::for_inode(inode_id)]);
        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &req_ctx.caller,
                GuardSpec::data_io(DataIoOp::Truncate).with_leader(),
                Some(plan.payload.mount_id),
                Some(authz),
            )
            .await
        {
            return error_response!(TruncateResponseProto, resp_header);
        }

        let freshness = Self::freshness_from_header(&req.header);
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
            Ok(success) => response_with_header!(
                TruncateResponseProto {
                    new_size: success.payload.new_size,
                    ..Default::default()
                },
                ok_header_from_core_success(&req_ctx, &success)
            ),
            Err(failure) => error_response!(TruncateResponseProto, header_from_core_failure(&req_ctx, &failure)),
        }
    }

    async fn stat_fs(&self, request: Request<StatFsRequestProto>) -> Result<Response<StatFsResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);

        let inode_id = match Self::inode_id_from_proto(req.inode_id.clone(), "inode_id") {
            Ok(inode_id) => inode_id,
            Err(err) => return Self::response_with_metadata_error(&req.header, err),
        };

        if let Some(resp_header) = self.guard_pre_read(&req.header, &req_ctx.caller).await {
            return error_response!(StatFsResponseProto, resp_header);
        }

        let authz = Self::authz_for_rpc(InodeRpcAuthz::StatFs, vec![AuthzTarget::for_inode(inode_id)]);
        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &req_ctx.caller,
                GuardSpec::metadata_read(),
                None,
                Some(authz),
            )
            .await
        {
            return error_response!(StatFsResponseProto, resp_header);
        }

        match self
            .fs_core
            .execute_stat_fs(StatFsInput {
                ctx: req_ctx.clone(),
                inode_id,
            })
            .await
        {
            Ok(success) => response_with_header!(
                StatFsResponseProto {
                    total_blocks: success.payload.total_blocks,
                    free_blocks: success.payload.free_blocks,
                    available_blocks: success.payload.available_blocks,
                    block_size: success.payload.block_size,
                    ..Default::default()
                },
                ok_header_from_core_success(&req_ctx, &success)
            ),
            Err(failure) => error_response!(StatFsResponseProto, header_from_core_failure(&req_ctx, &failure)),
        }
    }

    async fn access(&self, request: Request<AccessRequestProto>) -> Result<Response<AccessResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);

        let inode_id = match Self::inode_id_from_proto(req.inode_id.clone(), "inode_id") {
            Ok(inode_id) => inode_id,
            Err(err) => return Self::response_with_metadata_error(&req.header, err),
        };

        if let Some(resp_header) = self.guard_pre_read(&req.header, &req_ctx.caller).await {
            return error_response!(AccessResponseProto, resp_header);
        }

        let authz = Self::authz_for_rpc(InodeRpcAuthz::Access, vec![AuthzTarget::for_inode(inode_id)]);
        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &req_ctx.caller,
                GuardSpec::metadata_read(),
                None,
                Some(authz),
            )
            .await
        {
            return error_response!(AccessResponseProto, resp_header);
        }

        match self
            .fs_core
            .execute_access(AccessInput {
                ctx: req_ctx.clone(),
                inode_id,
                mode: req.mode,
            })
            .await
        {
            Ok(success) => response_with_header!(
                AccessResponseProto {
                    granted: success.payload.granted,
                    ..Default::default()
                },
                ok_header_from_core_success(&req_ctx, &success)
            ),
            Err(failure) => error_response!(AccessResponseProto, header_from_core_failure(&req_ctx, &failure)),
        }
    }

    async fn symlink(&self, request: Request<SymlinkRequestProto>) -> Result<Response<SymlinkResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);

        let parent_inode_id = match Self::inode_id_from_proto(req.parent_inode_id.clone(), "parent_inode_id") {
            Ok(inode_id) => inode_id,
            Err(err) => return Self::response_with_metadata_error(&req.header, err),
        };
        let attrs = match Self::proto_to_file_attrs(req.attrs) {
            Ok(attrs) => attrs,
            Err(err) => return Self::response_with_metadata_error(&req.header, err),
        };

        if let Some(resp_header) = self.guard_pre_write(&req.header, &req_ctx.caller).await {
            return error_response!(SymlinkResponseProto, resp_header);
        }

        let authz = Self::authz_for_rpc(InodeRpcAuthz::Symlink, vec![AuthzTarget::for_inode(parent_inode_id)]);
        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &req_ctx.caller,
                GuardSpec::metadata_write(),
                None,
                Some(authz),
            )
            .await
        {
            return error_response!(SymlinkResponseProto, resp_header);
        }

        let freshness = Self::freshness_from_header(&req.header);
        match self
            .fs_core
            .execute_symlink(SymlinkInput {
                ctx: req_ctx.clone(),
                parent_inode_id,
                name: req.name,
                target: req.target,
                attrs,
                freshness,
            })
            .await
        {
            Ok(success) => response_with_header!(
                SymlinkResponseProto {
                    inode: success.payload.inode.as_ref().map(Self::inode_to_proto),
                    attrs: success.payload.attrs.as_ref().map(Self::file_attrs_to_proto),
                    ..Default::default()
                },
                ok_header_from_core_success(&req_ctx, &success)
            ),
            Err(failure) => error_response!(SymlinkResponseProto, header_from_core_failure(&req_ctx, &failure)),
        }
    }

    async fn readlink(
        &self,
        request: Request<ReadlinkRequestProto>,
    ) -> Result<Response<ReadlinkResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);

        let inode_id = match Self::inode_id_from_proto(req.inode_id.clone(), "inode_id") {
            Ok(inode_id) => inode_id,
            Err(err) => return Self::response_with_metadata_error(&req.header, err),
        };

        if let Some(resp_header) = self.guard_pre_read(&req.header, &req_ctx.caller).await {
            return error_response!(ReadlinkResponseProto, resp_header);
        }

        let authz = Self::authz_for_rpc(InodeRpcAuthz::Readlink, vec![AuthzTarget::for_inode(inode_id)]);
        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &req_ctx.caller,
                GuardSpec::metadata_read(),
                None,
                Some(authz),
            )
            .await
        {
            return error_response!(ReadlinkResponseProto, resp_header);
        }

        match self
            .fs_core
            .execute_readlink(ReadlinkInput {
                ctx: req_ctx.clone(),
                inode_id,
            })
            .await
        {
            Ok(success) => {
                let header = ok_header_from_core_success(&req_ctx, &success);
                let payload = success.payload;
                response_with_header!(
                    ReadlinkResponseProto {
                        target: payload.target,
                        ..Default::default()
                    },
                    header
                )
            }
            Err(failure) => error_response!(ReadlinkResponseProto, header_from_core_failure(&req_ctx, &failure)),
        }
    }

    async fn link(&self, request: Request<LinkRequestProto>) -> Result<Response<LinkResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);

        let src_inode_id = match Self::inode_id_from_proto(req.src_inode_id.clone(), "src_inode_id") {
            Ok(inode_id) => inode_id,
            Err(err) => return Self::response_with_metadata_error(&req.header, err),
        };
        let dst_parent_inode_id =
            match Self::inode_id_from_proto(req.dst_parent_inode_id.clone(), "dst_parent_inode_id") {
                Ok(inode_id) => inode_id,
                Err(err) => return Self::response_with_metadata_error(&req.header, err),
            };

        if let Some(resp_header) = self.guard_pre_write(&req.header, &req_ctx.caller).await {
            return error_response!(LinkResponseProto, resp_header);
        }

        let authz = Self::authz_for_rpc(
            InodeRpcAuthz::Link,
            vec![
                AuthzTarget::for_inode(src_inode_id),
                AuthzTarget::for_inode(dst_parent_inode_id),
            ],
        );
        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &req_ctx.caller,
                GuardSpec::metadata_write(),
                None,
                Some(authz),
            )
            .await
        {
            return error_response!(LinkResponseProto, resp_header);
        }

        let freshness = Self::freshness_from_header(&req.header);
        match self
            .fs_core
            .execute_link(LinkInput {
                ctx: req_ctx.clone(),
                src_inode_id,
                dst_parent_inode_id,
                dst_name: req.dst_name,
                freshness,
            })
            .await
        {
            Ok(success) => response_with_header!(
                LinkResponseProto {
                    inode: success.payload.inode.as_ref().map(Self::inode_to_proto),
                    attrs: success.payload.attrs.as_ref().map(Self::file_attrs_to_proto),
                    ..Default::default()
                },
                ok_header_from_core_success(&req_ctx, &success)
            ),
            Err(failure) => error_response!(LinkResponseProto, header_from_core_failure(&req_ctx, &failure)),
        }
    }

    async fn set_xattr(
        &self,
        request: Request<SetXattrRequestProto>,
    ) -> Result<Response<SetXattrResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);

        let inode_id = match Self::inode_id_from_proto(req.inode_id.clone(), "inode_id") {
            Ok(inode_id) => inode_id,
            Err(err) => return Self::response_with_metadata_error(&req.header, err),
        };

        if let Some(resp_header) = self.guard_pre_write(&req.header, &req_ctx.caller).await {
            return error_response!(SetXattrResponseProto, resp_header);
        }

        let authz = Self::authz_for_rpc(InodeRpcAuthz::SetXattr, vec![AuthzTarget::for_inode(inode_id)]);
        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &req_ctx.caller,
                GuardSpec::metadata_write(),
                None,
                Some(authz),
            )
            .await
        {
            return error_response!(SetXattrResponseProto, resp_header);
        }

        let freshness = Self::freshness_from_header(&req.header);
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
                if let Some(reader) = self.inode_perm_reader.as_ref() {
                    reader.invalidate(inode_id);
                }
                response_with_header!(
                    SetXattrResponseProto::default(),
                    ok_header_from_core_success(&req_ctx, &success)
                )
            }
            Err(failure) => error_response!(SetXattrResponseProto, header_from_core_failure(&req_ctx, &failure)),
        }
    }

    async fn get_xattr(
        &self,
        request: Request<GetXattrRequestProto>,
    ) -> Result<Response<GetXattrResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);

        let inode_id = match Self::inode_id_from_proto(req.inode_id.clone(), "inode_id") {
            Ok(inode_id) => inode_id,
            Err(err) => return Self::response_with_metadata_error(&req.header, err),
        };

        if let Some(resp_header) = self.guard_pre_read(&req.header, &req_ctx.caller).await {
            return error_response!(GetXattrResponseProto, resp_header);
        }

        let authz = Self::authz_for_rpc(InodeRpcAuthz::GetXattr, vec![AuthzTarget::for_inode(inode_id)]);
        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &req_ctx.caller,
                GuardSpec::metadata_read(),
                None,
                Some(authz),
            )
            .await
        {
            return error_response!(GetXattrResponseProto, resp_header);
        }

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
                let payload = success.payload;
                response_with_header!(
                    GetXattrResponseProto {
                        value: payload.value,
                        ..Default::default()
                    },
                    header
                )
            }
            Err(failure) => error_response!(GetXattrResponseProto, header_from_core_failure(&req_ctx, &failure)),
        }
    }

    async fn list_xattr(
        &self,
        request: Request<ListXattrRequestProto>,
    ) -> Result<Response<ListXattrResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);

        let inode_id = match Self::inode_id_from_proto(req.inode_id.clone(), "inode_id") {
            Ok(inode_id) => inode_id,
            Err(err) => return Self::response_with_metadata_error(&req.header, err),
        };

        if let Some(resp_header) = self.guard_pre_read(&req.header, &req_ctx.caller).await {
            return error_response!(ListXattrResponseProto, resp_header);
        }

        let authz = Self::authz_for_rpc(InodeRpcAuthz::ListXattr, vec![AuthzTarget::for_inode(inode_id)]);
        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &req_ctx.caller,
                GuardSpec::metadata_read(),
                None,
                Some(authz),
            )
            .await
        {
            return error_response!(ListXattrResponseProto, resp_header);
        }

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
                let payload = success.payload;
                response_with_header!(
                    ListXattrResponseProto {
                        names: payload.names,
                        ..Default::default()
                    },
                    header
                )
            }
            Err(failure) => error_response!(ListXattrResponseProto, header_from_core_failure(&req_ctx, &failure)),
        }
    }

    async fn remove_xattr(
        &self,
        request: Request<RemoveXattrRequestProto>,
    ) -> Result<Response<RemoveXattrResponseProto>, Status> {
        let req = request.into_inner();
        let req_ctx = request_context_from_proto(&req.header);

        let inode_id = match Self::inode_id_from_proto(req.inode_id.clone(), "inode_id") {
            Ok(inode_id) => inode_id,
            Err(err) => return Self::response_with_metadata_error(&req.header, err),
        };

        if let Some(resp_header) = self.guard_pre_write(&req.header, &req_ctx.caller).await {
            return error_response!(RemoveXattrResponseProto, resp_header);
        }

        let authz = Self::authz_for_rpc(InodeRpcAuthz::RemoveXattr, vec![AuthzTarget::for_inode(inode_id)]);
        if let Some(resp_header) = self
            .guard_request(
                &req.header,
                &req_ctx.caller,
                GuardSpec::metadata_write(),
                None,
                Some(authz),
            )
            .await
        {
            return error_response!(RemoveXattrResponseProto, resp_header);
        }

        let freshness = Self::freshness_from_header(&req.header);
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
                if let Some(reader) = self.inode_perm_reader.as_ref() {
                    reader.invalidate(inode_id);
                }
                response_with_header!(
                    RemoveXattrResponseProto::default(),
                    ok_header_from_core_success(&req_ctx, &success)
                )
            }
            Err(failure) => error_response!(RemoveXattrResponseProto, header_from_core_failure(&req_ctx, &failure)),
        }
    }
}
