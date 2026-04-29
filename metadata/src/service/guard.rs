// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Request guard pipeline for metadata services.

use super::auth::{NonePermissionChecker, PermissionBits, PermissionChecker, SetAttrPerm};
use super::domain::RequestContext;
use crate::data_io::DataIoOp;
use crate::error::{to_canonical_rpc, MetadataError};
use crate::mount::{DataIoPolicy, MountTable, ROOT_MOUNT_PREFIX};
use crate::path_resolver::ResolvedPath;
use crate::raft::AppRaftNode;
use crate::readiness::RootReadinessGate;
use common::error::canonical::{CanonicalError, RefreshHint, RefreshReason};
use common::header::RpcErrorCode;
use std::sync::Arc;
use types::fs::FsErrorCode;
use types::ids::MountId;

pub trait LeadershipChecker: Send + Sync {
    fn is_leader(&self) -> bool;
    fn leader_endpoint(&self) -> Option<String> {
        None
    }
}

impl LeadershipChecker for AppRaftNode {
    fn is_leader(&self) -> bool {
        self.is_leader()
    }

    fn leader_endpoint(&self) -> Option<String> {
        let leader_id = self.get_leader_id()?;
        let membership = self.get_membership()?;
        let leader_node = membership.nodes().find(|(node_id, _)| **node_id == leader_id)?.1;
        Some(leader_node.address.clone())
    }
}

#[derive(Clone, Debug)]
pub struct GuardFailure {
    pub err: CanonicalError,
    pub group_id: Option<u64>,
    pub mount_epoch: Option<u64>,
}

impl GuardFailure {
    fn new(err: CanonicalError) -> Self {
        Self {
            err,
            group_id: None,
            mount_epoch: None,
        }
    }

    fn from_rpc_metadata_error(err: MetadataError) -> Self {
        Self::new(to_canonical_rpc(err))
    }

    fn with_mount(mut self, group_id: Option<u64>, mount_epoch: Option<u64>) -> Self {
        self.group_id = group_id;
        self.mount_epoch = mount_epoch;
        self
    }
}

#[derive(Clone)]
pub struct GuardChain {
    readiness: ReadinessGuard,
    leadership: LeadershipGuard,
    data_io: DataIoPolicyGuard,
    permission: Arc<dyn PermissionChecker>,
}

impl GuardChain {
    pub fn new(mount_table: Arc<MountTable>) -> Self {
        Self {
            readiness: ReadinessGuard { readiness_gate: None },
            leadership: LeadershipGuard { checker: None },
            data_io: DataIoPolicyGuard { mount_table },
            permission: Arc::new(NonePermissionChecker),
        }
    }

    pub(crate) fn with_readiness_gate(mut self, gate: Option<Arc<RootReadinessGate>>) -> Self {
        self.readiness.readiness_gate = gate;
        self
    }

    pub(crate) fn with_leadership_checker(mut self, checker: Option<Arc<dyn LeadershipChecker>>) -> Self {
        self.leadership.checker = checker;
        self
    }

    pub(crate) fn with_permission_checker(mut self, checker: Arc<dyn PermissionChecker>) -> Self {
        self.permission = checker;
        self
    }

    #[cfg(test)]
    pub(crate) fn set_readiness_gate(&mut self, gate: Arc<RootReadinessGate>) {
        self.readiness.readiness_gate = Some(gate);
    }

    #[cfg(test)]
    pub fn set_leadership_checker<T>(&mut self, checker: Arc<T>)
    where
        T: LeadershipChecker + 'static,
    {
        self.leadership.checker = Some(checker);
    }

    #[cfg(test)]
    pub(crate) fn set_permission_checker(&mut self, checker: Arc<dyn PermissionChecker>) {
        self.permission = checker;
    }

    pub async fn check_meta_read(&self, _ctx: &RequestContext) -> Result<(), GuardFailure> {
        self.readiness.check()
    }

    pub async fn check_meta_write(&self, ctx: &RequestContext) -> Result<(), GuardFailure> {
        self.readiness.check()?;
        self.leadership.check(ctx)
    }

    pub async fn check_data_read(&self, _ctx: &RequestContext, mount_id: MountId) -> Result<(), GuardFailure> {
        self.readiness.check()?;
        self.data_io.check(mount_id, DataIoOp::Read)
    }

    pub async fn check_data_write(&self, ctx: &RequestContext, mount_id: MountId) -> Result<(), GuardFailure> {
        self.readiness.check()?;
        self.leadership.check(ctx)?;
        self.data_io.check(mount_id, DataIoOp::Write)
    }

    pub async fn check_perm(
        &self,
        ctx: &RequestContext,
        bits: PermissionBits,
        path: &str,
        resolved: &ResolvedPath,
    ) -> Result<(), GuardFailure> {
        self.permission
            .check_perm(ctx, bits, path, resolved)
            .await
            .map_err(GuardFailure::new)
    }

    pub async fn check_parent_perm(
        &self,
        ctx: &RequestContext,
        bits: PermissionBits,
        path: &str,
        resolved: &ResolvedPath,
    ) -> Result<(), GuardFailure> {
        self.permission
            .check_parent_perm(ctx, bits, path, resolved)
            .await
            .map_err(GuardFailure::new)
    }

    pub async fn check_set_attr_perm(
        &self,
        ctx: &RequestContext,
        path: &str,
        resolved: &ResolvedPath,
        req: SetAttrPerm,
    ) -> Result<(), GuardFailure> {
        self.permission
            .check_set_attr_perm(ctx, path, resolved, req)
            .await
            .map_err(GuardFailure::new)
    }

    pub async fn check_super(&self, ctx: &RequestContext) -> Result<(), GuardFailure> {
        self.permission.check_super(ctx).await.map_err(GuardFailure::new)
    }
}

#[derive(Clone)]
struct ReadinessGuard {
    readiness_gate: Option<Arc<RootReadinessGate>>,
}

impl ReadinessGuard {
    fn check(&self) -> Result<(), GuardFailure> {
        let Some(gate) = self.readiness_gate.as_ref() else {
            return Ok(());
        };
        if gate.is_ready() {
            return Ok(());
        }
        Err(GuardFailure::from_rpc_metadata_error(
            MetadataError::ServiceUnavailable("root mount not ready".to_string()),
        ))
    }
}

#[derive(Clone)]
struct LeadershipGuard {
    checker: Option<Arc<dyn LeadershipChecker>>,
}

impl LeadershipGuard {
    fn check(&self, ctx: &RequestContext) -> Result<(), GuardFailure> {
        let Some(checker) = self.checker.as_ref() else {
            return Err(GuardFailure::from_rpc_metadata_error(
                MetadataError::ServiceUnavailable("raft node not available".to_string()),
            ));
        };
        if checker.is_leader() {
            Ok(())
        } else {
            let hint = RefreshHint {
                leader_endpoint: checker.leader_endpoint(),
                group_id: ctx.caller.group_id,
                ..Default::default()
            };
            Err(GuardFailure::new(CanonicalError::need_refresh_with_hint(
                RpcErrorCode::NotLeader,
                RefreshReason::NotLeader,
                hint,
                "not leader",
            )))
        }
    }
}

#[derive(Clone)]
struct DataIoPolicyGuard {
    mount_table: Arc<MountTable>,
}

impl DataIoPolicyGuard {
    fn check(&self, mount_id: MountId, op: DataIoOp) -> Result<(), GuardFailure> {
        let mount_entry = self
            .mount_table
            .get_mount(mount_id)
            .map_err(GuardFailure::from_rpc_metadata_error)?
            .ok_or_else(|| {
                GuardFailure::from_rpc_metadata_error(MetadataError::NotFound(format!(
                    "Mount not found: {:?}",
                    mount_id
                )))
            })?;

        if mount_entry.data_io_policy != DataIoPolicy::Forbid {
            return Ok(());
        }

        let reason = if mount_entry.mount_prefix == ROOT_MOUNT_PREFIX {
            "RootDataIoForbidden"
        } else {
            "MountHasNoUfs"
        };
        let err = CanonicalError::fatal_fs(
            FsErrorCode::ENotsup,
            format!("{reason}: op={} mount_prefix={}", op.as_str(), mount_entry.mount_prefix),
        );
        Err(GuardFailure::new(err).with_mount(
            Some(mount_entry.namespace_owner_group_id.as_raw()),
            Some(mount_entry.config_version),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mount::{DataIoPolicy, MountKind, ROOT_INODE_ID};
    use crate::readiness::RootReadinessGate;
    use async_trait::async_trait;
    use common::error::canonical::{ErrorClass, ErrorCode};
    use common::header::{RequestHeader, RpcErrorCode};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use types::ids::ShardGroupId;

    struct StaticLeader(bool);

    impl LeadershipChecker for StaticLeader {
        fn is_leader(&self) -> bool {
            self.0
        }
    }

    struct CountingPermissionChecker {
        perm_calls: Arc<AtomicUsize>,
        parent_calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl PermissionChecker for CountingPermissionChecker {
        async fn check_perm(
            &self,
            _ctx: &RequestContext,
            _bits: PermissionBits,
            _path: &str,
            _resolved: &ResolvedPath,
        ) -> Result<(), CanonicalError> {
            self.perm_calls.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        async fn check_parent_perm(
            &self,
            _ctx: &RequestContext,
            _bits: PermissionBits,
            _path: &str,
            _resolved: &ResolvedPath,
        ) -> Result<(), CanonicalError> {
            self.parent_calls.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        async fn check_super(&self, _ctx: &RequestContext) -> Result<(), CanonicalError> {
            Ok(())
        }

        async fn get_perm(
            &self,
            _ctx: &RequestContext,
            _path: &str,
            _resolved: &ResolvedPath,
        ) -> Result<PermissionBits, CanonicalError> {
            Ok(PermissionBits::all())
        }

        async fn check_set_attr_perm(
            &self,
            _ctx: &RequestContext,
            _path: &str,
            _resolved: &ResolvedPath,
            _req: SetAttrPerm,
        ) -> Result<(), CanonicalError> {
            Ok(())
        }
    }

    fn request_context(client_id: u64) -> RequestContext {
        let caller = RequestHeader::new(types::ClientId::new(client_id));
        RequestContext {
            caller: caller.clone(),
            traceparent: caller.traceparent.clone(),
            route_epoch: None,
            principal: caller.principal.clone(),
            real_user: caller.real_user.clone(),
            doas: caller.doas.clone(),
            authn_type: caller.authn_type,
        }
    }

    fn resolved_path() -> ResolvedPath {
        ResolvedPath {
            mount_ctx: crate::path_resolver::MountContext {
                mount_id: MountId::new(1),
                mount_epoch: 1,
                owner_group_id: ShardGroupId::new(1),
                root_inode_id: ROOT_INODE_ID,
            },
            parent_inode_id: Some(ROOT_INODE_ID),
            name: Some("file".to_string()),
            inode_id: Some(types::fs::InodeId::new(2)),
            traverse_dir_inode_ids: vec![ROOT_INODE_ID],
        }
    }

    #[tokio::test]
    async fn readiness_guard_blocks_when_not_ready() {
        let mount_table = Arc::new(MountTable::new());
        let mut chain = GuardChain::new(mount_table);
        let gate = Arc::new(RootReadinessGate::new(None));
        chain.set_readiness_gate(Arc::clone(&gate));

        let err = chain.check_meta_read(&request_context(1)).await.unwrap_err();
        assert_eq!(err.err.class, ErrorClass::Retryable);
        assert_eq!(err.err.code, Some(ErrorCode::RpcCode(RpcErrorCode::NodeUnavailable)));
        assert!(!gate.is_ready());
    }

    #[tokio::test]
    async fn check_meta_write_checks_readiness_then_leadership() {
        let mut chain = GuardChain::new(Arc::new(MountTable::new()));
        let gate = Arc::new(RootReadinessGate::new(None));
        chain.set_readiness_gate(Arc::clone(&gate));
        chain.set_leadership_checker(Arc::new(StaticLeader(false)));

        let err = chain.check_meta_write(&request_context(2)).await.unwrap_err();
        assert_eq!(err.err.class, ErrorClass::Retryable);
        assert_eq!(err.err.code, Some(ErrorCode::RpcCode(RpcErrorCode::NodeUnavailable)));
    }

    #[tokio::test]
    async fn leadership_guard_returns_not_leader() {
        let mut chain = GuardChain::new(Arc::new(MountTable::new()));
        chain.set_leadership_checker(Arc::new(StaticLeader(false)));

        let err = chain.check_meta_write(&request_context(2)).await.unwrap_err();
        assert_eq!(err.err.class, ErrorClass::NeedRefresh);
        assert_eq!(err.err.code, Some(ErrorCode::RpcCode(RpcErrorCode::NotLeader)));
    }

    #[tokio::test]
    async fn check_data_write_checks_leadership_before_data_io_policy() {
        let mount_table = Arc::new(MountTable::new());
        let root_entry = mount_table
            .create_mount(
                ROOT_MOUNT_PREFIX.to_string(),
                MountKind::Internal,
                None,
                DataIoPolicy::Forbid,
                ShardGroupId::new(1),
                ROOT_INODE_ID,
            )
            .unwrap();
        let mut chain = GuardChain::new(Arc::clone(&mount_table));
        chain.set_leadership_checker(Arc::new(StaticLeader(false)));

        let err = chain
            .check_data_write(&request_context(3), root_entry.mount_id)
            .await
            .unwrap_err();
        assert_eq!(err.err.class, ErrorClass::NeedRefresh);
        assert_eq!(err.err.code, Some(ErrorCode::RpcCode(RpcErrorCode::NotLeader)));
    }

    #[tokio::test]
    async fn data_io_guard_forbids_root() {
        let mount_table = Arc::new(MountTable::new());
        let root_entry = mount_table
            .create_mount(
                ROOT_MOUNT_PREFIX.to_string(),
                MountKind::Internal,
                None,
                DataIoPolicy::Forbid,
                ShardGroupId::new(1),
                ROOT_INODE_ID,
            )
            .unwrap();
        let chain = GuardChain::new(Arc::clone(&mount_table));

        let err = chain
            .check_data_read(&request_context(3), root_entry.mount_id)
            .await
            .unwrap_err();
        assert_eq!(err.err.class, ErrorClass::Fatal);
        assert_eq!(err.err.code, Some(ErrorCode::FsErrno(FsErrorCode::ENotsup)));
        assert!(err.err.message.contains("RootDataIoForbidden"));
        assert_eq!(err.group_id, Some(root_entry.namespace_owner_group_id.as_raw()));
        assert_eq!(err.mount_epoch, Some(root_entry.config_version));
    }

    #[tokio::test]
    async fn permission_methods_delegate_to_configured_checker() {
        let perm_calls = Arc::new(AtomicUsize::new(0));
        let parent_calls = Arc::new(AtomicUsize::new(0));
        let mut chain = GuardChain::new(Arc::new(MountTable::new()));
        chain.set_permission_checker(Arc::new(CountingPermissionChecker {
            perm_calls: Arc::clone(&perm_calls),
            parent_calls: Arc::clone(&parent_calls),
        }));
        let ctx = request_context(5);
        let resolved = resolved_path();

        chain
            .check_perm(&ctx, PermissionBits::READ, "/file", &resolved)
            .await
            .unwrap();
        chain
            .check_parent_perm(&ctx, PermissionBits::WRITE, "/file", &resolved)
            .await
            .unwrap();
        chain.check_super(&ctx).await.unwrap();
        chain
            .check_set_attr_perm(&ctx, "/file", &resolved, SetAttrPerm::default())
            .await
            .unwrap();

        assert_eq!(perm_calls.load(Ordering::Relaxed), 1);
        assert_eq!(parent_calls.load(Ordering::Relaxed), 1);
    }
}
