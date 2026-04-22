// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Request guard pipeline for metadata services.

use super::authz::{AllowAllAuthz, AuthzOp, AuthzProvider, AuthzTarget};
use super::domain::RequestContext;
use crate::data_io::DataIoOp;
use crate::error::MetadataError;
use crate::mount::{DataIoPolicy, MountTable, ROOT_MOUNT_PREFIX};
use crate::raft::AppRaftNode;
use crate::readiness::RootReadinessGate;
use common::error::canonical::{CanonicalError, RefreshHint, RefreshReason};
use common::header::{RequestHeader, RpcErrorCode};
use std::sync::Arc;
use types::fs::FsErrorCode;
use types::ids::MountId;

#[derive(Clone, Debug)]
pub struct AuthzCheck {
    pub op: AuthzOp,
    pub target: AuthzTarget,
}

#[derive(Clone, Debug)]
pub struct AuthzContext {
    pub op: AuthzOp,
    pub targets: Vec<AuthzTarget>,
    pub pre_checks: Vec<AuthzCheck>,
}

impl AuthzContext {
    pub fn new(op: AuthzOp, targets: Vec<AuthzTarget>) -> Self {
        Self {
            op,
            targets,
            pre_checks: Vec::new(),
        }
    }

    pub fn with_pre_checks(mut self, pre_checks: Vec<AuthzCheck>) -> Self {
        self.pre_checks = pre_checks;
        self
    }
}

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

#[derive(Clone, Copy, Debug)]
pub struct GuardPolicy {
    pub requires_root_ready: bool,
    pub requires_raft: bool,
    pub requires_leader: bool,
    pub data_io_op: Option<DataIoOp>,
    pub enforce_authz: bool,
}

impl GuardPolicy {
    pub const PATH_READ_PRE: Self = Self {
        requires_root_ready: true,
        requires_raft: false,
        requires_leader: false,
        data_io_op: None,
        enforce_authz: false,
    };

    pub const PATH_WRITE_PRE: Self = Self {
        requires_root_ready: true,
        requires_raft: false,
        requires_leader: true,
        data_io_op: None,
        enforce_authz: false,
    };

    pub const METADATA_READ: Self = Self {
        requires_root_ready: true,
        requires_raft: false,
        requires_leader: false,
        data_io_op: None,
        enforce_authz: true,
    };

    pub const METADATA_WRITE: Self = Self {
        requires_root_ready: true,
        requires_raft: false,
        requires_leader: true,
        data_io_op: None,
        enforce_authz: true,
    };

    pub const fn data_io(op: DataIoOp) -> Self {
        Self {
            requires_root_ready: true,
            requires_raft: false,
            requires_leader: false,
            data_io_op: Some(op),
            enforce_authz: false,
        }
    }

    pub const fn with_leader(mut self) -> Self {
        self.requires_leader = true;
        self
    }

    pub const fn with_raft(mut self) -> Self {
        self.requires_raft = true;
        self
    }

    pub const fn with_authz(mut self) -> Self {
        self.enforce_authz = true;
        self
    }
}

#[derive(Clone, Debug)]
pub struct GuardContext<'a> {
    pub req_header_proto: &'a Option<proto::common::RequestHeaderProto>,
    pub caller: &'a RequestHeader,
    pub policy: GuardPolicy,
    pub mount_id: Option<MountId>,
    pub authz: Option<AuthzContext>,
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
    authz: AuthGuard,
}

impl GuardChain {
    pub fn new(mount_table: Arc<MountTable>) -> Self {
        Self {
            readiness: ReadinessGuard { readiness_gate: None },
            leadership: LeadershipGuard { checker: None },
            data_io: DataIoPolicyGuard { mount_table },
            authz: AuthGuard {
                provider: Arc::new(AllowAllAuthz),
            },
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

    pub(crate) fn with_authz_provider(mut self, provider: Arc<dyn AuthzProvider>) -> Self {
        self.authz.provider = provider;
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
    pub(crate) fn set_authz_provider(&mut self, provider: Arc<dyn AuthzProvider>) {
        self.authz.provider = provider;
    }

    pub async fn check(&self, ctx: &GuardContext<'_>) -> Result<(), GuardFailure> {
        if ctx.policy.requires_root_ready {
            self.readiness.check(ctx)?;
        }
        if ctx.policy.data_io_op.is_some() {
            self.data_io.check(ctx)?;
        }
        if ctx.policy.requires_raft || ctx.policy.requires_leader {
            self.leadership.check(ctx)?;
        }
        if ctx.policy.enforce_authz {
            if ctx.authz.is_none() {
                return Err(GuardFailure::new(
                    MetadataError::InvalidArgument("missing authz context".to_string()).into(),
                ));
            }
            self.authz.check(ctx).await?;
        }
        Ok(())
    }

    pub async fn check_request(
        &self,
        req_header_proto: &Option<proto::common::RequestHeaderProto>,
        caller: &RequestHeader,
        policy: GuardPolicy,
        mount_id: Option<MountId>,
        authz: Option<AuthzContext>,
    ) -> Result<(), GuardFailure> {
        let ctx = GuardContext {
            req_header_proto,
            caller,
            policy,
            mount_id,
            authz,
        };
        self.check(&ctx).await
    }

    pub async fn check_system(&self, policy: GuardPolicy) -> Result<(), GuardFailure> {
        let caller = RequestHeader::new(types::ClientId::new(0));
        let ctx = GuardContext {
            req_header_proto: &None,
            caller: &caller,
            policy,
            mount_id: None,
            authz: None,
        };
        self.check(&ctx).await
    }
}

#[derive(Clone)]
struct ReadinessGuard {
    readiness_gate: Option<Arc<RootReadinessGate>>,
}

impl ReadinessGuard {
    fn check(&self, _ctx: &GuardContext) -> Result<(), GuardFailure> {
        let Some(gate) = self.readiness_gate.as_ref() else {
            return Ok(());
        };
        if gate.is_ready() {
            return Ok(());
        }
        Err(GuardFailure::new(
            MetadataError::ServiceUnavailable("root mount not ready".to_string()).into(),
        ))
    }
}

#[derive(Clone)]
struct LeadershipGuard {
    checker: Option<Arc<dyn LeadershipChecker>>,
}

impl LeadershipGuard {
    fn check(&self, ctx: &GuardContext) -> Result<(), GuardFailure> {
        let Some(checker) = self.checker.as_ref() else {
            return Err(GuardFailure::new(
                MetadataError::ServiceUnavailable("raft node not available".to_string()).into(),
            ));
        };
        if checker.is_leader() {
            Ok(())
        } else {
            let group_id = ctx
                .req_header_proto
                .as_ref()
                .and_then(|header| (header.group_id != 0).then_some(header.group_id))
                .or(ctx.caller.group_id);
            let hint = RefreshHint {
                leader_endpoint: checker.leader_endpoint(),
                group_id,
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
    fn check(&self, ctx: &GuardContext) -> Result<(), GuardFailure> {
        let Some(op) = ctx.policy.data_io_op else {
            return Ok(());
        };
        let mount_id = ctx.mount_id.ok_or_else(|| {
            GuardFailure::new(MetadataError::InvalidArgument("missing mount_id for data-io guard".to_string()).into())
        })?;

        let mount_entry = self
            .mount_table
            .get_mount(mount_id)
            .map_err(|err| GuardFailure::new(err.into()))?
            .ok_or_else(|| {
                GuardFailure::new(MetadataError::NotFound(format!("Mount not found: {:?}", mount_id)).into())
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

#[derive(Clone)]
struct AuthGuard {
    provider: Arc<dyn AuthzProvider>,
}

impl AuthGuard {
    async fn check(&self, ctx: &GuardContext<'_>) -> Result<(), GuardFailure> {
        let Some(authz_ctx) = ctx.authz.as_ref() else {
            return Ok(());
        };
        let req_ctx = RequestContext {
            caller: ctx.caller.clone(),
            traceparent: ctx.caller.traceparent.clone(),
            route_epoch: ctx.req_header_proto.as_ref().and_then(|h| h.route_epoch),
            principal: ctx.caller.principal.clone(),
            real_user: ctx.caller.real_user.clone(),
            doas: ctx.caller.doas.clone(),
            authn_type: ctx.caller.authn_type,
        };
        for check in &authz_ctx.pre_checks {
            self.provider
                .authorize(&req_ctx, check.target.clone(), check.op)
                .await
                .map_err(GuardFailure::new)?;
        }
        for target in &authz_ctx.targets {
            self.provider
                .authorize(&req_ctx, target.clone(), authz_ctx.op)
                .await
                .map_err(GuardFailure::new)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::authz::AuthzScheme;
    use super::*;
    use crate::mount::{DataIoPolicy, MountKind, ROOT_INODE_ID};
    use crate::readiness::RootReadinessGate;
    use async_trait::async_trait;
    use common::error::canonical::CanonicalError;
    use common::error::canonical::{ErrorClass, ErrorCode};
    use common::header::RpcErrorCode;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use types::ids::ShardGroupId;

    struct StaticLeader(bool);

    impl LeadershipChecker for StaticLeader {
        fn is_leader(&self) -> bool {
            self.0
        }
    }

    struct CountingAuthz {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl AuthzProvider for CountingAuthz {
        fn scheme(&self) -> AuthzScheme {
            AuthzScheme::None
        }

        async fn authorize(
            &self,
            _req_ctx: &RequestContext,
            _target: AuthzTarget,
            _op: AuthzOp,
        ) -> Result<(), CanonicalError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    fn base_context<'a>(
        policy: GuardPolicy,
        mount_id: Option<MountId>,
        authz: Option<AuthzContext>,
        caller: &'a RequestHeader,
        req_header: &'a Option<proto::common::RequestHeaderProto>,
    ) -> GuardContext<'a> {
        GuardContext {
            req_header_proto: req_header,
            caller,
            policy,
            mount_id,
            authz,
        }
    }

    #[tokio::test]
    async fn readiness_guard_blocks_when_not_ready() {
        let mount_table = Arc::new(MountTable::new());
        let mut chain = GuardChain::new(mount_table);
        let gate = Arc::new(RootReadinessGate::new(None));
        chain.set_readiness_gate(Arc::clone(&gate));

        let caller = RequestHeader::new(types::ClientId::new(1));
        let req_header = None;
        let ctx = base_context(GuardPolicy::METADATA_READ, None, None, &caller, &req_header);

        let err = chain.check(&ctx).await.unwrap_err();
        assert_eq!(err.err.class, ErrorClass::Retryable);
        assert_eq!(err.err.code, Some(ErrorCode::RpcCode(RpcErrorCode::NodeUnavailable)));
        assert_eq!(gate.is_ready(), false);
    }

    #[tokio::test]
    async fn leadership_guard_returns_not_leader() {
        let mount_table = Arc::new(MountTable::new());
        let mut chain = GuardChain::new(mount_table);
        chain.set_leadership_checker(Arc::new(StaticLeader(false)));

        let caller = RequestHeader::new(types::ClientId::new(2));
        let req_header = None;
        let ctx = base_context(
            GuardPolicy::METADATA_WRITE,
            None,
            Some(AuthzContext {
                op: AuthzOp::Write,
                targets: vec![AuthzTarget::for_path("/test".to_string())],
                pre_checks: Vec::new(),
            }),
            &caller,
            &req_header,
        );

        let err = chain.check(&ctx).await.unwrap_err();
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

        let caller = RequestHeader::new(types::ClientId::new(3));
        let req_header = None;
        let ctx = base_context(
            GuardPolicy::data_io(DataIoOp::Read),
            Some(root_entry.mount_id),
            None,
            &caller,
            &req_header,
        );

        let err = chain.check(&ctx).await.unwrap_err();
        assert_eq!(err.err.class, ErrorClass::Fatal);
        assert_eq!(err.err.code, Some(ErrorCode::FsErrno(FsErrorCode::ENotsup)));
        assert!(err.err.message.contains("RootDataIoForbidden"));
        assert_eq!(err.group_id, Some(root_entry.namespace_owner_group_id.as_raw()));
        assert_eq!(err.mount_epoch, Some(root_entry.config_version));
    }

    #[tokio::test]
    async fn metadata_read_requires_authz_context() {
        let chain = GuardChain::new(Arc::new(MountTable::new()));
        let caller = RequestHeader::new(types::ClientId::new(4));
        let req_header = None;
        let ctx = base_context(GuardPolicy::METADATA_READ, None, None, &caller, &req_header);

        let err = chain.check(&ctx).await.unwrap_err();
        assert_eq!(err.err.class, ErrorClass::Fatal);
        assert!(err.err.message.contains("missing authz context"));
    }

    #[tokio::test]
    async fn auth_guard_authorizes_all_targets() {
        let call_counter = Arc::new(AtomicUsize::new(0));
        let mut chain = GuardChain::new(Arc::new(MountTable::new()));
        chain.set_authz_provider(Arc::new(CountingAuthz {
            calls: Arc::clone(&call_counter),
        }));

        let caller = RequestHeader::new(types::ClientId::new(5));
        let req_header = None;
        let ctx = base_context(
            GuardPolicy::METADATA_READ,
            None,
            Some(AuthzContext {
                op: AuthzOp::Rename,
                targets: vec![
                    AuthzTarget::for_path("/src".to_string()),
                    AuthzTarget::for_path_parent("/dst-parent", "name"),
                ],
                pre_checks: Vec::new(),
            }),
            &caller,
            &req_header,
        );

        chain.check(&ctx).await.unwrap();
        assert_eq!(call_counter.load(Ordering::Relaxed), 2);
    }
}
