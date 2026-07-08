// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Request guard pipeline for metadata services.

use super::auth::{self, PermissionBits};
use super::domain::RequestContext;
use crate::data_io::DataIoOp;
use crate::error::{to_rpc_error, MetadataError};
use crate::mount::{DataIoPolicy, MountTable, ROOT_MOUNT_PREFIX};
use crate::path_resolver::ResolvedPath;
use crate::raft::AppRaftNode;
use crate::readiness::RootReadinessGate;
use common::error::rpc::{ErrorKind, MetadataErrorKind, RefreshHint, RpcErrorDetail};
use std::sync::Arc;
use types::fs::FsErrorCode;
use types::ids::MountId;
use types::GroupName;

#[derive(Clone, Debug)]
pub struct GuardFailure {
    pub err: Box<RpcErrorDetail>,
    pub group_name: Option<GroupName>,
    pub mount_epoch: Option<u64>,
}

impl GuardFailure {
    fn new(err: RpcErrorDetail) -> Self {
        Self {
            err: Box::new(err),
            group_name: None,
            mount_epoch: None,
        }
    }

    fn from_rpc_metadata_error(err: MetadataError) -> Self {
        Self::new(to_rpc_error(err))
    }

    fn with_mount(mut self, group_name: Option<GroupName>, mount_epoch: Option<u64>) -> Self {
        self.group_name = group_name;
        self.mount_epoch = mount_epoch;
        self
    }
}

#[derive(Clone)]
pub struct GuardChain {
    readiness: ReadinessGuard,
    leadership: LeadershipGuard,
    data_io: DataIoPolicyGuard,
}

impl GuardChain {
    pub fn new(mount_table: Arc<MountTable>) -> Self {
        Self {
            readiness: ReadinessGuard { readiness_gate: None },
            leadership: LeadershipGuard { raft_node: None },
            data_io: DataIoPolicyGuard { mount_table },
        }
    }

    pub(crate) fn with_readiness_gate(mut self, gate: Option<Arc<RootReadinessGate>>) -> Self {
        self.readiness.readiness_gate = gate;
        self
    }

    pub(crate) fn with_raft_node(mut self, raft_node: Option<Arc<AppRaftNode>>) -> Self {
        self.leadership.raft_node = raft_node;
        self
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
        auth::check_perm(ctx, bits, path, resolved);
        Ok(())
    }

    pub async fn check_parent_perm(
        &self,
        ctx: &RequestContext,
        bits: PermissionBits,
        path: &str,
        resolved: &ResolvedPath,
    ) -> Result<(), GuardFailure> {
        auth::check_parent_perm(ctx, bits, path, resolved);
        Ok(())
    }

    pub async fn check_super(&self, ctx: &RequestContext) -> Result<(), GuardFailure> {
        auth::check_super(ctx);
        Ok(())
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
    raft_node: Option<Arc<AppRaftNode>>,
}

impl LeadershipGuard {
    fn check(&self, ctx: &RequestContext) -> Result<(), GuardFailure> {
        let Some(raft_node) = self.raft_node.as_ref() else {
            return Err(GuardFailure::from_rpc_metadata_error(
                MetadataError::ServiceUnavailable("raft node not available".to_string()),
            ));
        };
        if raft_node.is_leader() {
            Ok(())
        } else {
            let hint = RefreshHint {
                leader_endpoint: leader_endpoint(raft_node),
                group_name: ctx.caller.group_name.as_ref().map(ToString::to_string),
                ..Default::default()
            };
            Err(GuardFailure::new(RpcErrorDetail::refresh_metadata(
                ErrorKind::Metadata(MetadataErrorKind::NotLeader),
                hint,
                "not leader",
            )))
        }
    }
}

fn leader_endpoint(raft_node: &AppRaftNode) -> Option<String> {
    let leader_id = raft_node.get_leader_id()?;
    let membership = raft_node.get_membership()?;
    let leader_node = membership.nodes().find(|(node_id, _)| **node_id == leader_id)?.1;
    Some(leader_node.address.clone())
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
        let err = RpcErrorDetail::fs(
            FsErrorCode::ENotsup,
            format!("{reason}: op={} mount_prefix={}", op.as_str(), mount_entry.mount_prefix),
        );
        Err(GuardFailure::new(err).with_mount(
            Some(mount_entry.namespace_owner_group_name),
            Some(mount_entry.mount_epoch),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RaftConfig;
    use crate::mount::{DataIoPolicy, MountKind, ROOT_INODE_ID};
    use crate::raft::{AppRaftNode, AppRaftStateMachine, RocksDBStorage};
    use crate::readiness::RootReadinessGate;
    use common::error::rpc::InternalErrorKind;
    use common::error::rpc::{ErrorKind, RecoveryAction};
    use common::header::RequestHeader;
    use tempfile::TempDir;
    use types::GroupName;

    fn group_name(raw: &str) -> GroupName {
        GroupName::parse(raw).unwrap()
    }

    fn request_context(client_id: u128) -> RequestContext {
        let caller = RequestHeader::new(types::ClientId::new(client_id));
        RequestContext {
            caller: caller.clone(),
            traceparent: caller.trace_context.traceparent.clone(),
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
                owner_group_name: group_name("root"),
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
        let gate = Arc::new(RootReadinessGate::new(None));
        let chain = GuardChain::new(mount_table).with_readiness_gate(Some(Arc::clone(&gate)));

        let err = chain.check_meta_read(&request_context(1)).await.unwrap_err();
        assert_eq!(err.err.kind, ErrorKind::Internal(InternalErrorKind::NodeUnavailable));
        assert_eq!(err.err.recovery, RecoveryAction::Retry { after_ms: Some(1000) });
        assert!(!gate.is_ready());
    }

    #[tokio::test]
    async fn check_meta_write_checks_readiness_then_leadership() {
        let gate = Arc::new(RootReadinessGate::new(None));
        let chain = GuardChain::new(Arc::new(MountTable::new())).with_readiness_gate(Some(Arc::clone(&gate)));

        let err = chain.check_meta_write(&request_context(2)).await.unwrap_err();
        assert_eq!(err.err.kind, ErrorKind::Internal(InternalErrorKind::NodeUnavailable));
        assert_eq!(err.err.recovery, RecoveryAction::Retry { after_ms: Some(1000) });
    }

    #[tokio::test]
    async fn leadership_guard_without_raft_node_returns_unavailable() {
        let chain = GuardChain::new(Arc::new(MountTable::new()));

        let err = chain.check_meta_write(&request_context(2)).await.unwrap_err();
        assert_eq!(err.err.kind, ErrorKind::Internal(InternalErrorKind::NodeUnavailable));
        assert_eq!(err.err.recovery, RecoveryAction::Retry { after_ms: Some(1000) });
    }

    #[tokio::test]
    async fn leadership_guard_returns_not_leader_for_nonleader_raft_node() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table)));
        let raft_config = RaftConfig::default();
        let raft_node = Arc::new(AppRaftNode::new(1, storage, state_machine, &raft_config).await.unwrap());
        assert!(!raft_node.is_leader());
        let chain = GuardChain::new(mount_table).with_raft_node(Some(raft_node));

        let err = chain.check_meta_write(&request_context(2)).await.unwrap_err();

        assert_eq!(err.err.kind, ErrorKind::Metadata(MetadataErrorKind::NotLeader));
        assert!(matches!(err.err.recovery, RecoveryAction::RefreshMetadata { .. }));
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
                group_name("root"),
                ROOT_INODE_ID,
            )
            .unwrap();
        let chain = GuardChain::new(Arc::clone(&mount_table));

        let err = chain
            .check_data_write(&request_context(3), root_entry.mount_id)
            .await
            .unwrap_err();
        assert_eq!(err.err.kind, ErrorKind::Internal(InternalErrorKind::NodeUnavailable));
        assert_eq!(err.err.recovery, RecoveryAction::Retry { after_ms: Some(1000) });
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
                group_name("root"),
                ROOT_INODE_ID,
            )
            .unwrap();
        let chain = GuardChain::new(Arc::clone(&mount_table));

        let err = chain
            .check_data_read(&request_context(3), root_entry.mount_id)
            .await
            .unwrap_err();
        assert_eq!(err.err.kind, ErrorKind::Fs(FsErrorCode::ENotsup));
        assert_eq!(err.err.recovery, RecoveryAction::Fail);
        assert!(err.err.message.contains("RootDataIoForbidden"));
        assert_eq!(err.group_name, Some(root_entry.namespace_owner_group_name.clone()));
        assert_eq!(err.mount_epoch, Some(root_entry.mount_epoch));
    }

    #[tokio::test]
    async fn permission_methods_use_current_none_policy() {
        let chain = GuardChain::new(Arc::new(MountTable::new()));
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
    }
}
