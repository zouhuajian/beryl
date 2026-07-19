// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Admission and freshness guards for filesystem requests.

use super::{fs_failure_from_metadata_error, refresh_metadata_fs_failure, Freshness, FsFailure, RequestContext};
use crate::data_io::DataIoOp;
use crate::error::{to_rpc_error, MetadataError, MetadataResult};
use crate::mount::{DataIoPolicy, MountTable};
use crate::raft::AppRaftNode;
use crate::readiness::RootReadinessGate;
use crate::state::StateStore;
use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, RefreshHint, RpcErrorDetail};
use beryl_types::fs::FsErrorCode;
use beryl_types::ids::MountId;
use beryl_types::{GroupName, RaftLogId};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct AdmissionFailure {
    pub err: Box<RpcErrorDetail>,
    pub group_name: Option<GroupName>,
    pub mount_epoch: Option<u64>,
}

impl AdmissionFailure {
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
pub struct AdmissionGuard {
    readiness: ReadinessGuard,
    leadership: LeadershipGuard,
    data_io: DataIoPolicyGuard,
}

impl AdmissionGuard {
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

    pub async fn check_meta_read(&self, _ctx: &RequestContext) -> Result<(), AdmissionFailure> {
        self.readiness.check()
    }

    pub async fn check_meta_write(&self, ctx: &RequestContext) -> Result<(), AdmissionFailure> {
        self.readiness.check()?;
        self.leadership.check(ctx)
    }

    pub async fn check_data_read(&self, _ctx: &RequestContext, mount_id: MountId) -> Result<(), AdmissionFailure> {
        self.readiness.check()?;
        self.data_io.check(mount_id, DataIoOp::Read)
    }

    pub async fn check_data_write(&self, ctx: &RequestContext, mount_id: MountId) -> Result<(), AdmissionFailure> {
        self.readiness.check()?;
        self.leadership.check(ctx)?;
        self.data_io.check(mount_id, DataIoOp::Write)
    }
}

#[derive(Clone)]
struct ReadinessGuard {
    readiness_gate: Option<Arc<RootReadinessGate>>,
}

impl ReadinessGuard {
    fn check(&self) -> Result<(), AdmissionFailure> {
        let Some(gate) = self.readiness_gate.as_ref() else {
            return Ok(());
        };
        if gate.is_ready() {
            return Ok(());
        }
        Err(AdmissionFailure::from_rpc_metadata_error(
            MetadataError::ServiceUnavailable("root mount not ready".to_string()),
        ))
    }
}

#[derive(Clone)]
struct LeadershipGuard {
    raft_node: Option<Arc<AppRaftNode>>,
}

impl LeadershipGuard {
    fn check(&self, ctx: &RequestContext) -> Result<(), AdmissionFailure> {
        let Some(raft_node) = self.raft_node.as_ref() else {
            return Err(AdmissionFailure::from_rpc_metadata_error(
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
            Err(AdmissionFailure::new(RpcErrorDetail::refresh_metadata(
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
    fn check(&self, mount_id: MountId, op: DataIoOp) -> Result<(), AdmissionFailure> {
        let mount_entry = self
            .mount_table
            .get_mount(mount_id)
            .map_err(AdmissionFailure::from_rpc_metadata_error)?
            .ok_or_else(|| {
                AdmissionFailure::from_rpc_metadata_error(MetadataError::NotFound(format!(
                    "Mount not found: {:?}",
                    mount_id
                )))
            })?;

        if mount_entry.data_io_policy != DataIoPolicy::Forbid {
            return Ok(());
        }

        let err = RpcErrorDetail::fs(
            FsErrorCode::ENotsup,
            format!(
                "MountDataIoForbidden: op={} mount_prefix={}",
                op.as_str(),
                mount_entry.mount_prefix
            ),
        );
        Err(AdmissionFailure::new(err).with_mount(
            Some(mount_entry.namespace_owner_group_name),
            Some(mount_entry.mount_epoch),
        ))
    }
}

#[cfg(test)]
mod admission_tests {
    use super::*;
    use crate::config::RaftConfig;
    use crate::mount::{DataIoPolicy, MountKind, ROOT_INODE_ID, ROOT_MOUNT_PREFIX};
    use crate::raft::{AppRaftNode, AppRaftStateMachine, RocksDBStorage};
    use crate::readiness::RootReadinessGate;
    use beryl_common::error::rpc::InternalErrorKind;
    use beryl_common::error::rpc::{ErrorKind, RecoveryAction};
    use beryl_common::header::RequestHeader;
    use beryl_types::GroupName;
    use tempfile::TempDir;

    fn group_name(raw: &str) -> GroupName {
        GroupName::parse(raw).unwrap()
    }

    fn request_context(client_id: u128) -> RequestContext {
        let caller = RequestHeader::new(beryl_types::ClientId::new(client_id));
        RequestContext {
            caller,
            route_epoch: None,
        }
    }

    #[tokio::test]
    async fn readiness_guard_blocks_when_not_ready() {
        let mount_table = Arc::new(MountTable::new());
        let gate = Arc::new(RootReadinessGate::new(None));
        let chain = AdmissionGuard::new(mount_table).with_readiness_gate(Some(Arc::clone(&gate)));

        let err = chain.check_meta_read(&request_context(1)).await.unwrap_err();
        assert_eq!(err.err.kind, ErrorKind::Internal(InternalErrorKind::NodeUnavailable));
        assert_eq!(err.err.recovery, RecoveryAction::Retry { after_ms: Some(1000) });
        assert!(!gate.is_ready());
    }

    #[tokio::test]
    async fn check_meta_write_checks_readiness_then_leadership() {
        let gate = Arc::new(RootReadinessGate::new(None));
        let chain = AdmissionGuard::new(Arc::new(MountTable::new())).with_readiness_gate(Some(Arc::clone(&gate)));

        let err = chain.check_meta_write(&request_context(2)).await.unwrap_err();
        assert_eq!(err.err.kind, ErrorKind::Internal(InternalErrorKind::NodeUnavailable));
        assert_eq!(err.err.recovery, RecoveryAction::Retry { after_ms: Some(1000) });
    }

    #[tokio::test]
    async fn leadership_guard_without_raft_node_returns_unavailable() {
        let chain = AdmissionGuard::new(Arc::new(MountTable::new()));

        let err = chain.check_meta_write(&request_context(2)).await.unwrap_err();
        assert_eq!(err.err.kind, ErrorKind::Internal(InternalErrorKind::NodeUnavailable));
        assert_eq!(err.err.recovery, RecoveryAction::Retry { after_ms: Some(1000) });
    }

    #[tokio::test]
    async fn leadership_guard_returns_not_leader_for_nonleader_raft_node() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::new());
        let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage)));
        let raft_config = RaftConfig::default();
        let raft_node = Arc::new(
            AppRaftNode::new(1, storage, state_machine, Arc::clone(&mount_table), &raft_config)
                .await
                .unwrap(),
        );
        assert!(!raft_node.is_leader());
        let chain = AdmissionGuard::new(mount_table).with_raft_node(Some(raft_node));

        let err = chain.check_meta_write(&request_context(2)).await.unwrap_err();

        assert_eq!(err.err.kind, ErrorKind::Metadata(MetadataErrorKind::NotLeader));
        assert!(matches!(err.err.recovery, RecoveryAction::RefreshMetadata { .. }));
    }

    #[tokio::test]
    async fn check_data_write_checks_leadership_before_data_io_policy() {
        let mount_table = Arc::new(MountTable::new());
        let mount_entry = mount_table
            .create_mount(
                "/archive".to_string(),
                MountKind::External,
                Some("s3://archive".to_string()),
                DataIoPolicy::Forbid,
                group_name("root"),
                ROOT_INODE_ID,
            )
            .unwrap();
        let chain = AdmissionGuard::new(Arc::clone(&mount_table));

        let err = chain
            .check_data_write(&request_context(3), mount_entry.mount_id)
            .await
            .unwrap_err();
        assert_eq!(err.err.kind, ErrorKind::Internal(InternalErrorKind::NodeUnavailable));
        assert_eq!(err.err.recovery, RecoveryAction::Retry { after_ms: Some(1000) });
    }

    #[tokio::test]
    async fn data_io_guard_allows_writable_root() {
        let mount_table = Arc::new(MountTable::new());
        let root_entry = mount_table
            .create_mount(
                ROOT_MOUNT_PREFIX.to_string(),
                MountKind::Internal,
                None,
                DataIoPolicy::Allow,
                group_name("root"),
                ROOT_INODE_ID,
            )
            .unwrap();
        let chain = AdmissionGuard::new(Arc::clone(&mount_table));

        chain
            .check_data_read(&request_context(3), root_entry.mount_id)
            .await
            .unwrap();
    }
}

#[derive(Clone)]
pub(super) struct FreshnessValidator {
    state_store: Arc<dyn StateStore>,
    mount_table: Arc<MountTable>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum StaleStateStatus {
    Ready,
    UnknownLastApplied,
}

impl FreshnessValidator {
    pub(super) fn new(state_store: Arc<dyn StateStore>, mount_table: Arc<MountTable>) -> Self {
        Self {
            state_store,
            mount_table,
        }
    }

    pub(super) async fn authoritative_route_epoch(&self) -> MetadataResult<u64> {
        self.state_store.get_route_epoch().await.map(|epoch| epoch.as_u64())
    }

    pub(super) fn mount_hints_for_mount(&self, mount_id: MountId) -> (Option<GroupName>, Option<u64>) {
        match self.mount_table.get_mount(mount_id) {
            Ok(Some(mount_entry)) => (
                Some(mount_entry.namespace_owner_group_name),
                Some(mount_entry.mount_epoch),
            ),
            _ => (None, None),
        }
    }

    pub(super) fn validate_mount_epoch(
        &self,
        ctx: &RequestContext,
        freshness: Freshness,
        mount_id: MountId,
    ) -> Result<(Option<GroupName>, Option<u64>), FsFailure> {
        let (group_name, mount_epoch) = self.mount_hints_for_mount(mount_id);
        if let (Some(client_mount_epoch), Some(server_mount_epoch)) =
            (freshness.mount_epoch.or(ctx.caller.mount_epoch), mount_epoch)
        {
            if client_mount_epoch != server_mount_epoch {
                return Err(refresh_metadata_fs_failure(
                    ctx,
                    ErrorKind::Metadata(MetadataErrorKind::MountEpochMismatch),
                    format!(
                        "mount_epoch mismatch: client={}, server={}; {}",
                        client_mount_epoch,
                        server_mount_epoch,
                        Self::replay_hint("request")
                    ),
                    group_name.clone(),
                    Some(server_mount_epoch),
                    None,
                    Some(RefreshHint {
                        group_name: group_name.as_ref().map(ToString::to_string),
                        mount_epoch: Some(server_mount_epoch),
                        ..Default::default()
                    }),
                ));
            }
        }
        Ok((group_name, mount_epoch))
    }

    pub(super) async fn validate_route_epoch(
        &self,
        ctx: &RequestContext,
        freshness: Freshness,
        group_name: Option<GroupName>,
        mount_epoch: Option<u64>,
        intent: &str,
    ) -> Result<Option<u64>, FsFailure> {
        let client_route_epoch = freshness.route_epoch.or(ctx.route_epoch);

        let server_route_epoch = match self.state_store.get_route_epoch().await {
            Ok(v) => v.as_u64(),
            Err(err) => {
                return Err(fs_failure_from_metadata_error(
                    ctx,
                    err,
                    group_name.clone(),
                    mount_epoch,
                    None,
                ));
            }
        };

        if let Some(client_route_epoch) = client_route_epoch {
            if client_route_epoch != server_route_epoch {
                return Err(refresh_metadata_fs_failure(
                    ctx,
                    ErrorKind::Metadata(MetadataErrorKind::RouteEpochMismatch),
                    format!(
                        "route_epoch mismatch: client={}, server={}; refresh route and replay {}",
                        client_route_epoch, server_route_epoch, intent
                    ),
                    group_name.clone(),
                    mount_epoch,
                    Some(server_route_epoch),
                    Some(RefreshHint {
                        group_name: group_name.as_ref().map(ToString::to_string),
                        route_epoch: Some(server_route_epoch),
                        mount_epoch,
                        ..Default::default()
                    }),
                ));
            }
        }

        Ok(Some(server_route_epoch))
    }

    pub(super) fn validate_stale_state(
        &self,
        ctx: &RequestContext,
        last_applied: Option<RaftLogId>,
        group_name: Option<GroupName>,
        mount_epoch: Option<u64>,
    ) -> Result<StaleStateStatus, FsFailure> {
        let Some(group_name) = group_name else {
            return Ok(StaleStateStatus::Ready);
        };
        let required_state_id = ctx
            .caller
            .state
            .iter()
            .find(|watermark| watermark.group_name == group_name)
            .map(|watermark| watermark.state_id);
        let Some(required_state_id) = required_state_id else {
            return Ok(StaleStateStatus::Ready);
        };
        let Some(last_applied) = last_applied else {
            return Ok(StaleStateStatus::UnknownLastApplied);
        };
        if !last_applied.has_reached(&required_state_id) {
            return Err(refresh_metadata_fs_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::StaleState),
                format!(
                    "Stale state: last_applied={:?} < required={:?}",
                    last_applied, required_state_id
                ),
                Some(group_name),
                mount_epoch,
                None,
                None,
            ));
        }
        Ok(StaleStateStatus::Ready)
    }

    fn replay_hint(intent: &str) -> String {
        format!("refresh metadata and reopen write handle, then replay {}", intent)
    }
}

#[cfg(test)]
mod freshness_tests {
    use super::*;
    use crate::error::MetadataError;
    use crate::state::RouteEpoch;

    struct FailingStateStore;

    #[async_trait::async_trait]
    impl StateStore for FailingStateStore {
        async fn get_route_epoch(&self) -> MetadataResult<RouteEpoch> {
            Err(MetadataError::Internal("route epoch unavailable".to_string()))
        }
    }

    #[tokio::test]
    async fn authoritative_route_epoch_propagates_state_store_failure() {
        let validator = FreshnessValidator::new(Arc::new(FailingStateStore), Arc::new(MountTable::new()));

        let error = validator.authoritative_route_epoch().await.unwrap_err();

        assert!(matches!(error, MetadataError::Internal(_)));
    }
    use crate::service::filesystem::test_support::*;

    #[test]
    fn freshness_validator_rejects_mount_epoch_with_replay_hint() {
        let mount_id = MountId::new(12);
        let group_name_value = group_name("g4");
        let mount_table = Arc::new(MountTable::new());
        mount_table
            .upsert(MountEntry {
                mount_id,
                mount_prefix: "/data".to_string(),
                mount_kind: MountKind::Internal,
                ufs_uri: None,
                data_io_policy: DataIoPolicy::Allow,
                mount_epoch: 9,
                namespace_owner_group_name: group_name_value.clone(),
                root_inode_id: ROOT_INODE_ID,
            })
            .unwrap();
        let validator = FreshnessValidator::new(Arc::new(MemoryStateStore::new()), mount_table);
        let ctx = request_context();

        let failure = validator
            .validate_mount_epoch(
                &ctx,
                Freshness {
                    mount_epoch: Some(4),
                    route_epoch: None,
                },
                mount_id,
            )
            .unwrap_err();

        assert_refresh_metadata(
            &failure.error,
            ErrorKind::Metadata(MetadataErrorKind::MountEpochMismatch),
        );
        assert_eq!(
            failure.error.message,
            "mount_epoch mismatch: client=4, server=9; refresh metadata and reopen write handle, then replay request"
        );
        let hint = refresh_hint(&failure.error);
        assert_eq!(hint.group_name, Some(group_name_value.to_string()));
        assert_eq!(hint.mount_epoch, Some(9));
        assert_eq!(failure.group_name, Some(group_name_value.clone()));
        assert_eq!(failure.mount_epoch, Some(9));
    }

    #[test]
    fn freshness_validator_rejects_stale_state_watermark() {
        let group_name_value = group_name("g4");
        let validator = FreshnessValidator::new(Arc::new(MemoryStateStore::new()), Arc::new(MountTable::new()));
        let mut ctx = request_context();
        ctx.caller.state = vec![beryl_types::GroupStateWatermark::new(
            group_name_value.clone(),
            beryl_types::RaftLogId::new(1, 7, 12),
        )];

        let failure = validator
            .validate_stale_state(
                &ctx,
                Some(beryl_types::RaftLogId::new(1, 7, 10)),
                Some(group_name_value.clone()),
                Some(9),
            )
            .unwrap_err();

        assert_refresh_metadata(&failure.error, ErrorKind::Metadata(MetadataErrorKind::StaleState));
        assert_eq!(
        failure.error.message,
        "Stale state: last_applied=RaftLogId { term: 1, leader_node_id: 7, index: 10 } < required=RaftLogId { term: 1, leader_node_id: 7, index: 12 }"
    );
        assert_eq!(failure.group_name, Some(group_name_value.clone()));
        assert_eq!(failure.mount_epoch, Some(9));
        assert!(failure.state.is_empty());

        let unknown = validator
            .validate_stale_state(&ctx, None, Some(group_name_value.clone()), Some(9))
            .expect("missing last_applied should preserve existing precheck fallback");
        assert_eq!(unknown, StaleStateStatus::UnknownLastApplied);
    }
}
