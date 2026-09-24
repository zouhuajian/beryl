// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Admission and freshness guards for filesystem requests.

use super::{
    fs_failure_from_metadata_error, refresh_metadata_fs_failure, FsFailure, MetadataFileSystem, RequestHeader,
};
use crate::error::{to_rpc_error, MetadataError};
use crate::raft::AppRaftNode;
use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, RefreshHint, RpcErrorDetail};
use beryl_types::ids::MountId;
use beryl_types::{GroupName, RaftLogId};

impl MetadataFileSystem {
    pub(super) fn check_meta_write(&self, ctx: &RequestHeader) -> Result<(), FsFailure> {
        self.check_readiness()?;
        self.check_leadership(ctx)
    }

    pub(super) fn check_data_read(&self, mount_id: MountId) -> Result<(), FsFailure> {
        self.check_readiness()?;
        self.check_mount(mount_id)
    }

    pub(super) fn check_data_write(&self, ctx: &RequestHeader, mount_id: MountId) -> Result<(), FsFailure> {
        self.check_readiness()?;
        self.check_leadership(ctx)?;
        self.check_mount(mount_id)
    }

    pub(super) fn check_readiness(&self) -> Result<(), FsFailure> {
        if self.readiness_gate.is_ready() {
            return Ok(());
        }
        Err(FsFailure::new(
            to_rpc_error(MetadataError::ServiceUnavailable("root mount not ready".to_string())),
            None,
        ))
    }

    fn check_leadership(&self, ctx: &RequestHeader) -> Result<(), FsFailure> {
        let raft_node = &self.raft_node;
        if raft_node.is_leader() {
            Ok(())
        } else {
            let hint = RefreshHint {
                leader_endpoint: leader_endpoint(raft_node),
                group_name: ctx.group_name.as_ref().map(ToString::to_string),
            };
            Err(FsFailure::new(
                RpcErrorDetail::refresh_metadata(ErrorKind::Metadata(MetadataErrorKind::NotLeader), hint, "not leader"),
                None,
            ))
        }
    }

    fn check_mount(&self, mount_id: MountId) -> Result<(), FsFailure> {
        self.mount_table.get_mount(mount_id).ok_or_else(|| {
            FsFailure::new(
                to_rpc_error(MetadataError::NotFound(format!("Mount not found: {mount_id:?}"))),
                None,
            )
        })?;
        Ok(())
    }
}

fn leader_endpoint(raft_node: &AppRaftNode) -> Option<String> {
    let leader_id = raft_node.get_leader_id()?;
    let membership = raft_node.get_membership()?;
    let leader_node = membership.nodes().find(|(node_id, _)| **node_id == leader_id)?.1;
    Some(leader_node.address.clone())
}

impl MetadataFileSystem {
    pub(super) fn mount_owner(&self, mount_id: MountId) -> Option<GroupName> {
        self.mount_table
            .get_mount(mount_id)
            .map(|mount| mount.namespace_owner_group_name)
    }

    pub(super) async fn check_leader(
        &self,
        ctx: &RequestHeader,
        group_name: Option<GroupName>,
    ) -> Result<(), FsFailure> {
        self.raft_node
            .read(false, || Ok(()))
            .await
            .map_err(|error| fs_failure_from_metadata_error(ctx, error, group_name))
    }

    pub(super) fn validate_stale_state(
        &self,
        ctx: &RequestHeader,
        last_applied: Option<RaftLogId>,
        group_name: Option<GroupName>,
    ) -> Result<(), FsFailure> {
        let Some(group_name) = group_name else {
            return Ok(());
        };
        let required_state_id = ctx
            .state
            .as_ref()
            .filter(|watermark| watermark.group_name == group_name)
            .map(|watermark| watermark.state_id);
        let Some(required_state_id) = required_state_id else {
            return Ok(());
        };
        let Some(last_applied) = last_applied else {
            return Err(refresh_metadata_fs_failure(
                ctx,
                ErrorKind::Metadata(MetadataErrorKind::StaleState),
                "local applied state is unavailable for read freshness validation",
                Some(group_name),
                None,
            ));
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
                None,
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::{AppRaftNode, AppRaftStateMachine, RocksDBStorage};
    use crate::service::filesystem::tests::*;
    use beryl_common::error::rpc::{InternalErrorKind, RecoveryAction};
    use std::sync::Arc;
    use tempfile::TempDir;

    #[tokio::test]
    async fn readiness_guard_blocks_after_shutdown() {
        let filesystem = filesystem_builder_with_mount(MountId::new(1), &group_name("root"))
            .build()
            .await;
        filesystem.readiness_gate.begin_shutdown();
        let err = filesystem.open_file(&request_context(), "/").await.unwrap_err();
        assert_eq!(err.error.kind, ErrorKind::Internal(InternalErrorKind::NodeUnavailable));
        assert_eq!(err.error.recovery, RecoveryAction::Retry { after_ms: Some(1000) });
        filesystem.raft_node().shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn leadership_guard_returns_not_leader_for_nonleader_raft_node() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let builder = filesystem_builder_with_mount(MountId::new(1), &group_name("root"));
        let state_machine = AppRaftStateMachine::new(Arc::clone(&storage));
        let raft_node = Arc::new(
            AppRaftNode::new(1, Arc::clone(&storage), state_machine, builder.mount_table())
                .await
                .unwrap(),
        );
        let filesystem = builder
            .with_storage(storage)
            .with_raft_node(Arc::clone(&raft_node))
            .build()
            .await;
        let ctx = RequestHeader::new(beryl_types::ClientId::new(2)).with_group_name(group_name("requested"));
        let err = filesystem.check_meta_write(&ctx).unwrap_err();
        assert_eq!(err.error.kind, ErrorKind::Metadata(MetadataErrorKind::NotLeader));
        assert!(matches!(
            &err.error.recovery,
            RecoveryAction::RefreshMetadata { hint } if hint.group_name.as_deref() == Some("requested")
        ));
        let header = crate::service::wire::header_from_fs_failure(&ctx, &err);
        assert!(header.group_name.is_empty());
        assert!(header.state.is_none());
        let failure = filesystem.check_leader(&ctx, ctx.group_name.clone()).await.unwrap_err();
        assert_eq!(failure.error.kind, ErrorKind::Metadata(MetadataErrorKind::NotLeader));
        raft_node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn freshness_validator_rejects_stale_state_watermark() {
        let group_name_value = group_name("g4");
        let filesystem = filesystem_builder_with_mount(MountId::new(1), &group_name_value)
            .build()
            .await;
        let mut ctx = request_context();
        ctx.state = Some(GroupStateWatermark::new(
            group_name_value.clone(),
            RaftLogId::new(1, 7, 12),
        ));
        let failure = filesystem
            .validate_stale_state(&ctx, Some(RaftLogId::new(1, 7, 10)), Some(group_name_value.clone()))
            .unwrap_err();
        assert_refresh_metadata(&failure.error, ErrorKind::Metadata(MetadataErrorKind::StaleState));
        assert_eq!(failure.group_name, Some(group_name_value.clone()));
        assert!(crate::service::wire::header_from_fs_failure(&ctx, &failure)
            .state
            .is_none());
        let failure = filesystem
            .validate_stale_state(&ctx, None, Some(group_name_value))
            .expect_err("reads require known applied state to satisfy a client watermark");
        assert_refresh_metadata(&failure.error, ErrorKind::Metadata(MetadataErrorKind::StaleState));
    }
}
