// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use super::{fs_failure_from_metadata_error, refresh_metadata_fs_failure, Freshness, FsFailure, RequestContext};
use crate::error::MetadataResult;
use crate::mount::MountTable;
use crate::state::StateStore;
use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, RefreshHint};
use beryl_types::ids::MountId;
use beryl_types::{GroupName, RaftLogId};
use std::sync::Arc;

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
mod tests {
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
