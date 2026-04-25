// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use crate::mount::MountTable;
use crate::service::core_util::{core_failure_from_metadata_error, need_refresh_core_failure};
use crate::service::domain::{CoreFailure, Freshness, RequestContext};
use crate::state::StateStore;
use common::error::canonical::{RefreshHint, RefreshReason};
use common::header::RpcErrorCode;
use std::sync::Arc;
use types::ids::MountId;
use types::RaftLogId;

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

    pub(super) fn state_id_from_ctx(ctx: &RequestContext) -> Option<RaftLogId> {
        ctx.caller.state_id
    }

    pub(super) async fn authoritative_route_epoch(&self) -> Option<u64> {
        self.state_store.get_route_epoch().await.ok().map(|v| v.as_u64())
    }

    pub(super) fn mount_hints_for_mount(&self, mount_id: MountId) -> (Option<u64>, Option<u64>) {
        match self.mount_table.get_mount(mount_id) {
            Ok(Some(mount_entry)) => (
                Some(mount_entry.namespace_owner_group_id.as_raw()),
                Some(mount_entry.config_version),
            ),
            _ => (None, None),
        }
    }

    pub(super) fn validate_mount_epoch_for_mount(
        &self,
        ctx: &RequestContext,
        freshness: Freshness,
        mount_id: MountId,
    ) -> Result<(Option<u64>, Option<u64>), CoreFailure> {
        self.validate_routed_write_mount_epoch(ctx, freshness, mount_id)
    }

    pub(super) fn validate_routed_write_mount_epoch(
        &self,
        ctx: &RequestContext,
        freshness: Freshness,
        mount_id: MountId,
    ) -> Result<(Option<u64>, Option<u64>), CoreFailure> {
        self.validate_mount_epoch_for_mount_with_replay(ctx, freshness, mount_id, Some("request"))
    }

    pub(super) fn validate_mount_freshness(
        &self,
        ctx: &RequestContext,
        freshness: Freshness,
        mount_id: MountId,
    ) -> Result<(Option<u64>, Option<u64>), CoreFailure> {
        self.validate_mount_epoch_for_mount_with_replay(ctx, freshness, mount_id, None)
    }

    fn validate_mount_epoch_for_mount_with_replay(
        &self,
        ctx: &RequestContext,
        freshness: Freshness,
        mount_id: MountId,
        replay_intent: Option<&str>,
    ) -> Result<(Option<u64>, Option<u64>), CoreFailure> {
        let (group_id, mount_epoch) = self.mount_hints_for_mount(mount_id);
        if let (Some(client_mount_epoch), Some(server_mount_epoch)) =
            (freshness.mount_epoch.or(ctx.caller.mount_epoch), mount_epoch)
        {
            if client_mount_epoch != server_mount_epoch {
                let message = match replay_intent {
                    Some(intent) => format!(
                        "mount_epoch mismatch: client={}, server={}; {}",
                        client_mount_epoch,
                        server_mount_epoch,
                        Self::replay_hint(intent)
                    ),
                    None => format!(
                        "mount_epoch mismatch: client={}, server={}",
                        client_mount_epoch, server_mount_epoch
                    ),
                };
                return Err(need_refresh_core_failure(
                    ctx,
                    RpcErrorCode::MountEpochMismatch,
                    RefreshReason::MountEpochMismatch,
                    message,
                    group_id,
                    Some(server_mount_epoch),
                    None,
                    Some(RefreshHint {
                        group_id,
                        mount_epoch: Some(server_mount_epoch),
                        ..Default::default()
                    }),
                ));
            }
        }
        Ok((group_id, mount_epoch))
    }

    pub(super) async fn validate_route_epoch(
        &self,
        ctx: &RequestContext,
        freshness: Freshness,
        group_id: Option<u64>,
        mount_epoch: Option<u64>,
        intent: &str,
    ) -> Result<Option<u64>, CoreFailure> {
        let client_route_epoch = freshness.route_epoch.or(ctx.route_epoch);

        let server_route_epoch = match self.state_store.get_route_epoch().await {
            Ok(v) => v.as_u64(),
            Err(err) => {
                return Err(core_failure_from_metadata_error(ctx, err, group_id, mount_epoch, None));
            }
        };

        if let Some(client_route_epoch) = client_route_epoch {
            if client_route_epoch != server_route_epoch {
                return Err(need_refresh_core_failure(
                    ctx,
                    RpcErrorCode::RouteEpochMismatch,
                    RefreshReason::RouteEpochMismatch,
                    format!(
                        "route_epoch mismatch: client={}, server={}; refresh route and replay {}",
                        client_route_epoch, server_route_epoch, intent
                    ),
                    group_id,
                    mount_epoch,
                    Some(server_route_epoch),
                    Some(RefreshHint {
                        group_id,
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
        group_id: Option<u64>,
        mount_epoch: Option<u64>,
    ) -> Result<StaleStateStatus, CoreFailure> {
        let Some(required_state_id) = ctx.caller.state_id else {
            return Ok(StaleStateStatus::Ready);
        };
        let Some(last_applied) = last_applied else {
            return Ok(StaleStateStatus::UnknownLastApplied);
        };
        if last_applied < required_state_id {
            return Err(need_refresh_core_failure(
                ctx,
                RpcErrorCode::StaleState,
                RefreshReason::StaleState,
                format!(
                    "Stale state: last_applied={:?} < required={:?}",
                    last_applied, required_state_id
                ),
                group_id,
                mount_epoch,
                None,
                None,
            ));
        }
        Ok(StaleStateStatus::Ready)
    }

    fn replay_hint(intent: &str) -> String {
        format!("refresh metadata and re-open write session, then replay {}", intent)
    }
}
