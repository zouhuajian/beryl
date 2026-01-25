#![deny(deprecated)]
// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors


//! Helper for metadata RPC calls with group_id and state_id management.
//!
//! This module provides unified handling for:
//! - Group ID routing (path -> group_id)
//! - State ID watermark management (group_id -> state_id cache)
//! - Follower read selection
//! - Error handling and retry (STALE_STATE -> Msync, NOT_LEADER -> refresh route)

use crate::cache::StateIdCache;
use crate::canonical::{
    handle_response_header as canonical_handle_response_header, retry_metadata_once, ClientAction, RetryOutcome,
};
use crate::error::{ClientError, ClientResult};
use crate::meta::MetadataClient;
use crate::routing::{GroupRoleCache, RouteTable};
use common::error::canonical::RefreshReason;
use common::header::{RequestHeader, ResponseHeader};
use std::sync::Arc;
use types::fs::InodeId;
use types::ids::ShardGroupId;
use types::GroupWatermark;
use types::RaftLogId;

/// RPC helper for metadata operations with group-aware state management.
pub struct MetadataRpcHelper {
    /// State ID cache (group_id -> state_id).
    state_cache: Arc<StateIdCache>,
    /// Route table (path/data_handle_id -> group_id).
    route_table: Arc<RouteTable>,
    /// Group role cache (group_id -> leader/followers).
    group_role: Arc<GroupRoleCache>,
    /// Metadata client.
    metadata_client: Arc<MetadataClient>,
}

impl MetadataRpcHelper {
    /// Create a new RPC helper.
    pub fn new(
        state_cache: Arc<StateIdCache>,
        route_table: Arc<RouteTable>,
        group_role: Arc<GroupRoleCache>,
        metadata_client: Arc<MetadataClient>,
    ) -> Self {
        Self {
            state_cache,
            route_table,
            group_role,
            metadata_client,
        }
    }

    /// Resolve path to group_id.
    /// Returns None if path cannot be resolved (need to call GetRouteTable first).
    pub fn resolve_path_to_group(&self, _path: &str) -> Option<ShardGroupId> {
        // TODO: Implement path -> data_handle_id -> group_id resolution
        // For now, return None to indicate need for route table refresh
        None
    }

    /// Resolve inode_id to group_id.
    pub fn resolve_inode_id_to_group(&self, inode_id: InodeId) -> Option<ShardGroupId> {
        self.route_table.route_inode_id(inode_id).map(|(gid, _)| gid)
    }

    /// Get state_id for a group (from cache).
    pub fn get_state_id(&self, group_id: &ShardGroupId) -> Option<RaftLogId> {
        self.state_cache.get(group_id).map(|w| w.state_id)
    }

    /// Update state_id for a group (from response header).
    /// Uses response.header.group_id as the key (not request group_id) to avoid cross-group updates.
    pub fn update_state_id_from_response(&self, response_header: &ResponseHeader) {
        if let (Some(group_id_raw), Some(state_id)) = (response_header.group_id, response_header.state_id.as_ref()) {
            let group_id = ShardGroupId::new(group_id_raw);
            let watermark = GroupWatermark::new(group_id, *state_id);
            self.state_cache.update_if_ahead(watermark);
        }
    }

    /// Create a request header with group_id and state_id filled.
    pub fn create_request_header(
        &self,
        base_header: &RequestHeader,
        group_id: Option<ShardGroupId>,
        is_read: bool,
    ) -> RequestHeader {
        let mut header = base_header.child();

        if let Some(gid) = group_id {
            header.group_id = Some(gid.as_raw());

            // For read requests, fill state_id from cache
            if is_read {
                if let Some(state_id) = self.get_state_id(&gid) {
                    header.state_id = Some(state_id);
                }
            }
        }

        header
    }

    /// Handle response header: update state_id cache and check for errors.
    ///
    /// This function now delegates to `client::canonical::handle_response_header` for
    /// unified error decision-making. The canonical handler converts ResponseHeader to
    /// ClientAction, which we then convert to ClientError for backward compatibility.
    ///
    /// TODO: Once all callers are updated to use ClientAction directly, this function
    /// can be simplified or removed.
    pub fn handle_response_header(&self, response_header: &ResponseHeader) -> ClientResult<()> {
        // Update state_id cache using response.group_id
        self.update_state_id_from_response(response_header);

        // Use canonical error handler for unified decision-making
        match canonical_handle_response_header(response_header) {
            Ok(()) => Ok(()),
            Err(action) => match action {
                ClientAction::Refresh(reason) => {
                    let msg = match reason {
                        RefreshReason::StaleState => "STALE_STATE",
                        RefreshReason::NotLeader => "NOT_LEADER",
                        RefreshReason::Moved | RefreshReason::RouteEpochMismatch => "SHARD_MOVED",
                        RefreshReason::MountEpochMismatch => "MOUNT_EPOCH_MISMATCH",
                        _ => "NEED_REFRESH",
                    };
                    Err(ClientError::Metadata(format!("{}: need refresh", msg)))
                }
                ClientAction::Retry { after_ms } => Err(ClientError::Metadata(format!(
                    "RETRYABLE: retry after {}ms",
                    after_ms.unwrap_or(0)
                ))),
                ClientAction::Fail(err) => Err(ClientError::Metadata(format!("FATAL: {}", err.message))),
            },
        }
    }

    /// Execute a metadata RPC with a single automatic refresh/ retry based on canonical_error.
    ///
    /// The `call` closure must return the parsed `ResponseHeader` plus payload for the RPC.
    /// Refresh behaviour:
    /// - MountEpochMismatch: adopt mount_epoch hint from response and retry once.
    /// - RouteEpochMismatch/Moved/NotLeader: refresh route table then retry once.
    /// - StaleState: msync group if group_id/state_id is available, then retry once.
    /// - Retryable: single bounded retry with optional backoff.
    pub async fn call_with_refresh<T, CallFut>(
        &self,
        base_header: &RequestHeader,
        group_id: Option<ShardGroupId>,
        is_read: bool,
        call: impl FnMut(RequestHeader) -> CallFut,
    ) -> ClientResult<RetryOutcome<(ResponseHeader, T)>>
    where
        CallFut: std::future::Future<Output = ClientResult<(ResponseHeader, T)>>,
    {
        let initial_header = self.create_request_header(base_header, group_id, is_read);
        let mut call = call;
        let outcome = retry_metadata_once(
            initial_header,
            move |hdr| {
                let fut = call(hdr.clone());
                async move { fut.await }
            },
            |reason, resp_header| {
                let mount_epoch = resp_header.mount_epoch;
                let group_id = resp_header.group_id;
                let state_id = resp_header.state_id;
                async move {
                    metrics::counter!(
                        "client_metadata_refresh_total",
                        "reason" => format!("{:?}", reason),
                        "group_id" => group_id.unwrap_or(0).to_string()
                    )
                    .increment(1);

                    let mut next_header = base_header.child_with_same_call_id();
                    next_header.group_id = group_id.or(next_header.group_id);
                    if let Some(state_id) = state_id {
                        next_header.state_id = Some(state_id);
                    }

                    match reason {
                        RefreshReason::MountEpochMismatch => {
                            if let Some(epoch) = mount_epoch {
                                next_header.mount_epoch = Some(epoch);
                            }
                        }
                        RefreshReason::RouteEpochMismatch | RefreshReason::Moved | RefreshReason::NotLeader => {
                            let refresh_header = base_header.child();
                            // Ignore refresh failures here; let the retry surface the error.
                            let _ = self.refresh_route_table(&refresh_header).await;
                        }
                        RefreshReason::StaleState => {
                            if let Some(gid) = next_header.group_id {
                                let refresh_header = base_header.child().with_group_id(gid);
                                let _ = self
                                    .msync_group(&refresh_header, ShardGroupId::new(gid), state_id)
                                    .await;
                            }
                        }
                        _ => {}
                    }

                    Ok(next_header)
                }
            },
        )
        .await?;

        // Update state cache from the final response.
        self.update_state_id_from_response(&outcome.result.0);

        Ok(outcome)
    }

    /// Call msync for a group to advance state_id.
    pub async fn msync_group(
        &self,
        base_header: &RequestHeader,
        group_id: ShardGroupId,
        min_state_id: Option<RaftLogId>,
    ) -> ClientResult<()> {
        let mut header = base_header.child();
        header.group_id = Some(group_id.as_raw());
        if let Some(sid) = min_state_id {
            header.state_id = Some(sid);
        }

        // Use MetadataClient's msync method directly
        let response = self.metadata_client.msync(&header, false).await?;

        // Update state_id from response
        if let Some(ref resp_header) = response.header {
            let resp_header: ResponseHeader = resp_header
                .clone()
                .try_into()
                .map_err(|e| ClientError::Metadata(format!("Failed to parse response header: {}", e)))?;
            self.update_state_id_from_response(&resp_header);
        }

        Ok(())
    }

    /// Refresh route table from metadata service.
    pub async fn refresh_route_table(&self, base_header: &RequestHeader) -> ClientResult<()> {
        let header = base_header.child();
        let response = self.metadata_client.get_route_table(&header).await?;

        // Update route table
        if let Some(ref resp_header) = response.header {
            let resp_header: ResponseHeader = resp_header
                .clone()
                .try_into()
                .map_err(|e| ClientError::Metadata(format!("Failed to parse response header: {}", e)))?;
            self.update_state_id_from_response(&resp_header);
        }

        let route_epoch = response.route_epoch;
        self.route_table
            .update_from_route_table(route_epoch, response.shard_to_group);

        // Update group role cache
        for (group_id_raw, leader_id) in &response.group_to_leader {
            let group_id = ShardGroupId::new(*group_id_raw);
            let followers = response
                .group_to_followers
                .get(group_id_raw)
                .map(|nl| nl.node_ids.clone())
                .unwrap_or_default();
            self.group_role.update(group_id, Some(*leader_id), followers);
        }

        Ok(())
    }
}
