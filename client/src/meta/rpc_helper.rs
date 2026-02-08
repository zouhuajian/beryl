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
use crate::canonical::{retry_metadata_once, validate_header_or_action, RefreshDispatchContext, RetryOutcome};
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RefreshDispatchAction {
    RefreshMountAndRoute,
    RefreshRoute,
    RefreshWorker,
    RefreshFencing,
    RefreshState,
}

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

    /// Handle response header: validate canonical error semantics and update state cache.
    pub fn handle_response_header(&self, response_header: &ResponseHeader) -> ClientResult<()> {
        match validate_header_or_action(response_header) {
            Ok(()) => {
                self.update_state_id_from_response(response_header);
                Ok(())
            }
            Err(action) => Err(ClientError::from(action)),
        }
    }

    fn refresh_action_for_reason(reason: RefreshReason) -> RefreshDispatchAction {
        match reason {
            RefreshReason::MountEpochMismatch => RefreshDispatchAction::RefreshMountAndRoute,
            RefreshReason::RouteEpochMismatch | RefreshReason::NotLeader => RefreshDispatchAction::RefreshRoute,
            RefreshReason::WorkerEpochMismatch => RefreshDispatchAction::RefreshWorker,
            RefreshReason::Fencing | RefreshReason::SessionInvalid | RefreshReason::SessionExpired => {
                RefreshDispatchAction::RefreshFencing
            }
            RefreshReason::StaleState | RefreshReason::BlockStampMismatch | RefreshReason::EpochMismatch => {
                RefreshDispatchAction::RefreshState
            }
            _ => RefreshDispatchAction::RefreshRoute,
        }
    }

    async fn dispatch_refresh(
        &self,
        base_header: &RequestHeader,
        current_header: RequestHeader,
        dispatch_ctx: RefreshDispatchContext,
    ) -> ClientResult<RequestHeader> {
        let RefreshDispatchContext {
            reason,
            hint,
            canonical: _canonical,
            response_header,
        } = dispatch_ctx;

        let action = Self::refresh_action_for_reason(reason);
        metrics::counter!(
            "client_metadata_refresh_dispatch_total",
            "reason" => format!("{:?}", reason),
            "action" => format!("{:?}", action)
        )
        .increment(1);

        let mut next_header = current_header.child_with_same_call_id();
        if let Some(gid) = hint.group_id.or(response_header.group_id) {
            next_header.group_id = Some(gid);
        }
        if let Some(state_id) = response_header.state_id {
            next_header.state_id = Some(state_id);
        }
        if let Some(mount_epoch) = hint.mount_epoch.or(response_header.mount_epoch) {
            next_header.mount_epoch = Some(mount_epoch);
        }

        match action {
            RefreshDispatchAction::RefreshMountAndRoute => {
                // TODO(ERR-4): replace route refresh fallback with dedicated mount-table refresh
                // once a mount refresh API is available on the metadata service.
                self.refresh_route_table(base_header).await?;
            }
            RefreshDispatchAction::RefreshRoute => {
                self.refresh_route_table(base_header).await?;
            }
            RefreshDispatchAction::RefreshWorker => {
                // Best available worker metadata refresh in metadata-plane helper.
                self.refresh_route_table(base_header).await?;
            }
            RefreshDispatchAction::RefreshFencing => {
                if let Some(gid) = next_header.group_id {
                    self.msync_group(base_header, ShardGroupId::new(gid), next_header.state_id)
                        .await?;
                }
                self.refresh_route_table(base_header).await?;
            }
            RefreshDispatchAction::RefreshState => {
                if let Some(gid) = next_header.group_id {
                    self.msync_group(base_header, ShardGroupId::new(gid), next_header.state_id)
                        .await?;
                } else {
                    self.refresh_route_table(base_header).await?;
                }
            }
        }

        Ok(next_header)
    }

    /// Execute a metadata RPC with bounded refresh/retry based on canonical_error.
    ///
    /// The `call` closure must return the parsed `ResponseHeader` plus payload for the RPC.
    /// Refresh behaviour is dispatched by `dispatch_refresh()` using refresh reason + hints.
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
        let base_header = base_header.clone();
        let initial_header = self.create_request_header(&base_header, group_id, is_read);
        let mut call = call;
        let outcome = retry_metadata_once(
            initial_header,
            move |hdr| {
                let fut = call(hdr.clone());
                async move { fut.await }
            },
            |dispatch_ctx, current_header| {
                let base_header = base_header.clone();
                async move { self.dispatch_refresh(&base_header, current_header, dispatch_ctx).await }
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

        let resp_header = response
            .header
            .ok_or_else(|| ClientError::Metadata("Missing response header".to_string()))?;
        let resp_header: ResponseHeader = resp_header
            .try_into()
            .map_err(|e| ClientError::Metadata(format!("Failed to parse response header: {}", e)))?;
        self.handle_response_header(&resp_header)?;

        Ok(())
    }

    /// Refresh route table from metadata service.
    pub async fn refresh_route_table(&self, base_header: &RequestHeader) -> ClientResult<()> {
        let header = base_header.child();
        let response = self.metadata_client.get_route_table(&header).await?;

        let resp_header = response
            .header
            .ok_or_else(|| ClientError::Metadata("Missing response header".to_string()))?;
        let resp_header: ResponseHeader = resp_header
            .try_into()
            .map_err(|e| ClientError::Metadata(format!("Failed to parse response header: {}", e)))?;
        self.handle_response_header(&resp_header)?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::StateIdCache;
    use crate::meta::MetadataClient;
    use crate::routing::{GroupRoleCache, RouteTable};
    use common::error::canonical::CanonicalError;
    use common::header::RequestHeader;
    use common::header::{ClientInfo, ResponseHeader, RpcErrorCode};
    use proto::metadata::metadata_route_service_proto_server::{
        MetadataRouteServiceProto, MetadataRouteServiceProtoServer,
    };
    use proto::metadata::*;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{transport::Server, Request, Response, Status};
    use types::ids::ShardGroupId;
    use types::ClientId;

    #[derive(Clone, Default)]
    struct MockRouteService;

    fn error_header(group_id: Option<u64>) -> proto::common::ResponseHeaderProto {
        let canonical = CanonicalError::need_refresh(
            RpcErrorCode::NotLeader,
            common::error::canonical::RefreshReason::NotLeader,
            "not leader",
        );
        let client = ClientInfo::new(ClientId::new(1));
        let header = ResponseHeader::error(client, canonical);
        let header = if let Some(gid) = group_id {
            header.with_group_id(gid)
        } else {
            header
        };
        (&header).into()
    }

    #[tonic::async_trait]
    impl MetadataRouteServiceProto for MockRouteService {
        async fn get_file_meta(
            &self,
            _request: Request<GetFileMetaRequestProto>,
        ) -> Result<Response<GetFileMetaResponseProto>, Status> {
            Err(Status::unimplemented("not implemented"))
        }

        async fn refresh_route(
            &self,
            _request: Request<RefreshRouteRequestProto>,
        ) -> Result<Response<RefreshRouteResponseProto>, Status> {
            Err(Status::unimplemented("not implemented"))
        }

        async fn msync(&self, request: Request<MsyncRequestProto>) -> Result<Response<MsyncResponseProto>, Status> {
            let req = request.into_inner();
            let group_id = req
                .header
                .as_ref()
                .and_then(|h| if h.group_id != 0 { Some(h.group_id) } else { None });
            Ok(Response::new(MsyncResponseProto {
                header: Some(error_header(group_id)),
                readable_follower_ids: vec![],
            }))
        }

        async fn get_route_table(
            &self,
            _request: Request<GetRouteTableRequestProto>,
        ) -> Result<Response<GetRouteTableResponseProto>, Status> {
            Ok(Response::new(GetRouteTableResponseProto {
                header: Some(error_header(None)),
                route_epoch: 42,
                shard_to_group: Default::default(),
                group_to_leader: Default::default(),
                group_to_followers: Default::default(),
            }))
        }
    }

    async fn start_mock_server() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            Server::builder()
                .add_service(MetadataRouteServiceProtoServer::new(MockRouteService::default()))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        addr
    }

    #[derive(Clone)]
    struct DispatchRouteService {
        route_calls: Arc<AtomicUsize>,
        msync_calls: Arc<AtomicUsize>,
    }

    impl DispatchRouteService {
        fn ok_header(group_id: Option<u64>) -> proto::common::ResponseHeaderProto {
            let header = if let Some(gid) = group_id {
                ResponseHeader::ok(ClientInfo::new(ClientId::new(1))).with_group_id(gid)
            } else {
                ResponseHeader::ok(ClientInfo::new(ClientId::new(1)))
            };
            (&header).into()
        }
    }

    #[tonic::async_trait]
    impl MetadataRouteServiceProto for DispatchRouteService {
        async fn get_file_meta(
            &self,
            _request: Request<GetFileMetaRequestProto>,
        ) -> Result<Response<GetFileMetaResponseProto>, Status> {
            Err(Status::unimplemented("not implemented"))
        }

        async fn refresh_route(
            &self,
            _request: Request<RefreshRouteRequestProto>,
        ) -> Result<Response<RefreshRouteResponseProto>, Status> {
            Err(Status::unimplemented("not implemented"))
        }

        async fn msync(&self, request: Request<MsyncRequestProto>) -> Result<Response<MsyncResponseProto>, Status> {
            self.msync_calls.fetch_add(1, Ordering::SeqCst);
            let req = request.into_inner();
            let group_id = req
                .header
                .as_ref()
                .and_then(|h| if h.group_id != 0 { Some(h.group_id) } else { None });
            Ok(Response::new(MsyncResponseProto {
                header: Some(Self::ok_header(group_id)),
                readable_follower_ids: vec![],
            }))
        }

        async fn get_route_table(
            &self,
            _request: Request<GetRouteTableRequestProto>,
        ) -> Result<Response<GetRouteTableResponseProto>, Status> {
            self.route_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Response::new(GetRouteTableResponseProto {
                header: Some(Self::ok_header(None)),
                route_epoch: 99,
                shard_to_group: Default::default(),
                group_to_leader: Default::default(),
                group_to_followers: Default::default(),
            }))
        }
    }

    async fn start_dispatch_server(service: DispatchRouteService) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            Server::builder()
                .add_service(MetadataRouteServiceProtoServer::new(service))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        addr
    }

    async fn build_helper(endpoint: &str) -> MetadataRpcHelper {
        let state_cache = Arc::new(StateIdCache::new(60));
        let route_table = Arc::new(RouteTable::new(crate::cache::RouteCache::new(64, 60)));
        let group_role = Arc::new(GroupRoleCache::new(60));
        let metadata_client = Arc::new(MetadataClient::new(endpoint).await.unwrap());
        MetadataRpcHelper::new(state_cache, route_table, group_role, metadata_client)
    }

    #[tokio::test]
    async fn test_msync_group_fails_on_header_error() {
        let addr = start_mock_server().await;
        let endpoint = format!("http://{}", addr);
        let helper = build_helper(&endpoint).await;

        let base_header = RequestHeader::new(ClientId::new(1));
        let result = helper.msync_group(&base_header, ShardGroupId::new(7), None).await;
        assert!(result.is_err());
        assert!(helper.get_state_id(&ShardGroupId::new(7)).is_none());
    }

    #[tokio::test]
    async fn test_refresh_route_table_fails_on_header_error() {
        let addr = start_mock_server().await;
        let endpoint = format!("http://{}", addr);
        let helper = build_helper(&endpoint).await;

        let base_header = RequestHeader::new(ClientId::new(1));
        let result = helper.refresh_route_table(&base_header).await;
        assert!(result.is_err());
        assert_eq!(helper.route_table.route_epoch(), 0);
    }

    #[tokio::test]
    async fn test_dispatch_refresh_mount_epoch_uses_route_refresh() {
        let route_calls = Arc::new(AtomicUsize::new(0));
        let msync_calls = Arc::new(AtomicUsize::new(0));
        let service = DispatchRouteService {
            route_calls: route_calls.clone(),
            msync_calls: msync_calls.clone(),
        };

        let addr = start_dispatch_server(service).await;
        let endpoint = format!("http://{}", addr);
        let helper = build_helper(&endpoint).await;

        let base_header = RequestHeader::new(ClientId::new(1)).with_group_id(9);
        let current_header = base_header.child_with_same_call_id();
        let canonical = CanonicalError::need_refresh(
            RpcErrorCode::MountEpochMismatch,
            RefreshReason::MountEpochMismatch,
            "mount mismatch",
        );
        let mut response_header =
            ResponseHeader::from_canonical(base_header.client.clone(), canonical.clone()).with_group_id(9);
        response_header.mount_epoch = Some(42);

        let next_header = helper
            .dispatch_refresh(
                &base_header,
                current_header,
                RefreshDispatchContext {
                    reason: RefreshReason::MountEpochMismatch,
                    hint: crate::canonical::RefreshHint {
                        group_id: Some(9),
                        mount_epoch: Some(42),
                        ..Default::default()
                    },
                    canonical,
                    response_header,
                },
            )
            .await
            .expect("mount refresh dispatch");

        assert_eq!(next_header.mount_epoch, Some(42));
        assert_eq!(route_calls.load(Ordering::SeqCst), 1);
        assert_eq!(msync_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_dispatch_refresh_not_leader_uses_route_refresh() {
        let route_calls = Arc::new(AtomicUsize::new(0));
        let msync_calls = Arc::new(AtomicUsize::new(0));
        let service = DispatchRouteService {
            route_calls: route_calls.clone(),
            msync_calls: msync_calls.clone(),
        };

        let addr = start_dispatch_server(service).await;
        let endpoint = format!("http://{}", addr);
        let helper = build_helper(&endpoint).await;

        let base_header = RequestHeader::new(ClientId::new(1)).with_group_id(7);
        let current_header = base_header.child_with_same_call_id();
        let canonical = CanonicalError::need_refresh(RpcErrorCode::NotLeader, RefreshReason::NotLeader, "not leader");
        let response_header =
            ResponseHeader::from_canonical(base_header.client.clone(), canonical.clone()).with_group_id(7);

        let _ = helper
            .dispatch_refresh(
                &base_header,
                current_header,
                RefreshDispatchContext {
                    reason: RefreshReason::NotLeader,
                    hint: crate::canonical::RefreshHint {
                        group_id: Some(7),
                        ..Default::default()
                    },
                    canonical,
                    response_header,
                },
            )
            .await
            .expect("route refresh dispatch");

        assert_eq!(route_calls.load(Ordering::SeqCst), 1);
        assert_eq!(msync_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_dispatch_refresh_stale_state_uses_msync() {
        let route_calls = Arc::new(AtomicUsize::new(0));
        let msync_calls = Arc::new(AtomicUsize::new(0));
        let service = DispatchRouteService {
            route_calls: route_calls.clone(),
            msync_calls: msync_calls.clone(),
        };

        let addr = start_dispatch_server(service).await;
        let endpoint = format!("http://{}", addr);
        let helper = build_helper(&endpoint).await;

        let base_header = RequestHeader::new(ClientId::new(1)).with_group_id(5);
        let mut current_header = base_header.child_with_same_call_id();
        current_header.state_id = Some(RaftLogId::new(1, 1, 7));
        let canonical =
            CanonicalError::need_refresh(RpcErrorCode::StaleState, RefreshReason::StaleState, "stale state");
        let mut response_header =
            ResponseHeader::from_canonical(base_header.client.clone(), canonical.clone()).with_group_id(5);
        response_header.state_id = Some(RaftLogId::new(1, 1, 7));

        let _ = helper
            .dispatch_refresh(
                &base_header,
                current_header,
                RefreshDispatchContext {
                    reason: RefreshReason::StaleState,
                    hint: crate::canonical::RefreshHint {
                        group_id: Some(5),
                        ..Default::default()
                    },
                    canonical,
                    response_header,
                },
            )
            .await
            .expect("stale state dispatch");

        assert_eq!(route_calls.load(Ordering::SeqCst), 0);
        assert_eq!(msync_calls.load(Ordering::SeqCst), 1);
    }
}
