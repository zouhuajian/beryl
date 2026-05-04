#![deny(deprecated)]
// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Helper for metadata RPC calls with group_id and state_id management.
//!
//! This module provides unified handling for:
//! - Group ID routing (path -> group_id)
//! - State ID watermark management (group_id -> state_id cache)
//! - Follower read selection
//! - Error handling and retry (STALE_STATE -> Msync, route refresh stays filesystem-owned)

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
use types::{GroupStateWatermark, RaftLogId};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RefreshDispatchAction {
    UnsupportedRouteRefresh,
    RefreshState,
}

/// RPC helper for metadata operations with group-aware state management.
pub struct MetadataRpcHelper {
    /// State ID cache (group_id -> state_id).
    state_cache: Arc<StateIdCache>,
    /// Route table (path/data_handle_id -> group_id).
    route_table: Arc<RouteTable>,
    /// Group role cache (group_id -> leader/followers).
    _group_role: Arc<GroupRoleCache>,
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
            _group_role: group_role,
            metadata_client,
        }
    }

    /// Resolve path to group_id.
    /// Returns None if path cannot be resolved by the local route cache.
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
        if !response_header.state.is_empty() {
            self.state_cache.merge_if_ahead(response_header.state.clone());
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
                    header.state = vec![GroupStateWatermark::new(gid, state_id)];
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
            RefreshReason::StaleState => RefreshDispatchAction::RefreshState,
            _ => RefreshDispatchAction::UnsupportedRouteRefresh,
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
        if !response_header.state.is_empty() {
            next_header.state = response_header.state.clone();
        }
        if let Some(mount_epoch) = hint.mount_epoch.or(response_header.mount_epoch) {
            next_header.mount_epoch = Some(mount_epoch);
        }

        match action {
            RefreshDispatchAction::UnsupportedRouteRefresh => {
                self.fail_route_refresh_without_context(base_header).await?;
            }
            RefreshDispatchAction::RefreshState => {
                if let Some(gid) = next_header.group_id {
                    let group_id = ShardGroupId::new(gid);
                    let required_state = next_header
                        .state
                        .iter()
                        .find(|watermark| watermark.group_id == group_id)
                        .copied();
                    let response_state = self.msync_group(base_header, group_id).await?;
                    if let Some(required_state) = required_state {
                        Self::ensure_msync_reached_required_state(response_state, required_state)?;
                    }
                } else {
                    self.fail_route_refresh_without_context(base_header).await?;
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
    ) -> ClientResult<GroupStateWatermark> {
        let mut header = base_header.child();
        header.group_id = Some(group_id.as_raw());
        header.state.clear();

        let response = self.metadata_client.msync(&header).await?;

        let resp_header = response
            .header
            .ok_or_else(|| ClientError::Metadata("Missing response header".to_string()))?;
        let resp_header: ResponseHeader = resp_header
            .try_into()
            .map_err(|e| ClientError::Metadata(format!("Failed to parse response header: {}", e)))?;
        self.handle_response_header(&resp_header)?;
        let response_state: GroupStateWatermark = response
            .state
            .ok_or_else(|| ClientError::Metadata("MsyncResponseProto missing state".to_string()))?
            .try_into()
            .map_err(|e| ClientError::Metadata(format!("Failed to parse msync response state: {}", e)))?;
        if response_state.group_id != group_id {
            return Err(ClientError::Metadata(format!(
                "MsyncResponseProto state group_id {} does not match requested group_id {}",
                response_state.group_id.as_raw(),
                group_id.as_raw()
            )));
        }
        self.state_cache.update_if_ahead(response_state);

        Ok(response_state)
    }

    fn ensure_msync_reached_required_state(
        response_state: GroupStateWatermark,
        required_state: GroupStateWatermark,
    ) -> ClientResult<()> {
        if response_state.group_id != required_state.group_id {
            return Err(ClientError::Metadata(format!(
                "MsyncResponseProto state group_id {} does not match required group_id {}",
                response_state.group_id.as_raw(),
                required_state.group_id.as_raw()
            )));
        }
        if !response_state.state_id.has_reached(&required_state.state_id) {
            return Err(ClientError::Metadata(format!(
                "Msync response state not reached: current={:?}, required={:?}",
                response_state.state_id, required_state.state_id
            )));
        }
        Ok(())
    }

    /// Route-cache refresh is unavailable without a FileSystemService operation-specific path.
    pub async fn fail_route_refresh_without_context(&self, _base_header: &RequestHeader) -> ClientResult<()> {
        Err(ClientError::Metadata(
            "route cache refresh is unavailable without operation context".to_string(),
        ))
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
    use proto::metadata::file_system_service_proto_server::{FileSystemServiceProto, FileSystemServiceProtoServer};
    use proto::metadata::*;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{transport::Server, Request, Response, Status};
    use types::ids::ShardGroupId;
    use types::ClientId;

    macro_rules! impl_test_filesystem_service {
        ($ty:ty) => {
            #[tonic::async_trait]
            impl FileSystemServiceProto for $ty {
                async fn get_file_status(
                    &self,
                    _request: Request<GetFileStatusRequestProto>,
                ) -> Result<Response<GetFileStatusResponseProto>, Status> {
                    Err(Status::unimplemented("not implemented"))
                }
                async fn mkdir(
                    &self,
                    _request: Request<MkdirPathRequestProto>,
                ) -> Result<Response<MkdirPathResponseProto>, Status> {
                    Err(Status::unimplemented("not implemented"))
                }
                async fn create(
                    &self,
                    _request: Request<CreatePathRequestProto>,
                ) -> Result<Response<CreatePathResponseProto>, Status> {
                    Err(Status::unimplemented("not implemented"))
                }
                async fn delete(
                    &self,
                    _request: Request<DeleteRequestProto>,
                ) -> Result<Response<DeleteResponseProto>, Status> {
                    Err(Status::unimplemented("not implemented"))
                }
                async fn rename(
                    &self,
                    _request: Request<RenamePathRequestProto>,
                ) -> Result<Response<RenamePathResponseProto>, Status> {
                    Err(Status::unimplemented("not implemented"))
                }
                async fn list_status(
                    &self,
                    _request: Request<ListStatusPathRequestProto>,
                ) -> Result<Response<ListStatusPathResponseProto>, Status> {
                    Err(Status::unimplemented("not implemented"))
                }
                async fn open(
                    &self,
                    _request: Request<OpenPathRequestProto>,
                ) -> Result<Response<OpenPathResponseProto>, Status> {
                    Err(Status::unimplemented("not implemented"))
                }
                async fn release(
                    &self,
                    _request: Request<ReleasePathRequestProto>,
                ) -> Result<Response<ReleasePathResponseProto>, Status> {
                    Err(Status::unimplemented("not implemented"))
                }
                async fn fsync(
                    &self,
                    _request: Request<FsyncPathRequestProto>,
                ) -> Result<Response<FsyncPathResponseProto>, Status> {
                    Err(Status::unimplemented("not implemented"))
                }
                async fn hsync(
                    &self,
                    _request: Request<HsyncPathRequestProto>,
                ) -> Result<Response<HsyncPathResponseProto>, Status> {
                    Err(Status::unimplemented("not implemented"))
                }
                async fn hflush(
                    &self,
                    _request: Request<HflushPathRequestProto>,
                ) -> Result<Response<HflushPathResponseProto>, Status> {
                    Err(Status::unimplemented("not implemented"))
                }
                async fn truncate(
                    &self,
                    _request: Request<TruncatePathRequestProto>,
                ) -> Result<Response<TruncatePathResponseProto>, Status> {
                    Err(Status::unimplemented("not implemented"))
                }
                async fn set_xattr(
                    &self,
                    _request: Request<SetXattrPathRequestProto>,
                ) -> Result<Response<SetXattrPathResponseProto>, Status> {
                    Err(Status::unimplemented("not implemented"))
                }
                async fn get_xattr(
                    &self,
                    _request: Request<GetXattrPathRequestProto>,
                ) -> Result<Response<GetXattrPathResponseProto>, Status> {
                    Err(Status::unimplemented("not implemented"))
                }
                async fn list_xattr(
                    &self,
                    _request: Request<ListXattrPathRequestProto>,
                ) -> Result<Response<ListXattrPathResponseProto>, Status> {
                    Err(Status::unimplemented("not implemented"))
                }
                async fn remove_xattr(
                    &self,
                    _request: Request<RemoveXattrPathRequestProto>,
                ) -> Result<Response<RemoveXattrPathResponseProto>, Status> {
                    Err(Status::unimplemented("not implemented"))
                }
                async fn get_file_layout_by_path(
                    &self,
                    _request: Request<GetFileLayoutByPathRequestProto>,
                ) -> Result<Response<GetFileLayoutByPathResponseProto>, Status> {
                    Err(Status::unimplemented("not implemented"))
                }
                async fn open_write_by_path(
                    &self,
                    _request: Request<OpenWriteByPathRequestProto>,
                ) -> Result<Response<OpenWriteByPathResponseProto>, Status> {
                    Err(Status::unimplemented("not implemented"))
                }
                async fn close_write_session(
                    &self,
                    _request: Request<CloseWriteSessionRequestProto>,
                ) -> Result<Response<CloseWriteSessionResponseProto>, Status> {
                    Err(Status::unimplemented("not implemented"))
                }
                async fn renew_write_session_lease(
                    &self,
                    _request: Request<RenewWriteSessionLeaseRequestProto>,
                ) -> Result<Response<RenewWriteSessionLeaseResponseProto>, Status> {
                    Err(Status::unimplemented("not implemented"))
                }
                async fn fsync_session(
                    &self,
                    _request: Request<FsyncSessionRequestProto>,
                ) -> Result<Response<FsyncSessionResponseProto>, Status> {
                    Err(Status::unimplemented("not implemented"))
                }
                async fn hsync_session(
                    &self,
                    _request: Request<HsyncSessionRequestProto>,
                ) -> Result<Response<HsyncSessionResponseProto>, Status> {
                    Err(Status::unimplemented("not implemented"))
                }
                async fn hflush_session(
                    &self,
                    _request: Request<HflushSessionRequestProto>,
                ) -> Result<Response<HflushSessionResponseProto>, Status> {
                    Err(Status::unimplemented("not implemented"))
                }
                async fn release_session(
                    &self,
                    _request: Request<ReleaseSessionRequestProto>,
                ) -> Result<Response<ReleaseSessionResponseProto>, Status> {
                    Err(Status::unimplemented("not implemented"))
                }
                async fn msync(
                    &self,
                    request: Request<MsyncRequestProto>,
                ) -> Result<Response<MsyncResponseProto>, Status> {
                    self.handle_msync(request).await
                }
            }
        };
    }

    #[derive(Clone, Default)]
    struct MockFileSystemService;

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

    impl MockFileSystemService {
        async fn handle_msync(
            &self,
            request: Request<MsyncRequestProto>,
        ) -> Result<Response<MsyncResponseProto>, Status> {
            let req = request.into_inner();
            let group_id = req
                .header
                .as_ref()
                .and_then(|h| if h.group_id != 0 { Some(h.group_id) } else { None });
            Ok(Response::new(MsyncResponseProto {
                header: Some(error_header(group_id)),
                state: None,
            }))
        }
    }
    impl_test_filesystem_service!(MockFileSystemService);

    async fn start_mock_server() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            Server::builder()
                .add_service(FileSystemServiceProtoServer::new(MockFileSystemService))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        addr
    }

    #[derive(Clone)]
    struct DispatchFileSystemService {
        msync_calls: Arc<AtomicUsize>,
        response_index: u64,
        observed_header_state_len: Arc<AtomicUsize>,
    }

    impl DispatchFileSystemService {
        fn ok_header(group_id: Option<u64>) -> proto::common::ResponseHeaderProto {
            let header = if let Some(gid) = group_id {
                ResponseHeader::ok(ClientInfo::new(ClientId::new(1))).with_group_id(gid)
            } else {
                ResponseHeader::ok(ClientInfo::new(ClientId::new(1)))
            };
            (&header).into()
        }
    }

    impl DispatchFileSystemService {
        async fn handle_msync(
            &self,
            request: Request<MsyncRequestProto>,
        ) -> Result<Response<MsyncResponseProto>, Status> {
            self.msync_calls.fetch_add(1, Ordering::SeqCst);
            let req = request.into_inner();
            self.observed_header_state_len.store(
                req.header.as_ref().map_or(0, |header| header.state.len()),
                Ordering::SeqCst,
            );
            let group_id = req
                .header
                .as_ref()
                .and_then(|h| if h.group_id != 0 { Some(h.group_id) } else { None });
            let state = group_id.map(|gid| proto::common::GroupStateWatermarkProto {
                group_id: Some(proto::common::ShardGroupIdProto { value: gid }),
                state_id: Some(proto::common::RaftLogIdProto {
                    term: 1,
                    leader_node_id: 1,
                    index: self.response_index,
                }),
            });
            Ok(Response::new(MsyncResponseProto {
                header: Some(Self::ok_header(group_id)),
                state,
            }))
        }
    }
    impl_test_filesystem_service!(DispatchFileSystemService);

    async fn start_dispatch_server(service: DispatchFileSystemService) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            Server::builder()
                .add_service(FileSystemServiceProtoServer::new(service))
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
        let result = helper.msync_group(&base_header, ShardGroupId::new(7)).await;
        assert!(result.is_err());
        assert!(helper.get_state_id(&ShardGroupId::new(7)).is_none());
    }

    #[tokio::test]
    async fn test_route_refresh_without_context_returns_error() {
        let addr = start_mock_server().await;
        let endpoint = format!("http://{}", addr);
        let helper = build_helper(&endpoint).await;

        let base_header = RequestHeader::new(ClientId::new(1));
        let result = helper.fail_route_refresh_without_context(&base_header).await;
        assert!(result.is_err());
        assert_eq!(helper.route_table.route_epoch(), 0);
    }

    #[tokio::test]
    async fn test_dispatch_refresh_mount_epoch_returns_explicit_unavailable_error() {
        let msync_calls = Arc::new(AtomicUsize::new(0));
        let service = DispatchFileSystemService {
            msync_calls: msync_calls.clone(),
            response_index: 7,
            observed_header_state_len: Arc::new(AtomicUsize::new(0)),
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

        let result = helper
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
            .await;

        assert!(result.is_err());
        assert_eq!(msync_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_dispatch_refresh_not_leader_returns_explicit_unavailable_error() {
        let msync_calls = Arc::new(AtomicUsize::new(0));
        let service = DispatchFileSystemService {
            msync_calls: msync_calls.clone(),
            response_index: 7,
            observed_header_state_len: Arc::new(AtomicUsize::new(0)),
        };

        let addr = start_dispatch_server(service).await;
        let endpoint = format!("http://{}", addr);
        let helper = build_helper(&endpoint).await;

        let base_header = RequestHeader::new(ClientId::new(1)).with_group_id(7);
        let current_header = base_header.child_with_same_call_id();
        let canonical = CanonicalError::need_refresh(RpcErrorCode::NotLeader, RefreshReason::NotLeader, "not leader");
        let response_header =
            ResponseHeader::from_canonical(base_header.client.clone(), canonical.clone()).with_group_id(7);

        let result = helper
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
            .await;

        assert!(result.is_err());
        assert_eq!(msync_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_dispatch_refresh_block_stamp_returns_explicit_unavailable_error_without_msync() {
        let msync_calls = Arc::new(AtomicUsize::new(0));
        let service = DispatchFileSystemService {
            msync_calls: msync_calls.clone(),
            response_index: 7,
            observed_header_state_len: Arc::new(AtomicUsize::new(0)),
        };

        let addr = start_dispatch_server(service).await;
        let endpoint = format!("http://{}", addr);
        let helper = build_helper(&endpoint).await;

        let base_header = RequestHeader::new(ClientId::new(1)).with_group_id(8);
        let current_header = base_header.child_with_same_call_id();
        let canonical = CanonicalError::need_refresh(
            RpcErrorCode::BlockStampMismatch,
            RefreshReason::BlockStampMismatch,
            "block stamp mismatch",
        );
        let response_header =
            ResponseHeader::from_canonical(base_header.client.clone(), canonical.clone()).with_group_id(8);

        let result = helper
            .dispatch_refresh(
                &base_header,
                current_header,
                RefreshDispatchContext {
                    reason: RefreshReason::BlockStampMismatch,
                    hint: crate::canonical::RefreshHint {
                        group_id: Some(8),
                        ..Default::default()
                    },
                    canonical,
                    response_header,
                },
            )
            .await;

        assert!(result.is_err());
        assert_eq!(msync_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_dispatch_refresh_stale_state_uses_msync() {
        let msync_calls = Arc::new(AtomicUsize::new(0));
        let service = DispatchFileSystemService {
            msync_calls: msync_calls.clone(),
            response_index: 7,
            observed_header_state_len: Arc::new(AtomicUsize::new(0)),
        };

        let addr = start_dispatch_server(service).await;
        let endpoint = format!("http://{}", addr);
        let helper = build_helper(&endpoint).await;

        let base_header = RequestHeader::new(ClientId::new(1)).with_group_id(5);
        let mut current_header = base_header.child_with_same_call_id();
        current_header.state = vec![GroupStateWatermark::new(ShardGroupId::new(5), RaftLogId::new(1, 1, 7))];
        let canonical =
            CanonicalError::need_refresh(RpcErrorCode::StaleState, RefreshReason::StaleState, "stale state");
        let mut response_header =
            ResponseHeader::from_canonical(base_header.client.clone(), canonical.clone()).with_group_id(5);
        response_header.state = vec![GroupStateWatermark::new(ShardGroupId::new(5), RaftLogId::new(1, 1, 7))];

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

        assert_eq!(msync_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_msync_group_merges_response_state_without_sending_header_state() {
        let msync_calls = Arc::new(AtomicUsize::new(0));
        let observed_header_state_len = Arc::new(AtomicUsize::new(usize::MAX));
        let service = DispatchFileSystemService {
            msync_calls: msync_calls.clone(),
            response_index: 11,
            observed_header_state_len: observed_header_state_len.clone(),
        };

        let addr = start_dispatch_server(service).await;
        let endpoint = format!("http://{}", addr);
        let helper = build_helper(&endpoint).await;

        let group_id = ShardGroupId::new(6);
        let mut base_header = RequestHeader::new(ClientId::new(1)).with_group_id(group_id.as_raw());
        base_header.state = vec![GroupStateWatermark::new(group_id, RaftLogId::new(1, 1, 99))];

        let response_state = helper
            .msync_group(&base_header, group_id)
            .await
            .expect("msync_group should merge server state");

        assert_eq!(response_state.state_id, RaftLogId::new(1, 1, 11));
        assert_eq!(helper.get_state_id(&group_id), Some(RaftLogId::new(1, 1, 11)));
        assert_eq!(msync_calls.load(Ordering::SeqCst), 1);
        assert_eq!(observed_header_state_len.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_msync_required_watermark_comparison_is_client_side() {
        let group_id = ShardGroupId::new(6);
        let response_state = GroupStateWatermark::new(group_id, RaftLogId::new(1, 1, 10));
        let required_state = GroupStateWatermark::new(group_id, RaftLogId::new(1, 1, 11));

        let result = MetadataRpcHelper::ensure_msync_reached_required_state(response_state, required_state);

        assert!(result.is_err());
    }
}
