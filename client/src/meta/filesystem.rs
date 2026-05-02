// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! FileSystemService client action machine.
//!
//! This module implements a client-side closed loop for metadata filesystem RPCs:
//! - gRPC non-OK is treated as transport/framework failure.
//! - gRPC OK + `ResponseHeader.error` is treated as canonical business/protocol error.
//! - NEED_REFRESH triggers targeted refresh handlers and safe replay.
//! - Authz denials (`EACCES`/`EPERM`) are terminal and never replayed.

use crate::canonical::{parse_rpc_envelope, ClientAction, RefreshHint, RpcEnvelope};
use crate::error::{ClientError, ClientResult};
use async_trait::async_trait;
use common::error::canonical::{CanonicalError, ErrorClass, ErrorCode as CanonicalErrorCode, RefreshReason};
use common::header::RpcErrorCode;
use dashmap::DashMap;
use parking_lot::RwLock;
use proto::common::{ClientInfoProto, RequestHeaderProto, ResponseHeaderProto, WorkerEndpointInfoProto};
use proto::metadata::file_system_service_proto_client::FileSystemServiceProtoClient;
use proto::metadata::{
    CloseWriteSessionRequestProto, CloseWriteSessionResponseProto, DeleteRequestProto, DeleteResponseProto,
    FsyncSessionRequestProto, FsyncSessionResponseProto, GetFileLayoutByPathRequestProto,
    GetFileLayoutByPathResponseProto, GetFileStatusRequestProto, GetFileStatusResponseProto, HflushSessionRequestProto,
    HflushSessionResponseProto, HsyncSessionRequestProto, HsyncSessionResponseProto, ListStatusPathRequestProto,
    ListStatusPathResponseProto, OpenWriteByPathRequestProto, OpenWriteByPathResponseProto,
};
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Channel;
use types::fs::FsErrorCode;
use types::CallId;

/// Replay policy for a filesystem RPC.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplayPolicy {
    /// Idempotent metadata reads (`GetFileStatus`, `ListStatus`, `GetFileLayoutByPath`).
    IdempotentRead,
    /// Mutating metadata write/open operations.
    Mutation,
    /// Session barrier ops (`FsyncSession`/`HsyncSession`/`HflushSession`).
    SessionBarrier,
    /// Best-effort cleanup operations (currently unused by the FileSystemService client surface).
    CleanupBestEffort,
}

/// Stable identifier for each public FileSystemService client RPC.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FileSystemRpcMethod {
    /// `GetFileStatus`.
    GetFileStatus,
    /// `ListStatus`.
    ListStatus,
    /// `GetFileLayoutByPath`.
    GetFileLayoutByPath,
    /// `Delete`.
    Delete,
    /// `OpenWriteByPath`.
    OpenWriteByPath,
    /// `FsyncSession`.
    FsyncSession,
    /// `HsyncSession`.
    HsyncSession,
    /// `HflushSession`.
    HflushSession,
    /// `CloseWriteSession`.
    CloseWriteSession,
}

impl FileSystemRpcMethod {
    #[cfg(test)]
    const ALL: [Self; 9] = [
        Self::GetFileStatus,
        Self::ListStatus,
        Self::GetFileLayoutByPath,
        Self::Delete,
        Self::OpenWriteByPath,
        Self::FsyncSession,
        Self::HsyncSession,
        Self::HflushSession,
        Self::CloseWriteSession,
    ];
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ReplayPolicyMapping {
    method: FileSystemRpcMethod,
    policy: ReplayPolicy,
}

const FILESYSTEM_RPC_REPLAY_POLICIES: [ReplayPolicyMapping; 9] = [
    ReplayPolicyMapping {
        method: FileSystemRpcMethod::GetFileStatus,
        policy: ReplayPolicy::IdempotentRead,
    },
    ReplayPolicyMapping {
        method: FileSystemRpcMethod::ListStatus,
        policy: ReplayPolicy::IdempotentRead,
    },
    ReplayPolicyMapping {
        method: FileSystemRpcMethod::GetFileLayoutByPath,
        policy: ReplayPolicy::IdempotentRead,
    },
    ReplayPolicyMapping {
        method: FileSystemRpcMethod::Delete,
        policy: ReplayPolicy::Mutation,
    },
    ReplayPolicyMapping {
        method: FileSystemRpcMethod::OpenWriteByPath,
        policy: ReplayPolicy::Mutation,
    },
    ReplayPolicyMapping {
        method: FileSystemRpcMethod::FsyncSession,
        policy: ReplayPolicy::SessionBarrier,
    },
    ReplayPolicyMapping {
        method: FileSystemRpcMethod::HsyncSession,
        policy: ReplayPolicy::SessionBarrier,
    },
    ReplayPolicyMapping {
        method: FileSystemRpcMethod::HflushSession,
        policy: ReplayPolicy::SessionBarrier,
    },
    ReplayPolicyMapping {
        method: FileSystemRpcMethod::CloseWriteSession,
        policy: ReplayPolicy::SessionBarrier,
    },
];

/// Lookup table for FileSystemService replay behavior.
pub fn replay_policy_for_method(method: FileSystemRpcMethod) -> ReplayPolicy {
    FILESYSTEM_RPC_REPLAY_POLICIES
        .iter()
        .find_map(|entry| (entry.method == method).then_some(entry.policy))
        .expect("all FileSystemService methods must have replay policy entries")
}

/// Retry/refresh limits for the action machine.
#[derive(Clone, Debug)]
pub struct ActionMachinePolicy {
    /// Maximum NEED_REFRESH handling attempts per operation.
    pub max_refresh_attempts: u32,
    /// Maximum RETRYABLE canonical retries per operation.
    pub max_retryable_attempts: u32,
    /// Maximum transient gRPC transport retries per operation.
    pub max_transport_retries: u32,
    /// Base exponential backoff in milliseconds.
    pub base_backoff_ms: u64,
}

impl Default for ActionMachinePolicy {
    fn default() -> Self {
        Self {
            max_refresh_attempts: 3,
            max_retryable_attempts: 2,
            max_transport_retries: 2,
            base_backoff_ms: 50,
        }
    }
}

/// Mockable FileSystemService RPC trait.
#[async_trait]
pub trait FileSystemRpc: Send + Sync {
    /// Call `GetFileStatus`.
    async fn get_file_status(
        &self,
        request: GetFileStatusRequestProto,
    ) -> Result<GetFileStatusResponseProto, tonic::Status>;

    /// Call `ListStatus`.
    async fn list_status(
        &self,
        request: ListStatusPathRequestProto,
    ) -> Result<ListStatusPathResponseProto, tonic::Status>;

    /// Call `GetFileLayoutByPath`.
    async fn get_file_layout_by_path(
        &self,
        request: GetFileLayoutByPathRequestProto,
    ) -> Result<GetFileLayoutByPathResponseProto, tonic::Status>;

    /// Call `Delete`.
    async fn delete(&self, request: DeleteRequestProto) -> Result<DeleteResponseProto, tonic::Status>;

    /// Call `OpenWriteByPath`.
    async fn open_write_by_path(
        &self,
        request: OpenWriteByPathRequestProto,
    ) -> Result<OpenWriteByPathResponseProto, tonic::Status>;

    /// Call `FsyncSession`.
    async fn fsync_session(
        &self,
        request: FsyncSessionRequestProto,
    ) -> Result<FsyncSessionResponseProto, tonic::Status>;

    /// Call `HsyncSession`.
    async fn hsync_session(
        &self,
        request: HsyncSessionRequestProto,
    ) -> Result<HsyncSessionResponseProto, tonic::Status>;

    /// Call `HflushSession`.
    async fn hflush_session(
        &self,
        request: HflushSessionRequestProto,
    ) -> Result<HflushSessionResponseProto, tonic::Status>;

    /// Call `CloseWriteSession`.
    async fn close_write_session(
        &self,
        request: CloseWriteSessionRequestProto,
    ) -> Result<CloseWriteSessionResponseProto, tonic::Status>;

    /// Reconnect to a different metadata endpoint (leader refresh path).
    async fn reconnect(&self, _endpoint: &str) -> ClientResult<()> {
        Ok(())
    }

    /// Current active endpoint.
    fn current_endpoint(&self) -> Option<String> {
        None
    }
}

/// Real tonic-based FileSystemService RPC client.
pub struct TonicFileSystemRpc {
    endpoint: RwLock<String>,
    client: tokio::sync::Mutex<FileSystemServiceProtoClient<Channel>>,
}

impl TonicFileSystemRpc {
    /// Connect to a FileSystemService endpoint.
    pub async fn connect(endpoint: impl Into<String>) -> ClientResult<Self> {
        let endpoint = normalize_endpoint(&endpoint.into());
        let channel = connect_channel(&endpoint).await?;
        let client = FileSystemServiceProtoClient::new(channel);
        Ok(Self {
            endpoint: RwLock::new(endpoint),
            client: tokio::sync::Mutex::new(client),
        })
    }
}

#[async_trait]
impl FileSystemRpc for TonicFileSystemRpc {
    async fn get_file_status(
        &self,
        request: GetFileStatusRequestProto,
    ) -> Result<GetFileStatusResponseProto, tonic::Status> {
        let mut client = self.client.lock().await;
        client
            .get_file_status(tonic::Request::new(request))
            .await
            .map(|resp| resp.into_inner())
    }

    async fn list_status(
        &self,
        request: ListStatusPathRequestProto,
    ) -> Result<ListStatusPathResponseProto, tonic::Status> {
        let mut client = self.client.lock().await;
        client
            .list_status(tonic::Request::new(request))
            .await
            .map(|resp| resp.into_inner())
    }

    async fn get_file_layout_by_path(
        &self,
        request: GetFileLayoutByPathRequestProto,
    ) -> Result<GetFileLayoutByPathResponseProto, tonic::Status> {
        let mut client = self.client.lock().await;
        client
            .get_file_layout_by_path(tonic::Request::new(request))
            .await
            .map(|resp| resp.into_inner())
    }

    async fn delete(&self, request: DeleteRequestProto) -> Result<DeleteResponseProto, tonic::Status> {
        let mut client = self.client.lock().await;
        client
            .delete(tonic::Request::new(request))
            .await
            .map(|resp| resp.into_inner())
    }

    async fn open_write_by_path(
        &self,
        request: OpenWriteByPathRequestProto,
    ) -> Result<OpenWriteByPathResponseProto, tonic::Status> {
        let mut client = self.client.lock().await;
        client
            .open_write_by_path(tonic::Request::new(request))
            .await
            .map(|resp| resp.into_inner())
    }

    async fn fsync_session(
        &self,
        request: FsyncSessionRequestProto,
    ) -> Result<FsyncSessionResponseProto, tonic::Status> {
        let mut client = self.client.lock().await;
        client
            .fsync_session(tonic::Request::new(request))
            .await
            .map(|resp| resp.into_inner())
    }

    async fn hsync_session(
        &self,
        request: HsyncSessionRequestProto,
    ) -> Result<HsyncSessionResponseProto, tonic::Status> {
        let mut client = self.client.lock().await;
        client
            .hsync_session(tonic::Request::new(request))
            .await
            .map(|resp| resp.into_inner())
    }

    async fn hflush_session(
        &self,
        request: HflushSessionRequestProto,
    ) -> Result<HflushSessionResponseProto, tonic::Status> {
        let mut client = self.client.lock().await;
        client
            .hflush_session(tonic::Request::new(request))
            .await
            .map(|resp| resp.into_inner())
    }

    async fn close_write_session(
        &self,
        request: CloseWriteSessionRequestProto,
    ) -> Result<CloseWriteSessionResponseProto, tonic::Status> {
        let mut client = self.client.lock().await;
        client
            .close_write_session(tonic::Request::new(request))
            .await
            .map(|resp| resp.into_inner())
    }

    async fn reconnect(&self, endpoint: &str) -> ClientResult<()> {
        let endpoint = normalize_endpoint(endpoint);
        let channel = connect_channel(&endpoint).await?;
        let mut client = self.client.lock().await;
        *client = FileSystemServiceProtoClient::new(channel);
        *self.endpoint.write() = endpoint;
        Ok(())
    }

    fn current_endpoint(&self) -> Option<String> {
        Some(self.endpoint.read().clone())
    }
}

async fn connect_channel(endpoint: &str) -> ClientResult<Channel> {
    Channel::from_shared(endpoint.to_string())
        .map_err(|e| ClientError::Metadata(format!("invalid metadata endpoint {}: {}", endpoint, e)))?
        .connect()
        .await
        .map_err(|e| ClientError::Metadata(format!("failed to connect metadata endpoint {}: {}", endpoint, e)))
}

/// Operation wrapper consumed by [`ActionMachine::call_with_refresh`].
pub struct RpcOp<T> {
    method: FileSystemRpcMethod,
    request: RequestEnvelope,
    execute: RpcExecutor,
    decode: DecodeFn<T>,
    _marker: PhantomData<T>,
}

impl<T> RpcOp<T> {
    fn new(request: RequestEnvelope, decode: DecodeFn<T>) -> Self {
        let execute: RpcExecutor = Arc::new(|rpc, request| Box::pin(execute_request_on_rpc(rpc, request)));
        Self {
            method: request.method(),
            request,
            execute,
            decode,
            _marker: PhantomData,
        }
    }

    /// Return the RPC identifier associated with this operation.
    pub fn method(&self) -> FileSystemRpcMethod {
        self.method
    }
}

impl RpcOp<GetFileStatusResponseProto> {
    /// Build an operation for `GetFileStatus`.
    pub fn get_file_status(request: GetFileStatusRequestProto) -> Self {
        Self::new(
            RequestEnvelope::GetFileStatus(request),
            Arc::new(|resp| match resp {
                ResponseEnvelope::GetFileStatus(v) => Ok(v),
                other => Err(unexpected_response("GetFileStatus", other.op_name())),
            }),
        )
    }
}

impl RpcOp<ListStatusPathResponseProto> {
    /// Build an operation for `ListStatus`.
    pub fn list_status(request: ListStatusPathRequestProto) -> Self {
        Self::new(
            RequestEnvelope::ListStatus(request),
            Arc::new(|resp| match resp {
                ResponseEnvelope::ListStatus(v) => Ok(v),
                other => Err(unexpected_response("ListStatus", other.op_name())),
            }),
        )
    }
}

impl RpcOp<GetFileLayoutByPathResponseProto> {
    /// Build an operation for `GetFileLayoutByPath`.
    pub fn get_file_layout_by_path(request: GetFileLayoutByPathRequestProto) -> Self {
        Self::new(
            RequestEnvelope::GetFileLayoutByPath(request),
            Arc::new(|resp| match resp {
                ResponseEnvelope::GetFileLayoutByPath(v) => Ok(v),
                other => Err(unexpected_response("GetFileLayoutByPath", other.op_name())),
            }),
        )
    }
}

impl RpcOp<DeleteResponseProto> {
    /// Build an operation for `Delete`.
    pub fn delete(request: DeleteRequestProto) -> Self {
        Self::new(
            RequestEnvelope::Delete(request),
            Arc::new(|resp| match resp {
                ResponseEnvelope::Delete(v) => Ok(v),
                other => Err(unexpected_response("Delete", other.op_name())),
            }),
        )
    }
}

impl RpcOp<OpenWriteByPathResponseProto> {
    /// Build an operation for `OpenWriteByPath`.
    pub fn open_write_by_path(request: OpenWriteByPathRequestProto) -> Self {
        Self::new(
            RequestEnvelope::OpenWriteByPath(request),
            Arc::new(|resp| match resp {
                ResponseEnvelope::OpenWriteByPath(v) => Ok(v),
                other => Err(unexpected_response("OpenWriteByPath", other.op_name())),
            }),
        )
    }
}

impl RpcOp<FsyncSessionResponseProto> {
    /// Build an operation for `FsyncSession`.
    pub fn fsync_session(request: FsyncSessionRequestProto) -> Self {
        Self::new(
            RequestEnvelope::FsyncSession(request),
            Arc::new(|resp| match resp {
                ResponseEnvelope::FsyncSession(v) => Ok(v),
                other => Err(unexpected_response("FsyncSession", other.op_name())),
            }),
        )
    }
}

impl RpcOp<HsyncSessionResponseProto> {
    /// Build an operation for `HsyncSession`.
    pub fn hsync_session(request: HsyncSessionRequestProto) -> Self {
        Self::new(
            RequestEnvelope::HsyncSession(request),
            Arc::new(|resp| match resp {
                ResponseEnvelope::HsyncSession(v) => Ok(v),
                other => Err(unexpected_response("HsyncSession", other.op_name())),
            }),
        )
    }
}

impl RpcOp<HflushSessionResponseProto> {
    /// Build an operation for `HflushSession`.
    pub fn hflush_session(request: HflushSessionRequestProto) -> Self {
        Self::new(
            RequestEnvelope::HflushSession(request),
            Arc::new(|resp| match resp {
                ResponseEnvelope::HflushSession(v) => Ok(v),
                other => Err(unexpected_response("HflushSession", other.op_name())),
            }),
        )
    }
}

impl RpcOp<CloseWriteSessionResponseProto> {
    /// Build an operation for `CloseWriteSession`.
    pub fn close_write_session(request: CloseWriteSessionRequestProto) -> Self {
        Self::new(
            RequestEnvelope::CloseWriteSession(request),
            Arc::new(|resp| match resp {
                ResponseEnvelope::CloseWriteSession(v) => Ok(v),
                other => Err(unexpected_response("CloseWriteSession", other.op_name())),
            }),
        )
    }
}

/// Action machine driving refresh/replay loops for FileSystemService RPCs.
pub struct ActionMachine {
    rpc: Arc<dyn FileSystemRpc>,
    policy: ActionMachinePolicy,
    caches: ActionCaches,
    metadata_endpoints: Vec<String>,
    endpoint_cursor: AtomicUsize,
}

impl ActionMachine {
    /// Create a new action machine with default policy.
    pub fn new(rpc: Arc<dyn FileSystemRpc>, metadata_endpoints: Vec<String>) -> Self {
        let mut endpoints: Vec<String> = metadata_endpoints
            .into_iter()
            .map(|endpoint| normalize_endpoint(&endpoint))
            .collect();
        if endpoints.is_empty() {
            if let Some(endpoint) = rpc.current_endpoint() {
                endpoints.push(normalize_endpoint(&endpoint));
            }
        }
        Self {
            rpc,
            policy: ActionMachinePolicy::default(),
            caches: ActionCaches::default(),
            metadata_endpoints: endpoints,
            endpoint_cursor: AtomicUsize::new(0),
        }
    }

    /// Override retry/refresh policy.
    pub fn with_policy(mut self, policy: ActionMachinePolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Execute an RPC with canonical refresh/replay handling.
    pub async fn call_with_refresh<T>(&self, replay_policy: ReplayPolicy, op: RpcOp<T>) -> ClientResult<T> {
        let op_method = op.method;
        let expected_policy = replay_policy_for_method(op_method);
        if replay_policy != expected_policy {
            return Err(ClientError::Metadata(format!(
                "replay policy mismatch for {:?}: expected {:?}, got {:?}",
                op_method, expected_policy, replay_policy
            )));
        }
        let mut request = op.request.clone();
        let parent_id = self.parent_correlation_id(&request);
        let mut refresh_attempts = 0u32;
        let mut retryable_attempts = 0u32;
        let mut transport_attempts = 0u32;

        loop {
            self.prepare_attempt_header(
                &mut request,
                &parent_id,
                (refresh_attempts + retryable_attempts + transport_attempts) as i32,
            );

            let response = match (op.execute)(Arc::clone(&self.rpc), request.clone()).await {
                Ok(resp) => resp,
                Err(status) => {
                    if is_transient_transport_status(&status) && transport_attempts < self.policy.max_transport_retries
                    {
                        transport_attempts += 1;
                        tokio::time::sleep(Duration::from_millis(transport_backoff_ms(
                            self.policy.base_backoff_ms,
                            transport_attempts,
                        )))
                        .await;
                        continue;
                    }
                    return Err(ClientError::from(status));
                }
            };

            let header = response.header();
            self.update_header_hints(&request, header);

            match parse_rpc_envelope(Ok(()), header) {
                RpcEnvelope::Ok => {
                    self.update_success_caches(&request, &response);
                    return (op.decode)(response);
                }
                RpcEnvelope::CanonicalError(canonical) => {
                    if is_authz_denial(&canonical) {
                        return Err(ClientError::from(ClientAction::Fail { canonical }));
                    }

                    match canonical.class {
                        ErrorClass::Fatal => {
                            let reason = canonical
                                .reason
                                .unwrap_or_else(|| refresh_reason_from_code(canonical.code.clone()));
                            if self.should_reopen_session(&replay_policy, reason, &canonical, &request) {
                                self.reopen_session_and_rewrite(&mut request).await?;
                                continue;
                            }
                            return Err(ClientError::from(ClientAction::Fail { canonical }));
                        }
                        ErrorClass::Retryable => {
                            if retryable_attempts >= self.policy.max_retryable_attempts {
                                return Err(ClientError::from(ClientAction::Retry {
                                    after_ms: canonical.retry_after_ms,
                                    canonical,
                                }));
                            }
                            retryable_attempts += 1;
                            let delay = canonical.retry_after_ms.unwrap_or_else(|| {
                                transport_backoff_ms(self.policy.base_backoff_ms, retryable_attempts)
                            });
                            tokio::time::sleep(Duration::from_millis(delay)).await;
                        }
                        ErrorClass::NeedRefresh => {
                            let reason = canonical
                                .reason
                                .unwrap_or_else(|| refresh_reason_from_code(canonical.code.clone()));
                            if refresh_attempts >= self.policy.max_refresh_attempts {
                                return Err(ClientError::from(ClientAction::Refresh {
                                    reason,
                                    hint: self.build_refresh_hint(&request, header, &canonical),
                                    canonical,
                                }));
                            }
                            refresh_attempts += 1;
                            self.handle_need_refresh(&replay_policy, reason, &canonical, &mut request)
                                .await?;
                        }
                        ErrorClass::Ok => {
                            self.update_success_caches(&request, &response);
                            return (op.decode)(response);
                        }
                    }
                }
                RpcEnvelope::TransportError(status) => {
                    if is_transient_transport_status(&status) && transport_attempts < self.policy.max_transport_retries
                    {
                        transport_attempts += 1;
                        tokio::time::sleep(Duration::from_millis(transport_backoff_ms(
                            self.policy.base_backoff_ms,
                            transport_attempts,
                        )))
                        .await;
                        continue;
                    }
                    return Err(ClientError::from(status));
                }
            }
        }
    }

    fn parent_correlation_id(&self, request: &RequestEnvelope) -> String {
        if let Some(header) = request.header() {
            if !header.traceparent.is_empty() {
                return header.traceparent.clone();
            }
            if let Some(client) = header.client.as_ref() {
                if !client.call_id.is_empty() {
                    return client.call_id.clone();
                }
            }
        }
        CallId::new().to_string()
    }

    fn prepare_attempt_header(&self, request: &mut RequestEnvelope, parent_id: &str, retry_count: i32) {
        let path_hint = request.path_hint(&self.caches.sessions);
        let header = request.ensure_header_mut();

        if header.client.is_none() {
            header.client = Some(default_client_info_proto());
        }
        let client = header.client.as_mut().expect("client initialized");
        client.call_id = CallId::new().to_string();

        if header.traceparent.is_empty() {
            header.traceparent = parent_id.to_string();
        }
        header.retry_count = retry_count;

        if let Some(path) = path_hint.as_ref() {
            if let Some(epoch) = self.caches.mount_epoch_for_path(path) {
                header.mount_epoch = Some(epoch);
            }
            if let Some(route_epoch) = self.caches.route_epoch_for_path(path) {
                header.route_epoch = Some(route_epoch);
            }
        }
    }

    fn build_refresh_hint(
        &self,
        request: &RequestEnvelope,
        header: Option<&ResponseHeaderProto>,
        canonical: &CanonicalError,
    ) -> RefreshHint {
        let path_hint = request.path_hint(&self.caches.sessions);
        let canonical_hint = canonical.refresh_hint.as_ref();
        let route_epoch = canonical_hint
            .and_then(|hint| hint.route_epoch)
            .or_else(|| header.and_then(|h| h.route_epoch))
            .or_else(|| {
                path_hint
                    .as_ref()
                    .and_then(|path| self.caches.route_epoch_for_path(path))
            });
        let worker_epoch = canonical_hint.and_then(|hint| hint.worker_epoch).or_else(|| {
            path_hint
                .as_ref()
                .and_then(|path| self.caches.worker_epoch_for_path(path))
        });
        let group_id = canonical_hint
            .and_then(|hint| hint.group_id)
            .or_else(|| header.and_then(|h| if h.group_id == 0 { None } else { Some(h.group_id) }));
        let mount_epoch = canonical_hint
            .and_then(|hint| hint.mount_epoch)
            .or_else(|| header.and_then(|h| h.mount_epoch));
        let endpoint_hint = canonical_hint
            .and_then(|hint| hint.worker_endpoints.first())
            .map(|endpoint| crate::canonical::EndpointHint {
                worker_id: endpoint.worker_id,
                endpoint: endpoint.endpoint.clone(),
                net_transport_kind: endpoint.net_transport_kind,
                worker_epoch: endpoint.worker_epoch,
            });

        RefreshHint {
            group_id,
            route_epoch,
            mount_epoch,
            worker_epoch,
            endpoint_hint,
        }
    }

    fn update_header_hints(&self, request: &RequestEnvelope, header: Option<&ResponseHeaderProto>) {
        let Some(header) = header else {
            return;
        };
        let Some(path) = request.path_hint(&self.caches.sessions) else {
            return;
        };
        if let Some(mount_epoch) = header.mount_epoch {
            self.caches.record_mount_epoch(&path, mount_epoch);
        }
        if let Some(route_epoch) = header.route_epoch {
            self.caches.record_route_epoch(&path, route_epoch);
        }
    }

    fn update_success_caches(&self, request: &RequestEnvelope, response: &ResponseEnvelope) {
        if let Some(endpoint) = self.rpc.current_endpoint() {
            self.caches.set_leader_endpoint(normalize_endpoint(&endpoint));
        }

        self.update_payload_hints(request, response);
    }

    fn update_payload_hints(&self, request: &RequestEnvelope, response: &ResponseEnvelope) {
        match (request, response) {
            (RequestEnvelope::GetFileLayoutByPath(req), ResponseEnvelope::GetFileLayoutByPath(resp)) => {
                let path = req.path.clone();
                if let Some(route_epoch) = resp.header.as_ref().and_then(|header| header.route_epoch) {
                    self.caches.record_route_epoch(&path, route_epoch);
                }
                let (workers, worker_epoch) = workers_from_layout(resp);
                self.caches.record_worker_info(&path, workers, worker_epoch);
            }
            (RequestEnvelope::OpenWriteByPath(req), ResponseEnvelope::OpenWriteByPath(resp)) => {
                let path = req.path.clone();
                if let Some(route_epoch) = resp.header.as_ref().and_then(|header| header.route_epoch) {
                    self.caches.record_route_epoch(&path, route_epoch);
                }
                let (workers, worker_epoch) = workers_from_open(resp);
                self.caches.record_worker_info(&path, workers, worker_epoch);
                self.caches
                    .upsert_session(WriteSessionState::from_open_request(req, resp));
            }
            (RequestEnvelope::CloseWriteSession(req), ResponseEnvelope::CloseWriteSession(_)) => {
                self.caches.remove_session(req.file_handle);
            }
            _ => {}
        }
    }

    async fn handle_need_refresh(
        &self,
        replay_policy: &ReplayPolicy,
        reason: RefreshReason,
        canonical: &CanonicalError,
        request: &mut RequestEnvelope,
    ) -> ClientResult<()> {
        if self.should_reopen_session(replay_policy, reason, canonical, request) {
            self.reopen_session_and_rewrite(request).await?;
            return Ok(());
        }

        match reason {
            RefreshReason::NotLeader => self.refresh_leader(canonical, request).await,
            RefreshReason::MountEpochMismatch => self.refresh_mount(canonical, request).await,
            RefreshReason::RouteEpochMismatch => self.refresh_route(canonical, request).await,
            RefreshReason::WorkerEpochMismatch => self.refresh_worker(canonical, request).await,
            _ => self.refresh_route(canonical, request).await,
        }
    }

    fn should_reopen_session(
        &self,
        replay_policy: &ReplayPolicy,
        reason: RefreshReason,
        canonical: &CanonicalError,
        request: &RequestEnvelope,
    ) -> bool {
        if !matches!(replay_policy, ReplayPolicy::SessionBarrier) {
            return false;
        }
        if request.session_handle().is_none() {
            return false;
        }
        is_session_invalid_error(reason, canonical)
    }

    async fn reopen_session_and_rewrite(&self, request: &mut RequestEnvelope) -> ClientResult<()> {
        let old_handle = request
            .session_handle()
            .ok_or_else(|| ClientError::Metadata("missing session handle for reopen".to_string()))?;
        let session = self
            .caches
            .session(old_handle)
            .ok_or_else(|| ClientError::Metadata(format!("session {} not found in cache", old_handle)))?;

        let reopen_req = session.to_open_request(request.header().cloned());
        let reopen_op = RpcOp::open_write_by_path(reopen_req.clone());
        let reopen_policy = replay_policy_for_method(reopen_op.method());
        let reopen_resp = Box::pin(self.call_with_refresh(reopen_policy, reopen_op)).await?;

        let reopened_session = self
            .caches
            .session(reopen_resp.file_handle)
            .unwrap_or_else(|| WriteSessionState::from_open_request(&reopen_req, &reopen_resp));

        self.caches.remove_session(old_handle);
        request.apply_session(&reopened_session);
        Ok(())
    }

    async fn refresh_leader(&self, canonical: &CanonicalError, request: &RequestEnvelope) -> ClientResult<()> {
        let candidate = canonical
            .refresh_hint
            .as_ref()
            .and_then(|hint| hint.leader_endpoint.clone())
            .or_else(|| self.caches.leader_endpoint())
            .or_else(|| self.next_metadata_endpoint());

        if let Some(endpoint) = candidate {
            let endpoint = normalize_endpoint(&endpoint);
            self.rpc.reconnect(&endpoint).await?;
            self.caches.set_leader_endpoint(endpoint.clone());
        }

        if let Some(path) = request.path_hint(&self.caches.sessions) {
            self.refresh_route_for_path(&path, request.header().cloned()).await?;
        }

        Ok(())
    }

    async fn refresh_mount(&self, canonical: &CanonicalError, request: &RequestEnvelope) -> ClientResult<()> {
        if let Some(path) = request.path_hint(&self.caches.sessions) {
            if let Some(mount_epoch) = canonical.refresh_hint.as_ref().and_then(|hint| hint.mount_epoch) {
                self.caches.record_mount_epoch(&path, mount_epoch);
            }
            self.refresh_status_for_path(&path, request.header().cloned()).await
        } else {
            self.refresh_route(canonical, request).await
        }
    }

    async fn refresh_route(&self, canonical: &CanonicalError, request: &RequestEnvelope) -> ClientResult<()> {
        if let Some(path) = request.path_hint(&self.caches.sessions) {
            if let Some(route_epoch) = canonical.refresh_hint.as_ref().and_then(|hint| hint.route_epoch) {
                self.caches.record_route_epoch(&path, route_epoch);
            }
            self.refresh_route_for_path(&path, request.header().cloned()).await
        } else {
            Ok(())
        }
    }

    async fn refresh_worker(&self, canonical: &CanonicalError, request: &RequestEnvelope) -> ClientResult<()> {
        if let Some(path) = request.path_hint(&self.caches.sessions) {
            if let Some(hint) = canonical.refresh_hint.as_ref() {
                let worker_endpoints = hint
                    .worker_endpoints
                    .iter()
                    .map(|endpoint| WorkerEndpointInfoProto {
                        worker_id: endpoint.worker_id,
                        endpoint: endpoint.endpoint.clone(),
                        net_transport_kind: endpoint.net_transport_kind,
                        worker_epoch: endpoint.worker_epoch,
                    })
                    .collect::<Vec<_>>();
                self.caches
                    .record_worker_info(&path, worker_endpoints, hint.worker_epoch);
                if !hint.worker_resolve_required && !hint.worker_endpoints.is_empty() {
                    return Ok(());
                }
            }
            self.refresh_route_for_path(&path, request.header().cloned()).await
        } else {
            Ok(())
        }
    }

    async fn refresh_status_for_path(
        &self,
        path: &str,
        header_template: Option<RequestHeaderProto>,
    ) -> ClientResult<()> {
        let request = RequestEnvelope::GetFileStatus(GetFileStatusRequestProto {
            header: header_template,
            path: path.to_string(),
        });
        self.run_best_effort_refresh(request).await
    }

    async fn refresh_route_for_path(
        &self,
        path: &str,
        header_template: Option<RequestHeaderProto>,
    ) -> ClientResult<()> {
        let request = RequestEnvelope::GetFileLayoutByPath(GetFileLayoutByPathRequestProto {
            header: header_template,
            path: path.to_string(),
            range: None,
        });
        self.run_best_effort_refresh(request).await
    }

    async fn run_best_effort_refresh(&self, mut request: RequestEnvelope) -> ClientResult<()> {
        let parent_id = self.parent_correlation_id(&request);
        self.prepare_attempt_header(&mut request, &parent_id, 0);

        let response = match self.execute_request(request.clone()).await {
            Ok(resp) => resp,
            Err(status) => {
                if is_transient_transport_status(&status) {
                    tokio::time::sleep(Duration::from_millis(self.policy.base_backoff_ms)).await;
                    let mut retry_request = request.clone();
                    self.prepare_attempt_header(&mut retry_request, &parent_id, 1);
                    self.execute_request(retry_request.clone())
                        .await
                        .map_err(ClientError::from)?
                } else {
                    return Err(ClientError::from(status));
                }
            }
        };

        self.update_header_hints(&request, response.header());
        self.update_payload_hints(&request, &response);

        match parse_rpc_envelope(Ok(()), response.header()) {
            RpcEnvelope::Ok => Ok(()),
            RpcEnvelope::CanonicalError(canonical) => {
                if is_authz_denial(&canonical) {
                    Err(ClientError::from(ClientAction::Fail { canonical }))
                } else {
                    Ok(())
                }
            }
            RpcEnvelope::TransportError(status) => Err(ClientError::from(status)),
        }
    }

    async fn execute_request(&self, request: RequestEnvelope) -> Result<ResponseEnvelope, tonic::Status> {
        execute_request_on_rpc(Arc::clone(&self.rpc), request).await
    }

    fn next_metadata_endpoint(&self) -> Option<String> {
        if self.metadata_endpoints.is_empty() {
            return None;
        }
        let idx = self.endpoint_cursor.fetch_add(1, Ordering::Relaxed);
        self.metadata_endpoints
            .get(idx % self.metadata_endpoints.len())
            .cloned()
    }
}

type RpcFuture = Pin<Box<dyn Future<Output = Result<ResponseEnvelope, tonic::Status>> + Send>>;
type RpcExecutor = Arc<dyn Fn(Arc<dyn FileSystemRpc>, RequestEnvelope) -> RpcFuture + Send + Sync>;
type DecodeFn<T> = Arc<dyn Fn(ResponseEnvelope) -> ClientResult<T> + Send + Sync>;

#[derive(Clone, Debug)]
enum RequestEnvelope {
    GetFileStatus(GetFileStatusRequestProto),
    ListStatus(ListStatusPathRequestProto),
    GetFileLayoutByPath(GetFileLayoutByPathRequestProto),
    Delete(DeleteRequestProto),
    OpenWriteByPath(OpenWriteByPathRequestProto),
    FsyncSession(FsyncSessionRequestProto),
    HsyncSession(HsyncSessionRequestProto),
    HflushSession(HflushSessionRequestProto),
    CloseWriteSession(CloseWriteSessionRequestProto),
}

impl RequestEnvelope {
    fn method(&self) -> FileSystemRpcMethod {
        match self {
            RequestEnvelope::GetFileStatus(_) => FileSystemRpcMethod::GetFileStatus,
            RequestEnvelope::ListStatus(_) => FileSystemRpcMethod::ListStatus,
            RequestEnvelope::GetFileLayoutByPath(_) => FileSystemRpcMethod::GetFileLayoutByPath,
            RequestEnvelope::Delete(_) => FileSystemRpcMethod::Delete,
            RequestEnvelope::OpenWriteByPath(_) => FileSystemRpcMethod::OpenWriteByPath,
            RequestEnvelope::FsyncSession(_) => FileSystemRpcMethod::FsyncSession,
            RequestEnvelope::HsyncSession(_) => FileSystemRpcMethod::HsyncSession,
            RequestEnvelope::HflushSession(_) => FileSystemRpcMethod::HflushSession,
            RequestEnvelope::CloseWriteSession(_) => FileSystemRpcMethod::CloseWriteSession,
        }
    }

    #[cfg(test)]
    fn op_name(&self) -> &'static str {
        match self {
            RequestEnvelope::GetFileStatus(_) => "GetFileStatus",
            RequestEnvelope::ListStatus(_) => "ListStatus",
            RequestEnvelope::GetFileLayoutByPath(_) => "GetFileLayoutByPath",
            RequestEnvelope::Delete(_) => "Delete",
            RequestEnvelope::OpenWriteByPath(_) => "OpenWriteByPath",
            RequestEnvelope::FsyncSession(_) => "FsyncSession",
            RequestEnvelope::HsyncSession(_) => "HsyncSession",
            RequestEnvelope::HflushSession(_) => "HflushSession",
            RequestEnvelope::CloseWriteSession(_) => "CloseWriteSession",
        }
    }

    fn ensure_header_mut(&mut self) -> &mut RequestHeaderProto {
        match self {
            RequestEnvelope::GetFileStatus(req) => req.header.get_or_insert_with(default_request_header_proto),
            RequestEnvelope::ListStatus(req) => req.header.get_or_insert_with(default_request_header_proto),
            RequestEnvelope::GetFileLayoutByPath(req) => req.header.get_or_insert_with(default_request_header_proto),
            RequestEnvelope::Delete(req) => req.header.get_or_insert_with(default_request_header_proto),
            RequestEnvelope::OpenWriteByPath(req) => req.header.get_or_insert_with(default_request_header_proto),
            RequestEnvelope::FsyncSession(req) => req.header.get_or_insert_with(default_request_header_proto),
            RequestEnvelope::HsyncSession(req) => req
                .fsync
                .get_or_insert_with(FsyncSessionRequestProto::default)
                .header
                .get_or_insert_with(default_request_header_proto),
            RequestEnvelope::HflushSession(req) => req
                .fsync
                .get_or_insert_with(FsyncSessionRequestProto::default)
                .header
                .get_or_insert_with(default_request_header_proto),
            RequestEnvelope::CloseWriteSession(req) => req.header.get_or_insert_with(default_request_header_proto),
        }
    }

    fn header(&self) -> Option<&RequestHeaderProto> {
        match self {
            RequestEnvelope::GetFileStatus(req) => req.header.as_ref(),
            RequestEnvelope::ListStatus(req) => req.header.as_ref(),
            RequestEnvelope::GetFileLayoutByPath(req) => req.header.as_ref(),
            RequestEnvelope::Delete(req) => req.header.as_ref(),
            RequestEnvelope::OpenWriteByPath(req) => req.header.as_ref(),
            RequestEnvelope::FsyncSession(req) => req.header.as_ref(),
            RequestEnvelope::HsyncSession(req) => req.fsync.as_ref().and_then(|inner| inner.header.as_ref()),
            RequestEnvelope::HflushSession(req) => req.fsync.as_ref().and_then(|inner| inner.header.as_ref()),
            RequestEnvelope::CloseWriteSession(req) => req.header.as_ref(),
        }
    }

    fn path_hint(&self, sessions: &DashMap<u64, WriteSessionState>) -> Option<String> {
        match self {
            RequestEnvelope::GetFileStatus(req) => Some(req.path.clone()),
            RequestEnvelope::ListStatus(req) => Some(req.path.clone()),
            RequestEnvelope::GetFileLayoutByPath(req) => Some(req.path.clone()),
            RequestEnvelope::Delete(req) => Some(req.path.clone()),
            RequestEnvelope::OpenWriteByPath(req) => Some(req.path.clone()),
            RequestEnvelope::FsyncSession(req) => sessions.get(&req.file_handle).map(|s| s.path.clone()),
            RequestEnvelope::HsyncSession(req) => req
                .fsync
                .as_ref()
                .and_then(|inner| sessions.get(&inner.file_handle).map(|s| s.path.clone())),
            RequestEnvelope::HflushSession(req) => req
                .fsync
                .as_ref()
                .and_then(|inner| sessions.get(&inner.file_handle).map(|s| s.path.clone())),
            RequestEnvelope::CloseWriteSession(req) => sessions.get(&req.file_handle).map(|s| s.path.clone()),
        }
    }

    fn session_handle(&self) -> Option<u64> {
        match self {
            RequestEnvelope::FsyncSession(req) => Some(req.file_handle),
            RequestEnvelope::HsyncSession(req) => req.fsync.as_ref().map(|inner| inner.file_handle),
            RequestEnvelope::HflushSession(req) => req.fsync.as_ref().map(|inner| inner.file_handle),
            RequestEnvelope::CloseWriteSession(req) => Some(req.file_handle),
            _ => None,
        }
    }

    fn apply_session(&mut self, session: &WriteSessionState) {
        match self {
            RequestEnvelope::FsyncSession(req) => {
                req.file_handle = session.file_handle;
                req.lease_id = session.lease_id;
                req.lease_epoch = Some(session.lease_epoch);
                req.fencing_token = session.fencing_token;
            }
            RequestEnvelope::HsyncSession(req) => {
                let inner = req.fsync.get_or_insert_with(FsyncSessionRequestProto::default);
                inner.file_handle = session.file_handle;
                inner.lease_id = session.lease_id;
                inner.lease_epoch = Some(session.lease_epoch);
                inner.fencing_token = session.fencing_token;
            }
            RequestEnvelope::HflushSession(req) => {
                let inner = req.fsync.get_or_insert_with(FsyncSessionRequestProto::default);
                inner.file_handle = session.file_handle;
                inner.lease_id = session.lease_id;
                inner.lease_epoch = Some(session.lease_epoch);
                inner.fencing_token = session.fencing_token;
            }
            RequestEnvelope::CloseWriteSession(req) => {
                req.file_handle = session.file_handle;
                req.lease_id = session.lease_id;
                req.lease_epoch = session.lease_epoch;
                req.open_epoch = session.open_epoch;
                req.fencing_token = session.fencing_token;
            }
            _ => {}
        }
    }
}

#[derive(Clone, Debug)]
enum ResponseEnvelope {
    GetFileStatus(GetFileStatusResponseProto),
    ListStatus(ListStatusPathResponseProto),
    GetFileLayoutByPath(GetFileLayoutByPathResponseProto),
    Delete(DeleteResponseProto),
    OpenWriteByPath(OpenWriteByPathResponseProto),
    FsyncSession(FsyncSessionResponseProto),
    HsyncSession(HsyncSessionResponseProto),
    HflushSession(HflushSessionResponseProto),
    CloseWriteSession(CloseWriteSessionResponseProto),
}

impl ResponseEnvelope {
    fn header(&self) -> Option<&ResponseHeaderProto> {
        match self {
            ResponseEnvelope::GetFileStatus(resp) => resp.header.as_ref(),
            ResponseEnvelope::ListStatus(resp) => resp.header.as_ref(),
            ResponseEnvelope::GetFileLayoutByPath(resp) => resp.header.as_ref(),
            ResponseEnvelope::Delete(resp) => resp.header.as_ref(),
            ResponseEnvelope::OpenWriteByPath(resp) => resp.header.as_ref(),
            ResponseEnvelope::FsyncSession(resp) => resp.header.as_ref(),
            ResponseEnvelope::HsyncSession(resp) => resp.header.as_ref(),
            ResponseEnvelope::HflushSession(resp) => resp.header.as_ref(),
            ResponseEnvelope::CloseWriteSession(resp) => resp.header.as_ref(),
        }
    }

    fn op_name(&self) -> &'static str {
        match self {
            ResponseEnvelope::GetFileStatus(_) => "GetFileStatus",
            ResponseEnvelope::ListStatus(_) => "ListStatus",
            ResponseEnvelope::GetFileLayoutByPath(_) => "GetFileLayoutByPath",
            ResponseEnvelope::Delete(_) => "Delete",
            ResponseEnvelope::OpenWriteByPath(_) => "OpenWriteByPath",
            ResponseEnvelope::FsyncSession(_) => "FsyncSession",
            ResponseEnvelope::HsyncSession(_) => "HsyncSession",
            ResponseEnvelope::HflushSession(_) => "HflushSession",
            ResponseEnvelope::CloseWriteSession(_) => "CloseWriteSession",
        }
    }
}

async fn execute_request_on_rpc(
    rpc: Arc<dyn FileSystemRpc>,
    request: RequestEnvelope,
) -> Result<ResponseEnvelope, tonic::Status> {
    match request {
        RequestEnvelope::GetFileStatus(req) => rpc.get_file_status(req).await.map(ResponseEnvelope::GetFileStatus),
        RequestEnvelope::ListStatus(req) => rpc.list_status(req).await.map(ResponseEnvelope::ListStatus),
        RequestEnvelope::GetFileLayoutByPath(req) => rpc
            .get_file_layout_by_path(req)
            .await
            .map(ResponseEnvelope::GetFileLayoutByPath),
        RequestEnvelope::Delete(req) => rpc.delete(req).await.map(ResponseEnvelope::Delete),
        RequestEnvelope::OpenWriteByPath(req) => {
            rpc.open_write_by_path(req).await.map(ResponseEnvelope::OpenWriteByPath)
        }
        RequestEnvelope::FsyncSession(req) => rpc.fsync_session(req).await.map(ResponseEnvelope::FsyncSession),
        RequestEnvelope::HsyncSession(req) => rpc.hsync_session(req).await.map(ResponseEnvelope::HsyncSession),
        RequestEnvelope::HflushSession(req) => rpc.hflush_session(req).await.map(ResponseEnvelope::HflushSession),
        RequestEnvelope::CloseWriteSession(req) => rpc
            .close_write_session(req)
            .await
            .map(ResponseEnvelope::CloseWriteSession),
    }
}

#[derive(Clone, Debug)]
struct WriteSessionState {
    path: String,
    desired_len: Option<u64>,
    mode: i32,
    file_handle: u64,
    lease_id: Option<proto::common::LeaseIdProto>,
    lease_epoch: u64,
    open_epoch: u64,
    fencing_token: Option<proto::common::FencingTokenProto>,
}

impl WriteSessionState {
    fn from_open_request(request: &OpenWriteByPathRequestProto, response: &OpenWriteByPathResponseProto) -> Self {
        Self {
            path: request.path.clone(),
            desired_len: request.desired_len,
            mode: request.mode,
            file_handle: response.file_handle,
            lease_id: response.lease_id,
            lease_epoch: response.lease_epoch,
            open_epoch: response.open_epoch,
            fencing_token: response.fencing_token,
        }
    }

    fn to_open_request(&self, header_template: Option<RequestHeaderProto>) -> OpenWriteByPathRequestProto {
        OpenWriteByPathRequestProto {
            header: header_template,
            path: self.path.clone(),
            desired_len: self.desired_len,
            mode: self.mode,
        }
    }
}

#[derive(Default)]
struct ActionCaches {
    leader_endpoint: RwLock<Option<String>>,
    mount_epoch_prefix: DashMap<String, u64>,
    route_epoch_by_path: DashMap<String, u64>,
    worker_epoch_by_path: DashMap<String, u64>,
    worker_endpoints_by_path: DashMap<String, Vec<WorkerEndpointInfoProto>>,
    sessions: DashMap<u64, WriteSessionState>,
}

impl ActionCaches {
    fn set_leader_endpoint(&self, endpoint: String) {
        *self.leader_endpoint.write() = Some(endpoint);
    }

    fn leader_endpoint(&self) -> Option<String> {
        self.leader_endpoint.read().clone()
    }

    fn record_mount_epoch(&self, path: &str, mount_epoch: u64) {
        let key = normalized_path(path);
        self.mount_epoch_prefix.insert(key, mount_epoch);
    }

    fn mount_epoch_for_path(&self, path: &str) -> Option<u64> {
        let candidate = normalized_path(path);
        let mut best: Option<(usize, u64)> = None;
        for entry in self.mount_epoch_prefix.iter() {
            let prefix = entry.key();
            if is_path_prefix(prefix, &candidate) {
                let len = prefix.len();
                let value = *entry.value();
                if best.map(|(best_len, _)| len > best_len).unwrap_or(true) {
                    best = Some((len, value));
                }
            }
        }
        best.map(|(_, value)| value)
    }

    fn record_route_epoch(&self, path: &str, route_epoch: u64) {
        self.route_epoch_by_path.insert(normalized_path(path), route_epoch);
    }

    fn route_epoch_for_path(&self, path: &str) -> Option<u64> {
        self.route_epoch_by_path.get(&normalized_path(path)).map(|value| *value)
    }

    fn record_worker_info(
        &self,
        path: &str,
        worker_endpoints: Vec<WorkerEndpointInfoProto>,
        worker_epoch: Option<u64>,
    ) {
        let key = normalized_path(path);
        if !worker_endpoints.is_empty() {
            self.worker_endpoints_by_path.insert(key.clone(), worker_endpoints);
        }
        if let Some(worker_epoch) = worker_epoch {
            self.worker_epoch_by_path.insert(key, worker_epoch);
        }
    }

    fn worker_epoch_for_path(&self, path: &str) -> Option<u64> {
        self.worker_epoch_by_path
            .get(&normalized_path(path))
            .map(|value| *value)
    }

    fn upsert_session(&self, session: WriteSessionState) {
        self.sessions.insert(session.file_handle, session);
    }

    fn session(&self, file_handle: u64) -> Option<WriteSessionState> {
        self.sessions.get(&file_handle).map(|state| state.clone())
    }

    fn remove_session(&self, file_handle: u64) {
        self.sessions.remove(&file_handle);
    }
}

fn normalized_path(path: &str) -> String {
    if path.is_empty() {
        "/".to_string()
    } else {
        path.to_string()
    }
}

fn is_path_prefix(prefix: &str, path: &str) -> bool {
    if prefix == "/" {
        return true;
    }
    path == prefix || path.starts_with(&(prefix.to_string() + "/"))
}

fn workers_from_layout(response: &GetFileLayoutByPathResponseProto) -> (Vec<WorkerEndpointInfoProto>, Option<u64>) {
    let mut by_worker: HashMap<u64, WorkerEndpointInfoProto> = HashMap::new();
    let mut max_epoch: Option<u64> = None;

    for location in &response.locations {
        if let Some(epoch) = location.worker_epoch {
            max_epoch = Some(max_epoch.map(|cur| cur.max(epoch)).unwrap_or(epoch));
        }
        for worker in &location.workers {
            by_worker.entry(worker.worker_id).or_insert_with(|| worker.clone());
            max_epoch = Some(
                max_epoch
                    .map(|cur| cur.max(worker.worker_epoch))
                    .unwrap_or(worker.worker_epoch),
            );
        }
    }

    (by_worker.into_values().collect(), max_epoch)
}

fn workers_from_open(response: &OpenWriteByPathResponseProto) -> (Vec<WorkerEndpointInfoProto>, Option<u64>) {
    let mut by_worker: HashMap<u64, WorkerEndpointInfoProto> = HashMap::new();
    let mut max_epoch: Option<u64> = None;

    for target in &response.write_targets {
        for worker in &target.worker_endpoints {
            by_worker.entry(worker.worker_id).or_insert_with(|| worker.clone());
            max_epoch = Some(
                max_epoch
                    .map(|cur| cur.max(worker.worker_epoch))
                    .unwrap_or(worker.worker_epoch),
            );
        }
    }

    (by_worker.into_values().collect(), max_epoch)
}

fn default_client_info_proto() -> ClientInfoProto {
    ClientInfoProto {
        call_id: CallId::new().to_string(),
        client_id: 0,
        client_name: String::new(),
    }
}

fn default_request_header_proto() -> RequestHeaderProto {
    RequestHeaderProto {
        client: Some(default_client_info_proto()),
        group_id: 0,
        mount_epoch: None,
        deadline_ms: 0,
        traceparent: String::new(),
        caller_context: None,
        state_id: None,
        retry_count: 0,
        route_epoch: None,
        principal: String::new(),
        real_user: String::new(),
        doas: String::new(),
        authn_type: proto::common::AuthnTypeProto::Unspecified as i32,
    }
}

fn unexpected_response(expected: &str, actual: &str) -> ClientError {
    ClientError::Metadata(format!(
        "unexpected response envelope: expected {}, got {}",
        expected, actual
    ))
}

fn normalize_endpoint(endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{}", endpoint)
    }
}

fn refresh_reason_from_code(code: Option<CanonicalErrorCode>) -> RefreshReason {
    match code {
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::NotLeader)) => RefreshReason::NotLeader,
        // MOVED is de-scoped for FileSystemService; shard-moved codes use route refresh behavior.
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::ShardMoved)) => RefreshReason::RouteEpochMismatch,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::MountEpochMismatch)) => RefreshReason::MountEpochMismatch,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::RouteEpochMismatch)) => RefreshReason::RouteEpochMismatch,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::WorkerEpochMismatch)) => RefreshReason::WorkerEpochMismatch,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Fencing)) => RefreshReason::Fencing,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::EpochMismatch)) => RefreshReason::EpochMismatch,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::StaleState)) => RefreshReason::StaleState,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::BlockStampMismatch)) => RefreshReason::BlockStampMismatch,
        _ => RefreshReason::Unknown,
    }
}

fn is_authz_denial(canonical: &CanonicalError) -> bool {
    matches!(
        canonical.code,
        Some(CanonicalErrorCode::FsErrno(FsErrorCode::EAcces | FsErrorCode::EPerm))
            | Some(CanonicalErrorCode::RpcCode(RpcErrorCode::PermissionDenied))
    )
}

fn is_session_invalid_error(reason: RefreshReason, canonical: &CanonicalError) -> bool {
    if matches!(reason, RefreshReason::SessionInvalid | RefreshReason::SessionExpired) {
        return true;
    }
    matches!(
        canonical.reason,
        Some(RefreshReason::SessionInvalid | RefreshReason::SessionExpired)
    )
}

fn is_transient_transport_status(status: &tonic::Status) -> bool {
    matches!(status.code(), tonic::Code::Unavailable | tonic::Code::DeadlineExceeded)
}

fn transport_backoff_ms(base_ms: u64, attempt: u32) -> u64 {
    let shift = attempt.saturating_sub(1).min(5);
    base_ms.saturating_mul(1u64 << shift)
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::error::canonical::ErrorCode as CanonicalCode;
    use common::error::canonical::RefreshHint as CanonicalRefreshHint;
    use proto::convert::canonical_to_error_detail;
    use std::collections::VecDeque;
    use tokio::sync::Mutex;

    enum ScriptedResult {
        Response(ResponseEnvelope),
        Status(tonic::Status),
    }

    struct ScriptedRpc {
        scripted: Mutex<VecDeque<ScriptedResult>>,
        calls: Mutex<Vec<RequestEnvelope>>,
        reconnects: Mutex<Vec<String>>,
        endpoint: RwLock<Option<String>>,
    }

    impl ScriptedRpc {
        fn new(scripted: Vec<ScriptedResult>) -> Self {
            Self {
                scripted: Mutex::new(VecDeque::from(scripted)),
                calls: Mutex::new(Vec::new()),
                reconnects: Mutex::new(Vec::new()),
                endpoint: RwLock::new(Some("http://127.0.0.1:18080".to_string())),
            }
        }

        async fn next(&self, request: RequestEnvelope) -> Result<ResponseEnvelope, tonic::Status> {
            self.calls.lock().await.push(request);
            match self.scripted.lock().await.pop_front() {
                Some(ScriptedResult::Response(resp)) => Ok(resp),
                Some(ScriptedResult::Status(status)) => Err(status),
                None => Err(tonic::Status::internal("script exhausted")),
            }
        }
    }

    #[async_trait]
    impl FileSystemRpc for ScriptedRpc {
        async fn get_file_status(
            &self,
            request: GetFileStatusRequestProto,
        ) -> Result<GetFileStatusResponseProto, tonic::Status> {
            match self.next(RequestEnvelope::GetFileStatus(request)).await? {
                ResponseEnvelope::GetFileStatus(resp) => Ok(resp),
                other => Err(tonic::Status::internal(format!(
                    "expected GetFileStatus response, got {}",
                    other.op_name()
                ))),
            }
        }

        async fn list_status(
            &self,
            request: ListStatusPathRequestProto,
        ) -> Result<ListStatusPathResponseProto, tonic::Status> {
            match self.next(RequestEnvelope::ListStatus(request)).await? {
                ResponseEnvelope::ListStatus(resp) => Ok(resp),
                other => Err(tonic::Status::internal(format!(
                    "expected ListStatus response, got {}",
                    other.op_name()
                ))),
            }
        }

        async fn get_file_layout_by_path(
            &self,
            request: GetFileLayoutByPathRequestProto,
        ) -> Result<GetFileLayoutByPathResponseProto, tonic::Status> {
            match self.next(RequestEnvelope::GetFileLayoutByPath(request)).await? {
                ResponseEnvelope::GetFileLayoutByPath(resp) => Ok(resp),
                other => Err(tonic::Status::internal(format!(
                    "expected GetFileLayoutByPath response, got {}",
                    other.op_name()
                ))),
            }
        }

        async fn delete(&self, request: DeleteRequestProto) -> Result<DeleteResponseProto, tonic::Status> {
            match self.next(RequestEnvelope::Delete(request)).await? {
                ResponseEnvelope::Delete(resp) => Ok(resp),
                other => Err(tonic::Status::internal(format!(
                    "expected Delete response, got {}",
                    other.op_name()
                ))),
            }
        }

        async fn open_write_by_path(
            &self,
            request: OpenWriteByPathRequestProto,
        ) -> Result<OpenWriteByPathResponseProto, tonic::Status> {
            match self.next(RequestEnvelope::OpenWriteByPath(request)).await? {
                ResponseEnvelope::OpenWriteByPath(resp) => Ok(resp),
                other => Err(tonic::Status::internal(format!(
                    "expected OpenWriteByPath response, got {}",
                    other.op_name()
                ))),
            }
        }

        async fn fsync_session(
            &self,
            request: FsyncSessionRequestProto,
        ) -> Result<FsyncSessionResponseProto, tonic::Status> {
            match self.next(RequestEnvelope::FsyncSession(request)).await? {
                ResponseEnvelope::FsyncSession(resp) => Ok(resp),
                other => Err(tonic::Status::internal(format!(
                    "expected FsyncSession response, got {}",
                    other.op_name()
                ))),
            }
        }

        async fn hsync_session(
            &self,
            request: HsyncSessionRequestProto,
        ) -> Result<HsyncSessionResponseProto, tonic::Status> {
            match self.next(RequestEnvelope::HsyncSession(request)).await? {
                ResponseEnvelope::HsyncSession(resp) => Ok(resp),
                other => Err(tonic::Status::internal(format!(
                    "expected HsyncSession response, got {}",
                    other.op_name()
                ))),
            }
        }

        async fn hflush_session(
            &self,
            request: HflushSessionRequestProto,
        ) -> Result<HflushSessionResponseProto, tonic::Status> {
            match self.next(RequestEnvelope::HflushSession(request)).await? {
                ResponseEnvelope::HflushSession(resp) => Ok(resp),
                other => Err(tonic::Status::internal(format!(
                    "expected HflushSession response, got {}",
                    other.op_name()
                ))),
            }
        }

        async fn close_write_session(
            &self,
            request: CloseWriteSessionRequestProto,
        ) -> Result<CloseWriteSessionResponseProto, tonic::Status> {
            match self.next(RequestEnvelope::CloseWriteSession(request)).await? {
                ResponseEnvelope::CloseWriteSession(resp) => Ok(resp),
                other => Err(tonic::Status::internal(format!(
                    "expected CloseWriteSession response, got {}",
                    other.op_name()
                ))),
            }
        }

        async fn reconnect(&self, endpoint: &str) -> ClientResult<()> {
            self.reconnects.lock().await.push(endpoint.to_string());
            *self.endpoint.write() = Some(endpoint.to_string());
            Ok(())
        }

        fn current_endpoint(&self) -> Option<String> {
            self.endpoint.read().clone()
        }
    }

    fn request_header() -> RequestHeaderProto {
        RequestHeaderProto {
            client: Some(ClientInfoProto {
                call_id: CallId::new().to_string(),
                client_id: 1,
                client_name: "test-client".to_string(),
            }),
            group_id: 0,
            mount_epoch: None,
            deadline_ms: 0,
            traceparent: String::new(),
            caller_context: None,
            state_id: None,
            retry_count: 0,
            route_epoch: None,
            principal: String::new(),
            real_user: String::new(),
            doas: String::new(),
            authn_type: proto::common::AuthnTypeProto::Unspecified as i32,
        }
    }

    fn ok_header() -> ResponseHeaderProto {
        ResponseHeaderProto {
            client: Some(ClientInfoProto {
                call_id: CallId::new().to_string(),
                client_id: 1,
                client_name: "test-client".to_string(),
            }),
            error: None,
            state_id: None,
            group_id: 0,
            mount_epoch: Some(7),
            route_epoch: Some(7),
        }
    }

    fn ok_header_with_route(route_epoch: u64) -> ResponseHeaderProto {
        let mut header = ok_header();
        header.route_epoch = Some(route_epoch);
        header
    }

    fn err_header(canonical: CanonicalError) -> ResponseHeaderProto {
        ResponseHeaderProto {
            client: Some(ClientInfoProto {
                call_id: CallId::new().to_string(),
                client_id: 1,
                client_name: "test-client".to_string(),
            }),
            error: Some(canonical_to_error_detail(&canonical)),
            state_id: None,
            group_id: 0,
            mount_epoch: Some(7),
            route_epoch: Some(7),
        }
    }

    async fn call_machine<T>(machine: &ActionMachine, op: RpcOp<T>) -> ClientResult<T> {
        let policy = replay_policy_for_method(op.method());
        machine.call_with_refresh(policy, op).await
    }

    #[tokio::test]
    async fn not_leader_refresh_then_replay_succeeds() {
        let canonical = CanonicalError::need_refresh_with_hint(
            RpcErrorCode::NotLeader,
            RefreshReason::NotLeader,
            CanonicalRefreshHint {
                leader_endpoint: Some("127.0.0.2:18080".to_string()),
                ..Default::default()
            },
            "not leader",
        );

        let rpc = Arc::new(ScriptedRpc::new(vec![
            ScriptedResult::Response(ResponseEnvelope::GetFileStatus(GetFileStatusResponseProto {
                header: Some(err_header(canonical)),
                ..Default::default()
            })),
            ScriptedResult::Response(ResponseEnvelope::GetFileLayoutByPath(
                GetFileLayoutByPathResponseProto {
                    header: Some(ok_header()),
                    ..Default::default()
                },
            )),
            ScriptedResult::Response(ResponseEnvelope::GetFileStatus(GetFileStatusResponseProto {
                header: Some(ok_header()),
                ..Default::default()
            })),
        ]));

        let machine = ActionMachine::new(rpc.clone(), vec!["127.0.0.1:18080".to_string()]);
        let request = GetFileStatusRequestProto {
            header: Some(request_header()),
            path: "/mnt/a.txt".to_string(),
        };

        let response = call_machine(&machine, RpcOp::get_file_status(request))
            .await
            .expect("replay should succeed");
        assert!(response.header.as_ref().and_then(|h| h.error.as_ref()).is_none());

        let reconnects = rpc.reconnects.lock().await.clone();
        assert_eq!(reconnects.len(), 1);
        assert_eq!(reconnects[0], "http://127.0.0.2:18080");

        let calls = rpc.calls.lock().await;
        assert_eq!(calls.len(), 3);
        assert_eq!(calls[0].op_name(), "GetFileStatus");
        assert_eq!(calls[1].op_name(), "GetFileLayoutByPath");
        assert_eq!(calls[2].op_name(), "GetFileStatus");
    }

    #[tokio::test]
    async fn route_epoch_mismatch_refreshes_route_then_replays() {
        let canonical = CanonicalError::need_refresh_with_hint(
            RpcErrorCode::RouteEpochMismatch,
            RefreshReason::RouteEpochMismatch,
            CanonicalRefreshHint {
                route_epoch: Some(17),
                ..Default::default()
            },
            "route epoch mismatch",
        );

        let rpc = Arc::new(ScriptedRpc::new(vec![
            ScriptedResult::Response(ResponseEnvelope::GetFileStatus(GetFileStatusResponseProto {
                header: Some(err_header(canonical)),
                ..Default::default()
            })),
            ScriptedResult::Response(ResponseEnvelope::GetFileLayoutByPath(
                GetFileLayoutByPathResponseProto {
                    header: Some(ok_header_with_route(17)),
                    locations: vec![proto::metadata::FileBlockLocationProto {
                        block_id: None,
                        file_offset: 0,
                        len: 128,
                        workers: vec![WorkerEndpointInfoProto {
                            worker_id: 11,
                            endpoint: "127.0.0.1:19090".to_string(),
                            net_transport_kind: proto::common::NetTransportKindProto::NetTransportKindGrpc as i32,
                            worker_epoch: 9,
                        }],
                        worker_epoch: Some(9),
                    }],
                    ..Default::default()
                },
            )),
            ScriptedResult::Response(ResponseEnvelope::GetFileStatus(GetFileStatusResponseProto {
                header: Some(ok_header()),
                ..Default::default()
            })),
        ]));

        let machine = ActionMachine::new(rpc.clone(), vec![]);
        let request = GetFileStatusRequestProto {
            header: Some(request_header()),
            path: "/mnt/route.bin".to_string(),
        };

        call_machine(&machine, RpcOp::get_file_status(request))
            .await
            .expect("route refresh replay should succeed");

        let calls = rpc.calls.lock().await;
        assert_eq!(calls.len(), 3);
        assert_eq!(calls[0].op_name(), "GetFileStatus");
        assert_eq!(calls[1].op_name(), "GetFileLayoutByPath");
        assert_eq!(calls[2].op_name(), "GetFileStatus");

        let replay_header = calls[2].header().expect("replay header");
        assert_eq!(replay_header.route_epoch, Some(17));
    }

    #[tokio::test]
    async fn permission_denied_is_terminal_without_replay() {
        let canonical = CanonicalError::fatal_fs(FsErrorCode::EAcces, "permission denied");

        let rpc = Arc::new(ScriptedRpc::new(vec![ScriptedResult::Response(
            ResponseEnvelope::GetFileStatus(GetFileStatusResponseProto {
                header: Some(err_header(canonical)),
                ..Default::default()
            }),
        )]));

        let machine = ActionMachine::new(rpc.clone(), vec![]);
        let request = GetFileStatusRequestProto {
            header: Some(request_header()),
            path: "/mnt/deny.bin".to_string(),
        };

        let err = call_machine(&machine, RpcOp::get_file_status(request))
            .await
            .expect_err("permission denied should be terminal");

        match err {
            ClientError::Action(ClientAction::Fail { canonical }) => {
                assert!(matches!(
                    canonical.code,
                    Some(CanonicalCode::FsErrno(FsErrorCode::EAcces))
                ));
            }
            other => panic!("expected terminal canonical fail, got {:?}", other),
        }

        assert_eq!(rpc.calls.lock().await.len(), 1);
        assert_eq!(rpc.reconnects.lock().await.len(), 0);
    }

    #[tokio::test]
    async fn grpc_unavailable_retries_with_bound() {
        let rpc = Arc::new(ScriptedRpc::new(vec![
            ScriptedResult::Status(tonic::Status::unavailable("temporary outage")),
            ScriptedResult::Response(ResponseEnvelope::GetFileStatus(GetFileStatusResponseProto {
                header: Some(ok_header()),
                ..Default::default()
            })),
        ]));

        let machine = ActionMachine::new(rpc.clone(), vec![]).with_policy(ActionMachinePolicy {
            max_refresh_attempts: 1,
            max_retryable_attempts: 1,
            max_transport_retries: 1,
            base_backoff_ms: 0,
        });

        let request = GetFileStatusRequestProto {
            header: Some(request_header()),
            path: "/mnt/retry.bin".to_string(),
        };

        call_machine(&machine, RpcOp::get_file_status(request))
            .await
            .expect("transport retry should succeed");

        assert_eq!(rpc.calls.lock().await.len(), 2);
    }

    #[tokio::test]
    async fn delete_uses_delete_rpc_with_mutation_policy() {
        let rpc = Arc::new(ScriptedRpc::new(vec![ScriptedResult::Response(
            ResponseEnvelope::Delete(DeleteResponseProto {
                header: Some(ok_header()),
            }),
        )]));

        let machine = ActionMachine::new(rpc.clone(), vec![]);
        let request = DeleteRequestProto {
            header: Some(request_header()),
            path: "/mnt/delete.bin".to_string(),
            recursive: false,
        };

        machine
            .call_with_refresh(ReplayPolicy::Mutation, RpcOp::delete(request))
            .await
            .expect("delete should use the mutation policy");

        let calls = rpc.calls.lock().await;
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].op_name(), "Delete");
    }

    #[tokio::test]
    async fn fsync_session_reopens_session_on_invalid_error() {
        let open_resp_1 = OpenWriteByPathResponseProto {
            header: Some(ok_header()),
            file_handle: 10,
            lease_id: Some(proto::common::LeaseIdProto { high: 0, low: 1 }),
            fencing_token: Some(proto::common::FencingTokenProto {
                block_id: None,
                owner: 1,
                epoch: 1,
            }),
            write_targets: vec![],
            base_size: 0,
            open_epoch: 100,
            lease_epoch: 1,
            expires_at_ms: 1000,
        };

        let open_resp_2 = OpenWriteByPathResponseProto {
            header: Some(ok_header()),
            file_handle: 20,
            lease_id: Some(proto::common::LeaseIdProto { high: 0, low: 2 }),
            fencing_token: Some(proto::common::FencingTokenProto {
                block_id: None,
                owner: 1,
                epoch: 2,
            }),
            write_targets: vec![],
            base_size: 0,
            open_epoch: 200,
            lease_epoch: 2,
            expires_at_ms: 2000,
        };

        let invalid_session = CanonicalError {
            class: ErrorClass::Fatal,
            code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Fencing)),
            reason: Some(RefreshReason::SessionInvalid),
            retry_after_ms: None,
            message: "write session invalid; reopen session and replay fsync".to_string(),
            refresh_hint: None,
        };

        let rpc = Arc::new(ScriptedRpc::new(vec![
            ScriptedResult::Response(ResponseEnvelope::OpenWriteByPath(open_resp_1.clone())),
            ScriptedResult::Response(ResponseEnvelope::FsyncSession(FsyncSessionResponseProto {
                header: Some(err_header(invalid_session)),
            })),
            ScriptedResult::Response(ResponseEnvelope::OpenWriteByPath(open_resp_2.clone())),
            ScriptedResult::Response(ResponseEnvelope::FsyncSession(FsyncSessionResponseProto {
                header: Some(ok_header()),
            })),
        ]));

        let machine = ActionMachine::new(rpc.clone(), vec![]);

        let open_req = OpenWriteByPathRequestProto {
            header: Some(request_header()),
            path: "/mnt/writer.bin".to_string(),
            desired_len: Some(4096),
            mode: proto::metadata::WriteModeProto::WriteModeAppend as i32,
        };

        call_machine(&machine, RpcOp::open_write_by_path(open_req))
            .await
            .expect("initial open should succeed");

        let fsync_req = FsyncSessionRequestProto {
            header: Some(request_header()),
            file_handle: 10,
            flags: 0,
            lease_id: open_resp_1.lease_id,
            lease_epoch: Some(open_resp_1.lease_epoch),
            fencing_token: open_resp_1.fencing_token,
            worker_epoch: None,
            target_size: None,
        };

        call_machine(&machine, RpcOp::fsync_session(fsync_req))
            .await
            .expect("fsync should reopen and replay");

        let calls = rpc.calls.lock().await;
        assert_eq!(calls.len(), 4);
        assert_eq!(calls[0].op_name(), "OpenWriteByPath");
        assert_eq!(calls[1].op_name(), "FsyncSession");
        assert_eq!(calls[2].op_name(), "OpenWriteByPath");
        assert_eq!(calls[3].op_name(), "FsyncSession");

        match &calls[3] {
            RequestEnvelope::FsyncSession(req) => {
                assert_eq!(req.file_handle, 20);
                assert_eq!(req.lease_epoch, Some(2));
            }
            other => panic!("expected fsync replay request, got {}", other.op_name()),
        }
    }

    #[tokio::test]
    async fn fsync_session_reopens_session_on_expired_error() {
        let open_resp_1 = OpenWriteByPathResponseProto {
            header: Some(ok_header()),
            file_handle: 100,
            lease_id: Some(proto::common::LeaseIdProto { high: 0, low: 10 }),
            fencing_token: Some(proto::common::FencingTokenProto {
                block_id: None,
                owner: 1,
                epoch: 10,
            }),
            write_targets: vec![],
            base_size: 0,
            open_epoch: 1000,
            lease_epoch: 10,
            expires_at_ms: 10_000,
        };

        let open_resp_2 = OpenWriteByPathResponseProto {
            header: Some(ok_header()),
            file_handle: 200,
            lease_id: Some(proto::common::LeaseIdProto { high: 0, low: 20 }),
            fencing_token: Some(proto::common::FencingTokenProto {
                block_id: None,
                owner: 1,
                epoch: 20,
            }),
            write_targets: vec![],
            base_size: 0,
            open_epoch: 2000,
            lease_epoch: 20,
            expires_at_ms: 20_000,
        };

        let expired_session = CanonicalError {
            class: ErrorClass::Fatal,
            code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Fencing)),
            reason: Some(RefreshReason::SessionExpired),
            retry_after_ms: None,
            message: "session token stale".to_string(),
            refresh_hint: None,
        };

        let rpc = Arc::new(ScriptedRpc::new(vec![
            ScriptedResult::Response(ResponseEnvelope::OpenWriteByPath(open_resp_1.clone())),
            ScriptedResult::Response(ResponseEnvelope::FsyncSession(FsyncSessionResponseProto {
                header: Some(err_header(expired_session)),
            })),
            ScriptedResult::Response(ResponseEnvelope::OpenWriteByPath(open_resp_2.clone())),
            ScriptedResult::Response(ResponseEnvelope::FsyncSession(FsyncSessionResponseProto {
                header: Some(ok_header()),
            })),
        ]));

        let machine = ActionMachine::new(rpc.clone(), vec![]);

        call_machine(
            &machine,
            RpcOp::open_write_by_path(OpenWriteByPathRequestProto {
                header: Some(request_header()),
                path: "/mnt/writer-expired.bin".to_string(),
                desired_len: Some(4096),
                mode: proto::metadata::WriteModeProto::WriteModeAppend as i32,
            }),
        )
        .await
        .expect("initial open should succeed");

        call_machine(
            &machine,
            RpcOp::fsync_session(FsyncSessionRequestProto {
                header: Some(request_header()),
                file_handle: 100,
                flags: 0,
                lease_id: open_resp_1.lease_id,
                lease_epoch: Some(open_resp_1.lease_epoch),
                fencing_token: open_resp_1.fencing_token,
                worker_epoch: None,
                target_size: None,
            }),
        )
        .await
        .expect("session expired should reopen and replay");

        let calls = rpc.calls.lock().await;
        assert_eq!(calls.len(), 4);
        assert_eq!(calls[0].op_name(), "OpenWriteByPath");
        assert_eq!(calls[1].op_name(), "FsyncSession");
        assert_eq!(calls[2].op_name(), "OpenWriteByPath");
        assert_eq!(calls[3].op_name(), "FsyncSession");
        match &calls[3] {
            RequestEnvelope::FsyncSession(req) => {
                assert_eq!(req.file_handle, 200);
                assert_eq!(req.lease_epoch, Some(20));
            }
            other => panic!("expected fsync replay request, got {}", other.op_name()),
        }
    }

    #[tokio::test]
    async fn fsync_session_does_not_reopen_from_message_heuristics() {
        let open_resp = OpenWriteByPathResponseProto {
            header: Some(ok_header()),
            file_handle: 300,
            lease_id: Some(proto::common::LeaseIdProto { high: 0, low: 30 }),
            fencing_token: Some(proto::common::FencingTokenProto {
                block_id: None,
                owner: 1,
                epoch: 30,
            }),
            write_targets: vec![],
            base_size: 0,
            open_epoch: 3000,
            lease_epoch: 30,
            expires_at_ms: 30_000,
        };

        let fatal_unknown = CanonicalError {
            class: ErrorClass::Fatal,
            code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Fencing)),
            reason: Some(RefreshReason::Unknown),
            retry_after_ms: None,
            message: "please reopen session and replay".to_string(),
            refresh_hint: None,
        };

        let rpc = Arc::new(ScriptedRpc::new(vec![
            ScriptedResult::Response(ResponseEnvelope::OpenWriteByPath(open_resp.clone())),
            ScriptedResult::Response(ResponseEnvelope::FsyncSession(FsyncSessionResponseProto {
                header: Some(err_header(fatal_unknown)),
            })),
        ]));

        let machine = ActionMachine::new(rpc.clone(), vec![]);
        call_machine(
            &machine,
            RpcOp::open_write_by_path(OpenWriteByPathRequestProto {
                header: Some(request_header()),
                path: "/mnt/writer-no-message-heuristics.bin".to_string(),
                desired_len: Some(4096),
                mode: proto::metadata::WriteModeProto::WriteModeAppend as i32,
            }),
        )
        .await
        .expect("initial open should succeed");

        let err = call_machine(
            &machine,
            RpcOp::fsync_session(FsyncSessionRequestProto {
                header: Some(request_header()),
                file_handle: open_resp.file_handle,
                flags: 0,
                lease_id: open_resp.lease_id,
                lease_epoch: Some(open_resp.lease_epoch),
                fencing_token: open_resp.fencing_token,
                worker_epoch: None,
                target_size: None,
            }),
        )
        .await
        .expect_err("unknown reason must not trigger reopen");

        match err {
            ClientError::Action(ClientAction::Fail { canonical }) => {
                assert_eq!(canonical.reason, Some(RefreshReason::Unknown));
            }
            other => panic!("expected terminal fail, got {:?}", other),
        }

        let calls = rpc.calls.lock().await;
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].op_name(), "OpenWriteByPath");
        assert_eq!(calls[1].op_name(), "FsyncSession");
    }

    #[test]
    fn filesystem_rpc_replay_policy_table_is_complete_and_unique() {
        let mut seen = std::collections::HashSet::new();
        for entry in FILESYSTEM_RPC_REPLAY_POLICIES.iter() {
            assert!(
                seen.insert(entry.method),
                "duplicate replay policy mapping for {:?}",
                entry.method
            );
        }
        assert_eq!(seen.len(), FileSystemRpcMethod::ALL.len());
        for method in FileSystemRpcMethod::ALL.iter() {
            assert!(seen.contains(method), "missing replay policy mapping for {:?}", method);
            assert_eq!(
                replay_policy_for_method(*method),
                FILESYSTEM_RPC_REPLAY_POLICIES
                    .iter()
                    .find_map(|entry| (entry.method == *method).then_some(entry.policy))
                    .expect("table entry"),
            );
        }
    }

    #[test]
    fn shard_moved_code_maps_to_route_epoch_mismatch_reason() {
        let reason = refresh_reason_from_code(Some(CanonicalErrorCode::RpcCode(RpcErrorCode::ShardMoved)));
        assert_eq!(reason, RefreshReason::RouteEpochMismatch);
    }
}
