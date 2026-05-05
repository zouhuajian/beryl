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
use proto::common::{
    ClientInfoProto, GroupStateWatermarkProto, RaftLogIdProto, RequestHeaderProto, ResponseHeaderProto,
    ShardGroupIdProto, WorkerEndpointInfoProto,
};
use proto::metadata::file_system_service_proto_client::FileSystemServiceProtoClient;
use proto::metadata::{
    AbortFileWriteRequestProto, AbortFileWriteResponseProto, AddBlockRequestProto, AddBlockResponseProto,
    AppendFileRequestProto, AppendFileResponseProto, CommitFileRequestProto, CommitFileResponseProto,
    CreateDirectoryRequestProto, CreateDirectoryResponseProto, CreateFileRequestProto, CreateFileResponseProto,
    DeleteRequestProto, DeleteResponseProto, GetBlockLocationsRequestProto, GetBlockLocationsResponseProto,
    GetStatusRequestProto, GetStatusResponseProto, HflushRequestProto, HflushResponseProto, HsyncRequestProto,
    HsyncResponseProto, ListStatusRequestProto, ListStatusResponseProto, MsyncRequestProto, MsyncResponseProto,
    OpenFileRequestProto, OpenFileResponseProto, RenameRequestProto, RenameResponseProto, RenewLeaseRequestProto,
    RenewLeaseResponseProto, WriteHandleProto, WriteTargetProto,
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
    /// Idempotent metadata reads.
    IdempotentRead,
    /// Namespace or write-target mutations.
    Mutation,
    /// Write handle lifecycle operations.
    SessionBarrier,
    /// Best-effort cleanup operations (currently unused by the FileSystemService client surface).
    CleanupBestEffort,
}

/// Stable identifier for each public FileSystemService client RPC.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FileSystemRpcMethod {
    /// Get file or directory status.
    GetStatus,
    /// List directory status entries.
    ListStatus,
    /// Create a directory.
    CreateDirectory,
    /// Delete a file or directory.
    Delete,
    /// Rename a namespace entry.
    Rename,
    /// Open a file for direct read planning.
    OpenFile,
    /// Resolve direct read block locations.
    GetBlockLocations,
    /// Create a file and open a write handle.
    CreateFile,
    /// Open an existing file for append.
    AppendFile,
    /// Allocate the next write block target.
    AddBlock,
    /// Commit a write handle.
    CommitFile,
    /// Abort a write handle.
    AbortFileWrite,
    /// Renew a write lease.
    RenewLease,
    /// Flush write visibility.
    Hflush,
    /// Sync write durability.
    Hsync,
    /// Synchronize metadata state freshness.
    Msync,
}

impl FileSystemRpcMethod {
    #[cfg(test)]
    const ALL: [Self; 16] = [
        Self::GetStatus,
        Self::ListStatus,
        Self::CreateDirectory,
        Self::Delete,
        Self::Rename,
        Self::OpenFile,
        Self::GetBlockLocations,
        Self::CreateFile,
        Self::AppendFile,
        Self::AddBlock,
        Self::CommitFile,
        Self::AbortFileWrite,
        Self::RenewLease,
        Self::Hflush,
        Self::Hsync,
        Self::Msync,
    ];
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ReplayPolicyMapping {
    method: FileSystemRpcMethod,
    policy: ReplayPolicy,
}

const FILESYSTEM_RPC_REPLAY_POLICIES: [ReplayPolicyMapping; 16] = [
    ReplayPolicyMapping {
        method: FileSystemRpcMethod::GetStatus,
        policy: ReplayPolicy::IdempotentRead,
    },
    ReplayPolicyMapping {
        method: FileSystemRpcMethod::ListStatus,
        policy: ReplayPolicy::IdempotentRead,
    },
    ReplayPolicyMapping {
        method: FileSystemRpcMethod::CreateDirectory,
        policy: ReplayPolicy::Mutation,
    },
    ReplayPolicyMapping {
        method: FileSystemRpcMethod::Delete,
        policy: ReplayPolicy::Mutation,
    },
    ReplayPolicyMapping {
        method: FileSystemRpcMethod::Rename,
        policy: ReplayPolicy::Mutation,
    },
    ReplayPolicyMapping {
        method: FileSystemRpcMethod::OpenFile,
        policy: ReplayPolicy::IdempotentRead,
    },
    ReplayPolicyMapping {
        method: FileSystemRpcMethod::GetBlockLocations,
        policy: ReplayPolicy::IdempotentRead,
    },
    ReplayPolicyMapping {
        method: FileSystemRpcMethod::CreateFile,
        policy: ReplayPolicy::Mutation,
    },
    ReplayPolicyMapping {
        method: FileSystemRpcMethod::AppendFile,
        policy: ReplayPolicy::Mutation,
    },
    ReplayPolicyMapping {
        method: FileSystemRpcMethod::AddBlock,
        policy: ReplayPolicy::Mutation,
    },
    ReplayPolicyMapping {
        method: FileSystemRpcMethod::CommitFile,
        policy: ReplayPolicy::SessionBarrier,
    },
    ReplayPolicyMapping {
        method: FileSystemRpcMethod::AbortFileWrite,
        policy: ReplayPolicy::SessionBarrier,
    },
    ReplayPolicyMapping {
        method: FileSystemRpcMethod::RenewLease,
        policy: ReplayPolicy::SessionBarrier,
    },
    ReplayPolicyMapping {
        method: FileSystemRpcMethod::Hflush,
        policy: ReplayPolicy::SessionBarrier,
    },
    ReplayPolicyMapping {
        method: FileSystemRpcMethod::Hsync,
        policy: ReplayPolicy::SessionBarrier,
    },
    ReplayPolicyMapping {
        method: FileSystemRpcMethod::Msync,
        policy: ReplayPolicy::IdempotentRead,
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
    /// Call `GetStatus`.
    async fn get_status(&self, request: GetStatusRequestProto) -> Result<GetStatusResponseProto, tonic::Status>;

    /// Call `ListStatus`.
    async fn list_status(&self, request: ListStatusRequestProto) -> Result<ListStatusResponseProto, tonic::Status>;

    /// Call `CreateDirectory`.
    async fn create_directory(
        &self,
        request: CreateDirectoryRequestProto,
    ) -> Result<CreateDirectoryResponseProto, tonic::Status>;

    /// Call `Delete`.
    async fn delete(&self, request: DeleteRequestProto) -> Result<DeleteResponseProto, tonic::Status>;

    /// Call `Rename`.
    async fn rename(&self, request: RenameRequestProto) -> Result<RenameResponseProto, tonic::Status>;

    /// Call `OpenFile`.
    async fn open_file(&self, request: OpenFileRequestProto) -> Result<OpenFileResponseProto, tonic::Status>;

    /// Call `GetBlockLocations`.
    async fn get_block_locations(
        &self,
        request: GetBlockLocationsRequestProto,
    ) -> Result<GetBlockLocationsResponseProto, tonic::Status>;

    /// Call `CreateFile`.
    async fn create_file(&self, request: CreateFileRequestProto) -> Result<CreateFileResponseProto, tonic::Status>;

    /// Call `AppendFile`.
    async fn append_file(&self, request: AppendFileRequestProto) -> Result<AppendFileResponseProto, tonic::Status>;

    /// Call `AddBlock`.
    async fn add_block(&self, request: AddBlockRequestProto) -> Result<AddBlockResponseProto, tonic::Status>;

    /// Call `CommitFile`.
    async fn commit_file(&self, request: CommitFileRequestProto) -> Result<CommitFileResponseProto, tonic::Status>;

    /// Call `AbortFileWrite`.
    async fn abort_file_write(
        &self,
        request: AbortFileWriteRequestProto,
    ) -> Result<AbortFileWriteResponseProto, tonic::Status>;

    /// Call `RenewLease`.
    async fn renew_lease(&self, request: RenewLeaseRequestProto) -> Result<RenewLeaseResponseProto, tonic::Status>;

    /// Call `Hflush`.
    async fn hflush(&self, request: HflushRequestProto) -> Result<HflushResponseProto, tonic::Status>;

    /// Call `Hsync`.
    async fn hsync(&self, request: HsyncRequestProto) -> Result<HsyncResponseProto, tonic::Status>;

    /// Call `Msync`.
    async fn msync(&self, request: MsyncRequestProto) -> Result<MsyncResponseProto, tonic::Status>;

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
    async fn get_status(&self, request: GetStatusRequestProto) -> Result<GetStatusResponseProto, tonic::Status> {
        let mut client = self.client.lock().await;
        client
            .get_status(tonic::Request::new(request))
            .await
            .map(|resp| resp.into_inner())
    }

    async fn list_status(&self, request: ListStatusRequestProto) -> Result<ListStatusResponseProto, tonic::Status> {
        let mut client = self.client.lock().await;
        client
            .list_status(tonic::Request::new(request))
            .await
            .map(|resp| resp.into_inner())
    }

    async fn create_directory(
        &self,
        request: CreateDirectoryRequestProto,
    ) -> Result<CreateDirectoryResponseProto, tonic::Status> {
        let mut client = self.client.lock().await;
        client
            .create_directory(tonic::Request::new(request))
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

    async fn rename(&self, request: RenameRequestProto) -> Result<RenameResponseProto, tonic::Status> {
        let mut client = self.client.lock().await;
        client
            .rename(tonic::Request::new(request))
            .await
            .map(|resp| resp.into_inner())
    }

    async fn open_file(&self, request: OpenFileRequestProto) -> Result<OpenFileResponseProto, tonic::Status> {
        let mut client = self.client.lock().await;
        client
            .open_file(tonic::Request::new(request))
            .await
            .map(|resp| resp.into_inner())
    }

    async fn get_block_locations(
        &self,
        request: GetBlockLocationsRequestProto,
    ) -> Result<GetBlockLocationsResponseProto, tonic::Status> {
        let mut client = self.client.lock().await;
        client
            .get_block_locations(tonic::Request::new(request))
            .await
            .map(|resp| resp.into_inner())
    }

    async fn create_file(&self, request: CreateFileRequestProto) -> Result<CreateFileResponseProto, tonic::Status> {
        let mut client = self.client.lock().await;
        client
            .create_file(tonic::Request::new(request))
            .await
            .map(|resp| resp.into_inner())
    }

    async fn append_file(&self, request: AppendFileRequestProto) -> Result<AppendFileResponseProto, tonic::Status> {
        let mut client = self.client.lock().await;
        client
            .append_file(tonic::Request::new(request))
            .await
            .map(|resp| resp.into_inner())
    }

    async fn add_block(&self, request: AddBlockRequestProto) -> Result<AddBlockResponseProto, tonic::Status> {
        let mut client = self.client.lock().await;
        client
            .add_block(tonic::Request::new(request))
            .await
            .map(|resp| resp.into_inner())
    }

    async fn commit_file(&self, request: CommitFileRequestProto) -> Result<CommitFileResponseProto, tonic::Status> {
        let mut client = self.client.lock().await;
        client
            .commit_file(tonic::Request::new(request))
            .await
            .map(|resp| resp.into_inner())
    }

    async fn abort_file_write(
        &self,
        request: AbortFileWriteRequestProto,
    ) -> Result<AbortFileWriteResponseProto, tonic::Status> {
        let mut client = self.client.lock().await;
        client
            .abort_file_write(tonic::Request::new(request))
            .await
            .map(|resp| resp.into_inner())
    }

    async fn renew_lease(&self, request: RenewLeaseRequestProto) -> Result<RenewLeaseResponseProto, tonic::Status> {
        let mut client = self.client.lock().await;
        client
            .renew_lease(tonic::Request::new(request))
            .await
            .map(|resp| resp.into_inner())
    }

    async fn hflush(&self, request: HflushRequestProto) -> Result<HflushResponseProto, tonic::Status> {
        let mut client = self.client.lock().await;
        client
            .hflush(tonic::Request::new(request))
            .await
            .map(|resp| resp.into_inner())
    }

    async fn hsync(&self, request: HsyncRequestProto) -> Result<HsyncResponseProto, tonic::Status> {
        let mut client = self.client.lock().await;
        client
            .hsync(tonic::Request::new(request))
            .await
            .map(|resp| resp.into_inner())
    }

    async fn msync(&self, request: MsyncRequestProto) -> Result<MsyncResponseProto, tonic::Status> {
        let mut client = self.client.lock().await;
        client
            .msync(tonic::Request::new(request))
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

impl RpcOp<GetStatusResponseProto> {
    /// Build a `GetStatus` operation.
    pub fn get_status(request: GetStatusRequestProto) -> Self {
        Self::new(
            RequestEnvelope::GetStatus(request),
            Arc::new(|resp| match resp {
                ResponseEnvelope::GetStatus(v) => Ok(v),
                other => Err(unexpected_response("GetStatus", other.op_name())),
            }),
        )
    }
}

impl RpcOp<ListStatusResponseProto> {
    /// Build a `ListStatus` operation.
    pub fn list_status(request: ListStatusRequestProto) -> Self {
        Self::new(
            RequestEnvelope::ListStatus(request),
            Arc::new(|resp| match resp {
                ResponseEnvelope::ListStatus(v) => Ok(v),
                other => Err(unexpected_response("ListStatus", other.op_name())),
            }),
        )
    }
}

impl RpcOp<CreateDirectoryResponseProto> {
    /// Build a `CreateDirectory` operation.
    pub fn create_directory(request: CreateDirectoryRequestProto) -> Self {
        Self::new(
            RequestEnvelope::CreateDirectory(request),
            Arc::new(|resp| match resp {
                ResponseEnvelope::CreateDirectory(v) => Ok(v),
                other => Err(unexpected_response("CreateDirectory", other.op_name())),
            }),
        )
    }
}

impl RpcOp<DeleteResponseProto> {
    /// Build a `Delete` operation.
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

impl RpcOp<RenameResponseProto> {
    /// Build a `Rename` operation.
    pub fn rename(request: RenameRequestProto) -> Self {
        Self::new(
            RequestEnvelope::Rename(request),
            Arc::new(|resp| match resp {
                ResponseEnvelope::Rename(v) => Ok(v),
                other => Err(unexpected_response("Rename", other.op_name())),
            }),
        )
    }
}

impl RpcOp<OpenFileResponseProto> {
    /// Build an `OpenFile` operation.
    pub fn open_file(request: OpenFileRequestProto) -> Self {
        Self::new(
            RequestEnvelope::OpenFile(request),
            Arc::new(|resp| match resp {
                ResponseEnvelope::OpenFile(v) => Ok(v),
                other => Err(unexpected_response("OpenFile", other.op_name())),
            }),
        )
    }
}

impl RpcOp<GetBlockLocationsResponseProto> {
    /// Build a `GetBlockLocations` operation.
    pub fn get_block_locations(request: GetBlockLocationsRequestProto) -> Self {
        Self::new(
            RequestEnvelope::GetBlockLocations(request),
            Arc::new(|resp| match resp {
                ResponseEnvelope::GetBlockLocations(v) => Ok(v),
                other => Err(unexpected_response("GetBlockLocations", other.op_name())),
            }),
        )
    }
}

impl RpcOp<CreateFileResponseProto> {
    /// Build a `CreateFile` operation.
    pub fn create_file(request: CreateFileRequestProto) -> Self {
        Self::new(
            RequestEnvelope::CreateFile(request),
            Arc::new(|resp| match resp {
                ResponseEnvelope::CreateFile(v) => Ok(v),
                other => Err(unexpected_response("CreateFile", other.op_name())),
            }),
        )
    }
}

impl RpcOp<AppendFileResponseProto> {
    /// Build an `AppendFile` operation.
    pub fn append_file(request: AppendFileRequestProto) -> Self {
        Self::new(
            RequestEnvelope::AppendFile(request),
            Arc::new(|resp| match resp {
                ResponseEnvelope::AppendFile(v) => Ok(v),
                other => Err(unexpected_response("AppendFile", other.op_name())),
            }),
        )
    }
}

impl RpcOp<AddBlockResponseProto> {
    /// Build an `AddBlock` operation.
    pub fn add_block(request: AddBlockRequestProto) -> Self {
        Self::new(
            RequestEnvelope::AddBlock(request),
            Arc::new(|resp| match resp {
                ResponseEnvelope::AddBlock(v) => Ok(v),
                other => Err(unexpected_response("AddBlock", other.op_name())),
            }),
        )
    }
}

impl RpcOp<CommitFileResponseProto> {
    /// Build a `CommitFile` operation.
    pub fn commit_file(request: CommitFileRequestProto) -> Self {
        Self::new(
            RequestEnvelope::CommitFile(request),
            Arc::new(|resp| match resp {
                ResponseEnvelope::CommitFile(v) => Ok(v),
                other => Err(unexpected_response("CommitFile", other.op_name())),
            }),
        )
    }
}

impl RpcOp<AbortFileWriteResponseProto> {
    /// Build an `AbortFileWrite` operation.
    pub fn abort_file_write(request: AbortFileWriteRequestProto) -> Self {
        Self::new(
            RequestEnvelope::AbortFileWrite(request),
            Arc::new(|resp| match resp {
                ResponseEnvelope::AbortFileWrite(v) => Ok(v),
                other => Err(unexpected_response("AbortFileWrite", other.op_name())),
            }),
        )
    }
}

impl RpcOp<RenewLeaseResponseProto> {
    /// Build a `RenewLease` operation.
    pub fn renew_lease(request: RenewLeaseRequestProto) -> Self {
        Self::new(
            RequestEnvelope::RenewLease(request),
            Arc::new(|resp| match resp {
                ResponseEnvelope::RenewLease(v) => Ok(v),
                other => Err(unexpected_response("RenewLease", other.op_name())),
            }),
        )
    }
}

impl RpcOp<HflushResponseProto> {
    /// Build an `Hflush` operation.
    pub fn hflush(request: HflushRequestProto) -> Self {
        Self::new(
            RequestEnvelope::Hflush(request),
            Arc::new(|resp| match resp {
                ResponseEnvelope::Hflush(v) => Ok(v),
                other => Err(unexpected_response("Hflush", other.op_name())),
            }),
        )
    }
}

impl RpcOp<HsyncResponseProto> {
    /// Build an `Hsync` operation.
    pub fn hsync(request: HsyncRequestProto) -> Self {
        Self::new(
            RequestEnvelope::Hsync(request),
            Arc::new(|resp| match resp {
                ResponseEnvelope::Hsync(v) => Ok(v),
                other => Err(unexpected_response("Hsync", other.op_name())),
            }),
        )
    }
}

impl RpcOp<MsyncResponseProto> {
    /// Build an `Msync` operation.
    pub fn msync(request: MsyncRequestProto) -> Self {
        Self::new(
            RequestEnvelope::Msync(request),
            Arc::new(|resp| match resp {
                ResponseEnvelope::Msync(v) => Ok(v),
                other => Err(unexpected_response("Msync", other.op_name())),
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
                        return Err(ClientError::from(ClientAction::Fail {
                            canonical: Box::new(canonical),
                        }));
                    }

                    match canonical.class {
                        ErrorClass::Fatal => {
                            return Err(ClientError::from(ClientAction::Fail {
                                canonical: Box::new(canonical),
                            }));
                        }
                        ErrorClass::Retryable => {
                            if retryable_attempts >= self.policy.max_retryable_attempts {
                                return Err(ClientError::from(ClientAction::Retry {
                                    after_ms: canonical.retry_after_ms,
                                    canonical: Box::new(canonical),
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
                                    canonical: Box::new(canonical),
                                }));
                            }
                            refresh_attempts += 1;
                            let hint = self.build_refresh_hint(&request, header, &canonical);
                            let response_header = header.cloned();
                            self.handle_need_refresh(
                                &replay_policy,
                                reason,
                                &canonical,
                                hint,
                                response_header,
                                &mut request,
                            )
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
        let path_hint = request.path_hint(&self.caches.write_handles);
        let header = request.ensure_header_mut();

        if header.client.is_none() {
            header.client = Some(default_client_info_proto());
        }
        let client = header.client.as_mut().expect("client initialized");
        if client.call_id.is_empty() {
            client.call_id = CallId::new().to_string();
        }

        if header.traceparent.is_empty() {
            header.traceparent = parent_id.to_string();
        }
        header.retry_count = retry_count;
        if header.group_id != 0 {
            if let Some(state_id) = self.caches.state_for_group(header.group_id) {
                header.state = vec![GroupStateWatermarkProto {
                    group_id: Some(ShardGroupIdProto { value: header.group_id }),
                    state_id: Some(state_id),
                }];
            }
        }

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
        let path_hint = request.path_hint(&self.caches.write_handles);
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

    fn build_refresh_hint_for_header(
        &self,
        header: Option<&ResponseHeaderProto>,
        canonical: &CanonicalError,
    ) -> RefreshHint {
        let canonical_hint = canonical.refresh_hint.as_ref();
        let group_id = canonical_hint
            .and_then(|hint| hint.group_id)
            .or_else(|| header.and_then(|h| if h.group_id == 0 { None } else { Some(h.group_id) }));
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
            route_epoch: canonical_hint
                .and_then(|hint| hint.route_epoch)
                .or_else(|| header.and_then(|h| h.route_epoch)),
            mount_epoch: canonical_hint
                .and_then(|hint| hint.mount_epoch)
                .or_else(|| header.and_then(|h| h.mount_epoch)),
            worker_epoch: canonical_hint.and_then(|hint| hint.worker_epoch),
            endpoint_hint,
        }
    }

    fn update_header_hints(&self, request: &RequestEnvelope, header: Option<&ResponseHeaderProto>) -> bool {
        let Some(header) = header else {
            return false;
        };
        let mut refreshed = !header.state.is_empty();
        self.caches.merge_response_state(&header.state);
        let Some(path) = request.path_hint(&self.caches.write_handles) else {
            return refreshed;
        };
        if let Some(mount_epoch) = header.mount_epoch {
            self.caches.record_mount_epoch(&path, mount_epoch);
            refreshed = true;
        }
        if let Some(route_epoch) = header.route_epoch {
            self.caches.record_route_epoch(&path, route_epoch);
            refreshed = true;
        }
        refreshed
    }

    fn update_success_caches(&self, request: &RequestEnvelope, response: &ResponseEnvelope) -> bool {
        if let Some(endpoint) = self.rpc.current_endpoint() {
            self.caches.set_leader_endpoint(normalize_endpoint(&endpoint));
        }

        self.update_payload_hints(request, response)
    }

    fn update_payload_hints(&self, request: &RequestEnvelope, response: &ResponseEnvelope) -> bool {
        match (request, response) {
            (RequestEnvelope::OpenFile(req), ResponseEnvelope::OpenFile(resp)) => {
                let path = req.path.clone();
                let mut refreshed =
                    resp.header.as_ref().and_then(|header| header.route_epoch).is_some() || !resp.locations.is_empty();
                if let Some(route_epoch) = resp.header.as_ref().and_then(|header| header.route_epoch) {
                    self.caches.record_route_epoch(&path, route_epoch);
                }
                let (workers, worker_epoch) = workers_from_locations(&resp.locations);
                refreshed = refreshed || !workers.is_empty() || worker_epoch.is_some();
                self.caches.record_worker_info(&path, workers, worker_epoch);
                refreshed
            }
            (RequestEnvelope::GetBlockLocations(req), ResponseEnvelope::GetBlockLocations(resp)) => {
                let Some(path) = req_path_from_locations_request(req) else {
                    return !resp.locations.is_empty();
                };
                let mut refreshed =
                    resp.header.as_ref().and_then(|header| header.route_epoch).is_some() || !resp.locations.is_empty();
                if let Some(route_epoch) = resp.header.as_ref().and_then(|header| header.route_epoch) {
                    self.caches.record_route_epoch(&path, route_epoch);
                }
                let (workers, worker_epoch) = workers_from_locations(&resp.locations);
                refreshed = refreshed || !workers.is_empty() || worker_epoch.is_some();
                self.caches.record_worker_info(&path, workers, worker_epoch);
                refreshed
            }
            (RequestEnvelope::CreateFile(req), ResponseEnvelope::CreateFile(resp)) => {
                let path = req.path.clone();
                let refreshed = resp.header.as_ref().and_then(|header| header.route_epoch).is_some()
                    || resp
                        .write_handle
                        .as_ref()
                        .map(|handle| handle.handle_id != 0)
                        .unwrap_or(false);
                if let Some(route_epoch) = resp.header.as_ref().and_then(|header| header.route_epoch) {
                    self.caches.record_route_epoch(&path, route_epoch);
                }
                if let Some(handle) = WriteHandleState::from_create_response(req, resp) {
                    self.caches.upsert_handle(handle);
                }
                refreshed
            }
            (RequestEnvelope::AppendFile(req), ResponseEnvelope::AppendFile(resp)) => {
                let path = req.path.clone();
                let refreshed = resp.header.as_ref().and_then(|header| header.route_epoch).is_some()
                    || resp
                        .write_handle
                        .as_ref()
                        .map(|handle| handle.handle_id != 0)
                        .unwrap_or(false);
                if let Some(route_epoch) = resp.header.as_ref().and_then(|header| header.route_epoch) {
                    self.caches.record_route_epoch(&path, route_epoch);
                }
                if let Some(handle) = WriteHandleState::from_append_response(req, resp) {
                    self.caches.upsert_handle(handle);
                }
                refreshed
            }
            (RequestEnvelope::AddBlock(req), ResponseEnvelope::AddBlock(resp)) => {
                let Some(path) = request.path_hint(&self.caches.write_handles) else {
                    return resp.target.is_some();
                };
                let targets = resp.target.as_ref().into_iter().cloned().collect::<Vec<_>>();
                let (workers, worker_epoch) = workers_from_targets(&targets);
                self.caches.record_worker_info(&path, workers, worker_epoch);
                req.write_handle.as_ref().map(|h| h.handle_id != 0).unwrap_or(false) || resp.target.is_some()
            }
            (RequestEnvelope::GetStatus(_), ResponseEnvelope::GetStatus(resp)) => {
                resp.inode_id.is_some() || resp.attrs.is_some()
            }
            (RequestEnvelope::CommitFile(req), ResponseEnvelope::CommitFile(_)) => {
                if let Some(handle) = req.write_handle.as_ref() {
                    self.caches.remove_handle(handle.handle_id);
                }
                true
            }
            (RequestEnvelope::AbortFileWrite(req), ResponseEnvelope::AbortFileWrite(_)) => {
                if let Some(handle) = req.write_handle.as_ref() {
                    self.caches.remove_handle(handle.handle_id);
                }
                true
            }
            _ => false,
        }
    }

    async fn handle_need_refresh(
        &self,
        replay_policy: &ReplayPolicy,
        reason: RefreshReason,
        canonical: &CanonicalError,
        hint: RefreshHint,
        response_header: Option<ResponseHeaderProto>,
        request: &mut RequestEnvelope,
    ) -> ClientResult<()> {
        let _ = (replay_policy, canonical);

        match reason {
            RefreshReason::StaleState => self.refresh_state(hint, response_header.as_ref(), request).await,
            RefreshReason::NotLeader => self.refresh_leader(canonical, request).await,
            RefreshReason::MountEpochMismatch => self.refresh_mount(canonical, request).await,
            RefreshReason::RouteEpochMismatch => self.refresh_layout_context(canonical, request).await,
            RefreshReason::WorkerEpochMismatch => self.refresh_worker(canonical, request).await,
            RefreshReason::BlockStampMismatch => Err(self.unsupported_refresh_error(reason)),
            RefreshReason::Moved
            | RefreshReason::Fencing
            | RefreshReason::EpochMismatch
            | RefreshReason::SessionInvalid
            | RefreshReason::SessionExpired
            | RefreshReason::Unknown => Err(self.unsupported_refresh_error(reason)),
        }
    }

    fn unsupported_refresh_error(&self, reason: RefreshReason) -> ClientError {
        ClientError::Metadata(format!("unsupported FileSystemService refresh reason: {:?}", reason))
    }

    async fn refresh_state(
        &self,
        hint: RefreshHint,
        response_header: Option<&ResponseHeaderProto>,
        request: &RequestEnvelope,
    ) -> ClientResult<()> {
        let group_id = hint
            .group_id
            .or_else(|| {
                request
                    .header()
                    .and_then(|header| (header.group_id != 0).then_some(header.group_id))
            })
            .ok_or_else(|| ClientError::Metadata("state refresh is unavailable without group_id".to_string()))?;

        let required_state = response_header.and_then(|header| {
            header
                .state
                .iter()
                .find(|watermark| watermark.group_id.as_ref().map(|gid| gid.value) == Some(group_id))
                .and_then(|watermark| watermark.state_id)
        });

        let header_template = request.header().cloned().unwrap_or_else(default_request_header_proto);
        let request = MsyncRequestProto {
            header: Some(minimal_msync_header_proto(header_template, group_id)),
        };
        let response = self.rpc.msync(request).await.map_err(ClientError::from)?;

        match parse_rpc_envelope(Ok(()), response.header.as_ref()) {
            RpcEnvelope::Ok => {}
            RpcEnvelope::CanonicalError(canonical) => {
                return Err(self.refresh_canonical_error_for_msync(response.header.as_ref(), canonical));
            }
            RpcEnvelope::TransportError(status) => return Err(ClientError::from(status)),
        }

        let response_state = response
            .state
            .ok_or_else(|| ClientError::Metadata("MsyncResponseProto missing state".to_string()))?;
        let response_group = response_state
            .group_id
            .as_ref()
            .map(|group_id| group_id.value)
            .ok_or_else(|| ClientError::Metadata("MsyncResponseProto state missing group_id".to_string()))?;
        if response_group != group_id {
            return Err(ClientError::Metadata(format!(
                "MsyncResponseProto state group_id {} does not match requested group_id {}",
                response_group, group_id
            )));
        }
        let response_state_id = response_state
            .state_id
            .ok_or_else(|| ClientError::Metadata("MsyncResponseProto state missing state_id".to_string()))?;
        if let Some(required_state) = required_state {
            if !raft_log_id_has_reached(&response_state_id, &required_state) {
                return Err(ClientError::Metadata(format!(
                    "Msync response state not reached: current={:?}, required={:?}",
                    response_state_id, required_state
                )));
            }
        }
        self.caches.merge_response_state(&[GroupStateWatermarkProto {
            group_id: Some(ShardGroupIdProto { value: group_id }),
            state_id: Some(response_state_id),
        }]);

        Ok(())
    }

    fn refresh_canonical_error_for_msync(
        &self,
        header: Option<&ResponseHeaderProto>,
        canonical: CanonicalError,
    ) -> ClientError {
        if is_authz_denial(&canonical) {
            return ClientError::from(ClientAction::Fail {
                canonical: Box::new(canonical),
            });
        }
        match canonical.class {
            ErrorClass::NeedRefresh => {
                let reason = canonical
                    .reason
                    .unwrap_or_else(|| refresh_reason_from_code(canonical.code.clone()));
                ClientError::from(ClientAction::Refresh {
                    reason,
                    hint: self.build_refresh_hint_for_header(header, &canonical),
                    canonical: Box::new(canonical),
                })
            }
            ErrorClass::Retryable => ClientError::from(ClientAction::Retry {
                after_ms: canonical.retry_after_ms,
                canonical: Box::new(canonical),
            }),
            ErrorClass::Fatal => ClientError::from(ClientAction::Fail {
                canonical: Box::new(canonical),
            }),
            ErrorClass::Ok => ClientError::Metadata("msync returned canonical OK error state".to_string()),
        }
    }

    async fn refresh_leader(&self, canonical: &CanonicalError, request: &RequestEnvelope) -> ClientResult<()> {
        let candidate = canonical
            .refresh_hint
            .as_ref()
            .and_then(|hint| hint.leader_endpoint.clone())
            .or_else(|| self.caches.leader_endpoint())
            .or_else(|| self.next_metadata_endpoint());
        let mut refreshed = false;

        if let Some(endpoint) = candidate {
            let endpoint = normalize_endpoint(&endpoint);
            if self.rpc.current_endpoint().as_deref() != Some(endpoint.as_str()) {
                self.rpc.reconnect(&endpoint).await?;
                self.caches.set_leader_endpoint(endpoint.clone());
                refreshed = true;
            }
        }

        if let Some(path) = request.path_hint(&self.caches.write_handles) {
            self.refresh_layout_for_path(&path, request.header().cloned()).await?;
            refreshed = true;
        }

        if !refreshed {
            return Err(ClientError::Metadata(
                "leader refresh is unavailable without a new leader endpoint or path context".to_string(),
            ));
        }

        Ok(())
    }

    async fn refresh_mount(&self, canonical: &CanonicalError, request: &RequestEnvelope) -> ClientResult<()> {
        if let Some(path) = request.path_hint(&self.caches.write_handles) {
            if let Some(mount_epoch) = canonical.refresh_hint.as_ref().and_then(|hint| hint.mount_epoch) {
                self.caches.record_mount_epoch(&path, mount_epoch);
            }
            self.refresh_status_for_path(&path, request.header().cloned()).await
        } else {
            self.refresh_layout_context(canonical, request).await
        }
    }

    async fn refresh_layout_context(&self, canonical: &CanonicalError, request: &RequestEnvelope) -> ClientResult<()> {
        if let Some(path) = request.path_hint(&self.caches.write_handles) {
            if let Some(route_epoch) = canonical.refresh_hint.as_ref().and_then(|hint| hint.route_epoch) {
                self.caches.record_route_epoch(&path, route_epoch);
            }
            self.refresh_layout_for_path(&path, request.header().cloned()).await
        } else {
            Err(ClientError::Metadata(
                "route refresh is unavailable without path or session context".to_string(),
            ))
        }
    }

    async fn refresh_worker(&self, canonical: &CanonicalError, request: &RequestEnvelope) -> ClientResult<()> {
        if let Some(path) = request.path_hint(&self.caches.write_handles) {
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
            self.refresh_layout_for_path(&path, request.header().cloned()).await
        } else {
            Err(ClientError::Metadata(
                "worker refresh is unavailable without path or session context".to_string(),
            ))
        }
    }

    async fn refresh_status_for_path(
        &self,
        path: &str,
        header_template: Option<RequestHeaderProto>,
    ) -> ClientResult<()> {
        let request = RequestEnvelope::GetStatus(GetStatusRequestProto {
            header: header_template,
            path: path.to_string(),
        });
        self.run_best_effort_refresh(request).await
    }

    async fn refresh_layout_for_path(
        &self,
        path: &str,
        header_template: Option<RequestHeaderProto>,
    ) -> ClientResult<()> {
        let request = RequestEnvelope::GetBlockLocations(GetBlockLocationsRequestProto {
            header: header_template,
            target: Some(proto::metadata::get_block_locations_request_proto::Target::Path(
                path.to_string(),
            )),
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

        match parse_rpc_envelope(Ok(()), response.header()) {
            RpcEnvelope::Ok => {
                let header_refreshed = self.update_header_hints(&request, response.header());
                let payload_refreshed = self.update_payload_hints(&request, &response);
                let refreshed = header_refreshed || payload_refreshed;
                if refreshed {
                    Ok(())
                } else {
                    Err(ClientError::Metadata(format!(
                        "{} refresh did not update or confirm a concrete cache/state/layout/endpoint result",
                        response.op_name()
                    )))
                }
            }
            RpcEnvelope::CanonicalError(canonical) => {
                Err(self.refresh_canonical_error(&request, response.header(), canonical))
            }
            RpcEnvelope::TransportError(status) => Err(ClientError::from(status)),
        }
    }

    fn refresh_canonical_error(
        &self,
        request: &RequestEnvelope,
        header: Option<&ResponseHeaderProto>,
        canonical: CanonicalError,
    ) -> ClientError {
        if is_authz_denial(&canonical) {
            return ClientError::from(ClientAction::Fail {
                canonical: Box::new(canonical),
            });
        }

        match canonical.class {
            ErrorClass::NeedRefresh => {
                let reason = canonical
                    .reason
                    .unwrap_or_else(|| refresh_reason_from_code(canonical.code.clone()));
                ClientError::from(ClientAction::Refresh {
                    reason,
                    hint: self.build_refresh_hint(request, header, &canonical),
                    canonical: Box::new(canonical),
                })
            }
            ErrorClass::Retryable => ClientError::from(ClientAction::Retry {
                after_ms: canonical.retry_after_ms,
                canonical: Box::new(canonical),
            }),
            ErrorClass::Fatal => ClientError::from(ClientAction::Fail {
                canonical: Box::new(canonical),
            }),
            ErrorClass::Ok => ClientError::Metadata("refresh returned canonical OK error state".to_string()),
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
    GetStatus(GetStatusRequestProto),
    ListStatus(ListStatusRequestProto),
    CreateDirectory(CreateDirectoryRequestProto),
    Delete(DeleteRequestProto),
    Rename(RenameRequestProto),
    OpenFile(OpenFileRequestProto),
    GetBlockLocations(GetBlockLocationsRequestProto),
    CreateFile(CreateFileRequestProto),
    AppendFile(AppendFileRequestProto),
    AddBlock(AddBlockRequestProto),
    CommitFile(CommitFileRequestProto),
    AbortFileWrite(AbortFileWriteRequestProto),
    RenewLease(RenewLeaseRequestProto),
    Hflush(HflushRequestProto),
    Hsync(HsyncRequestProto),
    Msync(MsyncRequestProto),
}

impl RequestEnvelope {
    fn method(&self) -> FileSystemRpcMethod {
        match self {
            RequestEnvelope::GetStatus(_) => FileSystemRpcMethod::GetStatus,
            RequestEnvelope::ListStatus(_) => FileSystemRpcMethod::ListStatus,
            RequestEnvelope::CreateDirectory(_) => FileSystemRpcMethod::CreateDirectory,
            RequestEnvelope::Delete(_) => FileSystemRpcMethod::Delete,
            RequestEnvelope::Rename(_) => FileSystemRpcMethod::Rename,
            RequestEnvelope::OpenFile(_) => FileSystemRpcMethod::OpenFile,
            RequestEnvelope::GetBlockLocations(_) => FileSystemRpcMethod::GetBlockLocations,
            RequestEnvelope::CreateFile(_) => FileSystemRpcMethod::CreateFile,
            RequestEnvelope::AppendFile(_) => FileSystemRpcMethod::AppendFile,
            RequestEnvelope::AddBlock(_) => FileSystemRpcMethod::AddBlock,
            RequestEnvelope::CommitFile(_) => FileSystemRpcMethod::CommitFile,
            RequestEnvelope::AbortFileWrite(_) => FileSystemRpcMethod::AbortFileWrite,
            RequestEnvelope::RenewLease(_) => FileSystemRpcMethod::RenewLease,
            RequestEnvelope::Hflush(_) => FileSystemRpcMethod::Hflush,
            RequestEnvelope::Hsync(_) => FileSystemRpcMethod::Hsync,
            RequestEnvelope::Msync(_) => FileSystemRpcMethod::Msync,
        }
    }

    fn ensure_header_mut(&mut self) -> &mut RequestHeaderProto {
        match self {
            RequestEnvelope::GetStatus(req) => req.header.get_or_insert_with(default_request_header_proto),
            RequestEnvelope::ListStatus(req) => req.header.get_or_insert_with(default_request_header_proto),
            RequestEnvelope::CreateDirectory(req) => req.header.get_or_insert_with(default_request_header_proto),
            RequestEnvelope::Delete(req) => req.header.get_or_insert_with(default_request_header_proto),
            RequestEnvelope::Rename(req) => req.header.get_or_insert_with(default_request_header_proto),
            RequestEnvelope::OpenFile(req) => req.header.get_or_insert_with(default_request_header_proto),
            RequestEnvelope::GetBlockLocations(req) => req.header.get_or_insert_with(default_request_header_proto),
            RequestEnvelope::CreateFile(req) => req.header.get_or_insert_with(default_request_header_proto),
            RequestEnvelope::AppendFile(req) => req.header.get_or_insert_with(default_request_header_proto),
            RequestEnvelope::AddBlock(req) => req.header.get_or_insert_with(default_request_header_proto),
            RequestEnvelope::CommitFile(req) => req.header.get_or_insert_with(default_request_header_proto),
            RequestEnvelope::AbortFileWrite(req) => req.header.get_or_insert_with(default_request_header_proto),
            RequestEnvelope::RenewLease(req) => req.header.get_or_insert_with(default_request_header_proto),
            RequestEnvelope::Hflush(req) => req.header.get_or_insert_with(default_request_header_proto),
            RequestEnvelope::Hsync(req) => req.header.get_or_insert_with(default_request_header_proto),
            RequestEnvelope::Msync(req) => req.header.get_or_insert_with(default_request_header_proto),
        }
    }

    fn header(&self) -> Option<&RequestHeaderProto> {
        match self {
            RequestEnvelope::GetStatus(req) => req.header.as_ref(),
            RequestEnvelope::ListStatus(req) => req.header.as_ref(),
            RequestEnvelope::CreateDirectory(req) => req.header.as_ref(),
            RequestEnvelope::Delete(req) => req.header.as_ref(),
            RequestEnvelope::Rename(req) => req.header.as_ref(),
            RequestEnvelope::OpenFile(req) => req.header.as_ref(),
            RequestEnvelope::GetBlockLocations(req) => req.header.as_ref(),
            RequestEnvelope::CreateFile(req) => req.header.as_ref(),
            RequestEnvelope::AppendFile(req) => req.header.as_ref(),
            RequestEnvelope::AddBlock(req) => req.header.as_ref(),
            RequestEnvelope::CommitFile(req) => req.header.as_ref(),
            RequestEnvelope::AbortFileWrite(req) => req.header.as_ref(),
            RequestEnvelope::RenewLease(req) => req.header.as_ref(),
            RequestEnvelope::Hflush(req) => req.header.as_ref(),
            RequestEnvelope::Hsync(req) => req.header.as_ref(),
            RequestEnvelope::Msync(req) => req.header.as_ref(),
        }
    }

    fn path_hint(&self, write_handles: &DashMap<u64, WriteHandleState>) -> Option<String> {
        match self {
            RequestEnvelope::GetStatus(req) => Some(req.path.clone()),
            RequestEnvelope::ListStatus(req) => Some(req.path.clone()),
            RequestEnvelope::CreateDirectory(req) => Some(req.path.clone()),
            RequestEnvelope::Delete(req) => Some(req.path.clone()),
            RequestEnvelope::Rename(req) => Some(req.src_path.clone()),
            RequestEnvelope::OpenFile(req) => Some(req.path.clone()),
            RequestEnvelope::GetBlockLocations(req) => req_path_from_locations_request(req),
            RequestEnvelope::CreateFile(req) => Some(req.path.clone()),
            RequestEnvelope::AppendFile(req) => Some(req.path.clone()),
            RequestEnvelope::AddBlock(req) => req
                .write_handle
                .as_ref()
                .and_then(|h| write_handles.get(&h.handle_id).map(|s| s.path.clone())),
            RequestEnvelope::CommitFile(req) => req
                .write_handle
                .as_ref()
                .and_then(|h| write_handles.get(&h.handle_id).map(|s| s.path.clone())),
            RequestEnvelope::AbortFileWrite(req) => req
                .write_handle
                .as_ref()
                .and_then(|h| write_handles.get(&h.handle_id).map(|s| s.path.clone())),
            RequestEnvelope::RenewLease(req) => req
                .write_handle
                .as_ref()
                .and_then(|h| write_handles.get(&h.handle_id).map(|s| s.path.clone())),
            RequestEnvelope::Hflush(req) => req
                .write_handle
                .as_ref()
                .and_then(|h| write_handles.get(&h.handle_id).map(|s| s.path.clone())),
            RequestEnvelope::Hsync(req) => req
                .write_handle
                .as_ref()
                .and_then(|h| write_handles.get(&h.handle_id).map(|s| s.path.clone())),
            RequestEnvelope::Msync(_) => None,
        }
    }
}

#[derive(Clone, Debug)]
enum ResponseEnvelope {
    GetStatus(GetStatusResponseProto),
    ListStatus(ListStatusResponseProto),
    CreateDirectory(CreateDirectoryResponseProto),
    Msync(MsyncResponseProto),
    Delete(DeleteResponseProto),
    Rename(RenameResponseProto),
    OpenFile(OpenFileResponseProto),
    GetBlockLocations(GetBlockLocationsResponseProto),
    CreateFile(CreateFileResponseProto),
    AppendFile(AppendFileResponseProto),
    AddBlock(AddBlockResponseProto),
    CommitFile(CommitFileResponseProto),
    AbortFileWrite(AbortFileWriteResponseProto),
    RenewLease(RenewLeaseResponseProto),
    Hflush(HflushResponseProto),
    Hsync(HsyncResponseProto),
}

impl ResponseEnvelope {
    fn header(&self) -> Option<&ResponseHeaderProto> {
        match self {
            ResponseEnvelope::GetStatus(resp) => resp.header.as_ref(),
            ResponseEnvelope::ListStatus(resp) => resp.header.as_ref(),
            ResponseEnvelope::CreateDirectory(resp) => resp.header.as_ref(),
            ResponseEnvelope::Msync(resp) => resp.header.as_ref(),
            ResponseEnvelope::Delete(resp) => resp.header.as_ref(),
            ResponseEnvelope::Rename(resp) => resp.header.as_ref(),
            ResponseEnvelope::OpenFile(resp) => resp.header.as_ref(),
            ResponseEnvelope::GetBlockLocations(resp) => resp.header.as_ref(),
            ResponseEnvelope::CreateFile(resp) => resp.header.as_ref(),
            ResponseEnvelope::AppendFile(resp) => resp.header.as_ref(),
            ResponseEnvelope::AddBlock(resp) => resp.header.as_ref(),
            ResponseEnvelope::CommitFile(resp) => resp.header.as_ref(),
            ResponseEnvelope::AbortFileWrite(resp) => resp.header.as_ref(),
            ResponseEnvelope::RenewLease(resp) => resp.header.as_ref(),
            ResponseEnvelope::Hflush(resp) => resp.header.as_ref(),
            ResponseEnvelope::Hsync(resp) => resp.header.as_ref(),
        }
    }

    fn op_name(&self) -> &'static str {
        match self {
            ResponseEnvelope::GetStatus(_) => "GetStatus",
            ResponseEnvelope::ListStatus(_) => "ListStatus",
            ResponseEnvelope::CreateDirectory(_) => "CreateDirectory",
            ResponseEnvelope::Msync(_) => "Msync",
            ResponseEnvelope::Delete(_) => "Delete",
            ResponseEnvelope::Rename(_) => "Rename",
            ResponseEnvelope::OpenFile(_) => "OpenFile",
            ResponseEnvelope::GetBlockLocations(_) => "GetBlockLocations",
            ResponseEnvelope::CreateFile(_) => "CreateFile",
            ResponseEnvelope::AppendFile(_) => "AppendFile",
            ResponseEnvelope::AddBlock(_) => "AddBlock",
            ResponseEnvelope::CommitFile(_) => "CommitFile",
            ResponseEnvelope::AbortFileWrite(_) => "AbortFileWrite",
            ResponseEnvelope::RenewLease(_) => "RenewLease",
            ResponseEnvelope::Hflush(_) => "Hflush",
            ResponseEnvelope::Hsync(_) => "Hsync",
        }
    }
}

async fn execute_request_on_rpc(
    rpc: Arc<dyn FileSystemRpc>,
    request: RequestEnvelope,
) -> Result<ResponseEnvelope, tonic::Status> {
    match request {
        RequestEnvelope::GetStatus(req) => rpc.get_status(req).await.map(ResponseEnvelope::GetStatus),
        RequestEnvelope::ListStatus(req) => rpc.list_status(req).await.map(ResponseEnvelope::ListStatus),
        RequestEnvelope::CreateDirectory(req) => rpc.create_directory(req).await.map(ResponseEnvelope::CreateDirectory),
        RequestEnvelope::Delete(req) => rpc.delete(req).await.map(ResponseEnvelope::Delete),
        RequestEnvelope::Rename(req) => rpc.rename(req).await.map(ResponseEnvelope::Rename),
        RequestEnvelope::OpenFile(req) => rpc.open_file(req).await.map(ResponseEnvelope::OpenFile),
        RequestEnvelope::GetBlockLocations(req) => rpc
            .get_block_locations(req)
            .await
            .map(ResponseEnvelope::GetBlockLocations),
        RequestEnvelope::CreateFile(req) => rpc.create_file(req).await.map(ResponseEnvelope::CreateFile),
        RequestEnvelope::AppendFile(req) => rpc.append_file(req).await.map(ResponseEnvelope::AppendFile),
        RequestEnvelope::AddBlock(req) => rpc.add_block(req).await.map(ResponseEnvelope::AddBlock),
        RequestEnvelope::CommitFile(req) => rpc.commit_file(req).await.map(ResponseEnvelope::CommitFile),
        RequestEnvelope::AbortFileWrite(req) => rpc.abort_file_write(req).await.map(ResponseEnvelope::AbortFileWrite),
        RequestEnvelope::RenewLease(req) => rpc.renew_lease(req).await.map(ResponseEnvelope::RenewLease),
        RequestEnvelope::Hflush(req) => rpc.hflush(req).await.map(ResponseEnvelope::Hflush),
        RequestEnvelope::Hsync(req) => rpc.hsync(req).await.map(ResponseEnvelope::Hsync),
        RequestEnvelope::Msync(req) => rpc.msync(req).await.map(ResponseEnvelope::Msync),
    }
}

#[derive(Clone, Debug)]
struct WriteHandleState {
    path: String,
    handle: WriteHandleProto,
}

impl WriteHandleState {
    fn from_create_response(request: &CreateFileRequestProto, response: &CreateFileResponseProto) -> Option<Self> {
        Some(Self {
            path: request.path.clone(),
            handle: response.write_handle?,
        })
    }

    fn from_append_response(request: &AppendFileRequestProto, response: &AppendFileResponseProto) -> Option<Self> {
        Some(Self {
            path: request.path.clone(),
            handle: response.write_handle?,
        })
    }
}

#[derive(Default)]
struct ActionCaches {
    leader_endpoint: RwLock<Option<String>>,
    mount_epoch_prefix: DashMap<String, u64>,
    route_epoch_by_path: DashMap<String, u64>,
    worker_epoch_by_path: DashMap<String, u64>,
    state_by_group: DashMap<u64, RaftLogIdProto>,
    worker_endpoints_by_path: DashMap<String, Vec<WorkerEndpointInfoProto>>,
    write_handles: DashMap<u64, WriteHandleState>,
}

impl ActionCaches {
    fn set_leader_endpoint(&self, endpoint: String) {
        *self.leader_endpoint.write() = Some(endpoint);
    }

    fn leader_endpoint(&self) -> Option<String> {
        self.leader_endpoint.read().clone()
    }

    fn state_for_group(&self, group_id: u64) -> Option<RaftLogIdProto> {
        self.state_by_group.get(&group_id).map(|entry| *entry)
    }

    fn merge_response_state(&self, state: &[GroupStateWatermarkProto]) {
        for watermark in state {
            let Some(group_id) = watermark.group_id.as_ref().map(|group_id| group_id.value) else {
                continue;
            };
            let Some(new_state) = watermark.state_id else {
                continue;
            };
            let should_update = self
                .state_by_group
                .get(&group_id)
                .map(|old_state| raft_log_id_is_ahead(&new_state, &old_state))
                .unwrap_or(true);
            if should_update {
                self.state_by_group.insert(group_id, new_state);
            }
        }
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

    fn upsert_handle(&self, handle: WriteHandleState) {
        self.write_handles.insert(handle.handle.handle_id, handle);
    }

    fn remove_handle(&self, file_handle: u64) {
        self.write_handles.remove(&file_handle);
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

fn req_path_from_locations_request(request: &GetBlockLocationsRequestProto) -> Option<String> {
    match request.target.as_ref()? {
        proto::metadata::get_block_locations_request_proto::Target::Path(path) => Some(path.clone()),
        _ => None,
    }
}

fn workers_from_locations(
    locations: &[proto::metadata::FileBlockLocationProto],
) -> (Vec<WorkerEndpointInfoProto>, Option<u64>) {
    let mut by_worker: HashMap<u64, WorkerEndpointInfoProto> = HashMap::new();
    let mut max_epoch: Option<u64> = None;

    for location in locations {
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

fn workers_from_targets(targets: &[WriteTargetProto]) -> (Vec<WorkerEndpointInfoProto>, Option<u64>) {
    let mut by_worker: HashMap<u64, WorkerEndpointInfoProto> = HashMap::new();
    let mut max_epoch: Option<u64> = None;

    for target in targets {
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
        state: Vec::new(),
        retry_count: 0,
        route_epoch: None,
        principal: String::new(),
        real_user: String::new(),
        doas: String::new(),
        authn_type: proto::common::AuthnTypeProto::Unspecified as i32,
    }
}

fn raft_log_id_is_ahead(new_state: &RaftLogIdProto, old_state: &RaftLogIdProto) -> bool {
    (new_state.index, new_state.term, new_state.leader_node_id)
        > (old_state.index, old_state.term, old_state.leader_node_id)
}

fn raft_log_id_has_reached(current: &RaftLogIdProto, required: &RaftLogIdProto) -> bool {
    current == required || raft_log_id_is_ahead(current, required)
}

fn minimal_msync_header_proto(mut header: RequestHeaderProto, group_id: u64) -> RequestHeaderProto {
    header.group_id = group_id;
    header.state.clear();
    header.mount_epoch = None;
    header.route_epoch = None;
    header
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
    use proto::metadata::CreateDispositionProto;
    use std::sync::Mutex;

    #[test]
    fn replay_policy_table_is_complete() {
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
        }
    }

    #[test]
    fn session_errors_are_not_reopenable() {
        let fatal = CanonicalError {
            class: ErrorClass::Fatal,
            code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Fencing)),
            reason: Some(RefreshReason::SessionInvalid),
            retry_after_ms: None,
            message: "invalid write handle".to_string(),
            refresh_hint: None,
        };
        let err = ClientError::from(ClientAction::Fail {
            canonical: Box::new(fatal),
        });
        match err {
            ClientError::Action(action) => {
                let ClientAction::Fail { canonical } = action.as_ref() else {
                    panic!("expected terminal fail, got {:?}", action);
                };
                assert_eq!(canonical.reason, Some(RefreshReason::SessionInvalid));
            }
            other => panic!("expected terminal fail, got {:?}", other),
        }
    }

    #[test]
    fn shard_moved_maps_to_route_refresh() {
        let reason = refresh_reason_from_code(Some(CanonicalErrorCode::RpcCode(RpcErrorCode::ShardMoved)));
        assert_eq!(reason, RefreshReason::RouteEpochMismatch);
    }

    struct RetryCallIdRpc {
        attempts: AtomicUsize,
        call_ids: Mutex<Vec<String>>,
    }

    impl RetryCallIdRpc {
        fn new() -> Self {
            Self {
                attempts: AtomicUsize::new(0),
                call_ids: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl FileSystemRpc for RetryCallIdRpc {
        async fn get_status(&self, _request: GetStatusRequestProto) -> Result<GetStatusResponseProto, tonic::Status> {
            Err(tonic::Status::unimplemented("get_status"))
        }

        async fn list_status(
            &self,
            _request: ListStatusRequestProto,
        ) -> Result<ListStatusResponseProto, tonic::Status> {
            Err(tonic::Status::unimplemented("list_status"))
        }

        async fn create_directory(
            &self,
            _request: CreateDirectoryRequestProto,
        ) -> Result<CreateDirectoryResponseProto, tonic::Status> {
            Err(tonic::Status::unimplemented("create_directory"))
        }

        async fn delete(&self, _request: DeleteRequestProto) -> Result<DeleteResponseProto, tonic::Status> {
            Err(tonic::Status::unimplemented("delete"))
        }

        async fn rename(&self, _request: RenameRequestProto) -> Result<RenameResponseProto, tonic::Status> {
            Err(tonic::Status::unimplemented("rename"))
        }

        async fn open_file(&self, _request: OpenFileRequestProto) -> Result<OpenFileResponseProto, tonic::Status> {
            Err(tonic::Status::unimplemented("open_file"))
        }

        async fn get_block_locations(
            &self,
            _request: GetBlockLocationsRequestProto,
        ) -> Result<GetBlockLocationsResponseProto, tonic::Status> {
            Err(tonic::Status::unimplemented("get_block_locations"))
        }

        async fn create_file(&self, request: CreateFileRequestProto) -> Result<CreateFileResponseProto, tonic::Status> {
            let call_id = request
                .header
                .as_ref()
                .and_then(|header| header.client.as_ref())
                .map(|client| client.call_id.clone())
                .unwrap_or_default();
            self.call_ids.lock().unwrap().push(call_id);

            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                let canonical = CanonicalError::retryable(RpcErrorCode::NodeUnavailable, Some(0), "retry once");
                return Ok(CreateFileResponseProto {
                    header: Some(ResponseHeaderProto {
                        error: Some(proto::convert::canonical_to_error_detail(&canonical)),
                        ..Default::default()
                    }),
                    ..Default::default()
                });
            }

            Ok(CreateFileResponseProto {
                header: Some(ResponseHeaderProto::default()),
                ..Default::default()
            })
        }

        async fn append_file(
            &self,
            _request: AppendFileRequestProto,
        ) -> Result<AppendFileResponseProto, tonic::Status> {
            Err(tonic::Status::unimplemented("append_file"))
        }

        async fn add_block(&self, _request: AddBlockRequestProto) -> Result<AddBlockResponseProto, tonic::Status> {
            Err(tonic::Status::unimplemented("add_block"))
        }

        async fn commit_file(
            &self,
            _request: CommitFileRequestProto,
        ) -> Result<CommitFileResponseProto, tonic::Status> {
            Err(tonic::Status::unimplemented("commit_file"))
        }

        async fn abort_file_write(
            &self,
            _request: AbortFileWriteRequestProto,
        ) -> Result<AbortFileWriteResponseProto, tonic::Status> {
            Err(tonic::Status::unimplemented("abort_file_write"))
        }

        async fn renew_lease(
            &self,
            _request: RenewLeaseRequestProto,
        ) -> Result<RenewLeaseResponseProto, tonic::Status> {
            Err(tonic::Status::unimplemented("renew_lease"))
        }

        async fn hflush(&self, _request: HflushRequestProto) -> Result<HflushResponseProto, tonic::Status> {
            Err(tonic::Status::unimplemented("hflush"))
        }

        async fn hsync(&self, _request: HsyncRequestProto) -> Result<HsyncResponseProto, tonic::Status> {
            Err(tonic::Status::unimplemented("hsync"))
        }

        async fn msync(&self, _request: MsyncRequestProto) -> Result<MsyncResponseProto, tonic::Status> {
            Err(tonic::Status::unimplemented("msync"))
        }
    }

    #[tokio::test]
    async fn retry_keeps_call_id() {
        let rpc = Arc::new(RetryCallIdRpc::new());
        let machine = ActionMachine::new(Arc::clone(&rpc) as Arc<dyn FileSystemRpc>, Vec::new()).with_policy(
            ActionMachinePolicy {
                max_refresh_attempts: 0,
                max_retryable_attempts: 1,
                max_transport_retries: 0,
                base_backoff_ms: 0,
            },
        );

        machine
            .call_with_refresh(
                ReplayPolicy::Mutation,
                RpcOp::create_file(CreateFileRequestProto {
                    path: "/retry".to_string(),
                    disposition: CreateDispositionProto::CreateNew as i32,
                    ..Default::default()
                }),
            )
            .await
            .expect("retry should eventually succeed");

        let call_ids = rpc.call_ids.lock().unwrap();
        assert_eq!(call_ids.len(), 2);
        assert!(!call_ids[0].is_empty());
        assert_eq!(call_ids[0], call_ids[1]);
    }
}
