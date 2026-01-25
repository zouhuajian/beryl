// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Header type definitions.

use crate::{
    error::canonical::{CanonicalError, ErrorClass as CanonicalErrorClass, ErrorCode as CanonicalErrorCode},
    time::Deadline,
};
use types::{CallId, ClientId, RaftLogId};

/// Client information for correlation and routing.
#[derive(Clone, Debug)]
pub struct ClientInfo {
    /// Unique identifier for this call.
    pub call_id: CallId,
    /// Client identifier.
    pub client_id: ClientId,
    /// Optional client name for diagnostics.
    pub client_name: Option<String>,
}

/// Request header carried with every RPC request.
#[derive(Clone, Debug)]
pub struct RequestHeader {
    /// Client information (call_id, client_id, client_name).
    pub client: ClientInfo,
    /// Group ID for this request (required for metadata-plane RPCs).
    /// Client must fill this field for all metadata RPCs.
    pub group_id: Option<u64>,
    /// Mount epoch for FS write operations.
    /// Client should provide the mount_epoch it knows for the mount being accessed.
    /// Server validates this against current mount.mount_epoch and returns NEED_REFRESH if mismatch.
    pub mount_epoch: Option<u64>,
    /// Absolute deadline (Unix epoch milliseconds).
    pub deadline: Deadline,
    /// W3C Trace Context: traceparent header value.
    pub traceparent: Option<String>,
    /// Optional caller context for auditing/diagnostics.
    pub caller_context: Option<CallerContext>,
    /// Optional state ID for consistency checking (read gating) and routing.
    pub state_id: Option<RaftLogId>,
    /// Optional retry count from client perspective (0 = first attempt).
    pub retry_count: i32,
}

/// Human-oriented context for auditing and lightweight diagnostics.
#[derive(Clone, Debug)]
pub struct CallerContext {
    /// Example: "type=spark,job=42"
    pub context: String,
    /// Optional signature for tamper detection or provenance (opaque).
    pub signature: Option<Vec<u8>>,
}

/// Response header carried with every RPC response.
#[derive(Clone, Debug)]
pub struct ResponseHeader {
    /// Client information (call_id, client_id, client_name).
    pub client: ClientInfo,
    /// High-level status for the RPC outcome.
    pub status: RpcStatus,
    /// Canonical error detail (single source of truth for error semantics).
    pub canonical_error: Option<CanonicalError>,
    /// Client-visible state watermark returned by the server.
    /// This is the latest state the client has "seen" from the server (typically equals last_applied_log_id),
    /// and should be used to advance the client's consistency watermark for subsequent requests.
    pub state_id: Option<RaftLogId>,
    /// Group ID that this response applies to (required for metadata-plane RPCs).
    /// Server must echo back the actual group_id that processed this request.
    pub group_id: Option<u64>,
    /// Mount epoch returned by server (for FS operations).
    /// Server fills this with the current mount.mount_epoch so client can update its cache.
    pub mount_epoch: Option<u64>,
}

/// High-level status for the RPC outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RpcStatus {
    /// RPC succeeded.
    Ok,
    /// RPC failed with a recoverable error (business/protocol error).
    /// Client should check ResponseHeader.canonical_error for details.
    Error,
    /// RPC failed with a fatal error (unrecoverable).
    Fatal,
}

/// Structured error details.
#[derive(Clone, Debug)]
pub struct RpcError {
    /// Error code.
    pub code: RpcErrorCode,
    /// Short message (no stack traces).
    pub message: String,
    /// Optional error type/category.
    pub error_type: Option<String>,
    /// Optional retry hints.
    pub retryable: bool,
    pub retry_after_ms: Option<u64>,
}

/// Error code enumeration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RpcErrorCode {
    Unspecified,
    // Framework / protocol
    NoSuchMethod,
    InvalidHeader,
    VersionMismatch,
    DeserializeRequest,
    SerializeResponse,
    // Auth / permission
    Unauthenticated,
    PermissionDenied,
    // Routing / topology / raft
    NotLeader,
    StaleState,
    MountEpochMismatch,
    RouteEpochMismatch,
    WorkerEpochMismatch,
    BlockStampMismatch,
    EpochMismatch,
    Fencing,
    ShardMoved,
    NodeUnavailable,
    // Application
    Application,
}

impl ClientInfo {
    /// Create a new ClientInfo with a client ID.
    pub fn new(client_id: impl Into<ClientId>) -> Self {
        Self {
            call_id: CallId::new(),
            client_id: client_id.into(),
            client_name: None,
        }
    }

    /// Set the client name.
    pub fn with_client_name(mut self, client_name: String) -> Self {
        self.client_name = Some(client_name);
        self
    }
}

impl RequestHeader {
    /// Create a new RequestHeader with a client ID.
    pub fn new(client_id: impl Into<ClientId>) -> Self {
        Self::with_deadline(client_id, Deadline::from_now(std::time::Duration::from_secs(30)))
    }

    /// Create a new RequestHeader with a client ID and deadline.
    pub fn with_deadline(client_id: impl Into<ClientId>, deadline: Deadline) -> Self {
        Self {
            client: ClientInfo::new(client_id),
            deadline,
            traceparent: None,
            caller_context: None,
            state_id: None,
            retry_count: 0,
            group_id: None,
            mount_epoch: None,
        }
    }

    /// Set the state ID for consistency checking.
    pub fn with_state_id(mut self, state_id: RaftLogId) -> Self {
        self.state_id = Some(state_id);
        self
    }

    /// Set the group ID.
    pub fn with_group_id(mut self, group_id: u64) -> Self {
        self.group_id = Some(group_id);
        self
    }

    /// Set the traceparent.
    pub fn with_traceparent(mut self, traceparent: String) -> Self {
        self.traceparent = Some(traceparent);
        self
    }

    /// Set the caller context.
    pub fn with_caller_context(mut self, caller_context: CallerContext) -> Self {
        self.caller_context = Some(caller_context);
        self
    }

    /// Set the retry count.
    pub fn with_retry_count(mut self, retry_count: i32) -> Self {
        self.retry_count = retry_count;
        self
    }

    /// Create a child header (for nested calls).
    ///
    /// Inherits client_id, deadline, traceparent, state_id, and group_id.
    /// Generates a new call_id by default.
    pub fn child(&self) -> Self {
        Self {
            client: ClientInfo {
                call_id: CallId::new(),
                client_id: self.client.client_id,
                client_name: self.client.client_name.clone(),
            },
            deadline: self.deadline,
            traceparent: self.traceparent.clone(),
            caller_context: self.caller_context.clone(),
            state_id: self.state_id,
            retry_count: 0,
            group_id: self.group_id,
            mount_epoch: self.mount_epoch,
        }
    }

    /// Create a child header with the same call_id (for retries).
    pub fn child_with_same_call_id(&self) -> Self {
        Self {
            client: ClientInfo {
                call_id: self.client.call_id,
                client_id: self.client.client_id,
                client_name: self.client.client_name.clone(),
            },
            deadline: self.deadline,
            traceparent: self.traceparent.clone(),
            caller_context: self.caller_context.clone(),
            state_id: self.state_id,
            retry_count: self.retry_count + 1,
            group_id: self.group_id,
            mount_epoch: self.mount_epoch,
        }
    }

    /// Convert RequestHeader to gRPC metadata for propagation.
    ///
    /// This function creates metadata entries for:
    /// - x-call-id: Call ID (UUID string)
    /// - x-client-id: Client ID (u64 as string)
    /// - x-state-id: State ID (term:leader_node_id:index as string, if present)
    /// - traceparent: W3C Trace Context (if present)
    /// - grpc-timeout: Deadline as gRPC timeout format (e.g., "30S")
    ///
    /// Returns a vector of (key, value) pairs suitable for use with tonic::metadata::MetadataMap.
    pub fn to_grpc_metadata(&self) -> Vec<(String, String)> {
        use crate::header::RequestHeaderCodec;
        RequestHeaderCodec::encode_to_headers(self)
    }

    /// Parse RequestHeader from gRPC metadata.
    ///
    /// This function extracts:
    /// - x-call-id: Call ID
    /// - x-client-id: Client ID
    /// - x-state-id: State ID (term:leader_node_id:index)
    /// - traceparent: W3C Trace Context
    /// - grpc-timeout: Deadline (converted from timeout to absolute deadline)
    ///
    /// Missing fields are filled with defaults (generates new call_id, uses unknown client_id).
    pub fn from_grpc_metadata<I>(iter: I) -> Self
    where
        I: Iterator<Item = (String, String)>,
    {
        use crate::header::RequestHeaderCodec;
        RequestHeaderCodec::decode_from_headers(iter)
    }
}

impl ResponseHeader {
    /// Create a successful response header.
    pub fn ok(client: ClientInfo) -> Self {
        Self {
            client,
            status: RpcStatus::Ok,
            canonical_error: None,
            state_id: None,
            group_id: None,
            mount_epoch: None,
        }
    }

    /// Create an error response header from canonical error.
    pub fn error(client: ClientInfo, canonical_error: CanonicalError) -> Self {
        Self::from_canonical(client, canonical_error)
    }

    /// Create an error response header from canonical error.
    pub fn from_canonical(client: ClientInfo, canonical_error: CanonicalError) -> Self {
        debug_assert!(
            !matches!(canonical_error.class, CanonicalErrorClass::Ok),
            "ResponseHeader::from_canonical must not be called with Ok class; use ResponseHeader::ok instead"
        );
        let status = match canonical_error.class {
            CanonicalErrorClass::Ok => RpcStatus::Ok,
            CanonicalErrorClass::NeedRefresh | CanonicalErrorClass::Retryable => RpcStatus::Error,
            CanonicalErrorClass::Fatal => RpcStatus::Fatal,
        };
        debug_assert!(
            status == RpcStatus::Ok || !matches!(canonical_error.class, CanonicalErrorClass::Ok),
            "status and canonical_error.class must align: Ok => None, non-Ok => Some"
        );
        Self {
            client,
            status,
            canonical_error: Some(canonical_error),
            state_id: None,
            group_id: None,
            mount_epoch: None,
        }
    }

    /// Set the state ID.
    pub fn with_state_id(mut self, state_id: RaftLogId) -> Self {
        self.state_id = Some(state_id);
        self
    }

    /// Set the group ID.
    pub fn with_group_id(mut self, group_id: u64) -> Self {
        self.group_id = Some(group_id);
        self
    }

    /// Derived legacy error view for compatibility/logging only.
    ///
    /// NOTE: This is derived from `canonical_error` and must NOT be used for control flow.
    /// Control flow must read `canonical_error` directly as the single source of truth.
    #[deprecated(note = "legacy_error is for logging only; control flow must read canonical_error")]
    pub fn legacy_error(&self) -> Option<RpcError> {
        self.canonical_error
            .as_ref()
            .and_then(|canonical| Self::rpc_error_from_canonical(canonical))
    }

    fn rpc_error_from_canonical(canonical_error: &CanonicalError) -> Option<RpcError> {
        if matches!(canonical_error.class, CanonicalErrorClass::Ok) {
            return None;
        }
        let code = match &canonical_error.code {
            Some(CanonicalErrorCode::RpcCode(code)) => *code,
            // Fs errno and missing codes fall back to Application for backward compatibility.
            _ => RpcErrorCode::Application,
        };
        Some(RpcError {
            code,
            message: canonical_error.message.clone(),
            error_type: None,
            retryable: matches!(canonical_error.class, CanonicalErrorClass::Retryable),
            retry_after_ms: canonical_error.retry_after_ms,
        })
    }
}
