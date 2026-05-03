// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Header type definitions.

use crate::{
    error::canonical::{CanonicalError, ErrorClass as CanonicalErrorClass, ErrorCode as CanonicalErrorCode},
    time::Deadline,
};
use types::{CallId, ClientId, GroupStateWatermark};

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
    /// Client-required state-machine applied watermarks.
    ///
    /// Each watermark is scoped by metadata Raft owner group. Empty means the
    /// request has no state freshness requirement.
    pub state: Vec<GroupStateWatermark>,
    /// Optional retry count from client perspective (0 = first attempt).
    pub retry_count: i32,
    /// Optional route epoch observed by client.
    pub route_epoch: Option<u64>,
    /// Authenticated principal/user identity.
    ///
    /// Required when ACL authorization mode is enabled.
    pub principal: Option<String>,
    /// Real user identity (proxy-user scenarios; reserved for future use).
    pub real_user: Option<String>,
    /// Proxy/doAs target user (reserved for future use).
    pub doas: Option<String>,
    /// Authentication type marker for the request.
    pub authn_type: AuthnType,
}

/// Human-oriented context for auditing and lightweight diagnostics.
#[derive(Clone, Debug)]
pub struct CallerContext {
    /// Example: "type=spark,job=42"
    pub context: String,
    /// Optional signature for tamper detection or provenance (opaque).
    pub signature: Option<Vec<u8>>,
}

/// Authentication type marker for request identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthnType {
    Unspecified,
    Simple,
    Kerberos,
    Token,
}

impl Default for AuthnType {
    fn default() -> Self {
        Self::Unspecified
    }
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
    /// Server-authorized client state cache updates.
    ///
    /// Leaders and msync may return non-empty state. Follower successful
    /// responses must leave this empty. Empty means no cache update, not stale.
    pub state: Vec<GroupStateWatermark>,
    /// Group ID that this response applies to (required for metadata-plane RPCs).
    /// Server must echo back the actual group_id that processed this request.
    pub group_id: Option<u64>,
    /// Mount epoch returned by server (for FS operations).
    /// Server fills this with the current mount.mount_epoch so client can update its cache.
    pub mount_epoch: Option<u64>,
    /// Route epoch returned by server (for FS route/layout operations).
    pub route_epoch: Option<u64>,
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
            state: Vec::new(),
            retry_count: 0,
            group_id: None,
            mount_epoch: None,
            route_epoch: None,
            principal: None,
            real_user: None,
            doas: None,
            authn_type: AuthnType::Unspecified,
        }
    }

    /// Set the state watermark vector for consistency checking.
    pub fn with_state(mut self, state: Vec<GroupStateWatermark>) -> Self {
        self.state = state;
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

    /// Set the authenticated principal.
    pub fn with_principal(mut self, principal: impl Into<String>) -> Self {
        self.principal = Some(principal.into());
        self
    }

    /// Set the real user identity.
    pub fn with_real_user(mut self, real_user: impl Into<String>) -> Self {
        self.real_user = Some(real_user.into());
        self
    }

    /// Set the doAs/proxy user identity.
    pub fn with_doas(mut self, doas: impl Into<String>) -> Self {
        self.doas = Some(doas.into());
        self
    }

    /// Set the request authentication type marker.
    pub fn with_authn_type(mut self, authn_type: AuthnType) -> Self {
        self.authn_type = authn_type;
        self
    }

    /// Create a child header (for nested calls).
    ///
    /// Inherits client_id, deadline, traceparent, state watermarks, and group_id.
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
            state: self.state.clone(),
            retry_count: 0,
            group_id: self.group_id,
            mount_epoch: self.mount_epoch,
            route_epoch: self.route_epoch,
            principal: self.principal.clone(),
            real_user: self.real_user.clone(),
            doas: self.doas.clone(),
            authn_type: self.authn_type,
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
            state: self.state.clone(),
            retry_count: self.retry_count + 1,
            group_id: self.group_id,
            mount_epoch: self.mount_epoch,
            route_epoch: self.route_epoch,
            principal: self.principal.clone(),
            real_user: self.real_user.clone(),
            doas: self.doas.clone(),
            authn_type: self.authn_type,
        }
    }

    /// Convert RequestHeader to gRPC metadata for propagation.
    ///
    /// This function creates metadata entries for:
    /// - x-call-id: Call ID (UUID string)
    /// - x-client-id: Client ID (u64 as string)
    /// - x-state-id: Group state watermarks as group:term:leader_node_id:index entries
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
    /// - x-state-id: Group state watermarks as group:term:leader_node_id:index entries
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
            state: Vec::new(),
            group_id: None,
            mount_epoch: None,
            route_epoch: None,
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
            state: Vec::new(),
            group_id: None,
            mount_epoch: None,
            route_epoch: None,
        }
    }

    /// Set the state watermark vector.
    pub fn with_state(mut self, state: Vec<GroupStateWatermark>) -> Self {
        self.state = state;
        self
    }

    /// Set the group ID.
    pub fn with_group_id(mut self, group_id: u64) -> Self {
        self.group_id = Some(group_id);
        self
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
