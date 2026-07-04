// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Header type definitions.

use crate::{
    error::canonical::{CanonicalError, ErrorClass as CanonicalErrorClass},
    time::Deadline,
};
use types::{CallId, ClientId, GroupName, GroupStateWatermark};

/// Client information for correlation and routing.
#[derive(Clone, Debug)]
pub struct ClientInfo {
    /// Unique identifier for this call.
    pub call_id: CallId,
    /// Internal client runtime identity.
    pub client_id: ClientId,
    /// Optional client name for diagnostics.
    pub client_name: Option<String>,
}

/// Basic request/response identity parsed from a header.
///
/// This is only the client/call/group shape. Freshness and replay validation
/// remain owned by route epoch, mount epoch, state watermark, and fingerprints.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeaderIdentity {
    pub call_id: CallId,
    pub client_id: ClientId,
    pub group_name: Option<GroupName>,
}

/// W3C trace propagation context.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TraceContext {
    /// W3C Trace Context: traceparent header value.
    pub traceparent: Option<String>,
    /// W3C Trace Context: tracestate header value.
    pub tracestate: Option<String>,
    /// W3C baggage header value. Do not put secrets or credentials here.
    pub baggage: Option<String>,
}

/// Request header carried with every RPC request.
#[derive(Clone, Debug)]
pub struct RequestHeader {
    /// Client information (call_id, client_id, client_name).
    pub client: ClientInfo,
    /// W3C trace propagation context.
    pub trace_context: TraceContext,
    /// Metadata group name for this request.
    pub group_name: Option<GroupName>,
    /// Mount epoch for FS write operations.
    /// Client should provide the mount_epoch it knows for the mount being accessed.
    /// Server validates this against current mount.mount_epoch and returns NEED_REFRESH if mismatch.
    pub mount_epoch: Option<u64>,
    /// Client-required state-machine applied watermarks.
    ///
    /// Each watermark is scoped by metadata group name. Empty means the
    /// request has no state freshness requirement.
    pub state: Vec<GroupStateWatermark>,
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
    /// Absolute deadline (Unix epoch milliseconds).
    pub deadline: Deadline,
    /// Optional caller context for auditing/diagnostics.
    pub caller_context: Option<CallerContext>,
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

pub const CALLER_CONTEXT_IP: &str = "ip";
pub const CALLER_CONTEXT_HOST: &str = "host";
pub const CALLER_CONTEXT_AZ: &str = "az";
pub const CALLER_CONTEXT_RACK: &str = "rack";
pub const CALLER_CONTEXT_REGION: &str = "region";

/// Parsed caller locality fields from `CallerContext.context`.
///
/// These fields are diagnostic and locality hints only. They are not
/// authenticated and must not be used as an authorization or fencing boundary.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CallerContextFields {
    ip: Option<String>,
    host: Option<String>,
    az: Option<String>,
    rack: Option<String>,
    region: Option<String>,
}

impl CallerContextFields {
    pub fn from_caller_context(context: &CallerContext) -> Self {
        Self::parse(&context.context)
    }

    pub fn parse(context: &str) -> Self {
        let mut fields = Self::default();
        for pair in context.split(',') {
            let Some((key, value)) = pair.split_once('=') else {
                continue;
            };
            let key = key.trim();
            let value = value.trim();
            if key.is_empty() || value.is_empty() {
                continue;
            }
            match key {
                CALLER_CONTEXT_IP if fields.ip.is_none() => fields.ip = Some(value.to_string()),
                CALLER_CONTEXT_HOST if fields.host.is_none() => fields.host = Some(value.to_string()),
                CALLER_CONTEXT_AZ if fields.az.is_none() => fields.az = Some(value.to_string()),
                CALLER_CONTEXT_RACK if fields.rack.is_none() => fields.rack = Some(value.to_string()),
                CALLER_CONTEXT_REGION if fields.region.is_none() => fields.region = Some(value.to_string()),
                _ => {}
            }
        }
        fields
    }

    pub fn ip(&self) -> Option<&str> {
        self.ip.as_deref()
    }

    pub fn host(&self) -> Option<&str> {
        self.host.as_deref()
    }

    pub fn az(&self) -> Option<&str> {
        self.az.as_deref()
    }

    pub fn rack(&self) -> Option<&str> {
        self.rack.as_deref()
    }

    pub fn region(&self) -> Option<&str> {
        self.region.as_deref()
    }
}

/// Authentication type marker for request identity.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AuthnType {
    #[default]
    Unspecified,
    Simple,
    Kerberos,
    Token,
}

/// Response header carried with every RPC response.
#[derive(Clone, Debug)]
pub struct ResponseHeader {
    /// Client information (call_id, client_id, client_name).
    pub client: ClientInfo,
    /// Canonical error detail (single source of truth for error semantics).
    pub canonical_error: Option<CanonicalError>,
    /// Server-authorized client state cache updates.
    ///
    /// Leaders and msync may return non-empty state. Follower successful
    /// responses must leave this empty. Empty means no cache update, not stale.
    pub state: Vec<GroupStateWatermark>,
    /// Mount epoch returned by server (for FS operations).
    /// Server fills this with the current mount.mount_epoch so client can update its cache.
    pub mount_epoch: Option<u64>,
    /// Route epoch returned by server (for FS route/layout operations).
    pub route_epoch: Option<u64>,
    /// Metadata group name that this response applies to.
    pub group_name: Option<GroupName>,
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

/// Error code enumeration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RpcErrorCode {
    Unspecified,
    // Framework / protocol
    InvalidHeader,
    // Routing / topology / raft
    NotLeader,
    StaleState,
    MountEpochMismatch,
    RouteEpochMismatch,
    WorkerNotRegistered,
    WorkerRunMismatch,
    WorkerDescriptorMismatch,
    FullReportRequired,
    BlockLocationUnavailable,
    BlockStampMismatch,
    EpochMismatch,
    Fencing,
    ShardMoved,
    NodeUnavailable,
    InvalidArgument,
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

    fn identity_with_group(&self, group_name: Option<GroupName>) -> HeaderIdentity {
        HeaderIdentity {
            call_id: self.call_id,
            client_id: self.client_id,
            group_name,
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
            trace_context: TraceContext::default(),
            group_name: None,
            mount_epoch: None,
            state: Vec::new(),
            route_epoch: None,
            principal: None,
            real_user: None,
            doas: None,
            authn_type: AuthnType::Unspecified,
            deadline,
            caller_context: None,
            retry_count: 0,
        }
    }

    /// Set the state watermark vector for consistency checking.
    pub fn with_state(mut self, state: Vec<GroupStateWatermark>) -> Self {
        self.state = state;
        self
    }

    /// Set the metadata group name.
    pub fn with_group_name(mut self, group_name: GroupName) -> Self {
        self.group_name = Some(group_name);
        self
    }

    /// Return the basic parsed header identity.
    pub fn identity(&self) -> HeaderIdentity {
        self.client.identity_with_group(self.group_name.clone())
    }

    /// Set the traceparent.
    pub fn with_traceparent(mut self, traceparent: String) -> Self {
        self.trace_context.traceparent = Some(traceparent);
        self
    }

    /// Set the tracestate header value.
    pub fn with_tracestate(mut self, tracestate: String) -> Self {
        self.trace_context.tracestate = Some(tracestate);
        self
    }

    /// Set the baggage header value.
    pub fn with_baggage(mut self, baggage: String) -> Self {
        self.trace_context.baggage = Some(baggage);
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
    /// Inherits client_id, deadline, trace context, state watermarks, and group name.
    /// Generates a new call_id by default.
    pub fn child(&self) -> Self {
        Self {
            client: ClientInfo {
                call_id: CallId::new(),
                client_id: self.client.client_id,
                client_name: self.client.client_name.clone(),
            },
            trace_context: self.trace_context.clone(),
            group_name: self.group_name.clone(),
            mount_epoch: self.mount_epoch,
            state: self.state.clone(),
            route_epoch: self.route_epoch,
            principal: self.principal.clone(),
            real_user: self.real_user.clone(),
            doas: self.doas.clone(),
            authn_type: self.authn_type,
            deadline: self.deadline,
            caller_context: self.caller_context.clone(),
            retry_count: 0,
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
            trace_context: self.trace_context.clone(),
            group_name: self.group_name.clone(),
            mount_epoch: self.mount_epoch,
            state: self.state.clone(),
            route_epoch: self.route_epoch,
            principal: self.principal.clone(),
            real_user: self.real_user.clone(),
            doas: self.doas.clone(),
            authn_type: self.authn_type,
            deadline: self.deadline,
            caller_context: self.caller_context.clone(),
            retry_count: self.retry_count + 1,
        }
    }

    /// Convert RequestHeader to gRPC metadata for propagation.
    ///
    /// This function creates metadata entries for:
    /// - x-call-id: Call ID (UUID string)
    /// - x-client-id: Client ID (u128 as string)
    /// - x-state-id: Group state watermarks as group:term:leader_node_id:index entries
    /// - traceparent/tracestate/baggage: W3C Trace Context (if present)
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
    /// - traceparent/tracestate/baggage: W3C Trace Context
    /// - grpc-timeout: Deadline (converted from timeout to absolute deadline)
    ///
    pub fn from_grpc_metadata<I>(iter: I) -> Result<Self, String>
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
            canonical_error: None,
            state: Vec::new(),
            mount_epoch: None,
            route_epoch: None,
            group_name: None,
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
        Self {
            client,
            canonical_error: Some(canonical_error),
            state: Vec::new(),
            mount_epoch: None,
            route_epoch: None,
            group_name: None,
        }
    }

    /// Derive high-level RPC status from canonical error detail.
    pub fn status(&self) -> RpcStatus {
        match self.canonical_error.as_ref().map(|error| error.class) {
            None | Some(CanonicalErrorClass::Ok) => RpcStatus::Ok,
            Some(CanonicalErrorClass::NeedRefresh | CanonicalErrorClass::Retryable) => RpcStatus::Error,
            Some(CanonicalErrorClass::Fatal) => RpcStatus::Fatal,
        }
    }

    /// Set the state watermark vector.
    pub fn with_state(mut self, state: Vec<GroupStateWatermark>) -> Self {
        self.state = state;
        self
    }

    /// Set the metadata group name.
    pub fn with_group_name(mut self, group_name: GroupName) -> Self {
        self.group_name = Some(group_name);
        self
    }

    /// Return the basic parsed header identity.
    pub fn identity(&self) -> HeaderIdentity {
        self.client.identity_with_group(self.group_name.clone())
    }
}
