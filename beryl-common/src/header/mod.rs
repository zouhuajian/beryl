// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Request and Response Headers for RPC calls.
//!
//! This module provides Rust domain types that correspond to protobuf header messages.
//! Conversions between these types and proto types are implemented in beryl_proto::convert.
//!
//! ## Import Paths
//!
//! - **Internal domain side**: `beryl_common::header::RequestHeader` (this module)
//! - **Protobuf side**: `beryl_proto::common::RequestHeaderProto`
//!
//! ## Conversions
//!
//! Conversions between `beryl_proto::common::RequestHeaderProto` and `beryl_common::header::RequestHeader`
//! are implemented in `beryl_proto::convert` module:
//!
//! - `TryFrom<beryl_proto::common::RequestHeaderProto> for RequestHeader`
//! - `From<&RequestHeader> for beryl_proto::common::RequestHeaderProto`
//!
//! These are the **authoritative** implementations. Do not implement these
//! conversions elsewhere.

/// Identifies a request rejected before any gRPC handler executes.
pub const HEADER_PRE_HANDLER_REJECTION: &str = "beryl-pre-handler-rejection";
/// Marker value for pre-handler request-concurrency rejection.
pub const PRE_HANDLER_REJECTION_RPC_CONCURRENCY: &str = "rpc-concurrency";
/// Identifies a Worker data rejection made before any local data side effect.
pub const HEADER_WORKER_DATA_REJECTION: &str = "beryl-worker-data-rejection";
/// Marker value for Worker capacity rejection before staging or read IO begins.
pub const WORKER_DATA_REJECTION_CAPACITY_BEFORE_SIDE_EFFECT: &str = "capacity-before-side-effect";
/// Identifies structured Worker data errors encoded in gRPC status details.
pub const HEADER_WORKER_DATA_ERROR_DETAIL: &str = "beryl-worker-data-error-detail";
/// Version marker for `DataResponseHeaderProto` encoded in status details.
pub const WORKER_DATA_ERROR_DETAIL_V1: &str = "v1";

use crate::{error::rpc::RpcErrorDetail, time::Deadline};
use beryl_types::{CallId, ClientId, GroupName, GroupStateWatermark};

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

/// W3C trace propagation context.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TraceContext {
    /// W3C Trace Context: traceparent header value.
    pub traceparent: Option<String>,
}

impl TraceContext {
    /// Returns whether all propagation fields are absent.
    pub fn is_empty(&self) -> bool {
        self.traceparent.is_none()
    }
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
    /// Client-required state-machine applied watermark.
    ///
    /// The watermark is scoped by metadata group name. None means the
    /// request has no state freshness requirement.
    pub state: Option<GroupStateWatermark>,
    /// Absolute deadline (Unix epoch milliseconds).
    pub deadline: Deadline,
    /// Optional caller context for auditing/diagnostics.
    pub caller_context: Option<CallerContext>,
}

/// Human-oriented context for auditing and lightweight diagnostics.
#[derive(Clone, Debug)]
pub struct CallerContext {
    /// Example: "type=spark,job=42"
    pub context: String,
}

pub const CALLER_CONTEXT_IP: &str = "ip";
pub const CALLER_CONTEXT_HOST: &str = "host";

/// Parsed caller locality fields from `CallerContext.context`.
///
/// These fields are diagnostic and locality hints only. They are not
/// authenticated and must not be used as an authorization or fencing boundary.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CallerContextFields {
    ip: Option<String>,
    host: Option<String>,
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
            if value.is_empty() {
                continue;
            }
            match key {
                CALLER_CONTEXT_IP if fields.ip.is_none() => fields.ip = Some(value.to_string()),
                CALLER_CONTEXT_HOST if fields.host.is_none() => fields.host = Some(value.to_string()),
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
}

/// Response header carried with every RPC response.
#[derive(Clone, Debug)]
pub struct ResponseHeader {
    /// Client information (call_id, client_id, client_name).
    pub client: ClientInfo,
    /// RPC error detail (single source of truth for error semantics).
    pub rpc_error: Option<RpcErrorDetail>,
    /// Server-authorized client state cache update.
    ///
    /// Leaders and msync may return a watermark. Follower successful
    /// responses must leave this absent. None means no cache update, not stale.
    pub state: Option<GroupStateWatermark>,
    /// Metadata group name that this response applies to.
    pub group_name: Option<GroupName>,
}

impl ClientInfo {
    /// Create a new ClientInfo with a client ID.
    fn new(client_id: impl Into<ClientId>) -> Self {
        Self {
            call_id: CallId::new(),
            client_id: client_id.into(),
            client_name: None,
        }
    }
}

impl RequestHeader {
    /// Create a new RequestHeader with a client ID.
    pub fn new(client_id: impl Into<ClientId>) -> Self {
        Self::with_deadline(client_id, Deadline::from_now(std::time::Duration::from_secs(30)))
    }

    /// Create a new RequestHeader with a client ID and deadline.
    fn with_deadline(client_id: impl Into<ClientId>, deadline: Deadline) -> Self {
        Self {
            client: ClientInfo::new(client_id),
            trace_context: TraceContext::default(),
            group_name: None,
            state: None,
            deadline,
            caller_context: None,
        }
    }

    /// Set the metadata group name.
    pub fn with_group_name(mut self, group_name: GroupName) -> Self {
        self.group_name = Some(group_name);
        self
    }
}

impl ResponseHeader {
    /// Create a successful response header.
    pub fn ok(client: ClientInfo) -> Self {
        Self {
            client,
            rpc_error: None,
            state: None,
            group_name: None,
        }
    }

    /// Set the metadata group name.
    pub fn with_group_name(mut self, group_name: GroupName) -> Self {
        self.group_name = Some(group_name);
        self
    }
}
