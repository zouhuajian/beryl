// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Stable logical operation and per-attempt request context.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use proto::common::{ClientInfoProto, RequestHeaderProto};
use proto::worker::DataRequestHeaderProto;
use types::{CallId, ClientId, GroupName};

#[cfg(test)]
use crate::config::DEFAULT_CLIENT_NAME;
use crate::error::{ClientError, ClientResult};
use crate::runtime::policy::{OperationKind, ReplaySafety};

/// Stable operation fingerprint used to guard replay of mutations.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct OperationFingerprint(u64);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ClientIdentity {
    client_id: ClientId,
    client_name: String,
}

impl ClientIdentity {
    pub(crate) fn generate(client_name: impl Into<String>) -> ClientResult<Self> {
        Self::new_checked(ClientId::generate(), client_name)
    }

    #[cfg(test)]
    pub(crate) fn from_parts(client_id: ClientId, client_name: impl Into<String>) -> ClientResult<Self> {
        Self::new_checked(client_id, client_name)
    }

    fn new_checked(client_id: ClientId, client_name: impl Into<String>) -> ClientResult<Self> {
        if client_id.is_zero() {
            return Err(ClientError::InvalidArgument(
                "ClientIdentity requires non-zero client_id".to_string(),
            ));
        }
        let client_name = client_name.into();
        if client_name.trim().is_empty() {
            return Err(ClientError::InvalidArgument(
                "ClientIdentity requires non-blank client_name".to_string(),
            ));
        }
        Ok(Self { client_id, client_name })
    }

    pub(crate) fn client_id(&self) -> ClientId {
        self.client_id
    }

    pub(crate) fn client_name(&self) -> &str {
        &self.client_name
    }

    pub(crate) fn new_call_id(&self) -> CallId {
        CallId::new()
    }
}

/// Stable identity fields that define one logical public operation.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct OperationIdentity {
    original_target_path: Option<String>,
    secondary_target_path: Option<String>,
    detail: Option<String>,
    session_identity: Option<String>,
}

impl OperationIdentity {
    /// Identity for a path-targeted operation.
    pub fn path(path: impl Into<String>) -> Self {
        Self {
            original_target_path: Some(path.into()),
            secondary_target_path: None,
            detail: None,
            session_identity: None,
        }
    }

    /// Identity for a two-path operation such as rename.
    pub fn path_pair(src: impl Into<String>, dst: impl Into<String>) -> Self {
        Self {
            original_target_path: Some(src.into()),
            secondary_target_path: Some(dst.into()),
            detail: None,
            session_identity: None,
        }
    }

    /// Identity for a session-scoped operation.
    pub(crate) fn session(path: impl Into<String>, session_identity: impl Into<String>) -> Self {
        Self {
            original_target_path: Some(path.into()),
            secondary_target_path: None,
            detail: None,
            session_identity: Some(session_identity.into()),
        }
    }

    /// Attach an operation-specific stable detail.
    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    /// Compute the replay fingerprint for this identity.
    pub fn fingerprint(&self, kind: OperationKind, operation_name: &str) -> OperationFingerprint {
        let mut hasher = DefaultHasher::new();
        kind.hash(&mut hasher);
        operation_name.hash(&mut hasher);
        self.hash(&mut hasher);
        OperationFingerprint(hasher.finish())
    }

    /// Original path used by this operation, if any.
    pub fn original_target_path(&self) -> Option<&str> {
        self.original_target_path.as_deref()
    }

    /// Return true when this operation has stable session identity.
    fn has_session_identity(&self) -> bool {
        self.session_identity.is_some()
    }
}

/// Stable context for one logical public operation.
#[derive(Clone, Debug)]
pub struct OperationContext {
    client_id: ClientId,
    client_name: String,
    call_id: CallId,
    kind: OperationKind,
    operation_name: String,
    replay_safety: ReplaySafety,
    operation_fingerprint: OperationFingerprint,
    identity: OperationIdentity,
}

impl OperationContext {
    /// Create a new logical operation with a fresh call id.
    #[cfg(test)]
    pub(crate) fn new(
        client_id: ClientId,
        kind: OperationKind,
        operation_name: impl Into<String>,
        identity: OperationIdentity,
    ) -> ClientResult<Self> {
        Self::with_call_id_named(
            client_id,
            DEFAULT_CLIENT_NAME,
            CallId::new(),
            kind,
            operation_name,
            identity,
        )
    }

    pub(crate) fn new_with_identity(
        client_identity: &ClientIdentity,
        kind: OperationKind,
        operation_name: impl Into<String>,
        identity: OperationIdentity,
    ) -> ClientResult<Self> {
        Self::with_call_id_named(
            client_identity.client_id(),
            client_identity.client_name(),
            client_identity.new_call_id(),
            kind,
            operation_name,
            identity,
        )
    }

    pub(crate) fn new_named(
        client_id: ClientId,
        client_name: impl Into<String>,
        kind: OperationKind,
        operation_name: impl Into<String>,
        identity: OperationIdentity,
    ) -> ClientResult<Self> {
        Self::with_call_id_named(client_id, client_name, CallId::new(), kind, operation_name, identity)
    }

    /// Create a logical operation with an explicit call id.
    #[cfg(test)]
    pub(crate) fn with_call_id(
        client_id: ClientId,
        call_id: CallId,
        kind: OperationKind,
        operation_name: impl Into<String>,
        identity: OperationIdentity,
    ) -> ClientResult<Self> {
        Self::with_call_id_named(client_id, DEFAULT_CLIENT_NAME, call_id, kind, operation_name, identity)
    }

    pub(crate) fn with_call_id_named(
        client_id: ClientId,
        client_name: impl Into<String>,
        call_id: CallId,
        kind: OperationKind,
        operation_name: impl Into<String>,
        identity: OperationIdentity,
    ) -> ClientResult<Self> {
        validate_client_id(client_id)?;
        let client_name = client_name.into();
        validate_client_name(&client_name)?;
        let operation_name = operation_name.into();
        let replay_safety = crate::runtime::policy::ReplayPolicyTable::safety_for(kind);
        let operation_fingerprint = identity.fingerprint(kind, &operation_name);
        Ok(Self {
            client_id,
            client_name,
            call_id,
            kind,
            operation_name,
            replay_safety,
            operation_fingerprint,
            identity,
        })
    }

    pub(crate) fn with_call_id_named_and_fingerprint(
        client_id: ClientId,
        client_name: impl Into<String>,
        call_id: CallId,
        kind: OperationKind,
        operation_name: impl Into<String>,
        identity: OperationIdentity,
        expected_fingerprint: OperationFingerprint,
    ) -> ClientResult<Self> {
        let operation = Self::with_call_id_named(client_id, client_name, call_id, kind, operation_name, identity)?;
        if operation.operation_fingerprint != expected_fingerprint {
            return Err(ClientError::InvalidArgument(
                "operation fingerprint changed for stable call_id".to_string(),
            ));
        }
        Ok(operation)
    }

    /// Logical operation kind.
    pub fn kind(&self) -> OperationKind {
        self.kind
    }

    /// Human readable operation name.
    pub fn operation_name(&self) -> &str {
        &self.operation_name
    }

    /// Replay safety required for this operation.
    pub fn replay_safety(&self) -> ReplaySafety {
        self.replay_safety
    }

    /// Stable operation fingerprint.
    pub fn operation_fingerprint(&self) -> OperationFingerprint {
        self.operation_fingerprint
    }

    /// Original target path, if present.
    pub fn original_target_path(&self) -> Option<&str> {
        self.identity.original_target_path()
    }

    /// Return true when replay is tied to a stable session identity.
    pub(crate) fn has_session_identity(&self) -> bool {
        self.identity.has_session_identity()
    }
}

/// Per-attempt context shared by metadata and worker adapters.
#[derive(Clone, Debug)]
pub struct AttemptContext {
    operation: OperationContext,
    call_id_text: String,
    group_name: Option<GroupName>,
    metadata_endpoint: Option<String>,
    attempt_number: u32,
    mount_epoch: Option<u64>,
    route_epoch: Option<u64>,
    state: Vec<proto::common::GroupStateWatermarkProto>,
    deadline_ms: i64,
}

impl AttemptContext {
    /// Create a metadata context and require an explicit group name.
    pub fn for_metadata(
        operation: &OperationContext,
        group_name: GroupName,
        attempt_number: u32,
    ) -> ClientResult<Self> {
        validate_client_id(operation.client_id)?;
        Ok(Self {
            call_id_text: operation.call_id.to_string(),
            operation: operation.clone(),
            group_name: Some(group_name),
            metadata_endpoint: None,
            attempt_number,
            mount_epoch: None,
            route_epoch: None,
            state: Vec::new(),
            deadline_ms: 0,
        })
    }

    /// Create a data-plane context. Data RPCs carry block ownership in their operation payload.
    pub fn for_data(operation: &OperationContext, attempt_number: u32) -> Self {
        Self {
            call_id_text: operation.call_id.to_string(),
            operation: operation.clone(),
            group_name: None,
            metadata_endpoint: None,
            attempt_number,
            mount_epoch: None,
            route_epoch: None,
            state: Vec::new(),
            deadline_ms: 0,
        }
    }

    /// Attach selected metadata endpoint for this attempt.
    pub fn with_metadata_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.metadata_endpoint = Some(endpoint.into());
        self
    }

    /// Attach known mount epoch.
    pub fn with_mount_epoch(mut self, mount_epoch: u64) -> Self {
        self.mount_epoch = Some(mount_epoch);
        self
    }

    /// Attach known route epoch.
    pub fn with_route_epoch(mut self, route_epoch: u64) -> Self {
        self.route_epoch = Some(route_epoch);
        self
    }

    /// Attach group-scoped state watermarks.
    pub fn with_state(mut self, state: Vec<proto::common::GroupStateWatermarkProto>) -> Self {
        self.state = state;
        self
    }

    /// Attach an absolute per-attempt deadline derived from the operation timeout.
    pub fn with_operation_timeout_ms(mut self, timeout_ms: Option<u64>) -> Self {
        if let Some(timeout_ms) = timeout_ms {
            self.deadline_ms = unix_now_ms().saturating_add(timeout_ms.min(i64::MAX as u64) as i64);
        }
        self
    }

    /// Return the stable logical call id.
    pub fn call_id(&self) -> &str {
        &self.call_id_text
    }

    /// Return the stable client identity for this attempt.
    pub fn client_id(&self) -> ClientId {
        self.operation.client_id
    }

    /// Return the metadata group name carried by this attempt, when present.
    pub(crate) fn group_name(&self) -> Option<&GroupName> {
        self.group_name.as_ref()
    }

    /// Return the absolute deadline in Unix epoch milliseconds, or zero when unset.
    pub fn deadline_ms(&self) -> i64 {
        self.deadline_ms
    }

    /// Return the remaining local timeout until this attempt's absolute deadline.
    pub(crate) fn timeout_remaining(&self) -> Option<Duration> {
        if self.deadline_ms <= 0 {
            return None;
        }
        let now = unix_now_ms();
        if self.deadline_ms <= now {
            Some(Duration::ZERO)
        } else {
            Some(Duration::from_millis((self.deadline_ms - now) as u64))
        }
    }

    /// Return the stable logical operation fingerprint.
    pub(crate) fn operation_fingerprint(&self) -> OperationFingerprint {
        self.operation.operation_fingerprint()
    }

    /// Return the selected metadata endpoint for this attempt.
    pub fn metadata_endpoint(&self) -> Option<&str> {
        self.metadata_endpoint.as_deref()
    }

    /// Build common client info for request headers.
    pub fn client_info(&self) -> ClientInfoProto {
        ClientInfoProto {
            call_id: self.call_id_text.clone(),
            client_id: Some(self.operation.client_id.into()),
            client_name: self.operation.client_name.clone(),
        }
    }

    /// Build a metadata request header for this attempt.
    pub fn metadata_header(&self) -> ClientResult<RequestHeaderProto> {
        let group_name = self
            .group_name
            .as_ref()
            .ok_or_else(|| ClientError::InvalidArgument("metadata AttemptContext missing group_name".to_string()))?;
        if self.operation.client_id.is_zero() {
            return Err(ClientError::InvalidArgument(
                "metadata AttemptContext requires non-zero client_id".to_string(),
            ));
        }
        Ok(RequestHeaderProto {
            client: Some(self.client_info()),
            trace_context: None,
            group_name: group_name.to_string(),
            mount_epoch: self.mount_epoch,
            deadline_ms: self.deadline_ms(),
            caller_context: None,
            state: self.state.clone(),
            retry_count: self.attempt_number as i32,
            route_epoch: self.route_epoch,
            principal: String::new(),
            real_user: String::new(),
            doas: String::new(),
            authn_type: 0,
        })
    }

    /// Build a worker data-plane request header for this attempt.
    pub fn data_header(&self) -> DataRequestHeaderProto {
        DataRequestHeaderProto {
            client: Some(self.client_info()),
            trace_context: None,
        }
    }
}

fn validate_client_id(client_id: ClientId) -> ClientResult<()> {
    if client_id.is_zero() {
        Err(ClientError::InvalidArgument(
            "AttemptContext requires non-zero client_id".to_string(),
        ))
    } else {
        Ok(())
    }
}

fn validate_client_name(client_name: &str) -> ClientResult<()> {
    if client_name.trim().is_empty() {
        Err(ClientError::InvalidArgument(
            "AttemptContext requires non-blank client_name".to_string(),
        ))
    } else {
        Ok(())
    }
}

fn unix_now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    use types::{CallId, ClientId};

    fn metadata_operation() -> OperationContext {
        OperationContext::new(
            ClientId::new(7),
            OperationKind::MetadataRead,
            "OpenFile",
            OperationIdentity::path("/alpha"),
        )
        .expect("operation context")
    }

    #[test]
    fn operation_context_uses_stable_call_id() {
        let call_id = CallId::new();
        let operation = OperationContext::with_call_id(
            ClientId::new(7),
            call_id,
            OperationKind::MetadataRead,
            "OpenFile",
            OperationIdentity::path("/alpha"),
        )
        .expect("operation context");

        assert_eq!(operation.call_id, call_id);
        assert_eq!(
            operation.operation_fingerprint,
            OperationIdentity::path("/alpha").fingerprint(OperationKind::MetadataRead, "OpenFile")
        );
    }

    #[test]
    fn metadata_header_carries_runtime_client_identity() {
        let identity = ClientIdentity::from_parts(ClientId::new(7), "prod_ns01").expect("client identity");
        let operation = OperationContext::new_with_identity(
            &identity,
            OperationKind::MetadataRead,
            "OpenFile",
            OperationIdentity::path("/alpha"),
        )
        .expect("operation context");
        let ctx =
            AttemptContext::for_metadata(&operation, GroupName::parse("root").unwrap(), 0).expect("metadata context");

        let header = ctx.metadata_header().expect("metadata header");
        let client = header.client.as_ref().expect("client info");
        let header_client_id = proto::convert::required_client_id(client.client_id, "client_id").expect("client id");

        assert_eq!(header_client_id, identity.client_id());
        assert_eq!(client.client_name, identity.client_name());
        assert!(!client.call_id.is_empty());
    }

    #[test]
    fn attempt_context_rejects_zero_client_id() {
        let invalid_operation = OperationContext {
            client_id: ClientId::new(u128::MIN),
            client_name: "default_client".to_string(),
            call_id: CallId::new(),
            kind: OperationKind::MetadataRead,
            operation_name: "OpenFile".to_string(),
            replay_safety: ReplaySafety::Idempotent,
            operation_fingerprint: OperationIdentity::path("/alpha")
                .fingerprint(OperationKind::MetadataRead, "OpenFile"),
            identity: OperationIdentity::path("/alpha"),
        };

        let err = AttemptContext::for_metadata(&invalid_operation, GroupName::parse("root").unwrap(), 0)
            .expect_err("metadata attempt must reject zero client_id");

        assert!(matches!(err, ClientError::InvalidArgument(msg) if msg.contains("client_id")));
    }

    #[test]
    fn replay_attempt_preserves_call_id() {
        let operation = metadata_operation();
        let first =
            AttemptContext::for_metadata(&operation, GroupName::parse("root").unwrap(), 0).expect("first attempt");
        let replay = AttemptContext::for_metadata(&operation, GroupName::parse("analytics").unwrap(), 1)
            .expect("replay attempt");

        assert_eq!(first.call_id(), replay.call_id());
        assert_eq!(first.metadata_header().expect("first header").group_name, "root");
        assert_eq!(replay.metadata_header().expect("replay header").group_name, "analytics");
    }

    #[test]
    fn attempt_headers_do_not_use_call_id_as_traceparent() {
        let operation = metadata_operation();
        let ctx =
            AttemptContext::for_metadata(&operation, GroupName::parse("root").unwrap(), 0).expect("metadata attempt");

        let metadata_header = ctx.metadata_header().expect("metadata header");
        let data_header = ctx.data_header();

        assert_eq!(
            metadata_header.client.as_ref().expect("metadata client").call_id,
            ctx.call_id()
        );
        assert_eq!(data_header.client.as_ref().expect("data client").call_id, ctx.call_id());
        assert!(metadata_header.trace_context.is_none());
        assert!(data_header.trace_context.is_none());
    }

    #[test]
    fn operation_timeout_sets_attempt_deadline_without_changing_call_id() {
        let operation = metadata_operation();
        let base = AttemptContext::for_metadata(&operation, GroupName::parse("root").unwrap(), 0).expect("attempt");
        let call_id = base.call_id().to_string();

        let timed = base.with_operation_timeout_ms(Some(50));
        let deadline_ms = timed.deadline_ms();

        assert!(deadline_ms > 0);
        assert_eq!(timed.call_id(), call_id);
        assert_eq!(
            timed.metadata_header().expect("metadata header").deadline_ms,
            deadline_ms
        );
    }

    #[test]
    fn absent_operation_timeout_keeps_no_deadline_behavior() {
        let operation = metadata_operation();
        let ctx = AttemptContext::for_metadata(&operation, GroupName::parse("root").unwrap(), 0)
            .expect("attempt")
            .with_operation_timeout_ms(None);

        assert_eq!(ctx.deadline_ms(), 0);
        assert_eq!(ctx.metadata_header().expect("metadata header").deadline_ms, 0);
    }
}
