// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Stable logical operation and per-attempt request context.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use beryl_common::header::HeaderIdentity;
use beryl_proto::common::{ClientInfoProto, RequestHeaderProto};
use beryl_proto::worker::DataRequestHeaderProto;
use beryl_types::{CallId, ClientId, GroupName};

use crate::error::{ClientError, ClientResult};

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

/// Shared deadline for every RPC in one public operation.
#[derive(Clone, Debug)]
pub(crate) struct OperationDeadline {
    instant: tokio::time::Instant,
    unix_ms: i64,
}

impl OperationDeadline {
    pub(crate) fn new(timeout_ms: u64) -> Self {
        let timeout = Duration::from_millis(timeout_ms);
        Self {
            instant: tokio::time::Instant::now() + timeout,
            unix_ms: unix_now_ms().saturating_add(timeout_ms.min(i64::MAX as u64) as i64),
        }
    }

    pub(crate) fn remaining(&self) -> Duration {
        self.instant.saturating_duration_since(tokio::time::Instant::now())
    }

    fn unix_ms(&self) -> i64 {
        self.unix_ms
    }
}

/// Stable context for one logical public operation.
#[derive(Clone, Debug)]
pub(crate) struct OperationContext {
    client_id: ClientId,
    client_name: String,
    call_id: CallId,
    operation_name: &'static str,
    route_path: Option<String>,
    deadline: OperationDeadline,
}

impl OperationContext {
    pub(crate) fn new_with_identity(
        client_identity: &ClientIdentity,
        operation_name: &'static str,
        route_path: Option<String>,
        deadline: OperationDeadline,
    ) -> ClientResult<Self> {
        Self::with_call_id_named(
            client_identity.client_id(),
            client_identity.client_name(),
            client_identity.new_call_id(),
            operation_name,
            route_path,
            deadline,
        )
    }

    pub(crate) fn new_named(
        client_id: ClientId,
        client_name: impl Into<String>,
        operation_name: &'static str,
        route_path: Option<String>,
        deadline: OperationDeadline,
    ) -> ClientResult<Self> {
        Self::with_call_id_named(
            client_id,
            client_name,
            CallId::new(),
            operation_name,
            route_path,
            deadline,
        )
    }

    pub(crate) fn with_call_id_named(
        client_id: ClientId,
        client_name: impl Into<String>,
        call_id: CallId,
        operation_name: &'static str,
        route_path: Option<String>,
        deadline: OperationDeadline,
    ) -> ClientResult<Self> {
        validate_client_id(client_id)?;
        let client_name = client_name.into();
        validate_client_name(&client_name)?;
        Ok(Self {
            client_id,
            client_name,
            call_id,
            operation_name,
            route_path,
            deadline,
        })
    }

    /// Human readable operation name.
    pub(crate) fn operation_name(&self) -> &'static str {
        self.operation_name
    }

    /// Original target path, if present.
    pub(crate) fn original_target_path(&self) -> Option<&str> {
        self.route_path.as_deref()
    }

    pub(crate) fn deadline(&self) -> &OperationDeadline {
        &self.deadline
    }
}

/// Per-attempt context shared by metadata and worker adapters.
#[derive(Clone, Debug)]
pub(crate) struct AttemptContext {
    operation: OperationContext,
    call_id_text: String,
    group_name: Option<GroupName>,
    metadata_endpoint: Option<String>,
    mount_epoch: Option<u64>,
    route_epoch: Option<u64>,
    state: Vec<beryl_proto::common::GroupStateWatermarkProto>,
    deadline_ms: i64,
}

impl AttemptContext {
    /// Create a metadata context and require an explicit group name.
    pub(crate) fn for_metadata(
        operation: &OperationContext,
        group_name: GroupName,
        _attempt_number: u32,
    ) -> ClientResult<Self> {
        validate_client_id(operation.client_id)?;
        Ok(Self {
            call_id_text: operation.call_id.to_string(),
            operation: operation.clone(),
            group_name: Some(group_name),
            metadata_endpoint: None,
            mount_epoch: None,
            route_epoch: None,
            state: Vec::new(),
            deadline_ms: operation.deadline.unix_ms(),
        })
    }

    /// Create a data-plane context. Data RPCs carry block ownership in their operation payload.
    pub(crate) fn for_data(operation: &OperationContext, _attempt_number: u32) -> Self {
        Self {
            call_id_text: operation.call_id.to_string(),
            operation: operation.clone(),
            group_name: None,
            metadata_endpoint: None,
            mount_epoch: None,
            route_epoch: None,
            state: Vec::new(),
            deadline_ms: operation.deadline.unix_ms(),
        }
    }

    /// Attach selected metadata endpoint for this attempt.
    pub(crate) fn with_metadata_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.metadata_endpoint = Some(endpoint.into());
        self
    }

    /// Attach known mount epoch.
    pub(crate) fn with_mount_epoch(mut self, mount_epoch: u64) -> Self {
        self.mount_epoch = Some(mount_epoch);
        self
    }

    /// Attach known route epoch.
    pub(crate) fn with_route_epoch(mut self, route_epoch: u64) -> Self {
        self.route_epoch = Some(route_epoch);
        self
    }

    /// Attach group-scoped state watermarks.
    pub(crate) fn with_state(mut self, state: Vec<beryl_proto::common::GroupStateWatermarkProto>) -> Self {
        self.state = state;
        self
    }

    /// Return the stable logical call id.
    #[cfg(test)]
    pub(crate) fn call_id(&self) -> &str {
        &self.call_id_text
    }

    /// Return the stable client identity for this attempt.
    #[cfg(test)]
    pub(crate) fn client_id(&self) -> ClientId {
        self.operation.client_id
    }

    /// Return the metadata group name carried by this attempt, when present.
    pub(crate) fn group_name(&self) -> Option<&GroupName> {
        self.group_name.as_ref()
    }

    /// Return the basic client/call/group identity for response integrity checks.
    pub(crate) fn header_identity(&self) -> HeaderIdentity {
        HeaderIdentity {
            call_id: self.operation.call_id,
            client_id: self.operation.client_id,
            group_name: self.group_name.clone(),
        }
    }

    /// Return the absolute deadline in Unix epoch milliseconds, or zero when unset.
    pub(crate) fn deadline_ms(&self) -> i64 {
        self.deadline_ms
    }

    /// Return the remaining local timeout until this attempt's absolute deadline.
    pub(crate) fn timeout_remaining(&self) -> Option<Duration> {
        Some(self.operation.deadline.remaining())
    }

    /// Return the selected metadata endpoint for this attempt.
    pub(crate) fn metadata_endpoint(&self) -> Option<&str> {
        self.metadata_endpoint.as_deref()
    }

    /// Build common client info for request headers.
    pub(crate) fn client_info(&self) -> ClientInfoProto {
        ClientInfoProto {
            call_id: self.call_id_text.clone(),
            client_id: Some(self.operation.client_id.into()),
            client_name: self.operation.client_name.clone(),
        }
    }

    /// Build a metadata request header for this attempt.
    pub(crate) fn metadata_header(&self) -> ClientResult<RequestHeaderProto> {
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
            route_epoch: self.route_epoch,
        })
    }

    /// Build a worker data-plane request header for this attempt.
    pub(crate) fn data_header(&self) -> DataRequestHeaderProto {
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
    use beryl_types::{CallId, ClientId};

    fn metadata_operation() -> OperationContext {
        OperationContext::new_named(
            ClientId::new(7),
            "prod_ns01",
            "OpenFile",
            Some("/alpha".to_string()),
            OperationDeadline::new(1_000),
        )
        .expect("operation context")
    }

    #[test]
    fn operation_context_uses_stable_call_id() {
        let call_id = CallId::new();
        let operation = OperationContext::with_call_id_named(
            ClientId::new(7),
            "prod_ns01",
            call_id,
            "OpenFile",
            Some("/alpha".to_string()),
            OperationDeadline::new(1_000),
        )
        .expect("operation context");

        assert_eq!(operation.call_id, call_id);
    }

    #[test]
    fn metadata_header_carries_runtime_client_identity() {
        let identity = ClientIdentity::from_parts(ClientId::new(7), "prod_ns01").expect("client identity");
        let operation = OperationContext::new_with_identity(
            &identity,
            "OpenFile",
            Some("/alpha".to_string()),
            OperationDeadline::new(1_000),
        )
        .expect("operation context");
        let ctx =
            AttemptContext::for_metadata(&operation, GroupName::parse("root").unwrap(), 0).expect("metadata context");

        let header = ctx.metadata_header().expect("metadata header");
        let client = header.client.as_ref().expect("client info");
        let header_client_id =
            beryl_proto::convert::required_client_id(client.client_id, "client_id").expect("client id");

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
            operation_name: "OpenFile",
            route_path: Some("/alpha".to_string()),
            deadline: OperationDeadline::new(1_000),
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
    fn shared_deadline_is_preserved_across_attempts() {
        let operation = metadata_operation();
        let base = AttemptContext::for_metadata(&operation, GroupName::parse("root").unwrap(), 0).expect("attempt");
        let call_id = base.call_id().to_string();
        let replay = AttemptContext::for_metadata(&operation, GroupName::parse("root").unwrap(), 1).expect("replay");
        let deadline_ms = base.deadline_ms();
        assert!(deadline_ms > 0);
        assert_eq!(replay.call_id(), call_id);
        assert_eq!(replay.deadline_ms(), deadline_ms);
    }
}
