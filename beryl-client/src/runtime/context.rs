// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Stable logical operation and per-attempt request context.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use beryl_common::header::HeaderIdentity;
use beryl_proto::common::{ClientInfoProto, RequestHeaderProto};
use beryl_proto::worker::DataRequestHeaderProto;
use beryl_types::{CallId, ClientId, GroupName};

use crate::error::{ClientError, ClientResult};

/// Logical client operations with their replay-safety contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Operation {
    GetStatus,
    ListStatus,
    CreateDirectory,
    CreateDirectoryRecursive,
    Delete,
    Rename,
    OpenFile,
    CreateFile,
    OpenWrite,
    AddBlock,
    CommitFile,
    AbortFileWrite,
    RenewLease,
    SyncWrite,
    Msync,
    Read,
    WriteBlock,
}

impl Operation {
    /// Returns the stable wire and metrics name for this operation.
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::GetStatus => "GetStatus",
            Self::ListStatus => "ListStatus",
            Self::CreateDirectory | Self::CreateDirectoryRecursive => "CreateDirectory",
            Self::Delete => "Delete",
            Self::Rename => "Rename",
            Self::OpenFile => "OpenFile",
            Self::CreateFile => "CreateFile",
            Self::OpenWrite => "OpenWrite",
            Self::AddBlock => "AddBlock",
            Self::CommitFile => "CommitFile",
            Self::AbortFileWrite => "AbortFileWrite",
            Self::RenewLease => "RenewLease",
            Self::SyncWrite => "SyncWrite",
            Self::Msync => "Msync",
            Self::Read => "Read",
            Self::WriteBlock => "WriteBlock",
        }
    }

    /// Returns the only replay authorization used by Metadata and Worker loops.
    pub(crate) const fn retry_safety(self) -> RetrySafety {
        match self {
            Self::GetStatus | Self::ListStatus | Self::OpenFile | Self::Msync | Self::Read => RetrySafety::ReadOnly,
            Self::CreateDirectoryRecursive
            | Self::CreateFile
            | Self::AddBlock
            | Self::CommitFile
            | Self::AbortFileWrite
            | Self::RenewLease
            | Self::SyncWrite => RetrySafety::ReplayableMutation,
            Self::CreateDirectory | Self::Delete | Self::Rename | Self::OpenWrite | Self::WriteBlock => {
                RetrySafety::NonReplayableMutation
            }
        }
    }
}

/// Transport replay authority attached to one typed logical operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RetrySafety {
    ReadOnly,
    ReplayableMutation,
    NonReplayableMutation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ClientIdentity {
    client_id: ClientId,
    client_name: String,
}

impl ClientIdentity {
    /// Creates one nonzero process-local identity reused by all operations.
    pub(crate) fn generate(client_name: impl Into<String>) -> ClientResult<Self> {
        Self::new_checked(ClientId::generate(), client_name)
    }

    fn new_checked(client_id: ClientId, client_name: impl Into<String>) -> ClientResult<Self> {
        if client_id.is_zero() {
            return Err(ClientError::invalid_argument(
                "ClientIdentity requires non-zero client_id",
            ));
        }
        let client_name = client_name.into();
        if client_name.trim().is_empty() {
            return Err(ClientError::invalid_argument(
                "ClientIdentity requires non-blank client_name",
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
    /// Captures one absolute deadline for all child RPCs of a public call.
    pub(crate) fn new(timeout_ms: u64) -> Self {
        let timeout = Duration::from_millis(timeout_ms);
        let now = tokio::time::Instant::now();
        Self {
            // Sealed configuration rejects this overflow. Internal callers
            // still fail closed with an already-expired deadline.
            instant: now.checked_add(timeout).unwrap_or(now),
            unix_ms: unix_now_ms().saturating_add(timeout_ms.min(i64::MAX as u64) as i64),
        }
    }

    /// Returns the remaining budget without extending the original deadline.
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
    operation: Operation,
    route_path: Option<String>,
    deadline: OperationDeadline,
}

impl OperationContext {
    /// Starts a new logical operation with a fresh call identity.
    pub(crate) fn new_with_identity(
        client_identity: &ClientIdentity,
        operation: Operation,
        route_path: Option<String>,
        deadline: OperationDeadline,
    ) -> ClientResult<Self> {
        Self::with_call_id_named(
            client_identity.client_id(),
            client_identity.client_name(),
            client_identity.new_call_id(),
            operation,
            route_path,
            deadline,
        )
    }

    /// Starts a logical operation for an explicitly supplied client identity.
    pub(crate) fn new_named(
        client_id: ClientId,
        client_name: impl Into<String>,
        operation: Operation,
        route_path: Option<String>,
        deadline: OperationDeadline,
    ) -> ClientResult<Self> {
        Self::with_call_id_named(client_id, client_name, CallId::new(), operation, route_path, deadline)
    }

    /// Reconstructs a frozen mutation intent with its original call identity.
    pub(crate) fn with_call_id_named(
        client_id: ClientId,
        client_name: impl Into<String>,
        call_id: CallId,
        operation: Operation,
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
            operation,
            route_path,
            deadline,
        })
    }

    /// Human readable operation name.
    pub(crate) fn operation_name(&self) -> &'static str {
        self.operation.name()
    }

    /// Returns the immutable call identity reused by retries of this intent.
    pub(crate) const fn call_id(&self) -> CallId {
        self.call_id
    }

    /// Returns whether ambiguous transport failure authorizes replay.
    pub(crate) const fn retry_safety(&self) -> RetrySafety {
        self.operation.retry_safety()
    }

    /// Original target path, if present.
    pub(crate) fn original_target_path(&self) -> Option<&str> {
        self.route_path.as_deref()
    }

    /// Returns the absolute deadline shared by every child attempt.
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

    /// Returns the immutable logical operation shared by all attempts.
    pub(crate) fn operation_context(&self) -> &OperationContext {
        &self.operation
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
            .ok_or_else(|| ClientError::invalid_argument("metadata AttemptContext missing group_name"))?;
        if self.operation.client_id.is_zero() {
            return Err(ClientError::invalid_argument(
                "metadata AttemptContext requires non-zero client_id",
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
        Err(ClientError::invalid_argument(
            "AttemptContext requires non-zero client_id",
        ))
    } else {
        Ok(())
    }
}

fn validate_client_name(client_name: &str) -> ClientResult<()> {
    if client_name.trim().is_empty() {
        Err(ClientError::invalid_argument(
            "AttemptContext requires non-blank client_name",
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
