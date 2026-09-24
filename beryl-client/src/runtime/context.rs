// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Stable logical operation and per-attempt request context.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use beryl_proto::common::{ClientInfoProto, GroupStateWatermarkProto, RequestHeaderProto};
use beryl_proto::worker::DataRequestHeaderProto;
use beryl_types::{CallId, ClientId, GroupName};

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
    AllocateBlock,
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
            Self::AllocateBlock => "AllocateBlock",
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
            | Self::AllocateBlock
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
    pub(crate) fn generate(client_name: impl Into<String>) -> Self {
        Self {
            client_id: ClientId::generate(),
            client_name: client_name.into(),
        }
    }

    pub(crate) fn client_id(&self) -> ClientId {
        self.client_id
    }

    pub(crate) fn client_name(&self) -> &str {
        &self.client_name
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
            // Fail closed if the deadline is no longer representable at this
            // operation's start, even after configuration-time validation.
            instant: now.checked_add(timeout).unwrap_or(now),
            unix_ms: (unix_now_ms().min(i64::MAX as u64) as i64).saturating_add(timeout_ms.min(i64::MAX as u64) as i64),
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
    deadline: OperationDeadline,
}

impl OperationContext {
    /// Starts a new logical operation with a fresh call identity.
    pub(crate) fn new_with_identity(
        client_identity: &ClientIdentity,
        operation: Operation,
        deadline: OperationDeadline,
    ) -> Self {
        Self::with_call_id_named(
            client_identity.client_id(),
            client_identity.client_name(),
            CallId::new(),
            operation,
            deadline,
        )
    }

    /// Starts a logical operation for an explicitly supplied client identity.
    pub(crate) fn new_named(
        client_id: ClientId,
        client_name: impl Into<String>,
        operation: Operation,
        deadline: OperationDeadline,
    ) -> Self {
        Self::with_call_id_named(client_id, client_name, CallId::new(), operation, deadline)
    }

    /// Reconstructs a frozen mutation intent with its original call identity.
    pub(crate) fn with_call_id_named(
        client_id: ClientId,
        client_name: impl Into<String>,
        call_id: CallId,
        operation: Operation,
        deadline: OperationDeadline,
    ) -> Self {
        let client_name = client_name.into();
        Self {
            client_id,
            client_name,
            call_id,
            operation,
            deadline,
        }
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

    /// Returns the absolute deadline shared by every child attempt.
    pub(crate) fn deadline(&self) -> &OperationDeadline {
        &self.deadline
    }
}

/// Metadata-specific routing and freshness for one RPC attempt.
#[derive(Clone, Debug)]
pub(crate) struct AttemptContext {
    operation: OperationContext,
    group_name: GroupName,
    metadata_endpoint: String,
    state: Option<GroupStateWatermarkProto>,
}

impl AttemptContext {
    pub(crate) fn for_metadata(
        operation: &OperationContext,
        group_name: GroupName,
        endpoint: impl Into<String>,
    ) -> Self {
        Self {
            operation: operation.clone(),
            group_name,
            metadata_endpoint: endpoint.into(),
            state: None,
        }
    }

    pub(crate) fn with_state(mut self, state: Option<GroupStateWatermarkProto>) -> Self {
        self.state = state;
        self
    }

    pub(crate) fn group_name(&self) -> &GroupName {
        &self.group_name
    }

    pub(crate) fn operation_context(&self) -> &OperationContext {
        &self.operation
    }

    pub(crate) fn metadata_endpoint(&self) -> &str {
        &self.metadata_endpoint
    }

    pub(crate) fn metadata_header(&self) -> RequestHeaderProto {
        RequestHeaderProto {
            client: Some(self.operation.client_info()),
            trace_context: None,
            group_name: self.group_name.to_string(),
            deadline_ms: self.operation.deadline.unix_ms(),
            caller_context: None,
            state: self.state.clone(),
        }
    }
}

impl OperationContext {
    pub(crate) const fn client_id(&self) -> ClientId {
        self.client_id
    }

    pub(crate) fn client_info(&self) -> ClientInfoProto {
        ClientInfoProto {
            call_id: self.call_id.to_string(),
            client_id: Some(self.client_id.into()),
            client_name: self.client_name.clone(),
        }
    }

    pub(crate) fn data_header(&self) -> DataRequestHeaderProto {
        DataRequestHeaderProto {
            client: Some(self.client_info()),
        }
    }

    /// Bounds a single RPC by the remaining logical-operation budget.
    pub(crate) fn tonic_request<T>(&self, message: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(message);
        request.set_timeout(self.deadline.remaining().max(Duration::from_millis(1)));
        request
    }
}

pub(crate) fn unix_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}
