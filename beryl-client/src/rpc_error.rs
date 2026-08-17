#![deny(deprecated)]
// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! RPC error handling for client operations.
//!
//! This module provides a unified entry point for client error decision-making,
//! converting response headers to a structured `ClientAction` that preserves
//! RPC error details and refresh hints.

use crate::error::ClientError;
use beryl_common::error::rpc::{ErrorKind, ProtocolErrorKind, RecoveryAction, RpcErrorDetail};
use beryl_common::header::ResponseHeader;
use beryl_proto::convert::rpc_error_from_proto;
use beryl_types::GroupName;

/// Endpoint hint preserved on refresh actions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EndpointHint {
    /// Worker ID for the hint.
    pub worker_id: u64,
    /// Endpoint address.
    pub endpoint: String,
}

impl From<beryl_proto::common::WorkerEndpointInfoProto> for EndpointHint {
    fn from(value: beryl_proto::common::WorkerEndpointInfoProto) -> Self {
        Self {
            worker_id: value.worker_id,
            endpoint: value.endpoint,
        }
    }
}

impl From<beryl_common::error::rpc::WorkerEndpointHint> for EndpointHint {
    fn from(value: beryl_common::error::rpc::WorkerEndpointHint) -> Self {
        Self {
            worker_id: value.worker_id,
            endpoint: value.endpoint,
        }
    }
}

/// Structured refresh hints preserved from response headers.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RefreshHint {
    /// Metadata leader endpoint from RPC refresh hint.
    pub leader_endpoint: Option<String>,
    /// Stable metadata group name hint from metadata header.
    pub group_name: Option<GroupName>,
    /// Mount prefix associated with the mount epoch, when the server provides it.
    pub mount_prefix: Option<String>,
    /// Route epoch hint (if present in future headers).
    pub route_epoch: Option<u64>,
    /// Mount epoch hint from metadata header.
    pub mount_epoch: Option<u64>,
    /// Primary endpoint hint for callers that expect a single target.
    pub endpoint_hint: Option<EndpointHint>,
    /// All worker endpoint hints from the RPC refresh hint.
    pub worker_endpoints: Vec<EndpointHint>,
    /// Whether the server requires worker placement re-resolution.
    pub worker_resolve_required: bool,
}

/// Client action determined from rpc header validation.
#[derive(Clone, Debug)]
pub(crate) enum ClientAction {
    /// Client must refresh state before retrying.
    Refresh {
        /// Structured refresh hints from response header.
        hint: Box<RefreshHint>,
        /// Original RPC error.
        rpc_error: Box<RpcErrorDetail>,
    },
    /// Client may retry; server retry_after_ms is preserved as a hint.
    Retry {
        /// Optional server retry delay hint in milliseconds.
        retry_after_ms_hint: Option<u64>,
        /// Original RPC error.
        rpc_error: Box<RpcErrorDetail>,
    },
    /// Unrecoverable business failure.
    Fail {
        /// Original RPC error.
        rpc_error: Box<RpcErrorDetail>,
    },
    /// gRPC transport/auth/framework failure (non-OK status).
    TransportFail {
        /// Original tonic status.
        status: Box<tonic::Status>,
    },
}

/// Validate metadata response header and return structured action on error.
///
/// This is the single entrypoint for header validation before response body use.
pub(crate) fn validate_header_or_action(header: &ResponseHeader) -> Result<(), ClientAction> {
    let Some(rpc_error) = header.rpc_error.clone() else {
        return Ok(());
    };

    let hint = refresh_hint_from_rpc_error_and_header(recovery_hint(&rpc_error.recovery), header);
    validate_rpc_error_with_hint(rpc_error, hint)
}

/// Validate worker data-plane header and return structured action on error.
pub(crate) fn validate_data_header_or_action(
    header: Option<&beryl_proto::worker::DataResponseHeaderProto>,
) -> Result<(), ClientAction> {
    let Some(header) = header else {
        return Ok(());
    };

    let Some(err_detail) = header.error.as_ref() else {
        return Ok(());
    };

    let rpc_error = rpc_error_from_proto(err_detail);
    let hint = refresh_hint_from_rpc_error(recovery_hint(&rpc_error.recovery));

    validate_rpc_error_with_hint(rpc_error, hint)
}

fn validate_rpc_error_with_hint(rpc_error: RpcErrorDetail, hint: RefreshHint) -> Result<(), ClientAction> {
    match &rpc_error.recovery {
        RecoveryAction::RefreshMetadata { .. } => Err(ClientAction::Refresh {
            hint: Box::new(hint),
            rpc_error: Box::new(rpc_error),
        }),
        RecoveryAction::ReopenWriteSession { .. } => Err(ClientAction::Refresh {
            hint: Box::new(hint),
            rpc_error: Box::new(rpc_error),
        }),
        RecoveryAction::Retry { after_ms } => Err(ClientAction::Retry {
            retry_after_ms_hint: *after_ms,
            rpc_error: Box::new(rpc_error),
        }),
        RecoveryAction::RegisterWorker | RecoveryAction::SendFullBlockReport => Err(ClientAction::Refresh {
            hint: Box::new(hint),
            rpc_error: Box::new(rpc_error),
        }),
        RecoveryAction::Fail => Err(ClientAction::Fail {
            rpc_error: Box::new(rpc_error),
        }),
    }
}

fn recovery_hint(recovery: &RecoveryAction) -> Option<&beryl_common::error::rpc::RefreshHint> {
    match recovery {
        RecoveryAction::RefreshMetadata { hint } | RecoveryAction::ReopenWriteSession { hint } => Some(hint),
        _ => None,
    }
}

pub(crate) fn invalid_header_action(message: impl Into<String>) -> ClientAction {
    ClientAction::Fail {
        rpc_error: Box::new(invalid_header_rpc_error(message)),
    }
}

fn invalid_header_rpc_error(message: impl Into<String>) -> RpcErrorDetail {
    RpcErrorDetail::fail(ErrorKind::Protocol(ProtocolErrorKind::InvalidHeader), message)
}

fn refresh_hint_from_rpc_error_and_header(
    rpc_hint: Option<&beryl_common::error::rpc::RefreshHint>,
    header: &ResponseHeader,
) -> RefreshHint {
    let mut hint = refresh_hint_from_rpc_error(rpc_hint);
    hint.group_name = hint.group_name.or_else(|| header.group_name.clone());
    hint.route_epoch = hint.route_epoch.or(header.route_epoch);
    hint.mount_epoch = hint.mount_epoch.or(header.mount_epoch);
    hint
}

fn refresh_hint_from_rpc_error(rpc_hint: Option<&beryl_common::error::rpc::RefreshHint>) -> RefreshHint {
    let Some(rpc_hint) = rpc_hint else {
        return RefreshHint::default();
    };
    let worker_endpoints = rpc_hint
        .worker_endpoints
        .iter()
        .cloned()
        .map(EndpointHint::from)
        .collect::<Vec<_>>();
    RefreshHint {
        leader_endpoint: rpc_hint.leader_endpoint.clone(),
        group_name: rpc_hint
            .group_name
            .as_deref()
            .and_then(|group_name| GroupName::parse(group_name).ok()),
        mount_prefix: rpc_hint.mount_prefix.clone(),
        route_epoch: rpc_hint.route_epoch,
        mount_epoch: rpc_hint.mount_epoch,
        endpoint_hint: worker_endpoints.first().cloned(),
        worker_endpoints,
        worker_resolve_required: rpc_hint.worker_resolve_required,
    }
}

impl ClientAction {
    /// Return RPC error if this action carries one.
    pub fn rpc_error(&self) -> Option<&RpcErrorDetail> {
        match self {
            ClientAction::Refresh { rpc_error, .. }
            | ClientAction::Retry { rpc_error, .. }
            | ClientAction::Fail { rpc_error } => Some(rpc_error.as_ref()),
            ClientAction::TransportFail { .. } => None,
        }
    }
}

impl From<ClientAction> for ClientError {
    fn from(action: ClientAction) -> Self {
        ClientError::Action(crate::error::ClientActionError::new(action))
    }
}
