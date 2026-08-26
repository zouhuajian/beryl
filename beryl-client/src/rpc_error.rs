#![deny(deprecated)]
// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Validated RPC failure extraction.

use beryl_common::error::rpc::RecoveryAction;
use beryl_common::header::ResponseHeader;
use beryl_proto::convert::rpc_error_from_proto;
use beryl_types::GroupName;

use crate::error::{ClientError, EndpointHint, RefreshHint};

/// Validates a Metadata response header before its body is consumed.
pub(crate) fn validate_header(header: &ResponseHeader) -> Result<(), ClientError> {
    let Some(rpc_error) = header.rpc_error.clone() else {
        return Ok(());
    };
    let hint = refresh_hint_from_rpc_error_and_header(recovery_hint(&rpc_error.recovery), header);
    Err(ClientError::from_remote(rpc_error, hint))
}

/// Validates an optional Worker data response header before its payload is consumed.
pub(crate) fn validate_data_header(
    header: Option<&beryl_proto::worker::DataResponseHeaderProto>,
) -> Result<(), ClientError> {
    let Some(error) = header.and_then(|header| header.error.as_ref()) else {
        return Ok(());
    };
    let rpc_error = rpc_error_from_proto(error);
    let hint = refresh_hint_from_rpc_error(recovery_hint(&rpc_error.recovery));
    Err(ClientError::from_remote(rpc_error, hint))
}

/// Constructs a terminal protocol error for a malformed validated response.
pub(crate) fn invalid_header_error(message: impl Into<String>) -> ClientError {
    ClientError::malformed_response(message)
}

fn recovery_hint(recovery: &RecoveryAction) -> Option<&beryl_common::error::rpc::RefreshHint> {
    match recovery {
        RecoveryAction::RefreshMetadata { hint } | RecoveryAction::ReopenWriteSession { hint } => Some(hint),
        _ => None,
    }
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
