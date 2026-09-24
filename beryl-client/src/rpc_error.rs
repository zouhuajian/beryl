#![deny(deprecated)]
// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Validated RPC failure extraction.

use beryl_common::error::rpc::RecoveryAction;
use beryl_common::header::ResponseHeader;
use beryl_proto::convert::rpc_error_from_proto;
use beryl_types::GroupName;

use crate::error::{ClientError, RefreshHint};

/// Maps an already decoded Metadata failure and its authority hints.
pub(crate) fn metadata_error(
    header: &ResponseHeader,
    rpc_error: beryl_common::error::rpc::RpcErrorDetail,
) -> ClientError {
    let rpc_hint = recovery_hint(&rpc_error.recovery);
    let group_name = match rpc_hint
        .and_then(|hint| hint.group_name.as_deref())
        .map(GroupName::parse)
        .transpose()
    {
        Ok(group) => group,
        Err(error) => {
            return ClientError::malformed_response(format!(
                "metadata error response invalid recovery group_name: {error}"
            ))
        }
    };
    if let (Some(header_group), Some(hinted_group)) = (&header.group_name, &group_name) {
        if header_group != hinted_group {
            return ClientError::malformed_response(format!(
                "metadata error response group_name conflicts with recovery hint: header={header_group}, hint={hinted_group}"
            ));
        }
    }
    let hint = RefreshHint {
        leader_endpoint: rpc_hint.and_then(|hint| hint.leader_endpoint.clone()),
        group_name: group_name.or_else(|| header.group_name.clone()),
    };
    ClientError::from_remote(rpc_error, hint)
}

/// Extracts a structured failure after Worker header identity validation.
pub(crate) fn validate_data_header(header: &beryl_proto::worker::DataResponseHeaderProto) -> Result<(), ClientError> {
    let Some(error) = header.error.as_ref() else {
        return Ok(());
    };
    let rpc_error = rpc_error_from_proto(error);
    let hint = refresh_hint_from_rpc_error(recovery_hint(&rpc_error.recovery));
    Err(ClientError::from_remote(rpc_error, hint))
}

fn recovery_hint(recovery: &RecoveryAction) -> Option<&beryl_common::error::rpc::RefreshHint> {
    match recovery {
        RecoveryAction::RefreshMetadata { hint } | RecoveryAction::ReopenWriteSession { hint } => Some(hint),
        _ => None,
    }
}

fn refresh_hint_from_rpc_error(rpc_hint: Option<&beryl_common::error::rpc::RefreshHint>) -> RefreshHint {
    let Some(rpc_hint) = rpc_hint else {
        return RefreshHint::default();
    };
    RefreshHint {
        leader_endpoint: rpc_hint.leader_endpoint.clone(),
        group_name: rpc_hint
            .group_name
            .as_deref()
            .and_then(|group_name| GroupName::parse(group_name).ok()),
    }
}
