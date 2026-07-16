#![deny(deprecated)]
// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! RPC error handling for client operations.
//!
//! This module provides a unified entry point for client error decision-making,
//! converting response headers to a structured `ClientAction` that preserves
//! RPC error details and refresh hints.

use crate::error::ClientError;
use crate::runtime::MetadataRefreshCause;
use beryl_common::error::rpc::{
    ErrorKind, MetadataErrorKind, ProtocolErrorKind, RecoveryAction, RpcErrorDetail, WorkerErrorKind,
};
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
    /// Worker network protocol as proto enum raw value.
    pub worker_net_protocol: i32,
}

impl From<beryl_proto::common::WorkerEndpointInfoProto> for EndpointHint {
    fn from(value: beryl_proto::common::WorkerEndpointInfoProto) -> Self {
        Self {
            worker_id: value.worker_id,
            endpoint: value.endpoint,
            worker_net_protocol: value.worker_net_protocol,
        }
    }
}

impl From<beryl_common::error::rpc::WorkerEndpointHint> for EndpointHint {
    fn from(value: beryl_common::error::rpc::WorkerEndpointHint) -> Self {
        Self {
            worker_id: value.worker_id,
            endpoint: value.endpoint,
            worker_net_protocol: value.worker_net_protocol,
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
        /// Local runtime refresh strategy label.
        reason: MetadataRefreshCause,
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
            reason: metadata_refresh_cause_from_kind(rpc_error.kind),
            hint: Box::new(hint),
            rpc_error: Box::new(rpc_error),
        }),
        RecoveryAction::ReopenWriteSession { .. } => Err(ClientAction::Refresh {
            reason: MetadataRefreshCause::Unknown,
            hint: Box::new(hint),
            rpc_error: Box::new(rpc_error),
        }),
        RecoveryAction::Retry { after_ms } => Err(ClientAction::Retry {
            retry_after_ms_hint: *after_ms,
            rpc_error: Box::new(rpc_error),
        }),
        RecoveryAction::RegisterWorker | RecoveryAction::SendFullBlockReport => Err(ClientAction::Refresh {
            reason: MetadataRefreshCause::Unknown,
            hint: Box::new(hint),
            rpc_error: Box::new(rpc_error),
        }),
        RecoveryAction::Fail => Err(ClientAction::Fail {
            rpc_error: Box::new(rpc_error),
        }),
    }
}

fn metadata_refresh_cause_from_kind(kind: ErrorKind) -> MetadataRefreshCause {
    match kind {
        ErrorKind::Metadata(MetadataErrorKind::NotLeader) => MetadataRefreshCause::NotLeader,
        ErrorKind::Metadata(MetadataErrorKind::OwnerGroupMismatch)
        | ErrorKind::Metadata(MetadataErrorKind::GroupMismatch) => MetadataRefreshCause::OwnerGroupMismatch,
        ErrorKind::Metadata(MetadataErrorKind::MountEpochMismatch) => MetadataRefreshCause::MountEpochMismatch,
        ErrorKind::Metadata(MetadataErrorKind::StaleState) => MetadataRefreshCause::StaleState,
        ErrorKind::Metadata(MetadataErrorKind::RouteEpochMismatch) => MetadataRefreshCause::RouteEpochMismatch,
        ErrorKind::Worker(WorkerErrorKind::RunMismatch) => MetadataRefreshCause::WorkerRunMismatch,
        ErrorKind::Worker(WorkerErrorKind::BlockStampMismatch) => MetadataRefreshCause::BlockStampMismatch,
        _ => MetadataRefreshCause::Unknown,
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

#[cfg(test)]
mod tests {
    use super::*;
    use beryl_common::error::rpc::InternalErrorKind;
    use beryl_common::error::rpc::{RefreshHint as RpcRefreshHint, WorkerEndpointHint};
    use beryl_common::header::{ClientInfo, ResponseHeader};
    use beryl_proto::convert::rpc_error_to_proto;
    use beryl_types::fs::FsErrorCode;

    #[derive(Clone, Debug)]
    enum RpcEnvelope {
        Ok,
        TransportStatus(tonic::Status),
        RpcErrorDetail(RpcErrorDetail),
    }

    fn parse_rpc_envelope(
        grpc_status: Result<(), tonic::Status>,
        response_header: Option<&beryl_proto::common::ResponseHeaderProto>,
    ) -> RpcEnvelope {
        match grpc_status {
            Err(status) => RpcEnvelope::TransportStatus(status),
            Ok(()) => {
                let Some(header) = response_header else {
                    return RpcEnvelope::RpcErrorDetail(invalid_header_rpc_error("missing response header"));
                };
                if header.group_name.is_empty() {
                    return RpcEnvelope::RpcErrorDetail(invalid_header_rpc_error(
                        "invalid response header: group_name missing",
                    ));
                }
                let header = match ResponseHeader::try_from(header.clone()) {
                    Ok(header) => header,
                    Err(err) => {
                        return RpcEnvelope::RpcErrorDetail(invalid_header_rpc_error(format!(
                            "invalid response header: {err}"
                        )));
                    }
                };
                match header.rpc_error {
                    None => RpcEnvelope::Ok,
                    Some(rpc_error) => RpcEnvelope::RpcErrorDetail(rpc_error),
                }
            }
        }
    }

    #[test]
    fn validate_ok_header_returns_ok() {
        let header = ResponseHeader::ok(ClientInfo::new(beryl_types::ClientId::new(1)));
        assert!(validate_header_or_action(&header).is_ok());
    }

    #[test]
    fn validate_refresh_metadata_preserves_cause_and_hint() {
        let rpc_error = RpcErrorDetail::refresh_metadata(
            ErrorKind::Metadata(MetadataErrorKind::NotLeader),
            RpcRefreshHint::default(),
            "not leader",
        );
        let mut header = ResponseHeader::error(ClientInfo::new(beryl_types::ClientId::new(1)), rpc_error.clone());
        header.group_name = Some(GroupName::parse("root").unwrap());
        header.mount_epoch = Some(12);

        let result = validate_header_or_action(&header);
        match result {
            Err(ClientAction::Refresh {
                reason,
                hint,
                rpc_error: returned,
            }) => {
                assert_eq!(reason, MetadataRefreshCause::NotLeader);
                assert_eq!(hint.group_name, Some(GroupName::parse("root").unwrap()));
                assert_eq!(hint.mount_epoch, Some(12));
                assert_eq!(returned.kind, rpc_error.kind);
                assert_eq!(returned.recovery, rpc_error.recovery);
            }
            _ => panic!("expected Refresh action"),
        }
    }

    #[test]
    fn validate_refresh_metadata_preserves_full_hint_fields() {
        let rpc_error = RpcErrorDetail::refresh_metadata(
            ErrorKind::Metadata(MetadataErrorKind::OwnerGroupMismatch),
            RpcRefreshHint {
                leader_endpoint: Some("http://127.0.0.1:18081".to_string()),
                group_name: Some("analytics".to_string()),
                mount_epoch: Some(31),
                mount_prefix: Some("/mnt".to_string()),
                route_epoch: Some(23),
                worker_endpoints: vec![
                    WorkerEndpointHint {
                        worker_id: 5,
                        endpoint: "127.0.0.1:9005".to_string(),
                        worker_net_protocol: beryl_proto::common::WorkerNetProtocolProto::WorkerNetProtocolGrpc as i32,
                    },
                    WorkerEndpointHint {
                        worker_id: 6,
                        endpoint: "127.0.0.1:9006".to_string(),
                        worker_net_protocol: beryl_proto::common::WorkerNetProtocolProto::WorkerNetProtocolGrpc as i32,
                    },
                ],
                worker_resolve_required: true,
            },
            "route moved",
        );
        let mut header = ResponseHeader::error(ClientInfo::new(beryl_types::ClientId::new(1)), rpc_error);
        header.group_name = Some(GroupName::parse("root").unwrap());
        header.mount_epoch = Some(111);
        header.route_epoch = Some(222);

        let result = validate_header_or_action(&header);

        match result {
            Err(ClientAction::Refresh { hint, .. }) => {
                assert_eq!(hint.leader_endpoint.as_deref(), Some("http://127.0.0.1:18081"));
                assert_eq!(hint.group_name, Some(GroupName::parse("analytics").unwrap()));
                assert_eq!(hint.mount_epoch, Some(31));
                assert_eq!(hint.mount_prefix.as_deref(), Some("/mnt"));
                assert_eq!(hint.route_epoch, Some(23));
                assert!(hint.worker_resolve_required);
                assert_eq!(hint.worker_endpoints.len(), 2);
                assert_eq!(hint.worker_endpoints[0].endpoint, "127.0.0.1:9005");
                assert_eq!(hint.worker_endpoints[1].endpoint, "127.0.0.1:9006");
            }
            _ => panic!("expected Refresh action"),
        }
    }

    #[test]
    fn validate_retryable_preserves_retry_after() {
        let rpc_error = RpcErrorDetail::retry(
            ErrorKind::Internal(InternalErrorKind::NodeUnavailable),
            Some(1000),
            "unavailable",
        );
        let header = ResponseHeader::error(ClientInfo::new(beryl_types::ClientId::new(1)), rpc_error.clone());
        let result = validate_header_or_action(&header);
        match result {
            Err(ClientAction::Retry {
                retry_after_ms_hint,
                rpc_error: returned,
            }) => {
                assert_eq!(retry_after_ms_hint, Some(1000));
                assert_eq!(returned.kind, rpc_error.kind);
                assert_eq!(returned.recovery, rpc_error.recovery);
            }
            _ => panic!("expected Retry action"),
        }
    }

    #[test]
    fn validate_fatal_returns_fail() {
        let rpc_error = RpcErrorDetail::fail(ErrorKind::Internal(InternalErrorKind::Internal), "fatal error");
        let header = ResponseHeader::error(ClientInfo::new(beryl_types::ClientId::new(1)), rpc_error.clone());
        let result = validate_header_or_action(&header);
        match result {
            Err(ClientAction::Fail { rpc_error: returned }) => {
                assert_eq!(returned.kind, ErrorKind::Internal(InternalErrorKind::Internal));
                assert_eq!(returned.recovery, RecoveryAction::Fail);
            }
            _ => panic!("expected Fail action"),
        }
    }

    #[test]
    fn parse_rpc_envelope_reports_transport_error() {
        match parse_rpc_envelope(Err(tonic::Status::unavailable("down")), None) {
            RpcEnvelope::TransportStatus(status) => assert_eq!(status.code(), tonic::Code::Unavailable),
            _ => panic!("expected transport envelope"),
        }
    }

    #[test]
    fn parse_rpc_envelope_reports_rpc_error() {
        let rpc_error = RpcErrorDetail::refresh_metadata(
            ErrorKind::Metadata(MetadataErrorKind::NotLeader),
            RpcRefreshHint::default(),
            "not leader",
        );
        let header = beryl_proto::common::ResponseHeaderProto {
            client: Some(beryl_proto::common::ClientInfoProto {
                call_id: beryl_types::CallId::new().to_string(),
                client_id: Some(beryl_types::ClientId::new(7).into()),
                client_name: "test".to_string(),
            }),
            error: Some(rpc_error_to_proto(&rpc_error)),
            state: Vec::new(),
            group_name: "root".to_string(),
            mount_epoch: None,
            route_epoch: None,
        };
        match parse_rpc_envelope(Ok(()), Some(&header)) {
            RpcEnvelope::RpcErrorDetail(err) => {
                assert_eq!(err.kind, ErrorKind::Metadata(MetadataErrorKind::NotLeader));
                assert!(matches!(err.recovery, RecoveryAction::RefreshMetadata { .. }));
            }
            _ => panic!("expected rpc error envelope"),
        }
    }

    #[test]
    fn parse_rpc_envelope_missing_header_is_fatal_rpc_error() {
        match parse_rpc_envelope(Ok(()), None) {
            RpcEnvelope::RpcErrorDetail(err) => {
                assert_eq!(err.kind, ErrorKind::Protocol(ProtocolErrorKind::InvalidHeader));
                assert_eq!(err.recovery, RecoveryAction::Fail);
            }
            _ => panic!("expected fatal rpc error envelope"),
        }
    }

    #[test]
    fn parse_rpc_envelope_malformed_ok_header_is_fatal_rpc_error() {
        let malformed = beryl_proto::common::ResponseHeaderProto::default();

        match parse_rpc_envelope(Ok(()), Some(&malformed)) {
            RpcEnvelope::RpcErrorDetail(err) => {
                assert_eq!(err.kind, ErrorKind::Protocol(ProtocolErrorKind::InvalidHeader));
                assert_eq!(err.recovery, RecoveryAction::Fail);
                assert!(err.message.contains("invalid response header"));
            }
            _ => panic!("expected fatal rpc error envelope"),
        }
    }

    #[test]
    fn parse_rpc_envelope_missing_group_name_ok_header_is_fatal_rpc_error() {
        let header = beryl_proto::common::ResponseHeaderProto {
            client: Some(beryl_proto::common::ClientInfoProto {
                call_id: beryl_types::CallId::new().to_string(),
                client_id: Some(beryl_types::ClientId::new(7).into()),
                client_name: "test".to_string(),
            }),
            error: None,
            state: Vec::new(),
            group_name: String::new(),
            mount_epoch: None,
            route_epoch: None,
        };

        match parse_rpc_envelope(Ok(()), Some(&header)) {
            RpcEnvelope::RpcErrorDetail(err) => {
                assert_eq!(err.kind, ErrorKind::Protocol(ProtocolErrorKind::InvalidHeader));
                assert_eq!(err.recovery, RecoveryAction::Fail);
                assert!(err.message.contains("group_name"));
            }
            _ => panic!("expected fatal rpc error envelope"),
        }
    }

    #[test]
    fn validate_fs_error_fails_without_refresh() {
        let rpc_error = RpcErrorDetail::fs(FsErrorCode::EPerm, "denied");
        let header = ResponseHeader::error(ClientInfo::new(beryl_types::ClientId::new(1)), rpc_error);

        let result = validate_header_or_action(&header);

        match result {
            Err(ClientAction::Fail { rpc_error }) => {
                assert_eq!(rpc_error.kind, ErrorKind::Fs(FsErrorCode::EPerm));
                assert_eq!(rpc_error.recovery, RecoveryAction::Fail);
            }
            _ => panic!("expected Fail action"),
        }
    }
}
