#![deny(deprecated)]
// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Canonical error handling for client operations.
//!
//! This module provides a unified entry point for client error decision-making,
//! converting response headers to a structured `ClientAction` that preserves
//! canonical error details and refresh hints.

use crate::error::ClientError;
use common::error::canonical::{CanonicalError, ErrorClass, RefreshReason};
use common::header::{ResponseHeader, RpcErrorCode};
use proto::convert::error_detail_to_canonical;

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

impl From<proto::common::WorkerEndpointInfoProto> for EndpointHint {
    fn from(value: proto::common::WorkerEndpointInfoProto) -> Self {
        Self {
            worker_id: value.worker_id,
            endpoint: value.endpoint,
            worker_net_protocol: value.worker_net_protocol,
        }
    }
}

impl From<common::error::canonical::WorkerEndpointHint> for EndpointHint {
    fn from(value: common::error::canonical::WorkerEndpointHint) -> Self {
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
    /// Metadata leader endpoint from canonical refresh hint.
    pub leader_endpoint: Option<String>,
    /// Group ID hint from metadata header.
    pub group_id: Option<u64>,
    /// Mount prefix associated with the mount epoch, when the server provides it.
    pub mount_prefix: Option<String>,
    /// Route epoch hint (if present in future headers).
    pub route_epoch: Option<u64>,
    /// Mount epoch hint from metadata header.
    pub mount_epoch: Option<u64>,
    /// Primary endpoint hint for callers that expect a single target.
    pub endpoint_hint: Option<EndpointHint>,
    /// All worker endpoint hints from the canonical refresh hint.
    pub worker_endpoints: Vec<EndpointHint>,
    /// Whether the server requires worker placement re-resolution.
    pub worker_resolve_required: bool,
}

/// Client action determined from canonical/header validation.
#[derive(Clone, Debug)]
pub enum ClientAction {
    /// Header is valid and body is safe to consume.
    Ok,
    /// Client must refresh state before retrying.
    Refresh {
        /// Refresh reason from canonical error.
        reason: RefreshReason,
        /// Structured refresh hints from response header.
        hint: Box<RefreshHint>,
        /// Original canonical error.
        canonical: Box<CanonicalError>,
    },
    /// Client may retry after delay.
    Retry {
        /// Retry delay in milliseconds.
        after_ms: Option<u64>,
        /// Original canonical error.
        canonical: Box<CanonicalError>,
    },
    /// Unrecoverable business failure.
    Fail {
        /// Original canonical error.
        canonical: Box<CanonicalError>,
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
pub fn validate_header_or_action(header: &ResponseHeader) -> Result<(), ClientAction> {
    debug_assert!(
        (header.status == common::header::RpcStatus::Ok) == header.canonical_error.is_none(),
        "response header status/canonical mismatch: status={:?} canonical_present={}",
        header.status,
        header.canonical_error.is_some()
    );

    let Some(canonical) = header.canonical_error.clone() else {
        return Ok(());
    };

    let hint = refresh_hint_from_canonical_and_header(canonical.refresh_hint.as_ref(), header);
    validate_canonical_with_hint(canonical, hint)
}

/// Validate worker data-plane header and return structured action on error.
pub fn validate_data_header_or_action(
    header: Option<&proto::worker::DataResponseHeaderProto>,
) -> Result<(), ClientAction> {
    let Some(header) = header else {
        return Ok(());
    };

    let Some(err_detail) = header.error.as_ref() else {
        return Ok(());
    };

    let canonical = error_detail_to_canonical(err_detail);
    let hint = refresh_hint_from_canonical(canonical.refresh_hint.as_ref());

    validate_canonical_with_hint(canonical, hint)
}

fn validate_canonical_with_hint(canonical: CanonicalError, hint: RefreshHint) -> Result<(), ClientAction> {
    match canonical.class {
        ErrorClass::Ok => Ok(()),
        ErrorClass::NeedRefresh => {
            let reason = canonical.reason.unwrap_or(RefreshReason::Unknown);
            debug_assert!(
                reason != RefreshReason::Unknown,
                "NeedRefresh canonical error should include a specific refresh reason"
            );
            Err(ClientAction::Refresh {
                reason,
                hint: Box::new(hint),
                canonical: Box::new(canonical),
            })
        }
        ErrorClass::Retryable => Err(ClientAction::Retry {
            after_ms: canonical.retry_after_ms,
            canonical: Box::new(canonical),
        }),
        ErrorClass::Fatal => Err(ClientAction::Fail {
            canonical: Box::new(canonical),
        }),
    }
}

pub(crate) fn invalid_header_action(message: impl Into<String>) -> ClientAction {
    ClientAction::Fail {
        canonical: Box::new(invalid_header_canonical(message)),
    }
}

fn invalid_header_canonical(message: impl Into<String>) -> CanonicalError {
    CanonicalError {
        class: ErrorClass::Fatal,
        code: Some(common::error::canonical::ErrorCode::RpcCode(
            RpcErrorCode::InvalidHeader,
        )),
        reason: None,
        retry_after_ms: None,
        message: message.into(),
        refresh_hint: None,
    }
}

fn refresh_hint_from_canonical_and_header(
    canonical_hint: Option<&common::error::canonical::RefreshHint>,
    header: &ResponseHeader,
) -> RefreshHint {
    let mut hint = refresh_hint_from_canonical(canonical_hint);
    hint.group_id = hint.group_id.or(header.group_id);
    hint.route_epoch = hint.route_epoch.or(header.route_epoch);
    hint.mount_epoch = hint.mount_epoch.or(header.mount_epoch);
    hint
}

fn refresh_hint_from_canonical(canonical_hint: Option<&common::error::canonical::RefreshHint>) -> RefreshHint {
    let Some(canonical_hint) = canonical_hint else {
        return RefreshHint::default();
    };
    let worker_endpoints = canonical_hint
        .worker_endpoints
        .iter()
        .cloned()
        .map(EndpointHint::from)
        .collect::<Vec<_>>();
    RefreshHint {
        leader_endpoint: canonical_hint.leader_endpoint.clone(),
        group_id: canonical_hint.group_id,
        mount_prefix: canonical_hint.mount_prefix.clone(),
        route_epoch: canonical_hint.route_epoch,
        mount_epoch: canonical_hint.mount_epoch,
        endpoint_hint: worker_endpoints.first().cloned(),
        worker_endpoints,
        worker_resolve_required: canonical_hint.worker_resolve_required,
    }
}

impl ClientAction {
    /// Return canonical error if this action carries one.
    pub fn canonical(&self) -> Option<&CanonicalError> {
        match self {
            ClientAction::Refresh { canonical, .. }
            | ClientAction::Retry { canonical, .. }
            | ClientAction::Fail { canonical } => Some(canonical.as_ref()),
            ClientAction::Ok | ClientAction::TransportFail { .. } => None,
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
    use common::error::canonical::{
        ErrorCode as CanonicalErrorCode, RefreshHint as CanonicalRefreshHint, WorkerEndpointHint,
    };
    use common::header::RpcErrorCode;
    use common::header::{ClientInfo, ResponseHeader};
    use proto::convert::canonical_to_error_detail;

    #[derive(Clone, Debug)]
    enum RpcEnvelope {
        Ok,
        TransportStatus(tonic::Status),
        CanonicalError(CanonicalError),
    }

    fn parse_rpc_envelope(
        grpc_status: Result<(), tonic::Status>,
        response_header: Option<&proto::common::ResponseHeaderProto>,
    ) -> RpcEnvelope {
        match grpc_status {
            Err(status) => RpcEnvelope::TransportStatus(status),
            Ok(()) => {
                let Some(header) = response_header else {
                    return RpcEnvelope::CanonicalError(invalid_header_canonical("missing response header"));
                };
                if header.group_id == 0 {
                    return RpcEnvelope::CanonicalError(invalid_header_canonical(
                        "invalid response header: group_id must be non-zero",
                    ));
                }
                let header = match ResponseHeader::try_from(header.clone()) {
                    Ok(header) => header,
                    Err(err) => {
                        return RpcEnvelope::CanonicalError(invalid_header_canonical(format!(
                            "invalid response header: {err}"
                        )));
                    }
                };
                match header.canonical_error {
                    None => RpcEnvelope::Ok,
                    Some(canonical) if matches!(canonical.class, ErrorClass::Ok) => RpcEnvelope::Ok,
                    Some(canonical) => RpcEnvelope::CanonicalError(canonical),
                }
            }
        }
    }

    #[test]
    fn validate_ok_header_returns_ok() {
        let header = ResponseHeader::ok(ClientInfo::new(types::ClientId::new(1)));
        assert!(validate_header_or_action(&header).is_ok());
    }

    #[test]
    fn validate_need_refresh_preserves_reason_and_hint() {
        let canonical = CanonicalError::need_refresh(RpcErrorCode::NotLeader, RefreshReason::NotLeader, "not leader");
        let mut header = ResponseHeader::error(ClientInfo::new(types::ClientId::new(1)), canonical.clone());
        header.group_id = Some(7);
        header.mount_epoch = Some(12);

        let result = validate_header_or_action(&header);
        match result {
            Err(ClientAction::Refresh {
                reason,
                hint,
                canonical: returned,
            }) => {
                assert_eq!(reason, RefreshReason::NotLeader);
                assert_eq!(hint.group_id, Some(7));
                assert_eq!(hint.mount_epoch, Some(12));
                assert_eq!(returned.reason, canonical.reason);
                assert_eq!(returned.code, canonical.code);
            }
            _ => panic!("expected Refresh action"),
        }
    }

    #[test]
    fn validate_need_refresh_preserves_full_refresh_hint_fields() {
        let canonical = CanonicalError::need_refresh_with_hint(
            RpcErrorCode::ShardMoved,
            RefreshReason::OwnerGroupMismatch,
            CanonicalRefreshHint {
                leader_endpoint: Some("http://127.0.0.1:18081".to_string()),
                group_id: Some(17),
                mount_epoch: Some(31),
                mount_prefix: Some("/mnt".to_string()),
                route_epoch: Some(23),
                worker_endpoints: vec![
                    WorkerEndpointHint {
                        worker_id: 5,
                        endpoint: "127.0.0.1:9005".to_string(),
                        worker_net_protocol: proto::common::WorkerNetProtocolProto::WorkerNetProtocolGrpc as i32,
                    },
                    WorkerEndpointHint {
                        worker_id: 6,
                        endpoint: "127.0.0.1:9006".to_string(),
                        worker_net_protocol: proto::common::WorkerNetProtocolProto::WorkerNetProtocolGrpc as i32,
                    },
                ],
                worker_resolve_required: true,
            },
            "route moved",
        );
        let mut header = ResponseHeader::error(ClientInfo::new(types::ClientId::new(1)), canonical);
        header.group_id = Some(99);
        header.mount_epoch = Some(111);
        header.route_epoch = Some(222);

        let result = validate_header_or_action(&header);

        match result {
            Err(ClientAction::Refresh { hint, .. }) => {
                assert_eq!(hint.leader_endpoint.as_deref(), Some("http://127.0.0.1:18081"));
                assert_eq!(hint.group_id, Some(17));
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
        let canonical = CanonicalError::retryable(RpcErrorCode::NodeUnavailable, Some(1000), "unavailable");
        let header = ResponseHeader::error(ClientInfo::new(types::ClientId::new(1)), canonical.clone());
        let result = validate_header_or_action(&header);
        match result {
            Err(ClientAction::Retry {
                after_ms,
                canonical: returned,
            }) => {
                assert_eq!(after_ms, Some(1000));
                assert_eq!(returned.retry_after_ms, canonical.retry_after_ms);
                assert_eq!(returned.code, canonical.code);
            }
            _ => panic!("expected Retry action"),
        }
    }

    #[test]
    fn validate_fatal_returns_fail() {
        let canonical = CanonicalError {
            class: ErrorClass::Fatal,
            code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Application)),
            reason: None,
            retry_after_ms: None,
            message: "fatal error".to_string(),
            refresh_hint: None,
        };
        let header = ResponseHeader::error(ClientInfo::new(types::ClientId::new(1)), canonical.clone());
        let result = validate_header_or_action(&header);
        match result {
            Err(ClientAction::Fail { canonical: returned }) => {
                assert_eq!(returned.class, ErrorClass::Fatal);
                assert_eq!(returned.code, canonical.code);
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
    fn parse_rpc_envelope_reports_canonical_error() {
        let canonical = CanonicalError::need_refresh(RpcErrorCode::NotLeader, RefreshReason::NotLeader, "not leader");
        let header = proto::common::ResponseHeaderProto {
            client: Some(proto::common::ClientInfoProto {
                call_id: types::CallId::new().to_string(),
                client_id: 7,
                client_name: "test".to_string(),
            }),
            error: Some(canonical_to_error_detail(&canonical)),
            state: Vec::new(),
            group_id: 7,
            mount_epoch: None,
            route_epoch: None,
        };
        match parse_rpc_envelope(Ok(()), Some(&header)) {
            RpcEnvelope::CanonicalError(err) => {
                assert_eq!(err.class, ErrorClass::NeedRefresh);
                assert_eq!(err.reason, Some(RefreshReason::NotLeader));
            }
            _ => panic!("expected canonical envelope"),
        }
    }

    #[test]
    fn parse_rpc_envelope_missing_header_is_fatal_canonical() {
        match parse_rpc_envelope(Ok(()), None) {
            RpcEnvelope::CanonicalError(err) => {
                assert_eq!(err.class, ErrorClass::Fatal);
                assert!(matches!(
                    err.code,
                    Some(CanonicalErrorCode::RpcCode(RpcErrorCode::InvalidHeader))
                ));
            }
            _ => panic!("expected fatal canonical envelope"),
        }
    }

    #[test]
    fn parse_rpc_envelope_malformed_ok_header_is_fatal_canonical() {
        let malformed = proto::common::ResponseHeaderProto::default();

        match parse_rpc_envelope(Ok(()), Some(&malformed)) {
            RpcEnvelope::CanonicalError(err) => {
                assert_eq!(err.class, ErrorClass::Fatal);
                assert!(matches!(
                    err.code,
                    Some(CanonicalErrorCode::RpcCode(RpcErrorCode::InvalidHeader))
                ));
                assert!(err.message.contains("invalid response header"));
            }
            _ => panic!("expected fatal canonical envelope"),
        }
    }

    #[test]
    fn parse_rpc_envelope_zero_group_id_ok_header_is_fatal_canonical() {
        const INVALID_GROUP_ID: u64 = 0;
        let header = proto::common::ResponseHeaderProto {
            client: Some(proto::common::ClientInfoProto {
                call_id: types::CallId::new().to_string(),
                client_id: 7,
                client_name: "test".to_string(),
            }),
            error: None,
            state: Vec::new(),
            group_id: INVALID_GROUP_ID,
            mount_epoch: None,
            route_epoch: None,
        };

        match parse_rpc_envelope(Ok(()), Some(&header)) {
            RpcEnvelope::CanonicalError(err) => {
                assert_eq!(err.class, ErrorClass::Fatal);
                assert!(matches!(
                    err.code,
                    Some(CanonicalErrorCode::RpcCode(RpcErrorCode::InvalidHeader))
                ));
                assert!(err.message.contains("group_id"));
            }
            _ => panic!("expected fatal canonical envelope"),
        }
    }
}
