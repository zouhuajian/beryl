#![deny(deprecated)]
// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Canonical error handling for client operations.
//!
//! This module provides a unified entry point for client error decision-making,
//! converting response headers to a structured `ClientAction` that preserves
//! canonical error details and refresh hints.

use crate::error::{ClientError, ClientResult};
use common::error::canonical::{CanonicalError, ErrorClass, RefreshReason};
use common::header::{RequestHeader, ResponseHeader, RpcErrorCode};
use proto::convert::error_detail_to_canonical;
use std::time::Duration;

/// Endpoint hint preserved on refresh actions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EndpointHint {
    /// Worker ID for the hint.
    pub worker_id: u64,
    /// Endpoint address.
    pub endpoint: String,
    /// Transport kind as proto enum raw value.
    pub net_transport_kind: i32,
    /// Worker epoch for the hinted endpoint.
    pub worker_epoch: u64,
}

impl From<proto::common::WorkerEndpointInfoProto> for EndpointHint {
    fn from(value: proto::common::WorkerEndpointInfoProto) -> Self {
        Self {
            worker_id: value.worker_id,
            endpoint: value.endpoint,
            net_transport_kind: value.net_transport_kind,
            worker_epoch: value.worker_epoch,
        }
    }
}

/// Structured refresh hints preserved from response headers.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RefreshHint {
    /// Group ID hint from metadata header.
    pub group_id: Option<u64>,
    /// Route epoch hint (if present in future headers).
    pub route_epoch: Option<u64>,
    /// Mount epoch hint from metadata header.
    pub mount_epoch: Option<u64>,
    /// Worker epoch hint from data header.
    pub worker_epoch: Option<u64>,
    /// Endpoint hint from data header.
    pub endpoint_hint: Option<EndpointHint>,
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
        hint: RefreshHint,
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

/// Unified RPC envelope parse outcome.
///
/// This models the contract:
/// - non-OK gRPC status => transport/framework failure
/// - gRPC OK + header.error => canonical business/protocol error
/// - gRPC OK + no header.error => success
#[derive(Clone, Debug)]
pub enum RpcEnvelope {
    /// gRPC OK and no canonical error in response header.
    Ok,
    /// gRPC non-OK transport/auth/framework error.
    TransportError(tonic::Status),
    /// gRPC OK with canonical business/protocol error.
    CanonicalError(CanonicalError),
}

/// Refresh dispatch context passed to the refresh action machine.
#[derive(Clone, Debug)]
pub struct RefreshDispatchContext {
    /// Refresh reason from canonical error.
    pub reason: RefreshReason,
    /// Parsed refresh hints for this response.
    pub hint: RefreshHint,
    /// Original canonical error.
    pub canonical: CanonicalError,
    /// Full response header that triggered refresh.
    pub response_header: ResponseHeader,
}

/// Bounded retry policy for the client action machine.
#[derive(Clone, Debug)]
pub struct RetryPolicy {
    /// Max refresh actions per operation.
    pub max_refresh_attempts: u32,
    /// Max retryable-class retries per operation.
    pub max_retryable_attempts: u32,
    /// Max transport retries for transient gRPC failures.
    pub max_transport_retries: u32,
    /// Base backoff for transport retries.
    pub transport_retry_base_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_refresh_attempts: 2,
            max_retryable_attempts: 1,
            max_transport_retries: 2,
            transport_retry_base_ms: 50,
        }
    }
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

    let canonical_hint = canonical.refresh_hint.as_ref();
    let hint = RefreshHint {
        group_id: canonical_hint.and_then(|hint| hint.group_id).or(header.group_id),
        route_epoch: canonical_hint.and_then(|hint| hint.route_epoch).or(header.route_epoch),
        mount_epoch: canonical_hint.and_then(|hint| hint.mount_epoch).or(header.mount_epoch),
        worker_epoch: canonical_hint.and_then(|hint| hint.worker_epoch),
        endpoint_hint: canonical_hint
            .and_then(|hint| hint.worker_endpoints.first())
            .map(|endpoint| EndpointHint {
                worker_id: endpoint.worker_id,
                endpoint: endpoint.endpoint.clone(),
                net_transport_kind: endpoint.net_transport_kind,
                worker_epoch: endpoint.worker_epoch,
            }),
    };
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
    let hint = RefreshHint {
        group_id: None,
        route_epoch: None,
        mount_epoch: None,
        worker_epoch: header.worker_epoch,
        endpoint_hint: header.endpoint_hint.clone().map(EndpointHint::from),
    };

    validate_canonical_with_hint(canonical, hint)
}

/// Parse gRPC status + response header into a unified envelope outcome.
///
/// `grpc_status` should be:
/// - `Ok(())` when the RPC transport returned gRPC OK and a response body.
/// - `Err(status)` when tonic returned a non-OK gRPC status.
pub fn parse_rpc_envelope(
    grpc_status: Result<(), tonic::Status>,
    response_header: Option<&proto::common::ResponseHeaderProto>,
) -> RpcEnvelope {
    match grpc_status {
        Err(status) => RpcEnvelope::TransportError(status),
        Ok(()) => {
            let Some(header) = response_header else {
                return RpcEnvelope::CanonicalError(CanonicalError {
                    class: ErrorClass::Fatal,
                    code: Some(common::error::canonical::ErrorCode::RpcCode(
                        RpcErrorCode::InvalidHeader,
                    )),
                    reason: None,
                    retry_after_ms: None,
                    message: "missing response header".to_string(),
                    refresh_hint: None,
                });
            };
            match header.error.as_ref() {
                None => RpcEnvelope::Ok,
                Some(err_detail) => {
                    let canonical = error_detail_to_canonical(err_detail);
                    if matches!(canonical.class, ErrorClass::Ok) {
                        RpcEnvelope::Ok
                    } else {
                        RpcEnvelope::CanonicalError(canonical)
                    }
                }
            }
        }
    }
}

/// Validate canonical error directly.
pub fn handle_canonical_error(err: &CanonicalError) -> Result<(), ClientAction> {
    validate_canonical_with_hint(err.clone(), RefreshHint::default())
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
                hint,
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

/// Outcome of a refresh-aware call.
#[derive(Clone, Debug)]
pub struct RetryOutcome<T> {
    /// Successful result returned by the final attempt.
    pub result: T,
    /// Count of refresh-triggered retries performed.
    pub refreshes: u32,
    /// Count of retry-after responses handled.
    pub retries: u32,
    /// Count of transport retries performed.
    pub transport_retries: u32,
    /// Last canonical error observed before success (if any).
    pub last_canonical_error: Option<CanonicalError>,
}

impl<T> RetryOutcome<T> {
    /// Create a new outcome with zero refresh/retry counters.
    pub fn new(result: T) -> Self {
        Self {
            result,
            refreshes: 0,
            retries: 0,
            transport_retries: 0,
            last_canonical_error: None,
        }
    }

    /// Map the inner result while preserving counters and error context.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> RetryOutcome<U> {
        RetryOutcome {
            result: f(self.result),
            refreshes: self.refreshes,
            retries: self.retries,
            transport_retries: self.transport_retries,
            last_canonical_error: self.last_canonical_error,
        }
    }
}

/// Retry with bounded refresh/retry/transport handling.
///
/// The refresh callback is the authoritative dispatch point that translates
/// refresh reasons into real refresh actions and produces a refreshed request
/// context for the next retry.
pub async fn retry_metadata_once<T, CallFut, RefreshFut>(
    header: RequestHeader,
    mut call: impl FnMut(RequestHeader) -> CallFut,
    mut dispatch_refresh: impl FnMut(RefreshDispatchContext, RequestHeader) -> RefreshFut,
) -> ClientResult<RetryOutcome<(ResponseHeader, T)>>
where
    CallFut: std::future::Future<Output = ClientResult<(ResponseHeader, T)>>,
    RefreshFut: std::future::Future<Output = ClientResult<RequestHeader>>,
{
    retry_metadata_with_policy(header, &mut call, &mut dispatch_refresh, RetryPolicy::default()).await
}

/// Same as [`retry_metadata_once`] with explicit retry policy.
pub async fn retry_metadata_with_policy<T, CallFut, RefreshFut>(
    header: RequestHeader,
    call: &mut impl FnMut(RequestHeader) -> CallFut,
    dispatch_refresh: &mut impl FnMut(RefreshDispatchContext, RequestHeader) -> RefreshFut,
    policy: RetryPolicy,
) -> ClientResult<RetryOutcome<(ResponseHeader, T)>>
where
    CallFut: std::future::Future<Output = ClientResult<(ResponseHeader, T)>>,
    RefreshFut: std::future::Future<Output = ClientResult<RequestHeader>>,
{
    let mut current_header = header;
    let mut refreshes = 0;
    let mut retries = 0;
    let mut transport_retries = 0;
    let mut last_canonical_error = None;

    loop {
        let rpc_result = call(current_header.clone()).await;
        let (resp_header, payload) = match rpc_result {
            Ok(ok) => ok,
            Err(err) => match err {
                ClientError::Action(action) => match *action {
                    ClientAction::TransportFail { status } => {
                        if transport_retries < policy.max_transport_retries && is_transient_transport_status(&status) {
                            let backoff_ms =
                                transport_backoff_ms(policy.transport_retry_base_ms, transport_retries + 1);
                            transport_retries += 1;
                            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                            current_header = current_header.child_with_same_call_id();
                            continue;
                        }
                        return Err(ClientError::from(ClientAction::TransportFail { status }));
                    }
                    other => return Err(ClientError::from(other)),
                },
                other => return Err(other),
            },
        };

        match validate_header_or_action(&resp_header) {
            Ok(()) => {
                return Ok(RetryOutcome {
                    result: (resp_header, payload),
                    refreshes,
                    retries,
                    transport_retries,
                    last_canonical_error,
                });
            }
            Err(ClientAction::Refresh {
                reason,
                hint,
                canonical,
            }) => {
                if refreshes >= policy.max_refresh_attempts {
                    return Err(ClientError::from(ClientAction::Refresh {
                        reason,
                        hint,
                        canonical,
                    }));
                }
                refreshes += 1;
                let canonical_value = canonical.as_ref().clone();
                last_canonical_error = Some(canonical_value.clone());
                current_header = dispatch_refresh(
                    RefreshDispatchContext {
                        reason,
                        hint,
                        canonical: canonical_value,
                        response_header: resp_header,
                    },
                    current_header,
                )
                .await?;
            }
            Err(ClientAction::Retry { after_ms, canonical }) => {
                if retries >= policy.max_retryable_attempts {
                    return Err(ClientError::from(ClientAction::Retry { after_ms, canonical }));
                }
                retries += 1;
                last_canonical_error = Some(canonical.as_ref().clone());
                if let Some(delay) = after_ms {
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
                current_header = current_header.child_with_same_call_id();
            }
            Err(ClientAction::Fail { canonical }) => {
                return Err(ClientError::from(ClientAction::Fail { canonical }));
            }
            Err(ClientAction::TransportFail { status }) => {
                if transport_retries < policy.max_transport_retries && is_transient_transport_status(&status) {
                    let backoff_ms = transport_backoff_ms(policy.transport_retry_base_ms, transport_retries + 1);
                    transport_retries += 1;
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    current_header = current_header.child_with_same_call_id();
                    continue;
                }
                return Err(ClientError::from(ClientAction::TransportFail { status }));
            }
            Err(ClientAction::Ok) => {
                debug_assert!(false, "ClientAction::Ok should never be returned as error");
                return Err(ClientError::Metadata("invalid client action state".to_string()));
            }
        }
    }
}

fn is_transient_transport_status(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded | tonic::Code::ResourceExhausted
    )
}

fn transport_backoff_ms(base_ms: u64, attempt: u32) -> u64 {
    let shift = attempt.saturating_sub(1).min(4);
    base_ms.saturating_mul(1u64 << shift)
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
        ClientError::Action(Box::new(action))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::error::canonical::ErrorCode as CanonicalErrorCode;
    use common::header::RpcErrorCode;
    use common::header::{ClientInfo, ResponseHeader};
    use proto::convert::canonical_to_error_detail;

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
            RpcEnvelope::TransportError(status) => assert_eq!(status.code(), tonic::Code::Unavailable),
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
            group_id: 0,
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

    #[tokio::test]
    async fn retry_loop_retries_transient_transport_without_refresh_dispatch() {
        let mut calls = 0usize;
        let mut refresh_dispatch_calls = 0usize;
        let header = RequestHeader::new(types::ClientId::new(1));

        let mut call = |hdr: RequestHeader| {
            let _ = hdr;
            calls += 1;
            async move {
                if calls == 1 {
                    Err(ClientError::from(tonic::Status::unavailable("temporary outage")))
                } else {
                    Ok((ResponseHeader::ok(ClientInfo::new(types::ClientId::new(1))), ()))
                }
            }
        };

        let mut dispatch_refresh = |ctx: RefreshDispatchContext, req: RequestHeader| {
            let _ = (ctx, req);
            refresh_dispatch_calls += 1;
            async move { Ok(RequestHeader::new(types::ClientId::new(1))) }
        };

        let policy = RetryPolicy {
            max_refresh_attempts: 1,
            max_retryable_attempts: 1,
            max_transport_retries: 1,
            transport_retry_base_ms: 0,
        };

        let outcome = retry_metadata_with_policy(header, &mut call, &mut dispatch_refresh, policy)
            .await
            .expect("retry succeeds");
        assert_eq!(calls, 2);
        assert_eq!(refresh_dispatch_calls, 0);
        assert_eq!(outcome.transport_retries, 1);
        assert_eq!(outcome.refreshes, 0);
        assert_eq!(outcome.retries, 0);
    }
}
