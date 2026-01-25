#![deny(deprecated)]
// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors


//! Canonical error handling for client operations.
//!
//! This module provides a unified entry point for client error decision-making,
//! converting ResponseHeader to a canonical ClientAction that the client can
//! use to determine refresh/retry/fail behavior.
//!
//! CURRENT IMPLEMENTATION:
//! - Reads from `ResponseHeader` which is converted from `ResponseHeaderProto.error` (ErrorDetailProto)
//! - All error semantics come from the single canonical error field

use crate::error::ClientError;
use common::error::canonical::{CanonicalError, ErrorClass, ErrorCode as CanonicalErrorCode, RefreshReason};
use common::header::{RequestHeader, ResponseHeader, RpcErrorCode, RpcStatus};

/// Client action determined from response header.
#[derive(Clone, Debug)]
pub enum ClientAction {
    /// Client must refresh routing/mount/state before retrying.
    Refresh(RefreshReason),
    /// Client can retry after the specified delay (milliseconds).
    Retry {
        /// Retry after delay in milliseconds (None = immediate retry).
        after_ms: Option<u64>,
    },
    /// Unrecoverable error; client should fail.
    Fail(CanonicalError),
}

/// Handle response header and return client action.
///
/// This is the **canonical entry point** for client error decision-making.
/// All client code should use this function to determine behavior from
/// ResponseHeader, ensuring consistent semantics across the codebase.
///
/// This function reads ONLY from `ResponseHeaderProto.error` (ErrorDetailProto),
/// which is the single source of truth for all error semantics.
///
/// For gRPC non-OK status codes, these are treated as transport/auth/framework
/// errors and mapped to appropriate CanonicalError.
pub fn handle_response_header(header: &common::header::ResponseHeader) -> Result<(), ClientAction> {
    match header.status {
        RpcStatus::Ok => {
            debug_assert!(
                header.canonical_error.is_none(),
                "RpcStatus::Ok must not carry canonical_error"
            );
            Ok(())
        }
        RpcStatus::Error | RpcStatus::Fatal => {
            let canonical = match header.canonical_error.clone() {
                Some(canonical) => canonical,
                None => {
                    debug_assert!(
                        false,
                        "non-OK response header missing canonical_error; treating as fatal"
                    );
                    CanonicalError {
                        class: ErrorClass::Fatal,
                        code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Application)),
                        reason: None,
                        retry_after_ms: None,
                        message: format!("response status {:?} missing canonical_error", header.status),
                    }
                }
            };
            handle_canonical(&canonical)
        }
    }
}

/// Handle canonical error directly (data-plane helpers convert to CanonicalError).
pub fn handle_canonical_error(err: &CanonicalError) -> Result<(), ClientAction> {
    handle_canonical(err)
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
            last_canonical_error: None,
        }
    }

    /// Map the inner result while preserving counters and error context.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> RetryOutcome<U> {
        RetryOutcome {
            result: f(self.result),
            refreshes: self.refreshes,
            retries: self.retries,
            last_canonical_error: self.last_canonical_error,
        }
    }
}

/// Retry once on NEED_REFRESH using canonical_error as the only control source.
///
/// - Executes `call` with the provided `RequestHeader`
/// - On NEED_REFRESH, invokes `refresh` to update the header using hints from the response,
///   then retries once.
/// - On RETRYABLE, performs a single bounded retry with optional backoff from retry_after_ms.
pub async fn retry_metadata_once<T, CallFut, RefreshFut>(
    header: RequestHeader,
    mut call: impl FnMut(RequestHeader) -> CallFut,
    mut refresh: impl FnMut(RefreshReason, &ResponseHeader) -> RefreshFut,
) -> crate::error::ClientResult<RetryOutcome<(ResponseHeader, T)>>
where
    CallFut: std::future::Future<Output = crate::error::ClientResult<(ResponseHeader, T)>>,
    RefreshFut: std::future::Future<Output = crate::error::ClientResult<RequestHeader>>,
{
    let mut refreshes = 0;
    let mut retries = 0;
    let mut last_canonical_error = None;

    // First attempt
    let (resp_header, payload) = call(header.clone()).await?;
    match handle_response_header(&resp_header) {
        Ok(()) => {
            return Ok(RetryOutcome {
                result: (resp_header, payload),
                refreshes,
                retries,
                last_canonical_error,
            });
        }
        Err(ClientAction::Refresh(reason)) => {
            last_canonical_error = resp_header.canonical_error.clone();
            refreshes = 1;
            let refreshed_header = refresh(reason, &resp_header).await?;
            let (resp_header, payload) = call(refreshed_header).await?;
            match handle_response_header(&resp_header) {
                Ok(()) => {
                    return Ok(RetryOutcome {
                        result: (resp_header, payload),
                        refreshes,
                        retries,
                        last_canonical_error,
                    });
                }
                Err(action) => return Err(action_to_client_error(action)),
            }
        }
        Err(ClientAction::Retry { after_ms }) => {
            last_canonical_error = resp_header.canonical_error.clone();
            retries = 1;
            if let Some(delay) = after_ms {
                tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
            }
            let (resp_header, payload) = call(header.child_with_same_call_id()).await?;
            match handle_response_header(&resp_header) {
                Ok(()) => {
                    return Ok(RetryOutcome {
                        result: (resp_header, payload),
                        refreshes,
                        retries,
                        last_canonical_error,
                    });
                }
                Err(action) => return Err(action_to_client_error(action)),
            }
        }
        Err(ClientAction::Fail(err)) => Err(crate::error::ClientError::Metadata(err.message)),
    }
}

fn handle_canonical(err: &CanonicalError) -> Result<(), ClientAction> {
    match err.class {
        ErrorClass::Ok => Ok(()),
        ErrorClass::NeedRefresh => Err(ClientAction::Refresh(err.reason.unwrap_or(RefreshReason::Unknown))),
        ErrorClass::Retryable => Err(ClientAction::Retry {
            after_ms: err.retry_after_ms,
        }),
        ErrorClass::Fatal => Err(ClientAction::Fail(err.clone())),
    }
}

fn action_to_client_error(action: ClientAction) -> ClientError {
    match action {
        ClientAction::Refresh(reason) => ClientError::NeedRefresh(format!("refresh required: {:?}", reason)),
        ClientAction::Retry { after_ms } => {
            ClientError::Metadata(format!("retry requested after {}ms", after_ms.unwrap_or(0)))
        }
        ClientAction::Fail(err) => ClientError::Metadata(err.message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::header::{ClientInfo, ResponseHeader};

    #[test]
    fn test_handle_ok_header() {
        let header = ResponseHeader::ok(ClientInfo::new(types::ClientId::new(1)));
        let result = handle_response_header(&header);
        assert!(result.is_ok());
    }

    #[test]
    fn test_handle_not_leader() {
        let canonical = CanonicalError::need_refresh(RpcErrorCode::NotLeader, RefreshReason::NotLeader, "not leader");
        let header = ResponseHeader::error(ClientInfo::new(types::ClientId::new(1)), canonical);
        let result = handle_response_header(&header);
        match result {
            Err(ClientAction::Refresh(RefreshReason::NotLeader)) => {}
            _ => panic!("expected Refresh(NotLeader)"),
        }
    }

    #[test]
    fn test_handle_retryable() {
        let canonical = CanonicalError::retryable(RpcErrorCode::NodeUnavailable, Some(1000), "unavailable");
        let header = ResponseHeader::error(ClientInfo::new(types::ClientId::new(1)), canonical);
        let result = handle_response_header(&header);
        match result {
            Err(ClientAction::Retry { after_ms: Some(1000) }) => {}
            // Note: use plain string here to avoid format-string `{}` parsing.
            _ => panic!("expected Retry with after_ms = Some(1000)"),
        }
    }

    #[test]
    fn test_handle_fatal() {
        let header = ResponseHeader::error(
            ClientInfo::new(types::ClientId::new(1)),
            CanonicalError {
                class: ErrorClass::Fatal,
                code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Application)),
                reason: None,
                retry_after_ms: None,
                message: "fatal error".to_string(),
            },
        );
        let result = handle_response_header(&header);
        match result {
            Err(ClientAction::Fail(err)) => {
                assert_eq!(err.class, ErrorClass::Fatal);
            }
            _ => panic!("expected Fail"),
        }
    }
}
