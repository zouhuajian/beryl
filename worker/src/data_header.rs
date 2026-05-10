// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Data-plane header types for worker operations.
//!
//! This module provides Rust domain types for data-plane specific headers
//! (DataRequestHeaderProto/DataResponseHeaderProto) used in OpenReadStream/OpenWriteStream operations.
//!
//! NOTE: Error semantics are now unified via common.ErrorDetailProto, matching control-plane ResponseHeaderProto.

use common::error::canonical::{CanonicalError, ErrorClass, RefreshReason};
use common::header::ClientInfo;
use proto::worker::{DataRequestHeaderProto, DataResponseHeaderProto};

/// Request header for data-plane operations (OpenReadStream/OpenWriteStream).
#[derive(Clone, Debug)]
pub struct DataRequestHeader {
    /// Client information (call_id, client_id, client_name).
    pub client: ClientInfo,
    /// W3C Trace Context: traceparent header value.
    pub traceparent: Option<String>,
}

/// Response header for data-plane operations (OpenReadStream/OpenWriteStream).
/// Uses the same canonical error model as control-plane ResponseHeaderProto.
#[derive(Clone, Debug)]
pub struct DataResponseHeader {
    /// Client information (call_id, client_id, client_name).
    pub client: ClientInfo,
    /// Canonical error detail (single source of truth for all error semantics).
    /// If None or error_class = OK, the operation succeeded.
    pub error: Option<CanonicalError>,
}

impl DataRequestHeader {
    /// Create a new DataRequestHeader from ClientInfo.
    pub fn new(client: ClientInfo) -> Self {
        Self {
            client,
            traceparent: None,
        }
    }

    /// Set the traceparent.
    pub fn with_traceparent(mut self, traceparent: String) -> Self {
        self.traceparent = Some(traceparent);
        self
    }
}

impl DataResponseHeader {
    /// Create a successful response header.
    pub fn ok(client: ClientInfo) -> Self {
        Self { client, error: None }
    }

    /// Create a NEED_REFRESH response header with reason and message.
    pub fn need_refresh(
        client: ClientInfo,
        reason: RefreshReason,
        rpc_code: common::header::RpcErrorCode,
        message: String,
    ) -> Self {
        Self {
            client,
            error: Some(CanonicalError::need_refresh(rpc_code, reason, message)),
        }
    }

    /// Create a RETRYABLE response header with retry_after_ms and message.
    pub fn retryable(
        client: ClientInfo,
        rpc_code: common::header::RpcErrorCode,
        retry_after_ms: Option<u64>,
        message: String,
    ) -> Self {
        Self {
            client,
            error: Some(CanonicalError::retryable(rpc_code, retry_after_ms, message)),
        }
    }

    /// Create a FATAL response header with message.
    pub fn fatal(client: ClientInfo, rpc_code: common::header::RpcErrorCode, message: String) -> Self {
        Self {
            client,
            error: Some(CanonicalError {
                class: ErrorClass::Fatal,
                code: Some(common::error::canonical::ErrorCode::RpcCode(rpc_code)),
                reason: None,
                retry_after_ms: None,
                message,
                refresh_hint: None,
            }),
        }
    }
}

// ============================================================================
// Proto Conversions
// ============================================================================

impl DataRequestHeader {
    /// Convert from proto.
    pub fn from_proto(proto: DataRequestHeaderProto) -> Result<Self, String> {
        let client = proto
            .client
            .clone()
            .ok_or_else(|| "missing client".to_string())?
            .try_into()
            .map_err(|e: String| format!("Failed to convert ClientInfoProto: {}", e))?;
        let traceparent = if proto.traceparent.is_empty() {
            None
        } else {
            Some(proto.traceparent)
        };

        Ok(DataRequestHeader { client, traceparent })
    }

    /// Convert to proto.
    pub fn to_proto(&self) -> DataRequestHeaderProto {
        DataRequestHeaderProto {
            client: Some((&self.client).into()),
            traceparent: self.traceparent.clone().unwrap_or_default(),
        }
    }
}

impl DataResponseHeader {
    /// Convert from proto.
    pub fn from_proto(proto: DataResponseHeaderProto) -> Result<Self, String> {
        let client = proto
            .client
            .clone()
            .ok_or_else(|| "missing client".to_string())?
            .try_into()
            .map_err(|e: String| format!("Failed to convert ClientInfoProto: {}", e))?;

        let error = proto.error.as_ref().map(proto::convert::error_detail_to_canonical);

        Ok(DataResponseHeader { client, error })
    }

    /// Convert to proto.
    pub fn to_proto(&self) -> DataResponseHeaderProto {
        let error_detail = self.error.as_ref().map(proto::convert::canonical_to_error_detail);

        DataResponseHeaderProto {
            client: Some((&self.client).into()),
            error: error_detail,
        }
    }
}
