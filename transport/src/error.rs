// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Transport layer errors.

use common::error::canonical::{CanonicalError, ErrorClass, ErrorCode as CanonicalErrorCode};
use common::header::RpcErrorCode;
use thiserror::Error;

/// Unified transport error type with gRPC status code mapping.
#[derive(Debug, Error)]
pub enum TransportError {
    #[error("not implemented: {0}")]
    NotImplemented(String),

    #[error("not supported: {0}")]
    NotSupported(String),

    #[error("deadline exceeded: {0}")]
    DeadlineExceeded(String),

    #[error("overloaded: {0}")]
    Overloaded(String),

    #[error("unavailable: {0}")]
    Unavailable(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("internal error: {0}")]
    Internal(String),

    #[error("remote status error: code={code}, message={message}")]
    RemoteStatus { code: u32, message: String },

    #[error("connection error: {0}")]
    Connection(String),

    #[error("timeout: {0}")]
    Timeout(String),

    #[error("backpressure: {0}")]
    Backpressure(String),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("deserialization error: {0}")]
    Deserialization(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("unknown error: {0}")]
    Unknown(String),
}

impl TransportError {
    /// Check if the error is retryable (for retry policy).
    pub fn is_retryable(&self) -> bool {
        match self {
            TransportError::Unavailable(_)
            | TransportError::DeadlineExceeded(_)
            | TransportError::Timeout(_)
            | TransportError::Connection(_) => true,
            TransportError::RemoteStatus { code, .. } => matches_retryable_grpc_code(*code),
            _ => false,
        }
    }

    /// Get a short error code for metrics labeling (avoid large strings).
    pub fn error_code(&self) -> &'static str {
        match self {
            TransportError::NotImplemented(_) => "not_implemented",
            TransportError::NotSupported(_) => "not_supported",
            TransportError::DeadlineExceeded(_) => "deadline_exceeded",
            TransportError::Overloaded(_) => "overloaded",
            TransportError::Unavailable(_) => "unavailable",
            TransportError::InvalidArgument(_) => "invalid_argument",
            TransportError::Internal(_) => "internal",
            TransportError::RemoteStatus { .. } => "remote_status",
            TransportError::Connection(_) => "connection",
            TransportError::Timeout(_) => "timeout",
            TransportError::Backpressure(_) => "backpressure",
            TransportError::Protocol(_) => "protocol",
            TransportError::Serialization(_) => "serialization",
            TransportError::Deserialization(_) => "deserialization",
            TransportError::Io(_) => "io",
            TransportError::Unknown(_) => "unknown",
        }
    }
}

/// Map gRPC status code to retryable check.
fn matches_retryable_grpc_code(code: u32) -> bool {
    // gRPC status codes: UNAVAILABLE=14, DEADLINE_EXCEEDED=4, RESOURCE_EXHAUSTED=8
    matches!(code, 4 | 8 | 14)
}

/// Convert tonic::Status to TransportError.
#[cfg(feature = "grpc")]
impl From<tonic::Status> for TransportError {
    fn from(status: tonic::Status) -> Self {
        use tonic::Code;
        let code = status.code() as u32;
        let message = status.message().to_string();

        match status.code() {
            Code::NotFound => TransportError::NotSupported(format!("resource not found: {}", message)),
            Code::InvalidArgument => TransportError::InvalidArgument(message),
            Code::DeadlineExceeded => TransportError::DeadlineExceeded(message),
            Code::ResourceExhausted => TransportError::Overloaded(message),
            Code::Unavailable => TransportError::Unavailable(message),
            Code::Internal => TransportError::Internal(message),
            Code::Unimplemented => TransportError::NotImplemented(message),
            _ => TransportError::RemoteStatus { code, message },
        }
    }
}

pub type TransportResult<T> = Result<T, TransportError>;

impl From<TransportError> for CanonicalError {
    fn from(err: TransportError) -> Self {
        let is_retryable = err.is_retryable();
        let msg = err.to_string();
        match err {
            TransportError::Unavailable(_)
            | TransportError::DeadlineExceeded(_)
            | TransportError::Timeout(_)
            | TransportError::Connection(_) => {
                CanonicalError::retryable(RpcErrorCode::NodeUnavailable, Some(1000), msg)
            }
            TransportError::Overloaded(_) => CanonicalError::retryable(RpcErrorCode::Application, Some(5000), msg),
            TransportError::RemoteStatus { code, message } => {
                if matches_retryable_grpc_code(code) {
                    CanonicalError::retryable(RpcErrorCode::NodeUnavailable, Some(1000), message)
                } else {
                    CanonicalError {
                        class: ErrorClass::Fatal,
                        code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Application)),
                        reason: None,
                        retry_after_ms: None,
                        message,
                    }
                }
            }
            _ => CanonicalError {
                class: if is_retryable {
                    ErrorClass::Retryable
                } else {
                    ErrorClass::Fatal
                },
                code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Application)),
                reason: None,
                retry_after_ms: if is_retryable { Some(1000) } else { None },
                message: msg,
            },
        }
    }
}

/// Local I/O engine errors.
#[derive(Debug, Error)]
pub enum IoError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("not implemented: {0}")]
    NotImplemented(String),

    #[error("not supported: {0}")]
    NotSupported(String),

    #[error("unexpected eof")]
    UnexpectedEof,

    #[error("unknown error: {0}")]
    Unknown(String),
}

pub type IoResult<T> = Result<T, IoError>;
