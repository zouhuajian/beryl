// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Error classification for observability.

/// Unified error kind enumeration for metrics labeling.
///
/// This enum provides a fixed set of error categories to avoid high-cardinality
/// label values in metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    /// Operation succeeded.
    Ok,
    /// Resource not found.
    NotFound,
    /// Permission denied.
    PermissionDenied,
    /// Invalid argument.
    InvalidArgument,
    /// Operation not supported.
    Unsupported,
    /// Operation not implemented.
    NotImplemented,
    /// Deadline exceeded.
    DeadlineExceeded,
    /// Service overloaded.
    Overloaded,
    /// Service unavailable.
    Unavailable,
    /// Connection error.
    Connection,
    /// Timeout error.
    Timeout,
    /// Backpressure.
    Backpressure,
    /// Protocol error.
    Protocol,
    /// Serialization error.
    Serialization,
    /// Deserialization error.
    Deserialization,
    /// I/O error.
    Io,
    /// Internal error.
    Internal,
    /// Unknown error.
    Unknown,
}

impl ErrorKind {
    /// Convert to string for metrics labeling.
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorKind::Ok => "ok",
            ErrorKind::NotFound => "not_found",
            ErrorKind::PermissionDenied => "permission_denied",
            ErrorKind::InvalidArgument => "invalid_argument",
            ErrorKind::Unsupported => "unsupported",
            ErrorKind::NotImplemented => "not_implemented",
            ErrorKind::DeadlineExceeded => "deadline_exceeded",
            ErrorKind::Overloaded => "overloaded",
            ErrorKind::Unavailable => "unavailable",
            ErrorKind::Connection => "connection",
            ErrorKind::Timeout => "timeout",
            ErrorKind::Backpressure => "backpressure",
            ErrorKind::Protocol => "protocol",
            ErrorKind::Serialization => "serialization",
            ErrorKind::Deserialization => "deserialization",
            ErrorKind::Io => "io",
            ErrorKind::Internal => "internal",
            ErrorKind::Unknown => "unknown",
        }
    }
}

/// Classify a transport error into ErrorKind.
pub fn classify_transport_error(error_code: &str) -> ErrorKind {
    match error_code {
        "ok" => ErrorKind::Ok,
        "not_implemented" => ErrorKind::NotImplemented,
        "not_supported" => ErrorKind::Unsupported,
        "deadline_exceeded" => ErrorKind::DeadlineExceeded,
        "overloaded" => ErrorKind::Overloaded,
        "unavailable" => ErrorKind::Unavailable,
        "invalid_argument" => ErrorKind::InvalidArgument,
        "internal" => ErrorKind::Internal,
        "remote_status" => ErrorKind::Internal,
        "connection" => ErrorKind::Connection,
        "timeout" => ErrorKind::Timeout,
        "backpressure" => ErrorKind::Backpressure,
        "protocol" => ErrorKind::Protocol,
        "serialization" => ErrorKind::Serialization,
        "deserialization" => ErrorKind::Deserialization,
        "io" => ErrorKind::Io,
        "unknown" => ErrorKind::Unknown,
        _ => ErrorKind::Unknown,
    }
}
