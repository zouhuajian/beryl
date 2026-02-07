// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Error types for UFS operations.

use thiserror::Error;

/// Errors that can occur during UFS operations.
#[derive(Error, Debug)]
pub enum UfsError {
    /// The requested path or resource was not found.
    #[error("not found: {0}")]
    NotFound(String),

    /// Permission denied for the operation.
    #[error("permission denied: {0}")]
    PermissionDenied(String),

    /// The operation is not supported by the backend.
    #[error("unsupported operation: {0}")]
    Unsupported(String),

    /// The operation is not implemented.
    #[error("not implemented: {0}")]
    NotImplemented(String),

    /// Invalid UFS specification.
    #[error("invalid spec: {0}")]
    InvalidSpec(String),

    /// Backend-specific error from OpenDAL.
    #[error("backend error: {0}")]
    Backend(opendal::Error),

    /// Invalid path or range.
    #[error("invalid path or range: {0}")]
    InvalidPath(String),

    /// Unexpected end of file (short read).
    #[error("unexpected eof: expected {expected} bytes, got {actual}")]
    UnexpectedEof { expected: usize, actual: usize },

    /// Internal error (e.g., concurrency limit, timeout).
    #[error("internal error: {0}")]
    Internal(String),

    /// Overloaded - too many concurrent requests.
    #[error("overloaded: {0}")]
    Overloaded(String),
}

impl UfsError {
    /// Check if the error is a NotFound error.
    pub fn is_not_found(&self) -> bool {
        matches!(self, UfsError::NotFound(_))
    }

    /// Check if the error is a PermissionDenied error.
    pub fn is_permission_denied(&self) -> bool {
        matches!(self, UfsError::PermissionDenied(_))
    }
}

impl From<opendal::Error> for UfsError {
    fn from(err: opendal::Error) -> Self {
        use opendal::ErrorKind;
        match err.kind() {
            ErrorKind::NotFound => UfsError::NotFound(err.to_string()),
            ErrorKind::PermissionDenied => UfsError::PermissionDenied(err.to_string()),
            _ => UfsError::Backend(err),
        }
    }
}

impl From<common::CommonError> for UfsError {
    fn from(err: common::CommonError) -> Self {
        use common::CommonErrorCode;
        match err.code {
            CommonErrorCode::Overloaded => UfsError::Overloaded(err.message),
            CommonErrorCode::Timeout => UfsError::Internal(format!("Timeout: {}", err.message)),
            _ => UfsError::Internal(err.message),
        }
    }
}

impl From<UfsError> for common::CommonError {
    fn from(err: UfsError) -> Self {
        use common::CommonErrorCode;
        match &err {
            UfsError::NotFound(_) => common::CommonError::new(CommonErrorCode::NotFound, err.to_string()),
            UfsError::PermissionDenied(_) => {
                common::CommonError::new(CommonErrorCode::PermissionDenied, err.to_string())
            }
            UfsError::Overloaded(_) => common::CommonError::new(CommonErrorCode::Overloaded, err.to_string()),
            UfsError::InvalidPath(_) | UfsError::InvalidSpec(_) => {
                common::CommonError::new(CommonErrorCode::InvalidArgument, err.to_string())
            }
            UfsError::Unsupported(_) | UfsError::NotImplemented(_) => {
                common::CommonError::new(CommonErrorCode::Internal, err.to_string())
            }
            UfsError::Backend(_) | UfsError::Internal(_) | UfsError::UnexpectedEof { .. } => {
                common::CommonError::new(CommonErrorCode::Internal, err.to_string())
            }
        }
    }
}
