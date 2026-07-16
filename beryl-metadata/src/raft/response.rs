// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Responses produced by committed metadata Raft commands.

use crate::error::MetadataError;
use beryl_types::fs::{FsErrorCode, InodeId};
use beryl_types::ids::{DataHandleId, WorkerId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Application-level response propagated from the state machine to the proposer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum AppDataResponse {
    /// Filesystem command result with errno fidelity.
    Fs(FsCommandResult),
    /// Mount-related command result.
    Mount(MountCommandResult),
    /// Worker-related command result.
    Worker(WorkerCommandResult),
    /// Deterministic application rejection committed by the state machine.
    Rejected(ApplyRejection),
    /// Explicitly empty result.
    None,
}

/// Recoverable error kinds that may be committed as deterministic apply results.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum ApplyRejectionKind {
    NotFound,
    AlreadyExists,
    InvalidArgument,
    NotDir,
    IsDir,
    DirectoryNotEmpty,
    CrossMountRename,
    PermissionDenied,
    NotSupported,
    Busy,
    ActiveWorkerConflict,
    Again,
    LeaseFenced { expected: u64, got: u64 },
}

/// Recoverable application failure stored in dedup state and returned through Raft.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ApplyRejection {
    pub kind: ApplyRejectionKind,
    pub message: String,
}

impl ApplyRejection {
    pub(crate) fn from_metadata_error(error: MetadataError) -> Result<Self, FatalApplyError> {
        let rejection = match error {
            MetadataError::NotFound(message) => Self {
                kind: ApplyRejectionKind::NotFound,
                message,
            },
            MetadataError::AlreadyExists(message) => Self {
                kind: ApplyRejectionKind::AlreadyExists,
                message,
            },
            MetadataError::InvalidArgument(message) => Self {
                kind: ApplyRejectionKind::InvalidArgument,
                message,
            },
            MetadataError::NotDir(message) => Self {
                kind: ApplyRejectionKind::NotDir,
                message,
            },
            MetadataError::IsDir(message) => Self {
                kind: ApplyRejectionKind::IsDir,
                message,
            },
            MetadataError::DirectoryNotEmpty(message) => Self {
                kind: ApplyRejectionKind::DirectoryNotEmpty,
                message,
            },
            MetadataError::CrossMountRename(message) => Self {
                kind: ApplyRejectionKind::CrossMountRename,
                message,
            },
            MetadataError::PermissionDenied(message) => Self {
                kind: ApplyRejectionKind::PermissionDenied,
                message,
            },
            MetadataError::NotSupported(message) => Self {
                kind: ApplyRejectionKind::NotSupported,
                message,
            },
            MetadataError::Busy(message) => Self {
                kind: ApplyRejectionKind::Busy,
                message,
            },
            MetadataError::ActiveWorkerConflict(message) => Self {
                kind: ApplyRejectionKind::ActiveWorkerConflict,
                message,
            },
            MetadataError::Again(message) => Self {
                kind: ApplyRejectionKind::Again,
                message,
            },
            MetadataError::LeaseFenced { expected, got } => Self {
                kind: ApplyRejectionKind::LeaseFenced { expected, got },
                message: format!("lease fenced: expected epoch >= {expected}, got {got}"),
            },
            fatal @ (MetadataError::LeaderChanged(_)
            | MetadataError::EpochMismatch { .. }
            | MetadataError::MountEpochMismatch { .. }
            | MetadataError::RoutingStale(_)
            | MetadataError::StaleState(_)
            | MetadataError::FullReportRequired(_)
            | MetadataError::Internal(_)
            | MetadataError::ServiceUnavailable(_)) => return Err(FatalApplyError(fatal)),
        };
        Ok(rejection)
    }

    pub fn into_metadata_error(self) -> MetadataError {
        match self.kind {
            ApplyRejectionKind::NotFound => MetadataError::NotFound(self.message),
            ApplyRejectionKind::AlreadyExists => MetadataError::AlreadyExists(self.message),
            ApplyRejectionKind::InvalidArgument => MetadataError::InvalidArgument(self.message),
            ApplyRejectionKind::NotDir => MetadataError::NotDir(self.message),
            ApplyRejectionKind::IsDir => MetadataError::IsDir(self.message),
            ApplyRejectionKind::DirectoryNotEmpty => MetadataError::DirectoryNotEmpty(self.message),
            ApplyRejectionKind::CrossMountRename => MetadataError::CrossMountRename(self.message),
            ApplyRejectionKind::PermissionDenied => MetadataError::PermissionDenied(self.message),
            ApplyRejectionKind::NotSupported => MetadataError::NotSupported(self.message),
            ApplyRejectionKind::Busy => MetadataError::Busy(self.message),
            ApplyRejectionKind::ActiveWorkerConflict => MetadataError::ActiveWorkerConflict(self.message),
            ApplyRejectionKind::Again => MetadataError::Again(self.message),
            ApplyRejectionKind::LeaseFenced { expected, got } => MetadataError::LeaseFenced { expected, got },
        }
    }
}

/// Infrastructure or invariant failure that must fail committed apply closed.
#[derive(Debug, Error)]
#[error("fatal metadata Raft apply error: {0}")]
pub(crate) struct FatalApplyError(MetadataError);

impl FatalApplyError {
    pub(crate) fn new(error: MetadataError) -> Self {
        Self(error)
    }

    pub(crate) fn into_inner(self) -> MetadataError {
        self.0
    }

    pub(crate) fn as_inner(&self) -> &MetadataError {
        &self.0
    }
}

/// Filesystem apply result returned synchronously via Raft.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum FsCommandResult {
    Ok(FsOkResult),
    Err(FsErrnoResult),
}

impl FsCommandResult {
    pub fn ok() -> Self {
        FsCommandResult::Ok(FsOkResult::default())
    }
}

/// Successful FS command payload (minimal for now; extensible).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
pub(crate) struct FsOkResult {
    pub inode_id: Option<InodeId>,
    pub data_handle_id: Option<DataHandleId>,
    pub file_version: Option<u64>,
}

/// FS errno surfaced by apply.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct FsErrnoResult {
    pub errno: FsErrorCode,
    pub message: String,
}

/// Mount command result.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum MountCommandResult {
    Upserted(crate::mount::MountEntry),
}

/// Worker command result.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum WorkerCommandResult {
    Upserted(WorkerId),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::MetadataError;

    #[test]
    fn recoverable_apply_errors_round_trip_without_becoming_internal() {
        let rejection = ApplyRejection::from_metadata_error(MetadataError::NotFound("inode 7".to_string())).unwrap();

        assert!(matches!(
            rejection.into_metadata_error(),
            MetadataError::NotFound(message) if message == "inode 7"
        ));
    }

    #[test]
    fn internal_apply_error_is_fatal() {
        let fatal =
            ApplyRejection::from_metadata_error(MetadataError::Internal("decode failed".to_string())).unwrap_err();

        assert!(matches!(
            fatal.into_inner(),
            MetadataError::Internal(message) if message == "decode failed"
        ));
    }

    #[test]
    fn lease_fencing_round_trip_preserves_epochs() {
        let rejection =
            ApplyRejection::from_metadata_error(MetadataError::LeaseFenced { expected: 11, got: 9 }).unwrap();

        assert!(matches!(
            rejection.into_metadata_error(),
            MetadataError::LeaseFenced { expected: 11, got: 9 }
        ));
    }
}
