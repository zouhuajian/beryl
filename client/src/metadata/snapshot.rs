// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata operation result types.

/// File status snapshot returned by metadata.
pub type StatusSnapshot = proto::metadata::GetStatusResponseProto;

/// Directory listing snapshot returned by metadata.
pub type ListSnapshot = proto::metadata::ListStatusResponseProto;

/// Delete result returned by metadata.
pub type DeleteResult = proto::metadata::DeleteResponseProto;

/// Rename result returned by metadata.
pub type RenameResult = proto::metadata::RenameResponseProto;

/// Read-open file snapshot returned by metadata.
pub type FileSnapshot = proto::metadata::OpenFileResponseProto;

/// File block layout snapshot returned by metadata.
pub type LayoutSnapshot = proto::metadata::GetBlockLocationsResponseProto;

/// Write-session seed returned by create or append.
#[derive(Clone, Debug)]
pub enum WriteSessionSeed {
    /// CreateFile response.
    Create(proto::metadata::CreateFileResponseProto),
    /// AppendFile response.
    Append(proto::metadata::AppendFileResponseProto),
}

/// Write target returned by AddBlock with its owner group.
#[derive(Clone, Debug)]
pub struct AddBlockResult {
    /// Metadata owner group for the block target.
    pub group_id: u64,
    /// Worker target for this block.
    pub target: proto::metadata::WriteTargetProto,
}

/// CommitFile result returned by metadata.
pub type CommitFileResult = proto::metadata::CommitFileResponseProto;

/// AbortFileWrite result returned by metadata.
pub type AbortFileWriteResult = proto::metadata::AbortFileWriteResponseProto;

/// RenewLease result returned by metadata.
pub type RenewLeaseResult = proto::metadata::RenewLeaseResponseProto;

/// Metadata state watermark returned by Msync.
pub type StateWatermark = proto::common::GroupStateWatermarkProto;
