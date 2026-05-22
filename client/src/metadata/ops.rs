// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata operation request types.

/// GetStatus request operation.
pub type GetStatusOp = proto::metadata::GetStatusRequestProto;

/// ListStatus request operation.
pub type ListStatusOp = proto::metadata::ListStatusRequestProto;

/// Delete request operation.
pub type DeleteOp = proto::metadata::DeleteRequestProto;

/// Rename request operation.
pub type RenameOp = proto::metadata::RenameRequestProto;

/// OpenFile request operation.
pub type OpenFileOp = proto::metadata::OpenFileRequestProto;

/// GetBlockLocations request operation.
pub type GetBlockLocationsOp = proto::metadata::GetBlockLocationsRequestProto;

/// CreateFile request operation.
pub type CreateFileOp = proto::metadata::CreateFileRequestProto;

/// AppendFile request operation.
pub type AppendFileOp = proto::metadata::AppendFileRequestProto;

/// AddBlock request operation.
pub type AddBlockOp = proto::metadata::AddBlockRequestProto;

/// CommitFile request operation.
pub type CommitFileOp = proto::metadata::CommitFileRequestProto;

/// AbortFileWrite request operation.
pub type AbortFileWriteOp = proto::metadata::AbortFileWriteRequestProto;

/// RenewLease request operation.
pub type RenewLeaseOp = proto::metadata::RenewLeaseRequestProto;

/// SyncWrite request operation.
pub type SyncWriteOp = proto::metadata::SyncWriteRequestProto;

/// Msync request operation.
pub type MsyncOp = proto::metadata::MsyncRequestProto;
