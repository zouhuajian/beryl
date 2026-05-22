// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata operation result types.

use crate::error::{ClientError, ClientResult};
use types::{DataHandleId, FileBlockLocation, InodeId, WriteTarget};

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
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LayoutSnapshot {
    /// Metadata owner group from the validated response header.
    pub group_id: u64,
    /// Inode identity this layout belongs to.
    pub inode_id: InodeId,
    /// Data handle identity this layout belongs to.
    pub data_handle_id: DataHandleId,
    /// Authoritative file size at this layout version.
    pub file_size: u64,
    /// Durable visible file-state version for this read plan.
    pub file_version: Option<u64>,
    /// Metadata-authoritative block locations for the requested range.
    pub locations: Vec<FileBlockLocation>,
}

impl LayoutSnapshot {
    /// Convert a metadata wire response into the client read-layout domain view.
    pub(crate) fn from_proto(
        group_id: u64,
        response: proto::metadata::GetBlockLocationsResponseProto,
    ) -> ClientResult<Self> {
        let inode_id = response
            .inode_id
            .map(|id| InodeId::new(id.value))
            .ok_or_else(|| ClientError::InvalidLayout("GetBlockLocationsResponseProto.inode_id missing".to_string()))?;
        let data_handle_id = response
            .data_handle_id
            .ok_or_else(|| {
                ClientError::InvalidLayout("GetBlockLocationsResponseProto.data_handle_id missing".to_string())
            })?
            .try_into()
            .map_err(|_| {
                ClientError::InvalidLayout("GetBlockLocationsResponseProto.data_handle_id invalid".to_string())
            })?;
        let locations = response
            .locations
            .into_iter()
            .map(FileBlockLocation::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(ClientError::InvalidLayout)?;
        Ok(Self {
            group_id,
            inode_id,
            data_handle_id,
            file_size: response.file_size,
            file_version: response.file_version,
            locations,
        })
    }
}

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
    pub target: WriteTarget,
}

/// CommitFile result returned by metadata.
pub type CommitFileResult = proto::metadata::CommitFileResponseProto;

/// AbortFileWrite result returned by metadata.
pub type AbortFileWriteResult = proto::metadata::AbortFileWriteResponseProto;

/// RenewLease result returned by metadata.
pub type RenewLeaseResult = proto::metadata::RenewLeaseResponseProto;

/// SyncWrite result returned by metadata.
pub type SyncWriteResult = proto::metadata::SyncWriteResponseProto;

/// Metadata state watermark returned by Msync.
pub type StateWatermark = proto::common::GroupStateWatermarkProto;
