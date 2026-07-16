// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Client-domain metadata result types.

use crate::error::{ClientError, ClientResult};
use beryl_types::{DataHandleId, FileBlockLocation, GroupName, WriteTarget};

/// Validated read layout returned by metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReadLayout {
    /// Metadata owner group from the validated response header.
    pub group_name: GroupName,
    /// Data handle identity this layout belongs to.
    pub data_handle_id: DataHandleId,
    /// Authoritative file size at this layout version.
    pub file_size: u64,
    /// Durable visible file-state version for this read plan.
    pub file_version: Option<u64>,
    /// Metadata-authoritative block locations for the requested range.
    pub locations: Vec<FileBlockLocation>,
}

impl ReadLayout {
    /// Convert a metadata wire response into the client read-layout domain view.
    pub(crate) fn from_get_block_locations_response(
        group_name: GroupName,
        response: beryl_proto::metadata::GetBlockLocationsResponseProto,
    ) -> ClientResult<Self> {
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
            group_name,
            data_handle_id,
            file_size: response.file_size,
            file_version: response.file_version,
            locations,
        })
    }
}

/// Write target returned by AddBlock with its owner group.
#[derive(Clone, Debug)]
pub(crate) struct AddBlockResult {
    /// Metadata owner group for the block target.
    pub group_name: GroupName,
    /// Worker target for this block.
    pub target: WriteTarget,
}
