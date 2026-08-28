// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Client-domain metadata result types.

use crate::error::{ClientError, ClientResult};
use beryl_types::{FileBlockLocation, GroupName, GroupStateWatermark, InodeId, WriteTarget};

/// Server-authorized metadata state learned from one validated successful response.
///
/// Every watermark is scoped to `group_name`. Epochs are scoped later to the
/// operation path because the response header does not carry a mount prefix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MetadataAuthorityUpdate {
    /// Metadata group that authorized this response.
    pub(crate) group_name: GroupName,
    /// Applied state-machine watermarks authorized by the group leader.
    pub(crate) state: Vec<GroupStateWatermark>,
    /// Current mount epoch for the operation path, when supplied.
    pub(crate) mount_epoch: Option<u64>,
    /// Current route epoch for the operation path, when supplied.
    pub(crate) route_epoch: Option<u64>,
}

/// Couples a response body with the authority state validated from its header.
///
/// The Metadata client must apply `authority` before exposing `body` upstream.
#[derive(Clone, Debug)]
pub(crate) struct ValidatedMetadataResponse<T> {
    authority: MetadataAuthorityUpdate,
    body: T,
}

impl<T> ValidatedMetadataResponse<T> {
    /// Creates a response whose header identity and success scope are validated.
    pub(crate) fn new(authority: MetadataAuthorityUpdate, body: T) -> Self {
        Self { authority, body }
    }

    /// Separates the authority update from the body at the client boundary.
    pub(crate) fn into_parts(self) -> (MetadataAuthorityUpdate, T) {
        (self.authority, self.body)
    }
}

/// Immutable file identity, visible revision, and length captured by `OpenFile`.
#[derive(Clone, Debug)]
pub(crate) struct OpenedFile {
    path: String,
    inode_id: InodeId,
    content_revision: u64,
    file_size: u64,
}

impl OpenedFile {
    /// Creates validated opened-file state from Metadata's response.
    pub(crate) fn new(path: String, inode_id: InodeId, content_revision: u64, file_size: u64) -> Self {
        Self {
            path,
            inode_id,
            content_revision,
            file_size,
        }
    }

    /// Returns the path used to open this file.
    pub(crate) fn path(&self) -> &str {
        &self.path
    }

    /// Returns the immutable inode identity used for layout requests.
    pub(crate) fn inode_id(&self) -> InodeId {
        self.inode_id
    }

    /// Returns the visible content revision fenced by this opened file.
    pub(crate) fn content_revision(&self) -> u64 {
        self.content_revision
    }

    /// Returns the file size observed by `OpenFile`.
    pub(crate) fn len(&self) -> u64 {
        self.file_size
    }
}

/// Validated read layout returned by metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReadLayout {
    /// Metadata owner group from the validated response header.
    pub group_name: GroupName,
    /// File inode identity this layout belongs to.
    pub inode_id: InodeId,
    /// Authoritative file size at this layout version.
    pub file_size: u64,
    /// Durable visible file-state version for this read plan.
    pub content_revision: Option<u64>,
    /// Metadata-authoritative block locations for the requested range.
    pub locations: Vec<FileBlockLocation>,
}

impl ReadLayout {
    /// Convert a metadata wire response into the client read-layout domain view.
    pub(crate) fn from_get_block_locations_response(
        group_name: GroupName,
        response: beryl_proto::metadata::GetBlockLocationsResponseProto,
    ) -> ClientResult<Self> {
        if response.inode_id == 0 {
            return Err(ClientError::invalid_layout(
                "GetBlockLocationsResponseProto.inode_id must be non-zero".to_string(),
            ));
        }
        let inode_id = InodeId::new(response.inode_id);
        let locations = response
            .locations
            .into_iter()
            .map(FileBlockLocation::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(ClientError::invalid_layout)?;
        Ok(Self {
            group_name,
            inode_id,
            file_size: response.file_size,
            content_revision: response.content_revision,
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
