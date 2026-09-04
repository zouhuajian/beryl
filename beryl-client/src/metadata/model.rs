// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Client-domain metadata result types.

use crate::api::FileStatus;
use crate::error::{ClientError, ClientResult};
use beryl_proto::metadata::GetBlockLocationsResponseProto;
use beryl_types::{ContentGeneration, FileBlockLocation, GroupName, GroupStateWatermark, InodeId, LocatedBlock};

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

/// One validated bounded directory page used by the public async iterator.
#[derive(Clone, Debug)]
pub(crate) struct ListStatusPage {
    /// Fully qualified child statuses returned by Metadata.
    pub(crate) entries: Vec<FileStatus>,
    /// Opaque continuation cursor, present exactly when `eof` is false.
    pub(crate) next_cursor: Option<Vec<u8>>,
    /// Whether this page reached the current end of the directory scan.
    pub(crate) eof: bool,
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

/// Immutable file identity, visible generation, and length captured by `OpenFile`.
#[derive(Clone, Debug)]
pub(crate) struct OpenedFile {
    path: String,
    inode_id: InodeId,
    generation: ContentGeneration,
    file_size: u64,
}

impl OpenedFile {
    /// Creates validated opened-file state from Metadata's response.
    pub(crate) fn new(path: String, inode_id: InodeId, generation: ContentGeneration, file_size: u64) -> Self {
        Self {
            path,
            inode_id,
            generation,
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

    /// Returns the visible content generation fenced by this opened file.
    pub(crate) fn generation(&self) -> ContentGeneration {
        self.generation
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
    /// Authoritative file size at this content generation.
    pub file_size: u64,
    /// Durable visible content generation for this read plan.
    pub generation: Option<ContentGeneration>,
    /// Metadata-authoritative block locations for the requested range.
    pub locations: Vec<FileBlockLocation>,
}

impl ReadLayout {
    /// Convert a metadata wire response into the client read-layout domain view.
    pub(crate) fn from_get_block_locations_response(
        group_name: GroupName,
        response: GetBlockLocationsResponseProto,
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
            generation: response.generation.map(ContentGeneration::new),
            locations,
        })
    }
}

/// Validated allocation result paired with the Metadata owner group for Worker IO.
#[derive(Clone, Debug)]
pub(crate) struct AllocateBlockResult {
    /// Metadata owner group for the block target.
    pub group_name: GroupName,
    /// Metadata-issued block and write authorization, including on allocation replay.
    pub block: LocatedBlock,
}
