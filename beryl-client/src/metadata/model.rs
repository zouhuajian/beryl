// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Client-domain metadata result types.

use crate::api::FileStatus;
use crate::error::{ClientError, ClientResult};
use beryl_proto::metadata::{FileBlockLocationProto, FileStatusProto};
use beryl_types::{FileBlockLocation, GroupName, GroupStateWatermark, LocatedBlock};

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

/// Validated inode state and range locations from one Metadata response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReadLayout {
    pub group_name: GroupName,
    status: FileStatus,
    locations: Vec<FileBlockLocation>,
}

impl ReadLayout {
    /// Checks the opened authority before this immutable layout enters a reader cache.
    pub(crate) fn validate_file(&self, file: &FileStatus) -> ClientResult<()> {
        if self.status.inode_id != file.inode_id() {
            return Err(ClientError::stale_handle("layout inode_id does not match opened inode"));
        }
        let generation = self.status.generation.expect("validated layout generation");
        let expected = file.generation().expect("validated file generation");
        if generation != expected {
            return Err(ClientError::generation_mismatch(expected, generation));
        }
        if self.status.len != file.len() {
            return Err(ClientError::stale_handle("layout length does not match opened length"));
        }
        Ok(())
    }

    /// Locations cover full visible blocks, even when the original request was smaller.
    pub(crate) fn location_at(&self, offset: u64) -> Option<&FileBlockLocation> {
        self.locations
            .iter()
            .find(|location| location.file_offset <= offset && offset < location.file_offset + location.len)
    }

    pub(crate) fn from_response(
        group_name: GroupName,
        status: Option<FileStatusProto>,
        locations: Vec<FileBlockLocationProto>,
    ) -> ClientResult<Self> {
        let status = beryl_types::FileStatus::try_from(
            status.ok_or_else(|| ClientError::invalid_layout("read response status missing"))?,
        )
        .map_err(ClientError::invalid_layout)?;
        if !status.kind.is_file() {
            return Err(ClientError::invalid_layout("read response is not a file"));
        }
        if locations.len() > beryl_types::MAX_FILE_BLOCKS {
            return Err(ClientError::invalid_layout("read layout exceeds file block limit"));
        }
        let mut locations: Vec<FileBlockLocation> = locations
            .into_iter()
            .map(FileBlockLocation::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(ClientError::invalid_layout)?;
        locations.sort_by_key(|location| location.file_offset);
        let mut previous_end = None;
        for location in &locations {
            let end = location
                .file_offset
                .checked_add(location.len)
                .filter(|end| *end <= status.len)
                .ok_or_else(|| ClientError::invalid_layout("block location exceeds file length"))?;
            if location.block_id.inode_id != status.inode_id {
                return Err(ClientError::invalid_layout(
                    "block inode_id does not match layout inode",
                ));
            }
            if location.len != location.effective_len {
                return Err(ClientError::invalid_layout(
                    "location length differs from visible block prefix",
                ));
            }
            if previous_end.is_some_and(|previous| previous > location.file_offset) {
                return Err(ClientError::invalid_layout("layout overlap"));
            }
            previous_end = Some(end);
        }
        Ok(Self {
            group_name,
            status,
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

#[cfg(test)]
mod tests {
    use super::*;
    use beryl_types::{BlockId, BlockIndex, ContentGeneration, FileType, InodeId};

    fn file() -> FileStatus {
        FileStatus {
            path: Some("/file".into()),
            inode_id: InodeId::new(10),
            kind: FileType::File,
            len: 16,
            generation: Some(ContentGeneration::new(1)),
            create_time: 0,
            modify_time: 0,
        }
    }

    fn location(inode_id: u64, file_offset: u64, len: u64) -> FileBlockLocationProto {
        FileBlockLocationProto {
            block_id: Some(BlockId::new(InodeId::new(inode_id), BlockIndex::new(4)).into()),
            file_offset,
            len,
            block_size: 4096,
            effective_len: len,
            workers: Vec::new(),
        }
    }

    #[test]
    fn layout_conversion_rejects_invalid_identity_shape_and_overlap() {
        let valid = location(10, 0, 8);
        let cases = [
            vec![location(10, 0, 0)],
            vec![location(11, 0, 8)],
            vec![location(10, 0, 8), location(10, 4, 8)],
            vec![location(10, u64::MAX, 8)],
            vec![location(10, 12, 8)],
            vec![FileBlockLocationProto {
                block_size: 0,
                ..valid.clone()
            }],
            vec![FileBlockLocationProto {
                effective_len: 3,
                ..valid.clone()
            }],
            vec![FileBlockLocationProto {
                effective_len: 4097,
                ..valid.clone()
            }],
            vec![valid; beryl_types::MAX_FILE_BLOCKS + 1],
        ];
        for locations in cases {
            ReadLayout::from_response(GroupName::parse("root").unwrap(), Some((&file()).into()), locations)
                .expect_err("malformed layout must fail at the response boundary");
        }
        let mut missing_generation = FileStatusProto::from(&file());
        missing_generation.generation = None;
        assert!(ReadLayout::from_response(
            GroupName::parse("root").unwrap(),
            Some(missing_generation),
            vec![location(10, 0, 8)]
        )
        .is_err());
    }

    #[test]
    fn layout_matches_opened_authority_independently_of_observation_path() {
        let file = file();
        let mut layout = ReadLayout::from_response(
            GroupName::parse("root").unwrap(),
            Some((&file).into()),
            vec![location(10, 0, 8)],
        )
        .unwrap();
        for path in [None, Some("/renamed".into())] {
            layout.status.path = path;
            layout.validate_file(&file).unwrap();
        }
        for opened in [
            FileStatus {
                inode_id: InodeId::new(11),
                ..file.clone()
            },
            FileStatus {
                generation: Some(ContentGeneration::new(2)),
                ..file.clone()
            },
            FileStatus { len: 17, ..file },
        ] {
            layout
                .validate_file(&opened)
                .expect_err("new layout must match the opened authority");
        }
    }
}
