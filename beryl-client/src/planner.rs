// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Planning one bounded block read from Metadata-authorized locations.

use crate::error::{ClientError, ClientResult, RefreshHint as ClientRefreshHint};
use crate::metadata::ReadLayout;
use beryl_common::error::rpc::{ErrorKind, RefreshHint, RpcErrorDetail, WorkerErrorKind};
use beryl_types::{BlockId, FileStatus, WorkerEndpointInfo};

/// A block-local worker read planned from metadata block locations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PlannedBlockRead {
    pub(crate) file_offset: u64,
    pub(crate) len: u32,
    pub(crate) block_id: BlockId,
    pub(crate) block_offset: u64,
    pub(crate) block_size: u64,
    pub(crate) effective_len: u64,
    pub(crate) workers: Vec<WorkerEndpointInfo>,
}

/// Plans the current block's prefix from a layout validated against the opened file.
pub(crate) fn plan_block_read(
    file: &FileStatus,
    offset: u64,
    max_len: u32,
    layout: &ReadLayout,
) -> ClientResult<PlannedBlockRead> {
    if max_len == 0 || offset >= file.len() {
        return Err(ClientError::invalid_argument(
            "block read requires a nonempty range before EOF",
        ));
    }
    let location = layout
        .location_at(offset)
        .ok_or_else(|| ClientError::invalid_layout("layout gap at requested offset"))?;
    let end = location.file_offset + location.len;
    if location.workers.is_empty() {
        return Err(block_location_unavailable_error(format!(
            "block location unavailable for {}",
            location.block_id
        )));
    }
    Ok(PlannedBlockRead {
        file_offset: offset,
        len: u64::from(max_len).min(end - offset) as u32,
        block_id: location.block_id,
        block_offset: offset - location.file_offset,
        block_size: location.block_size,
        effective_len: location.effective_len,
        workers: location.workers.clone(),
    })
}

pub(crate) fn block_location_unavailable_error(message: impl Into<String>) -> ClientError {
    let rpc_error = RpcErrorDetail::refresh_metadata(
        ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
        RefreshHint {
            worker_resolve_required: true,
            ..RefreshHint::default()
        },
        message,
    );
    ClientError::from_remote(rpc_error, ClientRefreshHint::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use beryl_types::{BlockIndex, ContentGeneration, FileBlockLocation, FileType, GroupName, InodeId, WorkerId};

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

    fn layout(file: &FileStatus, locations: Vec<FileBlockLocation>) -> ReadLayout {
        let layout = ReadLayout::from_response(
            GroupName::parse("root").unwrap(),
            Some(file.into()),
            locations.into_iter().map(Into::into).collect(),
        )
        .unwrap();
        layout.validate_file(file).unwrap();
        layout
    }

    #[test]
    fn block_plan_is_bounded_by_the_current_block_and_request() {
        let file = file();
        let layout = layout(&file, vec![location(10, 4, 0, 8)]);
        let plan = plan_block_read(&file, 3, 10, &layout).unwrap();
        assert_eq!((plan.file_offset, plan.block_offset, plan.len), (3, 3, 5));
        assert_eq!(plan.block_id.index.as_raw(), 4, "block index is not file offset");
        assert_eq!(plan_block_read(&file, 3, 2, &layout).unwrap().len, 2);
    }

    #[test]
    fn block_plan_requires_range_coverage_and_workers() {
        let file = file();
        let covered = layout(&file, vec![location(10, 0, 0, 8)]);
        for (offset, len) in [(8, 4), (16, 4), (0, 0)] {
            assert!(plan_block_read(&file, offset, len, &covered).is_err());
        }
        let mut unavailable = location(10, 0, 0, 8);
        unavailable.workers.clear();
        let unavailable = layout(&file, vec![unavailable]);
        assert!(plan_block_read(&file, 0, 4, &unavailable).is_err());
    }

    fn location(inode_id: u64, block_index: u32, file_offset: u64, len: u64) -> FileBlockLocation {
        FileBlockLocation {
            block_id: BlockId::new(InodeId::new(inode_id), BlockIndex::new(block_index)),
            file_offset,
            len,
            workers: vec![WorkerEndpointInfo {
                worker_id: WorkerId::new(1),
                endpoint: "127.0.0.1:19101".to_string(),
                worker_run_id: "550e8400-e29b-41d4-a716-446655440000".parse().unwrap(),
            }],

            block_size: 4096,
            effective_len: len,
        }
    }
}
