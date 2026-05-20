// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Read planner.

use crate::error::{ClientError, ClientResult};
use crate::metadata::LayoutSnapshot;
use types::{BlockId, DataHandleId, FileBlockLocation, InodeId, WorkerEndpointInfo};

/// Splits public read ranges into block-local worker reads.
#[derive(Clone, Debug, Default)]
pub struct ReadPlanner;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PlannedReadRange {
    pub(crate) file_offset: u64,
    pub(crate) len: u32,
}

impl PlannedReadRange {
    pub(crate) fn end_file_offset(self) -> u64 {
        self.file_offset + self.len as u64
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PlannedReadSegment {
    pub(crate) file_offset: u64,
    pub(crate) len: u32,
    pub(crate) end_file_offset: u64,
    pub(crate) block_id: BlockId,
    pub(crate) block_offset: u64,
    pub(crate) block_stamp: u64,
    pub(crate) workers: Vec<WorkerEndpointInfo>,
    pub(crate) worker_epoch: Option<u64>,
}

impl ReadPlanner {
    pub(crate) fn plan_requested_range(
        offset: u64,
        len: u32,
        file_size: u64,
    ) -> ClientResult<Option<PlannedReadRange>> {
        if len == 0 || offset >= file_size {
            return Ok(None);
        }
        let requested_end = offset
            .checked_add(len as u64)
            .ok_or_else(|| ClientError::InvalidArgument("read range offset overflow".to_string()))?;
        let end = requested_end.min(file_size);
        let effective_len = end
            .checked_sub(offset)
            .ok_or_else(|| ClientError::InvalidArgument("read range end precedes offset".to_string()))?;
        let effective_len = u32::try_from(effective_len)
            .map_err(|_| ClientError::InvalidArgument("read range length exceeds u32".to_string()))?;
        if effective_len == 0 {
            return Ok(None);
        }
        Ok(Some(PlannedReadRange {
            file_offset: offset,
            len: effective_len,
        }))
    }

    pub(crate) fn resolve_locations(
        expected_data_handle_id: DataHandleId,
        span: PlannedReadRange,
        locations: &[FileBlockLocation],
    ) -> ClientResult<Vec<PlannedReadSegment>> {
        let mut normalized = Vec::with_capacity(locations.len());
        for location in locations {
            if location.len == 0 {
                return Err(ClientError::InvalidLayout(
                    "zero-length block location segment".to_string(),
                ));
            }
            let end = location
                .file_offset
                .checked_add(location.len)
                .ok_or_else(|| ClientError::InvalidLayout("block location range overflow".to_string()))?;
            let block_id = location.block_id;
            if block_id.data_handle_id != expected_data_handle_id {
                return Err(ClientError::InvalidLayout(format!(
                    "block location data_handle_id {} does not match handle {}",
                    block_id.data_handle_id.as_raw(),
                    expected_data_handle_id.as_raw()
                )));
            }
            if location.workers.is_empty() {
                return Err(ClientError::InvalidLayout(
                    "block location has no worker candidates".to_string(),
                ));
            }
            let block_stamp = location.block_stamp;
            if block_stamp == 0 {
                return Err(ClientError::InvalidLayout(format!(
                    "block location {} has zero block_stamp",
                    block_id
                )));
            }
            if end <= span.file_offset || location.file_offset >= span.end_file_offset() {
                continue;
            }
            normalized.push((location.file_offset, end, block_id, block_stamp, location));
        }
        normalized.sort_by_key(|(start, _, block_id, _, _)| (*start, block_id.index.as_raw()));

        let mut segments = Vec::with_capacity(normalized.len());
        let mut cursor = span.file_offset;
        let requested_end = span.end_file_offset();
        let mut previous_end = None;

        for (start, end, block_id, block_stamp, location) in normalized {
            if let Some(prev_end) = previous_end {
                if start < prev_end {
                    return Err(ClientError::InvalidLayout(format!(
                        "layout overlap at file offset {start}"
                    )));
                }
            }
            previous_end = Some(end);

            if start > cursor {
                return Err(ClientError::InvalidLayout(format!(
                    "layout gap at file offset {cursor}"
                )));
            }
            if end <= cursor {
                continue;
            }

            let read_start = cursor.max(start);
            let read_end = requested_end.min(end);
            if read_start >= read_end {
                continue;
            }
            let len = u32::try_from(read_end - read_start)
                .map_err(|_| ClientError::InvalidLayout("planned segment length exceeds u32".to_string()))?;
            if len == 0 {
                return Err(ClientError::InvalidLayout(
                    "zero-length planned read segment".to_string(),
                ));
            }
            segments.push(PlannedReadSegment {
                file_offset: read_start,
                len,
                end_file_offset: read_end,
                block_id,
                block_offset: read_start - start,
                block_stamp,
                workers: location.workers.clone(),
                worker_epoch: location.worker_epoch,
            });
            cursor = read_end;
            if cursor == requested_end {
                break;
            }
        }

        if cursor < requested_end {
            return Err(ClientError::InvalidLayout(format!(
                "layout gap at file offset {cursor}"
            )));
        }
        Ok(segments)
    }

    pub(crate) fn resolve_response(
        expected_inode_id: InodeId,
        expected_data_handle_id: DataHandleId,
        expected_file_version: Option<u64>,
        span: PlannedReadRange,
        response: &LayoutSnapshot,
    ) -> ClientResult<(u64, Vec<PlannedReadSegment>)> {
        if response.group_id == 0 {
            return Err(ClientError::InvalidLayout(
                "GetBlockLocations response header has group_id 0".to_string(),
            ));
        }
        let group_id = response.group_id;
        let inode_id = response.inode_id;
        if inode_id != expected_inode_id {
            return Err(ClientError::StaleHandle {
                reason: format!(
                    "layout inode_id {} does not match handle {}",
                    inode_id.0, expected_inode_id.0
                ),
            });
        }
        let data_handle_id = response.data_handle_id;
        if data_handle_id != expected_data_handle_id {
            return Err(ClientError::StaleHandle {
                reason: format!(
                    "layout data_handle_id {} does not match handle {}",
                    data_handle_id.as_raw(),
                    expected_data_handle_id.as_raw()
                ),
            });
        }
        let actual_version =
            file_version_from_snapshot(response.file_version, "GetBlockLocationsResponseProto.file_version")?;
        let expected_version = expected_file_version.ok_or_else(|| ClientError::StaleHandle {
            reason: "read handle missing file_version".to_string(),
        })?;
        if actual_version != expected_version {
            return Err(ClientError::VersionMismatch {
                expected: expected_version,
                actual: actual_version,
            });
        }
        let segments = Self::resolve_locations(expected_data_handle_id, span, &response.locations)?;
        Ok((group_id, segments))
    }
}

fn file_version_from_snapshot(value: Option<u64>, field: &str) -> ClientResult<u64> {
    value.ok_or_else(|| ClientError::InvalidLayout(format!("{field} missing")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use types::{BlockId, BlockIndex, DataHandleId, WorkerEndpointInfo, WorkerId, WorkerNetProtocol};

    #[test]
    fn requested_range_is_truncated_at_eof() {
        let span = ReadPlanner::plan_requested_range(8, 10, 12)
            .expect("range planning succeeds")
            .expect("non-empty span");

        assert_eq!(span.file_offset, 8);
        assert_eq!(span.len, 4);
    }

    #[test]
    fn planner_supports_multi_block_reads() {
        let span = ReadPlanner::plan_requested_range(2, 12, 20)
            .expect("range planning succeeds")
            .expect("non-empty span");
        let locations = vec![location(10, 0, 0, 8, 101), location(10, 1, 8, 8, 202)];

        let segments =
            ReadPlanner::resolve_locations(DataHandleId::new(10), span, &locations).expect("locations cover range");

        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].file_offset, 2);
        assert_eq!(segments[0].block_offset, 2);
        assert_eq!(segments[0].len, 6);
        assert_eq!(segments[0].block_stamp, 101);
        assert_eq!(segments[1].file_offset, 8);
        assert_eq!(segments[1].block_offset, 0);
        assert_eq!(segments[1].len, 6);
        assert_eq!(segments[1].block_stamp, 202);
    }

    #[test]
    fn planner_normalizes_unordered_locations() {
        let span = ReadPlanner::plan_requested_range(0, 12, 20)
            .expect("range planning succeeds")
            .expect("non-empty span");
        let locations = vec![location(10, 1, 8, 8, 202), location(10, 0, 0, 8, 101)];

        let segments = ReadPlanner::resolve_locations(DataHandleId::new(10), span, &locations)
            .expect("unordered locations are sorted");

        assert_eq!(
            segments.iter().map(|segment| segment.file_offset).collect::<Vec<_>>(),
            vec![0, 8]
        );
        assert_eq!(
            segments.iter().map(|segment| segment.block_stamp).collect::<Vec<_>>(),
            vec![101, 202]
        );
    }

    #[test]
    fn planner_rejects_missing_coverage() {
        let span = ReadPlanner::plan_requested_range(0, 12, 20)
            .expect("range planning succeeds")
            .expect("non-empty span");
        let locations = vec![location(10, 0, 0, 4, 101), location(10, 1, 8, 8, 202)];

        let err = ReadPlanner::resolve_locations(DataHandleId::new(10), span, &locations).expect_err("gap must fail");

        assert!(format!("{err}").contains("layout gap"));
    }

    #[test]
    fn planner_rejects_overlapping_coverage() {
        let span = ReadPlanner::plan_requested_range(0, 12, 20)
            .expect("range planning succeeds")
            .expect("non-empty span");
        let locations = vec![location(10, 0, 0, 8, 101), location(10, 1, 4, 8, 202)];

        let err =
            ReadPlanner::resolve_locations(DataHandleId::new(10), span, &locations).expect_err("overlap must fail");

        assert!(format!("{err}").contains("layout overlap"));
    }

    #[test]
    fn planner_rejects_zero_length_location_segments() {
        let span = ReadPlanner::plan_requested_range(0, 4, 20)
            .expect("range planning succeeds")
            .expect("non-empty span");
        let locations = vec![location(10, 0, 0, 0, 101)];

        let err = ReadPlanner::resolve_locations(DataHandleId::new(10), span, &locations)
            .expect_err("zero-length location must fail");

        assert!(format!("{err}").contains("zero-length"));
    }

    #[test]
    fn planner_rejects_zero_block_stamp() {
        let span = ReadPlanner::plan_requested_range(0, 4, 20)
            .expect("range planning succeeds")
            .expect("non-empty span");
        let locations = vec![location(10, 0, 0, 4, 0)];

        let err = ReadPlanner::resolve_locations(DataHandleId::new(10), span, &locations)
            .expect_err("zero block stamp must fail");

        assert!(format!("{err}").contains("block_stamp"));
    }

    fn location(
        data_handle_id: u64,
        block_index: u32,
        file_offset: u64,
        len: u64,
        block_stamp: u64,
    ) -> FileBlockLocation {
        FileBlockLocation {
            block_id: BlockId::new(DataHandleId::new(data_handle_id), BlockIndex::new(block_index)),
            file_offset,
            len,
            workers: vec![WorkerEndpointInfo {
                worker_id: WorkerId::new(1),
                endpoint: "127.0.0.1:19101".to_string(),
                worker_net_protocol: WorkerNetProtocol::Grpc,
                worker_epoch: 7,
            }],
            worker_epoch: Some(7),
            block_stamp,
        }
    }
}
