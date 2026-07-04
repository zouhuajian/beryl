// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Read planning from metadata block locations to worker block reads.

use common::error::canonical::{CanonicalError, RefreshHint, RefreshReason};
use common::header::RpcErrorCode;

use crate::error::{ClientError, ClientResult};
use crate::metadata::ReadLayout;
use types::{BlockId, BlockShape, DataHandleId, FileBlockLocation, GroupName, WorkerEndpointInfo};

/// File byte range requested by a reader after EOF truncation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RequestedReadRange {
    pub(crate) file_offset: u64,
    pub(crate) len: u32,
}

impl RequestedReadRange {
    pub(crate) fn end_file_offset(self) -> u64 {
        self.file_offset + self.len as u64
    }
}

/// A block-local worker read planned from metadata block locations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PlannedBlockRead {
    pub(crate) file_offset: u64,
    pub(crate) len: u32,
    pub(crate) end_file_offset: u64,
    pub(crate) block_id: BlockId,
    pub(crate) block_offset: u64,
    pub(crate) block_stamp: u64,
    pub(crate) block_format_id: types::BlockFormatId,
    pub(crate) block_size: u64,
    pub(crate) chunk_size: u32,
    pub(crate) effective_len: u64,
    pub(crate) workers: Vec<WorkerEndpointInfo>,
}

pub(crate) fn requested_range(offset: u64, len: u32, file_size: u64) -> ClientResult<Option<RequestedReadRange>> {
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
    Ok(Some(RequestedReadRange {
        file_offset: offset,
        len: effective_len,
    }))
}

pub(crate) fn plan_block_reads(
    expected_data_handle_id: DataHandleId,
    requested_range: RequestedReadRange,
    locations: &[FileBlockLocation],
) -> ClientResult<Vec<PlannedBlockRead>> {
    let mut normalized = Vec::with_capacity(locations.len());
    for location in locations {
        if location.len == 0 {
            return Err(ClientError::InvalidLayout("zero-length block location".to_string()));
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
        let block_stamp = location.block_stamp;
        if block_stamp == 0 {
            return Err(ClientError::InvalidLayout(format!(
                "block location {} has zero block_stamp",
                block_id
            )));
        }
        BlockShape::new(
            location.block_format_id,
            location.block_size,
            location.chunk_size,
            location.effective_len,
        )
        .map_err(|err| ClientError::InvalidLayout(format!("block location {block_id} has invalid shape: {err}")))?;
        if location.workers.is_empty() {
            return Err(block_location_unavailable_error(format!(
                "block location unavailable: metadata returned no worker candidates for block {} file_offset={} len={} block_stamp={}",
                block_id, location.file_offset, location.len, block_stamp
            )));
        }
        if end <= requested_range.file_offset || location.file_offset >= requested_range.end_file_offset() {
            continue;
        }
        normalized.push((location.file_offset, end, block_id, block_stamp, location));
    }
    normalized.sort_by_key(|(start, _, block_id, _, _)| (*start, block_id.index.as_raw()));

    let mut block_reads = Vec::with_capacity(normalized.len());
    let mut cursor = requested_range.file_offset;
    let requested_end = requested_range.end_file_offset();
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
            .map_err(|_| ClientError::InvalidLayout("planned block read length exceeds u32".to_string()))?;
        if len == 0 {
            return Err(ClientError::InvalidLayout("zero-length planned block read".to_string()));
        }
        block_reads.push(PlannedBlockRead {
            file_offset: read_start,
            len,
            end_file_offset: read_end,
            block_id,
            block_offset: read_start - start,
            block_stamp,
            block_format_id: location.block_format_id,
            block_size: location.block_size,
            chunk_size: location.chunk_size,
            effective_len: location.effective_len,
            workers: location.workers.clone(),
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
    Ok(block_reads)
}

pub(crate) fn plan_block_reads_from_layout(
    expected_data_handle_id: DataHandleId,
    expected_file_version: Option<u64>,
    requested_range: RequestedReadRange,
    response: &ReadLayout,
) -> ClientResult<(GroupName, Vec<PlannedBlockRead>)> {
    let group_name = response.group_name.clone();
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
        file_version_from_response(response.file_version, "GetBlockLocationsResponseProto.file_version")?;
    let expected_version = expected_file_version.ok_or_else(|| ClientError::StaleHandle {
        reason: "read handle missing file_version".to_string(),
    })?;
    if actual_version != expected_version {
        return Err(ClientError::VersionMismatch {
            expected: expected_version,
            actual: actual_version,
        });
    }
    let block_reads = plan_block_reads(expected_data_handle_id, requested_range, &response.locations)?;
    Ok((group_name, block_reads))
}

fn file_version_from_response(value: Option<u64>, field: &str) -> ClientResult<u64> {
    value.ok_or_else(|| ClientError::InvalidLayout(format!("{field} missing")))
}

pub(crate) fn block_location_unavailable_error(message: impl Into<String>) -> ClientError {
    let canonical = CanonicalError::need_refresh_with_hint(
        RpcErrorCode::BlockLocationUnavailable,
        RefreshReason::BlockLocationUnavailable,
        RefreshHint {
            worker_resolve_required: true,
            ..RefreshHint::default()
        },
        message,
    );
    ClientError::from(crate::canonical::ClientAction::Refresh {
        reason: RefreshReason::BlockLocationUnavailable,
        hint: Box::new(crate::canonical::RefreshHint {
            worker_resolve_required: true,
            ..crate::canonical::RefreshHint::default()
        }),
        canonical: Box::new(canonical),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use types::{BlockId, BlockIndex, DataHandleId, WorkerEndpointInfo, WorkerId, WorkerNetProtocol};

    #[test]
    fn requested_range_is_truncated_at_eof() {
        let requested_range = requested_range(8, 10, 12)
            .expect("range planning succeeds")
            .expect("non-empty requested range");

        assert_eq!(requested_range.file_offset, 8);
        assert_eq!(requested_range.len, 4);
    }

    #[test]
    fn planner_supports_multi_block_reads() {
        let requested_range = requested_range(2, 12, 20)
            .expect("range planning succeeds")
            .expect("non-empty requested range");
        let locations = vec![location(10, 0, 0, 8, 101), location(10, 1, 8, 8, 202)];

        let block_reads =
            plan_block_reads(DataHandleId::new(10), requested_range, &locations).expect("locations cover range");

        assert_eq!(block_reads.len(), 2);
        assert_eq!(block_reads[0].file_offset, 2);
        assert_eq!(block_reads[0].block_offset, 2);
        assert_eq!(block_reads[0].len, 6);
        assert_eq!(block_reads[0].block_stamp, 101);
        assert_eq!(
            block_reads[0].block_format_id,
            types::BlockFormatId::CURRENT_FOR_NEW_FILE
        );
        assert_eq!(block_reads[0].block_size, 4096);
        assert_eq!(block_reads[0].chunk_size, 1024);
        assert_eq!(block_reads[0].effective_len, 8);
        assert_eq!(block_reads[1].file_offset, 8);
        assert_eq!(block_reads[1].block_offset, 0);
        assert_eq!(block_reads[1].len, 6);
        assert_eq!(block_reads[1].block_stamp, 202);
    }

    #[test]
    fn planner_normalizes_unordered_locations() {
        let requested_range = requested_range(0, 12, 20)
            .expect("range planning succeeds")
            .expect("non-empty requested range");
        let locations = vec![location(10, 1, 8, 8, 202), location(10, 0, 0, 8, 101)];

        let block_reads = plan_block_reads(DataHandleId::new(10), requested_range, &locations)
            .expect("unordered locations are sorted");

        assert_eq!(
            block_reads
                .iter()
                .map(|block_read| block_read.file_offset)
                .collect::<Vec<_>>(),
            vec![0, 8]
        );
        assert_eq!(
            block_reads
                .iter()
                .map(|block_read| block_read.block_stamp)
                .collect::<Vec<_>>(),
            vec![101, 202]
        );
    }

    #[test]
    fn planner_rejects_missing_coverage() {
        let requested_range = requested_range(0, 12, 20)
            .expect("range planning succeeds")
            .expect("non-empty requested range");
        let locations = vec![location(10, 0, 0, 4, 101), location(10, 1, 8, 8, 202)];

        let err = plan_block_reads(DataHandleId::new(10), requested_range, &locations).expect_err("gap must fail");

        assert!(format!("{err}").contains("layout gap"));
    }

    #[test]
    fn planner_rejects_overlapping_coverage() {
        let requested_range = requested_range(0, 12, 20)
            .expect("range planning succeeds")
            .expect("non-empty requested range");
        let locations = vec![location(10, 0, 0, 8, 101), location(10, 1, 4, 8, 202)];

        let err = plan_block_reads(DataHandleId::new(10), requested_range, &locations).expect_err("overlap must fail");

        assert!(format!("{err}").contains("layout overlap"));
    }

    #[test]
    fn planner_rejects_zero_length_block_locations() {
        let requested_range = requested_range(0, 4, 20)
            .expect("range planning succeeds")
            .expect("non-empty requested range");
        let locations = vec![location(10, 0, 0, 0, 101)];

        let err = plan_block_reads(DataHandleId::new(10), requested_range, &locations)
            .expect_err("zero-length location must fail");

        assert!(format!("{err}").contains("zero-length"));
    }

    #[test]
    fn planner_rejects_zero_block_stamp() {
        let requested_range = requested_range(0, 4, 20)
            .expect("range planning succeeds")
            .expect("non-empty requested range");
        let locations = vec![location(10, 0, 0, 4, 0)];

        let err = plan_block_reads(DataHandleId::new(10), requested_range, &locations)
            .expect_err("zero block stamp must fail");

        assert!(format!("{err}").contains("block_stamp"));
    }

    #[test]
    fn planner_rejects_empty_worker_candidates_from_metadata_as_block_location_unavailable() {
        let requested_range = requested_range(0, 4, 20)
            .expect("range planning succeeds")
            .expect("non-empty requested range");
        let mut location = location(10, 0, 0, 4, 101);
        location.workers.clear();

        let err = plan_block_reads(DataHandleId::new(10), requested_range, &[location])
            .expect_err("empty worker candidate list must not produce a read plan");

        assert_block_location_unavailable(&err);
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
                worker_run_id: "550e8400-e29b-41d4-a716-446655440000".parse().unwrap(),
            }],
            block_stamp,
            block_format_id: types::BlockFormatId::CURRENT_FOR_NEW_FILE,
            block_size: 4096,
            chunk_size: 1024,
            effective_len: len,
        }
    }

    fn assert_block_location_unavailable(err: &ClientError) {
        match err {
            ClientError::Action(action) => match action.as_ref() {
                crate::canonical::ClientAction::Refresh { reason, canonical, .. } => {
                    assert_eq!(
                        *reason,
                        common::error::canonical::RefreshReason::BlockLocationUnavailable
                    );
                    assert_eq!(
                        canonical.code,
                        Some(common::error::canonical::ErrorCode::RpcCode(
                            common::header::RpcErrorCode::BlockLocationUnavailable
                        ))
                    );
                }
                other => panic!("expected block-location refresh error, got {other:?}"),
            },
            other => panic!("expected action error, got {other:?}"),
        }
    }
}
