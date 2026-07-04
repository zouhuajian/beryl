// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker data service protocol helpers.
//!
//! This module builds worker RPC requests, parses and validates worker RPC
//! responses, and validates worker stream frames. It does not own channel
//! pooling or data-plane orchestration.
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use common::error::canonical::{CanonicalError, RefreshHint as CanonicalRefreshHint, RefreshReason};
use common::header::{HeaderIdentity, RpcErrorCode};
use types::chunk::ByteRange;
use types::{BlockShape, GroupName, WorkerEndpointInfo, WriteTarget};

use super::{WorkerBlockSyncResult, WorkerBlockWriteHandle, WorkerCommitResult, WorkerReadResult, WorkerWriteTarget};
use crate::canonical::{invalid_header_action, validate_data_header_or_action};
use crate::error::{invalid_response, side_effect_response_body_mismatch, ClientError, ClientResult};
use crate::planner::PlannedBlockRead;
use crate::runtime::AttemptContext;

pub(super) fn build_open_read_stream_request(
    attempt: &AttemptContext,
    group_name: &GroupName,
    block_read: &PlannedBlockRead,
    worker: &WorkerEndpointInfo,
) -> ClientResult<proto::worker::OpenReadStreamRequestProto> {
    if block_read.block_stamp == 0 {
        return Err(ClientError::InvalidLayout(
            "planned block read has zero block_stamp".to_string(),
        ));
    }
    BlockShape::new(
        block_read.block_format_id,
        block_read.block_size,
        block_read.chunk_size,
        block_read.effective_len,
    )
    .map_err(|err| ClientError::InvalidLayout(format!("planned block read has invalid expected block shape: {err}")))?;
    Ok(proto::worker::OpenReadStreamRequestProto {
        header: Some(attempt.data_header()),
        group_name: group_name.to_string(),
        block_id: Some(block_read.block_id.into()),
        byte_range: Some(
            ByteRange {
                offset: block_read.block_offset,
                len: block_read.len,
            }
            .into(),
        ),
        block_stamp: block_read.block_stamp,
        frame_size: default_frame_size(block_read.len),
        worker_run_id: worker.worker_run_id.to_string(),
        block_format_id: block_read.block_format_id.as_raw(),
        block_size: block_read.block_size,
        chunk_size: block_read.chunk_size,
        effective_len: block_read.effective_len,
    })
}

pub(super) fn validate_open_read_stream_response(
    block_read: &PlannedBlockRead,
    response: &proto::worker::OpenReadStreamResponseProto,
) -> ClientResult<()> {
    validate_worker_read_result(
        block_read,
        &WorkerReadResult {
            bytes: Bytes::new(),
            block_stamp: response.block_stamp,
            committed_length: response.committed_length,
        },
    )
}

pub(super) fn build_open_write_stream_request(
    attempt: &AttemptContext,
    target: &WorkerWriteTarget,
    worker: &WorkerEndpointInfo,
) -> ClientResult<proto::worker::OpenWriteStreamRequestProto> {
    validate_worker_write_target(target)?;
    Ok(proto::worker::OpenWriteStreamRequestProto {
        header: Some(attempt.data_header()),
        group_name: target.group_name.to_string(),
        block_id: Some(target.target.block_id.into()),
        block_size: target.target.block_size,
        block_stamp: target.target.block_stamp,
        chunk_size: target.target.chunk_size,
        checksum_kind: proto::worker::ChecksumKindProto::ChecksumKindNone as i32,
        token: Some(target.target.fencing_token.into()),
        frame_size: default_frame_size(target.target.effective_len.min(u64::from(u32::MAX)) as u32),
        block_format_id: target.target.block_format_id.as_raw(),
        worker_run_id: worker.worker_run_id.to_string(),
        effective_len: target.target.effective_len,
        tier: proto::common::TierProto::from(target.target.tier) as i32,
    })
}

pub(super) fn parse_open_write_stream_response(
    attempt: &AttemptContext,
    target: &WorkerWriteTarget,
    worker: &WorkerEndpointInfo,
    response: proto::worker::OpenWriteStreamResponseProto,
) -> ClientResult<WorkerBlockWriteHandle> {
    parse_worker_control_header(attempt, response.header.as_ref())?;
    let stream_id = response
        .stream_id
        .ok_or_else(|| side_effect_response_body_mismatch("OpenWriteStream", "missing stream_id"))?;
    if stream_id.high == 0 && stream_id.low == 0 {
        return Err(side_effect_response_body_mismatch(
            "OpenWriteStream",
            "stream_id is zero",
        ));
    }
    if response.block_stamp != target.target.block_stamp {
        return Err(side_effect_response_body_mismatch(
            "OpenWriteStream",
            format!(
                "block_stamp expected {}, got {}",
                target.target.block_stamp, response.block_stamp
            ),
        ));
    }
    if response.committed_length != 0 {
        return Err(side_effect_response_body_mismatch(
            "OpenWriteStream",
            format!("committed_length expected 0, got {}", response.committed_length),
        ));
    }
    Ok(WorkerBlockWriteHandle {
        group_name: target.group_name.clone(),
        worker: worker.clone(),
        target: target.target.clone(),
        stream_id,
        frame_size: response.frame_size.max(1),
        next_seq: 1,
    })
}

pub(super) fn build_write_stream_requests(
    handle: &WorkerBlockWriteHandle,
    data: Bytes,
) -> ClientResult<Vec<proto::worker::WriteStreamRequestProto>> {
    let frame_size = handle.frame_size.max(1) as usize;
    let frame_count = data.len().div_ceil(frame_size);
    let mut requests = Vec::with_capacity(frame_count);
    let mut offset = 0usize;
    while offset < data.len() {
        let end = (offset + frame_size).min(data.len());
        let seq = handle
            .next_seq
            .checked_add(requests.len() as u64)
            .ok_or_else(|| ClientError::Worker("worker write frame sequence overflow".to_string()))?;
        requests.push(proto::worker::WriteStreamRequestProto {
            stream_id: Some(handle.stream_id),
            seq,
            offset_in_block: offset as u64,
            data: data.slice(offset..end),
            checksum32: 0,
        });
        offset = end;
    }
    Ok(requests)
}

pub(super) fn validate_write_stream_response(
    response: proto::worker::WriteStreamResponseProto,
    expected_last_seq: u64,
    expected_written_through: u64,
) -> ClientResult<proto::worker::WriteStreamResponseProto> {
    if response.last_acked_seq != expected_last_seq {
        return Err(ClientError::UnknownOutcome(format!(
            "worker WriteStream ack mismatch: expected {}, got {}",
            expected_last_seq, response.last_acked_seq
        )));
    }
    if response.written_through != expected_written_through {
        return Err(ClientError::UnknownOutcome(format!(
            "worker WriteStream written_through mismatch: expected {}, got {}",
            expected_written_through, response.written_through
        )));
    }
    Ok(response)
}

pub(super) fn build_commit_write_request(
    attempt: &AttemptContext,
    handle: &WorkerBlockWriteHandle,
    effective_len: u64,
    commit_seq: u64,
    require_sync: bool,
) -> ClientResult<proto::worker::CommitWriteRequestProto> {
    validate_handle_for_worker_control(handle)?;
    Ok(proto::worker::CommitWriteRequestProto {
        header: Some(attempt.data_header()),
        group_name: handle.group_name.to_string(),
        block_id: Some(handle.target.block_id.into()),
        stream_id: Some(handle.stream_id),
        effective_len,
        block_stamp: handle.target.block_stamp,
        token: Some(handle.target.fencing_token.into()),
        commit_seq,
        require_sync,
        worker_run_id: handle.worker.worker_run_id.to_string(),
        block_format_id: handle.target.block_format_id.as_raw(),
        block_size: handle.target.block_size,
        chunk_size: handle.target.chunk_size,
    })
}

pub(super) fn parse_commit_write_response(
    attempt: &AttemptContext,
    handle: &WorkerBlockWriteHandle,
    effective_len: u64,
    response: proto::worker::CommitWriteResponseProto,
) -> ClientResult<WorkerCommitResult> {
    parse_worker_control_header(attempt, response.header.as_ref())?;
    if response.effective_len != effective_len {
        return Err(side_effect_response_body_mismatch(
            "CommitWrite",
            format!(
                "effective_len expected {}, got {}",
                effective_len, response.effective_len
            ),
        ));
    }
    if response.block_stamp != handle.target.block_stamp {
        return Err(side_effect_response_body_mismatch(
            "CommitWrite",
            format!(
                "block_stamp expected {}, got {}",
                handle.target.block_stamp, response.block_stamp
            ),
        ));
    }
    if response.written_through != effective_len {
        return Err(side_effect_response_body_mismatch(
            "CommitWrite",
            format!(
                "written_through expected {}, got {}",
                effective_len, response.written_through
            ),
        ));
    }
    Ok(WorkerCommitResult {
        effective_len: response.effective_len,
        block_stamp: response.block_stamp,
        written_through: response.written_through,
    })
}

pub(super) fn build_sync_committed_block_request(
    attempt: &AttemptContext,
    handle: &WorkerBlockWriteHandle,
    expected_len: u64,
) -> ClientResult<proto::worker::SyncCommittedBlockRequestProto> {
    validate_handle_for_worker_sync(handle)?;
    Ok(proto::worker::SyncCommittedBlockRequestProto {
        header: Some(attempt.data_header()),
        group_name: handle.group_name.to_string(),
        block_id: Some(handle.target.block_id.into()),
        block_stamp: handle.target.block_stamp,
        expected_block_len: expected_len,
        worker_run_id: handle.worker.worker_run_id.to_string(),
        block_format_id: handle.target.block_format_id.as_raw(),
        block_size: handle.target.block_size,
        chunk_size: handle.target.chunk_size,
    })
}

pub(super) fn parse_sync_committed_block_response(
    attempt: &AttemptContext,
    handle: &WorkerBlockWriteHandle,
    expected_len: u64,
    response: proto::worker::SyncCommittedBlockResponseProto,
) -> ClientResult<WorkerBlockSyncResult> {
    parse_worker_control_header(attempt, response.header.as_ref())?;
    if response.effective_len != expected_len {
        return Err(side_effect_response_body_mismatch(
            "SyncCommittedBlock",
            format!(
                "effective_len expected {}, got {}",
                expected_len, response.effective_len
            ),
        ));
    }
    if response.block_stamp != handle.target.block_stamp {
        return Err(side_effect_response_body_mismatch(
            "SyncCommittedBlock",
            format!(
                "block_stamp expected {}, got {}",
                handle.target.block_stamp, response.block_stamp
            ),
        ));
    }
    Ok(WorkerBlockSyncResult {
        effective_len: response.effective_len,
        block_stamp: response.block_stamp,
    })
}

pub(super) fn build_abort_write_request(
    attempt: &AttemptContext,
    handle: &WorkerBlockWriteHandle,
) -> ClientResult<proto::worker::AbortWriteRequestProto> {
    validate_handle_for_worker_control(handle)?;
    Ok(proto::worker::AbortWriteRequestProto {
        header: Some(attempt.data_header()),
        group_name: handle.group_name.to_string(),
        block_id: Some(handle.target.block_id.into()),
        stream_id: Some(handle.stream_id),
        token: Some(handle.target.fencing_token.into()),
    })
}

pub(super) fn validate_abort_write_response(
    attempt: &AttemptContext,
    response: proto::worker::AbortWriteResponseProto,
) -> ClientResult<()> {
    parse_worker_control_header(attempt, response.header.as_ref())?;
    if !response.aborted {
        return Err(ClientError::UnknownOutcome(
            "worker AbortWrite response did not confirm abort".to_string(),
        ));
    }
    Ok(())
}

pub(super) async fn read_stream_to_bytes(
    stream: &mut tonic::codec::Streaming<proto::worker::ReadStreamResponseProto>,
    block_read: &PlannedBlockRead,
) -> ClientResult<Bytes> {
    let mut output = BytesMut::with_capacity(block_read.len as usize);
    let mut expected_offset = block_read.block_offset;
    while let Some(frame) = stream.message().await.map_err(ClientError::from)? {
        if append_read_stream_frame(&mut output, &mut expected_offset, block_read, frame)? {
            break;
        }
    }
    finish_read_stream_output(output, block_read)
}

pub(super) fn append_read_stream_frame(
    output: &mut BytesMut,
    expected_offset: &mut u64,
    block_read: &PlannedBlockRead,
    frame: proto::worker::ReadStreamResponseProto,
) -> ClientResult<bool> {
    if frame.offset_in_block != *expected_offset {
        return Err(ClientError::Worker(format!(
            "worker read frame offset mismatch: expected {}, got {}",
            *expected_offset, frame.offset_in_block
        )));
    }
    if frame.data.is_empty() && !frame.eos {
        return Err(ClientError::Worker(
            "worker read returned zero-length non-final frame".to_string(),
        ));
    }
    let remaining = block_read.len as usize - output.len();
    if frame.data.len() > remaining {
        return Err(ClientError::Worker(format!(
            "worker read frame exceeded requested block read: remaining {}, got {}",
            remaining,
            frame.data.len()
        )));
    }
    *expected_offset = expected_offset
        .checked_add(frame.data.len() as u64)
        .ok_or_else(|| ClientError::Worker("worker read frame offset overflow".to_string()))?;
    output.extend_from_slice(&frame.data);
    Ok(frame.eos)
}

pub(super) fn finish_read_stream_output(output: BytesMut, block_read: &PlannedBlockRead) -> ClientResult<Bytes> {
    if output.len() != block_read.len as usize {
        return Err(ClientError::Worker(format!(
            "worker read ended after {} bytes, expected {}",
            output.len(),
            block_read.len
        )));
    }
    Ok(output.freeze())
}

pub(super) fn parse_worker_control_header(
    attempt: &AttemptContext,
    header: Option<&proto::worker::DataResponseHeaderProto>,
) -> ClientResult<()> {
    let Some(header) = header else {
        return Err(invalid_worker_header("worker OK response missing DataResponseHeader"));
    };
    let client = header.client.as_ref().ok_or_else(|| {
        invalid_worker_header("worker OK response invalid DataResponseHeader: missing client identity")
    })?;
    let client_id = proto::convert::required_client_id(client.client_id, "client_id")
        .map_err(|err| invalid_worker_header(format!("worker OK response invalid DataResponseHeader: {err}")))?;
    let call_id = proto::convert::require_call_id(&client.call_id, "call_id")
        .map_err(|err| invalid_worker_header(format!("worker OK response invalid DataResponseHeader: {err}")))?;
    let response_identity = HeaderIdentity {
        call_id,
        client_id,
        group_name: None,
    };
    let request_identity = attempt.header_identity();
    if response_identity.matches_request(&request_identity) {
        return validate_data_header_or_action(Some(header)).map_err(ClientError::from);
    }
    if response_identity.client_id != request_identity.client_id {
        return Err(invalid_worker_header(
            "worker OK response invalid DataResponseHeader: client_id mismatch",
        ));
    }
    if response_identity.call_id != request_identity.call_id {
        return Err(invalid_worker_header(
            "worker OK response invalid DataResponseHeader: call_id mismatch",
        ));
    }
    validate_data_header_or_action(Some(header)).map_err(ClientError::from)
}

pub(super) fn invalid_worker_header(message: impl Into<String>) -> ClientError {
    ClientError::from(invalid_header_action(message))
}

pub(super) fn is_transient_worker_transport_status(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded | tonic::Code::ResourceExhausted
    )
}

pub(super) fn build_tonic_request<T>(attempt: &AttemptContext, message: T) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    if let Some(timeout) = attempt.timeout_remaining() {
        request.set_timeout(timeout.max(Duration::from_millis(1)));
    }
    request
}

pub(super) fn default_frame_size(len: u32) -> u32 {
    len.clamp(1, 1024 * 1024)
}

pub(super) fn validate_worker_read_result(
    block_read: &PlannedBlockRead,
    result: &WorkerReadResult,
) -> ClientResult<()> {
    if result.block_stamp != block_read.block_stamp {
        return Err(block_stamp_mismatch_error(
            block_read,
            result.block_stamp,
            "OpenReadStream",
        ));
    }
    let required_committed_length = block_read
        .block_offset
        .checked_add(u64::from(block_read.len))
        .ok_or_else(|| ClientError::InvalidLayout("planned block read block range overflow".to_string()))?;
    if result.committed_length < required_committed_length {
        return Err(invalid_response(
            "OpenReadStream",
            format!(
                "committed_length {} does not cover requested block range ending at {}",
                result.committed_length, required_committed_length
            ),
        ));
    }
    Ok(())
}

fn block_stamp_mismatch_error(block_read: &PlannedBlockRead, actual: u64, operation: &'static str) -> ClientError {
    let message = format!(
        "block stamp mismatch from {operation}: block={} expected={}, got={}",
        block_read.block_id, block_read.block_stamp, actual
    );
    let canonical = CanonicalError::need_refresh_with_hint(
        RpcErrorCode::BlockStampMismatch,
        RefreshReason::BlockStampMismatch,
        CanonicalRefreshHint {
            worker_resolve_required: true,
            ..CanonicalRefreshHint::default()
        },
        message,
    );
    ClientError::from(crate::canonical::ClientAction::Refresh {
        reason: RefreshReason::BlockStampMismatch,
        hint: Box::new(crate::canonical::RefreshHint {
            worker_resolve_required: true,
            ..crate::canonical::RefreshHint::default()
        }),
        canonical: Box::new(canonical),
    })
}

fn validate_worker_write_target(target: &WorkerWriteTarget) -> ClientResult<()> {
    let block = target.target.block_id;
    if block.data_handle_id.as_raw() == 0 {
        return Err(ClientError::InvalidLayout(
            "write target block_id data_handle_id must be non-zero".to_string(),
        ));
    }
    BlockShape::new(
        target.target.block_format_id,
        target.target.block_size,
        target.target.chunk_size,
        target.target.effective_len,
    )
    .map_err(|err| ClientError::InvalidLayout(format!("write target has invalid shape: {err}")))?;
    if target.target.worker_endpoints.is_empty() {
        return Err(ClientError::InvalidLayout(
            "write target has no worker endpoints".to_string(),
        ));
    }
    if target.target.block_stamp == 0 {
        return Err(ClientError::InvalidLayout(
            "write target block_stamp must be non-zero".to_string(),
        ));
    }
    validate_fencing_token(&target.target)?;
    Ok(())
}

fn validate_handle_for_worker_control(handle: &WorkerBlockWriteHandle) -> ClientResult<()> {
    if handle.stream_id.high == 0 && handle.stream_id.low == 0 {
        return Err(ClientError::InvalidArgument(
            "worker write control requires non-zero stream_id".to_string(),
        ));
    }
    validate_fencing_token(&handle.target)
}

fn validate_fencing_token(target: &WriteTarget) -> ClientResult<()> {
    let block = target.block_id;
    let token = target.fencing_token;
    if token.owner.is_zero() || token.epoch == 0 {
        return Err(ClientError::InvalidLayout(
            "write target fencing_token owner and epoch must be non-zero".to_string(),
        ));
    }
    if token.block_id != block {
        return Err(ClientError::InvalidLayout(
            "write target fencing_token block_id must match target block_id".to_string(),
        ));
    }
    Ok(())
}

fn validate_handle_for_worker_sync(handle: &WorkerBlockWriteHandle) -> ClientResult<()> {
    if handle.target.block_stamp == 0 {
        return Err(ClientError::InvalidArgument(
            "worker block sync requires non-zero block_stamp".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use common::error::canonical::{
        CanonicalError, ErrorClass as CanonicalErrorClass, ErrorCode as CanonicalErrorCode,
        RefreshHint as CanonicalRefreshHint, RefreshReason as CanonicalRefreshReason,
    };
    use common::header::RpcErrorCode;
    use proto::convert::canonical_to_error_detail;
    use types::lease::FencingToken;
    use types::{BlockId, BlockIndex, ClientId, DataHandleId, WorkerEndpointInfo, WorkerId, WorkerNetProtocol};

    use crate::canonical::ClientAction;
    use crate::runtime::{ErrorClass, ErrorClassifier, OperationContext, OperationIdentity, OperationKind};

    #[test]
    fn missing_worker_control_header_is_invalid_header_action() {
        let attempt = data_attempt_context();
        let err = parse_worker_control_header(&attempt, None).expect_err("missing data header must fail");

        assert_ne!(ErrorClassifier.classify_error(&err), ErrorClass::RetryableTransport);
        match action(&err) {
            ClientAction::Fail { canonical } => {
                assert!(matches!(
                    canonical.code,
                    Some(common::error::canonical::ErrorCode::RpcCode(
                        RpcErrorCode::InvalidHeader
                    ))
                ));
                assert!(canonical.message.contains("missing DataResponseHeader"));
            }
            other => panic!("expected invalid header failure, got {other:?}"),
        }
    }

    #[test]
    fn malformed_worker_control_header_is_invalid_header_not_transport_retry() {
        let attempt = data_attempt_context();
        let malformed = proto::worker::DataResponseHeaderProto::default();

        let err = parse_worker_control_header(&attempt, Some(&malformed)).expect_err("malformed data header must fail");

        assert_ne!(ErrorClassifier.classify_error(&err), ErrorClass::RetryableTransport);
        match action(&err) {
            ClientAction::Fail { canonical } => {
                assert!(matches!(
                    canonical.code,
                    Some(common::error::canonical::ErrorCode::RpcCode(
                        RpcErrorCode::InvalidHeader
                    ))
                ));
                assert!(canonical.message.contains("invalid DataResponseHeader"));
            }
            other => panic!("expected invalid header failure, got {other:?}"),
        }
    }

    #[test]
    fn worker_control_header_preserves_refresh_reason() {
        let attempt = data_attempt_context();
        let canonical = CanonicalError::need_refresh_with_hint(
            RpcErrorCode::BlockStampMismatch,
            CanonicalRefreshReason::BlockStampMismatch,
            CanonicalRefreshHint {
                worker_resolve_required: true,
                ..CanonicalRefreshHint::default()
            },
            "worker requires refreshed location",
        );
        let header = proto::worker::DataResponseHeaderProto {
            client: Some(attempt.client_info()),
            error: Some(canonical_to_error_detail(&canonical)),
        };

        let err = parse_worker_control_header(&attempt, Some(&header)).expect_err("refresh error must surface");

        match action(&err) {
            ClientAction::Refresh { reason, hint, .. } => {
                assert_eq!(*reason, CanonicalRefreshReason::BlockStampMismatch);
                assert!(hint.worker_resolve_required);
            }
            other => panic!("expected refresh action, got {other:?}"),
        }
    }

    #[test]
    fn worker_read_result_block_stamp_mismatch_is_typed_refresh_error() {
        let block_read = planned_block_read(77);
        let err = validate_worker_read_result(
            &block_read,
            &WorkerReadResult {
                bytes: Bytes::new(),
                block_stamp: 78,
                committed_length: block_read.effective_len,
            },
        )
        .expect_err("block stamp mismatch must be typed");

        match action(&err) {
            ClientAction::Refresh { reason, canonical, .. } => {
                assert_eq!(*reason, CanonicalRefreshReason::BlockStampMismatch);
                assert_eq!(
                    canonical.code,
                    Some(CanonicalErrorCode::RpcCode(RpcErrorCode::BlockStampMismatch))
                );
            }
            other => panic!("expected block stamp refresh action, got {other:?}"),
        }
    }

    #[test]
    fn worker_control_header_with_wrong_client_id_is_invalid_header() {
        let attempt = write_attempt_context();
        let mut header = ok_data_header(&attempt);
        header.client.as_mut().expect("client").client_id =
            Some(ClientId::new(attempt.client_id().as_raw() + 1).into());

        let err = parse_worker_control_header(&attempt, Some(&header)).expect_err("wrong client_id must fail");

        assert_invalid_worker_header(&err);
        match action(&err) {
            ClientAction::Fail { canonical } => assert!(canonical.message.contains("client_id")),
            other => panic!("expected invalid header failure, got {other:?}"),
        }
    }

    #[test]
    fn worker_control_header_with_wrong_call_id_is_invalid_header() {
        let attempt = write_attempt_context();
        let mut header = ok_data_header(&attempt);
        header.client.as_mut().expect("client").call_id = types::CallId::new().to_string();

        let err = parse_worker_control_header(&attempt, Some(&header)).expect_err("wrong call_id must fail");

        assert_invalid_worker_header(&err);
        match action(&err) {
            ClientAction::Fail { canonical } => assert!(canonical.message.contains("call_id")),
            other => panic!("expected invalid header failure, got {other:?}"),
        }
    }

    #[test]
    fn open_read_stream_request_uses_metadata_block_stamp() {
        let attempt = data_attempt_context();
        let block_read = planned_block_read(77);
        let worker = worker_endpoint();
        let group_name = test_group_name();

        let request = build_open_read_stream_request(&attempt, &group_name, &block_read, &worker).expect("request");

        assert_eq!(request.block_stamp, 77);
        assert_eq!(request.worker_run_id, test_worker_run_id().to_string());
        assert_eq!(
            request.block_format_id,
            types::BlockFormatId::CURRENT_FOR_NEW_FILE.as_raw()
        );
        assert_eq!(request.block_size, 4096);
        assert_eq!(request.chunk_size, 4096);
        assert_eq!(request.effective_len, 5);
    }

    #[test]
    fn open_read_stream_request_rejects_zero_block_stamp() {
        let attempt = data_attempt_context();
        let block_read = planned_block_read(0);
        let worker = worker_endpoint();
        let group_name = test_group_name();

        let err = build_open_read_stream_request(&attempt, &group_name, &block_read, &worker)
            .expect_err("zero stamp must fail");

        assert!(matches!(err, ClientError::InvalidLayout(msg) if msg.contains("block_stamp")));
    }

    #[test]
    fn open_read_stream_request_rejects_zero_expected_fields() {
        let attempt = data_attempt_context();
        let mut block_read = planned_block_read(77);
        block_read.block_size = 0;
        let worker = worker_endpoint();
        let group_name = test_group_name();

        let err = build_open_read_stream_request(&attempt, &group_name, &block_read, &worker)
            .expect_err("zero block_size must not be defaulted");

        assert!(matches!(err, ClientError::InvalidLayout(msg) if msg.contains("expected block shape")));
    }

    #[test]
    fn open_write_stream_request_uses_metadata_target_fields() {
        let attempt = write_attempt_context();
        let target = worker_write_target();
        let worker = target.target.worker_endpoints[0].clone();

        let request = build_open_write_stream_request(&attempt, &target, &worker).expect("open write request");

        assert_eq!(request.group_name, "root");
        assert_eq!(request.block_id.as_ref().map(|block| block.data_handle_id), Some(202));
        assert_eq!(request.block_size, 4096);
        assert_eq!(request.block_stamp, 77);
        assert_eq!(request.chunk_size, 4096);
        assert_eq!(
            request.block_format_id,
            types::BlockFormatId::CURRENT_FOR_NEW_FILE.as_raw()
        );
        assert_eq!(request.worker_run_id, test_worker_run_id().to_string());
        assert_eq!(request.effective_len, 5);
        assert_eq!(
            request.token.as_ref().and_then(|token| token.owner),
            Some(ClientId::new(7).into())
        );
    }

    #[test]
    fn open_write_stream_request_rejects_zero_metadata_target_shape() {
        let attempt = write_attempt_context();
        let mut target = worker_write_target();
        target.target.chunk_size = 0;
        let worker = target.target.worker_endpoints[0].clone();

        let err = build_open_write_stream_request(&attempt, &target, &worker)
            .expect_err("zero chunk_size must not be defaulted");

        assert!(matches!(err, ClientError::InvalidLayout(msg) if msg.contains("chunk_size")));
    }

    #[test]
    fn write_stream_requests_are_monotonic() {
        let handle = worker_block_write_handle(4);

        let requests = build_write_stream_requests(&handle, Bytes::from_static(b"abcdef")).expect("requests");

        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].seq, 1);
        assert_eq!(requests[0].offset_in_block, 0);
        assert_eq!(requests[0].data, Bytes::from_static(b"abcd"));
        assert_eq!(requests[1].seq, 2);
        assert_eq!(requests[1].offset_in_block, 4);
        assert_eq!(requests[1].data, Bytes::from_static(b"ef"));
    }

    #[test]
    fn commit_write_request_uses_length_and_fencing_token() {
        let attempt = write_attempt_context();
        let handle = worker_block_write_handle(1024);

        let request = build_commit_write_request(&attempt, &handle, 5, 1, false).expect("commit write request");

        assert_eq!(request.effective_len, 5);
        assert_eq!(request.block_stamp, 77);
        assert_eq!(request.worker_run_id, test_worker_run_id().to_string());
        assert_eq!(
            request.block_format_id,
            types::BlockFormatId::CURRENT_FOR_NEW_FILE.as_raw()
        );
        assert_eq!(request.block_size, 4096);
        assert_eq!(request.chunk_size, 4096);
        assert_eq!(request.commit_seq, 1);
        assert!(!request.require_sync);
        assert_eq!(
            request.token.as_ref().and_then(|token| token.owner),
            Some(ClientId::new(7).into())
        );
    }

    #[test]
    fn open_write_stream_missing_stream_id_is_unknown_outcome() {
        let attempt = write_attempt_context();
        let target = worker_write_target();
        let worker = target.target.worker_endpoints[0].clone();
        let response = proto::worker::OpenWriteStreamResponseProto {
            header: Some(ok_data_header(&attempt)),
            stream_id: None,
            frame_size: 1024,
            block_stamp: target.target.block_stamp,
            ..proto::worker::OpenWriteStreamResponseProto::default()
        };

        let err = parse_open_write_stream_response(&attempt, &target, &worker, response)
            .expect_err("missing OpenWriteStream stream_id must be unknown");

        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("OpenWriteStream")));
    }

    #[test]
    fn open_write_stream_body_mismatch_is_unknown_outcome() {
        let attempt = write_attempt_context();
        let target = worker_write_target();
        let worker = target.target.worker_endpoints[0].clone();
        let response = proto::worker::OpenWriteStreamResponseProto {
            header: Some(ok_data_header(&attempt)),
            stream_id: Some(proto::common::StreamIdProto { high: 1, low: 1 }),
            frame_size: 1024,
            block_stamp: target.target.block_stamp + 1,
            ..proto::worker::OpenWriteStreamResponseProto::default()
        };

        let err = parse_open_write_stream_response(&attempt, &target, &worker, response)
            .expect_err("OpenWriteStream block_stamp mismatch must be unknown");

        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("OpenWriteStream")));
    }

    #[test]
    fn commit_write_body_mismatch_is_unknown_outcome() {
        let attempt = write_attempt_context();
        let handle = worker_block_write_handle(1024);
        let response = proto::worker::CommitWriteResponseProto {
            header: Some(ok_data_header(&attempt)),
            effective_len: 4,
            block_stamp: handle.target.block_stamp,
            written_through: 5,
        };

        let err = parse_commit_write_response(&attempt, &handle, 5, response)
            .expect_err("CommitWrite length mismatch must be unknown");

        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("CommitWrite")));
    }

    #[test]
    fn worker_fatal_fencing_mismatch_is_typed_error() {
        let attempt = write_attempt_context();
        let err = parse_worker_control_header(
            &attempt,
            Some(&data_header_with_error(
                &attempt,
                CanonicalError {
                    class: CanonicalErrorClass::Fatal,
                    code: Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Fencing)),
                    reason: None,
                    retry_after_ms: None,
                    message: "fencing mismatch".to_string(),
                    refresh_hint: None,
                },
            )),
        )
        .expect_err("fatal fencing mismatch must fail");

        assert_eq!(ErrorClassifier.classify_error(&err), ErrorClass::Fencing);
        assert_ne!(ErrorClassifier.classify_error(&err), ErrorClass::RetryableTransport);
    }

    #[test]
    fn worker_run_mismatch_is_typed_refresh_error() {
        let attempt = write_attempt_context();
        let err = parse_worker_control_header(
            &attempt,
            Some(&data_header_with_error(
                &attempt,
                CanonicalError::need_refresh(
                    RpcErrorCode::WorkerRunMismatch,
                    CanonicalRefreshReason::WorkerRunMismatch,
                    "worker run mismatch",
                ),
            )),
        )
        .expect_err("worker run mismatch must fail");

        assert_eq!(
            ErrorClassifier.classify_error(&err),
            ErrorClass::NeedRefresh(crate::runtime::RefreshReason::WorkerRunMismatch)
        );
    }

    #[test]
    fn tonic_request_uses_attempt_timeout_when_present() {
        let attempt = write_attempt_context().with_operation_timeout_ms(Some(5_000));

        let request = build_tonic_request(&attempt, ());

        assert!(request.metadata().get("grpc-timeout").is_some());
    }

    #[test]
    fn tonic_request_has_no_timeout_without_attempt_deadline() {
        let attempt = write_attempt_context().with_operation_timeout_ms(None);

        let request = build_tonic_request(&attempt, ());

        assert!(request.metadata().get("grpc-timeout").is_none());
    }

    #[test]
    fn write_stream_partial_ack_is_unknown_outcome() {
        let response = proto::worker::WriteStreamResponseProto {
            accepted: true,
            last_acked_seq: 1,
            written_through: 2,
        };

        let err = validate_write_stream_response(response, 2, 4).expect_err("partial WriteStream ack must be unknown");

        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("WriteStream")));
    }

    #[test]
    fn read_stream_frame_validation_rejects_offset_mismatch() {
        let block_read = planned_block_read(77);
        let mut output = BytesMut::new();
        let mut expected_offset = 0;

        let err = append_read_stream_frame(
            &mut output,
            &mut expected_offset,
            &block_read,
            read_frame(1, b"abcd", true),
        )
        .expect_err("offset mismatch must fail");

        assert!(matches!(err, ClientError::Worker(msg) if msg.contains("offset mismatch")));
        assert!(output.is_empty());
    }

    #[test]
    fn read_stream_frame_validation_rejects_oversized_frame() {
        let block_read = planned_block_read(77);
        let mut output = BytesMut::new();
        let mut expected_offset = 0;

        let err = append_read_stream_frame(
            &mut output,
            &mut expected_offset,
            &block_read,
            read_frame(0, b"abcde", true),
        )
        .expect_err("oversized frame must fail");

        assert!(matches!(err, ClientError::Worker(msg) if msg.contains("exceeded requested block read")));
        assert!(output.is_empty());
    }

    #[test]
    fn read_stream_frame_validation_rejects_zero_length_non_final_frame() {
        let block_read = planned_block_read(77);
        let mut output = BytesMut::new();
        let mut expected_offset = 0;

        let err = append_read_stream_frame(
            &mut output,
            &mut expected_offset,
            &block_read,
            read_frame(0, b"", false),
        )
        .expect_err("zero-length non-final frame must fail");

        assert!(matches!(err, ClientError::Worker(msg) if msg.contains("zero-length non-final")));
        assert!(output.is_empty());
    }

    #[test]
    fn read_stream_frame_validation_rejects_early_eof() {
        let block_read = planned_block_read(77);
        let mut output = BytesMut::new();
        output.extend_from_slice(b"ab");

        let err = finish_read_stream_output(output, &block_read).expect_err("short stream must fail");

        assert!(matches!(err, ClientError::Worker(msg) if msg.contains("ended after 2 bytes")));
    }

    fn data_attempt_context() -> AttemptContext {
        let operation = OperationContext::new(
            ClientId::new(7),
            OperationKind::WorkerReadData,
            "OpenReadStream",
            OperationIdentity::path("/alpha"),
        )
        .expect("operation context");
        AttemptContext::for_data(&operation, 0)
    }

    fn write_attempt_context() -> AttemptContext {
        let operation = OperationContext::new(
            ClientId::new(7),
            OperationKind::WorkerWriteData,
            "OpenWriteStream",
            OperationIdentity::session("/alpha", "handle=1"),
        )
        .expect("operation context");
        AttemptContext::for_data(&operation, 0)
    }

    fn worker_write_target() -> WorkerWriteTarget {
        WorkerWriteTarget {
            group_name: test_group_name(),
            target: WriteTarget {
                block_id: BlockId::new(DataHandleId::new(202), BlockIndex::new(0)),
                file_offset: 0,
                block_size: 4096,
                effective_len: 5,
                worker_endpoints: vec![worker_endpoint()],
                fencing_token: FencingToken {
                    block_id: BlockId::new(DataHandleId::new(202), BlockIndex::new(0)),
                    owner: ClientId::new(7),
                    epoch: 1,
                },
                block_stamp: 77,
                chunk_size: 4096,
                block_format_id: types::BlockFormatId::CURRENT_FOR_NEW_FILE,
                tier: types::Tier::Hdd,
            },
        }
    }

    fn worker_block_write_handle(frame_size: u32) -> WorkerBlockWriteHandle {
        WorkerBlockWriteHandle {
            group_name: test_group_name(),
            worker: worker_endpoint(),
            target: worker_write_target().target,
            stream_id: proto::common::StreamIdProto { high: 1, low: 1 },
            frame_size,
            next_seq: 1,
        }
    }

    fn worker_endpoint() -> WorkerEndpointInfo {
        WorkerEndpointInfo {
            worker_id: WorkerId::new(1),
            endpoint: "127.0.0.1:19101".to_string(),
            worker_net_protocol: WorkerNetProtocol::Grpc,
            worker_run_id: test_worker_run_id(),
        }
    }

    fn planned_block_read(block_stamp: u64) -> PlannedBlockRead {
        PlannedBlockRead {
            file_offset: 0,
            len: 4,
            end_file_offset: 4,
            block_id: BlockId::new(DataHandleId::new(202), BlockIndex::new(0)),
            block_offset: 0,
            workers: vec![worker_endpoint()],
            block_stamp,
            block_format_id: types::BlockFormatId::CURRENT_FOR_NEW_FILE,
            block_size: 4096,
            chunk_size: 4096,
            effective_len: 5,
        }
    }

    fn read_frame(offset_in_block: u64, data: &'static [u8], eos: bool) -> proto::worker::ReadStreamResponseProto {
        proto::worker::ReadStreamResponseProto {
            offset_in_block,
            data: Bytes::from_static(data),
            checksum32: 0,
            eos,
        }
    }

    fn data_header_with_error(
        attempt: &AttemptContext,
        canonical: CanonicalError,
    ) -> proto::worker::DataResponseHeaderProto {
        proto::worker::DataResponseHeaderProto {
            client: Some(attempt.client_info()),
            error: Some(canonical_to_error_detail(&canonical)),
        }
    }

    fn ok_data_header(attempt: &AttemptContext) -> proto::worker::DataResponseHeaderProto {
        proto::worker::DataResponseHeaderProto {
            client: Some(attempt.client_info()),
            error: None,
        }
    }

    fn test_worker_run_id() -> types::WorkerRunId {
        "550e8400-e29b-41d4-a716-446655440000"
            .parse()
            .expect("valid test WorkerRunId")
    }

    fn test_group_name() -> GroupName {
        GroupName::parse("root").unwrap()
    }

    fn assert_invalid_worker_header(err: &ClientError) {
        assert_ne!(ErrorClassifier.classify_error(err), ErrorClass::RetryableTransport);
        match action(err) {
            ClientAction::Fail { canonical } => {
                assert!(matches!(
                    canonical.code,
                    Some(common::error::canonical::ErrorCode::RpcCode(
                        RpcErrorCode::InvalidHeader
                    ))
                ));
            }
            other => panic!("expected invalid header failure, got {other:?}"),
        }
    }

    fn action(err: &ClientError) -> &ClientAction {
        match err {
            ClientError::Action(action) => action.as_ref(),
            other => panic!("expected action error, got {other:?}"),
        }
    }
}
