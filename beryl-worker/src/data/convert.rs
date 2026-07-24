// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Explicit conversion between worker wire messages and core domain types.

use beryl_common::header::RequestHeader;
use beryl_proto::common::{BlockIdProto, ByteRangeProto, FencingTokenProto, StreamIdProto, TierProto};
use beryl_proto::convert as proto_convert;
use beryl_proto::worker::{
    AbortWriteRequestProto, CommitWriteRequestProto, DataRequestHeaderProto, OpenReadStreamRequestProto,
    OpenWriteStreamRequestProto, ReadStreamResponseProto, SyncCommittedBlockRequestProto, WriteStreamRequestProto,
    WriteStreamResponseProto,
};
use beryl_types::chunk::ByteRange;
use beryl_types::ids::{BlockId, StreamId};
use beryl_types::layout::BlockFormatId;
use beryl_types::lease::FencingToken;
use beryl_types::{GroupName, WorkerRunId};

use crate::data::core::{
    AbortWriteRequest, CommitWriteRequest, ReadFrame, ReadOpenRequest, SyncCommittedBlockRequest, WorkerCoreResult,
    WriteFrame, WriteFrameResult, WriteOpenRequest,
};
use crate::error::WorkerError;
use crate::store::block::ChecksumKind;

pub fn proto_to_block_id(proto: Option<BlockIdProto>, field_name: &str) -> WorkerCoreResult<BlockId> {
    proto_convert::required_block_id(proto, field_name).map_err(WorkerError::InvalidArgument)
}

pub fn proto_to_stream_id(proto: Option<StreamIdProto>, field_name: &str) -> WorkerCoreResult<StreamId> {
    proto_convert::required_stream_id(proto, field_name).map_err(WorkerError::InvalidArgument)
}

pub fn stream_id_to_proto(stream_id: StreamId) -> StreamIdProto {
    stream_id.into()
}

pub fn proto_to_byte_range(proto: &ByteRangeProto) -> ByteRange {
    proto.into()
}

pub fn block_id_to_proto(block_id: BlockId) -> BlockIdProto {
    block_id.into()
}

pub fn byte_range_to_proto(byte_range: ByteRange) -> ByteRangeProto {
    byte_range.into()
}

pub fn fencing_token_to_proto(token: FencingToken) -> FencingTokenProto {
    token.into()
}

pub fn request_header_to_data_proto(ctx: &RequestHeader) -> DataRequestHeaderProto {
    ctx.into()
}

pub fn read_open_request_to_proto(req: ReadOpenRequest, ctx: &RequestHeader) -> OpenReadStreamRequestProto {
    OpenReadStreamRequestProto {
        header: Some(request_header_to_data_proto(ctx)),
        group_name: req.group_name.to_string(),
        block_id: Some(block_id_to_proto(req.block_id)),
        byte_range: Some(byte_range_to_proto(req.byte_range)),
        block_stamp: req.block_stamp,
        frame_size: req.frame_size,
        worker_run_id: req.worker_run_id.to_string(),
        block_format_id: req.block_format_id.as_raw(),
        block_size: req.block_size,
        chunk_size: req.chunk_size,
        effective_len: req.effective_len,
    }
}

pub fn write_open_request_to_proto(req: WriteOpenRequest, ctx: &RequestHeader) -> OpenWriteStreamRequestProto {
    OpenWriteStreamRequestProto {
        header: Some(request_header_to_data_proto(ctx)),
        group_name: req.group_name.to_string(),
        block_id: Some(block_id_to_proto(req.block_id)),
        block_size: req.block_size,
        block_stamp: req.block_stamp,
        chunk_size: req.chunk_size,
        token: Some(fencing_token_to_proto(req.token)),
        frame_size: req.frame_size,
        block_format_id: req.block_format_id.as_raw(),
        worker_run_id: req.worker_run_id.to_string(),
        effective_len: req.effective_len,
        tier: TierProto::from(req.tier) as i32,
    }
}

pub fn write_frame_to_proto(frame: WriteFrame) -> WriteStreamRequestProto {
    WriteStreamRequestProto {
        stream_id: Some(stream_id_to_proto(frame.stream_id)),
        seq: frame.seq,
        offset_in_block: frame.offset_in_block,
        data: frame.data,
    }
}

pub fn commit_write_request_to_proto(req: CommitWriteRequest, ctx: &RequestHeader) -> CommitWriteRequestProto {
    CommitWriteRequestProto {
        header: Some(request_header_to_data_proto(ctx)),
        group_name: req.group_name.to_string(),
        block_id: Some(block_id_to_proto(req.block_id)),
        stream_id: Some(stream_id_to_proto(req.stream_id)),
        effective_len: req.effective_len,
        block_stamp: req.block_stamp,
        token: Some(fencing_token_to_proto(req.token)),
        commit_seq: req.commit_seq,
        require_sync: req.require_sync,
        worker_run_id: req.worker_run_id.to_string(),
        block_format_id: req.block_format_id.as_raw(),
        block_size: req.block_size,
        chunk_size: req.chunk_size,
    }
}

pub fn sync_committed_block_request_to_proto(
    req: SyncCommittedBlockRequest,
    ctx: &RequestHeader,
) -> SyncCommittedBlockRequestProto {
    SyncCommittedBlockRequestProto {
        header: Some(request_header_to_data_proto(ctx)),
        group_name: req.group_name.to_string(),
        block_id: Some(block_id_to_proto(req.block_id)),
        block_stamp: req.block_stamp,
        expected_block_len: req.expected_block_len,
        worker_run_id: req.worker_run_id.to_string(),
        block_format_id: req.block_format_id.as_raw(),
        block_size: req.block_size,
        chunk_size: req.chunk_size,
    }
}

pub fn abort_write_request_to_proto(req: AbortWriteRequest, ctx: &RequestHeader) -> AbortWriteRequestProto {
    AbortWriteRequestProto {
        header: Some(request_header_to_data_proto(ctx)),
        group_name: req.group_name.to_string(),
        block_id: Some(block_id_to_proto(req.block_id)),
        stream_id: Some(stream_id_to_proto(req.stream_id)),
        token: Some(fencing_token_to_proto(req.token)),
    }
}

pub fn proto_to_read_frame(proto: ReadStreamResponseProto) -> ReadFrame {
    ReadFrame {
        offset_in_block: proto.offset_in_block,
        data: proto.data,
        checksum32: 0,
        eos: proto.eos,
    }
}

pub fn proto_to_write_frame_result(proto: WriteStreamResponseProto) -> WriteFrameResult {
    WriteFrameResult {
        accepted: proto.accepted,
        last_acked_seq: proto.last_acked_seq,
        written_through: proto.written_through,
    }
}

pub fn proto_to_read_open_request(proto: OpenReadStreamRequestProto) -> WorkerCoreResult<ReadOpenRequest> {
    let group_name = proto_to_group_name(&proto.group_name, "group_name")?;
    let block_id = proto_to_block_id(proto.block_id, "block_id")?;
    let byte_range = proto
        .byte_range
        .ok_or_else(|| WorkerError::InvalidArgument("missing byte_range".to_string()))?;
    let worker_run_id = proto_to_worker_run_id(&proto.worker_run_id)?;
    let block_format_id = BlockFormatId::from_raw(proto.block_format_id)
        .map_err(|err| WorkerError::InvalidArgument(format!("block_format_id invalid: {err}")))?;

    Ok(ReadOpenRequest {
        group_name,
        block_id,
        worker_run_id,
        byte_range: proto_to_byte_range(&byte_range),
        block_stamp: proto.block_stamp,
        block_format_id,
        block_size: proto.block_size,
        chunk_size: proto.chunk_size,
        effective_len: proto.effective_len,
        frame_size: proto.frame_size,
    })
}

pub fn proto_to_write_open_request(proto: OpenWriteStreamRequestProto) -> WorkerCoreResult<WriteOpenRequest> {
    let group_name = proto_to_group_name(&proto.group_name, "group_name")?;
    let block_id = proto_to_block_id(proto.block_id, "block_id")?;
    let token = proto_to_required_fencing_token(proto.token, "token")?;
    let block_format_id = BlockFormatId::from_raw(proto.block_format_id)
        .map_err(|err| WorkerError::InvalidArgument(format!("block_format_id invalid: {err}")))?;
    let worker_run_id = proto_to_worker_run_id(&proto.worker_run_id)?;
    let tier = proto_convert::parse_known_tier(proto.tier)
        .map_err(|err| WorkerError::InvalidArgument(format!("tier invalid: {err}")))?;

    Ok(WriteOpenRequest {
        group_name,
        block_id,
        worker_run_id,
        token,
        block_stamp: proto.block_stamp,
        frame_size: proto.frame_size,
        block_size: proto.block_size,
        block_format_id,
        chunk_size: proto.chunk_size,
        effective_len: proto.effective_len,
        checksum_kind: ChecksumKind::None,
        tier,
    })
}

pub fn proto_to_write_frame(proto: WriteStreamRequestProto) -> WorkerCoreResult<WriteFrame> {
    let stream_id = proto_to_stream_id(proto.stream_id, "stream_id")?;

    Ok(WriteFrame {
        stream_id,
        seq: proto.seq,
        offset_in_block: proto.offset_in_block,
        data: proto.data,
        checksum32: 0,
    })
}

pub fn proto_to_commit_write_request(proto: CommitWriteRequestProto) -> WorkerCoreResult<CommitWriteRequest> {
    let stream_id = proto_to_stream_id(proto.stream_id, "stream_id")?;
    let group_name = proto_to_group_name(&proto.group_name, "group_name")?;
    let block_id = proto_to_block_id(proto.block_id, "block_id")?;
    let token = proto_to_required_fencing_token(proto.token, "token")?;
    let worker_run_id = proto_to_worker_run_id(&proto.worker_run_id)?;
    let block_format_id = BlockFormatId::from_raw(proto.block_format_id)
        .map_err(|err| WorkerError::InvalidArgument(format!("block_format_id invalid: {err}")))?;

    Ok(CommitWriteRequest {
        stream_id,
        group_name,
        block_id,
        worker_run_id,
        token,
        commit_seq: proto.commit_seq,
        effective_len: proto.effective_len,
        block_stamp: proto.block_stamp,
        block_format_id,
        block_size: proto.block_size,
        chunk_size: proto.chunk_size,
        require_sync: proto.require_sync,
    })
}

pub fn proto_to_sync_committed_block_request(
    proto: SyncCommittedBlockRequestProto,
) -> WorkerCoreResult<SyncCommittedBlockRequest> {
    let group_name = proto_to_group_name(&proto.group_name, "group_name")?;
    let block_id = proto_to_block_id(proto.block_id, "block_id")?;
    let worker_run_id = proto_to_worker_run_id(&proto.worker_run_id)?;
    let block_format_id = BlockFormatId::from_raw(proto.block_format_id)
        .map_err(|err| WorkerError::InvalidArgument(format!("block_format_id invalid: {err}")))?;

    Ok(SyncCommittedBlockRequest {
        group_name,
        block_id,
        worker_run_id,
        block_stamp: proto.block_stamp,
        expected_block_len: proto.expected_block_len,
        block_format_id,
        block_size: proto.block_size,
        chunk_size: proto.chunk_size,
    })
}

pub fn proto_to_abort_write_request(proto: AbortWriteRequestProto) -> WorkerCoreResult<AbortWriteRequest> {
    let stream_id = proto_to_stream_id(proto.stream_id, "stream_id")?;
    let group_name = proto_to_group_name(&proto.group_name, "group_name")?;
    let block_id = proto_to_block_id(proto.block_id, "block_id")?;
    let token = proto_to_required_fencing_token(proto.token, "token")?;

    Ok(AbortWriteRequest {
        stream_id,
        group_name,
        block_id,
        token,
    })
}

pub fn proto_to_fencing_token(proto: &FencingTokenProto) -> WorkerCoreResult<FencingToken> {
    FencingToken::try_from(*proto).map_err(WorkerError::InvalidArgument)
}

fn proto_to_required_fencing_token(
    proto: Option<FencingTokenProto>,
    field_name: &str,
) -> WorkerCoreResult<FencingToken> {
    proto_convert::required_fencing_token(proto, field_name).map_err(WorkerError::InvalidArgument)
}

fn proto_to_worker_run_id(value: &str) -> WorkerCoreResult<WorkerRunId> {
    proto_convert::require_worker_run_id(value, "worker_run_id").map_err(WorkerError::InvalidArgument)
}

fn proto_to_group_name(value: &str, field_name: &str) -> WorkerCoreResult<GroupName> {
    if value.is_empty() {
        return Err(WorkerError::InvalidArgument(format!("missing {field_name}")));
    }
    GroupName::parse(value).map_err(|err| WorkerError::InvalidArgument(format!("{field_name} invalid: {err}")))
}

#[cfg(test)]
mod tests {
    use beryl_proto::common::{BlockIdProto, ByteRangeProto, ClientInfoProto, FencingTokenProto, StreamIdProto};
    use beryl_proto::worker::{
        AbortWriteRequestProto, CommitWriteRequestProto, DataRequestHeaderProto, OpenReadStreamRequestProto,
        OpenWriteStreamRequestProto, SyncCommittedBlockRequestProto, WriteStreamRequestProto,
    };
    use beryl_types::chunk::ByteRange;
    use beryl_types::ids::{BlockId, BlockIndex, ClientId, DataHandleId, StreamId};
    use beryl_types::layout::BlockFormatId;
    use beryl_types::{GroupName, Tier, WorkerRunId};
    use bytes::Bytes;

    use crate::data::convert::{
        proto_to_abort_write_request, proto_to_commit_write_request, proto_to_read_open_request,
        proto_to_sync_committed_block_request, proto_to_write_frame, proto_to_write_open_request,
    };
    use crate::store::block::ChecksumKind;

    const BLOCK_SIZE: u64 = 4096;
    const CHUNK_SIZE: u32 = 1024;
    const BLOCK_STAMP: u64 = 55;

    fn block_id() -> BlockId {
        BlockId::new(DataHandleId::new(7), BlockIndex::new(3))
    }

    fn group_name() -> GroupName {
        GroupName::parse("root").expect("test group name is valid")
    }

    fn stream_id() -> StreamId {
        StreamId::new((1u128 << 64) | 42)
    }

    fn test_block_id_proto() -> BlockIdProto {
        BlockIdProto {
            data_handle_id: 7,
            block_index: 3,
        }
    }

    fn test_stream_id_proto() -> StreamIdProto {
        StreamIdProto { high: 1, low: 42 }
    }

    fn test_token_proto() -> FencingTokenProto {
        FencingTokenProto {
            block_id: Some(test_block_id_proto()),
            owner: Some(ClientId::new(9).into()),
            epoch: 11,
        }
    }

    fn test_header() -> DataRequestHeaderProto {
        DataRequestHeaderProto {
            client: Some(ClientInfoProto {
                call_id: beryl_types::CallId::new().to_string(),
                client_id: Some(ClientId::new(9).into()),
                client_name: "worker-test".to_string(),
            }),
            trace_context: None,
        }
    }

    fn test_worker_run_id() -> WorkerRunId {
        "550e8400-e29b-41d4-a716-446655440000".parse().unwrap()
    }

    fn open_read_proto(offset: u64, len: u32, block_stamp: u64, frame_size: u32) -> OpenReadStreamRequestProto {
        OpenReadStreamRequestProto {
            header: Some(test_header()),
            group_name: "root".to_string(),
            block_id: Some(test_block_id_proto()),
            byte_range: Some(ByteRangeProto { offset, len }),
            block_stamp,
            frame_size,
            worker_run_id: test_worker_run_id().to_string(),
            block_format_id: BlockFormatId::FULL_EFFECTIVE.as_raw(),
            block_size: BLOCK_SIZE,
            chunk_size: CHUNK_SIZE,
            effective_len: BLOCK_SIZE,
        }
    }

    fn open_write_proto(frame_size: u32) -> OpenWriteStreamRequestProto {
        OpenWriteStreamRequestProto {
            header: Some(test_header()),
            group_name: "root".to_string(),
            block_id: Some(test_block_id_proto()),
            block_size: BLOCK_SIZE,
            block_format_id: BlockFormatId::FULL_EFFECTIVE.as_raw(),
            block_stamp: BLOCK_STAMP,
            chunk_size: CHUNK_SIZE,
            token: Some(test_token_proto()),
            frame_size,
            worker_run_id: test_worker_run_id().to_string(),
            effective_len: BLOCK_SIZE,
            tier: beryl_proto::common::TierProto::TierHdd as i32,
        }
    }

    fn commit_write_proto(stream_id: StreamId, commit_seq: u64, effective_len: u64) -> CommitWriteRequestProto {
        CommitWriteRequestProto {
            header: Some(test_header()),
            group_name: "root".to_string(),
            block_id: Some(test_block_id_proto()),
            stream_id: Some(crate::data::convert::stream_id_to_proto(stream_id)),
            effective_len,
            block_stamp: BLOCK_STAMP,
            token: Some(test_token_proto()),
            commit_seq,
            require_sync: true,
            worker_run_id: test_worker_run_id().to_string(),
            block_format_id: BlockFormatId::FULL_EFFECTIVE.as_raw(),
            block_size: BLOCK_SIZE,
            chunk_size: CHUNK_SIZE,
        }
    }

    fn sync_committed_block_proto(block_stamp: u64, expected_block_len: u64) -> SyncCommittedBlockRequestProto {
        SyncCommittedBlockRequestProto {
            header: Some(test_header()),
            group_name: "root".to_string(),
            block_id: Some(test_block_id_proto()),
            block_stamp,
            expected_block_len,
            worker_run_id: test_worker_run_id().to_string(),
            block_format_id: BlockFormatId::FULL_EFFECTIVE.as_raw(),
            block_size: BLOCK_SIZE,
            chunk_size: CHUNK_SIZE,
        }
    }

    fn assert_open_read_request_conversion() {
        let request = open_read_proto(128, 4096, 0, 8192);

        let domain = proto_to_read_open_request(request).unwrap();

        assert_eq!(domain.group_name, group_name());
        assert_eq!(domain.block_id, block_id());
        assert_eq!(domain.byte_range, ByteRange { offset: 128, len: 4096 });
        assert_eq!(domain.block_stamp, 0);
        assert_eq!(domain.worker_run_id, test_worker_run_id());
        assert_eq!(domain.block_format_id, BlockFormatId::FULL_EFFECTIVE);
        assert_eq!(domain.block_size, BLOCK_SIZE);
        assert_eq!(domain.chunk_size, CHUNK_SIZE);
        assert_eq!(domain.effective_len, BLOCK_SIZE);
        assert_eq!(domain.frame_size, 8192);
    }

    fn assert_open_write_request_conversion() {
        let request = open_write_proto(8192);

        let domain = proto_to_write_open_request(request).unwrap();

        assert_eq!(domain.group_name, group_name());
        assert_eq!(domain.block_id, block_id());
        assert_eq!(domain.token.owner, ClientId::new(9));
        assert_eq!(domain.token.epoch, 11);
        assert_eq!(domain.worker_run_id, test_worker_run_id());
        assert_eq!(domain.block_stamp, BLOCK_STAMP);
        assert_eq!(domain.block_format_id, BlockFormatId::FULL_EFFECTIVE);
        assert_eq!(domain.frame_size, 8192);
        assert_eq!(domain.block_size, BLOCK_SIZE);
        assert_eq!(domain.chunk_size, CHUNK_SIZE);
        assert_eq!(domain.effective_len, BLOCK_SIZE);
        assert_eq!(domain.checksum_kind, ChecksumKind::None);
        assert_eq!(domain.tier, Tier::Hdd);
    }

    fn assert_unknown_block_format_is_rejected() {
        let err = proto_to_write_open_request(OpenWriteStreamRequestProto {
            block_format_id: BlockFormatId::FULL_EFFECTIVE.as_raw() + 1,
            ..open_write_proto(8192)
        })
        .expect_err("unknown block format must fail conversion");

        assert!(err.to_string().contains("block_format_id"));
    }

    fn assert_write_frame_conversion_without_copying_payload() {
        let data = Bytes::from_static(b"frame-data");
        let request = WriteStreamRequestProto {
            stream_id: Some(test_stream_id_proto()),
            seq: 5,
            offset_in_block: 2048,
            data: data.clone(),
        };

        let domain = proto_to_write_frame(request).unwrap();

        assert_eq!(domain.stream_id, stream_id());
        assert_eq!(domain.seq, 5);
        assert_eq!(domain.offset_in_block, 2048);
        assert_eq!(domain.data, data);
        assert_eq!(domain.data.as_ptr(), data.as_ptr());
        assert_eq!(domain.checksum32, 0);
    }

    fn assert_commit_and_abort_request_conversion() {
        let commit = proto_to_commit_write_request(commit_write_proto(stream_id(), 8, 4096)).unwrap();

        assert_eq!(commit.stream_id, stream_id());
        assert_eq!(commit.group_name, group_name());
        assert_eq!(commit.block_id, block_id());
        assert_eq!(commit.token.epoch, 11);
        assert_eq!(commit.worker_run_id, test_worker_run_id());
        assert_eq!(commit.commit_seq, 8);
        assert_eq!(commit.effective_len, 4096);
        assert_eq!(commit.block_stamp, BLOCK_STAMP);
        assert_eq!(commit.block_format_id, BlockFormatId::FULL_EFFECTIVE);
        assert_eq!(commit.block_size, BLOCK_SIZE);
        assert_eq!(commit.chunk_size, CHUNK_SIZE);
        assert!(commit.require_sync);

        let abort = proto_to_abort_write_request(AbortWriteRequestProto {
            header: Some(test_header()),
            group_name: "root".to_string(),
            block_id: Some(test_block_id_proto()),
            stream_id: Some(test_stream_id_proto()),
            token: Some(test_token_proto()),
        })
        .unwrap();

        assert_eq!(abort.stream_id, stream_id());
        assert_eq!(abort.group_name, group_name());
        assert_eq!(abort.block_id, block_id());
        assert_eq!(abort.token.owner, ClientId::new(9));
    }

    fn assert_sync_committed_block_request_conversion() {
        let sync = proto_to_sync_committed_block_request(sync_committed_block_proto(BLOCK_STAMP, BLOCK_SIZE)).unwrap();

        assert_eq!(sync.group_name, group_name());
        assert_eq!(sync.block_id, block_id());
        assert_eq!(sync.worker_run_id, test_worker_run_id());
        assert_eq!(sync.block_stamp, BLOCK_STAMP);
        assert_eq!(sync.expected_block_len, BLOCK_SIZE);
        assert_eq!(sync.block_format_id, BlockFormatId::FULL_EFFECTIVE);
        assert_eq!(sync.block_size, BLOCK_SIZE);
        assert_eq!(sync.chunk_size, CHUNK_SIZE);
    }

    #[test]
    fn converts_valid_data_plane_requests_to_domain() {
        assert_open_read_request_conversion();
        assert_open_write_request_conversion();
        assert_write_frame_conversion_without_copying_payload();
        assert_commit_and_abort_request_conversion();
        assert_sync_committed_block_request_conversion();
    }

    #[test]
    fn conversion_reports_missing_required_fields_without_panic() {
        assert_unknown_block_format_is_rejected();

        let read_err = proto_to_read_open_request(OpenReadStreamRequestProto {
            header: Some(test_header()),
            group_name: "root".to_string(),
            block_id: None,
            byte_range: Some(ByteRangeProto { offset: 0, len: 1 }),
            block_stamp: 0,
            frame_size: 1024,
            worker_run_id: test_worker_run_id().to_string(),
            block_format_id: BlockFormatId::FULL_EFFECTIVE.as_raw(),
            block_size: BLOCK_SIZE,
            chunk_size: CHUNK_SIZE,
            effective_len: BLOCK_SIZE,
        })
        .unwrap_err();
        assert!(read_err.to_string().contains("missing block_id"));

        let read_err = proto_to_read_open_request(OpenReadStreamRequestProto {
            header: Some(test_header()),
            group_name: String::new(),
            block_id: Some(test_block_id_proto()),
            byte_range: Some(ByteRangeProto { offset: 0, len: 1 }),
            block_stamp: 0,
            frame_size: 1024,
            worker_run_id: test_worker_run_id().to_string(),
            block_format_id: BlockFormatId::FULL_EFFECTIVE.as_raw(),
            block_size: BLOCK_SIZE,
            chunk_size: CHUNK_SIZE,
            effective_len: BLOCK_SIZE,
        })
        .unwrap_err();
        assert!(read_err.to_string().contains("missing group_name"));

        let read_err = proto_to_read_open_request(OpenReadStreamRequestProto {
            header: Some(test_header()),
            group_name: "Root".to_string(),
            block_id: Some(test_block_id_proto()),
            byte_range: Some(ByteRangeProto { offset: 0, len: 1 }),
            block_stamp: 0,
            frame_size: 1024,
            worker_run_id: test_worker_run_id().to_string(),
            block_format_id: BlockFormatId::FULL_EFFECTIVE.as_raw(),
            block_size: BLOCK_SIZE,
            chunk_size: CHUNK_SIZE,
            effective_len: BLOCK_SIZE,
        })
        .unwrap_err();
        assert!(read_err.to_string().contains("group_name invalid"));

        let write_open_err = proto_to_write_open_request(OpenWriteStreamRequestProto {
            token: None,
            ..open_write_proto(1024)
        })
        .unwrap_err();
        assert!(write_open_err.to_string().contains("missing token"));

        let write_frame_err = proto_to_write_frame(WriteStreamRequestProto {
            stream_id: None,
            seq: 1,
            offset_in_block: 0,
            data: Bytes::new(),
        })
        .unwrap_err();
        assert!(write_frame_err.to_string().contains("missing stream_id"));

        let commit_err = proto_to_commit_write_request(CommitWriteRequestProto {
            header: Some(test_header()),
            group_name: "root".to_string(),
            block_id: Some(test_block_id_proto()),
            stream_id: None,
            effective_len: 1,
            block_stamp: BLOCK_STAMP,
            token: Some(test_token_proto()),
            commit_seq: 1,
            require_sync: false,
            worker_run_id: test_worker_run_id().to_string(),
            block_format_id: BlockFormatId::FULL_EFFECTIVE.as_raw(),
            block_size: BLOCK_SIZE,
            chunk_size: CHUNK_SIZE,
        })
        .unwrap_err();
        assert!(commit_err.to_string().contains("missing stream_id"));
    }
}
