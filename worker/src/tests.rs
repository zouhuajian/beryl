// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Unit tests for the worker data-plane skeleton.

#[cfg(test)]
#[allow(clippy::module_inception)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use futures::StreamExt;
    use proto::common::{
        BlockIdProto, ByteRangeProto, ClientInfoProto, ErrorClassProto, FencingTokenProto, RefreshReasonProto,
        ShardGroupIdProto, StreamIdProto,
    };
    use proto::worker::worker_data_service_server::WorkerDataService;
    use proto::worker::ChecksumKindProto;
    use proto::worker::{
        AbortWriteRequestProto, CommitWriteRequestProto, DataRequestHeaderProto, OpenReadStreamRequestProto,
        OpenWriteStreamRequestProto, ReadStreamRequestProto, SyncCommittedBlockRequestProto, WriteStreamRequestProto,
    };
    use tempfile::TempDir;
    use types::chunk::ByteRange;
    use types::ids::{BlockId, BlockIndex, ChunkIndex, ClientId, DataHandleId, ShardGroupId, StreamId};
    use types::lease::FencingToken;

    use crate::data::convert::{
        proto_to_abort_write_request, proto_to_commit_write_request, proto_to_read_open_request,
        proto_to_sync_committed_block_request, proto_to_write_frame, proto_to_write_open_request,
    };
    use crate::data::core::{
        AbortWriteRequest, CommitWriteRequest, RangeMapper, ReadOpenRequest, StreamContext, StreamMode,
        SyncCommittedBlockRequest, WorkerCore, WorkerCoreResult, WriteFrame, WriteOpenRequest,
    };
    use crate::error::WorkerError;
    use crate::net::server::grpc::WorkerDataServiceImpl;
    use crate::runtime::stream::{StreamManager, StreamState};
    use crate::store::block::{
        ChecksumKind, CreateStagingBlockRequest, FullBlockFileStore, FullBlockFileStoreConfig, PublishReadyRequest,
    };

    const BLOCK_SIZE: u64 = 4096;
    const CHUNK_SIZE: u32 = 1024;
    const BLOCK_STAMP: u64 = 55;

    fn block_id() -> BlockId {
        BlockId::new(DataHandleId::new(7), BlockIndex::new(3))
    }

    fn group_id() -> ShardGroupId {
        ShardGroupId::new(9)
    }

    fn stream_id() -> StreamId {
        StreamId::new((1u128 << 64) | 42)
    }

    fn token() -> FencingToken {
        FencingToken::new(block_id(), ClientId::new(9), 11)
    }

    fn test_block_id_proto() -> BlockIdProto {
        BlockIdProto {
            data_handle_id: 7,
            block_index: 3,
        }
    }

    fn test_group_id_proto() -> ShardGroupIdProto {
        ShardGroupIdProto { value: 9 }
    }

    fn test_stream_id_proto() -> StreamIdProto {
        StreamIdProto { high: 1, low: 42 }
    }

    fn test_token_proto() -> FencingTokenProto {
        FencingTokenProto {
            block_id: Some(test_block_id_proto()),
            owner: 9,
            epoch: 11,
        }
    }

    fn test_header() -> DataRequestHeaderProto {
        DataRequestHeaderProto {
            client: Some(ClientInfoProto {
                call_id: "call-1".to_string(),
                client_id: 9,
                client_name: "worker-test".to_string(),
            }),
            traceparent: String::new(),
        }
    }

    fn assert_need_refresh<T: std::fmt::Debug>(
        result: WorkerCoreResult<T>,
        expected_reason: common::error::canonical::RefreshReason,
    ) {
        let error = result.expect_err("operation should need refresh");
        match error {
            WorkerError::NeedRefresh { reason, .. } => assert_eq!(reason, expected_reason),
            other => panic!("expected NeedRefresh, got {other:?}"),
        }
    }

    fn assert_invalid_argument<T: std::fmt::Debug>(result: WorkerCoreResult<T>) {
        match result.expect_err("operation should fail") {
            WorkerError::InvalidArgument(_) => {}
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    fn assert_not_found<T: std::fmt::Debug>(result: WorkerCoreResult<T>) {
        match result.expect_err("operation should fail") {
            WorkerError::NotFound(_) => {}
            other => panic!("expected NotFound, got {other:?}"),
        }
    }

    fn write_open_request() -> WriteOpenRequest {
        WriteOpenRequest {
            group_id: group_id(),
            block_id: block_id(),
            token: token(),
            block_stamp: BLOCK_STAMP,
            frame_size: 8192,
            block_size: BLOCK_SIZE,
            chunk_size: CHUNK_SIZE,
            checksum_kind: ChecksumKind::None,
        }
    }

    fn commit_write_request() -> CommitWriteRequest {
        CommitWriteRequest {
            stream_id: stream_id(),
            group_id: group_id(),
            block_id: block_id(),
            token: token(),
            commit_seq: 8,
            effective_block_len: 4096,
            block_stamp: BLOCK_STAMP,
            require_sync: true,
        }
    }

    fn abort_write_request() -> AbortWriteRequest {
        AbortWriteRequest {
            stream_id: stream_id(),
            group_id: group_id(),
            block_id: block_id(),
            token: token(),
        }
    }

    fn sync_committed_block_request(block_stamp: u64, expected_block_len: u64) -> SyncCommittedBlockRequest {
        SyncCommittedBlockRequest {
            group_id: group_id(),
            block_id: block_id(),
            block_stamp,
            expected_block_len,
        }
    }

    fn stream_context() -> StreamContext {
        StreamContext {
            stream_id: stream_id(),
            group_id: group_id(),
            block_id: block_id(),
            mode: StreamMode::Read,
            start_offset: 0,
            end_offset: 4096,
            frame_size: 8192,
            window_bytes: 65_536,
            block_stamp: 17,
            committed_length: 4096,
            effective_block_len: 4096,
            chunk_size: CHUNK_SIZE,
            fencing_token: None,
        }
    }

    fn payload() -> Bytes {
        Bytes::from((0..BLOCK_SIZE).map(|idx| (idx % 251) as u8).collect::<Vec<_>>())
    }

    fn core_with_store(
        default_frame_size: u32,
        max_frame_size: u32,
        window_bytes: u32,
    ) -> (TempDir, Arc<FullBlockFileStore>, WorkerCore) {
        let temp = TempDir::new().expect("tempdir");
        let store = Arc::new(FullBlockFileStore::new(FullBlockFileStoreConfig::new(
            temp.path().to_path_buf(),
        )));
        let core = WorkerCore::with_local_store(
            CHUNK_SIZE,
            default_frame_size,
            max_frame_size,
            window_bytes,
            Duration::from_secs(60),
            store.clone(),
        );
        (temp, store, core)
    }

    fn publish_ready_block(store: &FullBlockFileStore, data: Bytes, block_stamp: u64) {
        store
            .create_staging_block(CreateStagingBlockRequest {
                group_id: group_id(),
                block_id: block_id(),
                block_size: BLOCK_SIZE,
                chunk_size: CHUNK_SIZE,
                checksum_kind: ChecksumKind::None,
            })
            .expect("create staging block");
        store
            .write_at(group_id(), block_id(), 0, data.clone())
            .expect("write staging block");
        store
            .publish_ready(PublishReadyRequest {
                group_id: group_id(),
                block_id: block_id(),
                effective_block_len: data.len() as u64,
                block_stamp,
            })
            .expect("publish ready block");
    }

    fn read_open_request_for(offset: u64, len: u32, block_stamp: u64, frame_size: u32) -> ReadOpenRequest {
        ReadOpenRequest {
            group_id: group_id(),
            block_id: block_id(),
            byte_range: ByteRange { offset, len },
            block_stamp,
            frame_size,
        }
    }

    fn write_stream_context() -> StreamContext {
        StreamContext {
            mode: StreamMode::Write,
            fencing_token: Some(token()),
            ..stream_context()
        }
    }

    fn open_read_proto(offset: u64, len: u32, block_stamp: u64, frame_size: u32) -> OpenReadStreamRequestProto {
        OpenReadStreamRequestProto {
            header: Some(test_header()),
            group_id: Some(test_group_id_proto()),
            block_id: Some(test_block_id_proto()),
            byte_range: Some(ByteRangeProto { offset, len }),
            block_stamp,
            frame_size,
        }
    }

    fn open_write_proto(frame_size: u32) -> OpenWriteStreamRequestProto {
        OpenWriteStreamRequestProto {
            header: Some(test_header()),
            group_id: Some(test_group_id_proto()),
            block_id: Some(test_block_id_proto()),
            block_size: BLOCK_SIZE,
            block_stamp: BLOCK_STAMP,
            chunk_size: CHUNK_SIZE,
            checksum_kind: ChecksumKindProto::ChecksumKindNone as i32,
            token: Some(test_token_proto()),
            frame_size,
        }
    }

    fn commit_write_proto(stream_id: StreamId, commit_seq: u64, effective_block_len: u64) -> CommitWriteRequestProto {
        CommitWriteRequestProto {
            header: Some(test_header()),
            group_id: Some(test_group_id_proto()),
            block_id: Some(test_block_id_proto()),
            stream_id: Some(crate::data::convert::stream_id_to_proto(stream_id)),
            effective_block_len,
            block_stamp: BLOCK_STAMP,
            token: Some(test_token_proto()),
            commit_seq,
            require_sync: true,
        }
    }

    fn sync_committed_block_proto(block_stamp: u64, expected_block_len: u64) -> SyncCommittedBlockRequestProto {
        SyncCommittedBlockRequestProto {
            header: Some(test_header()),
            group_id: Some(test_group_id_proto()),
            block_id: Some(test_block_id_proto()),
            block_stamp,
            expected_block_len,
        }
    }

    #[test]
    fn range_mapper_maps_range_inside_single_chunk() {
        let slices = RangeMapper::map_range(ByteRange { offset: 100, len: 200 }, 1024).unwrap();

        assert_eq!(slices.len(), 1);
        assert_eq!(slices[0].chunk_index, ChunkIndex::new(0));
        assert_eq!(slices[0].offset_in_chunk, 100);
        assert_eq!(slices[0].len, 200);
    }

    #[test]
    fn range_mapper_maps_range_across_two_chunks() {
        let slices = RangeMapper::map_range(ByteRange { offset: 900, len: 300 }, 1024).unwrap();

        assert_eq!(slices.len(), 2);
        assert_eq!(slices[0].chunk_index, ChunkIndex::new(0));
        assert_eq!(slices[0].offset_in_chunk, 900);
        assert_eq!(slices[0].len, 124);
        assert_eq!(slices[1].chunk_index, ChunkIndex::new(1));
        assert_eq!(slices[1].offset_in_chunk, 0);
        assert_eq!(slices[1].len, 176);
    }

    #[test]
    fn range_mapper_maps_range_starting_at_chunk_boundary() {
        let slices = RangeMapper::map_range(ByteRange { offset: 1024, len: 100 }, 1024).unwrap();

        assert_eq!(slices.len(), 1);
        assert_eq!(slices[0].chunk_index, ChunkIndex::new(1));
        assert_eq!(slices[0].offset_in_chunk, 0);
        assert_eq!(slices[0].len, 100);
    }

    #[test]
    fn range_mapper_maps_empty_range_to_no_slices() {
        let slices = RangeMapper::map_range(ByteRange { offset: 512, len: 0 }, 1024).unwrap();

        assert!(slices.is_empty());
    }

    #[test]
    fn range_mapper_maps_non_aligned_range() {
        let slices = RangeMapper::map_range(
            ByteRange {
                offset: 1537,
                len: 2000,
            },
            1024,
        )
        .unwrap();

        assert_eq!(slices.len(), 3);
        assert_eq!(slices[0].chunk_index, ChunkIndex::new(1));
        assert_eq!(slices[0].offset_in_chunk, 513);
        assert_eq!(slices[0].len, 511);
        assert_eq!(slices[1].chunk_index, ChunkIndex::new(2));
        assert_eq!(slices[1].offset_in_chunk, 0);
        assert_eq!(slices[1].len, 1024);
        assert_eq!(slices[2].chunk_index, ChunkIndex::new(3));
        assert_eq!(slices[2].offset_in_chunk, 0);
        assert_eq!(slices[2].len, 465);
    }

    #[test]
    fn converts_open_read_stream_request_to_domain() {
        let request = OpenReadStreamRequestProto {
            header: Some(test_header()),
            group_id: Some(test_group_id_proto()),
            block_id: Some(test_block_id_proto()),
            byte_range: Some(ByteRangeProto { offset: 128, len: 4096 }),
            block_stamp: 0,
            frame_size: 8192,
        };

        let domain = proto_to_read_open_request(request).unwrap();

        assert_eq!(domain.group_id, group_id());
        assert_eq!(domain.block_id, block_id());
        assert_eq!(domain.byte_range, ByteRange { offset: 128, len: 4096 });
        assert_eq!(domain.block_stamp, 0);
        assert_eq!(domain.frame_size, 8192);
    }

    #[test]
    fn converts_open_write_stream_request_to_domain() {
        let request = open_write_proto(8192);

        let domain = proto_to_write_open_request(request).unwrap();

        assert_eq!(domain.group_id, group_id());
        assert_eq!(domain.block_id, block_id());
        assert_eq!(domain.token.owner, ClientId::new(9));
        assert_eq!(domain.token.epoch, 11);
        assert_eq!(domain.block_stamp, BLOCK_STAMP);
        assert_eq!(domain.frame_size, 8192);
        assert_eq!(domain.block_size, BLOCK_SIZE);
        assert_eq!(domain.chunk_size, CHUNK_SIZE);
        assert_eq!(domain.checksum_kind, ChecksumKind::None);
    }

    #[test]
    fn converts_write_stream_request_to_domain_without_copying_payload() {
        let data = Bytes::from_static(b"frame-data");
        let request = WriteStreamRequestProto {
            stream_id: Some(test_stream_id_proto()),
            seq: 5,
            offset_in_block: 2048,
            data: data.clone(),
            checksum32: 123,
        };

        let domain = proto_to_write_frame(request).unwrap();

        assert_eq!(domain.stream_id, stream_id());
        assert_eq!(domain.seq, 5);
        assert_eq!(domain.offset_in_block, 2048);
        assert_eq!(domain.data, data);
        assert_eq!(domain.data.as_ptr(), data.as_ptr());
        assert_eq!(domain.checksum32, 123);
    }

    #[test]
    fn converts_commit_and_abort_write_requests_to_domain() {
        let commit = proto_to_commit_write_request(CommitWriteRequestProto {
            header: Some(test_header()),
            group_id: Some(test_group_id_proto()),
            block_id: Some(test_block_id_proto()),
            stream_id: Some(test_stream_id_proto()),
            effective_block_len: 4096,
            block_stamp: BLOCK_STAMP,
            token: Some(test_token_proto()),
            commit_seq: 8,
            require_sync: true,
        })
        .unwrap();

        assert_eq!(commit.stream_id, stream_id());
        assert_eq!(commit.group_id, group_id());
        assert_eq!(commit.block_id, block_id());
        assert_eq!(commit.token.epoch, 11);
        assert_eq!(commit.commit_seq, 8);
        assert_eq!(commit.effective_block_len, 4096);
        assert_eq!(commit.block_stamp, BLOCK_STAMP);
        assert!(commit.require_sync);

        let abort = proto_to_abort_write_request(AbortWriteRequestProto {
            header: Some(test_header()),
            group_id: Some(test_group_id_proto()),
            block_id: Some(test_block_id_proto()),
            stream_id: Some(test_stream_id_proto()),
            token: Some(test_token_proto()),
        })
        .unwrap();

        assert_eq!(abort.stream_id, stream_id());
        assert_eq!(abort.group_id, group_id());
        assert_eq!(abort.block_id, block_id());
        assert_eq!(abort.token.owner, ClientId::new(9));
    }

    #[test]
    fn converts_sync_committed_block_request_to_domain() {
        let sync = proto_to_sync_committed_block_request(sync_committed_block_proto(BLOCK_STAMP, BLOCK_SIZE)).unwrap();

        assert_eq!(sync.group_id, group_id());
        assert_eq!(sync.block_id, block_id());
        assert_eq!(sync.block_stamp, BLOCK_STAMP);
        assert_eq!(sync.expected_block_len, BLOCK_SIZE);
    }

    #[test]
    fn conversion_reports_missing_required_fields_without_panic() {
        let read_err = proto_to_read_open_request(OpenReadStreamRequestProto {
            header: Some(test_header()),
            group_id: Some(test_group_id_proto()),
            block_id: None,
            byte_range: Some(ByteRangeProto { offset: 0, len: 1 }),
            block_stamp: 0,
            frame_size: 1024,
        })
        .unwrap_err();
        assert!(read_err.to_string().contains("missing block_id"));

        let read_err = proto_to_read_open_request(OpenReadStreamRequestProto {
            header: Some(test_header()),
            group_id: None,
            block_id: Some(test_block_id_proto()),
            byte_range: Some(ByteRangeProto { offset: 0, len: 1 }),
            block_stamp: 0,
            frame_size: 1024,
        })
        .unwrap_err();
        assert!(read_err.to_string().contains("missing group_id"));

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
            checksum32: 0,
        })
        .unwrap_err();
        assert!(write_frame_err.to_string().contains("missing stream_id"));

        let commit_err = proto_to_commit_write_request(CommitWriteRequestProto {
            header: Some(test_header()),
            group_id: Some(test_group_id_proto()),
            block_id: Some(test_block_id_proto()),
            stream_id: None,
            effective_block_len: 1,
            block_stamp: BLOCK_STAMP,
            token: Some(test_token_proto()),
            commit_seq: 1,
            require_sync: false,
        })
        .unwrap_err();
        assert!(commit_err.to_string().contains("missing stream_id"));
    }

    #[tokio::test]
    async fn open_write_creates_staging_stream() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);

        let result = core.open_write(write_open_request()).await.expect("open write");

        assert_eq!(result.frame_size, 2048);
        assert_eq!(result.window_bytes, 4096);
        assert_eq!(result.block_stamp, BLOCK_STAMP);
        assert_eq!(result.committed_length, 0);
        assert_eq!(result.chunk_size, CHUNK_SIZE);

        let paths = store.paths(group_id(), block_id());
        assert!(paths.staging_data_path.exists());
        assert!(paths.staging_meta_path.exists());
        assert!(!paths.meta_path.exists());
        assert_not_found(store.read_at(group_id(), block_id(), 0, 1));

        let state = core
            .stream_manager()
            .get(result.stream_id)
            .await
            .expect("write stream registered");
        assert_eq!(state.context.group_id, group_id());
        assert_eq!(state.context.block_id, block_id());
        assert_eq!(state.context.mode, StreamMode::Write);
        assert_eq!(state.context.end_offset, BLOCK_SIZE);
        assert_eq!(state.context.chunk_size, CHUNK_SIZE);
        assert_eq!(state.cursor, 0);
        assert_eq!(state.last_acked_seq, 0);
        assert_eq!(state.written_through, 0);
    }

    #[tokio::test]
    async fn open_write_rejects_invalid_fencing_token() {
        let (_temp, _store, core) = core_with_store(512, 2048, 4096);
        let mut req = write_open_request();
        req.token = FencingToken::new(block_id(), ClientId::new(9), 0);

        match core.open_write(req).await.expect_err("zero epoch must be rejected") {
            WorkerError::Fencing(message) => assert!(message.contains("epoch")),
            other => panic!("expected Fencing, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn open_write_rejects_existing_ready_block() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        publish_ready_block(&store, payload(), BLOCK_STAMP);

        assert_need_refresh(
            core.open_write(write_open_request()).await,
            common::error::canonical::RefreshReason::Moved,
        );
        assert_eq!(core.stream_manager().active_count().await, 0);
    }

    #[tokio::test]
    async fn write_stream_writes_staging_data_and_advances_state() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        let open = core.open_write(write_open_request()).await.expect("open write");
        let data = Bytes::from_static(b"abcd");

        let result = core
            .write_stream(WriteFrame {
                stream_id: open.stream_id,
                seq: 1,
                offset_in_block: 0,
                data: data.clone(),
                checksum32: 0,
            })
            .await
            .expect("write frame");

        assert!(result.accepted);
        assert_eq!(result.last_acked_seq, 1);
        assert_eq!(result.written_through, data.len() as u64);
        let state = core.stream_manager().get(open.stream_id).await.expect("stream state");
        assert_eq!(state.cursor, data.len() as u64);
        assert_eq!(state.last_acked_seq, 1);
        assert_eq!(state.written_through, data.len() as u64);
        assert!(!store.paths(group_id(), block_id()).meta_path.exists());
    }

    #[tokio::test]
    async fn write_stream_rejects_seq_gap() {
        let (_temp, _store, core) = core_with_store(512, 2048, 4096);
        let open = core.open_write(write_open_request()).await.expect("open write");

        let result = core
            .write_stream(WriteFrame {
                stream_id: open.stream_id,
                seq: 2,
                offset_in_block: 0,
                data: Bytes::from_static(b"abcd"),
                checksum32: 0,
            })
            .await
            .expect("seq gap response");

        assert!(!result.accepted);
        assert_eq!(result.last_acked_seq, 0);
        assert_eq!(result.written_through, 0);
        assert_eq!(
            core.stream_manager().get(open.stream_id).await.expect("stream").cursor,
            0
        );
    }

    #[tokio::test]
    async fn write_stream_rejects_offset_gap() {
        let (_temp, _store, core) = core_with_store(512, 2048, 4096);
        let open = core.open_write(write_open_request()).await.expect("open write");

        let result = core
            .write_stream(WriteFrame {
                stream_id: open.stream_id,
                seq: 1,
                offset_in_block: 1,
                data: Bytes::from_static(b"abcd"),
                checksum32: 0,
            })
            .await
            .expect("offset gap response");

        assert!(!result.accepted);
        assert_eq!(result.last_acked_seq, 0);
        assert_eq!(result.written_through, 0);
        assert_eq!(
            core.stream_manager().get(open.stream_id).await.expect("stream").cursor,
            0
        );
    }

    #[tokio::test]
    async fn write_stream_rejects_read_stream() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        publish_ready_block(&store, payload(), BLOCK_STAMP);
        let open = core
            .open_read(read_open_request_for(0, 4, BLOCK_STAMP, 512))
            .await
            .expect("open read");

        match core
            .write_stream(WriteFrame {
                stream_id: open.stream_id,
                seq: 1,
                offset_in_block: 0,
                data: Bytes::from_static(b"abcd"),
                checksum32: 0,
            })
            .await
            .expect_err("read stream must reject writes")
        {
            WorkerError::InvalidArgument(message) => assert!(message.contains("not a write stream")),
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn commit_write_publishes_ready_block() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        let open = core.open_write(write_open_request()).await.expect("open write");
        let data = payload();
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data: data.slice(0..2048),
            checksum32: 0,
        })
        .await
        .expect("first frame");
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 2,
            offset_in_block: 2048,
            data: data.slice(2048..4096),
            checksum32: 0,
        })
        .await
        .expect("second frame");

        let result = core
            .commit_write(CommitWriteRequest {
                stream_id: open.stream_id,
                commit_seq: 2,
                effective_block_len: BLOCK_SIZE,
                ..commit_write_request()
            })
            .await
            .expect("commit write");

        assert_eq!(result.effective_block_len, BLOCK_SIZE);
        assert_eq!(result.block_stamp, BLOCK_STAMP);
        assert_eq!(result.written_through, BLOCK_SIZE);
        let meta = store.load_meta(group_id(), block_id()).expect("ready meta");
        assert_eq!(meta.visibility.block_state, crate::store::block::BlockState::Ready);
        assert_eq!(meta.visibility.block_stamp, BLOCK_STAMP);
        assert_eq!(store.read_at(group_id(), block_id(), 0, BLOCK_SIZE).unwrap(), data);
    }

    #[tokio::test]
    async fn commit_write_rejects_incomplete_block() {
        let (_temp, _store, core) = core_with_store(512, 2048, 4096);
        let open = core.open_write(write_open_request()).await.expect("open write");
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data: Bytes::from_static(b"abcd"),
            checksum32: 0,
        })
        .await
        .expect("write frame");

        assert_invalid_argument(
            core.commit_write(CommitWriteRequest {
                stream_id: open.stream_id,
                commit_seq: 1,
                effective_block_len: BLOCK_SIZE,
                ..commit_write_request()
            })
            .await,
        );
    }

    #[tokio::test]
    async fn commit_write_rejects_token_mismatch() {
        let (_temp, _store, core) = core_with_store(512, 2048, 4096);
        let open = core.open_write(write_open_request()).await.expect("open write");
        let data = payload();
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data,
            checksum32: 0,
        })
        .await
        .expect("write frame");

        match core
            .commit_write(CommitWriteRequest {
                stream_id: open.stream_id,
                token: FencingToken::new(block_id(), ClientId::new(99), 11),
                commit_seq: 1,
                effective_block_len: BLOCK_SIZE,
                ..commit_write_request()
            })
            .await
            .expect_err("token mismatch must be rejected")
        {
            WorkerError::Fencing(message) => assert!(message.contains("token")),
            other => panic!("expected Fencing, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn commit_write_removes_stream_after_success() {
        let (_temp, _store, core) = core_with_store(512, 2048, 4096);
        let open = core.open_write(write_open_request()).await.expect("open write");
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data: payload(),
            checksum32: 0,
        })
        .await
        .expect("write frame");

        core.commit_write(CommitWriteRequest {
            stream_id: open.stream_id,
            commit_seq: 1,
            effective_block_len: BLOCK_SIZE,
            ..commit_write_request()
        })
        .await
        .expect("commit write");

        assert!(core.stream_manager().get(open.stream_id).await.is_none());
        assert_not_found(
            core.commit_write(CommitWriteRequest {
                stream_id: open.stream_id,
                commit_seq: 1,
                effective_block_len: BLOCK_SIZE,
                ..commit_write_request()
            })
            .await,
        );
    }

    #[tokio::test]
    async fn sync_committed_block_succeeds_after_terminal_commit_without_stream() {
        let (_temp, _store, core) = core_with_store(512, 2048, 4096);
        let open = core.open_write(write_open_request()).await.expect("open write");
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data: payload(),
            checksum32: 0,
        })
        .await
        .expect("write frame");
        core.commit_write(CommitWriteRequest {
            stream_id: open.stream_id,
            commit_seq: 1,
            effective_block_len: BLOCK_SIZE,
            require_sync: false,
            ..commit_write_request()
        })
        .await
        .expect("visibility commit");
        assert!(core.stream_manager().get(open.stream_id).await.is_none());

        let result = core
            .sync_committed_block(sync_committed_block_request(BLOCK_STAMP, BLOCK_SIZE))
            .await
            .expect("sync committed block");

        assert_eq!(result.effective_block_len, BLOCK_SIZE);
        assert_eq!(result.block_stamp, BLOCK_STAMP);
    }

    #[tokio::test]
    async fn sync_committed_block_rejects_missing_wrong_generation_and_uncommitted_block() {
        let (_temp, _store, core) = core_with_store(512, 2048, 4096);
        assert_not_found(
            core.sync_committed_block(sync_committed_block_request(BLOCK_STAMP, BLOCK_SIZE))
                .await,
        );

        let open = core.open_write(write_open_request()).await.expect("open write");
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data: payload(),
            checksum32: 0,
        })
        .await
        .expect("write frame");
        assert_not_found(
            core.sync_committed_block(sync_committed_block_request(BLOCK_STAMP, BLOCK_SIZE))
                .await,
        );

        core.commit_write(CommitWriteRequest {
            stream_id: open.stream_id,
            commit_seq: 1,
            effective_block_len: BLOCK_SIZE,
            ..commit_write_request()
        })
        .await
        .expect("commit write");
        assert_need_refresh(
            core.sync_committed_block(sync_committed_block_request(BLOCK_STAMP + 1, BLOCK_SIZE))
                .await,
            common::error::canonical::RefreshReason::BlockStampMismatch,
        );
        assert_invalid_argument(
            core.sync_committed_block(sync_committed_block_request(BLOCK_STAMP, BLOCK_SIZE - 1))
                .await,
        );
    }

    #[tokio::test]
    async fn repeated_sync_committed_block_is_idempotent() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        publish_ready_block(store.as_ref(), payload(), BLOCK_STAMP);

        let first = core
            .sync_committed_block(sync_committed_block_request(BLOCK_STAMP, BLOCK_SIZE))
            .await
            .expect("first sync");
        let second = core
            .sync_committed_block(sync_committed_block_request(BLOCK_STAMP, BLOCK_SIZE))
            .await
            .expect("second sync");

        assert_eq!(first, second);
    }

    #[tokio::test]
    async fn abort_write_removes_stream_and_staging_block() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        let open = core.open_write(write_open_request()).await.expect("open write");
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data: Bytes::from_static(b"abcd"),
            checksum32: 0,
        })
        .await
        .expect("write frame");

        let result = core
            .abort_write(AbortWriteRequest {
                stream_id: open.stream_id,
                ..abort_write_request()
            })
            .await
            .expect("abort write");

        assert!(result.aborted);
        assert!(core.stream_manager().get(open.stream_id).await.is_none());
        let paths = store.paths(group_id(), block_id());
        assert!(!paths.staging_data_path.exists());
        assert!(!paths.staging_meta_path.exists());
    }

    #[tokio::test]
    async fn abort_write_keeps_no_readable_block() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        let open = core.open_write(write_open_request()).await.expect("open write");

        core.abort_write(AbortWriteRequest {
            stream_id: open.stream_id,
            ..abort_write_request()
        })
        .await
        .expect("abort write");

        assert_not_found(store.read_at(group_id(), block_id(), 0, 1));
        assert!(!store.paths(group_id(), block_id()).meta_path.exists());
    }

    #[tokio::test]
    async fn recover_after_uncommitted_write_is_not_readable() {
        let (temp, _store, core) = core_with_store(512, 2048, 4096);
        let open = core.open_write(write_open_request()).await.expect("open write");
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data: Bytes::from_static(b"abcd"),
            checksum32: 0,
        })
        .await
        .expect("write frame");

        let recovered_store = FullBlockFileStore::new(FullBlockFileStoreConfig::new(temp.path().to_path_buf()));
        assert_not_found(recovered_store.recover_block(group_id(), block_id()));
        assert_not_found(recovered_store.read_at(group_id(), block_id(), 0, 1));
    }

    #[tokio::test]
    async fn open_read_ready_block_succeeds() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        publish_ready_block(&store, payload(), BLOCK_STAMP);

        let result = core
            .open_read(read_open_request_for(128, 1024, BLOCK_STAMP, 0))
            .await
            .expect("open read");

        assert_eq!(result.frame_size, 512);
        assert_eq!(result.window_bytes, 4096);
        assert_eq!(result.block_stamp, BLOCK_STAMP);
        assert_eq!(result.committed_length, BLOCK_SIZE);
        assert_eq!(result.chunk_size, CHUNK_SIZE);

        let state = core
            .stream_manager()
            .get(result.stream_id)
            .await
            .expect("read stream registered");
        assert_eq!(state.context.group_id, group_id());
        assert_eq!(state.context.block_id, block_id());
        assert_eq!(state.context.mode, StreamMode::Read);
        assert_eq!(state.context.start_offset, 128);
        assert_eq!(state.context.end_offset, 1152);
        assert_eq!(state.cursor, 128);
        assert_eq!(state.context.effective_block_len, BLOCK_SIZE);
    }

    #[tokio::test]
    async fn worker_core_uses_configured_storage_root() {
        let custom_root = TempDir::new().expect("custom root");
        let other_root = TempDir::new().expect("other root");
        let store = FullBlockFileStore::new(FullBlockFileStoreConfig::new(custom_root.path().to_path_buf()));
        publish_ready_block(&store, payload(), BLOCK_STAMP);

        let core = WorkerCore::with_options(
            CHUNK_SIZE,
            512,
            2048,
            4096,
            Duration::from_secs(60),
            custom_root.path().to_path_buf(),
        );

        let result = core
            .open_read(read_open_request_for(0, 8, BLOCK_STAMP, 512))
            .await
            .expect("open read from configured root");
        assert!(core.stream_manager().get(result.stream_id).await.is_some());

        let paths = store.paths(group_id(), block_id());
        assert!(paths.data_path.starts_with(custom_root.path()));
        assert!(paths.meta_path.starts_with(custom_root.path()));
        assert!(
            paths.data_path.exists(),
            "ready block data must exist under custom root"
        );
        assert!(
            paths.meta_path.exists(),
            "ready block metadata must exist under custom root"
        );

        let other_store = FullBlockFileStore::new(FullBlockFileStoreConfig::new(other_root.path().to_path_buf()));
        let other_paths = other_store.paths(group_id(), block_id());
        assert!(
            !other_paths.data_path.exists(),
            "ready block data must not be created under other root"
        );
        assert!(
            !other_paths.meta_path.exists(),
            "ready block metadata must not be created under other root"
        );
    }

    #[tokio::test]
    async fn open_read_rejects_block_stamp_mismatch() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        publish_ready_block(&store, payload(), BLOCK_STAMP);

        assert_need_refresh(
            core.open_read(read_open_request_for(0, 1024, BLOCK_STAMP + 1, 512))
                .await,
            common::error::canonical::RefreshReason::BlockStampMismatch,
        );
        assert_eq!(core.stream_manager().active_count().await, 0);
    }

    #[tokio::test]
    async fn open_read_rejects_zero_block_stamp_for_ready_block() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        publish_ready_block(&store, payload(), BLOCK_STAMP);

        assert_invalid_argument(core.open_read(read_open_request_for(0, 1024, 0, 512)).await);
        assert_eq!(core.stream_manager().active_count().await, 0);
    }

    #[tokio::test]
    async fn open_read_rejects_missing_block() {
        let (_temp, _store, core) = core_with_store(512, 2048, 4096);

        assert_need_refresh(
            core.open_read(read_open_request_for(0, 1024, BLOCK_STAMP, 512)).await,
            common::error::canonical::RefreshReason::Moved,
        );
        assert_eq!(core.stream_manager().active_count().await, 0);
    }

    #[tokio::test]
    async fn open_read_rejects_out_of_bounds_range() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        publish_ready_block(&store, payload(), BLOCK_STAMP);

        assert_invalid_argument(core.open_read(read_open_request_for(4090, 16, BLOCK_STAMP, 512)).await);
        assert_eq!(core.stream_manager().active_count().await, 0);
    }

    #[tokio::test]
    async fn read_stream_reads_single_frame() {
        let (_temp, store, core) = core_with_store(1024, 2048, 4096);
        let data = payload();
        publish_ready_block(&store, data.clone(), BLOCK_STAMP);
        let open = core
            .open_read(read_open_request_for(10, 5, BLOCK_STAMP, 1024))
            .await
            .expect("open read");

        let frames = core.read_stream(open.stream_id, 0).await.expect("read stream");

        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].offset_in_block, 10);
        assert_eq!(frames[0].data, data.slice(10..15));
        assert!(frames[0].eos);
        assert_eq!(core.stream_manager().active_count().await, 0);
    }

    #[tokio::test]
    async fn read_stream_advances_cursor_across_calls() {
        let (_temp, store, core) = core_with_store(4, 16, 4096);
        let data = payload();
        publish_ready_block(&store, data.clone(), BLOCK_STAMP);
        let open = core
            .open_read(read_open_request_for(0, 8, BLOCK_STAMP, 4))
            .await
            .expect("open read");

        let first = core.read_stream(open.stream_id, 4).await.expect("first read");
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].data, data.slice(0..4));
        assert!(!first[0].eos);
        assert_eq!(
            core.stream_manager().get(open.stream_id).await.expect("stream").cursor,
            4
        );

        let second = core.read_stream(open.stream_id, 4).await.expect("second read");
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].data, data.slice(4..8));
        assert!(second[0].eos);
        assert!(core.stream_manager().get(open.stream_id).await.is_none());
    }

    #[tokio::test]
    async fn read_stream_respects_max_bytes() {
        let (_temp, store, core) = core_with_store(8, 16, 4096);
        let data = payload();
        publish_ready_block(&store, data.clone(), BLOCK_STAMP);
        let open = core
            .open_read(read_open_request_for(0, 8, BLOCK_STAMP, 8))
            .await
            .expect("open read");

        let frames = core.read_stream(open.stream_id, 3).await.expect("read stream");

        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data.len(), 3);
        assert_eq!(frames[0].data, data.slice(0..3));
        assert!(!frames[0].eos);
    }

    #[tokio::test]
    async fn read_stream_rejects_missing_stream() {
        let (_temp, _store, core) = core_with_store(8, 16, 4096);

        assert_not_found(core.read_stream(stream_id(), 1024).await);
    }

    #[tokio::test]
    async fn read_stream_rejects_write_stream() {
        let (_temp, _store, core) = core_with_store(8, 16, 4096);
        let state = StreamState::new(write_stream_context());
        core.stream_manager().register(state).await;

        match core
            .read_stream(stream_id(), 1024)
            .await
            .expect_err("write stream must not be readable")
        {
            WorkerError::InvalidArgument(message) => assert!(message.contains("not a read stream")),
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
        assert_eq!(core.stream_manager().get(stream_id()).await.expect("stream").cursor, 0);
    }

    #[tokio::test]
    async fn open_write_stream_returns_success_response() {
        let (_temp, _store, core) = core_with_store(512, 2048, 4096);
        let service = WorkerDataServiceImpl::new(Arc::new(core));

        let response = service
            .open_write_stream(tonic::Request::new(open_write_proto(0)))
            .await
            .expect("open write response")
            .into_inner();

        assert!(response.header.expect("header").error.is_none());
        assert!(response.stream_id.is_some());
        assert_eq!(response.frame_size, 512);
        assert_eq!(response.window_bytes, 4096);
        assert_eq!(response.block_stamp, BLOCK_STAMP);
        assert_eq!(response.committed_length, 0);
        assert_eq!(response.chunk_size, CHUNK_SIZE);
    }

    #[tokio::test]
    async fn write_stream_returns_written_through() {
        let (_temp, _store, core) = core_with_store(512, 2048, 4096);
        let core = Arc::new(core);
        let service = WorkerDataServiceImpl::new(core.clone());
        let open = core.open_write(write_open_request()).await.expect("open write");

        let response = service
            .handle_write_frames(futures::stream::iter(vec![Ok(WriteStreamRequestProto {
                stream_id: Some(crate::data::convert::stream_id_to_proto(open.stream_id)),
                seq: 1,
                offset_in_block: 0,
                data: Bytes::from_static(b"abcd"),
                checksum32: 0,
            })]))
            .await
            .expect("write stream response");

        assert!(response.accepted);
        assert_eq!(response.last_acked_seq, 1);
        assert_eq!(response.written_through, 4);
    }

    #[tokio::test]
    async fn commit_write_returns_success_after_full_write() {
        let (_temp, _store, core) = core_with_store(512, 2048, 4096);
        let core = Arc::new(core);
        let service = WorkerDataServiceImpl::new(core.clone());
        let open = service
            .open_write_stream(tonic::Request::new(open_write_proto(2048)))
            .await
            .expect("open write")
            .into_inner();
        let stream_id = crate::data::convert::proto_to_stream_id(open.stream_id, "stream_id").expect("stream id");
        let data = payload();
        core.write_stream(WriteFrame {
            stream_id,
            seq: 1,
            offset_in_block: 0,
            data: data.slice(0..2048),
            checksum32: 0,
        })
        .await
        .expect("first frame");
        core.write_stream(WriteFrame {
            stream_id,
            seq: 2,
            offset_in_block: 2048,
            data: data.slice(2048..4096),
            checksum32: 0,
        })
        .await
        .expect("second frame");

        let response = service
            .commit_write(tonic::Request::new(commit_write_proto(stream_id, 2, BLOCK_SIZE)))
            .await
            .expect("commit write response")
            .into_inner();

        assert!(response.header.expect("header").error.is_none());
        assert_eq!(response.effective_block_len, BLOCK_SIZE);
        assert_eq!(response.block_stamp, BLOCK_STAMP);
        assert_eq!(response.written_through, BLOCK_SIZE);
    }

    #[tokio::test]
    async fn sync_committed_block_returns_success_for_ready_block() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        publish_ready_block(&store, payload(), BLOCK_STAMP);
        let service = WorkerDataServiceImpl::new(Arc::new(core));

        let response = service
            .sync_committed_block(tonic::Request::new(sync_committed_block_proto(BLOCK_STAMP, BLOCK_SIZE)))
            .await
            .expect("sync committed block response")
            .into_inner();

        assert!(response.header.expect("header").error.is_none());
        assert_eq!(response.effective_block_len, BLOCK_SIZE);
        assert_eq!(response.block_stamp, BLOCK_STAMP);
    }

    #[tokio::test]
    async fn open_read_stream_returns_success_response_for_ready_block() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        publish_ready_block(&store, payload(), BLOCK_STAMP);
        let service = WorkerDataServiceImpl::new(Arc::new(core));

        let response = service
            .open_read_stream(tonic::Request::new(open_read_proto(0, 1024, BLOCK_STAMP, 0)))
            .await
            .expect("open read response")
            .into_inner();

        assert!(response.header.expect("header").error.is_none());
        assert!(response.stream_id.is_some());
        assert_eq!(response.frame_size, 512);
        assert_eq!(response.window_bytes, 4096);
        assert_eq!(response.block_stamp, BLOCK_STAMP);
        assert_eq!(response.committed_length, BLOCK_SIZE);
        assert_eq!(response.chunk_size, CHUNK_SIZE);
    }

    #[tokio::test]
    async fn open_read_stream_returns_need_refresh_on_stale_stamp() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        publish_ready_block(&store, payload(), BLOCK_STAMP);
        let service = WorkerDataServiceImpl::new(Arc::new(core));

        let response = service
            .open_read_stream(tonic::Request::new(open_read_proto(0, 1024, BLOCK_STAMP + 1, 512)))
            .await
            .expect("open read response")
            .into_inner();
        let error = response
            .header
            .expect("header")
            .error
            .expect("stale stamp should return structured error");

        assert_eq!(error.error_class, ErrorClassProto::ErrorClassNeedRefresh as i32);
        assert_eq!(
            error.refresh_reason,
            RefreshReasonProto::RefreshReasonBlockStampMismatch as i32
        );
        assert!(response.stream_id.is_none());
    }

    #[tokio::test]
    async fn open_read_stream_returns_header_error_on_zero_stamp() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        publish_ready_block(&store, payload(), BLOCK_STAMP);
        let service = WorkerDataServiceImpl::new(Arc::new(core));

        let response = service
            .open_read_stream(tonic::Request::new(open_read_proto(0, 1024, 0, 512)))
            .await
            .expect("open read response")
            .into_inner();
        let error = response
            .header
            .expect("header")
            .error
            .expect("zero stamp should return structured error");

        assert_eq!(error.error_class, ErrorClassProto::ErrorClassFatal as i32);
        assert!(error.message.contains("block_stamp"));
        assert!(response.stream_id.is_none());
    }

    #[tokio::test]
    async fn read_stream_returns_data_frames() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        let data = payload();
        publish_ready_block(&store, data.clone(), BLOCK_STAMP);
        let service = WorkerDataServiceImpl::new(Arc::new(core));

        let open = service
            .open_read_stream(tonic::Request::new(open_read_proto(4, 6, BLOCK_STAMP, 512)))
            .await
            .expect("open read response")
            .into_inner();
        let stream_id = open.stream_id.expect("stream id");
        let response_stream = service
            .read_stream(tonic::Request::new(ReadStreamRequestProto {
                stream_id: Some(stream_id),
                max_bytes: 0,
            }))
            .await
            .expect("read stream response")
            .into_inner();
        let frames = response_stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("stream frames");

        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].offset_in_block, 4);
        assert_eq!(frames[0].data, data.slice(4..10));
        assert!(frames[0].eos);
    }

    #[tokio::test]
    async fn service_read_stream_rejects_missing_stream() {
        let service = WorkerDataServiceImpl::new(Arc::new(WorkerCore::new(1024)));

        let read_status = match service
            .read_stream(tonic::Request::new(ReadStreamRequestProto {
                stream_id: Some(test_stream_id_proto()),
                max_bytes: 1024,
            }))
            .await
        {
            Ok(_) => panic!("ReadStream unexpectedly succeeded"),
            Err(status) => status,
        };
        assert_eq!(read_status.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn stream_manager_register_get_touch_remove_and_cleanup() {
        let manager = StreamManager::new(Duration::from_millis(50));
        let mut state = StreamState::new(stream_context());
        state.last_activity = Instant::now() - Duration::from_secs(10);

        manager.register(state.clone()).await;
        assert_eq!(manager.active_count().await, 1);
        assert_eq!(manager.get(stream_id()).await.unwrap().context.stream_id, stream_id());

        assert!(manager.touch(stream_id()).await);
        let touched = manager.get(stream_id()).await.unwrap();
        assert!(touched.last_activity > state.last_activity);

        manager.remove(stream_id()).await;
        assert_eq!(manager.active_count().await, 0);

        let mut idle = StreamState::new(stream_context());
        idle.last_activity = Instant::now() - Duration::from_secs(10);
        manager.register(idle).await;
        assert_eq!(manager.cleanup_idle_streams().await, 1);
        assert_eq!(manager.active_count().await, 0);
    }

    #[test]
    fn worker_lib_exports_only_current_data_plane_surface() {
        let lib = include_str!("lib.rs");

        for old_module in [
            "mod block_manager",
            "mod block_store",
            "mod convert",
            "pub mod core",
            "pub mod rpc_server",
            "pub mod service",
            "pub mod stream_manager",
            "pub mod admin",
            "pub mod combo_validator",
            "pub mod command_executor",
            "pub mod data_header",
            "pub mod delete_op_log",
            "pub mod eviction",
            "pub mod lifecycle",
            "pub mod metadata_client",
            "pub mod orphan",
            "pub mod pending_acks",
            "pub mod pipeline",
            "pub mod rebalance",
            "pub mod replication",
            "pub mod ufs_fill",
            "pub mod volume_health",
            "pub mod volume_manager",
            "#[path",
        ] {
            assert!(
                !lib.contains(old_module),
                "{old_module} must stay out of worker lib exports"
            );
        }

        for current_module in [
            "pub mod config",
            "pub mod data",
            "pub mod error",
            "pub mod net",
            "pub mod runtime",
            "pub mod store",
        ] {
            assert!(lib.contains(current_module), "lib.rs must declare {current_module}");
        }
    }

    #[test]
    fn worker_source_tree_matches_data_runtime_store_layout() {
        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");

        for required in [
            "data/mod.rs",
            "data/convert.rs",
            "data/core.rs",
            "net/mod.rs",
            "net/server/grpc.rs",
            "runtime/mod.rs",
            "runtime/stream.rs",
            "runtime/block.rs",
            "store/mod.rs",
            "store/block.rs",
        ] {
            assert!(src.join(required).exists(), "missing worker source file: {required}");
        }

        for removed in [
            "ufs_fill.rs",
            "replication.rs",
            "rebalance.rs",
            "eviction.rs",
            "orphan.rs",
            "command_executor.rs",
            "pipeline.rs",
            "delete_op_log.rs",
            "pending_acks.rs",
            "volume_health.rs",
            "volume_manager.rs",
            "metadata_client.rs",
            "lifecycle.rs",
            "combo_validator.rs",
            "admin.rs",
            "data_header.rs",
            "rpc_server.rs",
            "replication_tests.rs",
            "tests/delete_op_log_tests.rs",
        ] {
            assert!(
                !src.join(removed).exists(),
                "remove inactive worker source file: {removed}"
            );
        }
    }

    #[test]
    fn config_and_binary_do_not_initialize_inactive_paths() {
        let config = include_str!("config.rs");
        let main = include_str!("bin/main.rs");

        for forbidden in [
            "UfsConfig",
            "ReplicationConfig",
            "EvictionConfig",
            "OrphanConfig",
            "VolumeHealthConfig",
            "MetadataConfig",
            "combo",
            "fallback_transport",
            "heartbeat",
            "block_report",
        ] {
            assert!(
                !config.contains(forbidden),
                "config.rs must not retain inactive setting: {forbidden}"
            );
        }

        for forbidden in [
            "RpcServer",
            "Ufs",
            "Replication",
            "MetadataClient",
            "Rebalance",
            "Eviction",
            "Orphan",
            "Lifecycle",
            "Volume",
        ] {
            assert!(
                !main.contains(forbidden),
                "worker main must not initialize inactive path: {forbidden}"
            );
        }
    }

    #[test]
    fn core_does_not_import_wire_types() {
        let core = include_str!("data/core.rs");

        for forbidden in ["proto::", "prost", "tonic"] {
            assert!(!core.contains(forbidden), "core.rs must not import {forbidden}");
        }
    }

    #[test]
    fn grpc_server_stays_adapter_only() {
        let service = include_str!("net/server/grpc.rs");

        for forbidden in [
            "ufs",
            "replication",
            "tier",
            "quorum",
            "BlockStore",
            "BlockManager",
            "StreamManager",
            "FileLayout",
        ] {
            assert!(
                !service.contains(forbidden),
                "net/server/grpc.rs must not depend on {forbidden}"
            );
        }
    }

    #[test]
    fn block_manager_stays_validation_only_and_store_stays_local_only() {
        let block_manager = include_str!("runtime/block.rs");
        let block_store = include_str!("store/block.rs");
        let meta_codec = include_str!("store/meta_codec.rs");

        for forbidden in [
            "ReplicationClient",
            "replicate",
            "read_chunk",
            "write_chunk",
            "delete_block",
        ] {
            assert!(
                !block_manager.contains(forbidden),
                "block_manager.rs must not retain {forbidden}"
            );
        }

        for forbidden in [
            "proto::",
            "prost",
            "tonic",
            "WorkerCore::",
            "WorkerDataService",
            "StreamManager",
            "TransportFrame",
            "ReadChunk",
            "WriteChunk",
            "ReadRange",
            "read_chunk",
            "write_chunk",
            "ufs",
            "replication",
            "quorum",
            ".chunk\"",
        ] {
            assert!(
                !block_store.contains(forbidden),
                "block_store.rs must stay local-format only and avoid {forbidden}"
            );
        }

        assert!(!meta_codec.contains("tonic"), "meta_codec.rs must not depend on tonic");
        for forbidden in [
            "WorkerCore::",
            "WorkerDataService",
            "StreamManager",
            "TransportFrame",
            "ReadChunk",
            "WriteChunk",
            "ReadRange",
            "read_chunk",
            "write_chunk",
            "ufs",
            "replication",
            "quorum",
            ".chunk\"",
        ] {
            assert!(
                !meta_codec.contains(forbidden),
                "meta_codec.rs must stay metadata-payload-only and avoid {forbidden}"
            );
        }
    }

    #[test]
    fn stream_state_keeps_runtime_fields_out_of_open_context() {
        let stream_manager = include_str!("runtime/stream.rs");
        let core = include_str!("data/core.rs");

        assert!(stream_manager.contains("pub context: StreamContext"));
        assert!(
            !core.contains("last_activity"),
            "StreamContext must not carry runtime activity"
        );

        for duplicate in [
            "pub chunk_size:",
            "pub flow_control_window:",
            "pub block_stamp:",
            "pub committed_length:",
        ] {
            assert!(
                !stream_manager.contains(duplicate),
                "StreamState must not duplicate open context field {duplicate}"
            );
        }
    }

    #[test]
    fn worker_data_proto_excludes_old_chunk_range_api() {
        let sources = [
            include_str!("../../proto/worker/data.proto"),
            include_str!("data/core.rs"),
            include_str!("net/server/grpc.rs"),
            include_str!("data/convert.rs"),
            include_str!("runtime/block.rs"),
            include_str!("store/block.rs"),
            include_str!("store/meta_codec.rs"),
            include_str!("lib.rs"),
        ];

        for old_name in [
            "ReadChunk",
            "WriteChunk",
            "ReadRange",
            "ReadChunkRequestProto",
            "WriteChunkRequestProto",
            "ReadRangeRequestProto",
            "ChunkDataProto",
            "ChunkSliceProto",
        ] {
            assert!(
                sources.iter().all(|source| !source.contains(old_name)),
                "{old_name} must stay out of the worker data-plane skeleton"
            );
        }
    }

    #[test]
    fn worker_write_proto_fields_are_normalized() {
        let proto = include_str!("../../proto/worker/data.proto");

        assert_eq!(
            proto_message_fields(proto, "OpenWriteStreamRequestProto"),
            vec![
                ("worker.DataRequestHeaderProto", "header", 1),
                ("common.ShardGroupIdProto", "group_id", 2),
                ("common.BlockIdProto", "block_id", 3),
                ("uint64", "block_size", 4),
                ("uint64", "block_stamp", 5),
                ("uint32", "chunk_size", 6),
                ("worker.ChecksumKindProto", "checksum_kind", 7),
                ("common.FencingTokenProto", "token", 8),
                ("uint32", "frame_size", 9),
            ]
        );
        assert_eq!(
            proto_message_fields(proto, "CommitWriteRequestProto"),
            vec![
                ("worker.DataRequestHeaderProto", "header", 1),
                ("common.ShardGroupIdProto", "group_id", 2),
                ("common.BlockIdProto", "block_id", 3),
                ("common.StreamIdProto", "stream_id", 4),
                ("uint64", "effective_block_len", 5),
                ("uint64", "block_stamp", 6),
                ("common.FencingTokenProto", "token", 7),
                ("uint64", "commit_seq", 8),
                ("bool", "require_sync", 9),
            ]
        );
        assert_eq!(
            proto_message_fields(proto, "CommitWriteResponseProto"),
            vec![
                ("worker.DataResponseHeaderProto", "header", 1),
                ("uint64", "effective_block_len", 2),
                ("uint64", "block_stamp", 3),
                ("uint64", "written_through", 4),
            ]
        );
        assert_eq!(
            proto_message_fields(proto, "SyncCommittedBlockRequestProto"),
            vec![
                ("worker.DataRequestHeaderProto", "header", 1),
                ("common.ShardGroupIdProto", "group_id", 2),
                ("common.BlockIdProto", "block_id", 3),
                ("uint64", "block_stamp", 4),
                ("uint64", "expected_block_len", 5),
            ]
        );
        assert_eq!(
            proto_message_fields(proto, "SyncCommittedBlockResponseProto"),
            vec![
                ("worker.DataResponseHeaderProto", "header", 1),
                ("uint64", "effective_block_len", 2),
                ("uint64", "block_stamp", 3),
            ]
        );
        assert_eq!(
            proto_message_fields(proto, "AbortWriteRequestProto"),
            vec![
                ("worker.DataRequestHeaderProto", "header", 1),
                ("common.ShardGroupIdProto", "group_id", 2),
                ("common.BlockIdProto", "block_id", 3),
                ("common.StreamIdProto", "stream_id", 4),
                ("common.FencingTokenProto", "token", 5),
            ]
        );
        assert_eq!(
            proto_message_fields(proto, "WriteStreamResponseProto"),
            vec![
                ("bool", "accepted", 1),
                ("uint64", "last_acked_seq", 2),
                ("uint64", "written_through", 3),
            ]
        );
    }

    #[test]
    fn active_write_path_uses_written_through_name() {
        let forbidden = concat!("persisted", "_through");
        let sources = [
            include_str!("../../proto/worker/data.proto"),
            include_str!("data/core.rs"),
            include_str!("net/server/grpc.rs"),
            include_str!("runtime/stream.rs"),
            include_str!("data/convert.rs"),
        ];

        assert!(
            sources.iter().all(|source| !source.contains(forbidden)),
            "{forbidden} must not remain in active write-path code"
        );
    }

    #[test]
    fn active_worker_sources_do_not_use_staged_version_labels() {
        let sources = [
            include_str!("data/core.rs"),
            include_str!("net/server/grpc.rs"),
            include_str!("data/convert.rs"),
            include_str!("runtime/stream.rs"),
            include_str!("runtime/block.rs"),
            include_str!("store/block.rs"),
            include_str!("lib.rs"),
        ];

        for forbidden in [concat!("Pha", "se"), concat!("v", "1"), concat!("v", "2")] {
            assert!(
                sources.iter().all(|source| !source.contains(forbidden)),
                "{forbidden} must stay out of active worker source text"
            );
        }
    }

    fn proto_message_fields<'a>(source: &'a str, message: &str) -> Vec<(&'a str, &'a str, u32)> {
        let start = format!("message {message} {{");
        let mut in_message = false;
        let mut fields = Vec::new();
        for raw_line in source.lines() {
            let line = raw_line.trim();
            if line == start {
                in_message = true;
                continue;
            }
            if in_message && line == "}" {
                break;
            }
            if !in_message || line.starts_with("//") || line.is_empty() || !line.ends_with(';') {
                continue;
            }

            let field = line.trim_end_matches(';');
            let (left, tag) = field.split_once(" = ").expect("proto field must have tag");
            let mut left_parts = left.split_whitespace();
            let ty = left_parts.next().expect("proto field type");
            let name = left_parts.next().expect("proto field name");
            assert!(left_parts.next().is_none(), "unexpected proto field modifier: {line}");
            fields.push((ty, name, tag.parse().expect("numeric proto tag")));
        }
        assert!(in_message, "missing proto message {message}");
        fields
    }
}
