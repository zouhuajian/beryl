// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Unit tests for the worker data-plane skeleton.

#[cfg(test)]
#[allow(clippy::module_inception)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use proto::common::{
        error_detail_proto, BlockIdProto, ByteRangeProto, ClientInfoProto, ErrorClassProto, FencingTokenProto,
        FsErrnoProto, StreamIdProto,
    };
    use proto::worker::worker_data_service_server::WorkerDataService;
    use proto::worker::{
        AbortWriteRequestProto, CommitWriteRequestProto, DataRequestHeaderProto, DataResponseHeaderProto,
        OpenReadStreamRequestProto, OpenWriteStreamRequestProto, ReadStreamRequestProto, WriteStreamRequestProto,
    };
    use types::chunk::ByteRange;
    use types::ids::{BlockId, BlockIndex, ChunkIndex, ClientId, DataHandleId, StreamId};
    use types::lease::FencingToken;

    use crate::data::convert::{
        proto_to_abort_write_request, proto_to_commit_write_request, proto_to_read_open_request, proto_to_write_frame,
        proto_to_write_open_request,
    };
    use crate::data::core::{
        AbortWriteRequest, CommitWriteRequest, RangeMapper, ReadOpenRequest, StreamContext, StreamMode, WorkerCore,
        WorkerCoreResult, WriteFrame, WriteOpenRequest,
    };
    use crate::data::service::WorkerDataServiceImpl;
    use crate::error::WorkerError;
    use crate::runtime::stream::{StreamManager, StreamState};

    fn block_id() -> BlockId {
        BlockId::new(DataHandleId::new(7), BlockIndex::new(3))
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

    fn assert_unimplemented<T: std::fmt::Debug>(result: WorkerCoreResult<T>, operation: &str) {
        let error = result.expect_err("operation should be a placeholder");
        match error {
            WorkerError::Unimplemented(message) => {
                assert!(message.contains(operation), "unexpected placeholder message: {message}")
            }
            other => panic!("expected Unimplemented, got {other:?}"),
        }
    }

    fn assert_unimplemented_header(header: Option<DataResponseHeaderProto>) {
        let error = header.expect("missing header").error.expect("missing error");
        assert_eq!(error.error_class, ErrorClassProto::ErrorClassFatal as i32);
        assert_eq!(
            error.code,
            Some(error_detail_proto::Code::FsErrno(FsErrnoProto::FsErrnoEnotimpl as i32))
        );
        assert!(error.message.contains("not implemented"));
    }

    fn read_open_request() -> ReadOpenRequest {
        ReadOpenRequest {
            block_id: block_id(),
            byte_range: ByteRange { offset: 128, len: 4096 },
            block_stamp: 0,
            frame_size: 8192,
        }
    }

    fn write_open_request() -> WriteOpenRequest {
        WriteOpenRequest {
            block_id: block_id(),
            token: token(),
            block_stamp: 17,
            frame_size: 8192,
        }
    }

    fn commit_write_request() -> CommitWriteRequest {
        CommitWriteRequest {
            stream_id: stream_id(),
            block_id: block_id(),
            token: token(),
            commit_seq: 8,
            committed_length: 4096,
            require_sync: true,
        }
    }

    fn abort_write_request() -> AbortWriteRequest {
        AbortWriteRequest {
            stream_id: stream_id(),
            block_id: block_id(),
            token: token(),
            reason: "client cancelled".to_string(),
        }
    }

    fn stream_context() -> StreamContext {
        StreamContext {
            stream_id: stream_id(),
            block_id: block_id(),
            mode: StreamMode::Read,
            frame_size: 8192,
            window_bytes: 65_536,
            block_stamp: 17,
            committed_length: 4096,
            byte_range: Some(ByteRange { offset: 0, len: 4096 }),
            fencing_token: None,
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
            block_id: Some(test_block_id_proto()),
            byte_range: Some(ByteRangeProto { offset: 128, len: 4096 }),
            block_stamp: 0,
            frame_size: 8192,
        };

        let domain = proto_to_read_open_request(request).unwrap();

        assert_eq!(domain.block_id, block_id());
        assert_eq!(domain.byte_range, ByteRange { offset: 128, len: 4096 });
        assert_eq!(domain.block_stamp, 0);
        assert_eq!(domain.frame_size, 8192);
    }

    #[test]
    fn converts_open_write_stream_request_to_domain() {
        let request = OpenWriteStreamRequestProto {
            header: Some(test_header()),
            block_id: Some(test_block_id_proto()),
            token: Some(test_token_proto()),
            block_stamp: 17,
            frame_size: 8192,
        };

        let domain = proto_to_write_open_request(request).unwrap();

        assert_eq!(domain.block_id, block_id());
        assert_eq!(domain.token.owner, ClientId::new(9));
        assert_eq!(domain.token.epoch, 11);
        assert_eq!(domain.block_stamp, 17);
        assert_eq!(domain.frame_size, 8192);
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
            stream_id: Some(test_stream_id_proto()),
            block_id: Some(test_block_id_proto()),
            token: Some(test_token_proto()),
            commit_seq: 8,
            committed_length: 4096,
            require_sync: true,
        })
        .unwrap();

        assert_eq!(commit.stream_id, stream_id());
        assert_eq!(commit.block_id, block_id());
        assert_eq!(commit.token.epoch, 11);
        assert_eq!(commit.commit_seq, 8);
        assert_eq!(commit.committed_length, 4096);
        assert!(commit.require_sync);

        let abort = proto_to_abort_write_request(AbortWriteRequestProto {
            header: Some(test_header()),
            stream_id: Some(test_stream_id_proto()),
            block_id: Some(test_block_id_proto()),
            token: Some(test_token_proto()),
            reason: "client cancelled".to_string(),
        })
        .unwrap();

        assert_eq!(abort.stream_id, stream_id());
        assert_eq!(abort.block_id, block_id());
        assert_eq!(abort.token.owner, ClientId::new(9));
        assert_eq!(abort.reason, "client cancelled");
    }

    #[test]
    fn conversion_reports_missing_required_fields_without_panic() {
        let read_err = proto_to_read_open_request(OpenReadStreamRequestProto {
            header: Some(test_header()),
            block_id: None,
            byte_range: Some(ByteRangeProto { offset: 0, len: 1 }),
            block_stamp: 0,
            frame_size: 1024,
        })
        .unwrap_err();
        assert!(read_err.to_string().contains("missing block_id"));

        let write_open_err = proto_to_write_open_request(OpenWriteStreamRequestProto {
            header: Some(test_header()),
            block_id: Some(test_block_id_proto()),
            token: None,
            block_stamp: 0,
            frame_size: 1024,
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
            stream_id: None,
            block_id: Some(test_block_id_proto()),
            token: Some(test_token_proto()),
            commit_seq: 1,
            committed_length: 1,
            require_sync: false,
        })
        .unwrap_err();
        assert!(commit_err.to_string().contains("missing stream_id"));
    }

    #[tokio::test]
    async fn worker_core_open_commit_abort_placeholders_are_explicit() {
        let core = WorkerCore::new(1024);

        assert_unimplemented(core.open_read(read_open_request()).await, "OpenReadStream");
        assert_unimplemented(core.open_write(write_open_request()).await, "OpenWriteStream");
        assert_unimplemented(core.commit_write(commit_write_request()).await, "CommitWrite");
        assert_unimplemented(core.abort_write(abort_write_request()).await, "AbortWrite");
    }

    #[tokio::test]
    async fn worker_core_stream_placeholders_do_not_ack_data() {
        let core = WorkerCore::new(1024);
        let frame = WriteFrame {
            stream_id: stream_id(),
            seq: 1,
            offset_in_block: 0,
            data: Bytes::from_static(b"payload"),
            checksum32: 0,
        };

        assert_unimplemented(core.read_frame(stream_id(), 1024).await, "ReadStream");
        assert_unimplemented(core.read_stream(stream_id(), 1024).await, "ReadStream");
        assert_unimplemented(core.write_frame(frame.clone()).await, "WriteStream");
        assert_unimplemented(core.write_stream(frame).await, "WriteStream");
    }

    #[tokio::test]
    async fn service_open_and_commit_placeholders_return_data_header_errors() {
        let service = WorkerDataServiceImpl::new(Arc::new(WorkerCore::new(1024)));

        let open_read = service
            .open_read_stream(tonic::Request::new(OpenReadStreamRequestProto {
                header: Some(test_header()),
                block_id: Some(test_block_id_proto()),
                byte_range: Some(ByteRangeProto { offset: 0, len: 1024 }),
                block_stamp: 0,
                frame_size: 1024,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_unimplemented_header(open_read.header);

        let open_write = service
            .open_write_stream(tonic::Request::new(OpenWriteStreamRequestProto {
                header: Some(test_header()),
                block_id: Some(test_block_id_proto()),
                token: Some(test_token_proto()),
                block_stamp: 0,
                frame_size: 1024,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_unimplemented_header(open_write.header);

        let commit = service
            .commit_write(tonic::Request::new(CommitWriteRequestProto {
                header: Some(test_header()),
                stream_id: Some(test_stream_id_proto()),
                block_id: Some(test_block_id_proto()),
                token: Some(test_token_proto()),
                commit_seq: 1,
                committed_length: 1024,
                require_sync: false,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_unimplemented_header(commit.header);

        let abort = service
            .abort_write(tonic::Request::new(AbortWriteRequestProto {
                header: Some(test_header()),
                stream_id: Some(test_stream_id_proto()),
                block_id: Some(test_block_id_proto()),
                token: Some(test_token_proto()),
                reason: "client cancelled".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_unimplemented_header(abort.header);
    }

    #[tokio::test]
    async fn service_stream_placeholders_return_unimplemented_status() {
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
        assert_eq!(read_status.code(), tonic::Code::Unimplemented);

        let write_status = WorkerDataServiceImpl::write_stream_placeholder_status();
        assert_eq!(write_status.code(), tonic::Code::Unimplemented);
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
            "data/service.rs",
            "data/convert.rs",
            "data/core.rs",
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
    fn service_stays_adapter_only() {
        let service = include_str!("data/service.rs");

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
                "service.rs must not depend on {forbidden}"
            );
        }
    }

    #[test]
    fn block_manager_stays_placeholder_and_store_stays_local_only() {
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
            include_str!("data/service.rs"),
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
    fn active_worker_sources_do_not_use_staged_version_labels() {
        let sources = [
            include_str!("data/core.rs"),
            include_str!("data/service.rs"),
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
}
