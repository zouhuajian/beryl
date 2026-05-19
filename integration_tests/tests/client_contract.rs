// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

mod common;

use client::{ClientConfig, FsClient};
use common::{init_logging, MockMetadataServer, MockWorkerServer};
use proto::common::{
    error_detail_proto, BlockIdProto, ByteRangeProto, ClientInfoProto, ErrorClassProto, FencingTokenProto,
    FsErrnoProto, ShardGroupIdProto, StreamIdProto,
};
use proto::metadata::MsyncRequestProto;
use proto::worker::worker_data_service_server::WorkerDataService;
use proto::worker::{
    ChecksumKindProto, CommitWriteRequestProto, DataRequestHeaderProto, DataResponseHeaderProto,
    OpenReadStreamRequestProto, OpenWriteStreamRequestProto,
};
use tonic::Request;

/// Test client initialization stays lightweight without contacting endpoints.
#[tokio::test]
async fn test_client_init() {
    init_logging();

    let mut config = ClientConfig::default();
    config.inner.inner.set("client.id", 1i64);

    let client = FsClient::try_new(config).expect("FsClient construction must stay lazy");

    assert_eq!(client.config().metadata_group_ids, vec![1]);
}

fn data_request_header(call_id: &str) -> DataRequestHeaderProto {
    DataRequestHeaderProto {
        client: Some(ClientInfoProto {
            call_id: call_id.to_string(),
            client_id: 1,
            client_name: "integration-test".to_string(),
        }),
        traceparent: String::new(),
    }
}

fn block_id(data_handle_id: u64, block_index: u32) -> BlockIdProto {
    BlockIdProto {
        data_handle_id,
        block_index,
    }
}

fn group_id(value: u64) -> ShardGroupIdProto {
    ShardGroupIdProto { value }
}

fn assert_stream_unimplemented_header(header: DataResponseHeaderProto, operation: &str) {
    let error = header.error.expect("placeholder response should carry error detail");
    assert_eq!(error.error_class, ErrorClassProto::ErrorClassFatal as i32);
    assert_eq!(
        error.code,
        Some(error_detail_proto::Code::FsErrno(FsErrnoProto::FsErrnoEnotimpl as i32))
    );
    assert!(error.message.contains(operation));
}

/// Direct worker reads are stream placeholders until worker execution is wired.
#[tokio::test]
async fn test_direct_worker_open_read_stream_is_explicitly_unimplemented() {
    init_logging();

    let mock_worker = MockWorkerServer::new(1);
    let request = Request::new(OpenReadStreamRequestProto {
        header: Some(data_request_header("open-read-stream")),
        group_id: Some(group_id(1)),
        block_id: Some(block_id(100, 0)),
        byte_range: Some(ByteRangeProto { offset: 0, len: 9 }),
        block_stamp: 1,
        frame_size: 4096,
    });

    let response = WorkerDataService::open_read_stream(&mock_worker, request)
        .await
        .expect("open read placeholder should use structured response")
        .into_inner();

    assert!(response.stream_id.is_none());
    assert_eq!(response.frame_size, 0);
    assert_eq!(response.window_bytes, 0);
    assert_stream_unimplemented_header(response.header.expect("data header"), "OpenReadStream");
}

/// Test Msync with mock metadata server.
#[tokio::test]
async fn test_msync_with_mock() {
    init_logging();

    let mock_server = MockMetadataServer::new(1, vec![2, 3]);

    use proto::common::ClientInfoProto;
    use proto::common::RequestHeaderProto;
    let header = RequestHeaderProto {
        client: Some(ClientInfoProto {
            call_id: "test-call-id".to_string(),
            client_id: 1,
            client_name: "test-client".to_string(),
        }),
        deadline_ms: 0,
        traceparent: String::new(),
        caller_context: None,
        state: Vec::new(),
        retry_count: 0,
        group_id: 1,
        mount_epoch: None,
        route_epoch: None,
        principal: String::new(),
        real_user: String::new(),
        doas: String::new(),
        authn_type: proto::common::AuthnTypeProto::Unspecified as i32,
    };

    let request = tonic::Request::new(MsyncRequestProto { header: Some(header) });

    let response = mock_server.msync(request).await;
    assert!(response.is_ok());
    let resp = response.unwrap().into_inner();
    assert!(resp.header.is_some());
    assert!(resp.state.is_some());
}

#[tokio::test]
async fn test_msync_mock_rejects_missing_group_id() {
    init_logging();

    let mock_server = MockMetadataServer::new(1, vec![2, 3]);
    let request = tonic::Request::new(MsyncRequestProto {
        header: Some(proto::common::RequestHeaderProto {
            client: Some(proto::common::ClientInfoProto {
                call_id: "test-call-id".to_string(),
                client_id: 1,
                client_name: "test-client".to_string(),
            }),
            deadline_ms: 0,
            traceparent: String::new(),
            caller_context: None,
            state: Vec::new(),
            retry_count: 0,
            group_id: 0,
            mount_epoch: None,
            route_epoch: None,
            principal: String::new(),
            real_user: String::new(),
            doas: String::new(),
            authn_type: proto::common::AuthnTypeProto::Unspecified as i32,
        }),
    });

    let response = mock_server
        .msync(request)
        .await
        .expect("mock msync uses gRPC OK for application errors")
        .into_inner();

    let header = response.header.expect("response header");
    assert!(header.error.is_some());
    assert!(response.state.is_none());
}

/// Direct worker writes are stream placeholders until worker execution is wired.
#[tokio::test]
async fn test_direct_worker_open_write_stream_is_explicitly_unimplemented() {
    init_logging();

    let mock_worker = MockWorkerServer::new(1);
    let request = Request::new(OpenWriteStreamRequestProto {
        header: Some(data_request_header("open-write-stream")),
        group_id: Some(ShardGroupIdProto { value: 1 }),
        block_id: Some(block_id(10, 0)),
        block_size: 4096,
        block_stamp: 1,
        chunk_size: 1024,
        checksum_kind: ChecksumKindProto::ChecksumKindNone as i32,
        token: Some(FencingTokenProto {
            block_id: Some(block_id(10, 0)),
            owner: 1,
            epoch: 1,
        }),
        frame_size: 4096,
    });

    let response = WorkerDataService::open_write_stream(&mock_worker, request)
        .await
        .expect("open write placeholder should use structured response")
        .into_inner();

    assert!(response.stream_id.is_none());
    assert_eq!(response.frame_size, 0);
    assert_eq!(response.window_bytes, 0);
    assert_stream_unimplemented_header(response.header.expect("data header"), "OpenWriteStream");
}

/// CommitWrite is also a phase-1 placeholder and must not pretend data was persisted.
#[tokio::test]
async fn test_direct_worker_commit_write_is_explicitly_unimplemented() {
    init_logging();

    let mock_worker = MockWorkerServer::new(1);
    let request = Request::new(CommitWriteRequestProto {
        header: Some(data_request_header("commit-write")),
        group_id: Some(ShardGroupIdProto { value: 1 }),
        block_id: Some(block_id(20, 0)),
        stream_id: Some(StreamIdProto { high: 0, low: 7 }),
        effective_block_len: 4,
        block_stamp: 5,
        token: Some(FencingTokenProto {
            block_id: Some(block_id(20, 0)),
            owner: 1,
            epoch: 5,
        }),
        commit_seq: 1,
        require_sync: true,
    });

    let response = WorkerDataService::commit_write(&mock_worker, request)
        .await
        .expect("commit placeholder should use structured response")
        .into_inner();

    assert_eq!(response.effective_block_len, 0);
    assert_eq!(response.block_stamp, 0);
    assert_eq!(response.written_through, 0);
    assert_stream_unimplemented_header(response.header.expect("data header"), "CommitWrite");
}
