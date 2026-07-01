// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Focused unit tests for the public API facade and client runtime behavior.

use super::handle::{ReadHandle, WriteHandle};
use super::options::{DEFAULT_BLOCK_SIZE, DEFAULT_CHUNK_SIZE, DEFAULT_REPLICATION};
use super::*;
use crate::canonical::{ClientAction, RefreshHint};
use crate::config::{ClientConfig, MetadataGroupConfig};
use crate::data::{
    WorkerBlockSyncResult, WorkerBlockWriteHandle, WorkerCommitResult, WorkerDataClient, WorkerDataPlane,
    WorkerReadResult, WorkerWriteTarget,
};
use crate::error::{ClientError, ClientResult};
use crate::metadata::{AddBlockResult, MetadataGateway, ReadLayout};
use crate::planner::PlannedBlockRead;
use crate::runtime::{AttemptContext, ErrorClass, ErrorClassifier, MetadataTargets};
use crate::session::write_session::WriteSession;
use async_trait::async_trait;
use bytes::Bytes;
use common::error::canonical::{CanonicalError, RefreshHint as CanonicalRefreshHint, RefreshReason};
use common::header::RpcErrorCode;
use proto::common::{BlockIdProto, FencingTokenProto};
use proto::metadata::{
    AbortFileWriteResponseProto, AppendFileResponseProto, CommitFileResponseProto, CreateDirectoryResponseProto,
    CreateFileResponseProto, CreateModeProto, DeleteResponseProto, GetStatusResponseProto, ListStatusResponseProto,
    OpenFileResponseProto, RenameResponseProto, RenewLeaseResponseProto, SyncWriteResponseProto, WriteHandleProto,
    WriteSyncModeProto,
};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use types::lease::FencingToken;
use types::{
    BlockId, BlockIndex, ClientId, DataHandleId, FileBlockLocation, GroupName, WorkerEndpointInfo, WorkerId,
    WorkerNetProtocol, WriteTarget,
};

type EventLog = Arc<Mutex<Vec<&'static str>>>;

#[test]
fn independent_fs_clients_generate_distinct_nonzero_client_ids() {
    let first = fs_client_with_gateway(test_config("root"), Arc::new(MockGateway::default())).expect("first client");
    let second = fs_client_with_gateway(test_config("root"), Arc::new(MockGateway::default())).expect("second client");

    let first_id = first.runtime.executor.client_id();
    let second_id = second.runtime.executor.client_id();

    assert_ne!(first_id.as_raw(), 0);
    assert_ne!(second_id.as_raw(), 0);
    assert_ne!(first_id, second_id);
    assert_eq!(first.runtime.executor.client_name(), "default_client");
    assert_eq!(second.runtime.executor.client_name(), "default_client");
}

#[tokio::test]
async fn open_returns_reader_from_metadata_response() {
    let gateway = Arc::new(MockGateway::default());
    let client = fs_client_with_gateway(test_config("root"), gateway.clone()).expect("client");

    let reader = client.open("/alpha").await.expect("open succeeds");

    assert_eq!(reader.path(), "/alpha");
    assert_eq!(reader.size_hint(), 10);
    assert_eq!(methods(&gateway.calls()), vec!["open_file"]);
}

#[tokio::test]
async fn file_reader_debug_redacts_identity_names() {
    let config = ClientConfig {
        metadata_groups: vec![metadata_group_config("root")],
        ..ClientConfig::default()
    };
    let client = FsClient::try_new(config).expect("client");
    let read = FileReader::new(
        Arc::clone(&client.runtime),
        ReadHandle::new("/alpha".to_string(), DataHandleId::new(202), 3, 10),
    );
    let debug = format!("{read:?}");

    assert!(debug.contains("FileReader"));
    assert!(debug.contains("size_hint"));
    assert_debug_redacts_internal_identity_names(&debug);
}

#[tokio::test]
async fn create_returns_writer_and_maps_create_new_mode() {
    let gateway = Arc::new(MockGateway::default());
    let client = fs_client_with_gateway(test_config("root"), gateway.clone()).expect("client");

    assert_eq!(CreateOptions::create().create_mode, CreateMode::CreateNew);

    let writer = client
        .create("/created", CreateOptions::create())
        .await
        .expect("create writer");

    assert_eq!(writer.path(), "/created");
    let calls = gateway.calls();
    assert_eq!(methods(&calls), vec!["create_file"]);
    assert_eq!(calls[0].create_mode, Some(CreateModeProto::CreateNew as i32));
}

#[tokio::test]
async fn create_options_default_maps_current_layout_defaults() {
    let gateway = Arc::new(MockGateway::default());
    let client = fs_client_with_gateway(test_config("root"), gateway.clone()).expect("client");

    assert_eq!(CreateOptions::default().create_mode, CreateMode::CreateNew);

    client
        .create("/created", CreateOptions::default())
        .await
        .expect("create writer");

    let calls = gateway.calls();
    assert_eq!(methods(&calls), vec!["create_file"]);
    assert_eq!(calls[0].create_layout, Some(default_layout()));
}

#[tokio::test]
async fn create_options_custom_layout_maps_to_create_request() {
    let gateway = Arc::new(MockGateway::default());
    let client = fs_client_with_gateway(test_config("root"), gateway.clone()).expect("client");

    client
        .create(
            "/created",
            CreateOptions::create()
                .with_block_format_id(types::BlockFormatId::CURRENT_FOR_NEW_FILE)
                .with_block_size(8 * 1024 * 1024)
                .with_chunk_size(1024 * 1024),
        )
        .await
        .expect("create writer");

    let calls = gateway.calls();
    assert_eq!(methods(&calls), vec!["create_file"]);
    assert_eq!(
        calls[0].create_layout,
        Some(RecordedLayout {
            block_size: 8 * 1024 * 1024,
            chunk_size: 1024 * 1024,
            replication: DEFAULT_REPLICATION,
            block_format_id: types::BlockFormatId::CURRENT_FOR_NEW_FILE.as_raw(),
        })
    );
}

#[tokio::test]
async fn create_rejects_missing_response_layout() {
    let gateway = Arc::new(MockGateway::with_create_response_layout(None));
    let client = fs_client_with_gateway(test_config("root"), gateway.clone()).expect("client");

    let err = client
        .create("/created", CreateOptions::create())
        .await
        .expect_err("missing create response layout must fail");

    assert!(
        matches!(&err, ClientError::Metadata(msg) if msg.contains("CreateFileResponseProto.layout missing")),
        "unexpected error: {err:?}"
    );
    assert_eq!(methods(&gateway.calls()), vec!["create_file"]);
}

#[tokio::test]
async fn create_rejects_invalid_response_layout() {
    let gateway = Arc::new(MockGateway::with_create_response_layout(Some(recorded_layout_values(
        0,
        DEFAULT_CHUNK_SIZE,
    ))));
    let client = fs_client_with_gateway(test_config("root"), gateway.clone()).expect("client");

    let err = client
        .create("/created", CreateOptions::create())
        .await
        .expect_err("invalid create response layout must fail");

    assert!(
        matches!(&err, ClientError::InvalidLayout(msg) if msg.contains("CreateFileResponseProto.layout invalid")),
        "unexpected error: {err:?}"
    );
    assert_eq!(methods(&gateway.calls()), vec!["create_file"]);
}

#[tokio::test]
async fn overwrite_returns_writer_and_maps_create_or_overwrite_mode() {
    let gateway = Arc::new(MockGateway::default());
    let client = fs_client_with_gateway(test_config("root"), gateway.clone()).expect("client");

    assert_eq!(CreateOptions::overwrite().create_mode, CreateMode::CreateOrOverwrite);

    let writer = client
        .create("/overwrite", CreateOptions::overwrite())
        .await
        .expect("overwrite writer");

    assert_eq!(writer.path(), "/overwrite");
    let calls = gateway.calls();
    assert_eq!(methods(&calls), vec!["create_file"]);
    assert_eq!(calls[0].create_mode, Some(CreateModeProto::CreateOrOverwrite as i32));
}

#[tokio::test]
async fn append_returns_writer_from_metadata_session() {
    let gateway = Arc::new(MockGateway::default());
    let client = fs_client_with_gateway(test_config("root"), gateway.clone()).expect("client");

    let writer = client.append("/append").await.expect("append writer");

    assert_eq!(writer.path(), "/append");
    assert_eq!(writer.cursor(), 10);
    let calls = gateway.calls();
    assert_eq!(methods(&calls), vec!["append_file"]);
    assert_eq!(calls[0].create_layout, None);
}

#[tokio::test]
async fn append_rejects_missing_response_layout() {
    let gateway = Arc::new(MockGateway::with_append_response_layout(None));
    let client = fs_client_with_gateway(test_config("root"), gateway.clone()).expect("client");

    let err = client
        .append("/append")
        .await
        .expect_err("missing append response layout must fail");

    assert!(
        matches!(&err, ClientError::Metadata(msg) if msg.contains("AppendFileResponseProto.layout missing")),
        "unexpected error: {err:?}"
    );
    assert_eq!(methods(&gateway.calls()), vec!["append_file"]);
}

#[tokio::test]
async fn append_rejects_invalid_response_layout() {
    let gateway = Arc::new(MockGateway::with_append_response_layout(Some(recorded_layout_values(
        8, 3,
    ))));
    let client = fs_client_with_gateway(test_config("root"), gateway.clone()).expect("client");

    let err = client
        .append("/append")
        .await
        .expect_err("invalid append response layout must fail");

    assert!(
        matches!(&err, ClientError::InvalidLayout(msg) if msg.contains("AppendFileResponseProto.layout invalid")),
        "unexpected error: {err:?}"
    );
    assert_eq!(methods(&gateway.calls()), vec!["append_file"]);
}

#[tokio::test]
async fn stat_list_delete_and_rename_use_metadata_gateway() {
    let gateway = Arc::new(MockGateway::default());
    let client = fs_client_with_gateway(test_config("root"), gateway.clone()).expect("client");

    let status = client.stat("/alpha").await.expect("stat");
    let listing = client.list("/alpha", ListOptions::default()).await.expect("list");
    client.delete("/alpha", false).await.expect("delete");
    client.rename("/alpha", "/beta").await.expect("rename");

    assert_eq!(status.path(), "/alpha");
    assert_eq!(status.attrs.size, 10);
    assert_eq!(listing.path(), "/alpha");
    assert!(listing.eof);
    assert_eq!(listing.entries.len(), 1);
    assert_eq!(listing.entries[0].name, "child");
    assert_eq!(listing.entries[0].kind, Some(crate::api::InodeKind::File));
    assert_eq!(listing.entries[0].attrs.as_ref().expect("entry attrs").size, 4);
    let list_requests = gateway.list_requests();
    assert_eq!(list_requests.len(), 1);
    assert!(!list_requests[0].recursive);
    assert!(list_requests[0].cursor.is_empty());
    assert_eq!(list_requests[0].limit, 0);
    assert_eq!(
        methods(&gateway.calls()),
        vec!["get_status", "list_status", "delete", "rename"]
    );
}

#[tokio::test]
async fn mkdirs_uses_metadata_gateway_and_recursive_flag() {
    let gateway = Arc::new(MockGateway::default());
    let client = fs_client_with_gateway(test_config("root"), gateway.clone()).expect("client");

    let dir = client.mkdirs("/alpha", false).await.expect("mkdirs");
    let nested = client.mkdirs("/alpha/beta", true).await.expect("recursive mkdirs");

    assert_eq!(dir.path(), "/alpha");
    assert_eq!(dir.attrs.size, 0);
    assert_eq!(nested.path(), "/alpha/beta");
    assert_eq!(nested.attrs.size, 0);
    let requests = gateway.create_directory_requests();
    assert_eq!(
        requests,
        vec![("/alpha".to_string(), false), ("/alpha/beta".to_string(), true)]
    );
    assert_eq!(methods(&gateway.calls()), vec!["create_directory", "create_directory"]);
}

#[tokio::test]
async fn list_options_map_to_metadata_request() {
    let gateway = Arc::new(MockGateway::default());
    let client = fs_client_with_gateway(test_config("root"), gateway.clone()).expect("client");

    client
        .list(
            "/alpha",
            ListOptions {
                recursive: true,
                cursor: Some(vec![1, 2, 3]),
                limit: Some(50),
            },
        )
        .await
        .expect("list");

    let list_requests = gateway.list_requests();
    assert_eq!(list_requests.len(), 1);
    assert!(list_requests[0].recursive);
    assert_eq!(list_requests[0].cursor, vec![1, 2, 3]);
    assert_eq!(list_requests[0].limit, 50);
}

#[tokio::test]
async fn reader_empty_ranges_do_not_use_worker_io() {
    let gateway = Arc::new(MockGateway::default());
    let worker = Arc::new(MockDataClient::default());
    let client = fs_client_with_data_plane(test_config("root"), gateway, data_plane(worker.clone())).expect("client");
    let reader = read_reader(&client, 10);

    assert!(reader.read_at(0, 0).await.expect("zero read").is_empty());
    assert!(reader.read_at(10, 8).await.expect("EOF read").is_empty());
    assert_eq!(worker.calls(), 0);
}

#[tokio::test]
async fn reader_reads_normal_range_through_planner_and_worker() {
    let gateway = Arc::new(MockGateway::with_layout(layout_response(
        "root",
        202,
        Some(3),
        16,
        vec![location(202, 0, 0, 16)],
    )));
    let worker = Arc::new(MockDataClient::from_file(b"abcdefghijklmnop"));
    let client = fs_client_with_data_plane(test_config("root"), gateway.clone(), data_plane(worker)).expect("client");
    let reader = read_reader(&client, 16);

    let bytes = reader.read_at(2, 5).await.expect("read succeeds");

    assert_eq!(bytes, Bytes::from_static(b"cdefg"));
    let read_layout = gateway
        .calls()
        .into_iter()
        .find(|call| call.method == "read_layout")
        .expect("read layout call");
    assert_eq!(read_layout.target_data_handle_id, Some(202));
    assert_eq!(read_layout.range, Some((2, 5)));
}

#[tokio::test]
async fn reader_read_all_reads_full_opened_file() {
    let gateway = Arc::new(MockGateway::with_layout(layout_response(
        "root",
        202,
        Some(3),
        10,
        vec![location(202, 0, 0, 10)],
    )));
    let worker = Arc::new(MockDataClient::from_file(b"abcdefghijklmnop"));
    let client = fs_client_with_data_plane(test_config("root"), gateway.clone(), data_plane(worker)).expect("client");

    let reader = client.open("/alpha").await.expect("open succeeds");
    let bytes = reader.read_all().await.expect("read all");

    assert_eq!(bytes, Bytes::from_static(b"abcdefghij"));
    assert_eq!(methods(&gateway.calls()), vec!["open_file", "read_layout"]);
}

#[tokio::test]
async fn reader_read_exact_at_rejects_short_eof_read() {
    let gateway = Arc::new(MockGateway::default());
    let client = fs_client_with_gateway(test_config("root"), gateway).expect("client");
    let reader = read_reader(&client, 10);

    let err = reader
        .read_exact_at(10, 4)
        .await
        .expect_err("short EOF read must fail exact read");

    assert!(matches!(err, ClientError::InvalidArgument(msg)
        if msg.contains("read_exact_at") && msg.contains("requested 4 bytes")));
}

#[tokio::test]
async fn reader_rejects_worker_block_stamp_mismatch() {
    let gateway = Arc::new(MockGateway::with_layout(layout_response(
        "root",
        202,
        Some(3),
        16,
        vec![location(202, 0, 0, 16)],
    )));
    let worker = Arc::new(MockDataClient::with_read_block_stamp(b"abcdefghijklmnop", 99));
    let client = fs_client_with_data_plane(test_config("root"), gateway, data_plane(worker)).expect("client");
    let reader = read_reader(&client, 16);

    let err = reader
        .read_at(2, 5)
        .await
        .expect_err("worker block_stamp mismatch must fail");

    assert!(matches!(&err, ClientError::InvalidResponse { operation, reason }
        if *operation == "OpenReadStream" && reason.contains("block_stamp")));
}

#[tokio::test]
async fn reader_rejects_worker_committed_length_that_does_not_cover_range() {
    let gateway = Arc::new(MockGateway::with_layout(layout_response(
        "root",
        202,
        Some(3),
        16,
        vec![location(202, 0, 0, 16)],
    )));
    let worker = Arc::new(MockDataClient::with_read_committed_length(b"abcdefghijklmnop", 6));
    let client = fs_client_with_data_plane(test_config("root"), gateway, data_plane(worker)).expect("client");
    let reader = read_reader(&client, 16);

    let err = reader
        .read_at(2, 5)
        .await
        .expect_err("short worker committed_length must fail");

    assert!(matches!(&err, ClientError::InvalidResponse { operation, reason }
        if *operation == "OpenReadStream" && reason.contains("committed_length")));
}

#[tokio::test]
async fn reader_repeated_reads_fetch_current_metadata_locations() {
    let gateway = Arc::new(MockGateway::with_layout(layout_response(
        "root",
        202,
        Some(3),
        16,
        vec![location(202, 0, 0, 16)],
    )));
    let worker = Arc::new(MockDataClient::from_file(b"abcdefghijklmnop"));
    let client = fs_client_with_data_plane(test_config("root"), gateway.clone(), data_plane(worker)).expect("client");
    let reader = read_reader(&client, 16);

    let first = reader.read_at(2, 5).await.expect("first read succeeds");
    let second = reader.read_at(2, 5).await.expect("second read succeeds");

    assert_eq!(first, Bytes::from_static(b"cdefg"));
    assert_eq!(second, Bytes::from_static(b"cdefg"));
    assert_eq!(method_count(&gateway.calls(), "read_layout"), 2);
}

#[tokio::test]
async fn reader_replans_after_worker_refresh() {
    let gateway = Arc::new(MockGateway::with_layout(layout_response(
        "root",
        202,
        Some(3),
        16,
        vec![location(202, 0, 0, 16)],
    )));
    let worker = Arc::new(MockDataClient::with_refresh_once(
        b"abcdefghijklmnop",
        RefreshReason::WorkerRunMismatch,
    ));
    let client = fs_client_with_data_plane(test_config("root"), gateway.clone(), data_plane(worker)).expect("client");
    let reader = read_reader(&client, 16);

    let bytes = reader.read_at(1, 3).await.expect("read succeeds after refresh");

    assert_eq!(bytes, Bytes::from_static(b"bcd"));
    assert_eq!(method_count(&gateway.calls(), "read_layout"), 2);
}

#[tokio::test]
async fn writer_debug_redacts_write_session_identity_names() {
    let gateway = Arc::new(MockGateway::default());
    let client = fs_client_with_gateway(test_config("root"), gateway).expect("client");
    let writer = client
        .create("/created", CreateOptions::create())
        .await
        .expect("writer");
    let debug = format!("{writer:?}");

    assert!(debug.contains("cursor"));
    for needle in [
        concat!("inode", "_id"),
        concat!("data", "_handle_id"),
        concat!("file", "_version"),
        concat!("write", "_handle"),
        "fencing",
        concat!("route", "_epoch"),
        concat!("worker", "_run_id"),
        concat!("block", "_stamp"),
        concat!("call", "_id"),
        concat!("stream", "_id"),
    ] {
        assert!(
            !debug.contains(needle),
            "FileWriter Debug output leaked {needle}: {debug}"
        );
    }
}

#[tokio::test]
async fn writer_write_all_and_close_commit_final_size() {
    let gateway = Arc::new(MockGateway::default());
    let worker = Arc::new(MockDataClient::default());
    let client =
        fs_client_with_data_plane(test_config("root"), gateway.clone(), data_plane(worker.clone())).expect("client");
    let mut writer = client
        .create("/created", CreateOptions::create())
        .await
        .expect("writer");

    writer.write_all(Bytes::from_static(b"hello")).await.expect("write");
    writer.close().await.expect("close");

    assert_eq!(worker.written_bytes(), Bytes::from_static(b"hello"));
    let commit = gateway
        .calls()
        .into_iter()
        .find(|call| call.method == "commit_file")
        .expect("commit_file call");
    assert_eq!(commit.final_size, Some(5));
    assert_eq!(commit.committed_block_lens, vec![5]);
}

#[tokio::test]
async fn writer_create_uses_metadata_layout_block_size_for_chunking() {
    let layout = recorded_layout_values(8, 4);
    let gateway = Arc::new(MockGateway::default());
    let worker = Arc::new(MockDataClient::default());
    let client =
        fs_client_with_data_plane(test_config("root"), gateway.clone(), data_plane(worker.clone())).expect("client");
    let mut writer = client
        .create(
            "/created",
            CreateOptions::create()
                .with_block_size(layout.block_size)
                .with_chunk_size(layout.chunk_size),
        )
        .await
        .expect("writer");

    writer
        .write_all(Bytes::from(vec![b'x'; 20]))
        .await
        .expect("write should split by metadata layout");
    writer.close().await.expect("close");

    let calls = gateway.calls();
    let add_lens = add_block_lens(&calls);
    assert_eq!(add_lens, vec![8, 8, 4]);
    assert_eq!(worker.write_lens(), vec![8, 8, 4]);
    let commit = calls
        .into_iter()
        .find(|call| call.method == "commit_file")
        .expect("commit_file call");
    assert_eq!(commit.final_size, Some(20));
    assert_eq!(commit.committed_block_lens, vec![8, 8, 4]);
}

#[tokio::test]
async fn writer_uses_metadata_confirmed_layout_over_create_request_layout() {
    let requested = recorded_layout_values(64, 8);
    let confirmed = recorded_layout_values(8, 4);
    let gateway = Arc::new(MockGateway::with_create_response_layout(Some(confirmed)));
    let worker = Arc::new(MockDataClient::default());
    let client =
        fs_client_with_data_plane(test_config("root"), gateway.clone(), data_plane(worker.clone())).expect("client");
    let mut writer = client
        .create(
            "/created",
            CreateOptions::create()
                .with_block_size(requested.block_size)
                .with_chunk_size(requested.chunk_size),
        )
        .await
        .expect("writer");

    writer
        .write_all(Bytes::from(vec![b'x'; 20]))
        .await
        .expect("write should use confirmed metadata layout");
    writer.close().await.expect("close");

    let calls = gateway.calls();
    assert_eq!(calls[0].create_layout, Some(requested));
    assert_eq!(add_block_lens(&calls), vec![8, 8, 4]);
    assert_eq!(worker.write_lens(), vec![8, 8, 4]);
}

#[tokio::test]
async fn writer_append_uses_metadata_layout_block_size_for_chunking() {
    let layout = recorded_layout_values(6, 3);
    let gateway = Arc::new(MockGateway::with_append_write_layout(layout));
    let worker = Arc::new(MockDataClient::default());
    let client =
        fs_client_with_data_plane(test_config("root"), gateway.clone(), data_plane(worker.clone())).expect("client");
    let mut writer = client.append("/append").await.expect("writer");

    writer
        .write_all(Bytes::from(vec![b'x'; 14]))
        .await
        .expect("append write should split by metadata layout");
    writer.close().await.expect("close");

    let calls = gateway.calls();
    let append = calls
        .iter()
        .find(|call| call.method == "append_file")
        .expect("append_file call");
    assert_eq!(append.create_layout, None);
    assert_eq!(add_block_lens(&calls), vec![6, 6, 2]);
    assert_eq!(worker.write_lens(), vec![6, 6, 2]);
    let commit = calls
        .into_iter()
        .find(|call| call.method == "commit_file")
        .expect("commit_file call");
    assert_eq!(commit.final_size, Some(24));
    assert_eq!(commit.committed_block_offsets, vec![10, 16, 22]);
    assert_eq!(commit.committed_block_lens, vec![6, 6, 2]);
}

#[tokio::test]
async fn writer_multiple_small_writes_coalesce_until_close_flushes_pending_bytes() {
    let layout = recorded_layout_values(8, 4);
    let gateway = Arc::new(MockGateway::with_create_response_layout(Some(layout)));
    let worker = Arc::new(MockDataClient::default());
    let client =
        fs_client_with_data_plane(test_config("root"), gateway.clone(), data_plane(worker.clone())).expect("client");
    let mut writer = client
        .create("/created", CreateOptions::create())
        .await
        .expect("writer");

    writer.write_all(Bytes::from_static(b"hel")).await.expect("first write");
    writer.write_all(Bytes::from_static(b"lo")).await.expect("second write");

    assert_eq!(writer.cursor(), 5);
    assert_eq!(add_block_lens(&gateway.calls()), Vec::<u64>::new());
    assert_eq!(worker.write_lens(), Vec::<u64>::new());

    writer.close().await.expect("close");

    assert_eq!(writer.cursor(), 5);
    assert_eq!(worker.written_bytes(), Bytes::from_static(b"hello"));
    let commit = gateway
        .calls()
        .into_iter()
        .find(|call| call.method == "commit_file")
        .expect("commit_file call");
    assert_eq!(commit.final_size, Some(5));
    assert_eq!(commit.committed_block_offsets, vec![0]);
    assert_eq!(commit.committed_block_lens, vec![5]);
}

#[tokio::test]
async fn writer_write_crossing_block_boundary_emits_full_blocks_and_buffers_tail() {
    let layout = recorded_layout_values(8, 4);
    let gateway = Arc::new(MockGateway::with_create_response_layout(Some(layout)));
    let worker = Arc::new(MockDataClient::default());
    let client =
        fs_client_with_data_plane(test_config("root"), gateway.clone(), data_plane(worker.clone())).expect("client");
    let mut writer = client
        .create("/created", CreateOptions::create())
        .await
        .expect("writer");

    writer
        .write_all(Bytes::from(vec![b'x'; 20]))
        .await
        .expect("write should flush only complete blocks");

    assert_eq!(writer.cursor(), 20);
    assert_eq!(add_block_lens(&gateway.calls()), vec![8, 8]);
    assert_eq!(worker.write_lens(), vec![8, 8]);

    writer.close().await.expect("close");

    let calls = gateway.calls();
    assert_eq!(add_block_lens(&calls), vec![8, 8, 4]);
    let commit = calls
        .into_iter()
        .find(|call| call.method == "commit_file")
        .expect("commit_file call");
    assert_eq!(commit.final_size, Some(20));
    assert_eq!(commit.committed_block_lens, vec![8, 8, 4]);
}

#[tokio::test]
async fn writer_sync_publishes_prefix_and_keeps_writer_usable() {
    for (sync_mode, expected_sync_flags) in [
        (WriteSyncModeProto::WriteSyncModeVisibility, vec![false, false]),
        (WriteSyncModeProto::WriteSyncModeDurability, vec![true, false]),
    ] {
        let layout = recorded_layout_values(8, 4);
        let gateway = Arc::new(MockGateway::with_create_response_layout(Some(layout)));
        let worker = Arc::new(MockDataClient::default());
        let client = fs_client_with_data_plane(test_config("root"), gateway.clone(), data_plane(worker.clone()))
            .expect("client");
        let mut writer = client
            .create("/created", CreateOptions::create())
            .await
            .expect("writer");

        writer.write_all(Bytes::from_static(b"hello")).await.expect("write");

        assert_eq!(add_block_lens(&gateway.calls()), Vec::<u64>::new());
        assert_eq!(worker.write_lens(), Vec::<u64>::new());

        match sync_mode {
            WriteSyncModeProto::WriteSyncModeVisibility => {
                writer.sync_write_visibility().await.expect("visibility sync")
            }
            WriteSyncModeProto::WriteSyncModeDurability => {
                writer.sync_write_durability().await.expect("durability sync")
            }
            WriteSyncModeProto::WriteSyncModeUnspecified => unreachable!("sync test cases use explicit modes"),
        }
        writer
            .write_all(Bytes::from_static(b"!"))
            .await
            .expect("writer remains usable");
        writer.close().await.expect("close");

        let sync_call = gateway
            .calls()
            .into_iter()
            .find(|call| call.method == "sync_write")
            .expect("sync_write call");
        assert_eq!(sync_call.sync_mode, Some(sync_mode as i32));
        assert_eq!(sync_call.target_size, Some(5));
        assert_eq!(sync_call.committed_block_lens, vec![5]);
        assert_eq!(worker.commit_sync_flags(), expected_sync_flags);
    }
}

#[tokio::test]
async fn writer_durability_after_visibility_uses_sync_committed_block() {
    let gateway = Arc::new(MockGateway::default());
    let worker = Arc::new(MockDataClient::default());
    let client =
        fs_client_with_data_plane(test_config("root"), gateway.clone(), data_plane(worker.clone())).expect("client");
    let mut writer = client
        .create("/created", CreateOptions::create())
        .await
        .expect("writer");

    writer.write_all(Bytes::from_static(b"hello")).await.expect("write");
    writer.sync_write_visibility().await.expect("visibility sync");
    writer.sync_write_durability().await.expect("durability sync");

    let sync_modes = gateway
        .calls()
        .into_iter()
        .filter(|call| call.method == "sync_write")
        .map(|call| call.sync_mode)
        .collect::<Vec<_>>();
    assert_eq!(
        sync_modes,
        vec![
            Some(WriteSyncModeProto::WriteSyncModeVisibility as i32),
            Some(WriteSyncModeProto::WriteSyncModeDurability as i32),
        ]
    );
    assert_eq!(worker.commit_sync_flags(), vec![false]);
    assert_eq!(worker.block_sync_lens(), vec![5]);
}

#[tokio::test]
async fn writer_barrier_flush_worker_error_blocks_later_write_and_close() {
    let layout = recorded_layout_values(8, 4);
    let gateway = Arc::new(MockGateway::with_create_response_layout(Some(layout)));
    let worker = Arc::new(MockDataClient {
        write_stream_outcomes: Mutex::new(vec![WorkerWriteOutcome::WorkerError].into()),
        ..MockDataClient::default()
    });
    let client = fs_client_with_data_plane(test_config("root"), gateway.clone(), data_plane(worker)).expect("client");
    let mut writer = client
        .create("/created", CreateOptions::create())
        .await
        .expect("writer");

    writer.write_all(Bytes::from_static(b"hello")).await.expect("write");
    let err = writer
        .sync_write_visibility()
        .await
        .expect_err("flush failure must fail barrier");
    assert!(matches!(err, ClientError::Worker(msg) if msg.contains("injected WriteStream failure")));

    let err = writer
        .write_all(Bytes::from_static(b"!"))
        .await
        .expect_err("unsafe flush failure blocks writes");
    assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("invalid")));
    let err = writer
        .sync_write_durability()
        .await
        .expect_err("unsafe flush failure blocks durability sync");
    assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("invalid")));
    let err = writer.close().await.expect_err("unsafe flush failure blocks close");
    assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("invalid")));
    assert_eq!(method_count(&gateway.calls(), "commit_file"), 0);
}

#[tokio::test]
async fn writer_close_time_flush_worker_error_blocks_retry_commit() {
    let layout = recorded_layout_values(8, 4);
    let gateway = Arc::new(MockGateway::with_create_response_layout(Some(layout)));
    let worker = Arc::new(MockDataClient {
        write_stream_outcomes: Mutex::new(vec![WorkerWriteOutcome::WorkerError].into()),
        ..MockDataClient::default()
    });
    let client = fs_client_with_data_plane(test_config("root"), gateway.clone(), data_plane(worker)).expect("client");
    let mut writer = client
        .create("/created", CreateOptions::create())
        .await
        .expect("writer");

    writer.write_all(Bytes::from_static(b"hello")).await.expect("write");
    let err = writer.close().await.expect_err("close-time flush must fail");
    assert!(matches!(err, ClientError::Worker(msg) if msg.contains("injected WriteStream failure")));

    let err = writer
        .close()
        .await
        .expect_err("failed close-time flush must not become commit retry");
    assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("invalid")));
    assert_eq!(method_count(&gateway.calls(), "commit_file"), 0);
}

#[tokio::test]
async fn writer_renew_lease_keeps_writer_usable() {
    let gateway = Arc::new(MockGateway::default());
    let client = fs_client_with_gateway(test_config("root"), gateway.clone()).expect("client");
    let handle = write_handle_for_tests("/created", 0, u64::MAX / 4).expect("write handle");
    let mut writer = FileWriter::new(Arc::clone(&client.runtime), handle);

    writer.renew_lease().await.expect("renew lease");
    writer.abort().await.expect("renewed writer can still abort");

    assert_eq!(methods(&gateway.calls()), vec!["renew_lease", "abort_file_write"]);
}

#[tokio::test]
async fn writer_renew_lease_rejects_zero_expiry_without_updating_session() {
    let gateway = Arc::new(MockGateway::with_renew_outcomes(vec![RenewOutcome::ZeroExpiry]));
    let client = fs_client_with_gateway(test_config("root"), gateway.clone()).expect("client");
    let handle = write_handle_for_tests("/created", 0, u64::MAX / 4).expect("write handle");
    let mut writer = FileWriter::new(Arc::clone(&client.runtime), handle);

    let err = writer.renew_lease().await.expect_err("zero renew expiry must fail");
    assert!(matches!(&err, ClientError::InvalidResponse { operation, reason }
        if *operation == "RenewLease" && reason.contains("expires_at_ms")));

    writer
        .abort()
        .await
        .expect("zero expiry response must not poison session");
    assert_eq!(methods(&gateway.calls()), vec!["renew_lease", "abort_file_write"]);
}

#[tokio::test]
async fn writer_auto_renews_near_expiry_before_write() {
    let gateway = Arc::new(MockGateway::default());
    let worker = Arc::new(MockDataClient::default());
    let mut config = test_config("root");
    config.write_lease.auto_renew = true;
    config.write_lease.renew_before_expiry_ms = 120_000;
    let client = fs_client_with_data_plane(config, gateway.clone(), data_plane(worker)).expect("client");
    let handle = write_handle_for_tests("/created", 0, unix_now_ms() + 60_000).expect("write handle");
    let mut writer = FileWriter::new(Arc::clone(&client.runtime), handle);

    writer.write_all(Bytes::from_static(b"hello")).await.expect("write");

    assert_eq!(methods(&gateway.calls()), vec!["renew_lease"]);
}

#[tokio::test]
async fn writer_close_rejects_commit_size_shorter_than_final_size() {
    let gateway = Arc::new(MockGateway::with_commit_response_size(4));
    let worker = Arc::new(MockDataClient::default());
    let client = fs_client_with_data_plane(test_config("root"), gateway.clone(), data_plane(worker)).expect("client");
    let mut writer = client
        .create("/created", CreateOptions::create())
        .await
        .expect("writer");

    writer.write_all(Bytes::from_static(b"hello")).await.expect("write");
    let err = writer.close().await.expect_err("short committed_size must fail");

    assert!(matches!(&err, ClientError::InvalidResponse { operation, reason }
        if *operation == "CommitFile" && reason.contains("committed_size")));
}

#[tokio::test]
async fn writer_visibility_sync_rejects_synced_size_shorter_than_target() {
    let gateway = Arc::new(MockGateway::with_sync_response_size(4));
    let worker = Arc::new(MockDataClient::default());
    let client = fs_client_with_data_plane(test_config("root"), gateway.clone(), data_plane(worker)).expect("client");
    let mut writer = client
        .create("/created", CreateOptions::create())
        .await
        .expect("writer");

    writer.write_all(Bytes::from_static(b"hello")).await.expect("write");
    let err = writer
        .sync_write_visibility()
        .await
        .expect_err("short synced_size must fail");

    assert!(matches!(&err, ClientError::InvalidResponse { operation, reason }
        if *operation == "SyncWrite" && reason.contains("synced_size")));
}

#[tokio::test]
async fn writer_abort_cleans_worker_then_metadata_and_blocks_session() {
    let events = event_log();
    let layout = recorded_layout_values(5, 5);
    let gateway = Arc::new(MockGateway {
        create_response_layout: Mutex::new(Some(Some(layout))),
        events: Some(events.clone()),
        ..MockGateway::default()
    });
    let worker = Arc::new(MockDataClient::with_events(events.clone()));
    let client =
        fs_client_with_data_plane(test_config("root"), gateway.clone(), data_plane(worker.clone())).expect("client");
    let mut writer = client
        .create("/created", CreateOptions::create())
        .await
        .expect("writer");

    writer.write_all(Bytes::from_static(b"hello")).await.expect("write");
    writer.abort().await.expect("abort");

    assert_eq!(method_count(&gateway.calls(), "abort_file_write"), 1);
    assert_event_order(&events, "abort_block_write", "abort_file_write");
    let err = writer
        .write_all(Bytes::from_static(b"!"))
        .await
        .expect_err("aborted writer blocks writes");
    assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("aborted")));
}

#[tokio::test]
async fn writer_unknown_add_block_blocks_followup_writes() {
    let layout = recorded_layout_values(5, 5);
    let gateway = Arc::new(MockGateway {
        create_response_layout: Mutex::new(Some(Some(layout))),
        add_block_outcomes: Mutex::new(vec![AddBlockOutcome::TransportUnknown].into()),
        ..MockGateway::default()
    });
    let worker = Arc::new(MockDataClient::default());
    let client = fs_client_with_data_plane(test_config_with_retries("root", 0), gateway.clone(), data_plane(worker))
        .expect("client");
    let mut writer = client
        .create("/created", CreateOptions::create())
        .await
        .expect("writer");

    let err = writer
        .write_all(Bytes::from_static(b"hello"))
        .await
        .expect_err("AddBlock unknown outcome");
    assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("AddBlock")));

    let err = writer
        .write_all(Bytes::from_static(b"!"))
        .await
        .expect_err("unknown outcome blocks writes");
    assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("unknown outcome")));
    assert_eq!(method_count(&gateway.calls(), "add_block"), 1);
}

#[tokio::test]
async fn writer_session_expiry_blocks_followup_writes() {
    let gateway = Arc::new(MockGateway::with_renew_outcomes(vec![RenewOutcome::SessionExpired]));
    let worker = Arc::new(MockDataClient::default());
    let client = fs_client_with_data_plane(test_config("root"), gateway, data_plane(worker)).expect("client");
    let mut writer = client
        .create("/created", CreateOptions::create())
        .await
        .expect("writer");

    let err = writer.renew_lease().await.expect_err("session expired");
    assert_eq!(ErrorClassifier.classify_error(&err), ErrorClass::SessionExpired);
    let err = writer
        .write_all(Bytes::from_static(b"x"))
        .await
        .expect_err("expired session blocks writes");
    assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("session")));
}

fn event_log() -> EventLog {
    Arc::new(Mutex::new(Vec::new()))
}

fn methods(calls: &[RecordedCall]) -> Vec<&'static str> {
    calls.iter().map(|call| call.method).collect()
}

fn assert_debug_redacts_internal_identity_names(debug: &str) {
    for needle in [
        concat!("inode", "_id"),
        concat!("data", "_handle_id"),
        concat!("file", "_version"),
        concat!("write", "_handle"),
        concat!("fen", "cing"),
        concat!("route", "_epoch"),
        concat!("worker", "_run_id"),
        concat!("block", "_stamp"),
        concat!("call", "_id"),
        concat!("stream", "_id"),
    ] {
        assert!(
            !debug.contains(needle),
            "reader or writer Debug output must redact {needle}: {debug}"
        );
    }
}

fn method_count(calls: &[RecordedCall], method: &str) -> usize {
    calls.iter().filter(|call| call.method == method).count()
}

fn group_name_from(raw: &str) -> GroupName {
    GroupName::parse(raw).unwrap()
}

fn assert_event_order(events: &EventLog, before: &'static str, after: &'static str) {
    let events = events.lock().expect("events");
    let before_index = events
        .iter()
        .position(|event| *event == before)
        .unwrap_or_else(|| panic!("missing event {before}: {events:?}"));
    let after_index = events
        .iter()
        .position(|event| *event == after)
        .unwrap_or_else(|| panic!("missing event {after}: {events:?}"));
    assert!(
        before_index < after_index,
        "{before} must happen before {after}: {events:?}"
    );
}

fn test_config(group_name: &str) -> ClientConfig {
    ClientConfig {
        metadata_groups: vec![metadata_group_config(group_name)],
        ..ClientConfig::default()
    }
}

fn metadata_group_config(group_name: &str) -> MetadataGroupConfig {
    MetadataGroupConfig {
        group_name: group_name_from(group_name),
        endpoints: vec!["http://127.0.0.1:18080".to_string()],
    }
}

fn test_config_with_retries(group_name: &str, max_retry_attempts: usize) -> ClientConfig {
    let mut config = test_config(group_name);
    config.retry.max_retry_attempts = max_retry_attempts;
    config.refresh.max_refresh_attempts = max_retry_attempts;
    config
}

fn fs_client_with_gateway(config: ClientConfig, gateway: Arc<dyn MetadataGateway>) -> ClientResult<FsClient> {
    let metrics: Arc<dyn crate::metrics::ClientMetrics> = Arc::new(crate::metrics::NoopClientMetrics);
    let data_plane = WorkerDataPlane::from_config(&config, metrics);
    fs_client_with_data_plane(config, gateway, data_plane)
}

fn fs_client_with_data_plane(
    config: ClientConfig,
    gateway: Arc<dyn MetadataGateway>,
    data_plane: WorkerDataPlane,
) -> ClientResult<FsClient> {
    let metadata_targets = MetadataTargets::from_config(&config)?;
    FsClient::with_runtime_hooks(
        config,
        gateway,
        metadata_targets,
        data_plane,
        Arc::new(crate::runtime::TokioBackoffSleeper),
        Arc::new(crate::metrics::NoopClientMetrics),
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RecordedCall {
    method: &'static str,
    group_name: GroupName,
    call_id: String,
    target_data_handle_id: Option<u64>,
    range: Option<(u64, u32)>,
    target_size: Option<u64>,
    final_size: Option<u64>,
    committed_block_offsets: Vec<u64>,
    committed_block_lens: Vec<u64>,
    sync_mode: Option<i32>,
    create_mode: Option<i32>,
    create_layout: Option<RecordedLayout>,
    add_block_desired_len: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RecordedLayout {
    block_size: u32,
    chunk_size: u32,
    replication: u32,
    block_format_id: u32,
}

fn default_layout() -> RecordedLayout {
    recorded_layout_values(DEFAULT_BLOCK_SIZE, DEFAULT_CHUNK_SIZE)
}

fn recorded_layout_values(block_size: u32, chunk_size: u32) -> RecordedLayout {
    RecordedLayout {
        block_size,
        chunk_size,
        replication: DEFAULT_REPLICATION,
        block_format_id: types::BlockFormatId::CURRENT_FOR_NEW_FILE.as_raw(),
    }
}

fn recorded_layout(layout: &proto::common::FileLayoutProto) -> RecordedLayout {
    RecordedLayout {
        block_size: layout.block_size,
        chunk_size: layout.chunk_size,
        replication: layout.replication,
        block_format_id: layout.block_format_id,
    }
}

fn layout_proto(layout: RecordedLayout) -> proto::common::FileLayoutProto {
    proto::common::FileLayoutProto {
        block_size: layout.block_size,
        chunk_size: layout.chunk_size,
        replication: layout.replication,
        block_format_id: layout.block_format_id,
    }
}

fn add_block_lens(calls: &[RecordedCall]) -> Vec<u64> {
    calls
        .iter()
        .filter(|call| call.method == "add_block")
        .map(|call| call.add_block_desired_len.expect("add_block desired_len"))
        .collect()
}

fn unix_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after Unix epoch")
        .as_millis() as u64
}

#[derive(Debug, Default)]
struct MockGateway {
    calls: Mutex<Vec<RecordedCall>>,
    layouts: Mutex<VecDeque<ReadLayout>>,
    list_requests: Mutex<Vec<proto::metadata::ListStatusRequestProto>>,
    create_directory_requests: Mutex<Vec<(String, bool)>>,
    next_offsets: Mutex<HashMap<u64, u64>>,
    next_block_indexes: Mutex<HashMap<u64, u32>>,
    write_layouts: Mutex<HashMap<u64, RecordedLayout>>,
    append_write_layout: Mutex<Option<RecordedLayout>>,
    create_response_layout: Mutex<Option<Option<RecordedLayout>>>,
    append_response_layout: Mutex<Option<Option<RecordedLayout>>>,
    commit_response_size: Mutex<Option<u64>>,
    sync_response_size: Mutex<Option<u64>>,
    add_block_outcomes: Mutex<VecDeque<AddBlockOutcome>>,
    renew_outcomes: Mutex<VecDeque<RenewOutcome>>,
    events: Option<EventLog>,
}

impl MockGateway {
    fn calls(&self) -> Vec<RecordedCall> {
        self.calls.lock().expect("calls").clone()
    }

    fn list_requests(&self) -> Vec<proto::metadata::ListStatusRequestProto> {
        self.list_requests.lock().expect("list requests").clone()
    }

    fn create_directory_requests(&self) -> Vec<(String, bool)> {
        self.create_directory_requests
            .lock()
            .expect("create directory requests")
            .clone()
    }

    fn with_layout(layout: ReadLayout) -> Self {
        let mut layouts = VecDeque::new();
        layouts.push_back(layout);
        Self {
            layouts: Mutex::new(layouts),
            ..Self::default()
        }
    }

    fn with_create_response_layout(layout: Option<RecordedLayout>) -> Self {
        Self {
            create_response_layout: Mutex::new(Some(layout)),
            ..Self::default()
        }
    }

    fn with_append_write_layout(layout: RecordedLayout) -> Self {
        Self {
            append_write_layout: Mutex::new(Some(layout)),
            ..Self::default()
        }
    }

    fn with_append_response_layout(layout: Option<RecordedLayout>) -> Self {
        Self {
            append_response_layout: Mutex::new(Some(layout)),
            ..Self::default()
        }
    }

    fn with_commit_response_size(committed_size: u64) -> Self {
        Self {
            commit_response_size: Mutex::new(Some(committed_size)),
            ..Self::default()
        }
    }

    fn with_sync_response_size(synced_size: u64) -> Self {
        Self {
            sync_response_size: Mutex::new(Some(synced_size)),
            ..Self::default()
        }
    }

    fn with_renew_outcomes(outcomes: Vec<RenewOutcome>) -> Self {
        Self {
            renew_outcomes: Mutex::new(outcomes.into()),
            ..Self::default()
        }
    }

    fn next_add_block_outcome(&self) -> AddBlockOutcome {
        self.add_block_outcomes
            .lock()
            .expect("add block outcomes")
            .pop_front()
            .unwrap_or(AddBlockOutcome::Ok)
    }

    fn next_renew_outcome(&self) -> RenewOutcome {
        self.renew_outcomes
            .lock()
            .expect("renew outcomes")
            .pop_front()
            .unwrap_or(RenewOutcome::Ok)
    }

    fn record(&self, method: &'static str, ctx: &AttemptContext) {
        let header = ctx.metadata_header().expect("metadata header");
        self.calls.lock().expect("calls").push(RecordedCall {
            method,
            group_name: group_name_from(&header.group_name),
            call_id: header.client.as_ref().expect("client").call_id.clone(),
            target_data_handle_id: None,
            range: None,
            target_size: None,
            final_size: None,
            committed_block_offsets: Vec::new(),
            committed_block_lens: Vec::new(),
            sync_mode: None,
            create_mode: None,
            create_layout: None,
            add_block_desired_len: None,
        });
    }

    fn record_create_file(&self, ctx: &AttemptContext, req: &proto::metadata::CreateFileRequestProto) {
        let header = ctx.metadata_header().expect("metadata header");
        self.calls.lock().expect("calls").push(RecordedCall {
            method: "create_file",
            group_name: group_name_from(&header.group_name),
            call_id: header.client.as_ref().expect("client").call_id.clone(),
            target_data_handle_id: None,
            range: None,
            target_size: None,
            final_size: None,
            committed_block_offsets: Vec::new(),
            committed_block_lens: Vec::new(),
            sync_mode: None,
            create_mode: Some(req.create_mode),
            create_layout: req.layout.as_ref().map(recorded_layout),
            add_block_desired_len: None,
        });
    }

    fn record_read_layout(&self, ctx: &AttemptContext, req: &proto::metadata::GetBlockLocationsRequestProto) {
        let header = ctx.metadata_header().expect("metadata header");
        let target_data_handle_id = match req.target.as_ref() {
            Some(proto::metadata::get_block_locations_request_proto::Target::DataHandleId(id)) => Some(id.value),
            _ => None,
        };
        let range = req.range.as_ref().map(|range| (range.offset, range.len));
        self.calls.lock().expect("calls").push(RecordedCall {
            method: "read_layout",
            group_name: group_name_from(&header.group_name),
            call_id: header.client.as_ref().expect("client").call_id.clone(),
            target_data_handle_id,
            range,
            target_size: None,
            final_size: None,
            committed_block_offsets: Vec::new(),
            committed_block_lens: Vec::new(),
            sync_mode: None,
            create_mode: None,
            create_layout: None,
            add_block_desired_len: None,
        });
    }

    fn record_commit_file(&self, ctx: &AttemptContext, req: &proto::metadata::CommitFileRequestProto) {
        self.record_event("commit_file");
        let header = ctx.metadata_header().expect("metadata header");
        self.calls.lock().expect("calls").push(RecordedCall {
            method: "commit_file",
            group_name: group_name_from(&header.group_name),
            call_id: header.client.as_ref().expect("client").call_id.clone(),
            target_data_handle_id: req.data_handle_id.as_ref().map(|id| id.value),
            range: None,
            target_size: None,
            final_size: Some(req.final_size),
            committed_block_offsets: req.committed_blocks.iter().map(|block| block.file_offset).collect(),
            committed_block_lens: req.committed_blocks.iter().map(|block| block.len).collect(),
            sync_mode: None,
            create_mode: None,
            create_layout: None,
            add_block_desired_len: None,
        });
    }

    fn record_sync_write(&self, ctx: &AttemptContext, req: &proto::metadata::SyncWriteRequestProto) {
        let header = ctx.metadata_header().expect("metadata header");
        self.calls.lock().expect("calls").push(RecordedCall {
            method: "sync_write",
            group_name: group_name_from(&header.group_name),
            call_id: header.client.as_ref().expect("client").call_id.clone(),
            target_data_handle_id: req.data_handle_id.as_ref().map(|id| id.value),
            range: None,
            target_size: Some(req.target_size),
            final_size: None,
            committed_block_offsets: req.committed_blocks.iter().map(|block| block.file_offset).collect(),
            committed_block_lens: req.committed_blocks.iter().map(|block| block.len).collect(),
            sync_mode: Some(req.mode),
            create_mode: None,
            create_layout: None,
            add_block_desired_len: None,
        });
    }

    fn record_add_block(&self, ctx: &AttemptContext, req: &proto::metadata::AddBlockRequestProto) {
        let header = ctx.metadata_header().expect("metadata header");
        self.calls.lock().expect("calls").push(RecordedCall {
            method: "add_block",
            group_name: group_name_from(&header.group_name),
            call_id: header.client.as_ref().expect("client").call_id.clone(),
            target_data_handle_id: None,
            range: None,
            target_size: None,
            final_size: None,
            committed_block_offsets: Vec::new(),
            committed_block_lens: Vec::new(),
            sync_mode: None,
            create_mode: None,
            create_layout: None,
            add_block_desired_len: req.desired_len,
        });
    }

    fn record_event(&self, event: &'static str) {
        if let Some(events) = &self.events {
            events.lock().expect("events").push(event);
        }
    }
}

#[async_trait]
impl MetadataGateway for MockGateway {
    async fn get_status(
        &self,
        ctx: AttemptContext,
        _req: proto::metadata::GetStatusRequestProto,
    ) -> ClientResult<GetStatusResponseProto> {
        self.record("get_status", &ctx);
        Ok(GetStatusResponseProto {
            attrs: Some(file_attrs_proto(10)),
            ..GetStatusResponseProto::default()
        })
    }

    async fn list_status(
        &self,
        ctx: AttemptContext,
        req: proto::metadata::ListStatusRequestProto,
    ) -> ClientResult<ListStatusResponseProto> {
        self.record("list_status", &ctx);
        self.list_requests.lock().expect("list requests").push(req);
        Ok(ListStatusResponseProto {
            entries: vec![proto::fs::DirEntryProto {
                name: "child".to_string(),
                kind: proto::fs::InodeKindProto::InodeKindFile as i32,
                attrs: Some(file_attrs_proto(4)),
            }],
            eof: true,
            ..ListStatusResponseProto::default()
        })
    }

    async fn create_directory(
        &self,
        ctx: AttemptContext,
        req: proto::metadata::CreateDirectoryRequestProto,
    ) -> ClientResult<CreateDirectoryResponseProto> {
        self.record("create_directory", &ctx);
        self.create_directory_requests
            .lock()
            .expect("create directory requests")
            .push((req.path, req.recursive));
        Ok(CreateDirectoryResponseProto {
            attrs: Some(file_attrs_proto(0)),
            ..CreateDirectoryResponseProto::default()
        })
    }

    async fn delete(
        &self,
        ctx: AttemptContext,
        _req: proto::metadata::DeleteRequestProto,
    ) -> ClientResult<DeleteResponseProto> {
        self.record("delete", &ctx);
        Ok(DeleteResponseProto::default())
    }

    async fn rename(
        &self,
        ctx: AttemptContext,
        _req: proto::metadata::RenameRequestProto,
    ) -> ClientResult<RenameResponseProto> {
        self.record("rename", &ctx);
        Ok(RenameResponseProto::default())
    }

    async fn open_file(
        &self,
        ctx: AttemptContext,
        _req: proto::metadata::OpenFileRequestProto,
    ) -> ClientResult<OpenFileResponseProto> {
        self.record("open_file", &ctx);
        Ok(OpenFileResponseProto {
            data_handle_id: Some(proto::common::DataHandleIdProto { value: 202 }),
            file_size: 10,
            file_version: Some(3),
            ..OpenFileResponseProto::default()
        })
    }

    async fn read_layout(
        &self,
        ctx: AttemptContext,
        req: proto::metadata::GetBlockLocationsRequestProto,
    ) -> ClientResult<ReadLayout> {
        self.record_read_layout(&ctx, &req);
        let layouts = self.layouts.lock().expect("layouts");
        Ok(layouts
            .front()
            .cloned()
            .unwrap_or_else(|| layout_response("root", 202, Some(3), 10, Vec::new())))
    }

    async fn create_file(
        &self,
        ctx: AttemptContext,
        req: proto::metadata::CreateFileRequestProto,
    ) -> ClientResult<CreateFileResponseProto> {
        self.record_create_file(&ctx, &req);
        self.next_offsets.lock().expect("offsets").insert(1, 0);
        let layout = req.layout.as_ref().map(recorded_layout).unwrap_or_else(default_layout);
        self.write_layouts.lock().expect("write layouts").insert(1, layout);
        let response_layout = self
            .create_response_layout
            .lock()
            .expect("create response layout")
            .unwrap_or(Some(layout));
        Ok(CreateFileResponseProto {
            write_handle: Some(write_handle_proto(1, 302)),
            data_handle_id: Some(proto::common::DataHandleIdProto { value: 302 }),
            base_size: 0,
            expires_at_ms: u64::MAX / 2,
            layout: response_layout.map(layout_proto),
            ..CreateFileResponseProto::default()
        })
    }

    async fn append_file(
        &self,
        ctx: AttemptContext,
        _req: proto::metadata::AppendFileRequestProto,
    ) -> ClientResult<AppendFileResponseProto> {
        self.record("append_file", &ctx);
        self.next_offsets.lock().expect("offsets").insert(2, 10);
        let layout = self
            .append_write_layout
            .lock()
            .expect("append write layout")
            .unwrap_or_else(default_layout);
        self.write_layouts.lock().expect("write layouts").insert(2, layout);
        let response_layout = self
            .append_response_layout
            .lock()
            .expect("append response layout")
            .unwrap_or(Some(layout));
        Ok(AppendFileResponseProto {
            write_handle: Some(write_handle_proto(2, 402)),
            data_handle_id: Some(proto::common::DataHandleIdProto { value: 402 }),
            base_size: 10,
            expires_at_ms: u64::MAX / 2,
            layout: response_layout.map(layout_proto),
            ..AppendFileResponseProto::default()
        })
    }

    async fn add_block(
        &self,
        ctx: AttemptContext,
        req: proto::metadata::AddBlockRequestProto,
    ) -> ClientResult<AddBlockResult> {
        self.record_add_block(&ctx, &req);
        if matches!(self.next_add_block_outcome(), AddBlockOutcome::TransportUnknown) {
            return Err(ClientError::from(tonic::Status::unavailable(
                "injected AddBlock transport uncertainty",
            )));
        }
        let write_handle = req.write_handle.as_ref().expect("write handle");
        let requested_len = req.desired_len.expect("desired len");
        let layout = self
            .write_layouts
            .lock()
            .expect("write layouts")
            .get(&write_handle.handle_id)
            .copied()
            .unwrap_or_else(default_layout);
        let len = requested_len.min(u64::from(layout.block_size)).max(1);
        let offset = {
            let mut offsets = self.next_offsets.lock().expect("offsets");
            let offset = *offsets.entry(write_handle.handle_id).or_insert(0);
            offsets.insert(write_handle.handle_id, offset + len);
            offset
        };
        let block_index = {
            let mut indexes = self.next_block_indexes.lock().expect("block indexes");
            let index = *indexes.entry(write_handle.handle_id).or_insert(0);
            indexes.insert(write_handle.handle_id, index + 1);
            index
        };
        let data_handle_id = write_handle
            .fencing_token
            .as_ref()
            .and_then(|token| token.block_id.as_ref())
            .map(|block| block.data_handle_id)
            .expect("fencing block");
        let header = ctx.metadata_header().expect("metadata header");
        Ok(AddBlockResult {
            group_name: group_name_from(&header.group_name),
            target: write_target_with_layout(data_handle_id, block_index, offset, len, layout),
        })
    }

    async fn commit_file(
        &self,
        ctx: AttemptContext,
        req: proto::metadata::CommitFileRequestProto,
    ) -> ClientResult<proto::metadata::CommitFileResponseProto> {
        self.record_commit_file(&ctx, &req);
        let committed_size = self
            .commit_response_size
            .lock()
            .expect("commit response size")
            .unwrap_or(req.final_size);
        Ok(CommitFileResponseProto {
            committed_size,
            file_version: Some(1),
            ..CommitFileResponseProto::default()
        })
    }

    async fn abort_file_write(
        &self,
        ctx: AttemptContext,
        _req: proto::metadata::AbortFileWriteRequestProto,
    ) -> ClientResult<proto::metadata::AbortFileWriteResponseProto> {
        self.record_event("abort_file_write");
        self.record("abort_file_write", &ctx);
        Ok(AbortFileWriteResponseProto::default())
    }

    async fn renew_lease(
        &self,
        ctx: AttemptContext,
        _req: proto::metadata::RenewLeaseRequestProto,
    ) -> ClientResult<proto::metadata::RenewLeaseResponseProto> {
        self.record("renew_lease", &ctx);
        match self.next_renew_outcome() {
            RenewOutcome::Ok => Ok(RenewLeaseResponseProto {
                expires_at_ms: u64::MAX / 2,
                ..RenewLeaseResponseProto::default()
            }),
            RenewOutcome::SessionExpired => Err(session_error(RefreshReason::SessionExpired)),
            RenewOutcome::ZeroExpiry => Ok(RenewLeaseResponseProto {
                expires_at_ms: 0,
                ..RenewLeaseResponseProto::default()
            }),
        }
    }

    async fn sync_write(
        &self,
        ctx: AttemptContext,
        req: proto::metadata::SyncWriteRequestProto,
    ) -> ClientResult<SyncWriteResponseProto> {
        self.record_sync_write(&ctx, &req);
        let synced_size = self
            .sync_response_size
            .lock()
            .expect("sync response size")
            .unwrap_or(req.target_size);
        Ok(SyncWriteResponseProto {
            synced_size,
            file_version: Some(1),
            ..SyncWriteResponseProto::default()
        })
    }

    async fn msync(
        &self,
        ctx: AttemptContext,
        _req: proto::metadata::MsyncRequestProto,
    ) -> ClientResult<proto::common::GroupStateWatermarkProto> {
        self.record("msync", &ctx);
        Ok(proto::common::GroupStateWatermarkProto::default())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AddBlockOutcome {
    Ok,
    TransportUnknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RenewOutcome {
    Ok,
    SessionExpired,
    ZeroExpiry,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WorkerWriteOutcome {
    Ok,
    WorkerError,
}

#[derive(Debug)]
struct MockDataClient {
    file: Bytes,
    refresh_once: Mutex<Option<RefreshReason>>,
    calls: Mutex<usize>,
    written: Mutex<Vec<u8>>,
    written_lens: Mutex<Vec<u64>>,
    committed: Mutex<Vec<u64>>,
    committed_streams: Mutex<std::collections::HashSet<(u64, u64)>>,
    write_stream_outcomes: Mutex<VecDeque<WorkerWriteOutcome>>,
    commit_sync_flags: Mutex<Vec<bool>>,
    block_syncs: Mutex<Vec<u64>>,
    read_block_stamp: Mutex<Option<u64>>,
    read_committed_length: Mutex<Option<u64>>,
    record_written_body: bool,
    events: Option<EventLog>,
}

impl MockDataClient {
    fn from_file(file: &'static [u8]) -> Self {
        Self {
            file: Bytes::from_static(file),
            refresh_once: Mutex::new(None),
            calls: Mutex::new(0),
            written: Mutex::new(Vec::new()),
            written_lens: Mutex::new(Vec::new()),
            committed: Mutex::new(Vec::new()),
            committed_streams: Mutex::new(std::collections::HashSet::new()),
            write_stream_outcomes: Mutex::new(VecDeque::new()),
            commit_sync_flags: Mutex::new(Vec::new()),
            block_syncs: Mutex::new(Vec::new()),
            read_block_stamp: Mutex::new(None),
            read_committed_length: Mutex::new(None),
            record_written_body: true,
            events: None,
        }
    }

    fn with_events(events: EventLog) -> Self {
        Self {
            events: Some(events),
            ..Self::default()
        }
    }

    fn with_refresh_once(file: &'static [u8], reason: RefreshReason) -> Self {
        Self {
            refresh_once: Mutex::new(Some(reason)),
            ..Self::from_file(file)
        }
    }

    fn with_read_block_stamp(file: &'static [u8], block_stamp: u64) -> Self {
        Self {
            read_block_stamp: Mutex::new(Some(block_stamp)),
            ..Self::from_file(file)
        }
    }

    fn with_read_committed_length(file: &'static [u8], committed_length: u64) -> Self {
        Self {
            read_committed_length: Mutex::new(Some(committed_length)),
            ..Self::from_file(file)
        }
    }

    fn calls(&self) -> usize {
        *self.calls.lock().expect("calls")
    }

    fn written_bytes(&self) -> Bytes {
        Bytes::from(self.written.lock().expect("written").clone())
    }

    fn write_lens(&self) -> Vec<u64> {
        self.written_lens.lock().expect("written lens").clone()
    }

    fn commit_sync_flags(&self) -> Vec<bool> {
        self.commit_sync_flags.lock().expect("commit sync flags").clone()
    }

    fn block_sync_lens(&self) -> Vec<u64> {
        self.block_syncs.lock().expect("block syncs").clone()
    }

    fn record_event(&self, event: &'static str) {
        if let Some(events) = &self.events {
            events.lock().expect("events").push(event);
        }
    }

    fn next_write_stream_outcome(&self) -> WorkerWriteOutcome {
        self.write_stream_outcomes
            .lock()
            .expect("write stream outcomes")
            .pop_front()
            .unwrap_or(WorkerWriteOutcome::Ok)
    }
}

impl Default for MockDataClient {
    fn default() -> Self {
        Self::from_file(b"")
    }
}

#[async_trait]
impl WorkerDataClient for MockDataClient {
    async fn read_block_range(
        &self,
        _ctx: AttemptContext,
        _group_name: GroupName,
        block_read: &PlannedBlockRead,
    ) -> ClientResult<WorkerReadResult> {
        let call_number = {
            let mut calls = self.calls.lock().expect("calls");
            *calls += 1;
            *calls
        };
        if call_number == 1 {
            if let Some(reason) = self.refresh_once.lock().expect("refresh").take() {
                return Err(refresh_action_error(reason));
            }
        }
        let start = block_read.file_offset as usize;
        let end = start + block_read.len as usize;
        Ok(WorkerReadResult {
            bytes: self.file.slice(start..end),
            block_stamp: self
                .read_block_stamp
                .lock()
                .expect("read block stamp")
                .unwrap_or(block_read.block_stamp),
            committed_length: self
                .read_committed_length
                .lock()
                .expect("read committed length")
                .unwrap_or(block_read.block_offset + u64::from(block_read.len)),
        })
    }

    async fn open_block_write(
        &self,
        _ctx: AttemptContext,
        target: WorkerWriteTarget,
    ) -> ClientResult<WorkerBlockWriteHandle> {
        self.record_event("open_block_write");
        let call_number = {
            let mut calls = self.calls.lock().expect("calls");
            *calls += 1;
            *calls
        };
        Ok(WorkerBlockWriteHandle {
            group_name: target.group_name,
            worker: worker_endpoint(),
            target: target.target,
            stream_id: proto::common::StreamIdProto {
                high: 1,
                low: call_number as u64,
            },
            frame_size: 1024,
            next_seq: 1,
        })
    }

    async fn write_block_bytes(
        &self,
        _ctx: AttemptContext,
        block: &WorkerBlockWriteHandle,
        data: Bytes,
    ) -> ClientResult<proto::worker::WriteStreamResponseProto> {
        self.record_event("write_block_bytes");
        if matches!(self.next_write_stream_outcome(), WorkerWriteOutcome::WorkerError) {
            return Err(ClientError::Worker("injected WriteStream failure".to_string()));
        }
        self.written_lens.lock().expect("written lens").push(data.len() as u64);
        if self.record_written_body {
            self.written.lock().expect("written").extend_from_slice(&data);
        }
        let frame_size = block.frame_size.max(1) as usize;
        let frame_count = data.len().div_ceil(frame_size) as u64;
        Ok(proto::worker::WriteStreamResponseProto {
            accepted: true,
            last_acked_seq: block.next_seq + frame_count.saturating_sub(1),
            written_through: data.len() as u64,
        })
    }

    async fn commit_block_write(
        &self,
        _ctx: AttemptContext,
        block: &WorkerBlockWriteHandle,
        effective_len: u64,
        _commit_seq: u64,
        require_sync: bool,
    ) -> ClientResult<WorkerCommitResult> {
        self.record_event("commit_block_write");
        let stream_key = (block.stream_id.high, block.stream_id.low);
        if !self
            .committed_streams
            .lock()
            .expect("committed streams")
            .insert(stream_key)
        {
            return Err(ClientError::Worker(format!(
                "CommitWrite stream already committed and removed: high={} low={}",
                block.stream_id.high, block.stream_id.low
            )));
        }
        self.committed.lock().expect("committed").push(effective_len);
        self.commit_sync_flags
            .lock()
            .expect("commit sync flags")
            .push(require_sync);
        Ok(WorkerCommitResult {
            effective_len,
            block_stamp: block.target.block_stamp,
            written_through: effective_len,
        })
    }

    async fn sync_committed_block(
        &self,
        _ctx: AttemptContext,
        block: &WorkerBlockWriteHandle,
        expected_len: u64,
    ) -> ClientResult<WorkerBlockSyncResult> {
        self.record_event("sync_committed_block");
        self.block_syncs.lock().expect("block syncs").push(expected_len);
        Ok(WorkerBlockSyncResult {
            effective_len: expected_len,
            block_stamp: block.target.block_stamp,
        })
    }

    async fn abort_block_write(&self, _ctx: AttemptContext, _block: &WorkerBlockWriteHandle) -> ClientResult<()> {
        self.record_event("abort_block_write");
        Ok(())
    }
}

fn data_plane(client: Arc<MockDataClient>) -> WorkerDataPlane {
    WorkerDataPlane::with_client(client)
}

fn read_reader(client: &FsClient, file_size: u64) -> FileReader {
    FileReader::new(
        Arc::clone(&client.runtime),
        ReadHandle::new("/alpha".to_string(), DataHandleId::new(202), 3, file_size),
    )
}

fn layout_response(
    group_name: &str,
    data_handle_id: u64,
    file_version: Option<u64>,
    file_size: u64,
    locations: Vec<FileBlockLocation>,
) -> ReadLayout {
    ReadLayout {
        group_name: group_name_from(group_name),
        data_handle_id: DataHandleId::new(data_handle_id),
        file_size,
        file_version,
        locations,
    }
}

fn file_attrs_proto(size: u64) -> proto::fs::FileAttrsProto {
    proto::fs::FileAttrsProto {
        mode: 0o100644,
        uid: 1000,
        gid: 1000,
        size,
        atime_ms: 11,
        mtime_ms: 12,
        ctime_ms: 13,
        nlink: 1,
    }
}

fn write_handle_proto(handle_id: u64, data_handle_id: u64) -> WriteHandleProto {
    WriteHandleProto {
        handle_id,
        lease_id: Some(proto::common::LeaseIdProto {
            high: 0,
            low: handle_id,
        }),
        lease_epoch: 1,
        open_epoch: 1,
        fencing_token: Some(FencingTokenProto {
            block_id: Some(BlockIdProto {
                data_handle_id,
                block_index: 0,
            }),
            owner: Some(types::ClientId::new(7).into()),
            epoch: 1,
        }),
    }
}

fn write_handle_for_tests(path: &str, base_size: u64, expires_at_ms: u64) -> ClientResult<WriteHandle> {
    let data_handle_id = DataHandleId::new(302);
    let layout = types::FileLayout::try_from(layout_proto(default_layout()))
        .map_err(|err| ClientError::InvalidLayout(err.to_string()))?;
    let session = WriteSession::new(
        path.to_string(),
        data_handle_id,
        layout,
        write_handle_proto(1, data_handle_id.as_raw()),
        base_size,
        expires_at_ms,
    )?;
    Ok(WriteHandle::new(path.to_string(), data_handle_id, base_size, session))
}

fn write_target_with_layout(
    data_handle_id: u64,
    block_index: u32,
    file_offset: u64,
    len: u64,
    layout: RecordedLayout,
) -> WriteTarget {
    let block_id = BlockId::new(DataHandleId::new(data_handle_id), BlockIndex::new(block_index));
    WriteTarget {
        block_id,
        file_offset,
        block_size: u64::from(layout.block_size),
        effective_len: len,
        worker_endpoints: vec![worker_endpoint()],
        fencing_token: FencingToken::new(block_id, ClientId::new(7), 1),
        block_stamp: 1,
        chunk_size: layout.chunk_size,
        block_format_id: types::BlockFormatId::from_raw(layout.block_format_id).expect("known test block format"),
        tier: types::Tier::Hdd,
    }
}

fn worker_endpoint() -> WorkerEndpointInfo {
    WorkerEndpointInfo {
        worker_id: WorkerId::new(1),
        endpoint: "127.0.0.1:19101".to_string(),
        worker_net_protocol: WorkerNetProtocol::Grpc,
        worker_run_id: "550e8400-e29b-41d4-a716-446655440000"
            .parse()
            .expect("valid test WorkerRunId"),
    }
}

fn location(data_handle_id: u64, block_index: u32, file_offset: u64, len: u64) -> FileBlockLocation {
    FileBlockLocation {
        block_id: BlockId::new(DataHandleId::new(data_handle_id), BlockIndex::new(block_index)),
        file_offset,
        len,
        workers: vec![worker_endpoint()],
        block_stamp: u64::from(block_index) + 1,
        block_format_id: types::BlockFormatId::CURRENT_FOR_NEW_FILE,
        block_size: DEFAULT_BLOCK_SIZE as u64,
        chunk_size: DEFAULT_CHUNK_SIZE,
        effective_len: len,
    }
}

fn refresh_action_error(reason: RefreshReason) -> ClientError {
    let code = match reason {
        RefreshReason::WorkerRunMismatch => RpcErrorCode::WorkerRunMismatch,
        other => panic!("unsupported test refresh reason {other:?}"),
    };
    let canonical = CanonicalError::need_refresh_with_hint(
        code,
        reason,
        CanonicalRefreshHint {
            worker_resolve_required: true,
            ..CanonicalRefreshHint::default()
        },
        "worker requested refresh",
    );
    ClientError::from(ClientAction::Refresh {
        reason,
        hint: Box::new(RefreshHint {
            worker_resolve_required: true,
            ..RefreshHint::default()
        }),
        canonical: Box::new(canonical),
    })
}

fn session_error(reason: RefreshReason) -> ClientError {
    let canonical = CanonicalError::need_refresh(RpcErrorCode::Application, reason, "write session expired");
    ClientError::from(ClientAction::Refresh {
        reason,
        hint: Box::default(),
        canonical: Box::new(canonical),
    })
}
