// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Focused unit tests for the public API facade and client runtime behavior.

use super::fs_client::{DEFAULT_BLOCK_SIZE, DEFAULT_CHUNK_SIZE, DEFAULT_REPLICATION};
use super::handle::ReadHandle;
use super::*;
use crate::canonical::{ClientAction, RefreshHint};
use crate::config::ClientConfig;
use crate::data::{
    DataPlaneBoundary, WorkerBlockSyncResult, WorkerCommitResult, WorkerDataClient, WorkerWriteBlock, WorkerWriteTarget,
};
use crate::error::{ClientError, ClientResult};
use crate::metadata::{
    AbortFileWriteOp, AbortFileWriteResult, AddBlockOp, AddBlockResult, AppendFileOp, CommitFileOp, CommitFileResult,
    CreateFileOp, DeleteOp, GetBlockLocationsOp, GetStatusOp, LayoutSnapshot, ListStatusOp, MetadataGateway, MsyncOp,
    OpenFileOp, RenameOp, RenewLeaseOp, RenewLeaseResult, WriteSessionSeed,
};
use crate::planner::read_planner::PlannedReadSegment;
use crate::runtime::{AttemptContext, ErrorClass, ErrorClassifier};
use async_trait::async_trait;
use bytes::Bytes;
use common::error::canonical::{CanonicalError, RefreshHint as CanonicalRefreshHint, RefreshReason};
use common::header::RpcErrorCode;
use proto::common::{BlockIdProto, FencingTokenProto};
use proto::metadata::{
    AbortFileWriteResponseProto, AppendFileResponseProto, CommitFileResponseProto, CreateDispositionProto,
    CreateFileResponseProto, DeleteResponseProto, GetStatusResponseProto, ListStatusResponseProto,
    OpenFileResponseProto, RenameResponseProto, RenewLeaseResponseProto, SyncWriteResponseProto, WriteHandleProto,
    WriteSyncModeProto,
};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use types::lease::FencingToken;
use types::{
    BlockId, BlockIndex, ClientId, DataHandleId, FileBlockLocation, InodeId, WorkerEndpointInfo, WorkerId,
    WorkerNetProtocol, WriteTarget,
};

type EventLog = Arc<Mutex<Vec<&'static str>>>;

#[tokio::test]
async fn open_returns_reader_from_metadata_snapshot() {
    let gateway = Arc::new(MockGateway::default());
    let client = FsClient::with_metadata_gateway(test_config(9), gateway.clone()).expect("client");

    let reader = client
        .open("/alpha", OpenOptions::default())
        .await
        .expect("open succeeds");

    assert_eq!(reader.path(), "/alpha");
    assert_eq!(reader.size_hint(), 10);
    assert_eq!(reader.inode_id(), InodeId::new(101));
    assert_eq!(reader.data_handle_id(), DataHandleId::new(202));
    assert_eq!(methods(&gateway.calls()), vec!["open_file"]);
}

#[tokio::test]
async fn create_returns_writer_and_maps_create_disposition() {
    let gateway = Arc::new(MockGateway::default());
    let client = FsClient::with_metadata_gateway(test_config(9), gateway.clone()).expect("client");

    let writer = client
        .create("/created", CreateOptions::create())
        .await
        .expect("create writer");

    assert_eq!(writer.path(), "/created");
    let calls = gateway.calls();
    assert_eq!(methods(&calls), vec!["create_file"]);
    assert_eq!(
        calls[0].create_disposition,
        Some(CreateDispositionProto::CreateNew as i32)
    );
}

#[tokio::test]
async fn create_options_default_maps_current_layout_defaults() {
    let gateway = Arc::new(MockGateway::default());
    let client = FsClient::with_metadata_gateway(test_config(9), gateway.clone()).expect("client");

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
    let client = FsClient::with_metadata_gateway(test_config(9), gateway.clone()).expect("client");

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
    let client = FsClient::with_metadata_gateway(test_config(9), gateway.clone()).expect("client");

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
    let client = FsClient::with_metadata_gateway(test_config(9), gateway.clone()).expect("client");

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
async fn overwrite_returns_writer_and_maps_overwrite_disposition() {
    let gateway = Arc::new(MockGateway::default());
    let client = FsClient::with_metadata_gateway(test_config(9), gateway.clone()).expect("client");

    let writer = client
        .create("/overwrite", CreateOptions::overwrite())
        .await
        .expect("overwrite writer");

    assert_eq!(writer.path(), "/overwrite");
    let calls = gateway.calls();
    assert_eq!(methods(&calls), vec!["create_file"]);
    assert_eq!(
        calls[0].create_disposition,
        Some(CreateDispositionProto::Overwrite as i32)
    );
}

#[tokio::test]
async fn overwrite_preserves_custom_create_layout_mapping() {
    let gateway = Arc::new(MockGateway::default());
    let client = FsClient::with_metadata_gateway(test_config(9), gateway.clone()).expect("client");

    client
        .create(
            "/overwrite",
            CreateOptions::overwrite()
                .with_block_format_id(types::BlockFormatId::CURRENT_FOR_NEW_FILE)
                .with_block_size(16 * 1024 * 1024)
                .with_chunk_size(2 * 1024 * 1024),
        )
        .await
        .expect("overwrite writer");

    let calls = gateway.calls();
    assert_eq!(methods(&calls), vec!["create_file"]);
    assert_eq!(
        calls[0].create_disposition,
        Some(CreateDispositionProto::Overwrite as i32)
    );
    assert_eq!(
        calls[0].create_layout,
        Some(RecordedLayout {
            block_size: 16 * 1024 * 1024,
            chunk_size: 2 * 1024 * 1024,
            replication: DEFAULT_REPLICATION,
            block_format_id: types::BlockFormatId::CURRENT_FOR_NEW_FILE.as_raw(),
        })
    );
}

#[tokio::test]
async fn append_returns_writer_from_metadata_session() {
    let gateway = Arc::new(MockGateway::default());
    let client = FsClient::with_metadata_gateway(test_config(9), gateway.clone()).expect("client");

    let writer = client
        .append("/append", AppendOptions::default())
        .await
        .expect("append writer");

    assert_eq!(writer.path(), "/append");
    assert_eq!(writer.cursor(), 10);
    let calls = gateway.calls();
    assert_eq!(methods(&calls), vec!["append_file"]);
    assert_eq!(calls[0].create_layout, None);
}

#[tokio::test]
async fn append_rejects_missing_response_layout() {
    let gateway = Arc::new(MockGateway::with_append_response_layout(None));
    let client = FsClient::with_metadata_gateway(test_config(9), gateway.clone()).expect("client");

    let err = client
        .append("/append", AppendOptions::default())
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
    let client = FsClient::with_metadata_gateway(test_config(9), gateway.clone()).expect("client");

    let err = client
        .append("/append", AppendOptions::default())
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
    let client = FsClient::with_metadata_gateway(test_config(9), gateway.clone()).expect("client");

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
async fn list_options_map_to_metadata_request() {
    let gateway = Arc::new(MockGateway::default());
    let client = FsClient::with_metadata_gateway(test_config(9), gateway.clone()).expect("client");

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
    let client = FsClient::with_data_boundary(test_config(9), gateway, data_boundary(worker.clone())).expect("client");
    let reader = read_reader(&client, 10);

    assert!(reader.read_at(0, 0).await.expect("zero read").is_empty());
    assert!(reader.read_at(10, 8).await.expect("EOF read").is_empty());
    assert_eq!(worker.calls(), 0);
}

#[tokio::test]
async fn reader_reads_normal_range_through_planner_and_worker() {
    let gateway = Arc::new(MockGateway::with_layout(layout_response(
        9,
        101,
        202,
        Some(3),
        16,
        vec![location(202, 0, 0, 16)],
    )));
    let worker = Arc::new(MockDataClient::from_file(b"abcdefghijklmnop"));
    let client = FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker)).expect("client");
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
async fn reader_repeated_reads_fetch_current_metadata_locations() {
    let gateway = Arc::new(MockGateway::with_layout(layout_response(
        9,
        101,
        202,
        Some(3),
        16,
        vec![location(202, 0, 0, 16)],
    )));
    let worker = Arc::new(MockDataClient::from_file(b"abcdefghijklmnop"));
    let client = FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker)).expect("client");
    let reader = read_reader(&client, 16);

    let first = reader.read_at(2, 5).await.expect("first read succeeds");
    let second = reader.read_at(2, 5).await.expect("second read succeeds");

    assert_eq!(first, Bytes::from_static(b"cdefg"));
    assert_eq!(second, Bytes::from_static(b"cdefg"));
    assert_eq!(method_count(&gateway.calls(), "read_layout"), 2);
}

#[tokio::test]
async fn concurrent_reader_reads_fetch_layout_per_call() {
    let gateway = Arc::new(MockGateway::with_layout(layout_response(
        9,
        101,
        202,
        Some(3),
        16,
        vec![location(202, 0, 0, 16)],
    )));
    let worker = Arc::new(MockDataClient::from_file(b"abcdefghijklmnop"));
    let client = FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker)).expect("client");
    let reader = read_reader(&client, 16);

    let first = {
        let reader = reader.clone();
        tokio::spawn(async move { reader.read_at(2, 5).await })
    };
    let second = tokio::spawn(async move { reader.read_at(2, 5).await });

    assert_eq!(
        first.await.expect("first task").expect("first read"),
        Bytes::from_static(b"cdefg")
    );
    assert_eq!(
        second.await.expect("second task").expect("second read"),
        Bytes::from_static(b"cdefg")
    );
    assert_eq!(method_count(&gateway.calls(), "read_layout"), 2);
}

#[tokio::test]
async fn reader_replans_after_worker_refresh() {
    let gateway = Arc::new(MockGateway::with_layout(layout_response(
        9,
        101,
        202,
        Some(3),
        16,
        vec![location(202, 0, 0, 16)],
    )));
    let worker = Arc::new(MockDataClient::with_refresh_once(
        b"abcdefghijklmnop",
        RefreshReason::WorkerRunMismatch,
    ));
    let client = FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker)).expect("client");
    let reader = read_reader(&client, 16);

    let bytes = reader.read_at(1, 3).await.expect("read succeeds after refresh");

    assert_eq!(bytes, Bytes::from_static(b"bcd"));
    assert_eq!(method_count(&gateway.calls(), "read_layout"), 2);
}

#[tokio::test]
async fn writer_debug_redacts_write_session_identity_names() {
    let gateway = Arc::new(MockGateway::default());
    let client = FsClient::with_metadata_gateway(test_config(9), gateway).expect("client");
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
        FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker.clone())).expect("client");
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
        FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker.clone())).expect("client");
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
async fn writer_append_uses_metadata_layout_block_size_for_chunking() {
    let layout = recorded_layout_values(6, 3);
    let gateway = Arc::new(MockGateway::with_append_write_layout(layout));
    let worker = Arc::new(MockDataClient::default());
    let client =
        FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker.clone())).expect("client");
    let mut writer = client
        .append("/append", AppendOptions::default())
        .await
        .expect("writer");

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
async fn writer_default_create_layout_uses_default_block_size_for_chunking() {
    let gateway = Arc::new(MockGateway::default());
    let worker = Arc::new(MockDataClient::without_recorded_body());
    let client =
        FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker.clone())).expect("client");
    let mut writer = client
        .create("/created", CreateOptions::default())
        .await
        .expect("writer");
    let len = DEFAULT_BLOCK_SIZE as usize + 3;

    writer
        .write_all(Bytes::from(vec![b'x'; len]))
        .await
        .expect("write should split by default metadata layout");
    writer.close().await.expect("close");

    let calls = gateway.calls();
    assert_eq!(add_block_lens(&calls), vec![u64::from(DEFAULT_BLOCK_SIZE), 3]);
    assert_eq!(worker.write_lens(), vec![u64::from(DEFAULT_BLOCK_SIZE), 3]);
    let commit = calls
        .into_iter()
        .find(|call| call.method == "commit_file")
        .expect("commit_file call");
    assert_eq!(commit.final_size, Some(len as u64));
    assert_eq!(commit.committed_block_lens, vec![u64::from(DEFAULT_BLOCK_SIZE), 3]);
}

#[tokio::test]
async fn writer_multiple_sequential_writes_preserve_cursor_and_final_size() {
    let gateway = Arc::new(MockGateway::default());
    let worker = Arc::new(MockDataClient::default());
    let client =
        FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker.clone())).expect("client");
    let mut writer = client
        .create("/created", CreateOptions::create())
        .await
        .expect("writer");

    writer.write_all(Bytes::from_static(b"hel")).await.expect("first write");
    writer.write_all(Bytes::from_static(b"lo")).await.expect("second write");
    writer.close().await.expect("close");

    assert_eq!(writer.cursor(), 5);
    assert_eq!(worker.written_bytes(), Bytes::from_static(b"hello"));
    let commit = gateway
        .calls()
        .into_iter()
        .find(|call| call.method == "commit_file")
        .expect("commit_file call");
    assert_eq!(commit.final_size, Some(5));
    assert_eq!(commit.committed_block_offsets, vec![0, 3]);
    assert_eq!(commit.committed_block_lens, vec![3, 2]);
}

#[tokio::test]
async fn writer_visibility_sync_publishes_prefix_and_keeps_writer_usable() {
    let gateway = Arc::new(MockGateway::default());
    let worker = Arc::new(MockDataClient::default());
    let client =
        FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker.clone())).expect("client");
    let mut writer = client
        .create("/created", CreateOptions::create())
        .await
        .expect("writer");

    writer.write_all(Bytes::from_static(b"hello")).await.expect("write");
    writer.sync_write_visibility().await.expect("visibility sync");
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
    assert_eq!(
        sync_call.sync_mode,
        Some(WriteSyncModeProto::WriteSyncModeVisibility as i32)
    );
    assert_eq!(sync_call.target_size, Some(5));
    assert_eq!(sync_call.committed_block_lens, vec![5]);
    assert_eq!(worker.commit_sync_flags(), vec![false, false]);
}

#[tokio::test]
async fn writer_durability_sync_publishes_prefix_and_keeps_writer_usable() {
    let gateway = Arc::new(MockGateway::default());
    let worker = Arc::new(MockDataClient::default());
    let client =
        FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker.clone())).expect("client");
    let mut writer = client
        .create("/created", CreateOptions::create())
        .await
        .expect("writer");

    writer.write_all(Bytes::from_static(b"hello")).await.expect("write");
    writer.sync_write_durability().await.expect("durability sync");
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
    assert_eq!(
        sync_call.sync_mode,
        Some(WriteSyncModeProto::WriteSyncModeDurability as i32)
    );
    assert_eq!(sync_call.target_size, Some(5));
    assert_eq!(sync_call.committed_block_lens, vec![5]);
    assert_eq!(worker.commit_sync_flags(), vec![true, false]);
}

#[tokio::test]
async fn writer_durability_after_visibility_uses_sync_committed_block() {
    let gateway = Arc::new(MockGateway::default());
    let worker = Arc::new(MockDataClient::default());
    let client =
        FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker.clone())).expect("client");
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
async fn writer_renew_lease_updates_session_state() {
    let gateway = Arc::new(MockGateway::default());
    let worker = Arc::new(MockDataClient::default());
    let client = FsClient::with_data_boundary(test_config(9), gateway, data_boundary(worker)).expect("client");
    let mut writer = client
        .create("/created", CreateOptions::create())
        .await
        .expect("writer");

    writer.renew_lease().await.expect("renew lease");

    let session_ref = writer.write_session().expect("write session");
    let session = session_ref.lock().await;
    assert_eq!(session.expires_at_ms(), Some(u64::MAX / 2));
}

#[tokio::test]
async fn writer_abort_cleans_worker_then_metadata_and_blocks_session() {
    let events = event_log();
    let gateway = Arc::new(MockGateway::with_events(events.clone()));
    let worker = Arc::new(MockDataClient::with_events(events.clone()));
    let client =
        FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker.clone())).expect("client");
    let mut writer = client
        .create("/created", CreateOptions::create())
        .await
        .expect("writer");

    writer.write_all(Bytes::from_static(b"hello")).await.expect("write");
    writer.abort().await.expect("abort");

    assert_eq!(method_count(&gateway.calls(), "abort_file_write"), 1);
    assert_event_order(&events, "abort_write", "abort_file_write");
    let err = writer
        .write_all(Bytes::from_static(b"!"))
        .await
        .expect_err("aborted writer blocks writes");
    assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("aborted")));
}

#[tokio::test]
async fn writer_unknown_add_block_blocks_followup_writes() {
    let gateway = Arc::new(MockGateway::with_add_block_outcomes(vec![
        AddBlockOutcome::TransportUnknown,
    ]));
    let worker = Arc::new(MockDataClient::default());
    let client = FsClient::with_data_boundary(test_config_with_retries(9, 0), gateway.clone(), data_boundary(worker))
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
    let client = FsClient::with_data_boundary(test_config(9), gateway, data_boundary(worker)).expect("client");
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

fn method_count(calls: &[RecordedCall], method: &str) -> usize {
    calls.iter().filter(|call| call.method == method).count()
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

fn test_config(group_id: u64) -> ClientConfig {
    let mut config = ClientConfig {
        metadata_endpoints: vec!["http://127.0.0.1:18080".to_string()],
        metadata_group_ids: vec![group_id],
        ..ClientConfig::default()
    };
    config.inner.inner.set("client.id", 7i64);
    config
}

fn test_config_with_retries(group_id: u64, max_retries: usize) -> ClientConfig {
    let mut config = test_config(group_id);
    config.retry.max_retries = max_retries;
    config.retry.max_retry_attempts = max_retries;
    config.retry.metadata_retry_budget = max_retries;
    config.retry.worker_retry_budget = max_retries;
    config.refresh.max_refresh_attempts = max_retries;
    config
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RecordedCall {
    method: &'static str,
    group_id: u64,
    call_id: String,
    target_data_handle_id: Option<u64>,
    range: Option<(u64, u32)>,
    target_size: Option<u64>,
    final_size: Option<u64>,
    committed_block_offsets: Vec<u64>,
    committed_block_lens: Vec<u64>,
    sync_mode: Option<i32>,
    create_disposition: Option<i32>,
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

#[derive(Debug, Default)]
struct MockGateway {
    calls: Mutex<Vec<RecordedCall>>,
    layouts: Mutex<VecDeque<LayoutSnapshot>>,
    list_requests: Mutex<Vec<ListStatusOp>>,
    next_offsets: Mutex<HashMap<u64, u64>>,
    next_block_indexes: Mutex<HashMap<u64, u32>>,
    write_layouts: Mutex<HashMap<u64, RecordedLayout>>,
    append_write_layout: Mutex<Option<RecordedLayout>>,
    create_response_layout: Mutex<Option<Option<RecordedLayout>>>,
    append_response_layout: Mutex<Option<Option<RecordedLayout>>>,
    add_block_outcomes: Mutex<VecDeque<AddBlockOutcome>>,
    renew_outcomes: Mutex<VecDeque<RenewOutcome>>,
    events: Option<EventLog>,
}

impl MockGateway {
    fn calls(&self) -> Vec<RecordedCall> {
        self.calls.lock().expect("calls").clone()
    }

    fn list_requests(&self) -> Vec<ListStatusOp> {
        self.list_requests.lock().expect("list requests").clone()
    }

    fn with_events(events: EventLog) -> Self {
        Self {
            events: Some(events),
            ..Self::default()
        }
    }

    fn with_layout(layout: LayoutSnapshot) -> Self {
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

    fn with_add_block_outcomes(outcomes: Vec<AddBlockOutcome>) -> Self {
        Self {
            add_block_outcomes: Mutex::new(outcomes.into()),
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
            group_id: header.group_id,
            call_id: header.client.as_ref().expect("client").call_id.clone(),
            target_data_handle_id: None,
            range: None,
            target_size: None,
            final_size: None,
            committed_block_offsets: Vec::new(),
            committed_block_lens: Vec::new(),
            sync_mode: None,
            create_disposition: None,
            create_layout: None,
            add_block_desired_len: None,
        });
    }

    fn record_create_file(&self, ctx: &AttemptContext, req: &CreateFileOp) {
        let header = ctx.metadata_header().expect("metadata header");
        self.calls.lock().expect("calls").push(RecordedCall {
            method: "create_file",
            group_id: header.group_id,
            call_id: header.client.as_ref().expect("client").call_id.clone(),
            target_data_handle_id: None,
            range: None,
            target_size: None,
            final_size: None,
            committed_block_offsets: Vec::new(),
            committed_block_lens: Vec::new(),
            sync_mode: None,
            create_disposition: Some(req.disposition),
            create_layout: req.layout.as_ref().map(recorded_layout),
            add_block_desired_len: None,
        });
    }

    fn record_read_layout(&self, ctx: &AttemptContext, req: &GetBlockLocationsOp) {
        let header = ctx.metadata_header().expect("metadata header");
        let target_data_handle_id = match req.target.as_ref() {
            Some(proto::metadata::get_block_locations_request_proto::Target::DataHandleId(id)) => Some(id.value),
            _ => None,
        };
        let range = req.range.as_ref().map(|range| (range.offset, range.len));
        self.calls.lock().expect("calls").push(RecordedCall {
            method: "read_layout",
            group_id: header.group_id,
            call_id: header.client.as_ref().expect("client").call_id.clone(),
            target_data_handle_id,
            range,
            target_size: None,
            final_size: None,
            committed_block_offsets: Vec::new(),
            committed_block_lens: Vec::new(),
            sync_mode: None,
            create_disposition: None,
            create_layout: None,
            add_block_desired_len: None,
        });
    }

    fn record_commit_file(&self, ctx: &AttemptContext, req: &CommitFileOp) {
        self.record_event("commit_file");
        let header = ctx.metadata_header().expect("metadata header");
        self.calls.lock().expect("calls").push(RecordedCall {
            method: "commit_file",
            group_id: header.group_id,
            call_id: header.client.as_ref().expect("client").call_id.clone(),
            target_data_handle_id: req.data_handle_id.as_ref().map(|id| id.value),
            range: None,
            target_size: None,
            final_size: Some(req.final_size),
            committed_block_offsets: req.committed_blocks.iter().map(|block| block.file_offset).collect(),
            committed_block_lens: req.committed_blocks.iter().map(|block| block.len).collect(),
            sync_mode: None,
            create_disposition: None,
            create_layout: None,
            add_block_desired_len: None,
        });
    }

    fn record_sync_write(&self, ctx: &AttemptContext, req: &crate::metadata::SyncWriteOp) {
        let header = ctx.metadata_header().expect("metadata header");
        self.calls.lock().expect("calls").push(RecordedCall {
            method: "sync_write",
            group_id: header.group_id,
            call_id: header.client.as_ref().expect("client").call_id.clone(),
            target_data_handle_id: req.data_handle_id.as_ref().map(|id| id.value),
            range: None,
            target_size: Some(req.target_size),
            final_size: None,
            committed_block_offsets: req.committed_blocks.iter().map(|block| block.file_offset).collect(),
            committed_block_lens: req.committed_blocks.iter().map(|block| block.len).collect(),
            sync_mode: Some(req.mode),
            create_disposition: None,
            create_layout: None,
            add_block_desired_len: None,
        });
    }

    fn record_add_block(&self, ctx: &AttemptContext, req: &AddBlockOp) {
        let header = ctx.metadata_header().expect("metadata header");
        self.calls.lock().expect("calls").push(RecordedCall {
            method: "add_block",
            group_id: header.group_id,
            call_id: header.client.as_ref().expect("client").call_id.clone(),
            target_data_handle_id: None,
            range: None,
            target_size: None,
            final_size: None,
            committed_block_offsets: Vec::new(),
            committed_block_lens: Vec::new(),
            sync_mode: None,
            create_disposition: None,
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
    async fn get_status(&self, ctx: AttemptContext, _req: GetStatusOp) -> ClientResult<GetStatusResponseProto> {
        self.record("get_status", &ctx);
        Ok(GetStatusResponseProto {
            inode_id: Some(proto::fs::InodeIdProto { value: 101 }),
            attrs: Some(file_attrs_proto(10)),
            ..GetStatusResponseProto::default()
        })
    }

    async fn list_status(&self, ctx: AttemptContext, req: ListStatusOp) -> ClientResult<ListStatusResponseProto> {
        self.record("list_status", &ctx);
        self.list_requests.lock().expect("list requests").push(req);
        Ok(ListStatusResponseProto {
            entries: vec![proto::fs::DirEntryProto {
                name: "child".to_string(),
                inode_id: Some(proto::fs::InodeIdProto { value: 102 }),
                kind: proto::fs::InodeKindProto::InodeKindFile as i32,
                attrs: Some(file_attrs_proto(4)),
            }],
            eof: true,
            ..ListStatusResponseProto::default()
        })
    }

    async fn delete(&self, ctx: AttemptContext, _req: DeleteOp) -> ClientResult<DeleteResponseProto> {
        self.record("delete", &ctx);
        Ok(DeleteResponseProto::default())
    }

    async fn rename(&self, ctx: AttemptContext, _req: RenameOp) -> ClientResult<RenameResponseProto> {
        self.record("rename", &ctx);
        Ok(RenameResponseProto::default())
    }

    async fn open_file(&self, ctx: AttemptContext, _req: OpenFileOp) -> ClientResult<OpenFileResponseProto> {
        self.record("open_file", &ctx);
        Ok(OpenFileResponseProto {
            inode_id: Some(proto::fs::InodeIdProto { value: 101 }),
            data_handle_id: Some(proto::common::DataHandleIdProto { value: 202 }),
            file_size: 10,
            file_version: Some(3),
            ..OpenFileResponseProto::default()
        })
    }

    async fn read_layout(&self, ctx: AttemptContext, req: GetBlockLocationsOp) -> ClientResult<LayoutSnapshot> {
        self.record_read_layout(&ctx, &req);
        let layouts = self.layouts.lock().expect("layouts");
        Ok(layouts
            .front()
            .cloned()
            .unwrap_or_else(|| layout_response(9, 101, 202, Some(3), 10, Vec::new())))
    }

    async fn create_file(&self, ctx: AttemptContext, req: CreateFileOp) -> ClientResult<WriteSessionSeed> {
        self.record_create_file(&ctx, &req);
        self.next_offsets.lock().expect("offsets").insert(1, 0);
        let layout = req.layout.as_ref().map(recorded_layout).unwrap_or_else(default_layout);
        self.write_layouts.lock().expect("write layouts").insert(1, layout);
        let response_layout = self
            .create_response_layout
            .lock()
            .expect("create response layout")
            .unwrap_or(Some(layout));
        Ok(WriteSessionSeed::Create(CreateFileResponseProto {
            write_handle: Some(write_handle_proto(1, 302)),
            inode_id: Some(proto::fs::InodeIdProto { value: 301 }),
            data_handle_id: Some(proto::common::DataHandleIdProto { value: 302 }),
            base_size: 0,
            layout: response_layout.map(layout_proto),
            ..CreateFileResponseProto::default()
        }))
    }

    async fn append_file(&self, ctx: AttemptContext, _req: AppendFileOp) -> ClientResult<WriteSessionSeed> {
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
        Ok(WriteSessionSeed::Append(AppendFileResponseProto {
            write_handle: Some(write_handle_proto(2, 402)),
            inode_id: Some(proto::fs::InodeIdProto { value: 401 }),
            data_handle_id: Some(proto::common::DataHandleIdProto { value: 402 }),
            base_size: 10,
            layout: response_layout.map(layout_proto),
            ..AppendFileResponseProto::default()
        }))
    }

    async fn add_block(&self, ctx: AttemptContext, req: AddBlockOp) -> ClientResult<AddBlockResult> {
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
            group_id: header.group_id,
            target: write_target_with_layout(data_handle_id, block_index, offset, len, layout),
        })
    }

    async fn commit_file(&self, ctx: AttemptContext, req: CommitFileOp) -> ClientResult<CommitFileResult> {
        self.record_commit_file(&ctx, &req);
        Ok(CommitFileResponseProto {
            committed_size: req.final_size,
            file_version: Some(1),
            ..CommitFileResponseProto::default()
        })
    }

    async fn abort_file_write(
        &self,
        ctx: AttemptContext,
        _req: AbortFileWriteOp,
    ) -> ClientResult<AbortFileWriteResult> {
        self.record_event("abort_file_write");
        self.record("abort_file_write", &ctx);
        Ok(AbortFileWriteResponseProto::default())
    }

    async fn renew_lease(&self, ctx: AttemptContext, _req: RenewLeaseOp) -> ClientResult<RenewLeaseResult> {
        self.record("renew_lease", &ctx);
        match self.next_renew_outcome() {
            RenewOutcome::Ok => Ok(RenewLeaseResponseProto {
                expires_at_ms: u64::MAX / 2,
                ..RenewLeaseResponseProto::default()
            }),
            RenewOutcome::SessionExpired => Err(session_error(RefreshReason::SessionExpired)),
        }
    }

    async fn sync_write(
        &self,
        ctx: AttemptContext,
        req: crate::metadata::SyncWriteOp,
    ) -> ClientResult<SyncWriteResponseProto> {
        self.record_sync_write(&ctx, &req);
        Ok(SyncWriteResponseProto {
            synced_size: req.target_size,
            file_version: Some(1),
            ..SyncWriteResponseProto::default()
        })
    }

    async fn msync(&self, ctx: AttemptContext, _req: MsyncOp) -> ClientResult<proto::common::GroupStateWatermarkProto> {
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
    commit_sync_flags: Mutex<Vec<bool>>,
    block_syncs: Mutex<Vec<u64>>,
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
            commit_sync_flags: Mutex::new(Vec::new()),
            block_syncs: Mutex::new(Vec::new()),
            record_written_body: true,
            events: None,
        }
    }

    fn without_recorded_body() -> Self {
        Self {
            record_written_body: false,
            ..Self::default()
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
}

impl Default for MockDataClient {
    fn default() -> Self {
        Self::from_file(b"")
    }
}

#[async_trait]
impl WorkerDataClient for MockDataClient {
    async fn read_segment(
        &self,
        _ctx: AttemptContext,
        _group_id: u64,
        segment: &PlannedReadSegment,
    ) -> ClientResult<Bytes> {
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
        let start = segment.file_offset as usize;
        let end = start + segment.len as usize;
        Ok(self.file.slice(start..end))
    }

    async fn open_write(&self, _ctx: AttemptContext, target: WorkerWriteTarget) -> ClientResult<WorkerWriteBlock> {
        self.record_event("open_write");
        let call_number = {
            let mut calls = self.calls.lock().expect("calls");
            *calls += 1;
            *calls
        };
        Ok(WorkerWriteBlock {
            group_id: target.group_id,
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

    async fn write_stream(
        &self,
        block: &WorkerWriteBlock,
        data: Bytes,
    ) -> ClientResult<proto::worker::WriteStreamResponseProto> {
        self.record_event("write_stream");
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

    async fn commit_write(
        &self,
        _ctx: AttemptContext,
        block: &WorkerWriteBlock,
        effective_len: u64,
        _commit_seq: u64,
        require_sync: bool,
    ) -> ClientResult<WorkerCommitResult> {
        self.record_event("commit_write");
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
            effective_block_len: effective_len,
            block_stamp: block.target.block_stamp,
            written_through: effective_len,
        })
    }

    async fn sync_committed_block(
        &self,
        _ctx: AttemptContext,
        block: &WorkerWriteBlock,
        expected_len: u64,
    ) -> ClientResult<WorkerBlockSyncResult> {
        self.record_event("sync_committed_block");
        self.block_syncs.lock().expect("block syncs").push(expected_len);
        Ok(WorkerBlockSyncResult {
            effective_block_len: expected_len,
            block_stamp: block.target.block_stamp,
        })
    }

    async fn abort_write(&self, _ctx: AttemptContext, _block: &WorkerWriteBlock) -> ClientResult<()> {
        self.record_event("abort_write");
        Ok(())
    }
}

fn data_boundary(client: Arc<MockDataClient>) -> DataPlaneBoundary {
    DataPlaneBoundary::with_client(client)
}

fn read_reader(client: &FsClient, file_size: u64) -> FileReader {
    FileReader::new(
        client.clone(),
        ReadHandle::new(
            "/alpha".to_string(),
            InodeId::new(101),
            DataHandleId::new(202),
            3,
            file_size,
        ),
    )
}

fn layout_response(
    group_id: u64,
    inode_id: u64,
    data_handle_id: u64,
    file_version: Option<u64>,
    file_size: u64,
    locations: Vec<FileBlockLocation>,
) -> LayoutSnapshot {
    LayoutSnapshot {
        group_id,
        inode_id: InodeId::new(inode_id),
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
            owner: 7,
            epoch: 1,
        }),
    }
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
        effective_block_len: len,
        worker_endpoints: vec![worker_endpoint()],
        fencing_token: FencingToken::new(block_id, ClientId::new(7), 1),
        block_stamp: 1,
        chunk_size: layout.chunk_size,
        block_format_id: types::BlockFormatId::from_raw(layout.block_format_id).expect("known test block format"),
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
        effective_block_len: len,
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
