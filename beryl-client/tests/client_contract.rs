// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

mod support;

use beryl_client::{
    ClientConfig, ClientError, ClientErrorKind, DeleteOptions, FsClient, ListStatusOptions, MkdirOptions,
};
use beryl_common::error::rpc::{ErrorKind, InternalErrorKind, MetadataErrorKind, RefreshHint, RpcErrorDetail};
use beryl_common::header::{HEADER_PRE_HANDLER_REJECTION, PRE_HANDLER_REJECTION_RPC_CONCURRENCY};
use beryl_proto::common::{
    BlockIdProto, ClientIdProto, FencingTokenProto, GroupStateWatermarkProto, RaftLogIdProto, TierProto,
    WorkerEndpointInfoProto,
};
use beryl_proto::metadata::{
    AbortFileWriteResponseProto, AllocateBlockResponseProto, CommitFileResponseProto, CreateDirectoryResponseProto,
    CreateFileResponseProto, DirEntryProto, FileBlockLocationProto, FileTypeProto, GetBlockLocationsResponseProto,
    GetStatusResponseProto, ListStatusResponseProto, LocatedBlockProto, MsyncResponseProto, OpenFileResponseProto,
    RenewLeaseResponseProto, SyncWriteResponseProto, WriteHandleProto,
};
use bytes::Bytes;
use futures::io::{AsyncReadExt, AsyncSeekExt};
use std::collections::VecDeque;
use std::io::SeekFrom;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use support::{
    MetadataCall, MetadataReply, MetadataScript, MockMetadata, MockWorker, ReadReply, ResponseAuthority, WorkerScript,
    WriteReply,
};
use tonic::metadata::{MetadataMap, MetadataValue};
use tonic::{Code, Status};

const WORKER_RUN_ID: &str = "550e8400-e29b-41d4-a716-446655440000";

#[tokio::test]
async fn malformed_directory_status_reports_unknown_outcome_without_replaying_creation() {
    let metadata = MockMetadata::new(MetadataScript {
        create_directory: VecDeque::from([
            MetadataReply::success(CreateDirectoryResponseProto::default()),
            MetadataReply::success(CreateDirectoryResponseProto {
                status: Some(file_status(2, 0)),
                ..Default::default()
            }),
        ]),
        ..Default::default()
    });
    let server = metadata.start().await;
    let client = FsClient::new(client_config(server.endpoint(), 3)).unwrap();
    for create_parent in [false, true] {
        let before = metadata.calls().len();
        let error = client
            .mkdirs_with_options("/directory", MkdirOptions { create_parent })
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ClientErrorKind::InvalidResponse);
        assert!(error.is_outcome_unknown());
        assert_eq!(metadata.calls().len(), before + 1);
    }
    server.shutdown().await;
}

#[tokio::test]
async fn reader_queries_actual_ranges_reuses_locations_and_refreshes_failures() {
    let worker = MockWorker::new(WorkerScript {
        reads: VecDeque::from([
            ReadReply::Data(Bytes::from_static(b"abcdefgh")),
            ReadReply::Data(Bytes::from_static(b"abcdefgh")),
            ReadReply::RefreshMetadata,
            ReadReply::Data(Bytes::from_static(b"abcdefgh")),
            ReadReply::Data(Bytes::from_static(b"ABCDEFGH")),
        ]),
        ..Default::default()
    });
    let worker_server = worker.start().await;
    let tail = block_location(202, 1, 8, 8, worker_server.endpoint());
    let opened = open_file_response(202, 16);
    let mut changed = locations_response(202, 16, tail.clone());
    changed.status.as_mut().unwrap().generation = Some(4);
    let metadata = MockMetadata::new(MetadataScript {
        open_file: VecDeque::from([MetadataReply::success(opened)]),
        get_block_locations: VecDeque::from([
            MetadataReply::success(locations_response(202, 16, tail.clone())),
            MetadataReply::success(locations_response(202, 16, tail)),
            MetadataReply::success(locations_response(
                202,
                16,
                block_location(202, 0, 0, 8, worker_server.endpoint()),
            )),
            MetadataReply::success(changed),
        ]),
        ..Default::default()
    });
    let server = metadata.start().await;
    let client = FsClient::new(client_config(server.endpoint(), 2)).unwrap();
    let mut reader = client.open("/file").await.unwrap();
    assert_eq!(metadata.calls().len(), 1);
    assert!(calls_for(&metadata.calls(), "GetBlockLocations").is_empty());
    assert_eq!(reader.status().inode_id().as_raw(), 202);
    let mut bytes = [0; 3];
    assert_eq!(reader.read_range(10..13).await.unwrap(), b"cde"[..]);
    assert_eq!(reader.read_range(13..).await.unwrap(), b"fgh"[..]);
    assert_eq!(calls_for(&metadata.calls(), "GetBlockLocations").len(), 1);
    assert_eq!(reader.read_range(9..12).await.unwrap(), b"bcd"[..]);
    assert_eq!(calls_for(&metadata.calls(), "GetBlockLocations").len(), 2);
    assert_eq!(reader.read(&mut bytes).await.unwrap(), 3);
    assert_eq!(&bytes, b"ABC");
    assert_eq!(reader.position(), 3);
    let reads = worker.read_calls();
    let error = reader
        .read_range(10..13)
        .await
        .expect_err("refreshed generation must match open");
    assert!(error.message().contains("generation mismatch"));
    assert_eq!(worker.read_calls(), reads, "changed generation must not reach Worker");
    assert_eq!(reader.position(), 3);
    let requests = metadata.layout_requests();
    let ranges: Vec<_> = requests
        .iter()
        .map(|request| {
            assert_eq!(
                request.target,
                Some(beryl_proto::metadata::get_block_locations_request_proto::Target::InodeId(202))
            );
            let range = request.range.as_ref().unwrap();
            (range.offset, range.len)
        })
        .collect();
    assert_eq!(ranges, [(10, 1), (9, 1), (0, 1), (10, 1)]);
    server.shutdown().await;
    worker_server.shutdown().await;
}

#[tokio::test]
async fn successful_worker_retry_preserves_layout() {
    let data = Bytes::from_static(b"abcdefgh");
    let worker = MockWorker::new(WorkerScript {
        reads: VecDeque::from([
            ReadReply::Data(data.clone()),
            ReadReply::Chunks(vec![Err(Status::unavailable("transient Worker failure"))]),
            ReadReply::Data(data.clone()),
            ReadReply::Data(data),
        ]),
        ..Default::default()
    });
    let worker_server = worker.start().await;
    let metadata = MockMetadata::new(MetadataScript {
        open_file: VecDeque::from([MetadataReply::success(open_file_response(202, 8))]),
        get_block_locations: VecDeque::from([MetadataReply::success(locations_response(
            202,
            8,
            block_location(202, 0, 0, 8, worker_server.endpoint()),
        ))]),
        ..Default::default()
    });
    let server = metadata.start().await;
    let client = FsClient::new(client_config(server.endpoint(), 2)).unwrap();
    let reader = client.open("/file").await.unwrap();
    assert_eq!(reader.read_range(0..1).await.unwrap(), b"a"[..]);
    assert_eq!(reader.read_range(1..2).await.unwrap(), b"b"[..]);
    assert_eq!(reader.read_range(2..3).await.unwrap(), b"c"[..]);
    assert_eq!(
        metadata.layout_requests().len(),
        1,
        "successful retry retains its layout"
    );
    assert_eq!(worker.read_calls(), 4);
    server.shutdown().await;
    worker_server.shutdown().await;
}

#[tokio::test]
async fn exhausted_transport_retries_allow_the_next_read_to_find_a_replacement() {
    let data = Bytes::from_static(b"abcdefgh");
    let old_worker = MockWorker::new(WorkerScript {
        reads: VecDeque::from([
            ReadReply::Data(data.clone()),
            ReadReply::Chunks(vec![Err(Status::unavailable("old endpoint is unreachable"))]),
        ]),
        ..Default::default()
    });
    let old_server = old_worker.start().await;
    let new_worker = MockWorker::new(WorkerScript {
        reads: VecDeque::from([ReadReply::Data(data)]),
        ..Default::default()
    });
    let new_server = new_worker.start().await;
    let metadata = MockMetadata::new(MetadataScript {
        open_file: VecDeque::from([MetadataReply::success(open_file_response(202, 8))]),
        get_block_locations: [old_server.endpoint(), new_server.endpoint()]
            .into_iter()
            .map(|endpoint| MetadataReply::success(locations_response(202, 8, block_location(202, 0, 0, 8, endpoint))))
            .collect(),
        ..Default::default()
    });
    let server = metadata.start().await;
    let client = FsClient::new(client_config(server.endpoint(), 1)).unwrap();
    let reader = client.open("/file").await.unwrap();
    assert_eq!(reader.read_range(0..1).await.unwrap(), b"a"[..]);
    assert_eq!(
        reader.read_range(1..2).await.unwrap_err().kind(),
        ClientErrorKind::Unavailable
    );
    assert_eq!(reader.read_range(2..3).await.unwrap(), b"c"[..]);
    assert_eq!(metadata.layout_requests().len(), 2);
    assert_eq!(old_worker.read_calls(), 2);
    assert_eq!(new_worker.read_calls(), 1);
    server.shutdown().await;
    old_server.shutdown().await;
    new_server.shutdown().await;
}

#[tokio::test]
async fn stale_worker_location_is_evicted_even_on_the_final_attempt() {
    let data = Bytes::from_static(b"abcdefgh");
    let worker = MockWorker::new(WorkerScript {
        reads: VecDeque::from([
            ReadReply::Data(data.clone()),
            ReadReply::RefreshMetadata,
            ReadReply::Data(data),
        ]),
        ..Default::default()
    });
    let worker_server = worker.start().await;
    let layout = locations_response(202, 8, block_location(202, 0, 0, 8, worker_server.endpoint()));
    let metadata = MockMetadata::new(MetadataScript {
        open_file: VecDeque::from([MetadataReply::success(open_file_response(202, 8))]),
        get_block_locations: VecDeque::from([MetadataReply::success(layout.clone()), MetadataReply::success(layout)]),
        ..Default::default()
    });
    let server = metadata.start().await;
    let client = FsClient::new(client_config(server.endpoint(), 1)).unwrap();
    let reader = client.open("/file").await.unwrap();
    assert_eq!(reader.read_range(0..1).await.unwrap(), b"a"[..]);
    reader
        .read_range(1..2)
        .await
        .expect_err("stale location exhausts the only attempt");
    assert_eq!(reader.read_range(2..3).await.unwrap(), b"c"[..]);
    assert_eq!(
        metadata.layout_requests().len(),
        2,
        "next read must query a fresh layout"
    );
    server.shutdown().await;
    worker_server.shutdown().await;
}

#[tokio::test]
async fn old_failed_read_preserves_concurrently_replaced_layout() {
    let (started, waiting) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let worker = MockWorker::new(WorkerScript {
        reads: VecDeque::from([
            ReadReply::BlockedRefresh {
                started,
                release: released,
            },
            ReadReply::Data(Bytes::from_static(b"abcdefgh")),
            ReadReply::Data(Bytes::from_static(b"abcdefgh")),
        ]),
        ..Default::default()
    });
    let worker_server = worker.start().await;
    let metadata = MockMetadata::new(MetadataScript {
        open_file: VecDeque::from([MetadataReply::success(open_file_response(202, 16))]),
        get_block_locations: VecDeque::from([
            MetadataReply::success(locations_response(
                202,
                16,
                block_location(202, 0, 0, 8, worker_server.endpoint()),
            )),
            MetadataReply::success(locations_response(
                202,
                16,
                block_location(202, 1, 8, 8, worker_server.endpoint()),
            )),
        ]),
        ..Default::default()
    });
    let server = metadata.start().await;
    let client = FsClient::new(client_config(server.endpoint(), 1)).unwrap();
    let reader = client.open("/file").await.unwrap();
    let old_read = async { reader.read_range(0..1).await };
    let replace_layout = async {
        waiting.await.unwrap();
        assert_eq!(reader.read_range(8..9).await.unwrap(), b"a"[..]);
        release.send(()).unwrap();
    };
    let (failed, ()) = tokio::time::timeout(Duration::from_secs(5), async { tokio::join!(old_read, replace_layout) })
        .await
        .expect("controlled reads finish");
    failed.expect_err("old Worker request must fail");
    assert_eq!(reader.read_range(9..10).await.unwrap(), b"b"[..]);
    assert_eq!(
        metadata.layout_requests().len(),
        2,
        "the replacement layout must remain cached"
    );
    assert_eq!(reader.position(), 0);
    server.shutdown().await;
    worker_server.shutdown().await;
}

#[tokio::test]
async fn open_rejects_missing_file_state_and_directory_status() {
    let mut missing = open_file_response(202, 8);
    missing.status = None;
    let mut missing_generation = open_file_response(202, 8);
    missing_generation.status.as_mut().unwrap().generation = None;
    let mut directory = open_file_response(202, 0);
    directory.status.as_mut().unwrap().kind = FileTypeProto::FileTypeDir as i32;
    directory.status.as_mut().unwrap().generation = None;
    let metadata = MockMetadata::new(MetadataScript {
        open_file: VecDeque::from([missing, missing_generation, directory].map(MetadataReply::success)),
        ..Default::default()
    });
    let server = metadata.start().await;
    let client = FsClient::new(client_config(server.endpoint(), 1)).unwrap();
    for _ in 0..3 {
        client
            .open("/file")
            .await
            .expect_err("invalid open response must fail closed");
    }
    assert_eq!(metadata.calls().len(), 3);
    server.shutdown().await;
}

#[tokio::test]
async fn list_status_validates_options_and_drains_the_final_page() {
    let client = FsClient::new(client_config("127.0.0.1:1", 1)).expect("client");

    let error = match client
        .list_status_with_options("/alpha", ListStatusOptions { page_size: Some(0) })
        .await
    {
        Ok(_) => panic!("zero page size must fail before Metadata"),
        Err(error) => error,
    };

    assert_client_error(&error, ClientErrorKind::InvalidArgument, false, "greater than zero");

    let metadata = MockMetadata::new(MetadataScript {
        list_status: VecDeque::from([
            MetadataReply::success(ListStatusResponseProto {
                entries: vec![DirEntryProto {
                    name: "first".into(),
                    status: Some(file_status(1, 0)),
                }],
                next_cursor: vec![1],
                eof: false,
                ..Default::default()
            }),
            MetadataReply::status(Status::unavailable("page request failed")),
            MetadataReply::success(ListStatusResponseProto {
                entries: vec![DirEntryProto {
                    name: "last".into(),
                    status: Some(file_status(1, 0)),
                }],
                eof: true,
                ..Default::default()
            }),
        ]),
        ..MetadataScript::default()
    });
    let server = metadata.start().await;
    let client = FsClient::new(client_config(server.endpoint(), 1)).expect("client");
    let mut entries = client.list_status("/alpha").await.expect("first page");
    assert_eq!(entries.next().await.unwrap().unwrap().path(), Some("/alpha/first"));
    entries.next().await.expect_err("failed page remains retryable");
    assert_eq!(entries.next().await.unwrap().unwrap().path(), Some("/alpha/last"));
    let calls = metadata.calls().len();
    assert!(entries.next().await.unwrap().is_none());
    assert!(entries.next().await.unwrap().is_none());
    assert_eq!(metadata.calls().len(), calls);
    server.shutdown().await;
}

#[tokio::test]
async fn metadata_read_retries_reuse_one_identity_and_deadline() {
    let metadata = MockMetadata::new(MetadataScript {
        get_status: VecDeque::from([
            MetadataReply::status(pre_handler_rejection()),
            MetadataReply::error(RpcErrorDetail::retry(
                ErrorKind::Internal(InternalErrorKind::NodeUnavailable),
                Some(1),
                "scripted server retry",
            )),
            MetadataReply::success(status_response(10)),
        ]),
        ..MetadataScript::default()
    });
    let server = metadata.start().await;
    let client = FsClient::new(client_config(server.endpoint(), 3)).expect("client");

    let status = client.get_status("/alpha").await.expect("third attempt succeeds");
    assert_eq!(status.len(), 10);
    assert_eq!(status.path(), Some("/alpha"));

    let calls = metadata.calls();
    assert_methods(&calls, &["GetStatus", "GetStatus", "GetStatus"]);
    assert_same_identity_and_deadline(&calls);
    server.shutdown().await;
}

#[tokio::test]
async fn metadata_mutations_retry_only_when_the_public_operation_is_replayable() {
    let metadata = MockMetadata::new(MetadataScript {
        create_file: VecDeque::from([
            MetadataReply::status(Status::unavailable("CreateFile transport ambiguity")),
            MetadataReply::success(create_response(301, 8)),
        ]),
        create_directory: VecDeque::from([
            MetadataReply::status(Status::unavailable("recursive CreateDirectory transport ambiguity")),
            MetadataReply::success(CreateDirectoryResponseProto {
                status: Some(beryl_proto::metadata::FileStatusProto {
                    inode_id: 2,
                    kind: FileTypeProto::FileTypeDir as i32,
                    create_time: 11,
                    modify_time: 12,
                    ..Default::default()
                }),
                ..CreateDirectoryResponseProto::default()
            }),
            MetadataReply::status(Status::unavailable("non-recursive CreateDirectory transport ambiguity")),
        ]),
        open_write: VecDeque::from([MetadataReply::status(Status::unavailable(
            "OpenWrite transport ambiguity",
        ))]),
        delete: VecDeque::from([MetadataReply::status(Status::unavailable("Delete transport ambiguity"))]),
        rename: VecDeque::from([MetadataReply::status(Status::unavailable("Rename transport ambiguity"))]),
        ..MetadataScript::default()
    });
    let server = metadata.start().await;
    let client = FsClient::new(client_config(server.endpoint(), 3)).expect("client");

    let _writer = client.create("/created").await.expect("CreateFile safely replays");
    client
        .mkdirs("/parent/child")
        .await
        .expect("recursive mkdirs safely replays");

    let append = client
        .append("/created")
        .await
        .expect_err("OpenWrite ambiguity fails closed");
    assert!(append.is_outcome_unknown());
    let mkdir = client
        .mkdirs_with_options("/single", MkdirOptions { create_parent: false })
        .await
        .expect_err("non-recursive mkdir ambiguity fails closed");
    assert!(mkdir.is_outcome_unknown());
    let delete = client
        .delete_with_options("/created", DeleteOptions::default())
        .await
        .expect_err("Delete ambiguity fails closed");
    assert!(delete.is_outcome_unknown());
    let rename = client
        .rename("/created", "/renamed")
        .await
        .expect_err("Rename ambiguity fails closed");
    assert!(rename.is_outcome_unknown());

    let calls = metadata.calls();
    let create_calls = calls_for(&calls, "CreateFile");
    assert_eq!(create_calls.len(), 2);
    assert_same_identity_and_deadline(&create_calls);
    let mkdir_calls = calls_for(&calls, "CreateDirectory");
    assert_eq!(mkdir_calls.len(), 3);
    assert_same_identity_and_deadline(&mkdir_calls[..2]);
    assert_ne!(call_id(&mkdir_calls[0]), call_id(&mkdir_calls[2]));
    assert_eq!(calls_for(&calls, "OpenWrite").len(), 1);
    assert_eq!(calls_for(&calls, "Delete").len(), 1);
    assert_eq!(calls_for(&calls, "Rename").len(), 1);
    server.shutdown().await;
}

#[tokio::test]
async fn stale_metadata_refreshes_with_a_child_call_and_carries_new_authority() {
    let state_one = watermark(1);
    let state_nine = watermark(9);
    let metadata = MockMetadata::new(MetadataScript {
        get_status: VecDeque::from([
            MetadataReply::error(RpcErrorDetail::refresh_metadata(
                ErrorKind::Metadata(MetadataErrorKind::StaleState),
                RefreshHint::default(),
                "scripted stale state",
            )),
            MetadataReply::SuccessWithAuthority(
                status_response(10),
                ResponseAuthority {
                    state: vec![state_nine.clone()],
                    mount_epoch: Some(31),
                    route_epoch: Some(41),
                },
            ),
            MetadataReply::success(status_response(10)),
        ]),
        msync: VecDeque::from([MetadataReply::success(MsyncResponseProto {
            state: Some(state_one),
            ..MsyncResponseProto::default()
        })]),
        ..MetadataScript::default()
    });
    let server = metadata.start().await;
    let client = FsClient::new(client_config(server.endpoint(), 2)).expect("client");

    client.get_status("/alpha").await.expect("refresh retry succeeds");
    client
        .get_status("/alpha")
        .await
        .expect("next operation carries authority");

    let calls = metadata.calls();
    assert_methods(&calls, &["GetStatus", "Msync", "GetStatus", "GetStatus"]);
    assert_eq!(call_id(&calls[0]), call_id(&calls[2]));
    assert_ne!(call_id(&calls[0]), call_id(&calls[1]));
    assert_eq!(calls[0].header.deadline_ms, calls[1].header.deadline_ms);
    assert_eq!(calls[0].header.deadline_ms, calls[2].header.deadline_ms);
    assert!(calls[0].header.state.is_empty());
    assert_eq!(
        calls[2].header.state[0].state_id.as_ref().map(|state| state.index),
        Some(1)
    );
    assert_eq!(calls[3].header.mount_epoch, Some(31));
    assert_eq!(calls[3].header.route_epoch, Some(41));
    assert_eq!(
        calls[3].header.state[0].state_id.as_ref().map(|state| state.index),
        Some(9)
    );
    server.shutdown().await;
}

#[tokio::test]
async fn reader_replans_without_advancing_position_and_rejects_local_bounds_before_io() {
    let worker = MockWorker::new(WorkerScript {
        reads: VecDeque::from([
            ReadReply::RefreshMetadata,
            ReadReply::Data(Bytes::from_static(b"abcdefgh")),
            ReadReply::Data(Bytes::from_static(b"abcdefgh")),
            ReadReply::Data(Bytes::from_static(b"abcdefgh")),
            ReadReply::Data(Bytes::from_static(b"abcdefgh")),
        ]),
        ..WorkerScript::default()
    });
    let worker_server = worker.start().await;
    let location = block_location(202, 0, 0, 8, worker_server.endpoint());
    let metadata = MockMetadata::new(MetadataScript {
        open_file: VecDeque::from([
            MetadataReply::success(open_file_response(202, 8)),
            MetadataReply::success(open_file_response(203, 9)),
        ]),
        get_block_locations: VecDeque::from([
            MetadataReply::success(locations_response(202, 8, location.clone())),
            MetadataReply::success(locations_response(202, 8, location.clone())),
            MetadataReply::success(locations_response(202, 8, location)),
        ]),
        ..MetadataScript::default()
    });
    let metadata_server = metadata.start().await;
    let config = ClientConfig::builder()
        .client_name("public-reader-contract")
        .metadata_endpoints([metadata_server.endpoint()])
        .max_attempts(2)
        .max_read_step_bytes(3)
        .read_range_limit(8)
        .build()
        .expect("reader config");
    let client = FsClient::new(config).expect("client");

    let mut reader = client.open("/alpha").await.expect("open reader");
    let mut sequential = [0u8; 4];
    assert_eq!(reader.read(&mut sequential).await.expect("replanned read"), 3);
    assert_eq!(&sequential[..3], b"abc");
    assert_eq!(reader.position(), 3);

    let metadata_calls = metadata.calls();
    let layout_calls = calls_for(&metadata_calls, "GetBlockLocations");
    assert_eq!(layout_calls.len(), 2);
    assert_same_identity_and_deadline(&layout_calls);

    assert_eq!(reader.read_range(4..7).await.expect("positioned read"), b"efg"[..]);
    assert_eq!(reader.position(), 3);

    let before_cached_read = metadata.calls().len();
    assert_eq!(reader.read(&mut sequential).await.expect("cached block subrange"), 3);
    assert_eq!(&sequential[..3], b"def");
    assert_eq!(reader.read(&mut sequential).await.expect("cached block tail"), 2);
    assert_eq!(&sequential[..2], b"gh");
    assert_eq!(reader.position(), 8);
    assert_eq!(metadata.calls().len(), before_cached_read);

    let metadata_before_eof = metadata.calls().len();
    let worker_before_eof = worker.read_calls();
    let error = reader.read_range(8..9).await.expect_err("exact read beyond EOF");
    assert_client_error(&error, ClientErrorKind::UnexpectedEof, false, "opened file length");
    assert_eq!(metadata.calls().len(), metadata_before_eof);
    assert_eq!(worker.read_calls(), worker_before_eof);

    let oversized = client.open("/oversized").await.expect("open oversized reader");
    let metadata_before_bound = metadata.calls().len();
    let worker_before_bound = worker.read_calls();
    let error = oversized.read_range(..).await.expect_err("read_range bound");
    assert_client_error(&error, ClientErrorKind::InvalidArgument, false, "read_range maximum");
    assert_eq!(metadata.calls().len(), metadata_before_bound);
    assert_eq!(worker.read_calls(), worker_before_bound);

    metadata_server.shutdown().await;
    worker_server.shutdown().await;
}

#[tokio::test]
async fn malformed_create_and_allocate_block_successes_fail_closed_before_worker_io() {
    let invalid_capacity = create_response(301, 0);
    let mut zero_inode = create_response(301, 8);
    zero_inode.write_handle.as_mut().unwrap().inode_id = 0;
    let mut zero_epoch = create_response(301, 8);
    zero_epoch.write_handle.as_mut().unwrap().write_lease_epoch = 0;
    let metadata = MockMetadata::new(MetadataScript {
        create_file: [invalid_capacity, zero_inode, zero_epoch, create_response(302, 8)]
            .into_iter()
            .map(MetadataReply::success)
            .collect(),
        allocate_block: VecDeque::from([MetadataReply::success(AllocateBlockResponseProto {
            block: Some(write_target(302, 0, 1, "127.0.0.1:9", 8)),
            ..AllocateBlockResponseProto::default()
        })]),
        ..MetadataScript::default()
    });
    let server = metadata.start().await;
    let client = FsClient::new(client_config(server.endpoint(), 1)).expect("client");

    for field in [
        "block_size must be non-zero",
        "inode_id must be non-zero",
        "write_lease_epoch must be non-zero",
    ] {
        let error = client.create("/invalid-create").await.expect_err("malformed create");
        assert_client_error(&error, ClientErrorKind::InvalidResponse, true, field);
        assert_eq!(error.operation(), Some("CreateFile"));
    }

    let mut writer = client.create("/bad-target").await.expect("valid writer");
    let add_error = writer
        .write_all(b"x")
        .await
        .expect_err("mismatched AllocateBlock target");
    assert_client_error(
        &add_error,
        ClientErrorKind::InvalidResponse,
        true,
        "file_offset mismatch",
    );
    assert_eq!(add_error.operation(), Some("AllocateBlock"));
    let stale = writer
        .write_all(b"!")
        .await
        .expect_err("unknown AllocateBlock blocks writes");
    assert_client_error(&stale, ClientErrorKind::StaleHandle, false, "unknown outcome");
    assert_eq!(calls_for(&metadata.calls(), "AllocateBlock").len(), 1);
    server.shutdown().await;
}

#[tokio::test]
async fn ambiguous_commit_response_can_only_be_recovered_by_the_same_close() {
    for internal in [false, true] {
        let first_reply = if internal {
            MetadataReply::error(RpcErrorDetail::fail(
                ErrorKind::Internal(InternalErrorKind::Internal),
                "commit completion failed",
            ))
        } else {
            MetadataReply::success(CommitFileResponseProto {
                committed_len: 1,
                ..CommitFileResponseProto::default()
            })
        };
        let metadata = MockMetadata::new(MetadataScript {
            create_file: VecDeque::from([MetadataReply::success(create_response(303, 8))]),
            commit_file: VecDeque::from([
                first_reply,
                MetadataReply::error(RpcErrorDetail::fail(
                    ErrorKind::Metadata(MetadataErrorKind::SessionInvalid),
                    "receipt no longer available",
                )),
                MetadataReply::success(CommitFileResponseProto {
                    committed_len: 0,
                    ..CommitFileResponseProto::default()
                }),
            ]),
            ..MetadataScript::default()
        });
        let server = metadata.start().await;
        let client = FsClient::new(client_config(server.endpoint(), 1)).expect("client");
        let mut writer = client.create("/commit").await.expect("writer");

        let error = writer.close().await.expect_err("unconfirmed commit");
        let (kind, message) = if internal {
            (ClientErrorKind::Internal, "completion failed")
        } else {
            (ClientErrorKind::InvalidResponse, "committed_len")
        };
        assert_client_error(&error, kind, true, message);
        let error = writer
            .close()
            .await
            .expect_err("missing evidence cannot erase prior ambiguity");
        assert!(error.is_outcome_unknown());
        writer.close().await.expect("frozen close retry succeeds");

        let metadata_calls = metadata.calls();
        let calls = calls_for(&metadata_calls, "CommitFile");
        assert_eq!(calls.len(), 3);
        assert_same_call_id(&calls);
        server.shutdown().await;
    }
}

#[tokio::test]
async fn malformed_sync_response_blocks_new_writes_until_the_same_sync_resolves() {
    let worker = MockWorker::new(WorkerScript {
        writes: VecDeque::from([WriteReply::Success]),
        ..WorkerScript::default()
    });
    let worker_server = worker.start().await;
    let metadata = MockMetadata::new(MetadataScript {
        create_file: VecDeque::from([MetadataReply::success(create_response(304, 8))]),
        allocate_block: VecDeque::from([MetadataReply::success(AllocateBlockResponseProto {
            block: Some(write_target(304, 0, 0, worker_server.endpoint(), 8)),
            ..AllocateBlockResponseProto::default()
        })]),
        sync_write: VecDeque::from([
            MetadataReply::success(SyncWriteResponseProto {
                synced_len: 3,
                generation: None,
                ..SyncWriteResponseProto::default()
            }),
            MetadataReply::success(SyncWriteResponseProto {
                synced_len: 4,
                generation: Some(1),
                ..SyncWriteResponseProto::default()
            }),
            MetadataReply::success(SyncWriteResponseProto {
                synced_len: 3,
                generation: Some(1),
                ..SyncWriteResponseProto::default()
            }),
        ]),
        abort_file_write: VecDeque::from([MetadataReply::success(AbortFileWriteResponseProto::default())]),
        ..MetadataScript::default()
    });
    let metadata_server = metadata.start().await;
    let client = FsClient::new(client_config(metadata_server.endpoint(), 1)).expect("client");
    let mut writer = client.create("/sync").await.expect("writer");
    writer.write_all(b"abc").await.expect("write");

    for expected in ["generation missing", "synced_len"] {
        let error = writer.sync().await.expect_err("malformed SyncWrite response");
        assert_client_error(&error, ClientErrorKind::InvalidResponse, true, expected);
    }
    let stale = writer.write_all(b"x").await.expect_err("unresolved sync blocks writes");
    assert_client_error(&stale, ClientErrorKind::StaleHandle, false, "unresolved SyncWrite");
    writer.sync().await.expect("frozen sync retry succeeds");
    writer.abort().await.expect("abort resolved writer");

    let metadata_calls = metadata.calls();
    let calls = calls_for(&metadata_calls, "SyncWrite");
    assert_eq!(calls.len(), 3);
    assert_same_call_id(&calls);
    assert_eq!(worker.write_calls(), 1);
    assert_eq!(worker.write_data_frames(), 1);
    assert_eq!(worker.write_completions(), 1);
    metadata_server.shutdown().await;
    worker_server.shutdown().await;
}

#[tokio::test]
async fn malformed_lease_renewal_invalidates_the_writer_for_new_side_effects() {
    let metadata = MockMetadata::new(MetadataScript {
        create_file: VecDeque::from([MetadataReply::success(create_response(305, 8))]),
        renew_lease: VecDeque::from([MetadataReply::success(RenewLeaseResponseProto {
            expires_at_ms: 0,
            ..RenewLeaseResponseProto::default()
        })]),
        ..MetadataScript::default()
    });
    let server = metadata.start().await;
    let client = FsClient::new(client_config(server.endpoint(), 1)).expect("client");
    let mut writer = client.create("/renew").await.expect("writer");

    let error = writer.renew_lease().await.expect_err("invalid renewal response");
    assert_client_error(&error, ClientErrorKind::InvalidResponse, true, "expires_at_ms");
    let stale = writer.write_all(b"x").await.expect_err("unknown renewal blocks writes");
    assert_client_error(&stale, ClientErrorKind::StaleHandle, false, "unknown outcome");
    assert_methods(&metadata.calls(), &["CreateFile", "RenewLease"]);
    server.shutdown().await;
}

#[tokio::test]
async fn worker_failure_after_ack_invalidates_the_writer_and_prevents_commit() {
    let worker = MockWorker::new(WorkerScript {
        writes: VecDeque::from([WriteReply::AckThenUnavailable]),
        ..WorkerScript::default()
    });
    let worker_server = worker.start().await;
    let metadata = MockMetadata::new(MetadataScript {
        create_file: VecDeque::from([MetadataReply::success(create_response(306, 8))]),
        allocate_block: VecDeque::from([MetadataReply::success(AllocateBlockResponseProto {
            block: Some(write_target(306, 0, 0, worker_server.endpoint(), 8)),
            ..AllocateBlockResponseProto::default()
        })]),
        ..MetadataScript::default()
    });
    let metadata_server = metadata.start().await;
    let client = FsClient::new(client_config(metadata_server.endpoint(), 1)).expect("client");
    let mut writer = client.create("/worker-failure").await.expect("writer");

    let first = match writer.write_all(b"abc").await {
        Ok(()) => writer.sync().await.expect_err("sync observes Worker failure"),
        Err(error) => error,
    };
    assert!(first.is_outcome_unknown());
    assert_eq!(first.operation(), Some("WriteBlock"));
    let stale = writer
        .write_all(b"x")
        .await
        .expect_err("uncertain Worker write blocks later writes");
    assert_client_error(&stale, ClientErrorKind::StaleHandle, false, "unknown outcome");
    assert!(calls_for(&metadata.calls(), "CommitFile").is_empty());
    assert_eq!(worker.write_calls(), 1);
    metadata_server.shutdown().await;
    worker_server.shutdown().await;
}

#[tokio::test]
async fn allocation_replay_and_worker_capacity_retries_keep_the_same_block() {
    let worker = MockWorker::new(WorkerScript {
        writes: VecDeque::from([
            WriteReply::CapacityRejected,
            WriteReply::CapacityRejected,
            WriteReply::CapacityRejected,
            WriteReply::Success,
        ]),
        ..WorkerScript::default()
    });
    let worker_server = worker.start().await;
    let allocated = AllocateBlockResponseProto {
        block: Some(write_target(307, 0, 0, worker_server.endpoint(), 8)),
        ..AllocateBlockResponseProto::default()
    };
    let metadata = MockMetadata::new(MetadataScript {
        create_file: VecDeque::from([MetadataReply::success(create_response(307, 8))]),
        allocate_block: VecDeque::from([
            MetadataReply::status(Status::unavailable("allocation response lost")),
            MetadataReply::success(allocated.clone()),
            MetadataReply::success(allocated),
        ]),
        ..MetadataScript::default()
    });
    let metadata_server = metadata.start().await;
    let client = FsClient::new(client_config(metadata_server.endpoint(), 3)).expect("client");
    let mut writer = client.create("/capacity").await.expect("writer");

    let error = writer.write_all(b"x").await.expect_err("capacity attempts exhausted");
    assert_client_error(&error, ClientErrorKind::ResourceExhausted, false, "capacity exhausted");
    writer
        .write_all(&[])
        .await
        .expect("definite pre-side-effect rejection leaves writer open");
    assert_eq!(worker.write_calls(), 3);
    assert_eq!(worker.write_data_frames(), 0);
    writer.write_all(b"12345678").await.expect("capacity recovered");
    writer.flush().await.expect("Worker durability");
    assert_eq!(writer.position(), 8);
    assert_eq!(worker.write_calls(), 4);
    assert_eq!(worker.write_completions(), 1);
    let calls = metadata.calls();
    let allocations = calls_for(&calls, "AllocateBlock");
    assert_eq!(allocations.len(), 3);
    assert_same_identity_and_deadline(&allocations[..2]);
    for request in metadata.allocations() {
        assert_eq!(request.write_handle, Some(write_handle(307)));
        assert_eq!(request.previous_block_id, None);
    }
    metadata_server.shutdown().await;
    worker_server.shutdown().await;
}

#[tokio::test]
async fn capacity_retry_deadline_preserves_the_unmodified_write_session() {
    let worker = MockWorker::new(WorkerScript {
        writes: (0..10).map(|_| WriteReply::CapacityRejected).collect(),
        ..Default::default()
    });
    let worker_server = worker.start().await;
    let metadata = MockMetadata::new(MetadataScript {
        create_file: VecDeque::from([MetadataReply::success(create_response(411, 8))]),
        allocate_block: VecDeque::from([MetadataReply::success(AllocateBlockResponseProto {
            block: Some(write_target(411, 0, 0, worker_server.endpoint(), 8)),
            ..Default::default()
        })]),
        commit_file: VecDeque::from([MetadataReply::success(CommitFileResponseProto::default())]),
        ..Default::default()
    });
    let server = metadata.start().await;
    let config = ClientConfig::builder()
        .metadata_endpoints([server.endpoint()])
        .max_attempts(10)
        .operation_timeout(Duration::from_secs(1))
        .build()
        .unwrap();
    let client = FsClient::new(config).unwrap();
    let mut writer = client.create("/capacity-deadline").await.unwrap();
    let error = writer.write(b"unaccepted").await.unwrap_err();
    assert_eq!(error.kind(), ClientErrorKind::Timeout);
    assert!(!error.is_outcome_unknown());
    assert_eq!(writer.position(), 0);
    assert_eq!(worker.write_data_frames(), 0);
    writer.close().await.expect("a rejected write leaves the lease usable");
    assert_methods(&metadata.calls(), &["CreateFile", "AllocateBlock", "CommitFile"]);
    server.shutdown().await;
    worker_server.shutdown().await;
}

#[tokio::test]
async fn cancelled_write_opening_rejects_further_writes_without_accepting_bytes() {
    for block_metadata in [true, false] {
        let (started, waiting) = tokio::sync::oneshot::channel();
        let (release, gate) = tokio::sync::oneshot::channel();
        let mut metadata_gate = None;
        let reply = if block_metadata {
            metadata_gate = Some((started, gate));
            WriteReply::Success
        } else {
            WriteReply::BlockedOpen { started, release: gate }
        };
        let worker = MockWorker::new(WorkerScript {
            writes: VecDeque::from([reply]),
            ..Default::default()
        });
        let worker_server = worker.start().await;
        let target = AllocateBlockResponseProto {
            block: Some(write_target(401, 0, 0, worker_server.endpoint(), 8)),
            ..Default::default()
        };
        let allocation = match metadata_gate {
            Some((started, release)) => MetadataReply::Blocked {
                body: target,
                started,
                release,
            },
            None => MetadataReply::success(target),
        };
        let metadata = MockMetadata::new(MetadataScript {
            create_file: VecDeque::from([MetadataReply::success(create_response(401, 8))]),
            allocate_block: VecDeque::from([allocation]),
            ..Default::default()
        });
        let server = metadata.start().await;
        let client = FsClient::new(client_config(server.endpoint(), 1)).unwrap();
        let mut writer = client.create("/cancel-opening").await.unwrap();
        tokio::select! {
            result = writer.write(b"old") => panic!("opening must wait: {result:?}"),
            result = tokio::time::timeout(Duration::from_secs(2), waiting) => result.unwrap().unwrap(),
        }
        assert_eq!(writer.position(), 0);
        assert_eq!(writer.path(), "/cancel-opening");
        assert_eq!(writer.write(&[]).await.unwrap(), 0);
        assert_eq!(worker.write_data_frames(), 0);
        let _ = release.send(());
        let error = writer.write(b"new").await.unwrap_err();
        assert_eq!(error.kind(), ClientErrorKind::StaleHandle);
        assert!(error.message().contains("unknown outcome"));
        assert!(writer.close().await.is_err());
        assert_eq!(writer.position(), 0);
        assert_eq!(worker.write_data_frames(), 0);
        assert_eq!(metadata.allocations().len(), 1);
        assert!(calls_for(&metadata.calls(), "CommitFile").is_empty());
        drop(writer);
        server.shutdown().await;
        worker_server.shutdown().await;
    }
}

#[tokio::test]
async fn cancelled_flush_blocks_later_writes_and_publication() {
    let (started, waiting) = tokio::sync::oneshot::channel();
    let (release, gate) = tokio::sync::oneshot::channel();
    let worker = MockWorker::new(WorkerScript {
        writes: VecDeque::from([WriteReply::BlockedFinish { started, release: gate }]),
        ..Default::default()
    });
    let worker_server = worker.start().await;
    let metadata = MockMetadata::new(MetadataScript {
        create_file: VecDeque::from([MetadataReply::success(create_response(402, 8))]),
        allocate_block: VecDeque::from([MetadataReply::success(AllocateBlockResponseProto {
            block: Some(write_target(402, 0, 0, worker_server.endpoint(), 8)),
            ..Default::default()
        })]),
        ..Default::default()
    });
    let server = metadata.start().await;
    let client = FsClient::new(client_config(server.endpoint(), 1)).unwrap();
    let mut writer = client.create("/cancel-flush").await.unwrap();
    writer.write_all(b"abc").await.unwrap();
    tokio::select! {
        result = writer.flush() => panic!("checkpoint must wait: {result:?}"),
        result = tokio::time::timeout(Duration::from_secs(2), waiting) => result.unwrap().unwrap(),
    }
    assert_eq!(writer.position(), 3);
    assert_eq!(worker.written_data(), b"abc");
    assert_eq!(worker.write_completions(), 0);
    let error = writer.write(b"de").await.unwrap_err();
    assert_eq!(error.kind(), ClientErrorKind::StaleHandle);
    assert!(writer.flush().await.is_err());
    assert!(writer.sync().await.is_err());
    assert!(writer.close().await.is_err());
    assert_eq!(writer.position(), 3);
    assert_eq!(worker.write_calls(), 1);
    assert_methods(&metadata.calls(), &["CreateFile", "AllocateBlock"]);
    let _ = release.send(());
    server.shutdown().await;
    worker_server.shutdown().await;
}

#[tokio::test]
async fn cancelled_publications_replay_frozen_requests_and_reject_other_operations() {
    for method in ["SyncWrite", "CommitFile", "AbortFileWrite"] {
        let (started, waiting) = tokio::sync::oneshot::channel();
        let (release, gate) = tokio::sync::oneshot::channel();
        let mut script = MetadataScript {
            create_file: VecDeque::from([MetadataReply::success(create_response(403, 8))]),
            ..Default::default()
        };
        match method {
            "SyncWrite" => script.sync_write.push_back(MetadataReply::Blocked {
                body: SyncWriteResponseProto {
                    synced_len: 0,
                    generation: Some(0),
                    ..Default::default()
                },
                started,
                release: gate,
            }),
            "CommitFile" => script.commit_file.push_back(MetadataReply::Blocked {
                body: CommitFileResponseProto::default(),
                started,
                release: gate,
            }),
            _ => script.abort_file_write.push_back(MetadataReply::Blocked {
                body: AbortFileWriteResponseProto::default(),
                started,
                release: gate,
            }),
        }
        match method {
            "SyncWrite" => script
                .sync_write
                .push_back(MetadataReply::success(SyncWriteResponseProto {
                    synced_len: 0,
                    generation: Some(0),
                    ..Default::default()
                })),
            "CommitFile" => script
                .commit_file
                .push_back(MetadataReply::success(CommitFileResponseProto::default())),
            _ => script
                .abort_file_write
                .push_back(MetadataReply::success(AbortFileWriteResponseProto::default())),
        }
        let metadata = MockMetadata::new(script);
        let server = metadata.start().await;
        let client = FsClient::new(client_config(server.endpoint(), 1)).unwrap();
        let mut writer = client.create("/cancel-publication").await.unwrap();
        tokio::select! {
            result = writer_barrier(&mut writer, method) => panic!("publication must wait: {result:?}"),
            result = tokio::time::timeout(Duration::from_secs(2), waiting) => result.unwrap().unwrap(),
        }
        let error = writer.write(b"unrelated").await.unwrap_err();
        assert_eq!(error.kind(), ClientErrorKind::StaleHandle);
        assert!(writer.renew_lease().await.is_err());
        let _ = release.send(());
        writer_barrier(&mut writer, method).await.unwrap();
        let calls = metadata.calls();
        let attempts = calls_for(&calls, method);
        assert_eq!(attempts.len(), 2);
        assert_same_call_id(&attempts);
        drop(writer);
        server.shutdown().await;
    }
}

#[tokio::test]
async fn cancelled_renewal_blocks_further_writes_and_publication() {
    let (started, waiting) = tokio::sync::oneshot::channel();
    let (release, gate) = tokio::sync::oneshot::channel();
    let worker = MockWorker::new(WorkerScript {
        writes: VecDeque::from([WriteReply::Success]),
        ..Default::default()
    });
    let worker_server = worker.start().await;
    let metadata = MockMetadata::new(MetadataScript {
        create_file: VecDeque::from([MetadataReply::success(create_response(410, 8))]),
        allocate_block: VecDeque::from([MetadataReply::success(AllocateBlockResponseProto {
            block: Some(write_target(410, 0, 0, worker_server.endpoint(), 8)),
            ..Default::default()
        })]),
        renew_lease: VecDeque::from([MetadataReply::Blocked {
            body: RenewLeaseResponseProto {
                expires_at_ms: unix_now_ms() + 120_000,
                ..Default::default()
            },
            started,
            release: gate,
        }]),
        ..Default::default()
    });
    let server = metadata.start().await;
    let client = FsClient::new(client_config(server.endpoint(), 1)).unwrap();
    let mut writer = client.create("/cancel-renewal").await.unwrap();
    writer.write_all(b"abc").await.unwrap();
    tokio::select! {
        result = writer.renew_lease() => panic!("renewal must wait: {result:?}"),
        result = tokio::time::timeout(Duration::from_secs(2), waiting) => result.unwrap().unwrap(),
    }
    assert_eq!(writer.position(), 3);
    let error = writer.write(b"d").await.unwrap_err();
    assert_eq!(error.kind(), ClientErrorKind::StaleHandle);
    assert!(writer.close().await.is_err());
    assert_eq!(writer.position(), 3);
    assert_eq!(worker.write_calls(), 1);
    assert_eq!(worker.write_completions(), 0);
    assert_methods(&metadata.calls(), &["CreateFile", "AllocateBlock", "RenewLease"]);
    let _ = release.send(());
    server.shutdown().await;
    worker_server.shutdown().await;
}

#[tokio::test]
async fn cancelled_close_retries_frozen_identity_with_a_new_deadline() {
    let (started, waiting) = tokio::sync::oneshot::channel();
    let (release, gate) = tokio::sync::oneshot::channel();
    let metadata = MockMetadata::new(MetadataScript {
        create_file: VecDeque::from([MetadataReply::success(create_response(404, 8))]),
        commit_file: VecDeque::from([
            MetadataReply::Blocked {
                body: CommitFileResponseProto::default(),
                started,
                release: gate,
            },
            MetadataReply::success(CommitFileResponseProto::default()),
        ]),
        ..Default::default()
    });
    let server = metadata.start().await;
    let config = ClientConfig::builder()
        .metadata_endpoints([server.endpoint()])
        .max_attempts(1)
        .operation_timeout(Duration::from_secs(1))
        .build()
        .unwrap();
    let client = FsClient::new(config).unwrap();
    let mut writer = client.create("/close-deadline").await.unwrap();
    tokio::select! {
        result = writer.close() => panic!("commit must wait: {result:?}"),
        result = tokio::time::timeout(Duration::from_secs(2), waiting) => result.unwrap().unwrap(),
    }
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::time::resume();
    writer.close().await.expect("explicit retry receives a new deadline");
    writer.close().await.unwrap();
    let calls = metadata.calls();
    let commits = calls_for(&calls, "CommitFile");
    assert_eq!(commits.len(), 2);
    assert_same_call_id(&commits);
    let _ = release.send(());
    server.shutdown().await;
}

#[tokio::test]
async fn failed_next_block_preserves_exact_accepted_prefix() {
    let worker = MockWorker::new(WorkerScript {
        writes: VecDeque::from([WriteReply::Success]),
        ..Default::default()
    });
    let worker_server = worker.start().await;
    let metadata = MockMetadata::new(MetadataScript {
        create_file: VecDeque::from([MetadataReply::success(create_response(405, 4))]),
        allocate_block: [0, 5]
            .into_iter()
            .enumerate()
            .map(|(index, offset)| {
                MetadataReply::success(AllocateBlockResponseProto {
                    block: Some(write_target(405, index as u32, offset, worker_server.endpoint(), 4)),
                    ..Default::default()
                })
            })
            .collect(),
        ..Default::default()
    });
    let server = metadata.start().await;
    let client = FsClient::new(client_config(server.endpoint(), 1)).unwrap();
    let mut writer = client.create("/partial-write").await.unwrap();
    let error = writer.write_all(b"abcdef").await.unwrap_err();
    assert_eq!(error.kind(), ClientErrorKind::InvalidResponse);
    assert!(error.is_outcome_unknown());
    assert_eq!(writer.position(), 4);
    assert_eq!(worker.written_data(), b"abcd");
    assert_eq!(worker.write_completions(), 1);
    assert_eq!(metadata.allocations().len(), 2);
    assert!(writer.close().await.is_err());
    assert!(calls_for(&metadata.calls(), "CommitFile").is_empty());
    server.shutdown().await;
    worker_server.shutdown().await;
}

#[tokio::test]
async fn dropping_writer_cancels_stream_without_finishing_or_committing() {
    let worker = MockWorker::new(WorkerScript {
        writes: VecDeque::from([WriteReply::Success]),
        ..Default::default()
    });
    let worker_server = worker.start().await;
    let metadata = MockMetadata::new(MetadataScript {
        create_file: VecDeque::from([MetadataReply::success(create_response(406, 8))]),
        allocate_block: VecDeque::from([MetadataReply::success(AllocateBlockResponseProto {
            block: Some(write_target(406, 0, 0, worker_server.endpoint(), 8)),
            ..Default::default()
        })]),
        ..Default::default()
    });
    let server = metadata.start().await;
    let client = FsClient::new(client_config(server.endpoint(), 1)).unwrap();
    let mut writer = client.create("/drop-writer").await.unwrap();
    writer.write_all(b"abc").await.unwrap();
    drop(writer);
    tokio::time::timeout(Duration::from_secs(2), async {
        while worker.write_cancellations() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(worker.write_completions(), 0);
    assert_methods(&metadata.calls(), &["CreateFile", "AllocateBlock"]);
    server.shutdown().await;
    worker_server.shutdown().await;
}

#[tokio::test]
async fn cancelling_write_all_at_block_boundary_keeps_only_accepted_prefix() {
    let (started, waiting) = tokio::sync::oneshot::channel();
    let (release, gate) = tokio::sync::oneshot::channel();
    let worker = MockWorker::new(WorkerScript {
        writes: VecDeque::from([WriteReply::BlockedFinish { started, release: gate }]),
        ..Default::default()
    });
    let worker_server = worker.start().await;
    let metadata = MockMetadata::new(MetadataScript {
        create_file: VecDeque::from([MetadataReply::success(create_response(407, 4))]),
        allocate_block: VecDeque::from([MetadataReply::success(AllocateBlockResponseProto {
            block: Some(write_target(407, 0, 0, worker_server.endpoint(), 4)),
            ..Default::default()
        })]),
        ..Default::default()
    });
    let server = metadata.start().await;
    let client = FsClient::new(client_config(server.endpoint(), 1)).unwrap();
    let mut writer = client.create("/cancel-write-all").await.unwrap();
    tokio::select! {
        result = writer.write_all(b"abcdef") => panic!("block completion must wait: {result:?}"),
        result = tokio::time::timeout(Duration::from_secs(2), waiting) => result.unwrap().unwrap(),
    }
    assert_eq!(writer.position(), 4);
    assert_eq!(worker.written_data(), b"abcd");
    let _ = release.send(());
    assert!(writer.write_all(b"xy").await.is_err());
    assert!(writer.close().await.is_err());
    assert_eq!(writer.position(), 4);
    assert_eq!(worker.written_data(), b"abcd");
    assert_eq!(metadata.allocations().len(), 1);
    assert!(calls_for(&metadata.calls(), "CommitFile").is_empty());
    drop(writer);
    server.shutdown().await;
    worker_server.shutdown().await;
}

#[tokio::test]
async fn flush_timeout_blocks_later_publication() {
    let (started, waiting) = tokio::sync::oneshot::channel();
    let (release, gate) = tokio::sync::oneshot::channel();
    let worker = MockWorker::new(WorkerScript {
        writes: VecDeque::from([WriteReply::BlockedFinish { started, release: gate }]),
        ..Default::default()
    });
    let worker_server = worker.start().await;
    let metadata = MockMetadata::new(MetadataScript {
        create_file: VecDeque::from([MetadataReply::success(create_response(408, 8))]),
        allocate_block: VecDeque::from([MetadataReply::success(AllocateBlockResponseProto {
            block: Some(write_target(408, 0, 0, worker_server.endpoint(), 8)),
            ..Default::default()
        })]),
        ..Default::default()
    });
    let server = metadata.start().await;
    let client = FsClient::new(
        ClientConfig::builder()
            .metadata_endpoints([server.endpoint()])
            .max_attempts(1)
            .operation_timeout(Duration::from_secs(1))
            .build()
            .unwrap(),
    )
    .unwrap();
    let mut writer = client.create("/flush-deadline").await.unwrap();
    writer.write_all(b"abc").await.unwrap();
    let error = {
        let flush = writer.flush();
        tokio::pin!(flush);
        tokio::select! {
            result = &mut flush => panic!("checkpoint must wait: {result:?}"),
            result = tokio::time::timeout(Duration::from_secs(2), waiting) => result.unwrap().unwrap(),
        }
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(2)).await;
        let error = tokio::time::timeout(Duration::from_millis(50), flush)
            .await
            .expect("checkpoint must respect its deadline")
            .unwrap_err();
        tokio::time::resume();
        error
    };
    assert_eq!(error.kind(), ClientErrorKind::Timeout);
    assert!(error.is_outcome_unknown());
    assert_eq!(writer.position(), 3);
    let error = writer.close().await.unwrap_err();
    assert_eq!(error.kind(), ClientErrorKind::StaleHandle);
    assert!(error.message().contains("unknown outcome"));
    assert_methods(&metadata.calls(), &["CreateFile", "AllocateBlock"]);
    let _ = release.send(());
    drop(writer);
    server.shutdown().await;
    worker_server.shutdown().await;
}

async fn writer_barrier(writer: &mut beryl_client::FileWriter, method: &str) -> Result<(), ClientError> {
    match method {
        "SyncWrite" => writer.sync().await,
        "CommitFile" => writer.close().await,
        "AbortFileWrite" => writer.abort().await,
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn empty_ranges_bounds_and_local_seeks_do_not_perform_io() {
    use std::ops::Bound;
    let metadata = MockMetadata::new(MetadataScript {
        open_file: [open_file_response(202, 16), open_file_response(203, 0)]
            .into_iter()
            .map(MetadataReply::success)
            .collect(),
        ..Default::default()
    });
    let server = metadata.start().await;
    let config = ClientConfig::builder()
        .metadata_endpoints([server.endpoint()])
        .read_range_limit(8)
        .build()
        .unwrap();
    let client = FsClient::new(config).unwrap();
    let mut reader = client.open("/file").await.unwrap();
    for range in [0..0, 4..4, 16..16] {
        assert!(reader.read_range(range).await.unwrap().is_empty());
    }
    assert_eq!(
        reader.read_range(..).await.unwrap_err().kind(),
        ClientErrorKind::InvalidArgument
    );
    assert_eq!(
        reader.read_range(16..17).await.unwrap_err().kind(),
        ClientErrorKind::UnexpectedEof
    );
    assert_eq!(
        reader
            .read_range((Bound::Included(4), Bound::Excluded(3)))
            .await
            .unwrap_err()
            .kind(),
        ClientErrorKind::InvalidArgument
    );
    assert_eq!(
        reader.read_range(..=u64::MAX).await.unwrap_err().kind(),
        ClientErrorKind::InvalidArgument
    );
    assert_eq!(reader.read(&mut []).await.unwrap(), 0);
    assert_eq!(reader.seek(SeekFrom::End(-3)).await.unwrap(), 13);
    assert_eq!(
        reader.seek(SeekFrom::Current(-14)).await.unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
    assert_eq!(reader.position(), 13);
    reader.seek(SeekFrom::Start(u64::MAX)).await.unwrap();
    assert_eq!(
        reader.seek(SeekFrom::Current(1)).await.unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
    let mut byte = [42];
    assert_eq!(reader.read(&mut byte).await.unwrap(), 0);
    assert_eq!(byte, [42]);
    assert_eq!(reader.position(), u64::MAX);
    let mut empty = client.open("/empty").await.unwrap();
    assert!(empty.read_range(..).await.unwrap().is_empty());
    assert_eq!(empty.read(&mut byte).await.unwrap(), 0);
    assert_eq!(metadata.calls().len(), 2);
    server.shutdown().await;
}

#[tokio::test]
async fn cold_and_warm_streams_deliver_current_block_before_later_failure() {
    for warm in [false, true] {
        for operation in ["read_exact", "read_to_end", "read_range"] {
            let worker = MockWorker::new(WorkerScript {
                reads: (0..if warm { 2 } else { 1 })
                    .map(|_| ReadReply::Data(Bytes::from_static(b"abcd")))
                    .collect(),
                ..Default::default()
            });
            let worker_server = worker.start().await;
            let metadata = MockMetadata::new(MetadataScript {
                open_file: VecDeque::from([MetadataReply::success(open_file_response(202, 8))]),
                get_block_locations: VecDeque::from([
                    MetadataReply::success(locations_response(
                        202,
                        8,
                        block_location(202, 0, 0, 4, worker_server.endpoint()),
                    )),
                    MetadataReply::status(Status::unavailable("later block unavailable")),
                ]),
                ..Default::default()
            });
            let server = metadata.start().await;
            let client = FsClient::new(client_config(server.endpoint(), 1)).unwrap();
            let mut reader = client.open("/file").await.unwrap();
            if warm {
                assert_eq!(reader.read_range(..=0).await.unwrap(), b"a"[..]);
            }
            let error = if operation == "read_exact" {
                let mut output = [0; 8];
                let error = reader.read_exact(&mut output).await.unwrap_err();
                assert_eq!(&output[..4], b"abcd");
                assert_eq!(&output[4..], &[0; 4]);
                error
            } else if operation == "read_range" {
                std::io::Error::from(reader.read_range(..).await.unwrap_err())
            } else {
                let mut output = Vec::new();
                let error = reader.read_to_end(&mut output).await.unwrap_err();
                assert_eq!(output, b"abcd");
                error
            };
            assert_eq!(reader.position(), if operation == "read_range" { 0 } else { 4 });
            assert_eq!(
                error.get_ref().unwrap().downcast_ref::<ClientError>().unwrap().kind(),
                ClientErrorKind::Unavailable
            );
            let requests = worker.read_requests();
            let range = requests.last().unwrap().byte_range.as_ref().unwrap();
            assert_eq!((range.offset, range.len), (0, 4));
            assert_eq!(
                metadata
                    .layout_requests()
                    .iter()
                    .map(|r| (r.range.as_ref().unwrap().offset, r.range.as_ref().unwrap().len))
                    .collect::<Vec<_>>(),
                [(0, 1), (4, 1)]
            );
            server.shutdown().await;
            worker_server.shutdown().await;
        }
    }
}

#[tokio::test]
async fn cancelled_waits_retain_io_and_bytes_while_seek_and_drop_release_them() {
    let (started, waiting) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let (seek_started, seek_waiting) = tokio::sync::oneshot::channel();
    let (mut seek_release, seek_released) = tokio::sync::oneshot::channel();
    let (drop_started, drop_waiting) = tokio::sync::oneshot::channel();
    let (mut drop_release, drop_released) = tokio::sync::oneshot::channel();
    let worker = MockWorker::new(WorkerScript {
        reads: VecDeque::from([
            ReadReply::BlockedEnd {
                data: Bytes::from_static(b"abcd"),
                started,
                release: released,
            },
            ReadReply::BlockedEnd {
                data: Bytes::from_static(b"abcd"),
                started: seek_started,
                release: seek_released,
            },
            ReadReply::Data(Bytes::from_static(b"abcdefgh")),
            ReadReply::BlockedEnd {
                data: Bytes::from_static(b"abcd"),
                started: drop_started,
                release: drop_released,
            },
        ]),
        ..Default::default()
    });
    let worker_server = worker.start().await;
    let metadata = MockMetadata::new(MetadataScript {
        open_file: VecDeque::from([MetadataReply::success(open_file_response(202, 8))]),
        get_block_locations: VecDeque::from([MetadataReply::success(locations_response(
            202,
            8,
            block_location(202, 0, 0, 8, worker_server.endpoint()),
        ))]),
        ..Default::default()
    });
    let server = metadata.start().await;
    let client = FsClient::new(client_config(server.endpoint(), 1)).unwrap();
    let mut reader = client.open("/file").await.unwrap();
    let mut output = [42; 4];
    tokio::select! {
        result = reader.read(&mut output) => panic!("must wait for normal stream end: {result:?}"),
        result = waiting => result.unwrap(),
    }
    assert_eq!(output, [42; 4]);
    assert_eq!(reader.position(), 0);
    assert_eq!(
        reader.seek(SeekFrom::Current(-1)).await.unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
    let mut small = [0; 2];
    assert!(futures::poll!(reader.read(&mut small)).is_pending());
    assert_eq!(worker.read_calls(), 1);
    release.send(()).unwrap();
    assert_eq!(reader.read(&mut small).await.unwrap(), 2);
    assert_eq!(&small, b"ab");
    assert_eq!(reader.position(), 2);
    assert_eq!(
        reader.seek(SeekFrom::End(-9)).await.unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
    let mut byte = [0];
    assert_eq!(reader.read(&mut byte).await.unwrap(), 1);
    assert_eq!(&byte, b"c");
    assert_eq!(reader.position(), 3);
    assert_eq!(worker.read_calls(), 1, "remaining owned bytes need no further IO");
    // One unconsumed byte remains; the following seek must discard it.

    reader.seek(SeekFrom::Start(0)).await.unwrap();
    tokio::select! {
        result = reader.read(&mut output) => panic!("must remain pending: {result:?}"),
        result = seek_waiting => result.unwrap(),
    }
    reader.seek(SeekFrom::Start(6)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), seek_release.closed())
        .await
        .unwrap();
    reader.read_exact(&mut small).await.unwrap();
    assert_eq!(&small, b"gh");
    assert_eq!(reader.position(), 8);
    assert_eq!(worker.read_calls(), 3);

    reader.seek(SeekFrom::Start(0)).await.unwrap();
    tokio::select! {
        result = reader.read(&mut output) => panic!("must remain pending: {result:?}"),
        result = drop_waiting => result.unwrap(),
    }
    drop(reader);
    tokio::time::timeout(Duration::from_secs(2), drop_release.closed())
        .await
        .unwrap();
    assert_eq!(
        metadata.layout_requests().len(),
        1,
        "seek is local and preserves layout cache"
    );
    server.shutdown().await;
    worker_server.shutdown().await;
}

#[tokio::test]
async fn malformed_worker_streams_never_deliver_data_or_eof() {
    for (chunks, expected) in [
        (vec![], ClientErrorKind::InvalidResponse),
        (vec![Ok(Bytes::from_static(b"abc"))], ClientErrorKind::InvalidResponse),
        (vec![Ok(Bytes::from_static(b"abcde"))], ClientErrorKind::InvalidResponse),
        (vec![Ok(Bytes::new())], ClientErrorKind::InvalidResponse),
        (
            vec![Ok(Bytes::from_static(b"abcd")), Ok(Bytes::from_static(b"e"))],
            ClientErrorKind::InvalidResponse,
        ),
        (
            vec![Ok(Bytes::from_static(b"abcd")), Err(Status::cancelled("abnormal end"))],
            ClientErrorKind::Cancelled,
        ),
    ] {
        let worker = MockWorker::new(WorkerScript {
            reads: VecDeque::from([ReadReply::Chunks(chunks)]),
            ..Default::default()
        });
        let worker_server = worker.start().await;
        let metadata = MockMetadata::new(MetadataScript {
            open_file: VecDeque::from([MetadataReply::success(open_file_response(202, 4))]),
            get_block_locations: VecDeque::from([MetadataReply::success(locations_response(
                202,
                4,
                block_location(202, 0, 0, 4, worker_server.endpoint()),
            ))]),
            ..Default::default()
        });
        let server = metadata.start().await;
        let client = FsClient::new(client_config(server.endpoint(), 1)).unwrap();
        let mut reader = client.open("/file").await.unwrap();
        let mut output = [42; 4];
        let error = reader.read(&mut output).await.unwrap_err();
        assert_eq!(
            error.get_ref().unwrap().downcast_ref::<ClientError>().unwrap().kind(),
            expected
        );
        assert!(!matches!(
            error.kind(),
            std::io::ErrorKind::Interrupted | std::io::ErrorKind::WouldBlock
        ));
        assert_eq!(output, [42; 4]);
        assert_eq!(reader.position(), 0);
        assert_eq!(worker.read_calls(), 1);
        server.shutdown().await;
        worker_server.shutdown().await;
    }
}

#[tokio::test]
async fn complete_range_shares_deadline_across_chunks_and_retries() {
    let worker = MockWorker::new(WorkerScript {
        reads: VecDeque::from([
            ReadReply::Chunks(vec![Ok(Bytes::from_static(b"a")), Ok(Bytes::from_static(b"bc"))]),
            ReadReply::RefreshMetadata,
            ReadReply::Data(Bytes::from_static(b"abcd")),
            ReadReply::Data(Bytes::from_static(b"efgh")),
            ReadReply::Data(Bytes::from_static(b"efgh")),
        ]),
        ..Default::default()
    });
    let worker_server = worker.start().await;
    let metadata = MockMetadata::new(MetadataScript {
        open_file: VecDeque::from([MetadataReply::success(open_file_response(202, 8))]),
        get_block_locations: [0, 0, 4]
            .into_iter()
            .map(|offset| {
                MetadataReply::success(locations_response(
                    202,
                    8,
                    block_location(202, (offset / 4) as u32, offset, 4, worker_server.endpoint()),
                ))
            })
            .collect(),
        ..Default::default()
    });
    let server = metadata.start().await;
    let config = ClientConfig::builder()
        .metadata_endpoints([server.endpoint()])
        .max_read_step_bytes(3)
        .max_attempts(2)
        .build()
        .unwrap();
    let client = FsClient::new(config).unwrap();
    let reader = client.open("/file").await.unwrap();
    assert_eq!(reader.read_range(..).await.unwrap(), b"abcdefgh"[..]);
    assert_eq!(reader.position(), 0);
    let requests = worker.read_requests();
    assert_eq!(requests.len(), 5);
    let deadline = metadata.layout_requests()[0].header.as_ref().unwrap().deadline_ms;
    for request in &requests {
        assert_eq!(request.group_name, "root");
        assert!(request.byte_range.as_ref().unwrap().len <= 3);
    }
    for request in metadata.layout_requests() {
        assert_eq!(request.header.unwrap().deadline_ms, deadline);
    }
    server.shutdown().await;
    worker_server.shutdown().await;
}

#[tokio::test]
async fn cancelling_a_wait_does_not_reset_its_deadline() {
    let (started, waiting) = tokio::sync::oneshot::channel();
    let (mut release, released) = tokio::sync::oneshot::channel();
    let worker = MockWorker::new(WorkerScript {
        reads: VecDeque::from([
            ReadReply::BlockedEnd {
                data: Bytes::from_static(b"abcd"),
                started,
                release: released,
            },
            ReadReply::Data(Bytes::from_static(b"abcd")),
        ]),
        ..Default::default()
    });
    let worker_server = worker.start().await;
    let metadata = MockMetadata::new(MetadataScript {
        open_file: VecDeque::from([MetadataReply::success(open_file_response(202, 4))]),
        get_block_locations: (0..2)
            .map(|_| {
                MetadataReply::success(locations_response(
                    202,
                    4,
                    block_location(202, 0, 0, 4, worker_server.endpoint()),
                ))
            })
            .collect(),
        ..Default::default()
    });
    let server = metadata.start().await;
    let config = ClientConfig::builder()
        .metadata_endpoints([server.endpoint()])
        .operation_timeout(Duration::from_secs(1))
        .build()
        .unwrap();
    let client = FsClient::new(config).unwrap();
    let mut reader = client.open("/file").await.unwrap();
    let mut output = [42; 4];
    tokio::select! {
        result = reader.read(&mut output) => panic!("must remain pending: {result:?}"),
        result = waiting => result.unwrap(),
    }
    let deadline = metadata.layout_requests()[0].header.as_ref().unwrap().deadline_ms as u64;
    tokio::time::sleep(Duration::from_millis(deadline.saturating_sub(unix_now_ms()) + 25)).await;
    let error = reader.read(&mut output).await.unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
    assert_eq!(
        error.get_ref().unwrap().downcast_ref::<ClientError>().unwrap().kind(),
        ClientErrorKind::Timeout
    );
    assert_eq!(reader.position(), 0);
    assert_eq!(output, [42; 4]);
    assert_eq!(worker.read_calls(), 1);
    tokio::time::timeout(Duration::from_secs(2), release.closed())
        .await
        .unwrap();
    reader.read_exact(&mut output).await.unwrap();
    assert_eq!(&output, b"abcd");
    assert_eq!(
        metadata.layout_requests().len(),
        2,
        "deadline exhaustion allows a fresh layout"
    );
    server.shutdown().await;
    worker_server.shutdown().await;
}

fn client_config(metadata_endpoint: &str, max_attempts: usize) -> ClientConfig {
    ClientConfig::builder()
        .client_name("public-client-contract")
        .metadata_endpoints([metadata_endpoint])
        .operation_timeout(Duration::from_secs(2))
        .max_attempts(max_attempts)
        .build()
        .expect("client config")
}

fn status_response(size: u64) -> GetStatusResponseProto {
    GetStatusResponseProto {
        status: Some(file_status(1, size)),
        ..GetStatusResponseProto::default()
    }
}

fn create_response(inode_id: u64, block_size: u32) -> CreateFileResponseProto {
    CreateFileResponseProto {
        block_size,
        write_handle: Some(write_handle(inode_id)),
        expires_at_ms: unix_now_ms() + 60_000,
        generation: 0,
        ..CreateFileResponseProto::default()
    }
}

fn open_file_response(inode_id: u64, file_len: u64) -> OpenFileResponseProto {
    OpenFileResponseProto {
        status: Some(file_status(inode_id, file_len)),
        ..OpenFileResponseProto::default()
    }
}

fn locations_response(
    inode_id: u64,
    file_len: u64,
    location: FileBlockLocationProto,
) -> GetBlockLocationsResponseProto {
    GetBlockLocationsResponseProto {
        status: Some(file_status(inode_id, file_len)),
        locations: vec![location],
        ..GetBlockLocationsResponseProto::default()
    }
}

fn block_location(
    inode_id: u64,
    block_index: u32,
    file_offset: u64,
    len: u64,
    worker_endpoint: &str,
) -> FileBlockLocationProto {
    FileBlockLocationProto {
        block_id: Some(BlockIdProto { inode_id, block_index }),
        file_offset,
        len,
        workers: vec![worker(worker_endpoint)],
        block_size: 64 * 1024 * 1024,
        effective_len: len,
    }
}

fn write_target(
    inode_id: u64,
    block_index: u32,
    file_offset: u64,
    worker_endpoint: &str,
    block_size: u64,
) -> LocatedBlockProto {
    let block_id = BlockIdProto { inode_id, block_index };
    LocatedBlockProto {
        write_offset: 0,
        block_id: Some(block_id),
        file_offset,

        block_size,

        workers: vec![worker(worker_endpoint)],
        fencing_token: Some(FencingTokenProto {
            block_id: Some(block_id),
            owner: Some(ClientIdProto { high: 0, low: 7 }),
            epoch: 1,
        }),
        tier: TierProto::TierHdd as i32,
    }
}

fn worker(endpoint: &str) -> WorkerEndpointInfoProto {
    WorkerEndpointInfoProto {
        worker_id: 1,
        endpoint: endpoint.to_string(),
        worker_run_id: WORKER_RUN_ID.to_string(),
    }
}

fn write_handle(inode_id: u64) -> WriteHandleProto {
    WriteHandleProto {
        inode_id,
        write_lease_epoch: 1,
    }
}

fn watermark(index: u64) -> GroupStateWatermarkProto {
    GroupStateWatermarkProto {
        state_id: Some(RaftLogIdProto {
            term: 1,
            leader_node_id: 1,
            index,
        }),
        group_name: "root".to_string(),
    }
}

fn pre_handler_rejection() -> Status {
    let mut metadata = MetadataMap::new();
    metadata.insert(
        HEADER_PRE_HANDLER_REJECTION,
        MetadataValue::from_static(PRE_HANDLER_REJECTION_RPC_CONCURRENCY),
    );
    Status::with_metadata(
        Code::ResourceExhausted,
        "scripted Metadata capacity rejection",
        metadata,
    )
}

fn unix_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after Unix epoch")
        .as_millis() as u64
}

fn call_id(call: &MetadataCall) -> &str {
    &call.header.client.as_ref().expect("request client").call_id
}

fn calls_for(calls: &[MetadataCall], method: &str) -> Vec<MetadataCall> {
    calls.iter().filter(|call| call.method == method).cloned().collect()
}

fn assert_methods(calls: &[MetadataCall], expected: &[&str]) {
    assert_eq!(calls.iter().map(|call| call.method).collect::<Vec<_>>(), expected);
}

fn assert_same_call_id(calls: &[MetadataCall]) {
    assert!(!calls.is_empty());
    assert!(calls.iter().all(|call| call_id(call) == call_id(&calls[0])));
}

fn assert_same_identity_and_deadline(calls: &[MetadataCall]) {
    assert!(!calls.is_empty());
    let first = &calls[0];
    assert!(calls
        .iter()
        .all(|call| { call_id(call) == call_id(first) && call.header.deadline_ms == first.header.deadline_ms }));
}

fn assert_client_error(error: &ClientError, kind: ClientErrorKind, unknown: bool, message: &str) {
    assert_eq!(error.kind(), kind);
    assert_eq!(error.is_outcome_unknown(), unknown);
    assert!(error.message().contains(message), "unexpected error: {error:?}");
}

fn file_status(inode_id: u64, len: u64) -> beryl_proto::metadata::FileStatusProto {
    beryl_proto::metadata::FileStatusProto {
        inode_id,
        len,
        generation: Some(3),
        kind: FileTypeProto::FileTypeFile as i32,
        create_time: 11,
        modify_time: 12,
    }
}
