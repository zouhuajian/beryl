// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use beryl_client::{ClientError, CreateOptions, FileStatus};
use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, RecoveryAction};
use beryl_common::header::RequestHeader;
use beryl_e2e::{data::deterministic_bytes, TestCluster, TestResult};
use beryl_proto::common::{ByteRangeProto, FileLayoutProto, RequestHeaderProto, ResponseHeaderProto};
use beryl_proto::convert::rpc_error_from_proto;
use beryl_proto::fs::FileAttrsProto;
use beryl_proto::metadata::file_system_service_proto_client::FileSystemServiceProtoClient;
use beryl_proto::metadata::get_block_locations_request_proto;
use beryl_proto::metadata::{
    AddBlockRequestProto, CommitFileRequestProto, CommittedBlockProto, CreateFileRequestProto, CreateModeProto,
    GetBlockLocationsRequestProto, WriteHandleProto, WriteTargetProto,
};
use beryl_proto::worker::worker_data_service_client::WorkerDataServiceClient;
use beryl_proto::worker::{
    ChecksumKindProto, CommitWriteRequestProto, DataRequestHeaderProto, DataResponseHeaderProto,
    OpenWriteStreamRequestProto, WriteStreamRequestProto,
};
use beryl_types::fs::FsErrorCode;
use beryl_types::{BlockFormatId, ClientId};
use bytes::Bytes;
use tokio_stream::iter;
use tonic::Request;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_visible_file_survives_metadata_restart() {
    let mut cluster = TestCluster::start().await.expect("start cluster");
    let client = cluster.client().clone();
    let path = "/restart/committed";
    let payload = Bytes::from(deterministic_bytes(1_537));
    let create_options = CreateOptions::overwrite().with_block_size(1024).with_chunk_size(1024);

    client.mkdirs("/restart", true).await.expect("create restart dir");
    let mut writer = client.create(path, create_options).await.expect("create file");
    writer.write_all(payload.clone()).await.expect("write file");
    writer.close().await.expect("close file");
    cluster
        .converge_block_reports()
        .await
        .expect("pre-restart report convergence");

    let before = client.open(path).await.expect("open before restart").read_all().await;
    assert_eq!(before.expect("read before restart"), payload);

    cluster.restart_metadata().await.expect("restart metadata");

    let after = client.open(path).await.expect("open after restart").read_all().await;
    assert_eq!(after.expect("read after restart"), payload);
    cluster.shutdown().await.expect("shutdown cluster");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_after_create_before_close_fails_stale_writer_without_publishing_bytes() {
    let mut cluster = TestCluster::start().await.expect("start cluster");
    let client = cluster.client().clone();
    client.mkdirs("/restart", true).await.expect("create restart dir");

    let mut writer = client
        .create(
            "/restart/create-before-close",
            CreateOptions::overwrite().with_block_size(1024).with_chunk_size(1024),
        )
        .await
        .expect("create active writer");

    cluster.restart_metadata().await.expect("restart metadata");

    let err = writer.close().await.expect_err("stale writer must fail closed");
    assert_stale_writer_error(&err);
    assert_no_committed_bytes(&cluster, "/restart/create-before-close")
        .await
        .expect("no committed bytes");
    cluster.shutdown().await.expect("shutdown cluster");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_after_add_block_before_worker_commit_rejects_stale_writer_and_hides_data() {
    let mut cluster = TestCluster::start().await.expect("start cluster");
    let client = cluster.client().clone();
    client.mkdirs("/restart", true).await.expect("create restart dir");

    let mut writer = client
        .create(
            "/restart/add-block-before-worker-commit",
            CreateOptions::overwrite().with_block_size(1024).with_chunk_size(1024),
        )
        .await
        .expect("create active writer");
    writer
        .write_all(Bytes::from(deterministic_bytes(1024)))
        .await
        .expect("stage worker block without metadata close");

    cluster.restart_metadata().await.expect("restart metadata");

    let err = writer.renew_lease().await.expect_err("stale writer must fail closed");
    assert_stale_writer_error(&err);
    assert_no_committed_bytes(&cluster, "/restart/add-block-before-worker-commit")
        .await
        .expect("no committed bytes");
    assert_no_metadata_locations(&cluster, "/restart/add-block-before-worker-commit", 1024)
        .await
        .expect("no metadata locations");
    cluster.shutdown().await.expect("shutdown cluster");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_after_worker_commit_before_metadata_commit_hides_uncommitted_block() {
    let mut cluster = TestCluster::start().await.expect("start cluster");
    let active = raw_create_commit_worker_block(&cluster, "/restart/worker-commit-no-metadata", b"worker-ready")
        .await
        .expect("commit worker block without CommitFile");
    assert_eq!(cluster.ready_block_count().expect("ready blocks before restart"), 1);

    cluster.restart_metadata().await.expect("restart metadata");

    assert_stale_commit_file(&cluster, active)
        .await
        .expect("stale CommitFile must fail");
    assert_eq!(cluster.ready_block_count().expect("ready blocks after restart"), 1);
    assert_no_committed_bytes(&cluster, "/restart/worker-commit-no-metadata")
        .await
        .expect("worker-only block not visible");
    assert_no_metadata_locations(&cluster, "/restart/worker-commit-no-metadata", 11)
        .await
        .expect("worker-only block has no metadata locations");
    cluster.shutdown().await.expect("shutdown cluster");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn existing_visible_data_remains_readable_while_active_write_fails_closed() {
    let mut cluster = TestCluster::start().await.expect("start cluster");
    let client = cluster.client().clone();
    client.mkdirs("/restart", true).await.expect("create restart dir");

    let visible_path = "/restart/existing-visible";
    let active_path = "/restart/active-hidden";
    let visible = Bytes::from_static(b"already-visible");
    let hidden = Bytes::from_static(b"hidden-after-restart");
    let create_options = CreateOptions::overwrite().with_block_size(1024).with_chunk_size(1024);

    let mut visible_writer = client
        .create(visible_path, create_options)
        .await
        .expect("create visible file");
    visible_writer
        .write_all(visible.clone())
        .await
        .expect("write visible file");
    visible_writer.close().await.expect("close visible file");
    cluster
        .converge_block_reports()
        .await
        .expect("visible report convergence");

    let mut active_writer = client
        .create(active_path, create_options)
        .await
        .expect("create active file");
    active_writer
        .write_all(hidden)
        .await
        .expect("write active file without close");

    cluster.restart_metadata().await.expect("restart metadata");

    let visible_after = client
        .open(visible_path)
        .await
        .expect("open visible after restart")
        .read_all()
        .await
        .expect("read visible after restart");
    assert_eq!(visible_after, visible);
    let err = active_writer.close().await.expect_err("active writer must fail closed");
    assert_stale_writer_error(&err);
    assert_no_committed_bytes(&cluster, active_path)
        .await
        .expect("active path has no committed bytes");
    cluster.shutdown().await.expect("shutdown cluster");
}

struct RawWorkerCommittedWrite {
    write_handle: WriteHandleProto,
    data_handle_id: beryl_proto::common::DataHandleIdProto,
    committed_block: CommittedBlockProto,
}

async fn raw_create_commit_worker_block(
    cluster: &TestCluster,
    path: &str,
    payload: &[u8],
) -> TestResult<RawWorkerCommittedWrite> {
    let client = cluster.client();
    client.mkdirs("/restart", true).await.expect("create restart dir");

    let mut metadata = FileSystemServiceProtoClient::connect(cluster.metadata_endpoint()).await?;
    let create = metadata
        .create_file(Request::new(CreateFileRequestProto {
            header: Some(metadata_header(401)),
            path: path.to_string(),
            attrs: Some(FileAttrsProto {
                mode: 0o644,
                uid: 1000,
                gid: 1000,
                ..Default::default()
            }),
            layout: Some(FileLayoutProto {
                block_size: 1024,
                chunk_size: 1024,
                replication: 1,
                block_format_id: BlockFormatId::CURRENT_FOR_NEW_FILE.as_raw(),
            }),
            create_mode: CreateModeProto::CreateNew as i32,
            desired_len: Some(payload.len() as u64),
        }))
        .await?
        .into_inner();
    assert_metadata_ok(create.header);
    let write_handle = create.write_handle.expect("write handle");
    let data_handle_id = create.data_handle_id.expect("data handle id");

    let add_block = metadata
        .add_block(Request::new(AddBlockRequestProto {
            header: Some(metadata_header(402)),
            write_handle: Some(write_handle),
            desired_len: Some(payload.len() as u64),
        }))
        .await?
        .into_inner();
    assert_metadata_ok(add_block.header);
    let target = add_block.target.expect("write target");
    write_and_commit_worker_target(&target, payload).await?;
    let committed_block = CommittedBlockProto {
        block_id: target.block_id,
        file_offset: target.file_offset,
        len: payload.len() as u64,
        checksum: None,
    };

    Ok(RawWorkerCommittedWrite {
        write_handle,
        data_handle_id,
        committed_block,
    })
}

async fn assert_stale_commit_file(cluster: &TestCluster, active: RawWorkerCommittedWrite) -> TestResult<()> {
    let mut metadata = FileSystemServiceProtoClient::connect(cluster.metadata_endpoint()).await?;
    let final_size = active.committed_block.len;
    let stale_commit = metadata
        .commit_file(Request::new(CommitFileRequestProto {
            header: Some(metadata_header(403)),
            write_handle: Some(active.write_handle),
            data_handle_id: Some(active.data_handle_id),
            committed_blocks: vec![active.committed_block],
            final_size,
        }))
        .await?
        .into_inner();
    let err = stale_commit
        .header
        .expect("commit response header")
        .error
        .expect("stale commit error");
    let rpc_error = rpc_error_from_proto(&err);
    assert_eq!(rpc_error.kind, ErrorKind::Metadata(MetadataErrorKind::SessionInvalid));
    assert!(matches!(rpc_error.recovery, RecoveryAction::ReopenWriteSession { .. }));
    Ok(())
}

async fn write_and_commit_worker_target(target: &WriteTargetProto, payload: &[u8]) -> TestResult<()> {
    let worker = target
        .worker_endpoints
        .first()
        .expect("metadata write target has worker")
        .clone();
    let endpoint = if worker.endpoint.starts_with("http://") || worker.endpoint.starts_with("https://") {
        worker.endpoint.clone()
    } else {
        format!("http://{}", worker.endpoint)
    };
    let mut worker_client = WorkerDataServiceClient::connect(endpoint).await?;
    let open = worker_client
        .open_write_stream(Request::new(OpenWriteStreamRequestProto {
            header: Some(data_header(501)),
            group_name: "root".to_string(),
            block_id: target.block_id,
            block_format_id: target.block_format_id,
            block_size: target.block_size,
            chunk_size: target.chunk_size,
            checksum_kind: ChecksumKindProto::ChecksumKindNone as i32,
            block_stamp: target.block_stamp,
            token: target.fencing_token,
            frame_size: payload.len().max(1) as u32,
            worker_run_id: worker.worker_run_id.clone(),
            effective_len: target.effective_len,
            tier: target.tier,
        }))
        .await?
        .into_inner();
    assert_worker_ok(open.header);
    let stream_id = open.stream_id.expect("stream id");
    let write = worker_client
        .write_stream(Request::new(iter(vec![WriteStreamRequestProto {
            stream_id: Some(stream_id),
            seq: 1,
            offset_in_block: 0,
            data: payload.to_vec().into(),
            checksum32: 0,
        }])))
        .await?
        .into_inner();
    assert_eq!(write.last_acked_seq, 1);
    assert_eq!(write.written_through, payload.len() as u64);

    let commit = worker_client
        .commit_write(Request::new(CommitWriteRequestProto {
            header: Some(data_header(502)),
            group_name: "root".to_string(),
            block_id: target.block_id,
            stream_id: Some(stream_id),
            effective_len: payload.len() as u64,
            block_stamp: target.block_stamp,
            token: target.fencing_token,
            commit_seq: 1,
            require_sync: false,
            worker_run_id: worker.worker_run_id,
            block_format_id: target.block_format_id,
            block_size: target.block_size,
            chunk_size: target.chunk_size,
        }))
        .await?
        .into_inner();
    assert_worker_ok(commit.header);
    assert_eq!(commit.effective_len, payload.len() as u64);
    assert_eq!(commit.block_stamp, target.block_stamp);
    Ok(())
}

async fn assert_no_committed_bytes(cluster: &TestCluster, path: &str) -> TestResult<()> {
    match cluster.client().stat(path).await {
        Ok(FileStatus { attrs, .. }) => {
            assert_eq!(attrs.size, 0, "{path} must not publish incomplete bytes");
        }
        Err(err) => assert_not_found(&err),
    }
    Ok(())
}

async fn assert_no_metadata_locations(cluster: &TestCluster, path: &str, len: u32) -> TestResult<()> {
    let mut metadata = FileSystemServiceProtoClient::connect(cluster.metadata_endpoint()).await?;
    let response = metadata
        .get_block_locations(Request::new(GetBlockLocationsRequestProto {
            header: Some(metadata_header(601)),
            target: Some(get_block_locations_request_proto::Target::Path(path.to_string())),
            range: Some(ByteRangeProto { offset: 0, len }),
        }))
        .await?
        .into_inner();
    assert_metadata_ok(response.header);
    assert_eq!(response.file_size, 0);
    assert!(response.locations.is_empty(), "{path} returned locations");
    Ok(())
}

fn metadata_header(client_id: u128) -> RequestHeaderProto {
    let mut header: RequestHeaderProto = (&RequestHeader::new(ClientId::new(client_id))).into();
    header.group_name = "root".to_string();
    header
}

fn data_header(client_id: u128) -> DataRequestHeaderProto {
    (&RequestHeader::new(ClientId::new(client_id))).into()
}

fn assert_metadata_ok(header: Option<ResponseHeaderProto>) {
    assert!(
        header.expect("metadata response header").error.is_none(),
        "metadata response must not carry business error"
    );
}

fn assert_worker_ok(header: Option<DataResponseHeaderProto>) {
    assert!(
        header.expect("worker response header").error.is_none(),
        "worker response must not carry business error"
    );
}

fn assert_stale_writer_error(err: &ClientError) {
    match err {
        ClientError::Action(action) => {
            assert!(matches!(
                action.kind(),
                Some(
                    ErrorKind::Metadata(MetadataErrorKind::SessionInvalid)
                        | ErrorKind::Metadata(MetadataErrorKind::SessionExpired)
                        | ErrorKind::Metadata(MetadataErrorKind::Fencing)
                        | ErrorKind::Metadata(MetadataErrorKind::EpochMismatch)
                )
            ));
            assert!(matches!(
                action.recovery(),
                Some(RecoveryAction::ReopenWriteSession { .. })
            ));
        }
        ClientError::StaleHandle { reason } => {
            assert!(
                reason.contains("invalid") || reason.contains("expired") || reason.contains("unknown"),
                "unexpected stale handle reason: {reason}"
            );
        }
        other => panic!("expected stale writer error, got {other:?}"),
    }
}

fn assert_not_found(err: &ClientError) {
    if let ClientError::Action(action) = err {
        if matches!(
            action.kind(),
            Some(ErrorKind::Fs(FsErrorCode::ENoEnt) | ErrorKind::Metadata(MetadataErrorKind::NotFound))
        ) {
            return;
        }
    }
    panic!("expected not found error, got {err:?}");
}
