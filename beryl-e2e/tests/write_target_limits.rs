// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, RecoveryAction, RpcErrorDetail};
use beryl_common::header::RequestHeader;
use beryl_e2e::TestCluster;
use beryl_proto::common::{RequestHeaderProto, ResponseHeaderProto};
use beryl_proto::convert::rpc_error_from_proto;
use beryl_proto::metadata::file_system_service_proto_client::FileSystemServiceProtoClient;
use beryl_proto::metadata::{
    AbortFileWriteRequestProto, AllocateBlockRequestProto, CreateFileRequestProto, LocatedBlockProto,
    OpenWriteModeProto, OpenWriteRequestProto, WriteHandleProto,
};
use beryl_types::ClientId;
use tonic::transport::Channel;
use tonic::Request;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn low_target_limits_reject_before_raft_and_release_on_abort() {
    let mut cluster = TestCluster::start_with_write_target_limits(2, 1)
        .await
        .expect("start cluster");
    cluster
        .start_metadata_process(std::path::Path::new(env!("CARGO_BIN_EXE_metadata-e2e-server")))
        .await
        .expect("start production Metadata runtime with low target limits");
    let mut metadata = FileSystemServiceProtoClient::connect(cluster.metadata_endpoint())
        .await
        .expect("connect Metadata");

    let first_handle = create(&mut metadata, "/target-limit-a", 701).await;
    let first_target = allocate_block(&mut metadata, first_handle, None, 701)
        .await
        .expect("first target");
    let first_block = first_target.block_id.expect("first block id");
    let per_session = allocate_block(&mut metadata, first_handle, Some(first_block), 701)
        .await
        .expect_err("per-session limit must reject the second target");
    assert_resource_exhausted(&per_session, false);

    abort(&mut metadata, first_handle, 701).await;
    let reopened_handle = open(&mut metadata, "/target-limit-a", 701).await;
    let reopened_target = allocate_block(&mut metadata, reopened_handle, None, 701)
        .await
        .expect("target after abort");
    assert_eq!(
        reopened_target
            .block_id
            .as_ref()
            .expect("reopened block id")
            .block_index,
        first_block.block_index + 1,
        "per-session rejection must not allocate a block index"
    );

    let second_handle = create(&mut metadata, "/target-limit-b", 701).await;
    allocate_block(&mut metadata, second_handle, None, 701)
        .await
        .expect("second session target");
    let third_handle = create(&mut metadata, "/target-limit-c", 701).await;
    let global = allocate_block(&mut metadata, third_handle, None, 701)
        .await
        .expect_err("global target limit must reject the third session");
    assert_resource_exhausted(&global, true);

    let replay = allocate_block(&mut metadata, reopened_handle, None, 701)
        .await
        .expect("issued predecessor must replay while global capacity is full");
    assert_eq!(replay, reopened_target);

    abort(&mut metadata, second_handle, 701).await;
    let third_target = allocate_block(&mut metadata, third_handle, None, 701)
        .await
        .expect("target after global capacity is released");
    assert_eq!(
        third_target.block_id.expect("third block id").block_index,
        0,
        "global rejection must happen before the file's first block allocation"
    );

    abort(&mut metadata, reopened_handle, 701).await;
    abort(&mut metadata, third_handle, 701).await;
    cluster.shutdown().await.expect("shutdown cluster");
}

async fn create(metadata: &mut FileSystemServiceProtoClient<Channel>, path: &str, client_id: u64) -> WriteHandleProto {
    let create = metadata
        .create_file(Request::new(CreateFileRequestProto {
            header: Some(metadata_header(client_id)),
            path: path.to_string(),
        }))
        .await
        .expect("CreateFile transport")
        .into_inner();
    assert_metadata_ok(create.header);
    create.write_handle.expect("write handle")
}

async fn open(metadata: &mut FileSystemServiceProtoClient<Channel>, path: &str, client_id: u64) -> WriteHandleProto {
    let response = metadata
        .open_write(Request::new(OpenWriteRequestProto {
            header: Some(metadata_header(client_id)),
            path: path.to_string(),
            mode: OpenWriteModeProto::OpenWriteModeWrite as i32,
        }))
        .await
        .expect("OpenWrite transport")
        .into_inner();
    assert_metadata_ok(response.header);
    response.write_handle.expect("write handle")
}

async fn allocate_block(
    metadata: &mut FileSystemServiceProtoClient<Channel>,
    write_handle: WriteHandleProto,
    previous_block_id: Option<beryl_proto::common::BlockIdProto>,
    client_id: u64,
) -> Result<LocatedBlockProto, RpcErrorDetail> {
    let response = metadata
        .allocate_block(Request::new(AllocateBlockRequestProto {
            header: Some(metadata_header(client_id)),
            write_handle: Some(write_handle),
            previous_block_id,
        }))
        .await
        .expect("AllocateBlock transport")
        .into_inner();
    let header = response.header.expect("AllocateBlock response header");
    match header.error {
        Some(error) => Err(rpc_error_from_proto(&error)),
        None => Ok(response.block.expect("successful AllocateBlock target")),
    }
}

async fn abort(metadata: &mut FileSystemServiceProtoClient<Channel>, write_handle: WriteHandleProto, client_id: u64) {
    let response = metadata
        .abort_file_write(Request::new(AbortFileWriteRequestProto {
            header: Some(metadata_header(client_id)),
            write_handle: Some(write_handle),
        }))
        .await
        .expect("AbortFileWrite transport")
        .into_inner();
    assert_metadata_ok(response.header);
}

fn assert_resource_exhausted(error: &RpcErrorDetail, retryable: bool) {
    assert_eq!(error.kind, ErrorKind::Metadata(MetadataErrorKind::ResourceExhausted));
    if retryable {
        assert!(matches!(error.recovery, RecoveryAction::Retry { .. }));
    } else {
        assert_eq!(error.recovery, RecoveryAction::Fail);
    }
}

fn assert_metadata_ok(header: Option<ResponseHeaderProto>) {
    assert!(header.expect("response header").error.is_none());
}

fn metadata_header(client_id: u64) -> RequestHeaderProto {
    (&RequestHeader::new(ClientId::new(u128::from(client_id)))).into()
}
