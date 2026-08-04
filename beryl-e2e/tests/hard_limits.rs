// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use std::path::Path;

use beryl_e2e::TestCluster;
use beryl_proto::common::RequestHeaderProto;
use beryl_proto::metadata::file_system_service_proto_client::FileSystemServiceProtoClient;
use beryl_proto::metadata::metadata_worker_service_proto_client::MetadataWorkerServiceProtoClient;
use beryl_proto::metadata::{CommitFileRequestProto, RegisterWorkerRequestProto};
use tonic::{Code, Request};

const METADATA_REQUEST_LIMIT: usize = 4 * 1024 * 1024;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn production_metadata_services_reject_requests_above_four_mebibytes() {
    let mut cluster = TestCluster::start().await.expect("start cluster");
    cluster
        .start_metadata_process(Path::new(env!("CARGO_BIN_EXE_metadata-e2e-server")))
        .await
        .expect("start production metadata runtime");
    let endpoint = cluster.metadata_endpoint();
    let mut filesystem = FileSystemServiceProtoClient::connect(endpoint.clone())
        .await
        .expect("connect metadata filesystem service");
    let request = CommitFileRequestProto {
        header: Some(RequestHeaderProto {
            group_name: "x".repeat(METADATA_REQUEST_LIMIT + 1),
            ..Default::default()
        }),
        ..Default::default()
    };

    let error = filesystem
        .commit_file(Request::new(request))
        .await
        .expect_err("oversized filesystem request must be rejected by the transport");
    // Tonic rejects an oversized decoded message before dispatch with its
    // transport-level OutOfRange status.
    assert_eq!(error.code(), Code::OutOfRange);

    let mut worker = MetadataWorkerServiceProtoClient::connect(endpoint)
        .await
        .expect("connect metadata worker service");
    let error = worker
        .register_worker(Request::new(RegisterWorkerRequestProto {
            header: Some(RequestHeaderProto {
                group_name: "x".repeat(METADATA_REQUEST_LIMIT + 1),
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await
        .expect_err("oversized worker-control request must be rejected by the transport");
    assert_eq!(error.code(), Code::OutOfRange);

    cluster.shutdown().await.expect("shutdown cluster");
}
