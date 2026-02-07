// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Entrypoint exposure hardening tests.
//!
//! Verifies that external clients can reach FileSystemService while inode service
//! exposure is explicitly gated.

mod common;

use common::FsTestHarness;
use metadata::service::{MetadataFileSystemServiceImpl, MetadataFsServiceImpl};
use proto::metadata::file_system_service_proto_client::FileSystemServiceProtoClient;
use proto::metadata::file_system_service_proto_server::FileSystemServiceProtoServer;
use proto::metadata::metadata_fs_service_proto_client::MetadataFsServiceProtoClient;
use proto::metadata::metadata_fs_service_proto_server::MetadataFsServiceProtoServer;
use proto::metadata::{GetFileStatusRequestProto, LookupRequestProto};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Endpoint, Server};
use tonic::{Code, Request};
use types::ids::ShardGroupId;

struct ServiceHarness {
    _fs_harness: FsTestHarness,
    filesystem_service: MetadataFileSystemServiceImpl,
    inode_service: MetadataFsServiceImpl,
    root_inode_id: u64,
}

async fn build_service_harness() -> ServiceHarness {
    let fs_harness = FsTestHarness::new().await.unwrap();
    let (_mount_id, root_inode_id) = fs_harness
        .create_mount_with_root(
            "/mnt/test".to_string(),
            "file:///tmp/test".to_string(),
            ShardGroupId::new(1),
        )
        .await
        .unwrap();

    let metrics = Arc::new(metadata::metrics::MetadataMetrics::new());

    let inode_service = MetadataFsServiceImpl::new(
        fs_harness.state_store.clone() as Arc<dyn metadata::state::StateStore>,
        fs_harness.mount_table.clone(),
    )
    .with_storage(fs_harness.storage.clone())
    .with_raft_node(fs_harness.raft_node.clone())
    .with_metrics(metrics.clone());

    let fs_service_for_filesystem = MetadataFsServiceImpl::new(
        fs_harness.state_store.clone() as Arc<dyn metadata::state::StateStore>,
        fs_harness.mount_table.clone(),
    )
    .with_storage(fs_harness.storage.clone())
    .with_raft_node(fs_harness.raft_node.clone())
    .with_metrics(metrics.clone());
    let fs_core = fs_service_for_filesystem.fs_core();

    let filesystem_service =
        MetadataFileSystemServiceImpl::new(fs_harness.mount_table.clone(), fs_harness.storage.clone(), fs_core)
            .with_metrics(metrics);

    ServiceHarness {
        _fs_harness: fs_harness,
        filesystem_service,
        inode_service,
        root_inode_id: root_inode_id.as_raw(),
    }
}

async fn spawn_server(
    enable_inode_service: bool,
    harness: ServiceHarness,
) -> (std::net::SocketAddr, oneshot::Sender<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let ServiceHarness {
        _fs_harness: harness_guard,
        filesystem_service,
        inode_service,
        ..
    } = harness;

    tokio::spawn(async move {
        let _harness_guard = harness_guard;
        if enable_inode_service {
            Server::builder()
                .add_service(FileSystemServiceProtoServer::new(filesystem_service))
                .add_service(MetadataFsServiceProtoServer::new(inode_service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_rx.await;
                })
                .await
                .unwrap();
        } else {
            let _inode_service = inode_service;
            Server::builder()
                .add_service(FileSystemServiceProtoServer::new(filesystem_service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_rx.await;
                })
                .await
                .unwrap();
        }
    });

    (addr, shutdown_tx)
}

#[tokio::test]
async fn inode_service_disabled_by_default_not_registered() {
    let harness = build_service_harness().await;
    let root_inode_id = harness.root_inode_id;
    let (addr, shutdown_tx) = spawn_server(false, harness).await;

    let channel = Endpoint::from_shared(format!("http://{}", addr))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut filesystem_client = FileSystemServiceProtoClient::new(channel);
    let fs_resp = filesystem_client
        .get_file_status(Request::new(GetFileStatusRequestProto {
            header: FsTestHarness::create_test_request_header(),
            path: "/mnt/test/missing.txt".to_string(),
        }))
        .await
        .expect("filesystem service must be reachable")
        .into_inner();
    assert!(fs_resp.header.is_some());

    let channel = Endpoint::from_shared(format!("http://{}", addr))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut inode_client = MetadataFsServiceProtoClient::new(channel);
    let inode_result = inode_client
        .lookup(Request::new(LookupRequestProto {
            header: FsTestHarness::create_test_request_header(),
            parent_inode_id: Some(proto::fs::InodeIdProto { value: root_inode_id }),
            name: "missing.txt".to_string(),
        }))
        .await;
    let status = inode_result.expect_err("inode service should not be registered");
    assert!(
        status.code() == Code::Unimplemented || status.code() == Code::NotFound,
        "expected transport-level service absence, got {:?}: {}",
        status.code(),
        status.message()
    );

    let _ = shutdown_tx.send(());
}

#[tokio::test]
async fn inode_service_enabled_is_registered() {
    let harness = build_service_harness().await;
    let root_inode_id = harness.root_inode_id;
    let (addr, shutdown_tx) = spawn_server(true, harness).await;

    let channel = Endpoint::from_shared(format!("http://{}", addr))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut inode_client = MetadataFsServiceProtoClient::new(channel);
    let resp = inode_client
        .lookup(Request::new(LookupRequestProto {
            header: FsTestHarness::create_test_request_header(),
            parent_inode_id: Some(proto::fs::InodeIdProto { value: root_inode_id }),
            name: "missing.txt".to_string(),
        }))
        .await
        .expect("inode service should be reachable when enabled")
        .into_inner();
    assert!(resp.header.is_some());

    let _ = shutdown_tx.send(());
}
