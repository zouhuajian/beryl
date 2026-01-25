// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use common::header::RequestHeader;
use metadata::readiness::RootReadinessGate;
use metadata::service::MetadataFsServiceImpl;
use metadata::state::MemoryStateStore;
use metadata::MountTable;
use proto::metadata::metadata_fs_service_proto_client::MetadataFsServiceProtoClient;
use proto::metadata::metadata_fs_service_proto_server::MetadataFsServiceProtoServer;
use proto::metadata::GetAttrRequestProto;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::server::NamedService;
use tonic::transport::{Endpoint, Server};
use tonic::Request;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::server::health_reporter;
use types::ClientId;

#[tokio::test]
async fn readiness_gate_blocks_rpc_until_ready() {
    let dir = TempDir::new().unwrap();
    let storage = std::sync::Arc::new(metadata::raft::RocksDBStorage::open(dir.path()).unwrap());
    let mount_table = std::sync::Arc::new(MountTable::load_from_storage(&storage).unwrap());
    let readiness_gate = std::sync::Arc::new(RootReadinessGate::new(None));

    let fs_service = MetadataFsServiceImpl::new(
        std::sync::Arc::new(MemoryStateStore::new()),
        std::sync::Arc::clone(&mount_table),
    )
    .with_storage(std::sync::Arc::clone(&storage))
    .with_readiness_gate(std::sync::Arc::clone(&readiness_gate));

    let (health_reporter, health_service) = health_reporter();
    health_reporter
        .set_not_serving::<MetadataFsServiceProtoServer<MetadataFsServiceImpl>>()
        .await;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        Server::builder()
            .add_service(health_service)
            .add_service(MetadataFsServiceProtoServer::new(fs_service))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap();
    });

    let channel = Endpoint::from_shared(format!("http://{}", addr))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut health_client = HealthClient::new(channel);
    let health_resp = health_client
        .check(Request::new(HealthCheckRequest {
            service: MetadataFsServiceProtoServer::<MetadataFsServiceImpl>::NAME.to_string(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(health_resp.status, ServingStatus::NotServing as i32);

    let fs_channel = Endpoint::from_shared(format!("http://{}", addr))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut fs_client = MetadataFsServiceProtoClient::new(fs_channel);
    let req = GetAttrRequestProto {
        header: Some((&RequestHeader::new(ClientId::new(1))).into()),
        inode_id: Some(proto::fs::InodeIdProto { value: 1 }),
    };
    let resp = fs_client.get_attr(Request::new(req)).await.unwrap().into_inner();
    let err = resp.header.and_then(|h| h.error).expect("expected error");
    assert_eq!(
        err.error_class,
        proto::common::ErrorClassProto::ErrorClassRetryable as i32
    );
    match err.code {
        Some(proto::common::error_detail_proto::Code::RpcCode(code)) => {
            assert_eq!(code, proto::common::RpcErrorCodeProto::RpcErrCodeNodeUnavailable as i32);
        }
        _ => panic!("expected RpcCode"),
    }

    readiness_gate.set_ready();
    health_reporter
        .set_serving::<MetadataFsServiceProtoServer<MetadataFsServiceImpl>>()
        .await;

    let health_resp = health_client
        .check(Request::new(HealthCheckRequest {
            service: MetadataFsServiceProtoServer::<MetadataFsServiceImpl>::NAME.to_string(),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(health_resp.status, ServingStatus::Serving as i32);

    let _ = shutdown_tx.send(());
}
