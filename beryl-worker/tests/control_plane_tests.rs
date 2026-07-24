// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use beryl_common::error::rpc::{ErrorKind, InternalErrorKind, RefreshHint, RpcErrorDetail, WorkerErrorKind};
use beryl_proto::common::{EndpointProto, ResponseHeaderProto};
use beryl_proto::convert::rpc_error_to_proto;
use beryl_proto::metadata::metadata_worker_service_proto_server::{
    MetadataWorkerServiceProto, MetadataWorkerServiceProtoServer,
};
use beryl_proto::metadata::{
    BlockReportRequestProto, BlockReportResponseProto, HeartbeatRequestProto, HeartbeatResponseProto,
    RegisterWorkerRequestProto, RegisterWorkerResponseProto,
};
use beryl_types::fs::FsErrorCode;
use beryl_types::ids::{BlockId, BlockIndex, ClientId, DataHandleId, WorkerId};
use beryl_types::layout::BlockFormatId;
use beryl_types::{GroupName, Tier, TierFree, WorkerRunId};
use bytes::Bytes;
use tempfile::TempDir;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use beryl_worker::config::{StoreDirConfig, WorkerConfig, WorkerRegistrationConfig};
use beryl_worker::control::{
    BlockReportError, BlockReportOptions, HeartbeatError, HeartbeatSnapshot, MetadataBlockReportLoop,
    MetadataHeartbeatLoop, MetadataRegistrar, Registration, RegistrationDescriptor, RegistrationSet,
};
use beryl_worker::net::config::WorkerNetConfig;
use beryl_worker::net::protocol::WorkerNetProtocol;
use beryl_worker::store::block::{
    ChecksumKind, CreateStagingBlockRequest, FullBlockFileStore, FullBlockFileStoreConfig, LocalBlockStore,
    PublishReadyRequest,
};
use beryl_worker::store::dirs::{StoreDirs, StoreReport};

const BLOCK_SIZE: u64 = 4096;
const CHUNK_SIZE: u32 = 1024;

fn block_id() -> BlockId {
    BlockId::new(DataHandleId::new(7), BlockIndex::new(3))
}

fn group_name() -> GroupName {
    GroupName::parse("root").expect("test group name is valid")
}

fn test_worker_config() -> WorkerConfig {
    WorkerConfig {
        cluster_id: "local-beryl".to_string(),
        identity_path: std::path::PathBuf::from("data/worker/worker.identity"),
        rpc_bind: "0.0.0.0:9090".to_string(),
        rpc_advertised_endpoint: "http://127.0.0.1:9090".to_string(),
        rpc_max_inflight: 100,
        default_frame_size: 1024 * 1024,
        max_frame_size: 4 * 1024 * 1024,
        stream_idle_timeout_ms: 60_000,
        store: beryl_worker::config::WorkerStoreConfig::default(),
        net: WorkerNetConfig::grpc_from_rpc("0.0.0.0:9090".to_string(), 100, 4 * 1024 * 1024),
        metadata: WorkerRegistrationConfig::default(),
        observability: test_observability_config(),
    }
}

fn test_observability_config() -> beryl_common::observe::ObservabilityConfig {
    let mut flat = beryl_common::config::FlatConfig::new();
    flat.set("observe.log.format", "compact");
    flat.set("observe.log.output", "stderr");
    flat.set(
        "observe.log.level",
        "info,beryl_metadata=info,beryl_worker=info,beryl_common=info,openraft=warn,tonic=warn,tower=warn,h2=warn",
    );
    flat.set("observe.metrics.prometheus.bind", "127.0.0.1:19091");
    flat.set("observe.metrics.prometheus.path", "/metrics");
    beryl_common::observe::ObservabilityConfig::from_flat(&flat).expect("test observe config")
}

#[derive(Clone)]
enum MockRegisterReply {
    Ok { worker_id: u64, worker_run_id: WorkerRunId },
    HeaderErrorWithAcceptedBody { worker_id: u64, worker_run_id: WorkerRunId },
    HeaderError(RpcErrorDetail),
    Status(Status),
}

#[derive(Clone)]
enum MockHeartbeatReply {
    Ok { worker_id: u64, worker_run_id: WorkerRunId },
    HeaderError(RpcErrorDetail),
    Status(Status),
}

#[derive(Clone)]
enum MockBlockReportReply {
    Ok,
    HeaderError(RpcErrorDetail),
    Status(Status),
}

#[derive(Default)]
struct MockMetadataState {
    replies: Mutex<VecDeque<MockRegisterReply>>,
    heartbeat_replies: Mutex<VecDeque<MockHeartbeatReply>>,
    block_report_replies: Mutex<VecDeque<MockBlockReportReply>>,
    requests: Mutex<Vec<RegisterWorkerRequestProto>>,
    heartbeat_requests: Mutex<Vec<HeartbeatRequestProto>>,
    block_report_requests: Mutex<Vec<BlockReportRequestProto>>,
}

#[derive(Clone)]
struct MockMetadataWorkerService {
    state: Arc<MockMetadataState>,
}

#[tonic::async_trait]
impl MetadataWorkerServiceProto for MockMetadataWorkerService {
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequestProto>,
    ) -> Result<Response<RegisterWorkerResponseProto>, Status> {
        let request = request.into_inner();
        self.state.requests.lock().unwrap().push(request.clone());
        let reply = self
            .state
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("mock register reply");

        match reply {
            MockRegisterReply::Ok {
                worker_id,
                worker_run_id,
            } => Ok(Response::new(RegisterWorkerResponseProto {
                header: Some(response_header_from_request(&request, None)),
                worker_id,
                accepted_worker_run_id: worker_run_id.to_string(),
            })),
            MockRegisterReply::HeaderErrorWithAcceptedBody {
                worker_id,
                worker_run_id,
            } => Ok(Response::new(RegisterWorkerResponseProto {
                header: Some(response_header_from_request(
                    &request,
                    Some(RpcErrorDetail::fail(
                        ErrorKind::Internal(InternalErrorKind::Internal),
                        "malformed success header",
                    )),
                )),
                worker_id,
                accepted_worker_run_id: worker_run_id.to_string(),
            })),
            MockRegisterReply::HeaderError(error) => Ok(Response::new(RegisterWorkerResponseProto {
                header: Some(response_header_from_request(&request, Some(error))),
                worker_id: 0,
                accepted_worker_run_id: String::new(),
            })),
            MockRegisterReply::Status(status) => Err(status),
        }
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequestProto>,
    ) -> Result<Response<HeartbeatResponseProto>, Status> {
        let request = request.into_inner();
        self.state.heartbeat_requests.lock().unwrap().push(request.clone());
        let reply = self
            .state
            .heartbeat_replies
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(MockHeartbeatReply::Ok {
                worker_id: request.worker_id,
                worker_run_id: WorkerRunId::parse(&request.worker_run_id).unwrap_or_else(|_| test_worker_run_id()),
            });

        match reply {
            MockHeartbeatReply::Ok {
                worker_id,
                worker_run_id,
            } => Ok(Response::new(HeartbeatResponseProto {
                header: Some(response_header_from_heartbeat_request(&request, None)),
                worker_id,
                accepted_worker_run_id: worker_run_id.to_string(),
                liveness_timeout_ms: 5_000,
            })),
            MockHeartbeatReply::HeaderError(error) => Ok(Response::new(HeartbeatResponseProto {
                header: Some(response_header_from_heartbeat_request(&request, Some(error))),
                ..HeartbeatResponseProto::default()
            })),
            MockHeartbeatReply::Status(status) => Err(status),
        }
    }

    async fn block_report(
        &self,
        request: Request<BlockReportRequestProto>,
    ) -> Result<Response<BlockReportResponseProto>, Status> {
        let request = request.into_inner();
        self.state.block_report_requests.lock().unwrap().push(request.clone());
        let reply = self
            .state
            .block_report_replies
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(MockBlockReportReply::Ok);

        match reply {
            MockBlockReportReply::Ok => Ok(Response::new(BlockReportResponseProto {
                header: Some(response_header_from_block_report_request(&request, None)),
                report_seq: request.report_seq,
                next_delta_seq: 0,
            })),
            MockBlockReportReply::HeaderError(error) => Ok(Response::new(BlockReportResponseProto {
                header: Some(response_header_from_block_report_request(&request, Some(error))),
                report_seq: request.report_seq,
                next_delta_seq: 0,
            })),
            MockBlockReportReply::Status(status) => Err(status),
        }
    }
}

fn response_header_from_request(
    request: &RegisterWorkerRequestProto,
    error: Option<RpcErrorDetail>,
) -> ResponseHeaderProto {
    ResponseHeaderProto {
        client: request.header.as_ref().and_then(|header| header.client.clone()),
        error: error.as_ref().map(rpc_error_to_proto),
        state: Vec::new(),
        group_name: request
            .header
            .as_ref()
            .map(|header| header.group_name.clone())
            .unwrap_or_default(),
        mount_epoch: None,
        route_epoch: None,
    }
}

fn response_header_from_heartbeat_request(
    request: &HeartbeatRequestProto,
    error: Option<RpcErrorDetail>,
) -> ResponseHeaderProto {
    ResponseHeaderProto {
        client: request.header.as_ref().and_then(|header| header.client.clone()),
        error: error.as_ref().map(rpc_error_to_proto),
        state: Vec::new(),
        group_name: request
            .header
            .as_ref()
            .map(|header| header.group_name.clone())
            .unwrap_or_default(),
        mount_epoch: None,
        route_epoch: None,
    }
}

fn response_header_from_block_report_request(
    request: &BlockReportRequestProto,
    error: Option<RpcErrorDetail>,
) -> ResponseHeaderProto {
    ResponseHeaderProto {
        client: request.header.as_ref().and_then(|header| header.client.clone()),
        error: error.as_ref().map(rpc_error_to_proto),
        state: Vec::new(),
        group_name: request
            .header
            .as_ref()
            .map(|header| header.group_name.clone())
            .unwrap_or_default(),
        mount_epoch: None,
        route_epoch: None,
    }
}

fn control_call_identity(header: &beryl_proto::common::RequestHeaderProto) -> (ClientId, String) {
    let client = header.client.as_ref().expect("client info");
    let client_id = beryl_proto::convert::required_client_id(client.client_id, "client_id").expect("client_id");
    assert!(!client_id.is_zero());
    (client_id, client.call_id.clone())
}

async fn start_mock_metadata(
    replies: Vec<MockRegisterReply>,
) -> (String, Arc<MockMetadataState>, tokio::sync::oneshot::Sender<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock metadata");
    let addr = listener.local_addr().expect("mock metadata local addr");
    let state = Arc::new(MockMetadataState {
        replies: Mutex::new(VecDeque::from(replies)),
        heartbeat_replies: Mutex::new(VecDeque::new()),
        block_report_replies: Mutex::new(VecDeque::new()),
        requests: Mutex::new(Vec::new()),
        heartbeat_requests: Mutex::new(Vec::new()),
        block_report_requests: Mutex::new(Vec::new()),
    });
    let service = MockMetadataWorkerService {
        state: Arc::clone(&state),
    };
    let incoming = futures::stream::try_unfold(listener, |listener| async move {
        let (stream, _) = listener.accept().await?;
        Ok::<_, std::io::Error>(Some((stream, listener)))
    });
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

    tokio::spawn(async move {
        Server::builder()
            .add_service(MetadataWorkerServiceProtoServer::new(service))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("mock metadata server");
    });

    (format!("http://{addr}"), state, shutdown_tx)
}

async fn start_mock_metadata_with_heartbeat(
    replies: Vec<MockHeartbeatReply>,
) -> (String, Arc<MockMetadataState>, tokio::sync::oneshot::Sender<()>) {
    let (endpoint, state, shutdown) = start_mock_metadata(Vec::new()).await;
    *state.heartbeat_replies.lock().unwrap() = VecDeque::from(replies);
    (endpoint, state, shutdown)
}

async fn start_mock_metadata_with_block_reports(
    replies: Vec<MockBlockReportReply>,
) -> (String, Arc<MockMetadataState>, tokio::sync::oneshot::Sender<()>) {
    let (endpoint, state, shutdown) = start_mock_metadata(Vec::new()).await;
    *state.block_report_replies.lock().unwrap() = VecDeque::from(replies);
    (endpoint, state, shutdown)
}

fn test_registration_config(endpoint: String) -> WorkerRegistrationConfig {
    WorkerRegistrationConfig {
        group_name: group_name(),
        endpoints: vec![endpoint],
        register_timeout_ms: 1_000,
        register_retry_initial_backoff_ms: 1,
        register_retry_max_backoff_ms: 1,
    }
}

fn test_worker_run_id() -> WorkerRunId {
    "550e8400-e29b-41d4-a716-446655440000".parse().unwrap()
}

fn other_worker_run_id() -> WorkerRunId {
    "550e8400-e29b-41d4-a716-446655440001".parse().unwrap()
}

fn test_registration_descriptor(worker_run_id: WorkerRunId) -> RegistrationDescriptor {
    RegistrationDescriptor {
        group_name: group_name(),
        worker_id: WorkerId::new(42),
        worker_run_id,
        endpoint_host: "127.0.0.1".to_string(),
        endpoint_port: 9090,
        advertised_endpoint: "http://127.0.0.1:9090".to_string(),
        worker_net_protocol: WorkerNetProtocol::Grpc,
    }
}

fn payload() -> Bytes {
    Bytes::from((0..BLOCK_SIZE).map(|idx| (idx % 251) as u8).collect::<Vec<_>>())
}

fn report_store(temp: &TempDir) -> Arc<StoreDirs> {
    Arc::new(
        StoreDirs::open(
            BTreeMap::from([(
                "hdd0".to_string(),
                StoreDirConfig {
                    path: temp.path().join("hdd0"),
                    tier: Tier::Hdd,
                    capacity_bytes: 64 * 1024 * 1024,
                },
            )]),
            0,
            30_000,
        )
        .expect("open report store"),
    )
}

fn publish_ready_block_for(
    store: &(impl LocalBlockStore + ?Sized),
    group_name: GroupName,
    block_id: BlockId,
    data: Bytes,
    block_stamp: u64,
) {
    store
        .create_staging_block(CreateStagingBlockRequest {
            group_name: group_name.clone(),
            block_id,
            block_size: BLOCK_SIZE,
            block_format_id: BlockFormatId::FULL_EFFECTIVE,
            chunk_size: CHUNK_SIZE,
            checksum_kind: ChecksumKind::None,
            tier: Tier::Hdd,
        })
        .expect("create staging block");
    store
        .write_at(&group_name, block_id, 0, data.clone())
        .expect("write block");
    store
        .publish_ready(PublishReadyRequest {
            group_name,
            block_id,
            effective_len: data.len() as u64,
            block_stamp,
        })
        .expect("publish ready block");
}

#[tokio::test]
async fn registrar_sends_register_request_and_stores_worker_run_id() {
    let worker_run_id = test_worker_run_id();
    let (endpoint, mock, shutdown) = start_mock_metadata(vec![MockRegisterReply::Ok {
        worker_id: 42,
        worker_run_id,
    }])
    .await;
    let state = Arc::new(RegistrationSet::new());
    let registrar = MetadataRegistrar::new(
        test_registration_config(endpoint),
        test_registration_descriptor(worker_run_id),
        Arc::clone(&state),
    )
    .expect("registrar");

    let registration = registrar.register_once().await.expect("register once");

    assert_eq!(registration.worker_id, WorkerId::new(42));
    assert_eq!(registration.worker_run_id, worker_run_id);
    assert!(state.is_registered(&group_name()));
    assert!(!state.is_ready(&group_name()));
    assert_eq!(
        state.registration(&group_name()).expect("state registration"),
        registration
    );

    let requests = mock.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert_eq!(
        request.header.as_ref().expect("header").group_name,
        group_name().as_str()
    );
    assert_eq!(request.worker_id, 42);
    assert_eq!(request.worker_run_id, worker_run_id.to_string());
    assert_eq!(
        request.advertised_endpoint,
        Some(EndpointProto {
            host: "127.0.0.1".to_string(),
            port: 9090,
        })
    );
    shutdown.send(()).ok();
}

#[tokio::test]
async fn registrar_rejects_header_error_with_accepted_body_and_does_not_set_ready() {
    let worker_run_id = test_worker_run_id();
    let (endpoint, mock, shutdown) = start_mock_metadata(vec![MockRegisterReply::HeaderErrorWithAcceptedBody {
        worker_id: 44,
        worker_run_id,
    }])
    .await;
    let state = Arc::new(RegistrationSet::new());
    let registrar = MetadataRegistrar::new(
        test_registration_config(endpoint),
        test_registration_descriptor(worker_run_id),
        Arc::clone(&state),
    )
    .expect("registrar");

    let error = registrar
        .register_once()
        .await
        .expect_err("header error with accepted body must fail registration");

    assert!(error.to_string().contains("malformed success header"));
    assert!(!state.is_registered(&group_name()));
    assert!(!state.is_ready(&group_name()));
    assert_eq!(mock.requests.lock().unwrap().len(), 1);
    shutdown.send(()).ok();
}

#[tokio::test]
async fn registrar_rejects_worker_run_id_mismatch() {
    let worker_run_id = test_worker_run_id();
    let (endpoint, _mock, shutdown) = start_mock_metadata(vec![MockRegisterReply::Ok {
        worker_id: 42,
        worker_run_id: other_worker_run_id(),
    }])
    .await;
    let state = Arc::new(RegistrationSet::new());
    let registrar = MetadataRegistrar::new(
        test_registration_config(endpoint),
        test_registration_descriptor(worker_run_id),
        Arc::clone(&state),
    )
    .expect("registrar");

    let error = registrar
        .register_once()
        .await
        .expect_err("mismatched worker_run_id must fail registration");

    assert!(error.to_string().contains("worker_run_id"));
    assert!(!state.is_ready(&group_name()));
    shutdown.send(()).ok();
}

#[test]
fn descriptor_from_config_uses_advertised_endpoint_not_bind() {
    let mut config = test_worker_config();
    config.rpc_bind = "0.0.0.0:9090".to_string();
    config.rpc_advertised_endpoint = "http://127.0.0.1:19090".to_string();
    config.net =
        WorkerNetConfig::grpc_from_rpc(config.rpc_bind.clone(), config.rpc_max_inflight, config.max_frame_size);

    let descriptor = MetadataRegistrar::descriptor_from_config(&config, WorkerId::new(42)).expect("descriptor");

    assert_eq!(descriptor.endpoint_host, "127.0.0.1");
    assert_eq!(descriptor.endpoint_port, 19090);
}

#[tokio::test]
async fn retryable_register_failure_is_retried() {
    let retryable = RpcErrorDetail::retry(
        ErrorKind::Internal(InternalErrorKind::NodeUnavailable),
        Some(1),
        "metadata temporarily unavailable",
    );
    let worker_run_id = test_worker_run_id();
    let (endpoint, mock, shutdown) = start_mock_metadata(vec![
        MockRegisterReply::HeaderError(retryable),
        MockRegisterReply::Ok {
            worker_id: 42,
            worker_run_id,
        },
    ])
    .await;
    let state = Arc::new(RegistrationSet::new());
    let registrar = MetadataRegistrar::new(
        test_registration_config(endpoint),
        test_registration_descriptor(worker_run_id),
        Arc::clone(&state),
    )
    .expect("registrar");

    let registration = registrar
        .register_with_retry(std::future::pending::<()>())
        .await
        .expect("register with retry");

    assert_eq!(registration.worker_id, WorkerId::new(42));
    assert_eq!(registration.worker_run_id, worker_run_id);
    assert!(state.is_registered(&group_name()));
    assert!(!state.is_ready(&group_name()));
    let requests = mock.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].worker_run_id, worker_run_id.to_string());
    assert_eq!(requests[1].worker_run_id, worker_run_id.to_string());
    let first_header = requests[0].header.as_ref().expect("first header");
    let second_header = requests[1].header.as_ref().expect("second header");
    let first_client = first_header.client.as_ref().expect("first client info");
    let second_client = second_header.client.as_ref().expect("second client info");
    let first_client_id =
        beryl_proto::convert::required_client_id(first_client.client_id, "client_id").expect("client id");
    let second_client_id =
        beryl_proto::convert::required_client_id(second_client.client_id, "client_id").expect("client id");

    assert_ne!(first_client_id.as_raw(), 0);
    assert_eq!(first_client_id, second_client_id);
    assert_eq!(first_client.call_id, second_client.call_id);
    shutdown.send(()).ok();
}

#[tokio::test]
async fn transport_register_unavailable_is_retried() {
    let worker_run_id = test_worker_run_id();
    let (endpoint, mock, shutdown) = start_mock_metadata(vec![
        MockRegisterReply::Status(Status::unavailable("metadata temporarily unavailable")),
        MockRegisterReply::Ok {
            worker_id: 42,
            worker_run_id,
        },
    ])
    .await;
    let state = Arc::new(RegistrationSet::new());
    let registrar = MetadataRegistrar::new(
        test_registration_config(endpoint),
        test_registration_descriptor(worker_run_id),
        Arc::clone(&state),
    )
    .expect("registrar");

    let registration = registrar
        .register_with_retry(std::future::pending::<()>())
        .await
        .expect("register with transport retry");

    assert_eq!(registration.worker_id, WorkerId::new(42));
    assert_eq!(registration.worker_run_id, worker_run_id);
    assert!(state.is_registered(&group_name()));
    assert!(!state.is_ready(&group_name()));
    assert_eq!(mock.requests.lock().unwrap().len(), 2);
    shutdown.send(()).ok();
}

#[tokio::test]
async fn fatal_register_failure_stops_startup() {
    let fatal = RpcErrorDetail::fs(FsErrorCode::EInval, "bad worker descriptor");
    let (endpoint, mock, shutdown) = start_mock_metadata(vec![MockRegisterReply::HeaderError(fatal)]).await;
    let state = Arc::new(RegistrationSet::new());
    let registrar = MetadataRegistrar::new(
        test_registration_config(endpoint),
        test_registration_descriptor(test_worker_run_id()),
        Arc::clone(&state),
    )
    .expect("registrar");

    let error = registrar
        .register_with_retry(std::future::pending::<()>())
        .await
        .expect_err("fatal registration error");

    assert!(error.to_string().contains("fatal metadata registration error"));
    assert!(!state.is_ready(&group_name()));
    assert_eq!(mock.requests.lock().unwrap().len(), 1);
    shutdown.send(()).ok();
}

#[tokio::test]
async fn heartbeat_sends_registered_identity_to_all_configured_peers() {
    let worker_run_id = test_worker_run_id();
    let (endpoint_a, mock_a, shutdown_a) = start_mock_metadata_with_heartbeat(vec![MockHeartbeatReply::Ok {
        worker_id: 42,
        worker_run_id,
    }])
    .await;
    let (endpoint_b, mock_b, shutdown_b) = start_mock_metadata_with_heartbeat(vec![MockHeartbeatReply::Ok {
        worker_id: 42,
        worker_run_id,
    }])
    .await;
    let state = Arc::new(RegistrationSet::new());
    state.record_registered(Registration {
        group_name: group_name(),
        worker_id: WorkerId::new(42),
        worker_run_id,
        advertised_endpoint: "http://127.0.0.1:9090".to_string(),
    });
    let heartbeat = MetadataHeartbeatLoop::new(
        WorkerRegistrationConfig {
            endpoints: vec![endpoint_a.clone(), endpoint_b.clone()],
            ..test_registration_config(endpoint_a)
        },
        test_registration_descriptor(worker_run_id),
        Arc::clone(&state),
    )
    .expect("heartbeat loop");

    let round = heartbeat
        .send_once(HeartbeatSnapshot {
            capacity_total_bytes: 10,
            capacity_used_bytes: 3,
            capacity_available_bytes: 7,
            tier_free: vec![TierFree {
                tier: Tier::Ssd,
                free_bytes: 7,
            }],
            active_reads: 1,
            active_writes: 2,
        })
        .await
        .expect("heartbeat round");

    assert_eq!(round.attempted_peers, 2);
    assert_eq!(round.accepted_peers, 2);
    assert!(state.is_ready(&group_name()));
    let mut identities = Vec::new();
    for mock in [&mock_a, &mock_b] {
        let requests = mock.heartbeat_requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let request = &requests[0];
        assert_eq!(
            request.header.as_ref().expect("header").group_name,
            group_name().as_str()
        );
        identities.push(control_call_identity(request.header.as_ref().expect("header")));
        assert_eq!(request.worker_id, 42);
        assert_eq!(request.worker_run_id, worker_run_id.to_string());
        assert_eq!(request.heartbeat_seq, 1);
        assert_eq!(request.capacity.as_ref().unwrap().total_bytes, 10);
        assert_eq!(request.capacity.as_ref().unwrap().tier_free.len(), 1);
        assert_eq!(
            request.capacity.as_ref().unwrap().tier_free[0].tier,
            beryl_proto::common::TierProto::TierSsd as i32
        );
        assert_eq!(
            request.advertised_endpoint,
            Some(EndpointProto {
                host: "127.0.0.1".to_string(),
                port: 9090,
            })
        );
    }
    assert_eq!(identities[0], identities[1]);
    shutdown_a.send(()).ok();
    shutdown_b.send(()).ok();
}

#[tokio::test]
async fn heartbeat_sends_zero_capacity_when_store_report_has_failed_dir() {
    let worker_run_id = test_worker_run_id();
    let (endpoint, mock, shutdown) = start_mock_metadata_with_heartbeat(vec![MockHeartbeatReply::Ok {
        worker_id: 42,
        worker_run_id,
    }])
    .await;
    let state = Arc::new(RegistrationSet::new());
    state.record_registered(Registration {
        group_name: group_name(),
        worker_id: WorkerId::new(42),
        worker_run_id,
        advertised_endpoint: "http://127.0.0.1:9090".to_string(),
    });
    let heartbeat = MetadataHeartbeatLoop::new(
        test_registration_config(endpoint),
        test_registration_descriptor(worker_run_id),
        Arc::clone(&state),
    )
    .expect("heartbeat loop");
    let temp = TempDir::new().expect("tempdir");
    let failed_path = temp.path().join("hdd0");
    let store = StoreDirs::open(
        BTreeMap::from([(
            "hdd0".to_string(),
            StoreDirConfig {
                path: failed_path.clone(),
                tier: Tier::Hdd,
                capacity_bytes: 64 * 1024,
            },
        )]),
        0,
        1,
    )
    .expect("open store dirs");
    std::fs::remove_dir_all(&failed_path).expect("remove failed store dir");
    std::thread::sleep(Duration::from_millis(10));

    let snapshot = HeartbeatSnapshot::from(store.report().expect("degraded store report"));
    let round = heartbeat.send_once(snapshot).await.expect("heartbeat round");

    assert_eq!(round.accepted_peers, 1);
    let requests = mock.heartbeat_requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let capacity = requests[0].capacity.as_ref().expect("heartbeat capacity");
    assert_eq!(capacity.total_bytes, 64 * 1024);
    assert_eq!(capacity.available_bytes, 0);
    assert!(capacity.tier_free.is_empty());
    shutdown.send(()).ok();
}

#[tokio::test]
async fn heartbeat_ticks_reuse_runtime_client_id_with_new_call_id() {
    let worker_run_id = test_worker_run_id();
    let (endpoint, mock, shutdown) = start_mock_metadata_with_heartbeat(Vec::new()).await;
    let state = Arc::new(RegistrationSet::new());
    state.record_registered(Registration {
        group_name: group_name(),
        worker_id: WorkerId::new(42),
        worker_run_id,
        advertised_endpoint: "http://127.0.0.1:9090".to_string(),
    });
    let heartbeat = MetadataHeartbeatLoop::new(
        test_registration_config(endpoint),
        test_registration_descriptor(worker_run_id),
        Arc::clone(&state),
    )
    .expect("heartbeat loop");

    heartbeat
        .send_once(HeartbeatSnapshot::default())
        .await
        .expect("first heartbeat");
    heartbeat
        .send_once(HeartbeatSnapshot::default())
        .await
        .expect("second heartbeat");

    let requests = mock.heartbeat_requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    let (first_client_id, first_call_id) = control_call_identity(requests[0].header.as_ref().expect("first header"));
    let (second_client_id, second_call_id) = control_call_identity(requests[1].header.as_ref().expect("second header"));
    assert_eq!(first_client_id, second_client_id);
    assert_ne!(first_call_id, second_call_id);
    assert_eq!(requests[0].heartbeat_seq, 1);
    assert_eq!(requests[1].heartbeat_seq, 2);
    shutdown.send(()).ok();
}

#[tokio::test]
async fn heartbeat_without_registration_sends_no_requests() {
    let (endpoint, mock, shutdown) = start_mock_metadata_with_heartbeat(Vec::new()).await;
    let state = Arc::new(RegistrationSet::new());
    let heartbeat = MetadataHeartbeatLoop::new(
        test_registration_config(endpoint),
        test_registration_descriptor(test_worker_run_id()),
        Arc::clone(&state),
    )
    .expect("heartbeat loop");

    let round = heartbeat.send_once(HeartbeatSnapshot::default()).await.unwrap();

    assert_eq!(round.attempted_peers, 0);
    assert!(mock.heartbeat_requests.lock().unwrap().is_empty());
    shutdown.send(()).ok();
}

#[tokio::test]
async fn single_heartbeat_peer_failure_does_not_clear_ready_lease() {
    let worker_run_id = test_worker_run_id();
    let (endpoint_a, _mock_a, shutdown_a) = start_mock_metadata_with_heartbeat(vec![MockHeartbeatReply::Status(
        Status::unavailable("metadata peer down"),
    )])
    .await;
    let (endpoint_b, _mock_b, shutdown_b) = start_mock_metadata_with_heartbeat(vec![MockHeartbeatReply::Ok {
        worker_id: 42,
        worker_run_id,
    }])
    .await;
    let state = Arc::new(RegistrationSet::new());
    state.record_registered(Registration {
        group_name: group_name(),
        worker_id: WorkerId::new(42),
        worker_run_id,
        advertised_endpoint: "http://127.0.0.1:9090".to_string(),
    });
    state.record_heartbeat_success(&group_name(), Duration::from_secs(60));
    let heartbeat = MetadataHeartbeatLoop::new(
        WorkerRegistrationConfig {
            endpoints: vec![endpoint_a.clone(), endpoint_b.clone()],
            ..test_registration_config(endpoint_a)
        },
        test_registration_descriptor(worker_run_id),
        Arc::clone(&state),
    )
    .expect("heartbeat loop");

    let round = heartbeat.send_once(HeartbeatSnapshot::default()).await.unwrap();

    assert_eq!(round.attempted_peers, 2);
    assert_eq!(round.accepted_peers, 1);
    assert!(state.is_ready(&group_name()));
    shutdown_a.send(()).ok();
    shutdown_b.send(()).ok();
}

#[tokio::test]
async fn need_register_heartbeat_responses_clear_registration() {
    for (kind, message) in [
        (ErrorKind::Worker(WorkerErrorKind::NotRegistered), "register first"),
        (
            ErrorKind::Worker(WorkerErrorKind::DescriptorMismatch),
            "descriptor changed",
        ),
    ] {
        let worker_run_id = test_worker_run_id();
        let (endpoint, _mock, shutdown) = start_mock_metadata_with_heartbeat(vec![MockHeartbeatReply::HeaderError(
            RpcErrorDetail::register_worker(kind, message),
        )])
        .await;
        let state = Arc::new(RegistrationSet::new());
        state.record_registered(Registration {
            group_name: group_name(),
            worker_id: WorkerId::new(42),
            worker_run_id,
            advertised_endpoint: "http://127.0.0.1:9090".to_string(),
        });
        let heartbeat = MetadataHeartbeatLoop::new(
            test_registration_config(endpoint),
            test_registration_descriptor(worker_run_id),
            Arc::clone(&state),
        )
        .expect("heartbeat loop");

        let round = heartbeat.send_once(HeartbeatSnapshot::default()).await.unwrap();

        assert!(round.needs_register, "{kind:?} should request registration");
        assert!(
            !state.is_registered(&group_name()),
            "{kind:?} should clear registration"
        );
        assert!(!state.is_ready(&group_name()), "{kind:?} should clear readiness");
        shutdown.send(()).ok();
    }
}

#[tokio::test]
async fn heartbeat_refresh_metadata_recovery_does_not_clear_registration() {
    let worker_run_id = test_worker_run_id();
    let (endpoint, _mock, shutdown) =
        start_mock_metadata_with_heartbeat(vec![MockHeartbeatReply::HeaderError(RpcErrorDetail::refresh_metadata(
            ErrorKind::Worker(WorkerErrorKind::NotRegistered),
            RefreshHint::default(),
            "metadata refresh only",
        ))])
        .await;
    let state = Arc::new(RegistrationSet::new());
    state.record_registered(Registration {
        group_name: group_name(),
        worker_id: WorkerId::new(42),
        worker_run_id,
        advertised_endpoint: "http://127.0.0.1:9090".to_string(),
    });
    state.record_heartbeat_success(&group_name(), Duration::from_secs(60));
    let heartbeat = MetadataHeartbeatLoop::new(
        test_registration_config(endpoint),
        test_registration_descriptor(worker_run_id),
        Arc::clone(&state),
    )
    .expect("heartbeat loop");

    let err = heartbeat
        .send_once(HeartbeatSnapshot::default())
        .await
        .expect_err("refresh metadata recovery must not become a hard control outcome");

    assert!(matches!(err, HeartbeatError::Retryable(_)));
    assert!(state.is_registered(&group_name()));
    assert!(state.is_ready(&group_name()));
    shutdown.send(()).ok();
}

#[tokio::test]
async fn worker_run_mismatch_heartbeat_response_clears_registration_for_recovery() {
    let worker_run_id = test_worker_run_id();
    let (endpoint, mock, shutdown) = start_mock_metadata(vec![MockRegisterReply::Ok {
        worker_id: 42,
        worker_run_id,
    }])
    .await;
    *mock.heartbeat_replies.lock().unwrap() = VecDeque::from(vec![MockHeartbeatReply::HeaderError(
        RpcErrorDetail::register_worker(ErrorKind::Worker(WorkerErrorKind::RunMismatch), "stale worker run"),
    )]);
    let state = Arc::new(RegistrationSet::new());
    state.record_registered(Registration {
        group_name: group_name(),
        worker_id: WorkerId::new(42),
        worker_run_id,
        advertised_endpoint: "http://127.0.0.1:9090".to_string(),
    });
    state.record_heartbeat_success(&group_name(), Duration::from_secs(60));
    let heartbeat = MetadataHeartbeatLoop::new(
        test_registration_config(endpoint.clone()),
        test_registration_descriptor(worker_run_id),
        Arc::clone(&state),
    )
    .expect("heartbeat loop");
    let registrar = MetadataRegistrar::new(
        test_registration_config(endpoint.clone()),
        test_registration_descriptor(worker_run_id),
        Arc::clone(&state),
    )
    .expect("registrar");
    let temp = TempDir::new().expect("tempdir");
    let store = report_store(&temp);
    publish_ready_block_for(store.as_ref(), group_name(), block_id(), payload(), 101);
    let reporter = MetadataBlockReportLoop::new(
        test_registration_config(endpoint),
        test_registration_descriptor(worker_run_id),
        Arc::clone(&state),
        Arc::clone(&store),
    )
    .expect("block reporter");

    let round = heartbeat.send_once(HeartbeatSnapshot::default()).await.unwrap();

    assert!(round.worker_run_mismatch);
    assert!(!state.is_registered(&group_name()));
    assert!(!state.is_ready(&group_name()));
    assert_eq!(reporter.send_full_once().await.unwrap().attempted_peers, 0);

    registrar.register_once().await.expect("re-register after mismatch");
    assert!(state.is_registered(&group_name()));
    assert!(!state.is_ready(&group_name()));

    let ready = heartbeat.send_once(HeartbeatSnapshot::default()).await.unwrap();
    assert_eq!(ready.accepted_peers, 1);
    assert!(state.is_ready(&group_name()));

    let report_round = reporter.send_full_once().await.expect("block report after recovery");
    assert_eq!(report_round.accepted_peers, 1);
    shutdown.send(()).ok();
}

#[tokio::test]
async fn heartbeat_header_error_is_rejected() {
    let worker_run_id = test_worker_run_id();
    let (endpoint, _mock, shutdown) =
        start_mock_metadata_with_heartbeat(vec![MockHeartbeatReply::HeaderError(RpcErrorDetail::fail(
            ErrorKind::Internal(InternalErrorKind::Internal),
            "malformed success header",
        ))])
        .await;
    let state = Arc::new(RegistrationSet::new());
    state.record_registered(Registration {
        group_name: group_name(),
        worker_id: WorkerId::new(42),
        worker_run_id,
        advertised_endpoint: "http://127.0.0.1:9090".to_string(),
    });
    let heartbeat = MetadataHeartbeatLoop::new(
        test_registration_config(endpoint),
        test_registration_descriptor(worker_run_id),
        state,
    )
    .expect("heartbeat loop");

    let error = heartbeat
        .send_once(HeartbeatSnapshot::default())
        .await
        .expect_err("header error must fail heartbeat");

    assert!(error.to_string().contains("malformed success header"));
    shutdown.send(()).ok();
}

#[test]
fn worker_readiness_is_false_before_registration_and_true_after() {
    let state = RegistrationSet::new();

    assert!(!state.is_registered(&group_name()));
    assert!(!state.is_ready(&group_name()));
    assert!(!state.is_any_ready());
    assert!(state.registration(&group_name()).is_none());

    state.record_registered(Registration {
        group_name: group_name(),
        worker_id: WorkerId::new(44),
        worker_run_id: test_worker_run_id(),
        advertised_endpoint: "http://127.0.0.1:9090".to_string(),
    });

    assert!(state.is_registered(&group_name()));
    assert!(!state.is_ready(&group_name()));
    assert!(!state.is_any_ready());
    state.record_heartbeat_success(&group_name(), Duration::from_millis(1_000));
    assert!(state.is_ready(&group_name()));
    assert!(state.is_any_ready());
    assert_eq!(
        state.registration(&group_name()),
        Some(Registration {
            group_name: group_name(),
            worker_id: WorkerId::new(44),
            worker_run_id: test_worker_run_id(),
            advertised_endpoint: "http://127.0.0.1:9090".to_string(),
        })
    );
    state.mark_not_ready(&group_name());
    assert!(state.is_registered(&group_name()));
    assert!(!state.is_ready(&group_name()));
}

#[test]
fn heartbeat_snapshot_uses_store_report_capacity() {
    let snapshot = HeartbeatSnapshot::from(StoreReport {
        total_bytes: 10_000,
        used_bytes: 3_000,
        pending_bytes: 1_000,
        free_bytes: 6_000,
        tier_free: vec![TierFree {
            tier: Tier::Ssd,
            free_bytes: 6_000,
        }],
        dirs: Vec::new(),
    });

    assert_eq!(snapshot.capacity_total_bytes, 10_000);
    assert_eq!(snapshot.capacity_used_bytes, 3_000);
    assert_eq!(snapshot.capacity_available_bytes, 6_000);
    assert_eq!(
        snapshot.tier_free,
        vec![TierFree {
            tier: Tier::Ssd,
            free_bytes: 6_000,
        }]
    );
}

#[test]
fn block_report_scans_local_blocks_by_group_directory() {
    let temp = TempDir::new().expect("tempdir");
    let store = FullBlockFileStore::new(FullBlockFileStoreConfig::new(temp.path().to_path_buf()));
    let report_group = GroupName::parse("report").unwrap();
    let other_group = group_name();
    let report_block = BlockId::new(DataHandleId::new(77), BlockIndex::new(0));
    let other_block = BlockId::new(DataHandleId::new(78), BlockIndex::new(0));
    publish_ready_block_for(&store, report_group.clone(), report_block, payload(), 101);
    publish_ready_block_for(&store, other_group, other_block, payload(), 102);

    let scanned = store.scan_group_blocks(&report_group).expect("scan group blocks");

    assert_eq!(scanned.len(), 1);
    assert_eq!(scanned[0].identity.group_name, report_group);
    assert_eq!(scanned[0].identity.block_id, report_block);
}

#[tokio::test]
async fn invalid_local_ready_state_is_not_submitted_in_full_block_report() {
    let worker_run_id = test_worker_run_id();
    let (endpoint, mock, shutdown) = start_mock_metadata_with_block_reports(Vec::new()).await;
    let state = Arc::new(RegistrationSet::new());
    state.record_registered(Registration {
        group_name: group_name(),
        worker_id: WorkerId::new(42),
        worker_run_id,
        advertised_endpoint: "http://127.0.0.1:9090".to_string(),
    });
    state.record_heartbeat_success(&group_name(), Duration::from_secs(60));
    let temp = TempDir::new().expect("tempdir");
    let store = report_store(&temp);
    publish_ready_block_for(store.as_ref(), group_name(), block_id(), payload(), 101);
    let raw_store = FullBlockFileStore::new(FullBlockFileStoreConfig::new(temp.path().join("hdd0")));
    let paths = raw_store.paths(&group_name(), block_id());
    std::fs::remove_file(paths.data_path).expect("remove ready data file");
    let reporter = MetadataBlockReportLoop::new(
        test_registration_config(endpoint),
        test_registration_descriptor(worker_run_id),
        Arc::clone(&state),
        Arc::clone(&store),
    )
    .expect("block reporter");

    let error = reporter
        .send_full_once()
        .await
        .expect_err("invalid local Ready state must stop report construction");

    assert!(matches!(error, BlockReportError::Retryable(_)));
    assert!(mock.block_report_requests.lock().unwrap().is_empty());
    shutdown.send(()).ok();
}

#[tokio::test]
async fn block_report_waits_for_registration_and_heartbeat_readiness() {
    let worker_run_id = test_worker_run_id();
    let (endpoint, mock, shutdown) = start_mock_metadata_with_block_reports(Vec::new()).await;
    let state = Arc::new(RegistrationSet::new());
    let temp = TempDir::new().expect("tempdir");
    let store = report_store(&temp);
    publish_ready_block_for(store.as_ref(), group_name(), block_id(), payload(), 101);
    let reporter = MetadataBlockReportLoop::new(
        test_registration_config(endpoint),
        test_registration_descriptor(worker_run_id),
        Arc::clone(&state),
        Arc::clone(&store),
    )
    .expect("block reporter");

    let without_registration = reporter.send_full_once().await.expect("skip unregistered");
    assert_eq!(without_registration.attempted_peers, 0);
    state.record_registered(Registration {
        group_name: group_name(),
        worker_id: WorkerId::new(42),
        worker_run_id,
        advertised_endpoint: "http://127.0.0.1:9090".to_string(),
    });
    let without_heartbeat = reporter.send_full_once().await.expect("skip not ready");

    assert_eq!(without_heartbeat.attempted_peers, 0);
    assert!(mock.block_report_requests.lock().unwrap().is_empty());
    shutdown.send(()).ok();
}

#[tokio::test]
async fn full_block_report_batches_by_configured_limit() {
    let worker_run_id = test_worker_run_id();
    let (endpoint, mock, shutdown) = start_mock_metadata_with_block_reports(Vec::new()).await;
    let state = Arc::new(RegistrationSet::new());
    state.record_registered(Registration {
        group_name: group_name(),
        worker_id: WorkerId::new(42),
        worker_run_id,
        advertised_endpoint: "http://127.0.0.1:9090".to_string(),
    });
    state.record_heartbeat_success(&group_name(), Duration::from_secs(60));
    let temp = TempDir::new().expect("tempdir");
    let store = report_store(&temp);
    publish_ready_block_for(
        store.as_ref(),
        group_name(),
        BlockId::new(DataHandleId::new(7), BlockIndex::new(0)),
        payload(),
        101,
    );
    publish_ready_block_for(
        store.as_ref(),
        group_name(),
        BlockId::new(DataHandleId::new(7), BlockIndex::new(1)),
        payload(),
        102,
    );
    let reporter = MetadataBlockReportLoop::with_options(
        test_registration_config(endpoint),
        test_registration_descriptor(worker_run_id),
        Arc::clone(&state),
        Arc::clone(&store),
        BlockReportOptions {
            full_max_blocks_per_batch: 1,
            ..BlockReportOptions::default()
        },
    )
    .expect("block reporter");

    let round = reporter.send_full_once().await.expect("full report");

    assert_eq!(round.attempted_peers, 1);
    assert_eq!(round.accepted_peers, 1);
    let requests = mock.block_report_requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0].header.as_ref().expect("header").group_name,
        group_name().as_str()
    );
    let (first_client_id, first_call_id) = control_call_identity(requests[0].header.as_ref().expect("first header"));
    let (second_client_id, second_call_id) = control_call_identity(requests[1].header.as_ref().expect("second header"));
    assert_eq!(first_client_id, second_client_id);
    assert_ne!(first_call_id, second_call_id);
    assert_eq!(requests[0].worker_id, 42);
    assert_eq!(requests[0].worker_run_id, worker_run_id.to_string());
    match requests[0].report.as_ref().expect("first report") {
        beryl_proto::metadata::block_report_request_proto::Report::Full(full) => {
            assert_eq!(full.batch_seq, 0);
            assert!(!full.final_batch);
            assert_eq!(full.blocks.len(), 1);
        }
        other => panic!("expected full report, got {other:?}"),
    }
    match requests[1].report.as_ref().expect("second report") {
        beryl_proto::metadata::block_report_request_proto::Report::Full(full) => {
            assert_eq!(full.batch_seq, 1);
            assert!(full.final_batch);
            assert_eq!(full.blocks.len(), 1);
        }
        other => panic!("expected full report, got {other:?}"),
    }
    shutdown.send(()).ok();
}

#[tokio::test]
async fn full_block_report_stops_peer_batches_after_hard_report_errors() {
    for (error, expected_outcome) in [
        (
            RpcErrorDetail::send_full_block_report(
                ErrorKind::Worker(WorkerErrorKind::FullReportRequired),
                "send full report",
            ),
            "full_report_required",
        ),
        (
            RpcErrorDetail::register_worker(ErrorKind::Worker(WorkerErrorKind::NotRegistered), "register first"),
            "needs_register",
        ),
        (
            RpcErrorDetail::register_worker(ErrorKind::Worker(WorkerErrorKind::RunMismatch), "worker run mismatch"),
            "worker_run_mismatch",
        ),
    ] {
        let worker_run_id = test_worker_run_id();
        let (endpoint, mock, shutdown) =
            start_mock_metadata_with_block_reports(vec![MockBlockReportReply::HeaderError(error)]).await;
        let state = Arc::new(RegistrationSet::new());
        state.record_registered(Registration {
            group_name: group_name(),
            worker_id: WorkerId::new(42),
            worker_run_id,
            advertised_endpoint: "http://127.0.0.1:9090".to_string(),
        });
        state.record_heartbeat_success(&group_name(), Duration::from_secs(60));
        let temp = TempDir::new().expect("tempdir");
        let store = report_store(&temp);
        publish_ready_block_for(
            store.as_ref(),
            group_name(),
            BlockId::new(DataHandleId::new(7), BlockIndex::new(0)),
            payload(),
            101,
        );
        publish_ready_block_for(
            store.as_ref(),
            group_name(),
            BlockId::new(DataHandleId::new(7), BlockIndex::new(1)),
            payload(),
            102,
        );
        let reporter = MetadataBlockReportLoop::with_options(
            test_registration_config(endpoint),
            test_registration_descriptor(worker_run_id),
            Arc::clone(&state),
            Arc::clone(&store),
            BlockReportOptions {
                full_max_blocks_per_batch: 1,
                ..BlockReportOptions::default()
            },
        )
        .expect("block reporter");

        let round = reporter.send_full_once().await.expect("full report round");

        match expected_outcome {
            "full_report_required" => assert!(round.full_report_required),
            "needs_register" => assert!(round.needs_register),
            "worker_run_mismatch" => assert!(round.worker_run_mismatch),
            other => panic!("unexpected hard report outcome: {other}"),
        }
        assert_eq!(
            mock.block_report_requests.lock().unwrap().len(),
            1,
            "{expected_outcome} should stop later full-report batches for the same peer"
        );
        shutdown.send(()).ok();
    }
}

#[tokio::test]
async fn block_report_refresh_metadata_recovery_does_not_set_control_outcome() {
    let worker_run_id = test_worker_run_id();
    let error = RpcErrorDetail::refresh_metadata(
        ErrorKind::Worker(WorkerErrorKind::FullReportRequired),
        RefreshHint::default(),
        "metadata refresh only",
    );
    let (endpoint, _mock, shutdown) =
        start_mock_metadata_with_block_reports(vec![MockBlockReportReply::HeaderError(error)]).await;
    let state = Arc::new(RegistrationSet::new());
    state.record_registered(Registration {
        group_name: group_name(),
        worker_id: WorkerId::new(42),
        worker_run_id,
        advertised_endpoint: "http://127.0.0.1:9090".to_string(),
    });
    state.record_heartbeat_success(&group_name(), Duration::from_secs(60));
    let temp = TempDir::new().expect("tempdir");
    let store = report_store(&temp);
    publish_ready_block_for(store.as_ref(), group_name(), block_id(), payload(), 101);
    let reporter = MetadataBlockReportLoop::new(
        test_registration_config(endpoint),
        test_registration_descriptor(worker_run_id),
        Arc::clone(&state),
        Arc::clone(&store),
    )
    .expect("block reporter");

    let err = reporter
        .send_full_once()
        .await
        .expect_err("refresh metadata recovery must not become a hard block-report outcome");

    assert!(matches!(err, BlockReportError::Retryable(_)));
    assert!(state.is_registered(&group_name()));
    shutdown.send(()).ok();
}

#[tokio::test]
async fn full_report_required_from_one_peer_does_not_stop_other_peers() {
    let worker_run_id = test_worker_run_id();
    let full_required = RpcErrorDetail::send_full_block_report(
        ErrorKind::Worker(WorkerErrorKind::FullReportRequired),
        "send full report",
    );
    let (first_endpoint, first_mock, first_shutdown) =
        start_mock_metadata_with_block_reports(vec![MockBlockReportReply::HeaderError(full_required)]).await;
    let (second_endpoint, second_mock, second_shutdown) = start_mock_metadata_with_block_reports(Vec::new()).await;
    let state = Arc::new(RegistrationSet::new());
    state.record_registered(Registration {
        group_name: group_name(),
        worker_id: WorkerId::new(42),
        worker_run_id,
        advertised_endpoint: "http://127.0.0.1:9090".to_string(),
    });
    state.record_heartbeat_success(&group_name(), Duration::from_secs(60));
    let temp = TempDir::new().expect("tempdir");
    let store = report_store(&temp);
    publish_ready_block_for(
        store.as_ref(),
        group_name(),
        BlockId::new(DataHandleId::new(7), BlockIndex::new(0)),
        payload(),
        101,
    );
    publish_ready_block_for(
        store.as_ref(),
        group_name(),
        BlockId::new(DataHandleId::new(7), BlockIndex::new(1)),
        payload(),
        102,
    );
    let mut config = test_registration_config(first_endpoint);
    config.endpoints.push(second_endpoint);
    let reporter = MetadataBlockReportLoop::with_options(
        config,
        test_registration_descriptor(worker_run_id),
        Arc::clone(&state),
        Arc::clone(&store),
        BlockReportOptions {
            full_max_blocks_per_batch: 1,
            ..BlockReportOptions::default()
        },
    )
    .expect("block reporter");

    let round = reporter.send_full_once().await.expect("full report round");

    assert!(round.full_report_required);
    assert_eq!(round.accepted_peers, 1);
    assert_eq!(first_mock.block_report_requests.lock().unwrap().len(), 1);
    assert_eq!(second_mock.block_report_requests.lock().unwrap().len(), 2);
    first_shutdown.send(()).ok();
    second_shutdown.send(()).ok();
}

#[tokio::test]
async fn block_report_worker_run_mismatch_clears_registration() {
    let worker_run_id = test_worker_run_id();
    let mismatch =
        RpcErrorDetail::register_worker(ErrorKind::Worker(WorkerErrorKind::RunMismatch), "worker run mismatch");
    let (endpoint, mock, shutdown) =
        start_mock_metadata_with_block_reports(vec![MockBlockReportReply::HeaderError(mismatch)]).await;
    let state = Arc::new(RegistrationSet::new());
    state.record_registered(Registration {
        group_name: group_name(),
        worker_id: WorkerId::new(42),
        worker_run_id,
        advertised_endpoint: "http://127.0.0.1:9090".to_string(),
    });
    state.record_heartbeat_success(&group_name(), Duration::from_secs(60));
    let temp = TempDir::new().expect("tempdir");
    let store = report_store(&temp);
    publish_ready_block_for(store.as_ref(), group_name(), block_id(), payload(), 101);
    let reporter = MetadataBlockReportLoop::new(
        test_registration_config(endpoint),
        test_registration_descriptor(worker_run_id),
        Arc::clone(&state),
        Arc::clone(&store),
    )
    .expect("block reporter");

    let round = reporter.send_full_once().await.expect("full report round");
    let after_mismatch = reporter.send_full_once().await.expect("stale registration skipped");

    assert!(round.worker_run_mismatch);
    assert!(state.registration(&group_name()).is_none());
    assert_eq!(after_mismatch.attempted_peers, 0);
    assert_eq!(mock.block_report_requests.lock().unwrap().len(), 1);
    shutdown.send(()).ok();
}

#[tokio::test]
async fn full_block_report_peer_failure_does_not_skip_other_peers() {
    let worker_run_id = test_worker_run_id();
    let (first_endpoint, first_mock, first_shutdown) =
        start_mock_metadata_with_block_reports(vec![MockBlockReportReply::Status(Status::unavailable(
            "peer unavailable",
        ))])
        .await;
    let (second_endpoint, second_mock, second_shutdown) = start_mock_metadata_with_block_reports(Vec::new()).await;
    let state = Arc::new(RegistrationSet::new());
    state.record_registered(Registration {
        group_name: group_name(),
        worker_id: WorkerId::new(42),
        worker_run_id,
        advertised_endpoint: "http://127.0.0.1:9090".to_string(),
    });
    state.record_heartbeat_success(&group_name(), Duration::from_secs(60));
    let temp = TempDir::new().expect("tempdir");
    let store = report_store(&temp);
    publish_ready_block_for(store.as_ref(), group_name(), block_id(), payload(), 101);
    let mut config = test_registration_config(first_endpoint);
    config.endpoints.push(second_endpoint);
    let reporter = MetadataBlockReportLoop::new(
        config,
        test_registration_descriptor(worker_run_id),
        Arc::clone(&state),
        Arc::clone(&store),
    )
    .expect("block reporter");

    let round = reporter.send_full_once().await.expect("full report round");

    assert!(!round.full_report_required);
    assert_eq!(round.accepted_peers, 1);
    assert_eq!(first_mock.block_report_requests.lock().unwrap().len(), 1);
    assert_eq!(second_mock.block_report_requests.lock().unwrap().len(), 1);
    first_shutdown.send(()).ok();
    second_shutdown.send(()).ok();
}

#[tokio::test]
async fn delta_report_starts_after_full_and_full_required_resets_baseline() {
    let worker_run_id = test_worker_run_id();
    let full_required = RpcErrorDetail::send_full_block_report(
        ErrorKind::Worker(WorkerErrorKind::FullReportRequired),
        "send full report",
    );
    let (endpoint, mock, shutdown) = start_mock_metadata_with_block_reports(vec![
        MockBlockReportReply::Ok,
        MockBlockReportReply::HeaderError(full_required),
    ])
    .await;
    let state = Arc::new(RegistrationSet::new());
    state.record_registered(Registration {
        group_name: group_name(),
        worker_id: WorkerId::new(42),
        worker_run_id,
        advertised_endpoint: "http://127.0.0.1:9090".to_string(),
    });
    state.record_heartbeat_success(&group_name(), Duration::from_secs(60));
    let temp = TempDir::new().expect("tempdir");
    let store = report_store(&temp);
    let first = BlockId::new(DataHandleId::new(7), BlockIndex::new(0));
    let second = BlockId::new(DataHandleId::new(7), BlockIndex::new(1));
    publish_ready_block_for(store.as_ref(), group_name(), first, payload(), 101);
    let reporter = MetadataBlockReportLoop::new(
        test_registration_config(endpoint),
        test_registration_descriptor(worker_run_id),
        Arc::clone(&state),
        Arc::clone(&store),
    )
    .expect("block reporter");

    let before_full = reporter.send_delta_once().await.expect("no delta baseline");
    assert_eq!(before_full.attempted_peers, 0);
    reporter.send_full_once().await.expect("full report");
    publish_ready_block_for(store.as_ref(), group_name(), second, payload(), 102);
    let delta = reporter.send_delta_once().await.expect("delta report");

    assert!(delta.full_report_required);
    assert!(!reporter.has_delta_baseline(&group_name()));
    let requests = mock.block_report_requests.lock().unwrap();
    let (full_client_id, full_call_id) = control_call_identity(requests[0].header.as_ref().expect("full header"));
    let request = requests.last().expect("delta report request");
    let (delta_client_id, delta_call_id) = control_call_identity(request.header.as_ref().expect("delta header"));
    assert_eq!(full_client_id, delta_client_id);
    assert_ne!(full_call_id, delta_call_id);
    assert_eq!(
        request.header.as_ref().expect("header").group_name,
        group_name().as_str()
    );
    assert!(matches!(
        request.report.as_ref(),
        Some(beryl_proto::metadata::block_report_request_proto::Report::Delta(_))
    ));
    shutdown.send(()).ok();
}
