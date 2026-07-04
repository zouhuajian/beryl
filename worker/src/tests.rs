// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Unit tests for the worker data-plane service.

#[cfg(test)]
#[allow(clippy::module_inception)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use common::error::canonical::{CanonicalError, RefreshReason};
    use common::header::RpcErrorCode;
    use futures::StreamExt;
    use metrics::{Counter, Gauge, GaugeFn, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};
    use proto::common::{
        BlockIdProto, ByteRangeProto, ClientInfoProto, EndpointProto, ErrorClassProto, FencingTokenProto,
        RefreshReasonProto, ResponseHeaderProto, StreamIdProto,
    };
    use proto::convert::canonical_to_error_detail;
    use proto::metadata::metadata_worker_service_proto_server::{
        MetadataWorkerServiceProto, MetadataWorkerServiceProtoServer,
    };
    use proto::metadata::{
        BlockReportRequestProto, BlockReportResponseProto, HeartbeatRequestProto, HeartbeatResponseProto,
        MetadataServerRoleProto, RegisterWorkerRequestProto, RegisterWorkerResponseProto, WorkerCommandProto,
    };
    use proto::worker::worker_data_service_server::WorkerDataService;
    use proto::worker::ChecksumKindProto;
    use proto::worker::{
        AbortWriteRequestProto, CommitWriteRequestProto, DataRequestHeaderProto, OpenReadStreamRequestProto,
        OpenWriteStreamRequestProto, ReadStreamRequestProto, SyncCommittedBlockRequestProto, WriteStreamRequestProto,
    };
    use tempfile::TempDir;
    use tonic::transport::Server;
    use tonic::{Request, Response, Status};
    use types::chunk::ByteRange;
    use types::fs::FsErrorCode;
    use types::ids::{BlockId, BlockIndex, ChunkIndex, ClientId, DataHandleId, StreamId, WorkerId};
    use types::layout::BlockFormatId;
    use types::lease::FencingToken;
    use types::{GroupName, Tier, TierFree, WorkerRunId};

    use crate::config::{StoreDirConfig, WorkerConfig, WorkerRegistrationConfig};
    use crate::control::identity::resolve_worker_id;
    use crate::control::{
        BlockReportOptions, HeartbeatSnapshot, MetadataBlockReportLoop, MetadataHeartbeatLoop, MetadataRegistrar,
        Registration, RegistrationDescriptor, RegistrationSet,
    };
    use crate::data::convert::{
        proto_to_abort_write_request, proto_to_commit_write_request, proto_to_read_open_request,
        proto_to_sync_committed_block_request, proto_to_write_frame, proto_to_write_open_request,
    };
    use crate::data::core::{
        AbortWriteRequest, CommitWriteRequest, RangeMapper, ReadOpenRequest, StreamContext, StreamMode,
        SyncCommittedBlockRequest, WorkerCore, WorkerCoreResult, WriteFrame, WriteOpenRequest,
    };
    use crate::error::WorkerError;
    use crate::net::config::WorkerNetConfig;
    use crate::net::protocol::WorkerNetProtocol;
    use crate::net::server::grpc::WorkerDataServiceImpl;
    use crate::observe::WORKER_STREAM_INFLIGHT;
    use crate::runtime::stream::{StreamManager, StreamState};
    use crate::store::block::{
        ChecksumKind, CreateStagingBlockRequest, FullBlockFileStore, FullBlockFileStoreConfig, LocalBlockStore,
        PublishReadyRequest,
    };
    use crate::store::dirs::{StoreDirs, StoreReport};

    const BLOCK_SIZE: u64 = 4096;
    const CHUNK_SIZE: u32 = 1024;
    const BLOCK_STAMP: u64 = 55;

    fn block_id() -> BlockId {
        BlockId::new(DataHandleId::new(7), BlockIndex::new(3))
    }

    fn group_name() -> GroupName {
        GroupName::parse("root").expect("test group name is valid")
    }

    fn test_worker_config() -> WorkerConfig {
        WorkerConfig {
            cluster_id: "local-vecton".to_string(),
            identity_path: std::path::PathBuf::from("data/worker/worker.identity"),
            rpc_bind: "0.0.0.0:9090".to_string(),
            rpc_advertised_endpoint: "http://127.0.0.1:9090".to_string(),
            rpc_max_inflight: 100,
            default_frame_size: 1024 * 1024,
            max_frame_size: 4 * 1024 * 1024,
            window_bytes: 8 * 1024 * 1024,
            stream_idle_timeout_ms: 60_000,
            store: crate::config::WorkerStoreConfig::default(),
            net: WorkerNetConfig::grpc_from_rpc("0.0.0.0:9090".to_string(), 100, 4 * 1024 * 1024),
            metadata: WorkerRegistrationConfig::default(),
            observability: test_observability_config(),
        }
    }

    fn test_observability_config() -> common::observe::ObservabilityConfig {
        let mut flat = common::config::FlatConfig::new();
        flat.set("observe.log.format", "compact");
        flat.set("observe.log.output", "stderr");
        flat.set(
            "observe.log.level",
            "info,vecton=info,metadata=info,worker=info,common=info,openraft=warn,tonic=warn,tower=warn,h2=warn",
        );
        flat.set("observe.metrics.prometheus.bind", "127.0.0.1:19091");
        flat.set("observe.metrics.prometheus.path", "/metrics");
        common::observe::ObservabilityConfig::from_flat(&flat).expect("test observe config")
    }

    fn stream_id() -> StreamId {
        StreamId::new((1u128 << 64) | 42)
    }

    fn token() -> FencingToken {
        FencingToken::new(block_id(), ClientId::new(9), 11)
    }

    fn test_block_id_proto() -> BlockIdProto {
        BlockIdProto {
            data_handle_id: 7,
            block_index: 3,
        }
    }

    fn test_stream_id_proto() -> StreamIdProto {
        StreamIdProto { high: 1, low: 42 }
    }

    fn test_token_proto() -> FencingTokenProto {
        FencingTokenProto {
            block_id: Some(test_block_id_proto()),
            owner: Some(ClientId::new(9).into()),
            epoch: 11,
        }
    }

    fn test_header() -> DataRequestHeaderProto {
        DataRequestHeaderProto {
            client: Some(ClientInfoProto {
                call_id: "call-1".to_string(),
                client_id: Some(ClientId::new(9).into()),
                client_name: "worker-test".to_string(),
            }),
            trace_context: None,
        }
    }

    fn assert_need_refresh<T: std::fmt::Debug>(
        result: WorkerCoreResult<T>,
        expected_reason: common::error::canonical::RefreshReason,
    ) {
        let error = result.expect_err("operation should need refresh");
        match error {
            WorkerError::NeedRefresh { reason, .. } => assert_eq!(reason, expected_reason),
            other => panic!("expected NeedRefresh, got {other:?}"),
        }
    }

    fn assert_invalid_argument<T: std::fmt::Debug>(result: WorkerCoreResult<T>) {
        match result.expect_err("operation should fail") {
            WorkerError::InvalidArgument(_) => {}
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    fn assert_not_found<T: std::fmt::Debug>(result: WorkerCoreResult<T>) {
        match result.expect_err("operation should fail") {
            WorkerError::NotFound(_) => {}
            other => panic!("expected NotFound, got {other:?}"),
        }
    }

    #[derive(Clone)]
    enum MockRegisterReply {
        Ok { worker_id: u64, worker_run_id: WorkerRunId },
        MalformedOkHeader { worker_id: u64, worker_run_id: WorkerRunId },
        HeaderError(CanonicalError),
        Status(Status),
    }

    #[derive(Clone)]
    enum MockHeartbeatReply {
        Ok {
            worker_id: u64,
            worker_run_id: WorkerRunId,
            server_role: MetadataServerRoleProto,
            commands: Vec<WorkerCommandProto>,
        },
        HeaderError(CanonicalError),
        Status(Status),
    }

    #[derive(Clone)]
    enum MockBlockReportReply {
        Ok,
        HeaderError(CanonicalError),
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
                    group_name: request.group_name.clone(),
                    worker_id,
                    accepted_worker_run_id: worker_run_id.to_string(),
                })),
                MockRegisterReply::MalformedOkHeader {
                    worker_id,
                    worker_run_id,
                } => Ok(Response::new(RegisterWorkerResponseProto {
                    header: Some(response_header_from_request(
                        &request,
                        Some(CanonicalError::ok("malformed ok")),
                    )),
                    group_name: request.group_name.clone(),
                    worker_id,
                    accepted_worker_run_id: worker_run_id.to_string(),
                })),
                MockRegisterReply::HeaderError(error) => Ok(Response::new(RegisterWorkerResponseProto {
                    header: Some(response_header_from_request(&request, Some(error))),
                    group_name: request.group_name.clone(),
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
                    worker_run_id: request.worker_run_id.parse().unwrap_or_else(|_| test_worker_run_id()),
                    server_role: MetadataServerRoleProto::MetadataServerRoleFollower,
                    commands: Vec::new(),
                });

            match reply {
                MockHeartbeatReply::Ok {
                    worker_id,
                    worker_run_id,
                    server_role,
                    commands,
                } => Ok(Response::new(HeartbeatResponseProto {
                    header: Some(response_header_from_heartbeat_request(&request, None)),
                    commands,
                    group_name: request.group_name.clone(),
                    worker_id,
                    accepted_worker_run_id: worker_run_id.to_string(),
                    heartbeat_interval_ms: 1_000,
                    liveness_timeout_ms: 5_000,
                    server_role: server_role as i32,
                    leader_hint: None,
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
                    retry_after_ms: 0,
                })),
                MockBlockReportReply::HeaderError(error) => Ok(Response::new(BlockReportResponseProto {
                    header: Some(response_header_from_block_report_request(&request, Some(error))),
                    report_seq: request.report_seq,
                    next_delta_seq: 0,
                    retry_after_ms: 0,
                })),
                MockBlockReportReply::Status(status) => Err(status),
            }
        }
    }

    fn response_header_from_request(
        request: &RegisterWorkerRequestProto,
        error: Option<CanonicalError>,
    ) -> ResponseHeaderProto {
        ResponseHeaderProto {
            client: request.header.as_ref().and_then(|header| header.client.clone()),
            error: error.as_ref().map(canonical_to_error_detail),
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
        error: Option<CanonicalError>,
    ) -> ResponseHeaderProto {
        ResponseHeaderProto {
            client: request.header.as_ref().and_then(|header| header.client.clone()),
            error: error.as_ref().map(canonical_to_error_detail),
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
        error: Option<CanonicalError>,
    ) -> ResponseHeaderProto {
        ResponseHeaderProto {
            client: request.header.as_ref().and_then(|header| header.client.clone()),
            error: error.as_ref().map(canonical_to_error_detail),
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

    fn control_call_identity(header: &proto::common::RequestHeaderProto) -> (ClientId, String) {
        let client = header.client.as_ref().expect("client info");
        let client_id = proto::convert::required_client_id(client.client_id, "client_id").expect("client_id");
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
            version: "worker-test".to_string(),
            capabilities: 0,
            labels: BTreeMap::new(),
        }
    }

    fn mark_registered(state: &RegistrationSet) {
        state.record_registered(Registration {
            group_name: group_name(),
            worker_id: WorkerId::new(46),
            worker_run_id: test_worker_run_id(),
            advertised_endpoint: "http://127.0.0.1:9090".to_string(),
        });
        state.record_heartbeat_success(&group_name(), Duration::from_secs(60));
    }

    fn registered_data_service(core: Arc<WorkerCore>) -> WorkerDataServiceImpl {
        let state = Arc::new(RegistrationSet::new());
        mark_registered(&state);
        WorkerDataServiceImpl::new(core, state)
    }

    #[test]
    fn worker_id_local_identity_generation_and_load_are_stable() {
        let temp = TempDir::new().expect("tempdir");
        let config = WorkerConfig {
            identity_path: temp.path().join("worker.identity"),
            ..test_worker_config()
        };

        let first = resolve_worker_id(&config).expect("generated worker id");
        let second = resolve_worker_id(&config).expect("loaded worker id");

        assert_ne!(first.as_raw(), 0);
        assert_eq!(second, first);
        let stored = std::fs::read_to_string(&config.identity_path).expect("identity file");
        assert!(stored.trim().parse::<uuid::Uuid>().is_ok());
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
        assert_eq!(request.group_name, group_name().as_str());
        assert_eq!(request.header.as_ref().expect("header").group_name, request.group_name);
        assert_eq!(request.worker_id, 42);
        assert_eq!(request.worker_run_id, worker_run_id.to_string());
        assert_eq!(
            request.advertised_endpoint,
            Some(EndpointProto {
                host: "127.0.0.1".to_string(),
                port: 9090,
                protocol: "grpc".to_string(),
            })
        );
        assert_eq!(
            request.worker_net_protocol,
            proto::common::WorkerNetProtocolProto::WorkerNetProtocolGrpc as i32
        );
        assert_eq!(request.version, "worker-test");
        shutdown.send(()).ok();
    }

    #[tokio::test]
    async fn registrar_rejects_malformed_ok_header_error_and_does_not_set_ready() {
        let worker_run_id = test_worker_run_id();
        let (endpoint, mock, shutdown) = start_mock_metadata(vec![MockRegisterReply::MalformedOkHeader {
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
            .expect_err("malformed OK header error must fail registration");

        assert!(error.to_string().contains("malformed"));
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
        let retryable = CanonicalError::retryable(
            RpcErrorCode::NodeUnavailable,
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
            proto::convert::required_client_id(first_client.client_id, "client_id").expect("client id");
        let second_client_id =
            proto::convert::required_client_id(second_client.client_id, "client_id").expect("client id");

        assert_ne!(first_client_id.as_raw(), 0);
        assert_eq!(first_client_id, second_client_id);
        assert_eq!(first_client.call_id, second_client.call_id);
        assert_eq!(first_header.retry_count, 0);
        assert_eq!(second_header.retry_count, 1);
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
        let fatal = CanonicalError::fatal_fs(FsErrorCode::EInval, "bad worker descriptor");
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
            server_role: MetadataServerRoleProto::MetadataServerRoleLeader,
            commands: Vec::new(),
        }])
        .await;
        let (endpoint_b, mock_b, shutdown_b) = start_mock_metadata_with_heartbeat(vec![MockHeartbeatReply::Ok {
            worker_id: 42,
            worker_run_id,
            server_role: MetadataServerRoleProto::MetadataServerRoleFollower,
            commands: Vec::new(),
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
                cpu_usage_percent: 4,
                memory_used_bytes: 5,
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
            assert_eq!(request.group_name, group_name().as_str());
            assert_eq!(request.header.as_ref().expect("header").group_name, request.group_name);
            identities.push(control_call_identity(request.header.as_ref().expect("header")));
            assert_eq!(request.worker_id, 42);
            assert_eq!(request.worker_run_id, worker_run_id.to_string());
            assert_eq!(request.heartbeat_seq, 1);
            assert_eq!(request.capacity.as_ref().unwrap().total_bytes, 10);
            assert_eq!(request.capacity.as_ref().unwrap().tier_free.len(), 1);
            assert_eq!(
                request.capacity.as_ref().unwrap().tier_free[0].tier,
                proto::common::TierProto::TierSsd as i32
            );
            assert_eq!(
                request.advertised_endpoint,
                Some(EndpointProto {
                    host: "127.0.0.1".to_string(),
                    port: 9090,
                    protocol: "grpc".to_string(),
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
            server_role: MetadataServerRoleProto::MetadataServerRoleLeader,
            commands: Vec::new(),
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
        let (first_client_id, first_call_id) =
            control_call_identity(requests[0].header.as_ref().expect("first header"));
        let (second_client_id, second_call_id) =
            control_call_identity(requests[1].header.as_ref().expect("second header"));
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
            server_role: MetadataServerRoleProto::MetadataServerRoleFollower,
            commands: Vec::new(),
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
        for (code, message) in [
            (RpcErrorCode::WorkerNotRegistered, "register first"),
            (RpcErrorCode::WorkerDescriptorMismatch, "descriptor changed"),
        ] {
            let worker_run_id = test_worker_run_id();
            let (endpoint, _mock, shutdown) =
                start_mock_metadata_with_heartbeat(vec![MockHeartbeatReply::HeaderError(
                    CanonicalError::need_refresh(code, RefreshReason::NeedRegister, message),
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

            assert!(round.needs_register, "{code:?} should request registration");
            assert!(
                !state.is_registered(&group_name()),
                "{code:?} should clear registration"
            );
            assert!(!state.is_ready(&group_name()), "{code:?} should clear readiness");
            shutdown.send(()).ok();
        }
    }

    #[tokio::test]
    async fn worker_run_mismatch_heartbeat_response_clears_registration_for_recovery() {
        let worker_run_id = test_worker_run_id();
        let (endpoint, mock, shutdown) = start_mock_metadata(vec![MockRegisterReply::Ok {
            worker_id: 42,
            worker_run_id,
        }])
        .await;
        *mock.heartbeat_replies.lock().unwrap() =
            VecDeque::from(vec![MockHeartbeatReply::HeaderError(CanonicalError::need_refresh(
                RpcErrorCode::WorkerRunMismatch,
                common::error::canonical::RefreshReason::WorkerRunMismatch,
                "stale worker run",
            ))]);
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
    async fn follower_heartbeat_commands_are_ignored_for_readiness() {
        let worker_run_id = test_worker_run_id();
        let (endpoint, _mock, shutdown) = start_mock_metadata_with_heartbeat(vec![MockHeartbeatReply::Ok {
            worker_id: 42,
            worker_run_id,
            server_role: MetadataServerRoleProto::MetadataServerRoleFollower,
            commands: vec![WorkerCommandProto {
                task_id: 1,
                command: None,
            }],
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

        let round = heartbeat.send_once(HeartbeatSnapshot::default()).await.unwrap();

        assert_eq!(round.accepted_peers, 1);
        assert!(state.is_ready(&group_name()));
        shutdown.send(()).ok();
    }

    #[tokio::test]
    async fn malformed_ok_heartbeat_header_is_rejected() {
        let worker_run_id = test_worker_run_id();
        let (endpoint, _mock, shutdown) = start_mock_metadata_with_heartbeat(vec![MockHeartbeatReply::HeaderError(
            CanonicalError::ok("malformed ok"),
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
            state,
        )
        .expect("heartbeat loop");

        let error = heartbeat
            .send_once(HeartbeatSnapshot::default())
            .await
            .expect_err("malformed OK header must fail heartbeat");

        assert!(error.to_string().contains("malformed OK"));
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
        assert_eq!(requests[0].group_name, group_name().as_str());
        assert_eq!(
            requests[0].header.as_ref().expect("header").group_name,
            requests[0].group_name
        );
        let (first_client_id, first_call_id) =
            control_call_identity(requests[0].header.as_ref().expect("first header"));
        let (second_client_id, second_call_id) =
            control_call_identity(requests[1].header.as_ref().expect("second header"));
        assert_eq!(first_client_id, second_client_id);
        assert_ne!(first_call_id, second_call_id);
        assert_eq!(requests[0].worker_id, 42);
        assert_eq!(requests[0].worker_run_id, worker_run_id.to_string());
        match requests[0].report.as_ref().expect("first report") {
            proto::metadata::block_report_request_proto::Report::Full(full) => {
                assert_eq!(full.batch_seq, 0);
                assert!(!full.final_batch);
                assert_eq!(full.blocks.len(), 1);
            }
            other => panic!("expected full report, got {other:?}"),
        }
        match requests[1].report.as_ref().expect("second report") {
            proto::metadata::block_report_request_proto::Report::Full(full) => {
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
        for (code, reason, message) in [
            (
                RpcErrorCode::FullReportRequired,
                RefreshReason::FullReportRequired,
                "send full report",
            ),
            (
                RpcErrorCode::WorkerNotRegistered,
                RefreshReason::NeedRegister,
                "register first",
            ),
            (
                RpcErrorCode::WorkerRunMismatch,
                RefreshReason::WorkerRunMismatch,
                "worker run mismatch",
            ),
        ] {
            let worker_run_id = test_worker_run_id();
            let error = CanonicalError::need_refresh(code, reason, message);
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

            match reason {
                RefreshReason::FullReportRequired => assert!(round.full_report_required),
                RefreshReason::NeedRegister => assert!(round.needs_register),
                RefreshReason::WorkerRunMismatch => assert!(round.worker_run_mismatch),
                other => panic!("unexpected hard report reason: {other:?}"),
            }
            assert_eq!(
                mock.block_report_requests.lock().unwrap().len(),
                1,
                "{code:?} should stop later full-report batches for the same peer"
            );
            shutdown.send(()).ok();
        }
    }

    #[tokio::test]
    async fn full_report_required_from_one_peer_does_not_stop_other_peers() {
        let worker_run_id = test_worker_run_id();
        let full_required = CanonicalError::need_refresh(
            RpcErrorCode::FullReportRequired,
            RefreshReason::FullReportRequired,
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
        let mismatch = CanonicalError::need_refresh(
            RpcErrorCode::WorkerRunMismatch,
            RefreshReason::WorkerRunMismatch,
            "worker run mismatch",
        );
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
        let full_required = CanonicalError::need_refresh(
            RpcErrorCode::FullReportRequired,
            RefreshReason::FullReportRequired,
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
        assert_eq!(request.header.as_ref().expect("header").group_name, request.group_name);
        assert!(matches!(
            request.report.as_ref(),
            Some(proto::metadata::block_report_request_proto::Report::Delta(_))
        ));
        shutdown.send(()).ok();
    }

    fn write_open_request() -> WriteOpenRequest {
        WriteOpenRequest {
            group_name: group_name(),
            block_id: block_id(),
            worker_run_id: test_worker_run_id(),
            token: token(),
            block_stamp: BLOCK_STAMP,
            frame_size: 8192,
            block_size: BLOCK_SIZE,
            block_format_id: BlockFormatId::FULL_EFFECTIVE,
            chunk_size: CHUNK_SIZE,
            effective_len: BLOCK_SIZE,
            checksum_kind: ChecksumKind::None,
            tier: Tier::Hdd,
        }
    }

    fn commit_write_request() -> CommitWriteRequest {
        CommitWriteRequest {
            stream_id: stream_id(),
            group_name: group_name(),
            block_id: block_id(),
            worker_run_id: test_worker_run_id(),
            token: token(),
            commit_seq: 8,
            effective_len: 4096,
            block_stamp: BLOCK_STAMP,
            block_format_id: BlockFormatId::FULL_EFFECTIVE,
            block_size: BLOCK_SIZE,
            chunk_size: CHUNK_SIZE,
            require_sync: true,
        }
    }

    fn abort_write_request() -> AbortWriteRequest {
        AbortWriteRequest {
            stream_id: stream_id(),
            group_name: group_name(),
            block_id: block_id(),
            token: token(),
        }
    }

    fn sync_committed_block_request(block_stamp: u64, expected_block_len: u64) -> SyncCommittedBlockRequest {
        SyncCommittedBlockRequest {
            group_name: group_name(),
            block_id: block_id(),
            worker_run_id: test_worker_run_id(),
            block_stamp,
            expected_block_len,
            block_format_id: BlockFormatId::FULL_EFFECTIVE,
            block_size: BLOCK_SIZE,
            chunk_size: CHUNK_SIZE,
        }
    }

    fn stream_context() -> StreamContext {
        StreamContext {
            stream_id: stream_id(),
            group_name: group_name(),
            block_id: block_id(),
            mode: StreamMode::Read,
            start_offset: 0,
            end_offset: 4096,
            frame_size: 8192,
            window_bytes: 65_536,
            block_stamp: 17,
            block_format_id: BlockFormatId::FULL_EFFECTIVE,
            block_size: BLOCK_SIZE,
            chunk_size: CHUNK_SIZE,
            committed_length: 4096,
            effective_len: 4096,
            worker_run_id: test_worker_run_id(),
            fencing_token: None,
        }
    }

    fn payload() -> Bytes {
        Bytes::from((0..BLOCK_SIZE).map(|idx| (idx % 251) as u8).collect::<Vec<_>>())
    }

    fn core_with_store(
        default_frame_size: u32,
        max_frame_size: u32,
        window_bytes: u32,
    ) -> (TempDir, Arc<FullBlockFileStore>, WorkerCore) {
        let temp = TempDir::new().expect("tempdir");
        let store = Arc::new(FullBlockFileStore::new(FullBlockFileStoreConfig::new(
            temp.path().to_path_buf(),
        )));
        let core = WorkerCore::with_local_store(
            default_frame_size,
            max_frame_size,
            window_bytes,
            Duration::from_secs(60),
            store.clone(),
        );
        (temp, store, core)
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

    fn publish_ready_block(store: &FullBlockFileStore, data: Bytes, block_stamp: u64) {
        store
            .create_staging_block(CreateStagingBlockRequest {
                group_name: group_name(),
                block_id: block_id(),
                block_size: BLOCK_SIZE,
                block_format_id: BlockFormatId::FULL_EFFECTIVE,
                chunk_size: CHUNK_SIZE,
                checksum_kind: ChecksumKind::None,
                tier: Tier::Hdd,
            })
            .expect("create staging block");
        store
            .write_at(&group_name(), block_id(), 0, data.clone())
            .expect("write staging block");
        store
            .publish_ready(PublishReadyRequest {
                group_name: group_name(),
                block_id: block_id(),
                effective_len: data.len() as u64,
                block_stamp,
            })
            .expect("publish ready block");
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

    fn read_open_request_for(offset: u64, len: u32, block_stamp: u64, frame_size: u32) -> ReadOpenRequest {
        read_open_request_for_len(offset, len, block_stamp, BLOCK_SIZE, frame_size)
    }

    fn read_open_request_for_len(
        offset: u64,
        len: u32,
        block_stamp: u64,
        effective_len: u64,
        frame_size: u32,
    ) -> ReadOpenRequest {
        ReadOpenRequest {
            group_name: group_name(),
            block_id: block_id(),
            worker_run_id: test_worker_run_id(),
            byte_range: ByteRange { offset, len },
            block_stamp,
            block_format_id: BlockFormatId::FULL_EFFECTIVE,
            block_size: BLOCK_SIZE,
            chunk_size: CHUNK_SIZE,
            effective_len,
            frame_size,
        }
    }

    async fn collect_core_read(core: &WorkerCore, stream_id: StreamId, max_bytes: u32) -> Bytes {
        let mut out = Vec::new();
        loop {
            let frames = core.read_stream(stream_id, max_bytes).await.expect("read stream");
            let Some(frame) = frames.into_iter().next() else {
                break;
            };
            let eos = frame.eos;
            out.extend_from_slice(&frame.data);
            if eos {
                break;
            }
        }
        Bytes::from(out)
    }

    async fn wait_for_active_stream_count(core: &WorkerCore, expected: usize) {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            let active = core.stream_manager().active_count().await;
            if active == expected {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "active stream count stayed at {active}, expected {expected}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    fn write_stream_context() -> StreamContext {
        StreamContext {
            mode: StreamMode::Write,
            fencing_token: Some(token()),
            ..stream_context()
        }
    }

    fn open_read_proto(offset: u64, len: u32, block_stamp: u64, frame_size: u32) -> OpenReadStreamRequestProto {
        OpenReadStreamRequestProto {
            header: Some(test_header()),
            group_name: "root".to_string(),
            block_id: Some(test_block_id_proto()),
            byte_range: Some(ByteRangeProto { offset, len }),
            block_stamp,
            frame_size,
            worker_run_id: test_worker_run_id().to_string(),
            block_format_id: BlockFormatId::FULL_EFFECTIVE.as_raw(),
            block_size: BLOCK_SIZE,
            chunk_size: CHUNK_SIZE,
            effective_len: BLOCK_SIZE,
        }
    }

    fn open_write_proto(frame_size: u32) -> OpenWriteStreamRequestProto {
        OpenWriteStreamRequestProto {
            header: Some(test_header()),
            group_name: "root".to_string(),
            block_id: Some(test_block_id_proto()),
            block_size: BLOCK_SIZE,
            block_format_id: BlockFormatId::FULL_EFFECTIVE.as_raw(),
            block_stamp: BLOCK_STAMP,
            chunk_size: CHUNK_SIZE,
            checksum_kind: ChecksumKindProto::ChecksumKindNone as i32,
            token: Some(test_token_proto()),
            frame_size,
            worker_run_id: test_worker_run_id().to_string(),
            effective_len: BLOCK_SIZE,
            tier: proto::common::TierProto::TierHdd as i32,
        }
    }

    fn commit_write_proto(stream_id: StreamId, commit_seq: u64, effective_len: u64) -> CommitWriteRequestProto {
        CommitWriteRequestProto {
            header: Some(test_header()),
            group_name: "root".to_string(),
            block_id: Some(test_block_id_proto()),
            stream_id: Some(crate::data::convert::stream_id_to_proto(stream_id)),
            effective_len,
            block_stamp: BLOCK_STAMP,
            token: Some(test_token_proto()),
            commit_seq,
            require_sync: true,
            worker_run_id: test_worker_run_id().to_string(),
            block_format_id: BlockFormatId::FULL_EFFECTIVE.as_raw(),
            block_size: BLOCK_SIZE,
            chunk_size: CHUNK_SIZE,
        }
    }

    fn sync_committed_block_proto(block_stamp: u64, expected_block_len: u64) -> SyncCommittedBlockRequestProto {
        SyncCommittedBlockRequestProto {
            header: Some(test_header()),
            group_name: "root".to_string(),
            block_id: Some(test_block_id_proto()),
            block_stamp,
            expected_block_len,
            worker_run_id: test_worker_run_id().to_string(),
            block_format_id: BlockFormatId::FULL_EFFECTIVE.as_raw(),
            block_size: BLOCK_SIZE,
            chunk_size: CHUNK_SIZE,
        }
    }

    #[test]
    fn range_mapper_maps_range_inside_single_chunk() {
        let slices = RangeMapper::map_range(ByteRange { offset: 100, len: 200 }, 1024).unwrap();

        assert_eq!(slices.len(), 1);
        assert_eq!(slices[0].chunk_index, ChunkIndex::new(0));
        assert_eq!(slices[0].offset_in_chunk, 100);
        assert_eq!(slices[0].len, 200);
    }

    #[test]
    fn range_mapper_maps_range_across_two_chunks() {
        let slices = RangeMapper::map_range(ByteRange { offset: 900, len: 300 }, 1024).unwrap();

        assert_eq!(slices.len(), 2);
        assert_eq!(slices[0].chunk_index, ChunkIndex::new(0));
        assert_eq!(slices[0].offset_in_chunk, 900);
        assert_eq!(slices[0].len, 124);
        assert_eq!(slices[1].chunk_index, ChunkIndex::new(1));
        assert_eq!(slices[1].offset_in_chunk, 0);
        assert_eq!(slices[1].len, 176);
    }

    #[test]
    fn range_mapper_maps_range_starting_at_chunk_boundary() {
        let slices = RangeMapper::map_range(ByteRange { offset: 1024, len: 100 }, 1024).unwrap();

        assert_eq!(slices.len(), 1);
        assert_eq!(slices[0].chunk_index, ChunkIndex::new(1));
        assert_eq!(slices[0].offset_in_chunk, 0);
        assert_eq!(slices[0].len, 100);
    }

    #[test]
    fn range_mapper_maps_empty_range_to_no_slices() {
        let slices = RangeMapper::map_range(ByteRange { offset: 512, len: 0 }, 1024).unwrap();

        assert!(slices.is_empty());
    }

    #[test]
    fn range_mapper_maps_non_aligned_range() {
        let slices = RangeMapper::map_range(
            ByteRange {
                offset: 1537,
                len: 2000,
            },
            1024,
        )
        .unwrap();

        assert_eq!(slices.len(), 3);
        assert_eq!(slices[0].chunk_index, ChunkIndex::new(1));
        assert_eq!(slices[0].offset_in_chunk, 513);
        assert_eq!(slices[0].len, 511);
        assert_eq!(slices[1].chunk_index, ChunkIndex::new(2));
        assert_eq!(slices[1].offset_in_chunk, 0);
        assert_eq!(slices[1].len, 1024);
        assert_eq!(slices[2].chunk_index, ChunkIndex::new(3));
        assert_eq!(slices[2].offset_in_chunk, 0);
        assert_eq!(slices[2].len, 465);
    }

    #[test]
    fn converts_open_read_stream_request_to_domain() {
        let request = open_read_proto(128, 4096, 0, 8192);

        let domain = proto_to_read_open_request(request).unwrap();

        assert_eq!(domain.group_name, group_name());
        assert_eq!(domain.block_id, block_id());
        assert_eq!(domain.byte_range, ByteRange { offset: 128, len: 4096 });
        assert_eq!(domain.block_stamp, 0);
        assert_eq!(domain.worker_run_id, test_worker_run_id());
        assert_eq!(domain.block_format_id, BlockFormatId::FULL_EFFECTIVE);
        assert_eq!(domain.block_size, BLOCK_SIZE);
        assert_eq!(domain.chunk_size, CHUNK_SIZE);
        assert_eq!(domain.effective_len, BLOCK_SIZE);
        assert_eq!(domain.frame_size, 8192);
    }

    #[test]
    fn converts_open_write_stream_request_to_domain() {
        let request = open_write_proto(8192);

        let domain = proto_to_write_open_request(request).unwrap();

        assert_eq!(domain.group_name, group_name());
        assert_eq!(domain.block_id, block_id());
        assert_eq!(domain.token.owner, ClientId::new(9));
        assert_eq!(domain.token.epoch, 11);
        assert_eq!(domain.worker_run_id, test_worker_run_id());
        assert_eq!(domain.block_stamp, BLOCK_STAMP);
        assert_eq!(domain.block_format_id, BlockFormatId::FULL_EFFECTIVE);
        assert_eq!(domain.frame_size, 8192);
        assert_eq!(domain.block_size, BLOCK_SIZE);
        assert_eq!(domain.chunk_size, CHUNK_SIZE);
        assert_eq!(domain.effective_len, BLOCK_SIZE);
        assert_eq!(domain.checksum_kind, ChecksumKind::None);
        assert_eq!(domain.tier, Tier::Hdd);
    }

    #[test]
    fn rejects_open_write_stream_request_with_unknown_block_format_id() {
        let err = proto_to_write_open_request(OpenWriteStreamRequestProto {
            block_format_id: BlockFormatId::FULL_EFFECTIVE.as_raw() + 1,
            ..open_write_proto(8192)
        })
        .expect_err("unknown block format must fail conversion");

        assert!(err.to_string().contains("block_format_id"));
    }

    #[test]
    fn converts_write_stream_request_to_domain_without_copying_payload() {
        let data = Bytes::from_static(b"frame-data");
        let request = WriteStreamRequestProto {
            stream_id: Some(test_stream_id_proto()),
            seq: 5,
            offset_in_block: 2048,
            data: data.clone(),
            checksum32: 123,
        };

        let domain = proto_to_write_frame(request).unwrap();

        assert_eq!(domain.stream_id, stream_id());
        assert_eq!(domain.seq, 5);
        assert_eq!(domain.offset_in_block, 2048);
        assert_eq!(domain.data, data);
        assert_eq!(domain.data.as_ptr(), data.as_ptr());
        assert_eq!(domain.checksum32, 123);
    }

    #[test]
    fn converts_commit_and_abort_write_requests_to_domain() {
        let commit = proto_to_commit_write_request(commit_write_proto(stream_id(), 8, 4096)).unwrap();

        assert_eq!(commit.stream_id, stream_id());
        assert_eq!(commit.group_name, group_name());
        assert_eq!(commit.block_id, block_id());
        assert_eq!(commit.token.epoch, 11);
        assert_eq!(commit.worker_run_id, test_worker_run_id());
        assert_eq!(commit.commit_seq, 8);
        assert_eq!(commit.effective_len, 4096);
        assert_eq!(commit.block_stamp, BLOCK_STAMP);
        assert_eq!(commit.block_format_id, BlockFormatId::FULL_EFFECTIVE);
        assert_eq!(commit.block_size, BLOCK_SIZE);
        assert_eq!(commit.chunk_size, CHUNK_SIZE);
        assert!(commit.require_sync);

        let abort = proto_to_abort_write_request(AbortWriteRequestProto {
            header: Some(test_header()),
            group_name: "root".to_string(),
            block_id: Some(test_block_id_proto()),
            stream_id: Some(test_stream_id_proto()),
            token: Some(test_token_proto()),
        })
        .unwrap();

        assert_eq!(abort.stream_id, stream_id());
        assert_eq!(abort.group_name, group_name());
        assert_eq!(abort.block_id, block_id());
        assert_eq!(abort.token.owner, ClientId::new(9));
    }

    #[test]
    fn converts_sync_committed_block_request_to_domain() {
        let sync = proto_to_sync_committed_block_request(sync_committed_block_proto(BLOCK_STAMP, BLOCK_SIZE)).unwrap();

        assert_eq!(sync.group_name, group_name());
        assert_eq!(sync.block_id, block_id());
        assert_eq!(sync.worker_run_id, test_worker_run_id());
        assert_eq!(sync.block_stamp, BLOCK_STAMP);
        assert_eq!(sync.expected_block_len, BLOCK_SIZE);
        assert_eq!(sync.block_format_id, BlockFormatId::FULL_EFFECTIVE);
        assert_eq!(sync.block_size, BLOCK_SIZE);
        assert_eq!(sync.chunk_size, CHUNK_SIZE);
    }

    #[test]
    fn conversion_reports_missing_required_fields_without_panic() {
        let read_err = proto_to_read_open_request(OpenReadStreamRequestProto {
            header: Some(test_header()),
            group_name: "root".to_string(),
            block_id: None,
            byte_range: Some(ByteRangeProto { offset: 0, len: 1 }),
            block_stamp: 0,
            frame_size: 1024,
            worker_run_id: test_worker_run_id().to_string(),
            block_format_id: BlockFormatId::FULL_EFFECTIVE.as_raw(),
            block_size: BLOCK_SIZE,
            chunk_size: CHUNK_SIZE,
            effective_len: BLOCK_SIZE,
        })
        .unwrap_err();
        assert!(read_err.to_string().contains("missing block_id"));

        let read_err = proto_to_read_open_request(OpenReadStreamRequestProto {
            header: Some(test_header()),
            group_name: String::new(),
            block_id: Some(test_block_id_proto()),
            byte_range: Some(ByteRangeProto { offset: 0, len: 1 }),
            block_stamp: 0,
            frame_size: 1024,
            worker_run_id: test_worker_run_id().to_string(),
            block_format_id: BlockFormatId::FULL_EFFECTIVE.as_raw(),
            block_size: BLOCK_SIZE,
            chunk_size: CHUNK_SIZE,
            effective_len: BLOCK_SIZE,
        })
        .unwrap_err();
        assert!(read_err.to_string().contains("missing group_name"));

        let read_err = proto_to_read_open_request(OpenReadStreamRequestProto {
            header: Some(test_header()),
            group_name: "Root".to_string(),
            block_id: Some(test_block_id_proto()),
            byte_range: Some(ByteRangeProto { offset: 0, len: 1 }),
            block_stamp: 0,
            frame_size: 1024,
            worker_run_id: test_worker_run_id().to_string(),
            block_format_id: BlockFormatId::FULL_EFFECTIVE.as_raw(),
            block_size: BLOCK_SIZE,
            chunk_size: CHUNK_SIZE,
            effective_len: BLOCK_SIZE,
        })
        .unwrap_err();
        assert!(read_err.to_string().contains("group_name invalid"));

        let write_open_err = proto_to_write_open_request(OpenWriteStreamRequestProto {
            token: None,
            ..open_write_proto(1024)
        })
        .unwrap_err();
        assert!(write_open_err.to_string().contains("missing token"));

        let write_frame_err = proto_to_write_frame(WriteStreamRequestProto {
            stream_id: None,
            seq: 1,
            offset_in_block: 0,
            data: Bytes::new(),
            checksum32: 0,
        })
        .unwrap_err();
        assert!(write_frame_err.to_string().contains("missing stream_id"));

        let commit_err = proto_to_commit_write_request(CommitWriteRequestProto {
            header: Some(test_header()),
            group_name: "root".to_string(),
            block_id: Some(test_block_id_proto()),
            stream_id: None,
            effective_len: 1,
            block_stamp: BLOCK_STAMP,
            token: Some(test_token_proto()),
            commit_seq: 1,
            require_sync: false,
            worker_run_id: test_worker_run_id().to_string(),
            block_format_id: BlockFormatId::FULL_EFFECTIVE.as_raw(),
            block_size: BLOCK_SIZE,
            chunk_size: CHUNK_SIZE,
        })
        .unwrap_err();
        assert!(commit_err.to_string().contains("missing stream_id"));
    }

    #[tokio::test]
    async fn open_write_creates_staging_stream() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);

        let result = core.open_write(write_open_request()).await.expect("open write");

        assert_eq!(result.frame_size, 2048);
        assert_eq!(result.window_bytes, 4096);
        assert_eq!(result.block_stamp, BLOCK_STAMP);
        assert_eq!(result.committed_length, 0);

        let paths = store.paths(&group_name(), block_id());
        assert!(paths.staging_data_path.exists());
        assert!(paths.staging_meta_path.exists());
        assert!(!paths.meta_path.exists());
        assert_not_found(store.read_at(&group_name(), block_id(), 0, 1));

        let state = core
            .stream_manager()
            .get(result.stream_id)
            .await
            .expect("write stream registered");
        assert_eq!(state.context.group_name, group_name());
        assert_eq!(state.context.block_id, block_id());
        assert_eq!(state.context.mode, StreamMode::Write);
        assert_eq!(state.context.end_offset, BLOCK_SIZE);
        assert_eq!(state.cursor, 0);
        assert_eq!(state.last_acked_seq, 0);
        assert_eq!(state.written_through, 0);
    }

    #[tokio::test]
    async fn open_write_rejects_invalid_metadata_shape_before_staging() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        let paths = store.paths(&group_name(), block_id());

        let mut zero_stamp = write_open_request();
        zero_stamp.block_stamp = 0;
        assert_invalid_argument(core.open_write(zero_stamp).await);

        let mut non_aligned = write_open_request();
        non_aligned.chunk_size = 1000;
        assert_invalid_argument(core.open_write(non_aligned).await);

        let mut over_len = write_open_request();
        over_len.effective_len = BLOCK_SIZE + 1;
        assert_invalid_argument(core.open_write(over_len).await);

        assert!(!paths.staging_data_path.exists());
        assert!(!paths.staging_meta_path.exists());
        assert_eq!(core.stream_manager().active_count().await, 0);
    }

    #[tokio::test]
    async fn open_write_rejects_invalid_fencing_token() {
        let (_temp, _store, core) = core_with_store(512, 2048, 4096);
        let mut req = write_open_request();
        req.token = FencingToken::new(block_id(), ClientId::new(9), 0);

        match core.open_write(req).await.expect_err("zero epoch must be rejected") {
            WorkerError::Fencing(message) => assert!(message.contains("epoch")),
            other => panic!("expected Fencing, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn open_write_rejects_existing_ready_block() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        publish_ready_block(&store, payload(), BLOCK_STAMP);

        assert_need_refresh(
            core.open_write(write_open_request()).await,
            common::error::canonical::RefreshReason::Moved,
        );
        assert_eq!(core.stream_manager().active_count().await, 0);
    }

    #[tokio::test]
    async fn open_write_rejects_existing_ready_block_shape_mismatch() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        publish_ready_block(&store, payload(), BLOCK_STAMP);
        let mut req = write_open_request();
        req.block_size = BLOCK_SIZE * 2;

        assert_need_refresh(
            core.open_write(req).await,
            common::error::canonical::RefreshReason::StaleState,
        );
        assert_eq!(core.stream_manager().active_count().await, 0);
    }

    #[tokio::test]
    async fn open_write_rejects_existing_ready_block_stamp_mismatch() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        publish_ready_block(&store, payload(), BLOCK_STAMP + 1);

        assert_need_refresh(
            core.open_write(write_open_request()).await,
            common::error::canonical::RefreshReason::BlockStampMismatch,
        );
        assert_eq!(core.stream_manager().active_count().await, 0);
    }

    #[tokio::test]
    async fn write_stream_writes_staging_data_and_advances_state() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        let open = core.open_write(write_open_request()).await.expect("open write");
        let data = Bytes::from_static(b"abcd");

        let result = core
            .write_stream(WriteFrame {
                stream_id: open.stream_id,
                seq: 1,
                offset_in_block: 0,
                data: data.clone(),
                checksum32: 0,
            })
            .await
            .expect("write frame");

        assert!(result.accepted);
        assert_eq!(result.last_acked_seq, 1);
        assert_eq!(result.written_through, data.len() as u64);
        let state = core.stream_manager().get(open.stream_id).await.expect("stream state");
        assert_eq!(state.cursor, data.len() as u64);
        assert_eq!(state.last_acked_seq, 1);
        assert_eq!(state.written_through, data.len() as u64);
        assert!(!store.paths(&group_name(), block_id()).meta_path.exists());
    }

    #[tokio::test]
    async fn write_stream_rejects_seq_gap() {
        let (_temp, _store, core) = core_with_store(512, 2048, 4096);
        let open = core.open_write(write_open_request()).await.expect("open write");

        let result = core
            .write_stream(WriteFrame {
                stream_id: open.stream_id,
                seq: 2,
                offset_in_block: 0,
                data: Bytes::from_static(b"abcd"),
                checksum32: 0,
            })
            .await
            .expect("seq gap response");

        assert!(!result.accepted);
        assert_eq!(result.last_acked_seq, 0);
        assert_eq!(result.written_through, 0);
        assert_eq!(
            core.stream_manager().get(open.stream_id).await.expect("stream").cursor,
            0
        );
    }

    #[tokio::test]
    async fn write_stream_rejects_offset_gap() {
        let (_temp, _store, core) = core_with_store(512, 2048, 4096);
        let open = core.open_write(write_open_request()).await.expect("open write");

        let result = core
            .write_stream(WriteFrame {
                stream_id: open.stream_id,
                seq: 1,
                offset_in_block: 1,
                data: Bytes::from_static(b"abcd"),
                checksum32: 0,
            })
            .await
            .expect("offset gap response");

        assert!(!result.accepted);
        assert_eq!(result.last_acked_seq, 0);
        assert_eq!(result.written_through, 0);
        assert_eq!(
            core.stream_manager().get(open.stream_id).await.expect("stream").cursor,
            0
        );
    }

    #[tokio::test]
    async fn write_stream_rejects_read_stream() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        publish_ready_block(&store, payload(), BLOCK_STAMP);
        let open = core
            .open_read(read_open_request_for(0, 4, BLOCK_STAMP, 512))
            .await
            .expect("open read");

        match core
            .write_stream(WriteFrame {
                stream_id: open.stream_id,
                seq: 1,
                offset_in_block: 0,
                data: Bytes::from_static(b"abcd"),
                checksum32: 0,
            })
            .await
            .expect_err("read stream must reject writes")
        {
            WorkerError::InvalidArgument(message) => assert!(message.contains("not a write stream")),
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn commit_write_publishes_ready_block() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        let open = core.open_write(write_open_request()).await.expect("open write");
        let data = payload();
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data: data.slice(0..2048),
            checksum32: 0,
        })
        .await
        .expect("first frame");
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 2,
            offset_in_block: 2048,
            data: data.slice(2048..4096),
            checksum32: 0,
        })
        .await
        .expect("second frame");

        let result = core
            .commit_write(CommitWriteRequest {
                stream_id: open.stream_id,
                commit_seq: 2,
                effective_len: BLOCK_SIZE,
                ..commit_write_request()
            })
            .await
            .expect("commit write");

        assert_eq!(result.effective_len, BLOCK_SIZE);
        assert_eq!(result.block_stamp, BLOCK_STAMP);
        assert_eq!(result.written_through, BLOCK_SIZE);
        let meta = store.load_meta(&group_name(), block_id()).expect("ready meta");
        assert_eq!(meta.visibility.block_state, crate::store::block::BlockState::Ready);
        assert_eq!(meta.visibility.block_stamp, BLOCK_STAMP);
        assert_eq!(store.read_at(&group_name(), block_id(), 0, BLOCK_SIZE).unwrap(), data);
    }

    #[tokio::test]
    async fn multichunk_write_commit_and_read_returns_exact_effective_bytes() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        let effective_len = 3073;
        let data = payload().slice(0..effective_len as usize);
        let mut open_req = write_open_request();
        open_req.effective_len = effective_len;
        let open = core.open_write(open_req).await.expect("open write");

        let chunks = [
            data.slice(0..700),
            data.slice(700..1536),
            data.slice(1536..2500),
            data.slice(2500..effective_len as usize),
        ];
        let mut offset = 0u64;
        for (idx, chunk) in chunks.into_iter().enumerate() {
            core.write_stream(WriteFrame {
                stream_id: open.stream_id,
                seq: (idx + 1) as u64,
                offset_in_block: offset,
                data: chunk.clone(),
                checksum32: 0,
            })
            .await
            .expect("write chunk");
            offset += chunk.len() as u64;
        }

        let result = core
            .commit_write(CommitWriteRequest {
                stream_id: open.stream_id,
                commit_seq: 4,
                effective_len,
                ..commit_write_request()
            })
            .await
            .expect("commit write");

        assert_eq!(result.effective_len, effective_len);
        assert_eq!(result.written_through, effective_len);
        let meta = store.load_meta(&group_name(), block_id()).expect("ready meta");
        assert_eq!(meta.source.effective_len, effective_len);
        assert_eq!(
            store.read_at(&group_name(), block_id(), 0, effective_len).unwrap(),
            data
        );

        let open_read = core
            .open_read(read_open_request_for_len(
                0,
                effective_len as u32,
                BLOCK_STAMP,
                effective_len,
                600,
            ))
            .await
            .expect("open read");
        assert_eq!(collect_core_read(&core, open_read.stream_id, 600).await, data);

        let eof_read = core
            .open_read(read_open_request_for_len(
                effective_len,
                0,
                BLOCK_STAMP,
                effective_len,
                600,
            ))
            .await
            .expect("open eof read");
        assert!(collect_core_read(&core, eof_read.stream_id, 600).await.is_empty());
        assert_invalid_argument(
            core.open_read(read_open_request_for_len(
                effective_len,
                1,
                BLOCK_STAMP,
                effective_len,
                600,
            ))
            .await,
        );
    }

    #[tokio::test]
    async fn commit_write_accepts_non_chunk_aligned_tail_and_persists_full_block_shape() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        let open = core.open_write(write_open_request()).await.expect("open write");
        let effective_len = u64::from(CHUNK_SIZE) + 1;
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data: Bytes::from(vec![7; effective_len as usize]),
            checksum32: 0,
        })
        .await
        .expect("tail frame");

        let result = core
            .commit_write(CommitWriteRequest {
                stream_id: open.stream_id,
                commit_seq: 1,
                effective_len,
                ..commit_write_request()
            })
            .await
            .expect("tail commit");

        assert_eq!(result.effective_len, effective_len);
        assert_eq!(result.written_through, effective_len);
        let meta = store.load_meta(&group_name(), block_id()).expect("ready meta");
        assert_eq!(meta.format.block_size, BLOCK_SIZE);
        assert_eq!(meta.source.effective_len, effective_len);
    }

    #[tokio::test]
    async fn commit_write_rejects_effective_len_larger_than_block_size() {
        let (_temp, _store, core) = core_with_store(512, 2048, 4096);
        let open = core.open_write(write_open_request()).await.expect("open write");

        assert_invalid_argument(
            core.commit_write(CommitWriteRequest {
                stream_id: open.stream_id,
                commit_seq: 0,
                effective_len: BLOCK_SIZE + 1,
                ..commit_write_request()
            })
            .await,
        );
    }

    #[tokio::test]
    async fn commit_write_rejects_layout_mismatch_against_open_request() {
        let (_temp, _store, core) = core_with_store(512, 2048, 4096);
        let mut open_req = write_open_request();
        open_req.effective_len = 4;
        let open = core.open_write(open_req).await.expect("open write");
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data: Bytes::from_static(b"abcd"),
            checksum32: 0,
        })
        .await
        .expect("write frame");

        assert_need_refresh(
            core.commit_write(CommitWriteRequest {
                stream_id: open.stream_id,
                commit_seq: 1,
                effective_len: 4,
                chunk_size: CHUNK_SIZE * 2,
                ..commit_write_request()
            })
            .await,
            common::error::canonical::RefreshReason::StaleState,
        );
    }

    #[tokio::test]
    async fn commit_write_rejects_incomplete_block() {
        let (_temp, _store, core) = core_with_store(512, 2048, 4096);
        let open = core.open_write(write_open_request()).await.expect("open write");
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data: Bytes::from_static(b"abcd"),
            checksum32: 0,
        })
        .await
        .expect("write frame");

        assert_invalid_argument(
            core.commit_write(CommitWriteRequest {
                stream_id: open.stream_id,
                commit_seq: 1,
                effective_len: BLOCK_SIZE,
                ..commit_write_request()
            })
            .await,
        );
    }

    #[tokio::test]
    async fn commit_write_rejects_token_mismatch() {
        let (_temp, _store, core) = core_with_store(512, 2048, 4096);
        let open = core.open_write(write_open_request()).await.expect("open write");
        let data = payload();
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data,
            checksum32: 0,
        })
        .await
        .expect("write frame");

        match core
            .commit_write(CommitWriteRequest {
                stream_id: open.stream_id,
                token: FencingToken::new(block_id(), ClientId::new(99), 11),
                commit_seq: 1,
                effective_len: BLOCK_SIZE,
                ..commit_write_request()
            })
            .await
            .expect_err("token mismatch must be rejected")
        {
            WorkerError::Fencing(message) => assert!(message.contains("token")),
            other => panic!("expected Fencing, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn commit_write_removes_stream_after_success() {
        let (_temp, _store, core) = core_with_store(512, 2048, 4096);
        let open = core.open_write(write_open_request()).await.expect("open write");
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data: payload(),
            checksum32: 0,
        })
        .await
        .expect("write frame");

        core.commit_write(CommitWriteRequest {
            stream_id: open.stream_id,
            commit_seq: 1,
            effective_len: BLOCK_SIZE,
            ..commit_write_request()
        })
        .await
        .expect("commit write");

        assert!(core.stream_manager().get(open.stream_id).await.is_none());
        assert_not_found(
            core.commit_write(CommitWriteRequest {
                stream_id: open.stream_id,
                commit_seq: 1,
                effective_len: BLOCK_SIZE,
                ..commit_write_request()
            })
            .await,
        );
    }

    #[tokio::test]
    async fn duplicate_commit_fails_without_republishing_or_corrupting_ready_block() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        let open = core.open_write(write_open_request()).await.expect("open write");
        let data = payload();
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data: data.clone(),
            checksum32: 0,
        })
        .await
        .expect("write frame");

        core.commit_write(CommitWriteRequest {
            stream_id: open.stream_id,
            commit_seq: 1,
            effective_len: BLOCK_SIZE,
            ..commit_write_request()
        })
        .await
        .expect("first commit");
        assert_not_found(
            core.commit_write(CommitWriteRequest {
                stream_id: open.stream_id,
                commit_seq: 1,
                effective_len: BLOCK_SIZE,
                ..commit_write_request()
            })
            .await,
        );

        let scanned = store.scan_group_blocks(&group_name()).expect("scan group");
        assert_eq!(scanned.len(), 1);
        assert_eq!(
            scanned[0].visibility.block_state,
            crate::store::block::BlockState::Ready
        );
        assert_eq!(store.read_at(&group_name(), block_id(), 0, BLOCK_SIZE).unwrap(), data);
    }

    #[tokio::test]
    async fn sync_committed_block_succeeds_after_terminal_commit_without_stream() {
        let (_temp, _store, core) = core_with_store(512, 2048, 4096);
        let open = core.open_write(write_open_request()).await.expect("open write");
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data: payload(),
            checksum32: 0,
        })
        .await
        .expect("write frame");
        core.commit_write(CommitWriteRequest {
            stream_id: open.stream_id,
            commit_seq: 1,
            effective_len: BLOCK_SIZE,
            require_sync: false,
            ..commit_write_request()
        })
        .await
        .expect("visibility commit");
        assert!(core.stream_manager().get(open.stream_id).await.is_none());

        let result = core
            .sync_committed_block(sync_committed_block_request(BLOCK_STAMP, BLOCK_SIZE))
            .await
            .expect("sync committed block");

        assert_eq!(result.effective_len, BLOCK_SIZE);
        assert_eq!(result.block_stamp, BLOCK_STAMP);
    }

    #[tokio::test]
    async fn sync_committed_block_rejects_missing_wrong_generation_and_uncommitted_block() {
        let (_temp, _store, core) = core_with_store(512, 2048, 4096);
        assert_need_refresh(
            core.sync_committed_block(sync_committed_block_request(BLOCK_STAMP, BLOCK_SIZE))
                .await,
            common::error::canonical::RefreshReason::Moved,
        );

        let open = core.open_write(write_open_request()).await.expect("open write");
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data: payload(),
            checksum32: 0,
        })
        .await
        .expect("write frame");
        assert_need_refresh(
            core.sync_committed_block(sync_committed_block_request(BLOCK_STAMP, BLOCK_SIZE))
                .await,
            common::error::canonical::RefreshReason::Moved,
        );

        core.commit_write(CommitWriteRequest {
            stream_id: open.stream_id,
            commit_seq: 1,
            effective_len: BLOCK_SIZE,
            ..commit_write_request()
        })
        .await
        .expect("commit write");
        assert_need_refresh(
            core.sync_committed_block(sync_committed_block_request(BLOCK_STAMP + 1, BLOCK_SIZE))
                .await,
            common::error::canonical::RefreshReason::BlockStampMismatch,
        );
        assert_need_refresh(
            core.sync_committed_block(sync_committed_block_request(BLOCK_STAMP, BLOCK_SIZE - 1))
                .await,
            common::error::canonical::RefreshReason::StaleState,
        );
    }

    #[tokio::test]
    async fn sync_committed_block_rejects_block_layout_mismatch() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        publish_ready_block(store.as_ref(), payload(), BLOCK_STAMP);

        let mut block_size_mismatch = sync_committed_block_request(BLOCK_STAMP, BLOCK_SIZE);
        block_size_mismatch.block_size = BLOCK_SIZE * 2;
        assert_need_refresh(
            core.sync_committed_block(block_size_mismatch).await,
            common::error::canonical::RefreshReason::StaleState,
        );

        let mut chunk_size_mismatch = sync_committed_block_request(BLOCK_STAMP, BLOCK_SIZE);
        chunk_size_mismatch.chunk_size = CHUNK_SIZE * 2;
        assert_need_refresh(
            core.sync_committed_block(chunk_size_mismatch).await,
            common::error::canonical::RefreshReason::StaleState,
        );
    }

    #[tokio::test]
    async fn repeated_sync_committed_block_is_idempotent() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        publish_ready_block(store.as_ref(), payload(), BLOCK_STAMP);

        let first = core
            .sync_committed_block(sync_committed_block_request(BLOCK_STAMP, BLOCK_SIZE))
            .await
            .expect("first sync");
        let second = core
            .sync_committed_block(sync_committed_block_request(BLOCK_STAMP, BLOCK_SIZE))
            .await
            .expect("second sync");

        assert_eq!(first, second);
    }

    #[tokio::test]
    async fn abort_write_removes_stream_and_staging_block() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        let open = core.open_write(write_open_request()).await.expect("open write");
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data: Bytes::from_static(b"abcd"),
            checksum32: 0,
        })
        .await
        .expect("write frame");

        let result = core
            .abort_write(AbortWriteRequest {
                stream_id: open.stream_id,
                ..abort_write_request()
            })
            .await
            .expect("abort write");

        assert!(result.aborted);
        assert!(core.stream_manager().get(open.stream_id).await.is_none());
        let paths = store.paths(&group_name(), block_id());
        assert!(!paths.staging_data_path.exists());
        assert!(!paths.staging_meta_path.exists());
    }

    #[tokio::test]
    async fn abort_write_keeps_no_readable_block() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        let open = core.open_write(write_open_request()).await.expect("open write");

        core.abort_write(AbortWriteRequest {
            stream_id: open.stream_id,
            ..abort_write_request()
        })
        .await
        .expect("abort write");

        assert_not_found(store.read_at(&group_name(), block_id(), 0, 1));
        assert!(!store.paths(&group_name(), block_id()).meta_path.exists());
    }

    #[tokio::test]
    async fn partial_write_then_abort_is_not_ready_or_reportable() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        let open = core.open_write(write_open_request()).await.expect("open write");
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data: Bytes::from_static(b"partial"),
            checksum32: 0,
        })
        .await
        .expect("partial frame");

        core.abort_write(AbortWriteRequest {
            stream_id: open.stream_id,
            ..abort_write_request()
        })
        .await
        .expect("abort write");

        assert!(core.stream_manager().get(open.stream_id).await.is_none());
        assert!(!store.paths(&group_name(), block_id()).meta_path.exists());
        assert_not_found(store.read_at(&group_name(), block_id(), 0, 1));
        assert_need_refresh(
            core.open_read(read_open_request_for(0, 1, BLOCK_STAMP, 512)).await,
            RefreshReason::BlockLocationUnavailable,
        );
        assert!(store.scan_group_blocks(&group_name()).expect("scan group").is_empty());
    }

    #[tokio::test]
    async fn commit_after_abort_fails_without_publishing_ready_block() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        let open = core.open_write(write_open_request()).await.expect("open write");
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data: Bytes::from_static(b"partial"),
            checksum32: 0,
        })
        .await
        .expect("partial frame");
        core.abort_write(AbortWriteRequest {
            stream_id: open.stream_id,
            ..abort_write_request()
        })
        .await
        .expect("abort write");

        assert_not_found(
            core.commit_write(CommitWriteRequest {
                stream_id: open.stream_id,
                commit_seq: 1,
                effective_len: 7,
                ..commit_write_request()
            })
            .await,
        );
        assert!(store.scan_group_blocks(&group_name()).expect("scan group").is_empty());
        assert_not_found(store.read_at(&group_name(), block_id(), 0, 1));
    }

    #[tokio::test]
    async fn duplicate_abort_fails_without_publishing_ready_block() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        let open = core.open_write(write_open_request()).await.expect("open write");

        core.abort_write(AbortWriteRequest {
            stream_id: open.stream_id,
            ..abort_write_request()
        })
        .await
        .expect("first abort");
        assert_not_found(
            core.abort_write(AbortWriteRequest {
                stream_id: open.stream_id,
                ..abort_write_request()
            })
            .await,
        );

        assert!(store.scan_group_blocks(&group_name()).expect("scan group").is_empty());
        assert_not_found(store.read_at(&group_name(), block_id(), 0, 1));
    }

    #[tokio::test]
    async fn abort_after_successful_commit_does_not_damage_ready_block() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        let open = core.open_write(write_open_request()).await.expect("open write");
        let data = payload();
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data: data.clone(),
            checksum32: 0,
        })
        .await
        .expect("write frame");
        core.commit_write(CommitWriteRequest {
            stream_id: open.stream_id,
            commit_seq: 1,
            effective_len: BLOCK_SIZE,
            ..commit_write_request()
        })
        .await
        .expect("commit write");

        assert_not_found(
            core.abort_write(AbortWriteRequest {
                stream_id: open.stream_id,
                ..abort_write_request()
            })
            .await,
        );

        let scanned = store.scan_group_blocks(&group_name()).expect("scan group");
        assert_eq!(scanned.len(), 1);
        assert_eq!(store.read_at(&group_name(), block_id(), 0, BLOCK_SIZE).unwrap(), data);
    }

    #[tokio::test]
    async fn write_stream_cancellation_discards_partial_staging_state() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        let core = Arc::new(core);
        let service = registered_data_service(Arc::clone(&core));
        let open = service
            .open_write_stream(tonic::Request::new(open_write_proto(0)))
            .await
            .expect("open write")
            .into_inner();
        let stream_id = crate::data::convert::proto_to_stream_id(open.stream_id, "stream_id").expect("stream id");
        let cancelled = Status::cancelled("client cancelled write stream");

        let status = service
            .handle_write_frames(futures::stream::iter(vec![
                Ok(WriteStreamRequestProto {
                    stream_id: Some(crate::data::convert::stream_id_to_proto(stream_id)),
                    seq: 1,
                    offset_in_block: 0,
                    data: Bytes::from_static(b"partial"),
                    checksum32: 0,
                }),
                Err(cancelled),
            ]))
            .await
            .expect_err("cancelled write stream must fail");

        assert_eq!(status.code(), tonic::Code::Cancelled);
        assert!(core.stream_manager().get(stream_id).await.is_none());
        assert!(!store.paths(&group_name(), block_id()).meta_path.exists());
        assert_not_found(store.read_at(&group_name(), block_id(), 0, 1));
        assert!(store.scan_group_blocks(&group_name()).expect("scan group").is_empty());
    }

    #[tokio::test]
    async fn recover_after_uncommitted_write_is_not_readable() {
        let (temp, _store, core) = core_with_store(512, 2048, 4096);
        let open = core.open_write(write_open_request()).await.expect("open write");
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data: Bytes::from_static(b"abcd"),
            checksum32: 0,
        })
        .await
        .expect("write frame");

        let recovered_store = FullBlockFileStore::new(FullBlockFileStoreConfig::new(temp.path().to_path_buf()));
        assert_not_found(recovered_store.recover_block(&group_name(), block_id()));
        assert_not_found(recovered_store.read_at(&group_name(), block_id(), 0, 1));
    }

    #[tokio::test]
    async fn incomplete_staging_write_is_ignored_by_ready_block_scan() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        let open = core.open_write(write_open_request()).await.expect("open write");
        core.write_stream(WriteFrame {
            stream_id: open.stream_id,
            seq: 1,
            offset_in_block: 0,
            data: Bytes::from_static(b"partial"),
            checksum32: 0,
        })
        .await
        .expect("partial frame");

        let paths = store.paths(&group_name(), block_id());
        assert!(paths.staging_data_path.exists());
        assert!(paths.staging_meta_path.exists());
        assert!(!paths.meta_path.exists());
        assert!(store.scan_group_blocks(&group_name()).expect("scan group").is_empty());
    }

    #[tokio::test]
    async fn open_read_ready_block_succeeds() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        publish_ready_block(&store, payload(), BLOCK_STAMP);

        let result = core
            .open_read(read_open_request_for(128, 1024, BLOCK_STAMP, 0))
            .await
            .expect("open read");

        assert_eq!(result.frame_size, 512);
        assert_eq!(result.window_bytes, 4096);
        assert_eq!(result.block_stamp, BLOCK_STAMP);
        assert_eq!(result.committed_length, BLOCK_SIZE);

        let state = core
            .stream_manager()
            .get(result.stream_id)
            .await
            .expect("read stream registered");
        assert_eq!(state.context.group_name, group_name());
        assert_eq!(state.context.block_id, block_id());
        assert_eq!(state.context.mode, StreamMode::Read);
        assert_eq!(state.context.start_offset, 128);
        assert_eq!(state.context.end_offset, 1152);
        assert_eq!(state.cursor, 128);
        assert_eq!(state.context.effective_len, BLOCK_SIZE);
    }

    #[tokio::test]
    async fn worker_core_uses_configured_store_dir() {
        let custom_dir = TempDir::new().expect("custom store dir");
        let other_dir = TempDir::new().expect("other store dir");
        let store = FullBlockFileStore::new(FullBlockFileStoreConfig::new(custom_dir.path().to_path_buf()));
        publish_ready_block(&store, payload(), BLOCK_STAMP);

        let core = WorkerCore::with_options(
            512,
            2048,
            4096,
            Duration::from_secs(60),
            custom_dir.path().to_path_buf(),
        );

        let result = core
            .open_read(read_open_request_for(0, 8, BLOCK_STAMP, 512))
            .await
            .expect("open read from configured store dir");
        assert!(core.stream_manager().get(result.stream_id).await.is_some());

        let paths = store.paths(&group_name(), block_id());
        assert!(paths.data_path.starts_with(custom_dir.path()));
        assert!(paths.meta_path.starts_with(custom_dir.path()));
        assert!(
            paths.data_path.exists(),
            "ready block data must exist under custom store dir"
        );
        assert!(
            paths.meta_path.exists(),
            "ready block metadata must exist under custom store dir"
        );

        let other_store = FullBlockFileStore::new(FullBlockFileStoreConfig::new(other_dir.path().to_path_buf()));
        let other_paths = other_store.paths(&group_name(), block_id());
        assert!(
            !other_paths.data_path.exists(),
            "ready block data must not be created under other store dir"
        );
        assert!(
            !other_paths.meta_path.exists(),
            "ready block metadata must not be created under other store dir"
        );
    }

    #[tokio::test]
    async fn open_read_rejects_block_stamp_mismatch() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        publish_ready_block(&store, payload(), BLOCK_STAMP);

        assert_need_refresh(
            core.open_read(read_open_request_for(0, 1024, BLOCK_STAMP + 1, 512))
                .await,
            common::error::canonical::RefreshReason::BlockStampMismatch,
        );
        assert_eq!(core.stream_manager().active_count().await, 0);
    }

    #[tokio::test]
    async fn open_read_rejects_block_layout_mismatch() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        publish_ready_block(&store, payload(), BLOCK_STAMP);

        let mut block_size_mismatch = read_open_request_for(0, 1024, BLOCK_STAMP, 512);
        block_size_mismatch.block_size = BLOCK_SIZE * 2;
        assert_need_refresh(
            core.open_read(block_size_mismatch).await,
            common::error::canonical::RefreshReason::StaleState,
        );

        let mut chunk_size_mismatch = read_open_request_for(0, 1024, BLOCK_STAMP, 512);
        chunk_size_mismatch.chunk_size = CHUNK_SIZE * 2;
        assert_need_refresh(
            core.open_read(chunk_size_mismatch).await,
            common::error::canonical::RefreshReason::StaleState,
        );
        assert_eq!(core.stream_manager().active_count().await, 0);
    }

    #[tokio::test]
    async fn open_read_rejects_zero_block_stamp_for_ready_block() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        publish_ready_block(&store, payload(), BLOCK_STAMP);

        assert_invalid_argument(core.open_read(read_open_request_for(0, 1024, 0, 512)).await);
        assert_eq!(core.stream_manager().active_count().await, 0);
    }

    #[tokio::test]
    async fn open_read_rejects_missing_block() {
        let (_temp, _store, core) = core_with_store(512, 2048, 4096);

        assert_need_refresh(
            core.open_read(read_open_request_for(0, 1024, BLOCK_STAMP, 512)).await,
            common::error::canonical::RefreshReason::BlockLocationUnavailable,
        );
        assert_eq!(core.stream_manager().active_count().await, 0);
    }

    #[tokio::test]
    async fn open_read_rejects_out_of_bounds_range() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        publish_ready_block(&store, payload(), BLOCK_STAMP);

        assert_invalid_argument(core.open_read(read_open_request_for(4090, 16, BLOCK_STAMP, 512)).await);
        assert_eq!(core.stream_manager().active_count().await, 0);
    }

    #[tokio::test]
    async fn read_stream_reads_single_frame() {
        let (_temp, store, core) = core_with_store(1024, 2048, 4096);
        let data = payload();
        publish_ready_block(&store, data.clone(), BLOCK_STAMP);
        let open = core
            .open_read(read_open_request_for(10, 5, BLOCK_STAMP, 1024))
            .await
            .expect("open read");

        let frames = core.read_stream(open.stream_id, 0).await.expect("read stream");

        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].offset_in_block, 10);
        assert_eq!(frames[0].data, data.slice(10..15));
        assert!(frames[0].eos);
        assert_eq!(core.stream_manager().active_count().await, 0);
    }

    #[tokio::test]
    async fn read_stream_advances_cursor_across_calls() {
        let (_temp, store, core) = core_with_store(4, 16, 4096);
        let data = payload();
        publish_ready_block(&store, data.clone(), BLOCK_STAMP);
        let open = core
            .open_read(read_open_request_for(0, 8, BLOCK_STAMP, 4))
            .await
            .expect("open read");

        let first = core.read_stream(open.stream_id, 4).await.expect("first read");
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].data, data.slice(0..4));
        assert!(!first[0].eos);
        assert_eq!(
            core.stream_manager().get(open.stream_id).await.expect("stream").cursor,
            4
        );

        let second = core.read_stream(open.stream_id, 4).await.expect("second read");
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].data, data.slice(4..8));
        assert!(second[0].eos);
        assert!(core.stream_manager().get(open.stream_id).await.is_none());
    }

    #[tokio::test]
    async fn read_stream_respects_max_bytes() {
        let (_temp, store, core) = core_with_store(8, 16, 4096);
        let data = payload();
        publish_ready_block(&store, data.clone(), BLOCK_STAMP);
        let open = core
            .open_read(read_open_request_for(0, 8, BLOCK_STAMP, 8))
            .await
            .expect("open read");

        let frames = core.read_stream(open.stream_id, 3).await.expect("read stream");

        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data.len(), 3);
        assert_eq!(frames[0].data, data.slice(0..3));
        assert!(!frames[0].eos);
    }

    #[tokio::test]
    async fn read_stream_offset_length_and_eof_boundaries_are_exact() {
        let (_temp, store, core) = core_with_store(513, 2048, 4096);
        let effective_len = u64::from(CHUNK_SIZE) * 2 + 17;
        let data = payload().slice(0..effective_len as usize);
        publish_ready_block(&store, data.clone(), BLOCK_STAMP);

        let full = core
            .open_read(read_open_request_for_len(
                0,
                effective_len as u32,
                BLOCK_STAMP,
                effective_len,
                513,
            ))
            .await
            .expect("open full read");
        assert_eq!(collect_core_read(&core, full.stream_id, 513).await, data);

        let nonzero = core
            .open_read(read_open_request_for_len(17, 100, BLOCK_STAMP, effective_len, 64))
            .await
            .expect("open nonzero read");
        assert_eq!(
            collect_core_read(&core, nonzero.stream_id, 64).await,
            data.slice(17..117)
        );

        let short = core
            .open_read(read_open_request_for_len(100, 3, BLOCK_STAMP, effective_len, 64))
            .await
            .expect("open short read");
        assert_eq!(
            collect_core_read(&core, short.stream_id, 64).await,
            data.slice(100..103)
        );

        let boundary_offset = u64::from(CHUNK_SIZE) - 3;
        let across_chunk = core
            .open_read(read_open_request_for_len(
                boundary_offset,
                10,
                BLOCK_STAMP,
                effective_len,
                4,
            ))
            .await
            .expect("open chunk boundary read");
        assert_eq!(
            collect_core_read(&core, across_chunk.stream_id, 4).await,
            data.slice(boundary_offset as usize..boundary_offset as usize + 10)
        );

        let eof = core
            .open_read(read_open_request_for_len(
                effective_len,
                0,
                BLOCK_STAMP,
                effective_len,
                64,
            ))
            .await
            .expect("open eof read");
        assert!(collect_core_read(&core, eof.stream_id, 64).await.is_empty());

        assert_invalid_argument(
            core.open_read(read_open_request_for_len(
                effective_len,
                1,
                BLOCK_STAMP,
                effective_len,
                64,
            ))
            .await,
        );
        assert_invalid_argument(
            core.open_read(read_open_request_for_len(
                effective_len - 1,
                2,
                BLOCK_STAMP,
                effective_len,
                64,
            ))
            .await,
        );
    }

    #[tokio::test]
    async fn read_stream_rejects_missing_stream() {
        let (_temp, _store, core) = core_with_store(8, 16, 4096);

        assert_not_found(core.read_stream(stream_id(), 1024).await);
    }

    #[tokio::test]
    async fn read_stream_rejects_write_stream() {
        let (_temp, _store, core) = core_with_store(8, 16, 4096);
        let state = StreamState::new(write_stream_context());
        core.stream_manager().register(state).await;

        match core
            .read_stream(stream_id(), 1024)
            .await
            .expect_err("write stream must not be readable")
        {
            WorkerError::InvalidArgument(message) => assert!(message.contains("not a read stream")),
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
        assert_eq!(core.stream_manager().get(stream_id()).await.expect("stream").cursor, 0);
    }

    #[tokio::test]
    async fn open_write_stream_returns_success_response() {
        let (_temp, _store, core) = core_with_store(512, 2048, 4096);
        let service = registered_data_service(Arc::new(core));

        let response = service
            .open_write_stream(tonic::Request::new(open_write_proto(0)))
            .await
            .expect("open write response")
            .into_inner();

        assert!(response.header.expect("header").error.is_none());
        assert!(response.stream_id.is_some());
        assert_eq!(response.frame_size, 512);
        assert_eq!(response.window_bytes, 4096);
        assert_eq!(response.block_stamp, BLOCK_STAMP);
        assert_eq!(response.committed_length, 0);
    }

    #[tokio::test]
    async fn guarded_data_service_rejects_open_before_registration() {
        let (_temp, _store, core) = core_with_store(512, 2048, 4096);
        let state = Arc::new(RegistrationSet::new());
        let service = WorkerDataServiceImpl::new(Arc::new(core), Arc::clone(&state));

        let response = service
            .open_write_stream(tonic::Request::new(open_write_proto(0)))
            .await
            .expect("open write response")
            .into_inner();
        let error = response.header.expect("header").error.expect("header error");

        assert_eq!(error.error_class, ErrorClassProto::ErrorClassNeedRefresh as i32);
        assert!(error.message.contains("not registered"));
        assert!(response.stream_id.is_none());
    }

    #[tokio::test]
    async fn guarded_data_service_allows_open_after_registration() {
        let (_temp, _store, core) = core_with_store(512, 2048, 4096);
        let state = Arc::new(RegistrationSet::new());
        mark_registered(&state);
        let service = WorkerDataServiceImpl::new(Arc::new(core), Arc::clone(&state));

        let response = service
            .open_write_stream(tonic::Request::new(open_write_proto(0)))
            .await
            .expect("open write response")
            .into_inner();

        assert!(response.header.expect("header").error.is_none());
        assert!(response.stream_id.is_some());
    }

    #[tokio::test]
    async fn guarded_data_service_rejects_stale_worker_run_id() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        publish_ready_block(&store, payload(), BLOCK_STAMP);
        let service = registered_data_service(Arc::new(core));
        let mut request = open_read_proto(0, 1024, BLOCK_STAMP, 0);
        request.worker_run_id = other_worker_run_id().to_string();

        let response = service
            .open_read_stream(tonic::Request::new(request))
            .await
            .expect("open read response")
            .into_inner();
        let error = response.header.expect("header").error.expect("header error");

        assert_eq!(error.error_class, ErrorClassProto::ErrorClassNeedRefresh as i32);
        assert_eq!(
            error.refresh_reason,
            RefreshReasonProto::RefreshReasonWorkerRunMismatch as i32
        );
        assert!(response.stream_id.is_none());
    }

    #[tokio::test]
    async fn write_stream_returns_written_through() {
        let (_temp, _store, core) = core_with_store(512, 2048, 4096);
        let core = Arc::new(core);
        let state = Arc::new(RegistrationSet::new());
        mark_registered(&state);
        let service = WorkerDataServiceImpl::new(core.clone(), state);
        let open = core.open_write(write_open_request()).await.expect("open write");

        let response = service
            .handle_write_frames(futures::stream::iter(vec![Ok(WriteStreamRequestProto {
                stream_id: Some(crate::data::convert::stream_id_to_proto(open.stream_id)),
                seq: 1,
                offset_in_block: 0,
                data: Bytes::from_static(b"abcd"),
                checksum32: 0,
            })]))
            .await
            .expect("write stream response");

        assert!(response.accepted);
        assert_eq!(response.last_acked_seq, 1);
        assert_eq!(response.written_through, 4);
    }

    #[tokio::test]
    async fn write_stream_error_releases_store_dir_pending_reservation() {
        let temp = TempDir::new().expect("tempdir");
        let store = report_store(&temp);
        let core = Arc::new(WorkerCore::with_local_store(
            512,
            2048,
            4096,
            Duration::from_secs(60),
            store.clone(),
        ));
        let service = registered_data_service(Arc::clone(&core));
        let open = service
            .open_write_stream(tonic::Request::new(open_write_proto(0)))
            .await
            .expect("open write")
            .into_inner();
        let stream_id = crate::data::convert::proto_to_stream_id(open.stream_id, "stream_id").expect("stream id");

        assert_eq!(store.report().expect("store report").pending_bytes, BLOCK_SIZE);

        let response = service
            .handle_write_frames(futures::stream::iter(vec![Ok(WriteStreamRequestProto {
                stream_id: Some(crate::data::convert::stream_id_to_proto(stream_id)),
                seq: 2,
                offset_in_block: 0,
                data: Bytes::from_static(b"abcd"),
                checksum32: 0,
            })]))
            .await
            .expect("write stream response");

        assert!(!response.accepted);
        assert_eq!(store.report().expect("store report").pending_bytes, 0);
        assert!(core.stream_manager().get(stream_id).await.is_none());
    }

    #[tokio::test]
    async fn write_stream_frame_error_decrements_inflight_once() {
        let recorder = StreamGaugeRecorder::default();

        metrics::with_local_recorder(&recorder, || {
            futures::executor::block_on(async {
                let (_temp, _store, core) = core_with_store(512, 2048, 4096);
                let core = Arc::new(core);
                let service = registered_data_service(Arc::clone(&core));
                let open = service
                    .open_write_stream(tonic::Request::new(open_write_proto(0)))
                    .await
                    .expect("open write")
                    .into_inner();
                let stream_id =
                    crate::data::convert::proto_to_stream_id(open.stream_id, "stream_id").expect("stream id");

                let response = service
                    .handle_write_frames(futures::stream::iter(vec![Ok(WriteStreamRequestProto {
                        stream_id: Some(crate::data::convert::stream_id_to_proto(stream_id)),
                        seq: 2,
                        offset_in_block: 0,
                        data: Bytes::from_static(b"abcd"),
                        checksum32: 0,
                    })]))
                    .await
                    .expect("write stream response");

                assert!(!response.accepted);
                assert!(core.stream_manager().get(stream_id).await.is_none());
            });
        });

        assert_eq!(
            recorder.stream_values(),
            vec![("write".to_string(), 1.0), ("write".to_string(), -1.0)]
        );
    }

    #[tokio::test]
    async fn commit_write_success_decrements_inflight_once() {
        let recorder = StreamGaugeRecorder::default();

        metrics::with_local_recorder(&recorder, || {
            futures::executor::block_on(async {
                let (_temp, _store, core) = core_with_store(512, 2048, 4096);
                let service = registered_data_service(Arc::new(core));
                let open = service
                    .open_write_stream(tonic::Request::new(open_write_proto(2048)))
                    .await
                    .expect("open write")
                    .into_inner();
                let stream_id =
                    crate::data::convert::proto_to_stream_id(open.stream_id, "stream_id").expect("stream id");
                let data = payload();

                service
                    .handle_write_frames(futures::stream::iter(vec![
                        Ok(WriteStreamRequestProto {
                            stream_id: Some(crate::data::convert::stream_id_to_proto(stream_id)),
                            seq: 1,
                            offset_in_block: 0,
                            data: data.slice(0..2048),
                            checksum32: 0,
                        }),
                        Ok(WriteStreamRequestProto {
                            stream_id: Some(crate::data::convert::stream_id_to_proto(stream_id)),
                            seq: 2,
                            offset_in_block: 2048,
                            data: data.slice(2048..4096),
                            checksum32: 0,
                        }),
                    ]))
                    .await
                    .expect("write frames");

                let response = service
                    .commit_write(tonic::Request::new(commit_write_proto(stream_id, 2, BLOCK_SIZE)))
                    .await
                    .expect("commit write")
                    .into_inner();

                assert!(response.header.expect("header").error.is_none());
            });
        });

        assert_eq!(
            recorder.stream_values(),
            vec![("write".to_string(), 1.0), ("write".to_string(), -1.0)]
        );
    }

    #[tokio::test]
    async fn commit_write_error_decrements_inflight_once() {
        let recorder = StreamGaugeRecorder::default();

        metrics::with_local_recorder(&recorder, || {
            futures::executor::block_on(async {
                let (_temp, _store, core) = core_with_store(512, 2048, 4096);
                let core = Arc::new(core);
                let service = registered_data_service(Arc::clone(&core));
                let open = service
                    .open_write_stream(tonic::Request::new(open_write_proto(2048)))
                    .await
                    .expect("open write")
                    .into_inner();
                let stream_id =
                    crate::data::convert::proto_to_stream_id(open.stream_id, "stream_id").expect("stream id");

                let response = service
                    .commit_write(tonic::Request::new(commit_write_proto(stream_id, 1, BLOCK_SIZE)))
                    .await
                    .expect("commit error response")
                    .into_inner();

                assert!(response.header.expect("header").error.is_some());
                assert!(core.stream_manager().get(stream_id).await.is_none());
            });
        });

        assert_eq!(
            recorder.stream_values(),
            vec![("write".to_string(), 1.0), ("write".to_string(), -1.0)]
        );
    }

    #[tokio::test]
    async fn abort_write_success_decrements_inflight_once() {
        let recorder = StreamGaugeRecorder::default();

        metrics::with_local_recorder(&recorder, || {
            futures::executor::block_on(async {
                let (_temp, _store, core) = core_with_store(512, 2048, 4096);
                let service = registered_data_service(Arc::new(core));
                let open = service
                    .open_write_stream(tonic::Request::new(open_write_proto(2048)))
                    .await
                    .expect("open write")
                    .into_inner();
                let stream_id =
                    crate::data::convert::proto_to_stream_id(open.stream_id, "stream_id").expect("stream id");

                let response = service
                    .abort_write(tonic::Request::new(AbortWriteRequestProto {
                        header: Some(test_header()),
                        group_name: "root".to_string(),
                        block_id: Some(test_block_id_proto()),
                        stream_id: Some(crate::data::convert::stream_id_to_proto(stream_id)),
                        token: Some(test_token_proto()),
                    }))
                    .await
                    .expect("abort write")
                    .into_inner();

                assert!(response.header.expect("header").error.is_none());
                assert!(response.aborted);
            });
        });

        assert_eq!(
            recorder.stream_values(),
            vec![("write".to_string(), 1.0), ("write".to_string(), -1.0)]
        );
    }

    #[tokio::test]
    async fn commit_write_returns_success_after_full_write() {
        let (_temp, _store, core) = core_with_store(512, 2048, 4096);
        let core = Arc::new(core);
        let service = registered_data_service(Arc::clone(&core));
        let open = service
            .open_write_stream(tonic::Request::new(open_write_proto(2048)))
            .await
            .expect("open write")
            .into_inner();
        let stream_id = crate::data::convert::proto_to_stream_id(open.stream_id, "stream_id").expect("stream id");
        let data = payload();
        core.write_stream(WriteFrame {
            stream_id,
            seq: 1,
            offset_in_block: 0,
            data: data.slice(0..2048),
            checksum32: 0,
        })
        .await
        .expect("first frame");
        core.write_stream(WriteFrame {
            stream_id,
            seq: 2,
            offset_in_block: 2048,
            data: data.slice(2048..4096),
            checksum32: 0,
        })
        .await
        .expect("second frame");

        let response = service
            .commit_write(tonic::Request::new(commit_write_proto(stream_id, 2, BLOCK_SIZE)))
            .await
            .expect("commit write response")
            .into_inner();

        assert!(response.header.expect("header").error.is_none());
        assert_eq!(response.effective_len, BLOCK_SIZE);
        assert_eq!(response.block_stamp, BLOCK_STAMP);
        assert_eq!(response.written_through, BLOCK_SIZE);
    }

    #[tokio::test]
    async fn sync_committed_block_returns_success_for_ready_block() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        publish_ready_block(&store, payload(), BLOCK_STAMP);
        let service = registered_data_service(Arc::new(core));

        let response = service
            .sync_committed_block(tonic::Request::new(sync_committed_block_proto(BLOCK_STAMP, BLOCK_SIZE)))
            .await
            .expect("sync committed block response")
            .into_inner();

        assert!(response.header.expect("header").error.is_none());
        assert_eq!(response.effective_len, BLOCK_SIZE);
        assert_eq!(response.block_stamp, BLOCK_STAMP);
    }

    #[tokio::test]
    async fn open_read_stream_returns_success_response_for_ready_block() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        publish_ready_block(&store, payload(), BLOCK_STAMP);
        let service = registered_data_service(Arc::new(core));

        let response = service
            .open_read_stream(tonic::Request::new(open_read_proto(0, 1024, BLOCK_STAMP, 0)))
            .await
            .expect("open read response")
            .into_inner();

        assert!(response.header.expect("header").error.is_none());
        assert!(response.stream_id.is_some());
        assert_eq!(response.frame_size, 512);
        assert_eq!(response.window_bytes, 4096);
        assert_eq!(response.block_stamp, BLOCK_STAMP);
        assert_eq!(response.committed_length, BLOCK_SIZE);
    }

    #[tokio::test]
    async fn open_read_stream_returns_need_refresh_on_stale_stamp() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        publish_ready_block(&store, payload(), BLOCK_STAMP);
        let service = registered_data_service(Arc::new(core));

        let response = service
            .open_read_stream(tonic::Request::new(open_read_proto(0, 1024, BLOCK_STAMP + 1, 512)))
            .await
            .expect("open read response")
            .into_inner();
        let error = response
            .header
            .expect("header")
            .error
            .expect("stale stamp should return structured error");

        assert_eq!(error.error_class, ErrorClassProto::ErrorClassNeedRefresh as i32);
        assert_eq!(
            error.refresh_reason,
            RefreshReasonProto::RefreshReasonBlockStampMismatch as i32
        );
        assert!(response.stream_id.is_none());
    }

    #[tokio::test]
    async fn open_read_stream_returns_unavailable_location_on_missing_block() {
        let (_temp, _store, core) = core_with_store(512, 2048, 4096);
        let service = registered_data_service(Arc::new(core));

        let response = service
            .open_read_stream(tonic::Request::new(open_read_proto(0, 1024, BLOCK_STAMP, 512)))
            .await
            .expect("open read response")
            .into_inner();
        let error = response
            .header
            .expect("header")
            .error
            .expect("missing block should return structured error");

        assert_eq!(error.error_class, ErrorClassProto::ErrorClassNeedRefresh as i32);
        assert_eq!(
            error.refresh_reason,
            RefreshReasonProto::RefreshReasonBlockLocationUnavailable as i32
        );
        assert!(response.stream_id.is_none());
    }

    #[tokio::test]
    async fn open_read_stream_returns_header_error_on_zero_stamp() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        publish_ready_block(&store, payload(), BLOCK_STAMP);
        let service = registered_data_service(Arc::new(core));

        let response = service
            .open_read_stream(tonic::Request::new(open_read_proto(0, 1024, 0, 512)))
            .await
            .expect("open read response")
            .into_inner();
        let error = response
            .header
            .expect("header")
            .error
            .expect("zero stamp should return structured error");

        assert_eq!(error.error_class, ErrorClassProto::ErrorClassFatal as i32);
        assert!(error.message.contains("block_stamp"));
        assert!(response.stream_id.is_none());
    }

    #[tokio::test]
    async fn read_stream_returns_data_frames() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        let data = payload();
        publish_ready_block(&store, data.clone(), BLOCK_STAMP);
        let service = registered_data_service(Arc::new(core));

        let open = service
            .open_read_stream(tonic::Request::new(open_read_proto(4, 6, BLOCK_STAMP, 512)))
            .await
            .expect("open read response")
            .into_inner();
        let stream_id = open.stream_id.expect("stream id");
        let response_stream = service
            .read_stream(tonic::Request::new(ReadStreamRequestProto {
                stream_id: Some(stream_id),
                max_bytes: 0,
            }))
            .await
            .expect("read stream response")
            .into_inner();
        let frames = response_stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("stream frames");

        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].offset_in_block, 4);
        assert_eq!(frames[0].data, data.slice(4..10));
        assert!(frames[0].eos);
    }

    #[tokio::test]
    async fn read_stream_service_completion_decrements_inflight_once() {
        let recorder = StreamGaugeRecorder::default();

        metrics::with_local_recorder(&recorder, || {
            futures::executor::block_on(async {
                let (_temp, store, core) = core_with_store(512, 2048, 4096);
                publish_ready_block(&store, payload(), BLOCK_STAMP);
                let core = Arc::new(core);
                let service = registered_data_service(Arc::clone(&core));

                let open = service
                    .open_read_stream(tonic::Request::new(open_read_proto(
                        0,
                        BLOCK_SIZE as u32,
                        BLOCK_STAMP,
                        512,
                    )))
                    .await
                    .expect("open read")
                    .into_inner();
                let stream_id =
                    crate::data::convert::proto_to_stream_id(open.stream_id, "stream_id").expect("stream id");
                let response_stream = service
                    .read_stream(tonic::Request::new(ReadStreamRequestProto {
                        stream_id: Some(crate::data::convert::stream_id_to_proto(stream_id)),
                        max_bytes: 512,
                    }))
                    .await
                    .expect("read stream response")
                    .into_inner();
                let frames = response_stream
                    .collect::<Vec<_>>()
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()
                    .expect("read frames");

                assert!(frames.last().expect("last frame").eos);
                assert_eq!(core.stream_manager().active_count().await, 0);
            });
        });

        assert_eq!(
            recorder.stream_values(),
            vec![("read".to_string(), 1.0), ("read".to_string(), -1.0)]
        );
    }

    #[tokio::test]
    async fn read_stream_response_drop_decrements_inflight_once() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        publish_ready_block(&store, payload(), BLOCK_STAMP);
        let core = Arc::new(core);
        let service = registered_data_service(Arc::clone(&core));

        let open = service
            .open_read_stream(tonic::Request::new(open_read_proto(
                0,
                BLOCK_SIZE as u32,
                BLOCK_STAMP,
                512,
            )))
            .await
            .expect("open read")
            .into_inner();
        let stream_id = crate::data::convert::proto_to_stream_id(open.stream_id, "stream_id").expect("stream id");
        let response_stream = service
            .read_stream(tonic::Request::new(ReadStreamRequestProto {
                stream_id: Some(crate::data::convert::stream_id_to_proto(stream_id)),
                max_bytes: 512,
            }))
            .await
            .expect("read stream response")
            .into_inner();

        drop(response_stream);

        wait_for_active_stream_count(&core, 0).await;
    }

    #[tokio::test]
    async fn read_stream_early_drop_does_not_affect_later_read() {
        let (_temp, store, core) = core_with_store(512, 2048, 4096);
        let data = payload();
        publish_ready_block(&store, data.clone(), BLOCK_STAMP);
        let core = Arc::new(core);
        let service = registered_data_service(Arc::clone(&core));

        let open = service
            .open_read_stream(tonic::Request::new(open_read_proto(
                0,
                BLOCK_SIZE as u32,
                BLOCK_STAMP,
                512,
            )))
            .await
            .expect("open first read")
            .into_inner();
        let stream_id = open.stream_id.expect("stream id");
        let mut response_stream = service
            .read_stream(tonic::Request::new(ReadStreamRequestProto {
                stream_id: Some(stream_id),
                max_bytes: 512,
            }))
            .await
            .expect("read stream response")
            .into_inner();
        let first = response_stream
            .next()
            .await
            .expect("first frame")
            .expect("first frame ok");
        assert_eq!(first.data, data.slice(0..512));
        assert!(!first.eos);

        drop(response_stream);
        wait_for_active_stream_count(&core, 0).await;

        let second_open = service
            .open_read_stream(tonic::Request::new(open_read_proto(
                0,
                BLOCK_SIZE as u32,
                BLOCK_STAMP,
                512,
            )))
            .await
            .expect("open second read")
            .into_inner();
        let second_stream = service
            .read_stream(tonic::Request::new(ReadStreamRequestProto {
                stream_id: second_open.stream_id,
                max_bytes: 512,
            }))
            .await
            .expect("second read stream")
            .into_inner();
        let frames = second_stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("second read frames");
        let mut reread = Vec::new();
        for frame in &frames {
            reread.extend_from_slice(&frame.data);
        }

        assert_eq!(Bytes::from(reread), data);
        assert!(frames.last().expect("last frame").eos);
        assert_eq!(core.stream_manager().active_count().await, 0);
    }

    #[tokio::test]
    async fn read_stream_store_error_decrements_inflight_once() {
        let recorder = StreamGaugeRecorder::default();

        metrics::with_local_recorder(&recorder, || {
            futures::executor::block_on(async {
                let (_temp, store, core) = core_with_store(512, 2048, 4096);
                publish_ready_block(&store, payload(), BLOCK_STAMP);
                let paths = store.paths(&group_name(), block_id());
                let core = Arc::new(core);
                let service = registered_data_service(Arc::clone(&core));

                let open = service
                    .open_read_stream(tonic::Request::new(open_read_proto(
                        0,
                        BLOCK_SIZE as u32,
                        BLOCK_STAMP,
                        512,
                    )))
                    .await
                    .expect("open read")
                    .into_inner();
                let stream_id =
                    crate::data::convert::proto_to_stream_id(open.stream_id, "stream_id").expect("stream id");
                std::fs::remove_file(paths.data_path).expect("remove ready data file");

                let response_stream = service
                    .read_stream(tonic::Request::new(ReadStreamRequestProto {
                        stream_id: Some(crate::data::convert::stream_id_to_proto(stream_id)),
                        max_bytes: 512,
                    }))
                    .await
                    .expect("read stream response")
                    .into_inner();
                let result = response_stream
                    .collect::<Vec<_>>()
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>();

                assert!(result.is_err());
                assert_eq!(core.stream_manager().active_count().await, 0);
            });
        });

        assert_eq!(
            recorder.stream_values(),
            vec![("read".to_string(), 1.0), ("read".to_string(), -1.0)]
        );
    }

    #[tokio::test]
    async fn service_read_stream_rejects_missing_stream() {
        let (_temp, _store, core) = core_with_store(512, 2048, 4096);
        let service = registered_data_service(Arc::new(core));

        let read_status = match service
            .read_stream(tonic::Request::new(ReadStreamRequestProto {
                stream_id: Some(test_stream_id_proto()),
                max_bytes: 1024,
            }))
            .await
        {
            Ok(_) => panic!("ReadStream unexpectedly succeeded"),
            Err(status) => status,
        };
        assert_eq!(read_status.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn stream_manager_register_get_touch_remove_and_cleanup() {
        let manager = StreamManager::new(Duration::from_millis(50));
        let mut state = StreamState::new(stream_context());
        state.last_activity = Instant::now() - Duration::from_secs(10);

        manager.register(state.clone()).await;
        assert_eq!(manager.active_count().await, 1);
        assert_eq!(manager.get(stream_id()).await.unwrap().context.stream_id, stream_id());

        assert!(manager.touch(stream_id()).await);
        let touched = manager.get(stream_id()).await.unwrap();
        assert!(touched.last_activity > state.last_activity);

        manager.remove(stream_id()).await;
        assert_eq!(manager.active_count().await, 0);

        let mut idle = StreamState::new(stream_context());
        idle.last_activity = Instant::now() - Duration::from_secs(10);
        manager.register(idle).await;
        assert_eq!(manager.cleanup_idle_streams().await, 1);
        assert_eq!(manager.active_count().await, 0);
    }

    #[test]
    fn worker_lib_exports_only_current_data_plane_surface() {
        let lib = include_str!("lib.rs");

        for old_module in [
            "mod block_manager",
            "mod block_store",
            "mod convert",
            "pub mod core",
            "pub mod rpc_server",
            "pub mod service",
            "pub mod stream_manager",
            "pub mod admin",
            "pub mod combo_validator",
            "pub mod command_executor",
            "pub mod data_header",
            "pub mod delete_op_log",
            "pub mod eviction",
            "pub mod lifecycle",
            "pub mod metadata_client",
            "pub mod orphan",
            "pub mod pending_acks",
            "pub mod pipeline",
            "pub mod rebalance",
            "pub mod replication",
            "pub mod ufs_fill",
            "pub mod volume_health",
            "pub mod volume_manager",
            "#[path",
        ] {
            assert!(
                !lib.contains(old_module),
                "{old_module} must stay out of worker lib exports"
            );
        }

        for current_module in [
            "pub mod config",
            "pub mod data",
            "pub mod error",
            "pub mod net",
            "pub mod runtime",
            "pub mod store",
        ] {
            assert!(lib.contains(current_module), "lib.rs must declare {current_module}");
        }
    }

    #[test]
    fn worker_source_tree_matches_data_runtime_store_layout() {
        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");

        for required in [
            "data/mod.rs",
            "data/convert.rs",
            "data/core.rs",
            "net/mod.rs",
            "net/server/grpc.rs",
            "runtime/mod.rs",
            "runtime/stream.rs",
            "runtime/block.rs",
            "store/mod.rs",
            "store/block.rs",
        ] {
            assert!(src.join(required).exists(), "missing worker source file: {required}");
        }

        for removed in [
            "ufs_fill.rs",
            "replication.rs",
            "rebalance.rs",
            "eviction.rs",
            "orphan.rs",
            "command_executor.rs",
            "pipeline.rs",
            "delete_op_log.rs",
            "pending_acks.rs",
            "volume_health.rs",
            "volume_manager.rs",
            "metadata_client.rs",
            "lifecycle.rs",
            "combo_validator.rs",
            "admin.rs",
            "data_header.rs",
            "rpc_server.rs",
            "replication_tests.rs",
            "tests/delete_op_log_tests.rs",
        ] {
            assert!(
                !src.join(removed).exists(),
                "remove inactive worker source file: {removed}"
            );
        }
    }

    #[test]
    fn worker_net_active_surface_is_grpc_only_with_explicit_unsupported_protocol_rejection() {
        let net_mod = include_str!("net/mod.rs");
        let server_mod = include_str!("net/server/mod.rs");

        assert!(net_mod.contains("pub mod config;"));
        assert!(net_mod.contains("pub mod protocol;"));
        assert!(net_mod.contains("pub mod server;"));
        assert!(server_mod.contains("pub mod grpc;"));
        assert!(server_mod.contains("grpc::serve_grpc_worker_data_with_registration"));
        assert!(
            server_mod.contains("WorkerNetProtocol::Quic => bail!"),
            "QUIC listener values must remain explicitly rejected"
        );
        assert!(
            server_mod.contains("WorkerNetProtocol::Rdma => bail!"),
            "RDMA listener values must remain explicitly rejected"
        );
    }

    #[test]
    fn config_and_binary_do_not_initialize_inactive_paths() {
        let config = include_str!("config.rs");
        let main = include_str!("bin/main.rs");

        for forbidden in [
            "UfsConfig",
            "ReplicationConfig",
            "EvictionConfig",
            "OrphanConfig",
            "VolumeHealthConfig",
            "MetadataConfig",
            "combo",
            "fallback_transport",
            "block_report",
        ] {
            assert!(
                !config.contains(forbidden),
                "config.rs must not retain inactive setting: {forbidden}"
            );
        }

        for forbidden in [
            "RpcServer",
            "Ufs",
            "Replication",
            "MetadataClient",
            "Rebalance",
            "Eviction",
            "Orphan",
            "Lifecycle",
            "Volume",
        ] {
            assert!(
                !main.contains(forbidden),
                "worker main must not initialize inactive path: {forbidden}"
            );
        }

        let prepare_idx = main
            .find("prepare_worker_start(&config)")
            .expect("worker main must validate local storage before startup");
        let observability_idx = main
            .find("init_observability(&obs_config")
            .expect("worker main must initialize observability");
        assert!(
            prepare_idx < observability_idx,
            "worker main must validate local storage before binding observability or metrics"
        );
    }

    #[test]
    fn grpc_server_has_no_unregistered_production_constructor() {
        let grpc = include_str!("net/server/grpc.rs");
        let server_mod = include_str!("net/server/mod.rs");

        for forbidden in [
            concat!("registration_state: ", "Option"),
            concat!("registration_", "state: ", "No", "ne"),
            concat!("pub async fn serve_grpc_", "worker_data("),
        ] {
            assert!(
                !grpc.contains(forbidden),
                "gRPC server must not retain unregistered production path: {forbidden}"
            );
        }

        for forbidden in [
            concat!("pub async fn serve_", "worker_data("),
            concat!("registration_state: ", "Option"),
            concat!("None", " =>"),
        ] {
            assert!(
                !server_mod.contains(forbidden),
                "worker server must not retain unregistered production path: {forbidden}"
            );
        }
    }

    #[test]
    fn core_does_not_import_wire_types() {
        let core = include_str!("data/core.rs");

        for forbidden in ["proto::", "prost", "tonic"] {
            assert!(!core.contains(forbidden), "core.rs must not import {forbidden}");
        }
    }

    #[test]
    fn grpc_server_stays_adapter_only() {
        let service = include_str!("net/server/grpc.rs");

        for forbidden in [
            "ufs",
            "replication",
            "tier",
            "quorum",
            "BlockStore",
            "BlockManager",
            "StreamManager",
            "FileLayout",
        ] {
            assert!(
                !service.contains(forbidden),
                "net/server/grpc.rs must not depend on {forbidden}"
            );
        }
    }

    #[test]
    fn block_manager_stays_validation_only_and_store_stays_local_only() {
        let block_manager = include_str!("runtime/block.rs");
        let block_store = include_str!("store/block.rs");
        let meta_codec = include_str!("store/meta_codec.rs");

        for forbidden in [
            "ReplicationClient",
            "replicate",
            concat!("read_", "chunk"),
            "write_chunk",
            "delete_block",
        ] {
            assert!(
                !block_manager.contains(forbidden),
                "block_manager.rs must not retain {forbidden}"
            );
        }

        for forbidden in [
            "proto::",
            "prost",
            "tonic",
            "WorkerCore::",
            "WorkerDataService",
            "StreamManager",
            "TransportFrame",
            "ReadChunk",
            "WriteChunk",
            "ReadRange",
            concat!("read_", "chunk"),
            "write_chunk",
            "ufs",
            "replication",
            "quorum",
            ".chunk\"",
        ] {
            assert!(
                !block_store.contains(forbidden),
                "block_store.rs must stay local-format only and avoid {forbidden}"
            );
        }

        assert!(!meta_codec.contains("tonic"), "meta_codec.rs must not depend on tonic");
        for forbidden in [
            "WorkerCore::",
            "WorkerDataService",
            "StreamManager",
            "TransportFrame",
            "ReadChunk",
            "WriteChunk",
            "ReadRange",
            concat!("read_", "chunk"),
            "write_chunk",
            "ufs",
            "replication",
            "quorum",
            ".chunk\"",
        ] {
            assert!(
                !meta_codec.contains(forbidden),
                "meta_codec.rs must stay metadata-payload-only and avoid {forbidden}"
            );
        }
    }

    #[test]
    fn stream_state_keeps_runtime_fields_out_of_open_context() {
        let stream_manager = include_str!("runtime/stream.rs");
        let core = include_str!("data/core.rs");

        assert!(stream_manager.contains("pub context: StreamContext"));
        assert!(
            !core.contains("last_activity"),
            "StreamContext must not carry runtime activity"
        );

        for duplicate in [
            "pub chunk_size:",
            "pub flow_control_window:",
            "pub block_stamp:",
            "pub committed_length:",
        ] {
            assert!(
                !stream_manager.contains(duplicate),
                "StreamState must not duplicate open context field {duplicate}"
            );
        }
    }

    #[test]
    fn worker_data_proto_excludes_old_chunk_range_api() {
        let sources = [
            include_str!("../../proto/worker/data.proto"),
            include_str!("data/core.rs"),
            include_str!("net/server/grpc.rs"),
            include_str!("data/convert.rs"),
            include_str!("runtime/block.rs"),
            include_str!("store/block.rs"),
            include_str!("store/meta_codec.rs"),
            include_str!("lib.rs"),
        ];

        for old_name in [
            "ReadChunk",
            "WriteChunk",
            "ReadRange",
            "ReadChunkRequestProto",
            "WriteChunkRequestProto",
            "ReadRangeRequestProto",
            "ChunkDataProto",
            "ChunkSliceProto",
        ] {
            assert!(
                sources.iter().all(|source| !source.contains(old_name)),
                "{old_name} must stay out of the worker data-plane service"
            );
        }
    }

    #[test]
    fn worker_write_proto_fields_are_normalized() {
        let proto = include_str!("../../proto/worker/data.proto");

        assert_eq!(
            proto_message_fields(proto, "OpenWriteStreamRequestProto"),
            vec![
                ("worker.DataRequestHeaderProto", "header", 1),
                ("common.BlockIdProto", "block_id", 3),
                ("uint32", "block_format_id", 4),
                ("uint64", "block_size", 5),
                ("uint32", "chunk_size", 6),
                ("worker.ChecksumKindProto", "checksum_kind", 7),
                ("uint64", "block_stamp", 8),
                ("common.FencingTokenProto", "token", 9),
                ("uint32", "frame_size", 10),
                ("string", "worker_run_id", 11),
                ("uint64", "effective_len", 12),
                ("string", "group_name", 13),
                ("common.TierProto", "tier", 14),
            ]
        );
        assert_eq!(
            proto_message_fields(proto, "OpenReadStreamResponseProto"),
            vec![
                ("worker.DataResponseHeaderProto", "header", 1),
                ("common.StreamIdProto", "stream_id", 2),
                ("uint32", "frame_size", 3),
                ("uint32", "window_bytes", 4),
                ("uint64", "block_stamp", 5),
                ("uint64", "committed_length", 6),
            ]
        );
        assert_eq!(
            proto_message_fields(proto, "OpenWriteStreamResponseProto"),
            vec![
                ("worker.DataResponseHeaderProto", "header", 1),
                ("common.StreamIdProto", "stream_id", 2),
                ("uint32", "frame_size", 3),
                ("uint32", "window_bytes", 4),
                ("uint64", "block_stamp", 5),
                ("uint64", "committed_length", 6),
            ]
        );
        assert_eq!(
            proto_message_fields(proto, "CommitWriteRequestProto"),
            vec![
                ("worker.DataRequestHeaderProto", "header", 1),
                ("common.BlockIdProto", "block_id", 3),
                ("common.StreamIdProto", "stream_id", 4),
                ("uint64", "effective_len", 5),
                ("uint64", "block_stamp", 6),
                ("common.FencingTokenProto", "token", 7),
                ("uint64", "commit_seq", 8),
                ("bool", "require_sync", 9),
                ("string", "worker_run_id", 10),
                ("uint32", "block_format_id", 11),
                ("uint64", "block_size", 12),
                ("uint32", "chunk_size", 13),
                ("string", "group_name", 14),
            ]
        );
        assert_eq!(
            proto_message_fields(proto, "CommitWriteResponseProto"),
            vec![
                ("worker.DataResponseHeaderProto", "header", 1),
                ("uint64", "effective_len", 2),
                ("uint64", "block_stamp", 3),
                ("uint64", "written_through", 4),
            ]
        );
        assert_eq!(
            proto_message_fields(proto, "SyncCommittedBlockRequestProto"),
            vec![
                ("worker.DataRequestHeaderProto", "header", 1),
                ("common.BlockIdProto", "block_id", 3),
                ("uint64", "block_stamp", 4),
                ("uint64", "expected_block_len", 5),
                ("string", "worker_run_id", 6),
                ("uint32", "block_format_id", 7),
                ("uint64", "block_size", 8),
                ("uint32", "chunk_size", 9),
                ("string", "group_name", 10),
            ]
        );
        assert_eq!(
            proto_message_fields(proto, "SyncCommittedBlockResponseProto"),
            vec![
                ("worker.DataResponseHeaderProto", "header", 1),
                ("uint64", "effective_len", 2),
                ("uint64", "block_stamp", 3),
            ]
        );
        assert_eq!(
            proto_message_fields(proto, "AbortWriteRequestProto"),
            vec![
                ("worker.DataRequestHeaderProto", "header", 1),
                ("common.BlockIdProto", "block_id", 3),
                ("common.StreamIdProto", "stream_id", 4),
                ("common.FencingTokenProto", "token", 5),
                ("string", "group_name", 6),
            ]
        );
        assert_eq!(
            proto_message_fields(proto, "WriteStreamResponseProto"),
            vec![
                ("bool", "accepted", 1),
                ("uint64", "last_acked_seq", 2),
                ("uint64", "written_through", 3),
            ]
        );
    }

    #[test]
    fn active_write_path_uses_written_through_name() {
        let forbidden = concat!("persisted", "_through");
        let sources = [
            include_str!("../../proto/worker/data.proto"),
            include_str!("data/core.rs"),
            include_str!("net/server/grpc.rs"),
            include_str!("runtime/stream.rs"),
            include_str!("data/convert.rs"),
        ];

        assert!(
            sources.iter().all(|source| !source.contains(forbidden)),
            "{forbidden} must not remain in active write-path code"
        );
    }

    #[test]
    fn active_worker_sources_do_not_use_staged_version_labels() {
        let sources = [
            include_str!("data/core.rs"),
            include_str!("net/server/grpc.rs"),
            include_str!("data/convert.rs"),
            include_str!("runtime/stream.rs"),
            include_str!("runtime/block.rs"),
            include_str!("store/block.rs"),
            include_str!("lib.rs"),
        ];

        for forbidden in [concat!("Pha", "se"), concat!("v", "1"), concat!("v", "2")] {
            assert!(
                sources.iter().all(|source| !source.contains(forbidden)),
                "{forbidden} must stay out of active worker source text"
            );
        }
    }

    #[derive(Default)]
    struct StreamGaugeRecorder {
        stream_values: Arc<Mutex<Vec<(String, f64)>>>,
    }

    impl StreamGaugeRecorder {
        fn stream_values(&self) -> Vec<(String, f64)> {
            self.stream_values.lock().expect("stream gauge values poisoned").clone()
        }
    }

    impl Recorder for StreamGaugeRecorder {
        fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn register_counter(&self, _key: &Key, _metadata: &Metadata<'_>) -> Counter {
            Counter::noop()
        }

        fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
            if key.name() != WORKER_STREAM_INFLIGHT {
                return Gauge::noop();
            }
            let mode = key
                .labels()
                .find(|label| label.key() == "mode")
                .map(|label| label.value().to_string())
                .unwrap_or_default();
            Gauge::from_arc(Arc::new(StreamGauge {
                mode,
                values: Arc::clone(&self.stream_values),
            }))
        }

        fn register_histogram(&self, _key: &Key, _metadata: &Metadata<'_>) -> Histogram {
            Histogram::noop()
        }
    }

    struct StreamGauge {
        mode: String,
        values: Arc<Mutex<Vec<(String, f64)>>>,
    }

    impl GaugeFn for StreamGauge {
        fn increment(&self, value: f64) {
            self.values
                .lock()
                .expect("stream gauge values poisoned")
                .push((self.mode.clone(), value));
        }

        fn decrement(&self, value: f64) {
            self.values
                .lock()
                .expect("stream gauge values poisoned")
                .push((self.mode.clone(), -value));
        }

        fn set(&self, value: f64) {
            self.values
                .lock()
                .expect("stream gauge values poisoned")
                .push((self.mode.clone(), value));
        }
    }

    fn proto_message_fields<'a>(source: &'a str, message: &str) -> Vec<(&'a str, &'a str, u32)> {
        let start = format!("message {message} {{");
        let mut in_message = false;
        let mut fields = Vec::new();
        for raw_line in source.lines() {
            let line = raw_line.trim();
            if line == start {
                in_message = true;
                continue;
            }
            if in_message && line == "}" {
                break;
            }
            if !in_message
                || line.starts_with("//")
                || line.starts_with("reserved")
                || line.is_empty()
                || !line.ends_with(';')
            {
                continue;
            }

            let field = line.trim_end_matches(';');
            let (left, tag) = field.split_once(" = ").expect("proto field must have tag");
            let mut left_parts = left.split_whitespace();
            let ty = left_parts.next().expect("proto field type");
            let name = left_parts.next().expect("proto field name");
            assert!(left_parts.next().is_none(), "unexpected proto field modifier: {line}");
            fields.push((ty, name, tag.parse().expect("numeric proto tag")));
        }
        assert!(in_message, "missing proto message {message}");
        fields
    }
}
