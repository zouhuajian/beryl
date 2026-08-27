// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use std::collections::{BTreeMap, VecDeque};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use beryl_common::error::rpc::{ErrorKind, RpcErrorDetail, WorkerErrorKind};
use beryl_proto::common::ResponseHeaderProto;
use beryl_proto::convert::rpc_error_to_proto;
use beryl_proto::metadata::metadata_worker_service_proto_server::{
    MetadataWorkerServiceProto, MetadataWorkerServiceProtoServer,
};
use beryl_proto::metadata::{
    block_report_request_proto, BlockCleanupCommandProto, BlockReportBlockStateProto, BlockReportDeltaOpProto,
    BlockReportRequestProto, BlockReportResponseProto, HeartbeatRequestProto, HeartbeatResponseProto,
    RegisterWorkerRequestProto, RegisterWorkerResponseProto,
};
use beryl_proto::worker::worker_data_service_server::WorkerDataService;
use beryl_proto::worker::ReadBlockRequestProto;
use beryl_types::chunk::ByteRange;
use beryl_types::ids::{BlockId, BlockIndex, InodeId, WorkerId};
use beryl_types::layout::BlockFormatId;
use beryl_types::{GroupName, Tier, WorkerRunId};
use bytes::Bytes;
use futures::StreamExt;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, Command};
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use beryl_worker::config::{StoreDirConfig, WorkerConfig, WorkerRegistrationConfig};
use beryl_worker::control::{
    BlockCleanupOptions, BlockCleanupRuntime, HeartbeatSnapshot, MetadataBlockReportLoop, MetadataHeartbeatLoop,
    Registration, RegistrationDescriptor, RegistrationSet,
};
use beryl_worker::net::protocol::WorkerNetProtocol;
use beryl_worker::net::server::grpc::WorkerDataServiceImpl;
use beryl_worker::store::block::{
    ChecksumKind, CreateStagingBlockRequest, FullBlockFileStore, FullBlockFileStoreConfig, LocalBlockStore,
    PublishReadyRequest, ReclaimBlockRequest,
};
use beryl_worker::store::dirs::StoreDirs;
use beryl_worker::WorkerCore;

const BLOCK_SIZE: u64 = 4096;

fn chunk_size() -> u32 {
    BlockFormatId::FULL_EFFECTIVE.spec().unwrap().storage_chunk_size
}

fn block_id() -> BlockId {
    BlockId::new(InodeId::new(7), BlockIndex::new(3))
}

fn group_name() -> GroupName {
    GroupName::parse("root").expect("test group name is valid")
}

#[derive(Clone)]
enum MockRegisterReply {
    Echo,
}

#[derive(Clone)]
enum MockHeartbeatReply {
    Ok {
        worker_id: u64,
        worker_run_id: WorkerRunId,
    },
    OkWithCleanup {
        worker_id: u64,
        worker_run_id: WorkerRunId,
        cleanup_commands: Vec<BlockCleanupCommandProto>,
    },
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
            MockRegisterReply::Echo => Ok(Response::new(RegisterWorkerResponseProto {
                header: Some(response_header_from_request(&request, None)),
                worker_id: request.worker_id,
                accepted_worker_run_id: request.worker_run_id,
            })),
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
                cleanup_commands: Vec::new(),
            })),
            MockHeartbeatReply::OkWithCleanup {
                worker_id,
                worker_run_id,
                cleanup_commands,
            } => Ok(Response::new(HeartbeatResponseProto {
                header: Some(response_header_from_heartbeat_request(&request, None)),
                worker_id,
                accepted_worker_run_id: worker_run_id.to_string(),
                liveness_timeout_ms: 5_000,
                cleanup_commands,
            })),
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
        request_timeout_ms: 1_000,
        retry_initial_backoff_ms: 1,
        retry_max_backoff_ms: 1,
    }
}

fn test_worker_run_id() -> WorkerRunId {
    "550e8400-e29b-41d4-a716-446655440000".parse().unwrap()
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

fn test_worker_core(store: Arc<StoreDirs>) -> Arc<WorkerCore> {
    Arc::new(WorkerCore::with_local_store(1024, 1024, store))
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
            chunk_size: chunk_size(),
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

async fn wait_for_block_report_requests(mock: &MockMetadataState, expected: usize, timeout: Duration) {
    tokio::time::timeout(timeout, async {
        loop {
            if mock.block_report_requests.lock().unwrap().len() >= expected {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {expected} block report requests"));
}

async fn wait_for_block_report_delta(
    mock: &MockMetadataState,
    expected_op: BlockReportDeltaOpProto,
    expected_block_id: BlockId,
    timeout: Duration,
) {
    tokio::time::timeout(timeout, async {
        loop {
            let found = mock.block_report_requests.lock().unwrap().iter().any(|request| {
                let Some(block_report_request_proto::Report::Delta(delta)) = request.report.as_ref() else {
                    return false;
                };
                delta.deltas.iter().any(|entry| {
                    entry.op() == expected_op
                        && entry
                            .block
                            .as_ref()
                            .and_then(|block| block.block_id)
                            .is_some_and(|block_id| BlockId::try_from(block_id) == Ok(expected_block_id))
                })
            });
            if found {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {expected_op:?} delta for {expected_block_id}"));
}

#[cfg(unix)]
struct WorkerProcess {
    child: Child,
}

#[cfg(unix)]
impl WorkerProcess {
    fn start(config_path: &std::path::Path) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_beryl-worker"))
            .arg("start")
            .arg("--config")
            .arg(config_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("start Worker process");
        Self { child }
    }

    fn send_signal(&self, signal: i32) {
        let pid = self.child.id().expect("Worker process id");
        assert_eq!(unsafe { libc::kill(pid as i32, signal) }, 0);
    }

    async fn wait_successfully(mut self) {
        let status = tokio::time::timeout(Duration::from_secs(5), self.child.wait())
            .await
            .expect("Worker process shutdown must be bounded")
            .expect("wait for Worker process");
        assert!(status.success(), "Worker process must exit successfully: {status}");
    }

    async fn signal_and_wait(self, signal: i32) {
        self.send_signal(signal);
        self.wait_successfully().await;
    }
}

#[cfg(unix)]
async fn wait_for_worker_http_status(address: std::net::SocketAddr, path: &str, status: &[u8]) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(mut stream) = tokio::net::TcpStream::connect(address).await {
                let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
                stream.write_all(request.as_bytes()).await.ok();
                let mut response = Vec::new();
                stream.read_to_end(&mut response).await.ok();
                if response.starts_with(status) {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Worker HTTP status must become available");
}

#[cfg(unix)]
async fn request_http_keep_alive(stream: &mut tokio::net::TcpStream, path: &str) -> Vec<u8> {
    let request = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n");
    stream.write_all(request.as_bytes()).await.expect("write HTTP request");
    let mut response = Vec::new();
    loop {
        let mut chunk = [0u8; 1024];
        let count = stream.read(&mut chunk).await.expect("read HTTP response");
        assert_ne!(count, 0, "HTTP connection closed before a complete response");
        response.extend_from_slice(&chunk[..count]);
        let Some(header_end) = response.windows(4).position(|window| window == b"\r\n\r\n") else {
            continue;
        };
        let body_start = header_end + 4;
        let headers = std::str::from_utf8(&response[..header_end]).expect("HTTP response headers are UTF-8");
        let content_length = headers
            .lines()
            .find_map(|line| {
                line.split_once(':').and_then(|(name, value)| {
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().expect("valid content length"))
                })
            })
            .expect("HTTP response has content length");
        if response.len() >= body_start + content_length {
            response.truncate(body_start + content_length);
            return response;
        }
    }
}

#[cfg(unix)]
fn worker_process_config(endpoint: &str) -> (TempDir, std::path::PathBuf, std::net::SocketAddr, WorkerConfig) {
    let temp = TempDir::new().expect("worker process tempdir");
    let rpc_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve Worker RPC port");
    let rpc_addr = rpc_listener.local_addr().unwrap();
    let http_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve Worker HTTP port");
    let http_addr = http_listener.local_addr().unwrap();
    let identity_path = temp.path().join("worker.identity");
    let store_path = temp.path().join("hdd0");
    let config_path = temp.path().join("worker.yaml");
    let config_yaml = format!(
        r#"beryl.cluster.id: "process-shutdown"
beryl.worker.identity-file: {identity_path:?}
beryl.worker.host: "127.0.0.1"
beryl.worker.bind-host: "127.0.0.1"
beryl.worker.rpc.port: {rpc_port}
beryl.worker.rpc.max-concurrent-read-requests: 8
beryl.worker.rpc.max-concurrent-write-requests: 8
beryl.worker.http.port: {http_port}
beryl.worker.metadata.addresses: [{endpoint:?}]
beryl.worker.metadata.request-timeout: 1s
beryl.worker.metadata.retry.initial-backoff: 10ms
beryl.worker.metadata.retry.max-backoff: 100ms
beryl.worker.heartbeat.interval: 20ms
beryl.worker.block.report.interval: 20ms
beryl.worker.block.report.batch-size: 100
beryl.worker.block.cleanup.queue-capacity: 16
beryl.worker.block.cleanup.concurrency: 2
beryl.worker.block.cleanup.retry.initial-backoff: 10ms
beryl.worker.block.cleanup.retry.max-backoff: 100ms
beryl.worker.stream.frame-size: 1KiB
beryl.worker.stream.max-frame-size: 4KiB
beryl.worker.storage.dirs:
  hdd0:
    path: {store_path:?}
    tier: hdd
    capacity: 64MiB
beryl.worker.storage.reserved-space: 1MiB
beryl.worker.storage.check-interval: 1s
beryl.worker.shutdown.timeout: 200ms
beryl.logging.format: compact
beryl.logging.output: stderr
beryl.logging.level: warn
"#,
        identity_path = identity_path.to_string_lossy(),
        store_path = store_path.to_string_lossy(),
        rpc_port = rpc_addr.port(),
        http_port = http_addr.port(),
    );
    std::fs::write(&config_path, config_yaml).expect("write Worker process config");
    drop(rpc_listener);
    drop(http_listener);
    let config = WorkerConfig::load(&config_path).expect("load Worker process config");
    (temp, config_path, http_addr, config)
}

#[cfg(unix)]
async fn wait_for_full_report_count(mock: &MockMetadataState, block_id: BlockId, expected: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let count = mock
                .block_report_requests
                .lock()
                .unwrap()
                .iter()
                .filter(|request| {
                    let Some(block_report_request_proto::Report::Full(full)) = request.report.as_ref() else {
                        return false;
                    };
                    full.blocks.iter().any(|block| {
                        block
                            .block_id
                            .is_some_and(|reported| BlockId::try_from(reported) == Ok(block_id))
                    })
                })
                .count();
            if count >= expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Worker must report recovered Ready block");
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_signals_exit_cleanly_and_restart_reports_current_blocks() {
    let (endpoint, mock, metadata_shutdown) =
        start_mock_metadata(vec![MockRegisterReply::Echo, MockRegisterReply::Echo]).await;
    let (_temp, config_path, http_addr, config) = worker_process_config(&endpoint);
    beryl_worker::control::prepare_worker_start(&config).expect("format Worker storage");
    let store = StoreDirs::open(
        config.store.dirs.clone(),
        config.store.reserve_space_bytes,
        config.store.check_interval_ms,
    )
    .expect("open Worker process store");
    let ready_block = block_id();
    publish_ready_block_for(&store, group_name(), ready_block, payload(), 101);
    drop(store);

    let first = WorkerProcess::start(&config_path);
    wait_for_worker_http_status(http_addr, "/ready", b"HTTP/1.1 200").await;
    wait_for_full_report_count(&mock, ready_block, 1).await;
    let mut held_connection = tokio::net::TcpStream::connect(http_addr)
        .await
        .expect("open accepted HTTP connection");
    held_connection
        .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n")
        .await
        .unwrap();
    let mut readiness_connection = tokio::net::TcpStream::connect(http_addr)
        .await
        .expect("open readiness connection before shutdown");
    let ready_response = request_http_keep_alive(&mut readiness_connection, "/ready").await;
    assert!(ready_response.starts_with(b"HTTP/1.1 200"));
    first.send_signal(libc::SIGTERM);
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let response = request_http_keep_alive(&mut readiness_connection, "/ready").await;
            if response.starts_with(b"HTTP/1.1 503") {
                return;
            }
            assert!(response.starts_with(b"HTTP/1.1 200"));
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("readiness must close before accepted HTTP work drains");
    first.wait_successfully().await;
    drop(held_connection);
    drop(readiness_connection);

    let second = WorkerProcess::start(&config_path);
    wait_for_worker_http_status(http_addr, "/ready", b"HTTP/1.1 200").await;
    wait_for_full_report_count(&mock, ready_block, 2).await;
    second.signal_and_wait(libc::SIGINT).await;

    let registrations = mock.requests.lock().unwrap();
    assert_eq!(registrations.len(), 2);
    assert_ne!(registrations[0].worker_run_id, registrations[1].worker_run_id);
    metadata_shutdown.send(()).ok();
}

#[tokio::test]
async fn heartbeat_cleanup_command_reports_deleting_then_delta_remove() {
    let worker_run_id = test_worker_run_id();
    let (endpoint, mock, shutdown) = start_mock_metadata(Vec::new()).await;
    *mock.heartbeat_replies.lock().unwrap() = VecDeque::from([MockHeartbeatReply::OkWithCleanup {
        worker_id: 42,
        worker_run_id,
        cleanup_commands: vec![
            BlockCleanupCommandProto {
                block_id: Some(block_id().into()),
                expected_block_stamp: 101,
            },
            BlockCleanupCommandProto {
                block_id: Some(block_id().into()),
                expected_block_stamp: 101,
            },
        ],
    }]);
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
    let core = test_worker_core(Arc::clone(&store));
    let cleanup_runtime = BlockCleanupRuntime::start(
        Arc::clone(&core),
        Arc::clone(&state),
        BlockCleanupOptions {
            max_pending: 4,
            max_concurrent: 1,
            retry_initial_backoff: Duration::from_millis(10),
            retry_max_backoff: Duration::from_millis(10),
        },
    )
    .expect("cleanup executor");
    let cleanup = cleanup_runtime.executor();
    let heartbeat = MetadataHeartbeatLoop::new(
        test_registration_config(endpoint.clone()),
        test_registration_descriptor(worker_run_id),
        Arc::clone(&state),
        cleanup,
    )
    .expect("heartbeat loop");
    let reporter = MetadataBlockReportLoop::new(
        test_registration_config(endpoint),
        test_registration_descriptor(worker_run_id),
        Arc::clone(&state),
        Arc::clone(&store),
        Arc::clone(&core),
    )
    .expect("block reporter");

    let service = WorkerDataServiceImpl::new(Arc::clone(&core), Arc::clone(&state), 64, 32);
    let read = service
        .read_block(Request::new(ReadBlockRequestProto {
            header: None,
            group_name: group_name().to_string(),
            block_id: Some(block_id().into()),
            worker_run_id: worker_run_id.to_string(),
            byte_range: Some(ByteRange { offset: 0, len: 1 }.into()),
            block_stamp: 101,
            block_format_id: BlockFormatId::FULL_EFFECTIVE.as_raw(),
            block_size: BLOCK_SIZE,
            chunk_size: chunk_size(),
            effective_len: BLOCK_SIZE,
            frame_size: 1024,
        }))
        .await
        .expect("start pinned read")
        .into_inner();
    reporter.send_full_once().await.expect("publish Ready baseline");
    heartbeat
        .send_once(HeartbeatSnapshot::default())
        .await
        .expect("accept cleanup heartbeat");

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let round = reporter.send_delta_once().await.expect("send Deleting delta");
            if round.accepted_peers > 0
                && latest_delta_has(
                    &mock,
                    BlockReportDeltaOpProto::BlockReportDeltaOpAddUpdate,
                    BlockReportBlockStateProto::BlockReportBlockStateDeleting,
                )
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cleanup must become Deleting while the read pin is active");

    let chunks = read
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("finish pinned read");
    assert_eq!(chunks.len(), 1);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if store.report().expect("store report").dirs[0].block_count == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cleanup must delete the local block after the reader exits");

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let round = reporter.send_delta_once().await.expect("send REMOVE delta");
            if round.accepted_peers > 0
                && latest_delta_has(
                    &mock,
                    BlockReportDeltaOpProto::BlockReportDeltaOpRemove,
                    BlockReportBlockStateProto::BlockReportBlockStateDeleting,
                )
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cleanup must publish REMOVE after physical deletion completes");
    shutdown.send(()).ok();
}

fn latest_delta_has(
    mock: &MockMetadataState,
    expected_op: BlockReportDeltaOpProto,
    expected_state: BlockReportBlockStateProto,
) -> bool {
    let requests = mock.block_report_requests.lock().unwrap();
    let Some(request) = requests.last() else {
        return false;
    };
    let Some(block_report_request_proto::Report::Delta(delta)) = request.report.as_ref() else {
        return false;
    };
    delta.deltas.iter().any(|entry| {
        entry.op() == expected_op
            && entry
                .block
                .as_ref()
                .is_some_and(|block| block.block_state() == expected_state)
    })
}

#[tokio::test]
async fn block_report_loop_sends_coalesced_ready_and_remove_deltas_on_store_changes() {
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
    let first = BlockId::new(InodeId::new(7), BlockIndex::new(0));
    let second = BlockId::new(InodeId::new(7), BlockIndex::new(1));
    let core = test_worker_core(Arc::clone(&store));
    let reporter = MetadataBlockReportLoop::new(
        test_registration_config(endpoint),
        test_registration_descriptor(worker_run_id),
        Arc::clone(&state),
        Arc::clone(&store),
        Arc::clone(&core),
    )
    .expect("block reporter");
    let reporter_handle = reporter.spawn();
    wait_for_block_report_requests(&mock, 1, Duration::from_millis(500)).await;

    publish_ready_block_for(store.as_ref(), group_name(), first, payload(), 101);
    publish_ready_block_for(store.as_ref(), group_name(), second, payload(), 102);
    wait_for_block_report_requests(&mock, 2, Duration::from_millis(500)).await;

    {
        let requests = mock.block_report_requests.lock().unwrap();
        assert!(matches!(
            requests[0].report.as_ref(),
            Some(block_report_request_proto::Report::Full(_))
        ));
        let Some(block_report_request_proto::Report::Delta(delta)) = requests[1].report.as_ref() else {
            panic!("expected event-driven delta report");
        };
        assert_eq!(delta.deltas.len(), 2);
        assert!(delta
            .deltas
            .iter()
            .all(|entry| entry.op() == BlockReportDeltaOpProto::BlockReportDeltaOpAddUpdate));
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        mock.block_report_requests.lock().unwrap().len(),
        2,
        "rapid Ready changes should be coalesced before the periodic tick"
    );

    assert_eq!(
        core.reclaim_block(ReclaimBlockRequest {
            group_name: group_name(),
            block_id: first,
            expected_block_stamp: 101,
        })
        .await
        .expect("reclaim Ready block"),
        beryl_worker::ReclaimBlockResult::Deleted {
            effective_len: BLOCK_SIZE
        }
    );
    wait_for_block_report_delta(
        &mock,
        BlockReportDeltaOpProto::BlockReportDeltaOpRemove,
        first,
        Duration::from_millis(500),
    )
    .await;
    {
        let requests = mock.block_report_requests.lock().unwrap();
        assert!(requests.iter().any(|request| {
            let Some(block_report_request_proto::Report::Delta(delta)) = request.report.as_ref() else {
                return false;
            };
            delta.deltas.iter().any(|entry| {
                entry.op() == BlockReportDeltaOpProto::BlockReportDeltaOpRemove
                    && entry
                        .block
                        .as_ref()
                        .and_then(|block| block.block_id)
                        .is_some_and(|block_id| BlockId::try_from(block_id) == Ok(first))
            })
        }));
    }

    reporter_handle.abort();
    shutdown.send(()).ok();
}

#[tokio::test]
async fn event_driven_delta_failure_is_retried_by_periodic_reporting() {
    let worker_run_id = test_worker_run_id();
    let (endpoint, mock, shutdown) = start_mock_metadata_with_block_reports(vec![
        MockBlockReportReply::Ok,
        MockBlockReportReply::Status(Status::unavailable("delta unavailable")),
        MockBlockReportReply::Ok,
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
    let reporter = MetadataBlockReportLoop::new(
        test_registration_config(endpoint),
        test_registration_descriptor(worker_run_id),
        Arc::clone(&state),
        Arc::clone(&store),
        test_worker_core(Arc::clone(&store)),
    )
    .expect("block reporter");
    let reporter_handle = reporter.spawn();
    wait_for_block_report_requests(&mock, 1, Duration::from_millis(500)).await;

    publish_ready_block_for(store.as_ref(), group_name(), block_id(), payload(), 101);
    wait_for_block_report_requests(&mock, 2, Duration::from_millis(500)).await;
    wait_for_block_report_requests(&mock, 3, Duration::from_millis(1_500)).await;

    {
        let requests = mock.block_report_requests.lock().unwrap();
        assert!(matches!(
            requests[1].report.as_ref(),
            Some(block_report_request_proto::Report::Delta(_))
        ));
        assert!(matches!(
            requests[2].report.as_ref(),
            Some(block_report_request_proto::Report::Delta(_))
        ));
    }

    reporter_handle.abort();
    shutdown.send(()).ok();
}

#[tokio::test]
async fn re_registration_rebuilds_full_report_after_block_report_registration_errors() {
    for error in [
        RpcErrorDetail::register_worker(ErrorKind::Worker(WorkerErrorKind::NotRegistered), "register worker"),
        RpcErrorDetail::register_worker(ErrorKind::Worker(WorkerErrorKind::RunMismatch), "worker run mismatch"),
    ] {
        let worker_run_id = test_worker_run_id();
        let (endpoint, mock, shutdown) = start_mock_metadata_with_block_reports(vec![
            MockBlockReportReply::Ok,
            MockBlockReportReply::HeaderError(error),
            MockBlockReportReply::Ok,
        ])
        .await;
        let state = Arc::new(RegistrationSet::new());
        let registration = Registration {
            group_name: group_name(),
            worker_id: WorkerId::new(42),
            worker_run_id,
            advertised_endpoint: "http://127.0.0.1:9090".to_string(),
        };
        state.record_registered(registration.clone());
        state.record_heartbeat_success(&group_name(), Duration::from_secs(60));
        let temp = TempDir::new().expect("tempdir");
        let store = report_store(&temp);
        let reporter = MetadataBlockReportLoop::new(
            test_registration_config(endpoint),
            test_registration_descriptor(worker_run_id),
            Arc::clone(&state),
            Arc::clone(&store),
            test_worker_core(Arc::clone(&store)),
        )
        .expect("block reporter");
        let reporter_handle = reporter.spawn();
        wait_for_block_report_requests(&mock, 1, Duration::from_millis(500)).await;

        publish_ready_block_for(store.as_ref(), group_name(), block_id(), payload(), 101);
        wait_for_block_report_requests(&mock, 2, Duration::from_millis(500)).await;
        tokio::time::timeout(Duration::from_millis(500), async {
            while state.registration(&group_name()).is_some() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("block report registration error should clear the registration");

        state.record_registered(registration);
        state.record_heartbeat_success(&group_name(), Duration::from_secs(60));
        wait_for_block_report_requests(&mock, 3, Duration::from_millis(1_500)).await;

        {
            let requests = mock.block_report_requests.lock().unwrap();
            assert!(matches!(
                requests[0].report.as_ref(),
                Some(block_report_request_proto::Report::Full(_))
            ));
            assert!(matches!(
                requests[1].report.as_ref(),
                Some(block_report_request_proto::Report::Delta(_))
            ));
            assert!(matches!(
                requests[2].report.as_ref(),
                Some(block_report_request_proto::Report::Full(_))
            ));
        }

        reporter_handle.abort();
        shutdown.send(()).ok();
    }
}

#[tokio::test]
async fn startup_marker_recovery_precedes_first_full_block_report() {
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
    let data_root = temp.path().join("hdd0");
    let raw_store = FullBlockFileStore::new(FullBlockFileStoreConfig::new(data_root));
    publish_ready_block_for(&raw_store, group_name(), block_id(), payload(), 101);
    let paths = raw_store.paths(&group_name(), block_id());
    std::fs::create_dir_all(paths.deleting_marker_path.parent().expect("marker parent")).expect("create marker parent");
    std::fs::write(
        &paths.deleting_marker_path,
        serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "group_name": group_name().as_str(),
            "block_id": block_id(),
            "block_stamp": 101,
            "effective_len": BLOCK_SIZE,
        }))
        .expect("encode deleting marker"),
    )
    .expect("write deleting marker");
    drop(raw_store);

    let store = report_store(&temp);
    assert_eq!(store.report().expect("store report").dirs[0].block_count, 0);
    assert!(!paths.data_path.exists());
    assert!(!paths.meta_path.exists());
    assert!(!paths.deleting_marker_path.exists());

    let reporter = MetadataBlockReportLoop::new(
        test_registration_config(endpoint),
        test_registration_descriptor(worker_run_id),
        Arc::clone(&state),
        Arc::clone(&store),
        test_worker_core(Arc::clone(&store)),
    )
    .expect("block reporter");
    let round = reporter.send_full_once().await.expect("first full report");

    assert_eq!(round.accepted_peers, 1);
    let requests = mock.block_report_requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let block_report_request_proto::Report::Full(full) = requests[0].report.as_ref().expect("full report") else {
        panic!("expected full block report");
    };
    assert!(full.blocks.is_empty(), "recovered block must not reappear as Ready");
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
        test_worker_core(Arc::clone(&store)),
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
