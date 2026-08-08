// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

#![cfg(unix)]

use beryl_client::CreateOptions;
use beryl_e2e::{data::deterministic_bytes, TestCluster};
use bytes::Bytes;
use std::ffi::CString;
use std::process::Stdio;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::{HealthCheckRequest, HealthCheckResponse};

async fn hold_metadata_health_watch(endpoint: String) -> tonic::Streaming<HealthCheckResponse> {
    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .expect("valid Metadata health endpoint")
        .connect()
        .await
        .expect("connect to Metadata health service");
    let mut client = HealthClient::new(channel);
    let mut stream = client
        .watch(HealthCheckRequest { service: String::new() })
        .await
        .expect("start Metadata health watch")
        .into_inner();
    stream
        .message()
        .await
        .expect("read initial Metadata health status")
        .expect("Metadata health watch must emit initial status");
    stream
}

async fn hold_metadata_http_connection(address: std::net::SocketAddr) -> tokio::net::TcpStream {
    let mut stream = tokio::net::TcpStream::connect(address)
        .await
        .expect("connect to Metadata HTTP service");
    stream
        .write_all(b"GET /ready HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .expect("request Metadata readiness");
    let mut response = Vec::new();
    loop {
        let mut chunk = [0u8; 1024];
        let count = stream.read(&mut chunk).await.expect("read Metadata readiness");
        assert_ne!(count, 0, "Metadata closed HTTP connection before responding");
        response.extend_from_slice(&chunk[..count]);
        if response.ends_with(b"ready") {
            break;
        }
    }
    assert!(response.starts_with(b"HTTP/1.1 200"));
    stream
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_signals_before_config_load_exit_cleanly() {
    let executable = std::path::Path::new(env!("CARGO_BIN_EXE_metadata-e2e-server"));
    for signal in [libc::SIGTERM, libc::SIGINT] {
        let temp = tempfile::TempDir::new().expect("startup signal tempdir");
        let config_path = temp.path().join("startup.yaml");
        let c_path = CString::new(config_path.as_os_str().as_encoded_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) }, 0);
        let mut child = Command::new(executable)
            .arg(&config_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("start Metadata before config load");
        let mut config_writer = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            tokio::fs::OpenOptions::new().write(true).open(&config_path),
        )
        .await
        .expect("Metadata must open the config FIFO")
        .expect("open config FIFO writer");

        let pid = child.id().expect("Metadata startup process id");
        assert_eq!(unsafe { libc::kill(pid as i32, signal) }, 0);
        let storage_dir = temp.path().join("metadata");
        let config = format!(
            "beryl.cluster.id: startup-signal\n\
             beryl.metadata.host: 127.0.0.1\n\
             beryl.metadata.bind-host: 127.0.0.1\n\
             beryl.metadata.rpc.port: 19080\n\
             beryl.metadata.http.port: 19081\n\
             beryl.metadata.storage.dir: {:?}\n\
             beryl.logging.format: compact\n\
             beryl.logging.output: stderr\n\
             beryl.logging.level: warn\n",
            storage_dir
        );
        config_writer
            .write_all(config.as_bytes())
            .await
            .expect("complete Metadata startup config");
        drop(config_writer);

        let status = tokio::time::timeout(std::time::Duration::from_secs(5), child.wait())
            .await
            .expect("Metadata startup signal shutdown must be bounded")
            .expect("wait for Metadata startup process");
        assert!(
            status.success(),
            "Metadata startup signal must exit successfully: {status}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_signals_exit_cleanly_and_preserve_visible_data() {
    let mut cluster = TestCluster::start().await.expect("start cluster");
    let executable = std::path::Path::new(env!("CARGO_BIN_EXE_metadata-e2e-server"));
    cluster
        .start_metadata_process(executable)
        .await
        .expect("start full Metadata process");
    let client = cluster.client().clone();
    let payload = Bytes::from(deterministic_bytes(1_537));
    client
        .mkdirs("/process-shutdown", true)
        .await
        .expect("create directory");
    let mut writer = client
        .create(
            "/process-shutdown/visible",
            CreateOptions::create().with_block_size(1_024).with_chunk_size(1_024),
        )
        .await
        .expect("create file");
    writer.write_all(payload.clone()).await.expect("write file");
    writer.close().await.expect("commit file");
    cluster
        .converge_block_reports()
        .await
        .expect("converge pre-shutdown locations");

    for signal in [libc::SIGTERM, libc::SIGINT] {
        let mut held_health = hold_metadata_health_watch(cluster.metadata_endpoint()).await;
        let mut held_http = hold_metadata_http_connection(
            cluster
                .metadata_process_http_addr()
                .expect("Metadata process HTTP address"),
        )
        .await;
        cluster
            .restart_metadata_process_after_signal(executable, signal)
            .await
            .expect("gracefully restart Metadata process");
        let health_closed = tokio::time::timeout(std::time::Duration::from_secs(2), held_health.message())
            .await
            .expect("forced gRPC shutdown must close active health watch");
        assert!(
            health_closed.is_err() || health_closed.expect("health stream result checked").is_none(),
            "old Metadata health watch must not survive process shutdown"
        );
        let mut closed = [0u8; 1];
        assert_eq!(
            held_http.read(&mut closed).await.expect("read closed HTTP connection"),
            0
        );
        cluster
            .converge_block_reports()
            .await
            .expect("rebuild current locations");
        let actual = client
            .open("/process-shutdown/visible")
            .await
            .expect("open visible file after restart")
            .read_all()
            .await
            .expect("read visible file after restart");
        assert_eq!(actual, payload);
    }

    cluster.shutdown().await.expect("shutdown cluster");
}
