// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use std::sync::Arc;
use std::time::Duration;

use metadata::service::MetadataFileSystemServiceImpl;
use metadata::worker::MetadataWorkerServiceImpl;
use proto::metadata::file_system_service_proto_server::FileSystemServiceProtoServer;
use proto::metadata::metadata_worker_service_proto_server::MetadataWorkerServiceProtoServer;
use proto::worker::worker_data_service_server::WorkerDataServiceServer;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use worker::control::RegistrationSet;
use worker::net::server::grpc::WorkerDataServiceImpl;
use worker::WorkerCore;

use crate::TestResult;

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

pub struct MetadataServiceInstance {
    handle: ServerHandle,
}

impl MetadataServiceInstance {
    pub fn start(
        listener: TcpListener,
        filesystem: MetadataFileSystemServiceImpl,
        worker: MetadataWorkerServiceImpl,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            Server::builder()
                .add_service(FileSystemServiceProtoServer::new(filesystem))
                .add_service(MetadataWorkerServiceProtoServer::new(worker))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_rx.await;
                })
                .await?;
            Ok(())
        });
        Self {
            handle: ServerHandle::new(shutdown_tx, task),
        }
    }

    pub async fn shutdown(&mut self) -> TestResult<()> {
        self.handle.shutdown().await
    }

    pub fn abort(&mut self) {
        self.handle.abort();
    }
}

pub struct WorkerServiceInstance {
    handle: ServerHandle,
}

impl WorkerServiceInstance {
    pub fn start(listener: TcpListener, core: Arc<WorkerCore>, registration_state: Arc<RegistrationSet>) -> Self {
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let service = WorkerDataServiceImpl::new(core, registration_state);
            Server::builder()
                .add_service(WorkerDataServiceServer::new(service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_rx.await;
                })
                .await?;
            Ok(())
        });
        Self {
            handle: ServerHandle::new(shutdown_tx, task),
        }
    }

    pub async fn shutdown(&mut self) -> TestResult<()> {
        self.handle.shutdown().await
    }

    pub fn abort(&mut self) {
        self.handle.abort();
    }
}

struct ServerHandle {
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<TestResult<()>>,
}

impl ServerHandle {
    fn new(shutdown: oneshot::Sender<()>, task: JoinHandle<TestResult<()>>) -> Self {
        Self {
            shutdown: Some(shutdown),
            task,
        }
    }

    async fn shutdown(&mut self) -> TestResult<()> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        match timeout(SHUTDOWN_TIMEOUT, &mut self.task).await {
            Ok(result) => result?,
            Err(_) => {
                self.task.abort();
                Err("server shutdown timed out".into())
            }
        }
    }

    fn abort(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.task.abort();
    }
}
