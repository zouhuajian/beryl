// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! gRPC server for WorkerDataService.

use anyhow::{Context, Result};
use std::sync::Arc;
use tonic::transport::Server;
use tracing::info;

use crate::service::WorkerDataServiceImpl;
use proto::worker::worker_data_service_server::WorkerDataServiceServer;

/// gRPC server wrapper.
pub struct RpcServer {
    bind_addr: String,
    service: Arc<WorkerDataServiceImpl>,
}

impl RpcServer {
    /// Create a new RPC server.
    pub fn new(bind_addr: String, service: Arc<WorkerDataServiceImpl>) -> Self {
        Self { bind_addr, service }
    }

    /// Start the RPC server.
    pub async fn start(&self) -> Result<()> {
        let addr = self.bind_addr.parse().context("Invalid bind address")?;

        info!(addr = %self.bind_addr, "Starting gRPC server");

        let service = (*self.service).clone();
        Server::builder()
            .add_service(WorkerDataServiceServer::new(service))
            .serve(addr)
            .await
            .context("gRPC server error")?;

        Ok(())
    }
}
