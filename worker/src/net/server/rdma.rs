// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Inactive RDMA worker data-plane server placeholder.
//!
//! This file is not declared by the current worker server surface. RDMA listener
//! values are rejected explicitly by the active gRPC-only server entry point.

use std::sync::Arc;

use anyhow::bail;

use crate::data::core::WorkerCore;

pub async fn serve_rdma_worker_data(bind: &str, _core: Arc<WorkerCore>) -> anyhow::Result<()> {
    bail!("worker RDMA data server is not implemented for bind {bind}")
}
