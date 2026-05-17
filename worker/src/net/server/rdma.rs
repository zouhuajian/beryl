// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! RDMA worker data-plane server placeholder.

use std::sync::Arc;

use anyhow::bail;

use crate::data::core::WorkerCore;

pub async fn serve_rdma_worker_data(bind: &str, _core: Arc<WorkerCore>) -> anyhow::Result<()> {
    bail!("worker RDMA data server is not implemented for bind {bind}")
}
