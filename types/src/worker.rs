// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Shared worker identity and endpoint value objects.

use serde::{Deserialize, Serialize};

use crate::ids::WorkerId;

/// Worker network protocol advertised by metadata and consumed by clients/workers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum WorkerNetProtocol {
    Grpc,
    Quic,
    Rdma,
}

/// Metadata-authoritative worker endpoint advertised for data-plane access.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkerEndpointInfo {
    pub worker_id: WorkerId,
    pub endpoint: String,
    pub worker_net_protocol: WorkerNetProtocol,
    pub worker_epoch: u64,
}
