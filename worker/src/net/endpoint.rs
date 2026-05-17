// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker net endpoint descriptors.

use super::capability::WorkerNetCapabilities;
use super::protocol::WorkerNetProtocol;

/// Role served by a worker endpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerEndpointRole {
    ClientData,
    PeerData,
    Admin,
}

/// Worker data-plane endpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerNetEndpoint {
    pub protocol: WorkerNetProtocol,
    pub endpoint: String,
    pub role: WorkerEndpointRole,
    pub priority: u32,
    pub capabilities: WorkerNetCapabilities,
    pub worker_epoch: u64,
}
