// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Shared worker identity and endpoint value objects.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

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
    pub worker_run_id: WorkerRunId,
}

/// UUID generated once for a worker process run.
///
/// This identifies a worker process start for metadata registration. It is not
/// an epoch and intentionally has no ordering semantics.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkerRunId(Uuid);

impl WorkerRunId {
    /// Generate a new process-run identifier.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Create from a UUID.
    pub const fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Return the inner UUID.
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl Default for WorkerRunId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for WorkerRunId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for WorkerRunId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value).map(Self)
    }
}
