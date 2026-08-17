// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Shared worker identity and endpoint value objects.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

use crate::ids::WorkerId;

/// Largest number of block entries accepted in one full or delta report batch.
///
/// A complete full report may contain multiple batches. This ceiling bounds
/// one Worker-to-Metadata RPC without limiting the total blocks owned by a
/// Worker.
pub const MAX_REPORT_ENTRIES: usize = 1_000;

/// Worker network protocol advertised by metadata and consumed by clients/workers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum WorkerNetProtocol {
    Grpc,
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

    /// Parse a worker process-run identifier from its wire/storage string.
    pub fn parse(value: &str) -> Result<Self, uuid::Error> {
        Uuid::parse_str(value).map(Self)
    }

    /// Create from a UUID.
    pub const fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Return the inner UUID.
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }

    /// Compare two worker process-run identifiers without assigning ordering semantics.
    pub const fn matches(self, other: Self) -> bool {
        self.0.as_u128() == other.0.as_u128()
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
        Self::parse(value)
    }
}
