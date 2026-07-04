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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_run_id_parse_matches_from_str_and_rejects_invalid_values() {
        let value = "550e8400-e29b-41d4-a716-446655440000";
        let parsed = WorkerRunId::parse(value).expect("valid WorkerRunId");
        assert_eq!(parsed, value.parse::<WorkerRunId>().expect("valid WorkerRunId"));

        assert!(WorkerRunId::parse("").is_err());
        assert!(WorkerRunId::parse("not-a-uuid").is_err());
    }

    #[test]
    fn worker_run_id_matches_is_exact_equality() {
        let first = WorkerRunId::parse("550e8400-e29b-41d4-a716-446655440001").expect("valid WorkerRunId");
        let same = WorkerRunId::parse("550e8400-e29b-41d4-a716-446655440001").expect("valid WorkerRunId");
        let other = WorkerRunId::parse("550e8400-e29b-41d4-a716-446655440002").expect("valid WorkerRunId");

        assert!(first.matches(same));
        assert!(!first.matches(other));
    }
}
