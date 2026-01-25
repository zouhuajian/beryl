// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Consistency level definitions.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Consistency level for read operations.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ConsistencyLevel {
    /// Strong consistency (linearizable).
    ///
    /// - Must read from leader or use read-index/lease-read
    /// - Guarantees linearizability
    Strong,

    /// Normal consistency (bounded-stale).
    ///
    /// - Prefer followers if token is fresh enough
    /// - Use msync/refresh to compensate for staleness
    /// - Bounded by TTL/max-staleness window
    Normal,

    /// Weak consistency (best-effort).
    ///
    /// - Prefer followers and local cache
    /// - Fallback to metadata on failure
    /// - No staleness guarantees
    Weak,
}

impl ConsistencyLevel {
    /// Check if this level requires leader read.
    pub fn requires_leader(&self) -> bool {
        matches!(self, ConsistencyLevel::Strong)
    }

    /// Check if this level allows follower read.
    pub fn allows_follower(&self) -> bool {
        matches!(self, ConsistencyLevel::Normal | ConsistencyLevel::Weak)
    }

    /// Check if this level allows cache usage.
    pub fn allows_cache(&self) -> bool {
        matches!(self, ConsistencyLevel::Normal | ConsistencyLevel::Weak)
    }
}

impl fmt::Display for ConsistencyLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConsistencyLevel::Strong => write!(f, "strong"),
            ConsistencyLevel::Normal => write!(f, "normal"),
            ConsistencyLevel::Weak => write!(f, "weak"),
        }
    }
}

impl FromStr for ConsistencyLevel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "strong" => Ok(ConsistencyLevel::Strong),
            "normal" => Ok(ConsistencyLevel::Normal),
            "weak" => Ok(ConsistencyLevel::Weak),
            _ => Err(format!("Invalid consistency level: {}", s)),
        }
    }
}

/// Convert to proto ConsistencyLevelProto.
impl From<ConsistencyLevel> for proto::common::ConsistencyLevelProto {
    fn from(level: ConsistencyLevel) -> Self {
        match level {
            ConsistencyLevel::Strong => proto::common::ConsistencyLevelProto::ConsistencyStrong,
            ConsistencyLevel::Normal => proto::common::ConsistencyLevelProto::ConsistencyNormal,
            ConsistencyLevel::Weak => proto::common::ConsistencyLevelProto::ConsistencyWeak,
        }
    }
}

/// Convert from proto ConsistencyLevelProto.
impl From<proto::common::ConsistencyLevelProto> for ConsistencyLevel {
    fn from(level: proto::common::ConsistencyLevelProto) -> Self {
        match level {
            proto::common::ConsistencyLevelProto::ConsistencyStrong => ConsistencyLevel::Strong,
            proto::common::ConsistencyLevelProto::ConsistencyNormal => ConsistencyLevel::Normal,
            proto::common::ConsistencyLevelProto::ConsistencyWeak => ConsistencyLevel::Weak,
            _ => ConsistencyLevel::Normal, // Default
        }
    }
}
