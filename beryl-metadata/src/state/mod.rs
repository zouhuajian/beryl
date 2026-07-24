// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! State storage abstraction for metadata service.
//!
//! The production runtime uses `RaftStateStore`.

mod raft_store;

pub use raft_store::RaftStateStore;

use crate::error::MetadataResult;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Authoritative route epoch used by metadata stale-route validation.
///
/// This carrier is distinct from per-inode `FileLayout` state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RouteEpoch(u64);

impl RouteEpoch {
    pub fn new(epoch: u64) -> Self {
        Self(epoch)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

/// State store trait for freshness reads needed by `MetadataFileSystem`.
///
/// Authoritative metadata mutations go through Raft commands and apply batches.
/// This trait intentionally exposes only the current route freshness read.
#[async_trait]
pub trait StateStore: Send + Sync {
    /// Get the current authoritative route epoch.
    async fn get_route_epoch(&self) -> MetadataResult<RouteEpoch>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_epoch_preserves_value_and_identity() {
        let v1 = RouteEpoch::new(1);
        let v2 = RouteEpoch::new(2);

        assert_eq!(v1.as_u64(), 1);
        assert_eq!(v2.as_u64(), 2);
        assert_ne!(v1, v2);
    }
}
