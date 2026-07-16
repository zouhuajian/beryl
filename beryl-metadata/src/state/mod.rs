// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! State storage abstraction for metadata service.
//!
//! The production runtime uses `RaftStateStore`.

#[cfg(test)]
mod memory;
mod raft_store;

#[cfg(test)]
pub use memory::MemoryStateStore;
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
    use beryl_types::ids::DataHandleId;

    #[tokio::test]
    async fn test_route_epoch() {
        let v1 = RouteEpoch::new(1);
        let v2 = RouteEpoch::new(2);

        assert_eq!(v1.as_u64(), 1);
        assert_eq!(v2.as_u64(), 2);
        assert_ne!(v1, v2);
    }

    #[tokio::test]
    async fn test_validate_data_handle_owner() {
        use crate::raft::RocksDBStorage;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(dir.path()).unwrap();
        let dh1 = DataHandleId::new(1);
        let inode1 = beryl_types::fs::InodeId::new(10);
        storage.put_data_handle_owner(dh1, inode1).unwrap();

        // Success path
        let owner = storage.validate_data_handle_owner(dh1, None).unwrap();
        assert_eq!(owner, inode1);

        // Missing handle should return StaleState
        let missing = storage.validate_data_handle_owner(DataHandleId::new(99), None);
        assert!(missing.is_err());

        // Mismatch should return InvalidArgument
        let mismatch = storage.validate_data_handle_owner(dh1, Some(beryl_types::fs::InodeId::new(11)));
        assert!(mismatch.is_err());
    }
}
