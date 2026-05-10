// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Vecton Worker: Data plane node for high-throughput chunk storage and retrieval.

pub mod admin;
pub mod block_manager;
pub mod block_store;
pub mod combo_validator;
pub mod command_executor;
pub mod config;
pub mod convert;
pub mod core;
pub mod data_header;
pub mod delete_op_log;
pub mod error;
pub mod eviction;
pub mod lifecycle;
pub mod metadata_client;
pub mod orphan;
pub mod pending_acks;
pub mod pipeline;
pub mod rebalance;
pub mod replication;
pub mod rpc_server;
pub mod service;
pub mod stream_manager;
pub mod ufs_fill;
pub mod volume_health;
pub mod volume_manager;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod replication_tests;

#[cfg(test)]
#[path = "tests/delete_op_log_tests.rs"]
mod delete_op_log_tests;

pub use admin::{AdminService, HealthResponse};
pub use block_manager::{BlockManager, BlockStats, ReplicationClient};
pub use block_store::BlockStore;
pub use command_executor::CommandExecutor;
pub use config::{
    EvictionConfig, MetadataConfig, MetadataGroupConfig, OrphanConfig, ReplicationConfig, UfsConfig,
    VolumeHealthConfig, WorkerConfig,
};
pub use core::{
    AbortWriteRequest, AbortWriteResult, BlockManagerCore, BlockStoreCore, CommitWriteRequest, CommitWriteResult,
    RangeMapper, ReadFrame, ReadOpenRequest, ReadOpenResult, StorageChunkSlice, StreamContext, StreamMode, WorkerCore,
    WorkerCoreResult, WriteFrame, WriteOpenRequest, WriteOpenResult,
};
pub use error::{ErrorMetadata, WorkerError};
pub use eviction::{EvictionManager, EvictionMetrics, WatermarkConfig};
pub use lifecycle::{Lifecycle, WorkerState};
pub use metadata_client::{MetadataClient, MetadataSession, SessionState};
pub use orphan::{OrphanManager, OrphanMetrics, ReconcileResult};
pub use rebalance::RebalanceManager;
pub use replication::GrpcReplicationClient;
pub use rpc_server::RpcServer;
pub use service::WorkerDataServiceImpl;
pub use stream_manager::{StreamManager, StreamState};
pub use ufs_fill::UfsFiller;
pub use volume_health::{VolumeHealthManager, VolumeHealthMetrics};
pub use volume_manager::{VolumeInfo, VolumeManager, VolumeState};
