// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Worker-owned net configuration.

use crate::net::protocol::WorkerNetProtocol;
use crate::runtime::block::BlockManager;

pub(crate) const DEFAULT_GRPC_MAX_CONCURRENT_READS: usize = 64;
pub(crate) const DEFAULT_GRPC_MAX_CONCURRENT_WRITES: usize = 32;

/// Worker data-plane net configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerNetConfig {
    pub listeners: Vec<WorkerListenerConfig>,
}

/// Worker listener configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerListenerConfig {
    pub protocol: WorkerNetProtocol,
    pub bind: String,
    /// Maximum admitted read RPC lifecycles shared by all connections.
    pub max_concurrent_reads: usize,
    /// Maximum admitted write RPC lifecycles shared by all connections.
    pub max_concurrent_writes: usize,
    pub max_frame_size: u32,
}

impl WorkerNetConfig {
    pub fn grpc_from_rpc(
        bind: String,
        max_concurrent_reads: usize,
        max_concurrent_writes: usize,
        max_frame_size: u32,
    ) -> Self {
        Self {
            listeners: vec![WorkerListenerConfig::grpc(
                bind,
                max_concurrent_reads,
                max_concurrent_writes,
                max_frame_size,
            )],
        }
    }
}

impl Default for WorkerNetConfig {
    fn default() -> Self {
        Self::grpc_from_rpc(
            "0.0.0.0:9090".to_string(),
            DEFAULT_GRPC_MAX_CONCURRENT_READS,
            DEFAULT_GRPC_MAX_CONCURRENT_WRITES,
            BlockManager::MAX_FRAME_SIZE,
        )
    }
}

impl WorkerListenerConfig {
    pub fn grpc(bind: String, max_concurrent_reads: usize, max_concurrent_writes: usize, max_frame_size: u32) -> Self {
        Self {
            protocol: WorkerNetProtocol::Grpc,
            bind,
            max_concurrent_reads,
            max_concurrent_writes,
            max_frame_size,
        }
    }
}
