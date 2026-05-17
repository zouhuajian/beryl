// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker-owned net configuration.

use crate::net::endpoint::WorkerEndpointRole;
use crate::net::protocol::WorkerNetProtocol;
use crate::runtime::block::BlockManager;

/// Worker data-plane net configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerNetConfig {
    pub listeners: Vec<WorkerListenerConfig>,
    pub peer: WorkerPeerNetConfig,
}

/// Worker listener configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerListenerConfig {
    pub protocol: WorkerNetProtocol,
    pub bind: String,
    pub role: Vec<WorkerEndpointRole>,
    /// Per-connection gRPC concurrency limit for listener protocols that support it.
    pub max_inflight: usize,
    pub max_frame_size: u32,
}

/// Worker peer-client net configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerPeerNetConfig {
    pub enabled_protocols: Vec<WorkerNetProtocol>,
    pub selection_policy: PeerProtocolSelectionPolicy,
}

/// Protocol selection policy for worker-to-worker peers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerProtocolSelectionPolicy {
    PreferRdmaThenQuicThenGrpc,
    PreferGrpc,
}

impl WorkerNetConfig {
    pub fn grpc_from_legacy_rpc(bind: String, max_inflight: usize, max_frame_size: u32) -> Self {
        Self {
            listeners: vec![WorkerListenerConfig::grpc(bind, max_inflight, max_frame_size)],
            peer: WorkerPeerNetConfig::default(),
        }
    }
}

impl Default for WorkerNetConfig {
    fn default() -> Self {
        Self::grpc_from_legacy_rpc("0.0.0.0:9090".to_string(), 100, BlockManager::MAX_FRAME_SIZE)
    }
}

impl WorkerListenerConfig {
    pub fn grpc(bind: String, max_inflight: usize, max_frame_size: u32) -> Self {
        Self {
            protocol: WorkerNetProtocol::Grpc,
            bind,
            role: vec![WorkerEndpointRole::ClientData],
            max_inflight,
            max_frame_size,
        }
    }
}

impl Default for WorkerPeerNetConfig {
    fn default() -> Self {
        Self {
            enabled_protocols: vec![WorkerNetProtocol::Grpc],
            selection_policy: PeerProtocolSelectionPolicy::PreferGrpc,
        }
    }
}
