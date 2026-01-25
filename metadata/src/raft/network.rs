// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Raft network implementation.

use crate::raft::types::{MetadataNode, MetadataRaftTypeConfig};
use openraft::error::{RPCError, RaftError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest,
    VoteResponse,
};
use openraft::{RaftNetwork, RaftNetworkFactory};
use std::fmt;

/// Raft network implementation.
pub struct Network {
    target: u64,
    // TODO: Add actual network client (gRPC, HTTP, etc.)
    // For now, this is a placeholder
}

impl Network {
    pub fn new(target: u64) -> Self {
        Self { target }
    }
}

impl RaftNetwork<MetadataRaftTypeConfig> for Network {
    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<MetadataRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, MetadataNode, RaftError<u64>>> {
        // TODO: Implement actual RPC call to target node
        // For now, return an error indicating not implemented
        #[derive(Debug)]
        struct NotImplementedError(u64);
        impl fmt::Display for NotImplementedError {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "RPC to node {} not implemented yet", self.0)
            }
        }
        impl std::error::Error for NotImplementedError {}

        Err(RPCError::Unreachable(Unreachable::new(&NotImplementedError(
            self.target,
        ))))
    }

    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<MetadataRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, MetadataNode, RaftError<u64, openraft::error::InstallSnapshotError>>,
    > {
        // TODO: Implement actual RPC call to target node
        #[derive(Debug)]
        struct NotImplementedError(u64);
        impl fmt::Display for NotImplementedError {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "RPC to node {} not implemented yet", self.0)
            }
        }
        impl std::error::Error for NotImplementedError {}

        Err(RPCError::Unreachable(Unreachable::new(&NotImplementedError(
            self.target,
        ))))
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, MetadataNode, RaftError<u64>>> {
        // TODO: Implement actual RPC call to target node
        #[derive(Debug)]
        struct NotImplementedError(u64);
        impl fmt::Display for NotImplementedError {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "RPC to node {} not implemented yet", self.0)
            }
        }
        impl std::error::Error for NotImplementedError {}

        Err(RPCError::Unreachable(Unreachable::new(&NotImplementedError(
            self.target,
        ))))
    }
}

/// Raft network factory.
pub struct NetworkFactory {
    // TODO: Add network client pool or factory
}

impl NetworkFactory {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for NetworkFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftNetworkFactory<MetadataRaftTypeConfig> for NetworkFactory {
    type Network = Network;

    async fn new_client(&mut self, target: u64, _node: &MetadataNode) -> Self::Network {
        // Create a new network client for the target node
        // Connection is established lazily when needed
        Network::new(target)
    }
}
