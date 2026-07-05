// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Raft network placeholder.
//!
//! This network is not part of the active runtime. Metadata lifecycle rejects
//! cluster mode until metadata peer RPC semantics, membership, and freshness
//! fencing are implemented.

use crate::raft::types::{MetadataNode, MetadataRaftTypeConfig};
use openraft::error::{RPCError, RaftError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse, VoteRequest,
    VoteResponse,
};
use openraft::{RaftNetwork, RaftNetworkFactory};
use std::fmt;

/// Inactive Raft network adapter.
pub struct Network {
    target: u64,
    // No network client is stored because cluster mode is rejected before this
    // adapter can be part of the active runtime.
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
        // Keep the placeholder fail-closed if a test or exploratory path ever
        // constructs it directly.
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
        // Keep the placeholder fail-closed if a test or exploratory path ever
        // constructs it directly.
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
        // Keep the placeholder fail-closed if a test or exploratory path ever
        // constructs it directly.
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

/// Inactive Raft network factory.
pub struct NetworkFactory {
    // Cluster mode is rejected before this factory can select a real client.
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
        // The returned adapter is intentionally unreachable in active runtime.
        Network::new(target)
    }
}
