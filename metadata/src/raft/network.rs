// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Fail-closed network adapter for the single-node metadata runtime.
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

#[derive(Debug)]
struct PeerRpcDisabled(u64);

impl fmt::Display for PeerRpcDisabled {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "peer RPC to node {} is disabled in single-node mode", self.0)
    }
}

impl std::error::Error for PeerRpcDisabled {}

/// Inactive Raft network adapter.
pub(crate) struct SingleNodeNetwork {
    target: u64,
    // No network client is stored because cluster mode is rejected before this
    // adapter can be part of the active runtime.
}

impl SingleNodeNetwork {
    pub fn new(target: u64) -> Self {
        Self { target }
    }
}

impl RaftNetwork<MetadataRaftTypeConfig> for SingleNodeNetwork {
    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<MetadataRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, MetadataNode, RaftError<u64>>> {
        Err(RPCError::Unreachable(Unreachable::new(&PeerRpcDisabled(self.target))))
    }

    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<MetadataRaftTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, MetadataNode, RaftError<u64, openraft::error::InstallSnapshotError>>,
    > {
        Err(RPCError::Unreachable(Unreachable::new(&PeerRpcDisabled(self.target))))
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, MetadataNode, RaftError<u64>>> {
        Err(RPCError::Unreachable(Unreachable::new(&PeerRpcDisabled(self.target))))
    }
}

/// Inactive Raft network factory.
pub(crate) struct SingleNodeNetworkFactory {
    // Cluster mode is rejected before this factory can select a real client.
}

impl SingleNodeNetworkFactory {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for SingleNodeNetworkFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftNetworkFactory<MetadataRaftTypeConfig> for SingleNodeNetworkFactory {
    type Network = SingleNodeNetwork;

    async fn new_client(&mut self, target: u64, _node: &MetadataNode) -> Self::Network {
        // The returned adapter is intentionally unreachable in active runtime.
        SingleNodeNetwork::new(target)
    }
}
