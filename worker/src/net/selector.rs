// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker peer endpoint protocol selection.

use crate::net::config::PeerProtocolSelectionPolicy;
use crate::net::endpoint::WorkerNetEndpoint;
use crate::net::protocol::WorkerNetProtocol;

/// Selector for worker peer endpoints.
#[derive(Clone, Debug)]
pub struct WorkerPeerSelector {
    policy: PeerProtocolSelectionPolicy,
}

impl WorkerPeerSelector {
    pub const fn new(policy: PeerProtocolSelectionPolicy) -> Self {
        Self { policy }
    }

    pub fn select<'a>(&self, endpoints: &'a [WorkerNetEndpoint]) -> Option<&'a WorkerNetEndpoint> {
        self.select_enabled(
            endpoints,
            &[
                WorkerNetProtocol::Grpc,
                WorkerNetProtocol::Quic,
                WorkerNetProtocol::Rdma,
            ],
        )
    }

    pub fn select_enabled<'a>(
        &self,
        endpoints: &'a [WorkerNetEndpoint],
        enabled_protocols: &[WorkerNetProtocol],
    ) -> Option<&'a WorkerNetEndpoint> {
        match self.policy {
            PeerProtocolSelectionPolicy::PreferGrpc => {
                find_enabled_protocol(endpoints, enabled_protocols, WorkerNetProtocol::Grpc)
                    .or_else(|| first_enabled(endpoints, enabled_protocols))
            }
        }
    }
}

impl Default for WorkerPeerSelector {
    fn default() -> Self {
        Self::new(PeerProtocolSelectionPolicy::PreferGrpc)
    }
}

fn find_protocol(endpoints: &[WorkerNetEndpoint], protocol: WorkerNetProtocol) -> Option<&WorkerNetEndpoint> {
    endpoints.iter().find(|endpoint| endpoint.protocol == protocol)
}

fn find_enabled_protocol<'a>(
    endpoints: &'a [WorkerNetEndpoint],
    enabled_protocols: &[WorkerNetProtocol],
    protocol: WorkerNetProtocol,
) -> Option<&'a WorkerNetEndpoint> {
    if enabled_protocols.contains(&protocol) {
        find_protocol(endpoints, protocol)
    } else {
        None
    }
}

fn first_enabled<'a>(
    endpoints: &'a [WorkerNetEndpoint],
    enabled_protocols: &[WorkerNetProtocol],
) -> Option<&'a WorkerNetEndpoint> {
    endpoints
        .iter()
        .find(|endpoint| enabled_protocols.contains(&endpoint.protocol))
}
