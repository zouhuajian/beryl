// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker data-plane network protocol selection.

use std::fmt;

use proto::common::WorkerNetProtocolProto;
use proto::convert::parse_known_worker_net_protocol;

/// Worker data-plane network protocol.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum WorkerNetProtocol {
    #[default]
    Grpc,
    Quic,
    Rdma,
}

impl From<WorkerNetProtocolProto> for WorkerNetProtocol {
    fn from(value: WorkerNetProtocolProto) -> Self {
        match value {
            WorkerNetProtocolProto::WorkerNetProtocolQuic => Self::Quic,
            WorkerNetProtocolProto::WorkerNetProtocolRdma => Self::Rdma,
            WorkerNetProtocolProto::WorkerNetProtocolUnspecified | WorkerNetProtocolProto::WorkerNetProtocolGrpc => {
                Self::Grpc
            }
        }
    }
}

impl From<i32> for WorkerNetProtocol {
    fn from(value: i32) -> Self {
        parse_known_worker_net_protocol(value)
            .map(Self::from)
            .unwrap_or(Self::Grpc)
    }
}

impl fmt::Display for WorkerNetProtocol {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Grpc => formatter.write_str("grpc"),
            Self::Quic => formatter.write_str("quic"),
            Self::Rdma => formatter.write_str("rdma"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proto_unspecified_and_unknown_default_to_grpc() {
        assert_eq!(
            WorkerNetProtocol::from(WorkerNetProtocolProto::WorkerNetProtocolUnspecified),
            WorkerNetProtocol::Grpc
        );
        assert_eq!(WorkerNetProtocol::from(99), WorkerNetProtocol::Grpc);
    }

    #[test]
    fn proto_protocol_values_are_preserved() {
        assert_eq!(
            WorkerNetProtocol::from(WorkerNetProtocolProto::WorkerNetProtocolGrpc),
            WorkerNetProtocol::Grpc
        );
        assert_eq!(
            WorkerNetProtocol::from(WorkerNetProtocolProto::WorkerNetProtocolQuic),
            WorkerNetProtocol::Quic
        );
        assert_eq!(
            WorkerNetProtocol::from(WorkerNetProtocolProto::WorkerNetProtocolRdma),
            WorkerNetProtocol::Rdma
        );
    }
}
