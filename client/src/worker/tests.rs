// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Tests for worker client.

#[cfg(test)]
mod worker_client_tests {
    use super::super::client::{ClientWorkerNetProtocol, WorkerEndpointInfo};
    use types::ids::WorkerId;

    #[test]
    fn test_worker_endpoint_info_from_proto() {
        let proto = proto::common::WorkerEndpointInfoProto {
            worker_id: 1,
            endpoint: "127.0.0.1:9090".to_string(),
            worker_net_protocol: proto::common::WorkerNetProtocolProto::WorkerNetProtocolGrpc as i32,
            worker_epoch: 100,
        };

        let endpoint_info = WorkerEndpointInfo::from_proto(proto);
        assert_eq!(endpoint_info.worker_id, WorkerId::new(1));
        assert_eq!(endpoint_info.endpoint, "127.0.0.1:9090");
        assert_eq!(endpoint_info.worker_net_protocol, 1);
        assert_eq!(endpoint_info.worker_epoch, 100);
    }

    #[test]
    fn test_worker_net_protocol_conversion() {
        assert_eq!(
            WorkerEndpointInfo::worker_net_protocol_to_protocol(1),
            ClientWorkerNetProtocol::Grpc
        );
        assert_eq!(
            WorkerEndpointInfo::worker_net_protocol_to_protocol(2),
            ClientWorkerNetProtocol::Quic
        );
        assert_eq!(
            WorkerEndpointInfo::worker_net_protocol_to_protocol(3),
            ClientWorkerNetProtocol::Rdma
        );
        // Default to grpc for unknown values
        assert_eq!(
            WorkerEndpointInfo::worker_net_protocol_to_protocol(0),
            ClientWorkerNetProtocol::Grpc
        );
        assert_eq!(
            WorkerEndpointInfo::worker_net_protocol_to_protocol(99),
            ClientWorkerNetProtocol::Grpc
        );
    }
}
