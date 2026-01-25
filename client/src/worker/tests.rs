// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Tests for worker client.

#[cfg(test)]
mod tests {
    use super::super::client::WorkerEndpointInfo;
    use transport::net::NetTransportKind;
    use types::ids::WorkerId;

    #[test]
    fn test_worker_endpoint_info_from_proto() {
        let proto = proto::common::WorkerEndpointInfoProto {
            worker_id: 1,
            endpoint: "127.0.0.1:9090".to_string(),
            net_transport_kind: proto::common::NetTransportKindProto::NetTransportKindGrpc as i32,
            worker_epoch: 100,
        };

        let endpoint_info = WorkerEndpointInfo::from_proto(proto);
        assert_eq!(endpoint_info.worker_id, WorkerId::new(1));
        assert_eq!(endpoint_info.endpoint, "127.0.0.1:9090");
        assert_eq!(endpoint_info.net_transport_kind, 1);
        assert_eq!(endpoint_info.worker_epoch, 100);
    }

    #[test]
    fn test_net_transport_kind_conversion() {
        assert_eq!(
            WorkerEndpointInfo::net_transport_kind_to_transport_kind(1),
            NetTransportKind::Grpc
        );
        assert_eq!(
            WorkerEndpointInfo::net_transport_kind_to_transport_kind(2),
            NetTransportKind::Quic
        );
        assert_eq!(
            WorkerEndpointInfo::net_transport_kind_to_transport_kind(3),
            NetTransportKind::Rdma
        );
        // Default to grpc for unknown values
        assert_eq!(
            WorkerEndpointInfo::net_transport_kind_to_transport_kind(0),
            NetTransportKind::Grpc
        );
        assert_eq!(
            WorkerEndpointInfo::net_transport_kind_to_transport_kind(99),
            NetTransportKind::Grpc
        );
    }
}
