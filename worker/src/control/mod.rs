// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker control-plane startup registration.

use common::observe::propagation::{inject_trace_context, ExtractedContext};
use proto::common::RequestHeaderProto;
use types::{CallId, ClientId};

mod block_report;
mod heartbeat;
pub(crate) mod identity;
mod registrar;
mod registration;
mod storage;

pub use block_report::{BlockReportError, BlockReportOptions, BlockReportRound, MetadataBlockReportLoop};
pub use heartbeat::{HeartbeatError, HeartbeatRound, HeartbeatSnapshot, MetadataHeartbeatLoop};
pub use registrar::{MetadataRegistrar, RegistrationDescriptor, RegistrationError};
pub use registration::{Registration, RegistrationSet};
pub use storage::{prepare_worker_start, worker_storage_info_path, WorkerStorageInfo};

#[derive(Clone, Copy, Debug)]
struct ControlIdentity {
    client_id: ClientId,
}

impl ControlIdentity {
    /// This constructor creates a local runtime identity. It must not be used to decode external request headers.
    fn new_local() -> Self {
        Self {
            client_id: ClientId::generate(),
        }
    }

    fn new_op(self) -> ControlOp {
        ControlOp {
            client_id: self.client_id,
            call_id: CallId::new(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct ControlOp {
    client_id: ClientId,
    call_id: CallId,
}

fn metadata_tonic_request<T>(message: T, header: Option<&RequestHeaderProto>) -> tonic::Request<T> {
    let mut request = tonic::Request::new(message);
    if let Some(header) = header {
        if let Some(trace_context) = &header.trace_context {
            let context = ExtractedContext {
                traceparent: trace_context.traceparent.clone(),
                tracestate: trace_context.tracestate.clone(),
                baggage: trace_context.baggage.clone(),
            };
            inject_trace_context(request.metadata_mut(), &context);
        }
    }
    request
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::header::RequestHeader;
    use types::GroupName;

    #[test]
    fn metadata_request_injects_protocol_context_without_call_id_traceparent() {
        let group = GroupName::parse("root").unwrap();
        let header: RequestHeaderProto = (&RequestHeader::new(ClientId::new(7))
            .with_group_name(group)
            .with_tracestate("vendor=state".to_string())
            .with_baggage("tenant=local".to_string()))
            .into();

        let request = metadata_tonic_request((), Some(&header));
        let metadata = request.metadata();

        assert!(metadata.get("traceparent").is_none());
        assert_eq!(
            metadata.get("tracestate").and_then(|value| value.to_str().ok()),
            Some("vendor=state")
        );
        assert_eq!(
            metadata.get("baggage").and_then(|value| value.to_str().ok()),
            Some("tenant=local")
        );
        assert!(metadata.get(concat!("request", "-", "id")).is_none());
    }
}
