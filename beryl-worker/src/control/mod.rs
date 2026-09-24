// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Worker control-plane startup registration.

use beryl_common::header::RequestHeader;
use beryl_proto::common::RequestHeaderProto;
use beryl_types::{CallId, ClientId};

mod block_report;
mod cleanup;
mod heartbeat;
mod identity;
mod registrar;
mod registration;
mod storage;

pub use block_report::{BlockReportError, BlockReportOutcome, MetadataBlockReportLoop};
pub use cleanup::{BlockCleanupExecutor, BlockCleanupRuntime};
pub use heartbeat::{HeartbeatError, HeartbeatOutcome, MetadataHeartbeatLoop};
pub use registrar::{MetadataRegistrar, RegistrationDescriptor, RegistrationError};
pub use registration::{Registration, RegistrationState};
pub use storage::{prepare_worker_start, worker_storage_info_path};

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

impl ControlOp {
    fn request_header(&self, group_name: &beryl_types::GroupName) -> RequestHeaderProto {
        let mut header = RequestHeader::new(self.client_id).with_group_name(group_name.clone());
        header.client.call_id = self.call_id;
        (&header).into()
    }
}
