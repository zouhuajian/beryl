// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker control-plane startup registration.

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
