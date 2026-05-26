// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker control-plane startup registration.

mod block_report;
mod heartbeat;
mod identity;
mod registrar;
mod registration;

pub use block_report::{BlockReportError, BlockReportOptions, BlockReportRound, MetadataBlockReportLoop};
pub use heartbeat::{HeartbeatError, HeartbeatRound, HeartbeatSnapshot, MetadataHeartbeatLoop};
pub use identity::resolve_worker_id;
pub use registrar::{MetadataRegistrar, RegistrationDescriptor, RegistrationError};
pub use registration::{Registration, RegistrationSet};
