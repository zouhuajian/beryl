// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker control-plane startup registration.

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
