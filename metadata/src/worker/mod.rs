// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker management and BlockReport handling.
//!
//! This module implements:
//! - Worker registration and heartbeat
//! - BlockReport processing (full + delta)
//! - Block locations convergence
//! - Worker heartbeat command transport

mod command_router;
mod full_report_lease;
#[cfg(test)]
mod full_report_lease_tests;
mod manager;
pub(crate) mod metrics;
mod service;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod integration_tests;

pub(crate) use command_router::{DeleteCommandSource, RepairCommandSource, WorkerCommandRouter};
pub use full_report_lease::{FullReportLease, FullReportLeaseManager};
pub use manager::{HealthStatus, WorkerDescriptor, WorkerInfo, WorkerManager};
pub use metrics::WorkerMetrics;
pub use service::{MetadataWorkerServiceImpl, WorkerBackgroundHandle};
