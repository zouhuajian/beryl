// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker management and BlockReport handling.
//!
//! This module implements:
//! - Worker registration and heartbeat
//! - BlockReport processing (full + delta)
//! - Block locations convergence
//! - Repair queue and scheduling skeleton

mod command_router;
mod delete_executor;
#[cfg(test)]
mod delete_executor_tests;
mod full_report_lease;
#[cfg(test)]
mod full_report_lease_tests;
mod manager;
mod metrics;
mod repair;
mod service;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod integration_tests;

pub(crate) use command_router::{DeleteCommandSource, RepairCommandSource, WorkerCommandRouter};
pub use delete_executor::{DeleteExecutor, DeleteExecutorHandle};
pub use full_report_lease::{FullReportLease, FullReportLeaseManager};
pub use manager::{HealthStatus, WorkerDescriptor, WorkerInfo, WorkerManager};
pub use metrics::{RepairMetrics, WorkerMetrics};
pub use repair::{
    ErrorClass, OrphanMetrics, OrphanQueue, RepairPlanner, RepairQueue, RepairTask, RepairTaskId, RepairTaskRecord,
    TaskAckStatus,
};
pub use service::{MetadataWorkerServiceImpl, WorkerBackgroundHandle};
