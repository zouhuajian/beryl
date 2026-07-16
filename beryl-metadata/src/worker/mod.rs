// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Worker management and BlockReport handling.
//!
//! This module implements:
//! - Worker registration and volatile heartbeat liveness
//! - BlockReport processing (full + delta)
//! - Block locations convergence

mod manager;
pub(crate) mod metrics;
mod service;

#[cfg(test)]
mod tests;

pub use manager::{
    BlockReportBlock, BlockReportBlockState, HealthStatus, WorkerDescriptor, WorkerInfo, WorkerLiveState, WorkerManager,
};
pub use service::{MetadataWorkerServiceImpl, WorkerBackgroundHandle};
