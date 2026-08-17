// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Worker management and BlockReport handling.
//!
//! This module implements:
//! - Worker registration and volatile heartbeat liveness
//! - BlockReport processing (full + delta)
//! - Block locations convergence

mod manager;
mod service;

pub use manager::{
    BlockReportBlock, BlockReportBlockState, HealthStatus, ReplicaKey, WorkerDescriptor, WorkerInfo, WorkerLiveState,
    WorkerManager,
};
pub(crate) use manager::{PublishReadyConflict, PublishReadyStatus, PublishReadyTarget, ReadyReplicaCursor};
pub use service::MetadataWorkerServiceImpl;
