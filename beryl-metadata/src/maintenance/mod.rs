// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Background worker-state convergence and repair scheduling.
//!
//! Cleanup commands are derived from namespace authority and reconstructable
//! worker reports. Dispatch is enabled by default and remains guarded by exact
//! report identity, authority revalidation, and worker-local stamp checks.

mod cleanup;
mod detached_root;
mod lost_worker;
pub mod repair;
mod service;

pub(crate) use cleanup::BlockCleanupCoordinator;
pub(crate) use detached_root::DetachedRootReclaimer;
pub use service::{MaintenanceHandle, MaintenanceService};
