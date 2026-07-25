// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Background worker-state convergence and repair scheduling.
//!
//! Physical block reclamation is not part of the current runtime. Cleanup
//! detection is observe-only; namespace reachability remains authoritative and
//! worker block reports remain reconstructable soft state.

mod cleanup;
mod lost_worker;
pub mod repair;
mod service;

pub(crate) use cleanup::BlockCleanupScanner;
pub use service::{MaintenanceHandle, MaintenanceService};
