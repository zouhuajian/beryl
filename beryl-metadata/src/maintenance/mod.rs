// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Background worker-state convergence and repair scheduling.
//!
//! Physical block reclamation is not yet executed by workers. Cleanup commands
//! are derived from namespace authority and reconstructable worker reports, and
//! dispatch remains disabled by default until worker-side safety is available.

mod cleanup;
mod lost_worker;
pub mod repair;
mod service;

pub(crate) use cleanup::BlockCleanupCoordinator;
pub use service::{MaintenanceHandle, MaintenanceService};
