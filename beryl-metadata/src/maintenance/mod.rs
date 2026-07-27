// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Background worker-state convergence and repair scheduling.
//!
//! Cleanup commands are derived from namespace authority and reconstructable
//! worker reports. Dispatch remains disabled by default until end-to-end
//! recovery validation authorizes production enablement.

mod cleanup;
mod lost_worker;
pub mod repair;
mod service;

pub(crate) use cleanup::BlockCleanupCoordinator;
pub use service::{MaintenanceHandle, MaintenanceService};
