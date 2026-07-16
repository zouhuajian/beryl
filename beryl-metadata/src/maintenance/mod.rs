// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Background worker-state convergence and repair scheduling.
//!
//! Physical block reclamation is not part of the current runtime. Namespace
//! reachability remains authoritative in file layouts; worker block reports are
//! soft state used by the remaining repair paths.

mod lost_worker;
pub mod repair;
mod service;

pub use service::{MaintenanceHandle, MaintenanceService};
