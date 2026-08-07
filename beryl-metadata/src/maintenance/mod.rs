// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Background namespace deletion, block cleanup, and worker liveness maintenance.

mod cleanup;
mod detached_root;
mod lost_worker;
mod service;

pub(crate) use cleanup::BlockCleanupCoordinator;
pub(crate) use detached_root::DetachedRootReclaimer;
pub use service::{MaintenanceHandle, MaintenanceService};
