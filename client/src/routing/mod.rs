// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Routing and group role management.

pub mod group_role;
pub mod route_table;
pub mod selection;

pub use group_role::{GroupRole, GroupRoleCache};
pub use route_table::RouteTable;
pub use selection::{SelectionStrategy, WorkerSelector};
