// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Internal repair queue and planner primitives.
//!
//! These types are maintenance internals for safety and cleanup. They are not
//! a complete productized repair or rebalance lifecycle.
//!
//! This module is organized as follows:
//! - `types.rs`: Core types (RepairTaskId, RepairTask, RepairTaskState, etc.)
//! - `actions.rs`: RepairAction enum (planning layer output)
//! - `queue.rs`: RepairQueue (state machine, deduplication, retry)
//! - `planner.rs`: RepairPlanner (pure planning logic)

mod actions;
mod metrics;
mod planner;
mod policy;
mod queue;
mod types;

pub use actions::RepairAction;
pub(crate) use metrics::RepairMetrics;
pub use planner::RepairPlanner;
pub use policy::RepairPolicy;
pub use queue::RepairQueue;
pub use types::{
    RepairDedupKey, RepairTask, RepairTaskId, RepairTaskRecord, RepairTaskState, TaskAckStatus, TaskFailureClass,
};
