// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Operation context, authority refresh, and structured error classification.
//!
//! One stable [`OperationContext`] represents a logical public call and a
//! fresh [`AttemptContext`] represents each concrete RPC attempt.

pub(crate) mod classify;
pub(crate) mod context;
pub(crate) mod refresh;

pub(crate) use classify::{
    classify_error, is_definite_worker_capacity_rejection, is_worker_capacity_before_side_effect_rejection, ErrorClass,
};
pub(crate) use context::ClientIdentity;
pub(crate) use context::{AttemptContext, OperationContext, OperationDeadline};
pub(crate) use refresh::MetadataTargets;
