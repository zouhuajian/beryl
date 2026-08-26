// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Operation context, authority refresh, and replay decisions.
//!
//! One stable [`OperationContext`] represents a logical public call and a
//! fresh [`AttemptContext`] represents each concrete RPC attempt.

pub(crate) mod context;
pub(crate) mod refresh;
pub(crate) mod retry;

pub(crate) use context::ClientIdentity;
pub(crate) use context::{AttemptContext, Operation, OperationContext, OperationDeadline, RetrySafety};
pub(crate) use refresh::MetadataTargets;
pub(crate) use retry::{
    is_definite_worker_capacity_rejection, retry_decision, transport_outcome_is_ambiguous, RetryDecision,
};
