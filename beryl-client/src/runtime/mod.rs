// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Metadata execution, refresh, and structured error classification.
//!
//! The runtime routes metadata operations through [`MetadataExecutor`], with
//! one stable [`OperationContext`] per logical public call and a fresh
//! [`AttemptContext`] for each metadata attempt.

pub(crate) mod classify;
pub(crate) mod client_runtime;
pub(crate) mod context;
pub(crate) mod executor;
pub(crate) mod refresh;

pub(crate) use classify::{
    classify_error, is_definite_worker_capacity_rejection, is_worker_capacity_before_side_effect_rejection, ErrorClass,
};
pub(crate) use client_runtime::{
    is_unknown_session_barrier_outcome, mark_session_after_metadata_error, metric_labels, refresh_hint_from_error,
    ClientRuntime,
};
pub(crate) use context::ClientIdentity;
pub(crate) use context::{AttemptContext, OperationContext, OperationDeadline};
pub(crate) use executor::MetadataExecutor;
pub(crate) use refresh::MetadataTargets;
