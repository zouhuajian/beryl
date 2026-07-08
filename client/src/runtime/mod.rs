// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Operation execution, replay policy, refresh, and error classification.
//!
//! The runtime routes metadata operations through [`OperationExecutor`], with
//! one stable [`OperationContext`] per logical public call and a fresh
//! [`AttemptContext`] for each metadata attempt.

pub(crate) mod backoff;
pub(crate) mod classify;
pub(crate) mod client_runtime;
pub(crate) mod context;
pub(crate) mod executor;
pub(crate) mod policy;
pub(crate) mod refresh;

pub(crate) use backoff::{BackoffPolicy, BackoffSleeper, TokioBackoffSleeper};
pub(crate) use classify::{ErrorClass, ErrorClassifier, MetadataRefreshCause};
pub(crate) use client_runtime::{
    is_unknown_session_barrier_outcome, mark_session_after_metadata_error, metric_labels, refresh_hint_from_error,
    ClientRuntime,
};
pub(crate) use context::ClientIdentity;
pub(crate) use context::{AttemptContext, OperationContext, OperationIdentity};
pub(crate) use executor::OperationExecutor;
pub(crate) use policy::OperationKind;
pub(crate) use refresh::MetadataTargets;
