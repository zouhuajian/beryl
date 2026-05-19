// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Operation execution, replay policy, refresh, and error classification.
//!
//! The runtime routes metadata operations through [`OperationExecutor`], with
//! one stable [`OperationContext`] per logical public call and a fresh
//! [`AttemptContext`] for each metadata attempt.

pub(crate) mod backoff;
pub mod classify;
pub mod context;
pub mod decision;
pub mod executor;
pub mod policy;
pub mod refresh;

pub(crate) use backoff::{BackoffPolicy, BackoffSleeper, TokioBackoffSleeper};
pub use classify::{ErrorClass, ErrorClassifier, RefreshReason};
pub use context::{AttemptContext, OperationContext, OperationIdentity};
pub(crate) use decision::{RetryDecision, RetryDecisionInput};
pub(crate) use executor::{OperationExecutor, OperationRuntime};
pub use policy::OperationKind;
pub use refresh::RefreshManager;
