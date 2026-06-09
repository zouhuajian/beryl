// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Client-side caching for metadata freshness.

pub mod state_id;

pub use state_id::StateIdCache;

/// Low-cardinality cache invalidation reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CacheInvalidationReason {
    /// Worker process-run mismatch.
    WorkerRun,
    /// Worker endpoint unavailable.
    Unavailable,
}

impl CacheInvalidationReason {
    /// Low-cardinality metric label.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::WorkerRun => "worker_run",
            Self::Unavailable => "unavailable",
        }
    }
}
