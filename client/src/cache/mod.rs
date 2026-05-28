// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Client-side caching for metadata freshness.

pub mod state_id;
pub(crate) mod worker_endpoint;

pub use state_id::StateIdCache;
pub(crate) use worker_endpoint::WorkerEndpointCache;

use crate::metrics::ClientMetricLabels;

/// Low-cardinality cache invalidation reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CacheInvalidationReason {
    /// Entry expired by TTL.
    Ttl,
    /// Route epoch mismatch.
    RouteEpoch,
    /// Worker epoch mismatch.
    WorkerEpoch,
    /// Worker endpoint unavailable.
    Unavailable,
    /// Owner group or mount owner changed.
    Owner,
    /// Worker protocol mismatch or invalid protocol.
    Protocol,
}

impl CacheInvalidationReason {
    /// Low-cardinality metric label.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Ttl => "ttl",
            Self::RouteEpoch => "route_epoch",
            Self::WorkerEpoch => "worker_epoch",
            Self::Unavailable => "unavailable",
            Self::Owner => "owner",
            Self::Protocol => "protocol",
        }
    }
}

pub(crate) fn cache_labels(
    cache: &'static str,
    plane: &'static str,
    operation: &'static str,
    outcome: &'static str,
) -> ClientMetricLabels {
    ClientMetricLabels::default()
        .with_cache(cache)
        .with_target_plane(plane)
        .with_operation_name(operation)
        .with_outcome(outcome)
}
