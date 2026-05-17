// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metrics constants and helpers.

/// UFS metrics name constants.
pub mod ufs {
    /// Total UFS operations.
    pub const OPS_TOTAL: &str = "ufs_ops_total";
    /// UFS operation latency in milliseconds.
    pub const OP_LATENCY_MS: &str = "ufs_op_latency_ms";
    /// Total bytes transferred.
    pub const BYTES_TOTAL: &str = "ufs_bytes_total";
}

/// Replication metrics name constants.
pub mod replication {
    /// Total chunk replication attempts.
    pub const CHUNKS_TOTAL: &str = "replication_chunks_total";
    /// Chunk replication latency in milliseconds.
    pub const CHUNK_LATENCY_MS: &str = "replication_chunk_latency_ms";
    /// Total bytes replicated.
    pub const BYTES_TOTAL: &str = "replication_bytes_total";
    /// Inflight chunk replications.
    pub const INFLIGHT_CHUNKS: &str = "replication_inflight_chunks";
    /// Total block replications.
    pub const BLOCKS_TOTAL: &str = "replication_blocks_total";
    /// Block replication latency in milliseconds.
    pub const BLOCK_LATENCY_MS: &str = "replication_block_latency_ms";
    /// Failed block replications.
    pub const BLOCKS_FAILED: &str = "replication_blocks_failed";
    /// Pending block replications.
    pub const BLOCKS_PENDING: &str = "replication_blocks_pending";
    /// Replicating block replications.
    pub const BLOCKS_REPLICATING: &str = "replication_blocks_replicating";
    /// Completed block replications.
    pub const BLOCKS_COMPLETED: &str = "replication_blocks_completed";
    /// Replication retry count.
    pub const RETRY_COUNT: &str = "replication_retry_count";
    /// Replication health score (0-100, higher is better).
    pub const HEALTH_SCORE: &str = "replication_health_score";
}

/// Allowed label names (whitelist to prevent high-cardinality labels).
///
/// Allowed labels:
/// - `backend`: Backend identifier (e.g., "s3", "fs", "hdfs")
/// - `op`: Operation name (e.g., "read", "write", "list")
/// - `method`: RPC method name
/// - `status`: Status code (e.g., "ok", "error")
/// - `error_kind`: Error category (from ErrorKind enum)
/// - `kind`: General kind/category
/// - `mount`: Metadata mount point
///
/// Forbidden labels (high cardinality):
/// - `path`, `object_key`: Use operation name instead
/// - `endpoint_host`: Use backend identifier instead
/// - `tenant`: Use service-level attributes instead
pub const ALLOWED_LABELS: &[&str] = &["backend", "op", "method", "status", "error_kind", "kind", "mount"];

/*
/// Helper to record latency in milliseconds.
pub fn record_latency(histogram: &metrics::Histogram, start: std::time::Instant) {
    let elapsed = start.elapsed();
    histogram.record(elapsed.as_secs_f64() * 1000.0);

/// Helper to increment a counter with labels.
pub fn inc_counter(counter: &metrics::Counter, labels: &[(&str, &str)]) {
    // Validate labels are in whitelist
    for (key, _) in labels {
        if !ALLOWED_LABELS.contains(key) {
            tracing::warn!(
                label = key,
                "Label not in whitelist, may cause high-cardinality metrics"
            );
        }
    }
    counter.increment(1);
}
*/
