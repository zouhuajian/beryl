// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Configuration key constants.
//!
//! This module provides a centralized definition of all configuration keys used in the system.
//! All configuration keys should be defined here as constants, organized by functional modules.

/// Metadata RPC configuration keys.
pub mod metadata_rpc {
    /// RPC server bind address.
    pub const ADDR: &str = "metadata.rpc.addr";
    /// RPC server port.
    pub const PORT: &str = "metadata.rpc.port";
}

/// Metadata Raft configuration keys.
pub mod metadata_raft {
    /// Raft cluster ID.
    pub const CLUSTER_ID: &str = "metadata.raft.cluster_id";
    /// Raft node ID.
    pub const NODE_ID: &str = "metadata.raft.node_id";
    /// Raft peer addresses (comma-separated).
    pub const PEERS: &str = "metadata.raft.peers";
    /// Raft storage directory.
    pub const STORAGE_DIR: &str = "metadata.raft.storage.dir";
    /// Raft snapshot interval in seconds.
    pub const SNAPSHOT_INTERVAL_SECS: &str = "metadata.raft.snapshot.interval_secs";
    /// Raft snapshot retain count.
    pub const SNAPSHOT_RETAIN_COUNT: &str = "metadata.raft.snapshot.retain_count";
    /// Raft log compaction interval in seconds.
    pub const LOG_COMPACTION_INTERVAL_SECS: &str = "metadata.raft.log.compaction_interval_secs";
    /// Raft log retain count.
    pub const LOG_RETAIN_COUNT: &str = "metadata.raft.log.retain_count";
    /// Raft heartbeat interval in milliseconds.
    pub const HEARTBEAT_INTERVAL_MS: &str = "metadata.raft.heartbeat_interval_ms";
    /// Raft election timeout minimum in milliseconds.
    pub const ELECTION_TIMEOUT_MIN_MS: &str = "metadata.raft.election_timeout_min_ms";
    /// Raft election timeout maximum in milliseconds.
    pub const ELECTION_TIMEOUT_MAX_MS: &str = "metadata.raft.election_timeout_max_ms";
}

/// Metadata shard configuration keys.
pub mod metadata_shard {
    /// Number of shards.
    pub const NUM_SHARDS: &str = "metadata.shard.num_shards";
    /// Shard group ID.
    pub const GROUP_ID: &str = "metadata.shard.group_id";
}

/// Worker RPC configuration keys.
pub mod worker_rpc {
    /// RPC server bind address (format: "host:port").
    pub const BIND: &str = "worker.rpc.bind";
    /// Maximum concurrent inflight RPC requests.
    pub const MAX_INFLIGHT: &str = "worker.rpc.max_inflight";
}

/// Worker storage configuration keys.
pub mod worker_storage {
    /// Storage directories (comma-separated paths).
    pub const DIRS: &str = "worker.storage.dirs";
    /// Block size (e.g., "32MB" or bytes).
    pub const BLOCK_SIZE: &str = "worker.storage.block_size";
    /// Chunk size (e.g., "1MB" or bytes).
    pub const CHUNK_SIZE: &str = "worker.storage.chunk_size";
    /// Storage backend kind: "fs", "io_uring", or "spdk".
    pub const KIND: &str = "worker.storage.kind";
}

/// Worker concurrency configuration keys.
pub mod worker_concurrency {
    /// Maximum concurrent read operations.
    pub const MAX_READ_OPS: &str = "worker.concurrency.max_read_ops";
    /// Maximum concurrent write operations.
    pub const MAX_WRITE_OPS: &str = "worker.concurrency.max_write_ops";
    /// Request queue size.
    pub const QUEUE_SIZE: &str = "worker.concurrency.queue_size";
}

/// Worker eviction configuration keys.
pub mod worker_eviction {
    /// High watermark for eviction (0.0-1.0, e.g., "0.90" = 90%).
    pub const HIGH_WATERMARK: &str = "worker.eviction.high_watermark";
    /// Low watermark for eviction (must be < high_watermark).
    pub const LOW_WATERMARK: &str = "worker.eviction.low_watermark";
    /// Eviction rate in bytes per second (e.g., "100MB" or bytes).
    pub const RATE_BYTES_PER_SEC: &str = "worker.eviction.rate_bytes_per_sec";
    /// Eviction rate in IOPS.
    pub const RATE_IOPS: &str = "worker.eviction.rate_iops";
}

/// Worker orphan block configuration keys.
pub mod worker_orphan {
    /// Grace period before deleting orphan blocks (seconds).
    pub const GRACE_PERIOD_SECS: &str = "worker.orphan.grace_period_secs";
    /// Interval for scanning orphan blocks (seconds).
    pub const SCAN_INTERVAL_SECS: &str = "worker.orphan.scan_interval_secs";
}

/// Worker volume health configuration keys.
pub mod worker_volume_health {
    /// Error rate threshold (errors per second).
    pub const ERROR_RATE_THRESHOLD: &str = "worker.volume_health.error_rate_threshold";
    /// Number of consecutive failures before marking volume unhealthy.
    pub const CONSECUTIVE_FAILURES_THRESHOLD: &str = "worker.volume_health.consecutive_failures_threshold";
    /// Interval for probing volume recovery (seconds).
    pub const RECOVERY_PROBE_INTERVAL_SECS: &str = "worker.volume_health.recovery_probe_interval_secs";
    /// Timeout for recovery probe (seconds).
    pub const RECOVERY_PROBE_TIMEOUT_SECS: &str = "worker.volume_health.recovery_probe_timeout_secs";
}

/// Worker UFS configuration keys.
pub mod worker_ufs {
    /// Default UFS instance ID (optional).
    pub const DEFAULT_ID: &str = "worker.ufs.default_id";
    /// Maximum concurrent operations per UFS instance.
    pub const MAX_CONCURRENT_PER_INSTANCE: &str = "worker.ufs.max_concurrent_per_instance";
    /// Timeout for UFS operations (milliseconds).
    pub const TIMEOUT_MS: &str = "worker.ufs.timeout_ms";
    /// Enable async fill-back for UFS reads.
    pub const ASYNC_FILL: &str = "worker.ufs.async_fill";
}

/// Worker metadata communication configuration keys.
pub mod worker_metadata {
    /// Heartbeat interval to metadata service (seconds).
    pub const HEARTBEAT_INTERVAL_SEC: &str = "worker.metadata.heartbeat_interval_sec";
    /// Block report interval to metadata service (seconds).
    pub const BLOCK_REPORT_INTERVAL_SEC: &str = "worker.metadata.block_report_interval_sec";
    /// Backoff duration on metadata connection failure (seconds).
    pub const BACKOFF_DURATION_SEC: &str = "worker.metadata.backoff_duration_sec";
    /// Metadata group endpoints (comma-separated "group_id:endpoint" pairs).
    pub const GROUPS: &str = "worker.metadata.groups";
}

/// Worker replication configuration keys.
pub mod worker_replication {
    /// Connection pool size per peer worker.
    pub const PEER_CONNECTION_POOL_SIZE: &str = "worker.replication.peer_connection_pool_size";
    /// Maximum concurrent blocks being replicated.
    pub const MAX_CONCURRENT_BLOCKS: &str = "worker.replication.max_concurrent_blocks";
    /// Maximum concurrent chunks per block during replication.
    pub const MAX_CONCURRENT_CHUNKS_PER_BLOCK: &str = "worker.replication.max_concurrent_chunks_per_block";
    /// Timeout for chunk replication (milliseconds).
    pub const CHUNK_TIMEOUT_MS: &str = "worker.replication.chunk_timeout_ms";
    /// Peer worker endpoints (comma-separated "worker_id:endpoint" pairs).
    pub const PEER_ENDPOINTS: &str = "worker.replication.peer_endpoints";
    /// Fencing token mode: "strict", "special", or "skip".
    pub const FENCING_MODE: &str = "worker.replication.fencing.mode";
    /// Special token value for replication (when mode=special, optional).
    pub const FENCING_SPECIAL_TOKEN: &str = "worker.replication.fencing.special_token";
}

/// Worker transport configuration keys.
pub mod worker_transport {
    /// Transport kind: "grpc", "quic", "rdma", "io_uring", or "local".
    pub const KIND: &str = "worker.transport.kind";
    /// Connection timeout in milliseconds.
    pub const CONNECT_TIMEOUT_MS: &str = "worker.transport.connect_timeout_ms";
    /// Request timeout in milliseconds.
    pub const REQUEST_TIMEOUT_MS: &str = "worker.transport.request_timeout_ms";
    /// Maximum concurrent inflight requests (client-side backpressure).
    pub const MAX_INFLIGHT_REQUESTS: &str = "worker.transport.max_inflight_requests";
    /// Maximum concurrent inflight streams (for streaming RPCs).
    pub const MAX_INFLIGHT_STREAMS: &str = "worker.transport.max_inflight_streams";
    /// Server-side max inflight (for ingress backpressure).
    pub const SERVER_MAX_INFLIGHT: &str = "worker.transport.server.max_inflight";
    /// Keep-alive interval in milliseconds.
    pub const KEEPALIVE_INTERVAL_MS: &str = "worker.transport.keepalive_interval_ms";
    /// Keep-alive timeout in milliseconds.
    pub const KEEPALIVE_TIMEOUT_MS: &str = "worker.transport.keepalive_timeout_ms";
    /// Require zero-copy support for transport.
    pub const ZERO_COPY_REQUIRED: &str = "worker.transport.zero_copy.required";
    /// Allow fallback to another transport if combo is invalid.
    pub const COMBO_ALLOW_FALLBACK: &str = "worker.transport.combo.allow_fallback";
    /// Fallback transport kind (if allow_fallback is true, optional).
    pub const COMBO_FALLBACK_TRANSPORT: &str = "worker.transport.combo.fallback_transport";
}

/// Observability logging configuration keys.
pub mod observe_logging {
    /// Log level: "trace", "debug", "info", "warn", or "error".
    pub const LEVEL: &str = "observe.logging.level";
    /// Log format: "json" or "pretty".
    pub const FORMAT: &str = "observe.logging.format";
    /// Target filters (e.g., "metadata=debug,transport=info", optional).
    pub const TARGETS: &str = "observe.logging.targets";
    /// Output logs to stdout.
    pub const STDOUT: &str = "observe.logging.stdout";
}

/// Observability tracing configuration keys.
pub mod observe_tracing {
    /// Enable distributed tracing.
    pub const ENABLED: &str = "observe.tracing.enabled";
    /// Sampling ratio for traces (0.0-1.0, e.g., "1.0" = 100%).
    pub const SAMPLING_RATIO: &str = "observe.tracing.sampling.ratio";
    /// Use parent-based sampling (respect parent trace decision).
    pub const SAMPLING_PARENT_BASED: &str = "observe.tracing.sampling.parent_based";
    /// Enable OTLP trace export.
    pub const OTLP_ENABLED: &str = "observe.tracing.otlp.enabled";
    /// OTLP endpoint URL.
    pub const OTLP_ENDPOINT: &str = "observe.tracing.otlp.endpoint";
    /// OTLP protocol: "grpc" or "http".
    pub const OTLP_PROTOCOL: &str = "observe.tracing.otlp.protocol";
    /// OTLP export timeout in milliseconds.
    pub const OTLP_TIMEOUT_MS: &str = "observe.tracing.otlp.timeout_ms";
}

/// Observability metrics configuration keys.
pub mod observe_metrics {
    /// Enable metrics collection.
    pub const ENABLED: &str = "observe.metrics.enabled";
    /// Enable Prometheus metrics exporter.
    pub const PROMETHEUS_ENABLED: &str = "observe.metrics.prometheus.enabled";
    /// Prometheus bind address (format: "host:port").
    pub const PROMETHEUS_BIND: &str = "observe.metrics.prometheus.bind";
    /// HTTP path for Prometheus metrics.
    pub const PROMETHEUS_PATH: &str = "observe.metrics.prometheus.path";
    /// Enable OTLP metrics export.
    pub const OTLP_ENABLED: &str = "observe.metrics.otlp.enabled";
    /// OTLP metrics endpoint URL.
    pub const OTLP_ENDPOINT: &str = "observe.metrics.otlp.endpoint";
    /// OTLP metrics protocol: "grpc" or "http".
    pub const OTLP_PROTOCOL: &str = "observe.metrics.otlp.protocol";
    /// OTLP metrics export interval in milliseconds.
    pub const OTLP_INTERVAL_MS: &str = "observe.metrics.otlp.interval_ms";
}

/// Client configuration keys.
pub mod client {
    /// Client ID.
    pub const ID: &str = "client.id";
    /// Default timeout for client operations (milliseconds).
    pub const DEFAULT_TIMEOUT_MS: &str = "client.default_timeout_ms";
    /// Metadata service endpoints (comma-separated).
    pub const METADATA_ENDPOINTS: &str = "client.metadata.endpoints";
}

/// Client consistency configuration keys.
pub mod client_consistency {
    /// Default consistency level: "normal", "strong", or "weak".
    pub const DEFAULT: &str = "client.consistency.default";
}

/// Client read mode configuration keys.
pub mod client_read_mode {
    /// Default read mode: "cached" or "direct".
    pub const DEFAULT: &str = "client.read_mode.default";
    /// Fallback strategy: "direct" or "disable".
    pub const FALLBACK: &str = "client.read_mode.fallback";
}

/// Client write mode configuration keys.
pub mod client_write_mode {
    /// Default write mode: "back", "through", or "direct".
    pub const DEFAULT: &str = "client.write_mode.default";
    /// Fallback strategy: "through", "direct", or "disable".
    pub const FALLBACK: &str = "client.write_mode.fallback";
}

/// Client cache configuration keys.
pub mod client_cache {
    /// File metadata cache: maximum number of entries.
    pub const FILE_META_MAX_ENTRIES: &str = "client.cache.file_meta.max_entries";
    /// File metadata cache: maximum memory in bytes (optional).
    pub const FILE_META_MAX_BYTES: &str = "client.cache.file_meta.max_bytes";
    /// File metadata cache: TTL in seconds.
    pub const FILE_META_TTL_SECS: &str = "client.cache.file_meta.ttl_secs";
    /// Route table cache: maximum number of entries.
    pub const ROUTE_MAX_ENTRIES: &str = "client.cache.route.max_entries";
    /// Route table cache: TTL in seconds.
    pub const ROUTE_TTL_SECS: &str = "client.cache.route.ttl_secs";
}

/// Client retry configuration keys.
pub mod client_retry {
    /// Maximum number of retries for failed operations.
    pub const MAX_RETRIES: &str = "client.retry.max_retries";
    /// Initial backoff delay in milliseconds.
    pub const INITIAL_BACKOFF_MS: &str = "client.retry.initial_backoff_ms";
    /// Maximum backoff delay in milliseconds.
    pub const MAX_BACKOFF_MS: &str = "client.retry.max_backoff_ms";
    /// Multiplier for exponential backoff (must be > 0).
    pub const BACKOFF_MULTIPLIER: &str = "client.retry.backoff_multiplier";
}

/// Client worker direct read configuration keys.
pub mod client_worker_direct_read {
    /// Enable direct read from worker nodes.
    pub const ENABLED: &str = "client.worker.direct_read.enabled";
    /// Direct read cache: maximum number of entries.
    pub const CACHE_MAX_ENTRIES: &str = "client.worker.direct_read.cache.max_entries";
    /// Direct read cache: TTL in seconds.
    pub const CACHE_TTL_SECS: &str = "client.worker.direct_read.cache.ttl_secs";
    /// Enable version checking for direct reads.
    pub const VERSION_CHECK: &str = "client.worker.direct_read.version_check";
}

/// UFS configuration keys.
pub mod ufs {
    /// Get max inflight for a specific UFS instance.
    pub fn max_inflight(ufs_name: &str) -> String {
        format!("ufs.{}.max_inflight", ufs_name)
    }
}
