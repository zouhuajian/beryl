# Vecton Configuration Matrix

This document records the currently supported configuration keys and startup
behavior. It intentionally does not document removed compatibility keys as
available runtime options.

## Command Model

Vecton does not have a unified CLI yet. The supported process commands are:

- `metadata format --config <path>`
- `metadata start --config <path>`
- `worker start --config <path>`

The default split config pair is:

```bash
metadata format --config conf/metadata.yaml
metadata start --config conf/metadata.yaml
worker start --config conf/worker.yaml
```

The local runnable pair is:

```bash
metadata format --config conf/local/metadata.yaml
metadata start --config conf/local/metadata.yaml
worker start --config conf/local/worker.yaml
```

Metadata storage must be formatted explicitly before metadata starts.
`metadata start` is non-destructive: it never formats storage, initializes Raft
membership, creates the root namespace, repairs marker mismatch, or creates a
missing marker.

Workers have no format command. `worker start` initializes missing or empty
local worker storage, creates stable worker identity and `WorkerStorageInfo`,
and creates the configured group directory. It refuses unknown non-empty local
storage and refuses to recreate or rewrite identity for existing worker storage.

HTTP `/ready` and `/health` endpoints are not implemented. Root readiness uses
the existing metadata readiness watcher.

## Observability

`common/src/observe` owns observability initialization, the metrics
recorder/exporter, the Prometheus `/metrics` endpoint, and tracing/logging
subscriber setup. Metadata, worker, and client own their own signal names and
metric/log/span emission.

The supported metrics exporter is Prometheus. `observe.metrics.prometheus.bind`
controls the `/metrics` listener bind address and `observe.metrics.prometheus.path`
controls the HTTP path. `metadata.http.bind` and `worker.http.bind` are parsed
and logged as reserved HTTP/admin bind fields, but no current listener uses them
and they do not control `/metrics`. `metadata format` does not initialize metrics
or bind the Prometheus endpoint. Trace/log output uses the local tracing
subscriber. OTLP export and OpenTelemetry SDK export pipelines are unsupported
now. Client observability is partial and does not install a client-owned
exporter. `docs/observability.md` records the current observability contract.

## Identity Model

Metadata group identity is the stable string `GroupName`, not a numeric group
id. Group names are validated as `[a-z0-9][a-z0-9._-]{0,62}` after trimming.
Uppercase letters, spaces, slash, empty strings, and names longer than 63 bytes
are rejected.

Group rename is not supported. Changing `metadata.group.name` means a
different metadata group and fails against existing formatted metadata storage
when the marker was written with another name. `worker.metadata.group.name`
selects the worker-local group data path under `groups/<group_name>/`; changing
it means a different group identity and a different group-scoped local path.
Worker root `WorkerStorageInfo` is cluster/worker scoped and does not currently
lock the root to one metadata group.

## Active Metadata Keys

| key | default | validation | notes |
| --- | --- | --- | --- |
| `vecton.cluster.id` | `local-vecton` | non-empty string | Shared cluster identity validated against formatted metadata and worker storage. |
| `metadata.storage.dir` | `data/metadata` | non-empty path string | Metadata RocksDB, Raft state, and format marker root. |
| `metadata.group.name` | `root` | valid `GroupName` | Stable metadata group identity. |
| `metadata.raft.mode` | `single` | `single` or `cluster` | `single` is supported. `cluster` parses but start/format fail because real Raft networking is not implemented yet. |
| `metadata.raft.node_id` | `1` | positive integer | Local Raft node id for the formatted metadata node. |
| `metadata.rpc.addr` | `0.0.0.0` | valid socket host with port below | Metadata RPC bind host. |
| `metadata.rpc.port` | `18080` | `1..=65535` | Metadata RPC bind port. |
| `metadata.http.bind` | `0.0.0.0:18081` | valid socket address | Parsed/logged reserved HTTP/admin bind. No current listener uses it; it does not control `/metrics`. Cleanup candidate. |
| `metadata.authz.filesystem.mode` | `NONE` | `NONE`, `RANGER`, or `ACL` | Current runnable deployments use `NONE`. |
| `metadata.bootstrap.root_ready_initial_backoff_ms` | `200` | positive integer | Root readiness initial backoff. |
| `metadata.bootstrap.root_ready_max_backoff_ms` | `5000` | positive integer | Root readiness maximum backoff. |
| `metadata.bootstrap.root_ready_warn_after_ms` | `60000` | positive integer | Root readiness warning threshold. |
| `metadata.bootstrap.ready.timeout_ms` | `120000` | positive integer | Root readiness timeout. |
| `metadata.bootstrap.ready.warn_after_ms` | `60000` | positive integer | Alternate current readiness warning key. |
| `metadata.bootstrap.ready.fail_fast` | `false` | boolean | If true, readiness timeout exits the metadata process. |
| `metadata.repair.max_queue_size` | `10000` | positive integer | Repair queue capacity. |
| `metadata.repair.max_attempts` | `3` | positive integer fitting `u32` | Repair retry limit. |
| `metadata.repair.inflight_timeout_ms` | `300000` | positive integer | Repair in-flight timeout. |
| `metadata.repair.initial_backoff_ms` | `1000` | positive integer | Repair retry initial backoff. |
| `metadata.repair.max_backoff_ms` | `60000` | positive integer | Repair retry maximum backoff. |
| `metadata.repair.worker_inflight_limit` | `4` | positive integer | Per-worker repair in-flight limit. |

## Active Worker Keys

| key | default | validation | notes |
| --- | --- | --- | --- |
| `vecton.cluster.id` | `local-vecton` | non-empty string | Validated against `WorkerStorageInfo` on restart. |
| `worker.identity.path` | `data/worker/worker.identity` | path string | Created only for missing or empty worker storage; loaded only for existing `WorkerStorageInfo`. |
| `worker.store.dirs.<dir_id>.path` | required | non-empty path string; `<dir_id>` is a non-empty key segment | Worker local store directory path. Group data lives under each configured directory. |
| `worker.store.dirs.<dir_id>.tier` | required | `MEM`, `NVME`, `SSD`, or `HDD` | Worker local store directory tier. |
| `worker.store.dirs.<dir_id>.capacity` | required | positive byte size | Worker local store directory capacity limit. |
| `worker.store.reserve_space` | `1GB` | non-negative byte size | Per-mount filesystem free-space reserve for local store admission. |
| `worker.store.selection_policy` | `round_robin` | exactly `round_robin` | Worker-local store directory selection policy. `balanced` is future work only. |
| `worker.store.check_interval_ms` | `30000` | positive integer | Filesystem capacity refresh interval. |
| `worker.rpc.bind` | `0.0.0.0:9090` | valid socket address | Worker data-plane gRPC listener. |
| `worker.rpc.advertised_endpoint` | required in file configs | valid URI with host and port | Endpoint registered with metadata and returned to clients. |
| `worker.http.bind` | `0.0.0.0:19091` | valid socket address | Parsed/logged reserved HTTP/admin bind. No current listener uses it; it does not control `/metrics`. Cleanup candidate. |
| `worker.rpc.max_inflight` | `100` | positive integer | Per-connection concurrency limit. |
| `worker.default_frame_size` | `1MB` | positive bytes and <= max frame size | Default transport frame payload. |
| `worker.max_frame_size` | `4MB` | positive bytes | Maximum transport frame payload. |
| `worker.window_bytes` | `8MB` | positive bytes | Per-stream application in-flight window. |
| `worker.stream.idle_timeout_ms` | `60000` | positive integer | Runtime stream idle timeout. |
| `worker.metadata.group.name` | `root` | valid `GroupName` | Metadata group name used for registration, heartbeat, and block report. |
| `worker.metadata.endpoints` | `http://127.0.0.1:18080` | comma-separated valid endpoint URIs | Metadata endpoints for registration, heartbeat, and block report. |
| `worker.metadata.register_timeout_ms` | `5000` | positive integer | Register RPC timeout; heartbeat currently reuses this RPC timeout. |
| `worker.metadata.register_retry_initial_backoff_ms` | `200` | positive integer | Register retry initial backoff. |
| `worker.metadata.register_retry_max_backoff_ms` | `5000` | positive integer >= initial backoff | Register retry maximum backoff. |

## Active Client Keys

| key | default | validation | notes |
| --- | --- | --- | --- |
| `client.name` | `default_client` | non-blank string | Low-cardinality client display identity for diagnostics and audit. |
| `client.metadata.endpoints` | `127.0.0.1:18080` | at least one endpoint | Comma-separated metadata endpoint list. |
| `client.metadata.group.names` | `root` | one or more valid `GroupName` values | Paired with metadata endpoints by position. |
| `client.retry.max_retry_attempts` | `3` | non-negative integer | Logical operation retry limit. |
| `client.retry.metadata_budget` | `3` | non-negative integer | Metadata retry budget, capped by max attempts. |
| `client.retry.worker_budget` | `3` | non-negative integer | Worker retry budget, capped by max attempts. |
| `client.retry.session_barrier_budget` | `0` | non-negative integer | Write-session barrier retry budget. |
| `client.refresh.max_attempts` | `3` | non-negative integer | Refresh attempt limit. |
| `client.operation.timeout_ms` | `null` | null or non-negative integer | Per-operation deadline; null disables it. |
| `client.backoff.initial_ms` | `100` | non-negative integer | Retry initial backoff. |
| `client.backoff.max_ms` | `5000` | non-negative integer >= initial | Retry maximum backoff. |
| `client.backoff.multiplier` | `2.0` | finite number >= 1.0 | Retry backoff multiplier. |
| `client.cache.worker_endpoint.enabled` | `false` | boolean | Metadata-authoritative worker endpoint cache. |
| `client.cache.worker_endpoint.ttl_secs` | `0` | non-negative integer | Cache TTL; zero means immediate expiry. |
| `client.cache.worker_endpoint.max_entries` | `1024` | positive when cache enabled | Worker endpoint cache capacity. |
| `client.cache.worker_endpoint.health.enabled` | `true` | boolean | Temporary endpoint penalty switch. |
| `client.cache.worker_endpoint.health.failure_threshold` | `2` | positive when health enabled | Consecutive failure threshold. |
| `client.cache.worker_endpoint.health.ttl_secs` | `5` | non-negative integer | Endpoint penalty TTL. |
| `client.channel_pool.metadata.enabled` | `true` | boolean | Metadata channel pool switch. |
| `client.channel_pool.metadata.max_per_group` | `1` | positive integer | Max metadata channels per group. |
| `client.channel_pool.worker.enabled` | `true` | boolean | Worker channel pool switch. |
| `client.channel_pool.worker.max_per_worker` | `1` | positive integer | Max worker channels per worker. |

## Unsupported Behavior

Compatibility keys from earlier lifecycle designs are not supported by the
current metadata/worker startup model. The current model is intentionally
command-driven: metadata has explicit format and start commands, while worker
startup is responsible only for safe empty-storage initialization and normal
restart validation.

Real multi-node Raft networking, multi metadata group runtime, unified Vecton
CLI commands, and HTTP readiness or health endpoints are not implemented.
