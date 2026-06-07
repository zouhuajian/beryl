# Observability Architecture

## Goals

- Local human-readable compact logs.
- Production flat JSON logs.
- Prometheus metrics.
- Trace context propagation through Vecton headers.
- Config-file-only observe config.
- Minimal observe config surface.

## Current Architecture

- `common::observe` owns shared observability mechanisms: config parsing, logging setup, subscriber initialization, Prometheus recorder installation, and `/metrics` serving.
- `metadata::observe` owns metadata metric names and emission helpers.
- `worker::observe` owns worker metric names and emission helpers.
- `common::header` owns `ClientInfo`, `call_id`, and trace context domain values. `TraceContextProto` is the only wire container for trace propagation.
- `scripts/observability_smoke.sh` verifies metadata and worker startup metrics plus worker registration, heartbeat, and block report counters.
- Default metadata and worker startup use separate configs: `conf/metadata.yaml` and `conf/worker.yaml`.
- Local metadata and worker startup use separate configs: `conf/local/metadata.yaml` and `conf/local/worker.yaml`.

## Supported Now

- `observe.log.format`
- `observe.log.output`
- `observe.log.level`
- `observe.metrics.prometheus.bind`
- `observe.metrics.prometheus.path`
- Compact and JSON logging.
- Prometheus `/metrics`.
- `TraceContextProto` propagation.
- Metadata worker registration, heartbeat, and block report metric smoke coverage.
- Operational state-change logs for metadata and worker write paths.

## Unsupported Now

- Env observe config.
- Per-observe CLI overrides.
- OTLP exporter.
- OpenTelemetry SDK export pipeline.
- Admin dynamic log-level API.
- Dashboard.
- Collector deployment.
- Data-plane CLI smoke, until a checked-in CLI or test client exists.

## Future Extension Plan

- A global config override framework can be designed later.
- An OTLP exporter can be designed later.
- Admin log-level updates can be added later.
- Data-plane smoke can be upgraded when a checked-in CLI or test client exists.
- Dashboards and alerts can be added after metrics stabilize.

## Config Example

Metadata:

```yaml
observe.log.format: compact
observe.log.output: stderr
observe.log.level: "info,metadata=info,metadata.state=info,metadata.block=info,metadata.worker=info,worker=info,worker.state=info,worker.block=info,common=info,openraft=warn,tonic=warn,tower=warn,h2=warn"
observe.metrics.prometheus.bind: "127.0.0.1:18081"
observe.metrics.prometheus.path: "/metrics"
```

Worker:

```yaml
observe.log.format: compact
observe.log.output: stderr
observe.log.level: "info,metadata=info,metadata.state=info,metadata.block=info,metadata.worker=info,worker=info,worker.state=info,worker.block=info,common=info,openraft=warn,tonic=warn,tower=warn,h2=warn"
observe.metrics.prometheus.bind: "127.0.0.1:19091"
observe.metrics.prometheus.path: "/metrics"
```

`metadata.http.bind` and `worker.http.bind` are parsed and logged reserved
HTTP/admin bind fields. They do not control the Prometheus `/metrics` listener.

## Metrics Rules

- Counters end with `_total`.
- Gauges do not end with `_total`.
- Durations use `_duration_seconds` and record seconds.
- Bytes use `_bytes` and record bytes.
- Labels must stay low-cardinality.
- Labels must not include paths, storage directories, block IDs, stream IDs, request IDs, trace IDs, span IDs, worker run IDs, client IDs, users, tokens, secrets, authorization data, cookies, credentials, or raw error messages.

## Trace Rules

- `call_id` is the application-level call correlation ID.
- `TraceContextProto` carries `traceparent`, `tracestate`, and `baggage`.
- The legacy request ID field is not used because `call_id` covers request correlation semantics.
- `trace_id` is not a propagated protocol field.
- Baggage must not carry secrets, credentials, auth tokens, API keys, cookies, raw user IDs, or other PII.

## State-Change Logs

State-change logs are operational logs for semantic metadata and worker state transitions. They are emitted with direct `tracing::info!` or `tracing::warn!` calls at the state-change callsite where the final outcome is known. They are not per-RPC liveness logs, request-level audit logs, or persisted records in Raft, RocksDB, or worker metadata stores, and their text or field layout is not a stable external API.

Normal accepted heartbeats are intentionally not logged at `info` level. A successful heartbeat is a high-frequency liveness tick; capacity, available bytes, load, and heartbeat lag belong to metrics. Heartbeat-related logs should be reserved for semantic changes or abnormal outcomes such as `NeedRegister`, `WorkerRunMismatch`, expiry/dead/stale transitions, recovery from those states, or non-empty worker commands returned by metadata. Repeated identical heartbeat rejections are suppressed by group, worker, worker run, and rejection reason; a new worker run or a different reason emits a new `warn`.

Accepted block report summaries are `info` only when the report changes block state (`changed_block_count > 0`) or a final full report establishes or replaces the worker's baseline. Repeated no-change full reports and zero-change delta reports update metrics and protocol state without an `info` summary.

Targets:

- `metadata.state`: metadata namespace and write-session state changes such as CreateFile, CommitFile, AbortFileWrite, Delete, Rename, and RenewLease.
- `metadata.block`: metadata-side block allocation and change-based block report summaries.
- `metadata.worker`: metadata-side worker registration, abnormal heartbeat outcomes, and rejected worker reports.
- `worker.state`: worker write lifecycle transitions such as OpenWrite and CommitWrite.
- `worker.block`: worker block lifecycle transitions such as staging block creation and publish_ready.

Large target or worker lists must be logged as counts plus a small sample. Operational state-change logs must not dump full RPC requests or Debug-format request fields such as headers, auth context, extensions, write handles, fencing tokens, file attrs, committed block lists, frame bytes, user data, or token secrets. Successful state changes use `info`; expected recoverable rejections use `warn`; internal invariants, corruption, fatal local IO, and unrecoverable failures use `error`.

Example JSON lines:

```json
{"target":"metadata.state","level":"INFO","op":"CreateFile","result":"committed","error_code":"none","client_id":"0x0000000000000000000000000000002b","call_id":"7cc74b80-1240-40f2-9e23-0f75fb8a8ed4","path":"/datasets/a.bin","inode_id":42,"data_handle_id":42,"file_handle":7,"layout_block_size":134217728,"layout_chunk_size":1048576,"replication":1,"desired_len":268435456,"mount_version":1,"route_epoch":3,"message":"CreateFile committed"}
{"target":"metadata.block","level":"INFO","op":"AddBlock","result":"allocated","error_code":"none","client_id":"0x0000000000000000000000000000002b","call_id":"7cc74b80-1240-40f2-9e23-0f75fb8a8ed4","block_id":"42:0","block_index":0,"group_id":"root","desired_len":134217728,"target_count":1,"targets_sample":[1],"data_handle_id":42,"file_handle":7,"mount_version":1,"route_epoch":3,"message":"AddBlock allocated"}
{"target":"metadata.state","level":"INFO","op":"CommitFile","result":"committed","error_code":"none","client_id":"0x0000000000000000000000000000002b","call_id":"7cc74b80-1240-40f2-9e23-0f75fb8a8ed4","data_handle_id":42,"file_handle":7,"final_size":134217728,"committed_block_count":1,"committed_bytes":134217728,"file_version":9,"mount_version":1,"route_epoch":3,"message":"CommitFile committed"}
{"target":"metadata.block","level":"INFO","op":"DeltaBlockReport","result":"processed","error_code":"none","report_kind":"delta","client_id":"0x0000000000000000000000000000002b","call_id":"7cc74b80-1240-40f2-9e23-0f75fb8a8ed4","group_name":"root","worker_id":1,"worker_run_id":"550e8400-e29b-41d4-a716-000000000001","report_seq":7,"delta_seq":12,"next_delta_seq":13,"added_blocks":1,"removed_blocks":0,"changed_block_count":1,"message":"Delta block report processed"}
```

Audit logging is future work. It will need a separate design for durability, policy, redaction, actor identity, and retention.
