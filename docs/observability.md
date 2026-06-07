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
- `metadata::observe` owns metadata metric and event names.
- `worker::observe` owns worker metric and event names.
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
observe.log.level: "info,vecton=info,metadata=info,worker=info,common=info,openraft=warn,tonic=warn,tower=warn,h2=warn"
observe.metrics.prometheus.bind: "127.0.0.1:18081"
observe.metrics.prometheus.path: "/metrics"
```

Worker:

```yaml
observe.log.format: compact
observe.log.output: stderr
observe.log.level: "info,vecton=info,metadata=info,worker=info,common=info,openraft=warn,tonic=warn,tower=warn,h2=warn"
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
