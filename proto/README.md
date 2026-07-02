# proto

## Role

`proto` owns Vecton's protobuf/gRPC wire contracts, generated Rust bindings, and structural conversion between wire messages and shared domain values.

## How It Fits Into Vecton

- Defines the service contracts used today between client, metadata, and worker processes.
- Converts generated proto values to `types` and `common` values at service boundaries.
- Keeps wire compatibility concerns separate from business policy.

## Main Responsibilities

- `.proto` files, generated Rust modules, service contracts, field numbers, and enum values.
- Metadata filesystem, metadata-worker control, and worker data service contracts.
- Structural proto/domain conversion helpers and wire-level comments.

## Current Active Use

The current runtime uses metadata filesystem RPCs for client-to-metadata operations, metadata-worker control RPCs for registration/heartbeat/block reports, and worker data RPCs for metadata-authorized reads and writes.

## Not in Current Scope

- Business policy or authority decisions.
- Retry, replay, cache, endpoint-health, or route-refresh policy.
- Worker storage/runtime behavior.
- Admin, peer, or shard-style proto contracts as production-ready multi-group metadata unless they are wired into the current runtime.

## Contributor Notes

- Treat schema changes as compatibility-sensitive.
- Do not reuse field numbers or silently change enum values.
- Keep generated types at boundaries and convert to domain types where available.
