# Vecton

Vecton is an inode-centric distributed storage/cache system targeting HCFS (HDFS-like) semantics. It separates the data plane into **Block / StorageChunk / TransportFrame / Stream** layers and uses structured error semantics to provide predictable retry and self-healing behavior.

> Status: active development. APIs and on-disk formats may change.

## Key Concepts

- **Inode-centric metadata**: inode/dentry/attrs are authoritative; paths are for traversal only.
- **Block / StorageChunk / TransportFrame / Stream**
  - **Block**: the management, placement, replication, and lifecycle unit.
  - **StorageChunk**: the worker-local I/O, checksum, bitmap, and cache-fill unit.
  - **TransportFrame**: the network transfer batch/slice.
  - **Stream**: the continuous read/write session with flow control.
- **Client direct I/O**: metadata-routed client-to-worker data access is the target path, but client direct read/write wiring is intentionally deferred after transport removal and may return `Unimplemented` until the client data-plane refactor.
- **Canonical errors**: `OK / NEED_REFRESH / RETRYABLE / FATAL` to standardize retries and recovery.

## Architecture

```text
+--------------------+        +------------------------+
|       Client       |        |     Metadata Plane     |
|  (HCFS API, cache) | <----> | routing / raft / mounts|
+---------+----------+        +-----------+------------+
          |                               |
          | direct I/O (deferred)         | control-plane
          v                               v
+--------------------+        +------------------------+
|     Worker Plane   | <----> | maintenance / repair   |
| block/chunk/stream |        | scheduling (metadata)  |
| local store + xfer |        +------------------------+
+--------------------+
```

* **Client**: HCFS-style API, route cache, canonical retry (refresh loop).
* **Metadata**: path resolution, mount-based routing, Raft state machine, maintenance/repair scheduling.
* **Worker**: Block/StorageChunk/TransportFrame/Stream services, local persistence, replication/relocation.

## Build

Prerequisites: Rust (stable).

```bash
cargo build --workspace
cargo test  --workspace
```

## Workspace crates

| Crate | Role | Owns | Must not own |
| --- | --- | --- | --- |
| `common` | Shared infrastructure | Generic errors, request/response header domain types, config loading/flattening/env-key mapping, observability primitives, and module-independent utilities. | Metadata authority DTOs, worker store/runtime state, client retry/cache policy, UFS backend policy, generated proto types, or module-specific typed config. |
| `types` | Pure Rust domain model | Stable cross-module value objects, typed IDs, worker endpoints, file block locations, write targets, committed blocks, fencing/block/epoch/watermark helpers, and pure shared value validation. | Generated proto types, proto wire values, runtime policy, metadata internals, worker store state, client cache/replay policy, UFS internals, test fixtures, or placeholder abstractions. |
| `proto` | Wire schema and conversion | `.proto` files, generated Rust modules, gRPC service contracts, wire enum numeric values, structural proto/domain conversion, and schema-local codecs. | Business policy, retry/replay/cache decisions, metadata routing, worker runtime behavior, UFS behavior, or product-crate dependencies. |
| `metadata` | Product runtime: metadata authority | Inode/dentry/attrs authority, mount state, leases, write sessions, `FsCore`, Raft state machine, worker membership, maintenance routing, and metadata typed config. | Worker store execution, client cache/replay policy, UFS backend behavior, or duplicated structural proto/domain conversion already owned by `proto`. |
| `worker` | Product runtime: data plane | Local block store, chunk IO, checksum/repair execution, stream runtime, data service adapters, worker net server/client, and worker typed config. | Metadata authority policy, client cache/replay policy, generated schema ownership, or pushing store/runtime state into shared crates. |
| `client` | Product runtime: SDK and orchestration | SDK behavior, metadata gateway, layout cache, worker endpoint cache, retry/replay classification, planner behavior, data-plane adapter orchestration, and client typed config. | Metadata authority policy, worker store/runtime behavior, generic schema ownership, or long-lived raw proto state when a domain model exists. |
| `ufs` | External backend adapter | Backend integration, backend-specific config, OpenDAL setup, UFS path behavior, and backend capability decisions. | Metadata, worker, or client runtime policy; production helpers for unrelated crates; or dependencies on product runtime crates. |
| `integration_tests` | Test-only contracts | End-to-end fixtures, mock servers, cross-crate contract assertions, and raw proto checks for wire behavior. | Production helpers, canonical conversion code, or runtime abstractions used by product crates. |

### Dependency direction

Shared crates sit below product crates. `types` must stay independent of workspace crates. `common` may use `types`, but not `proto` or product crates. `proto` may use `types` and `common`, but not `metadata`, `worker`, `client`, or `ufs`. Production crates must not depend on each other in production code: `metadata`, `worker`, and `client` stay separated, and `ufs` does not depend on them. `integration_tests` may depend on production crates, but remains test-only.

Do not use `types` as a dumping ground for anything that merely appears in more than one place. Raw proto messages should stay near service or adapter boundaries and be converted before reaching long-lived business state. Runtime policy belongs to the owning module. `common` owns generic config loading mechanics; typed module config, defaults, and validation belong to the module that consumes them. Internal stale schemas and types should be deleted when they have no active use instead of wrapped in duplicate legacy APIs.

## License
Apache-2.0. See `LICENSE`.
