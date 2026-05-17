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
````

* **Client**: HCFS-style API, route cache, canonical retry (refresh loop).
* **Metadata**: path resolution, mount-based routing, Raft state machine, maintenance/repair scheduling.
* **Worker**: Block/StorageChunk/TransportFrame/Stream services, local persistence, replication/relocation.

## Build

Prerequisites: Rust (stable).

```bash
cargo build --workspace
cargo test  --workspace
```

## Repository Layout (typical)

* `common`: utilities, canonical error model, metrics/tracing
* `types`: domain types (IDs, epochs, tokens)
* `proto`: RPC/Proto definitions and generated code
* `metadata`: metadata plane (Raft, mounts, FS API, maintenance/repair)
* `worker`: data plane (storage, worker-local net server/peer-client code, replication/relocation)
* `client`: client library
* `ufs`: external backend integration
* `integration_tests`: end-to-end contract validation

## License
Apache-2.0. See `LICENSE`.
