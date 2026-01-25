# Vecton

Vecton is an inode-centric distributed storage/cache system targeting HCFS (HDFS-like) semantics. It separates the data plane into **Block / Chunk / Stream** layers and uses structured error semantics to provide predictable retry and self-healing behavior.

> Status: active development. APIs and on-disk formats may change.

## Key Concepts

- **Inode-centric metadata**: inode/dentry/attrs are authoritative; paths are for traversal only.
- **Block / Chunk / Stream**
  - **Block**: the only management unit (placement, replication, repair, migration, reporting).
  - **Chunk**: the physical storage/IO unit (checksums, refill, relocation granularity).
  - **Stream**: the transport unit (framing, backpressure, zero-copy).
- **Client direct I/O**: clients read/write to workers directly after routing via metadata, with cache + refresh on mismatch.
- **Canonical errors**: `OK / NEED_REFRESH / RETRYABLE / FATAL` to standardize retries and recovery.

## Architecture

```text
+--------------------+        +------------------------+
|       Client       |        |     Metadata Plane     |
|  (HCFS API, cache) | <----> | routing / raft / mounts|
+---------+----------+        +-----------+------------+
          |                               |
          | direct I/O                    | control-plane
          v                               v
+--------------------+        +------------------------+
|     Worker Plane   | <----> | maintenance / repair   |
| block/chunk/stream |        | scheduling (metadata)  |
| local store + xfer |        +------------------------+
+--------------------+
````

* **Client**: HCFS-style API, route cache, canonical retry (refresh loop).
* **Metadata**: path resolution, mount-based routing, Raft state machine, maintenance/repair scheduling.
* **Worker**: Block/Chunk/Stream services, local persistence, replication/relocation.

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
* `transport`: network transport abstraction
* `meta`: metadata plane (Raft, mounts, FS API, maintenance/repair)
* `worker`: data plane (storage, replication/relocation)
* `client`: client library
* `cli`: CLI tooling (optional)

## License
Apache-2.0. See `LICENSE`.
