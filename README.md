# Vecton

## Overview

Vecton is a Rust-based distributed storage/cache layer for big data and AI workloads. It uses a metadata control plane to manage namespace, file layout, data visibility, and worker-resident data. Clients coordinate metadata RPCs with worker data RPCs so visible data is served through metadata-authorized workers.

## Why Vecton?

- Large data systems need metadata paths that can scale with file count, namespace activity, and data-plane parallelism.
- Centralized metadata can become a bottleneck when all namespace, layout, and visibility decisions converge on one authority.
- Vecton explores a metadata-authorized architecture where metadata owns visibility and workers execute the data plane.
- The long-term direction is mount-level metadata sharding.

## Core Semantics

- Metadata is the authority for namespace, file layout, and data visibility.
- Workers store and serve blocks authorized by metadata.
- Data made visible by metadata is Vecton resident data.
- Namespace delete removes the metadata namespace entry and visible layout.
- Physical resident-block reclamation is not a completed product lifecycle unless explicitly implemented and tested.
- External storage integration is adapter-only today, not the active read/write path.

## Architecture

- Client
  - Exposes the Rust native API.
  - Orchestrates metadata RPCs and worker data RPCs.
- Metadata
  - Owns namespace, layout, visibility, leases, worker registry, block locations, freshness, and Raft/RocksDB-backed authority.
  - Issues the context required before workers serve data.
- Worker
  - Stores local blocks and executes read/write streams.
  - Handles block commit, abort, sync, heartbeat, and block reports.
- External storage / UFS boundary
  - Provides the adapter boundary for external backends.
  - Current reads and writes do not use it.
- Shared crates
  - `types`, `common`, and `proto` provide stable domain values, shared infrastructure, and wire contracts.

## Current Status

- The current runtime is focused on one metadata group.
- The current metadata runtime uses one leader.
- The Rust native client is the client interface used today.
- Reads and writes currently go through metadata-authorized worker storage.
- Worker registration, heartbeat, and full block-report convergence are active runtime paths.
- `route_epoch`, `mount_epoch`, and `GroupStateWatermark` are active freshness checks.
- Unsupported legacy, admin, peer, and cluster-mode config keys are rejected rather than treated as compatibility aliases.
- UFS is present as an adapter boundary, but current reads and writes do not use it.
- Multi-group metadata is future work.
- `/local` is the current local development namespace.

## What Works Today

- Metadata format/start lifecycle and gRPC filesystem service.
- Worker registration, heartbeat, and block reports.
- Rust client APIs for core file operations including status, non-recursive list, mkdirs, namespace delete, rename, open, create, append, read, write, sync, close, and abort.
- Metadata-authorized worker reads and writes.
- Metadata restart fail-closed behavior for active writes.
- Worker restart with full-report convergence for valid Ready blocks.
- Precise unavailable-block and stale-location errors for visible blocks without usable live replicas.
- Structured error and proto contracts for current metadata and worker paths.
- Default and local development configuration.

## Current Boundaries and Gaps

- Recursive listing is not supported; metadata rejects recursive list requests.
- Namespace delete is active, but complete worker-side physical block free is not complete.
- UFS remains an adapter boundary; active UFS read-through/write-through is future work.
- Admin and metadata-peer schemas are not active runtime services.
- Multi-group metadata, multiple metadata leaders, and metadata peer RPC are future work.
- Worker peer transfer and alternate worker transports such as QUIC or RDMA are future work.
- Maintenance internals exist for safety and cleanup, but complete replication, repair, or rebalancing is not productized behavior.
- POSIX, FUSE, and Hadoop compatibility are not implemented.

## Roadmap

- Keep the supported Rust client -> metadata -> worker path stable under default validation.
- Finish resident-block reclamation and worker delete lifecycle only with explicit design and tests.
- Design UFS read-through/write-through integration without changing metadata-owned visibility.
- Design multi-group metadata, metadata peer RPC, admin APIs, and ecosystem compatibility as future product work.
- Treat complete replication, repair, and rebalancing as future lifecycle features, separate from current maintenance internals.

## Crates

- `types`
  - Stable domain and value types shared by crates used in the current runtime.
  - Includes IDs, layout values, block values, epochs, and watermarks at the domain level.
- `common`
  - Shared infrastructure for errors, headers, config loading, retry/time helpers, and observability.
  - Does not own product behavior.
- `proto`
  - Protobuf/gRPC contracts and generated Rust bindings.
  - Covers the current metadata filesystem, metadata-worker control, and worker data services.
  - Admin and metadata-peer schemas are future/schema-only, not active runtime services.
- `metadata`
  - Namespace, layout, visibility, lease, worker registry, block location, freshness, and Raft/RocksDB authority.
  - Multi-group metadata remains future work.
  - Maintenance internals do not make repair/rebalance a completed product behavior.
- `worker`
  - Local block storage and metadata-authorized data-plane execution.
  - Does not own namespace visibility or file layout decisions.
  - Uses the current gRPC data service and worker-local filesystem storage path.
- `client`
  - Rust native API and orchestration for metadata and worker RPCs.
  - Does not provide POSIX, FUSE, or Hadoop compatibility today.
- `ufs`
  - External backend adapter boundary.
  - Current reads and writes do not use it.

## Quick Start

Vecton requires local metadata and worker configuration. The repository provides default and local debug profiles.

Development checks:

```bash
make fmt
make verify
```

`make verify` runs the workspace format check, metadata check, compile check, clippy, and tests.

Default one-metadata, one-worker startup:

```bash
metadata format --config conf/metadata.yaml
metadata start --config conf/metadata.yaml
worker start --config conf/worker.yaml
```

Local one-metadata, one-worker startup:

```bash
metadata format --config conf/local/metadata.yaml
metadata start --config conf/local/metadata.yaml
worker start --config conf/local/worker.yaml
```

The client reads `conf/client-site.yaml` or `conf/local/client-site.yaml` depending on caller configuration.

## Non-goals for Current Scope

- Alluxio full feature parity.
- Production-ready multi-group metadata.
- Multiple metadata leaders.
- Metadata peer RPC.
- Admin API.
- POSIX compatibility.
- FUSE.
- UFS-backed cache read/write path.
- Replication, repair, or rebalancing as completed user-facing behavior.
- Alternate transports such as QUIC or RDMA.
- Worker peer transfer.
- io_uring or SPDK worker runtime support.

## License

Apache-2.0. See `LICENSE`.
