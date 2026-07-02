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
- Vecton should keep resident data durable until explicit delete/free or a documented recovery state.
- External storage integration is future work unless it is wired into the current read/write path.

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
- UFS is present as an adapter boundary, but current reads and writes do not use it.
- Multi-group metadata is future work.
- `/local` is the current local development namespace.

## What Works Today

- Metadata format/start lifecycle and gRPC filesystem service.
- Worker registration, heartbeat, and block reports.
- Rust client APIs for core file operations including status, list, mkdirs, delete, rename, open, create, append, read, write, sync, close, and abort.
- Metadata-authorized worker reads and writes.
- Structured error and proto contracts for current metadata and worker paths.
- Default and local development configuration.

## Known Gaps

- Non-ignored metadata + worker + client E2E coverage.
- Worker stream correctness hardening.
- Worker block publish/recovery hardening.
- Metadata restart behavior for in-flight writes.
- Worker restart/full-report convergence.
- Precise no-replica and block-location-unavailable behavior.
- UFS integration for reads and writes.

## Roadmap

- P0: stabilize the single-group core path.
- P1: mount-level metadata sharding.
- P2: UFS read-through/write-through integration.
- P3: replication, repair, and ecosystem integration.

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
- `metadata`
  - Namespace, layout, visibility, lease, worker registry, block location, freshness, and Raft/RocksDB authority.
  - Multi-group metadata remains future work.
- `worker`
  - Local block storage and metadata-authorized data-plane execution.
  - Does not own namespace visibility or file layout decisions.
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
- POSIX compatibility.
- FUSE.
- UFS-backed cache read/write path.
- Replication, repair, or rebalancing as completed user-facing behavior.
- Alternate transports such as QUIC or RDMA.

## License

Apache-2.0. See `LICENSE`.
