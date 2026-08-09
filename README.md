# Beryl

## Overview

Beryl is a Rust-based distributed storage/cache layer for big data and AI workloads. It uses a metadata control plane to manage namespace, file layout, data visibility, and worker-resident data. Clients coordinate metadata RPCs with worker data RPCs so visible data is served through metadata-authorized workers.

## Why Beryl?

- Large data systems need metadata paths that can scale with file count, namespace activity, and data-plane parallelism.
- Centralized metadata can become a bottleneck when all namespace, layout, and visibility decisions converge on one authority.
- Beryl explores a metadata-authorized architecture where metadata owns visibility and workers execute the data plane.
- The long-term direction is mount-level metadata sharding.

## Core Semantics

- Metadata is the authority for namespace, file layout, and data visibility.
- Workers store and serve blocks authorized by metadata.
- Data made visible by metadata is Beryl resident data.
- Namespace delete removes the metadata namespace entry and visible layout; physical blocks are reclaimed asynchronously after the configured cleanup grace.
- Metadata dispatches report-derived cleanup commands through worker heartbeats by default; workers fence readers, validate block stamps, and reclaim exact local block versions with crash recovery.
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
  - `beryl-types`, `beryl-common`, and `beryl-proto` provide stable domain values, shared infrastructure, and wire contracts.

## Current Status

- The current runtime is focused on one metadata group.
- The current metadata runtime uses one leader.
- The Rust native client is the client interface used today.
- Reads and writes currently go through metadata-authorized worker storage.
- Worker registration, heartbeat, and full block-report convergence are active runtime paths.
- `route_epoch`, `mount_epoch`, and `GroupStateWatermark` are active freshness checks.
- UFS is present as an adapter boundary, but current reads and writes do not use it.
- Multi-group metadata is future work.
- The internal writable namespace is rooted at `/`; `/local` has no special namespace semantics.

## Internal Alpha Contract

- The first internal release target is `v0.1.0-alpha.1`.
- Metadata, Worker, and the Rust Client must come from the same release artifact.
- The first alpha requires clean Metadata and Worker storage. Untagged development data is not migrated or supported.
- Same-version stop, start, restart, and recovery are supported.
- Mixed-version clusters, upgrade, downgrade, rollback, and cross-version storage compatibility are not supported.
- The released runtime is the current single-Metadata resident-storage path; it does not provide UFS read-through, Metadata HA, or replication.

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
- Namespace delete and worker-side physical reclamation are active. Cleanup dispatch is enabled by default and can be disabled through metadata configuration.
- UFS remains an adapter boundary; active UFS read-through/write-through is future work.
- Admin and metadata-peer schemas are not active runtime services.
- Multi-group metadata, multiple metadata leaders, and metadata peer RPC are future work.
- Worker peer transfer and alternate worker transports such as QUIC or RDMA are future work.
- Lost-worker cleanup records affected blocks but does not schedule replication, repair, or rebalancing.
- POSIX, FUSE, and Hadoop compatibility are not implemented.

## Roadmap

- See [ROADMAP.md](ROADMAP.md) for the detailed product, architecture, quality,
  refactoring, validation, and delivery roadmap.
- See [RELEASING.md](RELEASING.md) for the internal tag, build, acceptance, and
  publication procedure.
- Keep the supported Rust client -> metadata -> worker path stable under default validation.
- Complete recovery soak and controlled default enablement for resident-block reclamation.
- Design UFS read-through/write-through integration without changing metadata-owned visibility.
- Design multi-group metadata, metadata peer RPC, admin APIs, and ecosystem compatibility as future product work.
- Design replication, repair, and rebalancing only as complete future lifecycles.

## Crates

- `beryl-cli`
  - Public `beryl` command, installed layout resolution, and package-internal
    process routing.
  - Does not link Metadata or Worker runtime policy.
- `beryl-types`
  - Stable domain and value types shared by crates used in the current runtime.
  - Includes IDs, layout values, block values, epochs, and watermarks at the domain level.
- `beryl-common`
  - Shared infrastructure for errors, headers, config loading, retry/time helpers, and observability.
  - Does not own product behavior.
- `beryl-proto`
  - Protobuf/gRPC contracts and generated Rust bindings.
  - Covers the current metadata filesystem, metadata-worker control, and worker data services.
  - Admin and metadata-peer schemas are future/schema-only, not active runtime services.
- `beryl-metadata`
  - Namespace, layout, visibility, lease, worker registry, block location, freshness, and Raft/RocksDB authority.
  - Multi-group metadata remains future work.
  - Lost-worker cleanup retains the explicit boundary where a future repair lifecycle may begin.
- `beryl-worker`
  - Local block storage and metadata-authorized data-plane execution.
  - Does not own namespace visibility or file layout decisions.
  - Uses the current gRPC data service and worker-local filesystem storage path.
- `beryl-client`
  - Rust native API and orchestration for metadata and worker RPCs.
  - Does not provide POSIX, FUSE, or Hadoop compatibility today.
- `beryl-ufs`
  - External backend adapter boundary.
  - Current reads and writes do not use it.

## Quick Start

Beryl requires local metadata and worker configuration. The repository provides one local-ready default profile.

For an extracted Linux release package, follow [OPERATIONS.md](OPERATIONS.md).
The packaged `install.sh` performs a validated clean installation but does not
format persistent storage or start services.

Development checks:

```bash
make fmt
make verify
```

`make verify` runs the workspace format check, metadata check, compile check, clippy, and tests.

Validate and format an installed one-Metadata, one-Worker deployment once:

```bash
bin/beryl validate-conf
bin/beryl format metadata
```

Then run the two foreground processes from separate systemd units or terminals:

```bash
# Metadata unit or terminal
bin/beryl metadata

# Worker unit or terminal
bin/beryl worker
```

The installed CLI resolves `conf/metadata.yaml` and `conf/worker.yaml` relative
to the archive root. Deployments use one explicit override, for example
`beryl --conf-dir /etc/beryl metadata`. Source-checkout development may invoke
the package-internal role binaries with explicit `start`/`format` and
`--config <file>` arguments.

The client reads `conf/client.yaml`.

Run the Rust Client CRUD example from the same checkout or release tag as the
running Metadata and Worker:

```bash
cargo run --locked -p beryl-client --example crud -- conf/client.yaml
```

The example creates, writes, stats, reads, verifies, and deletes one multi-block
file through the public Rust Client API. See
[beryl-client/README.md](beryl-client/README.md#runnable-crud-example).

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
