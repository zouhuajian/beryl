# Vecton

Vecton is an inode-centric distributed storage and cache system with filesystem-facing semantics. Metadata owns inode, dentry, attrs, mount, lease, route, and state freshness. Workers own local block/chunk/stream execution. Clients orchestrate metadata and worker calls without owning metadata or worker runtime policy.

> Status: active development. Internal APIs, config keys, and on-disk formats may change.

## Development Baseline

Vecton is developed and verified with Rust 1.95.0. The repository pins that baseline in `rust-toolchain.toml`; keep `rustfmt` and `clippy` installed for this toolchain.

Local verification entrypoints:

```bash
make fmt
make verify
```

`make verify` runs the non-mutating baseline gates:

```bash
cargo fmt --all --check
cargo metadata --format-version 1 --no-deps
cargo check --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

## Current Baseline

Currently supported:

- Rust workspace build, metadata generation, lint, and unit/contract test gates.
- Single-group metadata runtime with inode/dentry/attrs authority, root mount bootstrap, Raft-backed state, write sessions, leases, and route/state freshness.
- Worker startup registration with MetadataWorkerService using stable `WorkerId` plus per-process `WorkerRunId`, followed by a gRPC data service for local block/chunk/stream read and write execution.
- MetadataWorkerService structured application-error response contract for worker registration.
- Client facade with metadata gateway, retry/refresh classification, read planning, worker endpoint validation, and sequential write handles.
- Default `conf/core-site.yaml` and `conf/client-site.yaml` containing only keys consumed by current runtime code.

Intentionally not yet implemented in this baseline:

- Production worker heartbeat and block-report loops from worker binary to metadata.
- WorkerDataService public `block_stamp=0` hardening.
- Raft network implementation for production multi-node metadata.
- QUIC, RDMA, io_uring, or SPDK production data paths.
- Expanded maintenance repair/delete features.
- Real metadata + worker + client end-to-end system tests.

Worker startup registration is group-scoped. The worker resolves a stable `WorkerId` from `worker.id` or the persisted `worker.identity.path`, generates a UUID `WorkerRunId` once per process start, registers the advertised gRPC endpoint with the configured metadata group leader, and serves data-plane requests for a group only after that group accepts the registration. Metadata persists only the stable worker descriptor; `WorkerRunId` is live registration state and is not restored from metadata restart or snapshot reload. `WorkerRunId` is not an epoch and is not comparable. Client-to-worker reads, writes, commits, and syncs use `WorkerRunId` for worker process-run freshness and `block_stamp` for block generation freshness.

## Workspace Crates

| Crate | Role | Owns |
| --- | --- | --- |
| `types` | Pure Rust domain model | Stable IDs and shared value objects such as worker endpoints, block locations, write targets, committed blocks, fencing tokens, block stamps, epochs, and watermarks. |
| `common` | Generic shared infrastructure | Canonical errors, request/response headers, config loading mechanics, observability primitives, and module-independent utilities. |
| `proto` | Wire schema and structural conversion | Protobuf files, generated Rust modules, gRPC service contracts, wire enum values, and proto/domain conversion. |
| `metadata` | Metadata authority runtime | Inode/dentry/attrs, mounts, leases, write sessions, Raft state, worker membership service, maintenance routing, and metadata config. |
| `worker` | Data-plane runtime | Local block store, chunk IO, stream runtime, data service adapters, worker networking, and worker config. |
| `client` | SDK and orchestration runtime | Public facade, metadata gateway, worker endpoint resolution/cache, channel pooling, retry/refresh orchestration, read planning, worker data-plane access, and client config. The client obtains authoritative layout, write targets, and read locations from metadata, validates read locations with `ReadPlanner`, and then accesses workers on the normal data path. Client-side read layout caching has been removed from the current architecture. |
| `ufs` | External backend adapter | Backend integration, OpenDAL setup, backend-specific config, UFS path behavior, and backend capability decisions. |
| `integration_tests` | Test-only contracts | End-to-end fixtures, mock servers, contract assertions, and raw proto wire checks. |

Dependency direction is one-way from product crates to shared crates. `types` must not depend on workspace crates; `common` may depend on `types` but not `proto` or product crates; `proto` may depend on `types` and `common`; production `metadata`, `worker`, `client`, and `ufs` must not depend on each other in production code.

## Configuration

Default configuration files are active baselines, not roadmaps:

- `conf/core-site.yaml` contains current metadata and worker runtime keys only.
- `conf/client-site.yaml` contains current client runtime keys only.
- Planned capabilities may be described in docs, but they must not appear as deployable defaults until code consumes and validates them.

See `docs/CONFIG_MATRIX_ZH.md` for the current key ownership and status matrix.

## License

Apache-2.0. See `LICENSE`.
