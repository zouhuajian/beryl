# Vecton Agent Instructions

`AGENTS.md` files are operational instructions for AI coding agents. Follow this file first, then the local `AGENTS.md` for every touched subtree.

## Project Boundary

- The runtime currently focuses on one metadata group.
- The current metadata runtime has one leader.
- The Rust native client is the supported client interface today.
- Reads and writes currently go through metadata-authorized worker storage.
- UFS is not used for current reads or writes.
- Multi-group metadata is future work.
- `/local` is a current local development namespace, not the product identity.

## Core Rules

- Do not claim unsupported features are implemented.
- Do not expand scope casually.
- Do not add placeholder abstractions to the current runtime.
- Do not implement or document multi-group metadata, UFS read/write paths, replication, repair, FUSE, POSIX, Hadoop compatibility, or alternate transports unless explicitly requested.
- Preserve crate ownership boundaries.
- Prefer small correctness-preserving changes.
- Do not perform broad refactors before behavior is covered by tests.
- Keep docs factual and concise.
- Keep Rust comments in English.

## Architecture Ownership

- `types`: stable domain and value types.
- `common`: shared errors, headers, config mechanics, retry/time helpers, and observability utilities.
- `proto`: protobuf/gRPC schema, generated bindings, and structural conversions.
- `metadata`: namespace, layout, visibility, leases/write sessions, worker registry, block locations, freshness, and Raft/RocksDB-backed metadata state.
- `worker`: local block storage, stream execution, block commit/abort/sync, registration, heartbeat, and block reports.
- `client`: Rust native API and metadata/worker RPC orchestration.
- `ufs`: external backend and adapter boundary.

Production dependency direction must stay clean:

- `client` must not production-depend on `metadata` or `worker`.
- `worker` must not production-depend on `metadata` or `client`.
- `metadata`, `worker`, and `client` should use `types`, `common`, and `proto` for shared contracts as appropriate.
- `ufs` must not depend on `metadata`, `worker`, or `client`.

## Current Priorities

- Non-ignored metadata + worker + client E2E coverage.
- Worker stream correctness.
- Worker block publish/recovery hardening.
- Metadata restart fail-closed behavior.
- Worker restart full-report convergence.
- Precise no-replica/block-location-unavailable behavior.

## Validation

```bash
cargo fmt --all -- --check
cargo check --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

For documentation-only edits, also run:

```bash
git diff --check
```

## Non-goals

- Alluxio full feature parity.
- Production-ready multi-group metadata.
- POSIX compatibility.
- FUSE.
- UFS-backed read/write data path.
- Replication, repair, or rebalancing as completed user-facing behavior.
- Alternate transports such as QUIC or RDMA.
