# Vecton Agent Instructions

`AGENTS.md` files are operational instructions for AI coding agents. Follow this file first, then the local `AGENTS.md` for every touched subtree.

## Project Boundary

- The runtime currently focuses on one metadata group.
- The current metadata runtime has one leader.
- The Rust native client is the supported client interface today.
- Reads and writes currently go through metadata-authorized worker storage.
- UFS is not used for current reads or writes.
- Namespace delete is active; complete physical resident-block reclamation is not productized unless explicitly implemented and tested.
- `route_epoch`, `mount_epoch`, and `GroupStateWatermark` are active correctness mechanisms, not future-only noise.
- Multi-group metadata is future work.
- The internal writable namespace is rooted at `/`; `/local` has no special namespace semantics.

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

- Keep non-ignored current-path E2E coverage green.
- Preserve worker stream/session, Ready publish/recovery, restart/full-report convergence, and precise unavailable-block semantics.
- Preserve metadata restart fail-closed behavior for active writes.
- Keep freshness and owner-group routing fields intact unless replacement invariants are designed and tested.
- Keep maintenance internals separate from productized repair/rebalance behavior.
- Keep unsupported config and runtime surfaces fail-closed.

## Validation

```bash
cargo fmt --all -- --check
cargo check --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test -p e2e_tests
cargo test --workspace
```

For documentation-only edits, also run:

```bash
git diff --check
```

## Non-goals

- Alluxio full feature parity.
- Production-ready multi-group metadata.
- Multiple metadata leaders.
- Metadata peer RPC.
- Admin API.
- POSIX compatibility.
- FUSE.
- UFS-backed read/write data path.
- Replication, repair, or rebalancing as completed user-facing behavior.
- Alternate transports such as QUIC or RDMA.
- Worker peer transfer.
- io_uring or SPDK worker runtime support.
