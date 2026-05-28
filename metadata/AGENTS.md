# `metadata` Agent Instructions

This file applies to `metadata/`. Follow the root `AGENTS.md` first; this crate is the namespace and control-plane authority.

## Scope

`metadata` owns:

- inode, dentry, attrs, file layout, leases, route ownership, mount routing, and namespace semantics
- `FileSystemService` as the external path-first entrypoint
- `FsCore` as the internal semantic core for filesystem authority, freshness, session, and fencing behavior
- guard/handler orchestration, metadata typed config, and authority-side DTOs that are not stable shared contracts
- worker membership, block reports, maintenance routing, and metadata-side placement decisions
- Raft/RocksDB state machine apply, replay, snapshots, and persisted authoritative state

`metadata` must not own worker data execution, worker store layout, chunk IO, stream runtime, client retry/cache/SDK policy, UFS backend behavior, proto schema ownership, duplicated shared conversion, or stable domain definitions that belong in `types`.

## Local Rules

- Path is an adapter into inode/dentry authority, not a competing source of truth.
- Guard and handler layers should stay thin: readiness, data-IO policy, leadership, authz, path resolution, proto conversion, call `FsCore`, build response.
- Keep authz modes non-compositional where the design requires a single effective mode.
- Business errors should follow the project structured response-header error contract.
- Raft apply mutations must preserve atomicity, idempotence where replay requires it, and persisted replay semantics.
- Do not use runtime-only mutations for authoritative state that must survive Raft/RocksDB replay.
- Worker identity, registration, liveness, routing, placement, reports, delete, and repair state are group-scoped. Do not infer a metadata group from `WorkerId`, fall back to group 1, or perform any-group lookup in production code.
- Stable cross-module returned DTOs should use `types` domain models and shared `proto` conversion instead of metadata-local shadow models.

## Tests

- Test inode/dentry/path traversal, mount routing, child-over-parent mounts, same-mount rename atomicity, route/mount/state freshness, not-leader refresh, lease/write-session behavior, persistence, snapshots, and replay where relevant.
- Assert structured error class/reason fields when structured errors exist.
- Use integration tests for observable client/metadata/worker behavior; keep authority-policy tests in `metadata`.

## Local Self-Review

Apply the root self-review checklist, then check:

- Did inode/dentry/attrs/layout remain authoritative and path remain an adapter?
- Did service/guard code stay thin with semantics in `FsCore` or the Raft apply path?
- Did persisted authority avoid runtime-only state mutation?
- Did worker state remain group-scoped without `WorkerId`-only or group-1 fallbacks?
- Did structured business errors and freshness domains remain intact?
