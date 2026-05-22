# `metadata` Agent Instructions

This file applies to `metadata/`. Follow the root `AGENTS.md` first, then these local rules.

## Crate role

`metadata` owns the authoritative filesystem metadata plane. It is the source of truth for inode/dentry/attrs semantics, mount ownership, leases, write sessions, worker membership, and Raft-backed metadata state.

## Allowed changes

- Filesystem metadata authority: inode, dentry, attrs, and path traversal as an adapter.
- Mount state, mount routing, namespace ownership, and route freshness.
- Lease and write-session authority.
- `FsCore` and server-side filesystem behavior.
- Raft state machine, snapshots, replay, and metadata persistence.
- Worker membership and metadata maintenance routing.
- Metadata typed config, defaults, and validation.
- Authority-side DTOs that are not stable shared contracts.

## Forbidden changes

- Worker store execution, chunk IO, stream runtime, checksum/repair execution, or worker-local path layout.
- Client retry, replay, cache, endpoint-health, or SDK policy.
- UFS backend behavior or backend capability policy.
- Protobuf schema ownership.
- Structural proto/domain conversion duplicated when `proto` already owns it.
- Stable cross-module domain definitions that belong in `types`.
- Path-as-authority indexes or caches that can outvote inode/dentry state.

## Dependency rules

- `metadata` may depend on shared crates where appropriate.
- `metadata` must not depend on `worker` or `client` in production code.
- Test-only dependencies must stay explicit and narrow.
- Stable cross-module returned DTOs should use `types` domain models and `proto` conversion instead of metadata-local shadow models.

## Conversion and validation rules

- Authority policy stays local: owner group, route freshness, lease renewal, block allocation, metadata publication, delete/repair/GC scheduling, and write-session planning.
- Structural proto/domain conversion should be called from `proto` helpers when shared.
- Metadata validates namespace, inode/dentry/attrs, mount ownership, route epoch, mount epoch, state watermark, leases, and write-session semantics.
- Recoverable authority failures must be machine-usable through structured response errors.
- Follower successful responses must not advance client state cache.
- `applied_seq` must not be reintroduced as runtime, storage, snapshot, header, or client state.

## Testing guidance

- Add or update tests for inode/dentry/path traversal, mount routing, child-over-parent mounts, same-mount rename atomicity, stale route/mount/state behavior, not-leader refresh, lease/write-session behavior, persistence, and replay where relevant.
- Assert structured error class/reason fields when structured errors exist.
- Prefer focused metadata tests for authority policy; use integration tests for cross-crate behavior.

## Documentation guidance

- Update docs when filesystem-visible behavior, schema usage, config ownership, authority boundaries, or state freshness semantics change.
- Comments should explain authority invariants and recovery semantics, not restate syntax.
- Keep speculative multi-group msync or future routing designs out of local docs unless explicitly requested.

## Review checklist

- Did inode/dentry/attrs remain authoritative and path remain an adapter?
- Did authority policy stay in `metadata`?
- Did the change avoid dependencies on `worker` and `client` production code?
- Did it reuse `types` and `proto` conversion for stable shared DTOs?
- Did it preserve structured refreshable error behavior?
- Did it keep `route_epoch`, `mount_epoch`, `worker_epoch`, and `GroupStateWatermark` separate?
- Did tests/docs move with changed authority behavior?
