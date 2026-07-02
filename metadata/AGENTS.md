# metadata Agent Instructions

## Crate Boundary

`metadata` owns namespace, layout, visibility, leases/write sessions, worker registry, block locations, freshness, and Raft-backed metadata state.

## Allowed Changes

- Fix authority behavior for namespace, layout, visibility, lease, freshness, worker registry, or block-location paths.
- Improve Raft/RocksDB apply, replay, snapshot, and persistence handling.
- Tighten structured errors for consistency, availability, and storage failures.
- Add focused coverage for authority, replay, persistence, freshness, and error contracts when code changes require it.

## Do Not Do

- Do not implement or claim production-ready multi-group metadata casually.
- Do not bypass Raft-backed mutation paths for namespace, layout, visibility, lease, or worker-registration authority.
- Do not silently swallow consistency, storage, replay, or snapshot errors.
- Do not put worker data execution, client retry/cache policy, UFS backend behavior, or proto schema ownership here.
- Do not infer a metadata group from `WorkerId` or fall back to a hard-coded group in production code.
- Do not describe replication, repair, or rebalancing as complete user-facing behavior.

## Cross-Crate Rules

- Use `types`, `common`, and `proto` for shared contracts.
- `metadata` may depend on `ufs` only as an adapter boundary; do not document UFS as the current read/write path.
- `worker` and `client` must not be production dependencies of `metadata`.
- Preserve `route_epoch`, `mount_epoch`, and `GroupStateWatermark` as separate freshness domains.

## Validation Notes

- Root workspace validation applies.
- For focused checks, use `cargo test -p metadata`.
