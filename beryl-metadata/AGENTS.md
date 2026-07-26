# beryl-metadata Agent Instructions

Follow the repository root `AGENTS.md`. This file adds crate-specific
constraints.

## Crate Boundary

`beryl-metadata` owns namespace, layout, visibility, leases/write sessions,
durable worker descriptors, worker observations, block locations, freshness,
and Raft/RocksDB-backed metadata authority.

## Allowed Changes

- Correct authority behavior for namespace, layout, visibility, leases,
  freshness, worker state, and block locations.
- Improve Raft/RocksDB proposal, apply, replay, snapshot, persistence, and
  recovery behavior.
- Tighten structured consistency, availability, and storage failures.
- Add focused authority, replay, persistence, freshness, concurrency, and
  failure-recovery coverage.

## Prohibited Changes

- Do not mutate durable authority outside its ordered mutation path.
- Do not treat process-local observation as durable authority.
- Do not persist derived work without defined replay, recovery, fencing, and
  retirement semantics.
- Do not swallow consistency, storage, replay, or snapshot failures.
- Do not infer authority scope from unrelated identifiers or fall back to a
  hard-coded scope.
- Do not put worker data execution, client retry/cache policy, UFS backend
  behavior, or proto schema ownership here.
- Do not expose internal maintenance mechanisms as completed product behavior.

## Consistency Rules

- Separate durable state, leader-local state, and worker-reported observation
  explicitly.
- State publication must happen before a waiting caller can observe completion.
- Reads that require current authority must be fenced to the same leadership
  and state assumptions used by the decision.
- Restart and replay must either reconstruct required soft state or remain
  unavailable until the required evidence returns.
- Incomplete, stale, or ambiguous evidence must not authorize destructive or
  visibility-changing decisions.

## Cross-Crate Rules

- Use `beryl-types`, `beryl-common`, and `beryl-proto` for shared contracts.
- `beryl-worker` and `beryl-client` must not be production dependencies.
- `beryl-ufs` may be used only as an adapter boundary; metadata remains the
  authority for namespace and visibility.
- Keep independent freshness domains separate unless a replacement invariant is
  designed and tested.

## Focused Validation

```bash
cargo test -p beryl-metadata
```
