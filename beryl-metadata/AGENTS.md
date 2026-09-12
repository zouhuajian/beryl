# beryl-metadata Agent Instructions

Follow the repository root instructions.

## Responsibility

Own namespace, layout, visibility, write authority, worker registration,
locations, freshness, and durable metadata decisions.

## Invariants

- Distinguish durable authority, leader-local state, and worker observations.
  Process-local or reported state does not become durable authority by itself.
- Mutate durable authority through its ordered decision path. Publish state
  before reporting its completion to waiting callers.
- Fence authority-sensitive reads to the leadership and state assumptions on
  which their decisions depend.
- Establish authority scope from validated ownership, not unrelated identities
  or assumed defaults.
- Define replay, recovery, fencing, and retirement for persisted derived work.
  Do not hide persistence or recovery failures.
- After restart or replay, reconstruct required transient state or keep dependent
  operations unavailable until their required evidence returns.
- Incomplete, stale, or ambiguous evidence must not authorize destructive
  actions or visibility changes.
- Keep independently meaningful freshness domains separate unless a replacement
  is designed and validated to preserve their guarantees.
- Keep worker execution, client recovery policy, and external-storage IO outside
  metadata authority.

## Validation Focus

Verify affected authority decisions, publication ordering, freshness, and
behavior under concurrency, replay, restart, and incomplete evidence.
