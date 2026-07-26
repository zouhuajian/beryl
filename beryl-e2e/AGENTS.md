# beryl-e2e Agent Instructions

Follow the repository root `AGENTS.md`. This file adds crate-specific
constraints.

## Crate Boundary

`beryl-e2e` owns black-box validation of the supported runtime across public
client, metadata, worker, persistence, and RPC boundaries.

## Allowed Changes

- Add coverage for supported public behavior and cross-process invariants.
- Exercise restart, recovery, replay, freshness, fencing, visibility, and
  convergence through real runtime boundaries.
- Improve deterministic service startup, shutdown, temporary storage, and
  failure orchestration in the test harness.

## Prohibited Changes

- Do not add production APIs or widen production visibility for E2E setup.
- Do not validate private implementation shape when public behavior can express
  the invariant.
- Do not add coverage for unsupported product surfaces as if they were active.
- Do not hide failures with ignored tests, unbounded sleeps, blind retries, or
  assertions that accept multiple incompatible outcomes.
- Do not share mutable state between tests without explicit isolation.

## Test Rules

- Use public client and RPC boundaries for user-visible behavior.
- Use bounded readiness checks and deterministic synchronization.
- Give every test isolated ports, identities, and temporary persistent state.
- Assert both externally visible results and required recovery/convergence
  outcomes.
- Keep fault injection local to the test harness and remove it after each test.

## Focused Validation

```bash
cargo test -p beryl-e2e
```
