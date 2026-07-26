# beryl-worker Agent Instructions

Follow the repository root `AGENTS.md`. This file adds crate-specific
constraints.

## Crate Boundary

`beryl-worker` owns local block storage, stream execution, metadata-authorized
data operations, local lifecycle coordination, registration, heartbeat, and
block reports. It does not own namespace visibility or file layout policy.

## Allowed Changes

- Improve local storage, stream execution, block publication, abort, sync, and
  recovery behavior.
- Tighten validation of metadata-issued identity, fencing, freshness, and
  operation context.
- Improve control-plane registration, heartbeat, reports, readiness, runtime
  configuration, and data-service adapters.
- Add focused storage, stream, concurrency, crash-recovery, idempotency, and
  reporting coverage.

## Prohibited Changes

- Do not decide namespace visibility or file layout.
- Do not materialize, expose, mutate, or remove data without validated authority
  and exact local identity.
- Do not derive external paths from internal data identifiers.
- Do not make unpublished or ambiguous data readable.
- Do not add alternate transports before the supported path is correct.
- Do not describe partial lifecycle primitives as complete replication, repair,
  rebalancing, or cache behavior.

## Local Safety Rules

- Validate persisted shape before advertising or serving local data.
- Coordinate destructive mutations with the full lifetime of active readers and
  writers.
- Destructive mutations must be version-sensitive, retry-safe, and recoverable
  after interruption.
- Startup recovery must complete or quarantine incomplete local transitions
  before normal discovery and service.
- A failed or cancelled mutation must not silently restore unsafe availability.
- Reports must reflect recoverable local truth rather than desired state.

## Cross-Crate Rules

- Use `beryl-types`, `beryl-common`, and `beryl-proto` for shared contracts.
- `beryl-metadata` and `beryl-client` must not be production dependencies.
- Data access remains subordinate to metadata authorization.

## Focused Validation

```bash
cargo test -p beryl-worker
```
