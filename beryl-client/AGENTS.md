# beryl-client Agent Instructions

Follow the repository root instructions.

## Responsibility

Own the Rust native client API and coordination of metadata authority with
worker execution.

## Invariants

- Keep data access and cached routing subordinate to metadata authorization.
- Validate peer identity, freshness, and operation context before accepting
  results.
- Preserve the distinction between definite failure and unknown outcome.
- Retries after ambiguous side effects require stable operation identity,
  unchanged intent, and defined replay semantics.
- Keep partial-failure recovery bounded and make unresolved completion explicit
  to the caller.
- Keep wire conversion and validation at communication boundaries; do not move
  service authority or worker execution into the client.

## Validation Focus

Verify affected public behavior and orchestration, especially retry identity,
ambiguous outcomes, stale routing, authorization, and partial-failure recovery.
