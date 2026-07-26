# beryl-client Agent Instructions

Follow the repository root `AGENTS.md`. This file adds crate-specific
constraints.

## Crate Boundary

`beryl-client` owns the Rust native API and metadata/worker RPC orchestration.
It coordinates metadata authority with worker execution but does not replace
either authority.

## Allowed Changes

- Improve public API handles, options, status, and listing behavior.
- Improve metadata and worker RPC orchestration and response validation.
- Tighten identity, call IDs, retries, refresh, replay, unknown-outcome,
  endpoint-cache, and write-session behavior.
- Add focused public behavior, retry, freshness, fencing, and failure-recovery
  coverage.

## Prohibited Changes

- Do not production-depend on `beryl-metadata` or `beryl-worker`.
- Do not bypass metadata for direct worker access.
- Do not add blind retries, silent fallback, or stale success for consistency
  failures.
- Do not retry ambiguous side effects without stable operation identity and
  replay semantics.
- Do not expose unsupported runtime topology or compatibility claims through
  public APIs.

## Orchestration Rules

- Validate server identity, response headers, freshness, and operation context
  before accepting results.
- Keep cached routing and endpoint state subordinate to metadata authority.
- Preserve the distinction between a definite failure and an unknown outcome.
- Freeze retry identity and payload before a side effect can become ambiguous.
- Keep recovery work after partial failure bounded, replayable, and explicit to
  the caller when completion is uncertain.

## Cross-Crate Rules

- Use `beryl-types`, `beryl-common`, and `beryl-proto` for shared contracts.
- Convert raw proto values near service boundaries and use domain values after
  validation.
- Keep UFS behavior outside the supported client interface unless explicitly
  implemented end to end.

## Focused Validation

```bash
cargo test -p beryl-client
```
