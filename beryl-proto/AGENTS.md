# beryl-proto Agent Instructions

Follow the repository root `AGENTS.md`. This file adds crate-specific
constraints.

## Crate Boundary

`beryl-proto` owns protobuf/gRPC schema, generated modules, and structural
conversion between wire and shared domain values. It does not own service
policy.

## Allowed Changes

- Change wire contracts only for a current producer and consumer.
- Add or refine structural conversions and boundary validation.
- Document stable wire semantics, required identity, freshness, fencing, and
  failure behavior.
- Add wire, conversion, and error-mapping coverage.

## Prohibited Changes

- Do not hand-edit generated bindings.
- Do not reuse or silently change field numbers, enum values, or established
  wire meaning.
- Do not add compatibility aliases, decode fallbacks, or future service
  contracts without a current external requirement.
- Do not put authority decisions, business policy, retries, caching, or worker
  execution in this crate.
- Do not silently accept unknown or malformed correctness-sensitive values.

## Cross-Crate Rules

- Keep generated values at service boundaries and convert to domain types where
  an owned domain type exists.
- Every schema change must compile all current producers and consumers.
- Compatibility-sensitive changes require an explicit compatibility decision
  and focused wire coverage.

## Focused Validation

```bash
cargo test -p beryl-proto
```

Schema changes also require generated-code rebuild and compilation of affected
callers and handlers.
