# beryl-ufs Agent Instructions

Follow the repository root `AGENTS.md`. This file adds crate-specific
constraints.

## Crate Boundary

`beryl-ufs` owns external backend specifications, capability description,
configuration, construction, and adapter mechanics. It does not own active
metadata, worker, or client policy.

## Allowed Changes

- Improve backend specs, configuration, validation, and construction required
  by current callers.
- Improve adapter behavior and explicit capability mapping.
- Surface backend limitations and operational failures precisely.

## Prohibited Changes

- Do not claim read-through, write-through, fallback, or cache behavior that is
  not active in the supported runtime.
- Do not own metadata authority, namespace policy, worker lifecycle, or client
  retry/cache policy.
- Do not depend on `beryl-metadata`, `beryl-worker`, or `beryl-client`.
- Do not emulate unsupported backend semantics silently.
- Do not add speculative integration layers without a current caller.

## Cross-Crate Rules

- Keep backend-specific mechanics isolated at the adapter boundary.
- Expose capabilities and failures explicitly rather than selecting product
  policy.
- Any active integration must preserve metadata-owned visibility and the
  supported data model.

## Focused Validation

```bash
cargo test -p beryl-ufs
```
