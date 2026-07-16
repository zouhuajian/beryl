# beryl-ufs Agent Instructions

## Crate Boundary

`beryl-ufs` owns the external backend and adapter boundary. Current Beryl file IO does not use UFS for reads or writes.

## Allowed Changes

- Improve backend specs, backend-specific config, defaults, validation, and construction.
- Improve OpenDAL adapter setup and backend capability mapping.
- Clarify unsupported backend behavior explicitly.
- Prepare future integration only when it preserves unified Beryl resident data semantics.

## Do Not Do

- Do not document UFS read-through or write-through as implemented unless code proves it.
- Do not own metadata authority, namespace policy, worker runtime behavior, or client retry/replay/cache policy.
- Do not depend on `beryl-metadata`, `beryl-worker`, or `beryl-client`.
- Do not silently fake unsupported backend semantics such as rename, append, truncate, consistency, or directory behavior.
- Do not introduce separate cache-mode semantics in current docs.

## Cross-Crate Rules

- Keep backend-specific policy isolated behind adapter boundaries.
- Surface backend limitations through explicit capability and error contracts.
- Future UFS integration must preserve metadata-owned visibility and unified Beryl resident data semantics.

## Validation Notes

- Root workspace validation applies.
- For focused checks, use `cargo test -p beryl-ufs`.
