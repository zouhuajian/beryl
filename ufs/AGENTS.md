# ufs Agent Instructions

## Crate Boundary

`ufs` owns the external backend and adapter boundary. Current Vecton file IO does not use UFS for reads or writes.

## Allowed Changes

- Improve backend specs, backend-specific config, defaults, validation, and construction.
- Improve OpenDAL adapter setup and backend capability mapping.
- Clarify unsupported backend behavior explicitly.
- Prepare future integration only when it preserves unified Vecton resident data semantics.

## Do Not Do

- Do not document UFS read-through or write-through as implemented unless code proves it.
- Do not own metadata authority, namespace policy, worker runtime behavior, or client retry/replay/cache policy.
- Do not depend on `metadata`, `worker`, or `client`.
- Do not silently fake unsupported backend semantics such as rename, append, truncate, consistency, or directory behavior.
- Do not introduce separate cache-mode semantics in current docs.

## Cross-Crate Rules

- Keep backend-specific policy isolated behind adapter boundaries.
- Surface backend limitations through explicit capability and error contracts.
- Future UFS integration must preserve metadata-owned visibility and unified Vecton resident data semantics.

## Validation Notes

- Root workspace validation applies.
- For focused checks, use `cargo test -p ufs`.
