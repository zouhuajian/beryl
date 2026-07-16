# beryl-types Agent Instructions

## Crate Boundary

`beryl-types` owns stable Rust domain and value types shared by the Beryl crates used today. It may encode shared invariants, but it must not choose runtime policy.

## Allowed Changes

- Add or refine domain values required by current production code.
- Tighten constructors and validation for shared invariants.
- Clarify names when they reduce ambiguity across crates.
- Keep tests focused on value semantics when production code changes require them.

## Do Not Do

- Do not depend on runtime crates.
- Do not import generated proto types.
- Do not add client, metadata, worker, or UFS policy logic.
- Do not add future-only types unless current code requires them.
- Do not turn runtime implementation details into shared domain contracts.

## Cross-Crate Rules

- `beryl-types` should remain usable by `beryl-metadata`, `beryl-worker`, `beryl-client`, `beryl-proto`, and `beryl-common` without creating dependency cycles.
- Convert generated wire values in `beryl-proto` or boundary code, not by making `beryl-types` depend on proto modules.
- Keep shared values stable enough for multiple production crates to rely on.

## Validation Notes

- Root workspace validation applies.
- For focused checks, use `cargo test -p beryl-types`.
