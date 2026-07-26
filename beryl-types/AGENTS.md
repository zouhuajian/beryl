# beryl-types Agent Instructions

Follow the repository root `AGENTS.md`. This file adds crate-specific
constraints.

## Crate Boundary

`beryl-types` owns stable Rust domain and value types shared by the current
runtime. It may enforce domain invariants but must not choose runtime policy.

## Allowed Changes

- Add or refine values required by current production callers.
- Enforce invariants through constructors, parsing, and validation.
- Clarify ambiguous names and identity boundaries.
- Test value semantics and invalid states.

## Prohibited Changes

- Do not depend on runtime crates or generated proto modules.
- Do not add metadata, worker, client, proto, or UFS policy.
- Do not expose runtime implementation details as shared domain contracts.
- Do not add future-only values without a current producer and consumer.
- Do not weaken a type invariant for serialization or test convenience.

## Cross-Crate Rules

- Keep values usable across current crates without dependency cycles.
- Convert wire values at proto or service boundaries.
- Preserve identity, ordering, and serialization semantics when changing shared
  values.

## Focused Validation

```bash
cargo test -p beryl-types
```
