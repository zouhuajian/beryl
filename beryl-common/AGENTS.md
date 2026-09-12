# beryl-common Agent Instructions

Follow the repository root instructions.

## Responsibility

Own shared infrastructure mechanics for errors, request context, configuration,
retry and time handling, and observability. Service policy stays with the
owning runtime crate.

## Invariants

- Add shared functionality only when its ownership and reuse are concrete.
- Preserve structured, machine-readable failure information.
- Keep shared concepts consistent rather than introducing competing definitions.
- Remain independent of runtime implementations and wire-specific policy.

## Validation Focus

Validate changed shared semantics in affected producers and consumers as well
as at the local boundary.
