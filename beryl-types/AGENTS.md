# beryl-types Agent Instructions

Follow the repository root instructions.

## Responsibility

Own shared domain values and their invariants. Domain validity belongs here;
runtime policy and execution do not.

## Invariants

- Add values for current requirements with concrete callers.
- Keep domain values independent of runtime implementations and generated wire
  representations.
- Preserve identity, ordering, and serialization contracts unless their change
  is explicitly authorized.
- Do not weaken domain invariants for serialization or test convenience.

## Validation Focus

Verify affected value semantics and invalid states, including consumer behavior
when a shared contract changes.
