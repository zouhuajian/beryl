# beryl-proto Agent Instructions

Follow the repository root instructions.

## Responsibility

Own wire contracts, generated representations, and structural conversion
between wire and shared domain values. Service policy stays outside this crate.

## Invariants

- Define contracts for current requirements with concrete producers and
  consumers.
- Preserve established wire identity and meaning by default. When a breaking
  change is explicitly authorized, apply the new contract consistently across
  affected producers and consumers without unnecessary compatibility paths.
- Maintain generated artifacts through their source and generation workflow.
- Validate correctness-sensitive values at the boundary; do not silently accept
  malformed or unknown values that could undermine correctness.

## Validation Focus

Regenerate affected artifacts and compile all affected producers and consumers.
Verify wire semantics, conversions, and error mapping according to the agreed
compatibility contract.
