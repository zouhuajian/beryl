# `types` Agent Instructions

This file applies to `types/`. Follow the root `AGENTS.md` first, then these local rules.

## Crate role

`types` is a pure Rust domain-model crate. It owns stable value objects shared by multiple production modules. It must stay independent of generated proto types and product-runtime crates.

## Allowed changes

- Strongly typed IDs and value objects.
- Stable cross-module Rust domain models.
- Worker endpoint domain values.
- File block location, write target, and committed block domain values.
- Fencing token, block stamp, worker epoch, and state watermark helpers.
- Pure validation for shared values.
- Small semantic helpers that make illegal states harder to represent.

## Forbidden changes

- Generated proto types or imports from `proto`.
- Proto wire enum values or service-contract details.
- Metadata authority internals, state-machine code, or persistence engine policy.
- Worker store/runtime state, stream runtime state, path layout, publishability, recovery, or local execution helpers.
- Client retry, replay, cache, endpoint-health, or SDK policy.
- UFS adapter internals or backend behavior.
- Integration-test fixtures or test-only abstractions.
- Placeholder abstractions without active runtime use.
- Generic utilities that belong in `common`.

## Dependency rules

- `types` must not depend on any workspace crate: not `common`, `proto`, `metadata`, `worker`, `client`, `ufs`, or `integration_tests`.
- Keep external dependencies light and justified.
- Do not import generated proto code as the authoritative shape of any domain concept.

## Conversion and validation rules

- Domain construction and pure value validation may live here.
- Proto/domain conversion belongs in `proto`, not `types`.
- Avoid lossy `From`/`Into` implementations unless the loss is obvious and safe.
- Use `TryFrom` or explicit constructors when validation can fail.
- Keep identity separation explicit: inode identity, data identity, session handle, block identity, worker epoch, route epoch, and mount epoch are distinct concepts.
- Do not flatten Block, Chunk, and Stream into a generic range abstraction unless the abstraction is semantically proven and actively used.

## Testing guidance

- Test identity separation, invalid construction, equality/ordering semantics, and epoch/watermark comparison.
- Test serialization only when a type owns stable serialized semantics.
- Tests should prove domain semantics, not just derived trait behavior.

## Documentation guidance

- Document persisted or cross-node interpretation semantics.
- Document defaults only when a default is semantically valid.
- Update README or boundary docs when a shared domain value moves into or out of `types`.

## Review checklist

- Is this a stable Rust domain concept used by more than one production module?
- Does this accidentally encode runtime policy?
- Did `types` remain free of generated proto imports and workspace dependencies?
- Did the change preserve Block/Chunk/Stream separation and identity separation?
- Is every default, conversion, and validation rule semantically valid?
- Is this active runtime surface rather than a future placeholder?
