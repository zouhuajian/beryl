# `common` Agent Instructions

This file applies to `common/`. Follow the root `AGENTS.md` first; keep this crate limited to shared infrastructure.

## Scope

`common` owns infrastructure that is generic across product crates:

- canonical recoverable error primitives and service-independent error helpers
- request/response header domain types
- config loading, flattening, environment-key mapping, and low-level config mechanics
- observability, retry/time/path utilities, and other module-independent helpers
- generic validation only when it does not choose product policy

`common` must not own metadata authority, worker execution, client retry/cache policy, UFS backend policy, generated proto types, schema-local codecs, gRPC adapters, product runtime state, or typed module config semantics. Stable domain values belong in `types`, not here.

## Local Rules

- Do not use `common` as a dumping ground for code that lacks a clear owner.
- Keep helpers small and infrastructure-shaped; if correctness depends on metadata, worker, client, UFS, route, lease, repair, cache, or retry policy, keep the code in that product crate.
- Config utilities may parse and flatten inputs, but each module owns its typed config structs, defaults, validation, and key meaning.
- Shared error/header code may define structured domain shapes; structural proto conversion belongs in `proto`.
- Avoid broad dependencies for narrow convenience.

## Tests

- Test error classification, config mechanics, and generic validation with structured assertions.
- Include negative tests for invalid config or validation inputs when behavior is not obvious.
- Do not add integration-style tests here for product-crate policy.

## Local Self-Review

Apply the root self-review checklist, then check:

- Is the new code truly generic, stable, and cross-module?
- Should the value or validation live in `types` instead?
- Did typed config semantics stay in the owning module?
- Did this create a second error vocabulary or duplicate proto conversion?
- Did dependency direction remain `common` -> `types` only when needed?
