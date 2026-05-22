# `common` Agent Instructions

This file applies to `common/`. Follow the root `AGENTS.md` first, then these local rules.

## Crate role

`common` is generic shared infrastructure. It is not a fallback bucket for code that lacks a clearer owner.

## Allowed changes

- Canonical recoverable error types and shared error helpers.
- Request/response header domain types that are independent of one service implementation.
- Generic config loading, flattening, and environment-key mapping.
- Generic utilities, retry/time/path helpers, and observability primitives.
- Module-independent validation helpers.
- Shared low-level primitives that are stable across multiple modules.

## Forbidden changes

- Metadata authority DTOs, inode/dentry/mount policy, or filesystem authority rules.
- Worker store state, worker runtime state, replication/repair orchestration, or worker-local network behavior.
- Client retry, replay, refresh, route-cache, endpoint-health, or SDK error policy.
- UFS backend-specific policy or adapter behavior.
- Generated proto types, schema-local codecs, or gRPC adapters.
- Module-specific typed config structs, defaults, validation, or every module key/default as a dumping ground.
- Domain models that belong in `types`.

## Dependency rules

- `common` may depend on `types` for stable IDs and shared primitives.
- `common` must not depend on `proto`, `metadata`, `worker`, `client`, or `ufs`.
- Avoid heavy dependencies for narrow convenience.
- If a helper needs module policy to be correct, keep it out of `common`.

## Conversion and validation rules

- Shared error/header domain types may live here; structural proto conversion belongs in `proto`.
- Generic validation is allowed only when it does not choose metadata, worker, client, UFS, retry, refresh, route, repair, or cache policy.
- Config helpers may parse and flatten config data, but typed module config validation belongs to the owning module.
- Do not silently rewrite invalid config combinations.

## Testing guidance

- Add unit tests for error classification, config helpers, and generic validation.
- Prefer structured assertions over string-only assertions when structured fields exist.
- Add negative tests proving invalid combinations are rejected.
- Do not add integration-style tests here when the behavior belongs to a product crate.

## Documentation guidance

- Document shared primitives whose misuse would cause semantic bugs.
- Keep comments focused on invariants, ownership, and structured semantics.
- Update root or crate docs when shared infrastructure ownership changes.

## Review checklist

- Is this truly generic, stable, and cross-module?
- Should it live in `types` instead?
- Did it introduce product runtime policy or a product-crate dependency?
- Did it create a second error vocabulary?
- Did config handling stay generic while typed module config stayed local?
- Did stale internal code get deleted instead of wrapped when no compatibility requirement exists?
