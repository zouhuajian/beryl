# `proto` Agent Instructions

This file applies to `proto/`. Follow the root `AGENTS.md` first; keep this crate focused on wire contracts and structural conversion.

## Scope

`proto` owns:

- `.proto` files, generated Rust modules, and exports
- gRPC service contracts, wire messages, field numbers, and enum numeric values
- structural conversion between generated proto types and `types`/`common` domain types
- schema-local codecs when the persisted or transported payload is protobuf
- explicit compatibility notes when an external contract requires them

`proto` must not own business policy, authority decisions, retry/replay/cache behavior, endpoint-health policy, route refresh decisions, metadata routing, worker store/runtime behavior, UFS behavior, client SDK policy, or duplicated shadow models that compete with `types`.

## Local Rules

- `proto` may depend on `types` and `common`; it must not depend on product crates.
- Do not introduce proto messages only because two Rust structs look similar.
- Do not add compatibility aliases or decode fallbacks unless explicitly requested or required by an external consumer.
- Never silently change or reuse numeric field or enum values.
- Keep proto comments about wire-level behavior: required/advisory status, error contract, epoch/fencing fields, freshness, stream semantics, and compatibility implications.
- Avoid duplicating domain semantics in comments when the Rust domain type already owns the meaning.
- Raw proto values should stay near service or adapter boundaries; product runtime code should use domain models where one exists.

## Tests

- Schema changes require generated-code rebuild and compilation of active Rust callers.
- Add tests for conversion behavior, structured error mapping, identity/epoch propagation, and stream framing semantics where relevant.
- Do not stop at "proto compiles" when the wire contract changed.

## Local Self-Review

Apply the root self-review checklist, then check:

- Are active consumers, generated Rust references, and wire numeric values accounted for?
- Is the change schema or structural conversion only, with policy kept local to product crates?
- Did it avoid duplicate headers, duplicate error paths, and shadow domain models?
- Is every compatibility shim backed by a real external requirement?
- Do comments describe current wire contract rather than migration history?
