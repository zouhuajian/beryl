# `types` Agent Instructions

This file applies to `types/`. Follow the root `AGENTS.md` first; keep this crate a pure Rust domain model.

## Scope

`types` owns stable domain values shared by multiple production crates:

- typed IDs and value objects
- worker endpoint and capability domain values
- file block location, write target, committed block, byte range, fencing token, block stamp, worker run identity, and state watermark values
- pure constructors and validation that make illegal domain states harder to represent

`types` must not own generated proto types, wire enum values, service-level concerns, metadata authority internals, worker store/runtime state, client retry/cache policy, UFS adapter behavior, integration-test fixtures, placeholder abstractions, or generic utilities that belong in `common`.

## Local Rules

- `types` must not depend on any workspace crate.
- Keep external dependencies light and justified.
- Prefer domain names that reflect real concepts; do not encode transport, service, or persistence-engine detail into type names.
- Use `TryFrom` or explicit constructors when validation can fail. Avoid lossy `From`/`Into` conversions unless the loss is obvious and safe.
- Keep identity separation explicit: inode identity, data identity, session handles, block identity, route epoch, mount epoch, worker run identity, block stamp, and state watermark are distinct concepts.
- Do not flatten Block, StorageChunk, TransportFrame, and Stream into a generic range or IO abstraction unless the abstraction is semantically proven and actively used.

## Tests

- Test invalid construction, equality/ordering semantics, identity separation, epoch/watermark comparison, and domain invariants.
- Test serialization only when the type owns stable serialized semantics.
- Do not test derived trait behavior unless it protects a contract.

## Local Self-Review

Apply the root self-review checklist, then check:

- Is this a stable Rust domain concept used by more than one production crate?
- Did `types` remain free of generated proto imports and workspace dependencies?
- Did the change avoid runtime policy and service-level concerns?
- Are defaults, conversions, and validation rules semantically valid?
- Is the surface active runtime contract rather than future scaffolding?
