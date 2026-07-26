# beryl-common Agent Instructions

Follow the repository root `AGENTS.md`. This file adds crate-specific
constraints.

## Crate Boundary

`beryl-common` owns crate-independent infrastructure shared by current runtime
crates: structured errors, headers, config mechanics, retry/time helpers, and
observability utilities.

## Allowed Changes

- Improve shared error and header structures while preserving machine-readable
  detail.
- Add config, retry, time, or observability mechanics that are independent of
  service policy.
- Tighten validation and operational failure reporting at shared boundaries.

## Prohibited Changes

- Do not put metadata, worker, client, proto, or UFS policy here.
- Do not hide structured operational failures behind string-only errors.
- Do not create competing error, header, config, or retry vocabularies.
- Do not use this crate as a dumping ground for unrelated helpers.
- Do not add a shared helper until its ownership and reuse are concrete.

## Cross-Crate Rules

- Owning crates retain policy; `beryl-common` supplies mechanics.
- Shared values must not create dependency cycles or pull runtime crates into
  `beryl-common`.
- Changes to shared error or header semantics require validation in affected
  producers and consumers.

## Focused Validation

```bash
cargo test -p beryl-common
```
