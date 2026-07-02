# common Agent Instructions

## Crate Boundary

`common` owns shared infrastructure: canonical errors, headers, config mechanics, retry/time helpers, observability utilities, and small crate-independent helpers.

## Allowed Changes

- Improve canonical error and header structures without losing machine-readable detail.
- Add config, retry, time, or observability helpers that are genuinely crate-independent.
- Tighten validation mechanics that do not choose service policy.
- Keep operational failures explicit and structured.

## Do Not Do

- Do not put service-specific metadata, worker, client, or UFS behavior here.
- Do not hide operational failures behind generic string-only errors.
- Do not move product config semantics into common config mechanics.
- Do not create a second error vocabulary that competes with canonical errors.
- Do not use `common` as a dumping ground for unrelated helpers.

## Cross-Crate Rules

- Owning crates keep policy; `common` supplies mechanics.
- Shared errors and headers must remain usable by metadata, worker, client, proto, and UFS paths.
- Avoid dependencies that would pull runtime crates into `common`.

## Validation Notes

- Root workspace validation applies.
- For focused checks, use `cargo test -p common`.
