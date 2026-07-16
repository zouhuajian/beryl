# beryl-client Agent Instructions

## Crate Boundary

`beryl-client` owns the Rust native API and metadata/worker RPC orchestration. It coordinates authority decisions from metadata with data execution on workers.

## Allowed Changes

- Improve `FsClient`, reader/writer handles, operation options, and status/listing types.
- Improve metadata RPC orchestration, worker RPC orchestration, and response validation.
- Tighten client identity, call ID, retry, refresh, replay, unknown-outcome handling, endpoint cache, and write-session behavior.
- Add focused coverage for public API behavior and retry/freshness contracts when code changes require it.

## Do Not Do

- Do not production-depend on `beryl-metadata` or `beryl-worker` crates.
- Do not bypass metadata for direct worker access.
- Do not add POSIX, FUSE, or Hadoop compatibility claims unless implemented.
- Do not leak future-only metadata-group concepts into public APIs unless current code requires them.
- Do not add blind retry loops or silent fallback for consistency failures.
- Do not add test-only seeding, injection, force, or fake APIs to production modules.

## Cross-Crate Rules

- Use `beryl-types`, `beryl-common`, and `beryl-proto` for shared contracts.
- Convert raw proto near service boundaries and use domain objects after boundaries where available.
- Keep route/cache state subordinate to metadata authority.
- Keep UFS integration out of the current client interface unless explicitly implemented.

## Validation Notes

- Root workspace validation applies.
- For focused checks, use `cargo test -p beryl-client`.
