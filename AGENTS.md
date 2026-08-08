# Beryl Agent Instructions

`AGENTS.md` files are operational instructions for AI coding agents. Read this
file first, then the local `AGENTS.md` for every touched subtree. Local files
may narrow these rules for their crate, but must not weaken them.

## Decision Order

Resolve engineering trade-offs in this order:

1. Functional correctness.
2. Safety and invariant preservation.
3. Simplicity.
4. Readability.
5. Abstraction.

Do not make implicit trade-offs between these priorities.

## Work Contract

- Follow the user's requested mode: review, diagnose, plan, or change.
- A read-only request does not authorize edits, formatting, staging, commits,
  pushes, or commands that materially modify the workspace.
- Before editing, inspect the relevant code path, local instructions, current
  worktree state, and existing tests.
- Preserve unrelated user changes and keep the diff limited to the requested
  behavior.
- State assumptions when behavior, authority, failure handling, or scope is
  uncertain. Do not silently guess in correctness-sensitive paths.
- Do not claim unsupported behavior, unperformed validation, or incomplete
  lifecycle work as finished.

## Scope and Design

- Prefer direct, concrete implementations with clear local control flow.
- Add an abstraction only when it enforces a real invariant or boundary,
  isolates an external dependency, removes stable duplication, or materially
  improves testing of critical behavior.
- Do not add speculative traits, managers, wrappers, compatibility layers,
  fallback paths, or parallel implementations.
- Preserve external contracts unless the task explicitly authorizes a breaking
  change.
- Keep refactoring local to the behavior being changed. Do not use a focused
  change as authorization for cross-module reorganization.
- Remove obsolete code when replacing behavior; do not retain dead compatibility
  paths without a current requirement.

## Product Boundary

- The supported runtime currently uses one metadata group and one metadata
  leader.
- The Rust native client is the supported client interface.
- Reads and writes go through metadata-authorized worker storage.
- UFS is an adapter boundary, not the active read or write path.
- The internal writable namespace is rooted at `/`; `/local` has no special
  namespace semantics.
- Multi-group metadata, multiple metadata leaders, metadata peer RPC, admin
  APIs, replication, repair, rebalancing, alternate transports, POSIX, FUSE,
  Hadoop compatibility, and UFS-backed IO are outside the supported product
  boundary unless explicitly requested and completed end to end.

Do not add placeholder surfaces or documentation claims for unsupported
capabilities.

## Crate Ownership

- `beryl-cli`: public command contract, installed layout resolution, and
  package-internal process routing.
- `beryl-types`: stable domain and value types.
- `beryl-common`: shared errors, headers, config mechanics, retry/time helpers,
  and observability utilities.
- `beryl-proto`: protobuf/gRPC schema, generated bindings, and structural
  conversions.
- `beryl-metadata`: namespace, layout, visibility, leases/write sessions,
  worker registry, block locations, freshness, and Raft/RocksDB-backed metadata
  authority.
- `beryl-worker`: local block storage, stream execution, block lifecycle,
  registration, heartbeat, and block reports.
- `beryl-client`: Rust native API and metadata/worker RPC orchestration.
- `beryl-ufs`: external backend and adapter boundary.
- `beryl-e2e`: black-box coverage of the supported runtime path.

Production dependency direction must remain clean:

- `beryl-cli` must not production-depend on `beryl-metadata` or
  `beryl-worker`.
- `beryl-client` must not production-depend on `beryl-metadata` or
  `beryl-worker`.
- `beryl-worker` must not production-depend on `beryl-metadata` or
  `beryl-client`.
- `beryl-ufs` must not depend on `beryl-metadata`, `beryl-worker`, or
  `beryl-client`.
- Shared contracts belong in `beryl-types`, `beryl-common`, or `beryl-proto`
  according to their ownership.
- Test-only dependency direction must not leak into production dependency
  graphs.

## Correctness and Failure Handling

- Identify the source of truth before changing distributed or persistent
  behavior.
- Preserve ordering, fencing, identity, freshness, visibility, and ownership
  checks across RPC, concurrency, and restart boundaries.
- Treat timeout, cancellation, partial IO, process restart, replay, duplicate
  delivery, and stale state as normal failure modes.
- Fail closed when authority, persisted state, or destructive-operation
  preconditions cannot be verified.
- Destructive operations must resolve an exact target, be safe under retries,
  and have explicit crash-recovery behavior.
- Do not treat an in-memory lock as protection for a resource whose lifetime
  extends beyond that lock.
- Keep IO, retries, queues, batches, and background work bounded.
- Do not silently convert consistency failures into fallback or stale success.

## Rust Code

- Keep production code straightforward and use the narrowest necessary
  visibility.
- Do not widen visibility or add production APIs solely for tests.
- Keep Rust comments and documentation in English.
- Comments should explain stable responsibilities, preconditions, invariants,
  or failure behavior. Do not record task history, review history, or temporary
  implementation phases in production comments.
- Avoid unnecessary `allow` attributes. Fix warnings in touched code instead of
  suppressing them without justification.

## Test Organization

- Production items must appear before unit-test code.
- New or moved inline unit tests must be contained in one
  `#[cfg(test)] mod tests` at the end of the file.
- When tests are split into separate files, keep the `#[cfg(test)] mod tests;`
  declaration at the end of the production module.
- Keep test helpers, fixtures, helper implementations, and test-only types
  inside test modules or dedicated test files.
- Do not add test-only re-exports, visibility widening, getters, injection
  points, force methods, or fake APIs to production modules.
- Test private behavior in its owning module. Test cross-module behavior through
  existing production boundaries.
- Prefer a small number of strong behavior and invariant tests over broad
  implementation-detail coverage.
- Do not test source text, item ordering, directory layout, or the absence of
  obsolete names.
- Preserve coverage for edge cases, concurrency, failure recovery, replay,
  restart, wire contracts, and public behavior when relevant.
- Existing nonconforming test layout is not authorization for unrelated
  reorganization. Consolidate it only when the file is already in scope.

## Validation

Run validation in proportion to the change and report every command not run.

For every change:

```bash
git diff --check
```

For Rust code changes:

```bash
cargo fmt --all --check
cargo test -p <affected-crate>
cargo check --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Run `cargo test -p beryl-e2e` explicitly for changes to public behavior,
cross-crate orchestration, RPC contracts, persistence, restart, or lifecycle
semantics. Documentation-only changes do not require Cargo validation unless
they change generated artifacts, executable examples, or validation tooling.

## Review and Handoff

- Classify findings as `Blocking`, `Non-blocking`, or `Notes`.
- For each actionable finding, identify the affected symbol or boundary,
  impact, smallest safe correction, and required test.
- Passing tests are regression evidence, not proof that unsupported behavior is
  complete.
- Commit subjects must use `<type>(<scope>): <outcome>` with `feat`, `fix`,
  `refactor`, `test`, `docs`, or `chore` as the type.
- Keep commit messages concise and describe the behavioral or structural
  outcome.
- Do not stage, commit, push, or open a pull request unless the task requests
  that Git operation.
