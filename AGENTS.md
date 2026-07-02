# Vecton Agent Instructions

Vecton is a data access acceleration layer with metadata-controlled routing and client-to-worker data-plane access. The project is still in active design and stabilization: correctness, freshness, epoch/fencing validation, and clear module boundaries matter more than feature breadth.

`AGENTS.md` files are normative execution rules. If a subtree has a stricter `AGENTS.md`, follow it for that subtree.

## Required Reading

Before architectural, dependency-boundary, schema, config, shared-module, or cross-crate changes, read:

- this file
- the local `AGENTS.md` for every touched subtree
- task-specific design or audit documents named by the user

Keep changes inside the requested scope. Do not introduce architecture direction through opportunistic cleanup.

## Repository Boundaries

| Area | Owns | Must not own |
| --- | --- | --- |
| `types` | Pure Rust domain model, typed IDs, stable value validation. | Proto/generated types, product runtime policy, implementation details, test fixtures. |
| `proto` | Wire schema, gRPC contracts, generated modules, structural proto/domain conversion. | Business logic, authority policy, retry/cache policy, worker execution. |
| `common` | Shared infrastructure: canonical errors, headers, config loading mechanics, observability, generic utilities. | Domain dumping ground, module-specific config semantics, runtime state. |
| `client` | Public API, metadata RPC orchestration, worker data-plane orchestration, freshness/epoch validation, retry/refresh/replay behavior, caches and channels. | Metadata authority, worker internals, metadata-free cached direct access unless explicitly designed. |
| `metadata` | Namespace and control-plane truth: inode, dentry, attrs, layout, leases, route ownership, mount routing, guard pipeline, Raft/RocksDB apply semantics. | Worker data execution, client policy, UFS backend behavior. |
| `worker` | Data-plane execution, block store, local IO, stream handling, block reports, heartbeats. | File-level namespace authority, UFS path derivation from data handles, client retry/cache policy. |
| `ufs` | Backend integration, backend config, OpenDAL setup, UFS path behavior, backend capability decisions. | Metadata authority, worker runtime, client policy. |

Shared crates must not depend on product crates. `metadata`, `worker`, and `client` must not depend on each other in production code. `ufs` is a backend adapter crate and must not depend on `metadata`, `worker`, or `client`; `metadata` may depend on `ufs` only at mount/backend adapter boundaries. Test-only dependencies must stay explicit and narrow.

## Core Contracts

- Inode, dentry, attrs, layout, leases, and routing state are metadata-owned authority.
- Paths are external adapters and lookup inputs, not persisted source-of-truth authority.
- The normal data path is client -> metadata -> worker. Metadata issues layout, route, source, lease/fencing, and freshness context before worker access.
- Client-to-worker data-plane access after metadata-issued context must preserve freshness, epoch, fencing, and structured refresh semantics.
- Block is the management, reporting, lifecycle, replication, relocation, and repair unit.
- StorageChunk is the local IO, checksum, bitmap, and materialization unit.
- TransportFrame is the stream/network batching and flow-control unit.
- Stream is the continuous read/write session abstraction.
- `route_epoch`, `mount_epoch`, and `GroupStateWatermark` are separate metadata freshness domains.
- `WorkerRunId` is the worker process-run identity; `block_stamp` is the block data generation identifier.
- Production metadata msync is single-group. Multi-group msync is future work.
- `applied_seq` must not be reintroduced as runtime, storage, snapshot, header, or client state.

## Error Contract

- Recoverable business, protocol, and consistency failures must use structured `CanonicalError` in response headers where the API has that header error channel.
- Transport, auth, and framework failures may use non-OK transport errors with minimal correlation metadata.
- Do not replace machine-usable error class/reason fields with string-only errors.
- Keep policy-specific error mapping local to the owning crate; shared structural conversion belongs in `proto` or `common` only when it has no product policy.

## Code Shape

- Prefer direct, readable, engineering-oriented code with concise names and clear control flow.
- Use concrete types unless a trait or wrapper defines a real crate/backend boundary, owns state, enforces invariants, isolates side effects, or removes meaningful duplication.
- Do not create abstractions for future possibilities.
- Do not introduce builders, managers, contexts, strategies, resolvers, wrappers, parameter structs, or modules unless they encode real semantics or boundaries.
- Inline trivial one-use helpers that only forward parameters, assign fields, build simple values, or wrap `Option`/`Result`.
- Avoid single-implementation traits unless they define a real boundary.
- Keep the main flow readable without forcing readers through helper chains.
- Prefer the narrowest visibility that works.
- Delete obsolete internal paths instead of keeping compatibility aliases or fallback paths.

## Compatibility Policy

Breaking changes are acceptable during current development when they simplify design or remove stale internal contracts. Do not add compatibility bridges, aliases, decode fallbacks, or old/new parallel paths unless the user explicitly requests compatibility or an external consumer requires it.

Schema changes still require wire-number and active-consumer review. Do not reuse or silently change proto numeric values.

## Testing Expectations

- Tests should protect behavior, contracts, invariants, replay semantics, freshness validation, and error mapping.
- Prefer a few focused regression tests over large brittle suites or redundant implementation-detail tests.
- Do not add production APIs, `cfg(test)` production fields, special injection hooks, or fake/force helpers just for tests.
- Keep `#[cfg(test)] mod tests` near the bottom of source files; do not place production logic after test modules.
- Consolidate obvious redundant tests when modifying test-heavy areas.
- Use owner-crate integration tests for observable cross-crate contracts, not as production helper libraries.

## Documentation Expectations

- New or modified comments and docs must be in English unless the user requests another language.
- Comments should explain semantics, invariants, ownership, ordering, or correctness constraints, not restate syntax.
- Production and proto comments must describe current behavior, not PRs, phases, review history, or temporary milestones.
- Update docs only where behavior, schema, config, ownership, dependency direction, or public contract changes.
- Remove or clearly mark stale architecture statements. Do not claim planned features are current behavior.

## Self-Review Checklist

Before handoff, answer these for the actual diff:

- Does this change respect crate boundaries?
- Did it introduce unnecessary abstraction?
- Did it introduce stale compatibility code?
- Did it add test-only logic to production code?
- Are names concise and clear?
- Are comments current and semantic?
- Are errors mapped through the correct contract?
- Are tests behavior-oriented and non-redundant?
- Are docs updated only where relevant?
- Can every new type, trait, or helper be justified in one sentence?

Also verify the local `AGENTS.md` files for touched subtrees were read.

## Self-Check Commands

For code changes, run the relevant subset. The default full validation set is:

```bash
cargo fmt --all
cargo check --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

For documentation-only changes, run at least:

```bash
git diff --check
```

Use `cargo metadata --format-version 1` when dependency or workspace membership claims change. The Rust toolchain baseline is 1.95.0; keep `rust-toolchain.toml`, workspace `rust-version`, local verify entrypoints, and CI aligned with that baseline.
