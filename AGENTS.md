# Vecton Agent Instructions

Vecton is a distributed storage and cache acceleration system with filesystem-facing semantics, inode-centric metadata authority, and a direct client-to-worker data path. This file is the repository-wide execution contract for coding agents.

If a subtree has a stricter `AGENTS.md`, follow that local file. Keep changes inside the requested scope. Do not introduce architecture direction through opportunistic cleanup.

## Required reading

Before architectural, dependency-boundary, schema, config, shared-module, or cross-crate changes, read:

- this `AGENTS.md`
- the local `AGENTS.md` for every touched subtree
- `docs/ARCHITECTURE_BOUNDARIES.md`
- task-specific design or audit documents named by the user

`docs/` is informative by default. `AGENTS.md` files are normative unless the user explicitly says otherwise.

## Core architecture rules

- Inode, dentry, and attrs are authoritative for filesystem metadata.
- Path is an adapter, not a persisted source of truth.
- Block is the sole management, reporting, replication, relocation, and repair unit.
- Chunk is the physical IO, checksum, and repair granularity.
- Stream is the continuous read/write abstraction.
- Recoverable business, protocol, and consistency failures use gRPC OK plus `ResponseHeader.error`.
- Transport, auth, and framework failures use non-OK gRPC status.
- Direct client-to-worker paths must preserve route, epoch, and fencing validation.
- Metadata freshness is represented only by repeated `GroupStateWatermark`.
- `GroupStateWatermark` is `{ group_id, state_id: RaftLogId }`; `state_id` means state-machine-applied `RaftLogId`.
- Follower successful responses must not advance the client state cache.
- Production metadata msync is single-group; multi-group msync is future work.
- `applied_seq` must not be reintroduced as runtime, storage, snapshot, header, or client state.
- Do not add removed `applied_seq` snapshot decode fallback.
- `route_epoch`, `mount_epoch`, and `worker_epoch` are separate freshness domains.
- Breaking changes are allowed when requested or necessary. Do not keep compatibility bridges for internal-only stale code unless explicitly required.

## Crate ownership and dependency rules

| Crate               | Owns                                                         | Must not own or depend on                                    |
| ------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| `types`             | Pure shared Rust domain values, typed IDs, stable cross-module value validation. | `common`, `proto`, product crates, generated proto types, runtime policy, test fixtures. |
| `common`            | Generic infrastructure, canonical errors, request/response header domain types, config loading mechanics, observability, generic utilities. | `proto`, product crates, module policy, generated proto types, product runtime state, module-specific config semantics. |
| `proto`             | `.proto` files, generated modules, gRPC contracts, wire enum values, structural proto/domain conversion, schema-local codecs. | Product crates, business policy, retry/replay/cache behavior, authority routing, worker store behavior. |
| `metadata`          | Inode/dentry/attrs, mount state, leases, write sessions, `FsCore`, Raft, worker membership, maintenance routing, metadata config. | `worker` or `client` in production, worker execution, client policy, UFS behavior, duplicate shared conversion. |
| `worker`            | Local block store, chunk IO, checksum/repair execution, stream runtime, data services, worker networking, worker config. | `metadata` or `client` in production, metadata authority, client policy, shared schema ownership. |
| `client`            | SDK behavior, metadata gateway, layout/endpoint caches, retry/replay classification, planner behavior, data adapters, client config. | `metadata` or `worker` in production, metadata authority, worker runtime behavior, shared retry/cache policy. |
| `ufs`               | Backend integration, backend config, OpenDAL setup, UFS path behavior, backend capability decisions. | `metadata`, `worker`, or `client`; unrelated shared production helpers. |
| `integration_tests` | End-to-end fixtures, mock servers, contract assertions, raw wire checks. | Production helpers, canonical conversion code, runtime helpers used by product crates. |

## Definition, conversion, validation, and config ownership

Before adding a struct, enum, trait, function, proto message, config key, helper, or file:

- Put stable Rust domain concepts used by multiple production modules in `types`.
- Put on-wire schema and gRPC contracts in `proto`.
- Put generic infrastructure with no module policy in `common`.
- Keep metadata authority, worker execution, client policy, UFS behavior, and test fixtures local to the owning crate.
- Do not move a definition if doing so would make `types` or `common` depend on `proto` or a product crate.
- Treat numeric proto values and external consumers as schema compatibility review.
- Shared structural proto/domain conversion belongs in `proto`; policy decisions stay local to the owning crate.
- Raw proto messages should be converted at service or adapter boundaries and should not become long-lived business state.
- Pure value validation belongs with the value owner; authority, lease, route, repair, cache, and backend policy validation stays local to the owning crate.
- `common` owns generic config loading mechanics. Each module owns its typed config structs, defaults, validation, and key semantics.
- Do not centralize every module key/default in `common`.
- Do not reinterpret persisted state with runtime defaults.

## Naming and code shape

- Prefer simple, normal engineering names across files, traits, structs, enums, functions, and fields.
- Use the surrounding type/module context and clear English comments to explain semantics instead of encoding every detail into a name.
- Avoid verbose, abstract, or design-document-style names.
- Do not add IDs, epochs, states, errors, traits, managers, routers, planners, helpers, or wrapper types unless they solve a concrete correctness or ownership problem.
- Before adding a new abstraction, check whether an existing domain type or local function expresses the concept clearly.
- Keep code direct, domain-driven, and easy to read.
- Prefer clean replacement over old/new parallel paths. Delete obsolete internal methods in the same change that introduces their replacements.
- Prefer the narrowest visibility that works: private, then `pub(super)`, then `pub(crate)`, then `pub`.
- Keep `use` imports at the top of the file unless a local-scope import is clearly necessary.
- Use `Vec::with_capacity()` when size is known or can be estimated well.
- Wrap large or expensive-to-clone shared fields in `Arc<T>` instead of repeatedly deep-cloning.
- Use `Box::pin(...)` or `.boxed()`, but never both for the same value.
- Implement `Default` on config/options structs instead of standalone default helper functions.
- Remove dead code instead of adding `#[allow(dead_code)]`.
- Extract substantial production logic into normal submodules instead of growing already large files.
- If a submodule needs deep parent access, treat that as transitional and narrow the dependency surface over time.
- Choose log levels by audience: `debug!` for routine high-frequency operations, `info!` for operator-visible state changes, and `warn!` for unexpected but recoverable conditions.

## Comments and documentation

- New or modified code comments and project documentation must be in English unless the user requests another language for a specific artifact.
- Write comments for core structs, enums, traits, functions, protocol fields, invariants, ownership boundaries, lifecycle assumptions, and non-obvious tradeoffs.
- Do not write comments that only restate the code.
- Comments must describe stable semantics, not implementation history.
- Do not mention PR, Phase, review history, temporary milestones, or development process in production comments or proto comments.
- Update docs when behavior, schema, config, ownership, dependency direction, or public contract changes.
- Documentation must describe the current contract, not speculative future architecture.
- Prefer precise tables and checklists over long prose when they are clearer.
- Do not create broad new architecture documents unless requested.

## Testing rules

- Keep `#[cfg(test)] mod tests` as one block at the bottom of the file; never place production code after it.
- Do not put test-only logic in production code.
- Do not add `#[cfg(test)]` to production fields, methods, helper APIs, or production control flow.
- Do not expose test-only production APIs such as special injection, force, fake, or test helper methods.
- Test helpers must stay inside test modules, integration test fixtures, or `cfg(test)` test modules.
- Tests should use real production paths where practical.
- Do not use `#[path = "..."] mod tests;` in production code.
- Do not use `#[path = "..."]` to work around normal module organization except as a clearly temporary refactor step.
- If a file must be split, prefer normal directory modules.
- Shared crate tests should prove semantics, conversion, validation, and dependency boundaries.
- Product crate tests should prove module-owned policy and runtime behavior.
- Integration tests should prove observable cross-crate contracts and must not become production helper libraries.
- Add only core tests that protect correctness boundaries.
- Do not over-test trivial wrappers.
- Delete obsolete tests for removed behavior.
- Merge redundant or overlapping tests instead of adding parallel cases.
- Avoid large test modules unless necessary.

## Anti-patterns

- Moving runtime, policy, or state into shared crates.
- Treating `types` or `common` as dumping grounds.
- Adding proto messages just because two Rust structs look similar.
- Silently changing proto wire numeric values.
- Reimplementing shared structural conversion in each product crate.
- Keeping raw proto messages as long-lived business state.
- Adding compatibility wrappers for internal-only stale APIs unless explicitly required.
- Treating test fixtures as production shared abstractions.
- Letting a documentation-only task change Rust, proto, config, generated, or build files.
- Using arbitrary defaults or any-group fallbacks when an authoritative group, route, lease, or owner is required.

## Validation expectations

The development toolchain baseline is Rust 1.95.0. Keep the root `rust-toolchain.toml`, workspace `rust-version`, local verify entrypoint, and CI workflow aligned to that baseline.

For code changes, run the relevant subset. The default full validation set is:

```bash
cargo fmt --all
cargo check --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

For documentation-only changes, at minimum run:

```bash
git diff --check
```

Use `cargo metadata --format-version 1` when dependency or workspace membership claims change.

## Handoff and review checklist

Before handoff, report exact validation results and verify:

- local `AGENTS.md` files for touched subtrees were read.
- the change stayed inside requested scope.
- dependency direction was preserved.
- new definitions live in the owning crate.
- runtime policy stayed local to the owning product crate.
- proto wire values and service contracts are explicit.
- stale internal paths were deleted rather than wrapped when compatibility is not required.
- tests and docs were updated for changed behavior, schema, config, or ownership.
- naming is concise and not over-abstracted.
- no test-only production APIs were introduced.
- no PR/Phase/development wording appears in production or proto comments.