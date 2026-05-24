# Vecton Agent Instructions

Vecton is a distributed storage and cache acceleration system with filesystem-facing semantics, inode-centric metadata authority, and a direct client-to-worker data path. This file is the repository-wide execution contract for coding agents.

If a subdirectory contains a stricter `AGENTS.md`, follow that local file for files in that subtree. Do not modify code outside the requested scope. Do not introduce new architecture direction through opportunistic cleanup.

## Required reading

Before architectural, dependency-boundary, schema, config, or shared-module changes, read:

- `AGENTS.md`
- the local `AGENTS.md` for every touched subtree
- `docs/ARCHITECTURE_BOUNDARIES.md`
- any task-specific design or audit document named by the user

The `docs/` directory is informative by default; `AGENTS.md` files are normative for coding agents unless the user explicitly says otherwise.

## Repository-wide architecture rules

- Inode, dentry, and attrs are authoritative for filesystem metadata.
- Path is an adapter, not a persisted source of truth.
- Block is the sole management, reporting, replication, relocation, and repair unit.
- Chunk is the physical IO, checksum, and repair granularity.
- Stream is the continuous read/write abstraction.
- Recoverable business, protocol, and consistency failures use gRPC OK plus `ResponseHeader.error`.
- Transport, auth, and framework failures use non-OK gRPC status.
- Direct client-to-worker paths must preserve route, epoch, and fencing validation.
- Metadata state freshness is represented only by repeated `GroupStateWatermark`.
- `GroupStateWatermark` is `{ group_id, state_id: RaftLogId }`.
- `state_id` means state-machine applied `RaftLogId`.
- Follower successful responses must not advance the client state cache.
- Production metadata msync is single-group; multi-group msync is future work.
- `applied_seq` must not be reintroduced as runtime, storage, snapshot, header, or client state.
- Do not add removed `applied_seq` snapshot decode fallback.
- Do not bump snapshot version just to preserve removed `applied_seq` compatibility unless a real external compatibility requirement exists.
- `route_epoch`, `mount_epoch`, and `worker_epoch` are separate freshness domains.
- Breaking changes are allowed when requested or necessary; do not keep compatibility bridges for internal-only stale code unless explicitly required.

## Workspace dependency rules

| Crate | May depend on | Must not depend on |
| --- | --- | --- |
| `types` | External low-level Rust dependencies only. | `common`, `proto`, `metadata`, `worker`, `client`, `ufs`, `integration_tests`. |
| `common` | `types` when needed for stable shared primitives. | `proto`, `metadata`, `worker`, `client`, `ufs`. |
| `proto` | `types`, `common`. | `metadata`, `worker`, `client`, `ufs`. |
| `metadata` | Shared crates where appropriate. | `worker` or `client` in production code. |
| `worker` | Shared crates where appropriate. | `metadata` or `client` in production code. |
| `client` | Shared crates where appropriate. | `metadata` or `worker` in production code. |
| `ufs` | Shared crates where appropriate. | `metadata`, `worker`, `client`. |
| `integration_tests` | Production crates for end-to-end validation. | Production crates must not depend on it. |

## Crate responsibility matrix

| Crate | Role | Owns | Must not own |
| --- | --- | --- | --- |
| `common` | Generic shared infrastructure | Canonical errors, request/response header domain types, config loading mechanics, observability, generic utilities. | Module policy, generated proto types, product runtime state, module-specific typed config. |
| `types` | Pure Rust domain model | Stable cross-module value objects, typed IDs, worker endpoint values, block/location/write domain values, pure shared value validation. | Proto wire values, generated proto types, metadata/worker/client/UFS internals, runtime policy, test fixtures. |
| `proto` | Wire schema and structural conversion | `.proto` files, generated Rust modules, service contracts, wire enum values, proto/domain conversion, schema-local codecs. | Business policy, retry/replay/cache behavior, product runtime behavior, product crate dependencies. |
| `metadata` | Metadata authority runtime | Inode/dentry/attrs, mount state, leases, write sessions, `FsCore`, Raft, worker membership, maintenance routing, metadata config. | Worker execution, client policy, UFS behavior, duplicate structural proto conversion. |
| `worker` | Data-plane runtime | Local block store, chunk IO, checksum/repair, stream runtime, data services, worker networking, worker config. | Metadata authority, client policy, shared schema ownership, shared store/runtime state. |
| `client` | SDK and orchestration runtime | Metadata gateway, layout and endpoint caches, retry/replay classification, planner behavior, SDK policy, data adapters, client config. | Metadata authority, worker store/runtime behavior, generic shared schema, shared retry/cache policy. |
| `ufs` | External backend adapter | Backend integration, backend-specific config, OpenDAL setup, UFS path behavior, backend capability decisions. | Metadata, worker, or client runtime policy; shared helpers for unrelated crates. |
| `integration_tests` | Test-only contracts | End-to-end fixtures, mock servers, contract assertions, raw proto wire checks. | Production helpers, canonical conversion code, runtime helpers for product crates. |

## Crate boundaries

### `common`

#### Allowed

- Generic infrastructure.
- Canonical errors and request/response header domain types.
- Generic config loading, flattening, and environment-key mapping.
- Generic utilities and observability primitives.
- Module-independent validation helpers.

#### Forbidden

- Metadata authority DTOs.
- Worker store or runtime state.
- Client retry, replay, or cache policy.
- UFS backend policy.
- Generated proto types.
- Module-specific typed config ownership.
- Every module key/default as a dumping ground.

### `types`

#### Allowed

- Stable Rust domain models used by multiple production modules.
- Typed IDs and value objects.
- Worker endpoint domain values.
- File block location, write target, committed block.
- Fencing token, block stamp, worker epoch, and state watermark helpers.
- Pure validation for shared values.

#### Forbidden

- Generated proto types or proto wire enum values.
- Metadata authority internals.
- Worker store or runtime state.
- Client retry, replay, or cache policy.
- UFS adapter internals.
- Test fixtures.
- Placeholder abstractions without active runtime use.

### `proto`

#### Allowed

- `.proto` files.
- Generated Rust modules.
- gRPC service contracts.
- Wire messages and numeric enum values.
- Structural proto/domain conversion.
- Schema-local codecs.

#### Forbidden

- Business policy.
- Retry, replay, cache, or endpoint-health decisions.
- Metadata authority routing.
- Worker store or runtime behavior.
- UFS behavior.
- Product crate dependencies.
- Compatibility shims without documented external requirements.

### `metadata`

#### Allowed

- Filesystem metadata authority.
- Inode, dentry, attrs, and mount state.
- Leases and write sessions.
- `FsCore`.
- Raft state machine.
- Worker membership.
- Metadata maintenance routing.
- Metadata typed config and validation.

#### Forbidden

- Worker store execution.
- Client retry, replay, or cache policy.
- UFS backend behavior.
- Shared domain definitions that are stable and cross-module unless they move to `types`.
- Structural proto/domain conversion duplicated when `proto` already owns it.

### `worker`

#### Allowed

- Data-plane execution.
- Local block store.
- Chunk IO.
- Checksum and repair execution.
- Stream runtime.
- Data service adapters.
- Worker net server/client.
- Worker typed config and validation.

#### Forbidden

- Metadata authority policy.
- Client retry, replay, or cache policy.
- Shared domain definitions that belong in `types`.
- Generated schema ownership.
- Store or runtime state moved into `types`.

### `client`

#### Allowed

- SDK behavior.
- Metadata gateway.
- Layout cache and worker endpoint cache.
- Retry/replay classification.
- Planner behavior.
- Data-plane adapter orchestration.
- Client typed config and validation.

#### Forbidden

- Metadata authority policy.
- Worker store/runtime behavior.
- Generic shared schema ownership.
- Long-lived raw proto state when a domain model exists.
- Moving retry, replay, cache, endpoint-health, or SDK error policy into `common`, `types`, or `proto`.

### `ufs`

#### Allowed

- External backend integration.
- Backend-specific config.
- OpenDAL adapter setup.
- UFS path behavior.
- Backend capability decisions.

#### Forbidden

- Metadata, worker, or client runtime policy.
- Shared production helpers for unrelated crates.
- Dependencies on `metadata`, `worker`, or `client`.

### `integration_tests`

#### Allowed

- End-to-end fixtures.
- Mock servers.
- Contract assertions.
- Raw proto messages for wire-contract validation.

#### Forbidden

- Production shared helpers.
- Canonical conversion code.
- Runtime helpers used by product crates.

## New definition rules

Before adding a new struct, enum, function, trait, proto message, config key, or helper:

- If it is a stable Rust domain concept used by multiple production modules, put it in `types`.
- If it is only an on-wire schema or gRPC contract, put it in `proto`.
- If it is generic infrastructure with no module policy, put it in `common`.
- If it is tied to metadata authority, worker execution, client replay/cache policy, UFS adapter behavior, or test fixtures, keep it local.
- If moving it would make `types` or `common` depend on `proto` or a product crate, do not move it there.
- If numeric proto values or external consumers are involved, treat the change as a schema compatibility review.

## Conversion ownership

- Structural proto-to-domain and domain-to-proto conversion belongs in `proto` when shared by more than one module.
- Conversion that chooses retry, refresh, authority, storage, cache, compatibility, or backend policy stays local to the owning module.
- Raw proto messages should be converted at service or adapter boundaries.
- Do not keep generated proto messages as long-lived client, metadata, or worker business state when a domain model exists.
- Do not duplicate ID, header, canonical error, refresh reason, endpoint, fencing token, byte range, or watermark conversion when a shared helper exists.

## Validation ownership

- Pure value validation belongs with the value owner.
- Wire-specific structural validation belongs near `proto` conversion.
- Metadata validates authority, namespace, lease, route, mount, and filesystem semantics.
- Worker validates local execution, block store, stream, checksum, fencing, and block stamp invariants.
- Client validates received layouts, endpoint freshness, retry/replay safety, and SDK-facing behavior.
- UFS validates backend-specific config and capability semantics.
- Validation helpers must not hide retry, refresh, lease, route, repair, or cache policy.

## Config ownership

- `common` owns generic config loading, flattening, environment-key mapping, and shared low-level primitives.
- Each module owns its typed config structs, defaults, validation, and module-specific key semantics.
- Shared config constants belong in `common` only when the key is stable, cross-module, and not tied to one module runtime.
- Do not centralize every module key or default in `common`.
- Do not reinterpret persisted state with runtime defaults.

## Testing expectations

- Keep `#[cfg(test)] mod tests` as one block at the bottom of the file; never place production code after it.
- Do not use `#[path = "..."] mod tests;` in production code.
- Do not use `#[path = "..."]` to work around normal module organization except as a clearly temporary refactor step.
- If a file must be split, prefer normal directory modules (`mod.rs` plus submodules).
- Production module boundaries must be driven by production responsibilities, not test organization.
- Shared crate tests should prove semantics, conversion, validation, and dependency boundaries.
- Product crate tests should prove the module-owned policy and runtime behavior.
- Integration tests should prove observable cross-crate contracts and must not become production helper libraries.
- Do not over-test trivial wrappers; test the contract that can regress.

## Documentation expectations

- All new or modified code comments and project documentation must be in English unless the user requests another language for a specific artifact.
- Update docs when behavior, schema, config, ownership, dependency direction, or public contract changes.
- Documentation must describe the current contract, not speculative future architecture.
- Prefer precise tables and checklists over long prose when they are clearer.
- Do not create broad new architecture documents unless requested.

## Code style

- Use `Vec::with_capacity()` when the size is known or can be estimated well.
- Wrap large or expensive-to-clone shared fields in `Arc<T>` instead of repeatedly deep-cloning.
- Use `Box::pin(...)` or `.boxed()`, but never both for the same value.
- Remove dead code instead of adding `#[allow(dead_code)]`.
- Implement `Default` on config/options structs instead of standalone `default_*()` helpers.
- Keep `use` imports at the top of the file unless a local-scope import is clearly necessary.
- Extract substantial new logic into submodules instead of growing already large files.
- Delete obsolete internal methods in the same change that introduces their replacements.
- Choose log levels by audience: `debug!` for routine high-frequency operations, `info!` for operator-visible state changes, `warn!` for unexpected but recoverable conditions.
- Prefer the narrowest visibility that works: private, then `pub(super)`, then `pub(crate)`, then `pub`.
- If a submodule needs deep access to parent internals, treat that as transitional and narrow the dependency surface over time.
- Comments should explain invariants, ownership boundaries, lifecycle assumptions, or non-obvious tradeoffs.

## Anti-patterns

- Moving runtime, policy, or state into shared crates.
- Treating `types` as a dumping ground for local structs.
- Treating `common` as a dumping ground for every config key, default, helper, or module-specific utility.
- Adding proto messages just because two Rust structs look similar.
- Silently changing proto wire numeric values.
- Reimplementing shared structural conversion in each product crate.
- Keeping raw proto messages as long-lived business state.
- Adding compatibility wrappers for internal-only stale APIs unless explicitly required.
- Treating test fixtures as production shared abstractions.
- Letting a documentation-only task change Rust, proto, config, generated, or build files.

## Validation expectations

The development toolchain baseline is Rust 1.95.0. Keep the root
`rust-toolchain.toml`, workspace `rust-version`, local verify entrypoint, and CI
workflow aligned to that exact baseline.

Run the relevant subset before handoff. For code changes, the default full validation set is:

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

Use `cargo metadata --format-version 1` when dependency or workspace membership claims are changed.

## Review checklist

- Did I read the local `AGENTS.md` files for every touched subtree?
- Did I keep the change inside the requested scope?
- Did I preserve dependency direction?
- Did I put new definitions in the crate that owns the concept?
- Did I keep runtime policy local to the owning product crate?
- Did I keep proto wire values and service contracts explicit?
- Did I delete stale internal paths instead of wrapping them when compatibility is not required?
- Did I update tests and docs for changed behavior, schema, config, or ownership?
- Did I run the relevant validation and report exact results?
