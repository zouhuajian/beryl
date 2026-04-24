# AGENTS.md

Vecton is a distributed storage / cache acceleration system with filesystem-facing semantics, inode-centric metadata authority, and direct client→worker data path.

This is the repository-wide execution contract.

If a subdirectory contains a more specific `AGENTS.md`, that file overrides this one for files in that subtree.

## Code Style

When changing Rust code in this repository, follow these defaults unless a closer `AGENTS.md` says otherwise:

- use `Vec::with_capacity()` when the size is known or can be estimated well
- wrap large or expensive-to-clone shared fields in `Arc<T>` instead of repeatedly deep-cloning
- use `Box::pin(...)` or `.boxed()`, but never both for the same value
- remove dead code instead of adding `#[allow(dead_code)]`
- implement `Default` on config / options structs instead of standalone `default_*()` helpers
- keep `use` imports at the top of the file unless a local-scope import is clearly necessary
- extract substantial new logic into submodules instead of growing already large files
- delete obsolete internal (`pub(crate)` / private) methods in the same change that introduces their replacements
- choose log levels by audience: `debug!` for routine high-frequency operations, `info!` for operator-visible state changes, `warn!` for unexpected but recoverable conditions

### Tests and module structure

- keep `#[cfg(test)] mod tests` as one block at the bottom of the file; never place production code after it
- do not use `#[path = "..."] mod tests;` in production code
- do not use `#[path = "..."]` to work around normal module organization, except as a clearly temporary refactor step
- if a file must be split, prefer normal directory modules (`mod.rs` plus submodules)
- production module boundaries must be driven by production responsibilities, not test organization

### Visibility and comments

- prefer the narrowest visibility that works: private > `pub(super)` > `pub(crate)` > `pub`
- if a submodule needs deep access to parent internals, treat that as transitional and narrow the dependency surface over time
- all newly added or modified code comments must be in English
- comments should explain invariants, ownership boundaries, lifecycle assumptions, or non-obvious tradeoffs
## 1. Repository-wide invariants

These rules apply everywhere in the repo:

- inode / dentry / attrs are authoritative for filesystem metadata
- path is an adapter, not a persisted source of truth
- Block is the sole management / reporting / replication unit
- Chunk is the physical IO / checksum / repair unit
- Stream is the continuous read/write abstraction
- recoverable business / protocol / consistency failures use gRPC OK + `ResponseHeader.error`
- transport / auth / framework failures use non-OK gRPC status
- direct client→worker paths must preserve route / epoch / fencing validation
- breaking changes are allowed; do not keep compatibility bridges unless explicitly requested

## 2. Directory map

- `common/`: canonical shared primitives only (errors, config, observe, utilities)
- `types/`: authoritative domain model types and typed identifiers
- `proto/`: protobuf wire contracts only
- `metadata/`: metadata authority, mount/inode/dentry, raft state machine
- `transport/`: transport abstraction and adapters
- `worker/`: data-plane execution and block/chunk/stream handling
- `client/`: routing cache, refresh-replay, SDK behavior
- `ufs/`: external backend integration and LocalUFS-facing logic
- `integration_tests/`: end-to-end contract validation

Read the local `AGENTS.md` before changing files in a subdirectory.

## 3. Optional background documents

The `docs/` directory may contain architecture overviews, design notes, and explanatory materials for human readers.

- `docs/` is informative, not normative for AI contributors by default.
- `AGENTS.md` files are the only normative execution instructions for AI contributors in this repository.
- Do not assume files under `docs/` define binding implementation rules unless the relevant rule is also stated in an applicable `AGENTS.md`.

Examples of background materials may include:

- `docs/architecture/...`
- `docs/design/...`
- `docs/notes/...`

## 4. Default execution bias

When requirements are ambiguous, prefer:

- semantic correctness over compatibility
- one authoritative path over dual paths
- typed models over loose maps / strings
- deletion over deprecation for internal-only legacy code
- structured recoverable errors over free-form messages
- persisted self-description over runtime-default reinterpretation

## 5. Required validation

Run the relevant subset before handoff:

```bash
cargo fmt --all
cargo check --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

## 6. Handoff expectations

State clearly:

- what contract changed
- what old path was removed
- what invariants are now enforced
- what tests/docs were updated
- remaining risks or deferred work
