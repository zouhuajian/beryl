# Architecture Boundaries

## Purpose

This document defines ownership rules for Vecton workspace modules. It is a review aid for shared-module cleanup work: it says where shared Rust domain types, generic infrastructure, protobuf wire contracts, conversion code, validation helpers, and local runtime policy belong.

These rules do not replace local `AGENTS.md` files. If a subtree has a stricter local rule, follow the local rule for that subtree.

## Workspace Dependency Rules

- Dependency direction is one-way from product modules to shared modules: `metadata`, `worker`, `client`, `ufs`, and `integration_tests` may depend on `common`, `types`, and `proto` where appropriate.
- `types` is a pure Rust domain-model crate. It must not depend on `proto`, `metadata`, `worker`, `client`, `ufs`, or `integration_tests`.
- `common` may depend on `types` for stable identifiers and shared primitives, but must not depend on `proto`, `metadata`, `worker`, `client`, or `ufs`.
- `proto` owns generated wire schema and Rust-side proto/domain conversion. It may depend on `types` and `common`; it must not depend on `metadata`, `worker`, `client`, or `ufs`.
- `metadata`, `worker`, and `client` must not depend on each other in production code. Test-only dependencies must stay explicit and narrow.
- `ufs` must not depend on `metadata`, `worker`, or `client`.
- `integration_tests` may depend on production crates to validate end-to-end contracts, but must not become a source of shared production helpers.

## Block Layout, Format, and Store Backend Boundaries

Vecton block storage has three separate layers:

| Layer | Owner | Contract |
| --- | --- | --- |
| `FileLayout` | `metadata` | Logical file-version or data-handle layout: `block_size`, `chunk_size`, `replication`, and `block_format_id`. Client-supplied layout is create-time intent; metadata validates and persists the accepted layout, and the persisted `FileLayout` is authoritative for later planning. Current active writes require `replication == 1`; durable multi-replica write is future work. |
| `BlockFormat` | `metadata` selects, `worker` persists | Vecton block data/meta interpretation format. Workers persist full `FileLayout.block_size` in `BlockMeta.format.block_size`, persist the selected chunk size and format id, and publish tail or bounded valid length in `BlockMeta.source.effective_block_len`. Recovery and local reads interpret blocks from persisted `BlockMeta`, not runtime defaults. |
| `StoreBackend` / `IoEngine` | `worker` | Worker-local byte execution implementation such as filesystem IO, io_uring, SPDK, mmap, memory, or another engine. Metadata placement may consider supported `BlockFormat` capability, but must not inspect or control the local engine. |

SPDK, io_uring, and filesystem IO are worker `StoreBackend` / `IoEngine` choices, not `BlockFormatId` values. The same `BlockFormat` may run on different engines, and one engine may support multiple formats. For example, the current `FULL_EFFECTIVE` block format can be implemented through filesystem IO or io_uring without changing persisted interpretation. SPDK is also an engine/backend choice if it preserves the same `BlockMeta` plus block payload interpretation.

A new `BlockFormat` is needed when historical local block data cannot be interpreted with the old rules. Examples include:

- One logical block no longer maps to one independent block file.
- Packed small blocks share a physical segment and require a pack index.
- Fixed-offset addressing changes to an extent table.
- Compression or encryption changes logical-to-physical offset mapping.
- Checksum or bitmap layout changes in a non-backward-compatible way.
- Raw device or SPDK layout stops preserving the same `BlockMeta` plus block payload interpretation.

A new `BlockFormat` is not needed for:

- Changing default `block_size`.
- Changing default `chunk_size`.
- Changing worker `IoEngine` from filesystem IO to io_uring.
- Enabling SPDK while preserving the same persistent block interpretation.
- Adding optional diagnostics or statistics.
- Changing placement or eviction policy.

If a future "package block" or "packed small file block" design means multiple logical blocks share a physical pack file and require a pack index, it is a future `BlockFormat`, not merely an `IoEngine`.

Production reads remain `FileLayout` and file-extent authoritative. `GetBlockLocations` and `OpenFile(include_locations=true)` resolve candidates from in-memory reported block locations, filter them through live group-scoped worker registration and heartbeat state, and order eligible replicas with `PlacementPlanner(Read)`. Reported locations are soft state: they do not mutate `FileLayout`, and an existing block range may be returned with an empty worker list when no live reported replica is eligible.

The regular data path is metadata issued: client create/open/append/add-block/read-location/delete decisions go through metadata before the client contacts workers. Client cache-only, metadata-less worker access is not part of the current contract. Metadata delete removes logical namespace and layout visibility; physical worker block cleanup is deferred to explicit worker-command and cleanup flows.

## Shared Module Ownership

### `common`

#### Owns

- Generic infrastructure used across modules: error framework, request/response header domain types, config file loading and flattening, env-key mapping, retry/time/path utilities, and observability primitives.
- Canonical recoverable error categories that are independent of one service implementation.
- Generic validation helpers only when the rule is independent of metadata, worker, client, UFS, or test policy.

#### Does Not Own

- Module-specific runtime state, schedulers, caches, stores, clients, retry policy, write-session policy, repair policy, or stream execution state.
- Worker-local block metadata, metadata authority DTOs, client planner state, or UFS backend behavior.
- Protobuf-generated types, schema-specific codecs, or gRPC service adapters.
- Complete typed module config objects when the config is owned by one module.

### `types`

#### Owns

- Stable Rust domain models shared by multiple production modules.
- Typed identifiers and value objects such as inode, block, chunk, stream, lease, worker, shard group, mount, and state watermark identities.
- Cross-module domain concepts when they are not protobuf-specific and are not tied to one runtime: worker endpoint information, read/write block location, write target, committed block, byte range, fencing token, worker epoch, block stamp, and group state watermark.
- Shared validation helpers for these domain concepts when validation is pure and does not choose business policy.

#### Does Not Own

- Protobuf messages, generated structs, wire enum values, or service traits.
- Metadata authority internals, worker store/runtime internals, client retry/replay/cache policy, UFS adapter internals, or integration-test fixtures.
- Persistence-engine state machines, network transports, schema code generation, or compatibility adapters.
- Placeholder abstractions that are not used by active runtime paths.

### `proto`

#### Owns

- Protobuf files and generated Rust modules.
- gRPC service contracts and on-wire messages.
- Wire enum numeric values and compatibility review for schema changes.
- Rust-side conversion between generated proto types and `types`/`common` domain types.
- Schema-local codecs where the persisted or transported payload is protobuf and the conversion is purely structural.

#### Does Not Own

- Business policy, retry policy, authority decisions, route refresh decisions, cache behavior, worker scheduling, storage execution, or UFS semantics.
- Local runtime-only structs whose shape is driven by one module implementation.
- Compatibility bridges unless a real external compatibility requirement is documented.

## Local Module Ownership

### `metadata`

#### Owns

- Filesystem metadata authority: inode, dentry, attrs, mount state, leases, write sessions, `FsCore`, Raft state machine, worker membership, and maintenance routing.
- WorkerId is a stable worker identity only. Metadata-group-specific worker descriptor, registration, runtime, liveness, routing, placement, report, delete, and repair state must be looked up by `(ShardGroupId, WorkerId)`. Production code must not infer a metadata group from WorkerId alone, fall back to group 1, or treat report-derived group uniqueness as authoritative unless the report state has already been validated in a group-scoped domain object. `WorkerRunId` remains live-only state tied to group registration and heartbeat.
- Authority-side request/result DTOs that are not exposed as stable shared contracts.
- Decisions about owner group, route freshness, lease renewal, block allocation, and committed metadata publication.

#### Shared Candidates

- Stable DTOs returned to clients or workers: write target, committed block, file block location, worker endpoint hint, fencing token presentation, and group state watermark conversion.
- Structural validation helpers for metadata-produced shared DTOs.

#### Must Stay Local

- State-machine internals, authority routing policy, delete/repair/GC scheduling, write-session planning internals, and metadata service adapter logic.

### `worker`

#### Owns

- Data-plane execution: local block store, chunk IO, checksum/repair execution, stream runtime, data service adapters, worker net server, and worker peer client.
- Worker-local persisted metadata implementation and validation.
- Runtime state transitions for staging, ready, corrupt, readable, writable, and publishable local blocks.

#### Shared Candidates

- Stable worker endpoint domain values and endpoint capability/protocol parsing rules.
- Pure block/chunk/stream value objects that are used outside the worker runtime.
- Structural conversion helpers for worker data headers, byte ranges, and fencing tokens.

#### Must Stay Local

- Store path layout, filesystem sync behavior, chunk mapping execution, stream state machine, net listener implementation, local block recovery, and repair execution policy.

### `client`

#### Owns

- SDK behavior, routing cache, worker endpoint cache, refresh/replay classification, retry decisions, planner behavior, and data-plane adapter orchestration.
- Client-side validation of metadata and worker responses before use.

#### Shared Candidates

- Domain models for metadata-returned layout information before data-plane execution: file block location, write target, committed block, and worker endpoint info.
- Pure validation helpers for worker endpoint values, byte ranges, fencing tokens, and state watermarks.

#### Must Stay Local

- Replay policy, cache eviction and health policy, attempt scheduling, endpoint invalidation policy, and SDK-facing error classification.

### `ufs`

#### Owns

- External backend integration, backend-specific config, OpenDAL adapter setup, UFS capability probing, and LocalUFS-facing path behavior.

#### Shared Candidates

- Only stable logical identifiers or simple value objects already used by multiple modules.

#### Must Stay Local

- Backend-specific config maps, storage-service construction, file path mapping, adapter error mapping, and backend capability decisions.

### `integration_tests`

- Owns end-to-end fixtures, mock servers, and contract assertions.
- May use raw proto messages to validate wire contracts.
- Must not provide production shared modules, runtime helpers, or canonical conversion code.

## Where Conversion Logic Belongs

- Proto-to-domain and domain-to-proto conversions belong in `proto` when the conversion is structural and shared by more than one module.
- Conversions that require module policy stay local. Examples: client retry classification, metadata authority redirect policy, worker store state transitions, and UFS backend error interpretation.
- Local adapters may call shared conversion helpers, but should not duplicate ID, header, canonical error, refresh reason, endpoint, fencing token, byte range, or watermark parsing when a shared helper exists.
- Avoid converting directly from generated proto messages deep inside business logic. Convert at service boundaries or adapter boundaries.

## Where Validation Logic Belongs

- Pure value validation belongs with the value owner. For shared value objects, put it in `types` or a `proto` conversion helper if it is wire-specific.
- Business validation belongs in the module that owns the decision. Metadata validates authority and filesystem semantics; worker validates local execution and store invariants; client validates received layouts and retry safety; UFS validates backend-specific config.
- Validation helpers must not hide policy. If a helper decides retry, refresh, lease, route, or repair behavior, it is not generic validation.

## Proto Schema Rules

- `proto` owns wire schema, numeric enum values, service contracts, generated Rust modules, and schema compatibility notes.
- Schema changes must identify active consumers, generated Rust references, and external compatibility risks before deleting or reusing fields.
- Do not add a proto message merely because two Rust modules have similar local structs. First decide whether the shared concept is a Rust domain model, a wire contract, or a local implementation detail.
- Do not encode business policy as free-form strings when a stable enum or structured value is required.
- Stale schemas should be deleted or clearly marked inactive only after current consumers and compatibility requirements are known.

## Config Ownership Rules

- `common` owns generic config loading, flattening, environment key mapping, and shared low-level config primitives.
- A module owns its typed config structs, defaults, validation, and module-specific key semantics.
- Shared config constants belong in `common` only when the key is stable, cross-module, and not tied to one module runtime.
- Do not centralize every module key in `common` by default.

## Review Checklist for New Definitions

- Is this value a stable Rust domain concept used by multiple production modules? Put it in `types`.
- Is this value only an on-wire schema or gRPC service message? Put it in `proto`.
- Is this generic infrastructure with no module policy? Put it in `common`.
- Is this value tied to metadata authority, worker execution, client replay/cache policy, UFS adapter behavior, or test fixtures? Keep it local.
- Does conversion touch only wire/domain shape? Put it in `proto` conversion code.
- Does conversion choose retry, refresh, authority, storage, or compatibility policy? Keep that policy local and call shared structural helpers.
- Would moving the definition force `types` or `common` to depend on `proto`, `metadata`, `worker`, or `client`? Do not move it there.
- Are numeric proto values or external consumers involved? Treat it as a schema compatibility review before changing it.

## Anti-Patterns

- Keeping raw proto messages as long-lived client, metadata, or worker business-domain state.
- Reimplementing ID, header, canonical error, refresh reason, endpoint, byte range, fencing token, or watermark conversion in each module.
- Placing worker-local store/runtime state in `types`.
- Placing metadata authority DTOs or client retry policy in `common`.
- Adding compatibility aliases for internal-only stale types instead of deleting stale code in the cleanup PR that owns the deletion.
- Treating test fixtures as production shared abstractions.
- Letting `common` become a dumping ground for every config key or module default.

## Migration PR Map

- PR-1: shared conversion, parser, and validation foundation. This includes ID/header/error/refresh/watermark/fencing/byte-range/endpoint conversion cleanup and opt-in pure validation helpers.
- PR-2: shared Rust domain model consolidation. This includes worker endpoint info, file block location, write target, committed block, and stable block/epoch/stamp value helpers where they are truly cross-module.
- PR-3: stale schema and stale shared type cleanup. This includes old proto surfaces, obsolete `types` abstractions, task ack error classification cleanup, refresh taxonomy cleanup, and config ownership cleanup.
