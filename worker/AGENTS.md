# worker/AGENTS.md

This file applies to `worker/`.

## 1. Directory purpose

`worker/` is the data-plane execution layer.

This directory owns:

- direct client-to-worker block-local data path behavior
- worker data-plane RPC adapters and worker core orchestration
- stream open/read/write/commit/abort execution
- worker-local block storage interpretation and access
- local block read/write validation using metadata-derived context
- worker-local enforcement of block stamp and fencing constraints
- local block recovery and future block report production
- future block-level replication, relocation, repair, and maintenance execution

The worker is a data-plane executor, not a filesystem metadata authority.

## 2. What must NOT live here

Do not put the following into `worker/`:

- inode/dentry/path authority logic
- file-level layout authority
- mount ownership resolution logic as a source of truth
- client-side refresh/replay policy
- metadata-side placement decisions
- metadata-side Raft/state-machine logic
- generic transport abstraction ownership
- generic shared helpers that belong in `common/` or `types/`
- hidden cache shortcuts that bypass block validation
- UFS path inference from `data_handle_id` or `block_id`

The worker may validate metadata-derived context, but it must not become the metadata authority.

## 3. Core data-plane model: Block / StorageChunk / TransportFrame / Stream

The distinction is mandatory.

### 3.1 Block

Rules:

- Block is the sole management, reporting, replication, migration, eviction, and repair task unit.
- Administrative operations and future maintenance workflows must stay block-oriented.
- Block reports and placement/repair semantics must not be split across finer units.
- A worker-local block is addressed by metadata-derived context such as `group_id + block_id`.

Do not make chunk/page/segment state an alternate management authority.

### 3.2 StorageChunk

Rules:

- StorageChunk is the worker-local read/buffer/future checksum granularity.
- StorageChunk is not a repair task unit.
- StorageChunk is not a transport frame.
- For the current `FullBlockFileStore`, StorageChunk is not a persisted readiness unit.
- The current store does not persist ready/corrupt bitmaps or committed-length prefixes.
- Future partial-cache implementations may add chunk state, but that must be isolated behind `LocalBlockStore`.

Do not reintroduce partial chunk readiness into `FullBlockFileStore`.

### 3.3 TransportFrame

Rules:

- TransportFrame is the stream payload and flow-control granularity.
- TransportFrame does not define local storage layout.
- TransportFrame does not define StorageChunk size.
- High-frequency read/write frames must remain minimal and header-free.

Do not treat gRPC protobuf frame messages as the worker core's storage model.

### 3.4 Stream

Rules:

- Stream is the continuous read/write session abstraction.
- Open operations bind context, validate metadata-derived fields, and negotiate runtime sizes.
- Read/Write frames carry data using `stream_id` and minimal control fields.
- Stream state owns runtime cursor/sequence/activity, not durable block truth.

Do not flatten Block, StorageChunk, TransportFrame, and Stream into one concept.

## 4. Current local block store contract

The current default `LocalBlockStore` implementation is `FullBlockFileStore`.

Rules:

- `FullBlockFileStore` publishes only complete effective blocks.
- A Ready block must contain the complete effective block range `[0, effective_block_len)`.
- The final `.meta` file is the publication fact.
- A final `.blk` is readable only when the final `.meta` is Ready.
- Final `.meta` may represent Ready or Corrupt only.
- Loading is a staging/runtime state and must not be accepted as final metadata.
- `create_staging_block` must not create final `.meta`.
- `write_at` writes staging data only and must not publish visibility.
- `publish_ready` is the local publication boundary.
- `read_at` must read only final Ready blocks.
- `delete_block` should remove final `.meta` before final `.blk`.

Current `FullBlockFileStore` does not support:

- partial chunk cache
- ready/corrupt bitmap persistence
- committed length persistence
- append
- in-place replace/rebuild
- UFS materialization
- tiering
- replication/repair
- checksum verification

Future store designs must be added as separate `LocalBlockStore` implementations rather than weakening `FullBlockFileStore` invariants.

## 5. Local metadata and format rules

Worker storage must be interpreted from persisted block metadata, not runtime guesses.

Rules:

- persisted block metadata is the source of truth for local block interpretation.
- layout-relevant fields such as format id, block size, chunk size, checksum kind, and effective block length must come from persisted metadata.
- runtime defaults may be used to create new blocks, not reinterpret old ones.
- tail block semantics must be driven by `effective_block_len`.
- `checksum_kind` describes `.blk` StorageChunk data checksum, not `.meta` checksum.
- `.meta` payload uses protobuf as the on-disk schema.
- protobuf-generated types must stay inside the metadata codec boundary.
- store/core public APIs must use Rust domain types, not prost types.
- metadata bytes are not checksummed; correctness relies on atomic replacement, strict decode, and semantic validation.

Do not assume the current worker config can reinterpret historical block files.

## 6. Read path contract

Rules:

- `OpenReadStream` must be block-local.
- `OpenReadStream` must include enough context to locate local storage, including `group_id + block_id`.
- `OpenReadStream` must validate local Ready state, block stamp, and block-local range before registering a stream.
- `block_stamp = 0` on read may mean skip stamp validation only if the protocol/domain contract explicitly allows it.
- missing, not Ready, or Corrupt local block must return a refreshable local-miss result, not fake success.
- block stamp mismatch must return a structured stale result.
- `ReadStream` must read through `LocalBlockStore.read_at`.
- `ReadStream` must advance cursor only after successful reads.
- the final data frame should carry `eos = true`.
- high-frequency read frames must not carry headers.

Do not implement UFS fallback in the store or hide local miss behind implicit materialization.

## 7. Write path contract

Rules:

- `OpenWriteStream` creates a staging block and registers a write stream.
- `OpenWriteStream` must validate fencing token shape and block format shape.
- `block_stamp` is metadata-assigned; worker must not generate or increment it.
- `WriteStream` writes bytes to staging `.blk` only.
- `WriteStream` must validate `seq` and `offset_in_block` according to the supported ordering model.
- the current write path supports sequential frames only.
- `written_through` means the contiguous staging byte prefix written by the worker.
- `written_through` is not local read visibility and not metadata-visible file length.
- `CommitWrite` verifies full effective block coverage before publishing.
- `CommitWrite` calls `publish_ready` and persists the metadata-assigned block stamp.
- `AbortWrite` removes the write stream and staging files.
- Ready blocks must reject ordinary writes.
- append and in-place replace/rebuild are not supported unless explicitly designed.

Do not make staging data readable before final `.meta` is published.

## 8. Validation and consistency rules

Worker-side validation is mandatory.

Rules:

- direct read/write paths must validate metadata-derived block identity and freshness context.
- stale block stamp must not silently proceed.
- stale or malformed fencing token must be rejected where write rules require it.
- recoverable mismatches must produce structured refreshable errors.
- range checks must use checked arithmetic.
- `group_id` must not be guessed, defaulted, scanned, or derived from `block_id`.
- worker must not parse file-level layout.
- worker must not infer UFS source paths from block identity.

A worker that “tries anyway” on stale context is wrong.

## 9. Error rules

Worker must use the shared structured error contract.

Rules:

- low-frequency business/protocol/consistency failures use gRPC OK plus the defined data-plane response header error.
- transport/framework failures use non-OK gRPC status.
- high-frequency stream runtime failures may use transport status where frames do not carry headers.
- stale block stamp, local miss, fencing mismatch, invalid range, and block format mismatch must be machine-usable.
- do not invent worker-local parallel error vocabularies on the wire.

Strings are for humans. Structured fields are for clients.

## 10. Runtime vs authoritative state rules

Worker has both durable local facts and transient runtime state.

Rules:

- final `.meta` is durable local publication state.
- staging files and stream state are transient.
- transient runtime health/load/activity must not be treated as durable truth.
- stream cursor, sequence, written-through, and activity belong to runtime state.
- block reports must report durable local facts, not transient observations.
- readiness gates must respect metadata convergence and block publication semantics.

A transient local observation is not automatically globally authoritative.

## 11. Replication / repair / relocation rules

Worker may later participate in opaque block movement.

Rules:

- block relocation/replication should transfer block-local data according to the store contract.
- `FullBlockFileStore` Ready blocks are transferred as complete effective blocks.
- future partial-cache stores must keep their partial state behind `LocalBlockStore`.
- move semantics should preserve copy, verify, and evict safety when defined.
- verification must use structured, machine-usable outcomes.
- destructive actions must not become irreversible before verification succeeds.

A move path that can lose data is unacceptable.

## 12. Performance and transport-adjacent rules

Worker is on the hot path. Preserve data-path discipline.

Rules:

- preserve zero-copy or minimal-copy data flow where supported by the surrounding contract.
- use `bytes::Bytes` for data payloads.
- avoid unnecessary buffer concatenation.
- keep backpressure explicit.
- bound concurrency instead of spawning unbounded work.
- do not hold locks across await points without strong justification.
- keep StorageChunk size and network frame size explicit and separate.
- service code should stay an adapter; core/store logic must not be duplicated in service.

Do not trade away correctness for micro-optimizations, but do not introduce avoidable hot-path inefficiency.

## 13. Coding rules for this directory

- keep validation logic close to IO entrypoints.
- keep block-local range interpretation explicit.
- keep `service -> convert -> WorkerCore -> runtime/store` boundaries clear.
- avoid helpers that blur read/write/session/repair semantics.
- separate hot-path code from administrative/background code where practical.
- delete stale bridge logic instead of preserving dual behavior.
- avoid compatibility wrappers during active refactor.
- comments should explain invariants such as publication, fencing, block stamp validation, and staging visibility.
- do not use stale wording that contradicts the current store contract.

## 14. Tests required for changes here

A meaningful worker change should usually include the relevant subset of:

- block/store publication tests
- staging unreadability tests
- Ready block read tests
- write stream sequence and offset tests
- abort and uncommitted recovery tests
- tail effective block length tests
- stale block stamp rejection tests
- fencing token rejection tests
- persisted block metadata interpretation tests
- restart/reopen tests for local state
- structured error mapping tests
- stream cursor/eos/cleanup tests
- backpressure or bounded concurrency tests where relevant
- future relocation / replication copy-verify-evict tests when those features are implemented

Tests should assert structured semantic outcomes, not only generic success/failure.

## 15. Pre-merge checklist

Before submitting a worker change, verify:

- did I preserve Block / StorageChunk / TransportFrame / Stream separation?
- did I rely on runtime defaults to interpret persisted block data?
- did I weaken block stamp or fencing validation?
- did I introduce a second wire error vocabulary?
- did I make relocation/repair less safe?
- did I confuse StorageChunk with TransportFrame?
- did I make staging data readable before final publication?
- did I let Loading become final metadata?
- did I let the worker parse file-level layout?
- did I add hidden fallback behavior on stale context?
- did docs/tests stay aligned with the current store and stream contracts?