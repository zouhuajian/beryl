# worker/AGENTS.md

This file applies to `worker/`.

## 1. Directory purpose

`worker/` is the data-plane execution layer.

This directory owns:

- direct client→worker data path behavior
- block/chunk/stream execution
- worker-local block storage interpretation and access
- read/write data validation using route/version/epoch/fencing context
- replication / relocation / repair data transfer endpoints
- worker block lifecycle transitions
- block report production and worker-side readiness for metadata coordination
- worker-local enforcement of stale-route / stale-epoch detection

The worker is a data-plane executor, not a filesystem metadata authority.

## 2. What must NOT live here

Do not put the following into `worker/`:

- inode/dentry/path authority logic
- mount ownership resolution logic as a source of truth
- client-side refresh-replay policy
- protobuf schema ownership
- generic transport abstraction ownership
- generic shared helpers that belong in `common/`
- metadata-side raft/state-machine logic
- hidden cache-only shortcuts that bypass validation fields

The worker may validate metadata-derived context, but it must not become the metadata authority.

## 3. Core data-plane model: Block / Chunk / Stream

The distinction is mandatory.

### 3.1 Block

Rules:

- Block is the sole management / reporting / replication / migration / repair scheduling unit
- administrative operations are block-based
- block reports and placement/repair semantics must stay block-oriented
- do not split management authority across finer units

### 3.2 Chunk

Rules:

- Chunk is the physical IO / checksum / repair granularity
- partial readiness and transfer behavior may operate at chunk level
- do not treat transport frames as storage chunks
- do not collapse chunk semantics into arbitrary byte ranges without preserving correctness

### 3.3 Stream

Rules:

- Stream is the continuous read/write abstraction
- stream open/setup may negotiate runtime context
- post-open frames should stay minimal and efficient
- stream semantics must preserve flow control/backpressure correctness

Do not flatten Block, Chunk, and Stream into one concept.

## 4. Validation and consistency rules

Worker-side validation is mandatory.

Rules:

- direct read/write paths must validate relevant route/version/epoch/fencing context
- stale route or stale epoch must not silently proceed
- stale writer must be rejected when fencing/lease rules require it
- recoverable mismatches must produce structured refreshable errors
- do not replace semantic validation with best-effort retries

A worker that “tries anyway” on stale context is wrong.

## 5. Local storage interpretation rules

Worker storage must be interpreted from persisted block metadata, not runtime guesses.

Rules:

- persisted block metadata is the source of truth for block interpretation
- layout-relevant fields such as block size, chunk size, checksum algorithm, version/layout markers must come from persisted metadata where required
- runtime defaults may be used to create new blocks, not reinterpret old ones
- tail block / tail chunk semantics must remain correct
- sparse/local layout behavior must not be broken by convenience rewrites

Do not assume the current node default config can reinterpret historical block data.

## 6. Replication / repair / relocation rules

Worker participates in opaque block movement.

Rules:

- block relocation/replication is opaque block transfer, not re-encoding
- only valid/ready chunks should be transferred when the contract requires partial readiness semantics
- move semantics should preserve copy + verify + evict style safety when defined by the contract
- verification must use structured, machine-usable outcomes
- do not make destructive actions irreversible before verification succeeds

A move path that can lose data is unacceptable.

## 7. Error rules

Worker must use the shared structured error contract.

Rules:

- business / protocol / consistency / refreshable failures use gRPC OK + structured response header error or the defined data-plane equivalent
- transport/framework failures use non-OK gRPC status
- stale route, worker epoch mismatch, fencing mismatch, block stamp/layout mismatch, and similar recoverable cases must be machine-usable
- do not invent worker-local parallel error vocabularies on the wire

Strings are for humans. Structured fields are for clients.

## 8. Performance and transport-adjacent rules

Worker is on the hot path. Preserve data-path discipline.

Rules:

- prefer zero-copy or minimal-copy data flow where supported by the surrounding contract
- avoid unnecessary buffer concatenation
- keep backpressure explicit
- bounded concurrency is preferred over unbounded task spawning
- do not hold locks across await points without strong justification
- keep the distinction between storage chunk size and network frame size explicit

Do not trade away correctness for micro-optimizations, but do not introduce avoidable hot-path inefficiency.

## 9. Runtime vs authoritative state rules

Worker has both authoritative local facts and transient runtime signals.

Rules:

- authoritative block-local facts should be represented explicitly
- transient runtime health/load/activity should not be treated as durable truth
- worker reports/presence hints must not become silent alternate authority
- block report and readiness gates must respect the broader system convergence contract

A transient local observation is not automatically globally authoritative.

## 10. Coding rules for this directory

- keep validation logic close to IO entrypoints
- keep block/chunk layout interpretation explicit
- avoid helpers that blur read/write/session/repair semantics
- separate hot-path code from administrative/background code where practical
- delete stale bridge logic instead of preserving dual behavior
- comments should explain invariants such as fencing, route validation, chunk readiness, and copy/verify/evict safety

## 11. Tests required for changes here

A meaningful worker change should usually include the relevant subset of:

- block/chunk/stream boundary tests
- tail block / tail chunk tests
- stale route / stale epoch rejection tests
- fencing rejection tests
- worker epoch mismatch tests
- persisted block metadata interpretation tests
- relocation / replication copy+verify+evict tests
- partial ready-chunk transfer tests where applicable
- restart/reopen tests for persisted local state
- backpressure / bounded concurrency behavior tests where relevant

Tests should assert structured semantic outcomes, not only generic success/failure.

## 12. Pre-merge checklist

Before submitting a worker change, verify:

- did I preserve Block / Chunk / Stream separation?
- did I rely on runtime defaults to interpret persisted data?
- did I weaken route / epoch / fencing validation?
- did I introduce a second wire error vocabulary?
- did I make relocation/repair less safe?
- did I confuse storage chunks with network frames?
- did I add hidden fallback behavior on stale context?
- did docs/tests stay aligned with the contract?