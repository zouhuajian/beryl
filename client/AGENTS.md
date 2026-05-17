# client/AGENTS.md

This file applies to `client/`.

## 1. Directory purpose

`client/` is the routing, session, refresh-replay, and SDK behavior layer.

This directory owns:

- filesystem-facing client request orchestration
- routing cache and route refresh behavior
- state watermark / follower-read gating behavior where applicable
- refresh-replay action logic for recoverable failures
- session-scoped write/open context tracking
- client-side interpretation of structured response headers and recoverable errors
- endpoint selection based on authoritative server-advertised metadata
- SDK ergonomics that preserve system semantics

The client is an orchestrator and validator of server contracts. It is not an authority that invents alternate truth.

## 2. What must NOT live here

Do not put the following into `client/`:

- authoritative inode/dentry/mount logic
- worker-local block storage interpretation
- worker-local net server or peer-client implementation details
- protobuf schema ownership
- metadata raft/state-machine logic
- server-side business rules copied into client-side guessed behavior
- hidden compatibility bridges that paper over broken contracts
- blind retry loops without semantic classification

The client may cache and optimize, but it must not replace server authority.

## 3. Core client responsibility: refresh-replay, not blind retry

Client retry behavior must be semantic.

Rules:

- do not treat all failures as generic retryable failures
- recoverable failures must be interpreted through structured error class / reason / hints
- refresh-replay must be driven by contract, not by string matching
- client behavior should be table-driven or equivalently explicit for each RPC/action category
- retries without refresh on stale route/epoch/leader/session conditions are wrong

A correct client is not a “retry wrapper”; it is a contract-aware executor.

## 4. Routing cache rules

Client routing/cache behavior must remain subordinate to server authority.

Rules:

- route cache is an accelerator, not a source of truth
- route cache invalidation must be tied to structured refresh signals, epochs, or authoritative route versioning
- do not pin stale endpoints after receiving refreshable mismatch signals
- do not keep parallel old/new routing logic alive without explicit migration intent
- endpoint/protocol selection must honor what authoritative metadata or worker registration advertises

The client must prefer an explicit refresh over a best-effort guess when authority has changed.

## 5. State watermark and follower-read rules

Where the system supports follower-read or state-gated reads, the client must enforce the contract.

Rules:

- client-side state watermark tracking must be explicit
- do not issue relaxed reads to followers unless the read eligibility contract is satisfied
- watermark comparison must remain group-scoped and semantically valid
- do not compare state across unrelated raft groups
- metadata leader responses and refresh points must update the correct cached state

A follower read that ignores watermark semantics is a correctness bug.

## 6. Identity and session rules

The client must preserve strict identity separation.

Rules:

- `inode_id` is filesystem authority identity
- `data_handle_id` is stable data-plane/data-version identity
- `file_handle` is a session-scoped open-write handle
- do not collapse routing keys, cache keys, and write-session identity into one field
- keep session/open state explicit when required for fsync/hflush/hsync/close semantics

Any change touching identity must audit:

- request-building code
- session/open tables
- route cache keys
- replay logic
- worker-targeted data-path calls
- response interpretation

## 7. Error interpretation rules

The client is the main consumer of structured recoverable errors.

Rules:

- business / protocol / consistency failures must be interpreted from gRPC OK + structured response header error
- transport / framework failures must remain distinct from business-level recoverable failures
- not-leader, stale route, stale state, worker epoch mismatch, fencing/session problems, and similar recoverable conditions must map to explicit client actions
- do not parse free-form strings to decide replay policy when structured fields exist

Strings may support logging. They must not drive correctness behavior.

## 8. Session and write-path rules

Open/write/flush/sync/close behavior is a high-risk area.

Rules:

- client write-session state must preserve route/epoch/fencing context when required by the contract
- do not silently continue a stale write session after refreshable mismatches
- do not reuse expired or invalid session handles without explicit recovery semantics
- `file_handle` and durable data identity must remain distinct
- replay of write-adjacent operations must preserve idempotency and safety requirements

A client that “tries another worker” without revalidating session safety is wrong.

## 9. Protocol and endpoint selection rules

The client must construct the correct worker data-path behavior from authoritative endpoint information.

Rules:

- do not choose arbitrary worker network protocols when the server advertises the actual supported protocol
- client direct-worker code may use proto-generated stubs or client-local helpers until the client data-plane refactor
- a cached direct-read path must remain subordinate to authoritative route/worker information
- if the direct path becomes stale or mismatched, refresh from metadata before replaying
- do not hardcode fallback behavior that bypasses validation

## 10. Coding rules for this directory

- keep replay policy explicit and auditable
- separate transport failure handling from semantic contract recovery
- keep route-cache updates close to response/error interpretation points
- prefer typed session/routing structs over loose maps
- avoid helper abstractions that hide when refresh happens
- comments should explain replay conditions, invalidation triggers, and safety boundaries

## 11. Tests required for changes here

A meaningful client change should usually include the relevant subset of:

- route cache refresh tests
- not-leader refresh-replay tests
- stale route / mount epoch / worker epoch replay tests
- follower-read watermark gating tests
- group-scoped watermark comparison tests
- write-session invalidation / refresh tests
- fencing/session-expired handling tests
- transport failure vs business failure classification tests
- direct-path stale cache fallback-to-refresh tests

Tests should assert explicit client actions/policies, not only eventual success/failure.

## 12. Pre-merge checklist

Before submitting a client change, verify:

- did I preserve refresh-replay semantics instead of blind retry?
- did I keep route cache subordinate to authority?
- did I preserve group-scoped state watermark logic?
- did I mix inode identity, data identity, and session identity?
- did I make string parsing part of correctness behavior?
- did I add hidden fallback behavior that bypasses refresh?
- did I keep transport failure handling distinct from business-level recoverable failure handling?
- did docs/tests stay aligned with the contract?
