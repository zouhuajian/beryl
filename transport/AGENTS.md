# transport/AGENTS.md

This file applies to `transport/`.

## 1. Directory purpose

`transport/` defines the network transport abstraction and its concrete adapters.

This directory owns:

- transport traits/interfaces
- request/response streaming mechanics
- connection lifecycle management
- deadlines, timeouts, cancellation, and backpressure at transport level
- buffer/bytes handling for efficient IO
- protocol adapters such as gRPC and future transport implementations
- transport-level metadata propagation where contractually required
- uniform transport semantics across supported network protocols

`transport/` is a carrier of semantics, not a business authority layer.

## 2. What must NOT live here

Do not put the following into `transport/`:

- inode/dentry/mount authority logic
- metadata routing policy as authoritative business logic
- worker block/chunk storage interpretation
- client refresh-replay policy
- repair/replication orchestration decisions
- protobuf schema ownership
- ad hoc business rules tied to one specific service
- filesystem semantics disguised as transport convenience logic

If a rule depends on filesystem or data-plane business authority, it likely belongs outside `transport/`.

## 3. Core role: unify transport semantics, not business semantics

The transport layer must provide uniform behavior across concrete protocols.

Rules:

- preserve shared semantics for timeout, cancellation, backpressure, streaming, and error surfacing across supported transports
- keep business decisions outside transport interfaces unless the contract explicitly requires a shared carrier field
- transport abstractions should be minimal but semantically strong
- do not leak protocol-specific quirks upward as the default model

The purpose of `transport/` is to normalize network behavior, not to become the system’s control plane.

## 4. Error boundary rules

Transport must preserve the repo’s error boundary.

Rules:

- transport/framework/auth-level failures should surface as non-OK transport failures
- business/protocol/consistency failures must not be reclassified into transport failures
- do not invent a second business error channel inside transport adapters
- preserve structured headers/metadata needed by upper layers to interpret recoverable failures
- do not swallow correlation metadata that helps diagnose non-OK failures

Transport should carry recoverable structured business errors intact, not reinterpret them.

## 5. Streaming rules

Streaming behavior is central to the data plane.

Rules:

- stream open/setup may carry fuller context
- subsequent frames should remain minimal and efficient
- do not force per-frame duplication of full routing/session context unless the contract requires it
- keep read/write stream semantics explicit
- preserve ordering, cancellation, close, and flow-control behavior consistently across transports

Transport must not collapse stream semantics into ad hoc request batching.

## 6. Backpressure and concurrency rules

Backpressure is first-class.

Rules:

- use bounded concurrency
- make queueing/backpressure behavior explicit
- do not allow uncontrolled task spawning in hot transport paths
- cancellation must release resources promptly
- avoid holding locks across await points without strong justification
- avoid blocking operations on async hot paths unless isolated correctly

A transport layer that cannot bound load is a correctness and stability risk.

## 7. Buffer and copy rules

Transport is performance-sensitive.

Rules:

- prefer `Bytes` or equivalent zero-copy-friendly buffer handling
- avoid unnecessary buffer concatenation or cloning
- keep payload ownership/mutation rules explicit
- do not introduce convenience APIs that force hidden copies on hot paths
- distinguish transport frame size from storage chunk size and management block size

Transport efficiency should come from explicit buffer discipline, not from unsafe hidden tricks.

## 8. Protocol abstraction rules

Transport abstractions must remain future-proof without becoming vague.

Rules:

- define semantics that can be implemented consistently by gRPC and future transports such as QUIC/RDMA where intended
- do not encode gRPC-only assumptions into the abstract interface unless the repo explicitly standardizes on them
- protocol-specific optimizations must not break the shared contract
- worker/client must be able to derive the correct concrete transport from authoritative endpoint/protocol information

Abstraction is useful only if semantics stay stable across implementations.

## 9. Metadata propagation rules

Transport may carry metadata required by upper layers, but it must not own business meaning.

Rules:

- request identifiers, correlation ids, deadlines, and transport metadata may be propagated here
- shared header or per-call metadata should remain aligned with the higher-level wire contract
- do not duplicate business fields into transport metadata just for convenience
- do not turn transport-specific metadata into the authoritative source of routing/business truth

## 10. Coding rules for this directory

- keep traits small and semantically explicit
- isolate protocol-specific code from shared transport contracts
- keep timeout/cancellation/backpressure behavior easy to audit
- prefer typed configuration over loose option bags
- make lifecycle boundaries explicit: connect, open, send, receive, close, cancel
- comments should explain transport invariants, buffering behavior, and error boundaries

## 11. Tests required for changes here

A meaningful transport change should usually include the relevant subset of:

- timeout behavior tests
- cancellation propagation tests
- bounded concurrency/backpressure tests
- streaming open/frame/close behavior tests
- metadata/header propagation tests
- non-OK transport failure surfacing tests
- preservation of structured business error payload/header tests
- minimal-copy/buffer ownership tests where practical
- protocol-adapter parity tests where multiple implementations exist

Tests should validate semantics, not only that bytes moved from A to B.

## 12. Pre-merge checklist

Before submitting a transport change, verify:

- did I keep transport semantics separate from business semantics?
- did I preserve the transport-vs-business error boundary?
- did I weaken cancellation, timeout, or backpressure behavior?
- did I introduce hidden copies or buffer concatenation on hot paths?
- did I mix storage chunk semantics with transport frame semantics?
- did I leak protocol-specific assumptions into the abstract contract?
- did I preserve header/metadata propagation required by upper layers?
- did docs/tests stay aligned with the contract?