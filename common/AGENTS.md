# common/AGENTS.md

This file applies to `common/`.

## 1. Directory purpose

`common/` contains only repository-wide shared primitives that are stable across modules.

Typical allowed contents:

- canonical error model and shared error helpers
- config primitives and common config utilities
- observability primitives: logging / tracing / metrics context
- shared request / correlation identifiers
- generic utility code with broad multi-module reuse
- versioned low-level shared structs that do not belong to a domain crate

`common/` is **not** a fallback bucket for code that lacks a home.

## 2. What must NOT live here

Do not put the following into `common/`:

- filesystem authority logic
- inode / dentry / mount business rules
- worker execution logic
- transport protocol behavior
- replication / repair orchestration
- routing policy
- client refresh-replay logic
- protobuf-generated code
- domain models that belong in `types/`
- crate-specific helpers used by only one module

If code is specific to metadata, worker, client, transport, or ufs, keep it there.

## 3. Canonical error rules

`common/` owns the shared error vocabulary.

Rules:

- there must be one canonical recoverable error model shared across the repo
- do not introduce parallel wire/business error enums in other crates unless explicitly approved
- recoverable business/protocol/consistency failures must be representable in structured form
- error types must carry machine-usable fields, not only strings
- string messages are secondary; class/code/reason/retry hints are primary

When changing shared error types, review all of these together:

- proto header mapping
- server-side construction
- client-side interpretation
- retry / refresh behavior
- tests for semantic matching

## 4. Dependency discipline

`common/` must remain low in the dependency graph.

Rules:

- prefer zero or minimal dependencies
- do not depend on `metadata`, `worker`, `client`, `transport`, `ufs`, or generated `proto` crates unless the repo intentionally defines that edge
- avoid pulling in heavy crates for narrow convenience
- if a shared utility requires domain knowledge, it probably does not belong here

If a new dependency in `common/` increases coupling across the workspace, assume it is wrong until proven necessary.

## 5. Type design rules

Types in `common/` must be generic and semantic.

Rules:

- use precise names; avoid `Utils`, `Helper`, `ContextData`, `ExtraInfo`, `Meta`
- avoid raw `String` / `u64` where a typed wrapper is warranted and broadly reusable
- keep generic shared structs small and explicit
- prefer enums over magic numbers and string discriminators
- persistent or cross-process shared structs must be version-conscious

Do not create duplicate identity wrappers here if the authoritative domain identity belongs in `types/`.

## 6. Observability rules

`common/` should converge shared observability primitives, not business logging policy.

Rules:

- shared field names should be stable across modules
- prefer structured logging fields over interpolated strings
- request_id / trace_id / call_id / group_id / inode_id / block_id style identifiers should be easy to propagate
- do not hardcode module-specific policy into common observability helpers

Metrics/logging names should be reusable by multiple modules, not secretly tailored to one crate.

## 7. Config rules

Shared config code should define reusable primitives, validation helpers, and defaults policy.

Rules:

- config defaults may help create new runtime state, but must not reinterpret persisted state
- validate invalid combinations explicitly
- do not silently rewrite invalid config
- avoid module-specific config structs here unless they are intentionally shared across multiple crates

## 8. Coding rules for this directory

- keep files small and semantically tight
- delete dead shared abstractions instead of keeping compatibility layers
- prefer `pub(crate)` over `pub` unless the item is intentionally shared across crate boundaries
- avoid “temporary” bridge adapters
- add doc comments for shared primitives whose misuse would cause semantic bugs

## 9. Tests required for changes here

A change in `common/` usually requires:

- unit tests for error/classification mapping
- serialization/deserialization tests for stable structs when applicable
- validation tests for config helpers
- negative tests proving invalid combinations are rejected
- no test should assert only on string text when structured fields are available

## 10. Pre-merge checklist

Before submitting a change in `common/`, verify:

- is this truly cross-module and stable?
- should this actually live in `types/` instead?
- did I accidentally introduce domain/business logic here?
- did I create a second error vocabulary?
- did I add an unnecessary dependency edge?
- did I preserve structured semantics over stringly-typed behavior?