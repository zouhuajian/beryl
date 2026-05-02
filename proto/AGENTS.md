# proto/AGENTS.md

This file applies to `proto/`.

## 1. Directory purpose

`proto/` defines wire contracts only.

This directory owns:

- protobuf message schemas
- shared request/response header schemas
- service RPC contracts
- enum / oneof / field-level contract modeling
- compatibility/version comments where intentionally required

This directory does **not** own domain behavior or business logic.

## 2. What must NOT live here

Do not put the following into `proto/`:

- service implementation logic
- routing algorithms
- metadata authority logic
- client retry / refresh code
- worker-local storage interpretation
- business helper methods that belong in Rust domain crates
- duplicated shadow models that compete with `types/`

If a concept is only needed in runtime Rust logic and not on the wire, it likely belongs outside `proto/`.

## 3. Core wire contract rules

### 3.1 One error channel

Vecton uses one wire-level structured recoverable error model.

Rules:

- business / protocol / consistency / refreshable failures must be carried through gRPC OK + structured response header error
- transport / auth / framework failures use non-OK gRPC status
- do not invent parallel error fields on individual responses unless the protocol contract explicitly requires it
- do not rely on free-form error strings for machine behavior

### 3.2 Common headers first

Stable shared request/response metadata belongs in shared header protos.

Rules:

- shared fields go to common header protos, not duplicated in each RPC
- data-plane open/setup messages may carry fuller context
- subsequent stream frames should stay minimal
- do not duplicate header fields into inner payloads for convenience

### 3.3 Structured recoverability

If a caller can recover, the wire contract must make recovery machine-usable.

Rules:

- use structured enums / fields for refresh reasons and retry hints
- include epoch / version / route / leader / redirect-like data only when contractually needed
- avoid encoding recoverability into strings or comments only

## 4. Identity rules

Proto must preserve semantic identity separation.

Rules:

- do not conflate `inode_id`, `data_handle_id`, and `file_handle`
- do not add a generic `id` field where a domain-specific identity is required
- `BlockId`-related messages must remain aligned with data-plane identity semantics
- if multiple identities appear in one message, each must have a clear role

If you touch identity fields, audit:

- request messages
- response messages
- stream open messages
- metadata-to-worker messages
- client cache key assumptions
- existing docs/tests

## 5. Field design rules

- prefer explicit typed fields over stringly-typed maps
- prefer enums / oneof over boolean combinations
- avoid “reserved for future” junk fields without a concrete contract reason
- keep names semantically exact
- add units to numeric field names where ambiguity exists, such as `_ms`, `_bytes`
- avoid duplicate extent/range/location payloads across nested messages

Do not add fields just to mirror internal structs.

## 6. Compatibility policy

Default policy for internal Vecton proto evolution:

- breaking changes are allowed when they simplify the model
- remove conflicting old fields instead of keeping dual semantics
- do not keep compatibility bridges unless explicitly requested
- when removing or renaming fields, update all callers and tests in the same change

If a compatibility bridge is intentionally required, document:

- why it exists
- who still depends on it
- when it will be removed

## 7. Naming rules

- use consistent `Proto` suffix if that is the repo-wide proto naming convention
- enum names and values should reflect domain semantics, not implementation details
- avoid overloaded names like `version`, `epoch`, `state`, `status` without a clear domain qualifier
- prefer `route_epoch`, `worker_epoch`, `mount_epoch`, `file_version` style specificity
- External/public filesystem RPC names should reflect HCFS-facing operations, not low-level POSIX syscall names.
- Use `Delete` as the public path deletion operation. Do not reintroduce public `Unlink` / `Rmdir` RPCs unless the external contract explicitly changes.
- Avoid names like `DeletePath` when the service and request contract already make the path-facing nature clear.
- Internal/domain mutation names may still use precise terms such as `Unlink` and `Rmdir` when they model distinct metadata semantics.

## 8. Comments and documentation inside proto

Proto comments must explain contract semantics, not restate field names.

Good comment topics:

- when a field is required
- whether it is authoritative or advisory
- expected caller/server behavior on mismatch
- whether it participates in refresh / retry / replay logic
- versioning or persistence implications

Bad comment topics:

- obvious restatements
- implementation trivia
- stale migration notes with no owner

## 9. Tests and validation expectations

Any meaningful proto contract change should be accompanied by:

- generated code rebuild
- compilation of all Rust callers
- tests for structured error mapping where relevant
- tests for identity/epoch field propagation where relevant
- removal of obsolete fields/usages in the same change

Do not stop at “proto compiles”.

## 10. Pre-merge checklist

Before submitting a proto change, verify:

- did I create a second wire error path?
- did I duplicate header data into payload data?
- did I mix inode identity, data identity, and session identity?
- did I add fields that only serve one local implementation detail?
- did I preserve Block / Chunk / Stream separation?
- did I remove old semantics instead of keeping a hidden bridge?
- are the docs still aligned with the wire contract?