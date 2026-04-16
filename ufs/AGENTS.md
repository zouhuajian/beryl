# ufs/AGENTS.md

This file applies to `ufs/`.

## 1. Directory purpose

`ufs/` owns integration with external storage backends and LocalUFS-facing persistence backends.

This directory may contain:

- adapters for external backends such as HDFS / S3 / OSS / object/file backends
- LocalUFS-facing persistence integration
- backend capability discovery and mapping
- backend-specific IO bridging that preserves Vecton contracts
- translation between backend operations and Vecton-facing storage abstractions
- backend error normalization where required

`ufs/` is a backend integration layer. It is not the authority for filesystem metadata or client-visible consistency policy.

## 2. What must NOT live here

Do not put the following into `ufs/`:

- authoritative inode / dentry / mount logic
- metadata routing or namespace ownership logic
- client refresh-replay policy
- worker-side block/chunk/stream authority definitions
- protobuf schema ownership
- transport abstraction ownership
- ad hoc policy that changes visible filesystem semantics without a documented contract
- backend-specific shortcuts that bypass route / epoch / fencing validation

A backend adapter must not become an alternate control plane.

## 3. Authority boundary rules

The authority boundary must remain explicit.

Rules:

- filesystem metadata authority remains outside `ufs/`
- backend state must not become the authoritative source of inode/dentry/path truth
- UFS existence, listing, or object naming must not silently override metadata authority
- LocalUFS mode still does not make `ufs/` the metadata authority
- backend capabilities may constrain implementation choices, but they do not redefine Vecton semantics by default

Never “trust the backend” in a way that bypasses Vecton’s own authority model.

## 4. LocalUFS rules

LocalUFS is allowed as a persistence backend, but it must preserve Vecton’s semantics.

Rules:

- LocalUFS is a backend mode, not a reason to weaken consistency contracts
- LocalUFS-backed persistence must still preserve explicit layout/version/identity semantics
- runtime defaults must not reinterpret persisted data incorrectly
- LocalUFS integration must not collapse metadata identity and data-plane identity
- LocalUFS convenience must not bypass block/chunk/stream discipline

LocalUFS changes storage dependency shape, not system correctness requirements.

## 5. External backend mapping rules

Backend adapters must translate backend behavior deliberately.

Rules:

- map backend capabilities into explicit Vecton semantics
- do not assume all backends support rename, append, truncate, consistency, or directory behavior the same way
- capability gaps must be surfaced explicitly, not hidden
- avoid backend-specific behavior leaking into generic interfaces unless the contract says so
- backend object/file naming should not become user-visible filesystem truth unless intentionally modeled that way

When a backend lacks a needed semantic, do not fake it silently.

## 6. Error normalization rules

`ufs/` may normalize backend failures, but must not distort the overall contract.

Rules:

- backend-native errors should be mapped into Vecton-relevant structured categories where appropriate
- do not invent a parallel public error contract
- do not erase actionable backend detail needed for diagnosis
- preserve the distinction between backend transport/infrastructure failures and Vecton-level business/consistency failures
- strings may carry backend detail, but machine behavior must rely on structured meaning

A backend adapter should reduce ambiguity, not introduce it.

## 7. Data-plane and layout rules

`ufs/` must respect the Block / Chunk / Stream model.

Rules:

- do not flatten Block / Chunk / Stream into a backend-native abstraction that loses Vecton semantics
- if backend reads/writes operate on ranges or objects, keep the translation boundary explicit
- backend transfer granularity is not automatically the same as storage chunk size
- backend object boundaries are not automatically the same as block boundaries unless the design explicitly says so
- persisted layout interpretation must stay versioned and self-describing where required

A convenient backend abstraction is not a license to erase data-plane structure.

## 8. Replication / movement / repair rules

Backend integration must not weaken move/repair safety.

Rules:

- backend-assisted transfer must preserve copy + verify + evict style safety when the system contract requires it
- do not delete or overwrite authoritative data before verification completes
- partial readiness / chunk-validity semantics must stay explicit where relevant
- backend operations used in relocation/repair must keep failure modes machine-usable
- do not re-encode or reinterpret data unexpectedly during opaque relocation flows

Safety comes before backend-specific optimization.

## 9. Coding rules for this directory

- isolate backend-specific logic from generic backend traits/interfaces
- keep capability detection explicit
- use typed capability/config/state models rather than loose maps where feasible
- avoid hidden fallback behavior across backends
- document semantic mismatches clearly in code comments when unavoidable
- keep LocalUFS-specific logic explicit instead of mixing it invisibly with remote backend code

## 10. Tests required for changes here

A meaningful `ufs/` change should usually include the relevant subset of:

- backend capability mapping tests
- error normalization tests
- LocalUFS persistence interpretation tests where applicable
- range/object translation tests
- rename/append/truncate behavior tests where supported
- negative tests for unsupported backend semantics
- replication/repair path tests when backend IO participates in those flows
- restart/reopen tests for persisted backend-facing state where relevant

Tests should prove semantic preservation, not only successful backend IO.

## 11. Pre-merge checklist

Before submitting a `ufs/` change, verify:

- did I keep metadata authority outside `ufs/`?
- did I allow backend behavior to redefine filesystem truth?
- did I preserve Block / Chunk / Stream semantics?
- did I make LocalUFS look like a correctness shortcut?
- did I surface backend capability gaps explicitly?
- did I preserve the repo-wide error boundary?
- did I weaken copy/verify/evict or relocation safety?
- did docs/tests stay aligned with the contract?