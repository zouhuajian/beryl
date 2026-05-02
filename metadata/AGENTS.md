# metadata/AGENTS.md

This file applies to `metadata/`.

## 1. Directory purpose

`metadata/` is the authoritative filesystem metadata plane.

This directory owns:

- inode / dentry / attrs authority
- mount table and mount routing semantics
- metadata raft state machine
- namespace ownership and group routing
- commit-boundary metadata durability
- metadata-facing path traversal as an adapter over dentries/inodes
- authoritative validation for metadata-level epochs / versions / ownership
- metadata APIs and server-side behavior behind filesystem-facing contracts

This directory is the source of truth for filesystem metadata semantics.

## 2. What must NOT live here

Do not put the following into `metadata/`:

- data-plane block/chunk read/write execution
- worker-local storage layout logic
- transport framing / codec details
- generic shared utilities that belong in `common/`
- protobuf schema ownership
- client-side refresh-replay policy
- opaque cache-only shortcuts that bypass metadata authority
- path-as-authority indexing or duplicated path truth outside inode/dentry model

Metadata may coordinate with other modules, but it must not absorb their responsibilities.

## 3. Authority rules

### 3.1 Inode / dentry / attrs are authoritative

Rules:

- inode, dentry, and attrs are the authoritative filesystem model
- path is resolved by traversal, not by a persisted authoritative path index
- do not reintroduce file-path-based authority for convenience
- do not store or cache alternate truth that can drift from inode/dentry state

A path API is allowed only as an adapter over authoritative metadata structures.

### 3.2 FileSystemService is the external entrypoint

Rules:

- external filesystem-facing behavior must align with the filesystem entrypoint contract
- do not introduce side-channel external APIs that bypass the entrypoint contract
- internal services may exist, but they must not become competing external authority paths
- FileSystemService is client-facing and should expose HCFS-style filesystem operations.
- Public path deletion is represented by `Delete`.
- Do not expose public `Unlink` / `Rmdir` handlers from `path_service`; those names are internal domain mutations only.
- `path_service` may dispatch `Delete` to internal `FsCore::execute_unlink` or `FsCore::execute_rmdir`, but it must remain a path-first adapter over inode/dentry authority.

### 3.3 Rename and namespace rules

Rules:

- same-mount rename must preserve atomicity
- mount ownership must be explicit and stable
- cross-mount semantics must not be hidden behind same-mount assumptions
- do not add shortcuts that undermine namespace consistency

## 4. Mount routing rules

Mount handling is a high-risk area. Preserve explicit semantics.

Rules:

- mount routing follows namespace/mount ownership semantics, not generic hashing
- longest-prefix match must be unambiguous
- child mount prefixes override parent prefixes
- mount changes must carry monotonic versioning such as `mount_epoch` / `config_version`
- ownership, redirect, and refresh semantics must remain machine-usable
- leader/group mismatch must return structured refreshable errors, not opaque failures

Do not “temporarily” route metadata writes by hash when the contract requires mount ownership routing.

## 5. Raft and persistence rules

Metadata uses consensus for authoritative state, but only at the right boundary.

Rules:

- raft is for authoritative metadata state
- write raft at commit boundaries, not per data chunk or per transport frame
- high-frequency runtime chatter must not become raft write traffic
- snapshots/log replay must preserve authoritative metadata interpretation
- persisted metadata state must remain semantically explicit and version-conscious

Do not push transient worker/runtime/load signals through raft without an explicit documented reason.

## 6. Identity rules inside metadata

Metadata must preserve strict identity separation.

Rules:

- `inode_id` is the authoritative filesystem identity
- `data_handle_id` is related data-plane identity and must not replace filesystem authority
- `file_handle` is session-scoped and must not be treated as durable metadata identity
- do not collapse inode identity and data identity into a single field or cache key
- when metadata returns data-facing routing info, keep the semantic boundary explicit

Any change touching identity must audit:

- metadata storage schema
- path traversal logic
- mount/inode ownership checks
- RPC request/response types
- client cache/update behavior
- worker validation paths

## 7. Error and refresh rules

Metadata is a major producer of structured recoverable errors.

Rules:

- business / protocol / consistency failures must be returned as gRPC OK + structured response header error
- not-leader, stale route, mount epoch mismatch, stale state, and similar recoverable conditions must be machine-usable
- do not hide recoverability in strings
- response fields should enable client refresh-replay rather than forcing blind retries

Metadata must prefer actionable refresh semantics over generic failure responses.

## 8. Path traversal and caching rules

Rules:

- path traversal is an adapter over dentry/inode authority
- caches are accelerators, not alternate sources of truth
- cache invalidation/versioning must be grounded in authoritative metadata versions/epochs
- do not add hidden fallback paths that succeed while bypassing version checks

A metadata cache that can outvote authority is a bug.

## 9. Coding rules for this directory

- keep authority logic explicit and local
- prefer semantically named types over generic structs/maps
- keep mount routing, inode/dentry state, and raft application logic readable and testable
- delete legacy path-authority or hash-routing remnants instead of layering around them
- avoid helper abstractions that blur authority boundaries
- comments should explain invariants and recovery semantics, not restate syntax

## 10. Tests required for changes here

A meaningful metadata change should usually include the relevant subset of:

- inode/dentry/path traversal tests
- longest-prefix mount resolution tests
- child-overrides-parent mount tests
- same-mount rename atomicity tests
- mount epoch / config version mismatch tests
- not-leader structured refresh tests
- state watermark / stale-state related tests where applicable
- persistence / restart / replay tests
- negative tests proving stale or wrong ownership requests are rejected

Tests should assert semantic class/reason fields when structured errors exist.

## 11. Pre-merge checklist

Before submitting a metadata change, verify:

- did I preserve inode/dentry/attrs authority?
- did I reintroduce path-as-source-of-truth in any form?
- did I accidentally route by hash where ownership routing is required?
- did I mix inode identity, data identity, and session identity?
- did I push high-frequency state into raft?
- did I preserve structured refreshable error behavior?
- did I keep same-mount rename semantics intact?
- did docs/tests move with the contract?