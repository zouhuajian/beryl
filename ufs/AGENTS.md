# `ufs` Agent Instructions

This file applies to `ufs/`. Follow the root `AGENTS.md` first; this crate owns backend integration and adapter behavior.

## Scope

`ufs` owns:

- external backend adapters such as local FS, HDFS, S3, OSS, and object stores
- backend-specific config, defaults, validation, and construction
- OpenDAL adapter setup
- UFS path/object mapping and backend operation mapping
- backend capability discovery and explicit capability decisions
- backend error normalization when it preserves Vecton semantics

`ufs` must not own metadata authority, inode/dentry/mount policy, namespace ownership, worker store/runtime behavior, client retry/replay/cache policy, shared production helpers for unrelated crates, or dependencies on `metadata`, `worker`, or `client`.

## Local Rules

- Backend behavior must not redefine filesystem metadata truth.
- Keep backend-specific dependencies and policy isolated behind adapter boundaries.
- Do not promote backend-specific behavior into `common` as generic policy.
- Preserve Block, StorageChunk, TransportFrame, Stream, route, epoch, and fencing semantics at adapter boundaries.
- Do not silently fake unsupported backend semantics such as rename, append, truncate, consistency, or directory behavior.

## Tests

- Test backend capability mapping, config validation, error normalization, LocalUFS interpretation, range/object translation, unsupported semantic rejection, and restart/reopen behavior where relevant.
- Tests should prove semantic preservation, not only successful backend IO.
- Use integration tests when backend behavior affects cross-crate contracts.

## Local Self-Review

Apply the root self-review checklist, then check:

- Did metadata authority stay outside `ufs`?
- Did backend-specific policy stay local and explicit?
- Did unsupported backend semantics fail clearly?
- Did dependency direction avoid `metadata`, `worker`, and `client`?
- Did docs explain backend capability gaps only where relevant?
