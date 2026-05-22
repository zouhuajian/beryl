# `ufs` Agent Instructions

This file applies to `ufs/`. Follow the root `AGENTS.md` first, then these local rules.

## Crate role

`ufs` owns external backend integration and adapter behavior. It bridges backend capabilities into Vecton-facing storage semantics without becoming metadata authority or client-visible policy.

## Allowed changes

- External backend adapters such as HDFS, S3, OSS, object, or file backends.
- Backend-specific config, defaults, and validation.
- OpenDAL adapter setup and backend construction.
- UFS path behavior and backend operation mapping.
- Backend capability discovery and explicit capability decisions.
- Backend error normalization when it preserves Vecton semantics.

## Forbidden changes

- Metadata authority, inode/dentry/mount policy, or namespace ownership.
- Worker store/runtime behavior or client retry/replay/cache policy.
- Shared production helpers for unrelated crates.
- Dependencies on `metadata`, `worker`, or `client`.
- Backend-specific policy promoted into `common` as generic policy.
- Shortcuts that bypass route, epoch, fencing, or metadata authority validation.

## Dependency rules

- `ufs` may use `common` and `types` values as needed.
- `ufs` must not depend on `metadata`, `worker`, or `client`.
- Backend-specific dependencies should remain isolated behind adapter boundaries.

## Conversion and validation rules

- Translate backend capabilities and errors deliberately into Vecton-relevant structured meaning.
- Backend behavior must not redefine filesystem metadata truth.
- Keep backend path/object mapping local to `ufs`.
- Preserve Block/Chunk/Stream semantics at adapter boundaries.
- Do not silently fake unsupported backend semantics such as rename, append, truncate, consistency, or directory behavior.

## Testing guidance

- Add tests for backend capability mapping, backend config validation, error normalization, LocalUFS interpretation, range/object translation, unsupported semantic rejection, and restart/reopen behavior where relevant.
- Tests should prove semantic preservation, not only successful backend IO.
- Use integration tests when backend behavior affects cross-crate contracts.

## Documentation guidance

- Document backend capability gaps, semantic mismatches, and adapter-specific config.
- Update docs when backend behavior, config ownership, or capability decisions change.
- Do not turn backend-specific limitations into generic Vecton policy in documentation.

## Review checklist

- Did metadata authority stay outside `ufs`?
- Did `ufs` remain free of `metadata`, `worker`, and `client` dependencies?
- Did backend-specific policy stay local?
- Did capability gaps surface explicitly?
- Did the change preserve Block/Chunk/Stream and route/epoch/fencing semantics?
- Did tests/docs prove the adapter contract?
