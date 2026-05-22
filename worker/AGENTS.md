# `worker` Agent Instructions

This file applies to `worker/`. Follow the root `AGENTS.md` first, then these local rules.

## Crate role

`worker` owns data-plane execution. It executes block-local reads and writes, manages worker-local storage/runtime state, and validates metadata-derived context for direct client-to-worker paths.

## Allowed changes

- Local block store implementation and worker-local persisted metadata interpretation.
- Chunk IO, checksum, repair, replication, relocation, and maintenance execution.
- Stream open/read/write/commit/abort runtime.
- Data service adapters and worker core orchestration.
- Worker net server/client behavior.
- Worker typed config, defaults, and validation.
- Worker-local validation of block stamp, fencing, block format, range, and readiness.

## Forbidden changes

- Metadata authority policy, inode/dentry/path authority, mount ownership, or file-level layout authority.
- Client retry, replay, cache, endpoint-health, or SDK policy.
- Shared domain definitions that belong in `types`.
- Generated schema ownership or protobuf business policy.
- Moving store path layout, publishability, recovery, stream state, or runtime policy into `types`.
- UFS path inference from `data_handle_id` or `block_id`.
- Hidden fallback behavior that bypasses route, epoch, fencing, block stamp, or local-ready validation.

## Dependency rules

- `worker` may depend on shared crates where appropriate.
- `worker` must not depend on `metadata` or `client` in production code.
- Worker core/store public APIs should use Rust domain types, not prost/generated proto types.
- Generated worker block-meta proto types must stay behind codec boundaries.

## Conversion and validation rules

- Use shared domain values at service/core/store boundaries where they are stable.
- Keep store/runtime state local to `worker`.
- Structural proto/domain conversion should be called from `proto` helpers when shared; worker-only codec conversion may stay local when it is schema-local.
- Validate metadata-derived block identity and freshness context before data-plane execution.
- Recoverable mismatches such as stale block stamp, local miss, invalid range, or fencing mismatch must produce structured machine-usable outcomes.
- Do not make staging data readable before final publication.

## Testing guidance

- Add or update tests for block/store publication, staging unreadability, Ready reads, write stream sequencing, abort and recovery, tail effective length, stale block stamp, fencing, persisted metadata interpretation, restart/reopen, structured error mapping, stream cursor/eos/cleanup, and bounded concurrency where relevant.
- Tests should prove data-plane semantics, not only generic success.
- Use integration tests when metadata/client interaction is the behavior under test.

## Documentation guidance

- Document publication, fencing, block stamp validation, staging visibility, persisted metadata interpretation, and store/runtime boundaries.
- Update docs when worker store format, stream contract, config ownership, or data-plane validation changes.
- Do not document future partial-cache, append, materialization, or repair behavior as current behavior.

## Review checklist

- Did Block, StorageChunk, TransportFrame, and Stream remain separate concepts?
- Did store/runtime policy stay local to `worker`?
- Did the change avoid production dependencies on `metadata` and `client`?
- Did persisted block interpretation come from persisted metadata rather than runtime defaults?
- Did it preserve block stamp, fencing, range, and Ready-state validation?
- Did it avoid moving worker runtime state into shared crates?
- Did tests/docs stay aligned with the current store and stream contracts?
