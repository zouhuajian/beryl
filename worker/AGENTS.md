# `worker` Agent Instructions

This file applies to `worker/`. Follow the root `AGENTS.md` first; this crate is a data-plane executor, not namespace authority.

## Scope

`worker` owns:

- local block store, persisted local block metadata, and physical layout interpretation behind `BlockStore`
- chunk IO, checksum, and local repair/materialization execution when implemented
- open/commit control points and stream read/write/abort runtime
- data service adapters, worker core orchestration, worker networking, block reports, and heartbeats
- worker typed config, local validation, block stamp, fencing, block format, range, readiness, and publication state

`worker` must not own file-level namespace authority, metadata route ownership, metadata layout authority, client retry/cache/SDK policy, proto schema ownership, shared domain definitions that belong in `types`, or UFS path derivation from `data_handle_id` or `block_id`.

## Local Rules

- Metadata provides block identity, route/source binding, lease/fencing context, and freshness information. Validate that context before data-plane execution.
- Preserve the distinctions: Block is management/lifecycle/reporting; StorageChunk is local IO/checksum/bitmap/materialization; TransportFrame is stream/network batching and flow control; Stream is a continuous read/write session.
- `BlockStore` should hide physical layout assumptions from upper layers.
- Local block metadata must be self-describing; do not interpret persisted blocks through runtime defaults.
- Keep Open/Commit control points separate from the stream data path.
- Do not make staging data readable before final publication.
- Worker owns local repair and materialization when implemented. Unimplemented repair, materialization, QUIC/RDMA, peer RPC, partial-cache, or append surfaces must remain explicit placeholders and must not become half-implemented abstractions.
- Generated worker block-meta proto types should stay behind codec boundaries.

## Tests

- Test block publication, staging unreadability, Ready reads, write stream sequencing, commit/abort/recovery, effective tail length, stale block stamp, fencing, persisted metadata interpretation, restart/reopen, structured error mapping, stream cursor/eos/cleanup, and bounded concurrency where relevant.
- Tests should prove data-plane semantics, not generic success.
- Use integration tests when metadata/client interaction is the behavior under test.

## Local Self-Review

Apply the root self-review checklist, then check:

- Did worker remain a data-plane executor without namespace authority?
- Did metadata-derived context get validated before data-plane execution?
- Did Block, StorageChunk, TransportFrame, and Stream stay distinct?
- Did persisted interpretation come from local metadata instead of runtime defaults?
- Did Open/Commit and stream data paths remain clearly separated?
