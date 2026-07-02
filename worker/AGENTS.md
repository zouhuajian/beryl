# worker Agent Instructions

## Crate Boundary

`worker` owns local block storage and metadata-authorized data-plane execution. Worker code does not own namespace visibility or file layout decisions.

## Allowed Changes

- Improve local block storage, stream execution, block commit/abort/sync, and recovery behavior.
- Tighten validation of metadata-issued block, lease/fencing, worker-run, and freshness context.
- Improve registration, heartbeat, block report, data service adapters, runtime config, and readiness.
- Add focused coverage for stream correctness, publish/recovery, abort idempotency, and block reports when code changes require it.

## Do Not Do

- Do not own namespace visibility.
- Do not decide file layout independently.
- Do not materialize data without metadata-issued context.
- Do not derive UFS paths from data handles or block IDs.
- Do not add alternate transports before the gRPC path is correct.
- Do not make staging data readable before final publication.
- Do not describe replication, repair, rebalancing, partial cache, QUIC, RDMA, io_uring, or SPDK as production behavior used today.

## Cross-Crate Rules

- Use `types`, `common`, and `proto` for shared contracts.
- `worker` must not production-depend on `metadata` or `client`.
- Worker data access must remain subordinate to metadata authorization.
- Keep local block metadata self-describing and validate persisted shape explicitly.

## Validation Notes

- Root workspace validation applies.
- For focused checks, use `cargo test -p worker`.
