# beryl-worker

## Role

`beryl-worker` is Beryl's data-plane executor. It stores local blocks and serves read/write streams only with metadata-issued authorization.

## How It Fits Into Beryl

- Registers with metadata and reports its liveness and local block state.
- Executes worker data RPCs for client reads and writes after metadata grants context.
- Publishes, aborts, syncs, and recovers local block data.

## Main Responsibilities

- Local block store layout, persisted block metadata, and physical block interpretation.
- Read/write stream execution, block open/commit/abort/sync, and local recovery scan behavior.
- Worker registration, heartbeat, block reports, data service adapters, and runtime config.
- Validation of metadata-issued block, lease/fencing, worker-run, and freshness context.

## Current Active Use

The current runtime starts a gRPC worker data service, registers with metadata, sends heartbeats and block reports, stores Ready blocks locally through the filesystem I/O engine, and serves metadata-authorized read/write streams.

Worker-local block deletion exists as a store operation, but physical resident-block reclamation is future/partial. The current worker gRPC data service does not expose an active metadata-driven delete RPC path.

## Not in Current Scope

- Namespace visibility or file layout authority.
- Metadata route ownership.
- Client retry/cache policy.
- UFS path derivation from data handles or block IDs.
- Worker peer transfer.
- Production QUIC/RDMA/io_uring/SPDK data paths.
- Complete autonomous replication, repair, rebalancing, or partial-cache semantics.

## Contributor Notes

- Treat metadata-issued context as mandatory for materializing data.
- Prioritize stream correctness, publish/recovery hardening, abort idempotency, and block report correctness.
- Keep staging data unreadable until final publication.
