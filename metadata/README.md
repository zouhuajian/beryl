# metadata

## Role

`metadata` is Vecton's namespace, layout, and visibility authority. It owns the control-plane state that decides which data exists, where file blocks are located, and when written data becomes visible.

## How It Fits Into Vecton

- Serves metadata RPCs used by the Rust client for namespace and file-layout operations.
- Issues the metadata context workers require before serving reads or writes.
- Receives worker registration, heartbeat, and block report information.

## Main Responsibilities

- Namespace objects, inode/dentry/attrs state, file layout, and visibility.
- Leases, write sessions, fencing, freshness, and mount routing.
- Worker registry, worker liveness view, and authoritative block locations.
- Raft/RocksDB-backed metadata state, apply, replay, snapshots, and metadata config.

## Current Active Use

The current runtime uses one metadata group with one leader. The metadata service handles format/start lifecycle, filesystem RPCs, worker control RPCs, worker registration/heartbeat/full reports, freshness checks, and Raft/RocksDB-backed state for the current worker-authorized read/write path.

Namespace delete is active. Recursive delete removes namespace/layout state and creates delete-intent state for resident blocks, but complete physical worker-side block reclamation is future/partial. The current runtime does not wire an active worker delete RPC path or worker-side delete ack consumer.

Recursive listing is not supported. Metadata rejects recursive list requests with a structured unsupported error.

## Not in Current Scope

- Production-ready multi-group metadata.
- Multiple metadata leaders for different mount namespaces.
- Metadata peer RPC.
- Admin API.
- UFS-backed read/write namespace.
- POSIX, FUSE, or Hadoop compatibility.
- Complete replication, repair, rebalancing, or autonomous lifecycle management.

## Contributor Notes

- Keep metadata as the source of truth for namespace, layout, and visibility.
- Do not bypass Raft-backed mutation paths for authoritative state changes.
- Surface consistency, storage, replay, and snapshot errors explicitly.
- Treat maintenance internals as safety/cleanup mechanisms unless a complete lifecycle feature is designed and tested.
