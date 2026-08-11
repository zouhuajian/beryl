# Beryl Roadmap

Baseline: `main@fa1ad458e5cc8339bd1ec2e6507ad01acdb0e59b`,
`v0.1.0-alpha.1`

Long-term target: a multi-Metadata-group distributed cache.

## Scope

| Area | Current scope |
| --- | --- |
| Metadata | One Metadata group, one leader |
| Worker | Metadata-authorized local block storage |
| Client | Rust native Client |
| UFS | Adapter boundary only; no active read or write path |
| Release topology | 1 Metadata + 1 Worker on Anolis 8 |
| Active work | Metadata, Worker, Client, and multi-Worker operation within one Metadata group |
| External cache | Starts after the single-group storage path is complete |

## Current State

| Module | Available | Open work |
| --- | --- | --- |
| Metadata | Namespace, layout, visibility, leases, write sessions, Worker registry, block locations, freshness checks, Raft/RocksDB persistence | RPC admission, session limits, periodic session cleanup, incremental block-location updates |
| Worker | Block store, capacity reservation, read/write streams, heartbeat, full/delta block reports, fencing, block cleanup | Stream limits, abandoned-stream cleanup, multi-Worker qualification |
| Client | Namespace operations, create, append, write, sync, commit, abort, snapshot open, positioned read, `read_all` | Bounded sequential reads and whole-file read limit |
| E2E | CRUD, Metadata restart, Worker restart, block cleanup, process shutdown | Released-binary 1 Metadata + 2 Workers matrix |

### Architecture Boundaries

| Boundary | Rule |
| --- | --- |
| Namespace and visibility | Owned by Metadata/Raft/RocksDB |
| Write fencing | Persisted by Metadata; leader-local sessions are continuation state |
| Block locations | Derived from fresh, accepted Worker reports |
| Local blocks | Owned by Worker local lifecycle |
| Client reads and writes | Use metadata-authorized Worker targets |
| Future group ownership | One namespace partition belongs to one Metadata group at a route epoch |

## Milestones

| Horizon | Status | Milestone |
| --- | --- | --- |
| Near term | Active | Runtime admission and abandoned-state cleanup |
| Near term | Next | Bounded Rust Client reads |
| Near term | Next | Incremental block reports and qualified 1+2 operation |
| Medium term | Planned | Replication, repair, and Worker drain |
| Medium term | Later | Multi-peer Metadata group |
| Long term | Later | Read-only external cache, multi-group routing, migration, and scale work |

## Near Term: Runtime Admission and Cleanup

### Work

| Module | Work item |
| --- | --- |
| Metadata | Limit in-flight RPCs per connection and for the service |
| Metadata | Limit active write sessions globally and per Client |
| Metadata | Run periodic, bounded session-expiry cleanup |
| Metadata | Reject admission before request conversion or Raft mutation |
| Worker | Limit total, read, and write streams |
| Worker | Release read pins, staging files, and reserved capacity after timeout, cancellation, disconnect, and failed open |
| Worker | Bound idle cleanup and shutdown draining |
| Client | Classify `ResourceExhausted`; cap retries and backoff |
| Observability | Active, rejected, expired, and cleaned session/stream/RPC metrics |

### Exit Criteria

- Exact-limit and limit-plus-one tests for each admission boundary.
- No session, stream, read pin, staging file, or capacity reservation remains
  after the configured cleanup or restart-recovery path.
- Admission rejection occurs before authoritative mutation.
- Metadata and Worker restart tests pass with the default limits.
- Default limits are present in shipped configuration and operations docs.

## Near Term: Bounded Rust Client Reads

### Work

| Module | Work item |
| --- | --- |
| Client API | Add a public sequential or chunked read API |
| Client API | Add an explicit maximum for whole-file convenience reads |
| Client runtime | Split reads into bounded requests and buffers |
| Client runtime | Preserve the opened content revision and layout across chunks |
| Client runtime | Preserve operation deadline and cancellation across Metadata and Worker RPCs |
| Metadata/Worker contract | Validate block ID, block stamp, range, and content revision for every chunk |
| Retry | Retry only Ready locations authorized for the opened snapshot |

### Exit Criteria

- Peak Client memory is bounded independently of file size for sequential
  reads.
- Oversized whole-file reads fail before allocation.
- Cross-block, zero-length, stale-location, short-response, timeout,
  cancellation, and Worker-restart tests pass.
- Public API, configuration, and error behavior are documented.

## Near Term: Incremental Reports and Multi-Worker Qualification

### Metadata

- Update only changed block-location entries for a delta report.
- Preserve Worker run ID, report sequence, full-report baseline, freshness,
  block stamp, and Ready-state checks.
- Record full/delta report duration, inventory size, changed blocks, and
  rejection reason.

### Worker

- Keep full-report batches bounded.
- Preserve delta order and coalescing across reconnect.
- Fall back to a full report when the Metadata baseline is unavailable.

### Placement and Client

- Place new blocks on fresh Workers with available capacity.
- Support files whose blocks are placed on different Workers.
- Read each block from its metadata-authorized Worker location.
- Keep replication at one for this milestone.

### 1 Metadata + 2 Workers Matrix

- Multi-block create, write, sync, commit, read, append, rename, and delete.
- Placement on both Workers.
- Cleanup on the Worker that owns the block.
- Restart either Worker and reconverge its full report.
- Restart Metadata and rebuild current Worker locations.
- Reject stale locations after Worker restart.
- Keep unaffected blocks available when one Worker is unavailable.
- Cover abandoned writes, disk-full admission, interrupted reports, duplicate
  delivery, and process shutdown.
- Run the matrix from packaged binaries on the supported release platform.

### Exit Criteria

- A delta report does not clone or rebuild the Worker's complete published
  inventory.
- Benchmarks cover multiple inventory sizes and change-set sizes.
- The packaged 1+2 matrix passes.
- Release notes state the tested topology and platform.

## Medium Term: Replication, Repair, and Worker Drain

| Module | Work item |
| --- | --- |
| Metadata | Persist desired replication and derive current replicas from fresh Ready reports |
| Metadata | Schedule restart-safe, idempotent repair for under-replicated blocks |
| Metadata | Select targets by capacity and failure domain |
| Metadata | Fence placement during Worker drain and decommission |
| Worker | Transfer an exact block ID and block stamp through staging and checksum verification |
| Worker | Publish a transferred block as Ready only after atomic local commit |
| Worker | Clean incomplete transfers after cancellation or restart |
| Client | Select and retry metadata-authorized Ready replicas within one snapshot deadline |
| Operations | Replica, repair, transfer, and drain metrics |

### Exit Criteria

- A replication-two file remains readable after either replica Worker is lost.
- Desired replication is restored when replacement capacity is available.
- Partial, corrupt, stale, and duplicate transfers do not become Ready.
- Delete during repair and restart during transfer converge correctly.
- Worker drain completes without new placement after the drain fence.
- A packaged 1 Metadata + 3 Workers fault and soak matrix passes.

## Long Term

| Feature | Scope |
| --- | --- |
| Multi-peer Metadata group | Three-peer Raft, leader failover, membership changes, snapshot recovery, Client leader discovery |
| Metadata operations | Backup/restore, peer replacement, upgrade, TLS identity, and recovery procedures |
| Read-only external cache | One backend, strong `BackingFileVersion`, miss/fill, admission, eviction, and cache metrics |
| Static multi-group routing | One authoritative group per namespace partition, route epoch, independent groups, Client route refresh |
| Group migration | Fenced, resumable partition movement followed by placement and rebalance |
| Large metadata layouts | Paged extents or layout compaction after workload and benchmark evidence |
| Alternate data transport | Evaluate only after gRPC is measured as the active bottleneck |
| Client and compute integrations | Select from deployed workload demand |

- Initial multi-group support excludes cross-group rename and transactions.

## Deferred

- UFS-backed IO before the read-only external cache milestone.
- Write-through and write-back cache.
- Multiple external backends before one backend is complete.
- Recursive listing, POSIX, FUSE, and Hadoop compatibility.
- QUIC and RDMA without benchmark evidence.
- Placeholder admin, metadata-peer, replication, or routing services.
- Compatibility guarantees for unreleased storage formats.

## Release Criteria

Each milestone release includes:

- unit and affected-crate tests;
- supported-topology end-to-end tests;
- timeout, cancellation, retry, restart, and stale-state tests;
- resource-limit and cleanup tests;
- metrics and operational logs;
- packaged-binary acceptance; and
- supported-scope and non-goal documentation.

## Tracking

- Replication, Metadata HA, external caching, and multi-group routing require a
  separate design document and tracking issue.
- Completed milestones move to release notes.
