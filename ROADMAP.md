# Beryl Product and Engineering Roadmap

Status: Proposed

Baseline: main at 2dae33e, 2026-07-31

Audience: maintainers, reviewers, product owner, and pilot workload owner

## 1. Purpose

This document defines the recommended product direction, architecture evolution,
engineering priorities, refactoring boundaries, and delivery gates for Beryl.
It is a decision and sequencing document, not a promise that every listed
capability will be built.

The roadmap follows the repository decision order:

1. Functional correctness.
2. Safety and invariant preservation.
3. Simplicity.
4. Readability.
5. Abstraction.

No later milestone may weaken an invariant established by an earlier milestone.
Product expansion must stop when its acceptance gate is not met.

## 2. Product Positioning

Beryl should be positioned as a metadata-authorized, cross-engine data access
cache between compute engines and remote data systems such as HDFS, S3, and
OSS.

The initial product hypothesis is intentionally narrow:

- Read-first rather than write-back-first.
- Repeated access to large, immutable or strongly versioned datasets.
- Shared acceleration across engines rather than an engine-private cache.
- Metadata-owned file version and visibility.
- Worker-owned local block execution and cache residency.
- Explicit failure when authority or backing-version evidence is unavailable.

Beryl should not be positioned as:

- A Redis, Valkey, or Memcached replacement.
- A general POSIX filesystem.
- An Alluxio feature-parity project.
- A generic object-storage gateway.
- An LLM semantic cache or KV cache.
- A multi-group metadata platform before a single group is proven insufficient.

The product goal is lower repeated-read latency, lower backend traffic, and
predictable recovery without stale or unauthorized success. Metadata sharding,
multiple replicas, and additional transports are enabling mechanisms, not the
product goal itself.

## 3. Current Architecture Baseline

The supported runtime currently follows this path:

1. The Rust client resolves namespace, layout, freshness, and worker locations
   through Metadata.
2. Metadata commits namespace, file layout, fencing, and visibility mutations
   through OpenRaft and RocksDB.
3. The client sends data operations directly to Metadata-authorized Workers.
4. Workers write unpublished staging data and transition exact block versions
   to Ready.
5. Worker block reports reconstruct current physical observations in Metadata.
6. Metadata publishes file content only after exact Ready evidence is observed
   and revalidated.
7. Detached-root reclamation is durable and bounded; the companion recursive
   delete change moves namespace visibility to atomic detach before reclamation.

### 3.1 Authority boundaries

| State | Current owner | Durability | Required behavior |
| --- | --- | --- | --- |
| Namespace and dentries | Metadata Raft state machine | Durable | Ordered and replay-safe |
| File layout and visible extents | Metadata Raft state machine | Durable | Visible only after Ready evidence |
| Mount, route, and fencing epochs | Metadata | Durable | Fail closed on mismatch |
| Worker descriptor | Metadata Raft state machine | Durable | Bound to group and worker identity |
| Worker run and heartbeat | Metadata observation | Process-local | Rebuilt after restart |
| Block report baseline and locations | Metadata observation | Process-local | Unavailable until a current full report |
| Write session and admission state | Metadata leader | Process-local | Fail closed across restart or ambiguity |
| Local block bytes and Ready state | Worker | Durable local state | Exact block stamp and crash recovery |
| Cleanup candidates | Metadata leader | Process-local | Reconstructed from reports |
| Detached namespace roots | Metadata Raft state machine | Durable | Reclaimed in bounded deterministic batches |

This separation is the strongest part of the current design. Future features
must not turn worker-reported observations into durable namespace authority or
let workers decide file visibility.

### 3.2 Current supported boundary

The current product boundary is:

- One Metadata group.
- One Metadata leader.
- One active Rust client interface.
- Metadata-authorized Worker reads and writes.
- Replication factor one for active writes.
- UFS adapters present but not on the active read or write path.
- No supported metadata peer RPC, admin API, POSIX, FUSE, Hadoop compatibility,
  replication, repair, or rebalancing.

## 4. Current Capability Assessment

| Capability | Maturity | Assessment |
| --- | --- | --- |
| Namespace authority | Strong foundation | Raft/RocksDB-backed mutation, freshness, fencing, and exact path preconditions |
| Write visibility | Strong foundation | Ready-before-visible gate with current worker run and block stamp |
| Worker local lifecycle | Strong foundation | Staging, Ready publication, abort, sync, exact reclaim, and deletion-marker recovery |
| Metadata restart safety | Strong foundation | Active writes fail closed; durable state and snapshots are restored |
| Worker restart convergence | Strong foundation | Registration and full report rebuild current locations |
| Recursive namespace delete | In review | Bounded detach and detached-root reclamation are validated separately before landing |
| Physical block cleanup | Strong foundation | Bounded paged scan cycles, exact stamped commands, and crash recovery |
| Client retry semantics | Strong foundation | Response identity validation, freshness refresh, and explicit UnknownOutcome |
| Large directory handling | Strong foundation | Cursor pagination with server-enforced default and maximum page sizes |
| Large file metadata | Incomplete | PublishFile carries and rewrites the complete extent vector |
| Block report scalability | Incomplete | Delta processing rebuilds full worker and global location state |
| UFS read-through | Not implemented | Adapter-only boundary |
| Admission and eviction | Not implemented | Capacity exhaustion is not a cache policy |
| Replication and repair | Not implemented | No production queue, transfer protocol, or acknowledgement surface |
| Metadata high availability | Not implemented | Peer RPC is fail closed and cluster mode is rejected |
| Security and tenancy | Development only | No productized mTLS, authentication, authorization, or quota |
| Upgrade and backup | Development only | Schema mismatch requires reformat; no production migration or restore workflow |
| Ecosystem integration | Not implemented | Rust-native API only |

## 5. Engineering Findings

### 5.1 Release-blocking or scale-blocking findings

#### A. Bounded Delete needs a trusted release baseline

The companion bounded-delete change makes Delete carry a mount-relative path,
mount identity, mount epoch, and expected inode identity. Raft apply re-resolves
the path before mutation. This is the right authority direction because an
admitted request must not mutate a subtree that was detached before apply.

Before this work is published:

- Keep the feature in one repository-compliant, reviewable commit.
- Independently review stale path, ancestor detach, rename, mount boundary, and
  active-write behavior.
- Run the full validation matrix.
- Confirm restart continuation and deterministic apply under very small reclaim
  budgets.

#### B. Public and replicated inputs lack complete hard limits

The following paths need explicit server-side limits:

- Directory listing page size.
- Committed blocks in SyncWrite and CommitFile.
- Extents in PublishFile.
- Full and delta block-report entries.
- Serialized Raft command bytes.
- Block size and chunk size.
- Convenience read size.
- Active write sessions per client and per Metadata process.
- Metadata RPC concurrency and queued work.

Transport defaults are not a stable product invariant. Limits must be explicit,
versioned where they affect Raft apply, observable, and tested.

#### C. Large file metadata remains unbounded

PublishFile contains a complete vector of extents. Apply sorts, validates,
clones, and rewrites the entire file inode. Read paths also clone and filter the
complete extent list.

This makes Raft entry size, apply latency, inode value size, memory usage, and
snapshot cost grow with file block count.

The immediate action is a hard extent limit. The durable solution is paged
extent storage with an atomic visible revision switch.

### 5.2 High-priority maintainability and performance findings

#### A. WorkerManager combines multiple consistency domains

WorkerManager currently owns:

- Durable worker descriptors copied from authority.
- Accepted worker process runs.
- Heartbeat and liveness state.
- Heartbeat rejection log suppression.
- Full and delta block-report state.
- Worker-to-block index.
- Block-to-worker location index.
- Publication observation notifications.

The issue is not only file size. These fields participate in different
invariants and are protected by multiple independent locks.

The recommended concrete boundaries are:

- WorkerRegistryState: descriptor, accepted run, heartbeat sequence, and
  liveness.
- BlockReportIndex: report baseline, delta sequence, worker blocks, and block
  locations.
- PublicationObservation: versioned notification only.

No generic registry framework or manager hierarchy is needed.

#### B. Delta reports perform full rebuild work

Each accepted delta currently computes full old and new Ready sets, clones the
worker report, removes the Worker from every global location, and re-adds all
Ready blocks.

The target complexity is:

- Delta report: O(changed blocks).
- Full report: O(worker blocks plus changed location entries).
- Worker expiry or new run: O(worker blocks), not O(all global blocks).

#### C. Large-file client APIs are convenience-buffered

The client can buffer a complete configured block and read_all reserves memory
for the complete file. These APIs are acceptable conveniences only when their
limits are explicit.

The supported large-data API should expose bounded sequential streaming. A
read-all helper should have a conservative maximum or require an explicit
caller limit.

### 5.3 Low-risk hygiene findings

- Remove the unused beryl-ufs dependency from beryl-metadata until a real
  integration exists.
- Remove Command::SetAttr unless a current RPC and client contract is approved.
- Remove the accepted-but-rejected cluster Raft configuration surface until
  cluster mode is implemented end to end.
- Repair the missing freshness-and-ownership documentation reference.
- Replace the hard-coded Apple Silicon Homebrew libclang path with a real
  bootstrap script or documented environment discovery.
- Standardize Rust editions only as a dedicated toolchain change, not as part
  of correctness work.

## 6. Roadmap Overview

| Milestone | Outcome | Indicative duration | Gate |
| --- | --- | --- | --- |
| M0 Trusted Baseline | Current single-group behavior is reviewed, validated, and releasable | 1-2 weeks | Full correctness and CI matrix passes |
| M1 Bounded Runtime | Requests, Raft work, reports, cleanup, and memory are explicitly bounded | 2-4 weeks | Scale tests show bounded progress |
| M2 Efficient and Simple Core | Hot paths are incremental and incomplete lifecycle code is removed | 3-5 weeks | No O(global blocks) delta path; simpler production surface |
| M3 Read-Through Cache Pilot | One real immutable/versioned workload uses safe UFS read-through | 8-12 weeks | Product Go/No-Go metrics pass |
| M4 Production Readiness | HA, multiple replicas, security, migration, and operational recovery | 8-12+ weeks | Failure and upgrade gates pass |
| M5 Evidence-Driven Scale-Out | Mount-level sharding and broader integration only when measured | Unscheduled | Single-group bottleneck is proven |

Durations are planning ranges for a small focused team. M3 and later require a
named workload owner and operational support.

## 7. Milestone Plans

## M0: Trusted Baseline

### Objective

Turn the detached-root and bounded Delete work into a trusted baseline without
mixing in unrelated refactoring.

### Deliverables

- Final exact-path Delete command and deterministic apply behavior.
- Tests for:
  - Ancestor detached before a queued Delete applies.
  - Target renamed or replaced before apply.
  - Mount root and nested mount rejection.
  - Active writer under a recursive-delete target.
  - Metadata restart after detach and before complete reclamation.
  - Repeated ReclaimDetachedRoots proposals.
  - Small max_entries and max_batch_bytes budgets.
- A repository-compliant commit subject and reviewable commit history.
- Updated architecture documentation for durable and leader-local delete
  authority.

### Acceptance gate

- git diff --check
- cargo fmt --all --check
- cargo test -p beryl-metadata
- cargo test -p beryl-e2e
- cargo check --workspace
- cargo clippy --workspace --all-targets -- -D warnings
- cargo test --workspace

Externally visible acceptance:

- Recursive delete hides the complete target atomically.
- No stale command can mutate an unreachable or replaced path.
- Restart never resurrects the detached namespace.
- Background work is bounded and eventually completes.
- Physical block removal remains exact, stamped, retry-safe, and report-derived.

### Explicitly out of scope

- Extent redesign.
- UFS integration.
- Multiple Metadata nodes.
- Generic namespace refactoring.

## M1: Bounded Single-Group Runtime

### Objective

Guarantee that every user request, report, Raft mutation, background pass, and
in-memory queue has an explicit upper bound while still making progress.

### Deliverables

#### Bounded RPC and domain inputs

- Server default and maximum ListStatus page sizes.
- Maximum committed blocks per write barrier.
- Maximum full-report and delta-report entries.
- Maximum block and chunk sizes in beryl-types.
- Maximum convenience read allocation.
- Maximum active sessions and per-client sessions.
- Metadata server concurrency limits with fast ResourceExhausted rejection.

#### Bounded Raft

- Serialized command-byte measurement before proposal.
- Stable protocol maximum for command bytes and extent count.
- Apply-duration and batch-byte metrics.
- Fail-closed rejection before unbounded allocation.

#### Progress-preserving cleanup

- Stable Ready-replica page cursor.
- Cycle-start Worker high watermark and first-entry block high watermark per
  Worker.
- Exact replica identity revalidation before dispatch.
- Candidate retirement only after a complete cycle.
- Tests above 10,000 Ready replicas.

### Acceptance gate

- A directory with at least 100,000 entries is always returned in bounded pages.
- More than 10,000 Ready replicas do not stop cleanup progress.
- Oversized report, commit, block layout, and listing requests fail before large
  allocation.
- Command apply time and bytes are observable.
- No configured local-only value changes deterministic Raft apply behavior.

## M2: Efficient and Simple Core

### Objective

Remove unnecessary production machinery, reduce lock coupling, and make
high-frequency paths proportional to changed data.

### Deliverables

#### Incremental block-report indexing

- Separate WorkerRegistryState and BlockReportIndex responsibilities.
- Apply delta entries directly to affected locations.
- Maintain current worker run and report baseline as one validated snapshot.
- Clear one Worker using its worker-block index.
- Add report apply count, changed blocks, lock duration, and rebuild metrics.

#### Remove incomplete lifecycle code

- Remove production repair queue startup.
- Remove rebalance and timeout loops without Worker execution.
- Keep Worker liveness expiry and current block-location convergence.
- Reserve future repair work for an end-to-end ReplicaTransfer design.

#### Paged extent metadata

- Introduce an extent-page column family.
- Keep a visible extent root or content revision in the inode.
- Build new pages under a non-visible revision.
- Atomically switch the inode to the complete new revision.
- Reclaim obsolete pages asynchronously in bounded batches.
- Keep page and command budgets fixed or protocol-versioned.

#### Streaming client API

- Provide bounded sequential file reads.
- Keep worker frame and client assembly bounded.
- Retain read_all only as an explicitly limited convenience.
- Avoid POSIX semantics and compatibility layers.

### Acceptance gate

- Delta report cost is proportional to changed entries.
- No production repair task can be created without an execution path.
- A file with a large block count does not require one unbounded Raft entry or
  inode value.
- Large file reads do not require memory proportional to file size.
- Existing visibility, fencing, and UnknownOutcome behavior remains unchanged.

## M3: Read-Through Cache Pilot

### Objective

Validate Beryl as a real cross-engine cache on one named read-heavy workload.
This milestone is a product experiment, not a general platform release.

### Scope

- One backing system, selected from the target workload. HDFS is the preferred
  first candidate for an HDFS-centered deployment.
- Immutable datasets or a backend with a strong version token.
- One transparent integration.
- Read-through and eviction only.
- Internal Beryl writes remain a separate supported path.

### Proposed feature design

#### External file identity

Metadata owns a BackingFileVersion containing:

- Backend identity.
- Canonical path.
- Strong object or file version.
- File length.
- Optional authoritative checksum or content identity.

Modification time and length alone must not be treated as a strong version when
the backend can overwrite data without a unique generation.

#### Cache miss and fill

1. The client asks Metadata for a versioned file range.
2. Metadata checks for a current Ready resident block.
3. On a miss, Metadata authorizes one Worker to load an exact backend version
   and range.
4. The Worker reads into staging.
5. The Worker verifies version, length, and checksum requirements.
6. The Worker publishes the local block as Ready.
7. A block report supplies current Ready evidence.
8. Metadata exposes the resident location.
9. The client reads through the normal Metadata-authorized Worker path.

The client must not bypass Metadata and silently read a different backend
version.

#### Failure behavior

- Backend unavailable on miss: explicit unavailable result.
- Version changed during fill: discard staging and refresh Metadata.
- Worker restart during fill: staging remains unpublished and is recovered or
  removed.
- Corrupt resident block: quarantine, remove current location, and refill the
  same backing version.
- Metadata restart: resident locations remain unavailable until registration
  and a current full report converge.
- No verified backing version: no stale fallback.

#### Admission and eviction

Start with direct, observable policy:

- Dataset or mount allowlist.
- Maximum cacheable file and block size.
- Worker high and low disk watermarks.
- Minimum expected reuse or explicit workload hint.
- Bounded concurrent fills per Worker and backend.

Eviction is allowed only when:

- The resident block has a verified recoverable backing version, or
- Another healthy exact replica exists under a completed multi-replica
  lifecycle.

Internal single-replica Beryl data must never be evicted as cache.

Eviction reuses:

- Exact block identity and stamp.
- Reader lifetime fencing.
- Reclaiming transition.
- Durable deleting marker.
- Delta REMOVE or later full-report absence for convergence.

#### Transparent integration

For Spark, Hive, and Trino-oriented validation, prefer one Hadoop FileSystem
scheme integration over FUSE. FUSE introduces broader POSIX semantics that do
not help validate the initial cache hypothesis.

### Product acceptance gate

Correctness gates:

- Zero wrong-version reads under injected failure.
- Zero unexplained stale reads.
- Every miss, corrupt block, and lost Worker either refills the exact version or
  returns an explicit error.
- Eviction never removes the only unrecoverable copy.

Value gates on the same replayable trace:

- At least 50 percent reduction in backend bytes or requests.
- At least 30 percent improvement in warm-phase P95 read or job time.
- At least 60 percent steady-state cache hit rate.
- At least one significant advantage over direct backend access and the
  relevant mature alternative.
- Operational cost acceptable to the named workload owner.

Failure of a correctness gate stops product expansion. Failure of the value
gate stops the generic cache-platform roadmap and triggers a narrower use-case
decision.

## M4: Production Readiness

### Objective

Add availability, security, upgrade, and operational guarantees after the cache
value hypothesis passes.

### Deliverables

#### Three-node Metadata Raft

- Metadata peer RPC.
- Membership bootstrap and change procedure.
- Vote, append, and snapshot installation.
- Leader discovery and redirect.
- Linearizable read behavior under leader changes.
- Restart and rolling-upgrade procedure.
- Snapshot and log compaction controls from configuration.

Implement three nodes for one group before multi-group routing.

#### Multiple replicas

Define a concrete ReplicaTransfer contract with:

- Group name.
- Block ID and exact stamp.
- Source Worker and source run.
- Target Worker and target run.
- Layout, effective length, and checksum.
- Stable transfer identity.
- Bounded retries and concurrency.
- Target staging, Ready publication, report convergence, and acknowledgement.
- Restart recovery and cancellation behavior.

Metadata must treat a completed Ready report as physical evidence. A queued or
dispatched transfer is not a replica.

#### Security and tenancy

- mTLS for Client, Metadata, Worker, and peer RPC.
- Authenticated service and workload identity.
- Authorization for namespace and data access.
- Tenant and workload quotas.
- Per-tenant admission and rate limits.
- Audit records with bounded, non-sensitive labels.

#### Upgrade and disaster recovery

- Explicit RocksDB schema migrations.
- Backup and restore workflow.
- Snapshot compatibility decision.
- Offline verification tooling.
- Roll-forward and rollback procedure.
- Recovery point and recovery time objectives.

#### Operational validation

- Network partition.
- Leader SIGKILL.
- Worker SIGKILL during write, fill, read, and reclaim.
- Disk full and fsync failure.
- Corrupt block and corrupt deletion marker.
- Snapshot interruption and installation failure.
- Block-report flood.
- Backend throttling and outage.
- Multi-day workload soak.

### Acceptance gate

- No split-brain visibility or stale authority success.
- A single Metadata node failure preserves service through a new leader.
- A single Worker failure preserves reads for replicated or UFS-backed data.
- Rolling restart does not require reformat or data loss.
- Authentication and authorization are enforced on every active RPC path.
- Backup restoration is tested, not only documented.

## M5: Evidence-Driven Scale-Out

### Objective

Scale only the component proven to be the bottleneck.

Potential deliverables:

- Mount-level Metadata group assignment.
- Route discovery and group ownership changes.
- Per-mount migration protocol.
- Admin API for supported topology operations.
- Additional transparent connectors.
- Additional UFS backends.
- Write-through, only if a real workload requires it and exact failure semantics
  are designed.

Entry gate:

- One Metadata group is measurably the limiting resource.
- The workload cannot be solved by bounded requests, indexing, batching, or
  vertical scaling.
- Cross-group rename and failure behavior have an explicit product contract.

No multi-group placeholder types, peer schemas, or configuration should be
added before this gate.

## 8. Crate-Level Work Plan

| Crate | Near-term work | Later work | Must not own |
| --- | --- | --- | --- |
| beryl-types | Shared hard limits and validated value types | Backing-file and replica-transfer values with real producers and consumers | Runtime policy |
| beryl-common | Resource-limit mechanics and authenticated header support | Shared TLS and observability mechanics | Metadata, Worker, or cache policy |
| beryl-proto | Bounded current RPC fields and validation | UFS fill, replica transfer, and peer RPC only with active endpoints | Retry, placement, or authority policy |
| beryl-metadata | Cleanup pagination, command bounds, report index, extent pages | UFS version authority, HA, replica authority, sharding | Worker byte execution |
| beryl-worker | Bounded control path and reclaim module | UFS fill, eviction, replica transfer | Namespace visibility |
| beryl-client | Streaming, explicit allocation limits, stable retries | Cache-miss orchestration and one ecosystem integration | Direct authority bypass |
| beryl-ufs | Freeze broad expansion; select one backend for pilot | Additional backends after pilot evidence | Cache admission, visibility, or retry policy |
| beryl-e2e | Delete closure, scale bounds, cleanup progress | UFS, HA, replica, security, upgrade, and fault matrix | Production test hooks |

## 9. Refactoring Plan

Refactoring must be sequenced after the affected behavior is protected by tests.
Each pull request should change one invariant boundary.

### 9.1 Worker state and report indexing

Recommended sequence:

1. Add behavior and concurrency tests around run replacement, report baseline,
   location publication, and expiry.
2. Introduce WorkerRegistryState for registration and liveness.
3. Introduce BlockReportIndex for report and location invariants.
4. Change delta apply to update exact changed blocks.
5. Remove obsolete WorkerManager maps and forwarding methods.
6. Rename the remaining top-level owner only when its final responsibility is
   concrete.

### 9.2 Maintenance simplification

Recommended sequence:

1. Prove that repair tasks have no production dispatcher or acknowledgement.
2. Stop starting repair, rebalance, and timeout loops.
3. Remove unused config, metrics, types, and tests.
4. Retain liveness expiry, detached-root reclaim, and block cleanup.
5. Add ReplicaTransfer later as a clean end-to-end feature.

### 9.3 Extent storage

Recommended sequence:

1. Add protocol hard limits and metrics.
2. Define ExtentPage key, value, and revision semantics.
3. Add bounded page writes and reads behind current file behavior.
4. Atomically switch visible revision.
5. Add old-page reclamation.
6. Migrate existing schema explicitly.
7. Remove inline extent-vector storage only after migration tests pass.

### 9.4 Worker block storage

The durable deleting-marker lifecycle is a stable responsibility and may be
moved from the large block-store module into a reclaim module. The move must
preserve:

- Exact stamp validation.
- Reader and reclaim lifetime coordination.
- Marker fsync order.
- Idempotent retry.
- Startup completion before Ready discovery.

Do not introduce a storage-backend trait unless a second real backend exists.

### 9.5 Publication path

Publication should remain a readable linear state machine. Extraction is
justified only for concrete invariant owners such as:

- CommittedBlockSet validation.
- PublicationReadyTargets construction.
- Ready-evidence validation.

Do not replace the flow with strategies, callbacks, or generic orchestration
frameworks.

## 10. Quality and Validation Strategy

### 10.1 Unit and crate tests

Protect:

- Identity and stamp validation.
- Namespace and extent invariants.
- Replay and duplicate delivery.
- Lock and lifecycle concurrency.
- Crash-recovery state transitions.
- Cursor and bound behavior.

Prefer a small number of state-machine and behavior tests over source-shape
tests.

### 10.2 End-to-end tests

Maintain black-box suites for:

- Local CRUD and multi-block files.
- Ready-before-visible.
- Metadata restart at every write stage.
- Worker restart and full-report convergence.
- Recursive delete and detached-root continuation.
- Physical cleanup and exact stamp behavior.
- Large directory pagination.
- Cleanup above the former 10,000-replica boundary.
- UFS cold miss, warm hit, corruption, and version change.
- Metadata failover and replica loss when those features exist.

### 10.3 Fault and soak tests

Fault tests must use bounded deterministic synchronization, not blind sleeps.
Required injection points include:

- Before and after durable publication.
- Before and after Ready report acceptance.
- Before and after namespace detach.
- During detached-root batch commit.
- Before and after deleting-marker durability.
- During UFS fill and version validation.
- During snapshot installation.

### 10.4 Performance tests

Track separate benchmarks for:

- Metadata path resolution and listing.
- Raft proposal and apply.
- Full and delta report apply.
- Worker sequential read and write.
- Cache cold fill and warm read.
- Cleanup classification and dispatch.
- Snapshot build and install.

Performance changes must report workload shape and dataset size. Microbenchmark
improvement alone is not a product result.

### 10.5 CI evolution

Keep the current formatting, check, clippy, and workspace test job. Add stages
only when their capability becomes active:

- Focused crate tests for changed crates.
- E2E restart matrix.
- Feature matrix for selected UFS backend.
- Schema migration test.
- Security configuration test.
- Scheduled fault and soak jobs.

## 11. Observability and SLO Inputs

Before setting production SLOs, expose the measurements needed to define them.

### Metadata

- RPC count, latency, error kind, and inflight count by operation.
- Raft proposal bytes, apply duration, log size, and snapshot duration.
- Directory page entries and bytes.
- Extent pages per file and bytes per revision.
- Active sessions, expirations, and UnknownOutcome recovery.
- Worker report apply duration and changed-block count.
- Cleanup scan cycle progress, candidates, oldest age, dispatch, and reclaim.
- Detached-root count, oldest age, entries reclaimed, and batch bytes.

### Worker

- Read and write throughput and latency.
- Active streams and rejected work.
- Ready, staging, reclaiming, and quarantined blocks.
- Capacity by tier and high-watermark state.
- Cleanup and eviction bytes.
- UFS fill bytes, latency, retries, and version mismatches.
- Cache hit bytes, miss bytes, and corruption.

### Client and product

- Metadata and Worker retries.
- UnknownOutcome count.
- Cache hit ratio and byte hit ratio.
- Cold and warm P50, P95, and P99.
- Backend bytes and request savings.
- Job-level time and failure rate.

## 12. Risk Register

| Risk | Impact | Mitigation |
| --- | --- | --- |
| UFS version is weak or ambiguous | Stale or wrong data | Pilot immutable data; require strong version identity |
| Eviction precedes recoverability | Permanent data loss | Evict only verified UFS-backed or replicated blocks |
| Full report and cleanup scans grow without pagination | Metadata memory or cleanup stall | Leader-term-bound keyset pagination with positional Worker/block high watermarks and exact dispatch revalidation |
| Delta report keeps full rebuild behavior | Metadata lock and CPU saturation | Incremental BlockReportIndex |
| Extents remain inline | Large Raft entries and long apply stalls | Hard cap followed by paged extent revision |
| Repair placeholder is mistaken for availability | False operational confidence | Remove incomplete production loops |
| HA is built before product proof | High cost without user value | M3 Go/No-Go before M4 |
| Multi-group design expands authority complexity | Cross-group correctness failures | Prove single-group bottleneck first |
| Security is deferred into production | Unauthorized data access | Treat M4 security as release gate |
| Test count is mistaken for scale proof | Undetected production failure | Add fault, scale, soak, and workload evidence |

## 13. Delivery Governance

Every milestone should be delivered as small, reviewable pull requests:

- One invariant or behavior boundary per pull request.
- No unrelated cleanup.
- Design note before persistence, wire, authority, or lifecycle changes.
- Independent read-only review for destructive, visibility, recovery, and
  concurrency changes.
- Full validation once at milestone closure.
- Explicit list of commands not run.
- No production claim based only on passing tests.

Commit subjects use:

- feat(scope): outcome
- fix(scope): outcome
- refactor(scope): outcome
- test(scope): outcome
- docs(scope): outcome
- chore(scope): outcome

Temporary subjects such as tmp must not be published as roadmap milestone
commits.

## 14. Immediate Backlog

The next concrete work should be created in this order:

### P0

1. CORE-001: Review and validate exact-path Delete apply.
2. CORE-002: Land bounded Delete on a trusted release baseline.
3. CORE-005: Define hard block, report, commit, and Raft command limits.

### P1

1. CORE-101: Make delta block-report indexing incremental.
2. CORE-103: Add paged extent storage and atomic revision switch.
3. CORE-104: Add bounded streaming reads to the Rust client.
4. CORE-105: Add Metadata inflight limits and load rejection.
5. CORE-106: Repair documentation and macOS build bootstrap.

### P2

1. CACHE-201: Select one pilot workload and backend.
2. CACHE-202: Define BackingFileVersion and immutable mount semantics.
3. CACHE-203: Implement Worker UFS fill to staging and Ready.
4. CACHE-204: Implement miss coordination and refill.
5. CACHE-205: Implement disk-watermark admission and exact eviction.
6. CACHE-206: Add cache and backend-savings metrics.
7. CACHE-207: Implement one transparent ecosystem connector.
8. CACHE-208: Run replayable direct-backend and competitor comparison.

### P3

1. HA-301: Implement three-node peer RPC and membership.
2. HA-302: Validate leader change, snapshot install, and rolling restart.
3. DATA-303: Implement exact ReplicaTransfer.
4. DATA-304: Implement multi-replica write and read availability.
5. SEC-305: Implement mTLS, identity, authorization, and quota.
6. OPS-306: Implement schema migration, backup, and restore.
7. OPS-307: Complete fault and soak qualification.

## 15. Decision Summary

The required sequence is:

1. Trust the current single-group correctness baseline.
2. Bound every request and background lifecycle.
3. Make report and metadata hot paths incremental.
4. Remove code that claims an incomplete lifecycle.
5. Solve large-file metadata and streaming.
6. Validate one read-through cache workload.
7. Continue only when correctness and value gates pass.
8. Add HA, replicas, security, and upgrade operations.
9. Add metadata sharding only after measurement proves it is necessary.

This sequence keeps Beryl focused on its strongest differentiator: a small,
explicit authority model that can support a reliable cache without silently
serving stale, ambiguous, or unauthorized data.
