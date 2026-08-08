# Beryl Product and Engineering Roadmap

Status: Active

Baseline: main at fa81afa, 2026-08-07

Current release target: internal `v0.1.0-alpha.1`

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

Beryl's long-term product direction is a metadata-authorized, cross-engine data
access cache between compute engines and remote data systems such as HDFS, S3,
and OSS.

The current releasable product is narrower: a single-Metadata, Rust-client,
Metadata-authorized resident-storage system. The first internal alpha publishes
that existing path as an explicit baseline. It must not claim UFS read-through,
cache-miss refill, eviction, Metadata HA, or replication.

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
7. Recursive delete atomically detaches namespace visibility before durable,
   bounded detached-root reclamation.

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
| Recursive namespace delete | Strong foundation | Exact-path detach, bounded detached-root reclamation, and corrupt-authority fail-closed behavior are merged |
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
| Process lifecycle | Incomplete | Metadata accepts SIGINT/SIGTERM but shutdown ownership is incomplete; Worker does not stop its production server and background loops through one bounded shutdown path |
| Binary distribution | Not implemented | No versioned tarball, systemd units, checksum, or packaged-production-binary acceptance |
| Upgrade and backup | Alpha clean-install only | Persistent formats fail closed on mismatch; no cross-version migration, rollback, or restore workflow |
| Ecosystem integration | Not implemented | Rust-native API only |

## 5. Engineering Findings

### 5.1 Release-blocking or scale-blocking findings

#### A. Production process shutdown is not one owned lifecycle

Metadata already receives SIGINT and SIGTERM, but production shutdown does not
explicitly cancel and await every maintenance task or close the Raft runtime.
Worker uses Ctrl-C only while registration is retrying; after registration its
gRPC server, heartbeat, block-report, cleanup, and HTTP tasks have no shared
bounded shutdown path.

Before the first tag:

- Both processes must transition readiness to false before draining.
- New RPC work must stop while accepted work receives a bounded drain window.
- Every long-lived task must have explicit cancellation and ownership.
- Metadata must stop Raft and wait for its background tasks.
- SIGINT and SIGTERM must exit successfully within the systemd stop timeout.
- Same-version restart must recover visible data and current Worker locations.

#### B. The binary and compatibility contract is not release-ready

The current binaries accept explicit YAML configuration, but do not expose a
standard help/version contract or a side-effect-free configuration check. All
workspace crates carry duplicated `0.1.0` versions, while internal pre-release
persistent-format counters reflect discarded development history.

The first release must establish:

- One lockstep workspace version: `0.1.0-alpha.1`.
- One public `beryl` entry point with `--help`, `--version`, explicit role
  commands, and side-effect-free `validate-conf`.
- Version output containing package version, source revision, Rust version, and
  build target.
- Exact-release lockstep for Metadata, Worker, and Rust Client during alpha.
- A clean-install compatibility policy with fail-closed persistent-format
  checks.
- One pre-release reset of current persistent-format and schema counters to
  version 1, followed by monotonic changes after the first tag.

#### C. No user artifact is tested end to end

Workspace E2E tests exercise important runtime and recovery behavior, but they
do not extract a release archive and launch both production binaries from it.
The first release needs one black-box acceptance path that formats storage,
starts production Metadata and Worker processes, performs Rust-client CRUD,
restarts each process, verifies recovery, deletes data, and verifies the final
state using only the extracted package.

#### D. Large-file behavior is bounded but intentionally scale-limited

PublishFile still carries and rewrites a complete extent vector and `read_all`
still reserves memory proportional to the requested file. Compiled limits now
prevent unbounded requests and Raft entries, so paged extents and a stronger
streaming API are no longer first-release blockers. They remain explicit
limitations and become scheduled work only when a selected workload needs
larger files or measurements show material cost.

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
| M0 Trusted Baseline | Exact-path Delete and current single-group correctness are trusted | Completed | Correctness and recovery matrix passed |
| M1 Bounded Runtime | Listing, cleanup, reports, layouts, RPC decoding, and Raft commands are bounded | Substantially completed | Existing scale and hard-limit tests pass |
| R0 Internal `0.1.0-alpha.1` | Current resident-storage path becomes a versioned, installable, recoverable artifact | Current, 1-2 weeks | Extracted-package acceptance passes on Anolis 8 and Ubuntu |
| R1 `0.1` Stabilization | Deployment, lifecycle, configuration, and recovery defects are fixed without feature expansion | Release-driven | Internal soak supports `0.1.0` Developer Preview |
| T1 Evidence-Triggered Core Work | Report indexing, paged extents, streaming, and load shedding are pulled only by evidence | Unscheduled | A release workload or observed limit justifies each item |
| R2 HDFS Read-Through Alpha | One immutable or snapshot-backed HDFS path completes exact-version cold-miss-to-read | After `0.1` baseline | HDFS correctness and restart gates pass |
| R3 HDFS Cache Pilot | Admission, eviction, metrics, connector, and workload comparison validate product value | 8-12 weeks | Product Go/No-Go metrics pass |
| M4 Production Readiness | HA, multiple replicas, security, migration, and operational recovery | 8-12+ weeks | Failure and upgrade gates pass |
| M5 Evidence-Driven Scale-Out | Mount-level sharding and broader integration only when measured | Unscheduled | Single-group bottleneck is proven |

R0 publishes an internal artifact only. Normal pull-request, push, and merge
validation must not build or retain release binaries; only an exact release tag
may run the release build and package workflow. R2 and later require a named
workload owner and operational support.

## 7. Milestone Plans

## M0: Trusted Baseline

Status: Completed by the bounded recursive-delete and corrupt-authority work.

### Objective

Preserve the trusted detached-root and bounded Delete baseline without mixing
unrelated behavior into its authority path.

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

Status: Substantially completed. Cleanup-cycle pagination, bounded listing,
block/report/layout/Raft-command limits, and transport decoding limits are
merged. Active-session and global Metadata load shedding remain evidence-driven
follow-up work rather than R0 blockers.

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

## R0: Internal `v0.1.0-alpha.1` Release Foundation

### Objective

Publish the current single-Metadata resident-storage path as Beryl's first
internal, versioned, installable, operable, and recoverable artifact. R0 is a
release-engineering and lifecycle milestone, not a UFS cache milestone.

### Release identity and compatibility

- Use `v0.1.0-alpha.1` as the Git tag and `0.1.0-alpha.1` as the lockstep
  workspace package version.
- Metadata, Worker, and Rust Client must come from the same exact alpha release.
- The first alpha is clean-install only. Existing untagged development storage
  is not migrated or supported.
- Same-version stop/start, process restart, and data recovery are supported.
- Cross-version mixed clusters, rolling upgrade, downgrade, and rollback are
  not supported by `alpha.1`.
- Beginning with the first tag, every persistent-format version is monotonic and
  may change only with an explicit compatibility decision and tests.

#### One-time pre-release version normalization

Development-only version increments do not describe released compatibility.
Before the first tag, reset the current supported persistent formats to one
coherent version-1 baseline in a single incompatible cutover:

| Version family | Development value at baseline | First-release value |
| --- | ---: | ---: |
| Metadata storage marker | 2 | 1 |
| RocksDB schema | 10 | 1 |
| Snapshot format | 2 | 1 |
| Worker storage marker | 2 | 1 |
| Worker block metadata | 4 | 1 |
| Worker deleting marker | 2 | 1 |
| Storage-generation manifest | 1 | 1 |
| `BlockFormatId::FULL_EFFECTIVE` | 1 | 1 |

The cutover must update focused previous/future/malformed-version tests and must
require clean storage. Do not renumber protobuf field tags, protobuf enum
values, Raft log identities, block stamps, epochs, or other values whose number
is semantic identity rather than a local format revision.

### Production process lifecycle

- Introduce one explicit shutdown owner per process.
- Handle SIGINT and SIGTERM before and after Worker registration.
- Transition readiness to false before stopping new RPC work.
- Drain accepted RPC work for a configured bounded interval.
- Cancel and await HTTP, heartbeat, block-report, cleanup, maintenance, and
  readiness tasks.
- Stop and await the Metadata Raft runtime.
- Exit successfully on a normal signal and preserve same-version restart
  behavior.

### Binary contract

- Public commands are `beryl --help`, `beryl --version`, `beryl version`,
  `beryl metadata`, `beryl worker`, `beryl format metadata`, and
  `beryl validate-conf [metadata|worker]`.
- The public binary resolves `<install-root>/conf` by default and accepts only
  an explicit `--conf-dir <dir>` override.
- Package-internal `beryl-metadata` and `beryl-worker` retain `--help`,
  `--version`, and explicit `start`/`format`/`validate-conf --config <path>`
  actions required by the public router.
- Long-running role commands use process replacement so systemd PID and signal
  ownership remain with Metadata or Worker.
- Static configuration validation reads YAML and applies the same typed checks
  as startup, but does not initialize observability, storage, networking,
  signals, or asynchronous runtime tasks.
- Version output includes package version, source revision, Rust version, and
  build target from one shared build identity.
- Stable nonzero exit on invalid config, incompatible storage, bind failure,
  storage failure, or fatal registration failure.

### Build and package target

- Primary target: Anolis OS 8 on `x86_64`, GNU libc 2.28 baseline.
- Secondary runtime validation: the available Ubuntu host.
- Build target: `x86_64-unknown-linux-gnu`.
- Build inside a pinned Anolis 8-compatible image with fixed Rust and protobuf
  compiler inputs; do not build release binaries on a developer workstation or
  `ubuntu-latest`.
- Always build with `cargo build --release --locked`.

The archive is internal and initially contains:

```text
beryl-0.1.0-alpha.1-x86_64-unknown-linux-gnu/
  bin/beryl
  libexec/beryl-metadata
  libexec/beryl-worker
  conf/metadata.yaml
  conf/worker.yaml
  conf/client.yaml
  systemd/beryl-metadata.service
  systemd/beryl-worker.service
  docs/getting-started.md
  docs/deployment.md
  docs/operations.md
  docs/compatibility.md
  docs/known-limitations.md
  LICENSE
  README.md
  VERSION
```

Publish the `.tar.gz` together with a SHA-256 checksum to the approved internal
artifact location. Do not create a public GitHub Release.

### Tag-only release pipeline

- Pull requests and normal main pushes run validation but do not run
  `cargo build --release`, package binaries, or upload a release artifact.
- Only an exact `v*` tag starts the release build.
- The workflow must reject a tag that does not equal the workspace version.
- The workflow builds once, packages those exact outputs, extracts the archive,
  and runs acceptance against the extracted production binaries.
- Publishing happens only after acceptance succeeds.
- Concurrent publication of the same release is prohibited.

### Packaged-binary acceptance gate

Run the following with one Metadata and one Worker, then repeat the relevant
registration/read checks with one Metadata and two Workers:

1. Verify checksum, archive structure, `--version`, and side-effect-free config
   checks.
2. Format Metadata, start both production processes, and wait for `/ready`.
3. Use a Rust Client example pinned to the same tag to create, write, close,
   stat, and read a multi-block file.
4. Send SIGTERM to Metadata, require bounded successful exit, restart it, wait
   for Worker re-registration/full-report convergence, and read existing data.
5. Send SIGTERM to Worker, require bounded successful exit, restart it, wait for
   full-report convergence, and read existing data.
6. Delete the file, verify namespace absence and bounded physical cleanup,
   restart both processes, and verify the final state.
7. Install and repeat the smoke/restart path on Anolis 8 and Ubuntu.

### Explicitly out of scope

- UFS-backed reads or writes.
- CLI-based filesystem CRUD; the release uses a runnable Rust Client example.
- Public artifact publication.
- Cross-version data migration, rolling upgrade, or rollback.
- Metadata HA, replication, repair, rebalancing, TLS, authentication, or
  authorization.
- RPM, DEB, Docker, Helm, Kubernetes, macOS artifacts, documentation sites, or
  benchmark dashboards.

## R1: `0.1` Internal Stabilization

### Objective

Deploy `alpha.1`, operate it on the internal Anolis 8 and Ubuntu hosts, and use
subsequent `0.1.0-alpha.N` releases only to correct installation, configuration,
shutdown, recovery, observability, compatibility, and documentation defects.

No ordinary feature work enters an `alpha.N` patch unless it is required to
restore the published `0.1` contract. Promote `0.1.0` to Single-Metadata
Developer Preview only after repeated packaged deployments and restart/recovery
soak pass without unexplained data loss, stale visibility, or manual storage
repair.

## T1: Evidence-Triggered Efficient and Simple Core

Status: Deferred. This work is not a prerequisite for R0 or R2 unless release
evidence or the selected HDFS workload demonstrates a concrete limit.

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

## R2: HDFS Read-Through Alpha

### Objective

Complete one safe, user-visible HDFS cold-miss-to-Worker-read vertical slice
without making admission, eviction, or transparent ecosystem integration a
prerequisite for the first HDFS alpha.

### Scope

- HDFS is the selected first backing system.
- Only immutable HDFS paths or HDFS Snapshot paths with sufficient exact-version
  evidence are eligible. Modification time and length alone are insufficient
  for overwriteable paths.
- The supported entry point remains the Rust Client.
- Read-through and exact-version refill only; the selected test dataset must fit
  within an explicit bounded cache budget.
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

### R2 acceptance gate

- `Cold miss -> Metadata authorization -> Worker HDFS fill -> Ready report ->
  Metadata location -> Rust Client read` passes end to end.
- Concurrent misses for the same exact version and range are coalesced or
  rejected with bounded work; they do not create uncontrolled duplicate fills.
- Zero wrong-version or unexplained stale reads under HDFS mutation, fill
  interruption, Worker restart, Metadata restart, and resident corruption.
- Every failed miss either refills the same verified version or returns an
  explicit error.
- Internal resident Beryl data remains separate from recoverable HDFS-backed
  cache data.
- The HDFS path is tested through packaged production binaries before the R2
  internal tag is published.

## R3: HDFS Cache Pilot

### Objective

Add bounded cache lifecycle policy, measurements, and one transparent HDFS
integration, then decide from a replayable workload whether Beryl provides
enough value to continue as a cross-engine cache product.

### Deliverables

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
| beryl-cli | Public binary contract, installed layout, role process replacement, and aggregate config validation | New concrete operational commands only when a supported workflow needs them | Role runtime policy, daemonization, or process supervision |
| beryl-types | Preserve current hard limits; normalize only true pre-release format IDs | Backing-file and replica-transfer values with real producers and consumers | Runtime policy |
| beryl-common | Shared build identity and cancellable process-service mechanics only when concretely reused | Shared TLS and observability mechanics | Metadata, Worker, or cache policy |
| beryl-proto | Preserve current bounded RPC contracts | HDFS fill, replica transfer, and peer RPC only with active endpoints | Retry, placement, or authority policy |
| beryl-metadata | Version/config CLI, owned graceful shutdown, version-1 clean-install baseline | HDFS version authority, report index, extent pages, HA, sharding | Worker byte execution |
| beryl-worker | Version/config CLI, owned graceful shutdown, version-1 clean-install baseline | HDFS fill, eviction, replica transfer | Namespace visibility |
| beryl-client | Runnable same-tag CRUD example and release compatibility documentation | HDFS miss orchestration, streaming, one ecosystem integration | Direct authority bypass |
| beryl-ufs | Keep adapter-only for R0; prepare the selected HDFS implementation for R2 | Additional backends only after HDFS pilot evidence | Cache admission, visibility, or retry policy |
| beryl-e2e | Extracted-package production-process acceptance and signal/restart coverage | HDFS, HA, replica, security, upgrade, and fault matrix | Production test hooks |

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

Status: Completed for the pre-release baseline. Unsupported repair, rebalance,
and timeout production scaffolding has been removed; lost-worker observation,
detached-root reclamation, and exact block cleanup remain.

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

- Metadata and Worker production binaries extracted from the release archive.
- SIGINT/SIGTERM readiness transition, bounded drain, clean exit, and
  same-version restart.
- One-Metadata/one-Worker and one-Metadata/two-Worker packaged deployment.
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

Use two deliberately different paths:

#### Validation CI

- Pull requests and main pushes run formatting, workspace metadata, check,
  clippy, workspace tests, and E2E tests with the locked dependency graph.
- Validation may compile test code as required, but it must not run the release
  profile, create a distributable archive, or upload a release binary.
- Pin the runner version, Rust toolchain, protobuf compiler, action revisions,
  permissions, timeouts, and concurrency behavior.
- Keep one Linux validation platform until a supported capability requires a
  matrix.

#### Tag release CI

- Only an exact version tag runs `cargo build --release --locked`.
- Build in the pinned Anolis 8-compatible environment and validate execution on
  Anolis 8 plus Ubuntu.
- Validate tag/workspace/build-reported version equality.
- Package the exact build outputs once, generate SHA-256, extract the archive,
  and run production-binary acceptance.
- Upload only to the approved internal artifact destination after all gates
  pass; do not create a public GitHub Release.
- Add HDFS feature/acceptance jobs only when R2 becomes active.

Schema migration, security configuration, scheduled fault/soak, multi-platform
artifacts, and public release publication are added only when their supported
capability exists.

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
| Worker or Metadata exits without owned cancellation | Abrupt termination, incomplete drain, or non-deterministic restart | R0 lifecycle owner, SIGINT/SIGTERM production-process tests, bounded systemd stop timeout |
| Binary is built on a newer Ubuntu/glibc baseline | CI success but failure on internal Anolis nodes | Build on pinned Anolis 8-compatible glibc 2.28 baseline; execute on Anolis and Ubuntu |
| Development version counters are mistaken for released compatibility history | Confusing first-release support and migrations | One clean-install reset to version 1 before the first tag; monotonic decisions afterward |
| Source tests pass but the archive is incomplete or unusable | Internal users receive an untested artifact | Extracted-package production-binary acceptance before internal publication |
| Unauthenticated services are exposed outside a trusted network | Unauthorized data access | Internal-only alpha, safe local defaults, explicit trusted-network limitation; security is required before broader deployment |
| UFS version is weak or ambiguous | Stale or wrong data | Pilot immutable data; require strong version identity |
| Eviction precedes recoverability | Permanent data loss | Evict only verified UFS-backed or replicated blocks |
| Delta report keeps full rebuild behavior | Metadata lock and CPU saturation | Incremental BlockReportIndex |
| Extents remain inline | Large Raft entries and long apply stalls | Hard cap followed by paged extent revision |
| HA is built before product proof | High cost without user value | R3 Go/No-Go before M4 |
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
- No release-profile binary or distributable archive on ordinary push or merge;
  the exact version tag is the only release-build trigger.
- Internal publication consumes the already accepted archive and never rebuilds
  a second artifact.
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

### P0: `v0.1.0-alpha.1` functional release blockers

1. REL-001: Establish workspace `0.1.0-alpha.1` lockstep versioning and the
   internal Single-Metadata resident-storage product contract.
2. REL-002: Apply the clean-install version-1 normalization to Metadata marker,
   RocksDB schema, snapshot, Worker storage, block metadata, and deleting marker;
   update all focused compatibility tests.
3. REL-003: Implement owned, bounded SIGINT/SIGTERM shutdown for Metadata and
   Worker, including readiness transition, task cancellation, Raft shutdown,
   and successful same-version restart.
4. REL-004: Add the public `beryl` entry point, shared build-version output,
   explicit internal role commands, and side-effect-free aggregate
   `validate-conf`.
5. REL-005: Add the same-tag runnable Rust Client CRUD example used by both
   documentation and release acceptance.

### P1: Tag-only build, package, and internal release

1. REL-101: Define the pinned Anolis 8/x86_64/glibc 2.28 build environment and
   `cargo build --release --locked` entry point.
2. REL-102: Add one deterministic tarball layout, systemd units, VERSION
   manifest, and SHA-256 generation.
3. REL-103: Add extracted-package acceptance using production Metadata and
   Worker binaries for 1+1 and 1+2 topologies.
4. CI-104: Keep PR/main validation artifact-free; add no release build to normal
   push or merge.
5. CI-105: Add exact-tag validation, build-once/package-once acceptance, and
   internal artifact publication without a public GitHub Release.
6. DOC-106: Publish Getting Started, Deployment, Operations, Compatibility, and
   Known Limitations with trusted-network and clean-install warnings.
7. REL-107: Tag `v0.1.0-alpha.1`, deploy the accepted archive to Anolis 8 and
   Ubuntu, record results, and declare the internal release complete.

### P2: `0.1.0-alpha.N` stabilization

1. Fix only defects exposed by packaged installation, process lifecycle,
   configuration, observability, restart, recovery, or documentation.
2. Starting with `alpha.2`, test forward upgrade and rollback from the previous
   tagged alpha only if a migration is intentionally introduced.
3. Promote `0.1.0` only after repeated deployment and soak pass; do not add UFS
   merely to change the version label.

### P3: Next feature release, HDFS Read-Through Alpha

1. CACHE-201: Name the HDFS pilot workload and select immutable or HDFS Snapshot
   paths with authoritative version evidence.
2. CACHE-202: Define HDFS BackingFileVersion and read-only External Mount
   semantics owned by Metadata.
3. CACHE-203: Implement exact-version Worker HDFS fill into staging, validation,
   Ready publication, bounded concurrency, and interruption recovery.
4. CACHE-204: Implement Metadata miss coordination, duplicate-fill coalescing,
   exact-version refill, report convergence, and Rust Client read.
5. Publish the first R2 internal alpha only when the complete cold-miss-to-read
   chain passes packaged-binary restart and corruption tests.

### P4: HDFS cache pilot and evidence-triggered core work

1. CACHE-205: Implement disk-watermark admission and exact eviction without
   risking internal single-replica data.
2. CACHE-206: Add hit/miss, backend-byte/request, fill, version-mismatch, and
   eviction metrics.
3. CACHE-207: Implement one Hadoop FileSystem scheme connector.
4. CACHE-208: Run the same replayable trace against direct HDFS and the relevant
   mature alternative.
5. Pull CORE-101 incremental reports, CORE-103 paged extents, CORE-104 streaming,
   or CORE-105 load shedding only when R0 operations or the HDFS workload proves
   a concrete need.

### P5: Later production capabilities

1. HA-301: Implement three-node peer RPC and membership.
2. HA-302: Validate leader change, snapshot install, and rolling restart.
3. DATA-303: Implement exact ReplicaTransfer.
4. DATA-304: Implement multi-replica write and read availability.
5. SEC-305: Implement mTLS, identity, authorization, and quota before broader
   network deployment.
6. OPS-306: Implement schema migration, backup, and restore.
7. OPS-307: Complete fault and soak qualification.

## 15. Decision Summary

The required sequence is:

1. Freeze the current resident-storage product boundary for an internal alpha.
2. Normalize unreleased persistent-format counters to version 1 under the
   agreed clean-install cutover.
3. Complete owned process shutdown, binary/config contracts, and same-version
   restart recovery.
4. Build a release artifact only from an exact tag in the pinned Anolis 8
   environment.
5. Accept the extracted package with production binaries on Anolis 8 and Ubuntu,
   then publish it internally as `v0.1.0-alpha.1`.
6. Use `0.1.0-alpha.N` for deployment and recovery corrections, not ordinary
   feature expansion.
7. Build the next feature release as one exact-version HDFS cold-miss-to-read
   vertical slice.
8. Add HDFS admission, eviction, metrics, and a connector only after that slice
   is correct; continue only when the workload value gate passes.
9. Pull core performance/refactoring work from measured need rather than making
   it a standing prerequisite.
10. Add HA, replicas, security, migration, and sharding only behind their stated
    product and operational gates.

This sequence gives Beryl a real release baseline before HDFS integration while
preserving its strongest differentiator: a small, explicit authority model that
can support a reliable cache without silently serving stale, ambiguous, or
unauthorized data.
