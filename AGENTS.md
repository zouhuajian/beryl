# Beryl Agent Instructions

These rules apply across the repository. Read the local instructions for each
subtree involved in the task; they add crate-specific responsibilities and
invariants.

## Work Contract

- Follow the requested mode. Analysis, review, and planning are read-only unless
  the user also authorizes implementation.
- Before changing files, inspect the worktree and task-related differences.
  Read code, tests, and documentation as needed to establish the affected
  behavior and boundaries; do not require a full repository survey.
- Preserve unrelated changes and existing external contracts unless the user
  explicitly authorizes changing them.
- Within an authorized implementation, complete the requested behavior, correct
  regressions introduced by the change, and finish necessary validation.
  Respect user-requested checkpoints.
- Resolve routine implementation choices autonomously. Ask only when missing
  information materially changes scope, correctness, risk, or authorization.
- Git operations require authorization for the requested operation and include
  its necessary routine steps. Permission to commit includes staging reviewed
  changes, but does not authorize pushing or merging.

## Design and Scope

- Resolve trade-offs in this order: correctness, safety and invariants,
  simplicity, readability, then abstraction. Make material trade-offs explicit.
- Prefer direct, local implementations. Introduce abstractions only for a real
  boundary, invariant, external dependency, stable duplication, or critical
  testing need.
- Keep changes within the current requirement. Remove obsolete behavior when
  replacing it; do not add speculative capabilities or compatibility paths.
- Keep these instructions focused on responsibilities, observable contracts,
  and engineering constraints. Put implementation details and code walkthroughs
  in task-relevant documentation.

## Product Boundary

- The supported runtime has one metadata group and one metadata leader.
- The Rust native client is the supported client interface. Data access goes
  through metadata-authorized worker storage.
- The internal writable namespace is unified; names alone do not establish
  separate authority or storage behavior.
- Multi-group metadata, metadata peer services, administration APIs, replication,
  repair, rebalancing, alternate transports, POSIX, FUSE, Hadoop compatibility,
  and external-storage IO are outside the current supported product boundary.
  Expanding it requires an explicit user request and complete end-to-end work.
- Do not present internal primitives or partial implementations as supported
  product capabilities.

## Architectural Boundaries

- Metadata owns namespace and data-access authority. Workers own local data
  execution. The client coordinates those boundaries; the CLI owns process
  entry and command behavior.
- Shared domain values, common infrastructure, and wire contracts have distinct
  ownership. Keep service policy in its owning crate.
- Production dependencies must preserve these boundaries: the CLI and client
  must not depend on Metadata or Worker implementations; Worker must not depend
  on Metadata or Client implementations; Metadata must not depend on Worker or
  Client implementations.
- Shared crates must remain independent of runtime implementations. Test
  dependencies must not leak into production dependency relationships.

## Correctness and Recovery

- Identify authoritative state before changing distributed or persistent
  behavior. Preserve identity, ordering, fencing, freshness, and visibility
  across concurrency, communication, and restart boundaries.
- Treat cancellation, timeout, partial IO, duplicate delivery, and restart as
  normal failure cases. Distinguish definite failure from unknown outcome;
  retry only when side-effect and replay semantics make it safe.
- Fail closed when authority, persisted state, or destructive-operation
  preconditions cannot be verified. Do not turn consistency failures into stale
  success or silent fallback.
- Destructive operations require exact targets, protection over the full
  lifetime of affected activity, retry safety, and defined crash recovery.
- Keep resource use and recovery work bounded.

## Code and Tests

- Use the narrowest necessary visibility. Do not add production interfaces or
  widen access solely to support tests.
- Keep Rust comments and documentation in English. Explain stable contracts,
  non-obvious reasons, and failure behavior rather than task history.
- Address warnings in affected code rather than suppressing them without a
  concrete reason.
- Keep unit tests and their helpers together after production items, in a single
  terminal test module or a separate test file declared there. Keep test-only
  declarations and conditional test logic out of production items.
- Test private behavior within its owning module and cross-module behavior
  through existing production boundaries.
- Add tests for observable regressions, non-trivial invariants, and meaningful
  failure cases. Distinguish externally observable contracts from incidental
  source shape or obsolete names when selecting coverage.
- Use deterministic coordination for concurrency tests. Preserve relevant
  recovery, restart, and compatibility coverage.
- Do not reorganize unrelated tests merely because their file is touched.

## Validation

- Select validation from the actual differences, affected callers, and risk.
  Start with focused checks; use the project's existing build and validation
  entry points.
- Check every change for whitespace errors. For Rust changes, check formatting,
  run affected tests, and verify compilation and lint checks for affected
  targets.
- Shared-contract, dependency, or build changes require validation of affected
  producers and consumers. Run workspace-wide checks when impact spans the
  workspace or cannot be safely bounded.
- Public behavior, cross-crate orchestration, communication contracts,
  persistence, restart, and lifecycle changes require relevant end-to-end
  coverage through the supported runtime.
- Run required broader checks after the final differences are stable. Reuse
  results that cover the same code state; repeat or expand checks only for new
  changes, failures, unresolved risks, or explicit user requirements.
- Documentation-only changes need content and difference review, not runtime
  tests, unless executable examples, generated artifacts, or validation tooling
  are affected.

## Review and Handoff

- Lead reviews with the overall conclusion. Classify findings as Blocking,
  Non-blocking, or Notes, with concrete evidence, impact, and the smallest safe
  correction.
- Report actual changes, validation performed, relevant checks not run, and
  remaining uncertainty. Passing tests are evidence, not proof of completeness.
- Keep commit and pull-request metadata concise and in English. Use a
  conventional commit subject with a type, scope, and outcome.
