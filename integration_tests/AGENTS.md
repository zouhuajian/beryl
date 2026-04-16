# integration_tests/AGENTS.md

This file applies to `integration_tests/`.

## 1. Directory purpose

`integration_tests/` verifies end-to-end and cross-module contract behavior.

This directory owns:

- cross-crate correctness scenarios
- client ↔ metadata ↔ worker ↔ ufs interactions
- refresh-replay and structured recovery scenarios
- authority and routing contract validation
- restart/recovery and persistence-boundary validation
- replication / relocation / repair safety scenarios
- system-level negative tests for stale or invalid context

These tests must validate system semantics, not only isolated implementation details.

## 2. What must NOT live here

Do not put the following into `integration_tests/`:

- unit tests that only exercise one leaf function with mocks
- tests that duplicate crate-local unit test coverage without adding end-to-end value
- brittle snapshot tests of unstable strings/logs as primary correctness proof
- environment-coupled tests with hidden assumptions
- happy-path-only tests that ignore recoverable mismatch conditions
- “smoke only” tests presented as full contract coverage

If a test does not validate a cross-module contract or lifecycle boundary, question whether it belongs here.

## 3. Core principle: test contracts, not only code paths

Integration tests must validate the system’s declared behavior.

Rules:

- assert authority semantics, not just successful responses
- assert structured error classes/reasons where recoverable failures are expected
- assert refresh-replay behavior, not merely “eventually succeeded”
- assert identity/epoch/fencing behavior explicitly
- prefer semantic checkpoints over incidental implementation details

A passing request is not enough if it passed for the wrong reason.

## 4. Required contract areas

A healthy integration test suite should cover the relevant subset of the following areas.

### 4.1 Metadata authority

- inode / dentry / attrs authority
- path traversal as adapter behavior
- longest-prefix mount resolution
- child-overrides-parent mount behavior
- same-mount rename atomicity
- mount ownership/group routing behavior

### 4.2 Client refresh-replay

- not-leader handling
- stale route refresh
- mount epoch mismatch handling
- worker epoch mismatch handling
- stale state watermark handling
- session invalid / expired handling where applicable

### 4.3 Data-plane semantics

- Block / Chunk / Stream separation in real flows
- tail block / tail chunk behavior
- persisted layout interpretation across reopen/restart
- direct client→worker path validation
- route / version / fencing mismatch rejection

### 4.4 Replication / movement / repair safety

- copy + verify + evict style move safety
- partial ready-chunk transfer behavior where applicable
- stale or invalid destination/source handling
- destructive action gating
- idempotent retry behavior for repair/relocation tasks where required

### 4.5 Restart and recovery

- metadata restart / replay
- worker restart with persisted local state
- cache invalidation or refresh after restart
- persistence of authoritative vs transient state distinctions

## 5. Negative testing is mandatory

Vecton’s correctness depends heavily on rejection and recovery behavior.

Rules:

- include stale route cases
- include stale epoch/version cases
- include fencing/session invalidation cases
- include not-leader or equivalent refreshable mismatch cases
- include invalid ownership/mount resolution cases where relevant
- include restart/recovery scenarios that prove no hidden authority drift occurred

A suite that only proves success cases is incomplete.

## 6. Error assertion rules

When structured errors exist, assert them structurally.

Rules:

- prefer asserting error class / code / reason / retry hint fields
- do not rely on free-form message text as the primary oracle
- distinguish business/consistency failures from transport failures
- where transport failure is part of the test, assert that it stayed on the correct error channel
- do not hide semantic mismatches behind generic `is_err()` assertions

Tests should fail when semantics drift, not only when APIs panic.

## 7. Scenario design rules

Integration tests should be realistic but controlled.

Rules:

- create scenarios that cross real module boundaries
- keep fixtures small and explicit
- use deterministic setup where possible
- avoid over-mocking critical boundaries whose semantics are exactly what the test should validate
- when multiple phases exist, make them explicit: setup, action, failure/signal, refresh/replay, final validation
- name tests after the contract they prove

Good integration tests read like executable contract cases.

## 8. Flakiness rules

Do not accept flaky tests as normal.

Rules:

- avoid time-based races without bounded synchronization or explicit readiness checks
- avoid relying on log ordering as the correctness oracle
- use deterministic waits/checkpoints where possible
- if eventual consistency is part of the design, assert the designed convergence signal, not arbitrary sleeps
- keep randomness controlled and seeded when used

A flaky contract test is a weak contract test.

## 9. Coding rules for this directory

- prefer helper fixtures/builders that expose semantics clearly
- avoid giant opaque setup functions that hide the actual contract under test
- isolate reusable cluster/test-harness helpers from scenario-specific assertions
- keep assertions close to the triggering action
- comments should explain contract intent, not narrate trivial setup
- when a regression test exists for a previous bug, say what semantic bug it prevents

## 10. Minimum expectations for new features or fixes

A meaningful feature or bugfix that affects system behavior should usually add or update at least one integration test when it changes any of the following:

- external filesystem-visible behavior
- routing/refresh/replay behavior
- identity/epoch/fencing behavior
- persisted-state interpretation
- worker direct-path validation
- relocation/repair/destructive safety gates
- restart/recovery semantics

If no integration test is added, the reason should be explicit and credible.

## 11. Pre-merge checklist

Before submitting an integration test change, verify:

- does this test validate a real cross-module contract?
- does it prove semantic behavior rather than only success?
- does it assert structured errors where available?
- does it include the relevant stale/recovery/negative path?
- does it avoid brittle string/log assertions as the main oracle?
- does it avoid uncontrolled timing/flakiness?
- would this test fail if the contract regressed but the happy path still worked?