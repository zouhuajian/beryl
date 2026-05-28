# `integration_tests` Agent Instructions

This file applies to `integration_tests/`. Follow the root `AGENTS.md` first; this crate validates observable cross-crate contracts.

## Scope

`integration_tests` owns:

- end-to-end fixtures and controlled mock servers
- client/metadata/worker/UFS interaction scenarios
- contract assertions for authority, routing, freshness, retry/replay, identity, epoch, fencing, persistence, restart, and data-plane validation
- raw proto messages when validating wire contracts
- system-level negative tests for stale or invalid context

`integration_tests` must not own production helpers, canonical conversion code, runtime helpers used by product crates, new production abstractions introduced from tests, or happy-path-only tests presented as full contract coverage.

## Local Rules

- Production crates must not depend on `integration_tests`.
- Fixtures must remain test-only and explicit enough that the contract under test is visible.
- Raw proto usage is acceptable for wire-contract validation, not as canonical conversion ownership.
- Prefer deterministic setup and readiness checks over arbitrary sleeps.
- Avoid brittle string/log snapshot assertions as the primary oracle when structured fields exist.

## Tests

- A passing request is insufficient if it can pass for the wrong semantic reason.
- Assert structured errors by class, reason, and retry hint when structured fields exist.
- Name tests after the contract they prove.
- Comments should explain contract intent or prior regression semantics, not the mechanics of the test.

## Local Self-Review

Apply the root self-review checklist, then check:

- Does the test validate a real cross-module contract?
- Would it fail if the contract regressed but the happy path still worked?
- Did fixtures remain test-only and outside production crates?
- Did assertions avoid uncontrolled timing and brittle string/log oracles?
- Is raw proto usage limited to wire-contract validation?
