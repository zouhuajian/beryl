# `integration_tests` Agent Instructions

This file applies to `integration_tests/`. Follow the root `AGENTS.md` first, then these local rules.

## Crate role

`integration_tests` owns end-to-end and cross-crate contract validation. It is test-only and must not become a source of production helpers.

## Allowed changes

- End-to-end fixtures and controlled mock servers.
- Client/metadata/worker/UFS interaction scenarios.
- Contract assertions for authority, routing, refresh/replay, identity, epoch, fencing, persistence, and restart behavior.
- Raw proto messages for wire-contract validation.
- System-level negative tests for stale or invalid context.

## Forbidden changes

- Production shared helpers.
- Canonical conversion code.
- Runtime helpers used by product crates.
- New production abstractions introduced from tests.
- Happy-path-only tests presented as full contract coverage.
- Brittle string/log snapshot assertions as the primary oracle.

## Dependency rules

- `integration_tests` may depend on production crates to validate observable contracts.
- No production crate may depend on `integration_tests`.
- Test fixtures must remain test-only and must not be imported by production code.

## Conversion and validation rules

- Raw proto use is acceptable when validating wire contracts.
- Do not make tests the canonical owner of proto/domain conversion.
- Assert structured errors by class/reason/retry hint when structured fields exist.
- Keep assertions focused on observable behavior and cross-crate contracts.

## Testing guidance

- Cover metadata authority, client refresh/replay, data-plane validation, replication/repair safety, restart/recovery, stale route/epoch/fencing/session cases, and negative paths where relevant.
- Prefer deterministic setup and readiness checks over arbitrary sleeps.
- Keep fixtures explicit enough that the contract under test remains visible.
- A passing request is insufficient if it can pass for the wrong semantic reason.

## Documentation guidance

- Name tests after the contract they prove.
- Comments should explain contract intent or prior regression semantics.
- Update local docs when test harness behavior or observable cross-crate coverage changes.

## Review checklist

- Does the test validate a real cross-module contract?
- Would it fail if the contract regressed but the happy path still worked?
- Does it assert structured errors where available?
- Does it avoid production helper leakage?
- Does it avoid uncontrolled timing and brittle string/log oracles?
- Is raw proto usage limited to wire-contract validation?
