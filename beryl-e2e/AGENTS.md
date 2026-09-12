# beryl-e2e Agent Instructions

Follow the repository root instructions.

## Responsibility

Own black-box validation of the supported runtime across public client,
metadata, worker, persistence, and communication boundaries.

## Invariants

- Exercise user-visible behavior through public runtime boundaries.
- Isolate each test's service endpoints, identities, and persistent state.
- Use bounded readiness checks and deterministic failure coordination.
- Keep fault injection within the test harness and clean up its effects and
  resources after each test.
- Assert required recovery and convergence outcomes as well as immediate results.
- Do not mask failures with blind retries, disabled coverage, or assertions that
  accept incompatible outcomes.

## Validation Focus

Run scenarios affected by the change and verify that startup, failure
orchestration, shutdown, and cleanup remain isolated and bounded.
