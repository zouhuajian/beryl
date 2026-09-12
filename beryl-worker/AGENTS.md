# beryl-worker Agent Instructions

Follow the repository root instructions.

## Responsibility

Own local storage, data execution, local lifecycle, and reporting of worker
state. Metadata retains namespace, layout, and visibility authority.

## Invariants

- Validate metadata authorization and exact local identity before accessing or
  changing data. Internal identity alone does not authorize external access.
- Serve only data whose persisted state, publication, and authority permit it.
- Coordinate destructive changes with the full lifetime of active readers and
  writers; preserve version-sensitive retry safety and crash recovery.
- Complete recovery or isolate incomplete local transitions before advertising
  or serving affected data.
- Failure or cancellation must not silently restore unsafe availability.
- Reports must describe recoverable local state, not intended outcomes.

## Validation Focus

Verify affected storage and reporting behavior, especially publication,
concurrent access, cancellation, interrupted mutations, and restart recovery.
