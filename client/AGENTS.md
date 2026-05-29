# `client` Agent Instructions

This file applies to `client/`. Follow the root `AGENTS.md` first; this crate owns the public API and client-side orchestration.

## Scope

`client` owns:

- SDK facade and public API shape
- metadata RPC orchestration and response validation
- worker data-plane orchestration after metadata-issued layout, route, source, lease/fencing, and freshness context
- freshness, route epoch, mount epoch, worker run identity, block stamp, fencing, retry, refresh, replay, and unknown-outcome behavior
- worker endpoint cache, channel pooling, endpoint health, attempt scheduling, and client typed config
- session-scoped open/write/flush/sync/close state

`client` must not own metadata authority, server-side filesystem rules, worker internals, worker store/runtime behavior, generic schema ownership, or shared retry/cache policy in `common`, `types`, or `proto`.

## Local Rules

- Current focus is the normal client -> metadata -> worker data path, not metadata-free cached direct access.
- Do not bypass metadata for cached direct read/write unless an explicit design requests it.
- Keep route/cache state subordinate to metadata authority and freshness validation.
- Keep API modules simple. Do not split operations into tiny modules when `FsClient` remains the clearer public surface.
- Convert raw proto near metadata and worker service boundaries; use domain objects after boundaries where one exists.
- Avoid blind retry loops; classify retry, refresh, replay, endpoint invalidation, and unknown outcome explicitly.
- Do not add test-only cache seeding, injection, force, or fake APIs to production modules.

## Tests

- Test route refresh, not-leader refresh/replay, stale route/mount/worker-run/block-stamp behavior, follower-read watermark gating, group-scoped watermark comparison, write-session invalidation, fencing/session-expired behavior, transport-vs-business failure classification, and stale direct-path fallback-to-refresh where relevant.
- Assert explicit client actions and policies, not only eventual success.
- Use integration tests for end-to-end metadata/worker/client contracts.

## Local Self-Review

Apply the root self-review checklist, then check:

- Did client policy stay local and production dependencies avoid `metadata` and `worker`?
- Did worker access remain metadata-issued and validated?
- Did raw proto stay near service/adapter boundaries?
- Did retry/refresh/replay avoid blind fallback behavior?
- Did API shape stay simple without tiny operation modules or test-only production hooks?
