# `client` Agent Instructions

This file applies to `client/`. Follow the root `AGENTS.md` first, then these local rules.

## Crate role

`client` owns SDK behavior, metadata gateway orchestration, routing/session state, layout and worker endpoint caches, retry/replay classification, planner behavior, and data adapter orchestration.

## Allowed changes

- Filesystem-facing client request orchestration and SDK ergonomics.
- Metadata gateway behavior and service-boundary adapters.
- Layout cache, route cache, worker endpoint cache, and state watermark tracking.
- Refresh/replay classification and retry planning.
- Planner behavior and data-plane adapter orchestration.
- Session-scoped open/write/flush/sync/close state.
- Client typed config, defaults, and validation.

## Forbidden changes

- Metadata authority policy, inode/dentry/mount authority, or server-side filesystem rules.
- Worker store/runtime behavior or worker net server implementation details.
- Generic shared schema ownership.
- Long-lived raw proto state when a domain model exists.
- Moving retry, replay, cache, endpoint-health, or SDK error policy into `common`, `types`, or `proto`.
- Blind retry loops without semantic classification.
- Hidden fallback behavior that bypasses refresh or validation.

## Dependency rules

- `client` may depend on shared crates where appropriate.
- `client` must not depend on `metadata` or `worker` in production code.
- Prefer domain objects over raw proto messages after service/adapter boundaries.
- Keep test-only dependencies explicit and narrow.

## Conversion and validation rules

- Convert raw proto near metadata and worker service boundaries.
- Use shared `proto` conversion helpers for structural conversion when available.
- Client policy stays local: retry/replay classification, cache invalidation, endpoint health, SDK error mapping, and attempt scheduling.
- Validate received layouts, worker endpoints, route epochs, mount epochs, worker epochs, state watermarks, fencing/session signals, and replay safety before use.
- Keep transport failure handling distinct from business-level recoverable failure handling.

## Testing guidance

- Add or update tests for route cache refresh, not-leader refresh/replay, stale route/mount/worker epoch behavior, follower-read watermark gating, group-scoped watermark comparison, write-session invalidation, fencing/session-expired handling, transport-vs-business failure classification, and direct-path stale cache fallback-to-refresh where relevant.
- Tests should assert explicit client actions and policies, not only eventual success.
- Use integration tests for end-to-end metadata/worker/client contracts.

## Documentation guidance

- Document replay conditions, invalidation triggers, endpoint selection, state watermark behavior, and SDK-facing safety boundaries.
- Update README or local docs when SDK behavior, config ownership, or cache/replay contracts change.
- Do not document compatibility bridges for internal-only stale APIs unless explicitly required.

## Review checklist

- Did client policy stay local?
- Did raw proto stay near service/adapter boundaries?
- Did the change avoid production dependencies on `metadata` and `worker`?
- Did it preserve refresh/replay semantics instead of blind retry?
- Did it keep route/cache state subordinate to server authority?
- Did it preserve identity, session, epoch, fencing, and state watermark separation?
- Did tests/docs stay aligned with the client contract?
