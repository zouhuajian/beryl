# proto Agent Instructions

## Crate Boundary

`proto` owns protobuf/gRPC schema, generated modules, and structural conversion helpers. Schema changes are compatibility-sensitive.

## Allowed Changes

- Update wire contracts when a current caller or handler requires the change.
- Add or adjust structural conversion between generated proto values and `types`/`common` values.
- Clarify wire comments for current behavior, freshness, fencing, stream semantics, and compatibility.
- Add conversion or error-mapping tests when schema/conversion behavior changes.

## Do Not Do

- Do not change schema casually.
- Do not reuse or silently change field numbers or enum values.
- Do not add future service contracts unless explicitly requested.
- Do not put business policy, authority decisions, retry/cache policy, or worker execution here.
- Do not add compatibility aliases or decode fallbacks without a real external requirement.

## Cross-Crate Rules

- Current services are metadata filesystem, metadata-worker control, and worker data.
- Keep generated proto values at service boundaries and convert to domain types where available.
- Treat admin, peer, or shard-style proto contracts as future/experimental unless runtime code used today proves otherwise.

## Validation Notes

- Root workspace validation applies.
- For focused checks, use `cargo test -p proto`.
- Schema changes require generated-code rebuild and current caller compilation.
