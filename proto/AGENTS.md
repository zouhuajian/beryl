# `proto` Agent Instructions

This file applies to `proto/`. Follow the root `AGENTS.md` first, then these local rules.

## Crate role

`proto` owns protobuf schema, generated Rust modules, gRPC service contracts, wire enum numeric values, and structural proto/domain conversion.

## Allowed changes

- `.proto` files and generated Rust module exports.
- gRPC service contracts and wire messages.
- Wire enum numeric values and field-level schema contracts.
- Structural conversion between generated proto types and `types`/`common` domain types.
- Schema-local codecs where the persisted or transported payload is protobuf.
- Compatibility notes when an external contract requires them.

## Forbidden changes

- Business policy, authority decisions, retry/replay/cache behavior, endpoint-health policy, or route refresh decisions.
- Metadata authority routing, worker store/runtime behavior, UFS behavior, or client SDK policy.
- Product crate dependencies on `metadata`, `worker`, `client`, or `ufs`.
- Duplicated shadow models that compete with `types`.
- Proto messages added only because two Rust structs look similar.
- Compatibility shims without documented external requirements.

## Dependency rules

- `proto` may depend on `types` and `common`.
- `proto` must not depend on `metadata`, `worker`, `client`, or `ufs`.
- Schema changes must consider generated exports, active Rust consumers, wire numeric values, and external compatibility.

## Conversion and validation rules

- Keep structural proto/domain conversion in this crate when it is shared.
- Do not encode business policy in conversion helpers.
- Do not silently change enum numeric values.
- Do not rely on free-form strings for machine recoverability.
- Raw proto should stay near service or adapter boundaries; product runtime code should use domain models where one exists.
- Preserve identity separation in schema: inode identity, data identity, session handles, block identity, route epoch, mount epoch, worker epoch, and state watermark are distinct.

## Testing guidance

- Meaningful schema changes require generated-code rebuild and compilation of Rust callers.
- Add tests for structured error mapping, identity/epoch propagation, and conversion behavior where relevant.
- Remove obsolete fields/usages in the same change when compatibility does not require keeping them.
- Do not stop at "proto compiles" when the wire contract changed.

## Documentation guidance

- Proto comments must explain contract semantics, required fields, authoritative/advisory status, retry/refresh behavior, and compatibility implications.
- Avoid comments that restate field names or preserve stale migration notes without an owner.
- Update root docs or README when schema ownership or conversion ownership changes.

## Review checklist

- Did this preserve wire numeric values unless an explicit schema break is intended?
- Are active consumers and generated Rust references accounted for?
- Is the change schema/structural conversion only, with policy kept local?
- Did it avoid duplicate error paths and duplicate headers?
- Did it preserve Block/Chunk/Stream and identity separation?
- Is any compatibility shim backed by a documented external requirement?
