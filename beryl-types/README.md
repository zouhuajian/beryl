# beryl-types

## Role

`beryl-types` owns stable Rust domain and value types shared by Beryl crates. It keeps cross-crate identity, layout, block, lease, and freshness values explicit without choosing runtime policy.

## How It Fits Into Beryl

- Gives metadata, worker, client, proto conversion, and shared infrastructure code a common domain vocabulary.
- Keeps generated wire values separate from Rust domain values.
- Helps preserve invariants at crate boundaries through typed values and validation.

## Main Responsibilities

- IDs and value types for namespace, blocks, chunks, mounts, workers, streams, leases, and requests.
- File layout, block format, committed block, write target, byte range, worker run identity, epoch, and watermark values.
- Small constructors and validation helpers for shared domain invariants.

## Current Active Use

The current runtime uses `beryl-types` for metadata authority values, worker block/data-plane context, client RPC orchestration, and proto/domain conversion.

## Not in Current Scope

- Runtime policy for metadata, worker, client, or UFS.
- Generated proto types or gRPC service definitions.
- Test fixtures or generic utility dumping ground behavior.
- Future-only values unless current code needs them.

## Contributor Notes

- Keep values small, explicit, and stable.
- Add shared types only when more than one production crate needs the contract.
- Avoid turning implementation details into cross-crate domain concepts.
