# ufs

## Role

`ufs` is Vecton's external storage adapter boundary. It isolates backend configuration, capability description, and adapter construction from the current metadata and worker runtime.

## How It Fits Into Vecton

- Provides a crate boundary for future external storage integration.
- Describes backend capabilities and unsupported behavior explicitly.
- Keeps backend-specific mechanics separate from metadata, worker, and client policy.

## Main Responsibilities

- Backend specs, backend-specific config, defaults, validation, and construction.
- OpenDAL adapter setup for supported backend kinds.
- UFS metadata/data operation traits and backend capability mapping.
- Explicit unsupported-behavior handling for backend semantics.

## Current Active Use

`ufs` provides adapter types and traits, but current Vecton file IO does not use it for reads or writes. Current file IO uses metadata-authorized worker storage.

## Not in Current Scope

- Metadata namespace authority.
- Worker local block-store behavior.
- Client retry/replay/cache behavior.
- UFS-backed cache semantics.
- Current read-through or write-through file IO.

## Contributor Notes

- Do not document read-through/write-through as implemented unless the current read/write path proves it.
- Preserve unified Vecton resident-data semantics in future UFS integration.
- Keep unsupported backend behavior explicit instead of silently emulating it.
