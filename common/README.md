# common

## Role

`common` owns shared infrastructure that is useful across Vecton crates but does not define product behavior.

## How It Fits Into Vecton

- Provides the shared error, header, config, retry/time, and observability foundation used by current services.
- Keeps infrastructure mechanics separate from metadata, worker, client, and UFS policy.
- Supports structured operational behavior without owning runtime decisions.

## Main Responsibilities

- RPC errors and request/response headers.
- Config loading, flattening, environment-key mapping, and validation mechanics.
- Retry/time helpers, observability setup, and small module-independent utilities.

## Current Active Use

The current runtime uses `common` for config mechanics, structured error/header handling, retry/time utilities, and tracing/metrics setup across metadata, worker, client, proto, and UFS code.

## Not in Current Scope

- Metadata authority policy.
- Worker execution policy.
- Client retry/cache decisions.
- UFS backend policy.
- Generated proto types or gRPC adapters.

## Contributor Notes

- Keep errors structured, structured, and machine-usable.
- Keep service-specific behavior in the owning crate.
- Do not hide operational failures behind generic string-only errors.
