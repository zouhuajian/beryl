# client

## Role

`client` exposes the Rust native Vecton API and orchestrates metadata and worker RPCs on behalf of callers.

## How It Fits Into Vecton

- Uses metadata RPCs for namespace, layout, visibility, and write-session authority.
- Uses worker RPCs for data reads and writes after metadata issues the required context.
- Presents Rust API types for files, readers, writers, options, statuses, and listings.

## Main Responsibilities

- `FsClient`, file readers/writers, operation options, and status/listing types.
- Metadata RPC orchestration and metadata response validation.
- Worker RPC orchestration for metadata-authorized read, write, commit, sync, and abort.
- Client identity, call IDs, retry, refresh, replay, endpoint cache, and write-session state.

## Current Active Use

The Rust native API is the client interface used today. It supports core operations such as status, non-recursive list, mkdirs, namespace delete, rename, open, create, append, read, write, sync, close, and abort.

`ListOptions::recursive` is part of the Rust API shape, but recursive listing is not supported by the current metadata service. Requests with that flag are rejected instead of silently falling back to non-recursive listing.

## Not in Current Scope

- POSIX API.
- FUSE client.
- Hadoop-compatible filesystem client.
- Metadata-free direct worker reads or writes.
- Separate UFS-backed cache semantics.
- Recursive directory listing.

## Contributor Notes

- Keep the public API Rust-native and simple.
- Preserve client identity, call ID, retry, replay, and freshness semantics.
- Do not production-depend on `metadata` or `worker`.
