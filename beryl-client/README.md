# beryl-client

## Role

`beryl-client` exposes the Rust native Beryl API and orchestrates metadata and worker RPCs on behalf of callers.

## How It Fits Into Beryl

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

`FsClient::delete` requires `DeleteOptions`, which currently controls recursive namespace deletion. Physical reclamation remains asynchronous and uses the Metadata cleanup grace period.

## Runnable CRUD Example

Build the example from the same checkout or release tag as the deployed
Metadata and Worker. With both services ready and `conf/client.yaml` pointing
at the client-reachable Metadata endpoint, and with every Worker advertising an
address reachable by that client, run a complete disposable CRUD roundtrip:

```bash
cargo run --locked -p beryl-client --example crud -- conf/client.yaml
```

The optional positional argument is the client configuration path and defaults
to `conf/client.yaml`. The example creates `/examples/rust-client-crud.bin`,
writes deterministic data spanning multiple blocks, checks stat and read
results, then deletes the file. It exits nonzero on any failure or mismatch.

This example intentionally demonstrates only the public Client API. Starting,
restarting, and validating packaged Metadata and Worker processes belongs to
the release acceptance workflow rather than this source example.

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
- Do not production-depend on `beryl-metadata` or `beryl-worker`.
