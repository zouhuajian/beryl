# beryl-client

## Role

`beryl-client` exposes the Rust native Beryl API and orchestrates metadata and worker RPCs on behalf of callers.

## How It Fits Into Beryl

- Uses metadata RPCs for namespace, layout, visibility, and write-session authority.
- Uses worker RPCs for data reads and writes after metadata issues the required context.
- Presents Rust API types for files, readers, writers, namespace options, statuses, and listing iteration.

## Main Responsibilities

- `FsClient`, file readers/writers, operation options, `FileStatus`, and `ListStatusIterator`.
- Metadata RPC orchestration and metadata response validation.
- Worker RPC orchestration for metadata-authorized read, write, commit, sync, and abort.
- Client identity, call IDs, retry, refresh, replay, endpoint cache, and write-session state.

## Client Construction

`ClientConfig` is immutable after construction. Load the installed dotted-key
YAML with `ClientConfig::load`, or use `ClientConfig::builder` for embedded
callers and tests. Both paths apply the same defaults and validation. Creating
the runtime client revalidates the sealed value and therefore returns a
`ClientResult`:

```rust
use std::time::Duration;

use beryl_client::{ClientConfig, FsClient};

let config = ClientConfig::builder()
    .client_name("example-client")
    .metadata_endpoints(["127.0.0.1:18080"])
    .operation_timeout(Duration::from_secs(30))
    .build()?;
let client = FsClient::new(config)?;
# Ok::<(), beryl_client::ClientError>(())
```

The public crate surface exposes the filesystem API, sealed configuration and
builder, and stable client error types. Transport, routing, metrics plumbing,
and configuration parsing details remain crate-internal.

## Current Active Use

The Rust native API is the client interface used today. Its namespace surface follows common distributed-filesystem naming: `get_status`, `list_status`, `mkdirs`, `delete`, and `rename`. Methods with operation options use a `_with_options` suffix.

Reads fill caller-owned buffers through Worker requests bounded by
`beryl.client.read.max-request-bytes`. `FileReader::read` advances a sequential
position, while `read_at` and `read_exact_at` leave it unchanged.
`FileReader::read_to_end` is additionally bounded by
`beryl.client.read.max-buffered-bytes` and commits its position only after the
complete remaining file succeeds.

`FsClient::list_status` returns a `ListStatusIterator`. The client fetches one bounded page before returning it, then fetches later pages only as `next` consumes buffered statuses. Listing is non-recursive and weakly consistent across pages because Metadata retains no server-side snapshot.

`FsClient::delete` is non-recursive by default. `delete_with_options` accepts `DeleteOptions` for recursive namespace deletion. Physical reclamation remains asynchronous and uses the Metadata cleanup grace period.

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
writes deterministic data spanning multiple blocks, checks status and read
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
