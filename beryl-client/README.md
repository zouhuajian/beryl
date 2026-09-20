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

`get_status`, listing entries, directory creation, and `FileReader::status()`
reuse the same inode status fields: identity, kind, visible length, content
generation for files, and timestamps. The client re-exports `beryl_types::FileStatus`.
Its optional `path` records the request path or the full path of a listed child;
status without path context has `None`. The path is observation context and does
not participate in reader identity, generation, or length checks.

`open(path)` validates the file and returns its status without consulting Worker
locations. Reader operations specify the actual position and range; layout
queries return the full visible blocks intersecting that range, clipped at EOF.
The existing inline file block limit bounds layout size. Readers retain one layout
and reuse it for sequential and positioned reads, fetching another range on a
cache miss or refreshing failed locations. Open captures inode, generation, and
length; newly fetched layouts must match those values. Cached layouts do not
guarantee immediate detection of changes or deletion, and do not retain
historical contents. Renames do not rebind a reader to a different inode.

`FileReader` implements `futures::io::AsyncRead` and `AsyncSeek`. Import the
futures `AsyncReadExt` and `AsyncSeekExt` traits to use `read`, `read_exact`,
`read_to_end`, and `seek`; `futures::io::copy` works directly. `position()` tracks
only delivered bytes and successful seeks. Seeking uses `std::io::SeekFrom`,
allows positions beyond EOF, and cancels pending IO and buffered data without
querying Metadata. A cancelled read wait retains its pending request and any
undelivered data until another read, seek, or reader drop.

`read_range(range)` returns owned `Bytes` for the complete file-relative range
without moving the stream position; shared references support concurrent calls.
Unbounded endpoints use zero and the length captured at open. Explicit endpoints
beyond that length return `UnexpectedEof`; reversed ranges and boundary overflow
return `InvalidArgument`. Valid empty ranges do no IO. The result size is bounded
by `beryl.client.read.max-range-bytes` (builder: `read_range_limit`) before any
allocation or IO.

Both interfaces use the same bounded block read, layout validation, and retries.
On a layout miss, the client queries the block containing the actual offset,
then limits the Worker request to that block and
`beryl.client.read.max-request-bytes`. A future block's unavailability cannot
prevent delivery of the current block. The client accepts a bounded response
only after receiving exactly the requested bytes and a normal stream end.
Worker frames are collected into one owned request buffer. Stream reads copy
the result into the caller's buffer. A range completed by one request returns
that buffer directly; ranges spanning requests allocate a bounded aggregate.
Converting an owned `Vec` to `Bytes` reuses its allocation. Each in-flight read
uses at most one request buffer in addition to transport buffers and any range aggregate.
Concurrent range reads each have their own buffers, with no reader-wide total
memory budget.

Transient Worker failures retry the same locations and retain the layout on
success. A requested layout refresh or terminal read failure invalidates only
the layout used by that request, preserving any concurrent replacement. A later
read can then discover a replacement for an unreachable endpoint.

Each `read_range` shares one operation deadline across all chunks and retries.
Each underlying stream read has its own deadline, including retries. Callers
control the total timeout and destination size of `read_exact`, `read_to_end`,
and `copy`; the range limit does not bound their destination `Vec`. On a later
failure, the stream keeps progress from already delivered bytes. Stream errors
use `std::io::ErrorKind` and retain `ClientError` as the inner error, accessible
with `get_ref().and_then(|error| error.downcast_ref::<ClientError>())`.

The public IO traits do not depend on Tokio, but the current implementation
still requires a Tokio runtime for Tonic and timers. Tokio IO callers can use
`tokio-util` with its `compat` feature:

```rust
use tokio_util::compat::FuturesAsyncReadCompatExt;

let mut reader = client.open("/file").await?.compat();
let mut bytes = Vec::new();
tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut bytes).await?;
```

`FsClient::create` and `append` return a `FileWriter`. Its native `write`,
`write_all`, `flush`, `sync`, `close`, `abort`, and `renew_lease` methods all
return `ClientResult`. No extension trait import is required.

| Operation | Successful completion |
| --- | --- |
| `write` / `write_all` | Bytes accepted into the bounded Worker request stream; `position()` advances. |
| `flush` | All accepted bytes checkpointed durably at the Worker; Metadata visibility stays unchanged. |
| `sync` | Worker checkpoint complete and Metadata-visible contents published; the lease stays open. |
| `close` | Final contents published and the lease ended with durable commit evidence. |
| `abort` | The write session ended; previously synced contents remain visible. |

New blocks are allocated on demand. Append reuses the authorized partial tail.
Flush finishes the current block stream; later writes reopen its durable tail
under the same lease. Frequent flushes therefore add authorization RPCs and
Worker checkpoints. Empty writes perform no IO, and flushing an empty writer
does not allocate a block. Successful close is idempotent; subsequent nonempty
writes are rejected.

```rust,no_run
# async fn write(client: &beryl_client::FsClient) -> Result<(), Box<dyn std::error::Error>> {
let mut writer = client.create("/file").await?;
writer.write_all(b"published prefix").await?;
writer.sync().await?;
writer.write_all(b" and suffix").await?;
writer.close().await?;
# Ok(())
# }
```

Each accepted frame copies at most 1 MiB from the caller's slice into owned
`Bytes`. The request channel has one slot in addition to transport buffers.
Each `write` accepts bytes only after block preparation and capacity reservation
complete, then advances the position without another await. A failed or cancelled
`write_all` can have accepted a prefix; inspect `position()` and do not replay the
entire input blindly.
Writing a full block does not itself promise durability; use `flush` to confirm
it.

Native methods execute directly with one deadline per write step, flush, sync,
or close. Callers control the total timeout of `write_all`.
Cancelling pending write preparation, renewal, or a Worker checkpoint
makes the writer unusable for further writes or publication. Drop that writer;
Metadata lease expiry and server cleanup resolve any abandoned work.

Once a sync, commit, or abort request has been frozen for Metadata, cancellation
or an unknown result retains its call identity and exact payload. Retry the same
method on the same writer; an explicit retry gets a new bounded deadline. Other
operations cannot replace that unresolved intent. Cancellation during the Worker
checkpoint before publication does not provide this recovery path.

Automatic lease renewal occurs during writer operations, not in an idle
background task. `renew_lease` explicitly renews an open session. Dropping a
writer requests local Worker cancellation without committing or aborting the
Metadata session; lease expiry and server cleanup retain authority over it.
Submitted Metadata publications retain their server-side protection independently
of the caller's wait.

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
