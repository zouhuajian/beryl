// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Minimal end-to-end demo: Using GrpcTransport to call WriteChunk on a remote worker.
//!
//! This example demonstrates:
//! 1. Creating a GrpcTransport instance
//! 2. Connecting to a remote worker
//! 3. Converting domain ChunkData to proto
//! 4. Calling WriteChunk via transport
//! 5. Verifying the response
//!
//! To run this example:
//! ```bash
//! cargo run --example replication_demo -- --worker-addr http://127.0.0.1:50051
//! ```
//!
//! Note: This requires a running worker server. You can start one with:
//! ```bash
//! cargo run --bin worker -- --bind-addr 127.0.0.1:50051
//! ```

use bytes::Bytes;
use clap::Parser;
use common::Deadline;
use proto::common::{BlockIdProto as ProtoBlockId, FencingTokenProto as ProtoFencingToken};
use proto::worker::WriteChunkRequestProto;
use std::time::Duration;
use transport::convert::chunk_data_to_proto;
use transport::{GrpcTransport, NetTransport};
use types::chunk::{ChunkData, ChunkRef, ChunkSlice};
use types::lease::FencingToken;
use types::{BlockId, BlockIndex, ClientId, DataHandleId};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Worker gRPC address (e.g., http://127.0.0.1:50051)
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    worker_addr: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    // Create transport
    let transport = GrpcTransport::with_default_config();
    println!("Created GrpcTransport");

    // Connect to worker
    let connection = transport.connect(&args.worker_addr).await?;
    println!("Connected to worker at {}", args.worker_addr);

    // Create a test chunk
    let data_handle_id = DataHandleId::new(1);
    let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
    let chunk_ref = ChunkRef::new(block_id, 0);

    let chunk_slice = ChunkSlice {
        chunk: chunk_ref.clone(),
        offset_in_chunk: 0,
        len: 1024,
    };

    let chunk_data = ChunkData {
        slice: chunk_slice,
        data: Bytes::from(vec![0x42u8; 1024]), // 1KB of test data
        checksum32: 0,                         // TODO: compute actual checksum
    };

    println!("Created test chunk: {} bytes", chunk_data.data.len());

    // Convert to proto
    let proto_chunk_data = chunk_data_to_proto(&chunk_data);
    println!("Converted ChunkData to proto");

    // Create fencing token (required for WriteChunk)
    let fencing_token = FencingToken::new(
        block_id,
        ClientId::new(1),
        1, // epoch
    );

    let proto_fencing_token = ProtoFencingToken {
        block_id: Some(ProtoBlockId {
            data_handle_id: block_id.data_handle_id.as_raw(),
            block_index: block_id.index.as_raw(),
        }),
        owner: fencing_token.owner.as_raw(),
        epoch: fencing_token.epoch,
    };

    // Create WriteChunkRequest
    let caller_ctx =
        common::header::RequestHeader::with_deadline(ClientId::new(1), Deadline::from_now(Duration::from_secs(30)));
    let _proto_header: proto::common::RequestHeaderProto = (&caller_ctx).into();

    let write_request = WriteChunkRequestProto {
        token: Some(proto_fencing_token),
        data: Some(proto_chunk_data),
        write_id: 12345, // Idempotency key
        write_mode: proto::common::WriteModeProto::WriteModeUnspecified as i32,
        route_epoch: 0,
        worker_epoch: 0,
        file_version: 0,
    };

    // Create request header
    let ctx =
        common::header::RequestHeader::with_deadline(ClientId::new(1), Deadline::from_now(Duration::from_secs(30)));

    println!("Calling WriteChunk via transport...");

    // Call WriteChunk
    let response = transport.call_write_chunk(&connection, write_request, ctx).await?;

    if response.stored {
        println!("✓ Chunk written successfully!");
    } else {
        println!("✗ Chunk write failed (stored = false)");
    }

    Ok(())
}
