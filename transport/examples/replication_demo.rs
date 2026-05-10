// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Stream-v2 replication placeholder.
//!
//! WorkerDataService v1 chunk RPCs were removed. Real replication transfer must
//! be rewired through the block-local Stream v2 API in a later phase.

use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Worker gRPC address reserved for the future Stream v2 replication demo.
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    worker_addr: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    println!(
        "replication demo for {} is pending WorkerDataService Stream v2 wiring",
        args.worker_addr
    );
    Ok(())
}
