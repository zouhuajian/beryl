// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protos = [
        "common/common.proto",
        "common/header.proto",
        "common/errors.proto",
        "metadata/filesystem.proto",
        "metadata/worker.proto",
        "worker/data.proto",
        "worker/data_header.proto",
        "worker/block_meta.proto",
    ];
    for proto in &protos {
        println!("cargo:rerun-if-changed={proto}");
    }

    tonic_prost_build::configure()
        // Configure bytes fields to use Bytes type for zero-copy
        // This allows prost to use bytes::Bytes instead of Vec<u8> for bytes fields
        // Note: bytes() accepts a single path, so we call it for each field
        .bytes("worker.ReadBlockChunkProto.data")
        .bytes("worker.WriteBlockRequestProto.data")
        // Keep the hot data variant compact; the command is allocated only once per RPC.
        .boxed(".worker.WriteBlockRequestProto.payload.command")
        .compile_protos(&protos, &["."])?;
    Ok(())
}
