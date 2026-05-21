// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Watch all proto files for changes
    println!("cargo:rerun-if-changed=common/common.proto");
    println!("cargo:rerun-if-changed=common/header.proto");
    println!("cargo:rerun-if-changed=fs/types.proto");
    println!("cargo:rerun-if-changed=common/errors.proto");
    println!("cargo:rerun-if-changed=admin/admin.proto");
    println!("cargo:rerun-if-changed=metadata/filesystem.proto");
    println!("cargo:rerun-if-changed=metadata/worker.proto");
    println!("cargo:rerun-if-changed=metadata/peer.proto");
    println!("cargo:rerun-if-changed=worker/data.proto");
    println!("cargo:rerun-if-changed=worker/data_header.proto");
    println!("cargo:rerun-if-changed=worker/block_meta.proto");

    tonic_prost_build::configure()
        // Configure bytes fields to use Bytes type for zero-copy
        // This allows prost to use bytes::Bytes instead of Vec<u8> for bytes fields
        // Note: bytes() accepts a single path, so we call it for each field
        .bytes("worker.ReadStreamResponseProto.data")
        .bytes("worker.WriteStreamRequestProto.data")
        .compile_protos(
            &[
                "common/common.proto",
                "common/header.proto",
                "fs/types.proto",
                "common/errors.proto",
                "admin/admin.proto",
                "metadata/filesystem.proto",
                "metadata/worker.proto",
                "metadata/peer.proto",
                "worker/data.proto",
                "worker/data_header.proto",
                "worker/block_meta.proto",
            ],
            &["."], // Include root is now "." (proto crate root)
        )?;
    Ok(())
}
