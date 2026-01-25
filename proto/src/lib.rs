// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Vecton Protocol Buffers definitions and generated code.
//!
//! This crate provides gRPC service definitions and message types for:
//! - client → metadata: MetadataClientService
//! - client → worker: WorkerDataService
//! - worker → metadata: MetadataWorkerService
//! - metadata ↔ metadata: MetadataPeerService
//! - admin (shard management): ShardAdminService
//!
//! Module organization: Each proto package maps to a Rust module with the same name.
//! All types from a package are included once to avoid duplicate type definitions.
//!
//! ## Import Policy
//!
//! - Use explicit module paths: `proto::common::RequestHeaderProto`, `proto::metadata::CreateFileRequest`, etc.
//! - Do NOT use wildcard imports or re-export all types from a module.
//! - The `convert` module provides bidirectional conversions between proto types and domain types.

// Common types (IDs, headers, etc.)
// Package: common
pub mod common {
    tonic::include_proto!("common");
}

// FS domain shared types (Inode, FileAttrs, DirEntry, etc.)
pub mod fs {
    tonic::include_proto!("fs");
}

// Client → metadata RPC
pub mod metadata {
    tonic::include_proto!("metadata");
}

// Metadata ↔ metadata RPC
// Package: metapeer (from metadata/peer.proto)
pub mod metapeer {
    tonic::include_proto!("metapeer");
}

// Client → worker RPC
// Package: worker (from worker/data.proto)
pub mod worker {
    tonic::include_proto!("worker");
}

// Admin (shard management) RPC
// Package: admin
pub mod admin {
    tonic::include_proto!("admin");
}

// Conversion utilities between proto and types
pub mod convert;
