// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Beryl Protocol Buffers definitions and generated code.
//!
//! Active runtime services:
//! - client → metadata (filesystem): FileSystemServiceProto
//! - worker → metadata: MetadataWorkerService
//! - client → worker: WorkerDataService
//!
//! Module organization: Each proto package maps to a Rust module with the same name.
//! All types from a package are included once to avoid duplicate type definitions.
//!
//! ## Import Policy
//!
//! - Use explicit module paths: `beryl_proto::common::RequestHeaderProto`, `beryl_proto::metadata::CreateFileRequest`, etc.
//! - Do NOT use wildcard imports or re-export all types from a module.
//! - The `convert` module provides bidirectional conversions between proto types and domain types.

// Common types (IDs, headers, etc.)
// Package: common
pub mod common {
    tonic::include_proto!("common");
}

// Client → metadata RPC
pub mod metadata {
    tonic::include_proto!("metadata");
}

// Client → worker RPC
// Package: worker (from worker/data.proto)
pub mod worker {
    tonic::include_proto!("worker");
}

// Conversion utilities between proto and types
pub mod convert;
