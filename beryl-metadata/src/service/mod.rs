// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Metadata service implementation.
//!
//! Implements the thin FileSystemService adapter, path-first filesystem domain,
//! including Msync, used by the metadata runtime.

mod filesystem;
mod rpc;
mod wire;

pub(crate) use filesystem::{MetadataFileSystem, MetadataFileSystemDeps};
pub use rpc::MetadataFileSystemServiceImpl;
pub(crate) use wire::{extract_and_inject_context, ok_header_from_request};
