// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata service implementation.
//!
//! Implements the thin FileSystemService adapter, path-first filesystem domain,
//! and msync handler used by the metadata runtime.

mod filesystem;
mod msync;
mod rpc;
mod wire;

pub(crate) use filesystem::{MetadataFileSystem, MetadataFileSystemDeps};
pub(crate) use msync::MsyncHandler;
pub use rpc::MetadataFileSystemServiceImpl;
pub(crate) use wire::extract_and_inject_context;
pub use wire::header_from_rpc_error;
