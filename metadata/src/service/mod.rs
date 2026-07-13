// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata service implementation.
//!
//! Implements the FileSystemService adapter, guard chain, FsCore domain layer,
//! and msync handler used by the metadata runtime.

mod core_util;
pub mod domain;
mod fs_core;
mod guard;
mod msync;
mod path_service;

pub use core_util::{
    extract_and_inject_context, fatal_fs_header, fencing_to_proto, file_attrs_from_proto, file_attrs_to_proto,
    file_layout_from_proto, header_from_core_failure, header_from_rpc_error, lease_id_from_proto, lease_id_to_proto,
    location_to_proto, ok_header_from_core_success, ok_header_from_request, presented_fencing_from_proto,
    refresh_metadata_header, request_context_from_proto, retryable_header, validate_active_write_layout,
    worker_endpoint_from_parts, write_target_to_proto,
};
pub(crate) use fs_core::FsCore;
pub use fs_core::SharedWorkerCommitHook;
pub use guard::{GuardChain, GuardFailure};
pub(crate) use msync::MsyncHandler;
pub use path_service::MetadataFileSystemServiceImpl;
pub(crate) use path_service::{FileSystemAuthorityDeps, FileSystemRuntimeDeps, MetadataFileSystemServiceDeps};
