// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata service implementation.
//!
//! Implements the FileSystemService adapter, guard chain, FsCore domain layer,
//! msync handler, and permission-checking extension point used by the metadata
//! runtime.

pub mod auth;
mod core_util;
pub mod domain;
mod fs_core;
mod guard;
mod msync;
mod path_service;

pub use self::auth::{
    filesystem_permission_checker, NonePermissionChecker, PermissionBits, PermissionChecker, SetAttrPerm,
};
pub use core_util::{
    extract_and_inject_context, fatal_fs_header, fencing_to_proto, file_attrs_from_proto, file_attrs_to_proto,
    file_layout_from_proto, header_from_canonical_error, header_from_core_failure, lease_id_from_proto,
    lease_id_to_proto, location_to_proto, need_refresh_header, ok_header_from_core_success, ok_header_from_request,
    permission_denied_canonical_error, presented_fencing_from_proto, request_context_from_proto, retryable_header,
    worker_hint_to_proto, write_target_to_proto,
};
pub(crate) use fs_core::FsCore;
pub use fs_core::SharedWorkerCommitHook;
pub use guard::{GuardChain, GuardFailure, LeadershipChecker};
pub use msync::MsyncHandler;
pub use path_service::{
    FileSystemAuthorityDeps, FileSystemPolicyDeps, FileSystemRuntimeDeps, MetadataFileSystemServiceDeps,
    MetadataFileSystemServiceImpl,
};
