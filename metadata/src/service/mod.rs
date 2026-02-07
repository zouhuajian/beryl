// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata service implementation.
//!
//! Implements MetadataClientService RPC handlers with proper context propagation
//! and error handling.

mod core_util;
pub mod domain;
mod fs_core;
mod fs_service;
pub mod guard;
mod path_service;

// pub use client_service::MetadataClientServiceImpl;
pub use core_util::{
    extent_from_proto, extent_to_proto, extract_and_inject_context, fatal_fs_header, fencing_to_proto,
    header_from_canonical_error, header_from_core_failure, lease_id_from_proto, lease_id_to_proto, location_to_proto,
    need_refresh_header, ok_header_from_core_success, ok_header_from_request, presented_fencing_from_proto,
    request_context_from_proto, retryable_header, worker_hint_to_proto, write_target_to_proto,
};
pub(crate) use fs_core::FsCore;
pub use fs_service::{FsWriteOp, MetadataFsServiceImpl, RoutedFsWriteCtx};
pub use guard::{AuthzContext, AuthzOp, GuardChain, GuardSpec, LeadershipChecker};
pub use path_service::MetadataFileSystemServiceImpl;
