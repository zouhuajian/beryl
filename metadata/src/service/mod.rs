// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata service implementation.
//!
//! Implements MetadataClientService RPC handlers with proper context propagation
//! and error handling.

pub mod authz;
mod core_util;
pub mod domain;
mod fs_core;
pub mod guard;
mod inode_service;
mod path_service;

// pub use client_service::MetadataClientServiceImpl;
pub use self::authz::{
    cached_static_group_resolver, filesystem_authz_provider, inode_authz_provider, AclInodeAuthz, AllowAllAuthz,
    AuthzOp, AuthzProvider, AuthzProviderDeps, AuthzScheme, AuthzTarget, CachedGroupResolver, DenyAllAuthz,
    GroupResolver, InodePermInputs, InodePermReader, RocksDbInodePermReader, StaticGroupResolver, StubRangerAuthz,
};
pub use core_util::{
    extent_from_proto, extent_to_proto, extract_and_inject_context, fatal_fs_header, fencing_to_proto,
    header_from_canonical_error, header_from_core_failure, lease_id_from_proto, lease_id_to_proto, location_to_proto,
    need_refresh_header, ok_header_from_core_success, ok_header_from_request, permission_denied_canonical_error,
    presented_fencing_from_proto, request_context_from_proto, retryable_header, worker_hint_to_proto,
    write_target_to_proto,
};
pub(crate) use fs_core::FsCore;
pub use guard::{AuthzContext, GuardChain, GuardSpec, LeadershipChecker};
pub use inode_service::MetadataInodeServiceImpl;
pub use path_service::MetadataFileSystemServiceImpl;
