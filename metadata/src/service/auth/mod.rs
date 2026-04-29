// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Permission checker contract and mode-backed checker construction.

pub mod acl;
pub mod ranger;

pub use acl::{
    cached_static_group_resolver, AclPermissionChecker, CachedGroupResolver, GroupResolver, InodePermInputs,
    InodePermReader, RocksDbInodePermReader, StaticGroupResolver, StaticPermReader,
};
pub use ranger::RangerPermissionChecker;

use crate::config::FileSystemAuthzMode;
use crate::metrics::AUTHZ_ALLOW_NONE_TOTAL;
use crate::path_resolver::ResolvedPath;
use crate::service::domain::RequestContext;
use async_trait::async_trait;
use bitflags::bitflags;
use common::error::canonical::CanonicalError;
use std::sync::atomic::Ordering;
use std::sync::Arc;

bitflags! {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct PermissionBits: u8 {
        const READ = 0b001;
        const WRITE = 0b010;
        const EXECUTE = 0b100;
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SetAttrPerm {
    pub super_required: bool,
    pub owner_required: bool,
    pub write_required: bool,
}

#[async_trait]
pub trait PermissionChecker: Send + Sync {
    async fn check_perm(
        &self,
        ctx: &RequestContext,
        bits: PermissionBits,
        path: &str,
        resolved: &ResolvedPath,
    ) -> Result<(), CanonicalError>;

    async fn check_parent_perm(
        &self,
        ctx: &RequestContext,
        bits: PermissionBits,
        path: &str,
        resolved: &ResolvedPath,
    ) -> Result<(), CanonicalError>;

    async fn check_super(&self, ctx: &RequestContext) -> Result<(), CanonicalError>;

    async fn get_perm(
        &self,
        ctx: &RequestContext,
        path: &str,
        resolved: &ResolvedPath,
    ) -> Result<PermissionBits, CanonicalError>;

    async fn check_set_attr_perm(
        &self,
        ctx: &RequestContext,
        path: &str,
        resolved: &ResolvedPath,
        req: SetAttrPerm,
    ) -> Result<(), CanonicalError>;
}

#[derive(Clone, Debug)]
pub struct NonePermissionChecker;

#[async_trait]
impl PermissionChecker for NonePermissionChecker {
    async fn check_perm(
        &self,
        _ctx: &RequestContext,
        _bits: PermissionBits,
        _path: &str,
        _resolved: &ResolvedPath,
    ) -> Result<(), CanonicalError> {
        AUTHZ_ALLOW_NONE_TOTAL.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn check_parent_perm(
        &self,
        _ctx: &RequestContext,
        _bits: PermissionBits,
        _path: &str,
        _resolved: &ResolvedPath,
    ) -> Result<(), CanonicalError> {
        AUTHZ_ALLOW_NONE_TOTAL.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn check_super(&self, _ctx: &RequestContext) -> Result<(), CanonicalError> {
        AUTHZ_ALLOW_NONE_TOTAL.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn get_perm(
        &self,
        _ctx: &RequestContext,
        _path: &str,
        _resolved: &ResolvedPath,
    ) -> Result<PermissionBits, CanonicalError> {
        AUTHZ_ALLOW_NONE_TOTAL.fetch_add(1, Ordering::Relaxed);
        Ok(PermissionBits::all())
    }

    async fn check_set_attr_perm(
        &self,
        _ctx: &RequestContext,
        _path: &str,
        _resolved: &ResolvedPath,
        _req: SetAttrPerm,
    ) -> Result<(), CanonicalError> {
        AUTHZ_ALLOW_NONE_TOTAL.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[derive(Clone)]
pub struct PermissionCheckerDeps {
    pub group_resolver: Arc<dyn GroupResolver>,
    pub inode_perm_reader: Arc<dyn InodePermReader>,
}

impl PermissionCheckerDeps {
    pub fn new(group_resolver: Arc<dyn GroupResolver>, inode_perm_reader: Arc<dyn InodePermReader>) -> Self {
        Self {
            group_resolver,
            inode_perm_reader,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PermissionCheckerKind {
    None,
    Acl,
    Ranger,
}

fn filesystem_permission_checker_kind(mode: FileSystemAuthzMode) -> PermissionCheckerKind {
    match mode {
        FileSystemAuthzMode::None => PermissionCheckerKind::None,
        FileSystemAuthzMode::Acl => PermissionCheckerKind::Acl,
        FileSystemAuthzMode::Ranger => PermissionCheckerKind::Ranger,
    }
}

pub fn filesystem_permission_checker(
    mode: FileSystemAuthzMode,
    deps: &PermissionCheckerDeps,
) -> Arc<dyn PermissionChecker> {
    match filesystem_permission_checker_kind(mode) {
        PermissionCheckerKind::None => Arc::new(NonePermissionChecker),
        PermissionCheckerKind::Acl => Arc::new(AclPermissionChecker::new(
            Arc::clone(&deps.group_resolver),
            Arc::clone(&deps.inode_perm_reader),
        )),
        PermissionCheckerKind::Ranger => Arc::new(RangerPermissionChecker::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::header::RequestHeader;
    use std::collections::BTreeMap;
    use types::fs::InodeId;
    use types::ids::{ClientId, MountId, ShardGroupId};

    fn test_request_context(principal: Option<&str>) -> RequestContext {
        let mut caller = RequestHeader::new(ClientId::new(101));
        caller.principal = principal.map(ToString::to_string);
        RequestContext {
            caller,
            traceparent: None,
            route_epoch: None,
            principal: principal.map(ToString::to_string),
            real_user: None,
            doas: None,
            authn_type: common::header::AuthnType::Unspecified,
        }
    }

    fn resolved_parent(parent_inode_id: InodeId, traverse: Vec<InodeId>) -> ResolvedPath {
        ResolvedPath {
            mount_ctx: crate::path_resolver::MountContext {
                mount_id: MountId::new(1),
                mount_epoch: 1,
                owner_group_id: ShardGroupId::new(1),
                root_inode_id: traverse.first().copied().unwrap_or(parent_inode_id),
            },
            parent_inode_id: Some(parent_inode_id),
            name: Some("target".to_string()),
            inode_id: None,
            traverse_dir_inode_ids: traverse,
        }
    }

    #[tokio::test]
    async fn none_permission_checker_allows_resolved_operation_facts() {
        let req_ctx = test_request_context(None);
        let checker = NonePermissionChecker;
        let traverse = [InodeId::new(1), InodeId::new(2)];
        let resolved = resolved_parent(InodeId::new(3), traverse.to_vec());

        checker
            .check_parent_perm(&req_ctx, PermissionBits::WRITE, "/mnt/parent/new-file", &resolved)
            .await
            .expect("NONE permission mode must allow without reading facts");
    }

    #[test]
    fn auth_mode_selection_matches_permission_checker_contract() {
        assert_eq!(
            filesystem_permission_checker_kind(FileSystemAuthzMode::None),
            PermissionCheckerKind::None
        );
        assert_eq!(
            filesystem_permission_checker_kind(FileSystemAuthzMode::Acl),
            PermissionCheckerKind::Acl
        );
        assert_eq!(
            filesystem_permission_checker_kind(FileSystemAuthzMode::Ranger),
            PermissionCheckerKind::Ranger
        );
    }

    #[test]
    fn public_reexports_compile() {
        let resolver: Arc<dyn GroupResolver> = Arc::new(StaticGroupResolver::new(BTreeMap::new()));
        let reader: Arc<dyn InodePermReader> = Arc::new(StaticPermReader::new(Vec::new()));
        let _acl = AclPermissionChecker::new(Arc::clone(&resolver), Arc::clone(&reader));
        let _ranger = RangerPermissionChecker::new();
        let _deps = PermissionCheckerDeps::new(resolver, reader);
    }
}
