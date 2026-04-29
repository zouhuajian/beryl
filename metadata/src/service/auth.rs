// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Permission checker contract and current allow-all implementation.
//!
//! This module currently provides the permission-checking extension point and
//! the default allow-all implementation. Current active behavior is the NONE
//! allow-all mode. Vecton currently supports only the NONE permission mode in
//! this metadata service. ACL and Ranger providers are
//! expected future implementations and must be added as explicit
//! `PermissionChecker` implementations with tests before they can be enabled.

use crate::config::FileSystemAuthzMode;
use crate::metrics::AUTHZ_ALLOW_NONE_TOTAL;
use crate::path_resolver::ResolvedPath;
use crate::service::domain::RequestContext;
use async_trait::async_trait;
use bitflags::bitflags;
use common::error::canonical::CanonicalError;
use common::error::{CommonError, CommonErrorCode};
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

pub fn filesystem_permission_checker(mode: FileSystemAuthzMode) -> Result<Arc<dyn PermissionChecker>, CommonError> {
    match mode {
        FileSystemAuthzMode::None => Ok(Arc::new(NonePermissionChecker)),
        FileSystemAuthzMode::Acl => Err(CommonError::new(
            CommonErrorCode::InvalidArgument,
            "ACL permission mode is not implemented yet",
        )),
        FileSystemAuthzMode::Ranger => Err(CommonError::new(
            CommonErrorCode::InvalidArgument,
            "Ranger permission mode is not implemented yet",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::header::RequestHeader;
    use types::fs::InodeId;
    use types::ids::{ClientId, MountId, ShardGroupId};

    fn test_request_context() -> RequestContext {
        RequestContext {
            caller: RequestHeader::new(ClientId::new(101)),
            traceparent: None,
            route_epoch: None,
            principal: None,
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
    async fn none_permission_checker_allows_all_operations() {
        let req_ctx = test_request_context();
        let checker = NonePermissionChecker;
        let resolved = resolved_parent(InodeId::new(3), vec![InodeId::new(1), InodeId::new(2)]);

        checker
            .check_perm(&req_ctx, PermissionBits::READ, "/mnt/target", &resolved)
            .await
            .expect("NONE permission mode allows direct permission checks");
        checker
            .check_parent_perm(&req_ctx, PermissionBits::WRITE, "/mnt/parent/new-file", &resolved)
            .await
            .expect("NONE permission mode allows parent permission checks");
        checker
            .check_set_attr_perm(&req_ctx, "/mnt/target", &resolved, SetAttrPerm::default())
            .await
            .expect("NONE permission mode allows setattr permission checks");
        checker
            .check_super(&req_ctx)
            .await
            .expect("NONE permission mode allows superuser checks");

        assert_eq!(
            checker
                .get_perm(&req_ctx, "/mnt/target", &resolved)
                .await
                .expect("NONE permission mode reports allow-all bits"),
            PermissionBits::all()
        );
    }

    #[test]
    fn factory_constructs_none_and_rejects_future_modes() {
        assert!(filesystem_permission_checker(FileSystemAuthzMode::None).is_ok());

        match filesystem_permission_checker(FileSystemAuthzMode::Acl) {
            Ok(_) => panic!("ACL mode must fail fast instead of mapping to NONE"),
            Err(err) => assert_eq!(err.message, "ACL permission mode is not implemented yet"),
        }

        match filesystem_permission_checker(FileSystemAuthzMode::Ranger) {
            Ok(_) => panic!("Ranger mode must fail fast instead of mapping to NONE"),
            Err(err) => assert_eq!(err.message, "Ranger permission mode is not implemented yet"),
        }
    }
}
