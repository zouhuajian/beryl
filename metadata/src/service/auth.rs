// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Current filesystem permission policy.
//!
//! Current active behavior is the NONE allow-all mode. Vecton currently
//! supports only the NONE permission mode in this metadata service.
//! ACL and Ranger modes fail fast; they are not active behavior.

use crate::config::FileSystemAuthzMode;
use crate::metrics::AUTHZ_ALLOW_NONE_TOTAL;
use crate::path_resolver::ResolvedPath;
use crate::service::domain::RequestContext;
use bitflags::bitflags;
use common::error::{CommonError, CommonErrorCode};
use std::sync::atomic::Ordering;

bitflags! {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct PermissionBits: u8 {
        const READ = 0b001;
        const WRITE = 0b010;
        const EXECUTE = 0b100;
    }
}

fn record_none_allow() {
    AUTHZ_ALLOW_NONE_TOTAL.fetch_add(1, Ordering::Relaxed);
}

pub fn validate_filesystem_permission_mode(mode: FileSystemAuthzMode) -> Result<(), CommonError> {
    match mode {
        FileSystemAuthzMode::None => Ok(()),
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

pub fn check_perm(_ctx: &RequestContext, _bits: PermissionBits, _path: &str, _resolved: &ResolvedPath) {
    record_none_allow();
}

pub fn check_parent_perm(_ctx: &RequestContext, _bits: PermissionBits, _path: &str, _resolved: &ResolvedPath) {
    record_none_allow();
}

pub fn check_super(_ctx: &RequestContext) {
    record_none_allow();
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::header::RequestHeader;
    use types::fs::InodeId;
    use types::ids::{ClientId, MountId};
    use types::GroupName;

    fn group_name(raw: &str) -> GroupName {
        GroupName::parse(raw).unwrap()
    }

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
                owner_group_name: group_name("root"),
                root_inode_id: traverse.first().copied().unwrap_or(parent_inode_id),
            },
            parent_inode_id: Some(parent_inode_id),
            name: Some("target".to_string()),
            inode_id: None,
            traverse_dir_inode_ids: traverse,
        }
    }

    #[test]
    fn none_permission_policy_allows_current_guard_operations() {
        let req_ctx = test_request_context();
        let resolved = resolved_parent(InodeId::new(3), vec![InodeId::new(1), InodeId::new(2)]);

        check_perm(&req_ctx, PermissionBits::READ, "/mnt/target", &resolved);
        check_parent_perm(&req_ctx, PermissionBits::WRITE, "/mnt/parent/new-file", &resolved);
        check_super(&req_ctx);
    }

    #[test]
    fn permission_mode_validator_accepts_none_and_rejects_future_modes() {
        assert!(validate_filesystem_permission_mode(FileSystemAuthzMode::None).is_ok());

        match validate_filesystem_permission_mode(FileSystemAuthzMode::Acl) {
            Ok(_) => panic!("ACL mode must fail fast instead of mapping to NONE"),
            Err(err) => assert_eq!(err.message, "ACL permission mode is not implemented yet"),
        }

        match validate_filesystem_permission_mode(FileSystemAuthzMode::Ranger) {
            Ok(_) => panic!("Ranger mode must fail fast instead of mapping to NONE"),
            Err(err) => assert_eq!(err.message, "Ranger permission mode is not implemented yet"),
        }
    }
}
