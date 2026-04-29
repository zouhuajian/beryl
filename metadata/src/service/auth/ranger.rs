// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Ranger-backed path permission checker.

use super::{PermissionBits, PermissionChecker, SetAttrPerm};
use crate::metrics::AUTHZ_ALLOW_RANGER_PATH_TOTAL;
use crate::path_resolver::{PathResolver, ResolvedPath};
use crate::service::domain::RequestContext;
use async_trait::async_trait;
use common::error::canonical::{CanonicalError, ErrorClass, ErrorCode as CanonicalErrorCode};
use common::header::RpcErrorCode;
use std::sync::atomic::Ordering;
use tracing::debug;

fn record_ranger_allow() {
    AUTHZ_ALLOW_RANGER_PATH_TOTAL.fetch_add(1, Ordering::Relaxed);
}

fn rpc_fatal_canonical_error(code: RpcErrorCode, message: impl Into<String>) -> CanonicalError {
    CanonicalError {
        class: ErrorClass::Fatal,
        code: Some(CanonicalErrorCode::RpcCode(code)),
        reason: None,
        retry_after_ms: None,
        message: message.into(),
        refresh_hint: None,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RangerAction {
    Read,
    Write,
    Execute,
}

impl RangerAction {
    fn as_str(self) -> &'static str {
        match self {
            RangerAction::Read => "READ",
            RangerAction::Write => "WRITE",
            RangerAction::Execute => "EXECUTE",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum RangerTarget {
    Path { path: String },
    PathParent { parent_path: String, name: String },
}

impl RangerTarget {
    fn for_path(path: impl Into<String>) -> Self {
        Self::Path { path: path.into() }
    }

    fn for_path_parent(parent_path: impl Into<String>, name: impl Into<String>) -> Self {
        Self::PathParent {
            parent_path: parent_path.into(),
            name: name.into(),
        }
    }

    fn log_label(&self) -> &'static str {
        match self {
            RangerTarget::Path { .. } => "path",
            RangerTarget::PathParent { .. } => "path_parent",
        }
    }
}

#[derive(Clone, Debug)]
struct StubRangerAuthorizer;

impl StubRangerAuthorizer {
    async fn authorize(
        &self,
        req_ctx: &RequestContext,
        target: RangerTarget,
        op: RangerAction,
    ) -> Result<(), CanonicalError> {
        debug!(
            authz_stub = true,
            provider = "ranger",
            op = op.as_str(),
            target = %target.log_label(),
            client_id = req_ctx.caller.client.client_id.as_raw(),
            group_id = req_ctx.caller.group_id,
            "authz stub allow-all (RANGER): policy evaluation not implemented yet"
        );
        record_ranger_allow();
        Ok(())
    }
}

pub struct RangerPermissionChecker {
    provider: StubRangerAuthorizer,
}

impl RangerPermissionChecker {
    pub fn new() -> Self {
        Self {
            provider: StubRangerAuthorizer,
        }
    }

    fn path_target(path: &str) -> Result<RangerTarget, CanonicalError> {
        let path = PathResolver::normalize(path)
            .map_err(|err| rpc_fatal_canonical_error(RpcErrorCode::Application, err.to_string()))?;
        Ok(RangerTarget::for_path(path))
    }

    fn parent_target(path: &str) -> Result<RangerTarget, CanonicalError> {
        let normalized = PathResolver::normalize(path)
            .map_err(|err| rpc_fatal_canonical_error(RpcErrorCode::Application, err.to_string()))?;
        let (parent_path, name) = normalized.rsplit_once('/').ok_or_else(|| {
            rpc_fatal_canonical_error(RpcErrorCode::Application, format!("invalid path: {normalized}"))
        })?;
        let parent_path = if parent_path.is_empty() { "/" } else { parent_path };
        Ok(RangerTarget::for_path_parent(parent_path, name))
    }

    async fn authorize_bits(
        &self,
        ctx: &RequestContext,
        target: RangerTarget,
        bits: PermissionBits,
    ) -> Result<(), CanonicalError> {
        for (bit, op) in [
            (PermissionBits::READ, RangerAction::Read),
            (PermissionBits::WRITE, RangerAction::Write),
            (PermissionBits::EXECUTE, RangerAction::Execute),
        ] {
            if bits.contains(bit) {
                self.provider.authorize(ctx, target.clone(), op).await?;
            }
        }
        Ok(())
    }
}

impl Default for RangerPermissionChecker {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl PermissionChecker for RangerPermissionChecker {
    async fn check_perm(
        &self,
        ctx: &RequestContext,
        bits: PermissionBits,
        path: &str,
        _resolved: &ResolvedPath,
    ) -> Result<(), CanonicalError> {
        self.authorize_bits(ctx, Self::path_target(path)?, bits).await
    }

    async fn check_parent_perm(
        &self,
        ctx: &RequestContext,
        bits: PermissionBits,
        path: &str,
        _resolved: &ResolvedPath,
    ) -> Result<(), CanonicalError> {
        self.authorize_bits(ctx, Self::parent_target(path)?, bits).await
    }

    async fn check_super(&self, _ctx: &RequestContext) -> Result<(), CanonicalError> {
        Ok(())
    }

    async fn get_perm(
        &self,
        _ctx: &RequestContext,
        _path: &str,
        _resolved: &ResolvedPath,
    ) -> Result<PermissionBits, CanonicalError> {
        Ok(PermissionBits::all())
    }

    async fn check_set_attr_perm(
        &self,
        ctx: &RequestContext,
        path: &str,
        resolved: &ResolvedPath,
        req: SetAttrPerm,
    ) -> Result<(), CanonicalError> {
        if req.super_required {
            self.check_super(ctx).await?;
        }
        if req.write_required {
            self.check_perm(ctx, PermissionBits::WRITE, path, resolved).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::header::RequestHeader;
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

    fn resolved_parent() -> ResolvedPath {
        ResolvedPath {
            mount_ctx: crate::path_resolver::MountContext {
                mount_id: MountId::new(1),
                mount_epoch: 1,
                owner_group_id: ShardGroupId::new(1),
                root_inode_id: types::fs::InodeId::new(1),
            },
            parent_inode_id: Some(types::fs::InodeId::new(12)),
            name: Some("target".to_string()),
            inode_id: None,
            traverse_dir_inode_ids: vec![types::fs::InodeId::new(1), types::fs::InodeId::new(2)],
        }
    }

    #[tokio::test]
    async fn ranger_permission_checker_preserves_stub_allow_all_behavior() {
        let req_ctx = test_request_context(Some("2000"));
        let checker = RangerPermissionChecker::new();
        let resolved = resolved_parent();

        checker
            .check_parent_perm(&req_ctx, PermissionBits::WRITE, "/mnt/dst", &resolved)
            .await
            .expect("RANGER permission mode is currently a path-targeted allow-all stub");
    }

    #[tokio::test]
    async fn ranger_check_perm_maps_bits_to_path_actions_internally() {
        let req_ctx = test_request_context(Some("2000"));
        let checker = RangerPermissionChecker::new();
        let resolved = resolved_parent();

        checker
            .check_perm(
                &req_ctx,
                PermissionBits::READ | PermissionBits::EXECUTE,
                "/mnt/file",
                &resolved,
            )
            .await
            .expect("stub Ranger path action checks should allow");
    }

    #[test]
    fn ranger_parent_target_derives_parent_path_internally() {
        assert_eq!(
            RangerPermissionChecker::parent_target("/mnt/dir/file")
                .expect("target")
                .log_label(),
            "path_parent"
        );
        assert_eq!(
            RangerPermissionChecker::parent_target("/child")
                .expect("root parent")
                .log_label(),
            "path_parent"
        );
    }

    #[tokio::test]
    async fn stub_ranger_authorizer_allows_representative_path_checks() {
        let req_ctx = test_request_context(None);
        let provider = StubRangerAuthorizer;
        let targets = [
            RangerTarget::for_path("/mnt/stub-check".to_string()),
            RangerTarget::for_path_parent("/mnt", "child"),
        ];
        let ops = [RangerAction::Read, RangerAction::Write, RangerAction::Execute];
        for target in targets {
            for op in ops {
                provider
                    .authorize(&req_ctx, target.clone(), op)
                    .await
                    .expect("stub ranger must allow");
            }
        }
    }
}
