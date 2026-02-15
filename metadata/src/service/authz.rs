// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Authorization provider contracts and mode-backed providers.

use super::core_util::permission_denied_canonical_error;
use super::domain::RequestContext;
use crate::config::{FileSystemAuthzMode, InodeAuthzMode};
use async_trait::async_trait;
use common::error::canonical::CanonicalError;
use std::sync::Arc;
use tracing::debug;
use types::fs::InodeId;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Authorization operation primitive shared across providers and service surfaces.
///
/// Semantics:
/// - `Read`: client-visible metadata/data read
/// - `Write`: state-mutating write (content and metadata that persists state)
/// - `Execute`: directory traverse/search semantics (reserved for future traversal checks)
/// - `Rename`: structural move/rename (path flows require src + dst-parent checks)
/// - `Delete`: unlink/rmdir removal semantics
/// - `Xattr`: get/set/remove extended attributes
///
/// This enum is SSOT for both ACL and Ranger providers.
/// Do not introduce parallel provider-specific permission enums.
pub enum AuthzOp {
    Read,
    Write,
    Execute,
    Rename,
    Delete,
    Xattr,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthzScheme {
    RangerPath,
    AclInode,
    None,
}

impl AuthzOp {
    pub fn as_str(self) -> &'static str {
        match self {
            AuthzOp::Read => "READ",
            AuthzOp::Write => "WRITE",
            AuthzOp::Execute => "EXECUTE",
            AuthzOp::Rename => "RENAME",
            AuthzOp::Delete => "DELETE",
            AuthzOp::Xattr => "XATTR",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
/// Authorization target descriptor used by provider checks.
///
/// - `Inode`: inode-centric target for privileged/inode flows (optionally with parent inode context)
/// - `Session`: file-handle/session target for write-session operations
/// - `Path`: full path target for path-adapter checks before resolve/mutation
/// - `PathParent`: parent-path + child-name target for create/mkdir/unlink-style checks
pub enum AuthzTarget {
    Inode {
        inode_id: InodeId,
        parent_inode_id: Option<InodeId>,
    },
    Session {
        file_handle: u64,
        inode_id: Option<InodeId>,
    },
    Path {
        path: String,
    },
    PathParent {
        parent_path: String,
        name: String,
    },
}

impl AuthzTarget {
    pub fn for_inode(inode_id: InodeId) -> Self {
        Self::Inode {
            inode_id,
            parent_inode_id: None,
        }
    }

    pub fn with_parent(mut self, parent_inode_id: InodeId) -> Self {
        if let Self::Inode { parent_inode_id: p, .. } = &mut self {
            *p = Some(parent_inode_id);
        }
        self
    }

    pub fn for_session(file_handle: u64, inode_id: Option<InodeId>) -> Self {
        Self::Session { file_handle, inode_id }
    }

    pub fn for_file_handle(file_handle: u64) -> Self {
        Self::Session {
            file_handle,
            inode_id: None,
        }
    }

    pub fn for_path(path: impl Into<String>) -> Self {
        Self::Path { path: path.into() }
    }

    pub fn for_path_parent(parent_path: impl Into<String>, name: impl Into<String>) -> Self {
        Self::PathParent {
            parent_path: parent_path.into(),
            name: name.into(),
        }
    }

    pub fn describe(&self) -> Option<String> {
        match self {
            AuthzTarget::Inode {
                inode_id,
                parent_inode_id,
            } => Some(match parent_inode_id {
                Some(parent) => format!(
                    "Inode(inode_id={},parent_inode_id={})",
                    inode_id.as_raw(),
                    parent.as_raw()
                ),
                None => format!("Inode(inode_id={})", inode_id.as_raw()),
            }),
            AuthzTarget::Session { file_handle, inode_id } => Some(match inode_id {
                Some(id) => format!("Session(file_handle={},inode_id={})", file_handle, id.as_raw()),
                None => format!("Session(file_handle={})", file_handle),
            }),
            AuthzTarget::Path { path } => Some(format!("Path({path})")),
            AuthzTarget::PathParent { parent_path, name } => {
                Some(format!("PathParent(parent={parent_path},name={name})"))
            }
        }
    }
}

#[async_trait]
pub trait AuthzProvider: Send + Sync {
    fn scheme(&self) -> AuthzScheme;

    async fn authorize(&self, req_ctx: &RequestContext, target: AuthzTarget, op: AuthzOp)
        -> Result<(), CanonicalError>;
}

#[derive(Clone, Debug)]
pub struct AllowAllAuthz;

#[async_trait]
impl AuthzProvider for AllowAllAuthz {
    fn scheme(&self) -> AuthzScheme {
        AuthzScheme::None
    }

    async fn authorize(
        &self,
        _req_ctx: &RequestContext,
        _target: AuthzTarget,
        _op: AuthzOp,
    ) -> Result<(), CanonicalError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct DenyAllAuthz;

#[async_trait]
impl AuthzProvider for DenyAllAuthz {
    fn scheme(&self) -> AuthzScheme {
        AuthzScheme::None
    }

    async fn authorize(
        &self,
        _req_ctx: &RequestContext,
        target: AuthzTarget,
        op: AuthzOp,
    ) -> Result<(), CanonicalError> {
        Err(permission_denied_canonical_error(
            Some(op.as_str()),
            target.describe().as_deref(),
        ))
    }
}

#[derive(Clone, Debug)]
/// STUB: allow-all placeholder; real ACL enforcement is not implemented yet.
pub struct StubAclAuthz;

#[async_trait]
impl AuthzProvider for StubAclAuthz {
    fn scheme(&self) -> AuthzScheme {
        AuthzScheme::AclInode
    }

    async fn authorize(
        &self,
        req_ctx: &RequestContext,
        target: AuthzTarget,
        op: AuthzOp,
    ) -> Result<(), CanonicalError> {
        // AUTHZ_STUB=true: ACL mode currently resolves to allow-all placeholder behavior.
        debug!(
            authz_stub = AUTHZ_STUB,
            provider = "acl",
            op = op.as_str(),
            target = %stub_target_for_log(&target),
            client_id = req_ctx.caller.client.client_id.as_raw(),
            group_id = req_ctx.caller.group_id,
            "authz stub allow-all (ACL): policy evaluation not implemented yet"
        );
        Ok(())
    }
}

#[derive(Clone, Debug)]
/// STUB: allow-all placeholder; real Ranger enforcement is not implemented yet.
pub struct StubRangerAuthz;

#[async_trait]
impl AuthzProvider for StubRangerAuthz {
    fn scheme(&self) -> AuthzScheme {
        AuthzScheme::RangerPath
    }

    async fn authorize(
        &self,
        req_ctx: &RequestContext,
        target: AuthzTarget,
        op: AuthzOp,
    ) -> Result<(), CanonicalError> {
        // AUTHZ_STUB=true: Ranger mode currently resolves to allow-all placeholder behavior.
        debug!(
            authz_stub = AUTHZ_STUB,
            provider = "ranger",
            op = op.as_str(),
            target = %stub_target_for_log(&target),
            client_id = req_ctx.caller.client.client_id.as_raw(),
            group_id = req_ctx.caller.group_id,
            "authz stub allow-all (RANGER): policy evaluation not implemented yet"
        );
        Ok(())
    }
}

/// Marker for closeout state: authz mode-specific providers are stubs today.
const AUTHZ_STUB: bool = true;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AuthzProviderKind {
    AllowAll,
    StubAcl,
    StubRanger,
}

fn filesystem_authz_provider_kind(mode: FileSystemAuthzMode) -> AuthzProviderKind {
    match mode {
        FileSystemAuthzMode::None => AuthzProviderKind::AllowAll,
        FileSystemAuthzMode::Acl => AuthzProviderKind::StubAcl,
        FileSystemAuthzMode::Ranger => AuthzProviderKind::StubRanger,
    }
}

fn inode_authz_provider_kind(mode: InodeAuthzMode) -> AuthzProviderKind {
    match mode {
        InodeAuthzMode::None => AuthzProviderKind::AllowAll,
        InodeAuthzMode::Acl => AuthzProviderKind::StubAcl,
    }
}

fn build_authz_provider(kind: AuthzProviderKind) -> Arc<dyn AuthzProvider> {
    match kind {
        AuthzProviderKind::AllowAll => Arc::new(AllowAllAuthz),
        AuthzProviderKind::StubAcl => Arc::new(StubAclAuthz),
        AuthzProviderKind::StubRanger => Arc::new(StubRangerAuthz),
    }
}

fn stub_target_for_log(target: &AuthzTarget) -> &'static str {
    match target {
        AuthzTarget::Inode { parent_inode_id, .. } => {
            if parent_inode_id.is_some() {
                "inode_with_parent"
            } else {
                "inode"
            }
        }
        AuthzTarget::Session { inode_id, .. } => {
            if inode_id.is_some() {
                "session_with_inode"
            } else {
                "session"
            }
        }
        AuthzTarget::Path { .. } => "path",
        AuthzTarget::PathParent { .. } => "path_parent",
    }
}

/// Future ACL engine entrypoint for inode-service checks.
///
/// TODO(authz-acl): replace allow-all stub with POSIX ACL evaluation against persisted ACL blob.
#[allow(dead_code, unused_variables)]
fn evaluate_posix_acl(
    req_ctx: &RequestContext,
    inode_id: InodeId,
    op: AuthzOp,
    acl_blob: &[u8],
) -> Result<(), CanonicalError> {
    Ok(())
}

/// Future Ranger engine entrypoint for filesystem-service path checks.
///
/// TODO(authz-ranger): replace allow-all stub with Ranger policy lookup/evaluation.
#[allow(dead_code, unused_variables)]
fn evaluate_ranger_policy(req_ctx: &RequestContext, path: &str, op: AuthzOp) -> Result<(), CanonicalError> {
    Ok(())
}

pub fn filesystem_authz_provider(mode: FileSystemAuthzMode) -> Arc<dyn AuthzProvider> {
    build_authz_provider(filesystem_authz_provider_kind(mode))
}

pub fn inode_authz_provider(mode: InodeAuthzMode) -> Arc<dyn AuthzProvider> {
    build_authz_provider(inode_authz_provider_kind(mode))
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::error::canonical::ErrorClass;
    use common::header::RequestHeader;
    use types::ids::ClientId;

    fn test_request_context() -> RequestContext {
        RequestContext {
            caller: RequestHeader::new(ClientId::new(101)),
            traceparent: None,
            route_epoch: None,
        }
    }

    fn representative_targets() -> Vec<AuthzTarget> {
        vec![
            AuthzTarget::for_path("/mnt/stub-check".to_string()),
            AuthzTarget::for_path_parent("/mnt", "child"),
            AuthzTarget::for_inode(InodeId::new(42)),
            AuthzTarget::for_session(7, Some(InodeId::new(42))),
        ]
    }

    fn representative_ops() -> [AuthzOp; 5] {
        [
            AuthzOp::Read,
            AuthzOp::Write,
            AuthzOp::Rename,
            AuthzOp::Delete,
            AuthzOp::Xattr,
        ]
    }

    async fn assert_stub_never_denies(provider: &dyn AuthzProvider, req_ctx: &RequestContext, provider_name: &str) {
        for target in representative_targets() {
            for op in representative_ops() {
                match provider.authorize(req_ctx, target.clone(), op).await {
                    Ok(()) => {}
                    Err(err) => {
                        assert_ne!(
                            err.class,
                            ErrorClass::NeedRefresh,
                            "{provider_name} stub must never emit NEED_REFRESH on deny path"
                        );
                        panic!("{provider_name} stub unexpectedly denied: {:?}", err);
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn stub_acl_authorize_is_allow_all_for_representative_checks() {
        let req_ctx = test_request_context();
        let provider = StubAclAuthz;
        assert_stub_never_denies(&provider, &req_ctx, "acl").await;
    }

    #[tokio::test]
    async fn stub_ranger_authorize_is_allow_all_for_representative_checks() {
        let req_ctx = test_request_context();
        let provider = StubRangerAuthz;
        assert_stub_never_denies(&provider, &req_ctx, "ranger").await;
    }

    #[test]
    fn authz_mode_selection_matches_stub_contract() {
        assert_eq!(
            filesystem_authz_provider_kind(FileSystemAuthzMode::Acl),
            AuthzProviderKind::StubAcl
        );
        assert_eq!(
            filesystem_authz_provider_kind(FileSystemAuthzMode::Ranger),
            AuthzProviderKind::StubRanger
        );
        assert_eq!(
            inode_authz_provider_kind(InodeAuthzMode::Acl),
            AuthzProviderKind::StubAcl
        );
        assert_eq!(
            inode_authz_provider_kind(InodeAuthzMode::None),
            AuthzProviderKind::AllowAll
        );
    }
}
