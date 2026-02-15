// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Authorization provider contracts and mode-backed providers.
//!
//! ACL MVP subset contract (enforced fail-closed):
//! - ACL source: `system.posix_acl_access` xattr only (no default-acl evaluation).
//! - ACL encoding: canonical Vecton POSIX ACL binary (`types::acl::{encode,decode}_posix_acl`, version=1).
//! - Supported entries: numeric `User(uid)`, numeric `Group(gid)`, exactly one `Other`, optional single `Mask`.
//! - Unsupported ACLs: malformed encoding, unknown version/tag, duplicate `Other`/`Mask`, duplicate named
//!   user/group entries, or ACLs missing `Other`.
//! - Enforcement: unsupported ACL content must deny fail-closed via `PermissionDenied`
//!   (no mode-bit fallback when ACL xattr exists).
//! - Denial reason identifiers in messages: `UNSUPPORTED_ACL`, `ACL_MALFORMED`, `ACL_DENIED`,
//!   `GROUP_RESOLVE_FAILED`, `MISSING_PRINCIPAL`.
//! - Non-goals in this phase: no default ACL inheritance and no name-based uid/gid expansion.

use super::core_util::permission_denied_canonical_error;
use super::domain::RequestContext;
use crate::config::{FileSystemAuthzMode, InodeAuthzMode};
use crate::metrics::{
    AUTHZ_ALLOW_ACL_INODE_TOTAL, AUTHZ_ALLOW_NONE_TOTAL, AUTHZ_ALLOW_RANGER_PATH_TOTAL, AUTHZ_DENY_ACL_INODE_TOTAL,
    AUTHZ_DENY_NONE_TOTAL, AUTHZ_DENY_RANGER_PATH_TOTAL, AUTHZ_GROUPS_CACHE_EXPIRY_TOTAL, AUTHZ_GROUPS_CACHE_HIT_TOTAL,
    AUTHZ_GROUPS_CACHE_MISS_TOTAL, AUTHZ_GROUPS_RESOLVER_ERROR_TOTAL, AUTHZ_GROUPS_STALE_FALLBACK_USED_TOTAL,
    AUTHZ_PERM_CACHE_HIT_TOTAL, AUTHZ_PERM_CACHE_INVALIDATE_TOTAL, AUTHZ_PERM_CACHE_MISS_TOTAL,
};
use crate::raft::RocksDBStorage;
use async_trait::async_trait;
use common::error::canonical::{CanonicalError, ErrorClass, ErrorCode as CanonicalErrorCode};
use common::header::RpcErrorCode;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::debug;
use types::acl::{decode_posix_acl, AclCodecError, AclPerm, AclSubject, PosixAcl, POSIX_ACL_ACCESS_XATTR};
use types::fs::{FsErrorCode, InodeId};

const GROUP_RESOLVE_FAILED_REASON: &str = "GROUP_RESOLVE_FAILED";
const UNSUPPORTED_ACL_REASON: &str = "UNSUPPORTED_ACL";
const ACL_MALFORMED_REASON: &str = "ACL_MALFORMED";
const ACL_DENIED_REASON: &str = "ACL_DENIED";
const MISSING_PRINCIPAL_REASON: &str = "MISSING_PRINCIPAL";
const STICKY_BIT_DENIED_REASON: &str = "STICKY_BIT_DENIED";
const STICKY_BIT_MASK: u32 = 0o1000;

fn record_authz_allow(scheme: AuthzScheme) {
    match scheme {
        AuthzScheme::RangerPath => {
            AUTHZ_ALLOW_RANGER_PATH_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        AuthzScheme::AclInode => {
            AUTHZ_ALLOW_ACL_INODE_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        AuthzScheme::None => {
            AUTHZ_ALLOW_NONE_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn record_authz_deny(scheme: AuthzScheme) {
    match scheme {
        AuthzScheme::RangerPath => {
            AUTHZ_DENY_RANGER_PATH_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        AuthzScheme::AclInode => {
            AUTHZ_DENY_ACL_INODE_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        AuthzScheme::None => {
            AUTHZ_DENY_NONE_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Authorization operation primitive shared across providers and service surfaces.
///
/// Semantics:
/// - `Read`: client-visible metadata/data read
/// - `Write`: state-mutating write (content and metadata that persists state)
/// - `Execute`: directory traverse/search semantics
/// - `Rename`: structural move/rename (path flows require src + dst-parent checks)
/// - `Delete`: unlink/rmdir removal semantics
/// - `Xattr`: get/set/remove extended attributes
/// - `Sticky`: sticky-bit ownership policy check for delete/rename entry removal
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
    Sticky,
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
            AuthzOp::Sticky => "STICKY",
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

/// Server-side group resolver used by ACL provider.
pub trait GroupResolver: Send + Sync {
    fn groups_for(&self, principal: &str) -> Result<Vec<String>, CanonicalError>;
}

/// ACL authorization input read from inode metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InodePermInputs {
    pub inode_id: InodeId,
    pub uid: u32,
    pub gid: u32,
    pub mode: u32,
    pub access_acl: Option<Vec<u8>>,
}

/// Inode permission accessor for ACL authorization.
pub trait InodePermReader: Send + Sync {
    fn get_perm_inputs(&self, inode_id: InodeId) -> Result<Option<InodePermInputs>, CanonicalError>;

    fn invalidate(&self, inode_id: InodeId);
}

#[derive(Clone, Debug, Default)]
pub struct StaticGroupResolver {
    principal_to_groups: BTreeMap<String, Vec<String>>,
}

impl StaticGroupResolver {
    pub fn new(principal_to_groups: BTreeMap<String, Vec<String>>) -> Self {
        Self { principal_to_groups }
    }
}

impl GroupResolver for StaticGroupResolver {
    fn groups_for(&self, principal: &str) -> Result<Vec<String>, CanonicalError> {
        Ok(self.principal_to_groups.get(principal).cloned().unwrap_or_default())
    }
}

trait TimeSource: Send + Sync {
    fn now(&self) -> Instant;
}

#[derive(Clone, Debug, Default)]
struct SystemTimeSource;

impl TimeSource for SystemTimeSource {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

#[derive(Clone, Debug)]
struct GroupCacheEntry {
    groups: Vec<String>,
    expires_at: Instant,
}

/// Group resolver with TTL cache and stale-while-error behavior.
#[derive(Clone)]
pub struct CachedGroupResolver {
    inner: Arc<dyn GroupResolver>,
    ttl: Duration,
    stale_while_error: bool,
    cache: Arc<Mutex<HashMap<String, GroupCacheEntry>>>,
    clock: Arc<dyn TimeSource>,
    stale_fallback_total: Arc<AtomicU64>,
}

impl CachedGroupResolver {
    pub fn new(inner: Arc<dyn GroupResolver>, ttl_secs: u64, stale_while_error: bool) -> Self {
        Self {
            inner,
            ttl: Duration::from_secs(ttl_secs),
            stale_while_error,
            cache: Arc::new(Mutex::new(HashMap::new())),
            clock: Arc::new(SystemTimeSource),
            stale_fallback_total: Arc::new(AtomicU64::new(0)),
        }
    }

    #[cfg(test)]
    fn with_clock(
        inner: Arc<dyn GroupResolver>,
        ttl: Duration,
        stale_while_error: bool,
        clock: Arc<dyn TimeSource>,
    ) -> Self {
        Self {
            inner,
            ttl,
            stale_while_error,
            cache: Arc::new(Mutex::new(HashMap::new())),
            clock,
            stale_fallback_total: Arc::new(AtomicU64::new(0)),
        }
    }

    #[cfg(test)]
    fn stale_fallback_total(&self) -> u64 {
        self.stale_fallback_total.load(Ordering::Relaxed)
    }
}

impl GroupResolver for CachedGroupResolver {
    fn groups_for(&self, principal: &str) -> Result<Vec<String>, CanonicalError> {
        let now = self.clock.now();
        let cached = {
            self.cache
                .lock()
                .expect("group resolver cache lock poisoned")
                .get(principal)
                .cloned()
        };

        if let Some(entry) = cached.as_ref() {
            if now <= entry.expires_at {
                AUTHZ_GROUPS_CACHE_HIT_TOTAL.fetch_add(1, Ordering::Relaxed);
                return Ok(entry.groups.clone());
            }
            AUTHZ_GROUPS_CACHE_EXPIRY_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        AUTHZ_GROUPS_CACHE_MISS_TOTAL.fetch_add(1, Ordering::Relaxed);

        match self.inner.groups_for(principal) {
            Ok(groups) => {
                let expires_at = now + self.ttl;
                self.cache.lock().expect("group resolver cache lock poisoned").insert(
                    principal.to_string(),
                    GroupCacheEntry {
                        groups: groups.clone(),
                        expires_at,
                    },
                );
                Ok(groups)
            }
            Err(err) => {
                AUTHZ_GROUPS_RESOLVER_ERROR_TOTAL.fetch_add(1, Ordering::Relaxed);
                if self.stale_while_error {
                    if let Some(entry) = cached {
                        self.stale_fallback_total.fetch_add(1, Ordering::Relaxed);
                        AUTHZ_GROUPS_STALE_FALLBACK_USED_TOTAL.fetch_add(1, Ordering::Relaxed);
                        return Ok(entry.groups);
                    }
                }
                Err(rpc_permission_denied_with_reason(
                    GROUP_RESOLVE_FAILED_REASON,
                    format!(
                        "principal={} backend_error_class={:?} backend_error={}",
                        principal, err.class, err.message
                    ),
                ))
            }
        }
    }
}

#[derive(Clone, Debug)]
struct InodePermCacheEntry {
    value: Option<InodePermInputs>,
    expires_at: Instant,
}

/// RocksDB-backed inode permission reader with a small in-memory TTL cache.
#[derive(Clone)]
pub struct RocksDbInodePermReader {
    storage: Arc<RocksDBStorage>,
    cache_ttl: Duration,
    cache: Arc<Mutex<HashMap<InodeId, InodePermCacheEntry>>>,
}

impl RocksDbInodePermReader {
    pub fn new(storage: Arc<RocksDBStorage>, cache_ttl_secs: u64) -> Self {
        Self {
            storage,
            cache_ttl: Duration::from_secs(cache_ttl_secs),
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn load_from_storage(&self, inode_id: InodeId) -> Result<Option<InodePermInputs>, CanonicalError> {
        let inode = self.storage.get_inode(inode_id).map_err(|err| {
            rpc_fatal_canonical_error(
                RpcErrorCode::Application,
                format!(
                    "failed to read inode for authz: inode_id={} err={}",
                    inode_id.as_raw(),
                    err
                ),
            )
        })?;
        Ok(inode.map(|inode| InodePermInputs {
            inode_id: inode.inode_id,
            uid: inode.attrs.uid,
            gid: inode.attrs.gid,
            mode: inode.attrs.mode,
            access_acl: inode.xattrs.get(POSIX_ACL_ACCESS_XATTR).cloned(),
        }))
    }
}

impl InodePermReader for RocksDbInodePermReader {
    fn get_perm_inputs(&self, inode_id: InodeId) -> Result<Option<InodePermInputs>, CanonicalError> {
        let now = Instant::now();
        if let Some(entry) = self
            .cache
            .lock()
            .expect("inode perm cache lock poisoned")
            .get(&inode_id)
            .cloned()
        {
            if now <= entry.expires_at {
                AUTHZ_PERM_CACHE_HIT_TOTAL.fetch_add(1, Ordering::Relaxed);
                return Ok(entry.value);
            }
        }

        AUTHZ_PERM_CACHE_MISS_TOTAL.fetch_add(1, Ordering::Relaxed);
        let loaded = self.load_from_storage(inode_id)?;
        self.cache.lock().expect("inode perm cache lock poisoned").insert(
            inode_id,
            InodePermCacheEntry {
                value: loaded.clone(),
                expires_at: now + self.cache_ttl,
            },
        );
        Ok(loaded)
    }

    fn invalidate(&self, inode_id: InodeId) {
        AUTHZ_PERM_CACHE_INVALIDATE_TOTAL.fetch_add(1, Ordering::Relaxed);
        self.cache
            .lock()
            .expect("inode perm cache lock poisoned")
            .remove(&inode_id);
    }
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
        record_authz_allow(AuthzScheme::None);
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
        record_authz_deny(AuthzScheme::None);
        Err(permission_denied_canonical_error(
            Some(op.as_str()),
            target.describe().as_deref(),
        ))
    }
}

#[derive(Clone)]
pub struct AclInodeAuthz {
    group_resolver: Arc<dyn GroupResolver>,
    inode_perm_reader: Arc<dyn InodePermReader>,
}

impl AclInodeAuthz {
    pub fn new(group_resolver: Arc<dyn GroupResolver>, inode_perm_reader: Arc<dyn InodePermReader>) -> Self {
        Self {
            group_resolver,
            inode_perm_reader,
        }
    }

    fn authorize_sticky(
        &self,
        principal: &str,
        principal_uid: Option<u32>,
        target_inode_id: InodeId,
        parent_inode_id: Option<InodeId>,
    ) -> Result<(), CanonicalError> {
        let parent_inode_id = parent_inode_id.ok_or_else(|| {
            permission_denied_canonical_error(
                Some(AuthzOp::Sticky.as_str()),
                Some("sticky checks require parent inode context"),
            )
        })?;

        // Superuser bypass.
        if principal_uid == Some(0) {
            return Ok(());
        }

        let parent_perm = self
            .inode_perm_reader
            .get_perm_inputs(parent_inode_id)?
            .ok_or_else(|| {
                CanonicalError::fatal_fs(
                    FsErrorCode::ENoEnt,
                    format!("parent inode not found for sticky authz: {}", parent_inode_id.as_raw()),
                )
            })?;

        // Sticky bit off => no ownership restriction.
        if parent_perm.mode & STICKY_BIT_MASK == 0 {
            return Ok(());
        }

        let Some(caller_uid) = principal_uid else {
            return Err(sticky_denied_canonical_error(
                principal,
                parent_inode_id,
                target_inode_id,
                parent_perm.uid,
                None,
            ));
        };

        if caller_uid == parent_perm.uid {
            return Ok(());
        }

        // Missing target inode should be handled by core operation (ENOENT).
        let Some(target_perm) = self.inode_perm_reader.get_perm_inputs(target_inode_id)? else {
            return Ok(());
        };

        if caller_uid == target_perm.uid {
            return Ok(());
        }

        Err(sticky_denied_canonical_error(
            principal,
            parent_inode_id,
            target_inode_id,
            parent_perm.uid,
            Some(target_perm.uid),
        ))
    }

    fn authorize_inner(
        &self,
        req_ctx: &RequestContext,
        target: AuthzTarget,
        op: AuthzOp,
    ) -> Result<(), CanonicalError> {
        let principal = req_ctx
            .principal
            .as_deref()
            .or(req_ctx.caller.principal.as_deref())
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .ok_or_else(|| {
                rpc_fatal_with_reason(
                    RpcErrorCode::Unauthenticated,
                    MISSING_PRINCIPAL_REASON,
                    "acl authorization requires non-empty principal",
                )
            })?;

        let principal_uid = parse_principal_uid(principal);
        let target_detail = target.describe();
        let (inode_id, parent_inode_id) = match target {
            AuthzTarget::Inode {
                inode_id,
                parent_inode_id,
            } => (inode_id, parent_inode_id),
            AuthzTarget::Session {
                inode_id: Some(inode_id),
                ..
            } => (inode_id, None),
            other => {
                return Err(permission_denied_canonical_error(
                    Some(op.as_str()),
                    other.describe().as_deref(),
                ));
            }
        };

        if matches!(op, AuthzOp::Sticky) {
            return self.authorize_sticky(principal, principal_uid, inode_id, parent_inode_id);
        }

        let groups = self.group_resolver.groups_for(principal)?;
        let perm_inputs = self.inode_perm_reader.get_perm_inputs(inode_id)?.ok_or_else(|| {
            CanonicalError::fatal_fs(
                FsErrorCode::ENoEnt,
                format!("inode not found for authz: {}", inode_id.as_raw()),
            )
        })?;

        let group_ids = parse_group_ids(&groups);
        let required = required_perm_for_op(op);
        let (allowed, source) = evaluate_acl_mvp(principal_uid, &group_ids, &perm_inputs, required)?;
        if allowed {
            debug!(
                provider = "acl",
                op = op.as_str(),
                inode_id = inode_id.as_raw(),
                principal = principal,
                groups = ?groups,
                "acl allow"
            );
            Ok(())
        } else {
            Err(acl_denied_canonical_error(
                op,
                principal,
                inode_id,
                target_detail.as_deref(),
                source,
            ))
        }
    }
}

#[async_trait]
impl AuthzProvider for AclInodeAuthz {
    fn scheme(&self) -> AuthzScheme {
        AuthzScheme::AclInode
    }

    async fn authorize(
        &self,
        req_ctx: &RequestContext,
        target: AuthzTarget,
        op: AuthzOp,
    ) -> Result<(), CanonicalError> {
        let result = self.authorize_inner(req_ctx, target, op);
        if result.is_ok() {
            record_authz_allow(AuthzScheme::AclInode);
        } else {
            record_authz_deny(AuthzScheme::AclInode);
        }
        result
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
        debug!(
            authz_stub = true,
            provider = "ranger",
            op = op.as_str(),
            target = %stub_target_for_log(&target),
            client_id = req_ctx.caller.client.client_id.as_raw(),
            group_id = req_ctx.caller.group_id,
            "authz stub allow-all (RANGER): policy evaluation not implemented yet"
        );
        record_authz_allow(AuthzScheme::RangerPath);
        Ok(())
    }
}

#[derive(Clone)]
pub struct AuthzProviderDeps {
    pub group_resolver: Arc<dyn GroupResolver>,
    pub inode_perm_reader: Arc<dyn InodePermReader>,
}

impl AuthzProviderDeps {
    pub fn new(group_resolver: Arc<dyn GroupResolver>, inode_perm_reader: Arc<dyn InodePermReader>) -> Self {
        Self {
            group_resolver,
            inode_perm_reader,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AuthzProviderKind {
    AllowAll,
    AclInode,
    StubRanger,
}

fn filesystem_authz_provider_kind(mode: FileSystemAuthzMode) -> AuthzProviderKind {
    match mode {
        FileSystemAuthzMode::None => AuthzProviderKind::AllowAll,
        FileSystemAuthzMode::Acl => AuthzProviderKind::AclInode,
        FileSystemAuthzMode::Ranger => AuthzProviderKind::StubRanger,
    }
}

fn inode_authz_provider_kind(mode: InodeAuthzMode) -> AuthzProviderKind {
    match mode {
        InodeAuthzMode::None => AuthzProviderKind::AllowAll,
        InodeAuthzMode::Acl => AuthzProviderKind::AclInode,
    }
}

fn build_authz_provider(kind: AuthzProviderKind, deps: &AuthzProviderDeps) -> Arc<dyn AuthzProvider> {
    match kind {
        AuthzProviderKind::AllowAll => Arc::new(AllowAllAuthz),
        AuthzProviderKind::AclInode => Arc::new(AclInodeAuthz::new(
            Arc::clone(&deps.group_resolver),
            Arc::clone(&deps.inode_perm_reader),
        )),
        AuthzProviderKind::StubRanger => Arc::new(StubRangerAuthz),
    }
}

pub fn filesystem_authz_provider(mode: FileSystemAuthzMode, deps: &AuthzProviderDeps) -> Arc<dyn AuthzProvider> {
    build_authz_provider(filesystem_authz_provider_kind(mode), deps)
}

pub fn inode_authz_provider(mode: InodeAuthzMode, deps: &AuthzProviderDeps) -> Arc<dyn AuthzProvider> {
    build_authz_provider(inode_authz_provider_kind(mode), deps)
}

pub fn cached_static_group_resolver(
    principal_to_groups: BTreeMap<String, Vec<String>>,
    cache_ttl_secs: u64,
    stale_while_error: bool,
) -> Arc<dyn GroupResolver> {
    let backend: Arc<dyn GroupResolver> = Arc::new(StaticGroupResolver::new(principal_to_groups));
    Arc::new(CachedGroupResolver::new(backend, cache_ttl_secs, stale_while_error))
}

fn parse_principal_uid(principal: &str) -> Option<u32> {
    principal.parse::<u32>().ok().or_else(|| {
        principal
            .rsplit(':')
            .next()
            .and_then(|fragment| fragment.parse::<u32>().ok())
    })
}

fn parse_group_ids(groups: &[String]) -> Vec<u32> {
    groups.iter().filter_map(|group| group.parse::<u32>().ok()).collect()
}

fn required_perm_for_op(op: AuthzOp) -> AclPerm {
    match op {
        AuthzOp::Read => AclPerm::READ,
        AuthzOp::Execute => AclPerm::EXECUTE,
        // TODO(authz-acl): split richer semantics for rename/delete/xattr/traverse.
        AuthzOp::Write | AuthzOp::Rename | AuthzOp::Delete | AuthzOp::Xattr | AuthzOp::Sticky => AclPerm::WRITE,
    }
}

fn mode_class_perm(mode: u32, shift: u8) -> AclPerm {
    let bits = ((mode >> shift) & 0o7) as u8;
    let mut perm = AclPerm::empty();
    // POSIX mode bits use r=4,w=2,x=1; ACL codec bits use r=1,w=2,x=4.
    if bits & 0b100 != 0 {
        perm |= AclPerm::READ;
    }
    if bits & 0b010 != 0 {
        perm |= AclPerm::WRITE;
    }
    if bits & 0b001 != 0 {
        perm |= AclPerm::EXECUTE;
    }
    perm
}

fn mode_allow(principal_uid: Option<u32>, group_ids: &[u32], inode: &InodePermInputs, required: AclPerm) -> bool {
    let perms = if principal_uid == Some(inode.uid) {
        mode_class_perm(inode.mode, 6)
    } else if group_ids.iter().any(|gid| *gid == inode.gid) {
        mode_class_perm(inode.mode, 3)
    } else {
        mode_class_perm(inode.mode, 0)
    };
    perms.contains(required)
}

#[derive(Default)]
struct ValidatedAclSubset {
    user_perms: HashMap<u32, AclPerm>,
    group_perms: HashMap<u32, AclPerm>,
    other_perm: Option<AclPerm>,
    mask_perm: Option<AclPerm>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AclDecisionSource {
    ModeBits,
    AccessAcl,
}

fn rpc_permission_denied_with_reason(reason: &str, detail: impl Into<String>) -> CanonicalError {
    rpc_permission_denied_canonical_error(format!("permission denied: reason={} detail={}", reason, detail.into()))
}

fn rpc_fatal_with_reason(code: RpcErrorCode, reason: &str, detail: impl Into<String>) -> CanonicalError {
    rpc_fatal_canonical_error(code, format!("reason={} detail={}", reason, detail.into()))
}

fn acl_subset_denied_canonical_error(inode_id: InodeId, reason: &str, detail: impl Into<String>) -> CanonicalError {
    rpc_permission_denied_with_reason(reason, format!("inode_id={} {}", inode_id.as_raw(), detail.into()))
}

fn unsupported_acl_canonical_error(inode_id: InodeId, detail: impl Into<String>) -> CanonicalError {
    acl_subset_denied_canonical_error(inode_id, UNSUPPORTED_ACL_REASON, detail)
}

fn malformed_acl_canonical_error(inode_id: InodeId, detail: impl Into<String>) -> CanonicalError {
    acl_subset_denied_canonical_error(inode_id, ACL_MALFORMED_REASON, detail)
}

fn malformed_or_unsupported_acl_canonical_error(inode_id: InodeId, err: AclCodecError) -> CanonicalError {
    match err {
        AclCodecError::UnsupportedVersion { .. } | AclCodecError::InvalidSubjectTag { .. } => {
            unsupported_acl_canonical_error(inode_id, format!("decode failed: {err}"))
        }
        AclCodecError::Truncated { .. } | AclCodecError::InvalidPerms { .. } | AclCodecError::TrailingBytes { .. } => {
            malformed_acl_canonical_error(inode_id, format!("decode failed: {err}"))
        }
    }
}

fn validate_acl_subset(inode_id: InodeId, acl: &PosixAcl) -> Result<ValidatedAclSubset, CanonicalError> {
    let mut validated = ValidatedAclSubset::default();
    for entry in &acl.entries {
        match entry.subject {
            AclSubject::User(uid) => {
                if validated.user_perms.insert(uid, entry.perms).is_some() {
                    return Err(unsupported_acl_canonical_error(
                        inode_id,
                        format!("duplicate user entry for uid={uid}"),
                    ));
                }
            }
            AclSubject::Group(gid) => {
                if validated.group_perms.insert(gid, entry.perms).is_some() {
                    return Err(unsupported_acl_canonical_error(
                        inode_id,
                        format!("duplicate group entry for gid={gid}"),
                    ));
                }
            }
            AclSubject::Other => {
                if validated.other_perm.replace(entry.perms).is_some() {
                    return Err(unsupported_acl_canonical_error(inode_id, "duplicate other entry"));
                }
            }
            AclSubject::Mask => {
                if validated.mask_perm.replace(entry.perms).is_some() {
                    return Err(unsupported_acl_canonical_error(inode_id, "duplicate mask entry"));
                }
            }
        }
    }

    if validated.other_perm.is_none() {
        return Err(unsupported_acl_canonical_error(
            inode_id,
            "missing required other entry",
        ));
    }
    Ok(validated)
}

fn evaluate_acl_mvp(
    principal_uid: Option<u32>,
    group_ids: &[u32],
    inode: &InodePermInputs,
    required: AclPerm,
) -> Result<(bool, AclDecisionSource), CanonicalError> {
    if principal_uid == Some(inode.uid) {
        return Ok((
            mode_class_perm(inode.mode, 6).contains(required),
            AclDecisionSource::ModeBits,
        ));
    }

    let Some(access_acl) = inode.access_acl.as_ref() else {
        return Ok((
            mode_allow(principal_uid, group_ids, inode, required),
            AclDecisionSource::ModeBits,
        ));
    };

    let acl = decode_posix_acl(access_acl)
        .map_err(|err| malformed_or_unsupported_acl_canonical_error(inode.inode_id, err))?;
    let validated = validate_acl_subset(inode.inode_id, &acl)?;

    if let Some(uid) = principal_uid {
        if let Some(user_perm) = validated.user_perms.get(&uid).copied() {
            let effective = validated.mask_perm.map_or(user_perm, |mask_perm| user_perm & mask_perm);
            return Ok((effective.contains(required), AclDecisionSource::AccessAcl));
        }
    }

    let mut group_perm = AclPerm::empty();
    let mut matched_group = false;
    for gid in group_ids {
        if let Some(perms) = validated.group_perms.get(gid).copied() {
            matched_group = true;
            group_perm |= perms;
        }
    }
    if matched_group {
        let effective = validated
            .mask_perm
            .map_or(group_perm, |mask_perm| group_perm & mask_perm);
        return Ok((effective.contains(required), AclDecisionSource::AccessAcl));
    }

    Ok((
        validated
            .other_perm
            .expect("validated ACL subset must contain other entry")
            .contains(required),
        AclDecisionSource::AccessAcl,
    ))
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

fn rpc_permission_denied_canonical_error(message: impl Into<String>) -> CanonicalError {
    rpc_fatal_canonical_error(RpcErrorCode::PermissionDenied, message)
}

fn acl_denied_canonical_error(
    op: AuthzOp,
    principal: &str,
    inode_id: InodeId,
    target: Option<&str>,
    source: AclDecisionSource,
) -> CanonicalError {
    let source_label = match source {
        AclDecisionSource::ModeBits => "MODE_BITS",
        AclDecisionSource::AccessAcl => "ACL",
    };
    CanonicalError::fatal_fs(
        FsErrorCode::EAcces,
        format!(
            "permission denied: reason={} source={} op={} principal={} inode_id={} target={}",
            ACL_DENIED_REASON,
            source_label,
            op.as_str(),
            principal,
            inode_id.as_raw(),
            target.unwrap_or("unknown")
        ),
    )
}

fn sticky_denied_canonical_error(
    principal: &str,
    parent_inode_id: InodeId,
    target_inode_id: InodeId,
    parent_uid: u32,
    target_uid: Option<u32>,
) -> CanonicalError {
    let target_uid = target_uid
        .map(|uid| uid.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    rpc_permission_denied_with_reason(
        STICKY_BIT_DENIED_REASON,
        format!(
            "principal={} parent_inode_id={} target_inode_id={} parent_uid={} target_uid={}",
            principal,
            parent_inode_id.as_raw(),
            target_inode_id.as_raw(),
            parent_uid,
            target_uid
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::header::RequestHeader;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::TempDir;
    use types::acl::{encode_posix_acl, AclEntry, PosixAcl};
    use types::fs::{FileAttrs, Inode};
    use types::ids::{ClientId, DataHandleId, MountId};

    #[derive(Clone)]
    struct TestClock {
        now: Arc<Mutex<Instant>>,
    }

    impl TestClock {
        fn new(start: Instant) -> Self {
            Self {
                now: Arc::new(Mutex::new(start)),
            }
        }

        fn advance(&self, duration: Duration) {
            let mut now = self.now.lock().expect("test clock lock poisoned");
            *now += duration;
        }
    }

    impl TimeSource for TestClock {
        fn now(&self) -> Instant {
            *self.now.lock().expect("test clock lock poisoned")
        }
    }

    #[derive(Clone)]
    struct ScriptedGroupResolver {
        calls: Arc<AtomicUsize>,
        responses: Arc<Mutex<Vec<Result<Vec<String>, CanonicalError>>>>,
    }

    impl ScriptedGroupResolver {
        fn new(responses: Vec<Result<Vec<String>, CanonicalError>>) -> Self {
            Self {
                calls: Arc::new(AtomicUsize::new(0)),
                responses: Arc::new(Mutex::new(responses)),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }
    }

    impl GroupResolver for ScriptedGroupResolver {
        fn groups_for(&self, _principal: &str) -> Result<Vec<String>, CanonicalError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let mut responses = self.responses.lock().expect("scripted resolver lock poisoned");
            responses
                .remove(0)
                .map_err(|err| rpc_fatal_canonical_error(RpcErrorCode::NodeUnavailable, err.message))
        }
    }

    #[derive(Clone)]
    struct CountingFixedGroupResolver {
        calls: Arc<AtomicUsize>,
        groups: Vec<String>,
    }

    impl CountingFixedGroupResolver {
        fn new(groups: Vec<String>) -> Self {
            Self {
                calls: Arc::new(AtomicUsize::new(0)),
                groups,
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }
    }

    impl GroupResolver for CountingFixedGroupResolver {
        fn groups_for(&self, _principal: &str) -> Result<Vec<String>, CanonicalError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(self.groups.clone())
        }
    }

    #[derive(Clone)]
    struct StaticPermReader {
        data: Arc<HashMap<InodeId, InodePermInputs>>,
    }

    impl StaticPermReader {
        fn new(entries: Vec<InodePermInputs>) -> Self {
            let data = entries.into_iter().map(|entry| (entry.inode_id, entry)).collect();
            Self { data: Arc::new(data) }
        }
    }

    impl InodePermReader for StaticPermReader {
        fn get_perm_inputs(&self, inode_id: InodeId) -> Result<Option<InodePermInputs>, CanonicalError> {
            Ok(self.data.get(&inode_id).cloned())
        }

        fn invalidate(&self, _inode_id: InodeId) {}
    }

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

    #[test]
    fn group_resolver_cache_hit_miss_and_expiry() {
        let backend = Arc::new(ScriptedGroupResolver::new(vec![
            Ok(vec!["10".to_string()]),
            Ok(vec!["20".to_string()]),
        ]));
        let clock = Arc::new(TestClock::new(Instant::now()));
        let resolver = CachedGroupResolver::with_clock(backend.clone(), Duration::from_secs(10), false, clock.clone());

        let g1 = resolver.groups_for("alice").expect("first resolve must succeed");
        let g2 = resolver.groups_for("alice").expect("cache hit must succeed");
        assert_eq!(g1, vec!["10".to_string()]);
        assert_eq!(g2, vec!["10".to_string()]);
        assert_eq!(backend.calls(), 1);

        clock.advance(Duration::from_secs(11));
        let g3 = resolver.groups_for("alice").expect("cache refresh must succeed");
        assert_eq!(g3, vec!["20".to_string()]);
        assert_eq!(backend.calls(), 2);
    }

    #[test]
    fn group_resolver_stale_disabled_denies_without_serving_old_cache() {
        let backend = Arc::new(ScriptedGroupResolver::new(vec![
            Ok(vec!["100".to_string()]),
            Err(CanonicalError::retryable(
                RpcErrorCode::NodeUnavailable,
                None,
                "backend unavailable",
            )),
            Ok(vec!["200".to_string()]),
        ]));
        let clock = Arc::new(TestClock::new(Instant::now()));
        let resolver = CachedGroupResolver::with_clock(backend.clone(), Duration::from_secs(5), false, clock.clone());

        let first = resolver.groups_for("alice").expect("initial resolve must succeed");
        assert_eq!(first, vec!["100".to_string()]);
        clock.advance(Duration::from_secs(6));
        let err = resolver
            .groups_for("alice")
            .expect_err("stale disabled must deny on backend error");
        assert_eq!(
            err.code,
            Some(CanonicalErrorCode::RpcCode(RpcErrorCode::PermissionDenied))
        );
        assert!(err.message.contains(GROUP_RESOLVE_FAILED_REASON));
        assert_eq!(resolver.stale_fallback_total(), 0);
        assert_eq!(backend.calls(), 2);

        clock.advance(Duration::from_secs(1));
        let refreshed = resolver
            .groups_for("alice")
            .expect("resolver should refresh with new groups after backend recovery");
        assert_eq!(refreshed, vec!["200".to_string()]);
        assert_eq!(backend.calls(), 3);
    }

    #[test]
    fn group_resolver_stale_enabled_returns_cached_value_on_error() {
        let backend = Arc::new(ScriptedGroupResolver::new(vec![
            Ok(vec!["100".to_string()]),
            Err(CanonicalError::retryable(
                RpcErrorCode::NodeUnavailable,
                None,
                "backend unavailable",
            )),
        ]));
        let clock = Arc::new(TestClock::new(Instant::now()));
        let resolver = CachedGroupResolver::with_clock(backend.clone(), Duration::from_secs(5), true, clock.clone());

        let first = resolver.groups_for("alice").expect("initial resolve must succeed");
        assert_eq!(first, vec!["100".to_string()]);
        clock.advance(Duration::from_secs(6));

        let stale = resolver
            .groups_for("alice")
            .expect("stale cache value must be returned on backend error");
        assert_eq!(stale, vec!["100".to_string()]);
        assert_eq!(resolver.stale_fallback_total(), 1);
        assert_eq!(backend.calls(), 2);

        let no_cache_backend = Arc::new(ScriptedGroupResolver::new(vec![Err(CanonicalError::retryable(
            RpcErrorCode::NodeUnavailable,
            None,
            "backend unavailable",
        ))]));
        let resolver = CachedGroupResolver::with_clock(
            no_cache_backend.clone(),
            Duration::from_secs(5),
            true,
            Arc::new(TestClock::new(Instant::now())),
        );
        let err = resolver.groups_for("bob").expect_err("no cache should deny");
        assert_eq!(
            err.code,
            Some(CanonicalErrorCode::RpcCode(RpcErrorCode::PermissionDenied))
        );
        assert!(err.message.contains(GROUP_RESOLVE_FAILED_REASON));
    }

    #[tokio::test]
    async fn acl_mode_denies_when_principal_missing() {
        let inode_id = InodeId::new(42);
        let provider = AclInodeAuthz::new(
            Arc::new(StaticGroupResolver::default()),
            Arc::new(StaticPermReader::new(vec![InodePermInputs {
                inode_id,
                uid: 1000,
                gid: 2000,
                mode: 0o644,
                access_acl: None,
            }])),
        );

        let err = provider
            .authorize(
                &test_request_context(None),
                AuthzTarget::for_inode(inode_id),
                AuthzOp::Read,
            )
            .await
            .expect_err("missing principal must deny");

        assert_eq!(
            err.code,
            Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Unauthenticated))
        );
        assert!(err.message.contains(MISSING_PRINCIPAL_REASON));
    }

    #[tokio::test]
    async fn acl_mode_bits_allow_and_deny_with_resolved_group_cache() {
        let inode_id = InodeId::new(88);
        let backend = Arc::new(CountingFixedGroupResolver::new(vec!["300".to_string()]));
        let group_resolver: Arc<dyn GroupResolver> = Arc::new(CachedGroupResolver::new(backend.clone(), 300, false));
        let provider = AclInodeAuthz::new(
            group_resolver,
            Arc::new(StaticPermReader::new(vec![InodePermInputs {
                inode_id,
                uid: 1000,
                gid: 300,
                mode: 0o640,
                access_acl: None,
            }])),
        );
        let req_ctx = test_request_context(Some("2000"));

        provider
            .authorize(&req_ctx, AuthzTarget::for_inode(inode_id), AuthzOp::Read)
            .await
            .expect("group read bit should allow");

        let err = provider
            .authorize(&req_ctx, AuthzTarget::for_inode(inode_id), AuthzOp::Write)
            .await
            .expect_err("group write bit is not set");
        assert_eq!(err.code, Some(CanonicalErrorCode::FsErrno(FsErrorCode::EAcces)));
        assert!(err.message.contains(ACL_DENIED_REASON));
        assert!(err.message.contains("source=MODE_BITS"));
        assert_eq!(backend.calls(), 1, "group lookup should be cached");
    }

    #[tokio::test]
    async fn acl_traverse_execute_requires_all_intermediate_directories() {
        let root = InodeId::new(7001);
        let dir_a = InodeId::new(7002);
        let dir_b = InodeId::new(7003);
        let provider = AclInodeAuthz::new(
            Arc::new(StaticGroupResolver::default()),
            Arc::new(StaticPermReader::new(vec![
                InodePermInputs {
                    inode_id: root,
                    uid: 1000,
                    gid: 1000,
                    mode: 0o755,
                    access_acl: None,
                },
                InodePermInputs {
                    inode_id: dir_a,
                    uid: 1000,
                    gid: 1000,
                    mode: 0o755,
                    access_acl: None,
                },
                InodePermInputs {
                    inode_id: dir_b,
                    uid: 1000,
                    gid: 1000,
                    mode: 0o744, // others cannot execute
                    access_acl: None,
                },
            ])),
        );
        let req_ctx = test_request_context(Some("2000"));

        provider
            .authorize(&req_ctx, AuthzTarget::for_inode(root), AuthzOp::Execute)
            .await
            .expect("root traverse should allow");
        provider
            .authorize(&req_ctx, AuthzTarget::for_inode(dir_a), AuthzOp::Execute)
            .await
            .expect("first intermediate traverse should allow");

        let err = provider
            .authorize(&req_ctx, AuthzTarget::for_inode(dir_b), AuthzOp::Execute)
            .await
            .expect_err("missing execute on intermediate directory must deny");
        assert_eq!(err.code, Some(CanonicalErrorCode::FsErrno(FsErrorCode::EAcces)));
        assert!(err.message.contains(ACL_DENIED_REASON));
        assert!(err.message.contains("op=EXECUTE"));
    }

    #[tokio::test]
    async fn acl_sticky_bit_denies_non_owner_and_allows_owner_or_superuser() {
        let parent = InodeId::new(7101);
        let target = InodeId::new(7102);
        let provider = AclInodeAuthz::new(
            Arc::new(StaticGroupResolver::default()),
            Arc::new(StaticPermReader::new(vec![
                InodePermInputs {
                    inode_id: parent,
                    uid: 1001,
                    gid: 1001,
                    mode: 0o1777,
                    access_acl: None,
                },
                InodePermInputs {
                    inode_id: target,
                    uid: 1002,
                    gid: 1002,
                    mode: 0o644,
                    access_acl: None,
                },
            ])),
        );

        let sticky_target = AuthzTarget::for_inode(target).with_parent(parent);

        provider
            .authorize(&test_request_context(Some("0")), sticky_target.clone(), AuthzOp::Sticky)
            .await
            .expect("superuser should bypass sticky checks");
        provider
            .authorize(
                &test_request_context(Some("1001")),
                sticky_target.clone(),
                AuthzOp::Sticky,
            )
            .await
            .expect("sticky directory owner should be allowed");
        provider
            .authorize(
                &test_request_context(Some("1002")),
                sticky_target.clone(),
                AuthzOp::Sticky,
            )
            .await
            .expect("target owner should be allowed");

        let err = provider
            .authorize(&test_request_context(Some("2000")), sticky_target, AuthzOp::Sticky)
            .await
            .expect_err("non-owner should be denied by sticky bit");
        assert_eq!(
            err.code,
            Some(CanonicalErrorCode::RpcCode(RpcErrorCode::PermissionDenied))
        );
        assert!(err.message.contains(STICKY_BIT_DENIED_REASON));
    }

    #[test]
    fn inode_perm_reader_invalidate_forces_refetch() {
        let dir = TempDir::new().expect("create temp dir");
        let storage = Arc::new(RocksDBStorage::open(dir.path()).expect("open rocksdb"));
        let inode_id = InodeId::new(901);
        let mut attrs = FileAttrs::new();
        attrs.uid = 1000;
        attrs.gid = 1000;
        attrs.mode = 0o644;
        let inode = Inode::new_file(inode_id, attrs.clone(), MountId::new(1), DataHandleId::new(1));
        storage.put_inode(&inode).expect("put inode");

        let reader = RocksDbInodePermReader::new(storage.clone(), 300);
        let first = reader
            .get_perm_inputs(inode_id)
            .expect("first read")
            .expect("inode must exist");
        assert_eq!(first.mode, 0o644);

        let mut updated = inode.clone();
        updated.attrs.mode = 0o600;
        storage.put_inode(&updated).expect("put updated inode");

        let cached = reader
            .get_perm_inputs(inode_id)
            .expect("cached read")
            .expect("inode must exist");
        assert_eq!(
            cached.mode, 0o644,
            "cache should retain previous value before invalidation"
        );

        reader.invalidate(inode_id);
        let refreshed = reader
            .get_perm_inputs(inode_id)
            .expect("refreshed read")
            .expect("inode must exist");
        assert_eq!(refreshed.mode, 0o600);
    }

    #[tokio::test]
    async fn acl_mode_denies_malformed_acl_encoding_fail_closed() {
        let inode_id = InodeId::new(3001);
        let provider = AclInodeAuthz::new(
            Arc::new(StaticGroupResolver::default()),
            Arc::new(StaticPermReader::new(vec![InodePermInputs {
                inode_id,
                uid: 1000,
                gid: 2000,
                mode: 0o644,
                access_acl: Some(vec![1, 2, 3]),
            }])),
        );

        let err = provider
            .authorize(
                &test_request_context(Some("2001")),
                AuthzTarget::for_inode(inode_id),
                AuthzOp::Read,
            )
            .await
            .expect_err("malformed acl must fail closed");
        assert_eq!(
            err.code,
            Some(CanonicalErrorCode::RpcCode(RpcErrorCode::PermissionDenied))
        );
        assert!(err.message.contains(ACL_MALFORMED_REASON));
    }

    #[tokio::test]
    async fn acl_mode_denies_unknown_acl_encoding_version_fail_closed() {
        let inode_id = InodeId::new(3004);
        let mut acl_bytes = encode_posix_acl(&PosixAcl::new(vec![AclEntry {
            subject: AclSubject::Other,
            perms: AclPerm::READ,
        }]));
        acl_bytes[..4].copy_from_slice(&2u32.to_le_bytes());
        let provider = AclInodeAuthz::new(
            Arc::new(StaticGroupResolver::default()),
            Arc::new(StaticPermReader::new(vec![InodePermInputs {
                inode_id,
                uid: 1000,
                gid: 2000,
                mode: 0o644,
                access_acl: Some(acl_bytes),
            }])),
        );

        let err = provider
            .authorize(
                &test_request_context(Some("2001")),
                AuthzTarget::for_inode(inode_id),
                AuthzOp::Read,
            )
            .await
            .expect_err("unknown encoding version must fail closed");
        assert_eq!(
            err.code,
            Some(CanonicalErrorCode::RpcCode(RpcErrorCode::PermissionDenied))
        );
        assert!(err.message.contains(UNSUPPORTED_ACL_REASON));
    }

    #[tokio::test]
    async fn acl_mode_denies_unsupported_acl_entries_fail_closed() {
        let inode_id = InodeId::new(3002);
        let acl = PosixAcl::new(vec![
            AclEntry {
                subject: AclSubject::Other,
                perms: AclPerm::READ,
            },
            AclEntry {
                subject: AclSubject::Other,
                perms: AclPerm::WRITE,
            },
        ]);
        let provider = AclInodeAuthz::new(
            Arc::new(StaticGroupResolver::default()),
            Arc::new(StaticPermReader::new(vec![InodePermInputs {
                inode_id,
                uid: 1000,
                gid: 2000,
                mode: 0o644,
                access_acl: Some(encode_posix_acl(&acl)),
            }])),
        );

        let err = provider
            .authorize(
                &test_request_context(Some("2001")),
                AuthzTarget::for_inode(inode_id),
                AuthzOp::Read,
            )
            .await
            .expect_err("unsupported acl entries must fail closed");
        assert_eq!(
            err.code,
            Some(CanonicalErrorCode::RpcCode(RpcErrorCode::PermissionDenied))
        );
        assert!(err.message.contains(UNSUPPORTED_ACL_REASON));
    }

    #[tokio::test]
    async fn acl_mode_supported_subset_allows_and_denies_expected_ops() {
        let inode_id = InodeId::new(3003);
        let acl = PosixAcl::new(vec![
            AclEntry {
                subject: AclSubject::Group(300),
                perms: AclPerm::READ,
            },
            AclEntry {
                subject: AclSubject::Other,
                perms: AclPerm::empty(),
            },
            AclEntry {
                subject: AclSubject::Mask,
                perms: AclPerm::READ,
            },
        ]);

        let mut mappings = BTreeMap::new();
        mappings.insert("2001".to_string(), vec!["300".to_string()]);
        let provider = AclInodeAuthz::new(
            Arc::new(StaticGroupResolver::new(mappings)),
            Arc::new(StaticPermReader::new(vec![InodePermInputs {
                inode_id,
                uid: 1000,
                gid: 9999,
                mode: 0o600,
                access_acl: Some(encode_posix_acl(&acl)),
            }])),
        );

        provider
            .authorize(
                &test_request_context(Some("2001")),
                AuthzTarget::for_inode(inode_id),
                AuthzOp::Read,
            )
            .await
            .expect("supported subset must allow read");

        let err = provider
            .authorize(
                &test_request_context(Some("2001")),
                AuthzTarget::for_inode(inode_id),
                AuthzOp::Write,
            )
            .await
            .expect_err("supported subset must deny write");
        assert_eq!(err.code, Some(CanonicalErrorCode::FsErrno(FsErrorCode::EAcces)));
        assert!(err.message.contains(ACL_DENIED_REASON));
        assert!(err.message.contains("source=ACL"));
    }

    #[tokio::test]
    async fn acl_xattr_mutation_with_invalidation_denies_immediately_without_waiting_ttl() {
        let dir = TempDir::new().expect("create temp dir");
        let storage = Arc::new(RocksDBStorage::open(dir.path()).expect("open rocksdb"));
        let inode_id = InodeId::new(4001);
        let mut attrs = FileAttrs::new();
        attrs.uid = 1000;
        attrs.gid = 1000;
        attrs.mode = 0o004;
        let inode = Inode::new_file(inode_id, attrs, MountId::new(1), DataHandleId::new(1));
        storage.put_inode(&inode).expect("put inode");

        let reader = Arc::new(RocksDbInodePermReader::new(storage.clone(), 600));
        let provider = AclInodeAuthz::new(Arc::new(StaticGroupResolver::default()), reader.clone());
        let req_ctx = test_request_context(Some("2001"));

        provider
            .authorize(&req_ctx, AuthzTarget::for_inode(inode_id), AuthzOp::Read)
            .await
            .expect("no ACL + mode bits should allow read");

        let mut updated = inode.clone();
        let deny_acl = PosixAcl::new(vec![AclEntry {
            subject: AclSubject::Other,
            perms: AclPerm::empty(),
        }]);
        updated
            .xattrs
            .insert(POSIX_ACL_ACCESS_XATTR.to_string(), encode_posix_acl(&deny_acl));
        storage.put_inode(&updated).expect("put updated inode");

        provider
            .authorize(&req_ctx, AuthzTarget::for_inode(inode_id), AuthzOp::Read)
            .await
            .expect("without invalidation cached permission input is still used");

        reader.invalidate(inode_id);
        let err = provider
            .authorize(&req_ctx, AuthzTarget::for_inode(inode_id), AuthzOp::Read)
            .await
            .expect_err("after invalidation ACL deny must apply immediately");
        assert_eq!(err.code, Some(CanonicalErrorCode::FsErrno(FsErrorCode::EAcces)));
        assert!(err.message.contains(ACL_DENIED_REASON));
        assert!(err.message.contains("source=ACL"));
    }

    #[test]
    fn authz_mode_selection_matches_scheme_contract() {
        assert_eq!(
            filesystem_authz_provider_kind(FileSystemAuthzMode::Acl),
            AuthzProviderKind::AclInode
        );
        assert_eq!(
            filesystem_authz_provider_kind(FileSystemAuthzMode::Ranger),
            AuthzProviderKind::StubRanger
        );
        assert_eq!(
            inode_authz_provider_kind(InodeAuthzMode::Acl),
            AuthzProviderKind::AclInode
        );
        assert_eq!(
            inode_authz_provider_kind(InodeAuthzMode::None),
            AuthzProviderKind::AllowAll
        );
    }

    #[tokio::test]
    async fn stub_ranger_authorize_is_allow_all_for_representative_checks() {
        let req_ctx = test_request_context(None);
        let provider = StubRangerAuthz;
        let targets = [
            AuthzTarget::for_path("/mnt/stub-check".to_string()),
            AuthzTarget::for_path_parent("/mnt", "child"),
            AuthzTarget::for_inode(InodeId::new(42)),
            AuthzTarget::for_session(7, Some(InodeId::new(42))),
        ];
        let ops = [
            AuthzOp::Read,
            AuthzOp::Write,
            AuthzOp::Rename,
            AuthzOp::Delete,
            AuthzOp::Xattr,
        ];
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
