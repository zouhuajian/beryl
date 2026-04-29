// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

#[test]
fn path_service_does_not_use_guard_policy_or_permission_target_model() {
    let source = include_str!("../src/service/path_service.rs");
    for forbidden in [
        "GuardPolicy",
        "guard_policy",
        "PermissionOp",
        "PermissionCheck<",
        "PermissionCheck {",
        "PermissionCheck,",
        "PermissionTarget",
        "permission_check_for_",
        "build_permission_for_",
        "FileSystemAuthzMode",
        "RangerPermissionChecker",
        "AclPermissionChecker",
        "NonePermissionChecker",
    ] {
        assert!(
            !source.contains(forbidden),
            "path_service.rs must not contain `{forbidden}`"
        );
    }
}

#[test]
fn path_service_keeps_complete_filesystem_service_view() {
    let source = include_str!("../src/service/path_service.rs");
    assert!(source.contains("impl FileSystemServiceProto for MetadataFileSystemServiceImpl"));
    for handler in [
        "async fn get_file_status",
        "async fn mkdir",
        "async fn create",
        "async fn unlink",
        "async fn rmdir",
        "async fn rename",
        "async fn list_status",
        "async fn open(",
        "async fn release",
        "async fn fsync",
        "async fn truncate",
        "async fn set_xattr",
        "async fn get_xattr",
        "async fn list_xattr",
        "async fn remove_xattr",
        "async fn get_file_layout_by_path",
        "async fn open_write_by_path",
        "async fn close_write_session",
        "async fn renew_write_session_lease",
        "async fn fsync_session",
        "async fn release_session",
    ] {
        assert!(
            source.contains(handler),
            "path_service.rs must continue to contain `{handler}`"
        );
    }
}

#[test]
fn session_rpc_handlers_do_not_call_permission_checker() {
    let source = include_str!("../src/service/path_service.rs");
    for handler in [
        "async fn close_write_session",
        "async fn renew_write_session_lease",
        "async fn fsync_session",
        "async fn release_session",
    ] {
        let start = source
            .find(handler)
            .unwrap_or_else(|| panic!("path_service.rs must contain `{handler}`"));
        let tail = &source[start + handler.len()..];
        let next_handler = tail.find("\n    async fn ").unwrap_or(tail.len());
        let body = &tail[..next_handler];
        for forbidden in [".check_perm(", ".check_parent_perm(", ".check_set_attr_perm("] {
            assert!(
                !body.contains(forbidden),
                "`{handler}` must not call permission checker method `{forbidden}`"
            );
        }
    }
}

#[test]
fn auth_permission_contract_lives_in_auth_directory_without_legacy_public_surface() {
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let service_dir = manifest_dir.join("src/service");
    let auth_dir = service_dir.join("auth");

    assert!(auth_dir.join("mod.rs").is_file(), "auth/mod.rs must exist");
    assert!(auth_dir.join("acl.rs").is_file(), "auth/acl.rs must exist");
    assert!(auth_dir.join("ranger.rs").is_file(), "auth/ranger.rs must exist");
    assert!(
        !service_dir.join("authz.rs").exists(),
        "legacy authz.rs must not remain in use"
    );

    let service_mod = std::fs::read_to_string(service_dir.join("mod.rs")).expect("read service/mod.rs");
    assert!(service_mod.contains("pub mod auth;"));
    for forbidden in [
        "pub mod authz",
        "pub use auth as authz",
        "AuthzProvider",
        "AuthzTarget",
        "AuthzOp",
        "AllowAllAuthz",
        "AclInodeAuthz",
        "StubRangerAuthz",
    ] {
        assert!(
            !service_mod.contains(forbidden),
            "service/mod.rs must not expose legacy authz surface `{forbidden}`"
        );
    }

    let auth_mod = std::fs::read_to_string(auth_dir.join("mod.rs")).expect("read auth/mod.rs");
    assert!(auth_mod.contains("pub mod acl;"));
    assert!(auth_mod.contains("pub mod ranger;"));
    assert!(auth_mod.contains("pub struct NonePermissionChecker"));
    assert!(auth_mod.contains("pub trait PermissionChecker"));
    for forbidden in [
        "AuthzTarget",
        "AuthzProvider",
        "AuthzOp",
        "AclInodeAuthz",
        "StubRangerAuthz",
    ] {
        assert!(
            !auth_mod.contains(forbidden),
            "auth/mod.rs must not expose ACL/Ranger legacy implementation detail `{forbidden}`"
        );
    }
}
