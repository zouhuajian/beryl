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
        "InodePermReader",
        "PermissionCacheInvalidator",
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
        "async fn get_status",
        "async fn list_status",
        "async fn create_directory",
        "async fn delete",
        "async fn rename",
        "async fn open_file",
        "async fn get_block_locations",
        "async fn create_file",
        "async fn append_file",
        "async fn add_block",
        "async fn commit_file",
        "async fn abort_file_write",
        "async fn renew_lease",
        "async fn hflush",
        "async fn hsync",
        "async fn msync",
    ] {
        assert!(
            source.contains(handler),
            "path_service.rs must contain new external API handler `{handler}`"
        );
    }
}

#[test]
fn status_hides_inode() {
    let proto = include_str!("../../proto/metadata/filesystem.proto");
    let start = proto
        .find("message GetStatusResponseProto")
        .expect("GetStatusResponseProto must exist");
    let tail = &proto[start..];
    let end = tail.find("\n}\n").expect("GetStatusResponseProto must close");
    let body = &tail[..end];

    assert!(
        !body.contains("InodeProto"),
        "GetStatusResponseProto must not expose internal fs.InodeProto"
    );
}

#[test]
fn create_directory_hides_inode() {
    let proto = include_str!("../../proto/metadata/filesystem.proto");
    let start = proto
        .find("message CreateDirectoryResponseProto")
        .expect("CreateDirectoryResponseProto must exist");
    let tail = &proto[start..];
    let end = tail.find("\n}\n").expect("CreateDirectoryResponseProto must close");
    let body = &tail[..end];

    assert!(
        !body.contains("InodeProto"),
        "CreateDirectoryResponseProto must not expose internal fs.InodeProto"
    );
    assert!(
        body.contains("fs.InodeIdProto inode_id = 2"),
        "CreateDirectoryResponseProto must expose only inode_id"
    );
    assert!(
        body.contains("fs.FileAttrsProto attrs = 3"),
        "CreateDirectoryResponseProto must expose attrs"
    );
}

#[test]
fn block_locations_hide_extents() {
    let proto = include_str!("../../proto/metadata/filesystem.proto");
    let start = proto
        .find("message GetBlockLocationsResponseProto")
        .expect("GetBlockLocationsResponseProto must exist");
    let tail = &proto[start..];
    let end = tail.find("\n}\n").expect("GetBlockLocationsResponseProto must close");
    let body = &tail[..end];

    assert!(
        !body.contains("ExtentProto"),
        "GetBlockLocationsResponseProto must use external block locations, not fs.ExtentProto"
    );
}

#[test]
fn session_rpc_handlers_do_not_call_permission_checker() {
    let source = include_str!("../src/service/path_service.rs");
    for handler in [
        "async fn add_block",
        "async fn commit_file",
        "async fn abort_file_write",
        "async fn renew_lease",
        "async fn hflush",
        "async fn hsync",
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
fn auth_permission_contract_lives_in_single_file_without_legacy_public_surface() {
    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let service_dir = manifest_dir.join("src/service");
    let auth_file = service_dir.join("auth.rs");
    let auth_dir = service_dir.join("auth");

    assert!(auth_file.is_file(), "auth.rs must contain the metadata auth contract");
    assert!(
        !auth_dir.join("acl.rs").exists(),
        "auth/acl.rs must not contain an active ACL implementation"
    );
    assert!(
        !auth_dir.join("ranger.rs").exists(),
        "auth/ranger.rs must not contain an active Ranger implementation"
    );
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
        "AclPermissionChecker",
        "RangerPermissionChecker",
        "PermissionCacheInvalidator",
        "InodePermReader",
    ] {
        assert!(
            !service_mod.contains(forbidden),
            "service/mod.rs must not expose legacy authz surface `{forbidden}`"
        );
    }

    let auth_mod = std::fs::read_to_string(auth_file).expect("read auth.rs");
    assert!(auth_mod.contains("pub struct NonePermissionChecker"));
    assert!(auth_mod.contains("pub trait PermissionChecker"));
    assert!(
        auth_mod.contains("Current active behavior is the NONE")
            && auth_mod.contains("Vecton currently supports only the NONE")
            && auth_mod.contains("ACL and Ranger providers")
            && auth_mod.contains("expected future implementations"),
        "auth.rs must state that ACL/Ranger are future implementations, not active behavior"
    );
    for forbidden in [
        "AuthzTarget",
        "AuthzProvider",
        "AuthzOp",
        "AclInodeAuthz",
        "StubRangerAuthz",
        "pub mod acl",
        "pub mod ranger",
        "AclPermissionChecker",
        "RangerPermissionChecker",
        "PermissionCacheInvalidator",
        "GroupResolver",
        "InodePermReader",
        "StaticPermReader",
        "StaticGroupResolver",
    ] {
        assert!(
            !auth_mod.contains(forbidden),
            "auth.rs must not expose ACL/Ranger legacy implementation detail `{forbidden}`"
        );
    }
}
