// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Behavioral regression tests for path service guard/authz/error contracts.

use common::error::canonical::CanonicalError;
use common::header::{RequestHeader, RpcErrorCode};
use metadata::mount::{DataIoPolicy, MountKind, MountTable, ROOT_INODE_ID};
use metadata::raft::RocksDBStorage;
use metadata::readiness::RootReadinessGate;
use metadata::service::{
    AclPermissionChecker, CachedGroupResolver, FileSystemAuthorityDeps, FileSystemPolicyDeps, FileSystemRuntimeDeps,
    GroupResolver, InodePermReader, LeadershipChecker, MetadataFileSystemServiceDeps, MetadataFileSystemServiceImpl,
    NonePermissionChecker, PermissionChecker, RocksDbInodePermReader, SharedWorkerCommitHook, StaticGroupResolver,
};
use metadata::state::MemoryStateStore;
use proto::common::{
    error_detail_proto::Code as ErrorCodeProto, ErrorClassProto, RequestHeaderProto, ResponseHeaderProto,
    RpcErrorCodeProto,
};
use proto::metadata::file_system_service_proto_server::FileSystemServiceProto;
use proto::metadata::{
    FsyncSessionRequestProto, GetFileStatusRequestProto, MkdirPathRequestProto, OpenWriteByPathRequestProto,
    RenamePathRequestProto, RmdirPathRequestProto, UnlinkPathRequestProto, WriteModeProto,
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use tonic::Request;
use types::acl::{encode_posix_acl, AclEntry, AclPerm, AclSubject, PosixAcl, POSIX_ACL_ACCESS_XATTR};
use types::fs::{FileAttrs, Inode, InodeId};
use types::ids::{DataHandleId, ShardGroupId};
use types::ClientId;

struct PathTestEnv {
    _temp_dir: TempDir,
    storage: Arc<RocksDBStorage>,
    service: MetadataFileSystemServiceImpl,
    mount_id: types::ids::MountId,
    root_inode_id: InodeId,
}

#[derive(Clone)]
struct AlwaysLeader;

impl LeadershipChecker for AlwaysLeader {
    fn is_leader(&self) -> bool {
        true
    }
}

#[derive(Clone)]
struct NotLeader;

impl LeadershipChecker for NotLeader {
    fn is_leader(&self) -> bool {
        false
    }

    fn leader_endpoint(&self) -> Option<String> {
        Some("127.0.0.1:17000".to_string())
    }
}

#[derive(Clone, Default)]
struct FlakyGroupsBackend {
    calls: Arc<AtomicUsize>,
}

impl GroupResolver for FlakyGroupsBackend {
    fn groups_for(&self, _principal: &str) -> Result<Vec<String>, CanonicalError> {
        let call = self.calls.fetch_add(1, Ordering::Relaxed);
        if call == 0 {
            Ok(vec!["1000".to_string()])
        } else {
            Err(CanonicalError::retryable(
                RpcErrorCode::NodeUnavailable,
                None,
                "groups backend unavailable",
            ))
        }
    }
}

fn header(client_id: u64) -> Option<RequestHeaderProto> {
    Some((&RequestHeader::new(ClientId::new(client_id))).into())
}

fn header_with_principal(client_id: u64, principal: &str) -> Option<RequestHeaderProto> {
    let mut request_header = RequestHeader::new(ClientId::new(client_id));
    request_header.principal = Some(principal.to_string());
    Some((&request_header).into())
}

fn header_error(response_header: Option<ResponseHeaderProto>) -> proto::common::ErrorDetailProto {
    response_header
        .expect("response header must exist")
        .error
        .expect("header.error must exist")
}

fn assert_not_leader(err: &proto::common::ErrorDetailProto) {
    assert_eq!(err.error_class, ErrorClassProto::ErrorClassNeedRefresh as i32);
    match err.code {
        Some(ErrorCodeProto::RpcCode(code)) if code == RpcErrorCodeProto::RpcErrCodeNotLeader as i32 => {}
        other => panic!("expected NotLeader rpc code, got {:?}", other),
    }
}

fn assert_permission_denied_with_reason(err: &proto::common::ErrorDetailProto, reason_id: &str) {
    assert_eq!(err.error_class, ErrorClassProto::ErrorClassFatal as i32);
    match err.code {
        Some(ErrorCodeProto::RpcCode(code)) if code == RpcErrorCodeProto::RpcErrCodePermissionDenied as i32 => {}
        other => panic!("expected PermissionDenied rpc code, got {:?}", other),
    }
    assert!(
        err.message.contains(reason_id),
        "expected reason_id={} in message: {}",
        reason_id,
        err.message
    );
}

fn assert_session_invalid_fencing_refresh(err: &proto::common::ErrorDetailProto) {
    assert_eq!(err.error_class, ErrorClassProto::ErrorClassNeedRefresh as i32);
    match err.code {
        Some(ErrorCodeProto::RpcCode(code)) if code == RpcErrorCodeProto::RpcErrCodeFencing as i32 => {}
        other => panic!("expected Fencing rpc code, got {:?}", other),
    }
    assert!(
        err.message.contains("write session not found"),
        "expected missing-session detail in message: {}",
        err.message
    );
}

fn build_env(
    mount_prefix: &str,
    data_io_policy: DataIoPolicy,
    readiness_gate: Option<Arc<RootReadinessGate>>,
    leadership_checker: Option<Arc<dyn LeadershipChecker>>,
    permission_builder: impl FnOnce(&Arc<RocksDBStorage>) -> (Arc<dyn PermissionChecker>, Option<Arc<dyn InodePermReader>>),
) -> PathTestEnv {
    let temp_dir = TempDir::new().expect("create temp dir");
    let storage = Arc::new(RocksDBStorage::open(temp_dir.path()).expect("open rocksdb"));
    let mount_table = Arc::new(MountTable::new());

    let (mount_kind, ufs_uri, root_inode_id) = if mount_prefix == "/" {
        (MountKind::Internal, None, ROOT_INODE_ID)
    } else {
        (
            MountKind::External,
            Some(format!("file:///tmp{}", mount_prefix.replace('/', "_"))),
            InodeId::new(1000),
        )
    };
    let mount_entry = mount_table
        .create_mount(
            mount_prefix.to_string(),
            mount_kind,
            ufs_uri,
            data_io_policy,
            ShardGroupId::new(1),
            root_inode_id,
        )
        .expect("create mount");

    let mut root_attrs = FileAttrs::new();
    root_attrs.uid = 1000;
    root_attrs.gid = 1000;
    root_attrs.mode = 0o755;
    storage
        .put_inode(&Inode::new_dir(root_inode_id, root_attrs, mount_entry.mount_id))
        .expect("put root inode");

    let (permission_checker, inode_perm_reader) = permission_builder(&storage);
    let state_store: Arc<dyn metadata::state::StateStore> = Arc::new(MemoryStateStore::new());
    let write_session_manager = Arc::new(metadata::write_session::WriteSessionManager::default());
    let inode_lease_manager = Arc::new(metadata::inode_lease::InodeLeaseManager::default());
    let worker_commit_hook: SharedWorkerCommitHook = Arc::new(Mutex::new(None));

    let service = MetadataFileSystemServiceImpl::new(MetadataFileSystemServiceDeps {
        authority: FileSystemAuthorityDeps {
            state_store,
            mount_table,
            storage: Arc::clone(&storage),
            raft_node: None,
        },
        runtime: FileSystemRuntimeDeps {
            write_session_manager,
            inode_lease_manager,
            worker_commit_hook,
            worker_manager: None,
            metrics: None,
            readiness_gate,
        },
        policy: FileSystemPolicyDeps {
            leadership_checker,
            permission_checker,
            inode_perm_reader,
        },
    });

    PathTestEnv {
        _temp_dir: temp_dir,
        storage,
        service,
        mount_id: mount_entry.mount_id,
        root_inode_id,
    }
}

#[tokio::test]
async fn readiness_precedence_blocks_before_path_resolution() {
    let readiness_gate = Arc::new(RootReadinessGate::new(None));
    let env = build_env("/mnt/test", DataIoPolicy::Allow, Some(readiness_gate), None, |_| {
        (Arc::new(NonePermissionChecker), None)
    });

    let response = FileSystemServiceProto::get_file_status(
        &env.service,
        Request::new(GetFileStatusRequestProto {
            header: header(1),
            path: "".to_string(),
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();

    let err = header_error(response.header);
    assert_eq!(err.error_class, ErrorClassProto::ErrorClassRetryable as i32);
    match err.code {
        Some(ErrorCodeProto::RpcCode(code)) if code == RpcErrorCodeProto::RpcErrCodeNodeUnavailable as i32 => {}
        other => panic!("expected NodeUnavailable rpc code, got {:?}", other),
    }
}

#[tokio::test]
async fn leadership_precedence_write_returns_not_leader_before_not_found() {
    let env = build_env(
        "/mnt/test",
        DataIoPolicy::Allow,
        None,
        Some(Arc::new(NotLeader)),
        |_| (Arc::new(NonePermissionChecker), None),
    );

    let response = FileSystemServiceProto::mkdir(
        &env.service,
        Request::new(MkdirPathRequestProto {
            header: header(2),
            path: "/mnt/test/missing/child".to_string(),
            attrs: None,
            create_parents: false,
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();

    let err = header_error(response.header);
    assert_not_leader(&err);
}

#[tokio::test]
async fn leadership_precedence_data_io_returns_not_leader_before_root_policy_error() {
    let env = build_env("/", DataIoPolicy::Forbid, None, Some(Arc::new(NotLeader)), |_| {
        (Arc::new(NonePermissionChecker), None)
    });
    let file_inode_id = InodeId::new(2001);
    env.storage
        .put_inode(&Inode::new_file(
            file_inode_id,
            FileAttrs::new(),
            env.mount_id,
            DataHandleId::new(2001),
        ))
        .expect("put test file inode");
    env.storage
        .put_dentry(env.root_inode_id, "file", file_inode_id)
        .expect("put test file dentry");

    let response = FileSystemServiceProto::open_write_by_path(
        &env.service,
        Request::new(OpenWriteByPathRequestProto {
            header: header(3),
            path: "/file".to_string(),
            desired_len: Some(0),
            mode: WriteModeProto::WriteModeWrite as i32,
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();

    let err = header_error(response.header);
    assert_not_leader(&err);
}

#[tokio::test]
async fn acl_malformed_xattr_fails_closed_with_permission_denied_header_error() {
    let env = build_env("/mnt/test", DataIoPolicy::Allow, None, None, |storage| {
        let perm_reader: Arc<dyn InodePermReader> = Arc::new(RocksDbInodePermReader::new(Arc::clone(storage), 60));
        let permission_checker: Arc<dyn PermissionChecker> = Arc::new(AclPermissionChecker::new(
            Arc::new(StaticGroupResolver::new(BTreeMap::new())),
            Arc::clone(&perm_reader),
        ));
        (permission_checker, Some(perm_reader))
    });

    let mut root_inode = env
        .storage
        .get_inode(env.root_inode_id)
        .expect("read root inode")
        .expect("root inode exists");
    root_inode
        .xattrs
        .insert(POSIX_ACL_ACCESS_XATTR.to_string(), vec![1, 2, 3]);
    env.storage.put_inode(&root_inode).expect("update root inode");

    let response = FileSystemServiceProto::get_file_status(
        &env.service,
        Request::new(GetFileStatusRequestProto {
            header: header_with_principal(4, "2000"),
            path: "/mnt/test".to_string(),
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();

    let err = header_error(response.header);
    assert_permission_denied_with_reason(&err, "ACL_MALFORMED");
}

#[tokio::test]
async fn acl_unsupported_subset_fails_closed_with_permission_denied_header_error() {
    let env = build_env("/mnt/test", DataIoPolicy::Allow, None, None, |storage| {
        let perm_reader: Arc<dyn InodePermReader> = Arc::new(RocksDbInodePermReader::new(Arc::clone(storage), 60));
        let permission_checker: Arc<dyn PermissionChecker> = Arc::new(AclPermissionChecker::new(
            Arc::new(StaticGroupResolver::new(BTreeMap::new())),
            Arc::clone(&perm_reader),
        ));
        (permission_checker, Some(perm_reader))
    });

    let mut root_inode = env
        .storage
        .get_inode(env.root_inode_id)
        .expect("read root inode")
        .expect("root inode exists");
    let unsupported_acl = PosixAcl::new(vec![
        AclEntry {
            subject: AclSubject::Other,
            perms: AclPerm::READ,
        },
        AclEntry {
            subject: AclSubject::Other,
            perms: AclPerm::READ,
        },
    ]);
    root_inode
        .xattrs
        .insert(POSIX_ACL_ACCESS_XATTR.to_string(), encode_posix_acl(&unsupported_acl));
    env.storage.put_inode(&root_inode).expect("update root inode");

    let response = FileSystemServiceProto::get_file_status(
        &env.service,
        Request::new(GetFileStatusRequestProto {
            header: header_with_principal(5, "2000"),
            path: "/mnt/test".to_string(),
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();

    let err = header_error(response.header);
    assert_permission_denied_with_reason(&err, "UNSUPPORTED_ACL");
}

#[tokio::test]
async fn groups_resolver_stale_disabled_denies_on_backend_failure() {
    let backend = Arc::new(FlakyGroupsBackend::default());
    let env = build_env("/mnt/test", DataIoPolicy::Allow, None, None, |storage| {
        let perm_reader: Arc<dyn InodePermReader> = Arc::new(RocksDbInodePermReader::new(Arc::clone(storage), 60));
        let backend_resolver: Arc<dyn GroupResolver> = backend.clone();
        let group_resolver: Arc<dyn GroupResolver> = Arc::new(CachedGroupResolver::new(backend_resolver, 0, false));
        let permission_checker: Arc<dyn PermissionChecker> =
            Arc::new(AclPermissionChecker::new(group_resolver, Arc::clone(&perm_reader)));
        (permission_checker, Some(perm_reader))
    });

    let mut root_inode = env
        .storage
        .get_inode(env.root_inode_id)
        .expect("read root inode")
        .expect("root inode exists");
    root_inode.attrs.uid = 1000;
    root_inode.attrs.gid = 1000;
    root_inode.attrs.mode = 0o640;
    env.storage.put_inode(&root_inode).expect("update root inode");

    let first = FileSystemServiceProto::get_file_status(
        &env.service,
        Request::new(GetFileStatusRequestProto {
            header: header_with_principal(6, "2000"),
            path: "/mnt/test".to_string(),
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();
    assert!(
        first.header.as_ref().and_then(|h| h.error.as_ref()).is_none(),
        "first call should populate cache and succeed"
    );

    tokio::time::sleep(tokio::time::Duration::from_millis(2)).await;

    let second = FileSystemServiceProto::get_file_status(
        &env.service,
        Request::new(GetFileStatusRequestProto {
            header: header_with_principal(7, "2000"),
            path: "/mnt/test".to_string(),
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();
    let err = header_error(second.header);
    assert_permission_denied_with_reason(&err, "GROUP_RESOLVE_FAILED");
}

#[tokio::test]
async fn root_mount_data_io_gate_is_enforced() {
    let env = build_env("/", DataIoPolicy::Forbid, None, Some(Arc::new(AlwaysLeader)), |_| {
        (Arc::new(NonePermissionChecker), None)
    });
    let file_inode_id = InodeId::new(3001);
    env.storage
        .put_inode(&Inode::new_file(
            file_inode_id,
            FileAttrs::new(),
            env.mount_id,
            DataHandleId::new(3001),
        ))
        .expect("put test file inode");
    env.storage
        .put_dentry(env.root_inode_id, "file", file_inode_id)
        .expect("put test file dentry");

    let response = FileSystemServiceProto::open_write_by_path(
        &env.service,
        Request::new(OpenWriteByPathRequestProto {
            header: header(8),
            path: "/file".to_string(),
            desired_len: Some(0),
            mode: WriteModeProto::WriteModeWrite as i32,
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();
    let err = header_error(response.header);

    assert_eq!(err.error_class, ErrorClassProto::ErrorClassFatal as i32);
    match err.code {
        Some(ErrorCodeProto::FsErrno(errno)) if errno == proto::common::FsErrnoProto::FsErrnoEnotsup as i32 => {}
        other => panic!("expected ENOTSUP fs errno, got {:?}", other),
    }
    assert!(err.message.contains("RootDataIoForbidden"));
}

#[tokio::test]
async fn fsync_session_missing_session_preserves_refresh_header_contract() {
    let env = build_env(
        "/mnt/test",
        DataIoPolicy::Allow,
        None,
        Some(Arc::new(AlwaysLeader)),
        |_| (Arc::new(NonePermissionChecker), None),
    );

    let response = FileSystemServiceProto::fsync_session(
        &env.service,
        Request::new(FsyncSessionRequestProto {
            header: header(12),
            file_handle: 99,
            ..Default::default()
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();
    let err = header_error(response.header);
    assert_session_invalid_fencing_refresh(&err);
}

fn sticky_acl_env() -> PathTestEnv {
    build_env(
        "/mnt/test",
        DataIoPolicy::Allow,
        None,
        Some(Arc::new(AlwaysLeader)),
        |storage| {
            let perm_reader: Arc<dyn InodePermReader> = Arc::new(RocksDbInodePermReader::new(Arc::clone(storage), 60));
            let permission_checker: Arc<dyn PermissionChecker> = Arc::new(AclPermissionChecker::new(
                Arc::new(StaticGroupResolver::new(BTreeMap::new())),
                Arc::clone(&perm_reader),
            ));
            (permission_checker, Some(perm_reader))
        },
    )
}

fn setup_sticky_fixture(
    env: &PathTestEnv,
    target_name: &str,
    target_inode_id: InodeId,
    target_is_dir: bool,
) -> InodeId {
    let sticky_dir_inode_id = InodeId::new(4001);
    let mut sticky_attrs = FileAttrs::new();
    sticky_attrs.uid = 1001;
    sticky_attrs.gid = 1001;
    sticky_attrs.mode = 0o1777;
    env.storage
        .put_inode(&Inode::new_dir(sticky_dir_inode_id, sticky_attrs, env.mount_id))
        .expect("put sticky dir inode");
    env.storage
        .put_dentry(env.root_inode_id, "sticky", sticky_dir_inode_id)
        .expect("put sticky dir dentry");

    let mut target_attrs = FileAttrs::new();
    target_attrs.uid = 1002;
    target_attrs.gid = 1002;
    target_attrs.mode = if target_is_dir { 0o755 } else { 0o644 };
    let target = if target_is_dir {
        Inode::new_dir(target_inode_id, target_attrs, env.mount_id)
    } else {
        Inode::new_file(
            target_inode_id,
            target_attrs,
            env.mount_id,
            DataHandleId::new(target_inode_id.as_raw()),
        )
    };
    env.storage.put_inode(&target).expect("put target inode");
    env.storage
        .put_dentry(sticky_dir_inode_id, target_name, target_inode_id)
        .expect("put target dentry");
    sticky_dir_inode_id
}

#[tokio::test]
async fn sticky_check_denies_unlink_for_non_owner() {
    let env = sticky_acl_env();
    setup_sticky_fixture(&env, "victim", InodeId::new(4002), false);

    let response = FileSystemServiceProto::unlink(
        &env.service,
        Request::new(UnlinkPathRequestProto {
            header: header_with_principal(9, "2000"),
            path: "/mnt/test/sticky/victim".to_string(),
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();
    let err = header_error(response.header);
    assert_permission_denied_with_reason(&err, "STICKY_BIT_DENIED");
}

#[tokio::test]
async fn sticky_check_denies_rename_for_non_owner() {
    let env = sticky_acl_env();
    setup_sticky_fixture(&env, "src", InodeId::new(4102), false);

    let response = FileSystemServiceProto::rename(
        &env.service,
        Request::new(RenamePathRequestProto {
            header: header_with_principal(10, "2000"),
            src_path: "/mnt/test/sticky/src".to_string(),
            dst_path: "/mnt/test/sticky/dst".to_string(),
            flags: 0,
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();
    let err = header_error(response.header);
    assert_permission_denied_with_reason(&err, "STICKY_BIT_DENIED");
}

#[tokio::test]
async fn sticky_check_denies_rmdir_for_non_owner() {
    let env = sticky_acl_env();
    setup_sticky_fixture(&env, "victim_dir", InodeId::new(4202), true);

    let response = FileSystemServiceProto::rmdir(
        &env.service,
        Request::new(RmdirPathRequestProto {
            header: header_with_principal(11, "2000"),
            path: "/mnt/test/sticky/victim_dir".to_string(),
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();
    let err = header_error(response.header);
    assert_permission_denied_with_reason(&err, "STICKY_BIT_DENIED");
}
