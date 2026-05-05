// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Behavioral regression tests for path service guard/authz/error contracts.

use common::header::RequestHeader;
use metadata::config::RaftConfig;
use metadata::mount::{DataIoPolicy, MountKind, MountTable, ROOT_INODE_ID};
use metadata::raft::{AppRaftNode, AppRaftStateMachine, RocksDBStorage};
use metadata::readiness::RootReadinessGate;
use metadata::service::{
    FileSystemAuthorityDeps, FileSystemPolicyDeps, FileSystemRuntimeDeps, LeadershipChecker,
    MetadataFileSystemServiceDeps, MetadataFileSystemServiceImpl, NonePermissionChecker, PermissionChecker,
    SharedWorkerCommitHook,
};
use metadata::state::MemoryStateStore;
use proto::common::{
    error_detail_proto::Code as ErrorCodeProto, ErrorClassProto, FsErrnoProto, RequestHeaderProto, ResponseHeaderProto,
    RpcErrorCodeProto,
};
use proto::metadata::file_system_service_proto_server::FileSystemServiceProto;
use proto::metadata::{
    AppendFileRequestProto, CreateDirectoryRequestProto, DeleteRequestProto, GetStatusRequestProto, HflushRequestProto,
    WriteHandleProto,
};
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use tonic::Request;
use types::fs::{FileAttrs, Inode, InodeId};
use types::ids::{DataHandleId, ShardGroupId};
use types::layout::FileLayout;
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

fn header(client_id: u64) -> Option<RequestHeaderProto> {
    Some((&RequestHeader::new(ClientId::new(client_id))).into())
}

fn header_error(response_header: Option<ResponseHeaderProto>) -> proto::common::ErrorDetailProto {
    response_header
        .expect("response header must exist")
        .error
        .expect("header.error must exist")
}

fn assert_success_header(response_header: Option<ResponseHeaderProto>) {
    assert!(
        response_header.expect("response header must exist").error.is_none(),
        "response header must not contain a business error"
    );
}

fn assert_fs_errno(err: &proto::common::ErrorDetailProto, expected: FsErrnoProto) {
    assert_eq!(err.error_class, ErrorClassProto::ErrorClassFatal as i32);
    match err.code {
        Some(ErrorCodeProto::FsErrno(errno)) if errno == expected as i32 => {}
        other => panic!("expected {:?} fs errno, got {:?}", expected, other),
    }
}

fn assert_not_leader(err: &proto::common::ErrorDetailProto) {
    assert_eq!(err.error_class, ErrorClassProto::ErrorClassNeedRefresh as i32);
    match err.code {
        Some(ErrorCodeProto::RpcCode(code)) if code == RpcErrorCodeProto::RpcErrCodeNotLeader as i32 => {}
        other => panic!("expected NotLeader rpc code, got {:?}", other),
    }
}

fn assert_session_invalid_fencing_refresh(err: &proto::common::ErrorDetailProto) {
    assert_eq!(err.error_class, ErrorClassProto::ErrorClassNeedRefresh as i32);
    match err.code {
        Some(ErrorCodeProto::RpcCode(code)) if code == RpcErrorCodeProto::RpcErrCodeFencing as i32 => {}
        other => panic!("expected Fencing rpc code, got {:?}", other),
    }
    assert!(
        err.message.contains("write handle not found"),
        "expected missing write-handle detail in message: {}",
        err.message
    );
}

fn build_env(
    mount_prefix: &str,
    data_io_policy: DataIoPolicy,
    readiness_gate: Option<Arc<RootReadinessGate>>,
    leadership_checker: Option<Arc<dyn LeadershipChecker>>,
    permission_builder: impl FnOnce(&Arc<RocksDBStorage>) -> Arc<dyn PermissionChecker>,
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

    let permission_checker = permission_builder(&storage);
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
            shard_group_id: ShardGroupId::new(1),
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

async fn build_env_with_raft(
    mount_prefix: &str,
    data_io_policy: DataIoPolicy,
    permission_builder: impl FnOnce(&Arc<RocksDBStorage>) -> Arc<dyn PermissionChecker>,
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

    let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table)));
    let raft_config = RaftConfig {
        node_id: 1,
        peers: vec!["127.0.0.1:0".to_string()],
    };
    let raft_node = Arc::new(
        AppRaftNode::new(1, Arc::clone(&storage), state_machine, &raft_config)
            .await
            .expect("create raft node"),
    );
    for _ in 0..50 {
        if raft_node.is_leader() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(raft_node.is_leader(), "single-node raft must become leader");

    let permission_checker = permission_builder(&storage);
    let state_store: Arc<dyn metadata::state::StateStore> = Arc::new(MemoryStateStore::new());
    let write_session_manager = Arc::new(metadata::write_session::WriteSessionManager::default());
    let inode_lease_manager = Arc::new(metadata::inode_lease::InodeLeaseManager::default());
    let worker_commit_hook: SharedWorkerCommitHook = Arc::new(Mutex::new(None));

    let service = MetadataFileSystemServiceImpl::new(MetadataFileSystemServiceDeps {
        authority: FileSystemAuthorityDeps {
            state_store,
            mount_table,
            storage: Arc::clone(&storage),
            raft_node: Some(raft_node),
            shard_group_id: ShardGroupId::new(1),
        },
        runtime: FileSystemRuntimeDeps {
            write_session_manager,
            inode_lease_manager,
            worker_commit_hook,
            worker_manager: None,
            metrics: None,
            readiness_gate: None,
        },
        policy: FileSystemPolicyDeps {
            leadership_checker: None,
            permission_checker,
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
        Arc::new(NonePermissionChecker)
    });

    let response = FileSystemServiceProto::get_status(
        &env.service,
        Request::new(GetStatusRequestProto {
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
        |_| Arc::new(NonePermissionChecker),
    );

    let response = FileSystemServiceProto::create_directory(
        &env.service,
        Request::new(CreateDirectoryRequestProto {
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
        Arc::new(NonePermissionChecker)
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

    let response = FileSystemServiceProto::append_file(
        &env.service,
        Request::new(AppendFileRequestProto {
            header: header(3),
            path: "/file".to_string(),
            desired_len: Some(0),
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();

    let err = header_error(response.header);
    assert_not_leader(&err);
}

#[tokio::test]
async fn root_mount_data_io_gate_is_enforced() {
    let env = build_env("/", DataIoPolicy::Forbid, None, Some(Arc::new(AlwaysLeader)), |_| {
        Arc::new(NonePermissionChecker)
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

    let response = FileSystemServiceProto::append_file(
        &env.service,
        Request::new(AppendFileRequestProto {
            header: header(8),
            path: "/file".to_string(),
            desired_len: Some(0),
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
async fn hflush_missing_handle_preserves_refresh_header_contract() {
    let env = build_env(
        "/mnt/test",
        DataIoPolicy::Allow,
        None,
        Some(Arc::new(AlwaysLeader)),
        |_| Arc::new(NonePermissionChecker),
    );

    let response = FileSystemServiceProto::hflush(
        &env.service,
        Request::new(HflushRequestProto {
            header: header(12),
            write_handle: Some(WriteHandleProto {
                handle_id: 99,
                ..Default::default()
            }),
            ..Default::default()
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();
    let err = header_error(response.header);
    assert_session_invalid_fencing_refresh(&err);
}

#[tokio::test]
async fn delete_missing_path_returns_structured_header_error() {
    let env = build_env(
        "/mnt/test",
        DataIoPolicy::Allow,
        None,
        Some(Arc::new(AlwaysLeader)),
        |_| Arc::new(NonePermissionChecker),
    );

    let response = FileSystemServiceProto::delete(
        &env.service,
        Request::new(DeleteRequestProto {
            header: header(13),
            path: "/mnt/test/missing".to_string(),
            recursive: false,
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();

    let err = header_error(response.header);
    assert_eq!(err.error_class, ErrorClassProto::ErrorClassFatal as i32);
    match err.code {
        Some(ErrorCodeProto::FsErrno(errno)) if errno == proto::common::FsErrnoProto::FsErrnoEnoent as i32 => {}
        other => panic!("expected ENOENT fs errno, got {:?}", other),
    }
}

#[tokio::test]
async fn delete_directory_recursive_true_returns_not_supported_header_error() {
    let env = build_env(
        "/mnt/test",
        DataIoPolicy::Allow,
        None,
        Some(Arc::new(AlwaysLeader)),
        |_| Arc::new(NonePermissionChecker),
    );
    let dir_inode_id = InodeId::new(4001);
    env.storage
        .put_inode(&Inode::new_dir(dir_inode_id, FileAttrs::new(), env.mount_id))
        .expect("put test directory inode");
    env.storage
        .put_dentry(env.root_inode_id, "dir", dir_inode_id)
        .expect("put test directory dentry");

    let response = FileSystemServiceProto::delete(
        &env.service,
        Request::new(DeleteRequestProto {
            header: header(14),
            path: "/mnt/test/dir".to_string(),
            recursive: true,
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
    assert!(err.message.contains("recursive delete not yet implemented"));
}

#[tokio::test]
async fn delete_regular_empty_file_success_removes_namespace_layout_and_data_owner() {
    let env = build_env_with_raft("/mnt/test", DataIoPolicy::Allow, |_| Arc::new(NonePermissionChecker)).await;
    let file_inode_id = InodeId::new(5001);
    let data_handle_id = DataHandleId::new(5001);
    let parent = env
        .storage
        .get_inode(env.root_inode_id)
        .expect("load parent inode")
        .expect("parent inode must exist");
    let file_inode = Inode::new_file(file_inode_id, FileAttrs::new(), env.mount_id, data_handle_id);
    let layout = FileLayout::new(4096, 4096, 1);
    env.storage
        .create_file_atomic(env.root_inode_id, "file", &file_inode, &parent, layout)
        .expect("create empty file");

    let response = FileSystemServiceProto::delete(
        &env.service,
        Request::new(DeleteRequestProto {
            header: header(15),
            path: "/mnt/test/file".to_string(),
            recursive: false,
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();

    assert_success_header(response.header);
    assert_eq!(env.storage.get_dentry(env.root_inode_id, "file").unwrap(), None);
    assert!(env.storage.get_inode(file_inode_id).unwrap().is_none());
    assert!(env.storage.get_layout(file_inode_id).is_err());
    assert_eq!(env.storage.get_inode_by_data_handle(data_handle_id).unwrap(), None);
}

#[tokio::test]
async fn delete_empty_dir_success_removes_namespace_and_inode() {
    let env = build_env_with_raft("/mnt/test", DataIoPolicy::Allow, |_| Arc::new(NonePermissionChecker)).await;
    let dir_inode_id = InodeId::new(6001);
    env.storage
        .put_inode(&Inode::new_dir(dir_inode_id, FileAttrs::new(), env.mount_id))
        .expect("put empty directory inode");
    env.storage
        .put_dentry(env.root_inode_id, "dir", dir_inode_id)
        .expect("put empty directory dentry");

    let response = FileSystemServiceProto::delete(
        &env.service,
        Request::new(DeleteRequestProto {
            header: header(16),
            path: "/mnt/test/dir".to_string(),
            recursive: false,
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();

    assert_success_header(response.header);
    assert_eq!(env.storage.get_dentry(env.root_inode_id, "dir").unwrap(), None);
    assert!(env.storage.get_inode(dir_inode_id).unwrap().is_none());
    assert!(env.storage.get_inode(env.root_inode_id).unwrap().is_some());
}

#[tokio::test]
async fn delete_non_empty_dir_recursive_false_returns_structured_error_without_half_delete() {
    let env = build_env_with_raft("/mnt/test", DataIoPolicy::Allow, |_| Arc::new(NonePermissionChecker)).await;
    let dir_inode_id = InodeId::new(7001);
    let child_inode_id = InodeId::new(7002);
    env.storage
        .put_inode(&Inode::new_dir(dir_inode_id, FileAttrs::new(), env.mount_id))
        .expect("put directory inode");
    env.storage
        .put_dentry(env.root_inode_id, "dir", dir_inode_id)
        .expect("put directory dentry");
    env.storage
        .put_inode(&Inode::new_file(
            child_inode_id,
            FileAttrs::new(),
            env.mount_id,
            DataHandleId::new(7002),
        ))
        .expect("put child inode");
    env.storage
        .put_dentry(dir_inode_id, "child", child_inode_id)
        .expect("put child dentry");

    let response = FileSystemServiceProto::delete(
        &env.service,
        Request::new(DeleteRequestProto {
            header: header(17),
            path: "/mnt/test/dir".to_string(),
            recursive: false,
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();

    let err = header_error(response.header);
    assert_fs_errno(&err, FsErrnoProto::FsErrnoEnotempty);
    assert_eq!(
        env.storage.get_dentry(env.root_inode_id, "dir").unwrap(),
        Some(dir_inode_id)
    );
    assert!(env.storage.get_inode(dir_inode_id).unwrap().is_some());
    assert_eq!(
        env.storage.get_dentry(dir_inode_id, "child").unwrap(),
        Some(child_inode_id)
    );
    assert!(env.storage.get_inode(child_inode_id).unwrap().is_some());
}

#[tokio::test]
async fn delete_symlink_success_preserves_data_handle_owner_zero_mapping() {
    let env = build_env_with_raft("/mnt/test", DataIoPolicy::Allow, |_| Arc::new(NonePermissionChecker)).await;
    let symlink_inode_id = InodeId::new(8001);
    let sentinel_owner_inode_id = InodeId::new(8002);
    let symlink_inode = Inode::new_symlink(
        symlink_inode_id,
        FileAttrs::new(),
        "/mnt/test/target".to_string(),
        env.mount_id,
    );
    env.storage.put_inode(&symlink_inode).expect("put symlink inode");
    env.storage
        .put_dentry(env.root_inode_id, "link", symlink_inode_id)
        .expect("put symlink dentry");
    env.storage
        .put_data_handle_owner(DataHandleId::new(0), sentinel_owner_inode_id)
        .expect("put sentinel owner mapping");

    let response = FileSystemServiceProto::delete(
        &env.service,
        Request::new(DeleteRequestProto {
            header: header(18),
            path: "/mnt/test/link".to_string(),
            recursive: false,
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();

    assert_success_header(response.header);
    assert_eq!(env.storage.get_dentry(env.root_inode_id, "link").unwrap(), None);
    assert!(env.storage.get_inode(symlink_inode_id).unwrap().is_none());
    assert_eq!(
        env.storage.get_inode_by_data_handle(DataHandleId::new(0)).unwrap(),
        Some(sentinel_owner_inode_id)
    );
}
