// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Behavioral regression tests for path service guard/authz/error contracts.

use common::header::RequestHeader;
use metadata::mount::{DataIoPolicy, MountKind, MountTable, ROOT_INODE_ID};
use metadata::raft::RocksDBStorage;
use metadata::readiness::RootReadinessGate;
use metadata::service::{
    FileSystemAuthorityDeps, FileSystemPolicyDeps, FileSystemRuntimeDeps, LeadershipChecker,
    MetadataFileSystemServiceDeps, MetadataFileSystemServiceImpl, NonePermissionChecker, PermissionChecker,
    SharedWorkerCommitHook,
};
use metadata::state::MemoryStateStore;
use proto::common::{
    error_detail_proto::Code as ErrorCodeProto, ErrorClassProto, RequestHeaderProto, ResponseHeaderProto,
    RpcErrorCodeProto,
};
use proto::metadata::file_system_service_proto_server::FileSystemServiceProto;
use proto::metadata::{
    FsyncSessionRequestProto, GetFileStatusRequestProto, MkdirPathRequestProto, OpenWriteByPathRequestProto,
    WriteModeProto,
};
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use tonic::Request;
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

fn header(client_id: u64) -> Option<RequestHeaderProto> {
    Some((&RequestHeader::new(ClientId::new(client_id))).into())
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

#[tokio::test]
async fn readiness_precedence_blocks_before_path_resolution() {
    let readiness_gate = Arc::new(RootReadinessGate::new(None));
    let env = build_env("/mnt/test", DataIoPolicy::Allow, Some(readiness_gate), None, |_| {
        Arc::new(NonePermissionChecker)
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
        |_| Arc::new(NonePermissionChecker),
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
        |_| Arc::new(NonePermissionChecker),
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
