// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Behavioral regression tests for path service guard/authz/error contracts.

use common::header::RequestHeader;
use metadata::config::RaftConfig;
use metadata::mount::{DataIoPolicy, MountKind, MountTable, ROOT_INODE_ID};
use metadata::raft::{AppMetadataRaftState, AppRaftNode, AppRaftStateMachine, RocksDBStorage};
use metadata::readiness::RootReadinessGate;
use metadata::service::{
    FileSystemAuthorityDeps, FileSystemPolicyDeps, FileSystemRuntimeDeps, LeadershipChecker,
    MetadataFileSystemServiceDeps, MetadataFileSystemServiceImpl, NonePermissionChecker, PermissionChecker,
    SharedWorkerCommitHook,
};
use metadata::state::MemoryStateStore;
use metadata::worker::{HealthStatus, WorkerManager};
use openraft::{LeaderId, LogId};
use proto::common::{
    error_detail_proto::Code as ErrorCodeProto, DataHandleIdProto, ErrorClassProto, FsErrnoProto,
    GroupStateWatermarkProto, RaftLogIdProto, RefreshReasonProto, RequestHeaderProto, ResponseHeaderProto,
    RpcErrorCodeProto, ShardGroupIdProto,
};
use proto::metadata::file_system_service_proto_server::FileSystemServiceProto;
use proto::metadata::{
    get_block_locations_request_proto, AddBlockRequestProto, AppendFileRequestProto, CommitFileRequestProto,
    CommittedBlockProto, CreateDirectoryRequestProto, CreateDispositionProto, CreateFileRequestProto,
    DeleteRequestProto, GetBlockLocationsRequestProto, GetStatusRequestProto, HflushRequestProto, HsyncRequestProto,
    WriteHandleProto,
};
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use tonic::Request;
use types::fs::{Extent, FileAttrs, Inode, InodeId};
use types::ids::{BlockId, BlockIndex, DataHandleId, ShardGroupId, WorkerId};
use types::layout::FileLayout;
use types::{ClientId, RaftLogId};

const TEST_GROUP_ID: u64 = 1;

struct PathTestEnv {
    _temp_dir: TempDir,
    storage: Arc<RocksDBStorage>,
    mount_table: Arc<MountTable>,
    service: MetadataFileSystemServiceImpl,
    write_session_manager: Arc<metadata::write_session::WriteSessionManager>,
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

fn header_with_freshness(
    client_id: u64,
    mount_epoch: Option<u64>,
    route_epoch: Option<u64>,
    state: Vec<GroupStateWatermarkProto>,
) -> Option<RequestHeaderProto> {
    let mut request_header = header(client_id).expect("request header");
    request_header.group_id = TEST_GROUP_ID;
    request_header.mount_epoch = mount_epoch;
    request_header.route_epoch = route_epoch;
    request_header.state = state;
    Some(request_header)
}

fn watermark_proto(group_id: u64, state_id: RaftLogId) -> GroupStateWatermarkProto {
    GroupStateWatermarkProto {
        group_id: Some(ShardGroupIdProto { value: group_id }),
        state_id: Some(RaftLogIdProto {
            term: state_id.term,
            leader_node_id: state_id.leader_node_id,
            index: state_id.index,
        }),
    }
}

fn persist_last_applied(env: &PathTestEnv, state_id: RaftLogId) {
    env.storage
        .persist_raft_state(&AppMetadataRaftState {
            last_applied_log_id: Some(LogId::new(
                LeaderId::new(state_id.term, state_id.leader_node_id),
                state_id.index,
            )),
            ..Default::default()
        })
        .expect("persist last_applied state");
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

fn assert_state_id(actual: &RaftLogIdProto, expected: RaftLogId) {
    assert_eq!(actual.term, expected.term);
    assert_eq!(actual.leader_node_id, expected.leader_node_id);
    assert_eq!(actual.index, expected.index);
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

fn assert_need_refresh_rpc(
    err: &proto::common::ErrorDetailProto,
    expected_code: RpcErrorCodeProto,
    expected_reason: RefreshReasonProto,
) {
    assert_eq!(err.error_class, ErrorClassProto::ErrorClassNeedRefresh as i32);
    match err.code {
        Some(ErrorCodeProto::RpcCode(code)) if code == expected_code as i32 => {}
        other => panic!("expected {:?} rpc code, got {:?}", expected_code, other),
    }
    assert_eq!(err.refresh_reason, expected_reason as i32);
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
            mount_table: Arc::clone(&mount_table),
            storage: Arc::clone(&storage),
            raft_node: None,
            shard_group_id: ShardGroupId::new(1),
        },
        runtime: FileSystemRuntimeDeps {
            write_session_manager: Arc::clone(&write_session_manager),
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
        mount_table,
        service,
        write_session_manager,
        mount_id: mount_entry.mount_id,
        root_inode_id,
    }
}

async fn build_env_with_raft(
    mount_prefix: &str,
    data_io_policy: DataIoPolicy,
    permission_builder: impl FnOnce(&Arc<RocksDBStorage>) -> Arc<dyn PermissionChecker>,
) -> PathTestEnv {
    build_env_with_raft_and_workers(mount_prefix, data_io_policy, None, permission_builder).await
}

async fn build_env_with_raft_and_workers(
    mount_prefix: &str,
    data_io_policy: DataIoPolicy,
    worker_manager: Option<Arc<WorkerManager>>,
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
            mount_table: Arc::clone(&mount_table),
            storage: Arc::clone(&storage),
            raft_node: Some(raft_node),
            shard_group_id: ShardGroupId::new(1),
        },
        runtime: FileSystemRuntimeDeps {
            write_session_manager: Arc::clone(&write_session_manager),
            inode_lease_manager,
            worker_commit_hook,
            worker_manager,
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
        mount_table,
        service,
        write_session_manager,
        mount_id: mount_entry.mount_id,
        root_inode_id,
    }
}

fn worker_manager_for_write_targets() -> Arc<WorkerManager> {
    let manager = Arc::new(WorkerManager::new(60));
    for raw in 1..=3 {
        let worker_id = WorkerId::new(raw);
        manager
            .register_worker(worker_id, format!("127.0.0.1:{}", 9000 + raw), 1, 10 + raw, None)
            .expect("register worker");
        manager
            .update_runtime(
                worker_id,
                1,
                10 + raw,
                1024 * 1024,
                0,
                1024 * 1024,
                0,
                0,
                HealthStatus::Healthy,
            )
            .expect("update worker runtime");
    }
    manager
}

fn put_dir(env: &PathTestEnv, parent_inode_id: InodeId, name: &str, inode_id: InodeId) {
    env.storage
        .put_inode(&Inode::new_dir(inode_id, FileAttrs::new(), env.mount_id))
        .expect("put directory inode");
    env.storage
        .put_dentry(parent_inode_id, name, inode_id)
        .expect("put directory dentry");
}

fn put_empty_file(
    env: &PathTestEnv,
    parent_inode_id: InodeId,
    name: &str,
    inode_id: InodeId,
    data_handle_id: DataHandleId,
) {
    env.storage
        .put_inode(&Inode::new_file(
            inode_id,
            FileAttrs::new(),
            env.mount_id,
            data_handle_id,
        ))
        .expect("put file inode");
    env.storage
        .put_dentry(parent_inode_id, name, inode_id)
        .expect("put file dentry");
    env.storage
        .put_layout(inode_id, FileLayout::new(4096, 4096, 1))
        .expect("put file layout");
    env.storage
        .put_data_handle_owner(data_handle_id, inode_id)
        .expect("put data handle owner");
}

fn put_extent_file(
    env: &PathTestEnv,
    parent_inode_id: InodeId,
    name: &str,
    inode_id: InodeId,
    data_handle_id: DataHandleId,
    block_id: BlockId,
    len: u64,
) {
    let mut inode = Inode::new_file(inode_id, FileAttrs::new(), env.mount_id, data_handle_id);
    inode.attrs.size = len;
    if let types::fs::InodeData::File {
        extents,
        file_version,
        lease_epoch,
    } = &mut inode.data
    {
        *extents = vec![Extent {
            file_offset: 0,
            block_id,
            block_offset: 0,
            len,
            file_version: Some(1),
            block_stamp: Some(1),
        }];
        *file_version = Some(1);
        *lease_epoch = Some(1);
    }
    env.storage.put_inode(&inode).expect("put extent file inode");
    env.storage
        .put_dentry(parent_inode_id, name, inode_id)
        .expect("put extent file dentry");
    env.storage
        .put_layout(inode_id, FileLayout::new(4096, 4096, 1))
        .expect("put extent file layout");
    env.storage
        .put_data_handle_owner(data_handle_id, inode_id)
        .expect("put extent file owner");
}

#[tokio::test]
async fn stale_mount_epoch_returns_need_refresh_header_with_consumable_mount_hint() {
    let env = build_env(
        "/mnt/test",
        DataIoPolicy::Allow,
        None,
        Some(Arc::new(AlwaysLeader)),
        |_| Arc::new(NonePermissionChecker),
    );

    let response = FileSystemServiceProto::get_status(
        &env.service,
        Request::new(GetStatusRequestProto {
            header: header_with_freshness(101, Some(0), None, Vec::new()),
            path: "/mnt/test".to_string(),
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();

    let response_header = response.header.expect("response header must exist");
    let err = response_header.error.expect("header.error must exist");
    assert_need_refresh_rpc(
        &err,
        RpcErrorCodeProto::RpcErrCodeMountEpochMismatch,
        RefreshReasonProto::RefreshReasonMountEpochMismatch,
    );
    assert_eq!(response_header.group_id, TEST_GROUP_ID);
    assert_eq!(response_header.mount_epoch, Some(1));
    let hint = err.refresh_hint.expect("refresh hint");
    assert_eq!(hint.group_id, Some(TEST_GROUP_ID));
    assert_eq!(hint.mount_epoch, Some(1));
    assert_eq!(hint.route_epoch, None);
}

#[tokio::test]
async fn stale_route_epoch_returns_need_refresh_header_with_consumable_route_hint() {
    let env = build_env(
        "/mnt/test",
        DataIoPolicy::Allow,
        None,
        Some(Arc::new(AlwaysLeader)),
        |_| Arc::new(NonePermissionChecker),
    );

    let response = FileSystemServiceProto::get_status(
        &env.service,
        Request::new(GetStatusRequestProto {
            header: header_with_freshness(102, Some(1), Some(0), Vec::new()),
            path: "/mnt/test".to_string(),
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();

    let response_header = response.header.expect("response header must exist");
    let err = response_header.error.expect("header.error must exist");
    assert_need_refresh_rpc(
        &err,
        RpcErrorCodeProto::RpcErrCodeRouteEpochMismatch,
        RefreshReasonProto::RefreshReasonRouteEpochMismatch,
    );
    assert_eq!(response_header.group_id, TEST_GROUP_ID);
    assert_eq!(response_header.mount_epoch, Some(1));
    assert_eq!(response_header.route_epoch, Some(1));
    let hint = err.refresh_hint.expect("refresh hint");
    assert_eq!(hint.group_id, Some(TEST_GROUP_ID));
    assert_eq!(hint.mount_epoch, Some(1));
    assert_eq!(hint.route_epoch, Some(1));
}

#[tokio::test]
async fn stale_state_id_returns_stale_state_without_epoch_domain_mixup() {
    let env = build_env_with_raft("/mnt/test", DataIoPolicy::Allow, |_| Arc::new(NonePermissionChecker)).await;
    let local_state_id = RaftLogId::new(1, 1, 10);
    let required_state_id = RaftLogId::new(1, 1, 12);
    persist_last_applied(&env, local_state_id);

    let response = FileSystemServiceProto::get_status(
        &env.service,
        Request::new(GetStatusRequestProto {
            header: header_with_freshness(
                103,
                Some(1),
                Some(1),
                vec![watermark_proto(TEST_GROUP_ID, required_state_id)],
            ),
            path: "/mnt/test".to_string(),
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();

    let response_header = response.header.expect("response header must exist");
    let err = response_header.error.expect("header.error must exist");
    assert_need_refresh_rpc(
        &err,
        RpcErrorCodeProto::RpcErrCodeStaleState,
        RefreshReasonProto::RefreshReasonStaleState,
    );
    assert_eq!(response_header.group_id, TEST_GROUP_ID);
    assert_eq!(response_header.mount_epoch, Some(1));
    assert_ne!(response_header.mount_epoch, Some(required_state_id.index));
    assert_ne!(response_header.route_epoch, Some(required_state_id.index));
    assert!(response_header.state.is_empty());
    assert!(err.refresh_hint.is_none());
}

#[tokio::test]
async fn leader_success_header_includes_group_state_watermark_when_last_applied_is_known() {
    let env = build_env_with_raft("/mnt/test", DataIoPolicy::Allow, |_| Arc::new(NonePermissionChecker)).await;
    let last_applied = RaftLogId::new(2, 1, 20);
    persist_last_applied(&env, last_applied);

    let response = FileSystemServiceProto::get_status(
        &env.service,
        Request::new(GetStatusRequestProto {
            header: header_with_freshness(104, Some(1), Some(1), Vec::new()),
            path: "/mnt/test".to_string(),
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();

    let response_header = response.header.expect("response header must exist");
    assert!(response_header.error.is_none());
    assert_eq!(response_header.group_id, TEST_GROUP_ID);
    assert_eq!(response_header.mount_epoch, Some(1));
    assert_eq!(response_header.route_epoch, Some(1));
    assert_eq!(response_header.state.len(), 1);
    let state = &response_header.state[0];
    assert_eq!(state.group_id.as_ref().expect("state group").value, TEST_GROUP_ID);
    assert_state_id(state.state_id.as_ref().expect("state id"), last_applied);
}

#[tokio::test]
async fn non_leader_success_header_leaves_state_empty() {
    let env = build_env(
        "/mnt/test",
        DataIoPolicy::Allow,
        None,
        Some(Arc::new(AlwaysLeader)),
        |_| Arc::new(NonePermissionChecker),
    );

    let response = FileSystemServiceProto::get_status(
        &env.service,
        Request::new(GetStatusRequestProto {
            header: header_with_freshness(105, Some(1), Some(1), Vec::new()),
            path: "/mnt/test".to_string(),
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();

    let response_header = response.header.expect("response header must exist");
    assert!(response_header.error.is_none());
    assert_eq!(response_header.group_id, TEST_GROUP_ID);
    assert_eq!(response_header.mount_epoch, Some(1));
    assert_eq!(response_header.route_epoch, Some(1));
    assert!(response_header.state.is_empty());
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
async fn hflush_is_not_supported() {
    let env = build_env(
        "/mnt/test",
        DataIoPolicy::Allow,
        None,
        Some(Arc::new(AlwaysLeader)),
        |_| Arc::new(NonePermissionChecker),
    );
    let file_inode_id = InodeId::new(9001);
    let data_handle_id = DataHandleId::new(9001);
    let mut attrs = FileAttrs::new();
    attrs.size = 123;
    env.storage
        .put_inode(&Inode::new_file(file_inode_id, attrs, env.mount_id, data_handle_id))
        .expect("put test file inode");
    env.storage
        .put_layout(file_inode_id, FileLayout::new(4096, 4096, 1))
        .expect("put test layout");
    let before_inode = env.storage.get_inode(file_inode_id).unwrap();
    let before_layout = env.storage.get_layout(file_inode_id).unwrap();

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
    assert_fs_errno(&err, FsErrnoProto::FsErrnoEnotsup);
    assert!(err.message.contains("Hflush is reserved"));
    assert_eq!(env.storage.get_inode(file_inode_id).unwrap(), before_inode);
    assert_eq!(env.storage.get_layout(file_inode_id).unwrap(), before_layout);
}

#[tokio::test]
async fn hsync_is_not_supported() {
    let env = build_env(
        "/mnt/test",
        DataIoPolicy::Allow,
        None,
        Some(Arc::new(AlwaysLeader)),
        |_| Arc::new(NonePermissionChecker),
    );
    let file_inode_id = InodeId::new(9002);
    let data_handle_id = DataHandleId::new(9002);
    let mut attrs = FileAttrs::new();
    attrs.size = 456;
    env.storage
        .put_inode(&Inode::new_file(file_inode_id, attrs, env.mount_id, data_handle_id))
        .expect("put test file inode");
    env.storage
        .put_layout(file_inode_id, FileLayout::new(4096, 4096, 1))
        .expect("put test layout");
    let before_inode = env.storage.get_inode(file_inode_id).unwrap();
    let before_layout = env.storage.get_layout(file_inode_id).unwrap();

    let response = FileSystemServiceProto::hsync(
        &env.service,
        Request::new(HsyncRequestProto {
            header: header(19),
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
    assert_fs_errno(&err, FsErrnoProto::FsErrnoEnotsup);
    assert!(err.message.contains("Hsync is reserved"));
    assert_eq!(env.storage.get_inode(file_inode_id).unwrap(), before_inode);
    assert_eq!(env.storage.get_layout(file_inode_id).unwrap(), before_layout);
}

#[tokio::test]
async fn get_locations_rejects_stale_handle() {
    let env = build_env("/", DataIoPolicy::Allow, None, Some(Arc::new(AlwaysLeader)), |_| {
        Arc::new(NonePermissionChecker)
    });
    let file_inode_id = InodeId::new(9101);
    let current_handle = DataHandleId::new(99101);
    let stale_handle = DataHandleId::new(99100);
    let mut attrs = FileAttrs::new();
    attrs.size = 128;
    let mut inode = Inode::new_file(file_inode_id, attrs, env.mount_id, current_handle);
    inode.data = types::fs::InodeData::File {
        extents: vec![Extent {
            file_offset: 0,
            block_id: BlockId::new(current_handle, BlockIndex::new(0)),
            block_offset: 0,
            len: 128,
            file_version: Some(4),
            block_stamp: Some(4),
        }],
        file_version: Some(4),
        lease_epoch: Some(4),
    };
    env.storage.put_inode(&inode).expect("put file inode");
    env.storage
        .put_dentry(env.root_inode_id, "file", file_inode_id)
        .expect("put file dentry");
    env.storage
        .put_layout(file_inode_id, FileLayout::new(4096, 4096, 1))
        .expect("put layout");
    env.storage
        .put_data_handle_owner(current_handle, file_inode_id)
        .expect("put current owner");
    env.storage
        .put_data_handle_owner(stale_handle, file_inode_id)
        .expect("put stale owner");

    let response = FileSystemServiceProto::get_block_locations(
        &env.service,
        Request::new(GetBlockLocationsRequestProto {
            header: header(21),
            target: Some(get_block_locations_request_proto::Target::DataHandleId(
                DataHandleIdProto {
                    value: stale_handle.as_raw(),
                },
            )),
            range: None,
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();

    let err = header_error(response.header);
    assert_need_refresh_rpc(
        &err,
        RpcErrorCodeProto::RpcErrCodeStaleState,
        RefreshReasonProto::RefreshReasonStaleState,
    );
    assert!(err.message.contains("not current data_handle_id"));
}

#[tokio::test]
async fn get_locations_accepts_current_handle() {
    let env = build_env("/", DataIoPolicy::Allow, None, Some(Arc::new(AlwaysLeader)), |_| {
        Arc::new(NonePermissionChecker)
    });
    let file_inode_id = InodeId::new(9102);
    let current_handle = DataHandleId::new(99102);
    let mut attrs = FileAttrs::new();
    attrs.size = 128;
    let mut inode = Inode::new_file(file_inode_id, attrs, env.mount_id, current_handle);
    inode.data = types::fs::InodeData::File {
        extents: vec![Extent {
            file_offset: 0,
            block_id: BlockId::new(current_handle, BlockIndex::new(0)),
            block_offset: 0,
            len: 128,
            file_version: Some(4),
            block_stamp: Some(4),
        }],
        file_version: Some(4),
        lease_epoch: Some(4),
    };
    env.storage.put_inode(&inode).expect("put file inode");
    env.storage
        .put_dentry(env.root_inode_id, "file", file_inode_id)
        .expect("put file dentry");
    env.storage
        .put_layout(file_inode_id, FileLayout::new(4096, 4096, 1))
        .expect("put layout");
    env.storage
        .put_data_handle_owner(current_handle, file_inode_id)
        .expect("put current owner");

    let response = FileSystemServiceProto::get_block_locations(
        &env.service,
        Request::new(GetBlockLocationsRequestProto {
            header: header(22),
            target: Some(get_block_locations_request_proto::Target::DataHandleId(
                DataHandleIdProto {
                    value: current_handle.as_raw(),
                },
            )),
            range: None,
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();

    assert_success_header(response.header);
    assert_eq!(
        response.data_handle_id.expect("data handle").value,
        current_handle.as_raw()
    );
    assert_eq!(response.file_version, Some(4));
    assert_eq!(response.locations.len(), 1);
    assert_eq!(response.locations[0].block_stamp, Some(4));
    assert_eq!(
        response.locations[0]
            .block_id
            .as_ref()
            .expect("block id")
            .data_handle_id,
        current_handle.as_raw()
    );
}

#[tokio::test]
async fn get_locations_by_path_uses_current_handle() {
    let env = build_env("/", DataIoPolicy::Allow, None, Some(Arc::new(AlwaysLeader)), |_| {
        Arc::new(NonePermissionChecker)
    });
    let file_inode_id = InodeId::new(9103);
    let current_handle = DataHandleId::new(99103);
    let stale_handle = DataHandleId::new(99104);
    let mut attrs = FileAttrs::new();
    attrs.size = 128;
    let mut inode = Inode::new_file(file_inode_id, attrs, env.mount_id, current_handle);
    inode.data = types::fs::InodeData::File {
        extents: vec![Extent {
            file_offset: 0,
            block_id: BlockId::new(current_handle, BlockIndex::new(0)),
            block_offset: 0,
            len: 128,
            file_version: Some(8),
            block_stamp: Some(8),
        }],
        file_version: Some(8),
        lease_epoch: Some(8),
    };
    env.storage.put_inode(&inode).expect("put file inode");
    env.storage
        .put_dentry(env.root_inode_id, "file", file_inode_id)
        .expect("put file dentry");
    env.storage
        .put_layout(file_inode_id, FileLayout::new(4096, 4096, 1))
        .expect("put layout");
    env.storage
        .put_data_handle_owner(current_handle, file_inode_id)
        .expect("put current owner");
    env.storage
        .put_data_handle_owner(stale_handle, file_inode_id)
        .expect("put stale owner");

    let response = FileSystemServiceProto::get_block_locations(
        &env.service,
        Request::new(GetBlockLocationsRequestProto {
            header: header(23),
            target: Some(get_block_locations_request_proto::Target::Path("/file".to_string())),
            range: None,
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();

    assert_success_header(response.header);
    assert_eq!(
        response.data_handle_id.expect("data handle").value,
        current_handle.as_raw()
    );
    assert_eq!(response.file_version, Some(8));
    assert_eq!(
        response.locations[0]
            .block_id
            .as_ref()
            .expect("block id")
            .data_handle_id,
        current_handle.as_raw()
    );
}

#[tokio::test]
async fn create_file_failure_leaves_no_inode() {
    let env = build_env_with_raft("/mnt/test", DataIoPolicy::Allow, |_| Arc::new(NonePermissionChecker)).await;

    let response = FileSystemServiceProto::create_file(
        &env.service,
        Request::new(CreateFileRequestProto {
            header: header(20),
            path: "/mnt/test/new-file".to_string(),
            attrs: Some(proto::fs::FileAttrsProto {
                mode: 0o644,
                uid: 1000,
                gid: 1000,
                ..Default::default()
            }),
            layout: Some(proto::common::FileLayoutProto {
                block_size: 4096,
                chunk_size: 4096,
                replication: 1,
            }),
            disposition: CreateDispositionProto::CreateNew as i32,
            desired_len: Some(4096),
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();

    let err = header_error(response.header);
    assert_eq!(err.error_class, ErrorClassProto::ErrorClassRetryable as i32);
    assert_eq!(env.storage.get_dentry(env.root_inode_id, "new-file").unwrap(), None);
}

#[tokio::test]
async fn commit_file_public_replay_returns_persisted_result_and_rejects_fingerprint_mismatch() {
    let env = build_env_with_raft_and_workers(
        "/mnt/test",
        DataIoPolicy::Allow,
        Some(worker_manager_for_write_targets()),
        |_| Arc::new(NonePermissionChecker),
    )
    .await;

    let create = FileSystemServiceProto::create_file(
        &env.service,
        Request::new(CreateFileRequestProto {
            header: header(30),
            path: "/mnt/test/replay-file".to_string(),
            attrs: Some(proto::fs::FileAttrsProto {
                mode: 0o644,
                uid: 1000,
                gid: 1000,
                ..Default::default()
            }),
            layout: Some(proto::common::FileLayoutProto {
                block_size: 4096,
                chunk_size: 4096,
                replication: 1,
            }),
            disposition: CreateDispositionProto::CreateNew as i32,
            desired_len: Some(128),
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();
    assert_success_header(create.header);

    let write_handle = create.write_handle.expect("write handle");
    let data_handle_id = create.data_handle_id.expect("data handle").value;
    let inode_id = create.inode_id.expect("inode id").value;
    assert!(env.write_session_manager.get_session(write_handle.handle_id).is_some());

    let target = FileSystemServiceProto::add_block(
        &env.service,
        Request::new(AddBlockRequestProto {
            header: header(31),
            write_handle: Some(write_handle),
            desired_len: Some(128),
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner()
    .target
    .expect("write target");
    let block_id = target.block_id.expect("target block id");
    let committed_blocks = vec![CommittedBlockProto {
        block_id: Some(block_id),
        file_offset: target.file_offset,
        len: target.len,
        checksum: None,
    }];

    let commit_header = header(32);
    let first = FileSystemServiceProto::commit_file(
        &env.service,
        Request::new(CommitFileRequestProto {
            header: commit_header.clone(),
            write_handle: Some(write_handle),
            data_handle_id: Some(DataHandleIdProto { value: data_handle_id }),
            committed_blocks: committed_blocks.clone(),
            final_size: 128,
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();
    assert_success_header(first.header);
    assert_eq!(first.committed_size, 128);
    let first_file_version = first.file_version.expect("first file version");
    assert_ne!(first_file_version, 0);
    assert!(env.write_session_manager.get_session(write_handle.handle_id).is_none());
    let typed_block_id = BlockId::new(
        DataHandleId::new(block_id.data_handle_id),
        BlockIndex::new(block_id.block_index),
    );
    assert_eq!(env.storage.get_block_ref_count(typed_block_id).unwrap(), Some(1));

    let locations = FileSystemServiceProto::get_block_locations(
        &env.service,
        Request::new(GetBlockLocationsRequestProto {
            header: header(33),
            target: Some(get_block_locations_request_proto::Target::DataHandleId(
                DataHandleIdProto { value: data_handle_id },
            )),
            range: Some(proto::common::ByteRangeProto { offset: 0, len: 128 }),
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();
    assert_success_header(locations.header);
    assert_eq!(locations.file_version, Some(first_file_version));
    assert_eq!(locations.locations.len(), 1);
    assert_eq!(locations.locations[0].block_stamp, Some(first_file_version));

    let second = FileSystemServiceProto::commit_file(
        &env.service,
        Request::new(CommitFileRequestProto {
            header: commit_header.clone(),
            write_handle: Some(write_handle),
            data_handle_id: Some(DataHandleIdProto { value: data_handle_id }),
            committed_blocks: committed_blocks.clone(),
            final_size: 128,
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();
    assert_success_header(second.header);
    assert_eq!(second.committed_size, first.committed_size);
    assert_eq!(second.file_version, Some(first_file_version));
    assert_eq!(env.storage.get_block_ref_count(typed_block_id).unwrap(), Some(1));

    let inode = env
        .storage
        .get_inode(InodeId::new(inode_id))
        .unwrap()
        .expect("committed inode");
    assert_eq!(inode.attrs.size, 128);
    match inode.data {
        types::fs::InodeData::File {
            extents, file_version, ..
        } => {
            assert_eq!(file_version, Some(first_file_version));
            assert_eq!(extents.len(), 1);
            assert_eq!(extents[0].block_id, typed_block_id);
            assert_eq!(extents[0].len, 128);
            assert_eq!(extents[0].block_stamp, Some(first_file_version));
        }
        other => panic!("expected file inode data, got {:?}", other),
    }

    let mismatch = FileSystemServiceProto::commit_file(
        &env.service,
        Request::new(CommitFileRequestProto {
            header: commit_header,
            write_handle: Some(write_handle),
            data_handle_id: Some(DataHandleIdProto { value: data_handle_id }),
            committed_blocks,
            final_size: 129,
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();
    let err = header_error(mismatch.header);
    assert_fs_errno(&err, FsErrnoProto::FsErrnoEinval);
    assert!(err.message.contains("reused with different command payload"));
    assert_eq!(env.storage.get_block_ref_count(typed_block_id).unwrap(), Some(1));
    let after_mismatch = env
        .storage
        .get_inode(InodeId::new(inode_id))
        .unwrap()
        .expect("inode after mismatch");
    assert_eq!(after_mismatch.attrs.size, 128);
    match after_mismatch.data {
        types::fs::InodeData::File { file_version, .. } => {
            assert_eq!(file_version, Some(first_file_version));
        }
        other => panic!("expected file inode data, got {:?}", other),
    }
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
async fn recursive_delete_nested_tree_success_removes_subtree_only() {
    let env = build_env_with_raft("/mnt/test", DataIoPolicy::Allow, |_| Arc::new(NonePermissionChecker)).await;
    let dir = InodeId::new(4101);
    let a = InodeId::new(4102);
    let b = InodeId::new(4103);
    let empty_subdir = InodeId::new(4104);
    let file1 = InodeId::new(4105);
    let file2 = InodeId::new(4106);
    let file1_handle = DataHandleId::new(4105);
    let file2_handle = DataHandleId::new(4106);

    put_dir(&env, env.root_inode_id, "dir", dir);
    put_dir(&env, dir, "a", a);
    put_dir(&env, a, "b", b);
    put_dir(&env, dir, "empty_subdir", empty_subdir);
    put_empty_file(&env, a, "file1", file1, file1_handle);
    put_empty_file(&env, b, "file2", file2, file2_handle);

    let response = FileSystemServiceProto::delete(
        &env.service,
        Request::new(DeleteRequestProto {
            header: header(141),
            path: "/mnt/test/dir".to_string(),
            recursive: true,
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();

    assert_success_header(response.header);
    assert_eq!(env.storage.get_dentry(env.root_inode_id, "dir").unwrap(), None);
    for inode_id in [dir, a, b, empty_subdir, file1, file2] {
        assert!(env.storage.get_inode(inode_id).unwrap().is_none());
    }
    assert!(env.storage.get_inode(env.root_inode_id).unwrap().is_some());
    assert_eq!(env.storage.get_inode_by_data_handle(file1_handle).unwrap(), None);
    assert_eq!(env.storage.get_inode_by_data_handle(file2_handle).unwrap(), None);
    assert_eq!(env.storage.list_pending_delete_intents(10, u64::MAX).unwrap().len(), 0);
}

#[tokio::test]
async fn recursive_delete_extent_file_cleans_layout_refcount_intent_and_replays_once() {
    let env = build_env_with_raft("/mnt/test", DataIoPolicy::Allow, |_| Arc::new(NonePermissionChecker)).await;
    let dir = InodeId::new(4201);
    let file = InodeId::new(4202);
    let data_handle_id = DataHandleId::new(4202);
    let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
    put_dir(&env, env.root_inode_id, "dir", dir);
    put_extent_file(&env, dir, "file", file, data_handle_id, block_id, 64);
    env.storage
        .put_block_ref_count(block_id, 1)
        .expect("put block refcount");

    let delete_header = header(142);
    let first = FileSystemServiceProto::delete(
        &env.service,
        Request::new(DeleteRequestProto {
            header: delete_header.clone(),
            path: "/mnt/test/dir".to_string(),
            recursive: true,
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();

    assert_success_header(first.header);
    assert_eq!(env.storage.get_dentry(env.root_inode_id, "dir").unwrap(), None);
    assert!(env.storage.get_inode(file).unwrap().is_none());
    assert!(env.storage.get_layout(file).is_err());
    assert_eq!(env.storage.get_inode_by_data_handle(data_handle_id).unwrap(), None);
    assert_eq!(env.storage.get_block_ref_count(block_id).unwrap(), None);
    assert_eq!(env.storage.list_pending_delete_intents(10, u64::MAX).unwrap().len(), 1);

    let replay = FileSystemServiceProto::delete(
        &env.service,
        Request::new(DeleteRequestProto {
            header: delete_header,
            path: "/mnt/test/dir".to_string(),
            recursive: true,
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();

    assert_success_header(replay.header);
    assert_eq!(env.storage.get_block_ref_count(block_id).unwrap(), None);
    assert_eq!(env.storage.list_pending_delete_intents(10, u64::MAX).unwrap().len(), 1);
}

#[tokio::test]
async fn recursive_delete_rejects_active_write_session_without_half_delete() {
    let env = build_env_with_raft_and_workers(
        "/mnt/test",
        DataIoPolicy::Allow,
        Some(worker_manager_for_write_targets()),
        |_| Arc::new(NonePermissionChecker),
    )
    .await;
    let dir = InodeId::new(4301);
    let empty_subdir = InodeId::new(4302);
    put_dir(&env, env.root_inode_id, "dir", dir);
    put_dir(&env, dir, "empty_subdir", empty_subdir);

    let create = FileSystemServiceProto::create_file(
        &env.service,
        Request::new(CreateFileRequestProto {
            header: header(143),
            path: "/mnt/test/dir/file".to_string(),
            attrs: Some(proto::fs::FileAttrsProto {
                mode: 0o644,
                uid: 1000,
                gid: 1000,
                ..Default::default()
            }),
            layout: Some(proto::common::FileLayoutProto {
                block_size: 4096,
                chunk_size: 4096,
                replication: 1,
            }),
            disposition: CreateDispositionProto::CreateNew as i32,
            desired_len: Some(128),
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();
    assert_success_header(create.header);
    let write_handle = create.write_handle.expect("write handle");
    let file_inode_id = InodeId::new(create.inode_id.expect("inode id").value);
    let data_handle_id = DataHandleId::new(create.data_handle_id.expect("data handle").value);
    assert!(env.write_session_manager.get_session(write_handle.handle_id).is_some());

    let response = FileSystemServiceProto::delete(
        &env.service,
        Request::new(DeleteRequestProto {
            header: header(144),
            path: "/mnt/test/dir".to_string(),
            recursive: true,
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();

    let err = header_error(response.header);
    assert_fs_errno(&err, FsErrnoProto::FsErrnoEbusy);
    assert_eq!(env.storage.get_dentry(env.root_inode_id, "dir").unwrap(), Some(dir));
    assert_eq!(env.storage.get_dentry(dir, "empty_subdir").unwrap(), Some(empty_subdir));
    assert_eq!(env.storage.get_dentry(dir, "file").unwrap(), Some(file_inode_id));
    assert!(env.storage.get_inode(dir).unwrap().is_some());
    assert!(env.storage.get_inode(empty_subdir).unwrap().is_some());
    assert!(env.storage.get_inode(file_inode_id).unwrap().is_some());
    assert!(env.storage.get_layout(file_inode_id).is_ok());
    assert_eq!(
        env.storage.get_inode_by_data_handle(data_handle_id).unwrap(),
        Some(file_inode_id)
    );
    assert!(env.write_session_manager.get_session(write_handle.handle_id).is_some());
    assert_eq!(env.storage.list_pending_delete_intents(10, u64::MAX).unwrap().len(), 0);
}

#[tokio::test]
async fn recursive_delete_rejects_root_or_mount_root_without_mutation() {
    let env = build_env_with_raft("/mnt/test", DataIoPolicy::Allow, |_| Arc::new(NonePermissionChecker)).await;

    let mount_root_response = FileSystemServiceProto::delete(
        &env.service,
        Request::new(DeleteRequestProto {
            header: header(145),
            path: "/mnt/test".to_string(),
            recursive: true,
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();

    let err = header_error(mount_root_response.header);
    assert_fs_errno(&err, FsErrnoProto::FsErrnoEinval);
    assert!(env.storage.get_inode(env.root_inode_id).unwrap().is_some());

    let root_env = build_env_with_raft("/", DataIoPolicy::Forbid, |_| Arc::new(NonePermissionChecker)).await;
    let root_response = FileSystemServiceProto::delete(
        &root_env.service,
        Request::new(DeleteRequestProto {
            header: header(148),
            path: "/".to_string(),
            recursive: true,
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();

    let err = header_error(root_response.header);
    assert_fs_errno(&err, FsErrnoProto::FsErrnoEinval);
    assert!(root_env.storage.get_inode(root_env.root_inode_id).unwrap().is_some());
}

#[tokio::test]
async fn recursive_delete_rejects_cross_mount_subtree_without_half_delete() {
    let env = build_env_with_raft("/mnt/test", DataIoPolicy::Allow, |_| Arc::new(NonePermissionChecker)).await;
    let dir = InodeId::new(4401);
    let child_mount_root = InodeId::new(4402);
    put_dir(&env, env.root_inode_id, "dir", dir);
    let child_mount = env
        .mount_table
        .create_mount(
            "/mnt/test/dir/mnt".to_string(),
            MountKind::External,
            Some("file:///tmp/mnt_test_dir_mnt".to_string()),
            DataIoPolicy::Allow,
            ShardGroupId::new(1),
            child_mount_root,
        )
        .expect("create child mount");
    env.storage
        .put_inode(&Inode::new_dir(
            child_mount_root,
            FileAttrs::new(),
            child_mount.mount_id,
        ))
        .expect("put child mount root inode");
    env.storage
        .put_dentry(dir, "mnt", child_mount_root)
        .expect("put child mount dentry");

    let response = FileSystemServiceProto::delete(
        &env.service,
        Request::new(DeleteRequestProto {
            header: header(146),
            path: "/mnt/test/dir".to_string(),
            recursive: true,
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();

    let err = header_error(response.header);
    assert_fs_errno(&err, FsErrnoProto::FsErrnoExdev);
    assert_eq!(env.storage.get_dentry(env.root_inode_id, "dir").unwrap(), Some(dir));
    assert_eq!(env.storage.get_dentry(dir, "mnt").unwrap(), Some(child_mount_root));
    assert!(env.storage.get_inode(dir).unwrap().is_some());
    assert!(env.storage.get_inode(child_mount_root).unwrap().is_some());
}

#[tokio::test]
async fn recursive_delete_fingerprint_mismatch_does_not_mutate_second_tree() {
    let env = build_env_with_raft("/mnt/test", DataIoPolicy::Allow, |_| Arc::new(NonePermissionChecker)).await;
    let dir1 = InodeId::new(4501);
    let dir2 = InodeId::new(4502);
    put_dir(&env, env.root_inode_id, "dir1", dir1);
    put_dir(&env, env.root_inode_id, "dir2", dir2);
    let delete_header = header(147);

    let first = FileSystemServiceProto::delete(
        &env.service,
        Request::new(DeleteRequestProto {
            header: delete_header.clone(),
            path: "/mnt/test/dir1".to_string(),
            recursive: true,
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();
    assert_success_header(first.header);

    let mismatch = FileSystemServiceProto::delete(
        &env.service,
        Request::new(DeleteRequestProto {
            header: delete_header,
            path: "/mnt/test/dir2".to_string(),
            recursive: true,
        }),
    )
    .await
    .expect("transport status must remain OK")
    .into_inner();

    let err = header_error(mismatch.header);
    assert_fs_errno(&err, FsErrnoProto::FsErrnoEinval);
    assert!(err.message.contains("reused with different command payload"));
    assert_eq!(env.storage.get_dentry(env.root_inode_id, "dir1").unwrap(), None);
    assert_eq!(env.storage.get_dentry(env.root_inode_id, "dir2").unwrap(), Some(dir2));
    assert!(env.storage.get_inode(dir2).unwrap().is_some());
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
