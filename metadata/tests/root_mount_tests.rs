// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use common::header::RequestHeader;
use metadata::config::RaftConfig;
use metadata::ensure_root_mount;
use metadata::mount::{DataIoPolicy, MountKind, ROOT_INODE_ID, ROOT_MOUNT_PREFIX};
use metadata::raft::{AppRaftNode, AppRaftStateMachine, RocksDBStorage};
use metadata::readiness::{wait_for_root_ready, RootReadinessConfig, RootReadinessGate};
use metadata::service::MetadataFsServiceImpl;
use metadata::state::MemoryStateStore;
use proto::common::LeaseIdProto;
use proto::metadata::metadata_fs_service_proto_server::MetadataFsServiceProto;
use proto::metadata::{GetFileLayoutRequestProto, OpenWriteRequestProto, TruncateRequestProto};
use tempfile::TempDir;
use types::fs::{FileAttrs, Inode};
use types::ids::{DataHandleId, ShardGroupId};
use types::ClientId;

async fn bootstrap_raft(
    storage: std::sync::Arc<RocksDBStorage>,
    mount_table: std::sync::Arc<metadata::mount::MountTable>,
) -> std::sync::Arc<AppRaftNode> {
    let state_machine = std::sync::Arc::new(AppRaftStateMachine::new(
        std::sync::Arc::clone(&storage),
        std::sync::Arc::clone(&mount_table),
    ));
    let raft_config = RaftConfig {
        node_id: 1,
        cluster_id: "test".to_string(),
        peers: vec!["127.0.0.1:0".to_string()],
    };
    std::sync::Arc::new(AppRaftNode::new(1, storage, state_machine, &raft_config).await.unwrap())
}

#[tokio::test]
async fn root_forbids_truncate() {
    let dir = TempDir::new().unwrap();
    let storage = std::sync::Arc::new(RocksDBStorage::open(dir.path()).unwrap());
    let mount_table = std::sync::Arc::new(metadata::mount::MountTable::load_from_storage(&storage).unwrap());
    let raft_node = bootstrap_raft(std::sync::Arc::clone(&storage), std::sync::Arc::clone(&mount_table)).await;

    ensure_root_mount(
        std::sync::Arc::clone(&raft_node),
        std::sync::Arc::clone(&mount_table),
        ShardGroupId::new(1),
    )
    .await
    .unwrap();

    let readiness_gate = std::sync::Arc::new(RootReadinessGate::new(None));
    let readiness_config = RootReadinessConfig {
        initial_backoff_ms: 10,
        max_backoff_ms: 50,
        warn_after_ms: 200,
    };
    wait_for_root_ready(
        std::sync::Arc::clone(&raft_node),
        std::sync::Arc::clone(&mount_table),
        ShardGroupId::new(1),
        std::sync::Arc::clone(&readiness_gate),
        readiness_config,
    )
    .await
    .unwrap();

    let root = mount_table
        .list_mounts()
        .into_iter()
        .find(|entry| entry.mount_prefix == ROOT_MOUNT_PREFIX)
        .expect("root mount should exist");

    let inode_id = types::fs::InodeId::new(11);
    let mut attrs = FileAttrs::new();
    attrs.update_timestamps(1);
    let inode = Inode::new_file(inode_id, attrs, root.mount_id, DataHandleId::new(2));
    storage.put_inode(&inode).unwrap();

    let fs_service = MetadataFsServiceImpl::new(
        std::sync::Arc::new(MemoryStateStore::new()),
        std::sync::Arc::clone(&mount_table),
    )
    .with_storage(std::sync::Arc::clone(&storage));

    let header = RequestHeader::new(ClientId::new(1));
    let req = TruncateRequestProto {
        header: Some((&header).into()),
        inode_id: Some(proto::fs::InodeIdProto {
            value: inode_id.as_raw(),
        }),
        new_size: 0,
        lease_id: Some(LeaseIdProto { high: 1, low: 2 }),
        lease_epoch: 1,
    };

    let resp = fs_service
        .truncate(tonic::Request::new(req))
        .await
        .unwrap()
        .into_inner();
    let err = resp.header.and_then(|h| h.error).expect("expected error");

    assert_eq!(err.error_class, proto::common::ErrorClassProto::ErrorClassFatal as i32);
    match err.code {
        Some(proto::common::error_detail_proto::Code::FsErrno(errno)) => {
            assert_eq!(errno, proto::common::FsErrnoProto::FsErrnoEnotsup as i32);
        }
        _ => panic!("expected FsErrno"),
    }
    assert!(err.message.contains("RootDataIoForbidden"));
}

#[tokio::test]
async fn bootstrap_root_mount_exists() {
    let dir = TempDir::new().unwrap();
    let storage = std::sync::Arc::new(RocksDBStorage::open(dir.path()).unwrap());
    let mount_table = std::sync::Arc::new(metadata::mount::MountTable::load_from_storage(&storage).unwrap());
    let raft_node = bootstrap_raft(std::sync::Arc::clone(&storage), std::sync::Arc::clone(&mount_table)).await;

    ensure_root_mount(
        std::sync::Arc::clone(&raft_node),
        std::sync::Arc::clone(&mount_table),
        ShardGroupId::new(1),
    )
    .await
    .unwrap();
    ensure_root_mount(
        std::sync::Arc::clone(&raft_node),
        std::sync::Arc::clone(&mount_table),
        ShardGroupId::new(1),
    )
    .await
    .unwrap();

    let readiness_gate = std::sync::Arc::new(RootReadinessGate::new(None));
    let readiness_config = RootReadinessConfig {
        initial_backoff_ms: 10,
        max_backoff_ms: 50,
        warn_after_ms: 200,
    };
    wait_for_root_ready(
        std::sync::Arc::clone(&raft_node),
        std::sync::Arc::clone(&mount_table),
        ShardGroupId::new(1),
        std::sync::Arc::clone(&readiness_gate),
        readiness_config,
    )
    .await
    .unwrap();

    let root = mount_table
        .list_mounts()
        .into_iter()
        .find(|entry| entry.mount_prefix == ROOT_MOUNT_PREFIX)
        .expect("root mount should exist");
    assert_eq!(root.mount_kind, MountKind::Internal);
    assert!(root.ufs_uri.is_none());
    assert_eq!(root.data_io_policy, DataIoPolicy::Forbid);

    let persisted = metadata::mount::MountTable::load_from_storage(&storage).unwrap();
    let persisted_root = persisted
        .list_mounts()
        .into_iter()
        .find(|entry| entry.mount_prefix == ROOT_MOUNT_PREFIX)
        .expect("root mount should persist");
    assert_eq!(persisted_root.mount_kind, MountKind::Internal);
    assert!(persisted_root.ufs_uri.is_none());
    assert_eq!(persisted_root.data_io_policy, DataIoPolicy::Forbid);

    assert!(storage.get_inode(ROOT_INODE_ID).unwrap().is_some());
}

#[tokio::test]
async fn root_forbids_data_io_by_default() {
    let dir = TempDir::new().unwrap();
    let storage = std::sync::Arc::new(RocksDBStorage::open(dir.path()).unwrap());
    let mount_table = std::sync::Arc::new(metadata::mount::MountTable::load_from_storage(&storage).unwrap());
    let raft_node = bootstrap_raft(std::sync::Arc::clone(&storage), std::sync::Arc::clone(&mount_table)).await;

    ensure_root_mount(
        std::sync::Arc::clone(&raft_node),
        std::sync::Arc::clone(&mount_table),
        ShardGroupId::new(1),
    )
    .await
    .unwrap();

    let readiness_gate = std::sync::Arc::new(RootReadinessGate::new(None));
    let readiness_config = RootReadinessConfig {
        initial_backoff_ms: 10,
        max_backoff_ms: 50,
        warn_after_ms: 200,
    };
    wait_for_root_ready(
        std::sync::Arc::clone(&raft_node),
        std::sync::Arc::clone(&mount_table),
        ShardGroupId::new(1),
        std::sync::Arc::clone(&readiness_gate),
        readiness_config,
    )
    .await
    .unwrap();

    let root = mount_table
        .list_mounts()
        .into_iter()
        .find(|entry| entry.mount_prefix == ROOT_MOUNT_PREFIX)
        .expect("root mount should exist");

    let inode_id = types::fs::InodeId::new(10);
    let mut attrs = FileAttrs::new();
    attrs.update_timestamps(1);
    let inode = Inode::new_file(inode_id, attrs, root.mount_id, DataHandleId::new(1));
    storage.put_inode(&inode).unwrap();

    let fs_service = MetadataFsServiceImpl::new(
        std::sync::Arc::new(MemoryStateStore::new()),
        std::sync::Arc::clone(&mount_table),
    )
    .with_storage(std::sync::Arc::clone(&storage));

    let header = RequestHeader::new(ClientId::new(1));
    let req = OpenWriteRequestProto {
        header: Some((&header).into()),
        inode_id: Some(proto::fs::InodeIdProto {
            value: inode_id.as_raw(),
        }),
        desired_len: None,
        mode: proto::metadata::WriteModeProto::WriteModeWrite as i32,
    };

    let resp = fs_service
        .open_write(tonic::Request::new(req))
        .await
        .unwrap()
        .into_inner();
    let err = resp.header.and_then(|h| h.error).expect("expected error");

    assert_eq!(err.error_class, proto::common::ErrorClassProto::ErrorClassFatal as i32);
    match err.code {
        Some(proto::common::error_detail_proto::Code::FsErrno(errno)) => {
            assert_eq!(errno, proto::common::FsErrnoProto::FsErrnoEnotsup as i32);
        }
        _ => panic!("expected FsErrno"),
    }
    assert!(err.message.contains("RootDataIoForbidden"));
}

#[tokio::test]
async fn root_forbids_read_data_io() {
    let dir = TempDir::new().unwrap();
    let storage = std::sync::Arc::new(RocksDBStorage::open(dir.path()).unwrap());
    let mount_table = std::sync::Arc::new(metadata::mount::MountTable::load_from_storage(&storage).unwrap());
    let raft_node = bootstrap_raft(std::sync::Arc::clone(&storage), std::sync::Arc::clone(&mount_table)).await;

    ensure_root_mount(
        std::sync::Arc::clone(&raft_node),
        std::sync::Arc::clone(&mount_table),
        ShardGroupId::new(1),
    )
    .await
    .unwrap();

    let readiness_gate = std::sync::Arc::new(RootReadinessGate::new(None));
    let readiness_config = RootReadinessConfig {
        initial_backoff_ms: 10,
        max_backoff_ms: 50,
        warn_after_ms: 200,
    };
    wait_for_root_ready(
        std::sync::Arc::clone(&raft_node),
        std::sync::Arc::clone(&mount_table),
        ShardGroupId::new(1),
        std::sync::Arc::clone(&readiness_gate),
        readiness_config,
    )
    .await
    .unwrap();

    let root = mount_table
        .list_mounts()
        .into_iter()
        .find(|entry| entry.mount_prefix == ROOT_MOUNT_PREFIX)
        .expect("root mount should exist");

    let inode_id = types::fs::InodeId::new(12);
    let mut attrs = FileAttrs::new();
    attrs.update_timestamps(1);
    let inode = Inode::new_file(inode_id, attrs, root.mount_id, DataHandleId::new(3));
    storage.put_inode(&inode).unwrap();

    let fs_service = MetadataFsServiceImpl::new(
        std::sync::Arc::new(MemoryStateStore::new()),
        std::sync::Arc::clone(&mount_table),
    )
    .with_storage(std::sync::Arc::clone(&storage));

    let header = RequestHeader::new(ClientId::new(1));
    let req = GetFileLayoutRequestProto {
        header: Some((&header).into()),
        inode_id: Some(proto::fs::InodeIdProto {
            value: inode_id.as_raw(),
        }),
        range: None,
    };

    let resp = fs_service
        .get_file_layout(tonic::Request::new(req))
        .await
        .unwrap()
        .into_inner();
    let err = resp.header.and_then(|h| h.error).expect("expected error");

    assert_eq!(err.error_class, proto::common::ErrorClassProto::ErrorClassFatal as i32);
    match err.code {
        Some(proto::common::error_detail_proto::Code::FsErrno(errno)) => {
            assert_eq!(errno, proto::common::FsErrnoProto::FsErrnoEnotsup as i32);
        }
        _ => panic!("expected FsErrno"),
    }
    assert!(err.message.contains("RootDataIoForbidden"));
}
