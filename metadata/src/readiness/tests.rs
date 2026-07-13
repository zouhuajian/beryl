#![cfg(test)]

use super::*;
use crate::config::{RaftConfig, RaftMode};
use crate::mount::{DataIoPolicy, MountEntry, MountKind, ROOT_INODE_ID, ROOT_MOUNT_PREFIX};
use crate::raft::{AppRaftNode, AppRaftStateMachine, RocksDBStorage};
use std::sync::Arc;
use tempfile::TempDir;
use types::ids::MountId;
use types::GroupName;

async fn wait_for_root_ready(
    raft_node: Arc<AppRaftNode>,
    mount_table: Arc<MountTable>,
    namespace_owner_group_name: GroupName,
    readiness_gate: Arc<RootReadinessGate>,
    config: RootReadinessConfig,
) -> MetadataResult<()> {
    wait_for_root_ready_with_inputs(RootReadyInputs {
        raft_node,
        mount_table,
        storage: None,
        namespace_owner_group_name,
        readiness_gate,
        config,
        metrics: None,
        log_fields: RootReadinessLogFields {
            cluster_id: "test-cluster".to_string(),
            group_name: "root".to_string(),
            node_id: 1,
            storage_dir: "test".to_string(),
        },
    })
    .await
}

#[tokio::test]
async fn metadata_start_readiness_does_not_create_missing_root_mount() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_table = Arc::new(MountTable::load_from_storage(storage.as_ref()).unwrap());
    let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage)));
    let raft_config = RaftConfig {
        mode: RaftMode::Single,
        ..RaftConfig::default()
    };
    let raft_node = Arc::new(
        AppRaftNode::new(
            raft_config.node_id,
            Arc::clone(&storage),
            state_machine,
            Arc::clone(&mount_table),
            &raft_config,
        )
        .await
        .unwrap(),
    );
    raft_node
        .initialize_single_node("127.0.0.1:0".to_string())
        .await
        .unwrap();

    let err = wait_for_root_ready(
        raft_node,
        Arc::clone(&mount_table),
        GroupName::parse("root").unwrap(),
        Arc::new(RootReadinessGate::new(None)),
        RootReadinessConfig {
            initial_backoff_ms: 1,
            max_backoff_ms: 2,
            warn_after_ms: 1,
            timeout_ms: 10,
            fail_fast: true,
        },
    )
    .await
    .unwrap_err();

    let message = err.to_string();
    assert!(message.contains("RootMountMissing"), "{message}");
    assert!(mount_table
        .list_mounts()
        .into_iter()
        .all(|mount| mount.mount_prefix != ROOT_MOUNT_PREFIX));
    assert!(storage.get_inode(ROOT_INODE_ID).unwrap().is_none());
}

#[tokio::test]
async fn metadata_readiness_rejects_root_mount_with_wrong_owner_group() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_table = Arc::new(MountTable::load_from_storage(storage.as_ref()).unwrap());
    mount_table
        .upsert(MountEntry {
            mount_id: MountId::new(1),
            mount_prefix: ROOT_MOUNT_PREFIX.to_string(),
            mount_kind: MountKind::Internal,
            ufs_uri: None,
            data_io_policy: DataIoPolicy::Allow,
            mount_epoch: 1,
            namespace_owner_group_name: GroupName::parse("other").unwrap(),
            root_inode_id: ROOT_INODE_ID,
        })
        .unwrap();
    let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage)));
    let raft_config = RaftConfig {
        mode: RaftMode::Single,
        ..RaftConfig::default()
    };
    let raft_node = Arc::new(
        AppRaftNode::new(
            raft_config.node_id,
            Arc::clone(&storage),
            state_machine,
            Arc::clone(&mount_table),
            &raft_config,
        )
        .await
        .unwrap(),
    );
    raft_node
        .initialize_single_node("127.0.0.1:0".to_string())
        .await
        .unwrap();
    let readiness_gate = Arc::new(RootReadinessGate::new(None));

    let err = wait_for_root_ready(
        raft_node,
        Arc::clone(&mount_table),
        GroupName::parse("root").unwrap(),
        Arc::clone(&readiness_gate),
        RootReadinessConfig {
            initial_backoff_ms: 1,
            max_backoff_ms: 2,
            warn_after_ms: 1,
            timeout_ms: 10,
            fail_fast: true,
        },
    )
    .await
    .expect_err("wrong root owner group must not become ready");

    let message = err.to_string();
    assert!(
        message.contains("owner group") || message.contains("RootMountOwnerMismatch"),
        "{message}"
    );
    assert!(!readiness_gate.is_ready());
}

#[tokio::test]
async fn metadata_readiness_timeout_reports_raft_reason() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_table = Arc::new(MountTable::load_from_storage(storage.as_ref()).unwrap());
    let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage)));
    let raft_config = RaftConfig {
        mode: RaftMode::Single,
        ..RaftConfig::default()
    };
    let raft_node = Arc::new(
        AppRaftNode::new(
            raft_config.node_id,
            Arc::clone(&storage),
            state_machine,
            Arc::clone(&mount_table),
            &raft_config,
        )
        .await
        .unwrap(),
    );

    let err = wait_for_root_ready(
        raft_node,
        mount_table,
        GroupName::parse("root").unwrap(),
        Arc::new(RootReadinessGate::new(None)),
        RootReadinessConfig {
            initial_backoff_ms: 1,
            max_backoff_ms: 2,
            warn_after_ms: 1,
            timeout_ms: 10,
            fail_fast: true,
        },
    )
    .await
    .unwrap_err();

    let message = err.to_string();
    assert!(message.contains("RaftUninitialized"));
    assert!(message.contains("root readiness timed out"));
}
