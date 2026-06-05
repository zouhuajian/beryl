// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use metadata::config::{MetadataConfig, RaftMode};
use metadata::lifecycle::{
    format_metadata_storage, metadata_marker_path, prepare_metadata_start, MetadataStorageMarker,
};
use metadata::mount::{DataIoPolicy, MountEntry, MountKind, ROOT_INODE_ID, ROOT_MOUNT_PREFIX};
use metadata::raft::{AppRaftNode, AppRaftStateMachine, RocksDBStorage};
use metadata::readiness::{wait_for_root_ready, RootReadinessConfig, RootReadinessGate};
use metadata::{ensure_root_mount_for_format, MountTable};
use std::sync::Arc;
use tempfile::TempDir;
use types::ids::MountId;
use types::GroupName;

fn write_config(dir: &TempDir, group_name: &str, raft_mode: &str) -> std::path::PathBuf {
    let storage_dir = dir.path().join("metadata");
    let config_path = dir.path().join(format!("{group_name}-{raft_mode}-core-site.yaml"));
    std::fs::write(
        &config_path,
        format!(
            r#"
vecton.cluster.id: "test-cluster"
metadata.storage.dir: "{}"
metadata.group.name: "{group_name}"
metadata.raft.mode: "{raft_mode}"
metadata.raft.node_id: 1
metadata.rpc.addr: "127.0.0.1"
metadata.rpc.port: 18080
metadata.bootstrap.ready.timeout_ms: 2000
metadata.bootstrap.ready.warn_after_ms: 10
metadata.bootstrap.ready.fail_fast: true
"#,
            storage_dir.display()
        ),
    )
    .unwrap();
    config_path
}

fn storage_entries(path: &std::path::Path) -> Vec<String> {
    let mut entries = std::fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    entries.sort();
    entries
}

#[tokio::test]
async fn metadata_format_creates_marker_without_group_id() {
    let dir = TempDir::new().unwrap();
    let config_path = write_config(&dir, "root", "single");
    let config = MetadataConfig::load(&config_path).unwrap();

    let marker = format_metadata_storage(&config).await.unwrap();

    assert_eq!(marker.cluster_id, "test-cluster");
    assert_eq!(marker.group_name.as_str(), "root");
    assert_eq!(marker.node_id, 1);
    assert!(metadata_marker_path(&config).exists());

    let marker_json: serde_json::Value =
        serde_json::from_slice(&std::fs::read(metadata_marker_path(&config)).unwrap()).unwrap();
    assert!(marker_json.get("group_id").is_none());
    assert_eq!(marker_json["group_name"], "root");
}

#[tokio::test]
async fn metadata_format_refuses_existing_marker() {
    let dir = TempDir::new().unwrap();
    let config_path = write_config(&dir, "root", "single");
    let config = MetadataConfig::load(&config_path).unwrap();
    format_metadata_storage(&config).await.unwrap();

    let err = format_metadata_storage(&config).await.unwrap_err();

    assert!(err.to_string().contains("already formatted"));
}

#[tokio::test]
async fn metadata_format_refuses_non_empty_markerless_storage() {
    let dir = TempDir::new().unwrap();
    let config_path = write_config(&dir, "root", "single");
    let config = MetadataConfig::load(&config_path).unwrap();
    std::fs::create_dir_all(&config.storage_dir).unwrap();
    std::fs::write(config.storage_dir.join("old-store-file"), b"stale").unwrap();

    let err = format_metadata_storage(&config).await.unwrap_err();
    let message = err.to_string();

    assert!(message.contains("metadata.storage.dir"));
    assert!(message.contains(&config.storage_dir.display().to_string()));
    assert!(message.contains("marker missing"));
    assert!(message.contains("clean the directory manually"));
}

#[tokio::test]
async fn metadata_start_fails_without_marker() {
    let dir = TempDir::new().unwrap();
    let config_path = write_config(&dir, "root", "single");
    let config = MetadataConfig::load(&config_path).unwrap();

    let err = prepare_metadata_start(&config).await.unwrap_err();

    assert!(err.to_string().contains("metadata format --config"));
    assert!(!metadata_marker_path(&config).exists());
}

#[tokio::test]
async fn metadata_start_fails_when_marker_exists_without_rocksdb_state() {
    let dir = TempDir::new().unwrap();
    let config_path = write_config(&dir, "root", "single");
    let config = MetadataConfig::load(&config_path).unwrap();
    std::fs::create_dir_all(&config.storage_dir).unwrap();
    let marker = MetadataStorageMarker {
        cluster_id: config.cluster_id.clone(),
        group_name: config.authority.group_name.clone(),
        node_id: config.raft.node_id,
        storage_uuid: "marker-only-storage".to_string(),
        format_version: 1,
        created_at_ms: 1,
        software_version: "test".to_string(),
    };
    std::fs::write(
        metadata_marker_path(&config),
        serde_json::to_vec_pretty(&marker).unwrap(),
    )
    .unwrap();
    let entries_before = storage_entries(&config.storage_dir);

    let err = prepare_metadata_start(&config).await.unwrap_err();
    let message = err.to_string();

    assert!(message.contains("RocksDB state is missing or corrupt"), "{message}");
    assert_eq!(storage_entries(&config.storage_dir), entries_before);
    assert!(metadata_marker_path(&config).exists());
    assert!(!config.storage_dir.join("CURRENT").exists());
    assert!(!config.storage_dir.join("snapshots").exists());
}

#[tokio::test]
async fn metadata_start_fails_on_marker_config_mismatch() {
    let dir = TempDir::new().unwrap();
    let first_config_path = write_config(&dir, "root", "single");
    let first_config = MetadataConfig::load(&first_config_path).unwrap();
    format_metadata_storage(&first_config).await.unwrap();

    let second_config_path = write_config(&dir, "other", "single");
    let second_config = MetadataConfig::load(&second_config_path).unwrap();
    let err = prepare_metadata_start(&second_config).await.unwrap_err();

    assert!(err.to_string().contains("metadata marker mismatch"));
    assert!(err.to_string().contains("group_name"));
}

#[tokio::test]
async fn metadata_marker_rejects_legacy_group_id_and_unknown_fields() {
    for extra_field in ["group_id", "unknown_field"] {
        let dir = TempDir::new().unwrap();
        let config_path = write_config(&dir, "root", "single");
        let config = MetadataConfig::load(&config_path).unwrap();
        std::fs::create_dir_all(&config.storage_dir).unwrap();
        std::fs::write(
            metadata_marker_path(&config),
            format!(
                r#"{{
  "cluster_id": "test-cluster",
  "group_name": "root",
  "node_id": 1,
  "storage_uuid": "storage-a",
  "format_version": 1,
  "created_at_ms": 1,
  "software_version": "test",
  "{extra_field}": 1
}}"#
            ),
        )
        .unwrap();

        let err = prepare_metadata_start(&config).await.unwrap_err();
        let message = err.to_string();
        assert!(message.contains("old marker format unsupported"), "{message}");
        assert!(message.contains("clean storage"), "{message}");
        assert!(message.contains("future migration command"), "{message}");
    }
}

#[tokio::test]
async fn metadata_format_initializes_single_node_membership() {
    let dir = TempDir::new().unwrap();
    let config_path = write_config(&dir, "root", "single");
    let config = MetadataConfig::load(&config_path).unwrap();
    format_metadata_storage(&config).await.unwrap();

    let storage = Arc::new(RocksDBStorage::create_for_format(&config.storage_dir).unwrap());
    let mount_table = Arc::new(MountTable::load_from_storage(storage.as_ref()).unwrap());
    let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table)));
    let raft_node = AppRaftNode::new(config.raft.node_id, Arc::clone(&storage), state_machine, &config.raft)
        .await
        .unwrap();

    assert!(raft_node.is_initialized().await.unwrap());
}

#[tokio::test]
async fn metadata_start_readiness_does_not_create_missing_root_mount() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_table = Arc::new(MountTable::load_from_storage(storage.as_ref()).unwrap());
    let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table)));
    let raft_config = metadata::config::RaftConfig {
        mode: metadata::config::RaftMode::Single,
        ..metadata::config::RaftConfig::default()
    };
    let raft_node = Arc::new(
        AppRaftNode::new(raft_config.node_id, Arc::clone(&storage), state_machine, &raft_config)
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
    assert!(storage.get_inode(metadata::mount::ROOT_INODE_ID).unwrap().is_none());
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
            data_io_policy: DataIoPolicy::Forbid,
            mount_version: 1,
            namespace_owner_group_name: GroupName::parse("other").unwrap(),
            root_inode_id: ROOT_INODE_ID,
        })
        .unwrap();
    let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table)));
    let raft_config = metadata::config::RaftConfig {
        mode: metadata::config::RaftMode::Single,
        ..metadata::config::RaftConfig::default()
    };
    let raft_node = Arc::new(
        AppRaftNode::new(raft_config.node_id, Arc::clone(&storage), state_machine, &raft_config)
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
async fn metadata_format_creates_root_namespace_through_raft_path() {
    let dir = TempDir::new().unwrap();
    let config_path = write_config(&dir, "root", "single");
    let config = MetadataConfig::load(&config_path).unwrap();
    format_metadata_storage(&config).await.unwrap();

    prepare_metadata_start(&config).await.unwrap();

    let storage = RocksDBStorage::create_for_format(&config.storage_dir).unwrap();
    let mount_table = MountTable::load_from_storage(&storage).unwrap();
    assert!(storage.get_inode(metadata::mount::ROOT_INODE_ID).unwrap().is_some());
    assert!(mount_table
        .list_mounts()
        .into_iter()
        .any(|mount| mount.mount_prefix == metadata::mount::ROOT_MOUNT_PREFIX));
    let local = mount_table
        .list_mounts()
        .into_iter()
        .find(|mount| mount.mount_prefix == "/local")
        .expect("local writable mount should exist after format");
    assert_eq!(local.mount_kind, MountKind::Internal);
    assert_eq!(local.data_io_policy, DataIoPolicy::Allow);
    assert_eq!(local.namespace_owner_group_name, GroupName::parse("root").unwrap());
    assert!(storage.get_inode(local.root_inode_id).unwrap().is_some());
}

#[tokio::test]
async fn metadata_start_rejects_old_local_layout_without_local_mount() {
    let dir = TempDir::new().unwrap();
    let config_path = write_config(&dir, "root", "single");
    let config = MetadataConfig::load(&config_path).unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(&config.storage_dir).unwrap());
    let mount_table = Arc::new(MountTable::load_from_storage(storage.as_ref()).unwrap());
    let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table)));
    let raft_node = Arc::new(
        AppRaftNode::new(config.raft.node_id, Arc::clone(&storage), state_machine, &config.raft)
            .await
            .unwrap(),
    );
    raft_node
        .initialize_single_node(config.rpc_addr.to_string())
        .await
        .unwrap();
    ensure_root_mount_for_format(
        Arc::clone(&raft_node),
        Arc::clone(&mount_table),
        config.authority.group_name.clone(),
    )
    .await
    .unwrap();
    raft_node.shutdown().await.unwrap();
    let marker = MetadataStorageMarker {
        cluster_id: config.cluster_id.clone(),
        group_name: config.authority.group_name.clone(),
        node_id: config.raft.node_id,
        storage_uuid: "old-root-only-storage".to_string(),
        format_version: 1,
        created_at_ms: 1,
        software_version: "test".to_string(),
    };
    std::fs::write(
        metadata_marker_path(&config),
        serde_json::to_vec_pretty(&marker).unwrap(),
    )
    .unwrap();
    drop(raft_node);
    drop(mount_table);
    drop(storage);

    let err = prepare_metadata_start(&config)
        .await
        .expect_err("old local layout without /local must fail fast");
    let message = err.to_string();

    assert!(message.contains("missing required /local mount"), "{message}");
    assert!(message.contains("older layout"), "{message}");
    assert!(message.contains("Re-run metadata format"), "{message}");
    let storage = RocksDBStorage::create_for_format(&config.storage_dir).unwrap();
    let mounts = MountTable::load_from_storage(&storage).unwrap().list_mounts();
    assert!(mounts.iter().all(|mount| mount.mount_prefix != "/local"));
}

#[tokio::test]
async fn metadata_start_rejects_local_mount_without_data_io() {
    let dir = TempDir::new().unwrap();
    let config_path = write_config(&dir, "root", "single");
    let config = MetadataConfig::load(&config_path).unwrap();
    format_metadata_storage(&config).await.unwrap();
    let storage = RocksDBStorage::create_for_format(&config.storage_dir).unwrap();
    let mut local = MountTable::load_from_storage(&storage)
        .unwrap()
        .list_mounts()
        .into_iter()
        .find(|mount| mount.mount_prefix == "/local")
        .expect("/local mount after format");
    local.data_io_policy = DataIoPolicy::Forbid;
    storage.put_mount(&local).unwrap();
    drop(storage);

    let err = prepare_metadata_start(&config)
        .await
        .expect_err("/local must be writable for data IO on start");
    let message = err.to_string();

    assert!(message.contains("local writable mount"), "{message}");
    assert!(message.contains("violates"), "{message}");
}

#[tokio::test]
async fn metadata_start_recovers_formatted_storage_without_reformatting() {
    let dir = TempDir::new().unwrap();
    let config_path = write_config(&dir, "root", "single");
    let config = MetadataConfig::load(&config_path).unwrap();
    format_metadata_storage(&config).await.unwrap();
    let marker_before = std::fs::read(metadata_marker_path(&config)).unwrap();

    prepare_metadata_start(&config).await.unwrap();

    assert_eq!(std::fs::read(metadata_marker_path(&config)).unwrap(), marker_before);
}

#[tokio::test]
async fn metadata_cluster_mode_fails_as_unsupported() {
    let dir = TempDir::new().unwrap();
    let config_path = write_config(&dir, "root", "cluster");
    let config = MetadataConfig::load(&config_path).unwrap();
    assert_eq!(config.raft.mode, RaftMode::Cluster);

    let format_err = format_metadata_storage(&config).await.unwrap_err();
    let start_err = prepare_metadata_start(&config).await.unwrap_err();

    assert!(format_err.to_string().contains("cluster Raft mode is not implemented"));
    assert!(start_err.to_string().contains("cluster Raft mode is not implemented"));
}

#[tokio::test]
async fn metadata_readiness_timeout_reports_raft_reason() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_table = Arc::new(MountTable::load_from_storage(storage.as_ref()).unwrap());
    let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table)));
    let raft_config = metadata::config::RaftConfig {
        mode: metadata::config::RaftMode::Single,
        ..metadata::config::RaftConfig::default()
    };
    let raft_node = Arc::new(
        AppRaftNode::new(raft_config.node_id, Arc::clone(&storage), state_machine, &raft_config)
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
