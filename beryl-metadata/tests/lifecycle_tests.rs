// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use beryl_metadata::config::{MetadataConfig, RaftMode};
use beryl_metadata::lifecycle::{
    format_metadata_storage, metadata_marker_path, prepare_metadata_start, FormatState, MetadataStorageMarker,
};
use tempfile::TempDir;

fn write_config(dir: &TempDir, group_name: &str, raft_mode: &str) -> std::path::PathBuf {
    let storage_dir = dir.path().join("metadata");
    let config_path = dir.path().join(format!("{group_name}-{raft_mode}-metadata.yaml"));
    std::fs::write(
        &config_path,
        format!(
            r#"
cluster.id: "test-cluster"
metadata.storage.dir: "{}"
metadata.group.name: "{group_name}"
metadata.raft.mode: "{raft_mode}"
metadata.raft.node_id: 1
metadata.rpc.addr: "127.0.0.1"
metadata.rpc.port: 18080
metadata.bootstrap.ready.timeout_ms: 2000
metadata.bootstrap.ready.warn_after_ms: 10
metadata.bootstrap.ready.fail_fast: true
observe.log.format: compact
observe.log.output: stderr
observe.log.level: "info,beryl_metadata=info,beryl_worker=info,beryl_common=info,openraft=warn,tonic=warn,tower=warn,h2=warn"
observe.metrics.prometheus.bind: "127.0.0.1:18081"
observe.metrics.prometheus.path: "/metrics"
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

fn marker_for(config: &MetadataConfig, state: FormatState) -> MetadataStorageMarker {
    MetadataStorageMarker {
        state,
        cluster_id: config.cluster_id.clone(),
        group_name: config.authority.group_name.clone(),
        node_id: config.raft.node_id,
        storage_uuid: "test-storage".to_string(),
        format_version: 2,
        created_at_ms: 1,
        software_version: "test".to_string(),
        bootstrap_client_id: "42".to_string(),
        bootstrap_call_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
        bootstrap_proposed_at_ms: 1,
    }
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
    assert_eq!(marker.state, FormatState::Ready);
    assert_eq!(marker.format_version, 2);
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
async fn metadata_format_resumes_matching_formatting_marker_with_stable_bootstrap_identity() {
    let dir = TempDir::new().unwrap();
    let config_path = write_config(&dir, "root", "single");
    let config = MetadataConfig::load(&config_path).unwrap();
    std::fs::create_dir_all(&config.storage_dir).unwrap();
    let formatting = marker_for(&config, FormatState::Formatting);
    std::fs::write(
        metadata_marker_path(&config),
        serde_json::to_vec_pretty(&formatting).unwrap(),
    )
    .unwrap();

    let ready = format_metadata_storage(&config).await.unwrap();

    assert_eq!(ready.state, FormatState::Ready);
    assert_eq!(ready.storage_uuid, formatting.storage_uuid);
    assert_eq!(ready.bootstrap_client_id, formatting.bootstrap_client_id);
    assert_eq!(ready.bootstrap_call_id, formatting.bootstrap_call_id);
    assert_eq!(ready.bootstrap_proposed_at_ms, formatting.bootstrap_proposed_at_ms);
}

#[tokio::test]
async fn metadata_format_recovers_synced_unpublished_marker_temp() {
    let dir = TempDir::new().unwrap();
    let config_path = write_config(&dir, "root", "single");
    let config = MetadataConfig::load(&config_path).unwrap();
    std::fs::create_dir_all(&config.storage_dir).unwrap();
    let formatting = marker_for(&config, FormatState::Formatting);
    let marker_path = metadata_marker_path(&config);
    std::fs::write(
        marker_path.with_extension("json.tmp"),
        serde_json::to_vec_pretty(&formatting).unwrap(),
    )
    .unwrap();

    let ready = format_metadata_storage(&config).await.unwrap();

    assert_eq!(ready.state, FormatState::Ready);
    assert_eq!(ready.storage_uuid, formatting.storage_uuid);
    assert_eq!(ready.bootstrap_call_id, formatting.bootstrap_call_id);
    assert!(!marker_path.with_extension("json.tmp").exists());
}

#[tokio::test]
async fn metadata_start_rejects_formatting_marker_without_mutating_storage() {
    let dir = TempDir::new().unwrap();
    let config_path = write_config(&dir, "root", "single");
    let config = MetadataConfig::load(&config_path).unwrap();
    std::fs::create_dir_all(&config.storage_dir).unwrap();
    let formatting = marker_for(&config, FormatState::Formatting);
    std::fs::write(
        metadata_marker_path(&config),
        serde_json::to_vec_pretty(&formatting).unwrap(),
    )
    .unwrap();

    let error = prepare_metadata_start(&config).await.unwrap_err();

    assert!(error.to_string().contains("format is incomplete"));
    assert!(!config.storage_dir.join("CURRENT").exists());
}

#[tokio::test]
async fn metadata_format_validates_bootstrap_identity_before_creating_rocksdb() {
    let dir = TempDir::new().unwrap();
    let config_path = write_config(&dir, "root", "single");
    let config = MetadataConfig::load(&config_path).unwrap();
    std::fs::create_dir_all(&config.storage_dir).unwrap();
    let mut formatting = marker_for(&config, FormatState::Formatting);
    formatting.bootstrap_call_id = "not-a-uuid".to_string();
    std::fs::write(
        metadata_marker_path(&config),
        serde_json::to_vec_pretty(&formatting).unwrap(),
    )
    .unwrap();

    let error = format_metadata_storage(&config).await.unwrap_err();

    assert!(error.to_string().contains("bootstrap_call_id is invalid"));
    assert!(!config.storage_dir.join("CURRENT").exists());
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
    let marker = marker_for(&config, FormatState::Ready);
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
async fn metadata_start_rejects_marker_from_another_storage() {
    let first_dir = TempDir::new().unwrap();
    let first_config_path = write_config(&first_dir, "root", "single");
    let first_config = MetadataConfig::load(&first_config_path).unwrap();
    format_metadata_storage(&first_config).await.unwrap();

    let second_dir = TempDir::new().unwrap();
    let second_config_path = write_config(&second_dir, "root", "single");
    let second_config = MetadataConfig::load(&second_config_path).unwrap();
    format_metadata_storage(&second_config).await.unwrap();

    std::fs::write(
        metadata_marker_path(&first_config),
        std::fs::read(metadata_marker_path(&second_config)).unwrap(),
    )
    .unwrap();

    let error = prepare_metadata_start(&first_config)
        .await
        .expect_err("a marker must be bound to exactly one RocksDB store");

    assert!(error.to_string().contains("storage identity mismatch"), "{error}");
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
        assert!(message.contains("reformat metadata storage"), "{message}");
    }
}

#[tokio::test]
async fn metadata_start_rejects_format_v1_with_explicit_reformat_error() {
    let dir = TempDir::new().unwrap();
    let config_path = write_config(&dir, "root", "single");
    let config = MetadataConfig::load(&config_path).unwrap();
    format_metadata_storage(&config).await.unwrap();
    let mut marker: MetadataStorageMarker =
        serde_json::from_slice(&std::fs::read(metadata_marker_path(&config)).unwrap()).unwrap();
    marker.format_version = 1;
    std::fs::write(
        metadata_marker_path(&config),
        serde_json::to_vec_pretty(&marker).unwrap(),
    )
    .unwrap();

    let err = prepare_metadata_start(&config)
        .await
        .expect_err("format v1 must fail fast");
    let message = err.to_string();

    assert!(message.contains("format_version=1"), "{message}");
    assert!(message.contains("reformat metadata storage"), "{message}");
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
