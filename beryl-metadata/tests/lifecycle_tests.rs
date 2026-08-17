// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use beryl_metadata::config::MetadataConfig;
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
beryl.cluster.id: "test-cluster"
beryl.metadata.storage.dir: "{}"
beryl.metadata.host: "127.0.0.1"
beryl.metadata.bind-host: "127.0.0.1"
beryl.metadata.rpc.port: 18080
beryl.metadata.http.port: 18081
beryl.metadata.startup.timeout: 2s
beryl.metadata.startup.warn-after: 10ms
beryl.logging.format: compact
beryl.logging.output: stderr
beryl.logging.level: "info,beryl_metadata=info,beryl_worker=info,beryl_common=info,openraft=warn,tonic=warn,tower=warn,h2=warn"
"#,
            storage_dir.display()
        ),
    )
    .unwrap();
    config_path
}

fn marker_for(config: &MetadataConfig, state: FormatState) -> MetadataStorageMarker {
    MetadataStorageMarker {
        state,
        cluster_id: config.cluster_id.clone(),
        group_name: config.authority.group_name.clone(),
        node_id: config.raft.node_id,
        storage_uuid: "test-storage".to_string(),
        format_version: 1,
        created_at_ms: 1,
        software_version: "test".to_string(),
        bootstrap_client_id: "42".to_string(),
        bootstrap_call_id: "550e8400-e29b-41d4-a716-446655440000".to_string(),
        bootstrap_proposed_at_ms: 1,
    }
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
async fn metadata_start_rejects_non_current_marker_versions_without_rewriting_them() {
    for unsupported_version in [0, 2, u32::MAX] {
        let dir = TempDir::new().unwrap();
        let config_path = write_config(&dir, "root", "single");
        let config = MetadataConfig::load(&config_path).unwrap();
        format_metadata_storage(&config).await.unwrap();
        let marker_path = metadata_marker_path(&config);
        let mut marker: MetadataStorageMarker = serde_json::from_slice(&std::fs::read(&marker_path).unwrap()).unwrap();
        marker.format_version = unsupported_version;
        let unsupported_marker = serde_json::to_vec_pretty(&marker).unwrap();
        std::fs::write(&marker_path, &unsupported_marker).unwrap();

        let err = prepare_metadata_start(&config)
            .await
            .expect_err("a non-current metadata marker must fail fast");
        let message = err.to_string();

        assert!(
            message.contains(&format!("format_version={unsupported_version}")),
            "{message}"
        );
        assert!(message.contains("expected 1"), "{message}");
        assert!(message.contains("reformat metadata storage"), "{message}");
        assert_eq!(std::fs::read(&marker_path).unwrap(), unsupported_marker);
    }
}
