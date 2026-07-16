// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use beryl_worker::config::WorkerConfig;
use beryl_worker::control::{prepare_worker_start, worker_storage_info_path, MetadataRegistrar};
use tempfile::TempDir;

fn worker_storage_info_temp_path_for_test(config: &WorkerConfig) -> std::path::PathBuf {
    let info_path = worker_storage_info_path(config);
    let file_name = info_path.file_name().unwrap().to_string_lossy();
    info_path.with_file_name(format!("{file_name}.tmp"))
}

fn prepare_start_descriptor(config: &WorkerConfig) -> Result<(), String> {
    let worker_id = prepare_worker_start(config).map_err(|err| err.to_string())?;
    MetadataRegistrar::descriptor_from_config(config, worker_id)
        .map(|_| ())
        .map_err(|err| err.to_string())
}

fn write_config(dir: &TempDir, cluster_id: &str, group_name: &str) -> std::path::PathBuf {
    let worker_dir = dir.path().join("worker");
    let store_dir = worker_dir.join("hdd0");
    let identity_path = worker_dir.join("worker.identity");
    let config_path = dir.path().join("worker.yaml");
    std::fs::write(
        &config_path,
        format!(
            r#"
cluster.id: "{cluster_id}"
worker.identity.path: "{}"
worker.store.dirs.hdd0.path: "{}"
worker.store.dirs.hdd0.tier: "HDD"
worker.store.dirs.hdd0.capacity: "10GB"
worker.store.reserve_space: "1GB"
worker.store.selection_policy: "round_robin"
worker.store.check_interval_ms: 30000
worker.rpc.bind: "127.0.0.1:0"
worker.rpc.advertised_endpoint: "http://127.0.0.1:19090"
worker.metadata.group.name: "{group_name}"
worker.metadata.endpoints: "http://127.0.0.1:18080"
observe.log.format: compact
observe.log.output: stderr
observe.log.level: "info,beryl_metadata=info,beryl_worker=info,beryl_common=info,openraft=warn,tonic=warn,tower=warn,h2=warn"
observe.metrics.prometheus.bind: "127.0.0.1:19091"
observe.metrics.prometheus.path: "/metrics"
"#,
            identity_path.display(),
            store_dir.display()
        ),
    )
    .unwrap();
    config_path
}

#[test]
fn worker_start_on_missing_store_dirs_creates_identity_info() {
    let dir = TempDir::new().unwrap();
    let config_path = write_config(&dir, "cluster-a", "root");
    let config = WorkerConfig::load(&config_path).unwrap();

    let worker_id = prepare_worker_start(&config).unwrap();

    assert!(worker_id.as_raw() > 0);
    assert!(config.identity_path.exists());
    assert!(worker_storage_info_path(&config).exists());
    assert!(!worker_storage_info_temp_path_for_test(&config).exists());

    let info_json: serde_json::Value =
        serde_json::from_slice(&std::fs::read(worker_storage_info_path(&config)).unwrap()).unwrap();
    assert_eq!(info_json["cluster_id"], "cluster-a");
    assert_eq!(info_json["worker_id"], worker_id.as_raw());
    assert!(info_json.get("group_id").is_none());
    assert!(info_json.get("metadata_group_id").is_none());
}

#[test]
fn worker_start_on_empty_store_dirs_creates_identity_info() {
    let dir = TempDir::new().unwrap();
    let config_path = write_config(&dir, "cluster-a", "root");
    let config = WorkerConfig::load(&config_path).unwrap();
    std::fs::create_dir_all(&config.store.dirs["hdd0"].path).unwrap();

    let worker_id = prepare_worker_start(&config).unwrap();

    assert!(worker_id.as_raw() > 0);
    assert!(config.identity_path.exists());
    assert!(worker_storage_info_path(&config).exists());
}

#[test]
fn worker_start_on_existing_info_and_identity_succeeds_without_rewriting_identity() {
    let dir = TempDir::new().unwrap();
    let config_path = write_config(&dir, "cluster-a", "root");
    let config = WorkerConfig::load(&config_path).unwrap();
    let worker_id = prepare_worker_start(&config).unwrap();
    let identity_before = std::fs::read(&config.identity_path).unwrap();
    let info_before = std::fs::read(worker_storage_info_path(&config)).unwrap();

    let second_worker_id = prepare_worker_start(&config).unwrap();

    assert_eq!(second_worker_id, worker_id);
    assert_eq!(std::fs::read(&config.identity_path).unwrap(), identity_before);
    assert_eq!(std::fs::read(worker_storage_info_path(&config)).unwrap(), info_before);
}

#[test]
fn worker_start_refuses_existing_info_with_missing_identity_without_recreating_it() {
    let dir = TempDir::new().unwrap();
    let config_path = write_config(&dir, "cluster-a", "root");
    let config = WorkerConfig::load(&config_path).unwrap();
    prepare_worker_start(&config).unwrap();
    std::fs::remove_file(&config.identity_path).unwrap();

    let err = prepare_worker_start(&config).unwrap_err();

    assert!(err.to_string().contains("worker.identity.path"));
    assert!(!config.identity_path.exists());
}

#[test]
fn worker_start_refuses_malformed_identity_without_rewriting_it() {
    let dir = TempDir::new().unwrap();
    let config_path = write_config(&dir, "cluster-a", "root");
    let config = WorkerConfig::load(&config_path).unwrap();
    prepare_worker_start(&config).unwrap();
    let info_before = std::fs::read(worker_storage_info_path(&config)).unwrap();
    std::fs::write(&config.identity_path, b"not-a-uuid\n").unwrap();
    let identity_before = std::fs::read(&config.identity_path).unwrap();

    let err = prepare_start_descriptor(&config).unwrap_err();

    assert!(err.contains("worker.identity.path"));
    assert!(err.contains("must contain a UUID"));
    assert_eq!(std::fs::read(worker_storage_info_path(&config)).unwrap(), info_before);
    assert_eq!(std::fs::read(&config.identity_path).unwrap(), identity_before);
}

#[test]
fn worker_start_refuses_worker_id_mismatch_without_rewriting_storage() {
    let dir = TempDir::new().unwrap();
    let config_path = write_config(&dir, "cluster-a", "root");
    let config = WorkerConfig::load(&config_path).unwrap();
    prepare_worker_start(&config).unwrap();
    let mut info: serde_json::Value =
        serde_json::from_slice(&std::fs::read(worker_storage_info_path(&config)).unwrap()).unwrap();
    let original_worker_id = info["worker_id"].as_u64().unwrap();
    info["worker_id"] = serde_json::Value::from(original_worker_id + 1);
    let info_payload = serde_json::to_vec_pretty(&info).unwrap();
    std::fs::write(worker_storage_info_path(&config), &info_payload).unwrap();
    let identity_before = std::fs::read(&config.identity_path).unwrap();

    let err = prepare_start_descriptor(&config).unwrap_err();

    assert!(err.contains("worker storage info mismatch"));
    assert!(err.contains("worker_id"));
    assert_eq!(std::fs::read(worker_storage_info_path(&config)).unwrap(), info_payload);
    assert_eq!(std::fs::read(&config.identity_path).unwrap(), identity_before);
}

#[test]
fn worker_start_refuses_partial_storage_info_temp_without_final_marker() {
    let dir = TempDir::new().unwrap();
    let config_path = write_config(&dir, "cluster-a", "root");
    let config = WorkerConfig::load(&config_path).unwrap();
    let info_path = worker_storage_info_path(&config);
    let temp_path = worker_storage_info_temp_path_for_test(&config);
    std::fs::create_dir_all(temp_path.parent().unwrap()).unwrap();
    std::fs::write(&temp_path, br#"{"cluster_id":"cluster-a""#).unwrap();

    let err = prepare_worker_start(&config).unwrap_err();
    let message = err.to_string();

    assert!(message.contains("partial worker storage info"));
    assert!(message.contains(&temp_path.display().to_string()));
    assert!(temp_path.exists());
    assert!(!info_path.exists());
    assert!(!config.identity_path.exists());
}

#[test]
fn worker_start_ignores_temp_storage_info_when_valid_final_marker_exists() {
    let dir = TempDir::new().unwrap();
    let config_path = write_config(&dir, "cluster-a", "root");
    let config = WorkerConfig::load(&config_path).unwrap();
    let worker_id = prepare_worker_start(&config).unwrap();
    let info_before = std::fs::read(worker_storage_info_path(&config)).unwrap();
    let temp_path = worker_storage_info_temp_path_for_test(&config);
    std::fs::write(&temp_path, br#"{"cluster_id":"partial"}"#).unwrap();

    let second_worker_id = prepare_worker_start(&config).unwrap();

    assert_eq!(second_worker_id, worker_id);
    assert_eq!(std::fs::read(worker_storage_info_path(&config)).unwrap(), info_before);
    assert!(temp_path.exists());
}

#[test]
fn worker_start_refuses_invalid_final_storage_info() {
    let dir = TempDir::new().unwrap();
    let config_path = write_config(&dir, "cluster-a", "root");
    let config = WorkerConfig::load(&config_path).unwrap();
    prepare_worker_start(&config).unwrap();
    std::fs::write(worker_storage_info_path(&config), b"not-json").unwrap();

    let err = prepare_worker_start(&config).unwrap_err();
    let message = err.to_string();

    assert!(message.contains("worker storage info"));
    assert!(message.contains("malformed"));
    assert!(message.contains("clean storage"));
}

#[test]
fn worker_storage_info_rejects_legacy_group_id_and_unknown_fields() {
    for extra_field in ["group_id", "unknown_field"] {
        let dir = TempDir::new().unwrap();
        let config_path = write_config(&dir, "cluster-a", "root");
        let config = WorkerConfig::load(&config_path).unwrap();
        let worker_id = prepare_worker_start(&config).unwrap();
        std::fs::write(
            worker_storage_info_path(&config),
            format!(
                r#"{{
  "cluster_id": "cluster-a",
  "worker_id": {},
  "storage_uuid": "storage-a",
  "format_version": 1,
  "created_at_ms": 1,
  "software_version": "test",
  "{extra_field}": 1
}}"#,
                worker_id.as_raw()
            ),
        )
        .unwrap();

        let err = prepare_worker_start(&config).unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("old worker storage info format unsupported"),
            "{message}"
        );
        assert!(message.contains("clean storage"), "{message}");
        assert!(message.contains("future migration command"), "{message}");
    }
}

#[test]
fn worker_start_refuses_non_empty_unknown_store_dirs_without_creating_identity() {
    let dir = TempDir::new().unwrap();
    let config_path = write_config(&dir, "cluster-a", "root");
    let config = WorkerConfig::load(&config_path).unwrap();
    let store_dir = &config.store.dirs["hdd0"].path;
    std::fs::create_dir_all(store_dir).unwrap();
    std::fs::write(store_dir.join("old-block-file"), b"stale").unwrap();

    let err = prepare_worker_start(&config).unwrap_err();
    let message = err.to_string();

    assert!(message.contains("worker.store.dirs"));
    assert!(message.contains("WorkerStorageInfo is missing"));
    assert!(!worker_storage_info_path(&config).exists());
    assert!(!config.identity_path.exists());
}

#[test]
fn worker_registration_descriptor_uses_resolved_worker_id_and_group_name() {
    let dir = TempDir::new().unwrap();
    let config_path = write_config(&dir, "cluster-a", "root");
    let config = WorkerConfig::load(&config_path).unwrap();
    let worker_id = prepare_worker_start(&config).unwrap();
    std::fs::remove_file(&config.identity_path).unwrap();

    let descriptor = MetadataRegistrar::descriptor_from_config(&config, worker_id).unwrap();

    assert_eq!(descriptor.worker_id, worker_id);
    assert_eq!(descriptor.group_name.as_str(), "root");
    assert!(!config.identity_path.exists());
}

#[test]
fn group_name_validation_accepts_valid_names_and_rejects_invalid_names() {
    for group_name in ["root", "default", "analytics", "tenant-a", "hot_cache", "group.1"] {
        let dir = TempDir::new().unwrap();
        let config_path = write_config(&dir, "cluster-a", group_name);
        let config = WorkerConfig::load(&config_path).unwrap();
        assert_eq!(config.metadata.group_name.as_str(), group_name);
    }

    for group_name in [
        "",
        " ",
        "Root",
        "ROOT",
        "root/prod",
        "root prod",
        "-root",
        "root/",
        "a234567890123456789012345678901234567890123456789012345678901234",
    ] {
        let dir = TempDir::new().unwrap();
        let config_path = write_config(&dir, "cluster-a", group_name);
        let err = WorkerConfig::load(&config_path).unwrap_err();
        assert!(err.message.contains("worker.metadata.group.name"));
    }
}
