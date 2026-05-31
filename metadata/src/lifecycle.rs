// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Local metadata storage lifecycle.

use crate::config::{MetadataConfig, RaftMode};
use crate::ensure_root_mount_for_format;
use crate::error::{MetadataError, MetadataResult};
use crate::mount::{DataIoPolicy, MountKind, MountTable, ROOT_INODE_ID, ROOT_MOUNT_PREFIX};
use crate::raft::{AppRaftNode, AppRaftStateMachine, RocksDBStorage};
use crate::readiness::{wait_for_root_ready_with_inputs, RootReadinessGate, RootReadinessLogFields, RootReadyInputs};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;
use types::GroupName;
use uuid::Uuid;

const METADATA_MARKER_FILE: &str = "metadata.marker.json";
const FORMAT_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MetadataStorageMarker {
    pub cluster_id: String,
    pub group_name: GroupName,
    pub node_id: u64,
    pub storage_uuid: String,
    pub format_version: u32,
    pub created_at_ms: u64,
    pub software_version: String,
}

pub fn metadata_marker_path(config: &MetadataConfig) -> PathBuf {
    config.storage_dir.join(METADATA_MARKER_FILE)
}

pub async fn format_metadata_storage(config: &MetadataConfig) -> MetadataResult<MetadataStorageMarker> {
    validate_format_config(config)?;
    let marker_path = metadata_marker_path(config);
    validate_format_target(config, &marker_path)?;

    std::fs::create_dir_all(&config.storage_dir).map_err(|err| {
        MetadataError::Internal(format!(
            "failed to create metadata.storage.dir {}: {err}",
            config.storage_dir.display()
        ))
    })?;

    let storage = Arc::new(RocksDBStorage::create_for_format(&config.storage_dir)?);
    let mount_table = Arc::new(MountTable::load_from_storage(storage.as_ref())?);
    let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table)));
    let raft_node =
        Arc::new(AppRaftNode::new(config.raft.node_id, Arc::clone(&storage), state_machine, &config.raft).await?);

    raft_node.initialize_single_node(config.rpc_addr.to_string()).await?;
    wait_for_single_node_leader(&raft_node, config.bootstrap.root_readiness.timeout_ms).await?;

    let group_name = config.authority.group_name.clone();
    ensure_root_mount_for_format(Arc::clone(&raft_node), Arc::clone(&mount_table), group_name.clone()).await?;
    wait_for_root_ready_with_inputs(RootReadyInputs {
        raft_node: Arc::clone(&raft_node),
        mount_table: Arc::clone(&mount_table),
        storage: Some(Arc::clone(&storage)),
        namespace_owner_group_name: group_name.clone(),
        readiness_gate: Arc::new(RootReadinessGate::new(None)),
        config: config.bootstrap.root_readiness.clone(),
        metrics: None,
        log_fields: RootReadinessLogFields {
            cluster_id: config.cluster_id.clone(),
            group_name: group_name.to_string(),
            node_id: config.raft.node_id,
            storage_dir: config.storage_dir.display().to_string(),
        },
    })
    .await?;
    verify_root(&storage, &mount_table)?;

    let marker = MetadataStorageMarker {
        cluster_id: config.cluster_id.clone(),
        group_name: config.authority.group_name.clone(),
        node_id: config.raft.node_id,
        storage_uuid: Uuid::new_v4().to_string(),
        format_version: FORMAT_VERSION,
        created_at_ms: now_ms(),
        software_version: env!("CARGO_PKG_VERSION").to_string(),
    };
    raft_node.shutdown().await?;
    write_marker(&marker_path, &marker)?;
    Ok(marker)
}

pub async fn prepare_metadata_start(config: &MetadataConfig) -> MetadataResult<()> {
    validate_format_config(config)?;
    let marker_path = metadata_marker_path(config);
    if !marker_path.exists() {
        return Err(MetadataError::InvalidArgument(format!(
            "metadata storage is unformatted at {}; run `metadata format --config <path>` before start",
            config.storage_dir.display()
        )));
    }

    let marker = read_marker(&marker_path)?;
    validate_marker(config, &marker)?;
    let storage = RocksDBStorage::open_existing_for_start(&config.storage_dir)?;
    let mount_table = MountTable::load_from_storage(&storage)?;
    verify_root(&storage, &mount_table)?;
    Ok(())
}

fn validate_format_config(config: &MetadataConfig) -> MetadataResult<()> {
    if config.cluster_id.trim().is_empty() {
        return Err(MetadataError::InvalidArgument(
            "vecton.cluster.id must not be empty".to_string(),
        ));
    }
    if config.raft.mode == RaftMode::Cluster {
        return Err(MetadataError::InvalidArgument(
            "cluster Raft mode is not implemented yet".to_string(),
        ));
    }
    if config.raft.node_id == 0 {
        return Err(MetadataError::InvalidArgument(
            "metadata.raft.node_id must be greater than zero".to_string(),
        ));
    }
    if config.storage_dir.as_os_str().is_empty() {
        return Err(MetadataError::InvalidArgument(
            "metadata.storage.dir must not be empty".to_string(),
        ));
    }
    Ok(())
}

fn validate_marker(config: &MetadataConfig, marker: &MetadataStorageMarker) -> MetadataResult<()> {
    if marker.format_version != FORMAT_VERSION {
        return Err(MetadataError::InvalidArgument(format!(
            "metadata marker mismatch: format_version={}, expected {}",
            marker.format_version, FORMAT_VERSION
        )));
    }
    if marker.cluster_id != config.cluster_id {
        return Err(marker_mismatch("cluster_id", &marker.cluster_id, &config.cluster_id));
    }
    if marker.group_name != config.authority.group_name {
        return Err(marker_mismatch(
            "group_name",
            marker.group_name.as_str(),
            config.authority.group_name.as_str(),
        ));
    }
    if marker.node_id != config.raft.node_id {
        return Err(marker_mismatch(
            "node_id",
            &marker.node_id.to_string(),
            &config.raft.node_id.to_string(),
        ));
    }
    Ok(())
}

fn validate_format_target(config: &MetadataConfig, marker_path: &std::path::Path) -> MetadataResult<()> {
    if marker_path.exists() {
        return Err(MetadataError::AlreadyExists(format!(
            "metadata storage is already formatted at {}; start normally or remove the directory only if you intend to reset local metadata",
            config.storage_dir.display()
        )));
    }

    if storage_dir_has_entries(&config.storage_dir)? {
        return Err(marker_missing_error(config));
    }
    Ok(())
}

fn storage_dir_has_entries(path: &std::path::Path) -> MetadataResult<bool> {
    if !path.exists() {
        return Ok(false);
    }
    if !path.is_dir() {
        return Err(MetadataError::InvalidArgument(format!(
            "metadata.storage.dir {} exists but is not a directory",
            path.display()
        )));
    }
    let mut entries = std::fs::read_dir(path).map_err(|err| {
        MetadataError::Internal(format!("failed to read metadata.storage.dir {}: {err}", path.display()))
    })?;
    Ok(entries
        .next()
        .transpose()
        .map_err(|err| {
            MetadataError::Internal(format!("failed to read metadata.storage.dir {}: {err}", path.display()))
        })?
        .is_some())
}

fn marker_missing_error(config: &MetadataConfig) -> MetadataError {
    MetadataError::InvalidArgument(format!(
        "metadata.storage.dir {} is non-empty but metadata marker missing at {}; refusing to attach existing files to a new metadata identity; clean the directory manually before formatting",
        config.storage_dir.display(),
        metadata_marker_path(config).display()
    ))
}

fn marker_mismatch(field: &str, actual: &str, expected: &str) -> MetadataError {
    MetadataError::InvalidArgument(format!(
        "metadata marker mismatch for {field}: marker={actual}, config={expected}"
    ))
}

fn read_marker(path: &std::path::Path) -> MetadataResult<MetadataStorageMarker> {
    let raw = std::fs::read_to_string(path)
        .map_err(|err| MetadataError::Internal(format!("failed to read metadata marker {}: {err}", path.display())))?;
    serde_json::from_str(&raw).map_err(|err| {
        MetadataError::InvalidArgument(format!(
            "metadata marker {} is malformed: {err}; old marker format unsupported, clean storage or use future migration command",
            path.display()
        ))
    })
}

fn write_marker(path: &std::path::Path, marker: &MetadataStorageMarker) -> MetadataResult<()> {
    let payload = serde_json::to_vec_pretty(marker)
        .map_err(|err| MetadataError::Internal(format!("failed to encode metadata marker: {err}")))?;
    std::fs::write(path, payload)
        .map_err(|err| MetadataError::Internal(format!("failed to write metadata marker {}: {err}", path.display())))?;
    Ok(())
}

fn verify_root(storage: &RocksDBStorage, mount_table: &MountTable) -> MetadataResult<()> {
    let root = mount_table
        .list_mounts()
        .into_iter()
        .find(|entry| entry.mount_prefix == ROOT_MOUNT_PREFIX)
        .ok_or_else(|| MetadataError::ServiceUnavailable("root mount missing after metadata format".to_string()))?;
    if root.root_inode_id != ROOT_INODE_ID
        || root.mount_kind != MountKind::Internal
        || root.ufs_uri.is_some()
        || root.data_io_policy != DataIoPolicy::Forbid
    {
        return Err(MetadataError::InvalidArgument(
            "root mount exists but violates root invariants".to_string(),
        ));
    }
    let inode = storage
        .get_inode(ROOT_INODE_ID)?
        .ok_or_else(|| MetadataError::ServiceUnavailable("root inode missing after metadata format".to_string()))?;
    if !inode.kind.is_dir() {
        return Err(MetadataError::InvalidArgument(
            "root inode exists but is not a directory".to_string(),
        ));
    }
    Ok(())
}

async fn wait_for_single_node_leader(raft_node: &AppRaftNode, timeout_ms: u64) -> MetadataResult<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
    while tokio::time::Instant::now() < deadline {
        if raft_node.is_leader() {
            return Ok(());
        }
        sleep(Duration::from_millis(20)).await;
    }
    Err(MetadataError::ServiceUnavailable(
        "single-node raft did not become leader before readiness timeout".to_string(),
    ))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
