// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Local metadata storage lifecycle.

use crate::config::{MetadataConfig, RaftMode};
use crate::ensure_root_mount_for_format;
use crate::error::{MetadataError, MetadataResult};
use crate::mount::{DataIoPolicy, MountKind, MountTable, ROOT_INODE_ID, ROOT_MOUNT_PREFIX};
use crate::raft::{AppRaftNode, AppRaftStateMachine, Command, DedupKey, RocksDBStorage};
use crate::readiness::{wait_for_root_ready_with_inputs, RootReadinessGate, RootReadinessLogFields, RootReadyInputs};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;
use types::{FileAttrs, GroupName, Inode};
use uuid::Uuid;

const METADATA_MARKER_FILE: &str = "metadata.marker.json";
const FORMAT_VERSION: u32 = 1;
const LOCAL_WRITABLE_MOUNT_PREFIX: &str = "/local";
const OLD_LOCAL_LAYOUT_ERROR: &str = "local metadata store is missing required /local mount; this store was formatted by an older layout. Re-run metadata format or remove the local metadata directory.";

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
    ensure_local_writable_mount_for_format(
        Arc::clone(&raft_node),
        Arc::clone(&storage),
        Arc::clone(&mount_table),
        group_name.clone(),
    )
    .await?;
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
    verify_local_writable_mount(&storage, &mount_table, &group_name)?;

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
    verify_local_writable_mount_for_start(&storage, &mount_table, &config.authority.group_name)?;
    Ok(())
}

fn validate_format_config(config: &MetadataConfig) -> MetadataResult<()> {
    if config.cluster_id.trim().is_empty() {
        return Err(MetadataError::InvalidArgument(
            "vecton.cluster.id must not be empty".to_string(),
        ));
    }
    if config.raft.mode == RaftMode::Cluster {
        // Cluster mode is rejected until metadata peer RPC semantics,
        // membership, and freshness fencing are implemented.
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

async fn ensure_local_writable_mount_for_format(
    raft_node: Arc<AppRaftNode>,
    storage: Arc<RocksDBStorage>,
    mount_table: Arc<MountTable>,
    group_name: GroupName,
) -> MetadataResult<()> {
    if mount_table
        .list_mounts()
        .into_iter()
        .any(|entry| entry.mount_prefix == LOCAL_WRITABLE_MOUNT_PREFIX)
    {
        return verify_local_writable_mount(&storage, &mount_table, &group_name);
    }

    let mount_id = mount_table.allocate_mount_id();
    let root_inode_id = storage.allocate_inode_id()?;
    let mut attrs = FileAttrs::new();
    attrs.update_timestamps(now_ms());
    attrs.nlink = 1;
    storage.put_inode(&Inode::new_dir(root_inode_id, attrs, mount_id))?;

    let command = Command::CreateMount {
        dedup: DedupKey::system(),
        mount_id,
        mount_prefix: LOCAL_WRITABLE_MOUNT_PREFIX.to_string(),
        mount_kind: MountKind::Internal,
        ufs_uri: None,
        data_io_policy: DataIoPolicy::Allow,
        namespace_owner_group_name: group_name.clone(),
        root_inode_id,
    };
    raft_node.propose(command).await?;
    verify_local_writable_mount(&storage, &mount_table, &group_name)
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

fn verify_local_writable_mount(
    storage: &RocksDBStorage,
    mount_table: &MountTable,
    group_name: &GroupName,
) -> MetadataResult<()> {
    verify_local_writable_mount_with_missing_error(storage, mount_table, group_name, || {
        MetadataError::ServiceUnavailable("local writable mount missing after metadata format".to_string())
    })
}

fn verify_local_writable_mount_for_start(
    storage: &RocksDBStorage,
    mount_table: &MountTable,
    group_name: &GroupName,
) -> MetadataResult<()> {
    verify_local_writable_mount_with_missing_error(storage, mount_table, group_name, || {
        MetadataError::InvalidArgument(OLD_LOCAL_LAYOUT_ERROR.to_string())
    })
}

fn verify_local_writable_mount_with_missing_error(
    storage: &RocksDBStorage,
    mount_table: &MountTable,
    group_name: &GroupName,
    missing_error: impl FnOnce() -> MetadataError,
) -> MetadataResult<()> {
    let mount = mount_table
        .list_mounts()
        .into_iter()
        .find(|entry| entry.mount_prefix == LOCAL_WRITABLE_MOUNT_PREFIX)
        .ok_or_else(missing_error)?;
    if mount.mount_kind != MountKind::Internal
        || mount.ufs_uri.is_some()
        || mount.data_io_policy != DataIoPolicy::Allow
        || mount.namespace_owner_group_name != *group_name
    {
        return Err(MetadataError::InvalidArgument(
            "local writable mount exists but violates internal/allow-data-io invariants".to_string(),
        ));
    }
    let inode = storage
        .get_inode(mount.root_inode_id)?
        .ok_or_else(|| MetadataError::ServiceUnavailable("local writable mount root inode missing".to_string()))?;
    if !inode.kind.is_dir() || inode.mount_id != mount.mount_id {
        return Err(MetadataError::InvalidArgument(
            "local writable mount root inode violates directory/mount invariants".to_string(),
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
