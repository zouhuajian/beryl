// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Local worker storage lifecycle.

use crate::config::WorkerConfig;
use crate::WorkerError;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use types::ids::WorkerId;
use uuid::Uuid;

use super::identity::{resolve_existing_worker_id, resolve_worker_id};

const WORKER_STORAGE_INFO_FILE: &str = "worker.storage.json";
const FORMAT_VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WorkerStorageInfo {
    pub cluster_id: String,
    pub worker_id: u64,
    pub storage_uuid: String,
    pub format_version: u32,
    pub created_at_ms: u64,
    pub software_version: String,
}

pub fn worker_storage_info_path(config: &WorkerConfig) -> PathBuf {
    config.storage_root.join(WORKER_STORAGE_INFO_FILE)
}

pub fn prepare_worker_start(config: &WorkerConfig) -> Result<WorkerId, WorkerError> {
    validate_start_config(config)?;
    let info_path = worker_storage_info_path(config);
    if info_path.exists() {
        let info = read_info(&info_path)?;
        let worker_id = validate_info(config, &info)?;
        init_group_dir(config)?;
        return Ok(worker_id);
    }

    if storage_root_has_entries(&config.storage_root)? {
        return Err(info_missing_error(config));
    }

    std::fs::create_dir_all(&config.storage_root).map_err(|err| {
        WorkerError::Internal(format!(
            "failed to create worker.storage.root {}: {err}",
            config.storage_root.display()
        ))
    })?;
    let worker_id = resolve_worker_id(config).map_err(|err| WorkerError::InvalidArgument(err.to_string()))?;
    init_group_dir(config)?;
    let info = WorkerStorageInfo {
        cluster_id: config.cluster_id.clone(),
        worker_id: worker_id.as_raw(),
        storage_uuid: Uuid::new_v4().to_string(),
        format_version: FORMAT_VERSION,
        created_at_ms: now_ms(),
        software_version: env!("CARGO_PKG_VERSION").to_string(),
    };
    write_info(&info_path, &info)?;
    Ok(worker_id)
}

fn validate_start_config(config: &WorkerConfig) -> Result<(), WorkerError> {
    if config.cluster_id.trim().is_empty() {
        return Err(WorkerError::InvalidArgument(
            "vecton.cluster.id must not be empty".to_string(),
        ));
    }
    if config.storage_root.as_os_str().is_empty() {
        return Err(WorkerError::InvalidArgument(
            "worker.storage.root must not be empty".to_string(),
        ));
    }
    Ok(())
}

fn validate_info(config: &WorkerConfig, info: &WorkerStorageInfo) -> Result<WorkerId, WorkerError> {
    if info.format_version != FORMAT_VERSION {
        return Err(WorkerError::InvalidArgument(format!(
            "worker storage info mismatch: format_version={}, expected {}",
            info.format_version, FORMAT_VERSION
        )));
    }
    if info.cluster_id != config.cluster_id {
        return Err(info_mismatch("cluster_id", &info.cluster_id, &config.cluster_id));
    }
    let worker_id = resolve_existing_worker_id(config).map_err(|err| WorkerError::InvalidArgument(err.to_string()))?;
    if info.worker_id != worker_id.as_raw() {
        return Err(info_mismatch(
            "worker_id",
            &info.worker_id.to_string(),
            &worker_id.as_raw().to_string(),
        ));
    }
    Ok(worker_id)
}

fn storage_root_has_entries(path: &Path) -> Result<bool, WorkerError> {
    if !path.exists() {
        return Ok(false);
    }
    if !path.is_dir() {
        return Err(WorkerError::InvalidArgument(format!(
            "worker.storage.root {} exists but is not a directory",
            path.display()
        )));
    }
    let mut entries = std::fs::read_dir(path).map_err(|err| {
        WorkerError::Internal(format!("failed to read worker.storage.root {}: {err}", path.display()))
    })?;
    Ok(entries
        .next()
        .transpose()
        .map_err(|err| WorkerError::Internal(format!("failed to read worker.storage.root {}: {err}", path.display())))?
        .is_some())
}

fn init_group_dir(config: &WorkerConfig) -> Result<(), WorkerError> {
    let group_dir = config.group_storage_root();
    for name in ["blocks", "tmp", "gc"] {
        std::fs::create_dir_all(group_dir.join(name)).map_err(|err| {
            WorkerError::Internal(format!(
                "failed to create worker block store directory {}: {err}",
                group_dir.join(name).display()
            ))
        })?;
    }
    Ok(())
}

fn info_missing_error(config: &WorkerConfig) -> WorkerError {
    WorkerError::InvalidArgument(format!(
        "worker.storage.root {} is non-empty but WorkerStorageInfo missing at {}; refusing to take over unknown local data",
        config.storage_root.display(),
        worker_storage_info_path(config).display()
    ))
}

fn info_mismatch(field: &str, actual: &str, expected: &str) -> WorkerError {
    WorkerError::InvalidArgument(format!(
        "worker storage info mismatch for {field}: info={actual}, config={expected}"
    ))
}

fn read_info(path: &Path) -> Result<WorkerStorageInfo, WorkerError> {
    let raw = std::fs::read_to_string(path).map_err(|err| {
        WorkerError::Internal(format!("failed to read worker storage info {}: {err}", path.display()))
    })?;
    serde_json::from_str(&raw).map_err(|err| {
        WorkerError::InvalidArgument(format!(
            "worker storage info {} is malformed: {err}; old worker storage info format unsupported, clean storage or use future migration command",
            path.display()
        ))
    })
}

fn write_info(path: &Path, info: &WorkerStorageInfo) -> Result<(), WorkerError> {
    let payload = serde_json::to_vec_pretty(info)
        .map_err(|err| WorkerError::Internal(format!("failed to encode worker storage info: {err}")))?;
    std::fs::write(path, payload)
        .map_err(|err| WorkerError::Internal(format!("failed to write worker storage info {}: {err}", path.display())))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
