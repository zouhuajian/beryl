// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Stable worker identity resolution.

use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::Path;

use beryl_types::ids::WorkerId;
use uuid::Uuid;

use crate::config::WorkerConfig;
use crate::control::RegistrationError;

/// Resolve the stable WorkerId from the persisted local identity file.
pub(crate) fn resolve_worker_id(config: &WorkerConfig) -> Result<WorkerId, RegistrationError> {
    load_or_create_identity(&config.identity_path)
}

pub(super) fn resolve_existing_worker_id(config: &WorkerConfig) -> Result<WorkerId, RegistrationError> {
    load_identity(&config.identity_path)
}

fn load_identity(path: &Path) -> Result<WorkerId, RegistrationError> {
    read_identity(path).map_err(|error| match error {
        IdentityFileError::Io(error) if error.kind() == ErrorKind::NotFound => {
            RegistrationError::InvalidConfig(format!(
                "worker.identity.path {} is missing; worker start cannot recreate identity for existing WorkerStorageInfo",
                path.display()
            ))
        }
        IdentityFileError::Io(error) => RegistrationError::InvalidConfig(format!(
            "failed to read worker.identity.path {}: {error}",
            path.display()
        )),
        IdentityFileError::Malformed(message) => RegistrationError::InvalidConfig(message),
    })
}

fn load_or_create_identity(path: &Path) -> Result<WorkerId, RegistrationError> {
    match read_identity(path) {
        Ok(worker_id) => return Ok(worker_id),
        Err(IdentityFileError::Io(error)) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(RegistrationError::InvalidConfig(error.to_string())),
    }

    if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
        fs::create_dir_all(parent).map_err(|error| {
            RegistrationError::InvalidConfig(format!(
                "failed to create worker.identity.path parent {}: {error}",
                parent.display()
            ))
        })?;
    }

    let uuid = Uuid::new_v4();
    let payload = format!("{uuid}\n");
    let create_result = OpenOptions::new().write(true).create_new(true).open(path);
    let mut file = match create_result {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::AlreadyExists => return load_or_create_identity(path),
        Err(error) => {
            return Err(RegistrationError::InvalidConfig(format!(
                "failed to create worker.identity.path {}: {error}",
                path.display()
            )))
        }
    };
    file.write_all(payload.as_bytes()).map_err(|error| {
        RegistrationError::InvalidConfig(format!(
            "failed to write worker.identity.path {}: {error}",
            path.display()
        ))
    })?;
    file.sync_all().map_err(|error| {
        RegistrationError::InvalidConfig(format!(
            "failed to fsync worker.identity.path {}: {error}",
            path.display()
        ))
    })?;
    sync_parent(path)?;

    Ok(worker_id_from_uuid(uuid))
}

fn read_identity(path: &Path) -> Result<WorkerId, IdentityFileError> {
    let raw = fs::read_to_string(path).map_err(IdentityFileError::Io)?;
    let value = raw.trim();
    if value.is_empty() {
        return Err(IdentityFileError::Malformed(format!(
            "worker.identity.path {} is empty",
            path.display()
        )));
    }
    let uuid = Uuid::parse_str(value).map_err(|error| {
        IdentityFileError::Malformed(format!(
            "worker.identity.path {} must contain a UUID: {error}",
            path.display()
        ))
    })?;
    Ok(worker_id_from_uuid(uuid))
}

fn worker_id_from_uuid(uuid: Uuid) -> WorkerId {
    let raw = uuid.as_u128();
    let folded = ((raw >> 64) as u64) ^ raw as u64;
    WorkerId::new(folded.max(1))
}

fn sync_parent(path: &Path) -> Result<(), RegistrationError> {
    let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) else {
        return Ok(());
    };
    let parent_file = OpenOptions::new().read(true).open(parent).map_err(|error| {
        RegistrationError::InvalidConfig(format!(
            "failed to open worker.identity.path parent {} for fsync: {error}",
            parent.display()
        ))
    })?;
    parent_file.sync_all().map_err(|error| {
        RegistrationError::InvalidConfig(format!(
            "failed to fsync worker.identity.path parent {}: {error}",
            parent.display()
        ))
    })
}

#[derive(Debug)]
enum IdentityFileError {
    Io(std::io::Error),
    Malformed(String),
}

impl std::fmt::Display for IdentityFileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IdentityFileError::Io(error) => write!(f, "{error}"),
            IdentityFileError::Malformed(message) => write!(f, "{message}"),
        }
    }
}
