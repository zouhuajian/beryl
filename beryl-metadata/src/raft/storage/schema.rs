// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! RocksDB schema, identity, and open policy.

use super::{
    decode_from_slice, durable_raft_write_options, encode_to_vec, standard, ColumnFamilyDescriptor, MetadataError,
    MetadataResult, Options, Path, RocksDBStorage, StorageIdentity, CF_DENTRIES, CF_DETACHED_ROOTS, CF_INODES, CF_META,
    CF_MOUNTS, CF_RAFT_LOG, CF_RAFT_SNAPSHOT, CF_RAFT_STATE, CF_WORKERS, CURRENT_CFS, DB, ROCKSDB_SCHEMA_VERSION,
    ROCKSDB_SCHEMA_VERSION_KEY, STORAGE_IDENTITY_KEY,
};
use rocksdb::{IteratorMode, Snapshot};
use std::fs::{self, File};

impl RocksDBStorage {
    /// Initialize a fixed database directory before publishing it for format recovery.
    pub fn create_for_format<P: AsRef<Path>>(path: P) -> MetadataResult<Self> {
        let root = path.as_ref();
        fs::create_dir_all(root).map_err(|error| directory_error("create storage root", root, error))?;
        let snapshot_dir = root.join("snapshots");
        fs::create_dir_all(&snapshot_dir)
            .map_err(|error| directory_error("create snapshot directory", &snapshot_dir, error))?;
        let database_path = root.join("db");
        if !database_path.exists() {
            // An unpublished database contains no authority; format can recreate it.
            let temporary_path = root.join("db.tmp");
            if temporary_path.exists() {
                fs::remove_dir_all(&temporary_path)
                    .map_err(|error| directory_error("remove incomplete database", &temporary_path, error))?;
            }
            fs::create_dir(&temporary_path)
                .map_err(|error| directory_error("create database directory", &temporary_path, error))?;
            drop(open_database(&temporary_path, true)?);
            sync_directory(&temporary_path)?;
            fs::rename(&temporary_path, &database_path)
                .map_err(|error| directory_error("publish database directory", &database_path, error))?;
            sync_directory(root)?;
        }
        Self::open_existing_for_start(root)
    }

    /// Open existing storage without creating or repairing its directories.
    pub fn open_existing_for_start<P: AsRef<Path>>(path: P) -> MetadataResult<Self> {
        let root = path.as_ref();
        let database_path = root.join("db");
        let metadata = fs::symlink_metadata(&database_path)
            .map_err(|error| missing_rocksdb_state_error(&database_path, &error.to_string()))?;
        if !metadata.file_type().is_dir() {
            return Err(missing_rocksdb_state_error(
                &database_path,
                "expected a database directory",
            ));
        }
        let snapshot_dir = root.join("snapshots");
        if !snapshot_dir.is_dir() {
            return Err(missing_rocksdb_state_error(
                &snapshot_dir,
                "snapshot directory is missing",
            ));
        }
        Ok(Self {
            db: open_database(&database_path, false)?,
            snapshot_dir,
        })
    }

    pub(crate) fn db(&self) -> &DB {
        &self.db
    }

    pub(crate) fn with_snapshot<T>(
        &self,
        operation: impl FnOnce(&DB, &Snapshot<'_>) -> MetadataResult<T>,
    ) -> MetadataResult<T> {
        let snapshot = self.db().snapshot();
        operation(self.db(), &snapshot)
    }

    /// Bind a pristine formatted database to one lifecycle marker identity.
    pub(crate) fn bind_storage_identity(&self, expected: &StorageIdentity) -> MetadataResult<()> {
        let db = self.db();
        let meta = RocksDBStorage::cf(db, CF_META)?;
        match db.get_cf(meta, STORAGE_IDENTITY_KEY) {
            Ok(Some(raw)) => {
                let actual: StorageIdentity = decode_from_slice(&raw, standard())
                    .map_err(|error| MetadataError::InvalidArgument(format!("invalid storage identity: {error}")))?
                    .0;
                storage_identity_matches(&actual, expected)
            }
            Ok(None) => {
                if !can_bind_storage_identity(db)? {
                    return Err(MetadataError::InvalidArgument(
                        "storage identity is missing from non-pristine metadata state; reformat metadata storage"
                            .to_string(),
                    ));
                }
                let encoded = encode_to_vec(expected, standard())
                    .map_err(|error| MetadataError::Internal(format!("failed to encode storage identity: {error}")))?;
                db.put_cf_opt(meta, STORAGE_IDENTITY_KEY, encoded, &durable_raft_write_options())
                    .map_err(|error| MetadataError::Internal(format!("failed to persist storage identity: {error}")))
            }
            Err(error) => Err(MetadataError::Internal(format!(
                "failed to read storage identity: {error}"
            ))),
        }
    }

    /// Verify that an existing database belongs to the supplied lifecycle marker.
    pub(crate) fn validate_storage_identity(&self, expected: &StorageIdentity) -> MetadataResult<()> {
        let actual = self.storage_identity()?;
        storage_identity_matches(&actual, expected)
    }

    pub(crate) fn storage_identity(&self) -> MetadataResult<StorageIdentity> {
        let db = self.db();
        let meta = RocksDBStorage::cf(db, CF_META)?;
        let raw = db
            .get_cf(meta, STORAGE_IDENTITY_KEY)
            .map_err(|error| MetadataError::Internal(format!("failed to read storage identity: {error}")))?
            .ok_or_else(|| {
                MetadataError::InvalidArgument("storage identity is missing; reformat metadata storage".to_string())
            })?;
        let decoded: (StorageIdentity, usize) = decode_from_slice(&raw, standard())
            .map_err(|error| MetadataError::InvalidArgument(format!("invalid storage identity: {error}")))?;
        Ok(decoded.0)
    }

    /// Directory where snapshot files are materialized.
    pub fn snapshot_dir(&self) -> &Path {
        &self.snapshot_dir
    }
}

fn open_database(path: &Path, create_missing: bool) -> MetadataResult<DB> {
    let mut options = Options::default();
    options.create_if_missing(create_missing);
    options.create_missing_column_families(create_missing);
    let (descriptors, obsolete_column_families) = if create_missing {
        (cf_descriptors(), Vec::new())
    } else {
        let names = DB::list_cf(&Options::default(), path).map_err(|error| {
            missing_rocksdb_state_error(path, &format!("RocksDB column-family discovery failed: {error}"))
        })?;
        let descriptors = names
            .iter()
            .filter(|name| name.as_str() != "default")
            .map(|name| ColumnFamilyDescriptor::new(name.clone(), Options::default()))
            .collect();
        let mut obsolete = Vec::new();
        for name in &names {
            if name == "default" || is_current_column_family(name) {
                continue;
            }
            obsolete.push(name.clone());
        }
        (descriptors, obsolete)
    };
    let db = DB::open_cf_descriptors(&options, path, descriptors).map_err(|error| {
        if create_missing {
            MetadataError::Internal(format!(
                "failed to create RocksDB database at {}: {error}",
                path.display()
            ))
        } else {
            missing_rocksdb_state_error(path, &format!("RocksDB open failed: {error}"))
        }
    })?;
    let meta = db.cf_handle(CF_META).ok_or_else(|| {
        MetadataError::InvalidArgument(format!(
            "RocksDB column family {CF_META} is missing; reformat metadata storage"
        ))
    })?;
    match db.get_cf(meta, ROCKSDB_SCHEMA_VERSION_KEY) {
        Ok(Some(raw)) => {
            let (stored, bytes_read): (u64, usize) = decode_from_slice(&raw, standard()).map_err(|error| {
                MetadataError::InvalidArgument(format!(
                    "invalid RocksDB schema version: {error}; reformat metadata storage"
                ))
            })?;
            if bytes_read != raw.len() {
                return Err(MetadataError::InvalidArgument(
                    "invalid RocksDB schema version: trailing bytes; reformat metadata storage".to_string(),
                ));
            }
            if stored != ROCKSDB_SCHEMA_VERSION {
                return Err(MetadataError::InvalidArgument(format!(
                    "unsupported RocksDB schema version {stored}; expected {}; reformat metadata storage",
                    ROCKSDB_SCHEMA_VERSION
                )));
            }
        }
        Ok(None) if create_missing => {
            let encoded = encode_to_vec(ROCKSDB_SCHEMA_VERSION, standard()).map_err(|error| {
                MetadataError::Internal(format!("failed to encode RocksDB schema version: {error}"))
            })?;
            db.put_cf_opt(meta, ROCKSDB_SCHEMA_VERSION_KEY, encoded, &durable_raft_write_options())
                .map_err(|error| {
                    MetadataError::Internal(format!("failed to persist RocksDB schema version: {error}"))
                })?;
        }
        Ok(None) => {
            return Err(MetadataError::InvalidArgument(format!(
                "RocksDB schema version is missing; expected {}; reformat metadata storage",
                ROCKSDB_SCHEMA_VERSION
            )))
        }
        Err(error) => {
            return Err(MetadataError::Internal(format!(
                "failed to read RocksDB schema version: {error}"
            )))
        }
    }
    let missing_column_families = CURRENT_CFS
        .iter()
        .copied()
        .filter(|name| db.cf_handle(name).is_none())
        .collect::<Vec<_>>();
    if !missing_column_families.is_empty() {
        return Err(MetadataError::InvalidArgument(format!(
            "missing RocksDB column families {:?}; reformat metadata storage",
            missing_column_families
        )));
    }
    if !obsolete_column_families.is_empty() {
        return Err(MetadataError::InvalidArgument(format!(
            "obsolete RocksDB column families {:?}; reformat metadata storage",
            obsolete_column_families
        )));
    }
    validate_detached_root_records(&db)?;
    Ok(db)
}

fn is_current_column_family(name: &str) -> bool {
    CURRENT_CFS.contains(&name)
}

fn can_bind_storage_identity(db: &DB) -> MetadataResult<bool> {
    database_is_pristine(db, &[ROCKSDB_SCHEMA_VERSION_KEY])
}

fn database_is_pristine(db: &DB, allowed_meta_keys: &[&[u8]]) -> MetadataResult<bool> {
    for name in [
        CF_MOUNTS,
        CF_WORKERS,
        CF_RAFT_LOG,
        CF_RAFT_STATE,
        CF_RAFT_SNAPSHOT,
        CF_INODES,
        CF_DENTRIES,
        CF_DETACHED_ROOTS,
    ] {
        let cf = RocksDBStorage::cf(db, name)?;
        if let Some(item) = db.iterator_cf(cf, IteratorMode::Start).next() {
            item.map_err(|error| MetadataError::Internal(format!("failed to inspect {name} CF: {error}")))?;
            return Ok(false);
        }
    }

    let meta = RocksDBStorage::cf(db, CF_META)?;
    for item in db.iterator_cf(meta, IteratorMode::Start) {
        let (key, _) = item.map_err(|error| MetadataError::Internal(format!("failed to inspect meta CF: {error}")))?;
        if !allowed_meta_keys.iter().any(|allowed| *allowed == key.as_ref()) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn storage_identity_matches(actual: &StorageIdentity, expected: &StorageIdentity) -> MetadataResult<()> {
    if actual == expected {
        return Ok(());
    }
    Err(MetadataError::InvalidArgument(format!(
        "storage identity mismatch: RocksDB storage_uuid={}, marker storage_uuid={}; refusing to attach marker to different metadata state",
        actual.storage_uuid, expected.storage_uuid
    )))
}

pub(super) fn cf_descriptors() -> Vec<ColumnFamilyDescriptor> {
    CURRENT_CFS
        .iter()
        .map(|name| ColumnFamilyDescriptor::new(*name, Options::default()))
        .collect()
}

/// Validate every durable marker before serving metadata authority.
pub(super) fn validate_detached_root_records(db: &DB) -> MetadataResult<()> {
    let cf = RocksDBStorage::cf(db, CF_DETACHED_ROOTS)?;
    for item in db.iterator_cf(cf, IteratorMode::Start) {
        let (key, value) =
            item.map_err(|error| MetadataError::Internal(format!("failed to inspect detached roots: {error}")))?;
        let decoded = RocksDBStorage::decode_detached_root_key(&key)
            .and_then(|inode_id| RocksDBStorage::decode_detached_root(inode_id, &value));
        decoded.map_err(|error| MetadataError::InvalidArgument(format!("{error}; reformat metadata storage")))?;
    }
    Ok(())
}

fn missing_rocksdb_state_error(path: &Path, detail: &str) -> MetadataError {
    MetadataError::InvalidArgument(format!(
        "metadata storage is formatted but RocksDB state is missing or corrupt at {}; {detail}; run `beryl --conf-dir <dir> format metadata` only on empty storage, or clean/reset manually",
        path.display()
    ))
}

fn sync_directory(path: &Path) -> MetadataResult<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| directory_error("sync directory", path, error))
}

fn directory_error(operation: &str, path: &Path, error: std::io::Error) -> MetadataError {
    MetadataError::Internal(format!("{operation} {}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inode::Inode;
    use crate::inode::InodeAttrs;

    use beryl_types::{InodeId, MountId};
    use tempfile::TempDir;

    impl RocksDBStorage {
        pub(crate) fn with_db<T>(&self, operation: impl FnOnce(&DB) -> MetadataResult<T>) -> MetadataResult<T> {
            operation(self.db())
        }
    }

    #[test]
    fn format_recovers_unpublished_database_and_preserves_published_authority() {
        let dir = TempDir::new().unwrap();
        let temporary_path = dir.path().join("db.tmp");
        fs::create_dir(&temporary_path).unwrap();
        fs::write(temporary_path.join("incomplete"), b"interrupted initialization").unwrap();
        assert!(RocksDBStorage::open_existing_for_start(dir.path()).is_err());
        assert!(temporary_path.join("incomplete").exists());

        let inode = Inode::new_dir(InodeId::new(1), InodeAttrs::new(), MountId::new(1));
        let storage = RocksDBStorage::create_for_format(dir.path()).unwrap();
        storage.put_inode(&inode).unwrap();
        drop(storage);

        let storage = RocksDBStorage::create_for_format(dir.path()).unwrap();
        assert_eq!(storage.get_inode(inode.inode_id).unwrap(), Some(inode.clone()));
        drop(storage);
        let storage = RocksDBStorage::open_existing_for_start(dir.path()).unwrap();
        assert_eq!(storage.get_inode(inode.inode_id).unwrap(), Some(inode));
    }

    #[test]
    fn opening_non_current_schema_versions_requires_reformat_without_rewriting_them() {
        for unsupported_version in [0, ROCKSDB_SCHEMA_VERSION - 1, ROCKSDB_SCHEMA_VERSION + 1, u64::MAX] {
            let dir = TempDir::new().unwrap();
            let storage = RocksDBStorage::create_for_format(dir.path()).unwrap();
            drop(storage);

            let database_path = dir.path().join("db");
            let db = DB::open_cf_descriptors(&Options::default(), &database_path, cf_descriptors()).unwrap();
            let meta = db.cf_handle(CF_META).unwrap();
            let unsupported = bincode::serde::encode_to_vec(unsupported_version, bincode::config::standard()).unwrap();
            db.put_cf(meta, ROCKSDB_SCHEMA_VERSION_KEY, &unsupported).unwrap();
            drop(db);

            let error = match RocksDBStorage::open_existing_for_start(dir.path()) {
                Ok(_) => panic!("non-current schema store must not open"),
                Err(error) => error,
            };

            assert!(
                error.to_string().contains(&format!(
                    "unsupported RocksDB schema version {unsupported_version}; expected {ROCKSDB_SCHEMA_VERSION}"
                )),
                "unexpected startup error: {error}"
            );
            assert!(
                error.to_string().contains("reformat metadata storage"),
                "unexpected startup error: {error}"
            );

            let db = DB::open_cf_descriptors(&Options::default(), database_path, cf_descriptors()).unwrap();
            let meta = db.cf_handle(CF_META).unwrap();
            assert_eq!(
                db.get_cf(meta, ROCKSDB_SCHEMA_VERSION_KEY).unwrap().as_deref(),
                Some(unsupported.as_slice())
            );
        }
    }

    #[test]
    fn opening_malformed_detached_root_authority_requires_reformat() {
        let dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(dir.path()).unwrap();
        storage
            .with_db(|db| {
                let detached_roots = db.cf_handle(CF_DETACHED_ROOTS).unwrap();
                db.put_cf(detached_roots, b"short", b"invalid")
                    .map_err(|error| MetadataError::Internal(error.to_string()))
            })
            .unwrap();
        drop(storage);

        let error = match RocksDBStorage::open_existing_for_start(dir.path()) {
            Ok(_) => panic!("malformed detached-root authority must not open"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("Invalid detached-root key length"));
        assert!(error.to_string().contains("reformat metadata storage"));
    }

    #[test]
    fn format_resume_does_not_upgrade_missing_schema_with_authority_state() {
        let dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(dir.path()).unwrap();
        storage
            .put_inode(&Inode::new_dir(InodeId::new(1), InodeAttrs::new(), MountId::new(1)))
            .unwrap();
        storage
            .with_db(|db| {
                let meta = db.cf_handle(CF_META).unwrap();
                db.delete_cf(meta, ROCKSDB_SCHEMA_VERSION_KEY).unwrap();
                Ok(())
            })
            .unwrap();
        drop(storage);

        let error = match RocksDBStorage::create_for_format(dir.path()) {
            Ok(_) => panic!("non-empty schema-less store must not be upgraded"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("schema version is missing"));
    }
}
