// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! RocksDB schema, identity, and open policy.

use super::*;

impl RocksDBStorage {
    /// Create RocksDB state for `metadata format`.
    pub fn create_for_format<P: AsRef<Path>>(path: P) -> MetadataResult<Self> {
        Self::open_with_create_policy(path, true)
    }

    /// Open already formatted RocksDB state for `metadata start`.
    pub fn open_existing_for_start<P: AsRef<Path>>(path: P) -> MetadataResult<Self> {
        Self::open_with_create_policy(path, false)
    }

    fn open_with_create_policy<P: AsRef<Path>>(path: P, create_missing: bool) -> MetadataResult<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let generations = if create_missing {
            GenerationHandle::open_for_format(&path_buf, open_generation_db)?
        } else {
            GenerationHandle::open_for_start(&path_buf, open_generation_db)
                .map_err(|error| missing_rocksdb_state_error(&path_buf, &error.to_string()))?
        };
        let storage = Self { generations };
        Ok(storage)
    }

    pub(crate) fn pin_generation(&self) -> MetadataResult<PinnedGeneration<'_>> {
        self.generations.pin()
    }

    pub(crate) fn generation_write(&self) -> MetadataResult<GenerationWriteGuard<'_>> {
        self.generations.write()
    }

    pub(crate) fn create_staged_generation(&self) -> MetadataResult<StagedGeneration> {
        self.generations.create_staged(open_generation_db)
    }

    pub(crate) fn publish_staged_generation_with<B, A>(
        &self,
        staged: StagedGeneration,
        before_switch: B,
        after_switch: A,
    ) -> MetadataResult<()>
    where
        B: FnOnce(&DB, &DB) -> MetadataResult<()>,
        A: FnOnce(&DB) -> MetadataResult<()>,
    {
        self.generation_write()?.publish_staged_with(
            staged,
            open_generation_db,
            |old, staged| before_switch(old.db(), staged.db()),
            |new| after_switch(new.db()),
        )?;
        Ok(())
    }

    pub(crate) fn cleanup_retired_generations(&self) -> MetadataResult<()> {
        self.generations.cleanup_retired()
    }

    pub(crate) fn cleanup_unreferenced_generations(&self) -> MetadataResult<()> {
        self.generations.cleanup_unreferenced()
    }

    pub(crate) fn with_pinned_snapshot<T>(
        &self,
        operation: impl FnOnce(&DB, &rocksdb::Snapshot<'_>) -> MetadataResult<T>,
    ) -> MetadataResult<T> {
        let generation = self.pin_generation()?;
        let snapshot = generation.db().snapshot();
        operation(generation.db(), &snapshot)
    }

    /// Bind a pristine formatted database to one lifecycle marker identity.
    pub(crate) fn bind_storage_identity(&self, expected: &StorageIdentity) -> MetadataResult<()> {
        let generation = self.pin_generation()?;
        let db = generation.db();
        let meta = db
            .cf_handle(CF_META)
            .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;
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
        let generation = self.pin_generation()?;
        let db = generation.db();
        let meta = db
            .cf_handle(CF_META)
            .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;
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
    pub fn snapshot_dir(&self) -> std::path::PathBuf {
        self.generations.snapshot_dir()
    }
}

fn open_generation_db(path: &Path, create_missing: bool) -> MetadataResult<Arc<DB>> {
    let mut options = Options::default();
    options.create_if_missing(create_missing);
    options.create_missing_column_families(create_missing);
    let mut descriptors = cf_descriptors();
    let obsolete_column_families = if create_missing {
        Vec::new()
    } else {
        let names = DB::list_cf(&Options::default(), path).map_err(|error| {
            missing_rocksdb_state_error(path, &format!("RocksDB column-family discovery failed: {error}"))
        })?;
        let mut obsolete = Vec::new();
        for name in names {
            if name == "default" || is_current_column_family(&name) {
                continue;
            }
            descriptors.push(ColumnFamilyDescriptor::new(name.clone(), Options::default()));
            obsolete.push(name);
        }
        obsolete
    };
    let db = DB::open_cf_descriptors(&options, path, descriptors).map_err(|error| {
        if create_missing {
            MetadataError::Internal(format!(
                "failed to create RocksDB generation at {}: {error}",
                path.display()
            ))
        } else {
            missing_rocksdb_state_error(path, &format!("RocksDB open failed: {error}"))
        }
    })?;
    let meta = db
        .cf_handle(CF_META)
        .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;
    match db.get_cf(meta, ROCKSDB_SCHEMA_VERSION_KEY) {
        Ok(Some(raw)) => {
            let stored: u64 = decode_from_slice(&raw, standard())
                .map_err(|error| MetadataError::InvalidArgument(format!("invalid RocksDB schema version: {error}")))?
                .0;
            if stored != ROCKSDB_SCHEMA_VERSION {
                return Err(MetadataError::InvalidArgument(format!(
                    "unsupported RocksDB schema version {stored}; expected {}; reformat metadata storage",
                    ROCKSDB_SCHEMA_VERSION
                )));
            }
        }
        Ok(None) if create_missing && can_initialize_missing_schema(&db)? => {
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
    if !obsolete_column_families.is_empty() {
        return Err(MetadataError::InvalidArgument(format!(
            "obsolete RocksDB column families {:?}; reformat metadata storage",
            obsolete_column_families
        )));
    }
    Ok(Arc::new(db))
}

fn is_current_column_family(name: &str) -> bool {
    matches!(
        name,
        CF_MOUNTS | CF_WORKERS | CF_META | CF_RAFT_LOG | CF_RAFT_STATE | CF_RAFT_SNAPSHOT | CF_INODES | CF_DENTRIES
    )
}

fn can_initialize_missing_schema(db: &DB) -> MetadataResult<bool> {
    database_is_pristine(db, &[])
}

fn can_bind_storage_identity(db: &DB) -> MetadataResult<bool> {
    database_is_pristine(db, &[ROCKSDB_SCHEMA_VERSION_KEY])
}

fn database_is_pristine(db: &DB, allowed_meta_keys: &[&[u8]]) -> MetadataResult<bool> {
    use rocksdb::IteratorMode;

    for name in [
        CF_MOUNTS,
        CF_WORKERS,
        CF_RAFT_LOG,
        CF_RAFT_STATE,
        CF_RAFT_SNAPSHOT,
        CF_INODES,
        CF_DENTRIES,
    ] {
        let cf = db
            .cf_handle(name)
            .ok_or_else(|| MetadataError::Internal(format!("{name} CF not found")))?;
        if let Some(item) = db.iterator_cf(cf, IteratorMode::Start).next() {
            item.map_err(|error| MetadataError::Internal(format!("failed to inspect {name} CF: {error}")))?;
            return Ok(false);
        }
    }

    let meta = db
        .cf_handle(CF_META)
        .ok_or_else(|| MetadataError::Internal("Meta CF not found".to_string()))?;
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
    vec![
        ColumnFamilyDescriptor::new(CF_MOUNTS, Options::default()),
        ColumnFamilyDescriptor::new(CF_WORKERS, Options::default()),
        ColumnFamilyDescriptor::new(CF_META, Options::default()),
        ColumnFamilyDescriptor::new(CF_RAFT_LOG, Options::default()),
        ColumnFamilyDescriptor::new(CF_RAFT_STATE, Options::default()),
        ColumnFamilyDescriptor::new(CF_RAFT_SNAPSHOT, Options::default()),
        ColumnFamilyDescriptor::new(CF_INODES, Options::default()),
        ColumnFamilyDescriptor::new(CF_DENTRIES, Options::default()),
    ]
}

fn missing_rocksdb_state_error(path: &Path, detail: &str) -> MetadataError {
    MetadataError::InvalidArgument(format!(
        "metadata storage is formatted but RocksDB state is missing or corrupt at {}; {detail}; run `metadata format --config <path>` only on empty storage, or clean/reset manually",
        path.display()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    impl RocksDBStorage {
        pub(crate) fn with_pinned_db<T>(&self, operation: impl FnOnce(&DB) -> MetadataResult<T>) -> MetadataResult<T> {
            let generation = self.pin_generation()?;
            operation(generation.db())
        }
    }

    #[test]
    fn opening_existing_schema_v1_store_requires_reformat() {
        let dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(dir.path()).unwrap();
        storage
            .with_pinned_db(|db| {
                let meta = db.cf_handle(CF_META).unwrap();
                db.delete_cf(meta, ROCKSDB_SCHEMA_VERSION_KEY).unwrap();
                Ok(())
            })
            .unwrap();
        drop(storage);

        let error = match RocksDBStorage::open_existing_for_start(dir.path()) {
            Ok(_) => panic!("schema v1 store must not open"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("schema version is missing"));
        assert!(error.to_string().contains("reformat metadata storage"));
    }

    #[test]
    fn opening_previous_inode_schema_requires_reformat() {
        let dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(dir.path()).unwrap();
        drop(storage);

        let generation_path = dir.path().join("generations/gen-000001");
        let db = DB::open_cf_descriptors(&Options::default(), &generation_path, cf_descriptors()).unwrap();
        let meta = db.cf_handle(CF_META).unwrap();
        let previous = bincode::serde::encode_to_vec(7u64, bincode::config::standard()).unwrap();
        db.put_cf(meta, ROCKSDB_SCHEMA_VERSION_KEY, &previous).unwrap();
        drop(db);

        let error = match RocksDBStorage::open_existing_for_start(dir.path()) {
            Ok(_) => panic!("schema 7 store must not open after the inode format change"),
            Err(error) => error,
        };

        assert!(
            error
                .to_string()
                .contains("unsupported RocksDB schema version 7; expected 8"),
            "unexpected startup error: {error}"
        );
        assert!(
            error.to_string().contains("reformat metadata storage"),
            "unexpected startup error: {error}"
        );

        let db = DB::open_cf_descriptors(&Options::default(), generation_path, cf_descriptors()).unwrap();
        let meta = db.cf_handle(CF_META).unwrap();
        assert_eq!(
            db.get_cf(meta, ROCKSDB_SCHEMA_VERSION_KEY).unwrap().as_deref(),
            Some(previous.as_slice())
        );
    }

    #[test]
    fn format_resume_rejects_missing_schema_even_when_generation_is_pristine() {
        let dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(dir.path()).unwrap();
        storage
            .with_pinned_db(|db| {
                let meta = db.cf_handle(CF_META).unwrap();
                db.delete_cf(meta, ROCKSDB_SCHEMA_VERSION_KEY).unwrap();
                Ok(())
            })
            .unwrap();
        drop(storage);

        let error = match RocksDBStorage::create_for_format(dir.path()) {
            Ok(_) => panic!("schema-less generation must not resume"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("schema version is missing"));
    }

    #[test]
    fn format_resume_does_not_upgrade_missing_schema_with_authority_state() {
        let dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::create_for_format(dir.path()).unwrap();
        storage
            .put_inode(&Inode::new_dir(
                InodeId::new(1),
                beryl_types::FileAttrs::new(),
                MountId::new(1),
            ))
            .unwrap();
        storage
            .with_pinned_db(|db| {
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

    #[test]
    fn test_obsolete_cf_detection() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test_db");

        // Create a RocksDB with obsolete "files" CF.
        {
            let mut opts = Options::default();
            opts.create_if_missing(true);
            opts.create_missing_column_families(true);

            let cfs = vec![
                ColumnFamilyDescriptor::new("files", Options::default()),
                ColumnFamilyDescriptor::new("blocks", Options::default()),
            ];

            let db = DB::open_cf_descriptors(&opts, &db_path, cfs).unwrap();
            // Write something to files CF to ensure it exists
            let files_cf = db.cf_handle("files").unwrap();
            db.put_cf(files_cf, b"test_key", b"test_value").unwrap();
        }

        // Try to open with new code; obsolete CF layouts must fail fast.
        let result = RocksDBStorage::create_for_format(&db_path);
        assert!(result.is_err(), "Opening DB with obsolete 'files' CF should fail");
        match result {
            Err(e) => {
                let error_msg = format!("{}", e);
                assert!(
                    error_msg.contains("invalid CURRENT")
                        || error_msg.contains("obsolete column family")
                        || error_msg.contains("files"),
                    "Error message should mention obsolete column family 'files', got: {}",
                    error_msg
                );
            }
            Ok(_) => panic!("Expected error but got Ok"),
        }
    }
}
