// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use crate::error::{MetadataError, MetadataResult};
use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use rocksdb::DB;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

const CURRENT_FILE: &str = "CURRENT";
const CURRENT_TMP_FILE: &str = "CURRENT.tmp";
const GENERATIONS_DIR: &str = "generations";
const SNAPSHOTS_DIR: &str = "snapshots";
const GENERATION_MANIFEST_FILE: &str = "generation.json";
const GENERATION_MANIFEST_VERSION: u16 = 1;
const INITIAL_GENERATION: &str = "gen-000001";

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GenerationManifest {
    version: u16,
    generation: String,
}

#[derive(Debug)]
pub(crate) struct Generation {
    name: String,
    path: PathBuf,
    db: Arc<DB>,
}

impl Generation {
    pub(crate) fn db(&self) -> &DB {
        self.db.as_ref()
    }
}

struct RetiredGeneration {
    generation: Weak<Generation>,
    path: PathBuf,
}

pub(crate) struct GenerationHandle {
    root: PathBuf,
    gate: RwLock<()>,
    active: RwLock<Arc<Generation>>,
    retired: Mutex<Vec<RetiredGeneration>>,
    poisoned: AtomicBool,
}

impl std::fmt::Debug for GenerationHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GenerationHandle")
            .field("root", &self.root)
            .field("active", &self.active.read().name)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub(crate) struct PinnedGeneration<'a> {
    _gate: RwLockReadGuard<'a, ()>,
    generation: Arc<Generation>,
}

impl PinnedGeneration<'_> {
    pub(crate) fn db(&self) -> &DB {
        self.generation.db()
    }
}

pub(crate) struct StagedGeneration {
    name: String,
    temporary_path: PathBuf,
    final_path: PathBuf,
    db: Option<Arc<DB>>,
    published: bool,
}

impl StagedGeneration {
    pub(crate) fn db(&self) -> &DB {
        self.db.as_deref().expect("staged generation database is present")
    }

    pub(crate) fn sync(&self) -> MetadataResult<()> {
        self.db()
            .flush_wal(true)
            .map_err(|error| MetadataError::Internal(format!("sync staged generation WAL: {error}")))?;
        self.db()
            .flush()
            .map_err(|error| MetadataError::Internal(format!("flush staged generation: {error}")))?;
        sync_directory(&self.temporary_path)
    }
}

pub(crate) struct GenerationWriteGuard<'a> {
    handle: &'a GenerationHandle,
    _gate: RwLockWriteGuard<'a, ()>,
}

impl GenerationHandle {
    pub(crate) fn open_for_format<F>(root: &Path, open_db: F) -> MetadataResult<Self>
    where
        F: Fn(&Path, bool) -> MetadataResult<Arc<DB>> + Copy,
    {
        fs::create_dir_all(root).map_err(|error| io_error("create metadata storage root", root, error))?;
        let generations = root.join(GENERATIONS_DIR);
        let snapshots = root.join(SNAPSHOTS_DIR);
        fs::create_dir_all(&generations)
            .map_err(|error| io_error("create generation directory", &generations, error))?;
        fs::create_dir_all(&snapshots).map_err(|error| io_error("create snapshot directory", &snapshots, error))?;

        let current_path = root.join(CURRENT_FILE);
        if current_path.exists() {
            return Self::open_current(root, open_db);
        }

        let initial_path = generations.join(INITIAL_GENERATION);
        if initial_path.exists() {
            validate_generation(&initial_path, INITIAL_GENERATION)?;
            let db = open_db(&initial_path, false)?;
            publish_current(root, INITIAL_GENERATION)?;
            return Ok(Self::new(root, INITIAL_GENERATION, initial_path, db));
        }

        let temporary_path = generations.join(format!("{INITIAL_GENERATION}.tmp"));
        if temporary_path.exists() {
            fs::remove_dir_all(&temporary_path)
                .map_err(|error| io_error("remove stale initial generation", &temporary_path, error))?;
        }
        fs::create_dir(&temporary_path)
            .map_err(|error| io_error("create initial generation", &temporary_path, error))?;
        let db = open_db(&temporary_path, true)?;
        drop(db);
        write_generation_manifest(&temporary_path, INITIAL_GENERATION)?;
        sync_directory(&temporary_path)?;
        fs::rename(&temporary_path, &initial_path)
            .map_err(|error| io_error("publish initial generation", &initial_path, error))?;
        sync_directory(&generations)?;
        publish_current(root, INITIAL_GENERATION)?;
        let db = open_db(&initial_path, false)?;
        Ok(Self::new(root, INITIAL_GENERATION, initial_path, db))
    }

    pub(crate) fn open_for_start<F>(root: &Path, open_db: F) -> MetadataResult<Self>
    where
        F: Fn(&Path, bool) -> MetadataResult<Arc<DB>> + Copy,
    {
        Self::open_current(root, open_db)
    }

    fn open_current<F>(root: &Path, open_db: F) -> MetadataResult<Self>
    where
        F: Fn(&Path, bool) -> MetadataResult<Arc<DB>> + Copy,
    {
        let name = read_current(root)?;
        let snapshots = root.join(SNAPSHOTS_DIR);
        if !snapshots.is_dir() {
            return Err(MetadataError::InvalidArgument(format!(
                "metadata snapshot directory is missing at {}",
                snapshots.display()
            )));
        }
        let path = root.join(GENERATIONS_DIR).join(&name);
        validate_generation(&path, &name)?;
        let db = open_db(&path, false)?;
        Ok(Self::new(root, &name, path, db))
    }

    fn new(root: &Path, name: &str, path: PathBuf, db: Arc<DB>) -> Self {
        if let Ok(generation) = parse_generation_name(name) {
            crate::observe::record_raft_active_generation(generation);
        }
        Self {
            root: root.to_path_buf(),
            gate: RwLock::new(()),
            active: RwLock::new(Arc::new(Generation {
                name: name.to_string(),
                path,
                db,
            })),
            retired: Mutex::new(Vec::new()),
            poisoned: AtomicBool::new(false),
        }
    }

    pub(crate) fn snapshot_dir(&self) -> PathBuf {
        self.root.join(SNAPSHOTS_DIR)
    }

    pub(crate) fn pin(&self) -> MetadataResult<PinnedGeneration<'_>> {
        self.ensure_healthy()?;
        let gate = self.gate.read_recursive();
        self.ensure_healthy()?;
        let generation = Arc::clone(&self.active.read());
        Ok(PinnedGeneration {
            _gate: gate,
            generation,
        })
    }

    pub(crate) fn write(&self) -> MetadataResult<GenerationWriteGuard<'_>> {
        self.ensure_healthy()?;
        let gate = self.gate.write();
        self.ensure_healthy()?;
        Ok(GenerationWriteGuard {
            handle: self,
            _gate: gate,
        })
    }

    pub(crate) fn create_staged<F>(&self, open_db: F) -> MetadataResult<StagedGeneration>
    where
        F: Fn(&Path, bool) -> MetadataResult<Arc<DB>> + Copy,
    {
        self.ensure_healthy()?;
        let _gate = self.gate.read_recursive();
        self.ensure_healthy()?;
        let next = self.next_generation_number()?;
        let name = format!("gen-{next:06}");
        let generations = self.root.join(GENERATIONS_DIR);
        let temporary_path = generations.join(format!("{name}.tmp"));
        let final_path = generations.join(&name);
        if temporary_path.exists() || final_path.exists() {
            return Err(MetadataError::Internal(format!(
                "generation path already exists for {name}"
            )));
        }
        fs::create_dir(&temporary_path)
            .map_err(|error| io_error("create staged generation", &temporary_path, error))?;
        let db = match open_db(&temporary_path, true) {
            Ok(db) => db,
            Err(error) => {
                let _ = fs::remove_dir_all(&temporary_path);
                return Err(error);
            }
        };
        Ok(StagedGeneration {
            name,
            temporary_path,
            final_path,
            db: Some(db),
            published: false,
        })
    }

    pub(crate) fn cleanup_retired(&self) -> MetadataResult<()> {
        let _gate = self.pin()?;
        let mut retired = self.retired.lock();
        let mut retained = Vec::new();
        for entry in retired.drain(..) {
            if entry.generation.upgrade().is_some() {
                retained.push(entry);
            } else if entry.path.exists() {
                fs::remove_dir_all(&entry.path)
                    .map_err(|error| io_error("remove retired generation", &entry.path, error))?;
                crate::observe::record_raft_storage_cleanup("retired_generation", 1);
            }
        }
        *retired = retained;
        Ok(())
    }

    pub(crate) fn cleanup_unreferenced(&self) -> MetadataResult<()> {
        let _generation = self.pin()?;
        self.cleanup_unreferenced_on_start()
    }

    fn ensure_healthy(&self) -> MetadataResult<()> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err(MetadataError::Internal(
                "metadata generation handle is poisoned; restart is required".to_string(),
            ));
        }
        Ok(())
    }

    fn next_generation_number(&self) -> MetadataResult<u64> {
        let mut maximum = 0u64;
        let generations = self.root.join(GENERATIONS_DIR);
        for entry in fs::read_dir(&generations).map_err(|error| io_error("list generations", &generations, error))? {
            let entry = entry.map_err(|error| io_error("read generation entry", &generations, error))?;
            let raw = entry.file_name();
            let raw = raw.to_string_lossy();
            let name = raw.strip_suffix(".tmp").unwrap_or(&raw);
            if let Ok(number) = parse_generation_name(name) {
                maximum = maximum.max(number);
            }
        }
        maximum
            .checked_add(1)
            .filter(|value| *value <= 999_999)
            .ok_or_else(|| MetadataError::Internal("generation number exhausted".to_string()))
    }

    fn cleanup_unreferenced_on_start(&self) -> MetadataResult<()> {
        let active = self.active.read().name.clone();
        let generations = self.root.join(GENERATIONS_DIR);
        for entry in fs::read_dir(&generations).map_err(|error| io_error("list generations", &generations, error))? {
            let entry = entry.map_err(|error| io_error("read generation entry", &generations, error))?;
            let path = entry.path();
            let raw = entry.file_name();
            let raw = raw.to_string_lossy();
            if raw.ends_with(".tmp") {
                let name = raw.trim_end_matches(".tmp");
                parse_generation_name(name)?;
                fs::remove_dir_all(&path).map_err(|error| io_error("remove stale generation", &path, error))?;
                crate::observe::record_raft_storage_cleanup("stale_generation", 1);
                continue;
            }
            parse_generation_name(&raw)?;
            if raw != active {
                validate_generation(&path, &raw)?;
                fs::remove_dir_all(&path).map_err(|error| io_error("remove unreferenced generation", &path, error))?;
                crate::observe::record_raft_storage_cleanup("unreferenced_generation", 1);
            }
        }
        Ok(())
    }
}

impl GenerationWriteGuard<'_> {
    pub(crate) fn active(&self) -> Arc<Generation> {
        Arc::clone(&self.handle.active.read())
    }

    pub(crate) fn publish_staged_with<F, B, A>(
        &mut self,
        mut staged: StagedGeneration,
        open_db: F,
        before_switch: B,
        after_switch: A,
    ) -> MetadataResult<()>
    where
        F: Fn(&Path, bool) -> MetadataResult<Arc<DB>> + Copy,
        B: FnOnce(&Generation, &StagedGeneration) -> MetadataResult<()>,
        A: FnOnce(&Generation) -> MetadataResult<()>,
    {
        let previous = self.active();
        before_switch(previous.as_ref(), &staged)?;
        staged.sync()?;
        let name = staged.name.clone();
        let temporary_path = staged.temporary_path.clone();
        let final_path = staged.final_path.clone();
        let db = staged
            .db
            .take()
            .expect("staged generation database is present during publication");
        let db = Arc::try_unwrap(db).map_err(|_| {
            MetadataError::Internal(format!("staged generation {name} still has active database references"))
        })?;
        drop(db);
        write_generation_manifest(&temporary_path, &name)?;
        sync_directory(&temporary_path)?;
        fs::rename(&temporary_path, &final_path)
            .map_err(|error| io_error("publish staged generation", &final_path, error))?;
        sync_directory(&self.handle.root.join(GENERATIONS_DIR))?;
        let db = open_db(&final_path, false)?;
        if let Err(error) = publish_current(&self.handle.root, &name) {
            self.handle.poisoned.store(true, Ordering::Release);
            return Err(error);
        }
        staged.published = true;

        let next = Arc::new(Generation {
            name,
            path: final_path,
            db,
        });
        let previous = std::mem::replace(&mut *self.handle.active.write(), Arc::clone(&next));
        self.handle.retired.lock().push(RetiredGeneration {
            generation: Arc::downgrade(&previous),
            path: previous.path.clone(),
        });
        drop(previous);
        if let Err(error) = after_switch(next.as_ref()) {
            self.handle.poisoned.store(true, Ordering::Release);
            return Err(error);
        }
        if let Ok(generation) = parse_generation_name(&next.name) {
            crate::observe::record_raft_active_generation(generation);
        }
        Ok(())
    }
}

impl Drop for StagedGeneration {
    fn drop(&mut self) {
        if self.published {
            return;
        }
        self.db.take();
        if self.temporary_path.exists() {
            let _ = fs::remove_dir_all(&self.temporary_path);
        }
    }
}

fn read_current(root: &Path) -> MetadataResult<String> {
    let path = root.join(CURRENT_FILE);
    let metadata = fs::symlink_metadata(&path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            MetadataError::InvalidArgument(format!("metadata CURRENT is missing at {}", path.display()))
        } else {
            io_error("inspect CURRENT", &path, error)
        }
    })?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() || metadata.len() > 64 {
        return Err(MetadataError::InvalidArgument(
            "invalid CURRENT: expected a bounded regular file".to_string(),
        ));
    }
    let raw = fs::read(&path).map_err(|error| io_error("read CURRENT", &path, error))?;
    let text = std::str::from_utf8(&raw)
        .map_err(|_| MetadataError::InvalidArgument("invalid CURRENT: not UTF-8".to_string()))?;
    let name = text
        .strip_suffix('\n')
        .ok_or_else(|| MetadataError::InvalidArgument("invalid CURRENT: missing newline".to_string()))?;
    if name.contains('\n') || parse_generation_name(name).is_err() {
        return Err(MetadataError::InvalidArgument(format!(
            "invalid CURRENT generation {name:?}"
        )));
    }
    Ok(name.to_string())
}

fn parse_generation_name(name: &str) -> MetadataResult<u64> {
    let digits = name
        .strip_prefix("gen-")
        .filter(|digits| digits.len() == 6 && digits.bytes().all(|byte| byte.is_ascii_digit()))
        .ok_or_else(|| MetadataError::InvalidArgument(format!("invalid generation name {name:?}")))?;
    let number = digits
        .parse::<u64>()
        .map_err(|_| MetadataError::InvalidArgument(format!("invalid generation name {name:?}")))?;
    if number == 0 {
        return Err(MetadataError::InvalidArgument(format!(
            "invalid generation name {name:?}"
        )));
    }
    Ok(number)
}

fn write_generation_manifest(path: &Path, name: &str) -> MetadataResult<()> {
    let manifest_path = path.join(GENERATION_MANIFEST_FILE);
    let payload = serde_json::to_vec_pretty(&GenerationManifest {
        version: GENERATION_MANIFEST_VERSION,
        generation: name.to_string(),
    })
    .map_err(|error| MetadataError::Internal(format!("encode generation manifest: {error}")))?;
    let mut file =
        File::create(&manifest_path).map_err(|error| io_error("create generation manifest", &manifest_path, error))?;
    file.write_all(&payload)
        .map_err(|error| io_error("write generation manifest", &manifest_path, error))?;
    file.sync_all()
        .map_err(|error| io_error("sync generation manifest", &manifest_path, error))
}

fn validate_generation(path: &Path, expected_name: &str) -> MetadataResult<()> {
    let metadata = fs::symlink_metadata(path).map_err(|error| io_error("inspect generation", path, error))?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(MetadataError::InvalidArgument(format!(
            "CURRENT generation {} is missing",
            path.display()
        )));
    }
    let manifest_path = path.join(GENERATION_MANIFEST_FILE);
    let manifest_metadata = fs::symlink_metadata(&manifest_path)
        .map_err(|error| io_error("inspect generation manifest", &manifest_path, error))?;
    if !manifest_metadata.file_type().is_file()
        || manifest_metadata.file_type().is_symlink()
        || manifest_metadata.len() > 4096
    {
        return Err(MetadataError::InvalidArgument(
            "invalid generation manifest file".to_string(),
        ));
    }
    let payload =
        fs::read(&manifest_path).map_err(|error| io_error("read generation manifest", &manifest_path, error))?;
    let manifest: GenerationManifest = serde_json::from_slice(&payload)
        .map_err(|error| MetadataError::InvalidArgument(format!("invalid generation manifest: {error}")))?;
    if manifest.version != GENERATION_MANIFEST_VERSION || manifest.generation != expected_name {
        return Err(MetadataError::InvalidArgument(format!(
            "generation manifest mismatch for {expected_name}"
        )));
    }
    Ok(())
}

fn publish_current(root: &Path, generation: &str) -> MetadataResult<()> {
    parse_generation_name(generation)?;
    let temporary_path = root.join(CURRENT_TMP_FILE);
    let current_path = root.join(CURRENT_FILE);
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary_path)
        .map_err(|error| io_error("create CURRENT temporary file", &temporary_path, error))?;
    file.write_all(format!("{generation}\n").as_bytes())
        .map_err(|error| io_error("write CURRENT temporary file", &temporary_path, error))?;
    file.sync_all()
        .map_err(|error| io_error("sync CURRENT temporary file", &temporary_path, error))?;
    fs::rename(&temporary_path, &current_path).map_err(|error| io_error("publish CURRENT", &current_path, error))?;
    sync_directory(root)
}

fn sync_directory(path: &Path) -> MetadataResult<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| io_error("sync directory", path, error))
}

fn io_error(operation: &str, path: &Path, error: std::io::Error) -> MetadataError {
    MetadataError::Internal(format!("{operation} {}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{MetadataError, MetadataResult};
    use rocksdb::{Options, DB};
    use std::path::Path;
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;

    fn open_db(path: &Path, create: bool) -> MetadataResult<Arc<DB>> {
        let mut options = Options::default();
        options.create_if_missing(create);
        DB::open(&options, path)
            .map(Arc::new)
            .map_err(|error| MetadataError::Internal(format!("open test RocksDB: {error}")))
    }

    #[test]
    fn format_creates_generation_layout_and_restart_selects_current() {
        let dir = TempDir::new().unwrap();
        let handle = GenerationHandle::open_for_format(dir.path(), open_db).unwrap();
        handle.pin().unwrap().db().put(b"key", b"value").unwrap();

        assert_eq!(
            std::fs::read_to_string(dir.path().join("CURRENT")).unwrap(),
            "gen-000001\n"
        );
        assert!(dir.path().join("generations/gen-000001/CURRENT").is_file());
        assert!(dir.path().join("snapshots").is_dir());
        drop(handle);

        let reopened = GenerationHandle::open_for_start(dir.path(), open_db).unwrap();
        assert_eq!(
            reopened.pin().unwrap().db().get(b"key").unwrap().unwrap().as_slice(),
            b"value"
        );
    }

    #[test]
    fn start_rejects_missing_malformed_and_traversing_current() {
        let dir = TempDir::new().unwrap();
        let missing = GenerationHandle::open_for_start(dir.path(), open_db).unwrap_err();
        assert!(missing.to_string().contains("CURRENT"));

        std::fs::write(dir.path().join("CURRENT"), "MANIFEST-000001\n").unwrap();
        let malformed = GenerationHandle::open_for_start(dir.path(), open_db).unwrap_err();
        assert!(malformed.to_string().contains("invalid CURRENT"));

        std::fs::write(dir.path().join("CURRENT"), "../gen-000001\n").unwrap();
        let traversal = GenerationHandle::open_for_start(dir.path(), open_db).unwrap_err();
        assert!(traversal.to_string().contains("invalid CURRENT"));
    }

    #[test]
    fn publish_switches_reads_and_defers_old_cleanup_until_pin_drops() {
        let dir = TempDir::new().unwrap();
        let handle = GenerationHandle::open_for_format(dir.path(), open_db).unwrap();
        handle.pin().unwrap().db().put(b"key", b"old").unwrap();
        let old_reference = {
            let pin = handle.pin().unwrap();
            Arc::clone(&pin.generation)
        };
        let old_path = old_reference.path().to_path_buf();

        let staged = handle.create_staged(open_db).unwrap();
        staged.db().put(b"key", b"new").unwrap();
        handle.publish_staged(staged, open_db).unwrap();

        assert_eq!(
            handle.pin().unwrap().db().get(b"key").unwrap().unwrap().as_slice(),
            b"new"
        );
        assert!(old_path.exists());
        handle.cleanup_retired().unwrap();
        assert!(old_path.exists());

        drop(old_reference);
        handle.cleanup_retired().unwrap();
        assert!(!old_path.exists());
    }

    #[test]
    fn generation_write_waits_for_pinned_reader() {
        let dir = TempDir::new().unwrap();
        let handle = Arc::new(GenerationHandle::open_for_format(dir.path(), open_db).unwrap());
        let (pinned_tx, pinned_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let reader = {
            let handle = Arc::clone(&handle);
            std::thread::spawn(move || {
                let _pin = handle.pin().unwrap();
                pinned_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            })
        };
        pinned_rx.recv().unwrap();

        let staged = handle.create_staged(open_db).unwrap();
        staged.db().put(b"key", b"new").unwrap();
        let (switched_tx, switched_rx) = mpsc::channel();
        let switcher = {
            let handle = Arc::clone(&handle);
            std::thread::spawn(move || {
                handle.publish_staged(staged, open_db).unwrap();
                switched_tx.send(()).unwrap();
            })
        };

        assert!(switched_rx.recv_timeout(Duration::from_millis(100)).is_err());
        release_tx.send(()).unwrap();
        switched_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        reader.join().unwrap();
        switcher.join().unwrap();
    }

    #[test]
    fn abandoned_staged_generation_is_removed_without_changing_current() {
        let dir = TempDir::new().unwrap();
        let handle = GenerationHandle::open_for_format(dir.path(), open_db).unwrap();
        let staged = handle.create_staged(open_db).unwrap();
        let temporary_path = staged.temporary_path.clone();

        drop(staged);

        assert!(!temporary_path.exists());
        assert_eq!(
            std::fs::read_to_string(dir.path().join("CURRENT")).unwrap(),
            "gen-000001\n"
        );
    }

    #[test]
    fn publication_failure_after_current_switch_poisons_generation_handle() {
        let dir = TempDir::new().unwrap();
        let handle = GenerationHandle::open_for_format(dir.path(), open_db).unwrap();
        let staged = handle.create_staged(open_db).unwrap();
        let error = handle.write().unwrap().publish_staged_with(
            staged,
            open_db,
            |_old, _staged| Ok(()),
            |_new| Err(MetadataError::Internal("publication failed".to_string())),
        );

        assert!(error.is_err());
        assert!(handle.pin().unwrap_err().to_string().contains("poisoned"));
        drop(handle);
        assert!(GenerationHandle::open_for_start(dir.path(), open_db).is_ok());
    }

    impl Generation {
        pub(crate) fn path(&self) -> &Path {
            &self.path
        }
    }

    impl GenerationHandle {
        pub(crate) fn publish_staged<F>(&self, staged: StagedGeneration, open_db: F) -> MetadataResult<()>
        where
            F: Fn(&Path, bool) -> MetadataResult<Arc<DB>> + Copy,
        {
            self.write()?
                .publish_staged_with(staged, open_db, |_old, _staged| Ok(()), |_new| Ok(()))
        }
    }
}
