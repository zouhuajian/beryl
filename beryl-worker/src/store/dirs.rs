// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Worker-local store directory selection and capacity reporting.

use std::collections::{BTreeMap, HashMap};
use std::ffi::CString;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use beryl_types::{BlockId, GroupName, Tier, TierFree};
use bytes::Bytes;
use tokio::sync::Notify;
use tracing::{info, warn};
use uuid::Uuid;

use crate::config::StoreDirConfig;
use crate::error::WorkerError;
use crate::store::block::{
    BlockMetaPayload, CreateStagingBlockRequest, FullBlockFileStore, FullBlockFileStoreConfig, LocalBlockStore,
    PublishReadyRequest, ReclaimBlockRequest, ReclaimBlockResult, ReclaimBlockState, StoreResult,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreDirReport {
    pub id: String,
    pub path: PathBuf,
    pub tier: Tier,
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    pub pending_bytes: u64,
    pub block_count: u64,
    pub fs_total_bytes: u64,
    pub fs_free_bytes: u64,
    pub free_bytes: u64,
    pub writable: bool,
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreReport {
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub pending_bytes: u64,
    pub free_bytes: u64,
    pub tier_free: Vec<TierFree>,
    pub dirs: Vec<StoreDirReport>,
}

/// Aggregates local block stores and signals changes to their reportable view.
///
/// `block_report_changed` is a coalescing wake-up signal, not an event log.
/// Block reporting must rescan the stores after every wake-up.
#[derive(Debug)]
pub struct StoreDirs {
    inner: Mutex<StoreDirsState>,
    reserve_bytes: u64,
    check_interval: Duration,
    block_report_changed: Notify,
}

#[derive(Debug)]
struct StoreDirsState {
    dirs: Vec<StoreDirState>,
    round_robin: HashMap<Tier, usize>,
}

#[derive(Debug)]
struct StoreDirState {
    id: String,
    path: PathBuf,
    tier: Tier,
    capacity_bytes: u64,
    store: FullBlockFileStore,
    mount_key: u64,
    fs_total_bytes: u64,
    fs_free_bytes: u64,
    error: Option<String>,
    last_check: Instant,
    used_bytes: u64,
    pending_bytes: u64,
    block_count: u64,
    pending_blocks: HashMap<(GroupName, BlockId), u64>,
    writable: bool,
}

impl StoreDirs {
    pub fn open(
        configs: BTreeMap<String, StoreDirConfig>,
        reserve_space_bytes: u64,
        check_interval_ms: u64,
    ) -> StoreResult<Self> {
        if configs.is_empty() {
            return Err(WorkerError::InvalidArgument(
                "beryl.worker.storage.dirs must be non-empty".to_string(),
            ));
        }
        if check_interval_ms == 0 {
            return Err(WorkerError::InvalidArgument(
                "beryl.worker.storage.check-interval must be greater than zero".to_string(),
            ));
        }

        let mut dirs = Vec::with_capacity(configs.len());
        for (id, config) in configs {
            init_store_path(&config.path)?;
            let (fs_total_bytes, fs_free_bytes) = fs_stats(&config.path)?;
            let mount_key = mount_key(&config.path)?;
            let store = FullBlockFileStore::new(FullBlockFileStoreConfig::new(config.path.clone()));
            store.recover_deleting_markers()?;
            let recovered_unpublished = store.recover_unpublished_blocks()?;
            if recovered_unpublished > 0 {
                info!(
                    store_dir = %id,
                    recovered_unpublished,
                    "Worker startup removed unpublished blocks from an older run"
                );
            }
            let (used_bytes, block_count) = scan_store_usage(&store, &config.path)?;
            dirs.push(StoreDirState {
                id,
                path: config.path,
                tier: config.tier,
                capacity_bytes: config.capacity_bytes,
                store,
                mount_key,
                fs_total_bytes,
                fs_free_bytes,
                error: None,
                last_check: Instant::now(),
                used_bytes,
                pending_bytes: 0,
                block_count,
                pending_blocks: HashMap::new(),
                writable: true,
            });
        }

        Ok(Self {
            inner: Mutex::new(StoreDirsState {
                dirs,
                round_robin: HashMap::new(),
            }),
            reserve_bytes: reserve_space_bytes,
            check_interval: Duration::from_millis(check_interval_ms),
            block_report_changed: Notify::new(),
        })
    }

    pub fn report(&self) -> StoreResult<StoreReport> {
        let mut inner = self.inner.lock().expect("store dir state poisoned");
        self.refresh_due(&mut inner)?;
        Ok(build_report(&inner, self.reserve_bytes))
    }

    pub fn scan_group_blocks(&self, group_name: &GroupName) -> StoreResult<Vec<BlockMetaPayload>> {
        let stores: Vec<_> = self
            .inner
            .lock()
            .expect("store dir state poisoned")
            .dirs
            .iter()
            .map(|dir| dir.store.clone())
            .collect();
        let mut blocks = Vec::new();
        for store in stores {
            blocks.extend(store.scan_group_blocks(group_name)?);
        }
        blocks.sort_by_key(|meta| {
            (
                meta.identity.block_id.inode_id.as_raw(),
                meta.identity.block_id.index.as_raw(),
            )
        });
        Ok(blocks)
    }

    /// Waits until a successful local publication or deletion may change a
    /// block report.
    ///
    /// Multiple changes may share one notification because the consumer derives
    /// the authoritative state by scanning the stores.
    pub(crate) async fn wait_for_block_report_change(&self) {
        self.block_report_changed.notified().await;
    }

    fn refresh_due(&self, inner: &mut StoreDirsState) -> StoreResult<()> {
        for dir in &mut inner.dirs {
            if dir.last_check.elapsed() < self.check_interval {
                continue;
            }
            let now = Instant::now();
            match fs_stats(&dir.path).and_then(|(total, free)| {
                probe_store_path(&dir.path)?;
                Ok((total, free))
            }) {
                Ok((total, free)) => {
                    dir.fs_total_bytes = total;
                    dir.fs_free_bytes = free;
                    dir.writable = true;
                    dir.error = None;
                }
                Err(err) => {
                    dir.writable = false;
                    dir.fs_total_bytes = 0;
                    dir.fs_free_bytes = 0;
                    let error = format!("{}: {err}", dir.path.display());
                    warn!(
                        store_dir = %dir.id,
                        path = %dir.path.display(),
                        error = %err,
                        "Worker store dir capacity refresh failed"
                    );
                    dir.error = Some(error);
                }
            }
            dir.last_check = now;
        }
        Ok(())
    }

    fn reserve_dir(&self, req: &CreateStagingBlockRequest) -> StoreResult<(usize, FullBlockFileStore)> {
        let mut inner = self.inner.lock().expect("store dir state poisoned");
        self.refresh_due(&mut inner)?;
        let report = build_report(&inner, self.reserve_bytes);
        let candidates: Vec<usize> = report
            .dirs
            .iter()
            .enumerate()
            .filter(|(_, dir)| dir.tier == req.tier && dir.writable && dir.free_bytes >= req.block_size)
            .map(|(idx, _)| idx)
            .collect();
        if candidates.is_empty() {
            let max_free = report
                .dirs
                .iter()
                .filter(|dir| dir.tier == req.tier)
                .map(|dir| dir.free_bytes)
                .max()
                .unwrap_or(0);
            return Err(WorkerError::ResourceExhausted(format!(
                "no {} store dir has enough free space: required_bytes={}, max_free_bytes={}",
                req.tier, req.block_size, max_free
            )));
        }

        let cursor = inner.round_robin.entry(req.tier).or_insert(0);
        let selected = candidates[*cursor % candidates.len()];
        *cursor = cursor.saturating_add(1);

        let key = (req.group_name.clone(), req.block_id);
        if inner.dirs.iter().any(|dir| dir.pending_blocks.contains_key(&key)) {
            return Err(WorkerError::InvalidArgument(format!(
                "staging block already has a pending reservation: block_id={}",
                req.block_id
            )));
        }
        inner.dirs[selected].pending_bytes = inner.dirs[selected]
            .pending_bytes
            .checked_add(req.block_size)
            .ok_or_else(|| WorkerError::Corrupt("store dir pending byte accounting overflow".to_string()))?;
        inner.dirs[selected].pending_blocks.insert(key, req.block_size);
        Ok((selected, inner.dirs[selected].store.clone()))
    }

    fn release_pending(&self, dir_index: usize, group_name: &GroupName, block_id: BlockId) -> StoreResult<bool> {
        let mut inner = self.inner.lock().expect("store dir state poisoned");
        release_pending_locked(&mut inner.dirs[dir_index], group_name, block_id)
    }

    /// Resolves the exact directory that owns one live pending reservation.
    fn find_pending_store(
        &self,
        group_name: &GroupName,
        block_id: BlockId,
    ) -> StoreResult<Option<(usize, FullBlockFileStore)>> {
        let inner = self.inner.lock().expect("store dir state poisoned");
        let key = (group_name.clone(), block_id);
        let mut found = None;
        for (idx, dir) in inner.dirs.iter().enumerate() {
            if !dir.pending_blocks.contains_key(&key) {
                continue;
            }
            if found.is_some() {
                return Err(WorkerError::Corrupt(format!(
                    "pending block exists in multiple worker store dirs: group_name={}, block_id={}",
                    group_name, block_id
                )));
            }
            found = Some((idx, dir.store.clone()));
        }
        Ok(found)
    }

    fn find_staging_store(
        &self,
        group_name: &GroupName,
        block_id: BlockId,
    ) -> StoreResult<Option<(usize, FullBlockFileStore)>> {
        let inner = self.inner.lock().expect("store dir state poisoned");
        let mut found = None;
        for (idx, dir) in inner.dirs.iter().enumerate() {
            let paths = dir.store.paths(group_name, block_id);
            if !paths.staging_meta_path.exists() && !paths.staging_data_path.exists() {
                continue;
            }
            if found.is_some() {
                return Err(WorkerError::Corrupt(format!(
                    "staging block exists in multiple worker store dirs: group_name={}, block_id={}",
                    group_name, block_id
                )));
            }
            found = Some((idx, dir.store.clone()));
        }
        Ok(found)
    }

    fn find_final_store(&self, group_name: &GroupName, block_id: BlockId) -> Option<(usize, FullBlockFileStore)> {
        let inner = self.inner.lock().expect("store dir state poisoned");
        inner.dirs.iter().enumerate().find_map(|(idx, dir)| {
            let paths = dir.store.paths(group_name, block_id);
            (paths.meta_path.exists() || paths.data_path.exists()).then(|| (idx, dir.store.clone()))
        })
    }

    fn find_reclaim_store(
        &self,
        group_name: &GroupName,
        block_id: BlockId,
    ) -> StoreResult<Option<(usize, FullBlockFileStore)>> {
        let inner = self.inner.lock().expect("store dir state poisoned");
        let mut found = None;
        for (idx, dir) in inner.dirs.iter().enumerate() {
            let paths = dir.store.paths(group_name, block_id);
            if !paths.deleting_marker_path.exists()
                && !paths.temp_deleting_marker_path.exists()
                && !paths.meta_path.exists()
                && !paths.data_path.exists()
                && !paths.temp_meta_path.exists()
                && !paths.staging_data_path.exists()
                && !paths.staging_meta_path.exists()
            {
                continue;
            }
            if found.is_some() {
                return Err(WorkerError::Corrupt(format!(
                    "block exists in multiple worker store dirs: group_name={}, block_id={}",
                    group_name, block_id
                )));
            }
            found = Some((idx, dir.store.clone()));
        }
        Ok(found)
    }

    fn inspect_absent_reclaim(&self, req: &ReclaimBlockRequest) -> StoreResult<ReclaimBlockState> {
        let store = self
            .inner
            .lock()
            .expect("store dir state poisoned")
            .dirs
            .first()
            .expect("store dirs validated non-empty")
            .store
            .clone();
        store.inspect_reclaim_block(req)
    }
}

impl LocalBlockStore for StoreDirs {
    fn create_staging_block(&self, req: CreateStagingBlockRequest) -> StoreResult<BlockMetaPayload> {
        let group_name = req.group_name.clone();
        let block_id = req.block_id;
        let (dir_index, store) = self.reserve_dir(&req)?;
        match store.create_staging_block(req) {
            Ok(meta) => Ok(meta),
            Err(err) => match store.abort_staging_block(&group_name, block_id) {
                Ok(()) => {
                    self.release_pending(dir_index, &group_name, block_id)?;
                    Err(err)
                }
                Err(cleanup_error) => {
                    tracing::warn!(
                        group_name = %group_name,
                        block_id = %block_id,
                        create_error = %err,
                        cleanup_error = %cleanup_error,
                        "failed staging creation retained pending capacity for retry"
                    );
                    Err(cleanup_error)
                }
            },
        }
    }

    fn write_at(&self, group_name: &GroupName, block_id: BlockId, offset: u64, data: Bytes) -> StoreResult<()> {
        if let Some((_, store)) = self.find_final_store(group_name, block_id) {
            return store.write_at(group_name, block_id, offset, data);
        }
        let Some((_, store)) = self.find_pending_store(group_name, block_id)? else {
            return Err(WorkerError::NotFound(format!(
                "pending staging block not found: group_name={}, block_id={}",
                group_name, block_id
            )));
        };
        store.write_at(group_name, block_id, offset, data)
    }

    fn publish_ready(&self, req: PublishReadyRequest) -> StoreResult<BlockMetaPayload> {
        let group_name = req.group_name.clone();
        let block_id = req.block_id;
        let Some((dir_index, store)) = self.find_pending_store(&group_name, block_id)? else {
            return Err(WorkerError::NotFound(format!(
                "pending staging block not found: group_name={}, block_id={}",
                group_name, block_id
            )));
        };
        let meta = store.publish_ready(req)?;
        let mut inner = self.inner.lock().expect("store dir state poisoned");
        if !release_pending_locked(&mut inner.dirs[dir_index], &group_name, block_id)? {
            return Err(WorkerError::Corrupt(format!(
                "published block lost its pending reservation: group_name={}, block_id={}",
                group_name, block_id
            )));
        }
        inner.dirs[dir_index].used_bytes = inner.dirs[dir_index]
            .used_bytes
            .saturating_add(meta.source.effective_len);
        inner.dirs[dir_index].block_count = inner.dirs[dir_index].block_count.saturating_add(1);
        drop(inner);
        self.block_report_changed.notify_one();
        Ok(meta)
    }

    fn read_at(&self, group_name: &GroupName, block_id: BlockId, offset: u64, len: u64) -> StoreResult<Bytes> {
        let Some((_, store)) = self.find_final_store(group_name, block_id) else {
            return Err(WorkerError::NotFound(format!(
                "ready block not found: group_name={}, block_id={}",
                group_name, block_id
            )));
        };
        store.read_at(group_name, block_id, offset, len)
    }

    fn load_meta(&self, group_name: &GroupName, block_id: BlockId) -> StoreResult<BlockMetaPayload> {
        let Some((_, store)) = self.find_final_store(group_name, block_id) else {
            return Err(WorkerError::NotFound(format!(
                "block metadata not found: group_name={}, block_id={}",
                group_name, block_id
            )));
        };
        store.load_meta(group_name, block_id)
    }

    fn inspect_reclaim_block(&self, req: &ReclaimBlockRequest) -> StoreResult<ReclaimBlockState> {
        let Some((_, store)) = self.find_reclaim_store(&req.group_name, req.block_id)? else {
            return self.inspect_absent_reclaim(req);
        };
        store.inspect_reclaim_block(req)
    }

    fn reclaim_block(&self, req: &ReclaimBlockRequest) -> StoreResult<ReclaimBlockResult> {
        let Some((dir_index, store)) = self.find_reclaim_store(&req.group_name, req.block_id)? else {
            return Ok(ReclaimBlockResult::AlreadyAbsent);
        };
        let result = store.reclaim_block(req)?;
        if let ReclaimBlockResult::Deleted { effective_len } = result {
            let mut inner = self.inner.lock().expect("store dir state poisoned");
            release_pending_locked(&mut inner.dirs[dir_index], &req.group_name, req.block_id)?;
            inner.dirs[dir_index].used_bytes = inner.dirs[dir_index].used_bytes.saturating_sub(effective_len);
            inner.dirs[dir_index].block_count = inner.dirs[dir_index].block_count.saturating_sub(1);
            drop(inner);
            self.block_report_changed.notify_one();
        }
        Ok(result)
    }

    fn abort_staging_block(&self, group_name: &GroupName, block_id: BlockId) -> StoreResult<()> {
        let pending = self.find_pending_store(group_name, block_id)?;
        let staging = self.find_staging_store(group_name, block_id)?;
        if let (Some((pending_index, _)), Some((staging_index, _))) = (&pending, &staging) {
            if pending_index != staging_index {
                return Err(WorkerError::Corrupt(format!(
                    "pending reservation and staging artifacts use different worker store dirs: group_name={}, block_id={}",
                    group_name, block_id
                )));
            }
        }
        let Some((dir_index, store)) = pending.or(staging) else {
            return Ok(());
        };
        store.abort_staging_block(group_name, block_id)?;
        let mut inner = self.inner.lock().expect("store dir state poisoned");
        release_pending_locked(&mut inner.dirs[dir_index], group_name, block_id)?;
        Ok(())
    }
}

fn build_report(inner: &StoreDirsState, reserve_bytes: u64) -> StoreReport {
    let mut pending_by_mount: HashMap<u64, u64> = HashMap::new();
    for dir in &inner.dirs {
        let entry = pending_by_mount.entry(dir.mount_key).or_default();
        *entry = entry.saturating_add(dir.pending_bytes);
    }

    let mut mount_capacity_left: HashMap<u64, u64> = HashMap::new();
    let mut mount_left: HashMap<u64, u64> = HashMap::new();
    let mut free_by_tier: HashMap<Tier, u64> = HashMap::new();
    let mut reports = Vec::with_capacity(inner.dirs.len());
    for dir in &inner.dirs {
        let capacity_left = dir
            .capacity_bytes
            .saturating_sub(dir.used_bytes)
            .saturating_sub(dir.pending_bytes);
        let free_bytes = if dir.writable {
            let pending_mount = pending_by_mount.get(&dir.mount_key).copied().unwrap_or(0);
            let fs_left = dir
                .fs_free_bytes
                .saturating_sub(reserve_bytes)
                .saturating_sub(pending_mount);
            let free_bytes = capacity_left.min(fs_left);
            let capacity_entry = mount_capacity_left.entry(dir.mount_key).or_default();
            *capacity_entry = capacity_entry.saturating_add(capacity_left);
            mount_left
                .entry(dir.mount_key)
                .and_modify(|value| *value = (*value).min(fs_left))
                .or_insert(fs_left);
            free_bytes
        } else {
            0
        };
        if dir.writable {
            free_by_tier
                .entry(dir.tier)
                .and_modify(|value| *value = (*value).max(free_bytes))
                .or_insert(free_bytes);
        }
        reports.push(StoreDirReport {
            id: dir.id.clone(),
            path: dir.path.clone(),
            tier: dir.tier,
            capacity_bytes: dir.capacity_bytes,
            used_bytes: dir.used_bytes,
            pending_bytes: dir.pending_bytes,
            block_count: dir.block_count,
            fs_total_bytes: dir.fs_total_bytes,
            fs_free_bytes: dir.fs_free_bytes,
            free_bytes,
            writable: dir.writable,
            error: dir.error.clone(),
        });
    }

    let free_bytes = mount_capacity_left
        .into_iter()
        .map(|(mount, capacity_left)| capacity_left.min(mount_left.get(&mount).copied().unwrap_or(0)))
        .fold(0u64, u64::saturating_add);
    let mut tier_free: Vec<_> = free_by_tier
        .into_iter()
        .map(|(tier, free_bytes)| TierFree { tier, free_bytes })
        .collect();
    tier_free.sort_by_key(|entry| tier_report_rank(entry.tier));
    StoreReport {
        total_bytes: inner
            .dirs
            .iter()
            .map(|dir| dir.capacity_bytes)
            .fold(0, u64::saturating_add),
        used_bytes: inner.dirs.iter().map(|dir| dir.used_bytes).fold(0, u64::saturating_add),
        pending_bytes: inner
            .dirs
            .iter()
            .map(|dir| dir.pending_bytes)
            .fold(0, u64::saturating_add),
        free_bytes,
        tier_free,
        dirs: reports,
    }
}

fn tier_report_rank(tier: Tier) -> u8 {
    match tier {
        Tier::Nvme => 0,
        Tier::Ssd => 1,
        Tier::Hdd => 2,
        Tier::Mem => 3,
    }
}

fn release_pending_locked(dir: &mut StoreDirState, group_name: &GroupName, block_id: BlockId) -> StoreResult<bool> {
    let key = (group_name.clone(), block_id);
    if let Some(bytes) = dir.pending_blocks.get(&key).copied() {
        let remaining = dir.pending_bytes.checked_sub(bytes).ok_or_else(|| {
            WorkerError::Corrupt(format!(
                "store dir pending byte accounting underflow: group_name={}, block_id={}, pending_bytes={}, reserved_bytes={}",
                group_name, block_id, dir.pending_bytes, bytes
            ))
        })?;
        dir.pending_blocks.remove(&key);
        dir.pending_bytes = remaining;
        return Ok(true);
    }
    Ok(false)
}

fn init_store_path(path: &Path) -> StoreResult<()> {
    fs::create_dir_all(path)?;
    if !path.is_dir() {
        return Err(WorkerError::InvalidArgument(format!(
            "beryl.worker.storage.dirs path {} is not a directory",
            path.display()
        )));
    }
    probe_store_path(path)
}

fn probe_store_path(path: &Path) -> StoreResult<()> {
    let probe = path.join(format!(".beryl-probe-{}", Uuid::new_v4()));
    {
        let mut file = OpenOptions::new().write(true).create_new(true).open(&probe)?;
        file.write_all(b"probe")?;
        file.sync_all()?;
    }
    fs::remove_file(&probe)?;
    OpenOptions::new().read(true).open(path)?.sync_all()?;
    Ok(())
}

fn scan_store_usage(store: &FullBlockFileStore, path: &Path) -> StoreResult<(u64, u64)> {
    let groups = path.join("groups");
    if !groups.exists() {
        return Ok((0, 0));
    }
    let mut used = 0u64;
    let mut block_count = 0u64;
    for entry in fs::read_dir(groups)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().and_then(|name| GroupName::parse(name).ok()) else {
            continue;
        };
        for meta in store.scan_group_blocks(&name)? {
            used = used.saturating_add(meta.source.effective_len);
            block_count = block_count.saturating_add(1);
        }
    }
    Ok((used, block_count))
}

#[cfg(unix)]
fn fs_stats(path: &Path) -> StoreResult<(u64, u64)> {
    use std::mem::MaybeUninit;
    use std::os::unix::ffi::OsStrExt;

    let raw = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| WorkerError::InvalidArgument(format!("path contains NUL byte: {}", path.display())))?;
    let mut stat = MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: raw is a NUL-terminated path, and stat points to valid writable memory.
    let rc = unsafe { libc::statvfs(raw.as_ptr(), stat.as_mut_ptr()) };
    if rc != 0 {
        return Err(WorkerError::from(std::io::Error::last_os_error()));
    }
    // SAFETY: statvfs returned success and initialized stat.
    let stat = unsafe { stat.assume_init() };
    let fragment_size = stat.f_frsize.max(1);
    let total = Into::<u64>::into(stat.f_blocks).saturating_mul(fragment_size);
    let free = Into::<u64>::into(stat.f_bavail).saturating_mul(fragment_size);
    Ok((total, free))
}

#[cfg(not(unix))]
fn fs_stats(_path: &Path) -> StoreResult<(u64, u64)> {
    Ok((u64::MAX, u64::MAX))
}

#[cfg(unix)]
fn mount_key(path: &Path) -> StoreResult<u64> {
    use std::os::unix::fs::MetadataExt;

    Ok(fs::metadata(path)?.dev())
}

#[cfg(not(unix))]
fn mount_key(path: &Path) -> StoreResult<u64> {
    use std::hash::{Hash, Hasher};

    let canonical = fs::canonicalize(path)?;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    canonical.hash(&mut hasher);
    Ok(hasher.finish())
}
