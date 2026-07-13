// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Snapshot file lifecycle and incoming-install coordination.

use crate::error::{MetadataError, MetadataResult};
use openraft::LogId;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, OnceLock, Weak};
use std::task::{Context, Poll};
use tokio::fs::File;
use tokio::io::{self, AsyncRead, AsyncSeek, AsyncWrite};
use uuid::Uuid;

/// Wrapper around `tokio::fs::File` that retains the file path for later use
/// (e.g., rename/promote after a snapshot is fully written).
pub(crate) struct SnapshotFile {
    path: PathBuf,
    file: File,
    incoming: Option<IncomingSnapshotToken>,
    _read_lease: Option<Arc<()>>,
}

static SNAPSHOT_READERS: OnceLock<Mutex<HashMap<PathBuf, Weak<()>>>> = OnceLock::new();

fn snapshot_readers() -> &'static Mutex<HashMap<PathBuf, Weak<()>>> {
    SNAPSHOT_READERS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn acquire_read_lease(path: &Path) -> Arc<()> {
    let mut readers = snapshot_readers().lock();
    if let Some(lease) = readers.get(path).and_then(Weak::upgrade) {
        return lease;
    }
    let lease = Arc::new(());
    readers.insert(path.to_path_buf(), Arc::downgrade(&lease));
    lease
}

pub(crate) fn snapshot_file_in_use(path: &Path) -> bool {
    let mut readers = snapshot_readers().lock();
    match readers.get(path).and_then(Weak::upgrade) {
        Some(_) => true,
        None => {
            readers.remove(path);
            false
        }
    }
}

impl std::fmt::Debug for SnapshotFile {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SnapshotFile")
            .field("path", &self.path)
            .field("incoming", &self.incoming.is_some())
            .field("read_lease", &self._read_lease.is_some())
            .finish_non_exhaustive()
    }
}

impl SnapshotFile {
    /// Create or open a snapshot file at the given path.
    pub async fn create(path: PathBuf) -> io::Result<Self> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let file = File::options()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .await?;

        Ok(Self {
            path,
            file,
            incoming: None,
            _read_lease: None,
        })
    }

    /// Create a temporary file that owns one incoming-snapshot lifecycle token.
    pub(crate) async fn create_incoming(path: PathBuf, token: IncomingSnapshotToken) -> io::Result<Self> {
        let mut file = Self::create(path).await?;
        file.incoming = Some(token);
        Ok(file)
    }

    /// Re-open an existing snapshot file at path for read-only access.
    pub async fn open_read(path: PathBuf) -> io::Result<Self> {
        let file = File::options().read(true).open(&path).await?;
        let read_lease = acquire_read_lease(&path);
        Ok(Self {
            path,
            file,
            incoming: None,
            _read_lease: Some(read_lease),
        })
    }

    /// Return underlying path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Consume and convert into a blocking `std::fs::File`.
    pub(crate) async fn into_std_with_token(self) -> io::Result<(std::fs::File, Option<IncomingSnapshotToken>)> {
        let Self { file, incoming, .. } = self;
        Ok((file.into_std().await, incoming))
    }
}

impl AsyncRead for SnapshotFile {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.file).poll_read(cx, buf)
    }
}

impl AsyncWrite for SnapshotFile {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.file).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.file).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.file).poll_shutdown(cx)
    }
}

impl AsyncSeek for SnapshotFile {
    fn start_seek(mut self: Pin<&mut Self>, position: io::SeekFrom) -> io::Result<()> {
        Pin::new(&mut self.file).start_seek(position)
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        Pin::new(&mut self.file).poll_complete(cx)
    }
}

#[derive(Default)]
struct InstallState {
    pending_id: Option<Uuid>,
    deferred_purge: Option<LogId<u64>>,
}

/// Shared concrete state between the log store and state-machine store.
#[derive(Default)]
pub(crate) struct SnapshotInstallTracker {
    state: Mutex<InstallState>,
}

impl SnapshotInstallTracker {
    pub(crate) fn begin(self: &Arc<Self>) -> MetadataResult<IncomingSnapshotToken> {
        let mut state = self.state.lock();
        if state.pending_id.is_some() {
            return Err(MetadataError::ServiceUnavailable(
                "another incoming metadata snapshot is already pending".to_string(),
            ));
        }
        let id = Uuid::new_v4();
        state.pending_id = Some(id);
        state.deferred_purge = None;
        Ok(IncomingSnapshotToken {
            id,
            tracker: Arc::downgrade(self),
            completed: false,
        })
    }

    /// Returns true when purge must wait for the pending snapshot installation.
    pub(crate) fn defer_purge(&self, log_id: LogId<u64>) -> bool {
        let mut state = self.state.lock();
        if state.pending_id.is_none() {
            return false;
        }
        if state.deferred_purge.is_none_or(|current| current.index < log_id.index) {
            state.deferred_purge = Some(log_id);
        }
        true
    }

    fn complete(&self, id: Uuid) -> MetadataResult<Option<LogId<u64>>> {
        let mut state = self.state.lock();
        if state.pending_id != Some(id) {
            return Err(MetadataError::Internal(
                "incoming snapshot token no longer owns the pending installation".to_string(),
            ));
        }
        state.pending_id = None;
        Ok(state.deferred_purge.take())
    }

    fn cancel(&self, id: Uuid) {
        let mut state = self.state.lock();
        if state.pending_id == Some(id) {
            state.pending_id = None;
            state.deferred_purge = None;
        }
    }
}

/// RAII ownership of one incoming snapshot lifecycle.
pub(crate) struct IncomingSnapshotToken {
    id: Uuid,
    tracker: Weak<SnapshotInstallTracker>,
    completed: bool,
}

impl IncomingSnapshotToken {
    pub(crate) fn complete(mut self) -> MetadataResult<Option<LogId<u64>>> {
        let tracker = self
            .tracker
            .upgrade()
            .ok_or_else(|| MetadataError::Internal("snapshot install tracker was dropped".to_string()))?;
        let deferred = tracker.complete(self.id)?;
        self.completed = true;
        Ok(deferred)
    }
}

impl Drop for IncomingSnapshotToken {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        if let Some(tracker) = self.tracker.upgrade() {
            tracker.cancel(self.id);
        }
    }
}

#[cfg(test)]
mod tests {
    mod file {
        use super::super::*;
        use tempfile::TempDir;

        #[tokio::test]
        async fn read_lease_remains_live_until_last_snapshot_reader_drops() {
            let directory = TempDir::new().unwrap();
            let path = directory.path().join("snapshot.snap");
            tokio::fs::write(&path, b"snapshot").await.unwrap();

            let first = SnapshotFile::open_read(path.clone()).await.unwrap();
            let second = SnapshotFile::open_read(path.clone()).await.unwrap();
            assert!(snapshot_file_in_use(&path));

            drop(first);
            assert!(snapshot_file_in_use(&path));
            drop(second);
            assert!(!snapshot_file_in_use(&path));
        }
    }

    mod install {
        use super::super::*;
        use openraft::LeaderId;

        fn log(index: u64) -> LogId<u64> {
            LogId::new(LeaderId::new(1, 1), index)
        }

        #[test]
        fn pending_snapshot_defers_highest_purge_until_completion() {
            let tracker = Arc::new(SnapshotInstallTracker::default());
            let token = tracker.begin().unwrap();

            assert!(tracker.defer_purge(log(5)));
            assert!(tracker.defer_purge(log(3)));
            assert!(tracker.defer_purge(log(8)));
            assert_eq!(token.complete().unwrap().unwrap().index, 8);
            assert!(!tracker.defer_purge(log(9)));
        }

        #[test]
        fn abandoned_snapshot_releases_deferred_purge_state() {
            let tracker = Arc::new(SnapshotInstallTracker::default());
            let token = tracker.begin().unwrap();
            assert!(tracker.defer_purge(log(5)));

            drop(token);

            assert!(!tracker.defer_purge(log(6)));
            tracker.begin().expect("a later incoming snapshot can start");
        }

        #[test]
        fn concurrent_incoming_snapshot_is_rejected() {
            let tracker = Arc::new(SnapshotInstallTracker::default());
            let _token = tracker.begin().unwrap();

            let error = match tracker.begin() {
                Ok(_) => panic!("a second incoming snapshot must not start"),
                Err(error) => error,
            };
            assert!(error.to_string().contains("already pending"), "{error}");
        }
    }
}
