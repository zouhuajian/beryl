// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Snapshot file read leases protect readers from obsolete-file cleanup.

use parking_lot::Mutex;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, OnceLock, Weak};
use std::task::{Context, Poll};
use tokio::fs::File;
use tokio::io::{self, AsyncRead, AsyncSeek, AsyncWrite};

/// Read-only Raft snapshot file whose lease prevents cleanup while it is open.
pub(crate) struct SnapshotFile {
    path: PathBuf,
    file: File,
    _read_lease: Arc<()>,
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
            .finish_non_exhaustive()
    }
}

impl SnapshotFile {
    /// Re-open an existing snapshot file at path for read-only access.
    pub async fn open_read(path: PathBuf) -> io::Result<Self> {
        let file = File::options().read(true).open(&path).await?;
        let read_lease = acquire_read_lease(&path);
        Ok(Self {
            path,
            file,
            _read_lease: read_lease,
        })
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

#[cfg(test)]
mod tests {
    use super::*;
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
