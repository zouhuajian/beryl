// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Snapshot data handle backed by a filesystem file.
//!
//! Implements `AsyncRead`/`AsyncWrite`/`AsyncSeek` for use as `SnapshotData`
//! in openraft `RaftTypeConfig`.

use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::fs::File;
use tokio::io::{self, AsyncRead, AsyncSeek, AsyncWrite};

/// Wrapper around `tokio::fs::File` that retains the file path for later use
/// (e.g., rename/promote after a snapshot is fully written).
#[derive(Debug)]
pub struct SnapshotFile {
    path: PathBuf,
    file: File,
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

        Ok(Self { path, file })
    }

    /// Re-open an existing snapshot file at path for read-only access.
    pub async fn open_read(path: PathBuf) -> io::Result<Self> {
        let file = File::options().read(true).open(&path).await?;
        Ok(Self { path, file })
    }

    /// Return underlying path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Consume the wrapper and return the inner file.
    pub fn into_inner(self) -> File {
        self.file
    }

    /// Consume and convert into a blocking `std::fs::File`.
    pub async fn into_std(self) -> io::Result<std::fs::File> {
        Ok(self.file.into_std().await)
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
