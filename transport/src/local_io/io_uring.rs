// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! io_uring I/O engine placeholder implementation.

#[cfg(all(feature = "io_uring", target_os = "linux"))]
use crate::error::{IoError, IoResult};
#[cfg(all(feature = "io_uring", target_os = "linux"))]
use crate::local_io::LocalIoEngine;
#[cfg(all(feature = "io_uring", target_os = "linux"))]
use async_trait::async_trait;
#[cfg(all(feature = "io_uring", target_os = "linux"))]
use bytes::Bytes;
#[cfg(all(feature = "io_uring", target_os = "linux"))]
use std::path::Path;

/// io_uring I/O engine placeholder.
///
/// This is a placeholder implementation that returns `NotImplemented` errors.
/// A full implementation would use glommio or similar io_uring libraries.
#[cfg(all(feature = "io_uring", target_os = "linux"))]
pub struct IoUringIoEngine;

#[cfg(all(feature = "io_uring", target_os = "linux"))]
impl IoUringIoEngine {
    pub fn new() -> Self {
        Self
    }
}

#[cfg(all(feature = "io_uring", target_os = "linux"))]
impl Default for IoUringIoEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(all(feature = "io_uring", target_os = "linux"))]
#[async_trait]
impl LocalIoEngine for IoUringIoEngine {
    async fn write_all(&self, _path: &Path, _data: Bytes) -> IoResult<()> {
        Err(IoError::NotImplemented(
            "IoUringIoEngine::write_all not yet implemented".to_string(),
        ))
    }

    async fn read_range(&self, _path: &Path, _offset: u64, _len: usize) -> IoResult<Bytes> {
        Err(IoError::NotImplemented(
            "IoUringIoEngine::read_range not yet implemented".to_string(),
        ))
    }

    async fn sync(&self, _path: &Path) -> IoResult<()> {
        Err(IoError::NotImplemented(
            "IoUringIoEngine::sync not yet implemented".to_string(),
        ))
    }
}
