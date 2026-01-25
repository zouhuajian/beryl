// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! SPDK I/O engine placeholder implementation.

#[cfg(feature = "spdk")]
use crate::error::{IoError, IoResult};
#[cfg(feature = "spdk")]
use crate::local_io::LocalIoEngine;
#[cfg(feature = "spdk")]
use async_trait::async_trait;
#[cfg(feature = "spdk")]
use bytes::Bytes;
#[cfg(feature = "spdk")]
use std::path::Path;

/// SPDK I/O engine placeholder.
///
/// This is a placeholder implementation that returns `NotImplemented` errors.
/// A full implementation would use SPDK for high-performance NVMe device access.
#[cfg(feature = "spdk")]
pub struct SpdkIoEngine;

#[cfg(feature = "spdk")]
impl SpdkIoEngine {
    pub fn new() -> Self {
        Self
    }
}

#[cfg(feature = "spdk")]
impl Default for SpdkIoEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "spdk")]
#[async_trait]
impl LocalIoEngine for SpdkIoEngine {
    async fn write_all(&self, _path: &Path, _data: Bytes) -> IoResult<()> {
        Err(IoError::NotImplemented(
            "SpdkIoEngine::write_all not yet implemented".to_string(),
        ))
    }

    async fn read_range(&self, _path: &Path, _offset: u64, _len: usize) -> IoResult<Bytes> {
        Err(IoError::NotImplemented(
            "SpdkIoEngine::read_range not yet implemented".to_string(),
        ))
    }

    async fn sync(&self, _path: &Path) -> IoResult<()> {
        Err(IoError::NotImplemented(
            "SpdkIoEngine::sync not yet implemented".to_string(),
        ))
    }
}
