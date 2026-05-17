// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! io_uring I/O engine placeholder implementation.

use std::path::Path;

use async_trait::async_trait;
use bytes::Bytes;

use super::{IoError, IoResult, LocalIoEngine};

/// io_uring I/O engine placeholder.
pub struct IoUringIoEngine;

impl IoUringIoEngine {
    pub fn new() -> Self {
        Self
    }
}

impl Default for IoUringIoEngine {
    fn default() -> Self {
        Self::new()
    }
}

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
