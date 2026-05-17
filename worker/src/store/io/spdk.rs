// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! SPDK I/O engine placeholder implementation.

use std::path::Path;

use async_trait::async_trait;
use bytes::Bytes;

use super::{IoError, IoResult, LocalIoEngine};

/// SPDK I/O engine placeholder.
pub struct SpdkIoEngine;

impl SpdkIoEngine {
    pub fn new() -> Self {
        Self
    }
}

impl Default for SpdkIoEngine {
    fn default() -> Self {
        Self::new()
    }
}

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
