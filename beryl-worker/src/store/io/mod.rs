// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Worker-local filesystem I/O engine abstraction.
//!
//! The I/O engine controls how bytes are read and written locally. It does not
//! define Beryl block interpretation, wire schema, placement policy, or
//! `BlockFormatId`.

mod fs;

pub use fs::FsIoEngine;

use std::path::Path;

use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;

pub type IoResult<T> = Result<T, IoError>;

/// Worker-local I/O engine errors.
#[derive(Debug, Error)]
pub enum IoError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("not supported: {0}")]
    NotSupported(String),

    #[error("unexpected eof")]
    UnexpectedEof,

    #[error("unknown error: {0}")]
    Unknown(String),
}

/// Local I/O engine trait for worker-local file and device operations.
///
/// This is an implementation detail below the store boundary. WorkerCore must
/// continue to depend on LocalBlockStore rather than selecting an I/O engine.
/// Metadata placement sees supported block formats only, not these execution
/// internals.
#[async_trait]
pub trait LocalIoEngine: Send + Sync {
    /// Write all data to a file at the given path.
    async fn write_all(&self, path: &Path, data: Bytes) -> IoResult<()>;

    /// Read a byte range from a file.
    async fn read_range(&self, path: &Path, offset: u64, len: usize) -> IoResult<Bytes>;

    /// Sync file data to disk.
    async fn sync(&self, path: &Path) -> IoResult<()> {
        let _ = path;
        Ok(())
    }
}
