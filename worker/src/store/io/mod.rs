// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker-local I/O engine abstraction for file and device I/O.

mod config;
mod fs;
mod io_uring;
mod spdk;

pub use config::{build_local_io, LocalIoConfig, LocalIoKind};
pub use fs::FsIoEngine;
pub use io_uring::IoUringIoEngine;
pub use spdk::SpdkIoEngine;

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

    #[error("not implemented: {0}")]
    NotImplemented(String),

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
