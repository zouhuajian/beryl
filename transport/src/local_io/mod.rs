// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Local I/O engine abstraction for file and device I/O.

mod config;
mod fs;
mod io_uring;
mod spdk;

pub use config::{build_local_io, LocalIoConfig, LocalIoKind};
pub use fs::FsIoEngine;

#[cfg(all(feature = "io_uring", target_os = "linux"))]
pub use io_uring::IoUringIoEngine;

#[cfg(feature = "spdk")]
pub use spdk::SpdkIoEngine;

use crate::error::IoResult;
use async_trait::async_trait;
use bytes::Bytes;
use std::path::Path;

/// Local I/O engine trait for file and device I/O operations.
///
/// This trait provides a unified interface for different local I/O implementations:
/// - FsIoEngine: Standard file system I/O (default)
/// - IoUringIoEngine: Linux io_uring-based I/O (high performance)
/// - SpdkIoEngine: SPDK-based I/O (for NVMe devices)
///
/// The trait supports a "chunk" model: sequential write-once with range reads.
/// Random overwrites are not supported.
#[async_trait]
pub trait LocalIoEngine: Send + Sync {
    /// Write all data to a file at the given path.
    ///
    /// This operation:
    /// - Creates the file if it doesn't exist
    /// - Truncates the file if it exists (sequential write model)
    /// - Writes all data
    /// - Optionally syncs to disk
    async fn write_all(&self, path: &Path, data: Bytes) -> IoResult<()>;

    /// Read a range of bytes from a file.
    ///
    /// Reads `len` bytes starting at `offset`.
    /// Returns `IoError::UnexpectedEof` if EOF is reached before reading `len` bytes.
    async fn read_range(&self, path: &Path, offset: u64, len: usize) -> IoResult<Bytes>;

    /// Sync file data to disk (optional).
    ///
    /// Default implementation is a no-op.
    async fn sync(&self, path: &Path) -> IoResult<()> {
        let _ = path;
        Ok(())
    }
}
