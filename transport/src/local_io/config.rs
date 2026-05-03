// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Local I/O engine configuration and factory.

use crate::error::{IoError, IoResult};
use crate::local_io::{FsIoEngine, LocalIoEngine};
use std::sync::Arc;

#[cfg(all(feature = "io_uring", target_os = "linux"))]
use crate::local_io::IoUringIoEngine;

#[cfg(feature = "spdk")]
use crate::local_io::SpdkIoEngine;

/// Local I/O engine kind selection.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum LocalIoKind {
    /// File system I/O (default)
    #[default]
    Fs,
    /// io_uring I/O (Linux only)
    IoUring,
    /// SPDK I/O
    Spdk,
}

/// Local I/O engine configuration.
#[derive(Clone, Debug)]
pub struct LocalIoConfig {
    /// I/O engine kind to use
    pub kind: LocalIoKind,
}

impl Default for LocalIoConfig {
    fn default() -> Self {
        Self { kind: LocalIoKind::Fs }
    }
}

impl LocalIoConfig {
    pub fn new(kind: LocalIoKind) -> Self {
        Self { kind }
    }

    pub fn fs() -> Self {
        Self::new(LocalIoKind::Fs)
    }

    pub fn io_uring() -> Self {
        Self::new(LocalIoKind::IoUring)
    }

    pub fn spdk() -> Self {
        Self::new(LocalIoKind::Spdk)
    }
}

/// Build a local I/O engine from configuration.
pub fn build_local_io(cfg: &LocalIoConfig) -> IoResult<Arc<dyn LocalIoEngine>> {
    match cfg.kind {
        LocalIoKind::Fs => Ok(Arc::new(FsIoEngine::new())),
        #[cfg(all(feature = "io_uring", target_os = "linux"))]
        LocalIoKind::IoUring => Ok(Arc::new(IoUringIoEngine::new())),
        #[cfg(not(all(feature = "io_uring", target_os = "linux")))]
        LocalIoKind::IoUring => Err(IoError::NotSupported(
            "io_uring is only available on Linux with the 'io_uring' feature enabled".to_string(),
        )),
        #[cfg(feature = "spdk")]
        LocalIoKind::Spdk => Ok(Arc::new(SpdkIoEngine::new())),
        #[cfg(not(feature = "spdk"))]
        LocalIoKind::Spdk => Err(IoError::NotSupported(
            "SPDK requires the 'spdk' feature to be enabled".to_string(),
        )),
    }
}
