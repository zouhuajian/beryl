// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker-local I/O engine configuration and factory.

use std::sync::Arc;

use super::{FsIoEngine, IoResult, IoUringIoEngine, LocalIoEngine, SpdkIoEngine};

/// Worker-local I/O engine kind selection.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum LocalIoKind {
    /// File system I/O.
    #[default]
    Fs,
    /// Linux io_uring I/O placeholder.
    IoUring,
    /// SPDK I/O placeholder.
    Spdk,
}

/// Worker-local I/O engine configuration.
#[derive(Clone, Debug, Default)]
pub struct LocalIoConfig {
    pub kind: LocalIoKind,
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

/// Build a worker-local I/O engine from configuration.
pub fn build_local_io(cfg: &LocalIoConfig) -> IoResult<Arc<dyn LocalIoEngine>> {
    match cfg.kind {
        LocalIoKind::Fs => Ok(Arc::new(FsIoEngine::new())),
        LocalIoKind::IoUring => Ok(Arc::new(IoUringIoEngine::new())),
        LocalIoKind::Spdk => Ok(Arc::new(SpdkIoEngine::new())),
    }
}
