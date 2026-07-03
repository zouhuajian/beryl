// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use std::path::{Path, PathBuf};

use tempfile::TempDir;

use crate::TestResult;

pub struct TempState {
    root: TempDir,
}

impl TempState {
    pub fn new() -> TestResult<Self> {
        Ok(Self { root: TempDir::new()? })
    }

    pub fn root(&self) -> &Path {
        self.root.path()
    }

    pub fn metadata_dir(&self) -> PathBuf {
        self.root().join("metadata")
    }

    pub fn worker_root(&self) -> PathBuf {
        self.root().join("worker")
    }
}
