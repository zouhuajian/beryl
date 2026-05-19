// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Public filesystem open options.

/// File creation behavior for [`OpenOptions`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CreateMode {
    /// Do not create a file.
    #[default]
    None,
    /// Create only if the path does not exist.
    CreateNew,
    /// Create if absent or open if present.
    CreateOrOpen,
    /// Replace existing file contents.
    Overwrite,
}

/// Explicit public open options.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OpenOptions {
    /// Open for reads.
    pub read: bool,
    /// Open for writes.
    pub write: bool,
    /// Creation mode.
    pub create: CreateMode,
    /// Append to the current visible end of file.
    pub append: bool,
    /// Truncate existing contents.
    pub truncate: bool,
}

impl OpenOptions {
    /// Read-only open options.
    pub fn read_only() -> Self {
        Self {
            read: true,
            ..Self::default()
        }
    }

    /// Create-new sequential write options.
    pub fn create_new() -> Self {
        Self {
            write: true,
            create: CreateMode::CreateNew,
            ..Self::default()
        }
    }

    /// Overwrite sequential write options.
    pub fn overwrite() -> Self {
        Self {
            write: true,
            create: CreateMode::Overwrite,
            truncate: true,
            ..Self::default()
        }
    }

    /// Append sequential write options.
    pub fn append() -> Self {
        Self {
            write: true,
            append: true,
            ..Self::default()
        }
    }
}
