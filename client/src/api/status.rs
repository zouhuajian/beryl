// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Public namespace status snapshots.

use types::{FileAttrs, InodeKind};

/// Public file or directory status returned by [`crate::FsClient::stat`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileStatus {
    path: String,
    /// User-visible attributes for the namespace entry.
    pub attrs: FileAttrs,
}

impl FileStatus {
    pub(crate) fn new(path: impl Into<String>, attrs: FileAttrs) -> Self {
        Self {
            path: path.into(),
            attrs,
        }
    }

    /// Return the namespace path that was queried.
    pub fn path(&self) -> &str {
        &self.path
    }
}

/// Public directory entry returned by [`crate::FsClient::list`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirectoryEntry {
    /// Entry name relative to the listed directory.
    pub name: String,
    /// Entry kind when supplied by metadata.
    pub kind: Option<InodeKind>,
    /// Entry attributes when supplied by metadata.
    pub attrs: Option<FileAttrs>,
}

impl DirectoryEntry {
    pub(crate) fn new(name: impl Into<String>, kind: Option<InodeKind>, attrs: Option<FileAttrs>) -> Self {
        Self {
            name: name.into(),
            kind,
            attrs,
        }
    }
}

/// Public directory listing returned by [`crate::FsClient::list`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirectoryListing {
    path: String,
    /// Entries returned for the directory.
    pub entries: Vec<DirectoryEntry>,
    /// Opaque cursor for continuing a paginated listing when metadata returns one.
    pub next_cursor: Option<Vec<u8>>,
    /// Whether metadata reported the listing as complete.
    pub eof: bool,
}

impl DirectoryListing {
    pub(crate) fn new(
        path: impl Into<String>,
        entries: Vec<DirectoryEntry>,
        next_cursor: Option<Vec<u8>>,
        eof: bool,
    ) -> Self {
        Self {
            path: path.into(),
            entries,
            next_cursor,
            eof,
        }
    }

    /// Return the namespace path that was listed.
    pub fn path(&self) -> &str {
        &self.path
    }
}
