// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Public namespace status values.

use beryl_types::{FileAttrs, FileType};

/// Metadata-authorized status for one namespace entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileStatus {
    path: String,
    /// Namespace entry kind.
    pub kind: FileType,
    /// User-visible attributes for the namespace entry.
    pub attrs: FileAttrs,
}

impl FileStatus {
    /// Creates a status from a validated namespace path and Metadata response.
    pub(crate) fn new(path: impl Into<String>, kind: FileType, attrs: FileAttrs) -> Self {
        Self {
            path: path.into(),
            kind,
            attrs,
        }
    }

    /// Returns the full namespace path represented by this status.
    pub fn path(&self) -> &str {
        &self.path
    }
}
