// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Public filesystem operation options.

/// Options for deleting a namespace entry.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DeleteOptions {
    /// Whether a directory delete should recursively remove descendants.
    pub recursive: bool,
}

/// Options for listing a directory through [`FsClient::list`](crate::FsClient::list).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ListOptions {
    /// Whether the listing should recursively include descendants.
    pub recursive: bool,

    /// Opaque seek cursor returned by a previous page for the same directory.
    /// It does not identify a server-side iterator or snapshot.
    pub cursor: Option<Vec<u8>>,

    /// Maximum entries in one page. `None` selects the Metadata server default.
    ///
    /// A value above the server maximum is rejected instead of being silently
    /// truncated.
    pub limit: Option<u32>,
}
