// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Public filesystem operation options.

/// Options for creating a directory hierarchy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MkdirOptions {
    /// Whether missing parent directories should be created.
    pub create_parent: bool,
}

impl Default for MkdirOptions {
    fn default() -> Self {
        Self { create_parent: true }
    }
}

/// Options for deleting a namespace entry.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DeleteOptions {
    /// Whether a directory delete should recursively remove descendants.
    pub recursive: bool,
}

/// Options for listing a directory through [`FsClient::list_status`](crate::FsClient::list_status).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ListStatusOptions {
    /// Maximum entries fetched by each Metadata request.
    /// `None` selects the Metadata server default.
    ///
    /// A value above the server maximum is rejected instead of being silently
    /// truncated, and zero is rejected by the client.
    pub page_size: Option<u32>,
}
