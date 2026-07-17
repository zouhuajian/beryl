// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! High-level API modules.

pub(crate) mod client;
pub(crate) mod handle;
pub(crate) mod options;
pub(crate) mod path;
mod status;

pub use beryl_types::{BlockFormatId, FileAttrs, InodeKind};
pub use client::FsClient;
pub use handle::{FileReader, FileWriter};
pub use options::{CreateOptions, ListOptions};
pub use status::{DirectoryEntry, DirectoryListing, FileStatus};

#[cfg(test)]
mod tests;
