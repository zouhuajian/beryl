// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! High-level API modules.

pub(crate) mod client;
pub(crate) mod handle;
mod options;
pub(crate) mod path;
pub(crate) mod runtime;
mod status;

pub use client::FsClient;
pub use handle::{FileReader, FileWriter};
pub use options::{CreateMode, CreateOptions, ListOptions};
pub use status::{DirectoryEntry, DirectoryListing, FileStatus};
pub use types::{BlockFormatId, FileAttrs, InodeKind};

#[cfg(test)]
mod tests;
