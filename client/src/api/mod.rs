// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! High-level API modules.

mod fs_client;
mod handle;
mod options;
mod status;

pub use fs_client::FsClient;
pub use handle::{FileReader, FileWriter};
pub use options::{AppendOptions, CreateDisposition, CreateOptions, ListOptions, OpenOptions};
pub use status::{DirectoryEntry, DirectoryListing, FileStatus};
pub use types::{FileAttrs, InodeKind};

#[cfg(test)]
mod tests;
