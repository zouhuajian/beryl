// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! High-level API modules.

pub(crate) mod client;
pub(crate) mod options;
pub(crate) mod path;
mod reader;
mod status;
mod writer;

pub use beryl_types::{FileAttrs, InodeKind};
pub use client::FsClient;
pub use options::{DeleteOptions, ListOptions};
pub use reader::FileReader;
pub use status::{DirectoryEntry, DirectoryListing, FileStatus};
pub use writer::FileWriter;
