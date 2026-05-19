// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! High-level API modules.

pub mod fs_client;
pub mod handle;
pub mod options;

pub use fs_client::FsClient;
pub use handle::FileHandle;
pub use options::{CreateMode, OpenOptions};
