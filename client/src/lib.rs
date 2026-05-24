// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Vecton filesystem client.
//!
//! The public facade is centered on [`FsClient`], [`FileReader`],
//! [`FileWriter`], operation option types, and small namespace snapshot types.
//! Metadata-facing operations are executed through the internal operation
//! executor and metadata gateway, with hardened refresh, replay header, and
//! invalid response-header handling. Public reads return one complete buffer
//! through internal data-plane adapters; public writes use internal write-state
//! tracking and data-plane adapters.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod api;
mod cache;
mod canonical;
mod config;
mod consistency;
mod error;
mod metrics;
mod modes;
mod planner;
mod runtime;
mod session;

mod context;
mod data;
pub(crate) mod metadata;

// Re-export commonly used types
pub use api::{AppendOptions, CreateDisposition, CreateOptions, FileReader, FileWriter, FsClient};
pub use api::{DirectoryEntry, DirectoryListing, FileAttrs, FileStatus, InodeKind};
pub use api::{ListOptions, OpenOptions};
pub use config::ClientConfig;
pub use config::{
    BackoffConfig, CacheConfig, ChannelPoolConfig, ReadModeFallback, RefreshConfig, RetryConfig, WriteModeFallback,
};
pub use consistency::ConsistencyLevel;
pub use error::{ClientActionError, ClientError, ClientResult};
pub use modes::{ReadMode, WriteMode};
