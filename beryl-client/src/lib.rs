// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Beryl filesystem client.
//!
//! The public facade is centered on [`FsClient`], [`FileReader`],
//! [`FileWriter`], delete/list options, and small namespace snapshot
//! types.
//! Metadata-facing operations are executed through the internal metadata
//! client and transport, with bounded retry, structured refresh, and
//! invalid response-header handling. Public reads return one complete buffer
//! through internal data-plane adapters; public writes use internal write-state
//! tracking and data-plane adapters. Metadata selects and persists the layout
//! for new files; existing files reuse that stored `FileLayout`.
//! Public reads fetch metadata-authoritative layout per read, without a read
//! layout cache or metadata-less direct worker access. Writer sync APIs are
//! [`FileWriter::sync_write_visibility`] and
//! [`FileWriter::sync_write_durability`].

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod api;
mod cache;
mod client_inner;
mod config;
mod error;
mod metrics;
mod planner;
mod rpc_error;
mod runtime;
mod session;

pub(crate) mod metadata;
mod worker;

// Re-export commonly used types
pub use api::{DeleteOptions, ListOptions};
pub use api::{DirectoryEntry, DirectoryListing, FileAttrs, FileStatus, InodeKind};
pub use api::{FileReader, FileWriter, FsClient};
pub use config::ClientConfig;
pub use config::{ConnectionConfig, ReadConfig, RetryConfig, WriteLeaseConfig};
pub use error::{ClientError, ClientErrorKind, ClientResult};
pub use metrics::{ClientMetric, ClientMetricEvent, ClientMetricLabels, ClientMetrics, NoopClientMetrics};
