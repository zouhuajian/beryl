// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Beryl filesystem client.
//!
//! The public facade is centered on [`FsClient`], [`FileReader`],
//! [`FileWriter`], delete/list options, and small namespace value types.
//! Metadata-facing operations are executed through the internal metadata
//! client and transport, with bounded retry, structured refresh, and
//! invalid response-header handling. Public reads fill caller-owned buffers
//! through bounded data-plane steps; public writes use internal write-state
//! tracking and data-plane adapters. Metadata selects and persists the layout
//! for new files; existing files reuse that stored `FileLayout`.
//! Sequential reads retain only the current Metadata-authorized block plan;
//! positioned reads never bypass Metadata authority. Writer sync APIs are
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
