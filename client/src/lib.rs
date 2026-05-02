// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Vecton Client Library
//!
//! High-level client API for interacting with Vecton metadata and worker services.
//!
//! # Features
//!
//! - HCFS-style API: `open`, `read`, `write`, `delete`, `rename`, `list`, `stat`
//! - Multi-Raft group routing
//! - Consistency levels: `strong`, `normal` (bounded-stale), `weak`
//! - Follower reads with msync/refresh compensation
//! - Direct worker reads with version-based cache invalidation
//! - ReadMode/WriteMode with forward compatibility

#![forbid(unsafe_code)]
#![deny(missing_docs)]

pub mod api;
pub mod cache;
pub mod canonical;
pub mod config;
pub mod consistency;
pub mod context;
pub mod error;
pub mod meta;
pub mod metrics;
pub mod modes;
pub mod routing;
pub mod worker;

// Re-export commonly used types
pub use config::ClientConfig;
pub use error::{ClientError, ClientResult};
// RequestHeader is available from common::header::RequestHeader
pub use api::hcfs::{Client, Handle, OpenFlags};
pub use canonical::{validate_header_or_action, ClientAction};
pub use consistency::ConsistencyLevel;
pub use meta::{
    replay_policy_for_method, ActionMachine, ActionMachinePolicy, FileSystemRpc, FileSystemRpcMethod, ReplayPolicy,
    RpcOp, TonicFileSystemRpc,
};
pub use modes::{ReadMode, WriteMode};
