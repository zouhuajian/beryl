// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Metadata authority implementation for Beryl.
//!
//! This crate owns the filesystem metadata authority: inodes, dentries,
//! attributes, mounts, metadata-level freshness, write-session fencing, worker
//! descriptors, block metadata, and the Raft state machine that commits
//! metadata mutations.
//!
//! # Architecture
//!
//! ## Runtime Entry Points
//!
//! The process entrypoint builds a `MetadataServer` runtime. The externally
//! registered metadata/control-plane services are:
//!
//! - `MetadataFileSystemServiceImpl`: implements the external
//!   `FileSystemService` path-based metadata/control-plane API.
//! - `MetadataWorkerServiceImpl`: handles worker registration, heartbeat, block
//!   reports, task acknowledgements, and current worker RPC surfaces.
//!
//! Metadata does not perform data-plane IO. Clients read and write data
//! directly through workers; this crate only maintains or returns metadata and
//! control-plane information needed by that path.
//!
//! ## Filesystem Model
//!
//! The current model is inode-centric:
//!
//! - **Inodes** are the authoritative identity for filesystem objects.
//! - **Dentries** map `(parent_inode_id, name)` to child inode IDs.
//! - **Paths are adapters**, not persisted sources of truth. Path operations
//!   resolve through mount selection and dentry traversal.
//!
//! ## State Storage
//!
//! - **RocksDB** stores authoritative metadata state and Raft-backed replicated
//!   state.
//! - **Raft** commits metadata mutations at authority boundaries.
//! - **RaftStateStore** is the production route-epoch `StateStore`
//!   implementation.
//!
//! ## Freshness and Current Limitations
//!
//! `GroupStateWatermark` carries state-machine applied `RaftLogId` freshness.
//! `route_epoch` and `mount_epoch` remain separate metadata freshness domains.
//! These fields are active correctness checks for the current single-group
//! runtime; they do not mean multi-group metadata is supported. Product
//! boundaries are summarized in `docs/freshness-and-ownership.md` and the
//! crate README.
//!
//! Raft adapters and raw authority storage are intentionally not part of the
//! crate API:
//!
//! ```compile_fail
//! use beryl_metadata::raft::RocksDBStorage;
//! ```
//!
//! Derived routing state cannot be mutated through the crate API:
//!
//! ```compile_fail
//! use beryl_metadata::mount::{DataIoPolicy, MountKind};
//! use beryl_metadata::MountTable;
//! use beryl_types::fs::InodeId;
//! use beryl_types::GroupName;
//!
//! let table = MountTable::new();
//! table.create_mount(
//!     "/bypass".to_string(),
//!     MountKind::Internal,
//!     None,
//!     DataIoPolicy::Allow,
//!     GroupName::parse("root").unwrap(),
//!     InodeId::new(1),
//! );
//! ```

pub mod config;
pub(crate) mod data_io;
pub(crate) mod error;
pub(crate) mod inflight_registry;
pub mod inode_lease;
pub mod lifecycle;
pub mod maintenance;
pub(crate) mod metrics;
pub mod mount;
pub(crate) mod observe;
pub(crate) mod path_resolver;
pub mod placement;
pub(crate) mod raft;
pub mod readiness;
pub mod runtime;
pub mod service;
pub(crate) mod session_registry;
pub mod state;
pub mod worker;

pub use config::MetadataConfig;
pub use error::{MetadataError, MetadataResult};
pub use metrics::MetadataMetrics;
pub use mount::MountTable;
pub use readiness::{RootReadinessConfig, RootReadinessGate};
pub use state::{RouteEpoch, StateStore};
