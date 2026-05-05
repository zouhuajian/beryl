// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata authority implementation for Vecton.
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
//!   reports, task acknowledgements, and retained worker RPC compatibility
//!   surfaces.
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
//!   implementation. `MemoryStateStore` is retained under `state` for tests.
//!
//! ## Freshness and Current Limitations
//!
//! `GroupStateWatermark` carries state-machine applied `RaftLogId` freshness.
//! `route_epoch`, `mount_epoch`, and `worker_epoch` remain separate freshness
//! domains. Current no-op, legacy unsupported, and partially wired boundaries
//! are tracked in `metadata/README_ZH.md`.

pub mod bootstrap;
pub mod config;
pub mod data_io;
pub mod destructive_gate;
pub mod error;
pub mod file_handle;
pub mod inflight_registry;
pub mod inode_lease;
pub mod lease_runtime;
pub mod maintenance;
pub mod metrics;
pub mod mount;
pub mod path_resolver;
pub mod raft;
pub mod raft_conv;
pub mod readiness;
pub mod runtime;
pub mod service;
pub mod state;
pub mod worker;
pub mod write_session;

pub use bootstrap::ensure_root_mount;
pub use config::MetadataConfig;
pub use error::{MetadataError, MetadataResult};
pub use mount::MountTable;
pub use readiness::{wait_for_root_ready, RootReadinessConfig, RootReadinessGate};
pub use state::{RouteEpoch, StateStore};
