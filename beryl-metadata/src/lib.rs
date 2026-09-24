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
//!   reports and cleanup-command dispatch.
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
//! - **Raft** commits metadata mutations at authority boundaries and supplies
//!   leader-fenced route freshness reads.
//!
//! ## Freshness and Current Limitations
//!
//! `GroupStateWatermark` carries state-machine applied `RaftLogId` freshness.
//! The root mount has a fixed identity and owner. Lease epochs, content
//! generations, and Worker run IDs fence their respective mutable authority.
//! Reads remain leader-gated, and publication rechecks authority after waiting
//! for Worker Ready evidence.
//!
//! Raft adapters and raw authority storage are intentionally not part of the
//! crate API:
//!
//! ```compile_fail
//! use beryl_metadata::raft::RocksDBStorage;
//! ```
//!
pub mod config;
pub(crate) mod error;
pub(crate) mod inode;
pub mod lifecycle;
pub(crate) mod maintenance;
pub(crate) mod mount;
pub(crate) mod observe;
pub(crate) mod path_resolver;
pub(crate) mod placement;
pub(crate) mod raft;
pub(crate) mod readiness;
pub mod runtime;
pub(crate) mod service;
pub(crate) mod session_registry;
pub(crate) mod worker;

pub use config::MetadataConfig;
pub use error::{MetadataError, MetadataResult};
pub(crate) use mount::MountTable;
