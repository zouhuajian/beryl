// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata service for Vecton.
//!
//! This crate provides the metadata service that manages:
//! - **Inode/dentry-based filesystem**: Authoritative storage model using inodes and dentries
//! - **File and block metadata**: Block placement, leases, and data plane metadata
//! - **Mount table**: UFS path mapping with namespace routing
//! - **Raft state machine**: Distributed consensus for metadata operations
//!
//! # Architecture
//!
//! ## Filesystem Model
//!
//! The metadata service uses an **inode/dentry-based filesystem model**:
//!
//! - **Inodes**: Authoritative identity for filesystem objects (files, directories, symlinks)
//! - **Dentries**: Directory entries mapping (parent_inode_id, name) → child_inode_id
//! - **Paths are NOT authoritative**: All path operations must resolve through the dentry tree
//!
//! See `docs/metadata-fs-model.md` for detailed documentation.
//!
//! ## Services
//!
//! - **MetadataInodeServiceProto**: Authoritative inode-based FS service (see `service/inode_service.rs`)
//! - **FileSystemServiceProto**: External path-based filesystem entrypoint (path → walk → FS service)
//! - **MetadataClientService**: Supports data_handle_id-based data plane operations
//!
//! See `docs/metadata-services.md` for service architecture.
//!
//! ## State Storage
//!
//! - **RocksDB**: Persistent storage for inodes, dentries, blocks, leases, mounts
//! - **Raft**: Distributed consensus for write operations
//! - **StateStore trait**: Abstraction for state machine operations
//!
//! ## Routing and Consistency
//!
//! - **FS write routing**: All FS writes route to `mount.namespace_owner_group_id` for atomic rename
//! - **Read consistency**: Follower read gating via state_id comparison
//! - **Mount epoch**: Validates mount configuration freshness
//!
//! See `docs/metadata-routing-and-consistency.md` for details.

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
pub mod service;
pub mod state;
pub mod ufs_proxy;
pub mod worker;
pub mod write_session;

pub use bootstrap::ensure_root_mount;
pub use config::MetadataConfig;
pub use error::{MetadataError, MetadataResult};
pub use mount::MountTable;
pub use readiness::{wait_for_root_ready, RootReadinessConfig, RootReadinessGate};
pub use state::{LayoutVersion, MemoryStateStore, StateStore};
