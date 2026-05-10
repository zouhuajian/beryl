#![forbid(unsafe_code)]
// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//#![deny(missing_docs)]

//! Pure domain model.
//!
//! This crate must NOT depend on transport (gRPC/QUIC), storage engines, or OS specifics.
//! It contains only domain identifiers, layout/range/placement, block/chunk/stream/lease models,
//! and pure data structures like bitmap/range-set.

extern crate core;

pub mod acl;
pub mod block;
pub mod chunk;
pub mod fs;
pub mod group_watermark;
pub mod ids;
pub mod layout;
pub mod lease;
pub mod raft_log_id;
pub mod stream;

pub use acl::{
    AclCodecError, AclEntry, AclPerm, AclSubject, POSIX_ACL_ACCESS_XATTR, POSIX_ACL_DEFAULT_XATTR, PosixAcl,
    PosixDefaultAcl, decode_posix_acl, encode_posix_acl, is_acl_xattr_key,
};
pub use fs::{DirEntry, Extent, FileAttrs, FsErrorCode, Inode, InodeData, InodeId, InodeKind};
pub use group_watermark::{GroupStateWatermark, MountEpoch};
pub use ids::{
    BlockId, BlockIndex, CallId, ChunkId, ChunkIndex, ClientId, DataHandleId, LeaseId, MountId, RequestId,
    ShardGroupId, ShardId, StreamId, WorkerId,
};
pub use raft_log_id::RaftLogId;
