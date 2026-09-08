#![forbid(unsafe_code)]
// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//#![deny(missing_docs)]

//! Pure domain model.
//!
//! This crate must NOT depend on transport (gRPC/QUIC), storage engines, or OS specifics.
//! It defines shared domain identifiers, file and block layouts, byte ranges,
//! block locations, write authority, worker identities, storage tiers, and freshness watermarks.

extern crate core;

pub mod fs;
pub mod ids;
pub mod layout;
pub mod lease;
pub mod location;
pub mod range;
pub mod tier;
pub mod watermark;
pub mod worker;

pub use fs::{ContentGeneration, FileType, MAX_FILE_BLOCKS, WriteMode};
pub use ids::{BlockId, BlockIndex, CallId, ClientId, GroupName, GroupNameError, InodeId, MountId, WorkerId};
pub use layout::{
    BlockFormatId, BlockFormatIdError, BlockShape, BlockShapeError, FileLayout, FileLayoutError, MAX_BLOCK_SIZE,
};
pub use lease::{FencingToken, LeaseEpoch, WriteHandle};
pub use location::{CommittedBlock, FileBlockLocation, LocatedBlock};
pub use tier::{Tier, TierError, TierFree};
pub use watermark::{GroupStateWatermark, RaftLogId};
pub use worker::{MAX_REPORT_ENTRIES, WorkerEndpointInfo, WorkerRunId};
