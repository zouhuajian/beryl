// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Client-owned metadata control-plane boundary.
//!
//! [`MetadataGateway`] is the metadata RPC boundary used by the public
//! [`crate::FsClient`] facade. It builds request headers from runtime attempt
//! context and preserves structured refresh hints for executor replay
//! decisions. Worker data reads stay behind the internal data boundary.

pub(crate) mod gateway;
pub(crate) mod ops;
pub(crate) mod snapshot;

mod header;

pub(crate) use gateway::{MetadataGateway, TonicMetadataGateway};
pub(crate) use ops::{
    AbortFileWriteOp, AddBlockOp, AppendFileOp, CommitFileOp, CreateFileOp, DeleteOp, GetBlockLocationsOp, GetStatusOp,
    ListStatusOp, MsyncOp, OpenFileOp, RenameOp, RenewLeaseOp,
};
pub(crate) use snapshot::{
    AbortFileWriteResult, AddBlockResult, CommitFileResult, DeleteResult, FileSnapshot, LayoutSnapshot, ListSnapshot,
    RenameResult, RenewLeaseResult, StatusSnapshot, WriteSessionSeed,
};
