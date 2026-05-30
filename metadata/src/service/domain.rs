// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Domain types for FsCore APIs.

use common::error::canonical::CanonicalError;
use common::header::{AuthnType, RequestHeader};
use types::fs::{Extent, FileAttrs, InodeId, InodeKind};
use types::ids::{BlockId, DataHandleId, LeaseId, MountId};
use types::layout::FileLayout;
use types::lease::FencingToken;
use types::{CommittedBlock, FileBlockLocation, GroupStateWatermark, WriteTarget};

#[derive(Clone, Debug)]
pub struct RequestContext {
    pub caller: RequestHeader,
    pub traceparent: Option<String>,
    pub route_epoch: Option<u64>,
    pub principal: Option<String>,
    pub real_user: Option<String>,
    pub doas: Option<String>,
    pub authn_type: AuthnType,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Freshness {
    pub mount_epoch: Option<u64>,
    pub route_epoch: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct SessionKey {
    pub file_handle: u64,
    pub lease_id: LeaseId,
    pub lease_epoch: u64,
    pub open_epoch: u64,
    pub fencing_token: FencingToken,
}

#[derive(Clone, Debug)]
pub struct PresentedFencingToken {
    pub block_id: Option<BlockId>,
    pub owner: u64,
    pub epoch: u64,
}

#[derive(Clone, Debug)]
pub struct CloseWriteIntent {
    pub committed_blocks: Vec<CommittedBlock>,
    pub final_size: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct FileRange {
    pub offset: u64,
    pub len: u64,
}

#[derive(Clone, Debug)]
pub struct CoreSuccess<T> {
    pub payload: T,
    pub group_id: Option<u64>,
    pub mount_epoch: Option<u64>,
    pub route_epoch: Option<u64>,
    pub state: Vec<GroupStateWatermark>,
}

#[derive(Clone, Debug)]
pub struct CoreFailure {
    pub error: Box<CanonicalError>,
    pub group_id: Option<u64>,
    pub mount_epoch: Option<u64>,
    pub route_epoch: Option<u64>,
    pub state: Vec<GroupStateWatermark>,
}

impl CoreFailure {
    pub fn new(
        error: CanonicalError,
        group_id: Option<u64>,
        mount_epoch: Option<u64>,
        route_epoch: Option<u64>,
        state: Vec<GroupStateWatermark>,
    ) -> Self {
        Self {
            error: Box::new(error),
            group_id,
            mount_epoch,
            route_epoch,
            state,
        }
    }
}

pub type CoreResult<T> = Result<CoreSuccess<T>, CoreFailure>;

#[derive(Clone, Debug)]
pub struct GetAttrInput {
    pub ctx: RequestContext,
    pub inode_id: InodeId,
    pub freshness: Freshness,
}

#[derive(Clone, Debug)]
pub struct GetAttrOutput {
    pub attrs: FileAttrs,
}

#[derive(Clone, Debug)]
pub struct MkdirInput {
    pub ctx: RequestContext,
    pub parent_inode_id: InodeId,
    pub name: String,
    pub attrs: FileAttrs,
    pub freshness: Freshness,
}

#[derive(Clone, Debug, Default)]
pub struct MkdirOutput {
    pub inode_id: Option<InodeId>,
    pub attrs: Option<FileAttrs>,
}

#[derive(Clone, Debug)]
pub struct CreateInput {
    pub ctx: RequestContext,
    pub parent_inode_id: InodeId,
    pub name: String,
    pub attrs: FileAttrs,
    pub layout: FileLayout,
    pub freshness: Freshness,
}

#[derive(Clone, Debug, Default)]
pub struct CreateOutput {
    pub inode_id: Option<InodeId>,
    pub attrs: Option<FileAttrs>,
    pub data_handle_id: Option<DataHandleId>,
}

#[derive(Clone, Debug)]
pub struct UnlinkInput {
    pub ctx: RequestContext,
    pub parent_inode_id: InodeId,
    pub name: String,
    pub freshness: Freshness,
}

#[derive(Clone, Debug, Default)]
pub struct UnlinkOutput;

#[derive(Clone, Debug)]
pub struct DeleteEmptyDirInput {
    pub ctx: RequestContext,
    pub parent_inode_id: InodeId,
    pub name: String,
    pub freshness: Freshness,
}

#[derive(Clone, Debug, Default)]
pub struct DeleteEmptyDirOutput;

#[derive(Clone, Debug)]
pub struct DeleteTreeInput {
    pub ctx: RequestContext,
    pub parent_inode_id: InodeId,
    pub name: String,
    pub freshness: Freshness,
}

#[derive(Clone, Debug, Default)]
pub struct DeleteTreeOutput;

#[derive(Clone, Debug)]
pub struct RenameInput {
    pub ctx: RequestContext,
    pub src_parent_inode_id: InodeId,
    pub src_name: String,
    pub dst_parent_inode_id: InodeId,
    pub dst_name: String,
    pub flags: u32,
    pub freshness: Freshness,
}

#[derive(Clone, Debug, Default)]
pub struct RenameOutput;

#[derive(Clone, Debug)]
pub struct ReadDirInput {
    pub ctx: RequestContext,
    pub parent_inode_id: InodeId,
    pub cursor_key: Option<Vec<u8>>,
    pub max_entries: Option<usize>,
    pub freshness: Freshness,
}

#[derive(Clone, Debug)]
pub struct ReadDirEntry {
    pub name: String,
    pub inode_id: InodeId,
    pub kind: Option<InodeKind>,
    pub attrs: Option<FileAttrs>,
}

#[derive(Clone, Debug, Default)]
pub struct ReadDirOutput {
    pub entries: Vec<ReadDirEntry>,
    pub next_cursor_key: Vec<u8>,
    pub eof: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct InodeMountGuardInputs {
    pub inode_id: InodeId,
    pub mount_id: MountId,
}

#[derive(Clone, Debug)]
pub struct AbortWriteInput {
    pub ctx: RequestContext,
    pub file_handle: u64,
    pub lease_id: Option<LeaseId>,
    pub lease_epoch: u64,
    pub open_epoch: u64,
    pub fencing_token: Option<PresentedFencingToken>,
    pub freshness: Freshness,
}

#[derive(Clone, Debug, Default)]
pub struct AbortWriteOutput;

#[derive(Clone, Debug)]
pub struct GetFileLayoutInput {
    pub ctx: RequestContext,
    pub inode_id: InodeId,
    pub range: Option<FileRange>,
    pub requested_data_handle_id: Option<DataHandleId>,
    pub freshness: Freshness,
}

#[derive(Clone, Debug, Default)]
pub struct GetFileLayoutOutput {
    pub extents: Vec<Extent>,
    pub file_size: u64,
    pub file_version: Option<u64>,
    pub locations: Vec<FileBlockLocation>,
}

#[derive(Clone, Debug)]
pub struct OpenWriteInput {
    pub ctx: RequestContext,
    pub inode_id: InodeId,
    pub desired_len: Option<u64>,
    pub mode: crate::inode_lease::WriteMode,
    pub freshness: Freshness,
}

#[derive(Clone, Debug)]
pub struct OpenWriteOutput {
    pub inode_id: InodeId,
    pub data_handle_id: DataHandleId,
    pub session_key: SessionKey,
    pub layout: FileLayout,
    pub write_targets: Vec<WriteTarget>,
    pub base_size: u64,
    pub expires_at_ms: u64,
}

#[derive(Clone, Debug)]
pub struct AddBlockInput {
    pub ctx: RequestContext,
    pub file_handle: u64,
    pub lease_id: Option<LeaseId>,
    pub lease_epoch: u64,
    pub open_epoch: u64,
    pub fencing_token: Option<PresentedFencingToken>,
    pub desired_len: Option<u64>,
    pub freshness: Freshness,
}

#[derive(Clone, Debug)]
pub struct AddBlockOutput {
    pub target: WriteTarget,
}

#[derive(Clone, Debug)]
pub struct RenewLeaseInput {
    pub ctx: RequestContext,
    pub file_handle: u64,
    pub lease_id: Option<LeaseId>,
    pub lease_epoch: u64,
    pub open_epoch: u64,
    pub fencing_token: Option<PresentedFencingToken>,
    pub freshness: Freshness,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RenewLeaseOutput {
    pub expires_at_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncWriteMode {
    Visibility,
    Durability,
}

#[derive(Clone, Debug)]
pub struct SyncWriteInput {
    pub ctx: RequestContext,
    pub file_handle: u64,
    pub lease_id: Option<LeaseId>,
    pub lease_epoch: u64,
    pub open_epoch: u64,
    pub fencing_token: Option<PresentedFencingToken>,
    pub data_handle_id: DataHandleId,
    pub committed_blocks: Vec<CommittedBlock>,
    pub target_size: u64,
    pub flags: u32,
    pub mode: SyncWriteMode,
    pub freshness: Freshness,
}

/// Metadata result for a SyncWrite barrier that published or confirmed a visible prefix.
#[derive(Clone, Debug, Default)]
pub struct SyncWriteOutput {
    /// Visible prefix acknowledged by metadata for this barrier.
    pub synced_size: u64,
    /// File version visible to new read plans after the barrier.
    pub file_version: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct CloseWriteInput {
    pub ctx: RequestContext,
    pub file_handle: u64,
    pub lease_id: Option<LeaseId>,
    pub lease_epoch: u64,
    pub open_epoch: u64,
    pub fencing_token: Option<PresentedFencingToken>,
    pub intent: CloseWriteIntent,
    pub freshness: Freshness,
}

#[derive(Clone, Debug, Default)]
pub struct CloseWriteOutput {
    pub committed_size: u64,
    pub file_version: Option<u64>,
}
