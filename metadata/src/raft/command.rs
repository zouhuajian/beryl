// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Raft command definitions.
//!
//! All write operations are converted to Command and applied through Raft.

use serde::{Deserialize, Serialize};
use types::block::{BlockPlacement, BlockState};
use types::fs::{FileAttrs, InodeId};
use types::ids::{BlockId, ClientId, DataHandleId, MountId, ShardGroupId, ShardId, WorkerId};
use types::layout::FileLayout;
use types::lease::FencingToken;
use types::CallId;

/// Raft command for state machine operations.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Command {
    /// Update committed length.
    UpdateCommittedLength {
        request_id: CallId,
        data_handle_id: DataHandleId,
        committed_length: u64,
    },

    /// Allocate a new block.
    AllocateBlock {
        request_id: CallId,
        inode_id: InodeId,
        block_id: BlockId,
        placement: BlockPlacement,
    },

    /// Commit a block (seal it).
    CommitBlock {
        request_id: CallId,
        block_id: BlockId,
        token: FencingToken,
    },

    /// Update block state.
    UpdateBlockState {
        request_id: CallId,
        block_id: BlockId,
        state: BlockState,
    },

    /// Acquire or renew lease.
    AcquireLease {
        request_id: CallId,
        block_id: BlockId,
        client_id: ClientId,
        epoch: u64,
        expires_at_ms: u64,
    },

    /// Release lease.
    ReleaseLease { request_id: CallId, block_id: BlockId },

    /// Create mount entry.
    CreateMount {
        request_id: CallId,
        mount_id: MountId,
        mount_prefix: String,
        mount_kind: crate::mount::MountKind,
        ufs_uri: Option<String>,
        data_io_policy: crate::mount::DataIoPolicy,
        namespace_owner_group_id: ShardGroupId,
        root_inode_id: InodeId,
    },

    /// Delete mount entry.
    DeleteMount { request_id: CallId, mount_id: MountId },

    /// Increment layout version (for epoch updates).
    IncrementLayoutVersion { request_id: CallId },

    /// Add a new shard group.
    AddShardGroup {
        request_id: CallId,
        shard_group_id: ShardGroupId,
        shard_ids: Vec<ShardId>,
        initial_members: Vec<u64>, // node IDs
    },

    /// Upsert worker descriptor (low-frequency, authoritative).
    /// This replaces RegisterWorker and is the only worker-related command that writes to Raft.
    UpsertWorkerDescriptor {
        request_id: CallId,
        worker_id: WorkerId,
        address: String,
        net_transport_kind: i32,
        worker_epoch: u64,
        fault_domain: Option<String>,
    },

    /// Create delete intents (batch operation to avoid per-block Raft write amplification).
    CreateDeleteIntents {
        request_id: CallId,
        intents: Vec<crate::state::DeleteIntent>,
    },

    /// Create directory (Mkdir).
    Mkdir {
        request_id: CallId,
        parent_inode_id: InodeId,
        name: String,
        attrs: FileAttrs,
    },

    /// Create file (Create).
    Create {
        request_id: CallId,
        parent_inode_id: InodeId,
        name: String,
        attrs: FileAttrs,
        layout: FileLayout,
    },

    /// Unlink (delete file).
    Unlink {
        request_id: CallId,
        parent_inode_id: InodeId,
        name: String,
    },

    /// Remove directory (Rmdir).
    Rmdir {
        request_id: CallId,
        parent_inode_id: InodeId,
        name: String,
    },

    /// Rename (atomic within mount).
    Rename {
        request_id: CallId,
        src_parent_inode_id: InodeId,
        src_name: String,
        dst_parent_inode_id: InodeId,
        dst_name: String,
        flags: u32,
    },

    /// Set attributes.
    SetAttr {
        request_id: CallId,
        inode_id: InodeId,
        mask: u32, // Attribute mask (which fields to update)
        attrs: FileAttrs,
    },

    // ============================================================================
    // Write Path
    // ============================================================================
    /// Close write (commit extents).
    CloseWrite {
        request_id: CallId,
        inode_id: InodeId,
        extents: Vec<types::fs::Extent>,
        final_size: u64,
        lease_id: types::ids::LeaseId,
        open_epoch: u64,
        lease_epoch: u64,
    },
    /// Truncate file (shrink).
    Truncate {
        request_id: CallId,
        inode_id: InodeId,
        new_size: u64,
        lease_id: types::ids::LeaseId,
        lease_epoch: u64,
    },
    /// Set or update xattr.
    SetXattr {
        request_id: CallId,
        inode_id: InodeId,
        name: String,
        value: Vec<u8>,
        create: bool,
        replace: bool,
    },
    /// Remove xattr.
    RemoveXattr {
        request_id: CallId,
        inode_id: InodeId,
        name: String,
    },
}

impl Command {
    /// Get the request_id for idempotency checking.
    pub fn request_id(&self) -> &CallId {
        match self {
            Command::UpdateCommittedLength { request_id, .. }
            | Command::AllocateBlock { request_id, .. }
            | Command::CommitBlock { request_id, .. }
            | Command::UpdateBlockState { request_id, .. }
            | Command::AcquireLease { request_id, .. }
            | Command::ReleaseLease { request_id, .. }
            | Command::CreateMount { request_id, .. }
            | Command::DeleteMount { request_id, .. }
            | Command::IncrementLayoutVersion { request_id, .. }
            | Command::AddShardGroup { request_id, .. }
            | Command::UpsertWorkerDescriptor { request_id, .. }
            | Command::CreateDeleteIntents { request_id, .. }
            | Command::Mkdir { request_id, .. }
            | Command::Create { request_id, .. }
            | Command::Unlink { request_id, .. }
            | Command::Rmdir { request_id, .. }
            | Command::Rename { request_id, .. }
            | Command::SetAttr { request_id, .. }
            | Command::CloseWrite { request_id, .. }
            | Command::Truncate { request_id, .. }
            | Command::SetXattr { request_id, .. }
            | Command::RemoveXattr { request_id, .. } => request_id,
        }
    }
}
