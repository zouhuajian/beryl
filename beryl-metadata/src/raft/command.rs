// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Metadata authority commands replicated through Raft.

use crate::session_registry::CreateFileOperationId;
use beryl_types::fs::{Extent, FileAttrs};
use beryl_types::ids::{InodeId, MountId, WorkerId};
use beryl_types::layout::FileLayout;
use beryl_types::{ContentGeneration, GroupName, LeaseEpoch};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) const MAX_RECLAIM_DETACHED_ROOT_CANDIDATES: u32 = 64;
pub(crate) const MAX_RECLAIM_DETACHED_ROOT_ENTRIES: u32 = 256;
pub(crate) const MIN_RECLAIM_DETACHED_ROOT_BATCH_BYTES: u32 = 4 * 1024;
pub(crate) const MAX_RECLAIM_DETACHED_ROOT_BATCH_BYTES: u32 = 1024 * 1024;

/// Largest serialized application command admitted to Raft.
///
/// This limits the command payload before OpenRaft constructs or persists a
/// log entry. Semantic apply limits remain responsible for bounding state
/// machine work after replay.
pub(crate) const MAX_COMMAND_BYTES: usize = 4 * 1024 * 1024;

/// File publication precondition and merge behavior.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum PublishMode {
    /// Replace content only while the expected content generation is current.
    ReplaceIfUnchanged,
    /// Append content only while the expected content generation is current.
    AppendIfUnchanged,
}

/// One durable metadata authority operation.
///
/// RPC identity is absent except for atomic CreateFile, whose exact client/call
/// identity is part of its durable response-loss replay contract.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum Command {
    BootstrapNamespace {
        proposed_at_ms: u64,
        group_name: GroupName,
    },
    CreateDirectory {
        proposed_at_ms: u64,
        root_inode_id: InodeId,
        components: Vec<String>,
        attrs: FileAttrs,
        recursive: bool,
    },
    CreateFile {
        proposed_at_ms: u64,
        operation_id: CreateFileOperationId,
        request_deadline_ms: u64,
        session_expires_at_ms: u64,
        normalized_path: String,
        mount_id: MountId,
        expected_mount_epoch: u64,
        mount_root_inode_id: InodeId,
        relative_components: Vec<String>,
        attrs: FileAttrs,
        layout: FileLayout,
    },
    /// Delete one exact mount-relative target after revalidating its path.
    ///
    /// Recursive directories are detached with a constant-size namespace
    /// mutation; descendants are reclaimed later by `ReclaimDetachedRoots`.
    Delete {
        proposed_at_ms: u64,
        mount_id: MountId,
        expected_mount_epoch: u64,
        mount_root_inode_id: InodeId,
        relative_components: Vec<String>,
        expected_inode_id: InodeId,
        expected_file_lease_epoch: Option<LeaseEpoch>,
        recursive: bool,
    },
    Rename {
        proposed_at_ms: u64,
        src_parent_inode_id: InodeId,
        src_name: String,
        expected_src_inode_id: InodeId,
        dst_parent_inode_id: InodeId,
        dst_name: String,
        expected_dst_inode_id: Option<InodeId>,
        expected_dst_lease_epoch: Option<LeaseEpoch>,
        flags: u32,
    },
    AcquireWriteLease {
        proposed_at_ms: u64,
        inode_id: InodeId,
        expected_lease_epoch: LeaseEpoch,
    },
    AllocateBlock {
        inode_id: InodeId,
        lease_epoch: LeaseEpoch,
    },
    EndWriteLease {
        proposed_at_ms: u64,
        inode_id: InodeId,
        lease_epoch: LeaseEpoch,
    },
    PublishFile {
        proposed_at_ms: u64,
        inode_id: InodeId,
        extents: Vec<Extent>,
        target_size: u64,
        expected_generation: ContentGeneration,
        expected_file_size: u64,
        lease_epoch: LeaseEpoch,
        mode: PublishMode,
    },
    RegisterWorkerDescriptor {
        proposed_at_ms: u64,
        group_name: GroupName,
        worker_id: WorkerId,
        address: String,
        worker_net_protocol: i32,
        fault_domain: Option<String>,
    },
    /// Reclaim a bounded amount of namespace authority from detached roots.
    ///
    /// Every budget is part of the replicated command. Apply also enforces
    /// fixed protocol maxima so local configuration cannot make replicas
    /// execute different state transitions.
    ReclaimDetachedRoots {
        candidate_root_inode_ids: Vec<InodeId>,
        max_entries: u32,
        max_batch_bytes: u32,
    },
}

impl Command {
    /// Stable low-cardinality operation name for logs and metrics.
    pub(crate) fn operation_name(&self) -> &'static str {
        match self {
            Self::BootstrapNamespace { .. } => "bootstrap_namespace",
            Self::CreateDirectory { .. } => "create_directory",
            Self::CreateFile { .. } => "create_file",
            Self::Delete { .. } => "delete",
            Self::Rename { .. } => "rename",
            Self::AcquireWriteLease { .. } => "acquire_write_lease",
            Self::AllocateBlock { .. } => "allocate_block",
            Self::EndWriteLease { .. } => "end_write_lease",
            Self::PublishFile { .. } => "publish_file",
            Self::RegisterWorkerDescriptor { .. } => "register_worker_descriptor",
            Self::ReclaimDetachedRoots { .. } => "reclaim_detached_roots",
        }
    }
}

/// Capture the server proposal timestamp immediately before Raft submission.
pub(crate) fn proposal_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use beryl_types::{BlockId, BlockIndex, MAX_FILE_EXTENTS};

    #[test]
    fn maximum_publish_file_command_fits_command_limit() {
        let inode_id = InodeId::new(u64::MAX);
        let extents = (0..MAX_FILE_EXTENTS)
            .map(|index| Extent {
                file_offset: u64::MAX,
                block_id: BlockId::new(inode_id, BlockIndex::new(index as u32)),
                block_offset: u64::MAX,
                len: u64::MAX,
                generation: Some(ContentGeneration::new(u64::MAX)),
                block_stamp: Some(u64::MAX),
            })
            .collect();
        let command = Command::PublishFile {
            proposed_at_ms: u64::MAX,
            inode_id,
            extents,
            target_size: u64::MAX,
            expected_generation: ContentGeneration::new(u64::MAX),
            expected_file_size: u64::MAX,
            lease_epoch: LeaseEpoch::new(u64::MAX),
            mode: PublishMode::ReplaceIfUnchanged,
        };

        let encoded = serde_json::to_vec(&command).expect("maximum legal command must serialize");
        assert!(
            encoded.len() <= MAX_COMMAND_BYTES,
            "maximum legal PublishFile command is {} bytes, exceeding {}",
            encoded.len(),
            MAX_COMMAND_BYTES
        );
    }
}
