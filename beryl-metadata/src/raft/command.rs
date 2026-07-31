// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Metadata authority commands replicated through Raft.

use beryl_types::fs::{Extent, FileAttrs, InodeId};
use beryl_types::ids::{DataHandleId, WorkerId};
use beryl_types::layout::FileLayout;
use beryl_types::GroupName;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) const MAX_RECLAIM_DETACHED_ROOT_CANDIDATES: u32 = 64;
pub(crate) const MAX_RECLAIM_DETACHED_ROOT_ENTRIES: u32 = 256;
pub(crate) const MIN_RECLAIM_DETACHED_ROOT_BATCH_BYTES: u32 = 4 * 1024;
pub(crate) const MAX_RECLAIM_DETACHED_ROOT_BATCH_BYTES: u32 = 1024 * 1024;

/// File publication precondition and merge behavior.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum PublishMode {
    /// Replace content only while the expected content revision is current.
    ReplaceIfUnchanged,
    /// Append content only while the expected content revision is current.
    AppendIfUnchanged,
}

/// One durable metadata authority operation.
///
/// RPC identity is intentionally absent. Retry behavior is defined at the RPC
/// boundary; Raft contains only state transitions and their domain
/// preconditions.
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
        parent_inode_id: InodeId,
        name: String,
        attrs: FileAttrs,
        layout: FileLayout,
    },
    Delete {
        proposed_at_ms: u64,
        parent_inode_id: InodeId,
        name: String,
        expected_inode_id: InodeId,
        expected_file_lease_epochs: Vec<(InodeId, u64)>,
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
        expected_dst_lease_epoch: Option<u64>,
        flags: u32,
    },
    SetAttr {
        proposed_at_ms: u64,
        inode_id: InodeId,
        mask: u32,
        attrs: FileAttrs,
    },
    AcquireWriteLease {
        proposed_at_ms: u64,
        inode_id: InodeId,
        expected_lease_epoch: u64,
    },
    AllocateBlock {
        inode_id: InodeId,
        data_handle_id: DataHandleId,
        lease_epoch: u64,
    },
    EndWriteLease {
        proposed_at_ms: u64,
        inode_id: InodeId,
        lease_epoch: u64,
    },
    PublishFile {
        proposed_at_ms: u64,
        inode_id: InodeId,
        extents: Vec<Extent>,
        target_size: u64,
        expected_content_revision: u64,
        expected_file_size: u64,
        lease_epoch: u64,
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
            Self::SetAttr { .. } => "set_attr",
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
