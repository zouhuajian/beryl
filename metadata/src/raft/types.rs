// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Raft type definitions for metadata service.

use crate::raft::snapshot::SnapshotFile;
use crate::state::DeleteIntentStatus;
use openraft::RaftTypeConfig;
use serde::{Deserialize, Serialize};
use types::fs::{FsErrorCode, InodeId};
use types::ids::{ClientId, DataHandleId, WorkerId};
use types::{CallId, GroupName};

/// Raft type configuration for metadata service.
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct MetadataRaftTypeConfig;

impl RaftTypeConfig for MetadataRaftTypeConfig {
    type D = crate::raft::command::Command;
    type R = AppDataResponse;
    type NodeId = u64;
    type Node = MetadataNode;
    type Entry = openraft::Entry<Self>;
    type SnapshotData = SnapshotFile;
    type AsyncRuntime = openraft::TokioRuntime;
    type Responder = openraft::impls::OneshotResponder<Self>;
}

/// Raft state for metadata service.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppMetadataRaftState {
    pub last_applied_log_id: Option<openraft::LogId<u64>>,
    pub last_purged_log_id: Option<openraft::LogId<u64>>,
    // Note: HardState and StoredMembership structures may be different in openraft 0.9.21
    // We'll use a simplified structure for now
    pub vote: Option<openraft::Vote<u64>>,
    pub committed: Option<openraft::LogId<u64>>,
    pub membership: openraft::Membership<u64, MetadataNode>,
}

impl Default for AppMetadataRaftState {
    fn default() -> Self {
        Self {
            last_applied_log_id: None,
            last_purged_log_id: None,
            vote: None,
            committed: None,
            membership: openraft::Membership::new(vec![], None),
        }
    }
}

/// Node information for metadata service.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct MetadataNode {
    pub node_id: u64,
    pub address: String,
}

impl MetadataNode {
    pub fn new(node_id: u64, address: String) -> Self {
        Self { node_id, address }
    }
}

/// Application-level response propagated from the state machine to the proposer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AppDataResponse {
    /// Filesystem command result with errno fidelity.
    Fs(FsCommandResult),
    /// Block-related command result.
    Block(BlockCommandResult),
    /// Lease-related command result.
    Lease(LeaseCommandResult),
    /// Mount-related command result.
    Mount(MountCommandResult),
    /// Shard group creation/update.
    ShardGroup(ShardGroupInfo),
    /// Worker-related command result.
    Worker(WorkerCommandResult),
    /// Delete intent creation count.
    DeleteIntents(DeleteIntentsResult),
    /// Delete intent status update.
    DeleteIntentStatus(DeleteIntentStatusResult),
    /// Explicitly empty result.
    None,
}

/// Filesystem apply result returned synchronously via Raft.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum FsCommandResult {
    Ok(FsOkResult),
    Err(FsErrnoResult),
}

impl FsCommandResult {
    pub fn ok() -> Self {
        FsCommandResult::Ok(FsOkResult::default())
    }
}

/// Successful FS command payload (minimal for now; extensible).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct FsOkResult {
    pub inode_id: Option<InodeId>,
    pub data_handle_id: Option<DataHandleId>,
    pub file_version: Option<u64>,
}

/// FS errno surfaced by apply.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct FsErrnoResult {
    pub errno: FsErrorCode,
    pub message: String,
}

/// Deduplication key: (client_id, call_id).
///
/// DedupKey identifies a logical mutation request. It must not include command
/// payload, epochs, paths, inodes, or CommandFingerprint.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct DedupKey {
    pub client_id: ClientId,
    pub call_id: CallId,
}

impl DedupKey {
    pub fn new(client_id: ClientId, call_id: CallId) -> Self {
        Self { client_id, call_id }
    }

    /// Generate a dedup key for internal/system-triggered commands.
    /// Uses client_id=0 to mark non-user initiated operations.
    /// System calls generate a fresh call_id per submitted logical op; retries must reuse it.
    pub fn system() -> Self {
        Self {
            client_id: ClientId::new(0),
            call_id: CallId::new(),
        }
    }
}

/// Stable fingerprint of a command payload.
///
/// CommandFingerprint validates payload consistency under the same DedupKey. It
/// is deliberately separate from DedupKey so call_id reuse with different
/// payloads is detected instead of treated as a new request.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct CommandFingerprint(pub u64);

/// Block command result.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum BlockCommandResult {
    Allocated(crate::state::BlockMetaState),
    Committed,
    StateUpdated,
}

/// Lease command result.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LeaseCommandResult {
    Acquired(crate::state::LeaseState),
    Released,
}

/// Mount command result.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MountCommandResult {
    Upserted(crate::mount::MountEntry),
    Deleted,
}

/// Worker command result.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WorkerCommandResult {
    Upserted(WorkerId),
}

/// Delete intent creation result.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeleteIntentsResult {
    pub created: u64,
}

/// Delete intent status update result.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeleteIntentStatusResult {
    pub intent_id: u64,
    pub status: DeleteIntentStatus,
}

/// Shard group information.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardGroupInfo {
    pub group_name: GroupName,
    pub shard_ids: Vec<u64>,
    pub initial_members: Vec<u64>,
    pub version: u64,
}
