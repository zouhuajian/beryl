// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Raft command definitions.
//!
//! All write operations are converted to Command and applied through Raft.

use crate::raft::types::{CommandFingerprint, DedupKey};
use crate::state::DeleteIntentStatus;
use bincode::config::standard;
use bincode::serde::encode_to_vec;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use types::block::{BlockPlacement, BlockState};
use types::fs::{FileAttrs, InodeId};
use types::ids::{BlockId, ClientId, MountId, ShardGroupId, ShardId};
use types::layout::FileLayout;
use types::lease::FencingToken;
use types::CallId;

/// File layout publication semantics for a committed write.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileCommitMode {
    /// Replace the existing authoritative file layout with the committed blocks.
    ///
    /// Used by CreateFile(CREATE_NEW) and CreateFile(OVERWRITE). Even CREATE_NEW
    /// publishes a complete new layout rather than appending to prior state.
    Replace,
    /// Append the committed blocks after the existing authoritative file layout.
    ///
    /// Used by AppendFile. The committed block range must start from the
    /// current file size / append base size.
    Append,
}

/// Raft command for state machine operations.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Command {
    /// Allocate a new block.
    AllocateBlock {
        dedup: DedupKey,
        inode_id: InodeId,
        block_id: BlockId,
        placement: BlockPlacement,
    },

    /// Commit a block (seal it).
    CommitBlock {
        dedup: DedupKey,
        block_id: BlockId,
        token: FencingToken,
    },

    /// Update block state.
    UpdateBlockState {
        dedup: DedupKey,
        block_id: BlockId,
        state: BlockState,
    },

    /// Acquire or renew lease.
    AcquireLease {
        dedup: DedupKey,
        block_id: BlockId,
        client_id: ClientId,
        epoch: u64,
        expires_at_ms: u64,
    },

    /// Release lease.
    ReleaseLease { dedup: DedupKey, block_id: BlockId },

    /// Create mount entry.
    CreateMount {
        dedup: DedupKey,
        mount_id: MountId,
        mount_prefix: String,
        mount_kind: crate::mount::MountKind,
        ufs_uri: Option<String>,
        data_io_policy: crate::mount::DataIoPolicy,
        namespace_owner_group_id: ShardGroupId,
        root_inode_id: InodeId,
    },

    /// Delete mount entry.
    DeleteMount { dedup: DedupKey, mount_id: MountId },

    /// Add a new shard group.
    AddShardGroup {
        dedup: DedupKey,
        shard_group_id: ShardGroupId,
        shard_ids: Vec<ShardId>,
        initial_members: Vec<u64>, // node IDs
    },

    /// Register worker identity and descriptor through the Raft apply boundary.
    RegisterWorker {
        dedup: DedupKey,
        identity: String,
        address: String,
        net_transport_kind: i32,
        worker_epoch: u64,
        fault_domain: Option<String>,
    },

    /// Create delete intents (batch operation to avoid per-block Raft write amplification).
    CreateDeleteIntents {
        dedup: DedupKey,
        intents: Vec<crate::state::DeleteIntent>,
    },

    /// Allocate delete intent IDs in Raft apply and create the provided intent payloads.
    AllocateDeleteIntents {
        dedup: DedupKey,
        intents: Vec<crate::state::DeleteIntent>,
    },

    /// Update authoritative delete intent execution status.
    UpdateDeleteIntentStatus {
        dedup: DedupKey,
        intent_id: u64,
        status: DeleteIntentStatus,
        finished_at_ms: Option<u64>,
        error_msg: Option<String>,
    },

    /// Create directory (Mkdir).
    Mkdir {
        dedup: DedupKey,
        parent_inode_id: InodeId,
        name: String,
        attrs: FileAttrs,
    },

    /// Create file (Create).
    Create {
        dedup: DedupKey,
        parent_inode_id: InodeId,
        name: String,
        attrs: FileAttrs,
        layout: FileLayout,
    },

    /// Unlink (delete file).
    Unlink {
        dedup: DedupKey,
        parent_inode_id: InodeId,
        name: String,
    },

    /// Remove directory (Rmdir).
    Rmdir {
        dedup: DedupKey,
        parent_inode_id: InodeId,
        name: String,
    },

    /// Rename (atomic within mount).
    Rename {
        dedup: DedupKey,
        src_parent_inode_id: InodeId,
        src_name: String,
        dst_parent_inode_id: InodeId,
        dst_name: String,
        flags: u32,
    },

    /// Set attributes.
    SetAttr {
        dedup: DedupKey,
        inode_id: InodeId,
        mask: u32, // Attribute mask (which fields to update)
        attrs: FileAttrs,
    },

    // ============================================================================
    // Write Path
    // ============================================================================
    /// Close write (commit extents).
    CloseWrite {
        dedup: DedupKey,
        inode_id: InodeId,
        extents: Vec<types::fs::Extent>,
        final_size: u64,
        lease_id: types::ids::LeaseId,
        open_epoch: u64,
        lease_epoch: u64,
        commit_mode: FileCommitMode,
    },
    /// Truncate file (shrink).
    Truncate {
        dedup: DedupKey,
        inode_id: InodeId,
        new_size: u64,
        lease_id: types::ids::LeaseId,
        lease_epoch: u64,
    },
}

impl Command {
    /// Get the dedup key for idempotency checking.
    pub fn dedup_key(&self) -> &DedupKey {
        match self {
            Command::AllocateBlock { dedup, .. }
            | Command::CommitBlock { dedup, .. }
            | Command::UpdateBlockState { dedup, .. }
            | Command::AcquireLease { dedup, .. }
            | Command::ReleaseLease { dedup, .. }
            | Command::CreateMount { dedup, .. }
            | Command::DeleteMount { dedup, .. }
            | Command::AddShardGroup { dedup, .. }
            | Command::RegisterWorker { dedup, .. }
            | Command::CreateDeleteIntents { dedup, .. }
            | Command::AllocateDeleteIntents { dedup, .. }
            | Command::UpdateDeleteIntentStatus { dedup, .. }
            | Command::Mkdir { dedup, .. }
            | Command::Create { dedup, .. }
            | Command::Unlink { dedup, .. }
            | Command::Rmdir { dedup, .. }
            | Command::Rename { dedup, .. }
            | Command::SetAttr { dedup, .. }
            | Command::CloseWrite { dedup, .. }
            | Command::Truncate { dedup, .. } => dedup,
        }
    }

    /// Convenience accessor for call_id.
    pub fn call_id(&self) -> &CallId {
        &self.dedup_key().call_id
    }

    /// Stable fingerprint of the command payload, excluding DedupKey.
    ///
    /// CommandFingerprint validates payload consistency under the same
    /// DedupKey; do not merge it into the dedup key.
    pub fn fingerprint(&self) -> CommandFingerprint {
        let view: FingerprintView = self.into();
        let bytes = encode_to_vec(&view, standard()).expect("fingerprint serialization should not fail");
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        let digest = hasher.finalize();
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&digest[..8]);
        CommandFingerprint(u64::from_be_bytes(buf))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum FingerprintView {
    AllocateBlock {
        inode_id: InodeId,
        block_id: BlockId,
        placement: BlockPlacement,
    },
    CommitBlock {
        block_id: BlockId,
        token: FencingToken,
    },
    UpdateBlockState {
        block_id: BlockId,
        state: BlockState,
    },
    AcquireLease {
        block_id: BlockId,
        client_id: ClientId,
        epoch: u64,
        expires_at_ms: u64,
    },
    ReleaseLease {
        block_id: BlockId,
    },
    CreateMount {
        mount_id: MountId,
        mount_prefix: String,
        mount_kind: crate::mount::MountKind,
        ufs_uri: Option<String>,
        data_io_policy: crate::mount::DataIoPolicy,
        namespace_owner_group_id: ShardGroupId,
        root_inode_id: InodeId,
    },
    DeleteMount {
        mount_id: MountId,
    },
    AddShardGroup {
        shard_group_id: ShardGroupId,
        shard_ids: Vec<ShardId>,
        initial_members: Vec<u64>,
    },
    RegisterWorker {
        identity: String,
        address: String,
        net_transport_kind: i32,
        worker_epoch: u64,
        fault_domain: Option<String>,
    },
    CreateDeleteIntents {
        intents: Vec<crate::state::DeleteIntent>,
    },
    AllocateDeleteIntents {
        intents: Vec<crate::state::DeleteIntent>,
    },
    UpdateDeleteIntentStatus {
        intent_id: u64,
        status: DeleteIntentStatus,
        finished_at_ms: Option<u64>,
        error_msg: Option<String>,
    },
    Mkdir {
        parent_inode_id: InodeId,
        name: String,
        attrs: FileAttrs,
    },
    Create {
        parent_inode_id: InodeId,
        name: String,
        attrs: FileAttrs,
        layout: FileLayout,
    },
    Unlink {
        parent_inode_id: InodeId,
        name: String,
    },
    Rmdir {
        parent_inode_id: InodeId,
        name: String,
    },
    Rename {
        src_parent_inode_id: InodeId,
        src_name: String,
        dst_parent_inode_id: InodeId,
        dst_name: String,
        flags: u32,
    },
    SetAttr {
        inode_id: InodeId,
        mask: u32,
        attrs: FileAttrs,
    },
    CloseWrite {
        inode_id: InodeId,
        extents: Vec<types::fs::Extent>,
        final_size: u64,
        lease_id: types::ids::LeaseId,
        open_epoch: u64,
        lease_epoch: u64,
        commit_mode: FileCommitMode,
    },
    Truncate {
        inode_id: InodeId,
        new_size: u64,
        lease_id: types::ids::LeaseId,
        lease_epoch: u64,
    },
}

impl From<&Command> for FingerprintView {
    fn from(cmd: &Command) -> Self {
        match cmd {
            Command::AllocateBlock {
                inode_id,
                block_id,
                placement,
                ..
            } => FingerprintView::AllocateBlock {
                inode_id: *inode_id,
                block_id: *block_id,
                placement: placement.clone(),
            },
            Command::CommitBlock { block_id, token, .. } => FingerprintView::CommitBlock {
                block_id: *block_id,
                token: *token,
            },
            Command::UpdateBlockState { block_id, state, .. } => FingerprintView::UpdateBlockState {
                block_id: *block_id,
                state: *state,
            },
            Command::AcquireLease {
                block_id,
                client_id,
                epoch,
                expires_at_ms,
                ..
            } => FingerprintView::AcquireLease {
                block_id: *block_id,
                client_id: *client_id,
                epoch: *epoch,
                expires_at_ms: *expires_at_ms,
            },
            Command::ReleaseLease { block_id, .. } => FingerprintView::ReleaseLease { block_id: *block_id },
            Command::CreateMount {
                mount_id,
                mount_prefix,
                mount_kind,
                ufs_uri,
                data_io_policy,
                namespace_owner_group_id,
                root_inode_id,
                ..
            } => FingerprintView::CreateMount {
                mount_id: *mount_id,
                mount_prefix: mount_prefix.clone(),
                mount_kind: *mount_kind,
                ufs_uri: ufs_uri.clone(),
                data_io_policy: *data_io_policy,
                namespace_owner_group_id: *namespace_owner_group_id,
                root_inode_id: *root_inode_id,
            },
            Command::DeleteMount { mount_id, .. } => FingerprintView::DeleteMount { mount_id: *mount_id },
            Command::AddShardGroup {
                shard_group_id,
                shard_ids,
                initial_members,
                ..
            } => FingerprintView::AddShardGroup {
                shard_group_id: *shard_group_id,
                shard_ids: shard_ids.clone(),
                initial_members: initial_members.clone(),
            },
            Command::RegisterWorker {
                identity,
                address,
                net_transport_kind,
                worker_epoch,
                fault_domain,
                ..
            } => FingerprintView::RegisterWorker {
                identity: identity.clone(),
                address: address.clone(),
                net_transport_kind: *net_transport_kind,
                worker_epoch: *worker_epoch,
                fault_domain: fault_domain.clone(),
            },
            Command::CreateDeleteIntents { intents, .. } => FingerprintView::CreateDeleteIntents {
                intents: intents.clone(),
            },
            Command::AllocateDeleteIntents { intents, .. } => FingerprintView::AllocateDeleteIntents {
                intents: intents.clone(),
            },
            Command::UpdateDeleteIntentStatus {
                intent_id,
                status,
                finished_at_ms,
                error_msg,
                ..
            } => FingerprintView::UpdateDeleteIntentStatus {
                intent_id: *intent_id,
                status: *status,
                finished_at_ms: *finished_at_ms,
                error_msg: error_msg.clone(),
            },
            Command::Mkdir {
                parent_inode_id,
                name,
                attrs,
                ..
            } => FingerprintView::Mkdir {
                parent_inode_id: *parent_inode_id,
                name: name.clone(),
                attrs: attrs.clone(),
            },
            Command::Create {
                parent_inode_id,
                name,
                attrs,
                layout,
                ..
            } => FingerprintView::Create {
                parent_inode_id: *parent_inode_id,
                name: name.clone(),
                attrs: attrs.clone(),
                layout: *layout,
            },
            Command::Unlink {
                parent_inode_id, name, ..
            } => FingerprintView::Unlink {
                parent_inode_id: *parent_inode_id,
                name: name.clone(),
            },
            Command::Rmdir {
                parent_inode_id, name, ..
            } => FingerprintView::Rmdir {
                parent_inode_id: *parent_inode_id,
                name: name.clone(),
            },
            Command::Rename {
                src_parent_inode_id,
                src_name,
                dst_parent_inode_id,
                dst_name,
                flags,
                ..
            } => FingerprintView::Rename {
                src_parent_inode_id: *src_parent_inode_id,
                src_name: src_name.clone(),
                dst_parent_inode_id: *dst_parent_inode_id,
                dst_name: dst_name.clone(),
                flags: *flags,
            },
            Command::SetAttr {
                inode_id, mask, attrs, ..
            } => FingerprintView::SetAttr {
                inode_id: *inode_id,
                mask: *mask,
                attrs: attrs.clone(),
            },
            Command::CloseWrite {
                inode_id,
                extents,
                final_size,
                lease_id,
                open_epoch,
                lease_epoch,
                commit_mode,
                ..
            } => FingerprintView::CloseWrite {
                inode_id: *inode_id,
                extents: extents.clone(),
                final_size: *final_size,
                lease_id: *lease_id,
                open_epoch: *open_epoch,
                lease_epoch: *lease_epoch,
                commit_mode: *commit_mode,
            },
            Command::Truncate {
                inode_id,
                new_size,
                lease_id,
                lease_epoch,
                ..
            } => FingerprintView::Truncate {
                inode_id: *inode_id,
                new_size: *new_size,
                lease_id: *lease_id,
                lease_epoch: *lease_epoch,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use types::ids::DataHandleId;
    use uuid::Uuid;

    fn dedup(client: u64, call: u128) -> DedupKey {
        DedupKey::new(ClientId::new(client), CallId::from_uuid(Uuid::from_u128(call)))
    }

    fn rename_command(dedup: DedupKey, dst_name: &str) -> Command {
        Command::Rename {
            dedup,
            src_parent_inode_id: InodeId::new(10),
            src_name: "old".to_string(),
            dst_parent_inode_id: InodeId::new(20),
            dst_name: dst_name.to_string(),
            flags: 0,
        }
    }

    fn close_write_command(dedup: DedupKey, commit_mode: FileCommitMode) -> Command {
        Command::CloseWrite {
            dedup,
            inode_id: InodeId::new(20),
            extents: vec![types::fs::Extent {
                file_offset: 0,
                block_id: BlockId::new(DataHandleId::new(30), types::ids::BlockIndex::new(0)),
                block_offset: 0,
                len: 64,
                file_version: None,
                block_stamp: None,
            }],
            final_size: 64,
            lease_id: types::ids::LeaseId::new(40),
            open_epoch: 50,
            lease_epoch: 60,
            commit_mode,
        }
    }

    #[test]
    fn fingerprint_is_stable_for_same_dedup_and_same_payload() {
        let dedup = dedup(7, 1);

        let first = rename_command(dedup.clone(), "new");
        let second = rename_command(dedup, "new");

        assert_eq!(first.fingerprint(), second.fingerprint());
    }

    #[test]
    fn fingerprint_changes_for_same_dedup_and_different_payload() {
        let dedup = dedup(7, 2);

        let first = rename_command(dedup.clone(), "new-a");
        let second = rename_command(dedup, "new-b");

        assert_ne!(first.fingerprint(), second.fingerprint());
    }

    #[test]
    fn rename_fingerprint_covers_overwrite_target() {
        let dedup = dedup(7, 20);
        let overwrite_target = rename_command(dedup.clone(), "target-a");
        let different_target = rename_command(dedup.clone(), "target-b");
        let no_replace = Command::Rename {
            dedup,
            src_parent_inode_id: InodeId::new(10),
            src_name: "old".to_string(),
            dst_parent_inode_id: InodeId::new(20),
            dst_name: "target-a".to_string(),
            flags: 1,
        };

        assert_ne!(overwrite_target.fingerprint(), different_target.fingerprint());
        assert_ne!(overwrite_target.fingerprint(), no_replace.fingerprint());
    }

    #[test]
    fn fingerprint_excludes_call_id() {
        let first = rename_command(dedup(7, 3), "new");
        let second = rename_command(dedup(7, 4), "new");

        assert_ne!(first.call_id(), second.call_id());
        assert_eq!(first.fingerprint(), second.fingerprint());
    }

    #[test]
    fn fingerprint_includes_command_type() {
        let unlink = Command::Unlink {
            dedup: dedup(7, 5),
            parent_inode_id: InodeId::new(10),
            name: "entry".to_string(),
        };
        let rmdir = Command::Rmdir {
            dedup: dedup(7, 6),
            parent_inode_id: InodeId::new(10),
            name: "entry".to_string(),
        };

        assert_ne!(unlink.fingerprint(), rmdir.fingerprint());
    }

    #[test]
    fn fingerprint_includes_commit_mode() {
        let dedup = dedup(7, 7);
        let replace = close_write_command(dedup.clone(), FileCommitMode::Replace);
        let append = close_write_command(dedup, FileCommitMode::Append);

        assert_ne!(replace.fingerprint(), append.fingerprint());
    }
}
