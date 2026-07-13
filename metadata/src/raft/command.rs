// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Versioned commands replicated through the metadata Raft log.

use crate::error::{MetadataError, MetadataResult};
use crate::raft::types::{CommandFingerprint, DedupKey};
use bincode::config::standard;
use bincode::serde::encode_to_vec;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};
use types::fs::{FileAttrs, InodeId};
use types::ids::WorkerId;
use types::layout::FileLayout;
use types::GroupName;

pub(crate) const COMMAND_FORMAT_VERSION: u16 = 3;

/// File layout publication semantics for a committed write.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum FileCommitMode {
    /// Replace the existing authoritative file layout with the committed blocks.
    Replace,
    /// Append committed blocks after the current authoritative layout.
    Append,
}

/// Replicated command envelope.
///
/// The envelope owns replay identity and proposal time. `Mutation` is the
/// canonical authority payload used by the command fingerprint.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Command {
    version: u16,
    dedup: DedupKey,
    proposed_at_ms: u64,
    mutation: Mutation,
}

impl Command {
    pub fn new(dedup: DedupKey, proposed_at_ms: u64, mutation: Mutation) -> Self {
        Self {
            version: COMMAND_FORMAT_VERSION,
            dedup,
            proposed_at_ms,
            mutation,
        }
    }

    pub fn dedup_key(&self) -> &DedupKey {
        &self.dedup
    }

    pub fn mutation(&self) -> &Mutation {
        &self.mutation
    }

    pub(crate) fn into_parts(self) -> (u64, Mutation) {
        (self.proposed_at_ms, self.mutation)
    }

    pub(crate) fn validate_version(&self) -> MetadataResult<()> {
        if self.version != COMMAND_FORMAT_VERSION {
            return Err(MetadataError::InvalidArgument(format!(
                "unsupported metadata command format version {}; expected {}",
                self.version, COMMAND_FORMAT_VERSION
            )));
        }
        Ok(())
    }

    /// Stable fingerprint of the versioned canonical mutation.
    ///
    /// Dedup identity and proposal time are deliberately excluded.
    pub fn fingerprint(&self) -> CommandFingerprint {
        let bytes = encode_to_vec((self.version, &self.mutation), standard())
            .expect("fingerprint serialization should not fail");
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        let digest = hasher.finalize();
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&digest[..8]);
        CommandFingerprint(u64::from_be_bytes(buf))
    }
}

/// Canonical metadata authority mutation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum Mutation {
    BootstrapNamespace {
        group_name: GroupName,
    },
    Mkdir {
        parent_inode_id: InodeId,
        name: String,
        attrs: FileAttrs,
    },
    CreateFile {
        parent_inode_id: InodeId,
        name: String,
        attrs: FileAttrs,
        layout: FileLayout,
    },
    Unlink {
        parent_inode_id: InodeId,
        name: String,
    },
    DeleteEmptyDir {
        parent_inode_id: InodeId,
        name: String,
    },
    DeleteTree {
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
    SyncWrite {
        inode_id: InodeId,
        extents: Vec<types::fs::Extent>,
        target_size: u64,
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
    RegisterWorkerDescriptor {
        group_name: GroupName,
        worker_id: WorkerId,
        address: String,
        worker_net_protocol: i32,
        fault_domain: Option<String>,
    },
}

/// Capture the server proposal timestamp before submitting a command to Raft.
pub(crate) fn proposal_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use types::ids::{BlockId, ClientId, DataHandleId};
    use types::CallId;
    use uuid::Uuid;

    fn dedup(client: u128, call: u128) -> DedupKey {
        DedupKey::new(ClientId::new(client), CallId::from_uuid(Uuid::from_u128(call)))
    }

    fn rename_command(dedup: DedupKey, proposed_at_ms: u64, dst_name: &str) -> Command {
        Command::new(
            dedup,
            proposed_at_ms,
            Mutation::Rename {
                src_parent_inode_id: InodeId::new(10),
                src_name: "old".to_string(),
                dst_parent_inode_id: InodeId::new(20),
                dst_name: dst_name.to_string(),
                flags: 0,
            },
        )
    }

    fn close_write_command(dedup: DedupKey, commit_mode: FileCommitMode) -> Command {
        Command::new(
            dedup,
            100,
            Mutation::CloseWrite {
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
            },
        )
    }

    #[test]
    fn fingerprint_is_stable_for_same_payload() {
        let first = rename_command(dedup(7, 1), 100, "new");
        let second = rename_command(dedup(7, 1), 100, "new");
        assert_eq!(first.fingerprint(), second.fingerprint());
    }

    #[test]
    fn fingerprint_changes_for_different_payload() {
        let first = rename_command(dedup(7, 2), 100, "new-a");
        let second = rename_command(dedup(7, 2), 100, "new-b");
        assert_ne!(first.fingerprint(), second.fingerprint());
    }

    #[test]
    fn fingerprint_excludes_dedup_identity() {
        let first = rename_command(dedup(7, 3), 100, "new");
        let second = rename_command(dedup(7, 4), 100, "new");
        assert_ne!(first.call_id(), second.call_id());
        assert_eq!(first.fingerprint(), second.fingerprint());
    }

    #[test]
    fn fingerprint_excludes_proposal_timestamp() {
        let first = rename_command(dedup(7, 30), 100, "new");
        let retry = rename_command(dedup(7, 30), 200, "new");
        assert_eq!(first.fingerprint(), retry.fingerprint());
    }

    #[test]
    fn fingerprint_includes_command_version() {
        let current = rename_command(dedup(7, 31), 100, "new");
        let mut future = current.clone();
        future.version += 1;

        assert_ne!(current.fingerprint(), future.fingerprint());
    }

    #[test]
    fn fingerprint_includes_mutation_type() {
        let unlink = Command::new(
            dedup(7, 5),
            100,
            Mutation::Unlink {
                parent_inode_id: InodeId::new(10),
                name: "entry".to_string(),
            },
        );
        let delete_empty_dir = Command::new(
            dedup(7, 6),
            100,
            Mutation::DeleteEmptyDir {
                parent_inode_id: InodeId::new(10),
                name: "entry".to_string(),
            },
        );
        assert_ne!(unlink.fingerprint(), delete_empty_dir.fingerprint());
    }

    #[test]
    fn fingerprint_includes_commit_mode() {
        let replace = close_write_command(dedup(7, 7), FileCommitMode::Replace);
        let append = close_write_command(dedup(7, 7), FileCommitMode::Append);
        assert_ne!(replace.fingerprint(), append.fingerprint());
    }

    impl Command {
        pub fn call_id(&self) -> &types::CallId {
            &self.dedup.call_id
        }
    }
}
