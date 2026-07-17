// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Versioned commands replicated through the metadata Raft log.

use crate::error::{MetadataError, MetadataResult};
use crate::raft::types::{CommandFingerprint, DedupKey};
use beryl_types::fs::{FileAttrs, InodeId};
use beryl_types::ids::WorkerId;
use beryl_types::layout::FileLayout;
use beryl_types::GroupName;
use bincode::config::standard;
use bincode::serde::encode_to_vec;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) const COMMAND_FORMAT_VERSION: u16 = 5;

/// Durable namespace semantics for CreateFile.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum CreateFileMode {
    CreateNew,
    CreateOrOverwrite,
}

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
    canonical_namespace_request: Option<CanonicalNamespaceRequest>,
    mutation: Mutation,
}

impl Command {
    pub fn new(dedup: DedupKey, proposed_at_ms: u64, mutation: Mutation) -> Self {
        Self {
            version: COMMAND_FORMAT_VERSION,
            dedup,
            proposed_at_ms,
            canonical_namespace_request: None,
            mutation,
        }
    }

    pub(crate) fn new_namespace(
        dedup: DedupKey,
        proposed_at_ms: u64,
        canonical_request: CanonicalNamespaceRequest,
        mutation: Mutation,
    ) -> Self {
        assert!(
            canonical_request.matches_mutation(&mutation),
            "canonical namespace request must match its authority mutation"
        );
        Self {
            version: COMMAND_FORMAT_VERSION,
            dedup,
            proposed_at_ms,
            canonical_namespace_request: Some(canonical_request),
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
        let bytes = match &self.canonical_namespace_request {
            Some(request) => encode_to_vec((self.version, request), standard())
                .expect("namespace fingerprint serialization should not fail"),
            None => encode_to_vec((self.version, &self.mutation), standard())
                .expect("fingerprint serialization should not fail"),
        };
        Self::hash_fingerprint(bytes)
    }

    pub(crate) fn create_directory_fingerprint(path: &str, attrs: &FileAttrs, recursive: bool) -> CommandFingerprint {
        Self::hash_fingerprint(
            encode_to_vec(
                (
                    COMMAND_FORMAT_VERSION,
                    CanonicalNamespaceRequest::CreateDirectory {
                        path: path.to_string(),
                        attrs: attrs.clone(),
                        recursive,
                    },
                ),
                standard(),
            )
            .expect("namespace fingerprint serialization should not fail"),
        )
    }

    pub(crate) fn create_file_fingerprint(
        path: &str,
        attrs: &FileAttrs,
        layout: &FileLayout,
        mode: CreateFileMode,
    ) -> CommandFingerprint {
        Self::hash_fingerprint(
            encode_to_vec(
                (
                    COMMAND_FORMAT_VERSION,
                    CanonicalNamespaceRequest::CreateFile {
                        path: path.to_string(),
                        attrs: attrs.clone(),
                        layout: *layout,
                        mode,
                    },
                ),
                standard(),
            )
            .expect("namespace fingerprint serialization should not fail"),
        )
    }

    pub(crate) fn delete_fingerprint(path: &str, recursive: bool) -> CommandFingerprint {
        Self::hash_fingerprint(
            encode_to_vec(
                (
                    COMMAND_FORMAT_VERSION,
                    CanonicalNamespaceRequest::Delete {
                        path: path.to_string(),
                        recursive,
                    },
                ),
                standard(),
            )
            .expect("namespace fingerprint serialization should not fail"),
        )
    }

    pub(crate) fn rename_fingerprint(src_path: &str, dst_path: &str, flags: u32) -> CommandFingerprint {
        Self::hash_fingerprint(
            encode_to_vec(
                (
                    COMMAND_FORMAT_VERSION,
                    CanonicalNamespaceRequest::Rename {
                        src_path: src_path.to_string(),
                        dst_path: dst_path.to_string(),
                        flags,
                    },
                ),
                standard(),
            )
            .expect("namespace fingerprint serialization should not fail"),
        )
    }

    fn hash_fingerprint(bytes: Vec<u8>) -> CommandFingerprint {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        let digest = hasher.finalize();
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&digest[..8]);
        CommandFingerprint(u64::from_be_bytes(buf))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum CanonicalNamespaceRequest {
    CreateDirectory {
        path: String,
        attrs: FileAttrs,
        recursive: bool,
    },
    CreateFile {
        path: String,
        attrs: FileAttrs,
        layout: FileLayout,
        mode: CreateFileMode,
    },
    Delete {
        path: String,
        recursive: bool,
    },
    Rename {
        src_path: String,
        dst_path: String,
        flags: u32,
    },
}

impl CanonicalNamespaceRequest {
    fn matches_mutation(&self, mutation: &Mutation) -> bool {
        match (self, mutation) {
            (
                Self::CreateDirectory {
                    attrs,
                    recursive: false,
                    ..
                },
                Mutation::Mkdir {
                    attrs: mutation_attrs, ..
                },
            ) => attrs == mutation_attrs,
            (
                Self::CreateDirectory {
                    attrs, recursive: true, ..
                },
                Mutation::CreateDirectory {
                    attrs: mutation_attrs, ..
                },
            ) => attrs == mutation_attrs,
            (
                Self::CreateFile {
                    attrs, layout, mode, ..
                },
                Mutation::CreateFile {
                    attrs: mutation_attrs,
                    layout: mutation_layout,
                    mode: mutation_mode,
                    ..
                },
            ) => attrs == mutation_attrs && layout == mutation_layout && mode == mutation_mode,
            (
                Self::Delete { recursive, .. },
                Mutation::Delete {
                    recursive: mutation_recursive,
                    ..
                },
            ) => recursive == mutation_recursive,
            (
                Self::Rename { flags, .. },
                Mutation::Rename {
                    flags: mutation_flags, ..
                },
            ) => flags == mutation_flags,
            _ => false,
        }
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
    CreateDirectory {
        root_inode_id: InodeId,
        components: Vec<String>,
        attrs: FileAttrs,
    },
    CreateFile {
        parent_inode_id: InodeId,
        name: String,
        attrs: FileAttrs,
        layout: FileLayout,
        mode: CreateFileMode,
    },
    Delete {
        parent_inode_id: InodeId,
        name: String,
        recursive: bool,
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
        extents: Vec<beryl_types::fs::Extent>,
        final_size: u64,
        lease_id: beryl_types::ids::LeaseId,
        open_epoch: u64,
        lease_epoch: u64,
        commit_mode: FileCommitMode,
    },
    SyncWrite {
        inode_id: InodeId,
        extents: Vec<beryl_types::fs::Extent>,
        target_size: u64,
        lease_id: beryl_types::ids::LeaseId,
        open_epoch: u64,
        lease_epoch: u64,
        commit_mode: FileCommitMode,
    },
    Truncate {
        inode_id: InodeId,
        new_size: u64,
        lease_id: beryl_types::ids::LeaseId,
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
    use beryl_types::ids::{BlockId, ClientId, DataHandleId};
    use beryl_types::CallId;
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
                extents: vec![beryl_types::fs::Extent {
                    file_offset: 0,
                    block_id: BlockId::new(DataHandleId::new(30), beryl_types::ids::BlockIndex::new(0)),
                    block_offset: 0,
                    len: 64,
                    file_version: None,
                    block_stamp: None,
                }],
                final_size: 64,
                lease_id: beryl_types::ids::LeaseId::new(40),
                open_epoch: 50,
                lease_epoch: 60,
                commit_mode,
            },
        )
    }

    fn canonical_create_command(dedup: DedupKey, parent_inode_id: InodeId, path: &str) -> Command {
        let attrs = FileAttrs::new();
        let layout = FileLayout::new(4096, 4096, 1);
        Command::new_namespace(
            dedup,
            100,
            CanonicalNamespaceRequest::CreateFile {
                path: path.to_string(),
                attrs: attrs.clone(),
                layout,
                mode: CreateFileMode::CreateNew,
            },
            Mutation::CreateFile {
                parent_inode_id,
                name: "file".to_string(),
                attrs,
                layout,
                mode: CreateFileMode::CreateNew,
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
    fn namespace_fingerprint_uses_canonical_path_not_resolved_inode_ids() {
        let first = canonical_create_command(dedup(7, 40), InodeId::new(10), "/dir/file");
        let same_request_after_namespace_change = canonical_create_command(dedup(7, 40), InodeId::new(20), "/dir/file");
        let different_path = canonical_create_command(dedup(7, 40), InodeId::new(20), "/other/file");

        assert_eq!(first.fingerprint(), same_request_after_namespace_change.fingerprint());
        assert_ne!(first.fingerprint(), different_path.fingerprint());
        assert_eq!(
            first.fingerprint(),
            Command::create_file_fingerprint(
                "/dir/file",
                &FileAttrs::new(),
                &FileLayout::new(4096, 4096, 1),
                CreateFileMode::CreateNew,
            )
        );
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
        let mkdir = Command::new(
            dedup(7, 5),
            100,
            Mutation::Mkdir {
                parent_inode_id: InodeId::new(10),
                name: "entry".to_string(),
                attrs: FileAttrs::new(),
            },
        );
        let delete = Command::new(
            dedup(7, 6),
            100,
            Mutation::Delete {
                parent_inode_id: InodeId::new(10),
                name: "entry".to_string(),
                recursive: false,
            },
        );
        assert_ne!(mkdir.fingerprint(), delete.fingerprint());
    }

    #[test]
    fn fingerprint_includes_commit_mode() {
        let replace = close_write_command(dedup(7, 7), FileCommitMode::Replace);
        let append = close_write_command(dedup(7, 7), FileCommitMode::Append);
        assert_ne!(replace.fingerprint(), append.fingerprint());
    }

    impl Command {
        pub fn call_id(&self) -> &beryl_types::CallId {
            &self.dedup.call_id
        }
    }
}
