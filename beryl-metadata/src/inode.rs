// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Metadata-owned inode state and exact file-commit completion evidence.

use crate::error::{MetadataError, MetadataResult};
use beryl_types::{CallId, ClientId, ContentGeneration, Extent, FileAttrs, FileType, InodeId, LeaseEpoch, MountId};
use serde::{Deserialize, Serialize};

/// File publication precondition and merge behavior.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum PublishMode {
    /// Replace content only while the expected content generation is current.
    ReplaceIfUnchanged,
    /// Append content only while the expected content generation is current.
    AppendIfUnchanged,
}

/// Frozen business payload shared by publication validation and commit replay.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct FilePublication {
    pub(crate) extents: Vec<Extent>,
    pub(crate) target_size: u64,
    pub(crate) expected_generation: ContentGeneration,
    pub(crate) expected_file_size: u64,
    pub(crate) lease_epoch: LeaseEpoch,
    pub(crate) mode: PublishMode,
}

/// Latest completed CommitFile, stored inside the inode's existing RocksDB value.
///
/// The visible layout supplies the block payload for exact replay verification.
/// Content mutation retires this evidence; a later commit replaces it. Lease
/// changes alone preserve it so response loss can be resolved after a new open.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FileCommit {
    pub(crate) client_id: ClientId,
    pub(crate) call_id: CallId,
    pub(crate) lease_epoch: LeaseEpoch,
    pub(crate) expected_generation: ContentGeneration,
    pub(crate) expected_file_size: u64,
    pub(crate) mode: PublishMode,
    pub(crate) committed_size: u64,
    pub(crate) generation: ContentGeneration,
}

impl FilePublication {
    /// Confirm only this exact operation against one atomic inode read.
    /// Missing or superseded evidence never proves that an older call failed.
    pub(crate) fn resolve_commit(
        &self,
        inode: &Inode,
        client_id: ClientId,
        call_id: CallId,
    ) -> MetadataResult<Option<ContentGeneration>> {
        let InodeData::File {
            extents,
            generation,
            lease_epoch,
            last_commit,
            ..
        } = &inode.data
        else {
            return Err(MetadataError::InvalidArgument(
                "CommitFile requires a file inode".into(),
            ));
        };
        let Some(commit) = last_commit else {
            return Ok(None);
        };
        if commit.client_id != client_id || commit.call_id != call_id {
            return Ok(None);
        }
        if commit.generation != generation.unwrap_or_default()
            || commit.committed_size != inode.attrs.size
            || commit
                .lease_epoch
                .checked_next()
                .is_none_or(|ended| lease_epoch.unwrap_or_default() < ended)
        {
            return Err(MetadataError::Internal(
                "CommitFile evidence disagrees with its inode".into(),
            ));
        }
        let start = match self.mode {
            PublishMode::ReplaceIfUnchanged => 0,
            PublishMode::AppendIfUnchanged => self.expected_file_size,
        };
        let mut requested = self.extents.iter().collect::<Vec<_>>();
        requested.sort_by_key(|extent| extent.file_offset);
        let visible = extents
            .iter()
            .filter(|extent| extent.file_offset >= start)
            .collect::<Vec<_>>();
        let exact_blocks = requested.len() == visible.len()
            && requested.iter().zip(visible).all(|(a, b)| {
                a.block_id == b.block_id
                    && a.file_offset == b.file_offset
                    && a.block_offset == b.block_offset
                    && a.len == b.len
            });
        if commit.lease_epoch != self.lease_epoch
            || commit.expected_generation != self.expected_generation
            || commit.expected_file_size != self.expected_file_size
            || commit.mode != self.mode
            || commit.committed_size != self.target_size
            || !exact_blocks
        {
            return Err(MetadataError::InvalidArgument(
                "CommitFile payload changed for a completed operation".into(),
            ));
        }
        Ok(Some(commit.generation))
    }
}

/// Inode data (variant-specific information).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum InodeData {
    /// File inode data.
    /// Includes extents for the committed block map, generation for visible
    /// file state, lease_epoch for lease management, and the next durable block
    /// ordinal reserved for this file inode.
    File {
        /// File extents (block map).
        /// Supports append-only write path.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        extents: Vec<Extent>,
        /// Generation of the currently visible file contents.
        /// Advanced by authoritative metadata apply when committed content,
        /// size or read-plan state changes.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        generation: Option<ContentGeneration>,
        /// Lease epoch (monotonically increasing, for fencing).
        /// Persisted in inode for lease management.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        lease_epoch: Option<LeaseEpoch>,
        /// Next block ordinal to allocate for this file.
        /// This counter is monotonic and is not derived from visible extents.
        next_block_index: u64,
        /// Exact latest close result; never independent of the visible content.
        #[serde(skip_serializing_if = "Option::is_none")]
        last_commit: Option<FileCommit>,
    },
    /// Directory inode data.
    /// Payload intentionally empty; entries live in dentry/direntry index.
    Dir,
    /// Symlink inode data.
    /// Placeholder for target path.
    Symlink {
        /// Placeholder: future target path.
        #[serde(skip_serializing_if = "Option::is_none")]
        target: Option<String>,
    },
}

impl InodeData {
    /// Returns the FileType for this data.
    pub(crate) fn kind(&self) -> FileType {
        match self {
            InodeData::File { .. } => FileType::File,
            InodeData::Dir => FileType::Dir,
            InodeData::Symlink { .. } => FileType::Symlink,
        }
    }
}

/// Inode (filesystem object).
///
/// This is the authoritative representation of a filesystem object.
/// Each inode has a unique ID, kind, attributes, and optional variant-specific data.
///
/// Mount_id allows O(1) mount resolution during FS write routing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Inode {
    /// Inode ID.
    pub(crate) inode_id: InodeId,
    /// Inode kind.
    pub(crate) kind: FileType,
    /// File attributes.
    pub(crate) attrs: FileAttrs,
    /// Variant-specific data.
    pub(crate) data: InodeData,
    /// Mount ID: identifies which mount this inode belongs to.
    /// Root inode is set at mount creation; child inodes inherit from parent.
    /// Used for O(1) mount resolution during FS write routing.
    pub(crate) mount_id: MountId,
}

impl Inode {
    /// Creates a new inode with mount_id.
    pub(crate) fn new(inode_id: InodeId, kind: FileType, attrs: FileAttrs, mount_id: MountId) -> Self {
        let data = match kind {
            FileType::File => InodeData::File {
                extents: Vec::new(),
                generation: None,
                lease_epoch: None,
                next_block_index: 0,
                last_commit: None,
            },
            FileType::Dir => InodeData::Dir,
            FileType::Symlink => InodeData::Symlink { target: None },
        };
        Self {
            inode_id,
            kind,
            attrs,
            data,
            mount_id,
        }
    }

    /// Creates a new file inode.
    pub(crate) fn new_file(inode_id: InodeId, attrs: FileAttrs, mount_id: MountId) -> Self {
        Self::new(inode_id, FileType::File, attrs, mount_id)
    }

    /// Creates a new directory inode.
    pub(crate) fn new_dir(inode_id: InodeId, attrs: FileAttrs, mount_id: MountId) -> Self {
        Self::new(inode_id, FileType::Dir, attrs, mount_id)
    }
}
