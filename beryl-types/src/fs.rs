// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Filesystem (FS) domain types for inode/dentry-based metadata model.
//!
//! These types are the authoritative representation of filesystem metadata.
//! They are independent of transport (gRPC/proto) and storage (RocksDB) layers.

use crate::ids::{DataHandleId, MountId};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Inode identifier (64-bit).
///
/// Inodes are the authoritative identity for filesystem objects.
/// Each mount has a root inode, and all files/directories/symlinks have unique inodes.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
#[repr(transparent)]
pub struct InodeId(pub u64);

impl InodeId {
    /// Creates a new InodeId from a raw value.
    #[inline]
    pub const fn new(v: u64) -> Self {
        Self(v)
    }

    /// Returns the inner value.
    #[inline]
    pub const fn as_raw(self) -> u64 {
        self.0
    }

    /// Encodes as fixed-width big-endian bytes (8 bytes).
    /// Used for RocksDB key encoding.
    #[inline]
    pub fn to_be_bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    /// Decodes from fixed-width big-endian bytes (8 bytes).
    #[inline]
    pub fn from_be_bytes(bytes: [u8; 8]) -> Self {
        Self(u64::from_be_bytes(bytes))
    }
}

impl fmt::Debug for InodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("InodeId").field(&self.0).finish()
    }
}

impl fmt::Display for InodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for InodeId {
    #[inline]
    fn from(v: u64) -> Self {
        Self(v)
    }
}

impl From<InodeId> for u64 {
    #[inline]
    fn from(v: InodeId) -> Self {
        v.0
    }
}

/// Inode kind (file, directory, symlink).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InodeKind {
    /// Regular file.
    File,
    /// Directory.
    Dir,
    /// Symbolic link.
    Symlink,
}

impl InodeKind {
    /// Returns true if this is a directory.
    #[inline]
    pub fn is_dir(self) -> bool {
        matches!(self, InodeKind::Dir)
    }

    /// Returns true if this is a file.
    #[inline]
    pub fn is_file(self) -> bool {
        matches!(self, InodeKind::File)
    }

    /// Returns true if this is a symlink.
    #[inline]
    pub fn is_symlink(self) -> bool {
        matches!(self, InodeKind::Symlink)
    }
}

/// File attributes (metadata for inodes).
///
/// All timestamps are in milliseconds since Unix epoch.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileAttrs {
    /// File mode (permissions + type bits).
    pub mode: u32,
    /// User ID.
    pub uid: u32,
    /// Group ID.
    pub gid: u32,
    /// File size in bytes.
    pub size: u64,
    /// Access time (milliseconds since Unix epoch).
    pub atime_ms: u64,
    /// Modification time (milliseconds since Unix epoch).
    pub mtime_ms: u64,
    /// Change time (milliseconds since Unix epoch).
    pub ctime_ms: u64,
    /// Number of hard links.
    pub nlink: u32,
}

impl FileAttrs {
    /// Creates new file attributes with default values.
    pub fn new() -> Self {
        Self {
            mode: 0o644,
            uid: 0,
            gid: 0,
            size: 0,
            atime_ms: 0,
            mtime_ms: 0,
            ctime_ms: 0,
            nlink: 1,
        }
    }

    /// Updates timestamps to current time (in milliseconds).
    pub fn update_timestamps(&mut self, now_ms: u64) {
        self.atime_ms = now_ms;
        self.mtime_ms = now_ms;
        self.ctime_ms = now_ms;
    }

    /// Updates mtime and ctime.
    pub fn update_mtime_ctime(&mut self, now_ms: u64) {
        self.mtime_ms = now_ms;
        self.ctime_ms = now_ms;
    }

    /// Updates ctime only.
    pub fn update_ctime(&mut self, now_ms: u64) {
        self.ctime_ms = now_ms;
    }
}

impl Default for FileAttrs {
    fn default() -> Self {
        Self::new()
    }
}

/// File extent: maps file offset range to block (supports append-only write path).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Extent {
    /// File offset (start of this extent in the file).
    pub file_offset: u64,
    /// Block ID that contains this extent.
    pub block_id: crate::ids::BlockId,
    /// Offset within the block (where this extent starts in the block).
    pub block_offset: u64,
    /// Length of this extent in bytes.
    pub len: u64,
    /// File version for the committed file state that owns this extent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_revision: Option<u64>,
    /// Metadata-assigned block stamp for direct read validation.
    /// Readable committed extents must carry a non-zero value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_stamp: Option<u64>,
}

/// Inode data (variant-specific information).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InodeData {
    /// File inode data.
    /// Includes extents for the committed block map, content_revision for visible
    /// file state, and lease_epoch for lease management.
    File {
        /// File extents (block map).
        /// Supports append-only write path.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        extents: Vec<Extent>,
        /// Visible file state version.
        /// Advanced by authoritative metadata apply when committed content,
        /// size, data handle, or read-plan state changes.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_revision: Option<u64>,
        /// Lease epoch (monotonically increasing, for fencing).
        /// Persisted in inode for lease management.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        lease_epoch: Option<u64>,
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
    /// Returns the InodeKind for this data.
    pub fn kind(&self) -> InodeKind {
        match self {
            InodeData::File { .. } => InodeKind::File,
            InodeData::Dir => InodeKind::Dir,
            InodeData::Symlink { .. } => InodeKind::Symlink,
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
pub struct Inode {
    /// Inode ID.
    pub inode_id: InodeId,
    /// Inode kind.
    pub kind: InodeKind,
    /// File attributes.
    pub attrs: FileAttrs,
    /// Variant-specific data.
    pub data: InodeData,
    /// Mount ID: identifies which mount this inode belongs to.
    /// Root inode is set at mount creation; child inodes inherit from parent.
    /// Used for O(1) mount resolution during FS write routing.
    pub mount_id: MountId,
    /// Data handle for this inode (data-plane identity for the active data instance).
    /// This is the authoritative link from namespace (inode) to data-plane blocks.
    /// Non-file inodes should use DataHandleId::new(0) and ignore this field.
    pub data_handle_id: DataHandleId,
}

impl Inode {
    /// Creates a new inode with mount_id.
    pub fn new(
        inode_id: InodeId,
        kind: InodeKind,
        attrs: FileAttrs,
        mount_id: MountId,
        data_handle_id: DataHandleId,
    ) -> Self {
        let data = match kind {
            InodeKind::File => InodeData::File {
                extents: Vec::new(),
                content_revision: None,
                lease_epoch: None,
            },
            InodeKind::Dir => InodeData::Dir,
            InodeKind::Symlink => InodeData::Symlink { target: None },
        };
        Self {
            inode_id,
            kind,
            attrs,
            data,
            mount_id,
            data_handle_id,
        }
    }

    /// Creates a new file inode.
    pub fn new_file(inode_id: InodeId, attrs: FileAttrs, mount_id: MountId, data_handle_id: DataHandleId) -> Self {
        Self::new(inode_id, InodeKind::File, attrs, mount_id, data_handle_id)
    }

    /// Creates a new directory inode.
    pub fn new_dir(inode_id: InodeId, attrs: FileAttrs, mount_id: MountId) -> Self {
        Self::new(inode_id, InodeKind::Dir, attrs, mount_id, DataHandleId::new(0))
    }

    /// Creates a new symlink inode.
    pub fn new_symlink(inode_id: InodeId, attrs: FileAttrs, target: String, mount_id: MountId) -> Self {
        Self {
            inode_id,
            kind: InodeKind::Symlink,
            attrs,
            data: InodeData::Symlink { target: Some(target) },
            mount_id,
            data_handle_id: DataHandleId::new(0),
        }
    }
}

/// Filesystem error codes.
///
/// These map to standard POSIX error codes and are used in ResponseHeaderProto.error_code.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[repr(u32)]
pub enum FsErrorCode {
    /// No error (success).
    Ok = 0,
    /// No such file or directory.
    ENoEnt = 2,
    /// File exists.
    EExist = 17,
    /// Directory not empty.
    ENotEmpty = 39,
    /// Not a directory.
    ENotDir = 20,
    /// Is a directory.
    EIsDir = 21,
    /// Cross-device link (rename across mounts).
    EXDev = 18,
    /// Permission denied.
    EPerm = 1,
    /// Permission denied (access).
    EAcces = 13,
    /// Invalid argument.
    EInval = 22,
    /// Operation not supported.
    ENotsup = 45,
    /// Not implemented.
    ENotImpl = 38,
    /// Resource temporarily unavailable (e.g., lease conflict).
    EAgain = 11,
    /// Device or resource busy (e.g., file locked by another writer).
    EBusy = 16,
}

impl FsErrorCode {
    /// Converts to u32 for proto encoding.
    #[inline]
    pub fn as_u32(self) -> u32 {
        self as u32
    }

    /// Converts from u32 (for proto decoding).
    pub fn from_u32(v: u32) -> Option<Self> {
        match v {
            0 => Some(Self::Ok),
            2 => Some(Self::ENoEnt),
            17 => Some(Self::EExist),
            39 => Some(Self::ENotEmpty),
            20 => Some(Self::ENotDir),
            21 => Some(Self::EIsDir),
            18 => Some(Self::EXDev),
            1 => Some(Self::EPerm),
            13 => Some(Self::EAcces),
            22 => Some(Self::EInval),
            45 => Some(Self::ENotsup),
            38 => Some(Self::ENotImpl),
            11 => Some(Self::EAgain),
            16 => Some(Self::EBusy),
            _ => None,
        }
    }
}

/// Directory entry (name + inode reference).
///
/// Used in ReadDir responses.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirEntry {
    /// Entry name (UTF-8).
    pub name: String,
    /// Referenced inode ID.
    pub inode_id: InodeId,
    /// Inode kind (for quick filtering).
    pub kind: InodeKind,
    /// Optional attributes (for optimization, to avoid extra Lookup calls).
    pub attrs: Option<FileAttrs>,
}

impl DirEntry {
    /// Creates a new directory entry.
    pub fn new(name: String, inode_id: InodeId, kind: InodeKind) -> Self {
        Self {
            name,
            inode_id,
            kind,
            attrs: None,
        }
    }

    /// Creates a new directory entry with attributes.
    pub fn with_attrs(name: String, inode_id: InodeId, kind: InodeKind, attrs: FileAttrs) -> Self {
        Self {
            name,
            inode_id,
            kind,
            attrs: Some(attrs),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inode_id_encoding() {
        let id = InodeId::new(0x1234567890abcdef);
        let bytes = id.to_be_bytes();
        let decoded = InodeId::from_be_bytes(bytes);
        assert_eq!(id, decoded);
    }

    #[test]
    fn fs_error_code_conversion() {
        assert_eq!(FsErrorCode::ENoEnt.as_u32(), 2);
        assert_eq!(FsErrorCode::from_u32(2), Some(FsErrorCode::ENoEnt));
        assert_eq!(FsErrorCode::from_u32(999), None);
    }
}
