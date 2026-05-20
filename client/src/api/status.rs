// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Public namespace status snapshots.

use proto::fs::InodeKindProto;

use crate::error::{ClientError, ClientResult};

/// User-visible file attributes returned by namespace APIs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileAttrs {
    /// File mode bits.
    pub mode: u32,
    /// Owner user id.
    pub uid: u32,
    /// Owner group id.
    pub gid: u32,
    /// File length in bytes.
    pub size: u64,
    /// Last access time in milliseconds since Unix epoch.
    pub atime_ms: u64,
    /// Last modification time in milliseconds since Unix epoch.
    pub mtime_ms: u64,
    /// Last metadata change time in milliseconds since Unix epoch.
    pub ctime_ms: u64,
    /// Number of hard links.
    pub nlink: u32,
}

impl FileAttrs {
    pub(crate) fn from_proto(attrs: proto::fs::FileAttrsProto) -> Self {
        Self {
            mode: attrs.mode,
            uid: attrs.uid,
            gid: attrs.gid,
            size: attrs.size,
            atime_ms: attrs.atime_ms,
            mtime_ms: attrs.mtime_ms,
            ctime_ms: attrs.ctime_ms,
            nlink: attrs.nlink,
        }
    }
}

/// User-visible inode kind for directory entries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileKind {
    /// Regular file.
    File,
    /// Directory.
    Directory,
    /// Symbolic link.
    Symlink,
}

impl FileKind {
    fn from_proto(kind: i32) -> Option<Self> {
        match InodeKindProto::try_from(kind).ok()? {
            InodeKindProto::InodeKindFile => Some(Self::File),
            InodeKindProto::InodeKindDir => Some(Self::Directory),
            InodeKindProto::InodeKindSymlink => Some(Self::Symlink),
            InodeKindProto::InodeKindUnspecified => None,
        }
    }
}

/// Public file or directory status returned by [`crate::FsClient::stat`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileStatus {
    path: String,
    /// User-visible attributes for the namespace entry.
    pub attrs: FileAttrs,
}

impl FileStatus {
    /// Return the namespace path that was queried.
    pub fn path(&self) -> &str {
        &self.path
    }

    pub(crate) fn from_proto(path: &str, response: proto::metadata::GetStatusResponseProto) -> ClientResult<Self> {
        let attrs = response
            .attrs
            .ok_or_else(|| ClientError::Metadata("GetStatusResponseProto.attrs missing".to_string()))?;
        Ok(Self {
            path: path.to_string(),
            attrs: FileAttrs::from_proto(attrs),
        })
    }
}

/// Public directory entry returned by [`crate::FsClient::list`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirectoryEntry {
    /// Entry name relative to the listed directory.
    pub name: String,
    /// Entry kind when supplied by metadata.
    pub kind: Option<FileKind>,
    /// Entry attributes when supplied by metadata.
    pub attrs: Option<FileAttrs>,
}

impl DirectoryEntry {
    fn from_proto(entry: proto::fs::DirEntryProto) -> Self {
        Self {
            name: entry.name,
            kind: FileKind::from_proto(entry.kind),
            attrs: entry.attrs.map(FileAttrs::from_proto),
        }
    }
}

/// Public directory listing returned by [`crate::FsClient::list`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirectoryListing {
    path: String,
    /// Entries returned for the directory.
    pub entries: Vec<DirectoryEntry>,
    /// Opaque cursor for continuing a paginated listing when metadata returns one.
    pub next_cursor: Option<Vec<u8>>,
    /// Whether metadata reported the listing as complete.
    pub eof: bool,
}

impl DirectoryListing {
    /// Return the namespace path that was listed.
    pub fn path(&self) -> &str {
        &self.path
    }

    pub(crate) fn from_proto(path: &str, response: proto::metadata::ListStatusResponseProto) -> Self {
        let next_cursor = if response.next_cursor.is_empty() {
            None
        } else {
            Some(response.next_cursor)
        };
        Self {
            path: path.to_string(),
            entries: response.entries.into_iter().map(DirectoryEntry::from_proto).collect(),
            next_cursor,
            eof: response.eof,
        }
    }
}
