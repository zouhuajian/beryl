// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Public namespace status snapshots.

use types::{FileAttrs, InodeId, InodeKind};

use crate::error::{ClientError, ClientResult};

fn inode_kind_from_proto(kind: i32) -> Option<InodeKind> {
    proto::fs::InodeKindProto::try_from(kind).ok()?.try_into().ok()
}

/// Public file or directory status returned by [`crate::FsClient::stat`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileStatus {
    path: String,
    /// Stable metadata inode identity for the namespace entry.
    pub inode_id: InodeId,
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
        let inode_id = response
            .inode_id
            .ok_or_else(|| ClientError::Metadata("GetStatusResponseProto.inode_id missing".to_string()))?;
        Ok(Self {
            path: path.to_string(),
            inode_id: InodeId::new(inode_id.value),
            attrs: attrs.into(),
        })
    }
}

/// Public directory entry returned by [`crate::FsClient::list`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirectoryEntry {
    /// Entry name relative to the listed directory.
    pub name: String,
    /// Entry kind when supplied by metadata.
    pub kind: Option<InodeKind>,
    /// Entry attributes when supplied by metadata.
    pub attrs: Option<FileAttrs>,
}

impl DirectoryEntry {
    fn from_proto(entry: proto::fs::DirEntryProto) -> Self {
        Self {
            name: entry.name,
            kind: inode_kind_from_proto(entry.kind),
            attrs: entry.attrs.map(Into::into),
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

#[cfg(test)]
mod tests {
    use super::*;
    use types::InodeKind;

    fn dir_entry(name: &str, kind: i32) -> proto::fs::DirEntryProto {
        proto::fs::DirEntryProto {
            name: name.to_string(),
            inode_id: None,
            kind,
            attrs: None,
        }
    }

    #[test]
    fn directory_entry_unspecified_and_unknown_kind_map_to_none() {
        let unspecified = DirectoryEntry::from_proto(dir_entry(
            "unspecified",
            proto::fs::InodeKindProto::InodeKindUnspecified as i32,
        ));
        let unknown = DirectoryEntry::from_proto(dir_entry("unknown", 99));

        assert_eq!(unspecified.kind, None);
        assert_eq!(unknown.kind, None);
    }

    #[test]
    fn directory_entry_known_kinds_map_to_domain_inode_kind() {
        let cases = [
            (proto::fs::InodeKindProto::InodeKindFile, Some(InodeKind::File)),
            (proto::fs::InodeKindProto::InodeKindDir, Some(InodeKind::Dir)),
            (proto::fs::InodeKindProto::InodeKindSymlink, Some(InodeKind::Symlink)),
        ];

        for (proto_kind, expected_kind) in cases {
            let entry = DirectoryEntry::from_proto(dir_entry("entry", proto_kind as i32));
            assert_eq!(entry.kind, expected_kind);
        }
    }
}
