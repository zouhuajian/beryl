// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Shared filesystem attributes, write modes, and visible extent types.
//!
//! Metadata owns persisted inode state; these domain values remain independent
//! of transport (gRPC/proto) and storage (RocksDB) layers.

use crate::ids::BlockId;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter, Result};

/// Write behavior selected when opening a file session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteMode {
    /// Replace the currently visible file contents when publication succeeds.
    Overwrite,
    /// Append after the currently visible file contents.
    Append,
}

/// Change counter for the currently visible content of one inode.
///
/// Compare generations only within the same inode. Metadata advances this value
/// when visible content changes; zero is the initial generation. It does not
/// identify retained historical data, a writer lease, or a physical block.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ContentGeneration(u64);

impl ContentGeneration {
    /// Wraps a generation from persisted state or a protocol boundary.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the scalar used by storage and wire encodings.
    pub const fn as_raw(self) -> u64 {
        self.0
    }

    /// Returns the next generation, or None on exhaustion; never wraps.
    pub fn checked_next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

impl Display for ContentGeneration {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        self.0.fmt(f)
    }
}

/// Largest number of extents stored in one file inode by the inline layout.
///
/// This fixed ceiling bounds replicated publication and inode serialization.
/// Files that need more extents require paged extent storage rather than a
/// larger inline vector.
pub const MAX_FILE_EXTENTS: usize = 10_000;

/// Payload-free namespace type tag, independent of inode storage layout.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileType {
    /// Regular file.
    File,
    /// Directory.
    Dir,
    /// Symbolic link.
    Symlink,
}

impl FileType {
    /// Returns true if this is a directory.
    #[inline]
    pub fn is_dir(self) -> bool {
        matches!(self, FileType::Dir)
    }

    /// Returns true if this is a file.
    #[inline]
    pub fn is_file(self) -> bool {
        matches!(self, FileType::File)
    }

    /// Returns true if this is a symlink.
    #[inline]
    pub fn is_symlink(self) -> bool {
        matches!(self, FileType::Symlink)
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
    pub block_id: BlockId,
    /// Offset within the block (where this extent starts in the block).
    pub block_offset: u64,
    /// Length of this extent in bytes.
    pub len: u64,
    /// Content generation for the committed file state that owns this extent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation: Option<ContentGeneration>,
    /// Metadata-assigned block stamp for direct read validation.
    /// Readable committed extents must carry a non-zero value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block_stamp: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lease::LeaseEpoch;

    #[test]
    fn file_counters_preserve_scalar_encoding_and_never_wrap() {
        for raw in [0, 1, u64::MAX - 1, u64::MAX] {
            let generation = ContentGeneration::new(raw);
            let epoch = LeaseEpoch::new(raw);
            let encoded = raw.to_string();
            assert_eq!(serde_json::to_string(&generation).unwrap(), encoded);
            assert_eq!(serde_json::to_string(&epoch).unwrap(), encoded);
            assert_eq!(serde_json::from_str::<ContentGeneration>(&encoded).unwrap(), generation);
            assert_eq!(serde_json::from_str::<LeaseEpoch>(&encoded).unwrap(), epoch);
            assert_eq!(
                generation.checked_next().map(ContentGeneration::as_raw),
                raw.checked_add(1)
            );
            assert_eq!(epoch.checked_next().map(LeaseEpoch::as_raw), raw.checked_add(1));
        }
    }
}
