// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Public filesystem operation options.

use types::BlockFormatId;

pub(crate) const DEFAULT_BLOCK_SIZE: u32 = 64 * 1024 * 1024;
pub(crate) const DEFAULT_CHUNK_SIZE: u32 = 4 * 1024 * 1024;
pub(crate) const DEFAULT_REPLICATION: u32 = 1;
pub(crate) const MAX_PREALLOCATED_WRITE_BLOCKS: u64 = 10;

/// Options for creating a file write session and, for new files only, proposing a `FileLayout`.
///
/// Metadata validates and persists the accepted layout. Existing files opened
/// for append do not use these create-time layout fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CreateOptions {
    /// Creation behavior for the target path.
    pub create_mode: CreateMode,

    /// Block data/meta interpretation format for newly created files.
    pub block_format_id: BlockFormatId,

    /// Logical block size in bytes for newly created files.
    pub block_size: u32,

    /// Logical chunk size in bytes for newly created files.
    pub chunk_size: u32,
}

impl Default for CreateOptions {
    fn default() -> Self {
        Self::create()
    }
}

impl CreateOptions {
    /// Return options that create a new file and fail if it already exists.
    pub fn create() -> Self {
        Self {
            create_mode: CreateMode::CreateNew,
            block_format_id: BlockFormatId::CURRENT_FOR_NEW_FILE,
            block_size: DEFAULT_BLOCK_SIZE,
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }

    /// Return options that replace the file contents or create it if absent.
    pub fn overwrite() -> Self {
        Self {
            create_mode: CreateMode::CreateOrOverwrite,
            block_format_id: BlockFormatId::CURRENT_FOR_NEW_FILE,
            block_size: DEFAULT_BLOCK_SIZE,
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }

    /// Set the block format id proposed for a newly created file.
    pub fn with_block_format_id(mut self, block_format_id: BlockFormatId) -> Self {
        self.block_format_id = block_format_id;
        self
    }

    /// Set the block size proposed for a newly created file.
    pub fn with_block_size(mut self, block_size: u32) -> Self {
        self.block_size = block_size;
        self
    }

    /// Set the chunk size proposed for a newly created file.
    pub fn with_chunk_size(mut self, chunk_size: u32) -> Self {
        self.chunk_size = chunk_size;
        self
    }
}

/// Options for listing a directory through [`FsClient::list`](crate::FsClient::list).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ListOptions {
    /// Whether the listing should recursively include descendants.
    pub recursive: bool,

    /// Opaque cursor returned by a previous listing page.
    pub cursor: Option<Vec<u8>>,

    /// Maximum number of entries to return. `None` lets metadata choose.
    pub limit: Option<u32>,
}

/// Creation behavior for [`CreateOptions`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CreateMode {
    /// Create a new file and fail if the target path already exists.
    #[default]
    CreateNew,

    /// Create the file if it does not exist, or replace the existing file contents.
    CreateOrOverwrite,
}
