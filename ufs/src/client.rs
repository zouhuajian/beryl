// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! UFS client trait for reading chunks by DataHandleId/BlockId/ChunkIndex.

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use common::header::RequestHeader;
use types::ids::{BlockId, ChunkIndex, DataHandleId};
use types::layout::FileLayout;

use crate::error::UfsError;
use crate::traits::UfsData;

/// Client interface for reading chunks from UFS by logical identifiers.
///
/// This trait provides a higher-level interface than the path-based UfsData trait,
/// allowing clients to read chunks using DataHandleId, BlockId, and ChunkIndex.
#[async_trait]
pub trait UfsClient: Send + Sync {
    /// Read a chunk from UFS using logical identifiers.
    ///
    /// The implementation should convert the logical identifiers to a file path
    /// and use the underlying UFS to read the chunk data.
    async fn read_chunk(
        &self,
        data_handle_id: DataHandleId,
        block_id: BlockId,
        chunk_idx: ChunkIndex,
        layout: &FileLayout,
    ) -> Result<Bytes, UfsError>;
}

/// Adapter that implements UfsClient using a path-based UFS implementation.
pub struct UfsClientAdapter {
    ufs: Box<dyn UfsData + Send + Sync>,
    base_path: String,
}

impl UfsClientAdapter {
    /// Create a new adapter from a UFS implementation and base path.
    pub fn new(ufs: Box<dyn UfsData + Send + Sync>, base_path: String) -> Self {
        Self { ufs, base_path }
    }

    /// Convert logical identifiers to a file path.
    ///
    /// Path format: `{base_path}/{data_handle_id}`
    /// We read from the file at the appropriate offset based on block and chunk indices.
    fn file_path(&self, data_handle_id: DataHandleId) -> String {
        format!("{}/{}", self.base_path, data_handle_id.as_raw())
    }
}

#[async_trait]
impl UfsClient for UfsClientAdapter {
    async fn read_chunk(
        &self,
        data_handle_id: DataHandleId,
        block_id: BlockId,
        chunk_idx: ChunkIndex,
        layout: &FileLayout,
    ) -> Result<Bytes, UfsError> {
        let path = self.file_path(data_handle_id);

        // Calculate the absolute offset in the file
        let block_start = layout.block_start_offset(block_id.index);
        let chunk_start_in_block = layout.chunk_start_offset_in_block(chunk_idx) as u64;
        let offset = block_start + chunk_start_in_block;
        let len = layout.chunk_size as usize;

        let ctx = RequestHeader::new(types::ClientId::generate());

        self.ufs.read_range(&path, offset, len, &ctx).await
    }
}

/// Mock UFS client for testing.
///
/// Returns zero-filled data with the correct chunk size.
pub struct MockUfsClient;

#[async_trait]
impl UfsClient for MockUfsClient {
    async fn read_chunk(
        &self,
        _data_handle_id: DataHandleId,
        _block_id: BlockId,
        _chunk_idx: ChunkIndex,
        layout: &FileLayout,
    ) -> Result<Bytes, UfsError> {
        // Return mock data with correct chunk size
        Ok(Bytes::from(vec![0u8; layout.chunk_size as usize]))
    }
}
