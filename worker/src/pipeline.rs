// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Data pipeline: Stream ↔ Chunk mapping with chunk merging.
//!
//! This module implements the pipeline layer that:
//! - Converts transport Stream to Chunks (for writes)
//! - Converts Chunks to transport Stream (for reads)
//! - Merges small chunks to reduce I/O fragmentation
//! - Handles backpressure and flow control

use anyhow::Result;
use bytes::Bytes;
use futures::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, BufReader};

use types::ids::{BlockId, ChunkIndex};
use types::layout::FileLayout;

/// Chunk merger: merges small chunks into larger chunks to reduce I/O fragmentation.
///
/// Strategy:
/// - Storage chunk size (e.g., 128KB) is the range granularity
/// - Pipeline merges multiple storage chunks into larger chunks (e.g., 1MB) for I/O
/// - This decouples file count from chunk_size and reduces I/O overhead
pub struct ChunkMerger {
    /// Target merged chunk size (e.g., 1MB).
    target_size: u32,
    /// Current buffer accumulating chunks.
    buffer: Vec<u8>,
    /// Current merged chunk index.
    merged_chunk_idx: u32,
}

impl ChunkMerger {
    /// Create a new chunk merger.
    pub fn new(target_size: u32) -> Self {
        Self {
            target_size,
            buffer: Vec::with_capacity(target_size as usize),
            merged_chunk_idx: 0,
        }
    }

    /// Add a chunk to the merger.
    /// Returns merged chunks if buffer reaches or exceeds target size, None if still accumulating.
    pub fn add_chunk(&mut self, chunk_data: Bytes) -> Option<Bytes> {
        // If adding this chunk would exceed target size, flush buffer first.
        if !self.buffer.is_empty() && self.buffer.len() + chunk_data.len() > self.target_size as usize {
            let merged = Bytes::from(std::mem::take(&mut self.buffer));
            self.buffer.extend_from_slice(&chunk_data);
            self.merged_chunk_idx += 1;
            return Some(merged);
        }

        // Otherwise keep accumulating. We only flush on overflow, not on exact hit,
        // so callers should call `flush()` after the last chunk.
        self.buffer.extend_from_slice(&chunk_data);
        None
    }

    /// Flush remaining buffer (call when done adding chunks).
    pub fn flush(&mut self) -> Option<Bytes> {
        if !self.buffer.is_empty() {
            Some(Bytes::from(std::mem::take(&mut self.buffer)))
        } else {
            None
        }
    }

    /// Reset the merger for a new block.
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.merged_chunk_idx = 0;
    }
}

/// Stream to chunk converter: converts a byte stream into chunks.
///
/// This handles the Stream → Chunk mapping for write operations.
pub struct StreamToChunkConverter {
    reader: Pin<Box<dyn AsyncRead + Send>>,
    chunk_size: u32,
    buffer: Vec<u8>,
    done: bool,
}

impl StreamToChunkConverter {
    /// Create a new stream to chunk converter.
    pub fn new(reader: impl AsyncRead + Send + 'static, chunk_size: u32) -> Self {
        Self {
            reader: Box::pin(BufReader::new(reader)),
            chunk_size,
            buffer: vec![0u8; chunk_size as usize],
            done: false,
        }
    }

    /// Read next chunk from stream.
    pub async fn next_chunk(&mut self) -> Result<Option<Bytes>> {
        if self.done {
            return Ok(None);
        }

        use tokio::io::AsyncReadExt;
        let n = self.reader.read(&mut self.buffer).await?;

        if n == 0 {
            self.done = true;
            Ok(None)
        } else {
            Ok(Some(Bytes::copy_from_slice(&self.buffer[..n])))
        }
    }
}

/// Chunk to stream converter: converts chunks into a byte stream.
///
/// This handles the Chunk → Stream mapping for read operations.
pub struct ChunkToStreamConverter {
    chunks: Vec<Bytes>,
    current_chunk_idx: usize,
}

impl ChunkToStreamConverter {
    /// Create a new chunk to stream converter.
    pub fn new(chunks: Vec<Bytes>) -> Self {
        Self {
            chunks,
            current_chunk_idx: 0,
        }
    }
}

impl Stream for ChunkToStreamConverter {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.current_chunk_idx >= self.chunks.len() {
            return Poll::Ready(None);
        }

        let chunk = self.chunks[self.current_chunk_idx].clone();
        self.current_chunk_idx += 1;

        Poll::Ready(Some(Ok(chunk)))
    }
}

/// Pipeline helper: converts a range within a block to chunk ranges.
///
/// This is the unified function that should be used by all read/write paths.
pub fn range_to_chunks(layout: &FileLayout, block_id: BlockId, offset: u32, len: u32) -> Vec<(ChunkIndex, u32, u32)> {
    layout.range_to_chunks(block_id, offset, len)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[tokio::test]
    async fn test_chunk_merger() {
        let mut merger = ChunkMerger::new(1024 * 1024); // 1MB target

        // Add small chunks (128KB each)
        for _ in 0..8 {
            let chunk = Bytes::from(vec![1u8; 128 * 1024]);
            assert!(merger.add_chunk(chunk).is_none());
        }

        // Adding 9th chunk should trigger flush
        let chunk = Bytes::from(vec![2u8; 128 * 1024]);
        let merged = merger.add_chunk(chunk);
        assert!(merged.is_some());
        assert_eq!(merged.unwrap().len(), 1024 * 1024); // 1MB

        // Flush remaining
        let remaining = merger.flush();
        assert!(remaining.is_some());
        assert_eq!(remaining.unwrap().len(), 128 * 1024); // 128KB
    }

    #[tokio::test]
    async fn test_stream_to_chunk_converter() {
        let data = vec![1u8; 2 * 1024 * 1024]; // 2MB
        let reader = Cursor::new(data);
        let mut converter = StreamToChunkConverter::new(reader, 1024 * 1024); // 1MB chunks

        let chunk1 = converter.next_chunk().await.unwrap();
        assert!(chunk1.is_some());
        assert_eq!(chunk1.unwrap().len(), 1024 * 1024);

        let chunk2 = converter.next_chunk().await.unwrap();
        assert!(chunk2.is_some());
        assert_eq!(chunk2.unwrap().len(), 1024 * 1024);

        let chunk3 = converter.next_chunk().await.unwrap();
        assert!(chunk3.is_none());
    }
}
