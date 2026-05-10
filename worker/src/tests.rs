// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Unit tests for worker components.

#[cfg(test)]
mod tests {
    use crate::block_store::BlockStore;
    use crate::ufs_fill::UfsFiller;
    use crate::volume_manager::VolumeManager;
    use bytes::Bytes;
    use std::sync::Arc;
    use tempfile::TempDir;
    use types::chunk::{ByteRange, ChunkRef, ChunkSlice};
    use types::ids::{BlockId, BlockIndex, ChunkIndex, DataHandleId, ShardGroupId};
    use types::layout::FileLayout;
    use types::ClientId;

    fn create_test_layout() -> FileLayout {
        FileLayout::new(32 * 1024 * 1024, 1024 * 1024, 3) // 32MB blocks, 1MB chunks
    }

    #[tokio::test]
    async fn test_block_store_basic() {
        let layout = create_test_layout();
        let temp_dir = TempDir::new().unwrap();
        let block_store_dir = temp_dir.path().join("block_store");
        std::fs::create_dir_all(&block_store_dir).unwrap();

        let volume_manager = Arc::new(VolumeManager::new());
        volume_manager.open_volumes(&[block_store_dir.clone()]).unwrap();
        let block_store = Arc::new(BlockStore::new(
            volume_manager,
            block_store_dir.join("manifest.json"),
            layout.block_size,
            layout.chunk_size,
        ));
        block_store.init().await.unwrap();

        let data_handle_id = DataHandleId::new(1);
        let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        let chunk_idx = ChunkIndex::new(0);
        let group_id = ShardGroupId::new(0);

        // Write chunk
        let data = Bytes::from(vec![1u8; 1024 * 1024]);
        let chunk_ref = ChunkRef::new(block_id, chunk_idx.as_raw());
        let mut stream = tokio::io::BufReader::new(std::io::Cursor::new(data.clone()));
        block_store
            .write_chunk_stream(group_id, chunk_ref, &mut stream)
            .await
            .unwrap();

        // Read chunk
        let slice = ChunkSlice {
            chunk: ChunkRef::new(block_id, chunk_idx.as_raw()),
            offset_in_chunk: 0,
            len: layout.chunk_size,
        };
        let read_data = block_store.read_chunk_stream(group_id, slice).await.unwrap().unwrap();
        assert_eq!(read_data, data);

        // Check presence
        assert!(block_store.has_chunk(group_id, block_id, chunk_idx));
    }

    #[tokio::test]
    async fn test_block_store_slice_read() {
        let layout = create_test_layout();
        let temp_dir = TempDir::new().unwrap();
        let block_store_dir = temp_dir.path().join("block_store");
        std::fs::create_dir_all(&block_store_dir).unwrap();

        let volume_manager = Arc::new(VolumeManager::new());
        volume_manager.open_volumes(&[block_store_dir.clone()]).unwrap();
        let block_store = Arc::new(BlockStore::new(
            volume_manager,
            block_store_dir.join("manifest.json"),
            layout.block_size,
            layout.chunk_size,
        ));
        block_store.init().await.unwrap();

        let data_handle_id = DataHandleId::new(1);
        let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        let chunk_idx = ChunkIndex::new(0);
        let group_id = ShardGroupId::new(0);

        // Write chunk with known pattern
        let mut data = vec![0u8; 1024 * 1024];
        for i in 0..data.len() {
            data[i] = (i % 256) as u8;
        }
        let chunk_ref = ChunkRef::new(block_id, chunk_idx.as_raw());
        let mut stream = tokio::io::BufReader::new(std::io::Cursor::new(Bytes::from(data)));
        block_store
            .write_chunk_stream(group_id, chunk_ref, &mut stream)
            .await
            .unwrap();

        // Read slice
        let slice = ChunkSlice {
            chunk: ChunkRef::new(block_id, chunk_idx.as_raw()),
            offset_in_chunk: 100,
            len: 50,
        };
        let slice_data = block_store.read_chunk_stream(group_id, slice).await.unwrap().unwrap();
        assert_eq!(slice_data.len(), 50);
        assert_eq!(slice_data[0], 100u8);
    }

    #[test]
    fn test_layout_range_split() {
        let layout = create_test_layout();
        let data_handle_id = DataHandleId::new(1);

        // Test range that spans multiple chunks
        let range = ByteRange {
            offset: 500_000, // Start in middle of first chunk
            len: 2_500_000,  // Span 3 chunks
        };

        let slices = layout.split_range_to_chunk_slices(data_handle_id, range);
        assert_eq!(slices.len(), 3);

        // First slice should start at offset 500_000 in chunk 0
        assert_eq!(slices[0].chunk.chunk_idx, 0);
        assert_eq!(slices[0].offset_in_chunk, 500_000);

        // Last slice should end at offset 3_000_000
        let last_slice = &slices[slices.len() - 1];
        assert_eq!(last_slice.chunk.chunk_idx, 2);
    }

    #[test]
    fn test_block_index_calculation() {
        let layout = create_test_layout();
        let block_size = 32 * 1024 * 1024; // 33_554_432 bytes

        // Block 0: 0..32MB
        assert_eq!(layout.block_index_from_offset(0).as_raw(), 0);
        assert_eq!(layout.block_index_from_offset(16_000_000).as_raw(), 0);
        assert_eq!(layout.block_index_from_offset(block_size - 1).as_raw(), 0);

        // Block 1: 32MB..64MB
        assert_eq!(layout.block_index_from_offset(block_size).as_raw(), 1);
        assert_eq!(layout.block_index_from_offset(block_size + 1).as_raw(), 1);
        assert_eq!(layout.block_index_from_offset(48_000_000).as_raw(), 1);
    }

    #[tokio::test]
    async fn test_ufs_filler() {
        use crate::block_store::BlockStore;
        use crate::volume_manager::VolumeManager;
        use common::audit::AuditLogger;
        use common::header::RequestHeader;
        use std::io::Write;
        use tempfile::TempDir;
        use types::chunk::{ChunkRef, ChunkSlice};
        use types::ids::ShardGroupId;
        use types::ClientId;
        use ufs::{BackendConfig, BackendKind, FsConfig, UfsId, UfsRegistry, UfsSpec};

        let layout = create_test_layout();

        // Create temporary directories
        let temp_dir = TempDir::new().unwrap();
        let ufs_root = temp_dir.path().join("ufs");
        std::fs::create_dir_all(&ufs_root).unwrap();
        let block_store_dir = temp_dir.path().join("block_store");
        std::fs::create_dir_all(&block_store_dir).unwrap();
        let audit_dir = temp_dir.path().join("audit");
        std::fs::create_dir_all(&audit_dir).unwrap();

        // Create UfsRegistry with a filesystem backend
        let ufs_registry = Arc::new(UfsRegistry::new());
        let ufs_spec = UfsSpec::new(
            "test-ufs",
            BackendKind::Fs,
            BackendConfig::Fs(FsConfig {
                root: ufs_root.to_string_lossy().to_string(),
            }),
        );
        ufs_registry.upsert(ufs_spec).unwrap();

        // Create BlockStore
        let volume_manager = Arc::new(VolumeManager::new());
        volume_manager.open_volumes(&[block_store_dir.clone()]).unwrap();
        let block_store = Arc::new(BlockStore::new(
            volume_manager,
            block_store_dir.join("manifest.json"),
            layout.block_size,
            layout.chunk_size,
        ));
        block_store.init().await.unwrap();

        // Create AuditLogger
        let audit_logger = Arc::new(AuditLogger::new(&audit_dir).unwrap());

        // Create UfsFiller
        let filler = UfsFiller::new(
            ufs_registry,
            block_store.clone(),
            audit_logger,
            layout,
            Some(UfsId::new("test-ufs")),
            10,    // max_concurrent_per_ufs
            5000,  // ufs_timeout_ms
            false, // async_fill
        );
        filler.init_limiters(10);

        let data_handle_id = DataHandleId::new(1);
        // Seed UFS with a chunk so read-through has content.
        let ufs_file_path = ufs_root.join(data_handle_id.as_raw().to_string());
        let mut ufs_file = std::fs::File::create(&ufs_file_path).unwrap();
        let test_data = vec![7u8; layout.chunk_size as usize];
        ufs_file.write_all(&test_data).unwrap();
        ufs_file.sync_all().unwrap();

        let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        let chunk_idx = ChunkIndex::new(0);
        let group_id = ShardGroupId::new(1);

        // Create a chunk slice to read
        let chunk_slice = ChunkSlice {
            chunk: ChunkRef::new(block_id, chunk_idx.as_raw()),
            offset_in_chunk: 0,
            len: layout.chunk_size,
        };

        // Create caller context
        let caller_ctx = RequestHeader::new(ClientId::new(1));

        // Read chunk slice from UFS (will fill back to BlockStore)
        let data = filler
            .read_chunk_slice_stream(group_id, chunk_slice, &caller_ctx)
            .await
            .unwrap();

        // Verify data was read
        assert!(data.is_some());
        assert_eq!(data.unwrap().len(), layout.chunk_size as usize);

        // Verify chunk is now in BlockStore
        assert!(block_store.has_chunk(group_id, block_id, chunk_idx));
    }

    // ========== Three-layer definition validation tests ==========

    #[test]
    fn test_range_to_chunks_unified() {
        let layout = create_test_layout();
        let data_handle_id = DataHandleId::new(1);
        let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));

        // Test 1: Range aligned to chunk boundaries
        let chunks = crate::pipeline::range_to_chunks(&layout, block_id, 0, layout.chunk_size);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].0.as_raw(), 0);
        assert_eq!(chunks[0].1, 0);
        assert_eq!(chunks[0].2, layout.chunk_size);

        // Test 2: Range spanning multiple chunks
        let chunks = crate::pipeline::range_to_chunks(&layout, block_id, 500_000, 2_500_000);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].0.as_raw(), 0);
        assert_eq!(chunks[0].1, 500_000);
        assert_eq!(chunks[1].0.as_raw(), 1);
        assert_eq!(chunks[1].1, 0);
        assert_eq!(chunks[2].0.as_raw(), 2);
        assert_eq!(chunks[2].1, 0);

        // Test 3: Range at chunk boundary
        let chunks = crate::pipeline::range_to_chunks(&layout, block_id, layout.chunk_size, layout.chunk_size);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].0.as_raw(), 1);
        assert_eq!(chunks[0].1, 0);
        assert_eq!(chunks[0].2, layout.chunk_size);

        // Test 4: Range within single chunk (non-aligned)
        let chunks = crate::pipeline::range_to_chunks(&layout, block_id, 100, 50);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].0.as_raw(), 0);
        assert_eq!(chunks[0].1, 100);
        assert_eq!(chunks[0].2, 50);

        // Test 5: Empty range (len=0)
        let chunks = crate::pipeline::range_to_chunks(&layout, block_id, 100, 0);
        assert_eq!(chunks.len(), 0);

        // Test 6: Range crossing block boundary (should handle gracefully)
        let chunks = crate::pipeline::range_to_chunks(&layout, block_id, layout.block_size - 1000, 2000);
        // Should return chunks within this block only
        assert!(!chunks.is_empty());
    }

    #[tokio::test]
    async fn test_streaming_write_with_chunk_merging() {
        let layout = create_test_layout();
        let temp_dir = TempDir::new().unwrap();
        let block_store_dir = temp_dir.path().join("block_store");
        std::fs::create_dir_all(&block_store_dir).unwrap();

        let volume_manager = Arc::new(VolumeManager::new());
        volume_manager.open_volumes(&[block_store_dir.clone()]).unwrap();
        let block_store = Arc::new(BlockStore::new(
            volume_manager,
            block_store_dir.join("manifest.json"),
            layout.block_size,
            layout.chunk_size,
        ));
        block_store.init().await.unwrap();

        let data_handle_id = DataHandleId::new(1);
        let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        let group_id = ShardGroupId::new(0);

        // Write a full block (32MB) using small frames (128KB each) and merge into 1MB chunks.
        // ChunkMerger keeps semantics aligned with the pipeline layer.
        let frame_size = 128 * 1024; // 128KB frames
        let num_frames = (layout.block_size as usize) / frame_size;
        let mut merger = crate::pipeline::ChunkMerger::new(layout.chunk_size);
        let mut current_chunk_idx = 0;

        for frame_idx in 0..num_frames {
            let frame_data = Bytes::from(vec![(frame_idx % 256) as u8; frame_size]);
            if let Some(merged) = merger.add_chunk(frame_data) {
                let chunk_idx = ChunkIndex::new(current_chunk_idx);
                current_chunk_idx += 1;
                let mut reader = std::io::Cursor::new(merged);
                block_store
                    .commit_chunk(group_id, block_id, chunk_idx, &mut reader)
                    .await
                    .unwrap();
            }
        }

        // Flush remaining buffered data.
        if let Some(merged) = merger.flush() {
            let chunk_idx = ChunkIndex::new(current_chunk_idx);
            let mut reader = std::io::Cursor::new(merged);
            block_store
                .commit_chunk(group_id, block_id, chunk_idx, &mut reader)
                .await
                .unwrap();
        }

        // Verify all chunks are committed
        let block_meta = block_store.block_meta(group_id, block_id).unwrap().unwrap();
        let expected_chunks = layout.chunks_per_block();
        assert!(block_meta.is_complete(expected_chunks));

        // Verify block metadata
        assert_eq!(block_meta.committed_length, layout.block_size as u64);
        assert_eq!(block_meta.total_size, layout.block_size as u64);

        // Verify we can read back the data
        for chunk_idx in 0..expected_chunks {
            let data = block_store
                .read_chunk(group_id, block_id, ChunkIndex::new(chunk_idx))
                .await
                .unwrap();
            assert!(data.is_some());
            assert_eq!(data.unwrap().len(), layout.chunk_size as usize);
        }
    }

    #[tokio::test]
    async fn test_miss_fill_second_hit() {
        use crate::block_store::BlockStore;
        use crate::volume_manager::VolumeManager;
        use common::audit::AuditLogger;
        use common::header::RequestHeader;
        use std::io::Write;
        use tempfile::TempDir;
        use types::chunk::{ChunkRef, ChunkSlice};
        use types::ids::ShardGroupId;
        use types::ClientId;
        use ufs::{BackendConfig, BackendKind, FsConfig, UfsId, UfsRegistry, UfsSpec};

        let layout = create_test_layout();

        // Create temporary directories
        let temp_dir = TempDir::new().unwrap();
        let ufs_root = temp_dir.path().join("ufs");
        std::fs::create_dir_all(&ufs_root).unwrap();
        let block_store_dir = temp_dir.path().join("block_store");
        std::fs::create_dir_all(&block_store_dir).unwrap();
        let audit_dir = temp_dir.path().join("audit");
        std::fs::create_dir_all(&audit_dir).unwrap();

        // Create test file in UFS
        let data_handle_id = DataHandleId::new(1);
        let ufs_file_path = ufs_root.join(data_handle_id.as_raw().to_string());
        let mut ufs_file = std::fs::File::create(&ufs_file_path).unwrap();
        let test_data = vec![42u8; layout.chunk_size as usize];
        ufs_file.write_all(&test_data).unwrap();
        ufs_file.sync_all().unwrap();

        // Create UfsRegistry
        let ufs_registry = Arc::new(UfsRegistry::new());
        let ufs_spec = UfsSpec::new(
            "test-ufs",
            BackendKind::Fs,
            BackendConfig::Fs(FsConfig {
                root: ufs_root.to_string_lossy().to_string(),
            }),
        );
        ufs_registry.upsert(ufs_spec).unwrap();

        // Create BlockStore
        let volume_manager = Arc::new(VolumeManager::new());
        volume_manager.open_volumes(&[block_store_dir.clone()]).unwrap();
        let block_store = Arc::new(BlockStore::new(
            volume_manager,
            block_store_dir.join("manifest.json"),
            layout.block_size,
            layout.chunk_size,
        ));
        block_store.init().await.unwrap();

        // Create AuditLogger
        let audit_logger = Arc::new(AuditLogger::new(&audit_dir).unwrap());

        // Create UfsFiller
        let filler = UfsFiller::new(
            ufs_registry,
            block_store.clone(),
            audit_logger,
            layout,
            Some(UfsId::new("test-ufs")),
            10,
            5000,
            false, // sync fill
        );
        filler.init_limiters(10);

        let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        let chunk_idx = ChunkIndex::new(0);
        let group_id = ShardGroupId::new(1);
        let caller_ctx = RequestHeader::new(ClientId::new(1));

        // First read: should miss and fill from UFS
        let chunk_slice = ChunkSlice {
            chunk: ChunkRef::new(block_id, chunk_idx.as_raw()),
            offset_in_chunk: 0,
            len: layout.chunk_size,
        };

        // Verify chunk is NOT in BlockStore initially
        assert!(!block_store.has_chunk(group_id, block_id, chunk_idx));

        // Read from UFS (will fill back)
        let first_read = filler
            .read_chunk_slice_stream(group_id, chunk_slice, &caller_ctx)
            .await
            .unwrap();
        assert!(first_read.is_some());
        assert_eq!(first_read.unwrap(), Bytes::from(test_data.clone()));

        // Verify chunk is NOW in BlockStore
        assert!(block_store.has_chunk(group_id, block_id, chunk_idx));

        // Second read: should hit locally
        let second_read = block_store.read_chunk(group_id, block_id, chunk_idx).await.unwrap();
        assert!(second_read.is_some());
        assert_eq!(second_read.unwrap(), Bytes::from(test_data));

        // Verify both reads return identical data
        // (already verified above, but explicit check)
        let first_data = filler
            .read_chunk_slice_stream(group_id, chunk_slice, &caller_ctx)
            .await
            .unwrap()
            .unwrap();
        let second_data = block_store
            .read_chunk(group_id, block_id, chunk_idx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first_data, second_data);
    }

    #[tokio::test]
    async fn test_block_report_aggregation() {
        let layout = create_test_layout();
        let temp_dir = TempDir::new().unwrap();
        let block_store_dir = temp_dir.path().join("block_store");
        std::fs::create_dir_all(&block_store_dir).unwrap();

        let volume_manager = Arc::new(VolumeManager::new());
        volume_manager.open_volumes(&[block_store_dir.clone()]).unwrap();
        let block_store = Arc::new(BlockStore::new(
            volume_manager,
            block_store_dir.join("manifest.json"),
            layout.block_size,
            layout.chunk_size,
        ));
        block_store.init().await.unwrap();

        let data_handle_id = DataHandleId::new(1);
        let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        let group_id = ShardGroupId::new(0);

        // Commit multiple chunks to the same block
        let num_chunks = 5;
        for chunk_idx in 0..num_chunks {
            let chunk_data = Bytes::from(vec![chunk_idx as u8; layout.chunk_size as usize]);
            let mut reader = std::io::Cursor::new(chunk_data);

            block_store
                .commit_chunk(group_id, block_id, ChunkIndex::new(chunk_idx), &mut reader)
                .await
                .unwrap();
        }

        // Get block metadata
        let block_meta = block_store.block_meta(group_id, block_id).unwrap().unwrap();

        // Verify block report is at block level (not chunk level)
        let blocks = block_store.list_blocks(group_id);
        assert_eq!(blocks.len(), 1); // Only one block

        // Verify block metadata contains chunk bitmap
        let committed_chunks: u32 = block_meta.chunk_bitmap.bits.iter().map(|b| b.count_ones()).sum();
        assert_eq!(committed_chunks, num_chunks);

        // Verify committed_length is sum of chunk sizes
        assert_eq!(block_meta.committed_length, (num_chunks * layout.chunk_size) as u64);

        // Verify block_id is correct
        assert_eq!(blocks[0].block_id, block_id);

        // Verify chunk bitmap reflects committed chunks
        for chunk_idx in 0..num_chunks {
            assert!(block_meta.has_chunk(chunk_idx));
        }
        // Verify uncommitted chunks are not in bitmap
        for chunk_idx in num_chunks..layout.chunks_per_block() {
            assert!(!block_meta.has_chunk(chunk_idx));
        }
    }

    #[test]
    fn test_chunk_merger() {
        use crate::pipeline::ChunkMerger;
        use bytes::Bytes;

        let mut merger = ChunkMerger::new(1024 * 1024); // 1MB target

        // Add small chunks (128KB each)
        for i in 0..8 {
            let chunk = Bytes::from(vec![i as u8; 128 * 1024]);
            assert!(merger.add_chunk(chunk).is_none());
        }

        // Adding 9th chunk should trigger flush (8 * 128KB = 1MB)
        let chunk = Bytes::from(vec![8u8; 128 * 1024]);
        let merged = merger.add_chunk(chunk);
        assert!(merged.is_some());
        assert_eq!(merged.unwrap().len(), 1024 * 1024); // 1MB

        // Flush remaining
        let remaining = merger.flush();
        assert!(remaining.is_some());
        assert_eq!(remaining.unwrap().len(), 128 * 1024); // 128KB
    }

    // ========== BlockStore read/write tests ==========

    #[tokio::test]
    async fn test_block_store_read_range() {
        let layout = create_test_layout();
        let temp_dir = TempDir::new().unwrap();
        let block_store_dir = temp_dir.path().join("block_store");
        std::fs::create_dir_all(&block_store_dir).unwrap();

        let volume_manager = Arc::new(VolumeManager::new());
        volume_manager.open_volumes(&[block_store_dir.clone()]).unwrap();
        let block_store = Arc::new(BlockStore::new(
            volume_manager,
            block_store_dir.join("manifest.json"),
            layout.block_size,
            layout.chunk_size,
        ));
        block_store.init().await.unwrap();

        let data_handle_id = DataHandleId::new(1);
        let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        let group_id = ShardGroupId::new(0);

        // Write multiple chunks
        let num_chunks = 3;
        let mut expected_data = Vec::new();
        for chunk_idx in 0..num_chunks {
            let chunk_data = vec![chunk_idx as u8; layout.chunk_size as usize];
            expected_data.extend_from_slice(&chunk_data);
            let mut reader = std::io::Cursor::new(Bytes::from(chunk_data));
            block_store
                .commit_chunk(group_id, block_id, ChunkIndex::new(chunk_idx), &mut reader)
                .await
                .unwrap();
        }

        // Read range spanning multiple chunks
        let offset = layout.chunk_size / 2; // Start in middle of first chunk
        let len = layout.chunk_size * 2; // Span 2 chunks
        let mut reader = block_store
            .read_range(group_id, block_id, offset, len)
            .await
            .unwrap()
            .unwrap();

        let mut read_data = Vec::new();
        use tokio::io::AsyncReadExt;
        let mut buf = vec![0u8; 8192];
        loop {
            let n = reader.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            read_data.extend_from_slice(&buf[..n]);
        }

        // Verify data matches expected range
        let start = offset as usize;
        let end = (start + len as usize).min(expected_data.len());
        assert_eq!(read_data, expected_data[start..end]);
    }

    #[tokio::test]
    async fn test_block_store_write_multiple_chunks() {
        let layout = create_test_layout();
        let temp_dir = TempDir::new().unwrap();
        let block_store_dir = temp_dir.path().join("block_store");
        std::fs::create_dir_all(&block_store_dir).unwrap();

        let volume_manager = Arc::new(VolumeManager::new());
        volume_manager.open_volumes(&[block_store_dir.clone()]).unwrap();
        let block_store = Arc::new(BlockStore::new(
            volume_manager,
            block_store_dir.join("manifest.json"),
            layout.block_size,
            layout.chunk_size,
        ));
        block_store.init().await.unwrap();

        let data_handle_id = DataHandleId::new(1);
        let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        let group_id = ShardGroupId::new(0);

        // Write multiple chunks with different data patterns
        let num_chunks = 5;
        let mut written_chunks = Vec::new();
        for chunk_idx in 0..num_chunks {
            let chunk_data = Bytes::from(vec![(chunk_idx * 10) as u8; layout.chunk_size as usize]);
            written_chunks.push(chunk_data.clone());
            let chunk_ref = ChunkRef::new(block_id, ChunkIndex::new(chunk_idx).as_raw());
            let mut stream = tokio::io::BufReader::new(std::io::Cursor::new(chunk_data));
            block_store
                .write_chunk_stream(group_id, chunk_ref, &mut stream)
                .await
                .unwrap();
        }

        // Verify all chunks are present
        for chunk_idx in 0..num_chunks {
            assert!(block_store.has_chunk(group_id, block_id, ChunkIndex::new(chunk_idx)));

            // Verify data matches
            let read_data = block_store
                .read_chunk(group_id, block_id, ChunkIndex::new(chunk_idx))
                .await
                .unwrap();
            assert!(read_data.is_some());
            assert_eq!(read_data.unwrap(), written_chunks[chunk_idx as usize]);
        }

        // Verify block metadata
        let block_meta = block_store.block_meta(group_id, block_id).unwrap().unwrap();
        assert_eq!(block_meta.committed_length, (num_chunks * layout.chunk_size) as u64);
    }

    // ========== UFS Local filesystem tests ==========

    #[tokio::test]
    async fn test_ufs_read_write_local_fs() {
        use common::header::RequestHeader;
        use types::ClientId;
        use ufs::{BackendConfig, BackendKind, FsConfig, UfsRegistry, UfsSpec};

        let temp_dir = TempDir::new().unwrap();
        let ufs_root = temp_dir.path().join("ufs");
        std::fs::create_dir_all(&ufs_root).unwrap();

        // Create UfsRegistry with Local filesystem backend
        let ufs_registry = Arc::new(UfsRegistry::new());
        let ufs_spec = UfsSpec::new(
            "local-fs",
            BackendKind::Fs,
            BackendConfig::Fs(FsConfig {
                root: ufs_root.to_string_lossy().to_string(),
            }),
        );
        ufs_registry.upsert(ufs_spec).unwrap();

        let ufs = ufs_registry.get(&ufs::UfsId::new("local-fs")).unwrap();
        let ctx = RequestHeader::new(ClientId::new(1));

        // Test write_all
        let test_data = Bytes::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        ufs.write_all("test_file.txt", test_data.clone(), &ctx).await.unwrap();

        // Test read_all
        let read_data = ufs.read_all("test_file.txt", &ctx).await.unwrap();
        assert_eq!(read_data, test_data);

        // Test read_range
        let range_data = ufs.read_range("test_file.txt", 2, 4, &ctx).await.unwrap();
        assert_eq!(range_data, Bytes::from(vec![3, 4, 5, 6]));

        // Test stat
        let status = ufs.stat("test_file.txt", &ctx).await.unwrap();
        assert!(!status.is_dir);
        assert_eq!(status.size, Some(10));

        // Test exists
        assert!(ufs.exists("test_file.txt", &ctx).await.unwrap());
        assert!(!ufs.exists("nonexistent.txt", &ctx).await.unwrap());
    }

    #[tokio::test]
    async fn test_ufs_fill_from_local_fs_large_file() {
        use common::audit::AuditLogger;
        use common::header::RequestHeader;
        use std::io::Write;
        use types::chunk::{ChunkRef, ChunkSlice};
        use types::ClientId;
        use ufs::{BackendConfig, BackendKind, FsConfig, UfsId, UfsRegistry, UfsSpec};

        let layout = create_test_layout();
        let temp_dir = TempDir::new().unwrap();
        let ufs_root = temp_dir.path().join("ufs");
        std::fs::create_dir_all(&ufs_root).unwrap();
        let block_store_dir = temp_dir.path().join("block_store");
        std::fs::create_dir_all(&block_store_dir).unwrap();
        let audit_dir = temp_dir.path().join("audit");
        std::fs::create_dir_all(&audit_dir).unwrap();

        // Create large test file in UFS (multiple chunks)
        let data_handle_id = DataHandleId::new(1);
        let ufs_file_path = ufs_root.join(data_handle_id.as_raw().to_string());
        let mut ufs_file = std::fs::File::create(&ufs_file_path).unwrap();
        let num_chunks = 5;
        for chunk_idx in 0..num_chunks {
            let chunk_data = vec![(chunk_idx * 30) as u8; layout.chunk_size as usize];
            ufs_file.write_all(&chunk_data).unwrap();
        }
        ufs_file.sync_all().unwrap();

        // Create UfsRegistry
        let ufs_registry = Arc::new(UfsRegistry::new());
        let ufs_spec = UfsSpec::new(
            "local-fs",
            BackendKind::Fs,
            BackendConfig::Fs(FsConfig {
                root: ufs_root.to_string_lossy().to_string(),
            }),
        );
        ufs_registry.upsert(ufs_spec).unwrap();

        // Create BlockStore
        let volume_manager = Arc::new(VolumeManager::new());
        volume_manager.open_volumes(&[block_store_dir.clone()]).unwrap();
        let block_store = Arc::new(BlockStore::new(
            volume_manager,
            block_store_dir.join("manifest.json"),
            layout.block_size,
            layout.chunk_size,
        ));
        block_store.init().await.unwrap();

        // Create AuditLogger
        let audit_logger = Arc::new(AuditLogger::new(&audit_dir).unwrap());

        // Create UfsFiller
        let filler = UfsFiller::new(
            ufs_registry,
            block_store.clone(),
            audit_logger,
            layout,
            Some(UfsId::new("local-fs")),
            10,
            5000,
            false, // sync fill
        );
        filler.init_limiters(10);

        let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        let group_id = ShardGroupId::new(1);
        let caller_ctx = RequestHeader::new(ClientId::new(1));

        // Read and fill multiple chunks
        for chunk_idx in 0..num_chunks {
            let chunk_slice = ChunkSlice {
                chunk: ChunkRef::new(block_id, ChunkIndex::new(chunk_idx).as_raw()),
                offset_in_chunk: 0,
                len: layout.chunk_size,
            };

            // Verify chunk is NOT in BlockStore initially
            assert!(!block_store.has_chunk(group_id, block_id, ChunkIndex::new(chunk_idx)));

            // Read from UFS (will fill back)
            let data = filler
                .read_chunk_slice_stream(group_id, chunk_slice, &caller_ctx)
                .await
                .unwrap();
            assert!(data.is_some());
            assert_eq!(data.unwrap().len(), layout.chunk_size as usize);

            // Verify chunk is NOW in BlockStore
            assert!(block_store.has_chunk(group_id, block_id, ChunkIndex::new(chunk_idx)));
        }

        // Verify all chunks are in BlockStore
        let block_meta = block_store.block_meta(group_id, block_id).unwrap().unwrap();
        assert_eq!(block_meta.committed_length, (num_chunks * layout.chunk_size) as u64);
    }

    #[tokio::test]
    async fn test_ufs_fill_partial_chunk_slice() {
        use common::audit::AuditLogger;
        use common::header::RequestHeader;
        use std::io::Write;
        use types::chunk::{ChunkRef, ChunkSlice};
        use types::ClientId;
        use ufs::{BackendConfig, BackendKind, FsConfig, UfsId, UfsRegistry, UfsSpec};

        let layout = create_test_layout();
        let temp_dir = TempDir::new().unwrap();
        let ufs_root = temp_dir.path().join("ufs");
        std::fs::create_dir_all(&ufs_root).unwrap();
        let block_store_dir = temp_dir.path().join("block_store");
        std::fs::create_dir_all(&block_store_dir).unwrap();
        let audit_dir = temp_dir.path().join("audit");
        std::fs::create_dir_all(&audit_dir).unwrap();

        // Create test file in UFS with known pattern
        let data_handle_id = DataHandleId::new(1);
        let ufs_file_path = ufs_root.join(data_handle_id.as_raw().to_string());
        let mut ufs_file = std::fs::File::create(&ufs_file_path).unwrap();
        let mut test_data = vec![0u8; layout.chunk_size as usize];
        for i in 0..test_data.len() {
            test_data[i] = (i % 256) as u8;
        }
        ufs_file.write_all(&test_data).unwrap();
        ufs_file.sync_all().unwrap();

        // Create UfsRegistry
        let ufs_registry = Arc::new(UfsRegistry::new());
        let ufs_spec = UfsSpec::new(
            "local-fs",
            BackendKind::Fs,
            BackendConfig::Fs(FsConfig {
                root: ufs_root.to_string_lossy().to_string(),
            }),
        );
        ufs_registry.upsert(ufs_spec).unwrap();

        // Create BlockStore
        let volume_manager = Arc::new(VolumeManager::new());
        volume_manager.open_volumes(&[block_store_dir.clone()]).unwrap();
        let block_store = Arc::new(BlockStore::new(
            volume_manager,
            block_store_dir.join("manifest.json"),
            layout.block_size,
            layout.chunk_size,
        ));
        block_store.init().await.unwrap();

        // Create AuditLogger
        let audit_logger = Arc::new(AuditLogger::new(&audit_dir).unwrap());

        // Create UfsFiller
        let filler = UfsFiller::new(
            ufs_registry,
            block_store.clone(),
            audit_logger,
            layout,
            Some(UfsId::new("local-fs")),
            10,
            5000,
            false, // sync fill
        );
        filler.init_limiters(10);

        let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        let chunk_idx = ChunkIndex::new(0);
        let group_id = ShardGroupId::new(1);
        let caller_ctx = RequestHeader::new(ClientId::new(1));

        // Read partial slice (offset 100, len 200)
        let chunk_slice = ChunkSlice {
            chunk: ChunkRef::new(block_id, chunk_idx.as_raw()),
            offset_in_chunk: 100,
            len: 200,
        };

        let data = filler
            .read_chunk_slice_stream(group_id, chunk_slice, &caller_ctx)
            .await
            .unwrap();
        assert!(data.is_some());
        let slice_data = data.unwrap();
        assert_eq!(slice_data.len(), 200);

        // Verify data matches expected slice
        assert_eq!(slice_data[0], 100u8);
        assert_eq!(slice_data[199], 43u8); // (100 + 199) % 256 = 43

        // Verify full chunk is now in BlockStore (not just the slice)
        assert!(block_store.has_chunk(group_id, block_id, chunk_idx));

        // Verify we can read the full chunk
        let full_chunk = block_store.read_chunk(group_id, block_id, chunk_idx).await.unwrap();
        assert!(full_chunk.is_some());
        assert_eq!(full_chunk.unwrap(), Bytes::from(test_data));
    }

    #[tokio::test]
    async fn test_data_header_conversions() {
        use crate::data_header::{DataRequestHeader, DataResponseHeader};
        use common::header::ClientInfo;

        // Test DataRequestHeader conversion
        let client = ClientInfo::new(ClientId::new(123));
        let req_header = DataRequestHeader::new(client.clone())
            .with_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string());

        let proto_req = req_header.to_proto();
        assert!(proto_req.client.is_some());
        assert_eq!(proto_req.client.as_ref().unwrap().client_id, 123);
        assert_eq!(
            proto_req.traceparent,
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        );

        let req_header_back = DataRequestHeader::from_proto(proto_req).unwrap();
        assert_eq!(req_header_back.client.client_id.as_raw(), 123);
        assert_eq!(
            req_header_back.traceparent,
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string())
        );

        // Test DataResponseHeader conversion - OK
        let resp_header_ok = DataResponseHeader::ok(client.clone());
        let proto_resp_ok = resp_header_ok.to_proto();
        // Success: error should be None
        assert!(proto_resp_ok.error.is_none());

        // Test DataResponseHeader conversion - NEED_REFRESH
        let resp_header_refresh = DataResponseHeader::need_refresh(
            client.clone(),
            common::error::canonical::RefreshReason::BlockStampMismatch,
            common::header::RpcErrorCode::BlockStampMismatch,
            "Block stamp mismatch".to_string(),
        );
        let proto_resp_refresh = resp_header_refresh.to_proto();
        assert!(proto_resp_refresh.error.is_some());
        let error = proto_resp_refresh.error.unwrap();
        assert_eq!(
            error.error_class(),
            proto::common::ErrorClassProto::ErrorClassNeedRefresh
        );
        assert_eq!(
            error.refresh_reason(),
            proto::common::RefreshReasonProto::RefreshReasonBlockStampMismatch
        );
        assert_eq!(error.message, "Block stamp mismatch");

        // Test DataResponseHeader conversion - RETRYABLE
        let resp_header_retry = DataResponseHeader::retryable(
            client.clone(),
            common::header::RpcErrorCode::NodeUnavailable,
            Some(5000),
            "Temporary failure".to_string(),
        );
        let proto_resp_retry = resp_header_retry.to_proto();
        assert!(proto_resp_retry.error.is_some());
        let error = proto_resp_retry.error.unwrap();
        assert_eq!(error.error_class(), proto::common::ErrorClassProto::ErrorClassRetryable);
        assert_eq!(error.retry_after_ms, Some(5000));
        assert_eq!(error.message, "Temporary failure");

        // Test DataResponseHeader conversion - FATAL
        let resp_header_fatal = DataResponseHeader::fatal(
            client,
            common::header::RpcErrorCode::Application,
            "Unrecoverable error".to_string(),
        );
        let proto_resp_fatal = resp_header_fatal.to_proto();
        assert!(proto_resp_fatal.error.is_some());
        let error = proto_resp_fatal.error.unwrap();
        assert_eq!(error.error_class(), proto::common::ErrorClassProto::ErrorClassFatal);
        assert_eq!(error.message, "Unrecoverable error");
    }
}
