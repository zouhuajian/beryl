// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Local block storage boundary.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use bincode::config::standard;
use bincode::serde::{decode_from_slice, encode_to_vec};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use types::ids::{BlockId, ShardGroupId};

use crate::error::WorkerError;

mod meta_header {
    pub const MAGIC: [u8; 8] = *b"VBLKMETA";
    pub const LEN: usize = 40;
    pub const HEADER_VERSION: u32 = 1;
    pub const PAYLOAD_VERSION: u32 = 1;
    pub const CODEC_BINCODE: u32 = 1;
    pub const FLAGS_NONE: u32 = 0;
}

mod block_format {
    pub const FIXED_OFFSET: u32 = 1;
}

mod checksum {
    pub const CRC32C_POLY_REVERSED: u32 = 0x82f6_3b78;
}

type StoreResult<T> = Result<T, WorkerError>;

/// Fixed little-endian header for a block metadata file.
/// The header identifies the payload codec and protects the payload with CRC32C.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockMetaFileHeader {
    /// Fixed file magic used to identify Vecton block metadata.
    pub magic: [u8; 8],
    /// Version of this fixed header layout.
    pub header_version: u32,
    /// Version of the serialized metadata payload schema.
    pub payload_version: u32,
    /// Codec used to encode the payload.
    pub payload_codec: u32,
    /// Reserved feature flags. Unknown non-zero flags are rejected.
    pub flags: u32,
    /// Serialized payload length in bytes.
    pub payload_len: u64,
    /// CRC32C of the serialized payload.
    pub payload_crc32c: u32,
    /// CRC32C of this header with this field set to zero.
    pub header_crc32c: u32,
}

impl BlockMetaFileHeader {
    pub const fn encoded_len() -> usize {
        meta_header::LEN
    }

    fn for_payload(payload_len: usize, payload_crc32c: u32) -> StoreResult<Self> {
        let payload_len =
            u64::try_from(payload_len).map_err(|_| invalid_argument("meta payload length does not fit in u64"))?;
        let mut header = Self {
            magic: meta_header::MAGIC,
            header_version: meta_header::HEADER_VERSION,
            payload_version: meta_header::PAYLOAD_VERSION,
            payload_codec: meta_header::CODEC_BINCODE,
            flags: meta_header::FLAGS_NONE,
            payload_len,
            payload_crc32c,
            header_crc32c: 0,
        };
        header.header_crc32c = crc32c(&header.bytes_for_crc());
        Ok(header)
    }

    fn decode(encoded: &[u8]) -> StoreResult<Self> {
        if encoded.len() != meta_header::LEN {
            return Err(corrupt("invalid meta header length"));
        }

        let mut magic = [0u8; 8];
        magic.copy_from_slice(&encoded[0..8]);

        // Fixed little-endian on-disk header. Do not reorder fields without changing header_version.
        Ok(Self {
            magic,
            header_version: u32::from_le_bytes(encoded[8..12].try_into().expect("fixed header slice")),
            payload_version: u32::from_le_bytes(encoded[12..16].try_into().expect("fixed header slice")),
            payload_codec: u32::from_le_bytes(encoded[16..20].try_into().expect("fixed header slice")),
            flags: u32::from_le_bytes(encoded[20..24].try_into().expect("fixed header slice")),
            payload_len: u64::from_le_bytes(encoded[24..32].try_into().expect("fixed header slice")),
            payload_crc32c: u32::from_le_bytes(encoded[32..36].try_into().expect("fixed header slice")),
            header_crc32c: u32::from_le_bytes(encoded[36..40].try_into().expect("fixed header slice")),
        })
    }

    fn encode(self) -> [u8; meta_header::LEN] {
        // Fixed little-endian on-disk header. Do not reorder fields without changing header_version.
        let mut encoded = [0u8; meta_header::LEN];
        encoded[0..8].copy_from_slice(&self.magic);
        encoded[8..12].copy_from_slice(&self.header_version.to_le_bytes());
        encoded[12..16].copy_from_slice(&self.payload_version.to_le_bytes());
        encoded[16..20].copy_from_slice(&self.payload_codec.to_le_bytes());
        encoded[20..24].copy_from_slice(&self.flags.to_le_bytes());
        encoded[24..32].copy_from_slice(&self.payload_len.to_le_bytes());
        encoded[32..36].copy_from_slice(&self.payload_crc32c.to_le_bytes());
        encoded[36..40].copy_from_slice(&self.header_crc32c.to_le_bytes());
        encoded
    }

    fn bytes_for_crc(self) -> [u8; meta_header::LEN] {
        let mut header = self;
        header.header_crc32c = 0;
        header.encode()
    }

    fn validate(self) -> StoreResult<()> {
        if self.magic != meta_header::MAGIC {
            return Err(corrupt("invalid block meta magic"));
        }
        if self.header_version != meta_header::HEADER_VERSION {
            return Err(corrupt("unsupported block meta header version"));
        }
        if self.payload_version != meta_header::PAYLOAD_VERSION {
            return Err(corrupt("unsupported block meta payload version"));
        }
        if self.payload_codec != meta_header::CODEC_BINCODE {
            return Err(corrupt("unsupported block meta payload codec"));
        }
        if self.flags != meta_header::FLAGS_NONE {
            return Err(corrupt("unsupported block meta flags"));
        }
        if crc32c(&self.bytes_for_crc()) != self.header_crc32c {
            return Err(corrupt("block meta header checksum mismatch"));
        }
        Ok(())
    }
}

/// Self-describing metadata for one local block.
/// This payload is the source of truth for local read visibility.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockMetaPayload {
    /// Stable block identity.
    pub identity: BlockIdentity,
    /// Format parameters for interpreting `.blk` and `.meta`.
    pub format: BlockFormat,
    /// Source-independent local block length.
    pub source: BlockSource,
    /// Published local visibility state.
    pub visibility: BlockVisibility,
    /// Per-StorageChunk readiness state.
    pub chunks: BlockChunks,
}

/// Stable identity of the local block and owning group.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockIdentity {
    /// Stable block identifier.
    pub block_id: BlockId,
    /// Owning shard group.
    pub group_id: ShardGroupId,
}

/// On-disk format parameters used to interpret this block.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockFormat {
    /// Identifier of the block file format used by this block.
    pub format_id: u32,
    /// Maximum logical size of this block.
    pub block_size: u64,
    /// Worker-local StorageChunk size used by bitmaps and local IO.
    pub chunk_size: u64,
    /// Per-chunk checksum algorithm. The current format may use None.
    pub checksum_kind: ChecksumKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChecksumKind {
    None,
}

/// Source-independent effective length of this block.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockSource {
    /// Valid logical length of this block, capped by block_size.
    pub effective_block_len: u64,
}

/// Local visibility state persisted in metadata.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockVisibility {
    /// Block-local continuous Ready prefix.
    /// This is not the sum of all Ready chunks.
    pub committed_length: u64,
    /// Logical block stamp persisted with metadata.
    /// Local writes do not advance it until visibility is published.
    pub block_stamp: u64,
}

/// Per-StorageChunk visibility bitmaps.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockChunks {
    /// Number of StorageChunks described by the bitmaps.
    pub chunk_count: u32,
    /// Chunks that are published and can be read locally.
    pub ready_bitmap: Vec<u8>,
    /// Chunks known to be locally unusable.
    pub corrupt_bitmap: Vec<u8>,
}

impl BlockChunks {
    pub fn new(chunk_count: u32) -> Self {
        let bitmap_len = bitmap_len(chunk_count);
        Self {
            chunk_count,
            ready_bitmap: vec![0; bitmap_len],
            corrupt_bitmap: vec![0; bitmap_len],
        }
    }

    pub fn is_ready(&self, chunk_index: u32) -> StoreResult<bool> {
        self.validate_chunk_index(chunk_index)?;
        Ok(bit_is_set(&self.ready_bitmap, chunk_index))
    }

    pub fn is_corrupt(&self, chunk_index: u32) -> StoreResult<bool> {
        self.validate_chunk_index(chunk_index)?;
        Ok(bit_is_set(&self.corrupt_bitmap, chunk_index))
    }

    pub fn is_missing(&self, chunk_index: u32) -> StoreResult<bool> {
        Ok(!self.is_ready(chunk_index)? && !self.is_corrupt(chunk_index)?)
    }

    pub fn set_ready(&mut self, chunk_index: u32) -> StoreResult<()> {
        self.validate_chunk_index(chunk_index)?;
        if bit_is_set(&self.corrupt_bitmap, chunk_index) {
            return Err(corrupt(format!(
                "set_ready cannot mark corrupt chunk ready: chunk_index={chunk_index}"
            )));
        }
        set_bit(&mut self.ready_bitmap, chunk_index);
        Ok(())
    }

    pub fn set_corrupt(&mut self, chunk_index: u32) -> StoreResult<()> {
        self.validate_chunk_index(chunk_index)?;
        clear_bit(&mut self.ready_bitmap, chunk_index);
        set_bit(&mut self.corrupt_bitmap, chunk_index);
        Ok(())
    }

    pub fn clear_chunk(&mut self, chunk_index: u32) -> StoreResult<()> {
        self.validate_chunk_index(chunk_index)?;
        clear_bit(&mut self.ready_bitmap, chunk_index);
        clear_bit(&mut self.corrupt_bitmap, chunk_index);
        Ok(())
    }

    fn validate(&self) -> StoreResult<()> {
        let expected_len = bitmap_len(self.chunk_count);
        if self.ready_bitmap.len() != expected_len || self.corrupt_bitmap.len() != expected_len {
            return Err(corrupt("block meta bitmap length does not match chunk count"));
        }
        for (ready, corrupt_bits) in self.ready_bitmap.iter().zip(&self.corrupt_bitmap) {
            if ready & corrupt_bits != 0 {
                return Err(corrupt("chunk cannot be both ready and corrupt"));
            }
        }
        self.validate_unused_bits_clear(&self.ready_bitmap, "ready")?;
        self.validate_unused_bits_clear(&self.corrupt_bitmap, "corrupt")?;
        Ok(())
    }

    fn validate_chunk_index(&self, chunk_index: u32) -> StoreResult<()> {
        if chunk_index >= self.chunk_count {
            return Err(invalid_argument(format!(
                "chunk index {chunk_index} is outside chunk count {}",
                self.chunk_count
            )));
        }
        Ok(())
    }

    fn validate_unused_bits_clear(&self, bitmap: &[u8], name: &str) -> StoreResult<()> {
        let remaining_bits = self.chunk_count % 8;
        if remaining_bits == 0 || bitmap.is_empty() {
            return Ok(());
        }
        let valid_mask = (1u16 << remaining_bits) as u8 - 1;
        if bitmap[bitmap.len() - 1] & !valid_mask != 0 {
            return Err(corrupt(format!("{name} bitmap has bits beyond chunk count")));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockFileStoreConfig {
    pub data_root: PathBuf,
}

impl BlockFileStoreConfig {
    pub fn new(data_root: PathBuf) -> Self {
        Self { data_root }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockPaths {
    pub data_path: PathBuf,
    pub meta_path: PathBuf,
    pub temp_meta_path: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveredBlock {
    pub meta: BlockMetaPayload,
}

/// Local fixed-offset block file store.
/// `.meta` is the publication point. Data in `.blk` is readable only when
/// the corresponding StorageChunk is marked ready in metadata.
#[derive(Clone, Debug)]
pub struct BlockFileStore {
    config: BlockFileStoreConfig,
}

impl BlockFileStore {
    pub fn new(config: BlockFileStoreConfig) -> Self {
        Self { config }
    }

    pub fn create_block(
        &self,
        group_id: ShardGroupId,
        block_id: BlockId,
        block_size: u64,
        chunk_size: u64,
        effective_block_len: u64,
    ) -> StoreResult<BlockPaths> {
        validate_block_shape(block_size, chunk_size, effective_block_len)?;

        let paths = self.paths(group_id, block_id);
        let parent = paths.parent_dir()?;
        fs::create_dir_all(parent)?;
        if paths.data_path.exists() || paths.meta_path.exists() {
            return Err(invalid_argument(format!("block already exists: block_id={block_id}")));
        }

        let data = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&paths.data_path)?;
        data.sync_all()?;

        let chunk_count = chunk_count(effective_block_len, chunk_size)?;
        let meta = BlockMetaPayload {
            identity: BlockIdentity { block_id, group_id },
            format: BlockFormat {
                format_id: block_format::FIXED_OFFSET,
                block_size,
                chunk_size,
                checksum_kind: ChecksumKind::None,
            },
            source: BlockSource { effective_block_len },
            visibility: BlockVisibility {
                committed_length: 0,
                block_stamp: 0,
            },
            chunks: BlockChunks::new(chunk_count),
        };
        write_meta_new(&paths, &meta)?;
        Ok(paths)
    }

    pub fn load_meta(&self, group_id: ShardGroupId, block_id: BlockId) -> StoreResult<BlockMetaPayload> {
        let paths = self.paths(group_id, block_id);
        let meta = read_meta_file(&paths.meta_path)?;
        validate_meta_payload(&meta, group_id, block_id)?;
        Ok(meta)
    }

    /// Writes bytes to the block-local offset without publishing visibility.
    /// Visibility is published by updating `.meta`.
    pub fn write_at(&self, group_id: ShardGroupId, block_id: BlockId, offset: u64, data: Bytes) -> StoreResult<()> {
        let meta = self.load_meta(group_id, block_id)?;
        validate_range(&meta, offset, data.len() as u64)?;

        let paths = self.paths(group_id, block_id);
        let mut file = OpenOptions::new().write(true).open(&paths.data_path)?;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&data)?;
        Ok(())
    }

    /// Publishes selected chunks as Ready by atomically rewriting metadata.
    pub fn publish_ready(
        &self,
        group_id: ShardGroupId,
        block_id: BlockId,
        chunk_indices: &[u32],
        committed_length: u64,
        block_stamp: u64,
    ) -> StoreResult<()> {
        let paths = self.paths(group_id, block_id);
        let meta = self.load_meta(group_id, block_id)?;
        let projected =
            project_publish_ready_transition(&meta, block_id, chunk_indices, committed_length, block_stamp)?;
        validate_meta_payload(&projected, group_id, block_id)?;
        validate_ready_data_file(&paths, &projected)?;
        write_meta_atomic(&paths, &projected)
    }

    /// Reads only ranges fully covered by ready StorageChunks.
    /// Sparse file contents are never used as validity evidence.
    pub fn read_at(&self, group_id: ShardGroupId, block_id: BlockId, offset: u64, len: u64) -> StoreResult<Bytes> {
        let meta = self.load_meta(group_id, block_id)?;
        validate_range(&meta, offset, len)?;
        validate_ready_range(&meta, offset, len)?;

        let paths = self.paths(group_id, block_id);
        let mut file = OpenOptions::new()
            .read(true)
            .open(&paths.data_path)
            .map_err(|err| map_data_open_error(err, "ready block data file is missing"))?;
        file.seek(SeekFrom::Start(offset))?;
        let len = usize::try_from(len).map_err(|_| invalid_argument("read length does not fit in usize"))?;
        let mut buf = vec![0; len];
        file.read_exact(&mut buf)
            .map_err(|err| map_data_read_error(err, "ready range is not present in block data file"))?;
        Ok(Bytes::from(buf))
    }

    pub fn recover_block(&self, group_id: ShardGroupId, block_id: BlockId) -> StoreResult<RecoveredBlock> {
        let paths = self.paths(group_id, block_id);
        let meta = self.load_meta(group_id, block_id)?;
        validate_ready_data_file(&paths, &meta)?;
        Ok(RecoveredBlock { meta })
    }

    pub fn paths(&self, group_id: ShardGroupId, block_id: BlockId) -> BlockPaths {
        let (hash_a, hash_b) = block_hash_prefix(block_id);
        let stem = format!(
            "b_{:016x}_{:08x}",
            block_id.data_handle_id.as_raw(),
            block_id.index.as_raw()
        );
        let dir = self
            .config
            .data_root
            .join("groups")
            .join(format!("g_{:016x}", group_id.as_raw()))
            .join("blocks")
            .join(format!("{hash_a:02x}"))
            .join(format!("{hash_b:02x}"));

        BlockPaths {
            data_path: dir.join(format!("{stem}.blk")),
            meta_path: dir.join(format!("{stem}.meta")),
            temp_meta_path: dir.join(format!("{stem}.meta.tmp")),
        }
    }
}

impl BlockPaths {
    fn parent_dir(&self) -> StoreResult<&Path> {
        self.data_path
            .parent()
            .ok_or_else(|| invalid_argument("block path has no parent directory"))
    }
}

/// Placeholder for worker-local block storage.
///
/// The concrete file-backed store owns persisted block metadata, byte IO, and
/// readiness bitmaps. It remains detached from the upper data path until wired explicitly.
#[derive(Clone, Debug)]
pub struct BlockStore {
    /// Worker-local StorageChunk size.
    /// This is the IO/checksum/valid-bitmap granularity, not a transport frame size.
    chunk_size: u32,
}

impl BlockStore {
    pub const fn new(chunk_size: u32) -> Self {
        Self { chunk_size }
    }

    pub const fn chunk_size(&self) -> u32 {
        self.chunk_size
    }

    /// Read at a block-local offset.
    pub async fn read_at(&self, _block_id: BlockId, _offset: u64, _len: u32) -> StoreResult<Bytes> {
        Err(Self::not_implemented("BlockStore::read_at"))
    }

    /// Write at a block-local offset.
    pub async fn write_at(&self, _block_id: BlockId, _offset: u64, _data: Bytes) -> StoreResult<()> {
        Err(Self::not_implemented("BlockStore::write_at"))
    }

    /// Persist pending local data for a block.
    pub async fn sync_block(&self, _block_id: BlockId) -> StoreResult<u64> {
        Err(Self::not_implemented("BlockStore::sync_block"))
    }

    fn not_implemented(operation: &'static str) -> WorkerError {
        WorkerError::Unimplemented(format!("{operation} is not implemented"))
    }
}

pub fn recompute_committed_length(meta: &BlockMetaPayload) -> u64 {
    let chunk_size = meta.format.chunk_size;
    let effective_len = meta.source.effective_block_len;
    let mut committed = 0u64;

    for chunk_index in 0..meta.chunks.chunk_count {
        if !meta.chunks.is_ready(chunk_index).unwrap_or(false) {
            break;
        }
        let chunk_start = u64::from(chunk_index) * chunk_size;
        if chunk_start >= effective_len {
            break;
        }
        let chunk_end = ((u64::from(chunk_index) + 1) * chunk_size).min(effective_len);
        committed = chunk_end;
    }

    committed
}

fn write_meta_atomic(paths: &BlockPaths, meta: &BlockMetaPayload) -> StoreResult<()> {
    let parent = paths.parent_dir()?;
    fs::create_dir_all(parent)?;
    let encoded = encode_meta(meta)?;
    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&paths.temp_meta_path)?;
        file.write_all(&encoded)?;
        file.sync_all()?;
    }
    fs::rename(&paths.temp_meta_path, &paths.meta_path)?;
    sync_parent_dir(parent)?;
    Ok(())
}

fn write_meta_new(paths: &BlockPaths, meta: &BlockMetaPayload) -> StoreResult<()> {
    let parent = paths.parent_dir()?;
    fs::create_dir_all(parent)?;
    let encoded = encode_meta(meta)?;
    {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&paths.temp_meta_path)?;
        file.write_all(&encoded)?;
        file.sync_all()?;
    }
    let link_result = fs::hard_link(&paths.temp_meta_path, &paths.meta_path);
    let cleanup_result = fs::remove_file(&paths.temp_meta_path);
    link_result?;
    cleanup_result?;
    sync_parent_dir(parent)?;
    Ok(())
}

fn encode_meta(meta: &BlockMetaPayload) -> StoreResult<Vec<u8>> {
    let payload = encode_to_vec(meta, standard()).map_err(|err| WorkerError::Internal(err.to_string()))?;
    let header = BlockMetaFileHeader::for_payload(payload.len(), crc32c(&payload))?;
    let mut encoded = Vec::with_capacity(BlockMetaFileHeader::encoded_len() + payload.len());
    encoded.extend_from_slice(&header.encode());
    encoded.extend_from_slice(&payload);
    Ok(encoded)
}

fn read_meta_file(path: &Path) -> StoreResult<BlockMetaPayload> {
    let mut encoded = Vec::new();
    File::open(path)?.read_to_end(&mut encoded)?;
    if encoded.len() < BlockMetaFileHeader::encoded_len() {
        return Err(corrupt("block meta file is shorter than the header"));
    }

    let header = BlockMetaFileHeader::decode(&encoded[..BlockMetaFileHeader::encoded_len()])?;
    header.validate()?;
    let payload_len = usize::try_from(header.payload_len).map_err(|_| corrupt("meta payload length is too large"))?;
    let expected_len = BlockMetaFileHeader::encoded_len()
        .checked_add(payload_len)
        .ok_or_else(|| corrupt("meta file length overflow"))?;
    if encoded.len() != expected_len {
        return Err(corrupt("block meta file length does not match header"));
    }

    let payload = &encoded[BlockMetaFileHeader::encoded_len()..];
    if crc32c(payload) != header.payload_crc32c {
        return Err(corrupt("block meta payload checksum mismatch"));
    }
    let (meta, consumed) =
        decode_from_slice::<BlockMetaPayload, _>(payload, standard()).map_err(|err| corrupt(err.to_string()))?;
    if consumed != payload.len() {
        return Err(corrupt("block meta payload has trailing bytes"));
    }
    Ok(meta)
}

fn validate_meta_payload(meta: &BlockMetaPayload, group_id: ShardGroupId, block_id: BlockId) -> StoreResult<()> {
    if meta.identity.group_id != group_id {
        return Err(corrupt("block meta group id does not match path"));
    }
    if meta.identity.block_id != block_id {
        return Err(corrupt("block meta block id does not match path"));
    }
    if meta.format.format_id != block_format::FIXED_OFFSET {
        return Err(corrupt("unsupported block format id"));
    }
    if meta.format.checksum_kind != ChecksumKind::None {
        return Err(corrupt("unsupported checksum kind"));
    }
    validate_block_shape(
        meta.format.block_size,
        meta.format.chunk_size,
        meta.source.effective_block_len,
    )?;
    let expected_chunks = chunk_count(meta.source.effective_block_len, meta.format.chunk_size)?;
    if meta.chunks.chunk_count != expected_chunks {
        return Err(corrupt("block meta chunk count does not match effective block length"));
    }
    meta.chunks.validate()?;
    if meta.visibility.committed_length > meta.source.effective_block_len {
        return Err(corrupt("committed length exceeds effective block length"));
    }
    let recomputed = recompute_committed_length(meta);
    if meta.visibility.committed_length != recomputed {
        return Err(corrupt("committed length does not match ready prefix"));
    }
    Ok(())
}

fn validate_block_shape(block_size: u64, chunk_size: u64, effective_block_len: u64) -> StoreResult<()> {
    if block_size == 0 {
        return Err(invalid_argument("block size must be non-zero"));
    }
    if chunk_size == 0 {
        return Err(invalid_argument("chunk size must be non-zero"));
    }
    if effective_block_len > block_size {
        return Err(invalid_argument("effective block length exceeds block size"));
    }
    Ok(())
}

fn validate_range(meta: &BlockMetaPayload, offset: u64, len: u64) -> StoreResult<()> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| invalid_argument("block-local range overflows"))?;
    if offset > meta.source.effective_block_len || end > meta.source.effective_block_len {
        return Err(invalid_argument("block-local range exceeds effective block length"));
    }
    Ok(())
}

fn validate_ready_range(meta: &BlockMetaPayload, offset: u64, len: u64) -> StoreResult<()> {
    if len == 0 {
        return Ok(());
    }
    let first_chunk = (offset / meta.format.chunk_size) as u32;
    let last_chunk = ((offset + len - 1) / meta.format.chunk_size) as u32;
    for chunk_index in first_chunk..=last_chunk {
        if meta.chunks.is_corrupt(chunk_index)? {
            return Err(corrupt(format!("chunk {chunk_index} is corrupt")));
        }
        if !meta.chunks.is_ready(chunk_index)? {
            return Err(invalid_argument(format!("chunk {chunk_index} is not ready")));
        }
    }
    Ok(())
}

fn project_publish_ready_transition(
    meta: &BlockMetaPayload,
    block_id: BlockId,
    chunk_indices: &[u32],
    committed_length: u64,
    block_stamp: u64,
) -> StoreResult<BlockMetaPayload> {
    if committed_length > meta.source.effective_block_len {
        return Err(invalid_argument(format!(
            "publish_ready committed_length={committed_length} exceeds effective_block_len={} for block_id={block_id}",
            meta.source.effective_block_len
        )));
    }

    for chunk_index in chunk_indices {
        meta.chunks.validate_chunk_index(*chunk_index)?;
        if meta.chunks.is_corrupt(*chunk_index)? {
            return Err(corrupt(format!(
                "publish_ready cannot mark corrupt chunk ready: block_id={block_id}, chunk_index={chunk_index}"
            )));
        }
    }

    let mut projected = meta.clone();
    for chunk_index in chunk_indices {
        projected.chunks.set_ready(*chunk_index)?;
    }
    let ready_prefix = recompute_committed_length(&projected);
    if committed_length != ready_prefix {
        return Err(invalid_argument(format!(
            "publish_ready committed_length={committed_length} does not match ready prefix {ready_prefix} for block_id={block_id}"
        )));
    }
    projected.visibility.committed_length = committed_length;
    projected.visibility.block_stamp = block_stamp;

    Ok(projected)
}

fn validate_ready_data_file(paths: &BlockPaths, meta: &BlockMetaPayload) -> StoreResult<()> {
    let required_len = required_data_len_for_ready_chunks(meta)?;
    if required_len == 0 {
        return Ok(());
    }

    let len = fs::metadata(&paths.data_path)
        .map_err(|err| map_data_open_error(err, "ready block data file is missing"))?
        .len();
    if len < required_len {
        return Err(corrupt("ready block data file is shorter than ready chunks require"));
    }
    Ok(())
}

fn required_data_len_for_ready_chunks(meta: &BlockMetaPayload) -> StoreResult<u64> {
    let mut required_len = 0u64;
    for chunk_index in 0..meta.chunks.chunk_count {
        if meta.chunks.is_ready(chunk_index)? {
            let chunk_end =
                ((u64::from(chunk_index) + 1) * meta.format.chunk_size).min(meta.source.effective_block_len);
            required_len = required_len.max(chunk_end);
        }
    }
    Ok(required_len)
}

fn chunk_count(effective_block_len: u64, chunk_size: u64) -> StoreResult<u32> {
    let count = if effective_block_len == 0 {
        0
    } else {
        ((effective_block_len - 1) / chunk_size) + 1
    };
    u32::try_from(count).map_err(|_| invalid_argument("chunk count does not fit in u32"))
}

fn bitmap_len(chunk_count: u32) -> usize {
    usize::try_from(chunk_count.div_ceil(8)).expect("u32 bitmap length fits in usize")
}

fn bit_is_set(bitmap: &[u8], chunk_index: u32) -> bool {
    bitmap[bitmap_index(chunk_index)] & bitmap_mask(chunk_index) != 0
}

fn set_bit(bitmap: &mut [u8], chunk_index: u32) {
    bitmap[bitmap_index(chunk_index)] |= bitmap_mask(chunk_index);
}

fn clear_bit(bitmap: &mut [u8], chunk_index: u32) {
    bitmap[bitmap_index(chunk_index)] &= !bitmap_mask(chunk_index);
}

fn bitmap_index(chunk_index: u32) -> usize {
    usize::try_from(chunk_index / 8).expect("u32 bitmap index fits in usize")
}

fn bitmap_mask(chunk_index: u32) -> u8 {
    1u8 << (chunk_index % 8)
}

fn block_hash_prefix(block_id: BlockId) -> (u8, u8) {
    let mut value = block_id.data_handle_id.as_raw() ^ (u64::from(block_id.index.as_raw()) << 32);
    value ^= value >> 33;
    value = value.wrapping_mul(0xff51_afd7_ed55_8ccd);
    value ^= value >> 33;
    value = value.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    value ^= value >> 33;
    ((value >> 56) as u8, (value >> 48) as u8)
}

fn sync_parent_dir(parent: &Path) -> StoreResult<()> {
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (checksum::CRC32C_POLY_REVERSED & mask);
        }
    }
    !crc
}

fn map_data_open_error(err: std::io::Error, message: &str) -> WorkerError {
    if err.kind() == std::io::ErrorKind::NotFound {
        corrupt(message)
    } else {
        WorkerError::from(err)
    }
}

fn map_data_read_error(err: std::io::Error, message: &str) -> WorkerError {
    if err.kind() == std::io::ErrorKind::UnexpectedEof {
        corrupt(message)
    } else {
        WorkerError::from(err)
    }
}

fn invalid_argument(message: impl Into<String>) -> WorkerError {
    WorkerError::InvalidArgument(message.into())
}

fn corrupt(message: impl Into<String>) -> WorkerError {
    WorkerError::Corrupt(message.into())
}

#[cfg(test)]
mod tests {
    use std::fs::{self, OpenOptions};
    use std::io::{Read, Seek, SeekFrom, Write};

    use bytes::Bytes;
    use tempfile::TempDir;
    use types::ids::{BlockId, BlockIndex, DataHandleId, ShardGroupId};

    use super::*;

    const MB: u64 = 1024 * 1024;

    fn ids() -> (ShardGroupId, BlockId) {
        (
            ShardGroupId::new(9),
            BlockId::new(DataHandleId::new(0x1234), BlockIndex::new(7)),
        )
    }

    fn store() -> (TempDir, BlockFileStore) {
        let temp = TempDir::new().expect("tempdir");
        let store = BlockFileStore::new(BlockFileStoreConfig::new(temp.path().to_path_buf()));
        (temp, store)
    }

    fn assert_corrupt<T: std::fmt::Debug>(result: Result<T, WorkerError>) {
        match result.expect_err("operation should fail") {
            WorkerError::Corrupt(_) => {}
            other => panic!("expected corrupt error, got {other:?}"),
        }
    }

    fn persist_meta(store: &BlockFileStore, group_id: ShardGroupId, block_id: BlockId, meta: &BlockMetaPayload) {
        let paths = store.paths(group_id, block_id);
        write_meta_atomic(&paths, meta).expect("persist meta");
    }

    fn extend_data_file(paths: &BlockPaths, len: u64) {
        OpenOptions::new()
            .write(true)
            .open(&paths.data_path)
            .expect("open data")
            .set_len(len)
            .expect("extend data");
    }

    #[test]
    fn create_block_creates_blk_and_meta() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();

        let paths = store
            .create_block(group_id, block_id, 8 * MB, MB, 4 * MB)
            .expect("create block");

        assert!(paths.data_path.exists());
        assert!(paths.meta_path.exists());

        let meta = store.load_meta(group_id, block_id).expect("load meta");
        assert_eq!(meta.identity.group_id, group_id);
        assert_eq!(meta.identity.block_id, block_id);
        assert_eq!(meta.format.block_size, 8 * MB);
        assert_eq!(meta.format.chunk_size, MB);
        assert_eq!(meta.source.effective_block_len, 4 * MB);
        assert_eq!(meta.chunks.chunk_count, 4);
    }

    #[test]
    fn create_block_existing_fails() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        let paths = store
            .create_block(group_id, block_id, 4096, 1024, 4096)
            .expect("create block");
        fs::write(&paths.data_path, b"existing data").expect("write existing data");
        let meta_before = fs::read(&paths.meta_path).expect("read meta before");

        store
            .create_block(group_id, block_id, 4096, 1024, 4096)
            .expect_err("existing block should be rejected");

        assert_eq!(
            fs::read(&paths.data_path).expect("read existing data"),
            b"existing data"
        );
        assert_eq!(fs::read(&paths.meta_path).expect("read meta after"), meta_before);
    }

    #[test]
    fn write_at_does_not_publish_visibility() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        store
            .create_block(group_id, block_id, 4096, 1024, 4096)
            .expect("create block");

        store
            .write_at(group_id, block_id, 0, Bytes::from_static(b"hidden"))
            .expect("write");

        assert!(store.read_at(group_id, block_id, 0, 6).is_err());
        let meta = store.load_meta(group_id, block_id).expect("load meta");
        assert!(!meta.chunks.is_ready(0).expect("ready bit"));
        assert_eq!(meta.visibility.committed_length, 0);
        assert_eq!(meta.visibility.block_stamp, 0);
    }

    #[test]
    fn publish_ready_then_read_at_succeeds() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        store
            .create_block(group_id, block_id, 4096, 1024, 4096)
            .expect("create block");
        let data = Bytes::from(vec![7; 1024]);

        store.write_at(group_id, block_id, 0, data.clone()).expect("write");
        store
            .publish_ready(group_id, block_id, &[0], 1024, 11)
            .expect("publish");

        assert_eq!(store.read_at(group_id, block_id, 0, data.len() as u64).unwrap(), data);
    }

    #[test]
    fn publish_ready_rejects_short_blk() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        let paths = store
            .create_block(group_id, block_id, 4096, 1024, 1536)
            .expect("create block");
        store
            .write_at(group_id, block_id, 0, Bytes::from(vec![7; 1024]))
            .expect("write first chunk");
        let meta_before = fs::read(&paths.meta_path).expect("read meta before publish");

        assert_corrupt(store.publish_ready(group_id, block_id, &[0, 1], 1536, 1));

        assert_eq!(
            fs::read(&paths.meta_path).expect("read meta after publish"),
            meta_before
        );
        let reloaded = store.load_meta(group_id, block_id).expect("reload meta");
        assert!(!reloaded.chunks.is_ready(0).expect("ready bit"));
        assert!(!reloaded.chunks.is_ready(1).expect("ready bit"));
        assert_eq!(reloaded.visibility.committed_length, 0);
        assert_eq!(reloaded.visibility.block_stamp, 0);
    }

    #[test]
    fn publish_ready_rejects_corrupt_chunk() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        let paths = store.create_block(group_id, block_id, 12, 4, 12).expect("create block");

        let mut meta = store.load_meta(group_id, block_id).expect("load meta");
        meta.chunks.set_corrupt(1).expect("mark corrupt");
        persist_meta(&store, group_id, block_id, &meta);
        let meta_before = fs::read(&paths.meta_path).expect("read meta before publish");

        let error = store
            .publish_ready(group_id, block_id, &[0, 1], 8, 9)
            .expect_err("publish should reject corrupt chunk");
        match error {
            WorkerError::Corrupt(message) => {
                assert!(message.contains("publish_ready"));
                assert!(message.contains(&block_id.to_string()));
                assert!(message.contains("chunk_index=1"));
            }
            other => panic!("expected corrupt error, got {other:?}"),
        }

        let meta_after = fs::read(&paths.meta_path).expect("read meta after publish");
        assert_eq!(meta_after, meta_before);
        let reloaded = store.load_meta(group_id, block_id).expect("reload meta");
        assert!(!reloaded.chunks.is_ready(0).expect("ready bit"));
        assert!(!reloaded.chunks.is_ready(1).expect("ready bit"));
        assert!(reloaded.chunks.is_corrupt(1).expect("corrupt bit"));
        assert_corrupt(store.read_at(group_id, block_id, 4, 4));
    }

    #[test]
    fn publish_ready_is_idempotent_for_ready_chunk() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        store
            .create_block(group_id, block_id, 4096, 1024, 4096)
            .expect("create block");

        store
            .write_at(group_id, block_id, 0, Bytes::from(vec![5; 1024]))
            .expect("write");
        store
            .publish_ready(group_id, block_id, &[0], 1024, 1)
            .expect("first publish");
        store
            .publish_ready(group_id, block_id, &[0], 1024, 1)
            .expect("second publish");

        let reloaded = store.load_meta(group_id, block_id).expect("reload meta");
        assert!(reloaded.chunks.is_ready(0).expect("ready bit"));
        assert!(!reloaded.chunks.is_corrupt(0).expect("corrupt bit"));
    }

    #[test]
    fn missing_chunk_read_fails() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        store.create_block(group_id, block_id, 12, 4, 12).expect("create block");
        store
            .write_at(group_id, block_id, 0, Bytes::from_static(&[1, 1, 1, 1]))
            .expect("write chunk");
        store
            .write_at(group_id, block_id, 8, Bytes::from_static(&[3, 3, 3, 3]))
            .expect("write chunk");
        store.publish_ready(group_id, block_id, &[0, 2], 4, 1).expect("publish");

        assert!(store.read_at(group_id, block_id, 0, 12).is_err());
    }

    #[test]
    fn committed_length_stops_at_first_missing_chunk() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        let paths = store
            .create_block(group_id, block_id, 3 * MB, MB, 3 * MB)
            .expect("create block");
        extend_data_file(&paths, 3 * MB);
        store
            .publish_ready(group_id, block_id, &[0, 2], MB, 1)
            .expect("publish");

        let meta = store.load_meta(group_id, block_id).expect("load meta");
        assert_eq!(recompute_committed_length(&meta), MB);
    }

    #[test]
    fn partial_tail_block_chunk_count() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        store
            .create_block(group_id, block_id, 32 * MB, MB, 4 * MB)
            .expect("create block");

        let meta = store.load_meta(group_id, block_id).expect("load meta");
        assert_eq!(meta.chunks.chunk_count, 4);
    }

    #[test]
    fn partial_tail_chunk_committed_length() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        let effective_len = 2 * MB + MB / 2;
        let paths = store
            .create_block(group_id, block_id, 32 * MB, MB, effective_len)
            .expect("create block");
        extend_data_file(&paths, effective_len);
        store
            .publish_ready(group_id, block_id, &[0, 1, 2], effective_len, 1)
            .expect("publish");

        let meta = store.load_meta(group_id, block_id).expect("load meta");
        assert_eq!(meta.chunks.chunk_count, 3);
        assert_eq!(recompute_committed_length(&meta), effective_len);
    }

    #[test]
    fn meta_payload_corruption_is_detected() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        let paths = store
            .create_block(group_id, block_id, 4096, 1024, 4096)
            .expect("create block");

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&paths.meta_path)
            .expect("open meta");
        file.seek(SeekFrom::Start(BlockMetaFileHeader::encoded_len() as u64))
            .expect("seek payload");
        let mut byte = [0u8; 1];
        file.read_exact(&mut byte).expect("read payload byte");
        byte[0] ^= 0xff;
        file.seek(SeekFrom::Start(BlockMetaFileHeader::encoded_len() as u64))
            .expect("seek payload");
        file.write_all(&byte).expect("write payload byte");
        file.sync_all().expect("sync meta");

        assert_corrupt(store.load_meta(group_id, block_id));
    }

    #[test]
    fn meta_unknown_flags_are_rejected() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        let paths = store
            .create_block(group_id, block_id, 4096, 1024, 4096)
            .expect("create block");
        let mut encoded = fs::read(&paths.meta_path).expect("read meta");
        let mut header =
            BlockMetaFileHeader::decode(&encoded[..BlockMetaFileHeader::encoded_len()]).expect("decode header");
        header.flags = 1;
        header.header_crc32c = crc32c(&header.bytes_for_crc());
        encoded[..BlockMetaFileHeader::encoded_len()].copy_from_slice(&header.encode());
        fs::write(&paths.meta_path, encoded).expect("write meta");

        assert_corrupt(store.load_meta(group_id, block_id));
    }

    #[test]
    fn ready_corrupt_bitmap_conflict_is_rejected() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        store
            .create_block(group_id, block_id, 4096, 1024, 4096)
            .expect("create block");
        let mut meta = store.load_meta(group_id, block_id).expect("load meta");
        set_bit(&mut meta.chunks.ready_bitmap, 0);
        set_bit(&mut meta.chunks.corrupt_bitmap, 0);
        persist_meta(&store, group_id, block_id, &meta);

        assert_corrupt(store.load_meta(group_id, block_id));
    }

    #[test]
    fn crc32c_matches_known_vector() {
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
    }

    #[test]
    fn meta_temp_file_is_ignored_on_recovery() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        let paths = store
            .create_block(group_id, block_id, 4096, 1024, 4096)
            .expect("create block");
        fs::write(&paths.temp_meta_path, b"ignore this").expect("write temp meta");

        let recovered = store.recover_block(group_id, block_id).expect("recover");

        assert_eq!(recovered.meta.identity.block_id, block_id);
        assert_eq!(recovered.meta.identity.group_id, group_id);
    }

    #[test]
    fn ready_meta_but_missing_blk_is_corrupt() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        let paths = store
            .create_block(group_id, block_id, 4096, 1024, 4096)
            .expect("create block");
        store
            .write_at(group_id, block_id, 0, Bytes::from(vec![9; 1024]))
            .expect("write");
        store.publish_ready(group_id, block_id, &[0], 1024, 1).expect("publish");
        fs::remove_file(paths.data_path).expect("remove data");

        assert_corrupt(store.recover_block(group_id, block_id));
    }

    #[test]
    fn ready_meta_but_short_blk_is_corrupt() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        let paths = store
            .create_block(group_id, block_id, 4096, 1024, 4096)
            .expect("create block");
        store
            .write_at(group_id, block_id, 0, Bytes::from(vec![9; 1024]))
            .expect("write");
        store.publish_ready(group_id, block_id, &[0], 1024, 1).expect("publish");
        OpenOptions::new()
            .write(true)
            .open(paths.data_path)
            .expect("open data")
            .set_len(512)
            .expect("truncate data");

        assert_corrupt(store.recover_block(group_id, block_id));
    }

    #[test]
    fn zero_bytes_are_valid_only_when_ready() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        store
            .create_block(group_id, block_id, 4096, 1024, 4096)
            .expect("create block");

        assert!(store.read_at(group_id, block_id, 0, 8).is_err());
        store
            .write_at(group_id, block_id, 0, Bytes::from(vec![0; 1024]))
            .expect("write zeroes");
        assert!(store.read_at(group_id, block_id, 0, 8).is_err());
        store.publish_ready(group_id, block_id, &[0], 1024, 1).expect("publish");

        assert_eq!(
            store.read_at(group_id, block_id, 0, 8).unwrap(),
            Bytes::from(vec![0; 8])
        );
    }
}
