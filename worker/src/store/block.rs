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

pub type StoreResult<T> = Result<T, WorkerError>;

// Metadata file header constants.

const BLOCK_META_MAGIC: [u8; 8] = *b"VBLKMETA";
const BLOCK_META_HEADER_LEN: usize = 24;
const BLOCK_META_VERSION: u32 = 1;
const MAX_META_PAYLOAD_LEN: usize = 16 * 1024 * 1024;

// Supported local block file format identifiers.

const BLOCK_FORMAT_FULL_EFFECTIVE: u32 = 1;

/// Fixed little-endian header for a block metadata file.
/// The header identifies the format and bounds the serialized payload.
/// Metadata bytes are not checksummed; correctness relies on atomic
/// replacement, strict decoding, and semantic validation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockMetaFileHeader {
    /// Fixed file magic used to identify Vecton block metadata.
    pub magic: [u8; 8],
    /// Version of this fixed header and serialized payload layout.
    pub version: u32,
    /// Fixed header length in bytes.
    pub header_len: u32,
    /// Serialized payload length in bytes.
    pub payload_len: u64,
}

impl BlockMetaFileHeader {
    pub const fn encoded_len() -> usize {
        BLOCK_META_HEADER_LEN
    }

    fn for_payload(payload_len: usize) -> StoreResult<Self> {
        let payload_len =
            u64::try_from(payload_len).map_err(|_| invalid_argument("meta payload length does not fit in u64"))?;
        let header = Self {
            magic: BLOCK_META_MAGIC,
            version: BLOCK_META_VERSION,
            header_len: BLOCK_META_HEADER_LEN as u32,
            payload_len,
        };
        header.validate()?;
        Ok(header)
    }

    fn decode(encoded: &[u8]) -> StoreResult<Self> {
        if encoded.len() != BLOCK_META_HEADER_LEN {
            return Err(corrupt("invalid meta header length"));
        }

        let mut magic = [0u8; 8];
        magic.copy_from_slice(&encoded[0..8]);

        Ok(Self {
            magic,
            version: u32::from_le_bytes(encoded[8..12].try_into().expect("fixed header slice")),
            header_len: u32::from_le_bytes(encoded[12..16].try_into().expect("fixed header slice")),
            payload_len: u64::from_le_bytes(encoded[16..24].try_into().expect("fixed header slice")),
        })
    }

    fn encode(self) -> [u8; BLOCK_META_HEADER_LEN] {
        let mut encoded = [0u8; BLOCK_META_HEADER_LEN];
        encoded[0..8].copy_from_slice(&self.magic);
        encoded[8..12].copy_from_slice(&self.version.to_le_bytes());
        encoded[12..16].copy_from_slice(&self.header_len.to_le_bytes());
        encoded[16..24].copy_from_slice(&self.payload_len.to_le_bytes());
        encoded
    }

    fn validate(self) -> StoreResult<()> {
        if self.magic != BLOCK_META_MAGIC {
            return Err(corrupt("invalid block meta magic"));
        }
        if self.version != BLOCK_META_VERSION {
            return Err(corrupt("unsupported block meta version"));
        }
        if self.header_len != BLOCK_META_HEADER_LEN as u32 {
            return Err(corrupt("unsupported block meta header length"));
        }
        if self.payload_len == 0 {
            return Err(corrupt("block meta payload length must be non-zero"));
        }
        if self.payload_len > MAX_META_PAYLOAD_LEN as u64 {
            return Err(corrupt("block meta payload length exceeds limit"));
        }
        Ok(())
    }
}

/// Self-describing metadata for one local block.
/// The metadata state is the publication point for local reads.
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
    /// StorageChunk size used for local buffering and future data checksums.
    /// This is not a transport frame size.
    pub chunk_size: u64,
    /// Checksum algorithm for StorageChunk data in `.blk`.
    /// This does not protect the `.meta` header or payload.
    pub checksum_kind: ChecksumKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChecksumKind {
    None,
}

/// Source-independent effective length of this block.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockSource {
    /// Valid logical length of this block.
    /// Ready `.blk` files must have exactly this length.
    pub effective_block_len: u64,
}

/// Local visibility state persisted in metadata.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockVisibility {
    /// Local block visibility state.
    pub block_state: BlockState,
    /// Logical block stamp persisted when visibility is published.
    pub block_stamp: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlockState {
    Loading,
    Ready,
    Corrupt,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FullBlockFileStoreConfig {
    pub data_root: PathBuf,
}

impl FullBlockFileStoreConfig {
    pub fn new(data_root: PathBuf) -> Self {
        Self { data_root }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateLoadingBlockRequest {
    pub group_id: ShardGroupId,
    pub block_id: BlockId,
    pub block_size: u64,
    pub chunk_size: u64,
    pub effective_block_len: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockPaths {
    pub data_path: PathBuf,
    pub meta_path: PathBuf,
    pub temp_meta_path: PathBuf,
    pub staging_data_path: PathBuf,
    pub staging_meta_path: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveredBlock {
    pub meta: BlockMetaPayload,
}

/// FullBlockFileStore is the current default LocalBlockStore implementation.
/// It stores complete effective block files and publishes readability through `.meta`.
/// A block becomes locally readable only after metadata is published as Ready.
#[derive(Clone, Debug)]
pub struct FullBlockFileStore {
    config: FullBlockFileStoreConfig,
}

impl FullBlockFileStore {
    pub fn new(config: FullBlockFileStoreConfig) -> Self {
        Self { config }
    }

    pub fn create_loading_block(&self, req: CreateLoadingBlockRequest) -> StoreResult<BlockMetaPayload> {
        validate_create_block_shape(req.block_size, req.chunk_size, req.effective_block_len)?;

        let paths = self.paths(req.group_id, req.block_id);
        let parent = paths.parent_dir()?;
        let staging_parent = paths.staging_parent_dir()?;
        self.ensure_group_dirs(req.group_id)?;
        fs::create_dir_all(parent)?;
        fs::create_dir_all(staging_parent)?;
        if paths.meta_path.exists() {
            return Err(invalid_argument(format!(
                "block already exists: block_id={}",
                req.block_id
            )));
        }
        if paths.data_path.exists() {
            return Err(invalid_argument(format!(
                "block data exists without published metadata: block_id={}",
                req.block_id
            )));
        }
        if paths.staging_data_path.exists() || paths.staging_meta_path.exists() {
            return Err(invalid_argument(format!(
                "staging block already exists: block_id={}",
                req.block_id
            )));
        }
        remove_file_if_exists(&paths.temp_meta_path)?;

        let data = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&paths.staging_data_path)?;
        data.sync_all()?;

        let meta = BlockMetaPayload {
            identity: BlockIdentity {
                block_id: req.block_id,
                group_id: req.group_id,
            },
            format: BlockFormat {
                format_id: BLOCK_FORMAT_FULL_EFFECTIVE,
                block_size: req.block_size,
                chunk_size: req.chunk_size,
                checksum_kind: ChecksumKind::None,
            },
            source: BlockSource {
                effective_block_len: req.effective_block_len,
            },
            visibility: BlockVisibility {
                block_state: BlockState::Loading,
                block_stamp: 0,
            },
        };
        validate_staging_meta_payload(&meta, req.group_id, req.block_id)?;
        write_staging_meta_new(&paths, &meta)?;
        Ok(meta)
    }

    pub fn load_meta(&self, group_id: ShardGroupId, block_id: BlockId) -> StoreResult<BlockMetaPayload> {
        let paths = self.paths(group_id, block_id);
        let meta = read_meta_file(&paths.meta_path)?;
        validate_final_meta_payload(&meta, group_id, block_id)?;
        Ok(meta)
    }

    /// Writes bytes to an unpublished staging block.
    /// Overwrites are allowed before publication so a write stream can retry frames.
    /// Ready blocks are immutable in this store.
    pub fn write_at(&self, group_id: ShardGroupId, block_id: BlockId, offset: u64, data: Bytes) -> StoreResult<()> {
        let paths = self.paths(group_id, block_id);
        if paths.meta_path.exists() {
            let final_meta = self.load_meta(group_id, block_id)?;
            return reject_write_to_published(&final_meta);
        }
        if paths.data_path.exists() {
            return Err(invalid_argument("published block data exists without final metadata"));
        }

        let meta = self.load_staging_meta(group_id, block_id)?;
        ensure_loading(&meta)?;
        let len = u64::try_from(data.len()).map_err(|_| invalid_argument("write length does not fit in u64"))?;
        validate_range(&meta, offset, len)?;

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&paths.staging_data_path)
            .map_err(|err| map_staging_data_open_error(err, "staging block data file is missing"))?;
        let current_len = file.metadata()?.len();
        if offset > current_len {
            return Err(invalid_argument("write would create a block data gap"));
        }
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&data)?;
        Ok(())
    }

    /// Publishes a complete staging block as Ready.
    /// This does not support appending to or replacing an existing Ready block.
    pub fn publish_ready(
        &self,
        group_id: ShardGroupId,
        block_id: BlockId,
        block_stamp: u64,
    ) -> StoreResult<BlockMetaPayload> {
        let paths = self.paths(group_id, block_id);
        if paths.meta_path.exists() {
            let final_meta = self.load_meta(group_id, block_id)?;
            return reject_publish_to_published(&final_meta);
        }
        if paths.data_path.exists() {
            return Err(invalid_argument("published block data exists without final metadata"));
        }

        let meta = self.load_staging_meta(group_id, block_id)?;
        ensure_publishable(&meta)?;
        sync_and_validate_staging_data_file(&paths, &meta)?;

        let mut ready = meta;
        ready.visibility.block_state = BlockState::Ready;
        ready.visibility.block_stamp = block_stamp;
        validate_final_meta_payload(&ready, group_id, block_id)?;

        let parent = paths.parent_dir()?;
        fs::create_dir_all(parent)?;
        fs::rename(&paths.staging_data_path, &paths.data_path)?;
        sync_parent_dir(parent)?;
        validate_ready_data_file(&paths, &ready)?;
        write_meta_new(&paths, &ready)?;
        remove_staging_meta_after_commit(&paths.staging_meta_path);
        if let Some(staging_parent) = paths.staging_meta_path.parent() {
            sync_parent_dir_after_commit(staging_parent);
        }
        Ok(ready)
    }

    /// Reads only from blocks whose metadata state is Ready.
    pub fn read_at(&self, group_id: ShardGroupId, block_id: BlockId, offset: u64, len: u64) -> StoreResult<Bytes> {
        let meta = self.load_meta(group_id, block_id)?;
        ensure_readable(&meta)?;
        validate_range(&meta, offset, len)?;

        let paths = self.paths(group_id, block_id);
        validate_ready_data_file(&paths, &meta)?;

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
        match meta.visibility.block_state {
            BlockState::Ready => {
                validate_ready_data_file(&paths, &meta)?;
                Ok(RecoveredBlock { meta })
            }
            BlockState::Loading => Err(corrupt("loading block metadata is not valid final metadata")),
            BlockState::Corrupt => Err(corrupt("block metadata marks local block corrupt")),
        }
    }

    pub fn delete_block(&self, group_id: ShardGroupId, block_id: BlockId) -> StoreResult<()> {
        let paths = self.paths(group_id, block_id);
        remove_file_if_exists(&paths.meta_path)?;
        remove_file_if_exists(&paths.data_path)?;
        remove_file_if_exists(&paths.temp_meta_path)?;
        remove_file_if_exists(&paths.staging_data_path)?;
        remove_file_if_exists(&paths.staging_meta_path)?;
        if let Some(parent) = paths.data_path.parent() {
            if parent.exists() {
                sync_parent_dir(parent)?;
            }
        }
        if let Some(parent) = paths.staging_data_path.parent() {
            if parent.exists() {
                sync_parent_dir(parent)?;
            }
        }
        Ok(())
    }

    pub fn paths(&self, group_id: ShardGroupId, block_id: BlockId) -> BlockPaths {
        let (hash_a, hash_b) = block_hash_prefix(block_id);
        let stem = format!(
            "b_{:016x}_{:08x}",
            block_id.data_handle_id.as_raw(),
            block_id.index.as_raw()
        );
        let dir = self
            .group_dir(group_id)
            .join("blocks")
            .join(format!("{hash_a:02x}"))
            .join(format!("{hash_b:02x}"));
        let tmp_dir = self.group_dir(group_id).join("tmp");

        BlockPaths {
            data_path: dir.join(format!("{stem}.blk")),
            meta_path: dir.join(format!("{stem}.meta")),
            temp_meta_path: dir.join(format!("{stem}.meta.tmp")),
            staging_data_path: tmp_dir.join(format!("{stem}.blk.tmp")),
            staging_meta_path: tmp_dir.join(format!("{stem}.meta.tmp")),
        }
    }

    fn load_staging_meta(&self, group_id: ShardGroupId, block_id: BlockId) -> StoreResult<BlockMetaPayload> {
        let paths = self.paths(group_id, block_id);
        let meta = read_meta_file(&paths.staging_meta_path)?;
        validate_staging_meta_payload(&meta, group_id, block_id)?;
        Ok(meta)
    }

    fn group_dir(&self, group_id: ShardGroupId) -> PathBuf {
        self.config
            .data_root
            .join("groups")
            .join(format!("g_{:016x}", group_id.as_raw()))
    }

    fn ensure_group_dirs(&self, group_id: ShardGroupId) -> StoreResult<()> {
        let group_dir = self.group_dir(group_id);
        fs::create_dir_all(group_dir.join("blocks"))?;
        fs::create_dir_all(group_dir.join("tmp"))?;
        fs::create_dir_all(group_dir.join("gc"))?;
        Ok(())
    }
}

impl BlockPaths {
    fn parent_dir(&self) -> StoreResult<&Path> {
        self.data_path
            .parent()
            .ok_or_else(|| invalid_argument("block path has no parent directory"))
    }

    fn staging_parent_dir(&self) -> StoreResult<&Path> {
        self.staging_data_path
            .parent()
            .ok_or_else(|| invalid_argument("staging block path has no parent directory"))
    }
}

/// Local block store operations for the worker-local `.blk` + `.meta` format.
///
/// Ready blocks are immutable in this store. Tail append and rebuild require
/// explicit store operations and are not part of this minimal implementation.
pub trait LocalBlockStore {
    fn create_loading_block(&self, req: CreateLoadingBlockRequest) -> StoreResult<BlockMetaPayload>;

    fn write_at(&self, group_id: ShardGroupId, block_id: BlockId, offset: u64, data: Bytes) -> StoreResult<()>;

    fn publish_ready(
        &self,
        group_id: ShardGroupId,
        block_id: BlockId,
        block_stamp: u64,
    ) -> StoreResult<BlockMetaPayload>;

    fn read_at(&self, group_id: ShardGroupId, block_id: BlockId, offset: u64, len: u64) -> StoreResult<Bytes>;

    fn load_meta(&self, group_id: ShardGroupId, block_id: BlockId) -> StoreResult<BlockMetaPayload>;

    fn recover_block(&self, group_id: ShardGroupId, block_id: BlockId) -> StoreResult<RecoveredBlock>;

    fn delete_block(&self, group_id: ShardGroupId, block_id: BlockId) -> StoreResult<()>;
}

impl LocalBlockStore for FullBlockFileStore {
    fn create_loading_block(&self, req: CreateLoadingBlockRequest) -> StoreResult<BlockMetaPayload> {
        FullBlockFileStore::create_loading_block(self, req)
    }

    fn write_at(&self, group_id: ShardGroupId, block_id: BlockId, offset: u64, data: Bytes) -> StoreResult<()> {
        FullBlockFileStore::write_at(self, group_id, block_id, offset, data)
    }

    fn publish_ready(
        &self,
        group_id: ShardGroupId,
        block_id: BlockId,
        block_stamp: u64,
    ) -> StoreResult<BlockMetaPayload> {
        FullBlockFileStore::publish_ready(self, group_id, block_id, block_stamp)
    }

    fn read_at(&self, group_id: ShardGroupId, block_id: BlockId, offset: u64, len: u64) -> StoreResult<Bytes> {
        FullBlockFileStore::read_at(self, group_id, block_id, offset, len)
    }

    fn load_meta(&self, group_id: ShardGroupId, block_id: BlockId) -> StoreResult<BlockMetaPayload> {
        FullBlockFileStore::load_meta(self, group_id, block_id)
    }

    fn recover_block(&self, group_id: ShardGroupId, block_id: BlockId) -> StoreResult<RecoveredBlock> {
        FullBlockFileStore::recover_block(self, group_id, block_id)
    }

    fn delete_block(&self, group_id: ShardGroupId, block_id: BlockId) -> StoreResult<()> {
        FullBlockFileStore::delete_block(self, group_id, block_id)
    }
}

/// Placeholder for higher worker data path wiring.
///
/// The concrete local format lives in FullBlockFileStore and remains
/// detached from upper worker services until wired explicitly.
#[derive(Clone, Debug)]
pub struct BlockStore {
    /// Worker-local StorageChunk size.
    /// This is the local buffering unit, not a transport frame size.
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

#[cfg(test)]
fn write_meta_atomic(paths: &BlockPaths, meta: &BlockMetaPayload) -> StoreResult<()> {
    validate_final_meta_payload(meta, meta.identity.group_id, meta.identity.block_id)?;
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
    validate_final_meta_payload(meta, meta.identity.group_id, meta.identity.block_id)?;
    let parent = paths.parent_dir()?;
    fs::create_dir_all(parent)?;
    if paths.meta_path.exists() {
        return Err(invalid_argument("block metadata already exists"));
    }
    remove_file_if_exists(&paths.temp_meta_path)?;
    let encoded = encode_meta(meta)?;
    {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&paths.temp_meta_path)?;
        file.write_all(&encoded)?;
        file.sync_all()?;
    }
    sync_parent_dir(parent)?;
    if let Err(err) = fs::hard_link(&paths.temp_meta_path, &paths.meta_path) {
        let _ = remove_file_if_exists(&paths.temp_meta_path);
        if err.kind() == std::io::ErrorKind::AlreadyExists {
            return Err(invalid_argument("block metadata already exists"));
        }
        return Err(WorkerError::from(err));
    }

    // Final `.meta` visibility is the local commit point. Cleanup and the
    // post-commit directory sync are best-effort because reads now have a
    // complete Ready metadata file and an already-validated full-block `.blk`.
    remove_temp_meta_after_commit(&paths.temp_meta_path);
    sync_parent_dir_after_commit(parent);
    Ok(())
}

fn write_staging_meta_new(paths: &BlockPaths, meta: &BlockMetaPayload) -> StoreResult<()> {
    validate_staging_meta_payload(meta, meta.identity.group_id, meta.identity.block_id)?;
    let parent = paths.staging_parent_dir()?;
    fs::create_dir_all(parent)?;
    let encoded = encode_meta(meta)?;
    {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&paths.staging_meta_path)?;
        file.write_all(&encoded)?;
        file.sync_all()?;
    }
    sync_parent_dir(parent)?;
    Ok(())
}

fn encode_meta(meta: &BlockMetaPayload) -> StoreResult<Vec<u8>> {
    let payload = encode_to_vec(meta, standard()).map_err(|err| WorkerError::Internal(err.to_string()))?;
    let header = BlockMetaFileHeader::for_payload(payload.len())?;
    let mut encoded = Vec::with_capacity(BlockMetaFileHeader::encoded_len() + payload.len());
    encoded.extend_from_slice(&header.encode());
    encoded.extend_from_slice(&payload);
    Ok(encoded)
}

fn read_meta_file(path: &Path) -> StoreResult<BlockMetaPayload> {
    let mut file = File::open(path)?;
    let mut encoded_header = [0u8; BLOCK_META_HEADER_LEN];
    file.read_exact(&mut encoded_header)
        .map_err(|err| map_meta_read_error(err, "block meta file is shorter than the header"))?;

    let header = BlockMetaFileHeader::decode(&encoded_header)?;
    header.validate()?;
    let payload_len = usize::try_from(header.payload_len).map_err(|_| corrupt("meta payload length is too large"))?;
    let mut payload = vec![0; payload_len];
    file.read_exact(&mut payload)
        .map_err(|err| map_meta_read_error(err, "block meta payload is shorter than declared length"))?;
    let mut trailing = [0u8; 1];
    if file.read(&mut trailing)? != 0 {
        return Err(corrupt("block meta file has trailing bytes"));
    }
    let (meta, consumed) =
        decode_from_slice::<BlockMetaPayload, _>(&payload, standard()).map_err(|err| corrupt(err.to_string()))?;
    if consumed != payload.len() {
        return Err(corrupt("block meta payload has trailing bytes"));
    }
    Ok(meta)
}

fn validate_final_meta_payload(meta: &BlockMetaPayload, group_id: ShardGroupId, block_id: BlockId) -> StoreResult<()> {
    validate_meta_payload_shape(meta, group_id, block_id)?;
    match meta.visibility.block_state {
        BlockState::Ready | BlockState::Corrupt => Ok(()),
        BlockState::Loading => Err(corrupt("loading block metadata is not valid final metadata")),
    }
}

fn validate_staging_meta_payload(
    meta: &BlockMetaPayload,
    group_id: ShardGroupId,
    block_id: BlockId,
) -> StoreResult<()> {
    validate_meta_payload_shape(meta, group_id, block_id)?;
    match meta.visibility.block_state {
        BlockState::Loading => Ok(()),
        BlockState::Ready | BlockState::Corrupt => Err(corrupt("published block state is not valid staging metadata")),
    }
}

fn validate_meta_payload_shape(meta: &BlockMetaPayload, group_id: ShardGroupId, block_id: BlockId) -> StoreResult<()> {
    if meta.identity.group_id != group_id {
        return Err(corrupt("block meta group id does not match path"));
    }
    if meta.identity.block_id != block_id {
        return Err(corrupt("block meta block id does not match path"));
    }
    if meta.format.format_id != BLOCK_FORMAT_FULL_EFFECTIVE {
        return Err(corrupt("unsupported block format id"));
    }
    if meta.format.checksum_kind != ChecksumKind::None {
        return Err(corrupt("unsupported checksum kind"));
    }
    validate_meta_block_shape(
        meta.format.block_size,
        meta.format.chunk_size,
        meta.source.effective_block_len,
    )?;
    Ok(())
}

fn validate_create_block_shape(block_size: u64, chunk_size: u64, effective_block_len: u64) -> StoreResult<()> {
    validate_block_shape(block_size, chunk_size, effective_block_len, invalid_argument)
}

fn validate_meta_block_shape(block_size: u64, chunk_size: u64, effective_block_len: u64) -> StoreResult<()> {
    validate_block_shape(block_size, chunk_size, effective_block_len, corrupt)
}

fn validate_block_shape(
    block_size: u64,
    chunk_size: u64,
    effective_block_len: u64,
    error: fn(String) -> WorkerError,
) -> StoreResult<()> {
    if block_size == 0 {
        return Err(error("block size must be non-zero".to_string()));
    }
    if chunk_size == 0 {
        return Err(error("chunk size must be non-zero".to_string()));
    }
    if !block_size.is_multiple_of(chunk_size) {
        return Err(error("block size must be a multiple of chunk size".to_string()));
    }
    if effective_block_len == 0 {
        return Err(error("effective block length must be non-zero".to_string()));
    }
    if effective_block_len > block_size {
        return Err(error("effective block length exceeds block size".to_string()));
    }
    Ok(())
}

fn ensure_loading(meta: &BlockMetaPayload) -> StoreResult<()> {
    match meta.visibility.block_state {
        BlockState::Loading => Ok(()),
        BlockState::Ready => Err(invalid_argument("ready block cannot be written")),
        BlockState::Corrupt => Err(corrupt("corrupt block cannot be written")),
    }
}

fn ensure_publishable(meta: &BlockMetaPayload) -> StoreResult<()> {
    match meta.visibility.block_state {
        BlockState::Loading => Ok(()),
        BlockState::Ready => Err(invalid_argument("ready block cannot be published again")),
        BlockState::Corrupt => Err(corrupt("corrupt block cannot be published ready")),
    }
}

fn ensure_readable(meta: &BlockMetaPayload) -> StoreResult<()> {
    match meta.visibility.block_state {
        BlockState::Ready => Ok(()),
        BlockState::Loading => Err(invalid_argument("loading block is not readable")),
        BlockState::Corrupt => Err(corrupt("corrupt block is not readable")),
    }
}

fn reject_write_to_published(meta: &BlockMetaPayload) -> StoreResult<()> {
    match meta.visibility.block_state {
        BlockState::Ready => Err(invalid_argument("ready block cannot be written")),
        BlockState::Corrupt => Err(corrupt("corrupt block cannot be written")),
        BlockState::Loading => Err(corrupt("loading block metadata is not valid final metadata")),
    }
}

fn reject_publish_to_published(meta: &BlockMetaPayload) -> StoreResult<BlockMetaPayload> {
    match meta.visibility.block_state {
        BlockState::Ready => Err(invalid_argument("ready block cannot be published again")),
        BlockState::Corrupt => Err(corrupt("corrupt block cannot be published ready")),
        BlockState::Loading => Err(corrupt("loading block metadata is not valid final metadata")),
    }
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

fn sync_and_validate_staging_data_file(paths: &BlockPaths, meta: &BlockMetaPayload) -> StoreResult<()> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&paths.staging_data_path)
        .map_err(|err| map_staging_data_open_error(err, "staging block data file is missing"))?;
    file.sync_all()?;
    validate_ready_data_len(file.metadata()?.len(), meta)
}

fn validate_ready_data_file(paths: &BlockPaths, meta: &BlockMetaPayload) -> StoreResult<()> {
    let metadata =
        fs::metadata(&paths.data_path).map_err(|err| map_data_open_error(err, "ready block data file is missing"))?;
    if !metadata.is_file() {
        return Err(corrupt("ready block data path is not a file"));
    }
    validate_ready_data_len(metadata.len(), meta)
}

fn validate_ready_data_len(actual_len: u64, meta: &BlockMetaPayload) -> StoreResult<()> {
    let expected_len = meta.source.effective_block_len;
    if actual_len != expected_len {
        return Err(corrupt(format!(
            "ready block data length {actual_len} does not match effective block length {expected_len}"
        )));
    }
    Ok(())
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

fn remove_file_if_exists(path: &Path) -> StoreResult<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(WorkerError::from(err)),
    }
}

fn remove_temp_meta_after_commit(path: &Path) {
    #[cfg(test)]
    if maybe_fail_at(StoreFault::TempMetaCleanup).is_err() {
        return;
    }
    let _ = remove_file_if_exists(path);
}

fn remove_staging_meta_after_commit(path: &Path) {
    #[cfg(test)]
    if maybe_fail_at(StoreFault::StagingMetaCleanup).is_err() {
        return;
    }
    let _ = remove_file_if_exists(path);
}

fn sync_parent_dir_after_commit(parent: &Path) {
    #[cfg(test)]
    if maybe_fail_at(StoreFault::FinalMetaParentSync).is_err() {
        return;
    }
    let _ = sync_parent_dir(parent);
}

fn map_data_open_error(err: std::io::Error, message: &str) -> WorkerError {
    if err.kind() == std::io::ErrorKind::NotFound {
        corrupt(message)
    } else {
        WorkerError::from(err)
    }
}

fn map_staging_data_open_error(err: std::io::Error, message: &str) -> WorkerError {
    if err.kind() == std::io::ErrorKind::NotFound {
        not_found(message)
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

fn map_meta_read_error(err: std::io::Error, message: &str) -> WorkerError {
    if err.kind() == std::io::ErrorKind::UnexpectedEof {
        corrupt(message)
    } else {
        WorkerError::from(err)
    }
}

fn invalid_argument(message: impl Into<String>) -> WorkerError {
    WorkerError::InvalidArgument(message.into())
}

fn not_found(message: impl Into<String>) -> WorkerError {
    WorkerError::NotFound(message.into())
}

fn corrupt(message: impl Into<String>) -> WorkerError {
    WorkerError::Corrupt(message.into())
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StoreFault {
    TempMetaCleanup,
    StagingMetaCleanup,
    FinalMetaParentSync,
}

#[cfg(test)]
thread_local! {
    static STORE_FAULT: std::cell::Cell<Option<StoreFault>> = const { std::cell::Cell::new(None) };
}

#[cfg(test)]
struct StoreFaultGuard;

#[cfg(test)]
impl Drop for StoreFaultGuard {
    fn drop(&mut self) {
        STORE_FAULT.with(|fault| fault.set(None));
    }
}

#[cfg(test)]
fn fail_once_at(fault: StoreFault) -> StoreFaultGuard {
    STORE_FAULT.with(|current| current.set(Some(fault)));
    StoreFaultGuard
}

#[cfg(test)]
fn maybe_fail_at(fault: StoreFault) -> StoreResult<()> {
    let should_fail = STORE_FAULT.with(|current| {
        if current.get() == Some(fault) {
            current.set(None);
            true
        } else {
            false
        }
    });
    if should_fail {
        Err(WorkerError::DiskError(format!("injected store fault at {fault:?}")))
    } else {
        Ok(())
    }
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

    fn store() -> (TempDir, FullBlockFileStore) {
        let temp = TempDir::new().expect("tempdir");
        let store = FullBlockFileStore::new(FullBlockFileStoreConfig::new(temp.path().to_path_buf()));
        (temp, store)
    }

    fn request(
        group_id: ShardGroupId,
        block_id: BlockId,
        block_size: u64,
        chunk_size: u64,
        effective_block_len: u64,
    ) -> CreateLoadingBlockRequest {
        CreateLoadingBlockRequest {
            group_id,
            block_id,
            block_size,
            chunk_size,
            effective_block_len,
        }
    }

    fn create_default_block(store: &FullBlockFileStore, group_id: ShardGroupId, block_id: BlockId) {
        store
            .create_loading_block(request(group_id, block_id, 4096, 1024, 4096))
            .expect("create loading block");
    }

    fn publish_default_block(
        store: &FullBlockFileStore,
        group_id: ShardGroupId,
        block_id: BlockId,
    ) -> BlockMetaPayload {
        create_default_block(store, group_id, block_id);
        store
            .write_at(group_id, block_id, 0, Bytes::from(vec![1; 4096]))
            .expect("write default block");
        store
            .publish_ready(group_id, block_id, 1)
            .expect("publish default block")
    }

    fn assert_corrupt<T: std::fmt::Debug>(result: Result<T, WorkerError>) {
        match result.expect_err("operation should fail") {
            WorkerError::Corrupt(_) => {}
            other => panic!("expected corrupt error, got {other:?}"),
        }
    }

    fn assert_invalid_argument<T: std::fmt::Debug>(result: Result<T, WorkerError>) {
        match result.expect_err("operation should fail") {
            WorkerError::InvalidArgument(_) => {}
            other => panic!("expected invalid argument error, got {other:?}"),
        }
    }

    fn assert_not_found<T: std::fmt::Debug>(result: Result<T, WorkerError>) {
        match result.expect_err("operation should fail") {
            WorkerError::NotFound(_) => {}
            other => panic!("expected not found error, got {other:?}"),
        }
    }

    fn persist_meta(store: &FullBlockFileStore, group_id: ShardGroupId, block_id: BlockId, meta: &BlockMetaPayload) {
        let paths = store.paths(group_id, block_id);
        write_meta_atomic(&paths, meta).expect("persist meta");
    }

    fn persist_raw_meta_payload(paths: &BlockPaths, meta: &BlockMetaPayload) {
        let payload = encode_to_vec(meta, standard()).expect("encode payload");
        let header = BlockMetaFileHeader::for_payload(payload.len()).expect("header");
        let mut encoded = Vec::with_capacity(BlockMetaFileHeader::encoded_len() + payload.len());
        encoded.extend_from_slice(&header.encode());
        encoded.extend_from_slice(&payload);
        fs::write(&paths.meta_path, encoded).expect("write raw meta");
    }

    fn overwrite_header_u32(paths: &BlockPaths, offset: u64, value: u32) {
        let mut file = OpenOptions::new()
            .write(true)
            .open(&paths.meta_path)
            .expect("open meta");
        file.seek(SeekFrom::Start(offset)).expect("seek header field");
        file.write_all(&value.to_le_bytes()).expect("write header field");
        file.sync_all().expect("sync meta");
    }

    fn overwrite_header_u64(paths: &BlockPaths, offset: u64, value: u64) {
        let mut file = OpenOptions::new()
            .write(true)
            .open(&paths.meta_path)
            .expect("open meta");
        file.seek(SeekFrom::Start(offset)).expect("seek header field");
        file.write_all(&value.to_le_bytes()).expect("write header field");
        file.sync_all().expect("sync meta");
    }

    fn set_data_len(paths: &BlockPaths, len: u64) {
        OpenOptions::new()
            .write(true)
            .open(&paths.data_path)
            .expect("open data")
            .set_len(len)
            .expect("set data len");
    }

    #[test]
    fn write_meta_new_does_not_fail_after_final_meta_commit() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        create_default_block(&store, group_id, block_id);
        let paths = store.paths(group_id, block_id);
        let mut meta = store.load_staging_meta(group_id, block_id).expect("load staging meta");
        meta.visibility.block_state = BlockState::Ready;
        meta.visibility.block_stamp = 9;
        let _fault = fail_once_at(StoreFault::TempMetaCleanup);

        let result = write_meta_new(&paths, &meta);

        assert!(result.is_ok(), "final meta commit should be success: {result:?}");
        let reloaded = read_meta_file(&paths.meta_path).expect("read committed meta");
        assert_eq!(reloaded.visibility.block_state, BlockState::Ready);
        assert_eq!(reloaded.visibility.block_stamp, 9);
    }

    #[test]
    fn publish_ready_treats_post_commit_cleanup_failure_as_success() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        create_default_block(&store, group_id, block_id);
        let data = Bytes::from(vec![7; 4096]);
        store.write_at(group_id, block_id, 0, data.clone()).expect("write");
        let _fault = fail_once_at(StoreFault::StagingMetaCleanup);

        let meta = store
            .publish_ready(group_id, block_id, 17)
            .expect("post-commit cleanup failure must not fail publish");

        assert_eq!(meta.visibility.block_state, BlockState::Ready);
        assert_eq!(meta.visibility.block_stamp, 17);
        assert_eq!(store.read_at(group_id, block_id, 0, data.len() as u64).unwrap(), data);
    }

    #[test]
    fn create_loading_block_does_not_create_final_meta() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();

        let meta = store
            .create_loading_block(request(group_id, block_id, 8 * MB, MB, 4 * MB))
            .expect("create loading block");
        let paths = store.paths(group_id, block_id);

        assert!(!paths.data_path.exists());
        assert!(!paths.meta_path.exists());
        assert!(paths.staging_data_path.exists());
        assert!(paths.staging_meta_path.exists());
        assert_not_found(store.read_at(group_id, block_id, 0, 1));
        assert_eq!(meta.visibility.block_state, BlockState::Loading);
        assert_eq!(meta.visibility.block_stamp, 0);
        assert_eq!(meta.source.effective_block_len, 4 * MB);
        assert_eq!(meta.format.block_size, 8 * MB);
        assert_eq!(meta.format.chunk_size, MB);
        assert_eq!(meta.format.checksum_kind, ChecksumKind::None);
    }

    #[test]
    fn create_loading_block_existing_fails() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        create_default_block(&store, group_id, block_id);
        let paths = store.paths(group_id, block_id);
        fs::write(&paths.staging_data_path, b"existing data").expect("write existing data");
        let meta_before = fs::read(&paths.staging_meta_path).expect("read meta before");

        store
            .create_loading_block(request(group_id, block_id, 4096, 1024, 4096))
            .expect_err("existing block should be rejected");

        assert_eq!(
            fs::read(&paths.staging_data_path).expect("read existing data"),
            b"existing data"
        );
        assert_eq!(
            fs::read(&paths.staging_meta_path).expect("read meta after"),
            meta_before
        );
    }

    #[test]
    fn write_at_does_not_publish_visibility() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        create_default_block(&store, group_id, block_id);
        let data = Bytes::from(vec![7; 4096]);

        store.write_at(group_id, block_id, 0, data).expect("write");

        assert_not_found(store.read_at(group_id, block_id, 0, 8));
        assert!(store.load_meta(group_id, block_id).is_err());
    }

    #[test]
    fn write_at_allows_staging_overwrite() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        create_default_block(&store, group_id, block_id);
        let first = Bytes::from_static(b"first");
        let second = Bytes::from_static(b"again");

        store.write_at(group_id, block_id, 0, first).expect("write first");
        store
            .write_at(group_id, block_id, 0, second.clone())
            .expect("overwrite staging range");
        store
            .write_at(group_id, block_id, 5, Bytes::from(vec![0; 4091]))
            .expect("fill remaining bytes");
        store.publish_ready(group_id, block_id, 11).expect("publish");

        assert_eq!(
            store.read_at(group_id, block_id, 0, second.len() as u64).unwrap(),
            second
        );
    }

    #[test]
    fn publish_ready_then_read_at_succeeds() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        create_default_block(&store, group_id, block_id);
        let data = Bytes::from(vec![7; 4096]);

        store.write_at(group_id, block_id, 0, data.clone()).expect("write");
        let meta = store.publish_ready(group_id, block_id, 11).expect("publish");
        let paths = store.paths(group_id, block_id);

        assert_eq!(meta.visibility.block_state, BlockState::Ready);
        assert_eq!(meta.visibility.block_stamp, 11);
        assert_eq!(fs::metadata(&paths.data_path).expect("data metadata").len(), 4096);
        assert_eq!(store.read_at(group_id, block_id, 0, data.len() as u64).unwrap(), data);
    }

    #[test]
    fn write_at_rejects_ready_block() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        create_default_block(&store, group_id, block_id);
        store
            .write_at(group_id, block_id, 0, Bytes::from(vec![7; 4096]))
            .expect("write");
        store.publish_ready(group_id, block_id, 1).expect("publish");

        assert_invalid_argument(store.write_at(group_id, block_id, 0, Bytes::from_static(b"x")));
    }

    #[test]
    fn publish_ready_rejects_existing_ready_block() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        create_default_block(&store, group_id, block_id);
        store
            .write_at(group_id, block_id, 0, Bytes::from(vec![7; 4096]))
            .expect("write");
        let meta = store.publish_ready(group_id, block_id, 1).expect("publish");

        assert_invalid_argument(store.publish_ready(group_id, block_id, 2));
        let reloaded = store.load_meta(group_id, block_id).expect("load meta");
        assert_eq!(reloaded.visibility.block_stamp, meta.visibility.block_stamp);
    }

    #[test]
    fn publish_ready_requires_complete_effective_block() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        store
            .create_loading_block(request(group_id, block_id, 4096, 1024, 1536))
            .expect("create loading block");
        let paths = store.paths(group_id, block_id);
        store
            .write_at(group_id, block_id, 0, Bytes::from(vec![7; 1024]))
            .expect("write first bytes");

        assert_corrupt(store.publish_ready(group_id, block_id, 1));

        assert!(!paths.meta_path.exists());
    }

    #[test]
    fn publish_ready_creates_final_meta_only_on_success() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        store
            .create_loading_block(request(group_id, block_id, 4096, 1024, 1536))
            .expect("create loading block");
        let paths = store.paths(group_id, block_id);
        store
            .write_at(group_id, block_id, 0, Bytes::from(vec![7; 1024]))
            .expect("write first bytes");

        assert_corrupt(store.publish_ready(group_id, block_id, 1));
        assert!(!paths.meta_path.exists());

        store
            .write_at(group_id, block_id, 1024, Bytes::from(vec![8; 512]))
            .expect("finish staging bytes");
        let meta = store.publish_ready(group_id, block_id, 2).expect("publish");

        assert!(paths.meta_path.exists());
        assert_eq!(meta.visibility.block_state, BlockState::Ready);
        assert_eq!(meta.visibility.block_stamp, 2);
    }

    #[test]
    fn ready_block_requires_exact_effective_len() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        store
            .create_loading_block(request(group_id, block_id, 4096, 1024, 3072))
            .expect("create loading block");
        let paths = store.paths(group_id, block_id);
        store
            .write_at(group_id, block_id, 0, Bytes::from(vec![9; 3072]))
            .expect("write");
        store.publish_ready(group_id, block_id, 1).expect("publish");

        set_data_len(&paths, 2048);
        assert_corrupt(store.recover_block(group_id, block_id));

        set_data_len(&paths, 4096);
        assert_corrupt(store.recover_block(group_id, block_id));

        set_data_len(&paths, 3072);
        let recovered = store.recover_block(group_id, block_id).expect("recover ready block");
        assert_eq!(recovered.meta.visibility.block_state, BlockState::Ready);
    }

    #[test]
    fn ready_block_tail_effective_len() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        store
            .create_loading_block(request(group_id, block_id, 32 * MB, MB, 4 * MB))
            .expect("create loading block");
        let data = Bytes::from(vec![3; (4 * MB) as usize]);

        store.write_at(group_id, block_id, 0, data).expect("write");
        store.publish_ready(group_id, block_id, 1).expect("publish");

        let paths = store.paths(group_id, block_id);
        assert_eq!(fs::metadata(paths.data_path).expect("data metadata").len(), 4 * MB);
    }

    #[test]
    fn read_at_rejects_unpublished_staging_block() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        create_default_block(&store, group_id, block_id);
        store
            .write_at(group_id, block_id, 0, Bytes::from(vec![4; 4096]))
            .expect("write");

        assert_not_found(store.read_at(group_id, block_id, 0, 8));
    }

    #[test]
    fn read_at_rejects_corrupt_block() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        publish_default_block(&store, group_id, block_id);

        let mut meta = store.load_meta(group_id, block_id).expect("load meta");
        meta.visibility.block_state = BlockState::Corrupt;
        persist_meta(&store, group_id, block_id, &meta);

        assert_corrupt(store.read_at(group_id, block_id, 0, 8));
    }

    #[test]
    fn read_at_bounds_by_effective_len() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        store
            .create_loading_block(request(group_id, block_id, 4096, 1024, 1536))
            .expect("create loading block");
        store
            .write_at(group_id, block_id, 0, Bytes::from(vec![5; 1536]))
            .expect("write");
        store.publish_ready(group_id, block_id, 1).expect("publish");

        assert_invalid_argument(store.read_at(group_id, block_id, 1024, 513));
    }

    #[test]
    fn block_meta_header_is_minimal_fixed_header() {
        assert_eq!(BlockMetaFileHeader::encoded_len(), 24);

        let header = BlockMetaFileHeader::for_payload(17).expect("header");
        assert_eq!(header.magic, BLOCK_META_MAGIC);
        assert_eq!(header.version, BLOCK_META_VERSION);
        assert_eq!(header.header_len, BlockMetaFileHeader::encoded_len() as u32);
        assert_eq!(header.payload_len, 17);
    }

    #[test]
    fn meta_bad_magic_rejected() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        publish_default_block(&store, group_id, block_id);
        let paths = store.paths(group_id, block_id);
        let mut encoded = fs::read(&paths.meta_path).expect("read meta");
        encoded[0] ^= 0xff;
        fs::write(&paths.meta_path, encoded).expect("write meta");

        assert_corrupt(store.load_meta(group_id, block_id));
    }

    #[test]
    fn meta_unsupported_version_rejected() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        publish_default_block(&store, group_id, block_id);
        let paths = store.paths(group_id, block_id);
        overwrite_header_u32(&paths, 8, BLOCK_META_VERSION + 1);

        assert_corrupt(store.load_meta(group_id, block_id));
    }

    #[test]
    fn meta_wrong_header_len_rejected() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        publish_default_block(&store, group_id, block_id);
        let paths = store.paths(group_id, block_id);
        overwrite_header_u32(&paths, 12, BlockMetaFileHeader::encoded_len() as u32 + 4);

        assert_corrupt(store.load_meta(group_id, block_id));
    }

    #[test]
    fn meta_zero_payload_len_rejected() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        publish_default_block(&store, group_id, block_id);
        let paths = store.paths(group_id, block_id);
        overwrite_header_u64(&paths, 16, 0);

        assert_corrupt(store.load_meta(group_id, block_id));
    }

    #[test]
    fn meta_oversized_payload_len_rejected() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        publish_default_block(&store, group_id, block_id);
        let paths = store.paths(group_id, block_id);
        overwrite_header_u64(&paths, 16, MAX_META_PAYLOAD_LEN as u64 + 1);

        assert_corrupt(store.load_meta(group_id, block_id));
    }

    #[test]
    fn meta_truncated_payload_rejected() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        publish_default_block(&store, group_id, block_id);
        let paths = store.paths(group_id, block_id);
        let actual_len = fs::metadata(&paths.meta_path).expect("meta metadata").len();
        let payload_len = actual_len - BlockMetaFileHeader::encoded_len() as u64 + 1;
        overwrite_header_u64(&paths, 16, payload_len);

        assert_corrupt(store.load_meta(group_id, block_id));
    }

    #[test]
    fn payload_semantic_validation_rejects_invalid_lengths() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        publish_default_block(&store, group_id, block_id);
        let paths = store.paths(group_id, block_id);
        let valid = store.load_meta(group_id, block_id).expect("load meta");

        let mut invalid = valid.clone();
        invalid.source.effective_block_len = invalid.format.block_size + 1;
        persist_raw_meta_payload(&paths, &invalid);
        assert_corrupt(store.load_meta(group_id, block_id));

        let mut invalid = valid.clone();
        invalid.format.chunk_size = 0;
        persist_raw_meta_payload(&paths, &invalid);
        assert_corrupt(store.load_meta(group_id, block_id));

        let mut invalid = valid;
        invalid.format.block_size = 4097;
        persist_raw_meta_payload(&paths, &invalid);
        assert_corrupt(store.load_meta(group_id, block_id));
    }

    #[test]
    fn checksum_kind_none_does_not_verify_data() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        create_default_block(&store, group_id, block_id);
        store
            .write_at(group_id, block_id, 0, Bytes::from(vec![1; 4096]))
            .expect("write");
        store.publish_ready(group_id, block_id, 1).expect("publish");
        let paths = store.paths(group_id, block_id);
        {
            let mut file = OpenOptions::new()
                .write(true)
                .open(&paths.data_path)
                .expect("open data");
            file.seek(SeekFrom::Start(7)).expect("seek data");
            file.write_all(&[99]).expect("mutate data");
            file.sync_all().expect("sync data");
        }

        assert_eq!(
            store.read_at(group_id, block_id, 7, 1).unwrap(),
            Bytes::from_static(&[99])
        );
    }

    #[test]
    fn tmp_meta_is_ignored_on_recovery() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        create_default_block(&store, group_id, block_id);
        store
            .write_at(group_id, block_id, 0, Bytes::from(vec![6; 4096]))
            .expect("write");
        store.publish_ready(group_id, block_id, 1).expect("publish");
        let paths = store.paths(group_id, block_id);
        fs::write(&paths.temp_meta_path, b"ignore this").expect("write temp meta");

        let recovered = store.recover_block(group_id, block_id).expect("recover");

        assert_eq!(recovered.meta.visibility.block_state, BlockState::Ready);
        assert_eq!(recovered.meta.identity.block_id, block_id);
    }

    #[test]
    fn recover_ignores_orphan_staging_files() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        create_default_block(&store, group_id, block_id);
        store
            .write_at(group_id, block_id, 0, Bytes::from(vec![8; 4096]))
            .expect("write staging data");

        match store
            .recover_block(group_id, block_id)
            .expect_err("staging block is not recovered as ready")
        {
            WorkerError::NotFound(_) => {}
            other => panic!("expected not found error, got {other:?}"),
        }
        assert_not_found(store.read_at(group_id, block_id, 0, 8));
    }

    #[test]
    fn ready_meta_but_missing_blk_is_corrupt() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        create_default_block(&store, group_id, block_id);
        store
            .write_at(group_id, block_id, 0, Bytes::from(vec![9; 4096]))
            .expect("write");
        store.publish_ready(group_id, block_id, 1).expect("publish");
        let paths = store.paths(group_id, block_id);
        fs::remove_file(paths.data_path).expect("remove data");

        assert_corrupt(store.recover_block(group_id, block_id));
    }

    #[test]
    fn recover_rejects_final_loading_meta() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        create_default_block(&store, group_id, block_id);
        let paths = store.paths(group_id, block_id);
        let mut loading = store
            .create_loading_block(request(
                group_id,
                BlockId::new(DataHandleId::new(0x9999), BlockIndex::new(1)),
                4096,
                1024,
                4096,
            ))
            .expect("create separate loading block");
        loading.identity.block_id = block_id;
        persist_raw_meta_payload(&paths, &loading);

        assert_corrupt(store.load_meta(group_id, block_id));
        assert_corrupt(store.recover_block(group_id, block_id));
    }

    #[test]
    fn delete_block_ignores_missing_files() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        create_default_block(&store, group_id, block_id);

        store.delete_block(group_id, block_id).expect("delete block");
        store.delete_block(group_id, block_id).expect("delete again");

        let paths = store.paths(group_id, block_id);
        assert!(!paths.data_path.exists());
        assert!(!paths.meta_path.exists());
        assert!(!paths.staging_data_path.exists());
        assert!(!paths.staging_meta_path.exists());
    }

    #[test]
    fn load_meta_missing_returns_not_found() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();

        match store
            .load_meta(group_id, block_id)
            .expect_err("missing meta should fail")
        {
            WorkerError::NotFound(_) => {}
            other => panic!("expected not found error, got {other:?}"),
        }
    }

    #[test]
    fn data_writes_reject_gap_before_current_end() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        create_default_block(&store, group_id, block_id);

        assert_invalid_argument(store.write_at(group_id, block_id, 1024, Bytes::from(vec![1; 1024])));
    }

    #[test]
    fn payload_decode_failure_is_rejected() {
        let (_temp, store) = store();
        let (group_id, block_id) = ids();
        publish_default_block(&store, group_id, block_id);
        let paths = store.paths(group_id, block_id);

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
}
