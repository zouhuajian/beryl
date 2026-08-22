// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Local block storage boundary.

use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use beryl_common::error::rpc::{ErrorKind, WorkerErrorKind};
use beryl_types::ids::{BlockId, BlockIndex, InodeId};
use beryl_types::layout::{BlockFormatId, BlockShape};
use beryl_types::{GroupName, Tier};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use super::meta_codec::{
    decode_meta_payload, decode_staging_meta_payload, encode_meta_payload, encode_staging_meta_payload,
};
use crate::error::WorkerError;

pub type StoreResult<T> = Result<T, WorkerError>;

// Metadata file header constants.

const BLOCK_META_MAGIC: [u8; 4] = *b"BRYL";
const BLOCK_META_HEADER_LEN: usize = 20;
const BLOCK_META_VERSION: u32 = 1;
const MAX_META_PAYLOAD_LEN: usize = 16 * 1024 * 1024;
const DELETING_MARKER_VERSION: u32 = 1;

/// Fixed little-endian header for a block metadata file.
/// The header identifies the format and bounds the serialized payload.
/// Metadata bytes are not checksummed; correctness relies on atomic
/// replacement, strict decoding, and semantic validation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockMetaHeader {
    /// Fixed file magic used to identify Beryl block metadata.
    pub magic: [u8; 4],
    /// Version of this fixed header and serialized payload layout.
    pub version: u32,
    /// Fixed header length in bytes.
    pub header_len: u32,
    /// Serialized payload length in bytes.
    pub payload_len: u64,
}

impl BlockMetaHeader {
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

        let mut magic = [0u8; 4];
        magic.copy_from_slice(&encoded[0..4]);

        Ok(Self {
            magic,
            version: u32::from_le_bytes(encoded[4..8].try_into().expect("fixed header slice")),
            header_len: u32::from_le_bytes(encoded[8..12].try_into().expect("fixed header slice")),
            payload_len: u64::from_le_bytes(encoded[12..20].try_into().expect("fixed header slice")),
        })
    }

    fn encode(self) -> [u8; BLOCK_META_HEADER_LEN] {
        let mut encoded = [0u8; BLOCK_META_HEADER_LEN];
        encoded[0..4].copy_from_slice(&self.magic);
        encoded[4..8].copy_from_slice(&self.version.to_le_bytes());
        encoded[8..12].copy_from_slice(&self.header_len.to_le_bytes());
        encoded[12..20].copy_from_slice(&self.payload_len.to_le_bytes());
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
/// Final metadata state is the publication point for local reads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMetaPayload {
    /// Stable block identity.
    pub identity: BlockIdentity,
    /// Format parameters for interpreting `.blk` and `.meta`.
    pub format: BlockFormat,
    /// Source-independent local block length.
    pub source: BlockSource,
    /// Local visibility state.
    pub visibility: BlockVisibility,
    /// Worker-local tier where this replica was materialized.
    pub tier: Tier,
}

/// Stable identity of the local block and owning metadata group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockIdentity {
    /// Stable block identifier.
    pub block_id: BlockId,
    /// Owning metadata group.
    pub group_name: GroupName,
}

/// On-disk Beryl block data/meta interpretation parameters.
///
/// These fields are persisted in BlockMeta so recovery and local reads interpret
/// historical blocks from their own metadata, not from the worker's current
/// StoreBackend / IoEngine configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockFormat {
    /// Identifier of the block file format used by this block.
    pub format_id: BlockFormatId,
    /// Full logical block size from the persisted FileLayout.
    ///
    /// Tail or bounded valid length is stored in
    /// `BlockSource.effective_len`, not by shrinking this field.
    pub block_size: u64,
    /// StorageChunk size used for local buffering and future data checksums.
    /// This is not a transport frame size.
    pub chunk_size: u64,
    /// Checksum algorithm for StorageChunk data in `.blk`.
    /// This does not protect the `.meta` header or payload.
    pub checksum_kind: ChecksumKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChecksumKind {
    None,
}

/// Source-independent effective length of this block.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockSource {
    /// For final Ready/Corrupt metadata, this is the published valid logical length.
    /// For Loading staging metadata, this is only a placeholder and must equal `format.block_size`.
    /// Staging write bounds must use `format.block_size`, not this field.
    pub effective_len: u64,
}

/// Local visibility state for final metadata and staging runtime paths.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockVisibility {
    /// Final metadata may only persist Ready or Corrupt.
    pub block_state: BlockState,
    /// Metadata-assigned logical block stamp.
    /// The local store persists this value at publish time and never generates it.
    pub block_stamp: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlockState {
    /// Runtime/staging only; final metadata protobuf never encodes this state.
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
pub struct CreateStagingBlockRequest {
    pub group_name: GroupName,
    pub block_id: BlockId,
    /// Full logical block size from the persisted FileLayout.
    pub block_size: u64,
    /// Metadata-selected Beryl block data/meta interpretation format.
    pub block_format_id: BlockFormatId,
    pub chunk_size: u32,
    pub checksum_kind: ChecksumKind,
    pub tier: Tier,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublishReadyRequest {
    pub group_name: GroupName,
    pub block_id: BlockId,
    /// Complete effective block length to publish.
    pub effective_len: u64,
    /// Metadata-assigned logical block stamp.
    /// The local store persists this value at publish time and never generates it.
    pub block_stamp: u64,
}

/// Exact local Ready block version authorized for physical reclamation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReclaimBlockRequest {
    pub group_name: GroupName,
    pub block_id: BlockId,
    pub expected_block_stamp: u64,
}

/// Durable local state observed before a reclaim operation starts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReclaimBlockState {
    Ready,
    Deleting,
    Absent,
}

/// Result of one idempotent local reclaim attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReclaimBlockResult {
    Deleted { effective_len: u64 },
    AlreadyAbsent,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockPaths {
    pub data_path: PathBuf,
    pub meta_path: PathBuf,
    pub temp_meta_path: PathBuf,
    pub staging_data_path: PathBuf,
    pub staging_meta_path: PathBuf,
    pub deleting_marker_path: PathBuf,
    pub temp_deleting_marker_path: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct DeletingMarker {
    version: u32,
    group_name: String,
    block_id: BlockId,
    block_stamp: u64,
    effective_len: u64,
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

    /// Creates an unpublished staging block.
    /// This does not create final `.meta` and does not make the block readable.
    pub fn create_staging_block(&self, req: CreateStagingBlockRequest) -> StoreResult<BlockMetaPayload> {
        validate_store_block_shape(
            req.block_format_id,
            req.block_size,
            req.chunk_size,
            req.block_size,
            invalid_argument,
        )?;

        let paths = self.paths(&req.group_name, req.block_id);
        let parent = paths.parent_dir()?;
        let staging_parent = paths.staging_parent_dir()?;
        self.ensure_group_dirs(&req.group_name)?;
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
                group_name: req.group_name.clone(),
            },
            format: BlockFormat {
                format_id: req.block_format_id,
                block_size: req.block_size,
                chunk_size: u64::from(req.chunk_size),
                checksum_kind: req.checksum_kind,
            },
            source: BlockSource {
                effective_len: req.block_size,
            },
            visibility: BlockVisibility {
                block_state: BlockState::Loading,
                block_stamp: 0,
            },
            tier: req.tier,
        };
        validate_staging_meta_payload(&meta, &req.group_name, req.block_id)?;
        write_staging_meta_new(&paths, &meta)?;
        Ok(meta)
    }

    pub fn load_meta(&self, group_name: &GroupName, block_id: BlockId) -> StoreResult<BlockMetaPayload> {
        let paths = self.paths(group_name, block_id);
        let meta = read_meta_file(&paths.meta_path)?;
        validate_final_meta_payload(&meta, group_name, block_id)?;
        Ok(meta)
    }

    /// Writes bytes to an unpublished staging block.
    /// The storage primitive permits overwriting an existing staging prefix;
    /// the current ordered `WriteBlock` RPC only appends at its owned cursor.
    /// Ready blocks are immutable in this store, and writes do not change block stamps.
    pub fn write_at(&self, group_name: &GroupName, block_id: BlockId, offset: u64, data: Bytes) -> StoreResult<()> {
        let paths = self.paths(group_name, block_id);
        if paths.meta_path.exists() {
            let final_meta = self.load_meta(group_name, block_id)?;
            return reject_write_to_published(&final_meta);
        }
        if paths.data_path.exists() {
            return Err(invalid_argument("published block data exists without final metadata"));
        }

        let meta = self.load_staging_meta(group_name, block_id)?;
        let len = u64::try_from(data.len()).map_err(|_| invalid_argument("write length does not fit in u64"))?;
        validate_staging_write_range(&meta, offset, len)?;

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
    /// Persists the metadata-assigned block stamp supplied by the request.
    /// This does not support appending to or replacing an existing Ready block.
    pub fn publish_ready(&self, req: PublishReadyRequest) -> StoreResult<BlockMetaPayload> {
        let group_name = req.group_name;
        let block_id = req.block_id;
        let paths = self.paths(&group_name, block_id);
        if paths.meta_path.exists() {
            let final_meta = self.load_meta(&group_name, block_id)?;
            return reject_publish_to_published(&final_meta);
        }
        if paths.data_path.exists() {
            return Err(invalid_argument("published block data exists without final metadata"));
        }

        let meta = self.load_staging_meta(&group_name, block_id)?;
        ensure_publishable(&meta)?;

        let mut ready = meta;
        ready.source.effective_len = req.effective_len;
        ready.visibility.block_state = BlockState::Ready;
        ready.visibility.block_stamp = req.block_stamp;
        validate_final_meta_payload(&ready, &group_name, block_id)?;
        sync_and_validate_staging_data_file(&paths, &ready)?;

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
    pub fn read_at(&self, group_name: &GroupName, block_id: BlockId, offset: u64, len: u64) -> StoreResult<Bytes> {
        let meta = self.load_meta(group_name, block_id)?;
        validate_published_read_range(&meta, offset, len)?;

        let paths = self.paths(group_name, block_id);
        validate_ready_data_file(&paths, &meta)?;

        let mut file = OpenOptions::new()
            .read(true)
            .open(&paths.data_path)
            .map_err(|err| map_data_open_error(err, "ready block data file is missing"))?;
        file.seek(SeekFrom::Start(offset))?;
        let len = usize::try_from(len).map_err(|_| invalid_argument("read length does not fit in usize"))?;
        let mut buf = vec![0; len];
        file.read_exact(&mut buf)
            .map_err(|err| map_truncated_read_error(err, "ready range is not present in block data file"))?;
        Ok(Bytes::from(buf))
    }

    /// Scan final block metadata under one local group directory.
    ///
    /// The group directory is the source of the report group name. Staging files
    /// under `tmp/` are not scanned, and Ready entries are revalidated against
    /// their local `.blk` file before being reported.
    pub fn scan_group_blocks(&self, group_name: &GroupName) -> StoreResult<Vec<BlockMetaPayload>> {
        let blocks_dir = self.group_dir(group_name).join("blocks");
        if !blocks_dir.exists() {
            return Ok(Vec::new());
        }

        let mut blocks = Vec::new();
        for first_level in fs::read_dir(&blocks_dir)? {
            let first_level = first_level?;
            if !first_level.file_type()?.is_dir() {
                continue;
            }
            for second_level in fs::read_dir(first_level.path())? {
                let second_level = second_level?;
                if !second_level.file_type()?.is_dir() {
                    continue;
                }
                for entry in fs::read_dir(second_level.path())? {
                    let entry = entry?;
                    if !entry.file_type()?.is_file() {
                        continue;
                    }
                    let path = entry.path();
                    if path.extension().and_then(|ext| ext.to_str()) != Some("meta") {
                        continue;
                    }
                    let meta = read_meta_file(&path)?;
                    let block_id = meta.identity.block_id;
                    validate_final_meta_payload(&meta, group_name, block_id)?;
                    if meta.visibility.block_state != BlockState::Ready {
                        continue;
                    }
                    let paths = self.paths(group_name, block_id);
                    if paths.deleting_marker_path.exists() {
                        continue;
                    }
                    validate_ready_data_file(&paths, &meta)?;
                    blocks.push(meta);
                }
            }
        }
        blocks.sort_by_key(|meta| {
            (
                meta.identity.block_id.inode_id.as_raw(),
                meta.identity.block_id.index.as_raw(),
            )
        });
        Ok(blocks)
    }

    /// Validates the exact Ready block version before reader exclusion begins.
    ///
    /// A persisted marker is sufficient to resume an interrupted deletion. A
    /// fully absent Ready block is idempotent success, while unmarked final data
    /// without metadata is left untouched because its stamp cannot be proven.
    pub fn inspect_reclaim_block(&self, req: &ReclaimBlockRequest) -> StoreResult<ReclaimBlockState> {
        validate_reclaim_request(req)?;
        let paths = self.paths(&req.group_name, req.block_id);
        if paths.deleting_marker_path.exists() {
            let marker = read_deleting_marker(&paths.deleting_marker_path)?;
            validate_deleting_marker(&marker, req, &paths.deleting_marker_path)?;
            validate_deleting_marker_against_final_meta(&paths, &marker)?;
            return Ok(ReclaimBlockState::Deleting);
        }

        match self.load_meta(&req.group_name, req.block_id) {
            Ok(meta) => {
                ensure_readable(&meta)?;
                validate_reclaim_stamp(req, meta.visibility.block_stamp)?;
                Ok(ReclaimBlockState::Ready)
            }
            Err(WorkerError::NotFound(_)) => {
                if paths.data_path.exists()
                    || paths.temp_meta_path.exists()
                    || paths.staging_data_path.exists()
                    || paths.staging_meta_path.exists()
                    || paths.temp_deleting_marker_path.exists()
                {
                    return Err(corrupt(format!(
                        "unmarked block artifacts exist without final metadata: group_name={}, block_id={}",
                        req.group_name, req.block_id
                    )));
                }
                Ok(ReclaimBlockState::Absent)
            }
            Err(error) => Err(error),
        }
    }

    /// Durably reclaims one exact Ready block version.
    ///
    /// The caller must hold the block's exclusive reclaim permit. Once the
    /// marker is durable, every error leaves it in place so startup recovery or
    /// a later retry can finish all unlinks before the block is reported Ready.
    pub fn reclaim_block(&self, req: &ReclaimBlockRequest) -> StoreResult<ReclaimBlockResult> {
        match self.inspect_reclaim_block(req)? {
            ReclaimBlockState::Absent => return Ok(ReclaimBlockResult::AlreadyAbsent),
            ReclaimBlockState::Deleting => {
                let paths = self.paths(&req.group_name, req.block_id);
                let marker = read_deleting_marker(&paths.deleting_marker_path)?;
                validate_deleting_marker(&marker, req, &paths.deleting_marker_path)?;
                complete_deleting_marker(&paths, &marker)?;
                return Ok(ReclaimBlockResult::Deleted {
                    effective_len: marker.effective_len,
                });
            }
            ReclaimBlockState::Ready => {}
        }

        let meta = self.load_meta(&req.group_name, req.block_id)?;
        ensure_readable(&meta)?;
        validate_reclaim_stamp(req, meta.visibility.block_stamp)?;
        let paths = self.paths(&req.group_name, req.block_id);
        let marker = DeletingMarker {
            version: DELETING_MARKER_VERSION,
            group_name: req.group_name.as_str().to_string(),
            block_id: req.block_id,
            block_stamp: req.expected_block_stamp,
            effective_len: meta.source.effective_len,
        };
        write_deleting_marker(&paths, &marker)?;
        complete_deleting_marker(&paths, &marker)?;
        Ok(ReclaimBlockResult::Deleted {
            effective_len: marker.effective_len,
        })
    }

    /// Completes every durable deletion marker before local Ready discovery.
    pub fn recover_deleting_markers(&self) -> StoreResult<usize> {
        let groups_dir = self.config.data_root.join("groups");
        if !groups_dir.exists() {
            return Ok(0);
        }

        let mut recovered = 0usize;
        for group_entry in fs::read_dir(groups_dir)? {
            let group_entry = group_entry?;
            if !group_entry.file_type()?.is_dir() {
                continue;
            }
            let Some(group_raw) = group_entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let Ok(group_name) = GroupName::parse(&group_raw) else {
                continue;
            };
            let gc_dir = group_entry.path().join("gc");
            if !gc_dir.exists() {
                continue;
            }
            let mut temp_markers = Vec::new();
            for marker_entry in fs::read_dir(&gc_dir)? {
                let marker_entry = marker_entry?;
                let file_type = match marker_entry.file_type() {
                    Ok(file_type) => file_type,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(error) => return Err(WorkerError::from(error)),
                };
                if !file_type.is_file() {
                    continue;
                }
                let marker_path = marker_entry.path();
                if marker_path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.ends_with(".deleting.tmp"))
                {
                    temp_markers.push(marker_path);
                    continue;
                }
                if marker_path.extension().and_then(|ext| ext.to_str()) != Some("deleting") {
                    continue;
                }
                let marker = read_deleting_marker(&marker_path)?;
                let req = ReclaimBlockRequest {
                    group_name: group_name.clone(),
                    block_id: marker.block_id,
                    expected_block_stamp: marker.block_stamp,
                };
                let paths = self.paths(&group_name, marker.block_id);
                validate_deleting_marker(&marker, &req, &marker_path)?;
                if paths.deleting_marker_path != marker_path {
                    return Err(corrupt(format!(
                        "deleting marker path does not match block identity: path={}",
                        marker_path.display()
                    )));
                }
                complete_deleting_marker(&paths, &marker)?;
                recovered = recovered.saturating_add(1);
            }
            if !temp_markers.is_empty() {
                for temp_marker in temp_markers {
                    remove_file_if_exists(&temp_marker)?;
                }
                sync_parent_dir(&gc_dir)?;
            }
        }
        Ok(recovered)
    }

    /// Removes every exactly identified unpublished block left by an older run.
    ///
    /// Final `.meta` is the local visibility commit point. Recovery preserves a
    /// valid Ready pair, removes its disposable leftovers, and deletes an
    /// interrupted publication only when a staging path proves the block
    /// identity. Unknown files or ambiguous final data fail startup closed.
    pub fn recover_unpublished_blocks(&self) -> StoreResult<usize> {
        let groups_dir = self.config.data_root.join("groups");
        if !groups_dir.exists() {
            return Ok(0);
        }

        let mut recovered = 0usize;
        for group_entry in fs::read_dir(&groups_dir)? {
            let group_entry = group_entry?;
            if !group_entry.file_type()?.is_dir() {
                return Err(corrupt(format!(
                    "unexpected file in worker groups directory: path={}",
                    group_entry.path().display()
                )));
            }
            let group_raw = group_entry
                .file_name()
                .to_str()
                .ok_or_else(|| corrupt("worker group directory name is not UTF-8"))?
                .to_string();
            let group_name = GroupName::parse(&group_raw)
                .map_err(|err| corrupt(format!("invalid worker group directory {group_raw}: {err}")))?;
            let tmp_dir = group_entry.path().join("tmp");
            if !tmp_dir.exists() {
                continue;
            }

            let mut blocks = HashSet::new();
            for entry in fs::read_dir(&tmp_dir)? {
                let entry = entry?;
                if !entry.file_type()?.is_file() {
                    return Err(corrupt(format!(
                        "unexpected non-file in worker staging directory: path={}",
                        entry.path().display()
                    )));
                }
                let name = entry
                    .file_name()
                    .to_str()
                    .ok_or_else(|| corrupt("worker staging file name is not UTF-8"))?
                    .to_string();
                let block_id = parse_staging_block_file_name(&name).ok_or_else(|| {
                    corrupt(format!(
                        "unexpected worker staging file name: path={}",
                        entry.path().display()
                    ))
                })?;
                blocks.insert(block_id);
            }

            for block_id in blocks {
                let paths = self.paths(&group_name, block_id);
                if paths.meta_path.exists() {
                    let meta = self.load_meta(&group_name, block_id)?;
                    ensure_readable(&meta)?;
                    validate_ready_data_file(&paths, &meta)?;
                } else {
                    let has_staging_meta = paths.staging_meta_path.exists();
                    if has_staging_meta {
                        self.load_staging_meta(&group_name, block_id)?;
                    }
                    if paths.data_path.exists() && !has_staging_meta {
                        return Err(corrupt(format!(
                            "unpublished final data has no staging identity: group_name={}, block_id={}",
                            group_name, block_id
                        )));
                    }
                    if paths.data_path.exists() {
                        remove_file_if_exists(&paths.data_path)?;
                        if let Some(parent) = paths.data_path.parent() {
                            sync_parent_dir(parent)?;
                        }
                    }
                }

                remove_file_if_exists(&paths.staging_data_path)?;
                remove_file_if_exists(&paths.staging_meta_path)?;
                remove_file_if_exists(&paths.temp_meta_path)?;
                sync_parent_dir(&tmp_dir)?;
                if let Some(parent) = paths.data_path.parent() {
                    if parent.exists() {
                        sync_parent_dir(parent)?;
                    }
                }
                recovered = recovered.saturating_add(1);
            }
        }
        Ok(recovered)
    }

    /// Idempotently removes every unpublished artifact for an aborted write.
    ///
    /// A valid final `.meta` is preserved as the local commit point. Final data
    /// without that commit point is removed only when the staging sidecar proves
    /// it belongs to the interrupted publication.
    pub fn abort_staging_block(&self, group_name: &GroupName, block_id: BlockId) -> StoreResult<()> {
        let paths = self.paths(group_name, block_id);
        if paths.meta_path.exists() {
            let meta = self.load_meta(group_name, block_id)?;
            ensure_readable(&meta)?;
            validate_ready_data_file(&paths, &meta)?;
        } else {
            let has_staging_meta = paths.staging_meta_path.exists();
            if has_staging_meta {
                self.load_staging_meta(group_name, block_id)?;
            }
            if paths.data_path.exists() && !has_staging_meta {
                return Err(corrupt(format!(
                    "unpublished final data has no staging identity: group_name={}, block_id={}",
                    group_name, block_id
                )));
            }
            if paths.data_path.exists() {
                remove_file_if_exists(&paths.data_path)?;
                if let Some(parent) = paths.data_path.parent() {
                    sync_parent_dir(parent)?;
                }
            }
        }
        remove_file_if_exists(&paths.staging_data_path)?;
        remove_file_if_exists(&paths.staging_meta_path)?;
        remove_file_if_exists(&paths.temp_meta_path)?;
        if let Some(parent) = paths.staging_data_path.parent() {
            if parent.exists() {
                sync_parent_dir(parent)?;
            }
        }
        if let Some(parent) = paths.data_path.parent() {
            if parent.exists() {
                sync_parent_dir(parent)?;
            }
        }
        Ok(())
    }

    pub fn paths(&self, group_name: &GroupName, block_id: BlockId) -> BlockPaths {
        let (hash_a, hash_b) = block_hash_prefix(block_id);
        let stem = format!("b_{:016x}_{:08x}", block_id.inode_id.as_raw(), block_id.index.as_raw());
        let dir = self
            .group_dir(group_name)
            .join("blocks")
            .join(format!("{hash_a:02x}"))
            .join(format!("{hash_b:02x}"));
        let tmp_dir = self.group_dir(group_name).join("tmp");
        let gc_dir = self.group_dir(group_name).join("gc");

        BlockPaths {
            data_path: dir.join(format!("{stem}.blk")),
            meta_path: dir.join(format!("{stem}.meta")),
            temp_meta_path: dir.join(format!("{stem}.meta.tmp")),
            staging_data_path: tmp_dir.join(format!("{stem}.blk.tmp")),
            staging_meta_path: tmp_dir.join(format!("{stem}.meta.tmp")),
            deleting_marker_path: gc_dir.join(format!("{stem}.deleting")),
            temp_deleting_marker_path: gc_dir.join(format!("{stem}.deleting.tmp")),
        }
    }

    fn load_staging_meta(&self, group_name: &GroupName, block_id: BlockId) -> StoreResult<BlockMetaPayload> {
        let paths = self.paths(group_name, block_id);
        let meta = read_staging_meta_file(&paths.staging_meta_path)?;
        validate_staging_meta_payload(&meta, group_name, block_id)?;
        Ok(meta)
    }

    fn group_dir(&self, group_name: &GroupName) -> PathBuf {
        self.config.data_root.join("groups").join(group_name.as_str())
    }

    fn ensure_group_dirs(&self, group_name: &GroupName) -> StoreResult<()> {
        let group_dir = self.group_dir(group_name);
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
    fn create_staging_block(&self, req: CreateStagingBlockRequest) -> StoreResult<BlockMetaPayload>;

    fn write_at(&self, group_name: &GroupName, block_id: BlockId, offset: u64, data: Bytes) -> StoreResult<()>;

    fn publish_ready(&self, req: PublishReadyRequest) -> StoreResult<BlockMetaPayload>;

    fn read_at(&self, group_name: &GroupName, block_id: BlockId, offset: u64, len: u64) -> StoreResult<Bytes>;

    fn load_meta(&self, group_name: &GroupName, block_id: BlockId) -> StoreResult<BlockMetaPayload>;

    fn inspect_reclaim_block(&self, req: &ReclaimBlockRequest) -> StoreResult<ReclaimBlockState>;

    fn reclaim_block(&self, req: &ReclaimBlockRequest) -> StoreResult<ReclaimBlockResult>;

    fn abort_staging_block(&self, group_name: &GroupName, block_id: BlockId) -> StoreResult<()>;
}

impl LocalBlockStore for FullBlockFileStore {
    fn create_staging_block(&self, req: CreateStagingBlockRequest) -> StoreResult<BlockMetaPayload> {
        FullBlockFileStore::create_staging_block(self, req)
    }

    fn write_at(&self, group_name: &GroupName, block_id: BlockId, offset: u64, data: Bytes) -> StoreResult<()> {
        FullBlockFileStore::write_at(self, group_name, block_id, offset, data)
    }

    fn publish_ready(&self, req: PublishReadyRequest) -> StoreResult<BlockMetaPayload> {
        FullBlockFileStore::publish_ready(self, req)
    }

    fn read_at(&self, group_name: &GroupName, block_id: BlockId, offset: u64, len: u64) -> StoreResult<Bytes> {
        FullBlockFileStore::read_at(self, group_name, block_id, offset, len)
    }

    fn load_meta(&self, group_name: &GroupName, block_id: BlockId) -> StoreResult<BlockMetaPayload> {
        FullBlockFileStore::load_meta(self, group_name, block_id)
    }

    fn inspect_reclaim_block(&self, req: &ReclaimBlockRequest) -> StoreResult<ReclaimBlockState> {
        FullBlockFileStore::inspect_reclaim_block(self, req)
    }

    fn reclaim_block(&self, req: &ReclaimBlockRequest) -> StoreResult<ReclaimBlockResult> {
        FullBlockFileStore::reclaim_block(self, req)
    }

    fn abort_staging_block(&self, group_name: &GroupName, block_id: BlockId) -> StoreResult<()> {
        FullBlockFileStore::abort_staging_block(self, group_name, block_id)
    }
}

fn validate_reclaim_request(req: &ReclaimBlockRequest) -> StoreResult<()> {
    if req.expected_block_stamp == 0 {
        return Err(invalid_argument(
            "expected_block_stamp must be metadata-assigned and non-zero",
        ));
    }
    Ok(())
}

fn validate_reclaim_stamp(req: &ReclaimBlockRequest, actual_block_stamp: u64) -> StoreResult<()> {
    if req.expected_block_stamp != actual_block_stamp {
        return Err(WorkerError::RefreshMetadata {
            kind: ErrorKind::Worker(WorkerErrorKind::BlockStampMismatch),
            message: format!(
                "block stamp mismatch during local reclamation: group_name={}, block_id={}, expected={}, local={}",
                req.group_name, req.block_id, req.expected_block_stamp, actual_block_stamp
            ),
        });
    }
    Ok(())
}

fn write_deleting_marker(paths: &BlockPaths, marker: &DeletingMarker) -> StoreResult<()> {
    let parent = paths
        .deleting_marker_path
        .parent()
        .ok_or_else(|| invalid_argument("deleting marker path has no parent directory"))?;
    let parent_existed = parent.exists();
    fs::create_dir_all(parent)?;
    if !parent_existed {
        let group_dir = parent
            .parent()
            .ok_or_else(|| invalid_argument("deleting marker directory has no parent directory"))?;
        sync_parent_dir(group_dir)?;
    }

    if paths.deleting_marker_path.exists() {
        let persisted = read_deleting_marker(&paths.deleting_marker_path)?;
        let group_name =
            GroupName::parse(&marker.group_name).map_err(|err| corrupt(format!("invalid marker group name: {err}")))?;
        let req = ReclaimBlockRequest {
            group_name,
            block_id: marker.block_id,
            expected_block_stamp: marker.block_stamp,
        };
        validate_deleting_marker(&persisted, &req, &paths.deleting_marker_path)?;
        return Ok(());
    }

    remove_file_if_exists(&paths.temp_deleting_marker_path)?;
    let encoded =
        serde_json::to_vec(marker).map_err(|err| WorkerError::Internal(format!("encode deleting marker: {err}")))?;
    {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&paths.temp_deleting_marker_path)?;
        file.write_all(&encoded)?;
        file.sync_all()?;
    }
    if let Err(err) = fs::hard_link(&paths.temp_deleting_marker_path, &paths.deleting_marker_path) {
        let _ = remove_file_if_exists(&paths.temp_deleting_marker_path);
        if err.kind() == std::io::ErrorKind::AlreadyExists {
            let persisted = read_deleting_marker(&paths.deleting_marker_path)?;
            let group_name = GroupName::parse(&marker.group_name)
                .map_err(|parse_err| corrupt(format!("invalid marker group name: {parse_err}")))?;
            let req = ReclaimBlockRequest {
                group_name,
                block_id: marker.block_id,
                expected_block_stamp: marker.block_stamp,
            };
            validate_deleting_marker(&persisted, &req, &paths.deleting_marker_path)?;
            return Ok(());
        }
        return Err(WorkerError::from(err));
    }
    sync_parent_dir(parent)?;
    remove_file_if_exists(&paths.temp_deleting_marker_path)?;
    sync_parent_dir(parent)?;
    Ok(())
}

fn read_deleting_marker(path: &Path) -> StoreResult<DeletingMarker> {
    let encoded = fs::read(path)?;
    serde_json::from_slice(&encoded)
        .map_err(|err| corrupt(format!("invalid deleting marker {}: {err}", path.display())))
}

fn validate_deleting_marker(marker: &DeletingMarker, req: &ReclaimBlockRequest, marker_path: &Path) -> StoreResult<()> {
    if marker.version != DELETING_MARKER_VERSION {
        return Err(corrupt(format!(
            "unsupported deleting marker version {}: path={}",
            marker.version,
            marker_path.display()
        )));
    }
    if marker.group_name != req.group_name.as_str() || marker.block_id != req.block_id {
        return Err(corrupt(format!(
            "deleting marker identity does not match reclaim request: path={}",
            marker_path.display()
        )));
    }
    if marker.block_stamp == 0 {
        return Err(corrupt(format!(
            "deleting marker block stamp must be non-zero: path={}",
            marker_path.display()
        )));
    }
    validate_reclaim_stamp(req, marker.block_stamp)?;
    Ok(())
}

fn validate_deleting_marker_against_final_meta(paths: &BlockPaths, marker: &DeletingMarker) -> StoreResult<()> {
    let meta = match read_meta_file(&paths.meta_path) {
        Ok(meta) => meta,
        Err(WorkerError::NotFound(_)) => return Ok(()),
        Err(error) => return Err(error),
    };
    let group_name =
        GroupName::parse(&marker.group_name).map_err(|err| corrupt(format!("invalid marker group name: {err}")))?;
    validate_final_meta_payload(&meta, &group_name, marker.block_id)?;
    ensure_readable(&meta)?;
    if meta.visibility.block_stamp != marker.block_stamp || meta.source.effective_len != marker.effective_len {
        return Err(corrupt(format!(
            "deleting marker does not match final metadata: group_name={}, block_id={}, marker_stamp={}, meta_stamp={}, marker_effective_len={}, meta_effective_len={}",
            marker.group_name,
            marker.block_id,
            marker.block_stamp,
            meta.visibility.block_stamp,
            marker.effective_len,
            meta.source.effective_len
        )));
    }
    Ok(())
}

fn ensure_deleting_marker_durable(paths: &BlockPaths) -> StoreResult<()> {
    File::open(&paths.deleting_marker_path)?.sync_all()?;
    let parent = paths
        .deleting_marker_path
        .parent()
        .ok_or_else(|| invalid_argument("deleting marker path has no parent directory"))?;
    sync_parent_dir(parent)
}

fn complete_deleting_marker(paths: &BlockPaths, marker: &DeletingMarker) -> StoreResult<()> {
    let marker_group =
        GroupName::parse(&marker.group_name).map_err(|err| corrupt(format!("invalid marker group name: {err}")))?;
    let req = ReclaimBlockRequest {
        group_name: marker_group,
        block_id: marker.block_id,
        expected_block_stamp: marker.block_stamp,
    };
    validate_deleting_marker(marker, &req, &paths.deleting_marker_path)?;
    ensure_deleting_marker_durable(paths)?;
    validate_deleting_marker_against_final_meta(paths, marker)?;

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

    remove_file_if_exists(&paths.temp_deleting_marker_path)?;
    remove_file_if_exists(&paths.deleting_marker_path)?;
    if let Some(parent) = paths.deleting_marker_path.parent() {
        if let Err(error) = sync_parent_dir(parent) {
            // The block directories were already synced, so the physical
            // deletion is durable. A crash may resurrect only the marker,
            // which startup recovery can retire idempotently.
            tracing::warn!(
                target: "worker.block",
                op = "RetireDeletingMarker",
                group_name = %marker.group_name,
                block_id = %marker.block_id,
                error = %error,
                "block deletion completed but deleting marker retirement was not synced"
            );
        }
    }
    Ok(())
}

fn write_meta_new(paths: &BlockPaths, meta: &BlockMetaPayload) -> StoreResult<()> {
    validate_final_meta_payload(meta, &meta.identity.group_name, meta.identity.block_id)?;
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
    validate_staging_meta_payload(meta, &meta.identity.group_name, meta.identity.block_id)?;
    let parent = paths.staging_parent_dir()?;
    fs::create_dir_all(parent)?;
    let encoded = encode_staging_meta(meta)?;
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
    let payload = encode_meta_payload(meta)?;
    let header = BlockMetaHeader::for_payload(payload.len())?;
    let mut encoded = Vec::with_capacity(BlockMetaHeader::encoded_len() + payload.len());
    encoded.extend_from_slice(&header.encode());
    encoded.extend_from_slice(&payload);
    Ok(encoded)
}

fn encode_staging_meta(meta: &BlockMetaPayload) -> StoreResult<Vec<u8>> {
    let payload = encode_staging_meta_payload(meta)?;
    let header = BlockMetaHeader::for_payload(payload.len())?;
    let mut encoded = Vec::with_capacity(BlockMetaHeader::encoded_len() + payload.len());
    encoded.extend_from_slice(&header.encode());
    encoded.extend_from_slice(&payload);
    Ok(encoded)
}

fn read_meta_file(path: &Path) -> StoreResult<BlockMetaPayload> {
    let payload = read_meta_payload(path)?;
    decode_meta_payload(&payload)
}

fn read_staging_meta_file(path: &Path) -> StoreResult<BlockMetaPayload> {
    let payload = read_meta_payload(path)?;
    decode_staging_meta_payload(&payload)
}

fn read_meta_payload(path: &Path) -> StoreResult<Vec<u8>> {
    let mut file = File::open(path)?;
    let mut encoded_header = [0u8; BLOCK_META_HEADER_LEN];
    file.read_exact(&mut encoded_header)
        .map_err(|err| map_truncated_read_error(err, "block meta file is shorter than the header"))?;

    let header = BlockMetaHeader::decode(&encoded_header)?;
    header.validate()?;
    let payload_len = usize::try_from(header.payload_len).map_err(|_| corrupt("meta payload length is too large"))?;
    let mut payload = vec![0; payload_len];
    file.read_exact(&mut payload)
        .map_err(|err| map_truncated_read_error(err, "block meta payload is shorter than declared length"))?;
    let mut trailing = [0u8; 1];
    if file.read(&mut trailing)? != 0 {
        return Err(corrupt("block meta file has trailing bytes"));
    }
    Ok(payload)
}

fn validate_final_meta_payload(meta: &BlockMetaPayload, group_name: &GroupName, block_id: BlockId) -> StoreResult<()> {
    validate_common_meta_shape(meta, group_name, block_id)?;
    match meta.visibility.block_state {
        BlockState::Ready | BlockState::Corrupt => Ok(()),
        BlockState::Loading => Err(corrupt("loading block metadata is not valid final metadata")),
    }?;
    if meta.visibility.block_stamp == 0 {
        return Err(corrupt("final block metadata must carry a non-zero block stamp"));
    }
    BlockShape::validate_effective_len(meta.format.block_size, meta.source.effective_len)
        .map_err(|err| corrupt(err.to_string()))?;
    Ok(())
}

fn validate_staging_meta_payload(
    meta: &BlockMetaPayload,
    group_name: &GroupName,
    block_id: BlockId,
) -> StoreResult<()> {
    validate_common_meta_shape(meta, group_name, block_id)?;
    match meta.visibility.block_state {
        BlockState::Loading => Ok(()),
        BlockState::Ready | BlockState::Corrupt => Err(corrupt("published block state is not valid staging metadata")),
    }?;
    if meta.source.effective_len != meta.format.block_size {
        return Err(corrupt("staging effective length must equal block size"));
    }
    Ok(())
}

fn validate_common_meta_shape(meta: &BlockMetaPayload, group_name: &GroupName, block_id: BlockId) -> StoreResult<()> {
    if &meta.identity.group_name != group_name {
        return Err(corrupt("block meta group name does not match path"));
    }
    if meta.identity.block_id != block_id {
        return Err(corrupt("block meta block id does not match path"));
    }
    if let Err(err) = BlockFormatId::from_raw(meta.format.format_id.as_raw()) {
        return Err(corrupt(err.to_string()));
    }
    if meta.format.format_id != BlockFormatId::FULL_EFFECTIVE {
        return Err(corrupt("unsupported block format id"));
    }
    if meta.format.checksum_kind != ChecksumKind::None {
        return Err(corrupt("unsupported checksum kind"));
    }
    let chunk_size =
        u32::try_from(meta.format.chunk_size).map_err(|_| corrupt("chunk_size does not fit block metadata format"))?;
    validate_store_block_shape(
        meta.format.format_id,
        meta.format.block_size,
        chunk_size,
        meta.format.block_size,
        corrupt,
    )?;
    Ok(())
}

fn validate_store_block_shape(
    block_format_id: BlockFormatId,
    block_size: u64,
    chunk_size: u32,
    effective_len: u64,
    error: fn(String) -> WorkerError,
) -> StoreResult<()> {
    if block_format_id != BlockFormatId::FULL_EFFECTIVE {
        return Err(error(format!(
            "unsupported block_format_id {}",
            block_format_id.as_raw()
        )));
    }
    BlockShape::new(block_format_id, block_size, chunk_size, effective_len).map_err(|err| error(err.to_string()))?;
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
        BlockState::Loading => Err(invalid_argument("staging block is not readable")),
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

fn validate_staging_write_range(meta: &BlockMetaPayload, offset: u64, len: u64) -> StoreResult<()> {
    ensure_loading(meta)?;
    validate_range_bound(
        meta.format.block_size,
        offset,
        len,
        "block-local range exceeds block size",
    )
}

fn validate_published_read_range(meta: &BlockMetaPayload, offset: u64, len: u64) -> StoreResult<()> {
    ensure_readable(meta)?;
    validate_range_bound(
        meta.source.effective_len,
        offset,
        len,
        "block-local range exceeds effective length",
    )
}

fn validate_range_bound(bound: u64, offset: u64, len: u64, message: &'static str) -> StoreResult<()> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| invalid_argument("block-local range overflows"))?;
    if offset > bound || end > bound {
        return Err(invalid_argument(message));
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
    let expected_len = meta.source.effective_len;
    if actual_len != expected_len {
        return Err(corrupt(format!(
            "ready block data length {actual_len} does not match effective block length {expected_len}"
        )));
    }
    Ok(())
}

fn block_hash_prefix(block_id: BlockId) -> (u8, u8) {
    let mut value = block_id.inode_id.as_raw() ^ (u64::from(block_id.index.as_raw()) << 32);
    value ^= value >> 33;
    value = value.wrapping_mul(0xff51_afd7_ed55_8ccd);
    value ^= value >> 33;
    value = value.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    value ^= value >> 33;
    ((value >> 56) as u8, (value >> 48) as u8)
}

fn parse_staging_block_file_name(name: &str) -> Option<BlockId> {
    let stem = name
        .strip_suffix(".blk.tmp")
        .or_else(|| name.strip_suffix(".meta.tmp"))?
        .strip_prefix("b_")?;
    let (inode_raw, index_raw) = stem.split_once('_')?;
    if inode_raw.len() != 16 || index_raw.len() != 8 {
        return None;
    }
    let inode_id = u64::from_str_radix(inode_raw, 16).ok()?;
    let block_index = u32::from_str_radix(index_raw, 16).ok()?;
    Some(BlockId::new(InodeId::new(inode_id), BlockIndex::new(block_index)))
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
    let _ = remove_file_if_exists(path);
}

fn remove_staging_meta_after_commit(path: &Path) {
    let _ = remove_file_if_exists(path);
}

fn sync_parent_dir_after_commit(parent: &Path) {
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

fn map_truncated_read_error(err: std::io::Error, message: &str) -> WorkerError {
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
mod tests {
    use std::fs::{self, OpenOptions};
    use std::io::Write;
    use std::sync::OnceLock;

    use beryl_types::ids::{BlockId, BlockIndex, InodeId};
    use beryl_types::GroupName;
    use bytes::Bytes;
    use tempfile::TempDir;

    use super::*;

    fn ids() -> (&'static GroupName, BlockId) {
        (
            test_group_name(),
            BlockId::new(InodeId::new(0x1234), BlockIndex::new(7)),
        )
    }

    fn test_group_name() -> &'static GroupName {
        static NAME: OnceLock<GroupName> = OnceLock::new();
        NAME.get_or_init(|| GroupName::parse("root").unwrap())
    }

    fn store() -> (TempDir, FullBlockFileStore) {
        let temp = TempDir::new().expect("tempdir");
        let store = FullBlockFileStore::new(FullBlockFileStoreConfig::new(temp.path().to_path_buf()));
        (temp, store)
    }

    fn request(
        group_name: &GroupName,
        block_id: BlockId,
        block_size: u64,
        chunk_size: u32,
    ) -> CreateStagingBlockRequest {
        CreateStagingBlockRequest {
            group_name: group_name.to_owned(),
            block_id,
            block_size,
            block_format_id: BlockFormatId::FULL_EFFECTIVE,
            chunk_size,
            checksum_kind: ChecksumKind::None,
            tier: Tier::Hdd,
        }
    }

    fn publish_request(
        group_name: &GroupName,
        block_id: BlockId,
        effective_len: u64,
        block_stamp: u64,
    ) -> PublishReadyRequest {
        PublishReadyRequest {
            group_name: group_name.to_owned(),
            block_id,
            effective_len,
            block_stamp,
        }
    }

    fn create_default_block(store: &FullBlockFileStore, group_name: &GroupName, block_id: BlockId) {
        store
            .create_staging_block(request(group_name, block_id, 4096, 1024))
            .expect("create staging block");
    }

    fn publish_default_block(
        store: &FullBlockFileStore,
        group_name: &GroupName,
        block_id: BlockId,
    ) -> BlockMetaPayload {
        create_default_block(store, group_name, block_id);
        store
            .write_at(group_name, block_id, 0, Bytes::from(vec![1; 4096]))
            .expect("write default block");
        store
            .publish_ready(publish_request(group_name, block_id, 4096, 1))
            .expect("publish default block")
    }

    fn reclaim_request(group_name: &GroupName, block_id: BlockId, block_stamp: u64) -> ReclaimBlockRequest {
        ReclaimBlockRequest {
            group_name: group_name.clone(),
            block_id,
            expected_block_stamp: block_stamp,
        }
    }

    fn persist_deleting_marker(store: &FullBlockFileStore, group_name: &GroupName, block_id: BlockId) -> BlockPaths {
        let meta = store.load_meta(group_name, block_id).expect("ready meta");
        let paths = store.paths(group_name, block_id);
        write_deleting_marker(
            &paths,
            &DeletingMarker {
                version: DELETING_MARKER_VERSION,
                group_name: group_name.as_str().to_string(),
                block_id,
                block_stamp: meta.visibility.block_stamp,
                effective_len: meta.source.effective_len,
            },
        )
        .expect("persist deleting marker");
        paths
    }

    fn assert_reclaimed(paths: &BlockPaths) {
        assert!(!paths.data_path.exists(), "data file must be removed");
        assert!(!paths.meta_path.exists(), "meta file must be removed");
        assert!(!paths.temp_meta_path.exists(), "temp meta file must be removed");
        assert!(!paths.staging_data_path.exists(), "staging data must be removed");
        assert!(!paths.staging_meta_path.exists(), "staging meta must be removed");
        assert!(!paths.deleting_marker_path.exists(), "deleting marker must be removed");
        assert!(
            !paths.temp_deleting_marker_path.exists(),
            "temporary deleting marker must be removed"
        );
    }

    fn assert_corrupt<T: std::fmt::Debug>(result: Result<T, WorkerError>) {
        match result.expect_err("operation should fail") {
            WorkerError::Corrupt(_) => {}
            other => panic!("expected corrupt error, got {other:?}"),
        }
    }

    fn persist_meta(store: &FullBlockFileStore, group_name: &GroupName, block_id: BlockId, meta: &BlockMetaPayload) {
        let paths = store.paths(group_name, block_id);
        validate_final_meta_payload(meta, &meta.identity.group_name, meta.identity.block_id).expect("valid final meta");
        let parent = paths.parent_dir().expect("parent dir");
        fs::create_dir_all(parent).expect("create parent");
        let encoded = encode_meta(meta).expect("encode meta");
        {
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&paths.temp_meta_path)
                .expect("open temp meta");
            file.write_all(&encoded).expect("write temp meta");
            file.sync_all().expect("sync temp meta");
        }
        fs::rename(&paths.temp_meta_path, &paths.meta_path).expect("rename meta");
        sync_parent_dir(parent).expect("sync parent");
    }

    fn write_final_data(store: &FullBlockFileStore, group_name: &GroupName, block_id: BlockId, data: &[u8]) {
        let paths = store.paths(group_name, block_id);
        fs::create_dir_all(paths.parent_dir().expect("parent dir")).expect("create block parent");
        fs::write(&paths.data_path, data).expect("write final data");
    }

    fn assert_scan_corrupt(store: &FullBlockFileStore, group_name: &GroupName) {
        assert_corrupt(store.scan_group_blocks(group_name));
    }

    fn ready_meta(group_name: &GroupName, block_id: BlockId) -> BlockMetaPayload {
        BlockMetaPayload {
            identity: BlockIdentity {
                block_id,
                group_name: group_name.to_owned(),
            },
            format: BlockFormat {
                format_id: BlockFormatId::FULL_EFFECTIVE,
                block_size: 4096,
                chunk_size: 1024,
                checksum_kind: ChecksumKind::None,
            },
            source: BlockSource { effective_len: 3072 },
            visibility: BlockVisibility {
                block_state: BlockState::Ready,
                block_stamp: 99,
            },
            tier: Tier::Hdd,
        }
    }

    #[test]
    fn final_data_length_must_match_effective_length() {
        let cases = [
            ("missing data", None, 4096),
            ("short data", Some(2048), 3072),
            ("long data", Some(4096), 3072),
        ];

        for (case, data_len, effective_len) in cases {
            let (_temp, store) = store();
            let (group_name_value, block_id) = ids();
            let mut meta = ready_meta(group_name_value, block_id);
            meta.source.effective_len = effective_len;
            meta.visibility.block_stamp = 55;
            if let Some(data_len) = data_len {
                write_final_data(&store, group_name_value, block_id, &vec![7; data_len]);
            }
            persist_meta(&store, group_name_value, block_id, &meta);

            assert_scan_corrupt(&store, group_name_value);
            let result = store.read_at(group_name_value, block_id, 0, 1);
            assert!(
                matches!(result, Err(WorkerError::Corrupt(_))),
                "case {case} should reject block as corrupt, got {result:?}"
            );
        }
    }

    #[test]
    fn startup_recovery_removes_unpublished_shapes_and_preserves_ready_commit() {
        let (_temp, store) = store();
        let (group_name_value, staging_id) = ids();
        let interrupted_id = BlockId::new(staging_id.inode_id, BlockIndex::new(staging_id.index.as_raw() + 1));
        let ready_id = BlockId::new(staging_id.inode_id, BlockIndex::new(staging_id.index.as_raw() + 2));
        let ready_data = Bytes::from(vec![9; 4096]);

        create_default_block(&store, group_name_value, staging_id);
        store
            .write_at(group_name_value, staging_id, 0, Bytes::from_static(b"partial"))
            .expect("write staging block");

        create_default_block(&store, group_name_value, interrupted_id);
        store
            .write_at(group_name_value, interrupted_id, 0, Bytes::from(vec![8; 4096]))
            .expect("write interrupted block");
        let interrupted_paths = store.paths(group_name_value, interrupted_id);
        fs::create_dir_all(interrupted_paths.parent_dir().expect("block parent")).expect("create block parent");
        fs::rename(&interrupted_paths.staging_data_path, &interrupted_paths.data_path)
            .expect("simulate interrupted data publication");

        create_default_block(&store, group_name_value, ready_id);
        store
            .write_at(group_name_value, ready_id, 0, ready_data.clone())
            .expect("write ready block");
        let ready_paths = store.paths(group_name_value, ready_id);
        let leftover_data = fs::read(&ready_paths.staging_data_path).expect("read staging data");
        store
            .publish_ready(publish_request(group_name_value, ready_id, 4096, 9))
            .expect("publish ready block");
        fs::write(&ready_paths.staging_data_path, leftover_data).expect("restore staging data leftover");
        fs::write(&ready_paths.staging_meta_path, b"corrupt disposable staging meta")
            .expect("restore corrupt staging meta leftover");

        assert_eq!(store.recover_unpublished_blocks().expect("recover startup state"), 3);

        for block_id in [staging_id, interrupted_id, ready_id] {
            let paths = store.paths(group_name_value, block_id);
            assert!(!paths.staging_data_path.exists());
            assert!(!paths.staging_meta_path.exists());
        }
        assert!(!interrupted_paths.data_path.exists());
        assert_eq!(
            store
                .read_at(group_name_value, ready_id, 0, 4096)
                .expect("read preserved ready block"),
            ready_data
        );
    }

    #[test]
    fn startup_recovery_fails_closed_on_unknown_staging_file() {
        let (_temp, store) = store();
        let (group_name_value, block_id) = ids();
        create_default_block(&store, group_name_value, block_id);
        let paths = store.paths(group_name_value, block_id);
        fs::write(
            paths.staging_parent_dir().expect("staging parent").join("unknown.tmp"),
            b"unknown",
        )
        .expect("write unknown staging artifact");

        assert_corrupt(store.recover_unpublished_blocks());
        assert!(paths.staging_meta_path.exists());
        assert!(paths.staging_data_path.exists());
    }

    #[test]
    fn reclaim_block_is_stamp_checked_and_idempotent() {
        let (_temp, store) = store();
        let (group_name_value, block_id) = ids();
        publish_default_block(&store, group_name_value, block_id);
        let paths = store.paths(group_name_value, block_id);

        let mismatch = store
            .reclaim_block(&reclaim_request(group_name_value, block_id, 2))
            .expect_err("stale stamp must not reclaim");
        assert!(matches!(
            mismatch,
            WorkerError::RefreshMetadata {
                kind: ErrorKind::Worker(WorkerErrorKind::BlockStampMismatch),
                ..
            }
        ));
        assert!(paths.data_path.exists());
        assert!(paths.meta_path.exists());

        assert_eq!(
            store
                .reclaim_block(&reclaim_request(group_name_value, block_id, 1))
                .expect("reclaim block"),
            ReclaimBlockResult::Deleted { effective_len: 4096 }
        );
        assert_reclaimed(&paths);
        assert_eq!(
            store
                .reclaim_block(&reclaim_request(group_name_value, block_id, 1))
                .expect("repeat reclaim"),
            ReclaimBlockResult::AlreadyAbsent
        );
    }

    #[test]
    fn reclaim_fails_closed_on_unmarked_staging_or_temp_marker_artifacts() {
        for artifact in ["staging-data", "staging-meta", "temp-marker"] {
            let (_temp, store) = store();
            let (group_name_value, block_id) = ids();
            let paths = store.paths(group_name_value, block_id);
            match artifact {
                "staging-data" => {
                    fs::create_dir_all(paths.staging_data_path.parent().expect("staging parent"))
                        .expect("create staging parent");
                    fs::write(&paths.staging_data_path, b"staging").expect("write staging data");
                }
                "staging-meta" => {
                    fs::create_dir_all(paths.staging_meta_path.parent().expect("staging parent"))
                        .expect("create staging parent");
                    fs::write(&paths.staging_meta_path, b"staging").expect("write staging meta");
                }
                "temp-marker" => {
                    fs::create_dir_all(paths.temp_deleting_marker_path.parent().expect("marker parent"))
                        .expect("create marker parent");
                    fs::write(&paths.temp_deleting_marker_path, b"temporary").expect("write temp marker");
                }
                _ => unreachable!(),
            }

            assert_corrupt(store.reclaim_block(&reclaim_request(group_name_value, block_id, 1)));
            assert!(
                paths.staging_data_path.exists()
                    || paths.staging_meta_path.exists()
                    || paths.temp_deleting_marker_path.exists(),
                "unverified artifact must remain"
            );
            assert!(!paths.deleting_marker_path.exists());
        }
    }

    #[test]
    fn deleting_marker_must_match_remaining_final_metadata() {
        for tamper in ["stamp", "effective-len"] {
            let (_temp, store) = store();
            let (group_name_value, block_id) = ids();
            publish_default_block(&store, group_name_value, block_id);
            let paths = persist_deleting_marker(&store, group_name_value, block_id);
            let mut marker = read_deleting_marker(&paths.deleting_marker_path).expect("read marker");
            match tamper {
                "stamp" => marker.block_stamp = marker.block_stamp.saturating_add(1),
                "effective-len" => marker.effective_len = marker.effective_len.saturating_sub(1),
                _ => unreachable!(),
            }
            fs::write(
                &paths.deleting_marker_path,
                serde_json::to_vec(&marker).expect("encode marker"),
            )
            .expect("tamper marker");

            assert_corrupt(store.recover_deleting_markers());
            assert!(paths.data_path.exists(), "mismatched marker must not delete data");
            assert!(paths.meta_path.exists(), "mismatched marker must not delete metadata");
            assert!(paths.deleting_marker_path.exists(), "mismatched marker must remain");
        }
    }

    #[test]
    fn startup_recovery_completes_all_partial_deletion_shapes() {
        #[derive(Clone, Copy)]
        enum Shape {
            MarkerOnly,
            MetaAndData,
            DataOnly,
            MetaOnly,
            WithStaging,
        }

        for shape in [
            Shape::MarkerOnly,
            Shape::MetaAndData,
            Shape::DataOnly,
            Shape::MetaOnly,
            Shape::WithStaging,
        ] {
            let (_temp, store) = store();
            let (group_name_value, block_id) = ids();
            publish_default_block(&store, group_name_value, block_id);
            let paths = persist_deleting_marker(&store, group_name_value, block_id);
            fs::copy(&paths.deleting_marker_path, &paths.temp_deleting_marker_path)
                .expect("simulate leftover marker temp");

            match shape {
                Shape::MarkerOnly => {
                    fs::remove_file(&paths.meta_path).expect("remove meta");
                    fs::remove_file(&paths.data_path).expect("remove data");
                }
                Shape::MetaAndData => {}
                Shape::DataOnly => {
                    fs::remove_file(&paths.meta_path).expect("remove meta");
                }
                Shape::MetaOnly => {
                    fs::remove_file(&paths.data_path).expect("remove data");
                }
                Shape::WithStaging => {
                    fs::write(&paths.staging_data_path, b"staging data").expect("write staging data");
                    fs::write(&paths.staging_meta_path, b"staging meta").expect("write staging meta");
                }
            }

            assert_eq!(store.recover_deleting_markers().expect("recover marker"), 1);
            assert_reclaimed(&paths);
            assert!(store
                .scan_group_blocks(group_name_value)
                .expect("scan after recovery")
                .is_empty());
        }
    }

    #[test]
    fn startup_recovery_fails_closed_on_corrupt_marker() {
        let (_temp, store) = store();
        let (group_name_value, block_id) = ids();
        publish_default_block(&store, group_name_value, block_id);
        let paths = store.paths(group_name_value, block_id);
        fs::create_dir_all(paths.deleting_marker_path.parent().expect("marker parent")).expect("create gc");
        let corrupt_marker = b"not-json";
        fs::write(&paths.deleting_marker_path, corrupt_marker).expect("write corrupt marker");

        assert_corrupt(store.recover_deleting_markers());
        assert!(paths.data_path.exists());
        assert!(paths.meta_path.exists());
        assert_eq!(
            fs::read(&paths.deleting_marker_path).expect("read corrupt marker after rejection"),
            corrupt_marker
        );
    }
}
