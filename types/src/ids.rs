// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Identity (ID) types.
//!
//! Design principles:
//! - IDs are pure identity: stable, cheap to copy/clone, no mutable state.
//! - IDs are shared across crates (types/metadata/worker/client/proto).
//! - IDs should serialize cleanly for wire/proto/logging.
//! - Do NOT embed layout semantics, placement, or state in IDs.

use core::fmt;
use serde::{Deserialize, Serialize};

/// A strongly-typed identifier wrapper.
///
/// Domain rule: IDs are opaque. Do not encode transport/storage semantics into the value.
///
macro_rules! id_new_uint {
    ($(#[$attr:meta])* $name:ident ($ty:ty)) => {
        $(#[$attr])*
        #[repr(transparent)]
        #[derive(
            Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord,
            ::serde::Serialize, ::serde::Deserialize
        )]
        #[serde(transparent)]
        pub struct $name(
            /// The raw value of this identifier.
            pub $ty
        );

        impl $name {
            /// Creates a new ID from a raw value.
            #[inline]
            pub const fn new(v: $ty) -> Self { Self(v) }

            /// Returns the inner value.
            #[inline]
            pub const fn as_raw(self) -> $ty { self.0 }
        }

        impl ::core::fmt::Debug for $name {
            #[inline]
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                f.debug_tuple(stringify!($name)).field(&self.0).finish()
            }
        }

        impl From<$ty> for $name {
            #[inline]
            fn from(v: $ty) -> Self { Self(v) }
        }

        impl From<$name> for $ty {
            #[inline]
            fn from(v: $name) -> Self { v.0 }
        }
    };
}

id_new_uint!(
    /// Data handle identity for the data-plane.
    /// A DataHandleId identifies a concrete data instance bound to an inode at a specific point in time
    /// (e.g., after create, after a committed write session, or after a version switch).
    /// It is NOT a namespace identity and MUST NOT be used for directory semantics or rename routing.
    DataHandleId(u64)
);

impl fmt::Display for DataHandleId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Display as plain number, friendly for logs.
        write!(f, "{}", self.0)
    }
}

id_new_uint!(
    /// A monotonically increasing block index within a file.
    ///
    /// This is an ordinal (0,1,2,...) not a byte offset.
    BlockIndex(u32)
);

impl fmt::Display for BlockIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Data-plane block identity.
/// Blocks are addressed under a DataHandleId, not under an inode.
/// This prevents namespace identity (inode) from being conflated with data instances (handles).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BlockId {
    /// The data handle this block belongs to (data-plane scope).
    pub data_handle_id: DataHandleId,
    /// The index of this block within the data handle (ordinal, not byte offset).
    pub index: BlockIndex,
}

impl BlockId {
    /// Creates a new `BlockId` from a data handle ID and block index.
    #[inline]
    pub const fn new(data_handle: DataHandleId, index: BlockIndex) -> Self {
        Self {
            data_handle_id: data_handle,
            index,
        }
    }

    /// Convenience for tests/logging where you already have primitive values.
    #[inline]
    pub const fn from_u64_u32(data_handle: u64, index: u32) -> Self {
        Self {
            data_handle_id: DataHandleId(data_handle),
            index: BlockIndex(index),
        }
    }
}

impl fmt::Debug for BlockId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Concise but structured.
        write!(
            f,
            "BlockId(data_handle_id={}, index={})",
            self.data_handle_id.0, self.index.0
        )
    }
}
impl fmt::Display for BlockId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Stable, human-friendly: "<data_handle>:<block>"
        write!(f, "{}:{}", self.data_handle_id.0, self.index.0)
    }
}

impl std::str::FromStr for BlockId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 2 {
            return Err(format!(
                "Invalid BlockId format: expected 'data_handle_id:block_index', got '{}'",
                s
            ));
        }
        let data_handle_id = parts[0]
            .parse::<u64>()
            .map_err(|e| format!("Failed to parse data_handle_id: {}", e))?;
        let block_index = parts[1]
            .parse::<u32>()
            .map_err(|e| format!("Failed to parse block_index: {}", e))?;
        Ok(BlockId {
            data_handle_id: DataHandleId::new(data_handle_id),
            index: BlockIndex::new(block_index),
        })
    }
}

id_new_uint!(
    /// A chunk index within a block: 0..N-1 (derived from block_size/chunk_size).
    ChunkIndex(u32)
);

impl fmt::Display for ChunkIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Chunk identity: (BlockId, ChunkIndex).
///
/// Deterministic and scope-unique under a BlockId.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ChunkId {
    /// The block this chunk belongs to.
    pub block: BlockId,
    /// The index of this chunk within the block.
    pub index: ChunkIndex,
}

impl ChunkId {
    /// Creates a new `ChunkId` from a block ID and chunk index.
    #[inline]
    pub const fn new(block: BlockId, index: ChunkIndex) -> Self {
        Self { block, index }
    }
}

impl fmt::Debug for ChunkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ChunkId(data_handle={}, block={}, chunk={})",
            self.block.data_handle_id.0, self.block.index.0, self.index.0
        )
    }
}
impl fmt::Display for ChunkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // "<data_handle>:<block>:<chunk>"
        write!(
            f,
            "{}:{}:{}",
            self.block.data_handle_id.0, self.block.index.0, self.index.0
        )
    }
}

id_new_uint!(
    /// Worker identity.
    ///
    /// Stable logical worker identity. This must not be confused with
    /// `WorkerRunId`, which identifies a single worker process start.
    WorkerId(u64)
);
impl fmt::Display for WorkerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

id_new_uint!(
    /// Stream identity.
    ///
    /// Identifies an end-to-end read/write stream. Typically generated by client or metadata
    /// service and propagated to workers.
    ///
    /// If you need fencing semantics, pair it with LeaseId/Epoch in `lease.rs`, not here.
    StreamId(u128)
);
impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{:032x}", self.0)
    }
}

id_new_uint!(
    /// Lease identity (writer lease).
    ///
    /// Kept in ids.rs because it is identity-only and commonly referenced across metadata/worker/client/proto.
    /// Lease semantics (ttl, fencing, epoch rules) should live in `lease.rs`.
    LeaseId(u128)
);

impl fmt::Display for LeaseId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{:032x}", self.0)
    }
}

/// Internal client runtime identity.
///
/// This is generated when a client runtime is created. Display names belong in
/// client_name and must not be used as a correctness identity.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ClientId(u128);

impl ClientId {
    /// Creates a new ID from a raw value.
    #[inline]
    pub const fn new(v: u128) -> Self {
        Self(v)
    }

    /// Generates a non-zero 128-bit client identity.
    pub fn generate() -> Self {
        loop {
            let value = Uuid::new_v4().as_u128();
            if value != 0 {
                return Self(value);
            }
        }
    }

    /// Returns the inner value.
    #[inline]
    pub const fn as_raw(self) -> u128 {
        self.0
    }

    /// Returns true when the identity is the invalid all-zero value.
    #[inline]
    pub const fn is_zero(self) -> bool {
        self.0 == 0
    }
}

impl fmt::Debug for ClientId {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ClientId")
            .field(&format_args!("0x{:032x}", self.0))
            .finish()
    }
}

impl From<u128> for ClientId {
    #[inline]
    fn from(v: u128) -> Self {
        Self(v)
    }
}

impl From<ClientId> for u128 {
    #[inline]
    fn from(v: ClientId) -> Self {
        v.0
    }
}

impl fmt::Display for ClientId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{:032x}", self.0)
    }
}

// CallId and TxId: UUID-based identifiers for request context
use std::str::FromStr;
use uuid::Uuid;

/// Call ID: unique identifier for each RPC call.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CallId(Uuid);

impl CallId {
    /// Generate a new CallId.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Create from a UUID.
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Get the inner UUID.
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl Default for CallId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for CallId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CallId({})", self.0)
    }
}

impl fmt::Display for CallId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for CallId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Uuid::parse_str(s)?))
    }
}

id_new_uint!(
    /// Shard identity within a shard group.
    ///
    /// A shard is a logical partition of the metadata namespace.
    ShardId(u64)
);

impl fmt::Display for ShardId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Stable metadata group identity.
///
/// Group names are identity, not display labels. Renaming a group means creating
/// a different group.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct GroupName(String);

impl GroupName {
    /// Parses and validates a metadata group name.
    pub fn parse(raw: impl AsRef<str>) -> Result<Self, GroupNameError> {
        let value = raw.as_ref().trim();
        if value.is_empty() {
            return Err(GroupNameError::Empty);
        }
        if value.len() > 63 {
            return Err(GroupNameError::TooLong);
        }
        let mut chars = value.chars();
        let first = chars.next().ok_or(GroupNameError::Empty)?;
        if !first.is_ascii_lowercase() && !first.is_ascii_digit() {
            return Err(GroupNameError::InvalidStart);
        }
        if !chars.all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '_' | '-')) {
            return Err(GroupNameError::InvalidCharacter);
        }
        Ok(Self(value.to_string()))
    }

    /// Parses an optional metadata group name from a wire/config field.
    ///
    /// An empty string is treated as absent. Non-empty values must satisfy the
    /// same normalized `GroupName` contract as `parse`.
    pub fn parse_optional(raw: impl AsRef<str>) -> Result<Option<Self>, GroupNameError> {
        let value = raw.as_ref();
        if value.is_empty() {
            Ok(None)
        } else {
            Self::parse(value).map(Some)
        }
    }

    /// Returns the validated group name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for GroupName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("GroupName").field(&self.0).finish()
    }
}

impl fmt::Display for GroupName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::str::FromStr for GroupName {
    type Err = GroupNameError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl<'de> Deserialize<'de> for GroupName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        Self::parse(raw).map_err(serde::de::Error::custom)
    }
}

/// Validation error for `GroupName`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GroupNameError {
    Empty,
    TooLong,
    InvalidStart,
    InvalidCharacter,
}

impl fmt::Display for GroupNameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("must not be empty"),
            Self::TooLong => f.write_str("must be at most 63 characters"),
            Self::InvalidStart => f.write_str("must start with lowercase ASCII letter or digit"),
            Self::InvalidCharacter => f.write_str("must contain only lowercase ASCII letters, digits, '.', '_' or '-'"),
        }
    }
}

impl std::error::Error for GroupNameError {}

id_new_uint!(
    /// Mount identity.
    ///
    /// Identifies a mount point that maps a UFS path to the metadata namespace.
    MountId(u64)
);

impl fmt::Display for MountId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_formats_are_stable() {
        let bid = BlockId::from_u64_u32(42, 7);
        assert_eq!(bid.to_string(), "42:7");

        let cid = ChunkId::new(bid, ChunkIndex::new(3));
        assert_eq!(cid.to_string(), "42:7:3");

        let wid = WorkerId::new(9);
        assert_eq!(wid.to_string(), "9");

        let sid = StreamId::new(0x10);
        assert_eq!(sid.to_string(), "0x00000000000000000000000000000010");
    }

    #[test]
    fn serde_round_trip() {
        let bid = BlockId::from_u64_u32(1, 2);
        println!("{:#?}", bid);
        let s = serde_json::to_string(&bid).unwrap();
        let back: BlockId = serde_json::from_str(&s).unwrap();
        assert_eq!(bid, back);

        let cid = ChunkId::new(bid, ChunkIndex::new(5));
        let s2 = serde_json::to_string(&cid).unwrap();
        let back2: ChunkId = serde_json::from_str(&s2).unwrap();
        assert_eq!(cid, back2);
    }

    #[test]
    fn group_name_validation_contract() {
        for value in ["root", "default", "analytics", "tenant-a", "hot_cache", "group.1"] {
            assert_eq!(GroupName::parse(value).unwrap().as_str(), value);
        }

        for value in [
            "",
            " ",
            "Root",
            "ROOT",
            "root/prod",
            "root prod",
            "-root",
            "root/",
            "a234567890123456789012345678901234567890123456789012345678901234",
        ] {
            assert!(GroupName::parse(value).is_err(), "{value} must be invalid");
        }
    }

    #[test]
    fn optional_group_name_parse_keeps_absent_distinct_from_invalid() {
        assert_eq!(GroupName::parse_optional("").unwrap(), None);
        assert_eq!(
            GroupName::parse_optional("root").unwrap(),
            Some(GroupName::parse("root").unwrap())
        );
        assert!(GroupName::parse_optional(" ").is_err());
        assert!(GroupName::parse_optional("Root").is_err());
    }
}
