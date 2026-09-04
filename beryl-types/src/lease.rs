// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Shared lease fencing values, independent of session lifetime and renewal policy.

use crate::ids::{BlockId, ClientId, InodeId};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter, Result};

/// Per-inode lease fencing counter, distinct from the visible content generation.
///
/// Metadata persists this counter across restarts. Zero denotes an inode with
/// no acquired writer; an active write authorization requires a non-zero epoch.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LeaseEpoch(u64);

impl LeaseEpoch {
    /// Wraps an epoch from persisted state or a protocol boundary.
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the scalar used by storage and wire encodings.
    pub const fn as_raw(self) -> u64 {
        self.0
    }

    /// Advances fencing without wrapping and making an old writer current again.
    pub fn checked_next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

impl Display for LeaseEpoch {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        self.0.fmt(f)
    }
}

/// File write-session identity carried between Client and Metadata.
///
/// A handle identifies a lease, not proof that it remains active. RPC boundaries
/// require non-zero fields; Metadata separately checks the caller, expiry, and
/// durable epoch before authorizing an operation. Keep this value unchanged when
/// retrying a request whose outcome is unknown.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WriteHandle {
    pub inode_id: InodeId,
    pub lease_epoch: LeaseEpoch,
}

/// Metadata-issued block writer identity.
///
/// This value binds a block and client to the inode's lease epoch. Metadata
/// validates the durable fencing authority; possession alone is not authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FencingToken {
    /// Exact block authorized for this writer.
    pub block_id: BlockId,
    /// Client runtime that owns the write session.
    pub owner: ClientId,
    /// Durable inode fencing epoch under which the target was issued.
    pub epoch: LeaseEpoch,
}

impl FencingToken {
    /// Binds an authorized block and owner to their lease epoch.
    pub const fn new(block_id: BlockId, owner: ClientId, epoch: LeaseEpoch) -> Self {
        Self { block_id, owner, epoch }
    }
}
