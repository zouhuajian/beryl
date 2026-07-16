// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use crate::ids::{BlockId, ClientId};
use serde::{Deserialize, Serialize};

/// Fencing token: any write carrying an older epoch MUST be rejected by workers.
/// This is the core primitive preventing double-writer corruption.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FencingToken {
    pub block_id: BlockId,
    pub owner: ClientId,
    pub epoch: u64,
}

impl FencingToken {
    #[inline]
    pub const fn new(block_id: BlockId, owner: ClientId, epoch: u64) -> Self {
        Self { block_id, owner, epoch }
    }
}

/// Lease view stored in Meta state machine.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Lease {
    pub owner: ClientId,
    pub epoch: u64,
    /// Unix millis; meta uses it to expire/renew leases.
    pub expires_at_ms: u64,
}
