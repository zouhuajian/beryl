// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Repair policy defaults used by maintenance repair planning.

/// Lightweight repair policy placeholder until per-file or per-block policy exists.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RepairPolicy {
    pub default_replication_factor: u8,
}

impl Default for RepairPolicy {
    fn default() -> Self {
        Self {
            default_replication_factor: 3,
        }
    }
}
