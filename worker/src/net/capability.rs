// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker net capabilities advertised by a worker endpoint.

use crate::runtime::block::BlockManager;

/// Capabilities for a worker data-plane endpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerNetCapabilities {
    pub supports_read_stream: bool,
    pub supports_write_stream: bool,
    pub supports_zero_copy: bool,
    pub max_frame_size: u32,
}

impl Default for WorkerNetCapabilities {
    fn default() -> Self {
        Self {
            supports_read_stream: true,
            supports_write_stream: true,
            supports_zero_copy: false,
            max_frame_size: BlockManager::MAX_FRAME_SIZE,
        }
    }
}
