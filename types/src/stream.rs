// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use crate::ids::{DataHandleId, StreamId};
use serde::{Deserialize, Serialize};

/// Stream kind indicates read vs write semantic expectations.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamKind {
    Read,
    Write,
}

/// Stream metadata carried by clients.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamDesc {
    pub stream_id: StreamId,
    pub data_handle_id: DataHandleId,
    pub kind: StreamKind,
}

/// Cursor and windowing hints for read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadCursor {
    pub next_offset: u64,
    pub prefetch_window_bytes: u32,
}

/// Cursor for write.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteCursor {
    pub next_offset: u64,
}
