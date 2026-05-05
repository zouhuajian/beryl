// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Data IO operation classification for root/mount gating.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataIoOp {
    Read,
    Write,
}

impl DataIoOp {
    pub fn as_str(self) -> &'static str {
        match self {
            DataIoOp::Read => "read",
            DataIoOp::Write => "write",
        }
    }
}
