// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Data IO operation classification for root/mount gating.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataIoOp {
    OpenWrite,
    Write,
    Append,
    Truncate,
    Flush,
    Fsync,
    CloseWrite,
    Backfill,
    Passthrough,
    Read,
    RenewLease,
}

impl DataIoOp {
    pub fn as_str(self) -> &'static str {
        match self {
            DataIoOp::OpenWrite => "open_write",
            DataIoOp::Write => "write",
            DataIoOp::Append => "append",
            DataIoOp::Truncate => "truncate",
            DataIoOp::Flush => "flush",
            DataIoOp::Fsync => "fsync",
            DataIoOp::CloseWrite => "close_write",
            DataIoOp::Backfill => "backfill",
            DataIoOp::Passthrough => "passthrough",
            DataIoOp::Read => "read",
            DataIoOp::RenewLease => "renew_lease",
        }
    }
}
