// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! QUIC worker peer client placeholder.

use crate::error::WorkerError;

#[derive(Clone, Debug, Default)]
pub struct QuicWorkerPeerClient;

impl QuicWorkerPeerClient {
    pub fn new() -> Self {
        Self
    }
}

pub fn unimplemented(operation: &str) -> WorkerError {
    WorkerError::Unimplemented(format!("worker QUIC peer client {operation} is not implemented"))
}
