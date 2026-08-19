// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Worker runtime state.

use tokio::sync::OwnedSemaphorePermit;

use crate::observe;

/// Owns one admitted data RPC until all local work caused by that RPC is done.
#[derive(Debug)]
pub(crate) struct DataRpcPermit {
    _permit: OwnedSemaphorePermit,
    mode: &'static str,
}

impl DataRpcPermit {
    /// Binds the admitted semaphore slot to the lifecycle inflight metric.
    pub(crate) fn new(permit: OwnedSemaphorePermit, mode: &'static str) -> Self {
        observe::increment_stream_inflight(mode);
        Self { _permit: permit, mode }
    }
}

impl Drop for DataRpcPermit {
    fn drop(&mut self) {
        observe::decrement_stream_inflight(self.mode);
    }
}

pub mod block;
pub(crate) mod write;
