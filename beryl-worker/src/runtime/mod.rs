// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Worker-local data execution and resource lifecycle.

mod block;
mod permit;
mod worker;
mod write;

pub use worker::WorkerRuntime;

pub(crate) use permit::DataRpcPermit;
pub(crate) use worker::{ActiveBlockRead, ActiveBlockWrite, ReadBlockRequest, WriteBlockRequest};
