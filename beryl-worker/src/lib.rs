// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Beryl worker data-plane service.

pub mod config;
pub mod control;
pub mod data;
pub mod error;
pub mod net;
pub mod observe;
pub mod runtime;
pub mod store;

pub use data::core::{WorkerCore, WorkerCoreResult};
pub use error::{ErrorMetadata, WorkerError};
pub use runtime::block::BlockManager;
pub use store::block::{ReclaimBlockRequest, ReclaimBlockResult};
