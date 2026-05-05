// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! In-memory state store implementation.
//!
//! This route-epoch store is test support. Production metadata runtime uses
//! `RaftStateStore`; do not treat this type as metadata authority.

use super::{RouteEpoch, StateStore};
use crate::error::MetadataResult;
use parking_lot::RwLock;
use std::sync::Arc;

/// In-memory route-epoch `StateStore` for tests and lightweight helpers.
pub struct MemoryStateStore {
    route_epoch: Arc<RwLock<RouteEpoch>>,
}

impl MemoryStateStore {
    pub fn new() -> Self {
        Self {
            route_epoch: Arc::new(RwLock::new(RouteEpoch::new(1))),
        }
    }
}

impl Default for MemoryStateStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl StateStore for MemoryStateStore {
    async fn get_route_epoch(&self) -> MetadataResult<RouteEpoch> {
        Ok(*self.route_epoch.read())
    }
}
