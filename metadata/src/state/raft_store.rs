// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Raft-based StateStore implementation.

use crate::error::MetadataResult;
use crate::raft::AppRaftNode;
use crate::state::{RouteEpoch, StateStore};
use async_trait::async_trait;
use std::sync::Arc;

/// Raft-based route-epoch store implementation.
pub struct RaftStateStore {
    raft_node: Arc<AppRaftNode>,
}

impl RaftStateStore {
    pub fn new(raft_node: Arc<AppRaftNode>) -> Self {
        Self { raft_node }
    }
}

#[async_trait]
impl StateStore for RaftStateStore {
    async fn get_route_epoch(&self) -> MetadataResult<RouteEpoch> {
        self.raft_node.read(false, |sm| sm.storage().get_route_epoch()).await
    }
}
