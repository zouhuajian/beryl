// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Mock metadata server for integration tests.

use proto::common::{GroupStateWatermarkProto, RaftLogIdProto, ShardGroupIdProto};
use proto::metadata::*;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

/// Mock metadata server used by client contract tests.
pub struct MockMetadataServer {
    route_epoch: Arc<RwLock<u64>>,
    leader_id: u64,
    _follower_ids: Vec<u64>,
}

impl MockMetadataServer {
    pub fn new(leader_id: u64, follower_ids: Vec<u64>) -> Self {
        Self {
            route_epoch: Arc::new(RwLock::new(1)),
            leader_id,
            _follower_ids: follower_ids,
        }
    }
}

impl MockMetadataServer {
    pub async fn msync(&self, request: Request<MsyncRequestProto>) -> Result<Response<MsyncResponseProto>, Status> {
        let req = request.into_inner();

        let group_id = if let Some(ref header) = req.header {
            if header.group_id != 0 {
                header.group_id
            } else {
                0
            }
        } else {
            0
        };

        use proto::common::ResponseHeaderProto;

        let route_epoch = *self.route_epoch.read().await;
        let state_id = req.state.and_then(|state| state.state_id).unwrap_or(RaftLogIdProto {
            term: 1,
            leader_node_id: self.leader_id,
            index: route_epoch,
        });

        let response_header = ResponseHeaderProto {
            client: req.header.as_ref().and_then(|h| h.client.clone()),
            error: None,
            state: Vec::new(),
            group_id,
            mount_epoch: None,
            route_epoch: Some(route_epoch),
        };

        Ok(Response::new(MsyncResponseProto {
            header: Some(response_header),
            state: Some(GroupStateWatermarkProto {
                group_id: Some(ShardGroupIdProto { value: group_id }),
                state_id: Some(state_id),
            }),
        }))
    }
}
