// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Mock metadata server for integration tests.

use ::common::error::canonical::{CanonicalError, ErrorClass, ErrorCode};
use ::common::header::RpcErrorCode;
use proto::common::{GroupStateWatermarkProto, RaftLogIdProto, ResponseHeaderProto};
use proto::convert::canonical_to_error_detail;
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

        let Some(group_name) = req
            .header
            .as_ref()
            .and_then(|header| (!header.group_name.is_empty()).then_some(header.group_name.clone()))
        else {
            return Ok(Response::new(MsyncResponseProto {
                header: Some(ResponseHeaderProto {
                    client: req.header.as_ref().and_then(|h| h.client.clone()),
                    error: Some(canonical_to_error_detail(&CanonicalError {
                        class: ErrorClass::Fatal,
                        code: Some(ErrorCode::RpcCode(RpcErrorCode::InvalidHeader)),
                        reason: None,
                        retry_after_ms: None,
                        message: "MsyncRequestProto requires header.group_name".to_string(),
                        refresh_hint: None,
                    })),
                    state: Vec::new(),
                    group_name: String::new(),
                    mount_epoch: None,
                    route_epoch: None,
                }),
                state: None,
            }));
        };

        let route_epoch = *self.route_epoch.read().await;
        let state_id = RaftLogIdProto {
            term: 1,
            leader_node_id: self.leader_id,
            index: route_epoch,
        };

        let response_header = ResponseHeaderProto {
            client: req.header.as_ref().and_then(|h| h.client.clone()),
            error: None,
            state: Vec::new(),
            group_name: group_name.clone(),
            mount_epoch: None,
            route_epoch: Some(route_epoch),
        };

        Ok(Response::new(MsyncResponseProto {
            header: Some(response_header),
            state: Some(GroupStateWatermarkProto {
                group_name,
                state_id: Some(state_id),
            }),
        }))
    }
}
