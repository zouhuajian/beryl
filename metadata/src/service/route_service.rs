// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! MetadataRouteServiceProto implementation.

use crate::raft::AppRaftNode;
use common::error::canonical::{CanonicalError, ErrorClass, ErrorCode, RefreshReason};
use common::header::{ClientInfo, RequestHeader, ResponseHeader, RpcErrorCode};
use proto::metadata::metadata_route_service_proto_server::MetadataRouteServiceProto;
use proto::metadata::{
    GetFileMetaRequestProto, GetFileMetaResponseProto, GetRouteTableRequestProto, GetRouteTableResponseProto,
    MsyncRequestProto, MsyncResponseProto, RefreshRouteRequestProto, RefreshRouteResponseProto,
};
use std::collections::HashMap;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use types::ids::ShardGroupId;
use types::{ClientId, GroupStateWatermark, RaftLogId};

/// Production route/consistency service for metadata clients.
pub struct MetadataRouteServiceImpl {
    raft_node: Arc<AppRaftNode>,
    shard_group_id: ShardGroupId,
}

impl MetadataRouteServiceImpl {
    /// Create a route service bound to one authoritative metadata group.
    pub fn new(raft_node: Arc<AppRaftNode>, shard_group_id: ShardGroupId) -> Self {
        Self {
            raft_node,
            shard_group_id,
        }
    }

    fn unsupported_header(
        header: Option<&proto::common::RequestHeaderProto>,
        message: &'static str,
    ) -> proto::common::ResponseHeaderProto {
        let client = client_from_proto(header);
        let canonical = CanonicalError {
            class: ErrorClass::Fatal,
            code: Some(ErrorCode::RpcCode(RpcErrorCode::NoSuchMethod)),
            reason: None,
            retry_after_ms: None,
            message: message.to_string(),
            refresh_hint: None,
        };
        (&ResponseHeader::from_canonical(client, canonical)).into()
    }

    fn msync_header_error(
        client: ClientInfo,
        group_id: Option<ShardGroupId>,
        canonical: CanonicalError,
    ) -> proto::common::ResponseHeaderProto {
        let mut header = ResponseHeader::from_canonical(client, canonical);
        if let Some(group_id) = group_id {
            header.group_id = Some(group_id.as_raw());
        }
        (&header).into()
    }

    fn msync_ok_header(
        client: ClientInfo,
        group_id: ShardGroupId,
        state_id: RaftLogId,
    ) -> proto::common::ResponseHeaderProto {
        let watermark = GroupStateWatermark::new(group_id, state_id);
        (&ResponseHeader::ok(client)
            .with_group_id(group_id.as_raw())
            .with_state(vec![watermark]))
            .into()
    }

    fn parse_msync_header(
        proto: Option<proto::common::RequestHeaderProto>,
    ) -> Result<RequestHeader, (ClientInfo, String)> {
        let Some(proto) = proto else {
            return Err((
                ClientInfo::new(ClientId::new(0)),
                "MsyncRequestProto requires RequestHeader".to_string(),
            ));
        };
        let client = client_from_proto(Some(&proto));
        RequestHeader::try_from(proto).map_err(|err| (client, format!("invalid Msync RequestHeader: {err}")))
    }

    fn requested_group(header: &RequestHeader) -> Result<Option<ShardGroupId>, String> {
        let header_group = header.group_id.map(ShardGroupId::new);
        let mut state_group = None;
        for watermark in &header.state {
            match state_group {
                Some(existing) if existing != watermark.group_id => {
                    return Err("Msync currently supports exactly one group watermark".to_string());
                }
                Some(_) => {}
                None => state_group = Some(watermark.group_id),
            }
        }

        match (header_group, state_group) {
            (Some(header_group), Some(state_group)) if header_group != state_group => Err(format!(
                "Msync RequestHeader group_id {} does not match state watermark group_id {}",
                header_group.as_raw(),
                state_group.as_raw()
            )),
            (Some(group_id), _) => Ok(Some(group_id)),
            (None, Some(group_id)) => Ok(Some(group_id)),
            (None, None) => Ok(None),
        }
    }

    fn requested_min_state(header: &RequestHeader, group_id: ShardGroupId) -> Option<RaftLogId> {
        header
            .state
            .iter()
            .find(|watermark| watermark.group_id == group_id)
            .map(|watermark| watermark.state_id)
    }

    fn fatal_invalid_header(message: impl Into<String>) -> CanonicalError {
        CanonicalError {
            class: ErrorClass::Fatal,
            code: Some(ErrorCode::RpcCode(RpcErrorCode::InvalidHeader)),
            reason: None,
            retry_after_ms: None,
            message: message.into(),
            refresh_hint: None,
        }
    }
}

#[tonic::async_trait]
impl MetadataRouteServiceProto for MetadataRouteServiceImpl {
    async fn get_file_meta(
        &self,
        request: Request<GetFileMetaRequestProto>,
    ) -> Result<Response<GetFileMetaResponseProto>, Status> {
        let req = request.into_inner();
        Ok(Response::new(GetFileMetaResponseProto {
            header: Some(Self::unsupported_header(
                req.header.as_ref(),
                "MetadataRouteService.GetFileMeta is not implemented in production yet",
            )),
            meta: None,
            leader_id: 0,
            follower_ids: Vec::new(),
            route_epoch: 0,
            worker_info: HashMap::new(),
        }))
    }

    async fn refresh_route(
        &self,
        request: Request<RefreshRouteRequestProto>,
    ) -> Result<Response<RefreshRouteResponseProto>, Status> {
        let req = request.into_inner();
        Ok(Response::new(RefreshRouteResponseProto {
            header: Some(Self::unsupported_header(
                req.header.as_ref(),
                "MetadataRouteService.RefreshRoute is not implemented in production yet",
            )),
            route_epoch: 0,
            shard_to_group: HashMap::new(),
        }))
    }

    async fn msync(&self, request: Request<MsyncRequestProto>) -> Result<Response<MsyncResponseProto>, Status> {
        let req = request.into_inner();
        let header = match Self::parse_msync_header(req.header) {
            Ok(header) => header,
            Err((client, message)) => {
                return Ok(Response::new(MsyncResponseProto {
                    header: Some(Self::msync_header_error(
                        client,
                        None,
                        Self::fatal_invalid_header(message),
                    )),
                    readable_follower_ids: Vec::new(),
                }));
            }
        };
        let client = header.client.clone();
        let group_id = match Self::requested_group(&header) {
            Ok(Some(group_id)) => group_id,
            Ok(None) => {
                // Without a request group or a single state watermark, Msync has no path/inode
                // target from which to derive a group. Returning state here would forge authority.
                return Ok(Response::new(MsyncResponseProto {
                    header: Some(Self::msync_header_error(
                        client,
                        None,
                        Self::fatal_invalid_header("MsyncRequestProto requires header.group_id or one state watermark"),
                    )),
                    readable_follower_ids: Vec::new(),
                }));
            }
            Err(message) => {
                return Ok(Response::new(MsyncResponseProto {
                    header: Some(Self::msync_header_error(
                        client,
                        None,
                        Self::fatal_invalid_header(message),
                    )),
                    readable_follower_ids: Vec::new(),
                }));
            }
        };

        if group_id != self.shard_group_id {
            let canonical = CanonicalError::need_refresh(
                RpcErrorCode::RouteEpochMismatch,
                RefreshReason::RouteEpochMismatch,
                format!(
                    "requested group {} is not served by this metadata runtime",
                    group_id.as_raw()
                ),
            );
            return Ok(Response::new(MsyncResponseProto {
                header: Some(Self::msync_header_error(client, Some(group_id), canonical)),
                readable_follower_ids: Vec::new(),
            }));
        }

        if !self.raft_node.is_leader() {
            let canonical = CanonicalError::need_refresh(
                RpcErrorCode::NotLeader,
                RefreshReason::NotLeader,
                "msync requires leader",
            );
            return Ok(Response::new(MsyncResponseProto {
                header: Some(Self::msync_header_error(client, Some(group_id), canonical)),
                readable_follower_ids: Vec::new(),
            }));
        }

        let Some(last_applied) = self.raft_node.get_last_applied_state_id() else {
            let canonical = CanonicalError::retryable(
                RpcErrorCode::NodeUnavailable,
                Some(10),
                "last_applied_log_id is not available for msync",
            );
            return Ok(Response::new(MsyncResponseProto {
                header: Some(Self::msync_header_error(client, Some(group_id), canonical)),
                readable_follower_ids: Vec::new(),
            }));
        };

        if let Some(required) = Self::requested_min_state(&header, group_id) {
            if !last_applied.has_reached(&required) {
                let canonical = CanonicalError::retryable(
                    RpcErrorCode::NodeUnavailable,
                    Some(10),
                    format!("msync state not reached: current={last_applied:?}, required={required:?}"),
                );
                return Ok(Response::new(MsyncResponseProto {
                    header: Some(Self::msync_header_error(client, Some(group_id), canonical)),
                    readable_follower_ids: Vec::new(),
                }));
            }
        }

        Ok(Response::new(MsyncResponseProto {
            header: Some(Self::msync_ok_header(client, group_id, last_applied)),
            readable_follower_ids: Vec::new(),
        }))
    }

    async fn get_route_table(
        &self,
        request: Request<GetRouteTableRequestProto>,
    ) -> Result<Response<GetRouteTableResponseProto>, Status> {
        let req = request.into_inner();
        Ok(Response::new(GetRouteTableResponseProto {
            header: Some(Self::unsupported_header(
                req.header.as_ref(),
                "MetadataRouteService.GetRouteTable is not implemented in production yet",
            )),
            route_epoch: 0,
            shard_to_group: HashMap::new(),
            group_to_leader: HashMap::new(),
            group_to_followers: HashMap::new(),
        }))
    }
}

fn client_from_proto(header: Option<&proto::common::RequestHeaderProto>) -> ClientInfo {
    header
        .and_then(|header| header.client.clone())
        .and_then(|client| ClientInfo::try_from(client).ok())
        .unwrap_or_else(|| ClientInfo::new(ClientId::new(0)))
}
