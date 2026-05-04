// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! FileSystemService Msync handler.

use crate::raft::AppRaftNode;
use common::error::canonical::{CanonicalError, ErrorClass, ErrorCode, RefreshReason};
use common::header::{ClientInfo, RequestHeader, ResponseHeader, RpcErrorCode};
use proto::metadata::{MsyncRequestProto, MsyncResponseProto};
use std::sync::Arc;
use types::ids::ShardGroupId;
use types::{ClientId, GroupStateWatermark};

/// Handles metadata state sync for one local metadata raft group.
pub struct MsyncHandler {
    raft_node: Arc<AppRaftNode>,
    shard_group_id: ShardGroupId,
}

impl MsyncHandler {
    /// Create an Msync handler bound to one authoritative metadata group.
    pub fn new(raft_node: Arc<AppRaftNode>, shard_group_id: ShardGroupId) -> Self {
        Self {
            raft_node,
            shard_group_id,
        }
    }

    /// Handle one Msync request using application-level response errors.
    pub fn handle(&self, req: MsyncRequestProto) -> MsyncResponseProto {
        let header = match Self::parse_header(req.header) {
            Ok(header) => header,
            Err((client, message)) => {
                return Self::error_response(client, None, Self::fatal_invalid_header(message));
            }
        };
        let client = header.client.clone();
        let requested_state = match Self::parse_state(req.state) {
            Ok(state) => state,
            Err(message) => {
                return Self::error_response(
                    client,
                    header.group_id.map(ShardGroupId::new),
                    Self::fatal_invalid_header(message),
                );
            }
        };

        let Some(header_group_id) = header.group_id.map(ShardGroupId::new) else {
            return Self::error_response(
                client,
                Some(requested_state.group_id),
                Self::fatal_invalid_header("MsyncRequestProto requires header.group_id"),
            );
        };
        if header_group_id != requested_state.group_id {
            return Self::error_response(
                client,
                Some(header_group_id),
                Self::fatal_invalid_header(format!(
                    "Msync RequestHeader group_id {} does not match state watermark group_id {}",
                    header_group_id.as_raw(),
                    requested_state.group_id.as_raw()
                )),
            );
        }
        if requested_state.group_id != self.shard_group_id {
            let canonical = CanonicalError::need_refresh(
                RpcErrorCode::RouteEpochMismatch,
                RefreshReason::RouteEpochMismatch,
                format!(
                    "requested group {} is not served by this metadata runtime",
                    requested_state.group_id.as_raw()
                ),
            );
            return Self::error_response(client, Some(requested_state.group_id), canonical);
        }

        if !self.raft_node.is_leader() {
            let canonical = CanonicalError::need_refresh(
                RpcErrorCode::NotLeader,
                RefreshReason::NotLeader,
                "msync requires leader",
            );
            return Self::error_response(client, Some(requested_state.group_id), canonical);
        }

        let Some(last_applied) = self.raft_node.get_last_applied_state_id() else {
            let canonical = CanonicalError::retryable(
                RpcErrorCode::NodeUnavailable,
                Some(10),
                "last_applied_log_id is not available for msync",
            );
            return Self::error_response(client, Some(requested_state.group_id), canonical);
        };

        if !last_applied.has_reached(&requested_state.state_id) {
            let canonical = CanonicalError::retryable(
                RpcErrorCode::NodeUnavailable,
                Some(10),
                format!(
                    "msync state not reached: current={last_applied:?}, required={:?}",
                    requested_state.state_id
                ),
            );
            return Self::error_response(client, Some(requested_state.group_id), canonical);
        }

        let authoritative = GroupStateWatermark::new(self.shard_group_id, last_applied);
        MsyncResponseProto {
            header: Some((&ResponseHeader::ok(client).with_group_id(self.shard_group_id.as_raw())).into()),
            state: Some((&authoritative).into()),
        }
    }

    /// Return a structured application error for test-only services built without raft.
    pub fn unavailable(req: MsyncRequestProto) -> MsyncResponseProto {
        let client = client_from_proto(req.header.as_ref());
        let group_id = req
            .state
            .and_then(|state| state.group_id)
            .map(|group_id| ShardGroupId::new(group_id.value));
        Self::error_response(
            client,
            group_id,
            CanonicalError::retryable(
                RpcErrorCode::NodeUnavailable,
                Some(10),
                "msync raft node is not configured",
            ),
        )
    }

    fn parse_header(proto: Option<proto::common::RequestHeaderProto>) -> Result<RequestHeader, (ClientInfo, String)> {
        let Some(proto) = proto else {
            return Err((
                ClientInfo::new(ClientId::new(0)),
                "MsyncRequestProto requires RequestHeader".to_string(),
            ));
        };
        let client = client_from_proto(Some(&proto));
        RequestHeader::try_from(proto).map_err(|err| (client, format!("invalid Msync RequestHeader: {err}")))
    }

    fn parse_state(proto: Option<proto::common::GroupStateWatermarkProto>) -> Result<GroupStateWatermark, String> {
        let Some(proto) = proto else {
            return Err("MsyncRequestProto requires state watermark".to_string());
        };
        proto
            .try_into()
            .map_err(|err| format!("invalid Msync state watermark: {err}"))
    }

    fn error_response(
        client: ClientInfo,
        group_id: Option<ShardGroupId>,
        canonical: CanonicalError,
    ) -> MsyncResponseProto {
        let mut header = ResponseHeader::from_canonical(client, canonical);
        if let Some(group_id) = group_id {
            header.group_id = Some(group_id.as_raw());
        }
        MsyncResponseProto {
            header: Some((&header).into()),
            state: None,
        }
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

fn client_from_proto(header: Option<&proto::common::RequestHeaderProto>) -> ClientInfo {
    header
        .and_then(|header| header.client.clone())
        .and_then(|client| ClientInfo::try_from(client).ok())
        .unwrap_or_else(|| ClientInfo::new(ClientId::new(0)))
}
