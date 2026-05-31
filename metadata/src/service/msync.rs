// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! FileSystemService Msync handler.

use crate::raft::AppRaftNode;
use common::error::canonical::{CanonicalError, ErrorClass, ErrorCode, RefreshReason};
use common::header::{ClientInfo, RequestHeader, ResponseHeader, RpcErrorCode};
use proto::metadata::{MsyncRequestProto, MsyncResponseProto};
use std::sync::Arc;
use types::{ClientId, GroupName, GroupStateWatermark};

/// Handles metadata state sync for one local metadata raft group.
pub struct MsyncHandler {
    raft_node: Arc<AppRaftNode>,
    group_name: GroupName,
}

impl MsyncHandler {
    /// Create an Msync handler bound to one authoritative metadata group.
    pub fn new(raft_node: Arc<AppRaftNode>, group_name: GroupName) -> Self {
        Self { raft_node, group_name }
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

        let Some(header_group_name) = header.group_name else {
            return Self::error_response(
                client,
                None,
                Self::fatal_invalid_header("MsyncRequestProto requires header.group_name"),
            );
        };
        if header_group_name != self.group_name {
            let canonical = CanonicalError::need_refresh(
                RpcErrorCode::ShardMoved,
                RefreshReason::OwnerGroupMismatch,
                format!(
                    "requested group {} is not served by this metadata runtime",
                    header_group_name
                ),
            );
            return Self::error_response(client, Some(header_group_name), canonical);
        }

        if !self.raft_node.is_leader() {
            let canonical = CanonicalError::need_refresh(
                RpcErrorCode::NotLeader,
                RefreshReason::NotLeader,
                "msync requires leader",
            );
            return Self::error_response(client, Some(header_group_name), canonical);
        }

        let Some(last_applied) = self.raft_node.get_last_applied_state_id() else {
            let canonical = CanonicalError::retryable(
                RpcErrorCode::NodeUnavailable,
                Some(10),
                "last_applied_log_id is not available for msync",
            );
            return Self::error_response(client, Some(header_group_name), canonical);
        };

        let authoritative = GroupStateWatermark::new(self.group_name.clone(), last_applied);
        MsyncResponseProto {
            header: Some((&ResponseHeader::ok(client).with_group_name(self.group_name.clone())).into()),
            state: Some((&authoritative).into()),
        }
    }

    /// Return a structured application error for test-only services built without raft.
    pub fn unavailable(req: MsyncRequestProto) -> MsyncResponseProto {
        let client = client_from_proto(req.header.as_ref());
        let group_name = req
            .header
            .as_ref()
            .and_then(|header| (!header.group_name.is_empty()).then_some(header.group_name.as_str()))
            .and_then(|group_name| GroupName::parse(group_name).ok());
        Self::error_response(
            client,
            group_name,
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

    fn error_response(
        client: ClientInfo,
        group_name: Option<GroupName>,
        canonical: CanonicalError,
    ) -> MsyncResponseProto {
        let mut header = ResponseHeader::from_canonical(client, canonical);
        if let Some(group_name) = group_name {
            header.group_name = Some(group_name);
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
