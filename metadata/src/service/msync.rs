// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! FileSystemService Msync handler.

use super::core_util::{header_from_canonical_error, ok_header_from_request};
use crate::raft::AppRaftNode;
use common::error::canonical::{CanonicalError, ErrorClass, ErrorCode, RefreshReason};
use common::header::{RequestHeader, RpcErrorCode};
use proto::metadata::{MsyncRequestProto, MsyncResponseProto};
use std::sync::Arc;
use types::{GroupName, GroupStateWatermark};

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
        let req_header = req.header;
        let header = match Self::parse_header(req_header.clone()) {
            Ok(header) => header,
            Err(canonical) => {
                return Self::error_response(&req_header, None, canonical);
            }
        };

        let Some(header_group_name) = header.group_name.clone() else {
            return Self::error_response(
                &req_header,
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
            return Self::error_response(&req_header, Some(header_group_name), canonical);
        }

        if !self.raft_node.is_leader() {
            let canonical = CanonicalError::need_refresh(
                RpcErrorCode::NotLeader,
                RefreshReason::NotLeader,
                "msync requires leader",
            );
            return Self::error_response(&req_header, Some(header_group_name), canonical);
        }

        let Some(last_applied) = self.raft_node.get_last_applied_state_id() else {
            let canonical = CanonicalError::retryable(
                RpcErrorCode::NodeUnavailable,
                Some(10),
                "last_applied_log_id is not available for msync",
            );
            return Self::error_response(&req_header, Some(header_group_name), canonical);
        };

        let authoritative = GroupStateWatermark::new(self.group_name.clone(), last_applied);
        MsyncResponseProto {
            header: Some(ok_header_from_request(&req_header, Some(self.group_name.clone()), None)),
            state: Some((&authoritative).into()),
        }
    }

    /// Return a structured application error for test-only services built without raft.
    pub fn unavailable(req: MsyncRequestProto) -> MsyncResponseProto {
        let group_name = req
            .header
            .as_ref()
            .and_then(|header| GroupName::parse_optional(&header.group_name).ok().flatten());
        Self::error_response(
            &req.header,
            group_name,
            CanonicalError::retryable(
                RpcErrorCode::NodeUnavailable,
                Some(10),
                "msync raft node is not configured",
            ),
        )
    }

    #[allow(clippy::result_large_err)]
    fn parse_header(proto: Option<proto::common::RequestHeaderProto>) -> Result<RequestHeader, CanonicalError> {
        let Some(proto) = proto else {
            return Err(Self::fatal_invalid_header("MsyncRequestProto requires RequestHeader"));
        };
        RequestHeader::try_from(proto)
            .map_err(|err| Self::fatal_invalid_header(format!("invalid Msync RequestHeader: {err}")))
    }

    fn error_response(
        req_header: &Option<proto::common::RequestHeaderProto>,
        group_name: Option<GroupName>,
        canonical: CanonicalError,
    ) -> MsyncResponseProto {
        MsyncResponseProto {
            header: Some(header_from_canonical_error(req_header, group_name, None, &canonical)),
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
