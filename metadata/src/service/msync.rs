// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! FileSystemService Msync handler.

use super::core_util::{header_from_rpc_error, ok_header_from_request};
use crate::raft::AppRaftNode;
use common::error::rpc::{
    ErrorKind, InternalErrorKind, MetadataErrorKind, ProtocolErrorKind, RefreshHint, RpcErrorDetail,
};
use common::header::RequestHeader;
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
    pub(crate) fn new(raft_node: Arc<AppRaftNode>, group_name: GroupName) -> Self {
        Self { raft_node, group_name }
    }

    /// Handle one Msync request using application-level response errors.
    pub fn handle(&self, req: MsyncRequestProto) -> MsyncResponseProto {
        let req_header = req.header;
        let header = match Self::parse_header(req_header.clone()) {
            Ok(header) => header,
            Err(rpc_error) => {
                return Self::error_response(&req_header, None, rpc_error);
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
            let rpc_error = RpcErrorDetail::refresh_metadata(
                ErrorKind::Metadata(MetadataErrorKind::OwnerGroupMismatch),
                RefreshHint::default(),
                format!(
                    "requested group {} is not served by this metadata runtime",
                    header_group_name
                ),
            );
            return Self::error_response(&req_header, Some(header_group_name), rpc_error);
        }

        if !self.raft_node.is_leader() {
            let rpc_error = RpcErrorDetail::refresh_metadata(
                ErrorKind::Metadata(MetadataErrorKind::NotLeader),
                RefreshHint::default(),
                "msync requires leader",
            );
            return Self::error_response(&req_header, Some(header_group_name), rpc_error);
        }

        let Some(last_applied) = self.raft_node.get_last_applied_state_id() else {
            let rpc_error = RpcErrorDetail::retry(
                ErrorKind::Internal(InternalErrorKind::NodeUnavailable),
                Some(10),
                "last_applied_log_id is not available for msync",
            );
            return Self::error_response(&req_header, Some(header_group_name), rpc_error);
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
            RpcErrorDetail::retry(
                ErrorKind::Internal(InternalErrorKind::NodeUnavailable),
                Some(10),
                "msync raft node is not configured",
            ),
        )
    }

    #[allow(clippy::result_large_err)]
    fn parse_header(proto: Option<proto::common::RequestHeaderProto>) -> Result<RequestHeader, RpcErrorDetail> {
        let Some(proto) = proto else {
            return Err(Self::fatal_invalid_header("MsyncRequestProto requires RequestHeader"));
        };
        RequestHeader::try_from(proto)
            .map_err(|err| Self::fatal_invalid_header(format!("invalid Msync RequestHeader: {err}")))
    }

    fn error_response(
        req_header: &Option<proto::common::RequestHeaderProto>,
        group_name: Option<GroupName>,
        rpc_error: RpcErrorDetail,
    ) -> MsyncResponseProto {
        MsyncResponseProto {
            header: Some(header_from_rpc_error(req_header, group_name, None, &rpc_error)),
            state: None,
        }
    }

    fn fatal_invalid_header(message: impl Into<String>) -> RpcErrorDetail {
        RpcErrorDetail::fail(ErrorKind::Protocol(ProtocolErrorKind::InvalidHeader), message)
    }
}
