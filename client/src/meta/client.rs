// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata RPC client.

use crate::error::{ClientError, ClientResult};
use common::header::RequestHeader;
use proto::metadata::file_system_service_proto_client::FileSystemServiceProtoClient;
use proto::metadata::{MsyncRequestProto, MsyncResponseProto};
use std::sync::Arc;
use tonic::transport as tonic_net;

/// Metadata service client.
pub struct MetadataClient {
    /// gRPC client for FileSystemService.
    filesystem_client: Arc<FileSystemServiceProtoClient<tonic_net::Channel>>,
}

impl MetadataClient {
    /// Create a new metadata client.
    pub async fn new(endpoint: &str) -> ClientResult<Self> {
        let channel = tonic_net::Channel::from_shared(endpoint.to_string())
            .map_err(|e| ClientError::Metadata(format!("Invalid endpoint: {}", e)))?
            .connect()
            .await
            .map_err(|e| ClientError::Metadata(format!("Failed to connect: {}", e)))?;

        let filesystem_client = FileSystemServiceProtoClient::new(channel);

        Ok(Self {
            filesystem_client: Arc::new(filesystem_client),
        })
    }

    /// Msync: lightweight sync to advance state_id for a group.
    /// group_id must be set in ctx.group_id.
    pub async fn msync(&self, ctx: &RequestHeader) -> ClientResult<MsyncResponseProto> {
        let header = minimal_msync_header(ctx)
            .ok_or_else(|| ClientError::Metadata("MsyncRequestProto requires ctx.group_id to be set".to_string()))?;
        let request = MsyncRequestProto {
            header: Some((&header).into()),
        };

        let mut client = (*self.filesystem_client).clone();
        let response = client
            .msync(tonic::Request::new(request))
            .await
            .map_err(ClientError::from)?
            .into_inner();

        Ok(response)
    }
}

pub(crate) fn minimal_msync_header(ctx: &RequestHeader) -> Option<RequestHeader> {
    ctx.group_id?;
    let mut header = ctx.child();
    header.state.clear();
    header.mount_epoch = None;
    header.route_epoch = None;
    Some(header)
}

#[cfg(test)]
mod tests {
    use super::minimal_msync_header;
    use common::header::RequestHeader;
    use types::ids::ShardGroupId;
    use types::{ClientId, GroupStateWatermark, RaftLogId};

    #[test]
    fn minimal_msync_header_strips_state_and_epoch_context() {
        let group_id = ShardGroupId::new(7);
        let mut header = RequestHeader::new(ClientId::new(1)).with_group_id(group_id.as_raw());
        header.state = vec![GroupStateWatermark::new(group_id, RaftLogId::new(1, 2, 3))];
        header.mount_epoch = Some(11);
        header.route_epoch = Some(13);

        let msync_header = minimal_msync_header(&header).expect("valid msync header");

        assert_eq!(msync_header.group_id, Some(group_id.as_raw()));
        assert!(msync_header.state.is_empty());
        assert_eq!(msync_header.mount_epoch, None);
        assert_eq!(msync_header.route_epoch, None);
        assert_eq!(msync_header.client.client_id, header.client.client_id);
    }
}
