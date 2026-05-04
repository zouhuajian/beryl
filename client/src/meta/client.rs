// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata RPC client.

use crate::error::{ClientError, ClientResult};
use common::header::RequestHeader;
use proto::metadata::file_system_service_proto_client::FileSystemServiceProtoClient;
use proto::metadata::{MsyncRequestProto, MsyncResponseProto};
use std::sync::Arc;
use tonic::transport::Channel;

/// Metadata service client.
pub struct MetadataClient {
    /// gRPC client for FileSystemService.
    filesystem_client: Arc<FileSystemServiceProtoClient<Channel>>,
}

impl MetadataClient {
    /// Create a new metadata client.
    pub async fn new(endpoint: &str) -> ClientResult<Self> {
        let channel = Channel::from_shared(endpoint.to_string())
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
        let group_id = ctx
            .group_id
            .ok_or_else(|| ClientError::Metadata("MsyncRequestProto requires ctx.group_id to be set".to_string()))?;
        let state_id = ctx
            .state
            .iter()
            .find(|watermark| watermark.group_id.as_raw() == group_id)
            .map(|watermark| watermark.state_id)
            .unwrap_or_default();
        let state = types::GroupStateWatermark::new(types::ids::ShardGroupId::new(group_id), state_id);
        let request = MsyncRequestProto {
            header: Some(ctx.into()),
            state: Some((&state).into()),
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
