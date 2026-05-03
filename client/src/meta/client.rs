// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata RPC client.

use crate::consistency::ConsistencyLevel;
use crate::error::{ClientError, ClientResult};
use common::header::RequestHeader;
use proto::metadata::metadata_route_service_proto_client::MetadataRouteServiceProtoClient;
use proto::metadata::*;
use std::sync::Arc;
use tonic::transport::Channel;
use types::fs::InodeId;

/// Metadata service client.
pub struct MetadataClient {
    /// gRPC client for route service.
    route_client: Arc<MetadataRouteServiceProtoClient<Channel>>,
    /// Metadata endpoint.
    endpoint: String,
}

impl MetadataClient {
    /// Create a new metadata client.
    pub async fn new(endpoint: &str) -> ClientResult<Self> {
        let channel = Channel::from_shared(endpoint.to_string())
            .map_err(|e| ClientError::Metadata(format!("Invalid endpoint: {}", e)))?
            .connect()
            .await
            .map_err(|e| ClientError::Metadata(format!("Failed to connect: {}", e)))?;

        let route_client = MetadataRouteServiceProtoClient::new(channel);

        Ok(Self {
            route_client: Arc::new(route_client),
            endpoint: endpoint.to_string(),
        })
    }

    /// Get file metadata.
    pub async fn get_file_meta(
        &self,
        ctx: &RequestHeader,
        inode_id: Option<InodeId>,
        path: Option<&str>,
        consistency: ConsistencyLevel,
        _min_token: Option<u64>, // Deprecated: use ctx.state instead
    ) -> ClientResult<GetFileMetaResponseProto> {
        use proto::metadata::get_file_meta_request_proto;
        let mut request = GetFileMetaRequestProto {
            header: Some(ctx.into()),
            target: None,
            consistency: proto::common::ConsistencyLevelProto::from(consistency) as i32,
        };

        if let Some(iid) = inode_id {
            request.target = Some(get_file_meta_request_proto::Target::InodeId(iid.as_raw()));
        } else if let Some(p) = path {
            request.target = Some(get_file_meta_request_proto::Target::Path(p.to_string()));
        } else {
            return Err(ClientError::Metadata(
                "Either inode_id or path must be provided".to_string(),
            ));
        }

        let mut client = (*self.route_client).clone();
        let response = client
            .get_file_meta(tonic::Request::new(request))
            .await
            .map_err(|e| ClientError::from(e))?
            .into_inner();

        Ok(response)
    }

    /// Refresh route table.
    pub async fn refresh_route(
        &self,
        ctx: &RequestHeader,
        inode_id: Option<InodeId>,
    ) -> ClientResult<RefreshRouteResponseProto> {
        let request = RefreshRouteRequestProto {
            header: Some(ctx.into()),
            inode_id: inode_id.map(|i| i.as_raw()),
        };

        let mut client = (*self.route_client).clone();
        let response = client
            .refresh_route(tonic::Request::new(request))
            .await
            .map_err(|e| ClientError::from(e))?
            .into_inner();

        Ok(response)
    }

    /// Get route table.
    pub async fn get_route_table(&self, ctx: &RequestHeader) -> ClientResult<GetRouteTableResponseProto> {
        let request = GetRouteTableRequestProto {
            header: Some(ctx.into()),
        };

        let mut client = (*self.route_client).clone();
        let response = client
            .get_route_table(tonic::Request::new(request))
            .await
            .map_err(|e| ClientError::from(e))?
            .into_inner();

        Ok(response)
    }

    /// Msync: lightweight sync to advance state_id for a group.
    /// group_id must be set in ctx.group_id.
    pub async fn msync(
        &self,
        ctx: &RequestHeader,
        include_readable_followers: bool,
    ) -> ClientResult<MsyncResponseProto> {
        let request = MsyncRequestProto {
            header: Some(ctx.into()),
            include_readable_followers: Some(include_readable_followers),
        };

        let mut client = (*self.route_client).clone();
        let response = client
            .msync(tonic::Request::new(request))
            .await
            .map_err(|e| ClientError::from(e))?
            .into_inner();

        Ok(response)
    }
}
