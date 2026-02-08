// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Mock metadata server for integration tests.

use proto::common::{FileLayoutProto, FileMetaProto};
use proto::metadata::metadata_route_service_proto_server::MetadataRouteServiceProto;
use proto::metadata::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use types::ids::DataHandleId;

/// Mock metadata server used by client contract tests.
pub struct MockMetadataServer {
    files: Arc<RwLock<HashMap<DataHandleId, FileMetaProto>>>,
    route_epoch: Arc<RwLock<u64>>,
    leader_id: u64,
    follower_ids: Vec<u64>,
}

impl MockMetadataServer {
    pub fn new(leader_id: u64, follower_ids: Vec<u64>) -> Self {
        Self {
            files: Arc::new(RwLock::new(HashMap::new())),
            route_epoch: Arc::new(RwLock::new(1)),
            leader_id,
            follower_ids,
        }
    }

    pub async fn add_file(&self, data_handle_id: DataHandleId, meta: FileMetaProto) {
        let mut files = self.files.write().await;
        files.insert(data_handle_id, meta);
    }

    pub async fn get_file(&self, data_handle_id: DataHandleId) -> Option<FileMetaProto> {
        let files = self.files.read().await;
        files.get(&data_handle_id).cloned()
    }

    pub async fn increment_route_epoch(&self) {
        let mut epoch = self.route_epoch.write().await;
        *epoch += 1;
    }
}

#[tonic::async_trait]
impl MetadataRouteServiceProto for MockMetadataServer {
    async fn get_file_meta(
        &self,
        request: Request<GetFileMetaRequestProto>,
    ) -> Result<Response<GetFileMetaResponseProto>, Status> {
        let req = request.into_inner();
        let inode_id = match req.target {
            Some(get_file_meta_request_proto::Target::InodeId(id)) => id,
            Some(get_file_meta_request_proto::Target::Path(_)) => {
                return Err(Status::unimplemented("path target not supported in mock"))
            }
            None => return Err(Status::invalid_argument("inode_id required")),
        };

        let files = self.files.read().await;
        let meta = files.values().find(|m| m.inode_id == inode_id).cloned();
        let route_epoch = *self.route_epoch.read().await;

        use proto::common::RaftLogIdProto;
        use proto::common::ResponseHeaderProto;

        let group_id = req
            .header
            .as_ref()
            .and_then(|h| if h.group_id != 0 { Some(h.group_id) } else { None })
            .unwrap_or(0);
        let state_id = RaftLogIdProto {
            term: 1,
            leader_node_id: self.leader_id,
            index: route_epoch,
        };
        let response_header = ResponseHeaderProto {
            client: req.header.as_ref().and_then(|h| h.client.clone()),
            error: None,
            state_id: Some(state_id),
            group_id,
            mount_epoch: None,
            route_epoch: Some(route_epoch),
        };

        Ok(Response::new(GetFileMetaResponseProto {
            header: Some(response_header),
            meta,
            leader_id: self.leader_id,
            follower_ids: self.follower_ids.clone(),
            route_epoch,
            worker_info: HashMap::new(),
        }))
    }

    async fn refresh_route(
        &self,
        request: Request<RefreshRouteRequestProto>,
    ) -> Result<Response<RefreshRouteResponseProto>, Status> {
        let req = request.into_inner();
        let route_epoch = *self.route_epoch.read().await;

        use proto::common::RaftLogIdProto;
        use proto::common::ResponseHeaderProto;

        let group_id = req
            .header
            .as_ref()
            .and_then(|h| if h.group_id != 0 { Some(h.group_id) } else { None })
            .unwrap_or(0);
        let state_id = RaftLogIdProto {
            term: 1,
            leader_node_id: self.leader_id,
            index: route_epoch,
        };
        let response_header = ResponseHeaderProto {
            client: req.header.as_ref().and_then(|h| h.client.clone()),
            error: None,
            state_id: Some(state_id),
            group_id,
            mount_epoch: None,
            route_epoch: Some(route_epoch),
        };

        Ok(Response::new(RefreshRouteResponseProto {
            header: Some(response_header),
            route_epoch,
            shard_to_group: HashMap::new(),
        }))
    }

    async fn msync(&self, request: Request<MsyncRequestProto>) -> Result<Response<MsyncResponseProto>, Status> {
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

        use proto::common::RaftLogIdProto;
        use proto::common::ResponseHeaderProto;

        let route_epoch = *self.route_epoch.read().await;
        let state_id = RaftLogIdProto {
            term: 1,
            leader_node_id: self.leader_id,
            index: route_epoch,
        };

        let response_header = ResponseHeaderProto {
            client: req.header.as_ref().and_then(|h| h.client.clone()),
            error: None,
            state_id: Some(state_id),
            group_id,
            mount_epoch: None,
            route_epoch: Some(route_epoch),
        };

        let readable_follower_ids = if req.include_readable_followers.unwrap_or(false) {
            self.follower_ids.clone()
        } else {
            vec![]
        };

        Ok(Response::new(MsyncResponseProto {
            header: Some(response_header),
            readable_follower_ids,
        }))
    }

    async fn get_route_table(
        &self,
        request: Request<GetRouteTableRequestProto>,
    ) -> Result<Response<GetRouteTableResponseProto>, Status> {
        let req = request.into_inner();
        let route_epoch = *self.route_epoch.read().await;
        let mut group_to_leader = HashMap::new();
        let mut group_to_followers = HashMap::new();

        group_to_leader.insert(0, self.leader_id);
        if !self.follower_ids.is_empty() {
            let mut node_list = NodeListProto::default();
            node_list.node_ids = self.follower_ids.clone();
            group_to_followers.insert(0, node_list);
        }
        use proto::common::RaftLogIdProto;
        use proto::common::ResponseHeaderProto;

        let group_id = req
            .header
            .as_ref()
            .and_then(|h| if h.group_id != 0 { Some(h.group_id) } else { None })
            .unwrap_or(0);
        let state_id = RaftLogIdProto {
            term: 1,
            leader_node_id: self.leader_id,
            index: route_epoch,
        };
        let response_header = ResponseHeaderProto {
            client: req.header.as_ref().and_then(|h| h.client.clone()),
            error: None,
            state_id: Some(state_id),
            group_id,
            mount_epoch: None,
            route_epoch: Some(route_epoch),
        };

        Ok(Response::new(GetRouteTableResponseProto {
            header: Some(response_header),
            route_epoch,
            shard_to_group: HashMap::new(),
            group_to_leader,
            group_to_followers,
        }))
    }
}

/// Helper to build a simple file metadata record for tests.
pub fn create_test_file_meta(data_handle_id: u64, version: u64, route_epoch: u64) -> FileMetaProto {
    FileMetaProto {
        inode_id: data_handle_id,
        data_handle_id,
        file_version: version,
        blocks: vec![],
        route_epoch,
        consistency_token: version,
        layout: Some(FileLayoutProto {
            block_size: 64 * 1024 * 1024,
            chunk_size: 64 * 1024,
            replication: 3,
        }),
        committed_length: 0,
    }
}
