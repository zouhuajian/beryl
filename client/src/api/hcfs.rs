// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! HCFS-style API implementation.

use crate::cache::{FileMetaCache, RouteCache};
use crate::canonical::validate_header_or_action;
use crate::config::ClientConfig;
use crate::consistency::ConsistencyLevel;
use crate::error::{ClientError, ClientResult};
use crate::meta::MetadataClient;
use crate::meta::{replay_policy_for_method, ActionMachine, RpcOp, TonicFileSystemRpc};
use crate::routing::{GroupRoleCache, RouteTable, WorkerSelector};
use bytes::Bytes;
use common::header::RequestHeader;
use proto::metadata::DeleteRequestProto;
use std::sync::Arc;
use types::fs::InodeId;
use types::ids::DataHandleId;

/// File open flags.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpenFlags {
    /// Open for reading.
    Read,
    /// Open for writing.
    Write,
    /// Open for reading and writing.
    ReadWrite,
    /// Create file if it doesn't exist.
    Create,
    /// Truncate file on open.
    Truncate,
}

/// File handle (bound to data_handle_id for stability).
#[derive(Clone, Debug)]
pub struct Handle {
    /// File ID (stable identifier).
    pub data_handle_id: DataHandleId,
    /// Namespace identity (authoritative inode).
    pub inode_id: InodeId,
    /// File path (for logging/debugging).
    pub path: String,
    /// Open flags.
    pub flags: OpenFlags,
}

/// Main client implementation.
pub struct Client {
    /// Client configuration.
    config: Arc<ClientConfig>,
    /// File metadata cache.
    file_meta_cache: Arc<FileMetaCache>,
    /// Route cache.
    route_cache: Arc<RouteCache>,
    /// Route table.
    route_table: Arc<RouteTable>,
    /// Group role cache.
    group_role_cache: Arc<GroupRoleCache>,
    /// Worker selector.
    worker_selector: Arc<WorkerSelector>,
    /// Metadata clients (one per endpoint).
    metadata_clients: Arc<Vec<MetadataClient>>,
}

impl Client {
    /// Create a new client.
    pub async fn new(config: ClientConfig) -> ClientResult<Self> {
        // Create caches
        let file_meta_cache = Arc::new(FileMetaCache::new(
            config.cache.max_file_meta_entries,
            config.cache.file_meta_ttl_secs,
        ));
        let route_cache = RouteCache::new(config.cache.max_route_entries, config.cache.route_ttl_secs);

        // Create route table (clone cache for route table)
        let route_table = Arc::new(RouteTable::new(route_cache.clone()));
        let route_cache = Arc::new(route_cache);

        // Create group role cache
        let group_role_cache = Arc::new(GroupRoleCache::new(60)); // 60s health timeout

        // Create worker selector
        let worker_selector = Arc::new(WorkerSelector::new(crate::routing::SelectionStrategy::First));

        // Create metadata clients
        let mut metadata_clients = Vec::new();
        for endpoint in &config.metadata_endpoints {
            let client = MetadataClient::new(endpoint).await?;
            metadata_clients.push(client);
        }

        Ok(Self {
            config: Arc::new(config),
            file_meta_cache,
            route_cache,
            route_table,
            group_role_cache,
            worker_selector,
            metadata_clients: Arc::new(metadata_clients),
        })
    }

    /// Open a file.
    pub async fn open(&self, path: &str, flags: OpenFlags) -> ClientResult<Handle> {
        // TODO: Resolve path to data_handle_id via metadata
        // For now, use a placeholder
        let data_handle_id = DataHandleId::new(0); // Placeholder
        let inode_id = InodeId::new(0); // Placeholder until inode lookup is wired

        Ok(Handle {
            data_handle_id,
            inode_id,
            path: path.to_string(),
            flags,
        })
    }

    /// Read from a file.
    pub async fn read(
        &self,
        handle: &Handle,
        _offset: u64,
        _len: u32,
        consistency: Option<ConsistencyLevel>,
    ) -> ClientResult<Bytes> {
        let consistency = consistency.unwrap_or(self.config.default_consistency);
        let client_id = self.config.inner.as_flat().get_i64("client.id").unwrap_or(0) as u64;
        let ctx = RequestHeader::new(types::ClientId::new(client_id));

        // Try cache first (if consistency allows)
        if consistency.allows_cache() {
            if let Some(_meta) = self.file_meta_cache.get(&handle.data_handle_id) {
                // TODO: Use cached metadata to read from worker
                // For now, fall through to metadata
            }
        }

        // Get file metadata from metadata service
        let meta_client = self
            .metadata_clients
            .first()
            .ok_or_else(|| ClientError::Metadata("No metadata endpoints available".to_string()))?;

        let response = meta_client
            .get_file_meta(&ctx, Some(handle.inode_id), None, consistency, None)
            .await?;

        let response_header = response
            .header
            .clone()
            .ok_or_else(|| ClientError::Metadata("Missing response header".to_string()))?;
        let response_header = common::header::ResponseHeader::try_from(response_header)
            .map_err(|e| ClientError::Metadata(format!("Failed to parse response header: {}", e)))?;
        validate_header_or_action(&response_header).map_err(ClientError::from)?;

        // Update caches
        if let Some(meta) = response.meta {
            self.file_meta_cache.put(handle.data_handle_id, meta.clone());
            // TODO: Read from worker using block locations
        }

        // Placeholder: return empty bytes
        Ok(Bytes::new())
    }

    /// Write to a file.
    pub async fn write(&self, _handle: &Handle, _offset: u64, _data: Bytes) -> ClientResult<()> {
        // TODO: Implement write logic
        // 1. Acquire lease
        // 2. Write chunks to workers
        // 3. Seal block
        // 4. Commit length

        Ok(())
    }

    /// Close a file handle.
    pub async fn close(&self, _handle: Handle) -> ClientResult<()> {
        // TODO: Release any resources
        Ok(())
    }

    /// Get file status.
    pub async fn stat(&self, _path: &str) -> ClientResult<FileStatus> {
        // TODO: Implement stat
        Err(ClientError::Unimplemented("stat not yet implemented".to_string()))
    }

    /// List directory.
    pub async fn list(&self, _path: &str) -> ClientResult<Vec<FileStatus>> {
        // TODO: Implement list
        Err(ClientError::Unimplemented("list not yet implemented".to_string()))
    }

    /// Rename a file or directory.
    pub async fn rename(&self, _src: &str, _dst: &str) -> ClientResult<()> {
        // TODO: Implement rename
        Err(ClientError::Unimplemented("rename not yet implemented".to_string()))
    }

    /// Delete a file, symlink, or empty directory.
    pub async fn delete(&self, path: &str, recursive: bool) -> ClientResult<()> {
        let endpoint = self
            .config
            .metadata_endpoints
            .first()
            .ok_or_else(|| ClientError::Metadata("No metadata endpoints available".to_string()))?;
        let rpc = Arc::new(TonicFileSystemRpc::connect(endpoint).await?);
        let machine = ActionMachine::new(rpc, self.config.metadata_endpoints.clone());
        let client_id = self.config.inner.as_flat().get_i64("client.id").unwrap_or(0) as u64;
        let request = DeleteRequestProto {
            header: Some((&RequestHeader::new(types::ClientId::new(client_id))).into()),
            path: path.to_string(),
            recursive,
        };
        let op = RpcOp::delete(request);
        let policy = replay_policy_for_method(op.method());
        machine.call_with_refresh(policy, op).await.map(|_| ())
    }
}

/// File status information.
#[derive(Clone, Debug)]
pub struct FileStatus {
    /// File path.
    pub path: String,
    /// File ID.
    pub data_handle_id: DataHandleId,
    /// Is directory.
    pub is_directory: bool,
    /// File length.
    pub length: u64,
}
