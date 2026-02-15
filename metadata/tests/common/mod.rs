// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Test harness for inode service tests.
//!
//! This module provides test-only utilities for creating in-memory Raft nodes,
//! temporary RocksDB instances, and mount initialization for testing.

use metadata::error::MetadataResult;
use metadata::mount::{DataIoPolicy, MountKind, MountTable};
use metadata::raft::{AppRaftNode, AppRaftStateMachine, RocksDBStorage};
use metadata::service::MetadataInodeServiceImpl;
use metadata::state::RaftStateStore;
use std::sync::Arc;
use tempfile::TempDir;
use types::fs::{FileAttrs, Inode, InodeId};
use types::ids::{MountId, ShardGroupId};

/// Test harness for inode service tests.
pub struct FsTestHarness {
    pub temp_dir: TempDir,
    pub storage: Arc<RocksDBStorage>,
    pub mount_table: Arc<MountTable>,
    pub state_machine: Arc<AppRaftStateMachine>,
    pub raft_node: Arc<AppRaftNode>,
    pub state_store: Arc<RaftStateStore>,
    pub inode_service: MetadataInodeServiceImpl,
}

impl FsTestHarness {
    /// Create a new test harness with in-memory-like setup.
    pub async fn new() -> MetadataResult<Self> {
        // Create temporary directory for RocksDB
        let temp_dir = tempfile::tempdir()
            .map_err(|e| metadata::error::MetadataError::Internal(format!("Failed to create temp dir: {}", e)))?;

        // Open RocksDB storage
        let db_path = temp_dir.path().join("metadata.db");
        let storage = Arc::new(RocksDBStorage::open(db_path.to_str().unwrap())?);

        // Create mount table
        let mount_table = Arc::new(MountTable::load_from_storage(&storage)?);

        // Create state machine
        let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table)));

        // Create Raft node (single node, node_id=1)
        use metadata::config::RaftConfig;
        let raft_config = RaftConfig {
            node_id: 1,
            cluster_id: "test-cluster".to_string(),
            peers: vec!["127.0.0.1:9001".to_string()],
        };
        let raft_node =
            Arc::new(AppRaftNode::new(1, Arc::clone(&storage), Arc::clone(&state_machine), &raft_config).await?);

        // Create state store
        let state_store = Arc::new(RaftStateStore::new(Arc::clone(&raft_node)));

        // Create inode service
        use metadata::metrics::MetadataMetrics;
        let metrics = Arc::new(MetadataMetrics::new());
        let inode_service = MetadataInodeServiceImpl::new(
            state_store.clone() as Arc<dyn metadata::state::StateStore>,
            mount_table.clone(),
        )
        .with_storage(Arc::clone(&storage))
        .with_raft_node(Arc::clone(&raft_node))
        .with_metrics(metrics);

        Ok(Self {
            temp_dir,
            storage,
            mount_table,
            state_machine,
            raft_node,
            state_store,
            inode_service,
        })
    }

    /// Create a mount with root inode.
    /// Returns (mount_id, root_inode_id).
    pub async fn create_mount_with_root(
        &self,
        mount_prefix: String,
        ufs_uri: String,
        namespace_owner_group_id: ShardGroupId,
    ) -> MetadataResult<(MountId, InodeId)> {
        // Generate a unique root inode_id for this mount
        // Use a simple counter based on mount count (for test determinism)
        // Start from 1000 to avoid conflicts with test-created inodes (which start from state_machine.next_inode_id)
        let mount_count = self.mount_table.list_mounts().len();
        let root_inode_id = InodeId::new(1000 + (mount_count as u64 * 1000)); // Use 1000, 2000, 3000, etc.

        // Create root inode (directory) first
        // We'll use a temporary mount_id for the inode, then update after mount creation
        let temp_mount_id = MountId::new(999); // Temporary, will be updated
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let mut root_attrs = FileAttrs::new();
        root_attrs.update_timestamps(now_ms);
        root_attrs.mode = 0o755;
        root_attrs.nlink = 1;

        let root_inode = Inode::new_dir(root_inode_id, root_attrs, temp_mount_id);

        // Store root inode (with temp mount_id, will be fixed after mount creation)
        self.storage.put_inode(&root_inode)?;

        // Create mount entry using MountTable (which will generate mount_id)
        let mount_entry = self.mount_table.create_mount(
            mount_prefix,
            MountKind::External,
            Some(ufs_uri),
            DataIoPolicy::Allow,
            namespace_owner_group_id,
            root_inode_id,
        )?;

        // Update root inode with correct mount_id
        let mut updated_root = root_inode;
        updated_root.mount_id = mount_entry.mount_id;
        self.storage.put_inode(&updated_root)?;

        // Store mount entry to RocksDB
        self.storage.put_mount(&mount_entry)?;

        Ok((mount_entry.mount_id, root_inode_id))
    }

    /// Helper: Create a simple request header for testing.
    pub fn create_test_request_header() -> Option<proto::common::RequestHeaderProto> {
        use common::header::RequestHeader;
        use types::ClientId;

        let header = RequestHeader::new(ClientId::new(1));
        Some((&header).into())
    }

    /// Helper: Extract error code from response header.
    ///
    /// In the new error model, FS errno / RPC code are carried via
    /// `ResponseHeaderProto.error` (ErrorDetailProto) oneof `code`.
    /// For FS tests, we primarily care about FS errno numeric values
    /// (e.g. EXDEV=18, ENOTEMPTY=39).
    pub fn extract_error_code(resp_header: &Option<proto::common::ResponseHeaderProto>) -> Option<u32> {
        use proto::common::error_detail_proto::Code;

        let header = resp_header.as_ref()?;
        let error = header.error.as_ref()?;

        match &error.code {
            Some(Code::FsErrno(errno)) => Some(*errno as u32),
            Some(Code::RpcCode(code)) => Some(*code as u32),
            None => None,
        }
    }

    /// Helper: Extract inode_id from Lookup response.
    pub fn extract_lookup_inode_id(resp: &proto::metadata::LookupResponseProto) -> Option<InodeId> {
        resp.inode
            .as_ref()
            .and_then(|i| i.inode_id.as_ref())
            .map(|id| InodeId::new(id.value))
    }
}
