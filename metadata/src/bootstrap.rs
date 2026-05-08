// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata bootstrap helpers.

use crate::error::{MetadataError, MetadataResult};
use crate::mount::{DataIoPolicy, MountKind, MountTable, ROOT_INODE_ID, ROOT_MOUNT_PREFIX};
use crate::raft::{AppRaftNode, Command, DedupKey};
use std::sync::Arc;
use tracing::warn;
use types::ids::ShardGroupId;

/// Ensure the root mount exists and is durable.
pub async fn ensure_root_mount(
    raft_node: Arc<AppRaftNode>,
    mount_table: Arc<MountTable>,
    namespace_owner_group_id: ShardGroupId,
) -> MetadataResult<()> {
    if let Some(existing) = mount_table
        .list_mounts()
        .into_iter()
        .find(|entry| entry.mount_prefix == ROOT_MOUNT_PREFIX)
    {
        if existing.root_inode_id != ROOT_INODE_ID {
            return Err(MetadataError::InvalidArgument(format!(
                "root inode invariant violated: expected inode_id={}, got {}. storage must be migrated or wiped",
                ROOT_INODE_ID.as_raw(),
                existing.root_inode_id.as_raw()
            )));
        }
        if existing.mount_kind != MountKind::Internal
            || existing.ufs_uri.is_some()
            || existing.data_io_policy != DataIoPolicy::Forbid
        {
            return Err(MetadataError::InvalidArgument(
                "root mount exists but violates internal/no-ufs/forbid-data-io invariants".to_string(),
            ));
        }
        return Ok(());
    }

    if !raft_node.is_leader() {
        return Ok(());
    }

    let mount_id = mount_table.allocate_mount_id();
    let command = Command::CreateMount {
        dedup: DedupKey::system(),
        mount_id,
        mount_prefix: ROOT_MOUNT_PREFIX.to_string(),
        mount_kind: MountKind::Internal,
        ufs_uri: None,
        data_io_policy: DataIoPolicy::Forbid,
        namespace_owner_group_id,
        root_inode_id: ROOT_INODE_ID,
    };

    match raft_node.propose(command).await {
        Ok(_) => Ok(()),
        Err(MetadataError::LeaderChanged(msg)) => {
            warn!(error = %msg, "Root mount ensure deferred to leader");
            Ok(())
        }
        Err(err) => Err(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RaftConfig;
    use crate::mount::MountTable;
    use crate::raft::{AppRaftNode, AppRaftStateMachine, RocksDBStorage};
    use crate::readiness::{wait_for_root_ready, RootReadinessConfig, RootReadinessGate};
    use tempfile::TempDir;
    use types::fs::InodeId;
    use types::ids::MountId;

    #[tokio::test]
    async fn bootstrap_root_mount_exists() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let mount_table = Arc::new(MountTable::load_from_storage(&storage).unwrap());
        let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table)));

        let raft_config = RaftConfig {
            node_id: 1,
            peers: vec!["127.0.0.1:0".to_string()],
        };
        let raft_node = Arc::new(
            AppRaftNode::new(1, Arc::clone(&storage), Arc::clone(&state_machine), &raft_config)
                .await
                .unwrap(),
        );

        ensure_root_mount(Arc::clone(&raft_node), Arc::clone(&mount_table), ShardGroupId::new(1))
            .await
            .unwrap();
        ensure_root_mount(Arc::clone(&raft_node), Arc::clone(&mount_table), ShardGroupId::new(1))
            .await
            .unwrap();

        let readiness_gate = Arc::new(RootReadinessGate::new(None));
        let readiness_config = RootReadinessConfig {
            initial_backoff_ms: 10,
            max_backoff_ms: 50,
            warn_after_ms: 200,
        };
        wait_for_root_ready(
            Arc::clone(&raft_node),
            Arc::clone(&mount_table),
            ShardGroupId::new(1),
            Arc::clone(&readiness_gate),
            readiness_config,
        )
        .await
        .unwrap();

        let root = mount_table
            .list_mounts()
            .into_iter()
            .find(|entry| entry.mount_prefix == ROOT_MOUNT_PREFIX)
            .expect("root mount should exist");
        assert_eq!(root.mount_kind, MountKind::Internal);
        assert!(root.ufs_uri.is_none());
        assert_eq!(root.data_io_policy, DataIoPolicy::Forbid);

        let reloaded = MountTable::load_from_storage(&storage).unwrap();
        let root_reload = reloaded
            .list_mounts()
            .into_iter()
            .find(|entry| entry.mount_prefix == ROOT_MOUNT_PREFIX)
            .expect("root mount should persist");
        assert_eq!(root_reload.mount_kind, MountKind::Internal);
        assert!(root_reload.ufs_uri.is_none());
        assert_eq!(root_reload.data_io_policy, DataIoPolicy::Forbid);

        let root_inode = storage.get_inode(ROOT_INODE_ID).unwrap();
        assert!(root_inode.is_some(), "root inode should exist");
    }

    #[tokio::test]
    async fn bootstrap_rejects_root_inode_mismatch() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
        let bad_root_inode = InodeId::new(2);
        let entry = crate::mount::MountEntry {
            mount_id: MountId::new(1),
            mount_prefix: ROOT_MOUNT_PREFIX.to_string(),
            mount_kind: MountKind::Internal,
            ufs_uri: None,
            data_io_policy: DataIoPolicy::Forbid,
            mount_version: 1,
            namespace_owner_group_id: ShardGroupId::new(1),
            root_inode_id: bad_root_inode,
        };
        storage.put_mount(&entry).unwrap();

        let mount_table = Arc::new(MountTable::load_from_storage(&storage).unwrap());
        let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table)));

        let raft_config = RaftConfig {
            node_id: 1,
            peers: vec!["127.0.0.1:0".to_string()],
        };
        let raft_node = Arc::new(
            AppRaftNode::new(1, Arc::clone(&storage), Arc::clone(&state_machine), &raft_config)
                .await
                .unwrap(),
        );

        let err = ensure_root_mount(raft_node, mount_table, ShardGroupId::new(1))
            .await
            .unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("root inode invariant violated"));
    }
}
