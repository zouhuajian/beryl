// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Maintenance subsystem entrypoint.
//!
//! The public surface is intentionally small. Repair, delete, GC, orphan,
//! over-replica cleanup, lost-worker cleanup, and destructive gates are
//! maintenance-owned implementation details wired by `MaintenanceService`.

pub(crate) mod delete;
mod gate;
mod gc;
mod lease_cleanup;
mod lost_worker;
mod orphan;
mod overrep;
pub mod repair;
mod service;

pub use service::{MaintenanceHandle, MaintenanceService};

use crate::error::{MetadataError, MetadataResult};
use crate::mount::MountTable;
use crate::raft::RocksDBStorage;
use types::ids::{BlockId, ShardGroupId};

/// Resolve the owner group for a block by looking up its inode and mount entry.
/// Uses the authoritative data_handle_id -> inode_id mapping persisted in metadata.
pub(crate) fn owner_group_for_block(
    storage: &RocksDBStorage,
    mount_table: &MountTable,
    block_id: BlockId,
) -> MetadataResult<ShardGroupId> {
    let inode_id = storage.validate_data_handle_owner(block_id.data_handle_id, None)?;
    let inode = storage.get_inode(inode_id)?.ok_or_else(|| {
        MetadataError::StaleState(format!(
            "Inode {} not found for block {}; client must refresh state",
            inode_id, block_id
        ))
    })?;
    let mount = mount_table.get_mount(inode.mount_id)?.ok_or_else(|| {
        MetadataError::StaleState(format!(
            "Mount {:?} not found for inode {}; client must refresh state",
            inode.mount_id, inode_id
        ))
    })?;
    Ok(mount.namespace_owner_group_id)
}

#[cfg(test)]
mod tests {
    use super::owner_group_for_block;
    use crate::mount::MountTable;
    use crate::raft::RocksDBStorage;
    use tempfile::TempDir;
    use types::fs::{FileAttrs, Inode, InodeId, InodeKind};
    use types::ids::{BlockId, DataHandleId, MountId, ShardGroupId};
    use types::BlockIndex;

    #[test]
    fn owner_group_errors_when_inode_missing() {
        let dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::open(dir.path()).unwrap();
        let mount_table = MountTable::new();
        let block_id = BlockId::new(DataHandleId::new(42), BlockIndex::new(0));
        storage
            .put_data_handle_owner(block_id.data_handle_id, InodeId::new(42))
            .unwrap();
        let err = owner_group_for_block(&storage, &mount_table, block_id).unwrap_err();
        assert!(format!("{err:?}").contains("Inode"));
    }

    #[test]
    fn owner_group_resolves_mount_owner() {
        let dir = TempDir::new().unwrap();
        let storage = RocksDBStorage::open(dir.path()).unwrap();
        let mount_table = MountTable::new();
        let mount_id = MountId::new(1);
        let root_inode_id = InodeId::new(100);
        let root_data_handle_id = DataHandleId::new(4_200);
        let attrs = FileAttrs::new();
        let inode = Inode::new(
            root_inode_id,
            InodeKind::Dir,
            attrs.clone(),
            mount_id,
            root_data_handle_id,
        );
        storage.put_inode(&inode).unwrap();
        storage
            .put_data_handle_owner(root_data_handle_id, root_inode_id)
            .unwrap();

        let owner_group = ShardGroupId::new(7);
        let mount_entry = mount_table
            .create_mount(
                "/mnt/test".to_string(),
                crate::mount::MountKind::External,
                Some("ufs://test".to_string()),
                crate::mount::DataIoPolicy::Allow,
                owner_group,
                root_inode_id,
            )
            .unwrap();

        // Update inode with the real mount_id assigned by mount_table.
        let mut fixed_inode = inode;
        fixed_inode.mount_id = mount_entry.mount_id;
        storage.put_inode(&fixed_inode).unwrap();

        let block_id = BlockId::new(root_data_handle_id, BlockIndex::new(0));
        let resolved = owner_group_for_block(&storage, &mount_table, block_id).unwrap();
        assert_eq!(resolved, owner_group);
    }
}
