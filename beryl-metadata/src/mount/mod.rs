// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Root namespace authority published from durable metadata state.

use crate::error::{MetadataError, MetadataResult};
use crate::raft::RocksDBStorage;
use beryl_types::ids::{InodeId, MountId};
use beryl_types::GroupName;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

pub const ROOT_INODE_ID: InodeId = InodeId::new(1);
pub const ROOT_MOUNT_ID: MountId = MountId::new(1);

/// Durable ownership and freshness of the unified writable namespace.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MountEntry {
    pub mount_id: MountId,
    pub namespace_owner_group_name: GroupName,
    pub root_inode_id: InodeId,
}

impl MountEntry {
    pub(crate) fn validate_root(&self) -> MetadataResult<()> {
        if self.mount_id != ROOT_MOUNT_ID || self.root_inode_id != ROOT_INODE_ID {
            return Err(MetadataError::Internal("invalid root namespace authority".into()));
        }
        Ok(())
    }
}

/// Published root authority; empty only before namespace bootstrap.
#[derive(Default)]
pub struct MountTable {
    root: RwLock<Option<MountEntry>>,
}

impl MountTable {
    pub(crate) fn load_from_storage(storage: &RocksDBStorage) -> MetadataResult<Self> {
        Ok(Self {
            root: RwLock::new(storage.get_root_mount()?),
        })
    }

    /// Publish committed root authority before exposing its applied index.
    pub(crate) fn upsert(&self, entry: MountEntry) {
        *self.root.write() = Some(entry);
    }

    pub(crate) fn root(&self) -> Option<MountEntry> {
        self.root.read().clone()
    }

    pub fn get_mount(&self, mount_id: MountId) -> Option<MountEntry> {
        self.root
            .read()
            .as_ref()
            .filter(|entry| entry.mount_id == mount_id)
            .cloned()
    }
}
