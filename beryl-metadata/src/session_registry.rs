// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Runtime registry for write-session handles.
//!
//! Sessions are leader-local and are normally removed on CommitFile or AbortFileWrite.
//! LeaseManager is the authority for whether a write is still active; this
//! registry only stores handle state needed to continue an admitted write.

use crate::inode_lease::{LeaseManager, WriteMode};
use beryl_types::fs::InodeId;
use beryl_types::ids::{DataHandleId, MountId};
use beryl_types::{BlockId, BlockShape, ClientId, FileLayout, WriteTarget};
use parking_lot::RwLock;
use std::collections::HashMap;

/// Write session (runtime-only, not persisted to Raft).
#[derive(Clone, Debug)]
pub struct WriteSession {
    /// Inode ID being written.
    pub inode_id: InodeId,
    /// Mount ID.
    pub mount_id: MountId,
    /// Data handle used by this write session.
    pub data_handle_id: DataHandleId,
    /// Lease epoch (for fencing validation).
    pub lease_epoch: u64,
    /// Base file size at open time (for append-only validation).
    pub base_size: u64,
    /// Last durable content revision observed by this session.
    pub content_revision: u64,
    /// Write mode (WRITE or APPEND).
    pub mode: WriteMode,
    /// Client that owns the OpenWrite call.
    pub open_client_id: ClientId,
    /// Layout returned by OpenWrite.
    pub layout: FileLayout,
    /// Exact lease expiry returned by OpenWrite.
    pub expires_at_ms: u64,
    /// Precomputed write targets for AddBlock.
    pub write_targets: Vec<WriteTarget>,
    /// Targets already issued to the client through AddBlock.
    pub issued_targets: Vec<WriteTarget>,
    /// Logical AddBlock steps issued for predecessor-based replay.
    issued_steps: Vec<IssuedTarget>,
    /// Next write target to hand out through AddBlock.
    pub next_target_index: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use beryl_types::ids::BlockIndex;
    use beryl_types::lease::FencingToken;
    use beryl_types::{BlockFormatId, Tier};

    fn write_target(data_handle_id: DataHandleId, index: u32) -> WriteTarget {
        let block_id = BlockId::new(data_handle_id, BlockIndex::new(index));
        WriteTarget {
            block_id,
            file_offset: 0,
            block_size: 64,
            effective_len: 64,
            worker_endpoints: Vec::new(),
            fencing_token: FencingToken {
                block_id,
                owner: ClientId::new(1),
                epoch: 1,
            },
            block_stamp: 1,
            chunk_size: 64,
            block_format_id: BlockFormatId::CURRENT_FOR_NEW_FILE,
            tier: Tier::Hdd,
        }
    }

    fn create_input(data_handle_id: DataHandleId) -> CreateSessionInput {
        CreateSessionInput {
            inode_id: InodeId::new(data_handle_id.as_raw()),
            mount_id: MountId::new(1),
            data_handle_id,
            lease_epoch: 7,
            base_size: 0,
            content_revision: 0,
            mode: WriteMode::Write,
            open_client_id: ClientId::new(1),
            layout: FileLayout::new(64, 64, 1),
            expires_at_ms: 1_000,
            write_targets: vec![write_target(data_handle_id, 0), write_target(data_handle_id, 1)],
        }
    }

    #[test]
    fn one_data_handle_has_at_most_one_active_session() {
        let registry = SessionRegistry::default();
        let data_handle_id = DataHandleId::new(10);
        registry.create_session(create_input(data_handle_id)).unwrap();

        assert!(registry.create_session(create_input(data_handle_id)).is_err());
        assert_eq!(registry.get_session(data_handle_id).unwrap().lease_epoch, 7);
        assert!(registry.remove_session_if_epoch(data_handle_id, 7).is_some());
        assert!(registry.get_session(data_handle_id).is_none());
    }

    #[test]
    fn delayed_cleanup_cannot_remove_a_newer_session() {
        let registry = SessionRegistry::default();
        let data_handle_id = DataHandleId::new(20);
        registry.create_session(create_input(data_handle_id)).unwrap();
        registry.remove_session_if_epoch(data_handle_id, 7).unwrap();
        let mut replacement = create_input(data_handle_id);
        replacement.lease_epoch = 8;
        registry.create_session(replacement).unwrap();

        assert!(registry.remove_session_if_epoch(data_handle_id, 7).is_none());
        assert_eq!(registry.get_session(data_handle_id).unwrap().lease_epoch, 8);
    }

    #[test]
    fn add_block_replays_by_predecessor_without_advancing() {
        let registry = SessionRegistry::default();
        let data_handle_id = DataHandleId::new(11);
        registry.create_session(create_input(data_handle_id)).unwrap();

        let first = registry.allocate_target(data_handle_id, 7, None, Some(32)).unwrap();
        let replay = registry.allocate_target(data_handle_id, 7, None, Some(32)).unwrap();
        assert_eq!(replay, first);
        assert_eq!(registry.get_session(data_handle_id).unwrap().next_target_index, 1);

        let second = registry
            .allocate_target(data_handle_id, 7, Some(first.block_id), Some(64))
            .unwrap();
        assert_eq!(second.block_id.index, BlockIndex::new(1));
        assert_eq!(second.file_offset, 32);
    }

    #[test]
    fn add_block_rejects_payload_drift_and_stale_lease_epoch() {
        let registry = SessionRegistry::default();
        let data_handle_id = DataHandleId::new(12);
        registry.create_session(create_input(data_handle_id)).unwrap();
        registry.allocate_target(data_handle_id, 7, None, Some(32)).unwrap();

        assert!(registry.allocate_target(data_handle_id, 7, None, Some(64)).is_err());
        assert!(registry.allocate_target(data_handle_id, 6, None, Some(32)).is_err());
        assert_eq!(registry.get_session(data_handle_id).unwrap().next_target_index, 1);
    }

    #[test]
    fn add_block_rejects_a_gap_in_the_predecessor_chain() {
        let registry = SessionRegistry::default();
        let data_handle_id = DataHandleId::new(13);
        registry.create_session(create_input(data_handle_id)).unwrap();
        let unknown = BlockId::new(data_handle_id, BlockIndex::new(99));

        assert!(registry
            .allocate_target(data_handle_id, 7, Some(unknown), Some(32))
            .unwrap_err()
            .contains("predecessor mismatch"));
        assert_eq!(registry.get_session(data_handle_id).unwrap().next_target_index, 0);
    }

    #[test]
    fn new_target_uses_next_content_revision_while_replay_keeps_original_stamp() {
        let registry = SessionRegistry::default();
        let data_handle_id = DataHandleId::new(14);
        registry.create_session(create_input(data_handle_id)).unwrap();

        let first = registry.allocate_target(data_handle_id, 7, None, Some(32)).unwrap();
        assert_eq!(first.block_stamp, 1);
        registry
            .update_published_state(data_handle_id, 7, 1, 32)
            .expect("advance published state");

        let replay = registry.allocate_target(data_handle_id, 7, None, Some(32)).unwrap();
        assert_eq!(replay, first);
        let second = registry
            .allocate_target(data_handle_id, 7, Some(first.block_id), Some(32))
            .unwrap();
        assert_eq!(second.block_stamp, 2);
        assert_eq!(second.file_offset, 32);
    }
}

/// Inputs needed to create a runtime write session.
#[derive(Clone)]
pub struct CreateSessionInput {
    pub inode_id: InodeId,
    pub mount_id: MountId,
    pub data_handle_id: DataHandleId,
    pub lease_epoch: u64,
    pub base_size: u64,
    pub content_revision: u64,
    pub mode: WriteMode,
    pub open_client_id: ClientId,
    pub layout: FileLayout,
    pub expires_at_ms: u64,
    pub write_targets: Vec<WriteTarget>,
}

#[derive(Clone, Debug)]
struct IssuedTarget {
    previous_block_id: Option<BlockId>,
    desired_len: Option<u64>,
    target: WriteTarget,
}

/// In-memory, leader-local registry of write-session handles.
pub struct SessionRegistry {
    /// At most one active session exists for one data handle.
    sessions: RwLock<HashMap<DataHandleId, WriteSession>>,
}

impl SessionRegistry {
    /// Create one leader-local session for a data handle.
    pub fn create_session(&self, input: CreateSessionInput) -> Result<WriteSession, String> {
        let mut sessions = self.sessions.write();
        if sessions.contains_key(&input.data_handle_id) {
            return Err("data handle already has an active write session".to_string());
        }

        let session = WriteSession {
            inode_id: input.inode_id,
            mount_id: input.mount_id,
            data_handle_id: input.data_handle_id,
            lease_epoch: input.lease_epoch,
            base_size: input.base_size,
            content_revision: input.content_revision,
            mode: input.mode,
            open_client_id: input.open_client_id,
            layout: input.layout,
            expires_at_ms: input.expires_at_ms,
            write_targets: input.write_targets,
            issued_targets: Vec::new(),
            issued_steps: Vec::new(),
            next_target_index: 0,
        };

        sessions.insert(input.data_handle_id, session.clone());
        Ok(session)
    }

    /// Allocate or replay one predecessor-addressed AddBlock step.
    pub fn allocate_target(
        &self,
        data_handle_id: DataHandleId,
        lease_epoch: u64,
        previous_block_id: Option<BlockId>,
        desired_len: Option<u64>,
    ) -> Result<WriteTarget, String> {
        let mut sessions = self.sessions.write();
        let session = sessions
            .get_mut(&data_handle_id)
            .ok_or_else(|| "write session not found".to_string())?;
        if session.lease_epoch != lease_epoch {
            return Err("write session lease epoch mismatch".to_string());
        }
        if let Some(step) = session
            .issued_steps
            .iter()
            .find(|step| step.previous_block_id == previous_block_id)
        {
            if step.desired_len == desired_len {
                return Ok(step.target.clone());
            }
            return Err("AddBlock predecessor reused with a different desired_len".to_string());
        }

        let expected_previous = session.issued_targets.last().map(|target| target.block_id);
        if previous_block_id != expected_previous {
            return Err(format!(
                "AddBlock predecessor mismatch: expected {expected_previous:?}, got {previous_block_id:?}"
            ));
        }

        let mut target = session
            .write_targets
            .get(session.next_target_index)
            .cloned()
            .ok_or_else(|| "no write target available".to_string())?;
        let next_file_offset = session
            .issued_targets
            .last()
            .and_then(|issued| issued.file_offset.checked_add(issued.effective_len))
            .unwrap_or(session.base_size);
        target.file_offset = next_file_offset;
        if let Some(len) = desired_len {
            target.effective_len = len.min(target.effective_len).max(1);
        }
        if BlockShape::new(
            target.block_format_id,
            target.block_size,
            target.chunk_size,
            target.effective_len,
        )
        .is_err()
        {
            return Err("invalid write target shape".to_string());
        }
        target.block_stamp = session
            .content_revision
            .checked_add(1)
            .ok_or_else(|| "content revision overflow".to_string())?;
        session.next_target_index += 1;
        session.issued_targets.push(target.clone());
        session.issued_steps.push(IssuedTarget {
            previous_block_id,
            desired_len,
            target: target.clone(),
        });
        Ok(target)
    }

    /// Get a write session by data handle.
    pub fn get_session(&self, data_handle_id: DataHandleId) -> Option<WriteSession> {
        self.sessions.read().get(&data_handle_id).cloned()
    }

    /// Remove only the session identified by the presented lease epoch.
    pub fn remove_session_if_epoch(&self, data_handle_id: DataHandleId, lease_epoch: u64) -> Option<WriteSession> {
        let mut sessions = self.sessions.write();
        if sessions
            .get(&data_handle_id)
            .is_none_or(|session| session.lease_epoch != lease_epoch)
        {
            return None;
        }
        sessions.remove(&data_handle_id)
    }

    pub fn update_published_state(
        &self,
        data_handle_id: DataHandleId,
        lease_epoch: u64,
        content_revision: u64,
        file_size: u64,
    ) -> Result<(), String> {
        let mut sessions = self.sessions.write();
        let session = sessions
            .get_mut(&data_handle_id)
            .ok_or_else(|| "write session not found".to_string())?;
        if session.lease_epoch != lease_epoch {
            return Err("write session lease epoch mismatch".to_string());
        }
        session.content_revision = content_revision;
        session.base_size = file_size;
        Ok(())
    }

    /// Remove handles for an inode whose lease is no longer current.
    pub fn remove_inactive_for_inode(&self, inode_id: InodeId, lease_manager: &LeaseManager) -> usize {
        let mut sessions = self.sessions.write();
        let previous_len = sessions.len();
        let removed = sessions
            .extract_if(|_, session| {
                session.inode_id == inode_id && !lease_manager.is_active_lease(session.inode_id, session.lease_epoch)
            })
            .count();
        let current_len = sessions.len();
        debug_assert_eq!(previous_len - current_len, removed);
        previous_len - current_len
    }
}

impl Default for SessionRegistry {
    fn default() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
        }
    }
}
