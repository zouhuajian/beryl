// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Runtime registry for write-session handles.
//!
//! Sessions are leader-local and are normally removed on CommitFile or AbortFileWrite.
//! LeaseManager is the authority for whether a write is still active; this
//! registry only stores handle state needed to continue an admitted write.

use crate::inode_lease::{LeaseManager, WriteMode};
use parking_lot::RwLock;
use std::collections::HashMap;
use types::fs::InodeId;
use types::ids::{DataHandleId, LeaseId, MountId};
use types::lease::FencingToken;
use types::{BlockShape, WriteTarget};

/// Write session (runtime-only, not persisted to Raft).
#[derive(Clone, Debug)]
pub struct WriteSession {
    /// Inode ID being written.
    pub inode_id: InodeId,
    /// Mount ID.
    pub mount_id: MountId,
    /// Data handle used by this write session.
    pub data_handle_id: DataHandleId,
    /// Lease ID / fencing token for this write session.
    pub lease_id: LeaseId,
    /// Lease epoch (for fencing validation).
    pub lease_epoch: u64,
    /// Fencing token (for worker validation).
    pub fencing_token: FencingToken,
    /// Open epoch (for idempotency and replay protection).
    pub open_epoch: u64,
    /// Base file size at open time (for append-only validation).
    pub base_size: u64,
    /// Write mode (WRITE or APPEND).
    pub mode: WriteMode,
    /// Precomputed write targets for AddBlock.
    pub write_targets: Vec<WriteTarget>,
    /// Targets already issued to the client through AddBlock.
    pub issued_targets: Vec<WriteTarget>,
    /// Next write target to hand out through AddBlock.
    pub next_target_index: usize,
}

/// Inputs needed to create a runtime write session.
pub struct CreateSessionInput {
    pub inode_id: InodeId,
    pub mount_id: MountId,
    pub data_handle_id: DataHandleId,
    pub lease_id: LeaseId,
    pub lease_epoch: u64,
    pub fencing_token: FencingToken,
    pub open_epoch: u64,
    pub base_size: u64,
    pub mode: WriteMode,
    pub write_targets: Vec<WriteTarget>,
}

/// In-memory, leader-local registry of write-session handles.
pub struct SessionRegistry {
    /// Write sessions: file_handle -> WriteSession.
    sessions: RwLock<HashMap<u64, WriteSession>>,
    /// Next file handle ID.
    next_file_handle: RwLock<u64>,
}

impl SessionRegistry {
    /// Create a new write session.
    pub fn create_session(&self, input: CreateSessionInput) -> u64 {
        let mut next_id = self.next_file_handle.write();
        let file_handle = *next_id;
        *next_id += 1;

        let session = WriteSession {
            inode_id: input.inode_id,
            mount_id: input.mount_id,
            data_handle_id: input.data_handle_id,
            lease_id: input.lease_id,
            lease_epoch: input.lease_epoch,
            fencing_token: input.fencing_token,
            open_epoch: input.open_epoch,
            base_size: input.base_size,
            mode: input.mode,
            write_targets: input.write_targets,
            issued_targets: Vec::new(),
            next_target_index: 0,
        };

        self.sessions.write().insert(file_handle, session);
        file_handle
    }

    /// Allocate the next precomputed write target for a session.
    pub fn allocate_target(&self, file_handle: u64, desired_len: Option<u64>) -> Option<WriteTarget> {
        let mut sessions = self.sessions.write();
        let session = sessions.get_mut(&file_handle)?;
        let mut target = session.write_targets.get(session.next_target_index).cloned()?;
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
            return None;
        }
        session.next_target_index += 1;
        session.issued_targets.push(target.clone());
        Some(target)
    }

    /// Get a write session by file handle.
    pub fn get_session(&self, file_handle: u64) -> Option<WriteSession> {
        self.sessions.read().get(&file_handle).cloned()
    }

    /// Remove a write session (on commit, abort, or error).
    pub fn remove_session(&self, file_handle: u64) -> Option<WriteSession> {
        self.sessions.write().remove(&file_handle)
    }

    /// Remove handles for an inode whose lease is no longer current.
    pub fn remove_inactive_for_inode(&self, inode_id: InodeId, lease_manager: &LeaseManager) -> usize {
        let mut sessions = self.sessions.write();
        let previous_len = sessions.len();
        sessions.retain(|_, session| {
            session.inode_id != inode_id
                || lease_manager.is_active_lease(session.inode_id, session.lease_id, session.lease_epoch)
        });
        previous_len - sessions.len()
    }
}

impl Default for SessionRegistry {
    fn default() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            next_file_handle: RwLock::new(1),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use types::ids::{BlockId, BlockIndex, ClientId};

    fn create_input(inode_id: InodeId) -> CreateSessionInput {
        let data_handle_id = DataHandleId::new(inode_id.as_raw());
        CreateSessionInput {
            inode_id,
            mount_id: MountId::new(1),
            data_handle_id,
            lease_id: LeaseId::new(inode_id.as_raw().into()),
            lease_epoch: 1,
            fencing_token: FencingToken {
                block_id: BlockId::new(data_handle_id, BlockIndex::new(0)),
                owner: ClientId::new(1),
                epoch: 1,
            },
            open_epoch: 1,
            base_size: 0,
            mode: WriteMode::Write,
            write_targets: Vec::new(),
        }
    }

    #[test]
    fn create_get_and_remove_session() {
        let registry = SessionRegistry::default();
        let inode_id = InodeId::new(7);

        let handle = registry.create_session(create_input(inode_id));

        assert_eq!(
            registry.get_session(handle).map(|session| session.inode_id),
            Some(inode_id)
        );
        assert_eq!(
            registry.remove_session(handle).map(|session| session.inode_id),
            Some(inode_id)
        );
        assert!(registry.get_session(handle).is_none());
    }

    #[test]
    fn concurrent_session_creation_allocates_unique_handles() {
        let registry = Arc::new(SessionRegistry::default());
        let workers = (0..8)
            .map(|worker| {
                let registry = Arc::clone(&registry);
                std::thread::spawn(move || {
                    (0..32)
                        .map(|index| {
                            let inode_id = InodeId::new(1 + worker * 32 + index);
                            registry.create_session(create_input(inode_id))
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect::<Vec<_>>();

        let mut handles = workers
            .into_iter()
            .flat_map(|worker| worker.join().expect("session creator must not panic"))
            .collect::<Vec<_>>();
        handles.sort_unstable();
        handles.dedup();

        assert_eq!(handles.len(), 8 * 32);
    }
}
