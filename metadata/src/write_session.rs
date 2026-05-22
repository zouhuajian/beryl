// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Write session management for the data plane.
//!
//! WriteSession is a runtime-only structure (not persisted to Raft).
//! It tracks internal write sessions and is cleaned up on CommitFile, AbortFileWrite, or TTL expiry.

use crate::inode_lease::WriteMode;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use types::fs::Extent;
use types::fs::InodeId;
use types::ids::{ClientId, DataHandleId, LeaseId, MountId};
use types::lease::FencingToken;
use types::WriteTarget;

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
    /// Pending extents (accumulated before close).
    pub pending_extents: Vec<Extent>,
    /// Pending size (accumulated before close).
    pub pending_size: u64,
    /// Precomputed write targets for AddBlock.
    pub write_targets: Vec<WriteTarget>,
    /// Targets already issued to the client through AddBlock.
    pub issued_targets: Vec<WriteTarget>,
    /// Next write target to hand out through AddBlock.
    pub next_target_index: usize,
    /// Writer identity (client_id / call_id).
    pub writer_identity: WriterIdentity,
    /// Created timestamp (for TTL cleanup).
    pub created_at_ms: u64,
}

/// Writer identity (client + call for tracking).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriterIdentity {
    pub client_id: ClientId,
    pub call_id: types::CallId,
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
    pub writer_identity: WriterIdentity,
}

/// Write session manager (in-memory, leader-only).
pub struct WriteSessionManager {
    /// Active write sessions: file_handle -> WriteSession.
    sessions: Arc<RwLock<HashMap<u64, WriteSession>>>,
    /// Next file handle ID.
    next_file_handle: Arc<RwLock<u64>>,
    /// Session TTL in milliseconds (default: 1 hour).
    session_ttl_ms: u64,
}

impl WriteSessionManager {
    /// Create a new WriteSessionManager.
    pub fn new(session_ttl_ms: u64) -> Self {
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            next_file_handle: Arc::new(RwLock::new(1)),
            session_ttl_ms,
        }
    }

    /// Create a new write session.
    pub fn create_session(&self, input: CreateSessionInput) -> u64 {
        let mut next_id = self.next_file_handle.write();
        let file_handle = *next_id;
        *next_id += 1;

        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

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
            pending_extents: Vec::new(),
            pending_size: 0,
            write_targets: input.write_targets,
            issued_targets: Vec::new(),
            next_target_index: 0,
            writer_identity: input.writer_identity,
            created_at_ms: now_ms,
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
            .and_then(|issued| issued.file_offset.checked_add(issued.len))
            .unwrap_or(session.base_size);
        target.file_offset = next_file_offset;
        if let Some(len) = desired_len {
            target.len = len.min(target.len).max(1);
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

    /// Clean up expired sessions (should be called periodically).
    pub fn cleanup_expired(&self) -> usize {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        let mut sessions = self.sessions.write();
        let expired: Vec<u64> = sessions
            .iter()
            .filter(|(_, session)| now_ms - session.created_at_ms > self.session_ttl_ms)
            .map(|(handle, _)| *handle)
            .collect();

        for handle in &expired {
            sessions.remove(handle);
        }

        expired.len()
    }

    /// Get all sessions for an inode (for conflict detection).
    pub fn get_sessions_for_inode(&self, inode_id: InodeId) -> Vec<u64> {
        self.sessions
            .read()
            .iter()
            .filter(|(_, session)| session.inode_id == inode_id)
            .map(|(handle, _)| *handle)
            .collect()
    }

    /// Check if an inode has an active write session.
    pub fn has_active_session(&self, inode_id: InodeId) -> bool {
        self.sessions
            .read()
            .values()
            .any(|session| session.inode_id == inode_id)
    }
}

impl Default for WriteSessionManager {
    fn default() -> Self {
        Self::new(3_600_000) // 1 hour default TTL
    }
}
