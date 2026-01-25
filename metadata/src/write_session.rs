// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Write session management for the data plane.
//!
//! WriteSession is a runtime-only structure (not persisted to Raft).
//! It tracks open write sessions and is cleaned up on CloseWrite or TTL expiry.

use parking_lot::RwLock;
use proto::metadata::WriteTargetProto;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use types::fs::Extent;
use types::fs::InodeId;
use types::ids::{ClientId, LeaseId, MountId};
use types::lease::FencingToken;

/// Write session (runtime-only, not persisted to Raft).
#[derive(Clone, Debug)]
pub struct WriteSession {
    /// Inode ID being written.
    pub inode_id: InodeId,
    /// Mount ID.
    pub mount_id: MountId,
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
    pub mode: crate::inode_lease::WriteMode,
    /// Pending extents (accumulated before close).
    pub pending_extents: Vec<Extent>,
    /// Pending size (accumulated before close).
    pub pending_size: u64,
    /// Write targets (worker endpoints) for barrier.
    pub write_targets: Vec<WriteTargetProto>,
    /// Writer identity (client_id / call_id).
    pub writer_identity: WriterIdentity,
    /// Created timestamp (for TTL cleanup).
    pub created_at_ms: u64,
    /// Last observed written length (for fsync target size).
    pub last_written: u64,
}

/// Writer identity (client + call for tracking).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriterIdentity {
    pub client_id: ClientId,
    pub call_id: types::CallId,
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
    pub fn create_session(
        &self,
        inode_id: InodeId,
        mount_id: MountId,
        lease_id: LeaseId,
        lease_epoch: u64,
        fencing_token: FencingToken,
        open_epoch: u64,
        base_size: u64,
        mode: crate::inode_lease::WriteMode,
        write_targets: Vec<proto::metadata::WriteTargetProto>,
        writer_identity: WriterIdentity,
    ) -> u64 {
        let mut next_id = self.next_file_handle.write();
        let file_handle = *next_id;
        *next_id += 1;

        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;

        let session = WriteSession {
            inode_id,
            mount_id,
            lease_id,
            lease_epoch,
            fencing_token,
            open_epoch,
            base_size,
            mode,
            pending_extents: Vec::new(),
            pending_size: 0,
            write_targets,
            writer_identity,
            created_at_ms: now_ms,
            last_written: base_size,
        };

        self.sessions.write().insert(file_handle, session);
        file_handle
    }

    /// Update the last_written watermark for a session.
    pub fn set_last_written(&self, file_handle: u64, written: u64) -> bool {
        let mut sessions = self.sessions.write();
        if let Some(session) = sessions.get_mut(&file_handle) {
            session.last_written = written.max(session.last_written);
            true
        } else {
            false
        }
    }

    /// Get a write session by file handle.
    pub fn get_session(&self, file_handle: u64) -> Option<WriteSession> {
        self.sessions.read().get(&file_handle).cloned()
    }

    /// Remove a write session (on close or error).
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
        Self::new(3600_000) // 1 hour default TTL
    }
}
