// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Runtime registry for write-session handles.
//!
//! Sessions are leader-local and are normally removed on CommitFile or AbortFileWrite.
//! LeaseManager is the authority for whether a write is still active; this
//! registry only stores handle state needed to continue an admitted write.

use crate::inode_lease::{LeaseManager, WriteMode};
use beryl_types::fs::InodeId;
use beryl_types::ids::{DataHandleId, LeaseId, MountId};
use beryl_types::lease::FencingToken;
use beryl_types::{BlockId, BlockShape, CallId, ClientId, FileLayout, WriteTarget};
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};

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
    /// Client that owns the OpenWrite call.
    pub open_client_id: ClientId,
    /// Call ID that created this session in the current metadata incarnation.
    pub open_call_id: CallId,
    /// Canonical namespace path from the OpenWrite request.
    pub open_path: String,
    /// Canonical desired length from OpenWrite.
    pub open_desired_len: Option<u64>,
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

/// Inputs needed to create a runtime write session.
#[derive(Clone)]
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
    pub open_client_id: ClientId,
    pub open_call_id: CallId,
    pub open_path: String,
    pub open_desired_len: Option<u64>,
    pub layout: FileLayout,
    pub expires_at_ms: u64,
    pub write_targets: Vec<WriteTarget>,
}

#[derive(Clone, Debug)]
struct IssuedTarget {
    client_id: ClientId,
    call_id: CallId,
    previous_block_id: Option<BlockId>,
    desired_len: Option<u64>,
    target: WriteTarget,
}

/// Exact successful AbortFileWrite payload retained for this leader incarnation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AbortCallPayload {
    pub(crate) file_handle: u64,
    pub(crate) lease_id: Option<LeaseId>,
    pub(crate) lease_epoch: u64,
    pub(crate) open_epoch: u64,
    pub(crate) fencing_block_id: Option<BlockId>,
    pub(crate) fencing_owner: Option<ClientId>,
    pub(crate) fencing_epoch: Option<u64>,
}

/// In-memory, leader-local registry of write-session handles.
pub struct SessionRegistry {
    /// Write sessions: file_handle -> WriteSession.
    sessions: RwLock<HashMap<u64, WriteSession>>,
    /// Next file handle ID.
    next_file_handle: RwLock<u64>,
    /// Successful AbortFileWrite calls for ensure-absent replay in this incarnation.
    completed_aborts: RwLock<HashMap<(ClientId, CallId), AbortCallPayload>>,
    /// OpenWrite and AddBlock calls from sessions retired in this incarnation.
    retired_session_calls: RwLock<HashSet<(ClientId, CallId)>>,
}

impl SessionRegistry {
    /// Return an existing OpenWrite result or create it once for the owner call.
    pub fn get_or_create_session(&self, input: CreateSessionInput) -> Result<(u64, WriteSession), String> {
        if self
            .retired_session_calls
            .read()
            .contains(&(input.open_client_id, input.open_call_id))
        {
            return Err("OpenWrite call_id belongs to a retired session; use a new call_id".to_string());
        }
        let mut sessions = self.sessions.write();
        if let Some((handle, session)) = sessions.iter().find(|(_, session)| {
            session.open_client_id == input.open_client_id && session.open_call_id == input.open_call_id
        }) {
            validate_open_replay(session, &input)?;
            return Ok((*handle, session.clone()));
        }

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
            open_client_id: input.open_client_id,
            open_call_id: input.open_call_id,
            open_path: input.open_path,
            open_desired_len: input.open_desired_len,
            layout: input.layout,
            expires_at_ms: input.expires_at_ms,
            write_targets: input.write_targets,
            issued_targets: Vec::new(),
            issued_steps: Vec::new(),
            next_target_index: 0,
        };

        sessions.insert(file_handle, session.clone());
        Ok((file_handle, session))
    }

    /// Look up an OpenWrite result by owner call before performing new side effects.
    pub fn get_open_session(
        &self,
        client_id: ClientId,
        call_id: CallId,
        open_path: &str,
        mode: WriteMode,
        desired_len: Option<u64>,
    ) -> Result<Option<(u64, WriteSession)>, String> {
        if self.retired_session_calls.read().contains(&(client_id, call_id)) {
            return Err("OpenWrite call_id belongs to a retired session; use a new call_id".to_string());
        }
        let sessions = self.sessions.read();
        let Some((handle, session)) = sessions
            .iter()
            .find(|(_, session)| session.open_client_id == client_id && session.open_call_id == call_id)
        else {
            return Ok(None);
        };
        if session.open_path != open_path || session.mode != mode || session.open_desired_len != desired_len {
            return Err("call_id reused with a different OpenWrite payload".to_string());
        }
        Ok(Some((*handle, session.clone())))
    }

    /// Allocate or replay one predecessor-addressed AddBlock step.
    pub fn allocate_target(
        &self,
        file_handle: u64,
        client_id: ClientId,
        call_id: CallId,
        previous_block_id: Option<BlockId>,
        desired_len: Option<u64>,
    ) -> Result<WriteTarget, String> {
        if self.completed_aborts.read().contains_key(&(client_id, call_id)) {
            return Err("AbortFileWrite call_id cannot be reused for AddBlock".to_string());
        }
        if self.retired_session_calls.read().contains(&(client_id, call_id)) {
            return Err("AddBlock call_id belongs to a retired session; use a new call_id".to_string());
        }
        let mut sessions = self.sessions.write();
        if sessions
            .values()
            .any(|session| session.open_client_id == client_id && session.open_call_id == call_id)
        {
            return Err("OpenWrite call_id cannot be reused for AddBlock".to_string());
        }
        if let Some((issued_handle, step)) = sessions.iter().find_map(|(handle, session)| {
            session
                .issued_steps
                .iter()
                .find(|step| step.client_id == client_id && step.call_id == call_id)
                .map(|step| (*handle, step))
        }) {
            if issued_handle == file_handle
                && step.previous_block_id == previous_block_id
                && step.desired_len == desired_len
            {
                return Ok(step.target.clone());
            }
            return Err("call_id reused with a different AddBlock payload or session".to_string());
        }
        let session = sessions
            .get_mut(&file_handle)
            .ok_or_else(|| "write session not found".to_string())?;
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
        session.next_target_index += 1;
        session.issued_targets.push(target.clone());
        session.issued_steps.push(IssuedTarget {
            client_id,
            call_id,
            previous_block_id,
            desired_len,
            target: target.clone(),
        });
        Ok(target)
    }

    /// Get a write session by file handle.
    pub fn get_session(&self, file_handle: u64) -> Option<WriteSession> {
        self.sessions.read().get(&file_handle).cloned()
    }

    /// Return whether an active session already owns this call identity.
    pub(crate) fn has_call_id(&self, client_id: ClientId, call_id: CallId) -> bool {
        self.completed_aborts.read().contains_key(&(client_id, call_id))
            || self.retired_session_calls.read().contains(&(client_id, call_id))
            || self.sessions.read().values().any(|session| {
                (session.open_client_id == client_id && session.open_call_id == call_id)
                    || session
                        .issued_steps
                        .iter()
                        .any(|step| step.client_id == client_id && step.call_id == call_id)
            })
    }

    /// Return an exact completed AbortFileWrite replay or reject payload drift.
    pub(crate) fn replay_completed_abort(
        &self,
        client_id: ClientId,
        call_id: CallId,
        payload: &AbortCallPayload,
    ) -> Result<bool, String> {
        let completed = self.completed_aborts.read();
        let Some(existing) = completed.get(&(client_id, call_id)) else {
            return Ok(false);
        };
        if existing == payload {
            Ok(true)
        } else {
            Err("call_id reused with a different AbortFileWrite payload".to_string())
        }
    }

    /// Record one successful ensure-absent AbortFileWrite result.
    pub(crate) fn record_completed_abort(
        &self,
        client_id: ClientId,
        call_id: CallId,
        payload: AbortCallPayload,
    ) -> Result<(), String> {
        let mut completed = self.completed_aborts.write();
        match completed.get(&(client_id, call_id)) {
            Some(existing) if existing == &payload => Ok(()),
            Some(_) => Err("call_id reused with a different AbortFileWrite payload".to_string()),
            None => {
                completed.insert((client_id, call_id), payload);
                Ok(())
            }
        }
    }

    /// Remove a write session (on commit, abort, or error).
    pub fn remove_session(&self, file_handle: u64) -> Option<WriteSession> {
        let session = self.sessions.write().remove(&file_handle);
        if let Some(session) = &session {
            self.retire_session_calls(session);
        }
        session
    }

    /// Remove handles for an inode whose lease is no longer current.
    pub fn remove_inactive_for_inode(&self, inode_id: InodeId, lease_manager: &LeaseManager) -> usize {
        let mut sessions = self.sessions.write();
        let previous_len = sessions.len();
        let removed = sessions
            .extract_if(|_, session| {
                session.inode_id == inode_id
                    && !lease_manager.is_active_lease(session.inode_id, session.lease_id, session.lease_epoch)
            })
            .map(|(_, session)| session)
            .collect::<Vec<_>>();
        let current_len = sessions.len();
        drop(sessions);
        for session in &removed {
            self.retire_session_calls(session);
        }
        previous_len - current_len
    }

    fn retire_session_calls(&self, session: &WriteSession) {
        let mut retired = self.retired_session_calls.write();
        retired.insert((session.open_client_id, session.open_call_id));
        retired.extend(session.issued_steps.iter().map(|step| (step.client_id, step.call_id)));
    }
}

fn validate_open_replay(session: &WriteSession, input: &CreateSessionInput) -> Result<(), String> {
    if session.inode_id != input.inode_id
        || session.open_path != input.open_path
        || session.mode != input.mode
        || session.open_desired_len != input.open_desired_len
    {
        return Err("call_id reused with a different OpenWrite payload".to_string());
    }
    Ok(())
}

impl Default for SessionRegistry {
    fn default() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            next_file_handle: RwLock::new(1),
            completed_aborts: RwLock::new(HashMap::new()),
            retired_session_calls: RwLock::new(HashSet::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use beryl_types::ids::{BlockId, BlockIndex, ClientId};
    use std::sync::Arc;

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
            open_client_id: ClientId::new(inode_id.as_raw().into()),
            open_call_id: CallId::new(),
            open_path: format!("/file-{}", inode_id.as_raw()),
            open_desired_len: None,
            layout: FileLayout::new(64, 64, 1),
            expires_at_ms: 1_000,
            write_targets: Vec::new(),
        }
    }

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
            block_format_id: beryl_types::BlockFormatId::CURRENT_FOR_NEW_FILE,
            tier: beryl_types::Tier::Hdd,
        }
    }

    #[test]
    fn create_get_and_remove_session() {
        let registry = SessionRegistry::default();
        let inode_id = InodeId::new(7);
        let input = create_input(inode_id);
        let open_client_id = input.open_client_id;
        let open_call_id = input.open_call_id;

        let handle = registry
            .get_or_create_session(input.clone())
            .expect("session created")
            .0;

        assert_eq!(
            registry.get_session(handle).map(|session| session.inode_id),
            Some(inode_id)
        );
        assert_eq!(
            registry.remove_session(handle).map(|session| session.inode_id),
            Some(inode_id)
        );
        assert!(registry.get_session(handle).is_none());
        assert!(registry.has_call_id(open_client_id, open_call_id));
        assert!(registry
            .get_or_create_session(input)
            .expect_err("retired OpenWrite call must fail closed")
            .contains("retired session"));
    }

    #[test]
    fn completed_abort_replays_exact_payload_and_rejects_drift() {
        let registry = SessionRegistry::default();
        let client_id = ClientId::new(9);
        let call_id = CallId::new();
        let payload = AbortCallPayload {
            file_handle: 7,
            lease_id: Some(LeaseId::new(8)),
            lease_epoch: 9,
            open_epoch: 10,
            fencing_block_id: None,
            fencing_owner: Some(client_id),
            fencing_epoch: Some(11),
        };

        registry
            .record_completed_abort(client_id, call_id, payload.clone())
            .expect("record abort");

        assert!(registry
            .replay_completed_abort(client_id, call_id, &payload)
            .expect("exact replay"));
        assert!(registry.has_call_id(client_id, call_id));
        let mut mismatch = payload;
        mismatch.file_handle += 1;
        assert!(registry
            .replay_completed_abort(client_id, call_id, &mismatch)
            .expect_err("payload drift rejected")
            .contains("different AbortFileWrite payload"));
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
                            registry
                                .get_or_create_session(create_input(inode_id))
                                .expect("session created")
                                .0
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

    #[test]
    fn open_write_replay_returns_same_session_and_rejects_payload_drift() {
        let registry = SessionRegistry::default();
        let inode_id = InodeId::new(900);
        let input = create_input(inode_id);
        let client_id = input.open_client_id;
        let call_id = input.open_call_id;
        let first = registry
            .get_or_create_session(input)
            .expect("first OpenWrite creates session");

        let mut replay = create_input(inode_id);
        replay.open_client_id = client_id;
        replay.open_call_id = call_id;
        let second = registry
            .get_or_create_session(replay)
            .expect("same OpenWrite replays session");
        assert_eq!(second.0, first.0);
        assert_eq!(second.1.lease_id, first.1.lease_id);
        assert_eq!(second.1.expires_at_ms, first.1.expires_at_ms);

        let mut mismatch = create_input(inode_id);
        mismatch.open_client_id = client_id;
        mismatch.open_call_id = call_id;
        mismatch.open_desired_len = Some(1);
        assert_eq!(
            registry.get_or_create_session(mismatch).unwrap_err(),
            "call_id reused with a different OpenWrite payload"
        );
    }

    #[test]
    fn add_block_replay_is_addressed_by_call_and_predecessor() {
        let registry = SessionRegistry::default();
        let inode_id = InodeId::new(901);
        let data_handle_id = DataHandleId::new(901);
        let mut input = create_input(inode_id);
        input.write_targets = vec![write_target(data_handle_id, 0), write_target(data_handle_id, 1)];
        let file_handle = registry.get_or_create_session(input).expect("session created").0;
        let client_id = ClientId::new(8);
        let first_call = CallId::new();
        let first = registry
            .allocate_target(file_handle, client_id, first_call, None, Some(64))
            .expect("first target");
        let same_call = registry
            .allocate_target(file_handle, client_id, first_call, None, Some(64))
            .expect("same call replays first target");
        let same_predecessor = registry
            .allocate_target(file_handle, client_id, CallId::new(), None, Some(64))
            .expect("same predecessor replays first target");
        assert_eq!(same_call.block_id, first.block_id);
        assert_eq!(same_predecessor.block_id, first.block_id);
        assert_eq!(registry.get_session(file_handle).unwrap().next_target_index, 1);

        assert!(registry
            .allocate_target(file_handle, client_id, first_call, None, Some(32))
            .unwrap_err()
            .contains("different AddBlock payload"));
        assert!(registry
            .allocate_target(file_handle, client_id, CallId::new(), None, Some(32))
            .unwrap_err()
            .contains("predecessor reused"));

        let second = registry
            .allocate_target(file_handle, client_id, CallId::new(), Some(first.block_id), Some(64))
            .expect("successor target");
        assert_ne!(second.block_id, first.block_id);
        assert_eq!(second.file_offset, first.effective_len);
        assert_eq!(registry.get_session(file_handle).unwrap().next_target_index, 2);
    }
}
