// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Runtime registry for write sessions.
//!
//! Sessions are leader-local and are normally removed on CommitFile or AbortFileWrite.
//! LeaseManager is the authority for whether a write is still active; this
//! registry only stores continuation state needed to continue an admitted write.

use crate::inode_lease::{LeaseManager, WriteMode};
use crate::observe;
use beryl_types::fs::InodeId;
use beryl_types::ids::MountId;
use beryl_types::{BlockId, BlockShape, ClientId, FileLayout, WriteTarget};
use parking_lot::RwLock;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

/// Maximum number of expired sessions retired by one global expiry-sweep invocation.
const MAX_EXPIRED_SESSION_RETIREMENTS_PER_CALL: usize = 64;

/// Leader-local continuation state for one admitted write.
///
/// This record is not durable authority. The persisted fencing epoch fences
/// writers across replay and leader restart.
#[derive(Clone, Debug)]
pub struct WriteSession {
    /// Inode ID being written.
    pub inode_id: InodeId,
    /// Mount ID.
    pub mount_id: MountId,
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
    /// Bounded mount-root-to-file chain captured while namespace topology was stable.
    ancestor_inode_ids: Vec<InodeId>,
    /// Targets already issued to the client through AddBlock.
    pub issued_targets: Vec<WriteTarget>,
    /// Logical AddBlock steps issued for predecessor-based replay.
    issued_steps: HashMap<Option<BlockId>, IssuedTarget>,
}

/// Inputs needed to create a runtime write session and its bounded ancestor index.
#[derive(Clone)]
pub struct CreateSessionInput {
    pub mount_id: MountId,
    pub inode_id: InodeId,
    pub lease_epoch: u64,
    pub base_size: u64,
    pub content_revision: u64,
    pub mode: WriteMode,
    pub open_client_id: ClientId,
    pub layout: FileLayout,
    pub expires_at_ms: u64,
    pub ancestor_inode_ids: Vec<InodeId>,
}

#[derive(Clone, Debug)]
struct IssuedTarget {
    desired_len: Option<u64>,
    target: WriteTarget,
}

/// In-memory, leader-local registry of write sessions and capacity indexes.
///
/// One lock protects the primary session map and every derived inode,
/// ancestor, and expiry index so readers never observe a partially updated
/// session lifecycle.
pub struct SessionRegistry {
    state: RwLock<SessionRegistryState>,
    max_sessions: usize,
    max_sessions_per_client: usize,
}

/// Capacity boundary that rejected one write-session reservation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WriteSessionLimit {
    /// Process-wide pending plus installed session capacity.
    Global,
    /// Pending plus installed capacity attributed to one client ID.
    PerClient,
}

impl WriteSessionLimit {
    /// Stable low-cardinality label used by capacity metrics and logs.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Global => "global",
            Self::PerClient => "per_client",
        }
    }
}

/// Exact reason why a write session could not reserve leader-local capacity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WriteSessionLimitExceeded {
    pub(crate) limit: WriteSessionLimit,
    pub(crate) maximum: usize,
}

/// Pending write-session capacity held across local lease acquisition and Raft proposal.
///
/// The reservation borrows its registry, so dropping an `OpenWrite` future
/// releases pending capacity synchronously without a detached cleanup task.
#[must_use = "dropping the reservation releases pending write-session capacity"]
pub(crate) struct WriteSessionReservation<'a> {
    registry: &'a SessionRegistry,
    client_id: ClientId,
    armed: bool,
}

/// Primary write-session state and all indexes that must change atomically with it.
#[derive(Default)]
struct SessionRegistryState {
    /// At most one active session exists for one inode.
    sessions: HashMap<InodeId, WriteSession>,
    /// Requests that reserved capacity but have not installed a session.
    pending_sessions: usize,
    /// Pending plus installed sessions attributed to each client ID.
    occupied_sessions_by_client: HashMap<ClientId, usize>,
    /// Bounded, subtree-size-independent activity state for every ancestor of an open file.
    ancestor_activity: HashMap<InodeId, AncestorWriteActivity>,
    /// Sessions ordered by mirrored lease expiry for amortized cleanup.
    sessions_by_expiry: BTreeSet<(u64, InodeId)>,
}

/// Expiry multiset for every write session whose captured path contains one inode.
///
/// The ancestor entry exists exactly while this multiset is non-empty. Counts
/// distinguish sessions that share the same expiry timestamp.
struct AncestorWriteActivity {
    sessions_by_expiry: BTreeMap<u64, usize>,
}

impl SessionRegistry {
    /// Create an empty leader-local registry with fixed process limits.
    pub(crate) fn new(max_sessions: usize, max_sessions_per_client: usize) -> Self {
        assert!(max_sessions > 0, "global write-session limit must be positive");
        assert!(
            max_sessions_per_client > 0 && max_sessions_per_client <= max_sessions,
            "per-client write-session limit must be positive and not exceed the global limit"
        );
        observe::set_write_sessions(0, 0);
        Self {
            state: RwLock::new(SessionRegistryState::default()),
            max_sessions,
            max_sessions_per_client,
        }
    }

    /// Reserve one global and per-client slot without waiting or allocating durable state.
    pub(crate) fn reserve_session(
        &self,
        client_id: ClientId,
    ) -> Result<WriteSessionReservation<'_>, WriteSessionLimitExceeded> {
        self.reserve_session_at(client_id, current_time_ms())
    }

    /// Reserve capacity after retiring one bounded batch at an explicit time.
    fn reserve_session_at(
        &self,
        client_id: ClientId,
        now_ms: u64,
    ) -> Result<WriteSessionReservation<'_>, WriteSessionLimitExceeded> {
        let mut state = self.state.write();
        Self::retire_expired_sessions(&mut state, now_ms);
        let occupied = state.sessions.len().saturating_add(state.pending_sessions);
        if occupied >= self.max_sessions {
            observe::record_write_session_rejected(WriteSessionLimit::Global.label());
            return Err(WriteSessionLimitExceeded {
                limit: WriteSessionLimit::Global,
                maximum: self.max_sessions,
            });
        }
        let client_occupied = state
            .occupied_sessions_by_client
            .get(&client_id)
            .copied()
            .unwrap_or_default();
        if client_occupied >= self.max_sessions_per_client {
            observe::record_write_session_rejected(WriteSessionLimit::PerClient.label());
            return Err(WriteSessionLimitExceeded {
                limit: WriteSessionLimit::PerClient,
                maximum: self.max_sessions_per_client,
            });
        }

        state.pending_sessions += 1;
        *state.occupied_sessions_by_client.entry(client_id).or_default() += 1;
        observe::set_write_sessions(state.pending_sessions, state.sessions.len());
        Ok(WriteSessionReservation {
            registry: self,
            client_id,
            armed: true,
        })
    }

    /// Atomically convert one pending reservation into an installed session.
    ///
    /// The caller must already have acquired the exact local lease and durable
    /// fencing epoch stored in `input`. Any error leaves the reservation armed,
    /// so its Drop path returns pending capacity.
    pub(crate) fn install_reserved_session(
        &self,
        reservation: WriteSessionReservation<'_>,
        input: CreateSessionInput,
    ) -> Result<WriteSession, String> {
        self.install_reserved_session_at(reservation, input, current_time_ms())
    }

    /// Install a reserved session using one explicit expiry-observation time.
    fn install_reserved_session_at(
        &self,
        mut reservation: WriteSessionReservation<'_>,
        input: CreateSessionInput,
        now_ms: u64,
    ) -> Result<WriteSession, String> {
        if !std::ptr::eq(self, reservation.registry) {
            return Err("write session reservation belongs to another registry".to_string());
        }
        if input.open_client_id != reservation.client_id {
            return Err("write session reservation client mismatch".to_string());
        }
        Self::validate_ancestor_chain(input.inode_id, &input.ancestor_inode_ids)?;

        let mut state = self.state.write();
        Self::retire_expired_sessions(&mut state, now_ms);
        Self::retire_expired_session_for_inode(&mut state, input.inode_id, now_ms);
        if state.sessions.contains_key(&input.inode_id) {
            return Err("inode already has an active write session".to_string());
        }
        let session = WriteSession {
            inode_id: input.inode_id,
            mount_id: input.mount_id,
            lease_epoch: input.lease_epoch,
            base_size: input.base_size,
            content_revision: input.content_revision,
            mode: input.mode,
            open_client_id: input.open_client_id,
            layout: input.layout,
            expires_at_ms: input.expires_at_ms,
            ancestor_inode_ids: input.ancestor_inode_ids,
            issued_targets: Vec::new(),
            issued_steps: HashMap::new(),
        };

        for ancestor_inode_id in &session.ancestor_inode_ids {
            let activity = state
                .ancestor_activity
                .entry(*ancestor_inode_id)
                .or_insert(AncestorWriteActivity {
                    sessions_by_expiry: BTreeMap::new(),
                });
            *activity.sessions_by_expiry.entry(session.expires_at_ms).or_default() += 1;
        }
        state
            .sessions_by_expiry
            .insert((session.expires_at_ms, session.inode_id));
        state.sessions.insert(input.inode_id, session.clone());
        state.pending_sessions = state
            .pending_sessions
            .checked_sub(1)
            .expect("installed write session must own one pending reservation");
        reservation.armed = false;
        observe::set_write_sessions(state.pending_sessions, state.sessions.len());
        Ok(session)
    }

    /// Return an issued predecessor-addressed AddBlock step, or validate that
    /// the caller may allocate the next step.
    pub fn lookup_issued_target(
        &self,
        inode_id: InodeId,
        lease_epoch: u64,
        previous_block_id: Option<BlockId>,
        desired_len: Option<u64>,
    ) -> Result<Option<WriteTarget>, String> {
        let state = self.state.read();
        let session = state
            .sessions
            .get(&inode_id)
            .ok_or_else(|| "write session not found".to_string())?;
        if session.lease_epoch != lease_epoch {
            return Err("write session lease epoch mismatch".to_string());
        }
        if let Some(step) = session.issued_steps.get(&previous_block_id) {
            if step.desired_len == desired_len {
                return Ok(Some(step.target.clone()));
            }
            return Err("AddBlock predecessor reused with a different desired_len".to_string());
        }

        let expected_previous = session.issued_targets.last().map(|target| target.block_id);
        if previous_block_id != expected_previous {
            return Err(format!(
                "AddBlock predecessor mismatch: expected {expected_previous:?}, got {previous_block_id:?}"
            ));
        }
        Ok(None)
    }

    /// Install one newly allocated target, or return the winner of a concurrent
    /// request for the same predecessor.
    pub fn install_issued_target(
        &self,
        inode_id: InodeId,
        lease_epoch: u64,
        previous_block_id: Option<BlockId>,
        desired_len: Option<u64>,
        target: WriteTarget,
    ) -> Result<WriteTarget, String> {
        let mut state = self.state.write();
        let session = state
            .sessions
            .get_mut(&inode_id)
            .ok_or_else(|| "write session not found".to_string())?;
        if session.lease_epoch != lease_epoch {
            return Err("write session lease epoch mismatch".to_string());
        }
        if let Some(step) = session.issued_steps.get(&previous_block_id) {
            if step.desired_len == desired_len {
                return Ok(step.target.clone());
            }
            return Err("AddBlock predecessor reused with a different desired_len".to_string());
        }
        let expected_previous = session.issued_targets.last().map(|issued| issued.block_id);
        if previous_block_id != expected_previous {
            return Err(format!(
                "AddBlock predecessor mismatch: expected {expected_previous:?}, got {previous_block_id:?}"
            ));
        }
        if target.block_id.inode_id != inode_id {
            return Err("write target inode mismatch".to_string());
        }
        if target.fencing_token.block_id != target.block_id
            || target.fencing_token.owner != session.open_client_id
            || target.fencing_token.epoch != lease_epoch
        {
            return Err("write target fencing token mismatch".to_string());
        }
        let expected_effective_len = desired_len.unwrap_or(target.block_size);
        if target.effective_len != expected_effective_len {
            return Err(format!(
                "write target effective length mismatch: expected {expected_effective_len}, got {}",
                target.effective_len
            ));
        }
        let next_file_offset = session
            .issued_targets
            .last()
            .and_then(|issued| issued.file_offset.checked_add(issued.effective_len))
            .unwrap_or(session.base_size);
        if target.file_offset != next_file_offset {
            return Err(format!(
                "write target file offset changed: expected {next_file_offset}, got {}",
                target.file_offset
            ));
        }
        let target_shape = BlockShape::new(
            target.block_format_id,
            target.block_size,
            target.chunk_size,
            target.effective_len,
        )
        .map_err(|error| format!("invalid write target shape: {error}"))?;
        let expected_shape = BlockShape::for_effective_len(&session.layout, target.effective_len)
            .map_err(|error| format!("invalid session layout shape: {error}"))?;
        if target_shape != expected_shape {
            return Err("write target shape does not match the session layout".to_string());
        }
        let expected_block_stamp = session
            .content_revision
            .checked_add(1)
            .ok_or_else(|| "content revision overflow".to_string())?;
        if target.block_stamp != expected_block_stamp {
            return Err(format!(
                "write target block stamp changed: expected {expected_block_stamp}, got {}",
                target.block_stamp
            ));
        }
        session.issued_targets.push(target.clone());
        session.issued_steps.insert(
            previous_block_id,
            IssuedTarget {
                desired_len,
                target: target.clone(),
            },
        );
        Ok(target)
    }

    /// Get a non-expired write session after bounded global and exact-inode retirement.
    pub fn get_session(&self, inode_id: InodeId) -> Option<WriteSession> {
        let mut state = self.state.write();
        let now_ms = current_time_ms();
        Self::retire_expired_sessions(&mut state, now_ms);
        Self::retire_expired_session_for_inode(&mut state, inode_id, now_ms);
        state.sessions.get(&inode_id).cloned()
    }

    /// Remove only the session identified by the presented lease epoch.
    pub fn remove_session_if_epoch(&self, inode_id: InodeId, lease_epoch: u64) -> Option<WriteSession> {
        let mut state = self.state.write();
        if state
            .sessions
            .get(&inode_id)
            .is_none_or(|session| session.lease_epoch != lease_epoch)
        {
            return None;
        }
        Self::remove_session(&mut state, inode_id)
    }

    /// Update the mirrored lease expiry used by bounded, subtree-size-independent admission checks.
    ///
    /// Every expiry index is moved before the bounded sweep runs, so a
    /// successful renewal cannot be retired under its previous timestamp.
    pub fn update_expiration(&self, inode_id: InodeId, lease_epoch: u64, expires_at_ms: u64) -> Result<(), String> {
        let mut state = self.state.write();
        let (ancestor_inode_ids, previous_expires_at_ms) = {
            let session = state
                .sessions
                .get(&inode_id)
                .ok_or_else(|| "write session not found".to_string())?;
            if session.lease_epoch != lease_epoch {
                return Err("write session lease epoch mismatch".to_string());
            }
            if expires_at_ms < session.expires_at_ms {
                return Err("write session expiry cannot move backwards".to_string());
            }
            (session.ancestor_inode_ids.clone(), session.expires_at_ms)
        };
        if ancestor_inode_ids
            .iter()
            .any(|ancestor_inode_id| !state.ancestor_activity.contains_key(ancestor_inode_id))
        {
            return Err("write session ancestor index is missing".to_string());
        }
        if !state.sessions_by_expiry.contains(&(previous_expires_at_ms, inode_id)) {
            return Err("write session expiry index is missing".to_string());
        }
        Self::remove_from_expiry_index(&mut state, inode_id, previous_expires_at_ms);
        state
            .sessions
            .get_mut(&inode_id)
            .expect("validated write session must exist")
            .expires_at_ms = expires_at_ms;
        state.sessions_by_expiry.insert((expires_at_ms, inode_id));
        for ancestor_inode_id in ancestor_inode_ids {
            let activity = state
                .ancestor_activity
                .get_mut(&ancestor_inode_id)
                .expect("validated write session ancestor index must exist");
            Self::decrement_expiry_count(&mut activity.sessions_by_expiry, previous_expires_at_ms);
            *activity.sessions_by_expiry.entry(expires_at_ms).or_default() += 1;
        }
        Self::retire_expired_sessions(&mut state, current_time_ms());
        Ok(())
    }

    /// Return whether the inode is or contains a non-expired write session.
    ///
    /// This does not walk namespace descendants. A bounded sweep may leave
    /// physically stale entries, but the maximum mirrored expiry prevents them
    /// from producing a false `EBUSY`.
    pub fn has_active_write_under(&self, inode_id: InodeId) -> bool {
        self.has_active_write_under_at(inode_id, current_time_ms())
    }

    fn has_active_write_under_at(&self, inode_id: InodeId, now_ms: u64) -> bool {
        let mut state = self.state.write();
        Self::retire_expired_sessions(&mut state, now_ms);
        state
            .ancestor_activity
            .get(&inode_id)
            .and_then(|activity| activity.sessions_by_expiry.last_key_value())
            .is_some_and(|(expires_at_ms, _)| *expires_at_ms > now_ms)
    }

    /// Validate the bounded, acyclic path identity stored by one write session.
    pub(crate) fn validate_ancestor_chain(inode_id: InodeId, ancestor_inode_ids: &[InodeId]) -> Result<(), String> {
        if ancestor_inode_ids.is_empty() {
            return Err("write session ancestor chain cannot be empty".to_string());
        }
        if ancestor_inode_ids.len() > crate::path_resolver::MAX_PATH_COMPONENTS + 1 {
            return Err("write session ancestor chain exceeds the path depth limit".to_string());
        }
        if ancestor_inode_ids.last() != Some(&inode_id) {
            return Err("write session ancestor chain must end at the file inode".to_string());
        }
        let mut unique_inode_ids = HashSet::with_capacity(ancestor_inode_ids.len());
        if ancestor_inode_ids
            .iter()
            .any(|ancestor_inode_id| !unique_inode_ids.insert(*ancestor_inode_id))
        {
            return Err("write session ancestor chain contains a cycle".to_string());
        }
        Ok(())
    }

    pub fn update_published_state(
        &self,
        inode_id: InodeId,
        lease_epoch: u64,
        content_revision: u64,
        file_size: u64,
    ) -> Result<(), String> {
        let mut state = self.state.write();
        let session = state
            .sessions
            .get_mut(&inode_id)
            .ok_or_else(|| "write session not found".to_string())?;
        if session.lease_epoch != lease_epoch {
            return Err("write session lease epoch mismatch".to_string());
        }
        session.content_revision = content_revision;
        session.base_size = file_size;
        Ok(())
    }

    /// Remove the session for an inode whose lease is no longer current.
    pub fn remove_inactive_for_inode(&self, inode_id: InodeId, lease_manager: &LeaseManager) -> usize {
        let mut state = self.state.write();
        let Some(session) = state.sessions.get(&inode_id) else {
            return 0;
        };
        if lease_manager.is_active_lease(session.inode_id, session.lease_epoch) {
            return 0;
        }
        usize::from(Self::remove_session(&mut state, inode_id).is_some())
    }

    /// Remove one primary session and every derived index entry under the state lock.
    fn remove_session(state: &mut SessionRegistryState, inode_id: InodeId) -> Option<WriteSession> {
        let client_id = state.sessions.get(&inode_id)?.open_client_id;
        assert!(
            state
                .occupied_sessions_by_client
                .get(&client_id)
                .copied()
                .unwrap_or_default()
                > 0,
            "installed write session must own one client capacity slot"
        );
        let session = state.sessions.remove(&inode_id)?;
        Self::remove_from_indexes(state, &session);
        Self::decrement_client_occupancy(state, session.open_client_id);
        observe::set_write_sessions(state.pending_sessions, state.sessions.len());
        Some(session)
    }

    /// Return one pending reservation without touching installed sessions.
    fn release_pending_reservation(&self, client_id: ClientId) {
        let mut state = self.state.write();
        let client_count = state
            .occupied_sessions_by_client
            .get(&client_id)
            .copied()
            .unwrap_or_default();
        if state.pending_sessions == 0 || client_count == 0 {
            tracing::error!(
                client_id = %client_id,
                pending_sessions = state.pending_sessions,
                client_sessions = client_count,
                "write session reservation accounting is missing; retaining capacity"
            );
            return;
        }
        state.pending_sessions -= 1;
        Self::decrement_client_occupancy(&mut state, client_id);
        observe::set_write_sessions(state.pending_sessions, state.sessions.len());
    }

    /// Decrement one pending-or-installed client slot and remove empty keys.
    fn decrement_client_occupancy(state: &mut SessionRegistryState, client_id: ClientId) {
        let remove_client = {
            let count = state
                .occupied_sessions_by_client
                .get_mut(&client_id)
                .expect("validated write-session client occupancy must exist");
            *count = count
                .checked_sub(1)
                .expect("validated write-session client occupancy must be positive");
            *count == 0
        };
        if remove_client {
            state.occupied_sessions_by_client.remove(&client_id);
        }
    }

    /// Remove the exact inode, global-expiry, and ancestor-expiry entries for a session.
    fn remove_from_indexes(state: &mut SessionRegistryState, session: &WriteSession) {
        Self::remove_from_expiry_index(state, session.inode_id, session.expires_at_ms);
        for ancestor_inode_id in &session.ancestor_inode_ids {
            let remove_entry = {
                let activity = state
                    .ancestor_activity
                    .get_mut(ancestor_inode_id)
                    .expect("write session ancestor index must exist");
                Self::decrement_expiry_count(&mut activity.sessions_by_expiry, session.expires_at_ms);
                activity.sessions_by_expiry.is_empty()
            };
            if remove_entry {
                state.ancestor_activity.remove(ancestor_inode_id);
            }
        }
    }

    /// Retire the earliest expired sessions without exceeding the global sweep budget.
    fn retire_expired_sessions(state: &mut SessionRegistryState, now_ms: u64) -> usize {
        let mut retired = 0;
        while retired < MAX_EXPIRED_SESSION_RETIREMENTS_PER_CALL {
            let Some(&(expires_at_ms, inode_id)) = state.sessions_by_expiry.first() else {
                break;
            };
            if expires_at_ms > now_ms {
                break;
            }
            if Self::remove_session(state, inode_id).is_none() {
                Self::remove_from_expiry_index(state, inode_id, expires_at_ms);
            }
            retired += 1;
        }
        retired
    }

    /// Retire one requested inode even when it remains beyond the global sweep budget.
    fn retire_expired_session_for_inode(state: &mut SessionRegistryState, inode_id: InodeId, now_ms: u64) -> bool {
        let is_expired = state
            .sessions
            .get(&inode_id)
            .is_some_and(|session| session.expires_at_ms <= now_ms);
        is_expired && Self::remove_session(state, inode_id).is_some()
    }

    fn remove_from_expiry_index(state: &mut SessionRegistryState, inode_id: InodeId, expires_at_ms: u64) {
        state.sessions_by_expiry.remove(&(expires_at_ms, inode_id));
    }

    fn decrement_expiry_count(expirations: &mut BTreeMap<u64, usize>, expires_at_ms: u64) {
        let remove_expiry = {
            let count = expirations
                .get_mut(&expires_at_ms)
                .expect("write session ancestor expiry must exist");
            *count -= 1;
            *count == 0
        };
        if remove_expiry {
            expirations.remove(&expires_at_ms);
        }
    }
}

impl Drop for WriteSessionReservation<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.registry.release_pending_reservation(self.client_id);
            self.armed = false;
        }
    }
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl Default for SessionRegistry {
    fn default() -> Self {
        let config = crate::config::MetadataWriteSessionLimitsConfig::default();
        Self::new(config.max_active, config.max_active_per_client)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use beryl_types::ids::BlockIndex;
    use beryl_types::lease::FencingToken;
    use beryl_types::{BlockFormatId, Tier};

    fn write_target(inode_id: InodeId, index: u32) -> WriteTarget {
        let block_id = BlockId::new(inode_id, BlockIndex::new(index));
        WriteTarget {
            block_id,
            file_offset: 0,
            block_size: 64,
            effective_len: 64,
            worker_endpoints: Vec::new(),
            fencing_token: FencingToken {
                block_id,
                owner: ClientId::new(1),
                epoch: 7,
            },
            block_stamp: 1,
            chunk_size: 64,
            block_format_id: BlockFormatId::CURRENT_FOR_NEW_FILE,
            tier: Tier::Hdd,
        }
    }

    fn create_input(inode_id: InodeId) -> CreateSessionInput {
        CreateSessionInput {
            inode_id,
            mount_id: MountId::new(1),
            lease_epoch: 7,
            base_size: 0,
            content_revision: 0,
            mode: WriteMode::Write,
            open_client_id: ClientId::new(1),
            layout: FileLayout::new(64, 64, 1),
            expires_at_ms: u64::MAX,
            ancestor_inode_ids: vec![inode_id],
        }
    }

    fn install_session(registry: &SessionRegistry, input: CreateSessionInput) -> Result<WriteSession, String> {
        let reservation = registry
            .reserve_session(input.open_client_id)
            .map_err(|exceeded| format!("write session rejected by {} limit", exceeded.limit.label()))?;
        registry.install_reserved_session(reservation, input)
    }

    fn install_session_at(
        registry: &SessionRegistry,
        input: CreateSessionInput,
        now_ms: u64,
    ) -> Result<WriteSession, String> {
        let reservation = registry
            .reserve_session_at(input.open_client_id, now_ms)
            .map_err(|exceeded| format!("write session rejected by {} limit", exceeded.limit.label()))?;
        registry.install_reserved_session_at(reservation, input, now_ms)
    }

    fn issue_target(
        registry: &SessionRegistry,
        inode_id: InodeId,
        previous_block_id: Option<BlockId>,
        desired_len: u64,
        index: u32,
        file_offset: u64,
        block_stamp: u64,
    ) -> WriteTarget {
        let mut target = write_target(inode_id, index);
        target.file_offset = file_offset;
        target.effective_len = desired_len;
        target.block_stamp = block_stamp;
        registry
            .install_issued_target(inode_id, 7, previous_block_id, Some(desired_len), target)
            .unwrap()
    }

    #[test]
    fn pending_and_installed_sessions_share_global_capacity() {
        let registry = SessionRegistry::new(2, 2);
        install_session(&registry, create_input(InodeId::new(1))).unwrap();
        let pending = registry.reserve_session(ClientId::new(1)).unwrap();

        let rejection = match registry.reserve_session(ClientId::new(2)) {
            Ok(_) => panic!("global capacity must reject limit plus one"),
            Err(rejection) => rejection,
        };
        assert_eq!(
            rejection,
            WriteSessionLimitExceeded {
                limit: WriteSessionLimit::Global,
                maximum: 2,
            }
        );

        drop(pending);
        let replacement = registry.reserve_session(ClientId::new(2)).unwrap();
        drop(replacement);
        assert!(registry.remove_session_if_epoch(InodeId::new(1), 7).is_some());
        let state = registry.state.read();
        assert_eq!(state.pending_sessions, 0);
        assert!(state.sessions.is_empty());
        assert!(state.occupied_sessions_by_client.is_empty());
    }

    #[test]
    fn per_client_capacity_does_not_block_another_client() {
        let registry = SessionRegistry::new(3, 1);
        install_session(&registry, create_input(InodeId::new(1))).unwrap();

        let rejection = match registry.reserve_session(ClientId::new(1)) {
            Ok(_) => panic!("per-client capacity must reject limit plus one"),
            Err(rejection) => rejection,
        };
        assert_eq!(
            rejection,
            WriteSessionLimitExceeded {
                limit: WriteSessionLimit::PerClient,
                maximum: 1,
            }
        );
        let other_client = registry.reserve_session(ClientId::new(2)).unwrap();
        drop(other_client);
    }

    #[test]
    fn dropped_or_invalid_reservations_restore_capacity() {
        let registry = SessionRegistry::new(1, 1);
        let pending = registry.reserve_session(ClientId::new(1)).unwrap();
        drop(pending);

        let reservation = registry.reserve_session(ClientId::new(1)).unwrap();
        let mut mismatched = create_input(InodeId::new(1));
        mismatched.open_client_id = ClientId::new(2);
        assert_eq!(
            registry.install_reserved_session(reservation, mismatched).unwrap_err(),
            "write session reservation client mismatch"
        );

        let replacement = registry.reserve_session(ClientId::new(1)).unwrap();
        drop(replacement);
        let state = registry.state.read();
        assert_eq!(state.pending_sessions, 0);
        assert!(state.occupied_sessions_by_client.is_empty());
    }

    #[test]
    fn concurrent_reservations_never_exceed_global_capacity() {
        let registry = std::sync::Arc::new(SessionRegistry::new(4, 4));
        let contender_count = 16;
        let start = std::sync::Arc::new(std::sync::Barrier::new(contender_count + 1));
        let release = std::sync::Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let mut joins = Vec::new();

        for _ in 0..contender_count {
            let registry = std::sync::Arc::clone(&registry);
            let start = std::sync::Arc::clone(&start);
            let release = std::sync::Arc::clone(&release);
            let result_tx = result_tx.clone();
            joins.push(std::thread::spawn(move || {
                start.wait();
                let reservation = registry.reserve_session(ClientId::new(1));
                result_tx.send(reservation.is_ok()).unwrap();
                if let Ok(reservation) = reservation {
                    let (released, wake) = &*release;
                    let mut released = released.lock().unwrap();
                    while !*released {
                        released = wake.wait(released).unwrap();
                    }
                    drop(reservation);
                }
            }));
        }
        drop(result_tx);
        start.wait();
        let reserved = (0..contender_count)
            .filter(|_| result_rx.recv().expect("reservation result"))
            .count();
        assert_eq!(reserved, 4);

        let (released, wake) = &*release;
        *released.lock().unwrap() = true;
        wake.notify_all();
        for join in joins {
            join.join().unwrap();
        }
        let state = registry.state.read();
        assert_eq!(state.pending_sessions, 0);
        assert!(state.occupied_sessions_by_client.is_empty());
    }

    #[test]
    fn one_inode_has_at_most_one_active_session() {
        let registry = SessionRegistry::default();
        let inode_id = InodeId::new(10);
        install_session(&registry, create_input(inode_id)).unwrap();

        assert!(install_session(&registry, create_input(inode_id)).is_err());
        assert_eq!(registry.get_session(inode_id).unwrap().lease_epoch, 7);
        assert!(registry.remove_session_if_epoch(inode_id, 7).is_some());
        assert!(registry.get_session(inode_id).is_none());
    }

    #[test]
    fn delayed_cleanup_cannot_remove_a_newer_session() {
        let registry = SessionRegistry::default();
        let inode_id = InodeId::new(20);
        install_session(&registry, create_input(inode_id)).unwrap();
        registry.remove_session_if_epoch(inode_id, 7).unwrap();
        let mut replacement = create_input(inode_id);
        replacement.lease_epoch = 8;
        install_session(&registry, replacement).unwrap();

        assert!(registry.remove_session_if_epoch(inode_id, 7).is_none());
        assert_eq!(registry.get_session(inode_id).unwrap().lease_epoch, 8);
    }

    #[test]
    fn ancestor_index_tracks_shared_ancestors_until_the_last_session_closes() {
        let registry = SessionRegistry::default();
        let root_inode_id = InodeId::new(1);
        let directory_inode_id = InodeId::new(2);
        let first_handle = InodeId::new(20);
        let second_handle = InodeId::new(21);
        let mut first = create_input(first_handle);
        first.ancestor_inode_ids = vec![root_inode_id, directory_inode_id, first.inode_id];
        let mut second = create_input(second_handle);
        second.ancestor_inode_ids = vec![root_inode_id, directory_inode_id, second.inode_id];

        install_session(&registry, first).unwrap();
        install_session(&registry, second).unwrap();
        assert!(registry.has_active_write_under(root_inode_id));
        assert!(registry.has_active_write_under(directory_inode_id));
        assert!(registry.has_active_write_under(InodeId::new(first_handle.as_raw())));
        assert!(registry.has_active_write_under(InodeId::new(second_handle.as_raw())));

        registry.remove_session_if_epoch(first_handle, 7).unwrap();
        assert!(registry.has_active_write_under(root_inode_id));
        assert!(!registry.has_active_write_under(InodeId::new(first_handle.as_raw())));

        registry.remove_session_if_epoch(second_handle, 7).unwrap();
        assert!(!registry.has_active_write_under(root_inode_id));
        assert!(!registry.has_active_write_under(directory_inode_id));
    }

    #[test]
    fn expired_session_does_not_keep_ancestor_busy() {
        let registry = SessionRegistry::default();
        let inode_id = InodeId::new(22);
        let root_inode_id = InodeId::new(1);
        let mut input = create_input(inode_id);
        input.expires_at_ms = 0;
        input.ancestor_inode_ids = vec![root_inode_id, input.inode_id];

        install_session(&registry, input).unwrap();

        assert!(!registry.has_active_write_under(root_inode_id));
        assert!(!registry.has_active_write_under(InodeId::new(inode_id.as_raw())));
        let state = registry.state.read();
        assert!(state.sessions.is_empty());
        assert!(state.ancestor_activity.is_empty());
        assert!(state.sessions_by_expiry.is_empty());
    }

    #[test]
    fn renewed_session_updates_ancestor_expiry() {
        let registry = SessionRegistry::default();
        let inode_id = InodeId::new(24);
        let root_inode_id = InodeId::new(1);
        let mut input = create_input(inode_id);
        input.expires_at_ms = 1;
        input.ancestor_inode_ids = vec![root_inode_id, input.inode_id];
        install_session_at(&registry, input, 0).unwrap();

        registry
            .update_expiration(inode_id, 7, u64::MAX)
            .expect("renewal moves every expiry index before sweeping the old expiry");

        assert!(registry.get_session(inode_id).is_some());
        assert!(registry.has_active_write_under(root_inode_id));
        let state = registry.state.read();
        assert_eq!(state.sessions_by_expiry, BTreeSet::from([(u64::MAX, inode_id)]));
        assert_eq!(
            state
                .ancestor_activity
                .get(&root_inode_id)
                .expect("renewed ancestor activity")
                .sessions_by_expiry,
            BTreeMap::from([(u64::MAX, 1)])
        );
    }

    #[test]
    fn expiry_renewal_and_close_retire_shared_ancestor_state() {
        let registry = SessionRegistry::default();
        let root_inode_id = InodeId::new(1);
        let expired_handle = InodeId::new(25);
        let active_handle = InodeId::new(26);
        let mut expired = create_input(expired_handle);
        expired.expires_at_ms = 0;
        expired.ancestor_inode_ids = vec![root_inode_id, expired.inode_id];
        let mut active = create_input(active_handle);
        active.expires_at_ms = current_time_ms().saturating_add(10_000);
        active.ancestor_inode_ids = vec![root_inode_id, active.inode_id];

        install_session(&registry, expired).unwrap();
        install_session(&registry, active).unwrap();
        {
            let state = registry.state.read();
            assert_eq!(state.sessions.len(), 1);
            assert_eq!(state.ancestor_activity.len(), 2);
            assert_eq!(state.sessions_by_expiry.len(), 1);
        }

        registry
            .update_expiration(active_handle, 7, u64::MAX)
            .expect("renewal updates expiry indexes");
        registry.remove_session_if_epoch(active_handle, 7).unwrap();

        let state = registry.state.read();
        assert!(state.sessions.is_empty());
        assert!(state.ancestor_activity.is_empty());
        assert!(state.sessions_by_expiry.is_empty());
    }

    #[test]
    fn expiry_sweep_is_bounded_and_queries_ignore_residual_expired_entries() {
        let historical_expired_count = 4_096;
        let registry = SessionRegistry::new(historical_expired_count + 1, historical_expired_count + 1);
        let residual_expired_count = MAX_EXPIRED_SESSION_RETIREMENTS_PER_CALL * 2 + 3;
        for raw in 1..=historical_expired_count {
            let inode_id = InodeId::new(raw as u64);
            let mut input = create_input(inode_id);
            input.expires_at_ms = 10;
            install_session_at(&registry, input, 0).unwrap();
        }
        for raw in 1..=(historical_expired_count - residual_expired_count) {
            registry.remove_session_if_epoch(InodeId::new(raw as u64), 7).unwrap();
        }
        let active_inode_id = InodeId::new(20_000);
        let mut active = create_input(active_inode_id);
        active.expires_at_ms = 20;
        active.ancestor_inode_ids = vec![active_inode_id];
        install_session_at(&registry, active, 0).unwrap();

        assert!(registry.has_active_write_under_at(active_inode_id, 11));
        {
            let state = registry.state.read();
            assert_eq!(
                state.sessions.len(),
                residual_expired_count + 1 - MAX_EXPIRED_SESSION_RETIREMENTS_PER_CALL
            );
            assert!(state
                .sessions_by_expiry
                .iter()
                .any(|(expires_at_ms, _)| *expires_at_ms == 10));
        }

        let residual_expired_inode_id = {
            let state = registry.state.read();
            state
                .sessions
                .values()
                .find(|session| session.expires_at_ms == 10)
                .expect("one expired session must remain after a bounded sweep")
                .inode_id
        };
        assert!(!registry.has_active_write_under_at(residual_expired_inode_id, 11));
        while registry
            .state
            .read()
            .sessions_by_expiry
            .iter()
            .any(|(expires_at_ms, _)| *expires_at_ms == 10)
        {
            assert!(registry.has_active_write_under_at(active_inode_id, 11));
        }

        registry.remove_session_if_epoch(active_inode_id, 7).unwrap();
        let state = registry.state.read();
        assert!(state.sessions.is_empty());
        assert!(state.ancestor_activity.is_empty());
        assert!(state.sessions_by_expiry.is_empty());
    }

    #[test]
    fn new_leader_registry_has_no_old_session_ancestor_state() {
        let old_registry = SessionRegistry::default();
        let inode_id = InodeId::new(23);
        let root_inode_id = InodeId::new(1);
        let mut input = create_input(inode_id);
        input.ancestor_inode_ids = vec![root_inode_id, input.inode_id];
        install_session(&old_registry, input).unwrap();
        assert!(old_registry.has_active_write_under(root_inode_id));

        let new_registry = SessionRegistry::default();
        assert!(!new_registry.has_active_write_under(root_inode_id));
    }

    #[test]
    fn add_block_replays_by_predecessor_without_advancing() {
        let registry = SessionRegistry::default();
        let inode_id = InodeId::new(11);
        install_session(&registry, create_input(inode_id)).unwrap();

        assert!(registry
            .lookup_issued_target(inode_id, 7, None, Some(32))
            .unwrap()
            .is_none());
        let first = issue_target(&registry, inode_id, None, 32, 0, 0, 1);
        let replay = registry
            .lookup_issued_target(inode_id, 7, None, Some(32))
            .unwrap()
            .unwrap();
        assert_eq!(replay, first);

        let second = issue_target(&registry, inode_id, Some(first.block_id), 64, 1, 32, 1);
        assert_eq!(second.block_id.index, BlockIndex::new(1));
        assert_eq!(second.file_offset, 32);
    }

    #[test]
    fn concurrent_duplicate_add_block_installs_one_target_and_returns_one_result() {
        let registry = std::sync::Arc::new(SessionRegistry::default());
        let inode_id = InodeId::new(15);
        install_session(&registry, create_input(inode_id)).unwrap();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));

        let mut joins = Vec::new();
        for index in 0..2 {
            let registry = std::sync::Arc::clone(&registry);
            let barrier = std::sync::Arc::clone(&barrier);
            joins.push(std::thread::spawn(move || {
                let mut target = write_target(inode_id, index);
                target.effective_len = 32;
                barrier.wait();
                registry
                    .install_issued_target(inode_id, 7, None, Some(32), target)
                    .unwrap()
            }));
        }

        let first = joins.remove(0).join().unwrap();
        let second = joins.remove(0).join().unwrap();
        assert_eq!(first, second);
        let session = registry.get_session(inode_id).unwrap();
        assert_eq!(session.issued_targets, vec![first]);
        assert_eq!(session.issued_steps.len(), 1);
    }

    #[test]
    fn add_block_rejects_payload_drift_and_stale_lease_epoch() {
        let registry = SessionRegistry::default();
        let inode_id = InodeId::new(12);
        install_session(&registry, create_input(inode_id)).unwrap();
        issue_target(&registry, inode_id, None, 32, 0, 0, 1);

        assert!(registry.lookup_issued_target(inode_id, 7, None, Some(64)).is_err());
        assert!(registry.lookup_issued_target(inode_id, 6, None, Some(32)).is_err());
        assert_eq!(registry.get_session(inode_id).unwrap().issued_targets.len(), 1);
    }

    #[test]
    fn add_block_rejects_a_gap_in_the_predecessor_chain() {
        let registry = SessionRegistry::default();
        let inode_id = InodeId::new(13);
        install_session(&registry, create_input(inode_id)).unwrap();
        let unknown = BlockId::new(inode_id, BlockIndex::new(99));

        assert!(registry
            .lookup_issued_target(inode_id, 7, Some(unknown), Some(32))
            .unwrap_err()
            .contains("predecessor mismatch"));
        assert!(registry.get_session(inode_id).unwrap().issued_targets.is_empty());
    }

    #[test]
    fn new_target_uses_next_content_revision_while_replay_keeps_original_stamp() {
        let registry = SessionRegistry::default();
        let inode_id = InodeId::new(14);
        install_session(&registry, create_input(inode_id)).unwrap();

        let first = issue_target(&registry, inode_id, None, 32, 0, 0, 1);
        assert_eq!(first.block_stamp, 1);
        registry
            .update_published_state(inode_id, 7, 1, 32)
            .expect("advance published state");

        let replay = registry
            .lookup_issued_target(inode_id, 7, None, Some(32))
            .unwrap()
            .unwrap();
        assert_eq!(replay, first);
        let second = issue_target(&registry, inode_id, Some(first.block_id), 32, 1, 32, 2);
        assert_eq!(second.block_stamp, 2);
        assert_eq!(second.file_offset, 32);
    }

    #[test]
    fn add_block_completion_cannot_install_a_target_from_an_old_revision() {
        let registry = SessionRegistry::default();
        let inode_id = InodeId::new(16);
        install_session(&registry, create_input(inode_id)).unwrap();
        let first = issue_target(&registry, inode_id, None, 32, 0, 0, 1);
        registry
            .update_published_state(inode_id, 7, 1, 32)
            .expect("advance published state while AddBlock is in flight");
        let mut stale = write_target(inode_id, 1);
        stale.file_offset = 32;
        stale.effective_len = 32;
        stale.block_stamp = 1;

        let error = registry
            .install_issued_target(inode_id, 7, Some(first.block_id), Some(32), stale)
            .expect_err("an AddBlock result from the previous revision must be rejected");

        assert!(error.contains("block stamp changed: expected 2, got 1"));
        assert_eq!(registry.get_session(inode_id).unwrap().issued_targets, vec![first]);
    }
}
