// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Leader-local write-session admission and lifecycle state.
//!
//! The registry owns write mutual exclusion, capacity, expiration, and
//! continuation state for the current Metadata process. The persisted inode
//! lease epoch remains the durable fencing authority across replay and restart.

use crate::observe;
use beryl_types::fs::InodeId;
use beryl_types::ids::MountId;
use beryl_types::{BlockId, BlockShape, ClientId, FileLayout, WriteTarget};
use parking_lot::RwLock;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

/// Maximum number of expired entries retired by one cleanup invocation.
pub(crate) const MAX_EXPIRED_SESSION_RETIREMENTS_PER_CALL: usize = 64;

/// Write behavior selected when opening one file session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WriteMode {
    /// Replace the currently visible file contents.
    Write,
    /// Append new extents after the currently visible file size.
    Append,
}

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
    issued_steps: HashMap<Option<BlockId>, usize>,
    /// The one logical AddBlock step allowed to cross Raft allocation at a time.
    pending_add_block: Option<PendingAddBlock>,
    /// Exact local publication currently freezing the issued-target sequence.
    active_publication: Option<WritePublicationId>,
}

/// Small active-session snapshot used before AddBlock reserves target state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WriteSessionIdentity {
    pub(crate) mount_id: MountId,
    pub(crate) lease_epoch: u64,
    pub(crate) open_client_id: ClientId,
}

/// Validated inputs needed before one `OpenWrite` crosses its Raft proposal.
#[derive(Clone)]
pub(crate) struct BeginSessionInput {
    pub mount_id: MountId,
    pub inode_id: InodeId,
    pub current_lease_epoch: Option<u64>,
    pub base_size: u64,
    pub content_revision: u64,
    pub mode: WriteMode,
    pub open_client_id: ClientId,
    pub layout: FileLayout,
    pub ancestor_inode_ids: Vec<InodeId>,
}

/// Process-local identity for one exact `OpenWrite` attempt.
///
/// The durable fencing epoch can be proposed by more than one attempt before
/// either proposal applies. This identity prevents a cancelled stale attempt
/// from removing a replacement `Opening` entry that has the same candidate
/// epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WriteOpeningId(u64);

/// Process-local identity for one exact SyncWrite or CommitFile attempt.
///
/// The identity prevents a cancelled stale owner from clearing a later
/// publication on a replacement session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct WritePublicationId(u64);

/// Leader-local state held while `OpenWrite` waits for durable epoch fencing.
#[derive(Clone, Debug)]
struct OpeningSession {
    opening_id: WriteOpeningId,
    inode_id: InodeId,
    mount_id: MountId,
    proposed_lease_epoch: u64,
    base_size: u64,
    content_revision: u64,
    mode: WriteMode,
    open_client_id: ClientId,
    layout: FileLayout,
    expires_at_ms: u64,
    ancestor_inode_ids: Vec<InodeId>,
}

/// Complete leader-local lifecycle state for one inode.
#[derive(Clone, Debug)]
enum WriteSessionEntry {
    /// `OpenWrite` owns capacity and inode exclusion while Raft fencing is pending.
    Opening(OpeningSession),
    /// The durable epoch was acquired and the session may continue write operations.
    Active(WriteSession),
}

impl WriteSessionEntry {
    fn client_id(&self) -> ClientId {
        match self {
            Self::Opening(opening) => opening.open_client_id,
            Self::Active(session) => session.open_client_id,
        }
    }

    fn expires_at_ms(&self) -> u64 {
        match self {
            Self::Opening(opening) => opening.expires_at_ms,
            Self::Active(session) => session.expires_at_ms,
        }
    }

    fn ancestor_inode_ids(&self) -> &[InodeId] {
        match self {
            Self::Opening(opening) => &opening.ancestor_inode_ids,
            Self::Active(session) => &session.ancestor_inode_ids,
        }
    }

    fn is_opening(&self) -> bool {
        matches!(self, Self::Opening(_))
    }
}

/// One predecessor-addressed AddBlock step reserved before Raft allocation.
#[derive(Clone, Debug, PartialEq, Eq)]
struct PendingAddBlock {
    previous_block_id: Option<BlockId>,
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
    max_write_targets: usize,
    max_write_targets_per_session: usize,
    /// Fixed lifetime assigned at opening and extended by successful renewal.
    session_ttl_ms: u64,
}

/// Capacity boundary that rejected one write-session reservation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WriteSessionLimit {
    /// Process-wide opening plus active session capacity.
    Global,
    /// Opening plus active capacity attributed to one client ID.
    PerClient,
}

/// Capacity boundary that rejected one pending write target.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WriteTargetLimit {
    /// Pending plus issued targets across the Metadata process.
    Global,
    /// Pending plus issued targets owned by one write session.
    PerSession,
}

impl WriteTargetLimit {
    /// Stable low-cardinality label used by capacity metrics and logs.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Global => "global",
            Self::PerSession => "per_session",
        }
    }
}

/// Exact write-target limit reached before Raft block allocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WriteTargetLimitExceeded {
    pub(crate) limit: WriteTargetLimit,
    pub(crate) maximum: usize,
}

/// Outcome of beginning one predecessor-addressed AddBlock step.
pub(crate) enum BeginAddBlock<'a> {
    /// The logical step was already issued and can be replayed without capacity.
    Replay(WriteTarget),
    /// New capacity is reserved and must be completed or released before return.
    Reserved(WriteTargetReservation<'a>),
}

/// Exact failure returned before an AddBlock step may allocate through Raft.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum BeginAddBlockError {
    /// The active session no longer exists or the presented epoch is stale.
    Session(String),
    /// The registry's compact replay index no longer resolves to its target.
    Internal(String),
    /// The predecessor is invalid for the active session.
    InvalidArgument(String),
    /// An identical logical step is already allocating and should be retried.
    Pending,
    /// SyncWrite or CommitFile is freezing the issued-target sequence.
    PublicationInProgress,
    /// Leader-local target capacity is exhausted.
    LimitExceeded(WriteTargetLimitExceeded),
}

/// Exact failure returned while converting a pending AddBlock into an issued target.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CompleteWriteTargetError {
    /// Expiry, abort, or replacement removed the reservation's active session.
    NotCurrent,
    /// The completed target no longer matches the reserved session state.
    InvalidTarget(String),
}

/// Exact leader-local target capacity held across Raft allocation and placement.
///
/// Dropping this owner releases only the matching pending step. Completing it
/// atomically converts the pending slot into one issued target without changing
/// the total occupied target count.
#[must_use = "dropping the reservation releases pending write-target capacity"]
pub(crate) struct WriteTargetReservation<'a> {
    registry: &'a SessionRegistry,
    inode_id: InodeId,
    lease_epoch: u64,
    pending: PendingAddBlock,
    layout: FileLayout,
    open_client_id: ClientId,
    file_offset: u64,
    block_stamp: u64,
    armed: bool,
}

/// Exact leader-local ownership of a stable issued-target sequence.
///
/// While this owner is alive, new AddBlock steps are rejected before block
/// allocation. Dropping it releases only the matching publication identity.
#[must_use = "dropping the owner releases its write-publication boundary"]
pub(crate) struct WritePublication<'a> {
    registry: &'a SessionRegistry,
    session: WriteSession,
    publication_id: WritePublicationId,
    armed: bool,
}

/// Exact reason why SyncWrite or CommitFile cannot freeze a session snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum BeginWritePublicationError {
    /// The active session no longer exists or the presented epoch is stale.
    Session(String),
    /// One AddBlock step is already crossing allocation or placement.
    AddBlockPending,
    /// Another SyncWrite or CommitFile already owns the session boundary.
    PublicationInProgress,
    /// The process-local publication identity cannot advance without reuse.
    PublicationIdExhausted,
}

impl WritePublication<'_> {
    /// Return the exact session snapshot frozen for this publication.
    pub(crate) fn session(&self) -> &WriteSession {
        &self.session
    }

    /// Refresh the frozen session after asynchronous Worker readiness checks.
    pub(crate) fn revalidate(&self) -> Result<WriteSession, String> {
        self.registry.revalidate_publication(
            self.session.inode_id,
            self.session.lease_epoch,
            self.publication_id,
            current_time_ms(),
        )
    }

    /// Install the successful SyncWrite revision and release the boundary.
    pub(crate) fn complete_sync(mut self, content_revision: u64, file_size: u64) -> Result<(), String> {
        let result = self.registry.complete_sync_publication(
            self.session.inode_id,
            self.session.lease_epoch,
            self.publication_id,
            content_revision,
            file_size,
            current_time_ms(),
        );
        if result.is_ok() {
            self.armed = false;
        }
        result
    }

    /// Remove the successfully committed session and release all target counts.
    pub(crate) fn complete_commit(mut self) -> Result<(), String> {
        let result = self.registry.complete_commit_publication(
            self.session.inode_id,
            self.session.lease_epoch,
            self.publication_id,
        );
        if result.is_ok() {
            self.armed = false;
        }
        result
    }
}

impl WriteTargetReservation<'_> {
    /// Return the persisted layout that the allocated block must use.
    pub(crate) fn layout(&self) -> FileLayout {
        self.layout
    }

    /// Return the client identity embedded in the target fencing token.
    pub(crate) fn open_client_id(&self) -> ClientId {
        self.open_client_id
    }

    /// Return the file offset reserved for this logical AddBlock step.
    pub(crate) fn file_offset(&self) -> u64 {
        self.file_offset
    }

    /// Return the content revision stamp captured for this target attempt.
    pub(crate) fn block_stamp(&self) -> u64 {
        self.block_stamp
    }

    /// Atomically install a validated target in place of this pending slot.
    pub(crate) fn complete(mut self, target: WriteTarget) -> Result<WriteTarget, CompleteWriteTargetError> {
        let result = self.registry.complete_write_target(
            self.inode_id,
            self.lease_epoch,
            &self.pending,
            target,
            current_time_ms(),
        );
        self.armed = false;
        result
    }
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

/// Exact leader-local `Opening` ownership held across the Raft proposal.
///
/// Dropping the owner after cancellation or an early error removes only the
/// matching opening identity and releases all derived accounting atomically.
#[must_use = "dropping the opening releases its leader-local write session"]
pub(crate) struct WriteOpening<'a> {
    registry: &'a SessionRegistry,
    inode_id: InodeId,
    opening_id: WriteOpeningId,
    proposed_lease_epoch: u64,
    armed: bool,
}

impl WriteOpening<'_> {
    /// Return the exact epoch that the matching Raft command must acquire.
    pub(crate) fn proposed_lease_epoch(&self) -> u64 {
        self.proposed_lease_epoch
    }

    /// Atomically convert the matching, non-expired opening into an active session.
    pub(crate) fn activate(mut self, returned_lease_epoch: u64) -> Result<WriteSession, WriteOpeningError> {
        let result =
            self.registry
                .activate_opening(self.inode_id, self.opening_id, returned_lease_epoch, current_time_ms());
        if result.is_ok() || matches!(&result, Err(WriteOpeningError::NotCurrent | WriteOpeningError::Expired)) {
            self.armed = false;
        }
        result
    }
}

/// Exact reason why an opening cannot become an active session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WriteOpeningError {
    /// The opening expired before the Raft result could be installed.
    Expired,
    /// Cleanup or replacement removed the exact opening identity.
    NotCurrent,
    /// The Raft result did not match the proposed fencing epoch.
    LeaseEpochMismatch { expected: u64, got: u64 },
}

/// Exact leader-local failure returned while beginning one write session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BeginSessionError {
    /// Another non-expired opening or active session owns the inode.
    Busy,
    /// Admission capacity was exhausted before any Raft mutation.
    LimitExceeded(WriteSessionLimitExceeded),
    /// The durable fencing epoch cannot advance.
    LeaseEpochExhausted,
    /// The process-local opening identity cannot advance without reuse.
    OpeningIdExhausted,
    /// The captured namespace path is empty, cyclic, too deep, or ends elsewhere.
    InvalidAncestorChain,
}

/// Exact failure returned by active-session validation or renewal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WriteSessionError {
    /// No active session exists for the inode.
    NotFound,
    /// The presented epoch does not identify the active session.
    LeaseEpochMismatch { expected: u64, got: u64 },
    /// The presented client does not own the active session.
    OwnerMismatch,
    /// The active session has expired and was retired.
    Expired,
}

/// Primary write-session state and all indexes that must change atomically with it.
struct SessionRegistryState {
    /// At most one opening or active session exists for one inode.
    entries: HashMap<InodeId, WriteSessionEntry>,
    /// Number of primary entries still waiting for durable fencing.
    opening_sessions: usize,
    /// Opening plus active sessions attributed to each client ID.
    occupied_sessions_by_client: HashMap<ClientId, usize>,
    /// Bounded activity state for ancestors of both opening and active writes.
    ancestor_activity: HashMap<InodeId, AncestorWriteActivity>,
    /// All primary entries ordered by expiry for bounded cleanup.
    entries_by_expiry: BTreeSet<(u64, InodeId)>,
    /// Next process-local opening identity; zero is never issued.
    next_opening_id: u64,
    /// Next process-local publication identity; zero is never issued.
    next_publication_id: u64,
    /// Pending plus issued targets retained across every active session.
    outstanding_write_targets: usize,
    /// Subset of outstanding targets currently crossing allocation or placement.
    pending_write_targets: usize,
}

impl Default for SessionRegistryState {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            opening_sessions: 0,
            occupied_sessions_by_client: HashMap::new(),
            ancestor_activity: HashMap::new(),
            entries_by_expiry: BTreeSet::new(),
            next_opening_id: 1,
            next_publication_id: 1,
            outstanding_write_targets: 0,
            pending_write_targets: 0,
        }
    }
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
    pub(crate) fn new(
        max_sessions: usize,
        max_sessions_per_client: usize,
        max_write_targets: usize,
        max_write_targets_per_session: usize,
        session_ttl_ms: u64,
    ) -> Self {
        assert!(max_sessions > 0, "global write-session limit must be positive");
        assert!(
            max_sessions_per_client > 0 && max_sessions_per_client <= max_sessions,
            "per-client write-session limit must be positive and not exceed the global limit"
        );
        assert!(max_write_targets > 0, "global write-target limit must be positive");
        assert!(
            max_write_targets_per_session > 0 && max_write_targets_per_session <= max_write_targets,
            "per-session write-target limit must be positive and not exceed the global limit"
        );
        observe::set_write_sessions(0, 0);
        observe::set_write_targets(0, 0);
        Self {
            state: RwLock::new(SessionRegistryState::default()),
            max_sessions,
            max_sessions_per_client,
            max_write_targets,
            max_write_targets_per_session,
            session_ttl_ms,
        }
    }

    /// Admit one exact `OpenWrite` attempt before it proposes a durable epoch.
    ///
    /// The opening immediately owns inode exclusion, ancestor activity, and
    /// global and per-client capacity. Dropping the returned owner rolls back
    /// only this exact process-local identity.
    pub(crate) fn begin_session(&self, input: BeginSessionInput) -> Result<WriteOpening<'_>, BeginSessionError> {
        self.begin_session_at(input, current_time_ms())
    }

    fn begin_session_at(&self, input: BeginSessionInput, now_ms: u64) -> Result<WriteOpening<'_>, BeginSessionError> {
        Self::validate_ancestor_chain(input.inode_id, &input.ancestor_inode_ids)
            .map_err(|_| BeginSessionError::InvalidAncestorChain)?;

        let mut state = self.state.write();
        Self::retire_expired_entry_for_inode(&mut state, input.inode_id, now_ms);
        Self::retire_expired_entries(&mut state, now_ms);
        if state.entries.contains_key(&input.inode_id) {
            return Err(BeginSessionError::Busy);
        }
        if state.entries.len() >= self.max_sessions {
            observe::record_write_session_rejected(WriteSessionLimit::Global.label());
            return Err(BeginSessionError::LimitExceeded(WriteSessionLimitExceeded {
                limit: WriteSessionLimit::Global,
                maximum: self.max_sessions,
            }));
        }
        let client_occupied = state
            .occupied_sessions_by_client
            .get(&input.open_client_id)
            .copied()
            .unwrap_or_default();
        if client_occupied >= self.max_sessions_per_client {
            observe::record_write_session_rejected(WriteSessionLimit::PerClient.label());
            return Err(BeginSessionError::LimitExceeded(WriteSessionLimitExceeded {
                limit: WriteSessionLimit::PerClient,
                maximum: self.max_sessions_per_client,
            }));
        }

        let proposed_lease_epoch = input
            .current_lease_epoch
            .unwrap_or_default()
            .checked_add(1)
            .ok_or(BeginSessionError::LeaseEpochExhausted)?;
        let opening_id = WriteOpeningId(state.next_opening_id);
        state.next_opening_id = state
            .next_opening_id
            .checked_add(1)
            .ok_or(BeginSessionError::OpeningIdExhausted)?;
        let expires_at_ms = now_ms.saturating_add(self.session_ttl_ms);
        let inode_id = input.inode_id;
        let opening = OpeningSession {
            opening_id,
            inode_id,
            mount_id: input.mount_id,
            proposed_lease_epoch,
            base_size: input.base_size,
            content_revision: input.content_revision,
            mode: input.mode,
            open_client_id: input.open_client_id,
            layout: input.layout,
            expires_at_ms,
            ancestor_inode_ids: input.ancestor_inode_ids,
        };
        let entry = WriteSessionEntry::Opening(opening);
        Self::insert_entry(&mut state, entry);

        Ok(WriteOpening {
            registry: self,
            inode_id,
            opening_id,
            proposed_lease_epoch,
            armed: true,
        })
    }

    /// Convert only the matching, non-expired opening into an active session.
    fn activate_opening(
        &self,
        inode_id: InodeId,
        opening_id: WriteOpeningId,
        returned_lease_epoch: u64,
        now_ms: u64,
    ) -> Result<WriteSession, WriteOpeningError> {
        let mut state = self.state.write();
        let opening = match state.entries.get(&inode_id) {
            Some(WriteSessionEntry::Opening(opening)) if opening.opening_id == opening_id => opening.clone(),
            _ => return Err(WriteOpeningError::NotCurrent),
        };
        if opening.expires_at_ms <= now_ms {
            Self::retire_expired_entry_for_inode(&mut state, inode_id, now_ms);
            return Err(WriteOpeningError::Expired);
        }
        if opening.proposed_lease_epoch != returned_lease_epoch {
            return Err(WriteOpeningError::LeaseEpochMismatch {
                expected: opening.proposed_lease_epoch,
                got: returned_lease_epoch,
            });
        }

        let session = WriteSession {
            inode_id: opening.inode_id,
            mount_id: opening.mount_id,
            lease_epoch: opening.proposed_lease_epoch,
            base_size: opening.base_size,
            content_revision: opening.content_revision,
            mode: opening.mode,
            open_client_id: opening.open_client_id,
            layout: opening.layout,
            expires_at_ms: opening.expires_at_ms,
            ancestor_inode_ids: opening.ancestor_inode_ids,
            issued_targets: Vec::new(),
            issued_steps: HashMap::new(),
            pending_add_block: None,
            active_publication: None,
        };
        let previous = state
            .entries
            .insert(inode_id, WriteSessionEntry::Active(session.clone()));
        assert!(
            matches!(previous, Some(WriteSessionEntry::Opening(current)) if current.opening_id == opening_id),
            "validated write opening must remain current under the registry lock"
        );
        state.opening_sessions = state
            .opening_sessions
            .checked_sub(1)
            .expect("activated write session must own one opening count");
        Self::record_session_gauges(&state);
        Ok(session)
    }

    /// Replay an issued AddBlock step or reserve capacity before Raft allocation.
    ///
    /// Replay is resolved before capacity checks. A new step installs the one
    /// pending slot and increments global occupancy under the registry lock, so
    /// limit-plus-one cannot cross the subsequent Raft boundary concurrently.
    pub(crate) fn begin_add_block(
        &self,
        inode_id: InodeId,
        lease_epoch: u64,
        previous_block_id: Option<BlockId>,
    ) -> Result<BeginAddBlock<'_>, BeginAddBlockError> {
        let mut state = self.state.write();
        let now_ms = current_time_ms();
        Self::retire_expired_entry_for_inode(&mut state, inode_id, now_ms);
        Self::retire_expired_entries(&mut state, now_ms);
        let pending = PendingAddBlock { previous_block_id };
        let (layout, open_client_id, file_offset, block_stamp) = {
            let session = Self::active_session_mut(&mut state, inode_id).map_err(BeginAddBlockError::Session)?;
            if session.lease_epoch != lease_epoch {
                return Err(BeginAddBlockError::Session(
                    "write session lease epoch mismatch".to_string(),
                ));
            }
            if let Some(target_index) = session.issued_steps.get(&previous_block_id) {
                let target = session.issued_targets.get(*target_index).cloned().ok_or_else(|| {
                    BeginAddBlockError::Internal("issued AddBlock target index is inconsistent".to_string())
                })?;
                return Ok(BeginAddBlock::Replay(target));
            }

            if session.active_publication.is_some() {
                return Err(BeginAddBlockError::PublicationInProgress);
            }

            let expected_previous = session.issued_targets.last().map(|target| target.block_id);
            if previous_block_id != expected_previous {
                return Err(BeginAddBlockError::InvalidArgument(format!(
                    "AddBlock predecessor mismatch: expected {expected_previous:?}, got {previous_block_id:?}"
                )));
            }
            if session.pending_add_block.is_some() {
                return Err(BeginAddBlockError::Pending);
            }
            let file_offset = Self::next_target_file_offset(session).map_err(BeginAddBlockError::InvalidArgument)?;
            let block_stamp = session
                .content_revision
                .checked_add(1)
                .ok_or_else(|| BeginAddBlockError::InvalidArgument("content revision overflow".to_string()))?;
            if session.issued_targets.len() >= self.max_write_targets_per_session {
                observe::record_write_target_rejected(WriteTargetLimit::PerSession.label());
                return Err(BeginAddBlockError::LimitExceeded(WriteTargetLimitExceeded {
                    limit: WriteTargetLimit::PerSession,
                    maximum: self.max_write_targets_per_session,
                }));
            }
            (session.layout, session.open_client_id, file_offset, block_stamp)
        };

        if state.outstanding_write_targets >= self.max_write_targets {
            observe::record_write_target_rejected(WriteTargetLimit::Global.label());
            return Err(BeginAddBlockError::LimitExceeded(WriteTargetLimitExceeded {
                limit: WriteTargetLimit::Global,
                maximum: self.max_write_targets,
            }));
        }
        let session = Self::active_session_mut(&mut state, inode_id)
            .expect("validated active session must remain current under the registry lock");
        assert!(session.pending_add_block.replace(pending.clone()).is_none());
        state.outstanding_write_targets = state
            .outstanding_write_targets
            .checked_add(1)
            .expect("write-target occupancy below its limit must increment");
        state.pending_write_targets = state
            .pending_write_targets
            .checked_add(1)
            .expect("pending write-target occupancy must increment");
        Self::record_write_target_gauges(&state);

        Ok(BeginAddBlock::Reserved(WriteTargetReservation {
            registry: self,
            inode_id,
            lease_epoch,
            pending,
            layout,
            open_client_id,
            file_offset,
            block_stamp,
            armed: true,
        }))
    }

    /// Freeze one active session's issued-target sequence for file publication.
    ///
    /// The pending-target check and publication identity installation happen
    /// under the same lock used by AddBlock, closing both allocation-completion
    /// and pre-proposal races.
    pub(crate) fn begin_publication(
        &self,
        inode_id: InodeId,
        lease_epoch: u64,
    ) -> Result<WritePublication<'_>, BeginWritePublicationError> {
        let mut state = self.state.write();
        let now_ms = current_time_ms();
        Self::retire_expired_entry_for_inode(&mut state, inode_id, now_ms);
        Self::retire_expired_entries(&mut state, now_ms);
        {
            let session =
                Self::active_session_mut(&mut state, inode_id).map_err(BeginWritePublicationError::Session)?;
            if session.lease_epoch != lease_epoch {
                return Err(BeginWritePublicationError::Session(
                    "write session lease epoch mismatch".to_string(),
                ));
            }
            if session.pending_add_block.is_some() {
                return Err(BeginWritePublicationError::AddBlockPending);
            }
            if session.active_publication.is_some() {
                return Err(BeginWritePublicationError::PublicationInProgress);
            }
        }

        let publication_id = WritePublicationId(state.next_publication_id);
        state.next_publication_id = state
            .next_publication_id
            .checked_add(1)
            .ok_or(BeginWritePublicationError::PublicationIdExhausted)?;
        let session = Self::active_session_mut(&mut state, inode_id)
            .expect("validated active session must remain current under the registry lock");
        assert!(session.active_publication.replace(publication_id).is_none());
        let session = session.clone();
        Ok(WritePublication {
            registry: self,
            session,
            publication_id,
            armed: true,
        })
    }

    /// Replace only the matching pending step with one fully validated target.
    fn complete_write_target(
        &self,
        inode_id: InodeId,
        lease_epoch: u64,
        pending: &PendingAddBlock,
        target: WriteTarget,
        now_ms: u64,
    ) -> Result<WriteTarget, CompleteWriteTargetError> {
        let mut state = self.state.write();
        Self::retire_expired_entry_for_inode(&mut state, inode_id, now_ms);
        Self::retire_expired_entries(&mut state, now_ms);
        let validation = {
            let session =
                Self::active_session_mut(&mut state, inode_id).map_err(|_| CompleteWriteTargetError::NotCurrent)?;
            if session.lease_epoch != lease_epoch {
                return Err(CompleteWriteTargetError::NotCurrent);
            }
            if session.pending_add_block.as_ref() != Some(pending) {
                return Err(CompleteWriteTargetError::NotCurrent);
            }
            Self::validate_write_target(session, lease_epoch, &target)
        };
        if let Err(error) = validation {
            Self::cancel_write_target_locked(&mut state, inode_id, lease_epoch, pending);
            return Err(CompleteWriteTargetError::InvalidTarget(error));
        }

        let session = Self::active_session_mut(&mut state, inode_id)
            .expect("validated active session must remain current under the registry lock");
        let target_index = session.issued_targets.len();
        session.issued_targets.push(target.clone());
        assert!(
            session
                .issued_steps
                .insert(pending.previous_block_id, target_index)
                .is_none(),
            "reserved AddBlock predecessor must not already be issued"
        );
        assert_eq!(session.pending_add_block.take().as_ref(), Some(pending));
        state.pending_write_targets = state
            .pending_write_targets
            .checked_sub(1)
            .expect("completed target must own one pending count");
        Self::record_write_target_gauges(&state);
        Ok(target)
    }

    /// Revalidate fencing, layout, offset, and revision before issuing a reserved target.
    fn validate_write_target(session: &WriteSession, lease_epoch: u64, target: &WriteTarget) -> Result<(), String> {
        if target.block_id.inode_id != session.inode_id {
            return Err("write target inode mismatch".to_string());
        }
        if target.fencing_token.block_id != target.block_id
            || target.fencing_token.owner != session.open_client_id
            || target.fencing_token.epoch != lease_epoch
        {
            return Err("write target fencing token mismatch".to_string());
        }
        let next_file_offset = Self::next_target_file_offset(session)?;
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
            target.block_size,
        )
        .map_err(|error| format!("invalid write target shape: {error}"))?;
        let expected_shape = BlockShape::for_effective_len(&session.layout, u64::from(session.layout.block_size))
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
        Ok(())
    }

    /// Return the next capacity-aligned target offset for the active session.
    ///
    /// Targets beginning below `base_size` belong to the already published
    /// prefix, including a partial block finalized by a previous SyncWrite.
    /// An unpublished target begins at or after `base_size` and advances the
    /// next offset by its full authorized capacity.
    fn next_target_file_offset(session: &WriteSession) -> Result<u64, String> {
        let Some(last) = session.issued_targets.last() else {
            return Ok(session.base_size);
        };
        if last.file_offset < session.base_size {
            return Ok(session.base_size);
        }
        last.file_offset
            .checked_add(last.block_size)
            .ok_or_else(|| "write target file offset overflow".to_string())
    }

    /// Get a non-expired write session after bounded global and exact-inode retirement.
    pub fn get_session(&self, inode_id: InodeId) -> Option<WriteSession> {
        let mut state = self.state.write();
        let now_ms = current_time_ms();
        Self::retire_expired_entry_for_inode(&mut state, inode_id, now_ms);
        Self::retire_expired_entries(&mut state, now_ms);
        match state.entries.get(&inode_id) {
            Some(WriteSessionEntry::Active(session)) => Some(session.clone()),
            Some(WriteSessionEntry::Opening(_)) | None => None,
        }
    }

    /// Get a lightweight active-session snapshot for admission, preflight, or presence checks.
    pub(crate) fn get_session_identity(&self, inode_id: InodeId) -> Option<WriteSessionIdentity> {
        let mut state = self.state.write();
        let now_ms = current_time_ms();
        Self::retire_expired_entry_for_inode(&mut state, inode_id, now_ms);
        Self::retire_expired_entries(&mut state, now_ms);
        match state.entries.get(&inode_id) {
            Some(WriteSessionEntry::Active(session)) => Some(WriteSessionIdentity {
                mount_id: session.mount_id,
                lease_epoch: session.lease_epoch,
                open_client_id: session.open_client_id,
            }),
            Some(WriteSessionEntry::Opening(_)) | None => None,
        }
    }

    /// Remove only the session identified by the presented lease epoch.
    pub fn remove_session_if_epoch(&self, inode_id: InodeId, lease_epoch: u64) -> Option<WriteSession> {
        let mut state = self.state.write();
        match state.entries.get(&inode_id) {
            Some(WriteSessionEntry::Active(session)) if session.lease_epoch == lease_epoch => {}
            Some(WriteSessionEntry::Opening(_) | WriteSessionEntry::Active(_)) | None => return None,
        }
        match Self::remove_entry(&mut state, inode_id) {
            Some(WriteSessionEntry::Active(session)) => Some(session),
            Some(WriteSessionEntry::Opening(_)) | None => {
                unreachable!("validated active session must remain current under the registry lock")
            }
        }
    }

    /// Validate that a non-expired active session owns the presented epoch.
    pub(crate) fn validate_session(&self, inode_id: InodeId, lease_epoch: u64) -> Result<(), WriteSessionError> {
        let mut state = self.state.write();
        let now_ms = current_time_ms();
        if state
            .entries
            .get(&inode_id)
            .is_some_and(|entry| entry.expires_at_ms() <= now_ms)
        {
            Self::retire_expired_entry_for_inode(&mut state, inode_id, now_ms);
            return Err(WriteSessionError::Expired);
        }
        Self::retire_expired_entries(&mut state, now_ms);
        let session = match state.entries.get(&inode_id) {
            Some(WriteSessionEntry::Active(session)) => session,
            Some(WriteSessionEntry::Opening(_)) | None => return Err(WriteSessionError::NotFound),
        };
        if session.lease_epoch != lease_epoch {
            return Err(WriteSessionError::LeaseEpochMismatch {
                expected: session.lease_epoch,
                got: lease_epoch,
            });
        }
        Ok(())
    }

    /// Atomically validate ownership and move every expiry index forward.
    pub(crate) fn renew_session(
        &self,
        inode_id: InodeId,
        lease_epoch: u64,
        client_id: ClientId,
    ) -> Result<u64, WriteSessionError> {
        self.renew_session_at(inode_id, lease_epoch, client_id, current_time_ms())
    }

    fn renew_session_at(
        &self,
        inode_id: InodeId,
        lease_epoch: u64,
        client_id: ClientId,
        now_ms: u64,
    ) -> Result<u64, WriteSessionError> {
        let mut state = self.state.write();
        if state
            .entries
            .get(&inode_id)
            .is_some_and(|entry| entry.expires_at_ms() <= now_ms)
        {
            Self::retire_expired_entry_for_inode(&mut state, inode_id, now_ms);
            return Err(WriteSessionError::Expired);
        }
        Self::retire_expired_entries(&mut state, now_ms);
        let (ancestor_inode_ids, previous_expires_at_ms) = match state.entries.get(&inode_id) {
            Some(WriteSessionEntry::Active(session)) => {
                if session.lease_epoch != lease_epoch {
                    return Err(WriteSessionError::LeaseEpochMismatch {
                        expected: session.lease_epoch,
                        got: lease_epoch,
                    });
                }
                if session.open_client_id != client_id {
                    return Err(WriteSessionError::OwnerMismatch);
                }
                (session.ancestor_inode_ids.clone(), session.expires_at_ms)
            }
            Some(WriteSessionEntry::Opening(_)) | None => return Err(WriteSessionError::NotFound),
        };
        let expires_at_ms = now_ms.saturating_add(self.session_ttl_ms).max(previous_expires_at_ms);

        assert!(
            state.entries_by_expiry.remove(&(previous_expires_at_ms, inode_id)),
            "active write session expiry index must exist"
        );
        state.entries_by_expiry.insert((expires_at_ms, inode_id));
        for ancestor_inode_id in &ancestor_inode_ids {
            let activity = state
                .ancestor_activity
                .get_mut(ancestor_inode_id)
                .expect("active write session ancestor index must exist");
            Self::decrement_expiry_count(&mut activity.sessions_by_expiry, previous_expires_at_ms);
            *activity.sessions_by_expiry.entry(expires_at_ms).or_default() += 1;
        }
        match state.entries.get_mut(&inode_id) {
            Some(WriteSessionEntry::Active(session)) => session.expires_at_ms = expires_at_ms,
            Some(WriteSessionEntry::Opening(_)) | None => {
                unreachable!("validated active session must remain current under the registry lock")
            }
        }
        Ok(expires_at_ms)
    }

    /// Retire at most one bounded batch of expired opening and active sessions.
    pub(crate) fn retire_expired_batch(&self) -> usize {
        let mut state = self.state.write();
        Self::retire_expired_entries(&mut state, current_time_ms())
    }

    /// Return whether this exact inode has a non-expired opening or active session.
    pub(crate) fn has_active_write(&self, inode_id: InodeId) -> bool {
        self.has_active_write_under(inode_id)
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
        Self::retire_expired_entries(&mut state, now_ms);
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

    /// Revalidate that this exact publication still owns a non-expired session.
    fn revalidate_publication(
        &self,
        inode_id: InodeId,
        lease_epoch: u64,
        publication_id: WritePublicationId,
        now_ms: u64,
    ) -> Result<WriteSession, String> {
        let mut state = self.state.write();
        Self::retire_expired_entry_for_inode(&mut state, inode_id, now_ms);
        Self::retire_expired_entries(&mut state, now_ms);
        let session = Self::active_session_mut(&mut state, inode_id)?;
        if session.lease_epoch != lease_epoch {
            return Err("write session lease epoch mismatch".to_string());
        }
        if session.active_publication != Some(publication_id) {
            return Err("write publication is no longer current".to_string());
        }
        Ok(session.clone())
    }

    /// Apply one successful SyncWrite result and release its exact ownership.
    fn complete_sync_publication(
        &self,
        inode_id: InodeId,
        lease_epoch: u64,
        publication_id: WritePublicationId,
        content_revision: u64,
        file_size: u64,
        now_ms: u64,
    ) -> Result<(), String> {
        let mut state = self.state.write();
        Self::retire_expired_entry_for_inode(&mut state, inode_id, now_ms);
        Self::retire_expired_entries(&mut state, now_ms);
        let session = Self::active_session_mut(&mut state, inode_id)?;
        if session.lease_epoch != lease_epoch {
            return Err("write session lease epoch mismatch".to_string());
        }
        if session.active_publication != Some(publication_id) {
            return Err("write publication is no longer current".to_string());
        }
        if session.content_revision == content_revision {
            if session.base_size != file_size {
                return Err(format!(
                    "replayed SyncWrite size changed: expected {}, got {file_size}",
                    session.base_size
                ));
            }
            session.active_publication = None;
            return Ok(());
        }
        let expected_content_revision = session
            .content_revision
            .checked_add(1)
            .ok_or_else(|| "content revision overflow".to_string())?;
        if content_revision != expected_content_revision {
            return Err(format!(
                "SyncWrite content revision changed: expected {expected_content_revision}, got {content_revision}"
            ));
        }

        // Before the new revision is installed, every target at or beyond the
        // visible end still belongs to the old session revision. Removing that
        // suffix prevents stale capacity-based offsets from being replayed.
        let retained_target_count = session
            .issued_targets
            .partition_point(|target| target.file_offset < file_size);
        let removed_target_count = session.issued_targets.len() - retained_target_count;
        session.issued_targets.truncate(retained_target_count);
        session
            .issued_steps
            .retain(|_, target_index| *target_index < retained_target_count);
        session.content_revision = content_revision;
        session.base_size = file_size;
        session.active_publication = None;
        state.outstanding_write_targets = state
            .outstanding_write_targets
            .checked_sub(removed_target_count)
            .expect("discarded issued targets must be included in global occupancy");
        Self::record_write_target_gauges(&state);
        Ok(())
    }

    /// Remove the active session only when the successful CommitFile still owns it.
    fn complete_commit_publication(
        &self,
        inode_id: InodeId,
        lease_epoch: u64,
        publication_id: WritePublicationId,
    ) -> Result<(), String> {
        let mut state = self.state.write();
        let matches = matches!(
            state.entries.get(&inode_id),
            Some(WriteSessionEntry::Active(session))
                if session.lease_epoch == lease_epoch
                    && session.active_publication == Some(publication_id)
        );
        if !matches {
            return Err("write publication is no longer current".to_string());
        }
        match Self::remove_entry(&mut state, inode_id) {
            Some(WriteSessionEntry::Active(_)) => Ok(()),
            Some(WriteSessionEntry::Opening(_)) | None => {
                unreachable!("validated publication must belong to an active session")
            }
        }
    }

    /// Release only the matching publication so stale owners cannot clear new state.
    fn cancel_publication(&self, inode_id: InodeId, lease_epoch: u64, publication_id: WritePublicationId) {
        let mut state = self.state.write();
        if let Ok(session) = Self::active_session_mut(&mut state, inode_id) {
            if session.lease_epoch == lease_epoch && session.active_publication == Some(publication_id) {
                session.active_publication = None;
            }
        }
    }

    /// Remove one exact opening after its owner is cancelled or returns early.
    fn cancel_opening(&self, inode_id: InodeId, opening_id: WriteOpeningId) {
        let mut state = self.state.write();
        if matches!(
            state.entries.get(&inode_id),
            Some(WriteSessionEntry::Opening(opening)) if opening.opening_id == opening_id
        ) {
            Self::remove_entry(&mut state, inode_id);
        }
    }

    /// Release only the matching pending AddBlock step after failure or cancellation.
    fn cancel_write_target(&self, inode_id: InodeId, lease_epoch: u64, pending: &PendingAddBlock) {
        let mut state = self.state.write();
        Self::cancel_write_target_locked(&mut state, inode_id, lease_epoch, pending);
    }

    /// Release only the exact pending step so a stale guard cannot cancel replacement state.
    fn cancel_write_target_locked(
        state: &mut SessionRegistryState,
        inode_id: InodeId,
        lease_epoch: u64,
        pending: &PendingAddBlock,
    ) -> bool {
        let matches = matches!(
            state.entries.get(&inode_id),
            Some(WriteSessionEntry::Active(session))
                if session.lease_epoch == lease_epoch && session.pending_add_block.as_ref() == Some(pending)
        );
        if !matches {
            return false;
        }
        let session = Self::active_session_mut(state, inode_id)
            .expect("matching pending target must belong to an active session");
        assert_eq!(session.pending_add_block.take().as_ref(), Some(pending));
        state.outstanding_write_targets = state
            .outstanding_write_targets
            .checked_sub(1)
            .expect("pending target must own one outstanding count");
        state.pending_write_targets = state
            .pending_write_targets
            .checked_sub(1)
            .expect("pending target must own one pending count");
        Self::record_write_target_gauges(state);
        true
    }

    /// Insert one primary entry and every derived index under the state lock.
    fn insert_entry(state: &mut SessionRegistryState, entry: WriteSessionEntry) {
        let inode_id = match &entry {
            WriteSessionEntry::Opening(opening) => opening.inode_id,
            WriteSessionEntry::Active(session) => session.inode_id,
        };
        let client_id = entry.client_id();
        let expires_at_ms = entry.expires_at_ms();
        for ancestor_inode_id in entry.ancestor_inode_ids() {
            let activity = state
                .ancestor_activity
                .entry(*ancestor_inode_id)
                .or_insert(AncestorWriteActivity {
                    sessions_by_expiry: BTreeMap::new(),
                });
            *activity.sessions_by_expiry.entry(expires_at_ms).or_default() += 1;
        }
        assert!(
            state.entries_by_expiry.insert((expires_at_ms, inode_id)),
            "new write session expiry index must be unique"
        );
        if entry.is_opening() {
            state.opening_sessions += 1;
        }
        *state.occupied_sessions_by_client.entry(client_id).or_default() += 1;
        assert!(state.entries.insert(inode_id, entry).is_none());
        Self::record_session_gauges(state);
    }

    /// Remove one primary entry and every derived index under the state lock.
    fn remove_entry(state: &mut SessionRegistryState, inode_id: InodeId) -> Option<WriteSessionEntry> {
        let entry = state.entries.get(&inode_id)?;
        let client_id = entry.client_id();
        let (owned_write_targets, pending_write_targets) = match entry {
            WriteSessionEntry::Opening(_) => (0, 0),
            WriteSessionEntry::Active(session) => (
                session.issued_targets.len() + usize::from(session.pending_add_block.is_some()),
                usize::from(session.pending_add_block.is_some()),
            ),
        };
        assert!(
            state
                .occupied_sessions_by_client
                .get(&client_id)
                .copied()
                .unwrap_or_default()
                > 0,
            "write session entry must own one client capacity slot"
        );
        let entry = state.entries.remove(&inode_id)?;
        if entry.is_opening() {
            state.opening_sessions = state
                .opening_sessions
                .checked_sub(1)
                .expect("opening entry must own one opening count");
        }
        Self::remove_from_indexes(state, inode_id, &entry);
        Self::decrement_client_occupancy(state, client_id);
        state.outstanding_write_targets = state
            .outstanding_write_targets
            .checked_sub(owned_write_targets)
            .expect("removed session targets must be included in global occupancy");
        state.pending_write_targets = state
            .pending_write_targets
            .checked_sub(pending_write_targets)
            .expect("removed session pending target must be included in pending occupancy");
        Self::record_session_gauges(state);
        Self::record_write_target_gauges(state);
        Some(entry)
    }

    /// Decrement one opening-or-active client slot and remove empty keys.
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

    /// Remove the exact global-expiry and ancestor-expiry entries for an entry.
    fn remove_from_indexes(state: &mut SessionRegistryState, inode_id: InodeId, entry: &WriteSessionEntry) {
        let expires_at_ms = entry.expires_at_ms();
        assert!(
            state.entries_by_expiry.remove(&(expires_at_ms, inode_id)),
            "write session expiry index must exist"
        );
        for ancestor_inode_id in entry.ancestor_inode_ids() {
            let remove_entry = {
                let activity = state
                    .ancestor_activity
                    .get_mut(ancestor_inode_id)
                    .expect("write session ancestor index must exist");
                Self::decrement_expiry_count(&mut activity.sessions_by_expiry, expires_at_ms);
                activity.sessions_by_expiry.is_empty()
            };
            if remove_entry {
                state.ancestor_activity.remove(ancestor_inode_id);
            }
        }
    }

    /// Retire the earliest expired entries without exceeding the sweep budget.
    fn retire_expired_entries(state: &mut SessionRegistryState, now_ms: u64) -> usize {
        let mut retired = 0;
        while retired < MAX_EXPIRED_SESSION_RETIREMENTS_PER_CALL {
            let Some(&(expires_at_ms, inode_id)) = state.entries_by_expiry.first() else {
                break;
            };
            if expires_at_ms > now_ms {
                break;
            }
            if Self::remove_entry(state, inode_id).is_none() {
                state.entries_by_expiry.remove(&(expires_at_ms, inode_id));
            }
            observe::record_write_session_expired();
            retired += 1;
        }
        retired
    }

    /// Retire one requested inode even when it lies beyond the sweep budget.
    fn retire_expired_entry_for_inode(state: &mut SessionRegistryState, inode_id: InodeId, now_ms: u64) -> bool {
        let is_expired = state
            .entries
            .get(&inode_id)
            .is_some_and(|entry| entry.expires_at_ms() <= now_ms);
        if is_expired && Self::remove_entry(state, inode_id).is_some() {
            observe::record_write_session_expired();
            return true;
        }
        false
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

    fn active_session_mut(state: &mut SessionRegistryState, inode_id: InodeId) -> Result<&mut WriteSession, String> {
        match state.entries.get_mut(&inode_id) {
            Some(WriteSessionEntry::Active(session)) => Ok(session),
            Some(WriteSessionEntry::Opening(_)) => Err("write session is still opening".to_string()),
            None => Err("write session not found".to_string()),
        }
    }

    fn record_session_gauges(state: &SessionRegistryState) {
        let active_sessions = state
            .entries
            .len()
            .checked_sub(state.opening_sessions)
            .expect("opening session count cannot exceed primary entries");
        observe::set_write_sessions(state.opening_sessions, active_sessions);
    }

    /// Publish issued occupancy as total outstanding capacity minus pending reservations.
    fn record_write_target_gauges(state: &SessionRegistryState) {
        let issued = state
            .outstanding_write_targets
            .checked_sub(state.pending_write_targets)
            .expect("pending write targets cannot exceed total occupancy");
        observe::set_write_targets(state.pending_write_targets, issued);
    }
}

impl Drop for WriteOpening<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.registry.cancel_opening(self.inode_id, self.opening_id);
            self.armed = false;
        }
    }
}

impl Drop for WriteTargetReservation<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.registry
                .cancel_write_target(self.inode_id, self.lease_epoch, &self.pending);
            self.armed = false;
        }
    }
}

impl Drop for WritePublication<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.registry
                .cancel_publication(self.session.inode_id, self.session.lease_epoch, self.publication_id);
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
        let config = crate::config::MetadataConfig::default();
        Self::new(
            config.write_session_limits.max_active,
            config.write_session_limits.max_active_per_client,
            config.write_target_limits.max_outstanding,
            config.write_target_limits.max_outstanding_per_session,
            config.write_lease_timeout_ms,
        )
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

    fn create_input(inode_id: InodeId) -> BeginSessionInput {
        BeginSessionInput {
            inode_id,
            mount_id: MountId::new(1),
            current_lease_epoch: Some(6),
            base_size: 0,
            content_revision: 0,
            mode: WriteMode::Write,
            open_client_id: ClientId::new(1),
            layout: FileLayout::new(64, 64, 1),
            ancestor_inode_ids: vec![inode_id],
        }
    }

    fn install_session(registry: &SessionRegistry, input: BeginSessionInput) -> Result<WriteSession, String> {
        let opening = registry
            .begin_session(input)
            .map_err(|error| format!("write session opening failed: {error:?}"))?;
        let lease_epoch = opening.proposed_lease_epoch();
        opening
            .activate(lease_epoch)
            .map_err(|error| format!("write session activation failed: {error:?}"))
    }

    fn install_session_at(
        registry: &SessionRegistry,
        input: BeginSessionInput,
        now_ms: u64,
    ) -> Result<WriteSession, String> {
        let mut opening = registry
            .begin_session_at(input, now_ms)
            .map_err(|error| format!("write session opening failed: {error:?}"))?;
        let result = registry.activate_opening(
            opening.inode_id,
            opening.opening_id,
            opening.proposed_lease_epoch,
            now_ms,
        );
        if result.is_ok() || matches!(&result, Err(WriteOpeningError::NotCurrent | WriteOpeningError::Expired)) {
            opening.armed = false;
        }
        result.map_err(|error| format!("write session activation failed: {error:?}"))
    }

    fn begin_opening(
        registry: &SessionRegistry,
        inode_id: InodeId,
        client_id: ClientId,
    ) -> Result<WriteOpening<'_>, BeginSessionError> {
        let mut input = create_input(inode_id);
        input.open_client_id = client_id;
        registry.begin_session(input)
    }

    fn issue_target(
        registry: &SessionRegistry,
        inode_id: InodeId,
        previous_block_id: Option<BlockId>,
        index: u32,
        file_offset: u64,
        block_stamp: u64,
    ) -> WriteTarget {
        let mut target = write_target(inode_id, index);
        target.file_offset = file_offset;
        target.block_stamp = block_stamp;
        let reservation = match registry.begin_add_block(inode_id, 7, previous_block_id).unwrap() {
            BeginAddBlock::Reserved(reservation) => reservation,
            BeginAddBlock::Replay(_) => panic!("new test target must reserve capacity"),
        };
        reservation.complete(target).unwrap()
    }

    #[test]
    fn opening_and_active_sessions_share_global_capacity() {
        let registry = SessionRegistry::new(2, 2, 100, 100, 60_000);
        install_session(&registry, create_input(InodeId::new(1))).unwrap();
        let opening = begin_opening(&registry, InodeId::new(2), ClientId::new(1)).unwrap();

        let rejection = match begin_opening(&registry, InodeId::new(3), ClientId::new(2)) {
            Ok(_) => panic!("global capacity must reject limit plus one"),
            Err(rejection) => rejection,
        };
        assert_eq!(
            rejection,
            BeginSessionError::LimitExceeded(WriteSessionLimitExceeded {
                limit: WriteSessionLimit::Global,
                maximum: 2,
            })
        );

        drop(opening);
        let replacement = begin_opening(&registry, InodeId::new(3), ClientId::new(2)).unwrap();
        drop(replacement);
        assert!(registry.remove_session_if_epoch(InodeId::new(1), 7).is_some());
        let state = registry.state.read();
        assert_eq!(state.opening_sessions, 0);
        assert!(state.entries.is_empty());
        assert!(state.occupied_sessions_by_client.is_empty());
    }

    #[test]
    fn per_client_capacity_does_not_block_another_client() {
        let registry = SessionRegistry::new(3, 1, 100, 100, 60_000);
        install_session(&registry, create_input(InodeId::new(1))).unwrap();

        let rejection = match begin_opening(&registry, InodeId::new(2), ClientId::new(1)) {
            Ok(_) => panic!("per-client capacity must reject limit plus one"),
            Err(rejection) => rejection,
        };
        assert_eq!(
            rejection,
            BeginSessionError::LimitExceeded(WriteSessionLimitExceeded {
                limit: WriteSessionLimit::PerClient,
                maximum: 1,
            })
        );
        let other_client = begin_opening(&registry, InodeId::new(2), ClientId::new(2)).unwrap();
        drop(other_client);
    }

    #[test]
    fn dropped_or_invalid_openings_restore_capacity() {
        let registry = SessionRegistry::new(1, 1, 100, 100, 60_000);
        let opening = begin_opening(&registry, InodeId::new(1), ClientId::new(1)).unwrap();
        drop(opening);

        let mut invalid = create_input(InodeId::new(1));
        invalid.ancestor_inode_ids.clear();
        match registry.begin_session(invalid) {
            Err(error) => assert_eq!(error, BeginSessionError::InvalidAncestorChain),
            Ok(_) => panic!("invalid ancestor chain must be rejected"),
        }

        let replacement = begin_opening(&registry, InodeId::new(1), ClientId::new(1)).unwrap();
        drop(replacement);
        let state = registry.state.read();
        assert_eq!(state.opening_sessions, 0);
        assert!(state.occupied_sessions_by_client.is_empty());
    }

    #[test]
    fn opening_owns_inode_ancestor_and_capacity_until_drop() {
        let registry = SessionRegistry::new(1, 1, 100, 100, 60_000);
        let inode_id = InodeId::new(30);
        let root_inode_id = InodeId::new(1);
        let mut input = create_input(inode_id);
        input.ancestor_inode_ids = vec![root_inode_id, inode_id];
        let opening = registry.begin_session(input.clone()).unwrap();

        assert!(registry.has_active_write(inode_id));
        assert!(registry.has_active_write_under(root_inode_id));
        assert!(matches!(registry.begin_session(input), Err(BeginSessionError::Busy)));

        drop(opening);
        assert!(!registry.has_active_write(inode_id));
        assert!(!registry.has_active_write_under(root_inode_id));
        assert!(registry.state.read().occupied_sessions_by_client.is_empty());
    }

    #[test]
    fn stale_opening_drop_cannot_remove_replacement_with_same_proposed_epoch() {
        let registry = SessionRegistry::new(1, 1, 100, 100, 1);
        let inode_id = InodeId::new(31);
        let input = create_input(inode_id);
        let stale = registry.begin_session_at(input.clone(), 0).unwrap();
        let mut replacement = registry.begin_session_at(input, 1).unwrap();
        assert_eq!(stale.proposed_lease_epoch, replacement.proposed_lease_epoch);
        assert_ne!(stale.opening_id, replacement.opening_id);

        drop(stale);
        assert!(matches!(
            registry.state.read().entries.get(&inode_id),
            Some(WriteSessionEntry::Opening(opening)) if opening.opening_id == replacement.opening_id
        ));

        let session = registry
            .activate_opening(
                replacement.inode_id,
                replacement.opening_id,
                replacement.proposed_lease_epoch,
                1,
            )
            .unwrap();
        replacement.armed = false;
        assert_eq!(session.lease_epoch, 7);
    }

    #[test]
    fn expired_opening_cannot_activate_and_releases_all_indexes() {
        let registry = SessionRegistry::new(1, 1, 100, 100, 1);
        let inode_id = InodeId::new(32);
        let opening = registry.begin_session_at(create_input(inode_id), 0).unwrap();

        assert!(matches!(
            registry.activate_opening(inode_id, opening.opening_id, opening.proposed_lease_epoch, 1),
            Err(WriteOpeningError::Expired)
        ));
        drop(opening);

        let state = registry.state.read();
        assert!(state.entries.is_empty());
        assert!(state.entries_by_expiry.is_empty());
        assert!(state.ancestor_activity.is_empty());
        assert!(state.occupied_sessions_by_client.is_empty());
    }

    #[test]
    fn epoch_and_opening_identity_exhaustion_do_not_consume_registry_state() {
        let registry = SessionRegistry::new(1, 1, 100, 100, 60_000);
        let inode_id = InodeId::new(34);
        let mut exhausted_epoch = create_input(inode_id);
        exhausted_epoch.current_lease_epoch = Some(u64::MAX);
        assert!(matches!(
            registry.begin_session(exhausted_epoch),
            Err(BeginSessionError::LeaseEpochExhausted)
        ));
        assert_eq!(registry.state.read().next_opening_id, 1);

        registry.state.write().next_opening_id = u64::MAX;
        assert!(matches!(
            registry.begin_session(create_input(inode_id)),
            Err(BeginSessionError::OpeningIdExhausted)
        ));

        let state = registry.state.read();
        assert!(state.entries.is_empty());
        assert_eq!(state.opening_sessions, 0);
        assert!(state.entries_by_expiry.is_empty());
        assert!(state.ancestor_activity.is_empty());
        assert!(state.occupied_sessions_by_client.is_empty());
    }

    #[tokio::test]
    async fn maintenance_runtime_retires_expired_sessions_and_shuts_down() {
        use crate::config::{BlockCleanupConfig, NamespaceDeleteConfig, RaftConfig};
        use crate::maintenance::{BlockCleanupCoordinator, DetachedRootReclaimer, MaintenanceService};
        use crate::mount::MountTable;
        use crate::raft::{AppRaftNode, AppRaftStateMachine, RocksDBStorage};
        use crate::worker::WorkerManager;
        use beryl_types::GroupName;
        use std::sync::Arc;
        use std::time::Duration;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage)));
        let raft_node = Arc::new(
            AppRaftNode::new(
                1,
                Arc::clone(&storage),
                state_machine,
                Arc::new(MountTable::new()),
                &RaftConfig::default(),
            )
            .await
            .unwrap(),
        );
        let worker_manager = Arc::new(WorkerManager::new(60_000));
        let registry = Arc::new(SessionRegistry::new(1, 1, 100, 100, 100));
        let inode_id = InodeId::new(35);
        install_session(&registry, create_input(inode_id)).unwrap();
        assert!(registry.state.read().entries.contains_key(&inode_id));

        let cleanup_config = BlockCleanupConfig {
            enabled: false,
            ..BlockCleanupConfig::default()
        };
        let cleanup = Arc::new(BlockCleanupCoordinator::new(
            Arc::clone(&raft_node),
            Arc::clone(&storage),
            Arc::clone(&worker_manager),
            Arc::clone(&registry),
            GroupName::parse("root").unwrap(),
            &cleanup_config,
        ));
        let reclaimer = Arc::new(DetachedRootReclaimer::new(
            Arc::clone(&raft_node),
            storage,
            NamespaceDeleteConfig::default(),
        ));
        let service = MaintenanceService::new(
            raft_node,
            worker_manager,
            cleanup,
            reclaimer,
            Duration::from_secs(60),
            Arc::clone(&registry),
            Duration::from_millis(5),
        );
        let handle = service.start();
        assert_eq!(handle.task_count(), 3);

        tokio::time::timeout(Duration::from_secs(2), async {
            while registry.state.read().entries.contains_key(&inode_id) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("maintenance must retire the expired session without request traffic");
        handle.shutdown().await.unwrap();
        assert!(registry.state.read().entries.is_empty());
    }

    #[test]
    fn concurrent_openings_never_exceed_global_capacity() {
        let registry = std::sync::Arc::new(SessionRegistry::new(4, 4, 100, 100, 60_000));
        let contender_count = 16;
        let start = std::sync::Arc::new(std::sync::Barrier::new(contender_count + 1));
        let release = std::sync::Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let mut joins = Vec::new();

        for contender in 0..contender_count {
            let registry = std::sync::Arc::clone(&registry);
            let start = std::sync::Arc::clone(&start);
            let release = std::sync::Arc::clone(&release);
            let result_tx = result_tx.clone();
            joins.push(std::thread::spawn(move || {
                start.wait();
                let opening = begin_opening(&registry, InodeId::new(contender as u64 + 1), ClientId::new(1));
                result_tx.send(opening.is_ok()).unwrap();
                if let Ok(opening) = opening {
                    let (released, wake) = &*release;
                    let mut released = released.lock().unwrap();
                    while !*released {
                        released = wake.wait(released).unwrap();
                    }
                    drop(opening);
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
        assert_eq!(state.opening_sessions, 0);
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
        replacement.current_lease_epoch = Some(7);
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
        let registry = SessionRegistry::new(10, 10, 100, 100, 1);
        let inode_id = InodeId::new(22);
        let root_inode_id = InodeId::new(1);
        let mut input = create_input(inode_id);
        input.ancestor_inode_ids = vec![root_inode_id, input.inode_id];

        install_session_at(&registry, input, 0).unwrap();

        assert!(!registry.has_active_write_under_at(root_inode_id, 1));
        assert!(!registry.has_active_write_under_at(InodeId::new(inode_id.as_raw()), 1));
        let state = registry.state.read();
        assert!(state.entries.is_empty());
        assert!(state.ancestor_activity.is_empty());
        assert!(state.entries_by_expiry.is_empty());
    }

    #[test]
    fn renewed_session_updates_ancestor_expiry() {
        let registry = SessionRegistry::new(10, 10, 100, 100, 10);
        let inode_id = InodeId::new(24);
        let root_inode_id = InodeId::new(1);
        let mut input = create_input(inode_id);
        input.ancestor_inode_ids = vec![root_inode_id, input.inode_id];
        install_session_at(&registry, input, 0).unwrap();

        let expires_at_ms = registry
            .renew_session_at(inode_id, 7, ClientId::new(1), 5)
            .expect("renewal moves every expiry index before sweeping the old expiry");
        assert_eq!(expires_at_ms, 15);

        assert!(registry.has_active_write_under_at(inode_id, 14));
        assert!(registry.has_active_write_under_at(root_inode_id, 14));
        let state = registry.state.read();
        assert_eq!(state.entries_by_expiry, BTreeSet::from([(15, inode_id)]));
        assert_eq!(
            state
                .ancestor_activity
                .get(&root_inode_id)
                .expect("renewed ancestor activity")
                .sessions_by_expiry,
            BTreeMap::from([(15, 1)])
        );
    }

    #[test]
    fn rejected_renewal_leaves_primary_and_expiry_indexes_unchanged() {
        let registry = SessionRegistry::new(10, 10, 100, 100, 10);
        let inode_id = InodeId::new(33);
        install_session_at(&registry, create_input(inode_id), 100).unwrap();

        assert_eq!(
            registry.renew_session_at(inode_id, 7, ClientId::new(2), 105),
            Err(WriteSessionError::OwnerMismatch)
        );
        assert_eq!(
            registry.renew_session_at(inode_id, 6, ClientId::new(1), 105),
            Err(WriteSessionError::LeaseEpochMismatch { expected: 7, got: 6 })
        );
        let expires_at_ms = registry
            .renew_session_at(inode_id, 7, ClientId::new(1), 90)
            .expect("clock rollback must not shorten an active session");
        assert_eq!(expires_at_ms, 110);

        let state = registry.state.read();
        assert_eq!(state.entries_by_expiry, BTreeSet::from([(110, inode_id)]));
        assert!(matches!(
            state.entries.get(&inode_id),
            Some(WriteSessionEntry::Active(session)) if session.expires_at_ms == 110
        ));
    }

    #[test]
    fn expiry_renewal_and_close_retire_shared_ancestor_state() {
        let registry = SessionRegistry::new(10, 10, 100, 100, 10);
        let root_inode_id = InodeId::new(1);
        let expired_handle = InodeId::new(25);
        let active_handle = InodeId::new(26);
        let mut expired = create_input(expired_handle);
        expired.ancestor_inode_ids = vec![root_inode_id, expired.inode_id];
        let mut active = create_input(active_handle);
        active.ancestor_inode_ids = vec![root_inode_id, active.inode_id];

        install_session_at(&registry, expired, 0).unwrap();
        install_session_at(&registry, active, 1).unwrap();
        {
            let state = registry.state.read();
            assert_eq!(state.entries.len(), 2);
            assert_eq!(state.ancestor_activity.len(), 3);
            assert_eq!(state.entries_by_expiry.len(), 2);
        }

        registry
            .renew_session_at(active_handle, 7, ClientId::new(1), 10)
            .expect("renewal updates expiry indexes");
        registry.remove_session_if_epoch(active_handle, 7).unwrap();
        SessionRegistry::retire_expired_entries(&mut registry.state.write(), 10);

        let state = registry.state.read();
        assert!(state.entries.is_empty());
        assert!(state.ancestor_activity.is_empty());
        assert!(state.entries_by_expiry.is_empty());
    }

    #[test]
    fn expiry_sweep_is_bounded_and_queries_ignore_residual_expired_entries() {
        let historical_expired_count = MAX_EXPIRED_SESSION_RETIREMENTS_PER_CALL * 3 + 17;
        let registry = SessionRegistry::new(historical_expired_count + 1, historical_expired_count + 1, 100, 100, 10);
        let residual_expired_count = MAX_EXPIRED_SESSION_RETIREMENTS_PER_CALL * 2 + 3;
        for raw in 1..=historical_expired_count {
            let inode_id = InodeId::new(raw as u64);
            install_session_at(&registry, create_input(inode_id), 0).unwrap();
        }
        for raw in 1..=(historical_expired_count - residual_expired_count) {
            registry.remove_session_if_epoch(InodeId::new(raw as u64), 7).unwrap();
        }
        let active_inode_id = InodeId::new(20_000);
        let mut active = create_input(active_inode_id);
        active.ancestor_inode_ids = vec![active_inode_id];
        install_session_at(&registry, active, 1).unwrap();

        assert!(registry.has_active_write_under_at(active_inode_id, 10));
        {
            let state = registry.state.read();
            assert_eq!(
                state.entries.len(),
                residual_expired_count + 1 - MAX_EXPIRED_SESSION_RETIREMENTS_PER_CALL
            );
            assert!(state
                .entries_by_expiry
                .iter()
                .any(|(expires_at_ms, _)| *expires_at_ms == 10));
        }

        let residual_expired_inode_id = {
            let state = registry.state.read();
            state
                .entries
                .values()
                .find(|entry| entry.expires_at_ms() == 10)
                .and_then(|entry| match entry {
                    WriteSessionEntry::Active(session) => Some(session.inode_id),
                    WriteSessionEntry::Opening(_) => None,
                })
                .expect("one expired active session must remain after a bounded sweep")
        };
        assert!(!registry.has_active_write_under_at(residual_expired_inode_id, 10));
        while registry
            .state
            .read()
            .entries_by_expiry
            .iter()
            .any(|(expires_at_ms, _)| *expires_at_ms == 10)
        {
            assert!(registry.has_active_write_under_at(active_inode_id, 10));
        }

        registry.remove_session_if_epoch(active_inode_id, 7).unwrap();
        let state = registry.state.read();
        assert!(state.entries.is_empty());
        assert!(state.ancestor_activity.is_empty());
        assert!(state.entries_by_expiry.is_empty());
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

        let first = issue_target(&registry, inode_id, None, 0, 0, 1);
        let replay = match registry.begin_add_block(inode_id, 7, None).unwrap() {
            BeginAddBlock::Replay(target) => target,
            BeginAddBlock::Reserved(_) => panic!("issued predecessor must replay"),
        };
        assert_eq!(replay, first);

        let second = issue_target(&registry, inode_id, Some(first.block_id), 1, 64, 1);
        assert_eq!(second.block_id.index, BlockIndex::new(1));
        assert_eq!(second.file_offset, 64);
    }

    #[test]
    fn concurrent_duplicate_add_block_reserves_one_target_before_completion() {
        let registry = std::sync::Arc::new(SessionRegistry::default());
        let inode_id = InodeId::new(15);
        install_session(&registry, create_input(inode_id)).unwrap();
        let start = std::sync::Arc::new(std::sync::Barrier::new(2));
        let reserved = std::sync::Arc::new(std::sync::Barrier::new(2));

        let mut joins = Vec::new();
        for index in 0..2 {
            let registry = std::sync::Arc::clone(&registry);
            let start = std::sync::Arc::clone(&start);
            let reserved = std::sync::Arc::clone(&reserved);
            joins.push(std::thread::spawn(move || {
                start.wait();
                match registry.begin_add_block(inode_id, 7, None) {
                    Ok(BeginAddBlock::Reserved(reservation)) => {
                        reserved.wait();
                        Some(reservation.complete(write_target(inode_id, index)).unwrap())
                    }
                    Err(BeginAddBlockError::Pending) => {
                        reserved.wait();
                        None
                    }
                    Ok(BeginAddBlock::Replay(_)) | Err(_) => panic!("unexpected concurrent AddBlock outcome"),
                }
            }));
        }

        let first = joins.remove(0).join().unwrap();
        let second = joins.remove(0).join().unwrap();
        let issued = first.or(second).expect("one request must issue the target");
        let session = registry.get_session(inode_id).unwrap();
        assert_eq!(session.issued_targets, vec![issued]);
        assert_eq!(session.issued_steps.len(), 1);
        assert_eq!(registry.state.read().outstanding_write_targets, 1);
        assert_eq!(registry.state.read().pending_write_targets, 0);
    }

    #[test]
    fn add_block_rejects_stale_lease_epoch() {
        let registry = SessionRegistry::default();
        let inode_id = InodeId::new(12);
        install_session(&registry, create_input(inode_id)).unwrap();
        issue_target(&registry, inode_id, None, 0, 0, 1);

        assert!(matches!(
            registry.begin_add_block(inode_id, 6, None),
            Err(BeginAddBlockError::Session(_))
        ));
        assert_eq!(registry.get_session(inode_id).unwrap().issued_targets.len(), 1);
    }

    #[test]
    fn add_block_rejects_a_gap_in_the_predecessor_chain() {
        let registry = SessionRegistry::default();
        let inode_id = InodeId::new(13);
        install_session(&registry, create_input(inode_id)).unwrap();
        let unknown = BlockId::new(inode_id, BlockIndex::new(99));

        assert!(matches!(
            registry.begin_add_block(inode_id, 7, Some(unknown)),
            Err(BeginAddBlockError::InvalidArgument(message)) if message.contains("predecessor mismatch")
        ));
        assert!(registry.get_session(inode_id).unwrap().issued_targets.is_empty());
    }

    #[test]
    fn published_partial_rebases_the_next_target_and_preserves_replay_stamp() {
        let registry = SessionRegistry::default();
        let inode_id = InodeId::new(14);
        install_session(&registry, create_input(inode_id)).unwrap();

        let first = issue_target(&registry, inode_id, None, 0, 0, 1);
        assert_eq!(first.block_stamp, 1);
        assert_eq!(first.block_size, 64);
        registry
            .begin_publication(inode_id, 7)
            .expect("freeze issued targets")
            .complete_sync(1, 32)
            .expect("advance published state");

        let replay = match registry.begin_add_block(inode_id, 7, None).unwrap() {
            BeginAddBlock::Replay(target) => target,
            BeginAddBlock::Reserved(_) => panic!("issued predecessor must replay"),
        };
        assert_eq!(replay, first);
        let second = issue_target(&registry, inode_id, Some(first.block_id), 1, 32, 2);
        assert_eq!(second.block_stamp, 2);
        assert_eq!(second.file_offset, 32);
    }

    #[test]
    fn add_block_and_publication_owners_exclude_each_other_until_drop() {
        let registry = SessionRegistry::default();
        let inode_id = InodeId::new(16);
        install_session(&registry, create_input(inode_id)).unwrap();
        let first = issue_target(&registry, inode_id, None, 0, 0, 1);
        let reservation = match registry.begin_add_block(inode_id, 7, Some(first.block_id)).unwrap() {
            BeginAddBlock::Reserved(reservation) => reservation,
            BeginAddBlock::Replay(_) => panic!("new predecessor must reserve"),
        };
        assert!(matches!(
            registry.begin_publication(inode_id, 7),
            Err(BeginWritePublicationError::AddBlockPending)
        ));
        drop(reservation);

        let publication = registry
            .begin_publication(inode_id, 7)
            .expect("released AddBlock must unblock publication");
        assert!(matches!(
            registry.begin_add_block(inode_id, 7, Some(first.block_id)),
            Err(BeginAddBlockError::PublicationInProgress)
        ));
        drop(publication);

        let second = issue_target(&registry, inode_id, Some(first.block_id), 1, 64, 1);
        assert_eq!(second.file_offset, 64);
        assert_eq!(second.block_stamp, 1);
        assert_eq!(registry.state.read().outstanding_write_targets, 2);
        assert_eq!(registry.state.read().pending_write_targets, 0);
    }

    #[test]
    fn first_sync_install_discards_old_revision_targets_at_and_after_visible_end() {
        let registry = SessionRegistry::default();
        let inode_id = InodeId::new(26);
        install_session(&registry, create_input(inode_id)).unwrap();
        let first = issue_target(&registry, inode_id, None, 0, 0, 1);
        let stale_second = issue_target(&registry, inode_id, Some(first.block_id), 1, 64, 1);
        let stale_third = issue_target(&registry, inode_id, Some(stale_second.block_id), 2, 128, 1);

        registry
            .begin_publication(inode_id, 7)
            .expect("freeze the old revision targets")
            .complete_sync(1, 64)
            .expect("install the first SyncWrite result");

        let session = registry.get_session(inode_id).unwrap();
        assert_eq!(session.issued_targets, vec![first.clone()]);
        assert_eq!(session.active_publication, None);
        assert_eq!(registry.state.read().outstanding_write_targets, 1);
        for stale_block_id in [stale_second.block_id, stale_third.block_id] {
            assert!(matches!(
                registry.begin_add_block(inode_id, 7, Some(stale_block_id)),
                Err(BeginAddBlockError::InvalidArgument(_))
            ));
        }
        let replacement = issue_target(&registry, inode_id, Some(first.block_id), 3, 64, 2);
        assert_eq!(replacement.file_offset, 64);
        assert_eq!(replacement.block_stamp, 2);
    }

    #[test]
    fn replayed_or_failed_sync_completion_preserves_current_targets_and_releases_owner() {
        let registry = SessionRegistry::default();
        let inode_id = InodeId::new(28);
        install_session(&registry, create_input(inode_id)).unwrap();
        let first = issue_target(&registry, inode_id, None, 0, 0, 1);
        registry
            .begin_publication(inode_id, 7)
            .expect("freeze the first revision")
            .complete_sync(1, 64)
            .expect("install the first SyncWrite result");
        let second = issue_target(&registry, inode_id, Some(first.block_id), 1, 64, 2);
        let third = issue_target(&registry, inode_id, Some(second.block_id), 2, 128, 2);

        registry
            .begin_publication(inode_id, 7)
            .expect("freeze the installed revision")
            .complete_sync(1, 64)
            .expect("replay the installed SyncWrite result");

        let session = registry.get_session(inode_id).unwrap();
        assert_eq!(
            session.issued_targets,
            vec![first.clone(), second.clone(), third.clone()]
        );
        assert_eq!(session.active_publication, None);
        assert_eq!(registry.state.read().outstanding_write_targets, 3);
        match registry.begin_add_block(inode_id, 7, Some(second.block_id)).unwrap() {
            BeginAddBlock::Replay(target) => assert_eq!(target, third),
            BeginAddBlock::Reserved(_) => panic!("replayed predecessor must retain its target"),
        };

        let error = registry
            .begin_publication(inode_id, 7)
            .expect("freeze issued targets")
            .complete_sync(3, 192)
            .expect_err("skipped content revision must fail closed");
        assert!(error.contains("expected 2, got 3"));

        let session = registry.get_session(inode_id).unwrap();
        assert_eq!(session.issued_targets, vec![first, second, third.clone()]);
        assert_eq!(session.active_publication, None);
        assert_eq!(registry.state.read().outstanding_write_targets, 3);
        let fourth = issue_target(&registry, inode_id, Some(third.block_id), 3, 192, 2);
        assert_eq!(fourth.file_offset, 192);
        assert_eq!(fourth.block_stamp, 2);
    }

    #[test]
    fn target_limits_count_pending_and_issued_while_replay_bypasses_capacity() {
        let registry = SessionRegistry::new(3, 3, 2, 1, 60_000);
        let first_inode = InodeId::new(31);
        let second_inode = InodeId::new(32);
        let third_inode = InodeId::new(33);
        for inode_id in [first_inode, second_inode, third_inode] {
            install_session(&registry, create_input(inode_id)).unwrap();
        }

        let first = issue_target(&registry, first_inode, None, 0, 0, 1);
        assert!(matches!(
            registry.begin_add_block(first_inode, 7, Some(first.block_id)),
            Err(BeginAddBlockError::LimitExceeded(WriteTargetLimitExceeded {
                limit: WriteTargetLimit::PerSession,
                maximum: 1,
            }))
        ));
        issue_target(&registry, second_inode, None, 0, 0, 1);
        assert!(matches!(
            registry.begin_add_block(third_inode, 7, None),
            Err(BeginAddBlockError::LimitExceeded(WriteTargetLimitExceeded {
                limit: WriteTargetLimit::Global,
                maximum: 2,
            }))
        ));
        assert!(matches!(
            registry.begin_add_block(first_inode, 7, None),
            Ok(BeginAddBlock::Replay(target)) if target == first
        ));

        registry.remove_session_if_epoch(second_inode, 7).unwrap();
        assert!(matches!(
            registry.begin_add_block(third_inode, 7, None),
            Ok(BeginAddBlock::Reserved(_))
        ));
    }

    #[test]
    fn pending_target_drop_and_session_removal_release_exact_capacity() {
        let registry = SessionRegistry::new(1, 1, 1, 1, 60_000);
        let inode_id = InodeId::new(34);
        install_session(&registry, create_input(inode_id)).unwrap();

        let reservation = match registry.begin_add_block(inode_id, 7, None).unwrap() {
            BeginAddBlock::Reserved(reservation) => reservation,
            BeginAddBlock::Replay(_) => panic!("new target must reserve"),
        };
        assert_eq!(registry.state.read().outstanding_write_targets, 1);
        assert_eq!(registry.state.read().pending_write_targets, 1);
        drop(reservation);
        assert_eq!(registry.state.read().outstanding_write_targets, 0);
        assert_eq!(registry.state.read().pending_write_targets, 0);

        issue_target(&registry, inode_id, None, 0, 0, 1);
        assert_eq!(registry.state.read().outstanding_write_targets, 1);
        assert_eq!(registry.state.read().pending_write_targets, 0);
        registry.remove_session_if_epoch(inode_id, 7).unwrap();
        assert_eq!(registry.state.read().outstanding_write_targets, 0);
        assert_eq!(registry.state.read().pending_write_targets, 0);
    }

    #[test]
    fn expiry_releases_issued_and_pending_target_capacity_exactly_once() {
        let registry = SessionRegistry::new(2, 2, 2, 1, 60_000);
        let issued_inode = InodeId::new(35);
        let pending_inode = InodeId::new(36);
        install_session(&registry, create_input(issued_inode)).unwrap();
        install_session(&registry, create_input(pending_inode)).unwrap();
        issue_target(&registry, issued_inode, None, 0, 0, 1);
        let reservation = match registry.begin_add_block(pending_inode, 7, None).unwrap() {
            BeginAddBlock::Reserved(reservation) => reservation,
            BeginAddBlock::Replay(_) => panic!("new target must reserve"),
        };
        assert_eq!(registry.state.read().outstanding_write_targets, 2);
        assert_eq!(registry.state.read().pending_write_targets, 1);

        {
            let mut state = registry.state.write();
            assert_eq!(SessionRegistry::retire_expired_entries(&mut state, u64::MAX), 2);
        }
        assert_eq!(registry.state.read().outstanding_write_targets, 0);
        assert_eq!(registry.state.read().pending_write_targets, 0);
        drop(reservation);
        assert_eq!(registry.state.read().outstanding_write_targets, 0);
        assert_eq!(registry.state.read().pending_write_targets, 0);
    }
}
