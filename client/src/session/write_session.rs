// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Client-side sequential write session state.

use std::fmt::Write as _;
use std::time::{SystemTime, UNIX_EPOCH};

use proto::metadata::{AbortFileWriteRequestProto, CommitFileRequestProto, WriteHandleProto};
use types::{CallId, ClientId, CommittedBlock, DataHandleId, InodeId, WriteTarget};

use crate::data::WorkerWriteBlock;
use crate::error::{ClientError, ClientResult};
use crate::runtime::context::{OperationContext, OperationFingerprint, OperationIdentity};
use crate::runtime::policy::OperationKind;

const LEASE_EXPIRY_SAFETY_WINDOW_MS: u64 = 1_000;

/// Open sequential write session tracked by an internal file handle field.
#[derive(Clone, Debug)]
pub(crate) struct WriteSession {
    path: String,
    inode_id: InodeId,
    data_handle_id: DataHandleId,
    file_version: Option<u64>,
    write_handle: WriteHandleProto,
    cursor: u64,
    base_size: u64,
    expires_at_ms: Option<u64>,
    pending_blocks: Vec<PendingBlock>,
    state: WriteSessionState,
    commit: Option<CommitFileState>,
    abort: Option<AbortCleanupState>,
}

impl WriteSession {
    /// Create a new client-side write session from metadata open-write state.
    pub(crate) fn new(
        path: String,
        inode_id: InodeId,
        data_handle_id: DataHandleId,
        write_handle: WriteHandleProto,
        base_size: u64,
    ) -> ClientResult<Self> {
        validate_write_handle(&write_handle)?;
        Ok(Self {
            path,
            inode_id,
            data_handle_id,
            file_version: None,
            write_handle,
            cursor: base_size,
            base_size,
            expires_at_ms: None,
            pending_blocks: Vec::new(),
            state: WriteSessionState::Open,
            commit: None,
            abort: None,
        })
    }

    /// Path associated with the original open operation.
    pub(crate) fn path(&self) -> &str {
        &self.path
    }

    /// Current sequential write cursor.
    pub(crate) fn cursor(&self) -> u64 {
        self.cursor
    }

    /// Metadata write handle.
    pub(crate) fn write_handle(&self) -> WriteHandleProto {
        self.write_handle
    }

    /// Stable identity used by replay gates for session-scoped operations.
    pub(crate) fn session_identity(&self) -> String {
        format!(
            "handle={} inode={} data_handle={} base_size={} lease_epoch={} open_epoch={} fencing={}",
            self.write_handle.handle_id,
            self.inode_id.as_raw(),
            self.data_handle_id.as_raw(),
            self.base_size,
            self.write_handle.lease_epoch,
            self.write_handle.open_epoch,
            fencing_identity(&self.write_handle)
        )
    }

    /// Validate that a non-empty write may start at the supplied offset.
    pub(crate) fn validate_write_offset(&mut self, offset: u64) -> ClientResult<()> {
        self.ensure_open_for_write()?;
        if offset != self.cursor {
            return Err(ClientError::InvalidArgument(format!(
                "sequential write cursor mismatch: expected {}, got {}",
                self.cursor, offset
            )));
        }
        Ok(())
    }

    /// Validate a metadata write target before opening the worker stream.
    pub(crate) fn validate_target(&mut self, target: &WriteTarget, expected_len: u64) -> ClientResult<()> {
        self.ensure_open_for_write()?;
        if target.file_offset != self.cursor {
            return Err(ClientError::InvalidLayout(format!(
                "write target file_offset mismatch: expected {}, got {}",
                self.cursor, target.file_offset
            )));
        }
        if target.len != expected_len {
            return Err(ClientError::InvalidLayout(format!(
                "write target len mismatch: expected {}, got {}",
                expected_len, target.len
            )));
        }
        let block = target.block_id;
        if block.data_handle_id != self.data_handle_id {
            return Err(ClientError::StaleHandle {
                reason: format!(
                    "write target data_handle_id {} does not match session data_handle_id {}",
                    block.data_handle_id.as_raw(),
                    self.data_handle_id.as_raw()
                ),
            });
        }
        if target.block_stamp == 0 {
            return Err(ClientError::InvalidLayout(
                "write target block_stamp must be non-zero".to_string(),
            ));
        }
        if target.chunk_size == 0 {
            return Err(ClientError::InvalidLayout(
                "write target chunk_size must be non-zero".to_string(),
            ));
        }
        Ok(())
    }

    /// Record a worker-accepted block and advance the cursor.
    pub(crate) fn push_pending_block(
        &mut self,
        target: WriteTarget,
        worker_block: WorkerWriteBlock,
        written_len: u64,
        commit_seq: u64,
    ) -> ClientResult<()> {
        if commit_seq == 0 {
            return Err(ClientError::Worker(
                "worker WriteStream acknowledged no non-empty frame".to_string(),
            ));
        }
        let final_offset = self
            .cursor
            .checked_add(written_len)
            .ok_or_else(|| ClientError::InvalidArgument("write cursor overflow".to_string()))?;
        self.pending_blocks.push(PendingBlock {
            target,
            worker_block,
            written_len,
            commit_seq,
            worker_commit_level: WorkerCommitLevel::Uncommitted,
        });
        self.cursor = final_offset;
        Ok(())
    }

    /// Return pending worker blocks.
    pub(crate) fn pending_blocks_mut(&mut self) -> &mut [PendingBlock] {
        &mut self.pending_blocks
    }

    /// Freeze and return the CommitFile operation for this write session.
    pub(crate) fn prepare_commit_file(
        &mut self,
        client_id: ClientId,
        committed_blocks: Vec<CommittedBlock>,
        final_size: u64,
    ) -> ClientResult<(OperationContext, CommitFileRequestProto)> {
        match self.state {
            WriteSessionState::Open => {
                let session_identity = self.session_identity();
                let detail = commit_fingerprint_detail(
                    &self.write_handle,
                    self.inode_id,
                    self.data_handle_id,
                    final_size,
                    &committed_blocks,
                );
                let identity =
                    OperationIdentity::session(self.path.clone(), session_identity.clone()).with_detail(detail.clone());
                let commit_fingerprint = identity.fingerprint(OperationKind::MetadataSessionBarrier, "CommitFile");
                self.commit = Some(CommitFileState {
                    commit_call_id: CallId::new(),
                    commit_fingerprint,
                    commit_write_handle: self.write_handle,
                    commit_final_size: final_size,
                    commit_committed_blocks_snapshot: committed_blocks,
                    session_identity,
                    detail,
                });
                self.state = WriteSessionState::CommitStarted;
            }
            WriteSessionState::CommitStarted | WriteSessionState::CommitUnknown => {
                let commit = self.commit.as_ref().ok_or_else(|| {
                    ClientError::InvalidArgument("CommitFile state missing frozen identity".to_string())
                })?;
                if commit.commit_final_size != final_size || commit.commit_committed_blocks_snapshot != committed_blocks
                {
                    return Err(ClientError::InvalidArgument(
                        "CommitFile replay payload changed after commit started".to_string(),
                    ));
                }
                let session_identity = self.session_identity();
                let detail = commit_fingerprint_detail(
                    &self.write_handle,
                    self.inode_id,
                    self.data_handle_id,
                    final_size,
                    &committed_blocks,
                );
                if commit.session_identity != session_identity || commit.detail != detail {
                    return Err(ClientError::InvalidArgument(
                        "CommitFile replay identity changed after commit started".to_string(),
                    ));
                }
            }
            WriteSessionState::Closed => {
                return Err(ClientError::StaleHandle {
                    reason: "write handle is closed".to_string(),
                });
            }
            WriteSessionState::Aborted => {
                return Err(ClientError::StaleHandle {
                    reason: "write handle is aborted".to_string(),
                });
            }
            WriteSessionState::UnknownOutcome => {
                return Err(ClientError::StaleHandle {
                    reason: "write handle has an unknown outcome".to_string(),
                });
            }
            WriteSessionState::SessionInvalid => {
                return Err(ClientError::StaleHandle {
                    reason: "write session is invalid".to_string(),
                });
            }
            WriteSessionState::SessionExpired => {
                return Err(ClientError::StaleHandle {
                    reason: "write session lease expired".to_string(),
                });
            }
            WriteSessionState::AbortUnknown => {
                return Err(ClientError::StaleHandle {
                    reason: "write handle abort outcome is unknown".to_string(),
                });
            }
        }

        let commit = self
            .commit
            .as_ref()
            .ok_or_else(|| ClientError::InvalidArgument("CommitFile state missing frozen identity".to_string()))?;
        let operation = OperationContext::with_call_id_and_fingerprint(
            client_id,
            commit.commit_call_id,
            OperationKind::MetadataSessionBarrier,
            "CommitFile",
            OperationIdentity::session(self.path.clone(), commit.session_identity.clone())
                .with_detail(commit.detail.clone()),
            commit.commit_fingerprint,
        )?;
        Ok((
            operation,
            CommitFileRequestProto {
                header: None,
                write_handle: Some(commit.commit_write_handle),
                data_handle_id: Some(proto::common::DataHandleIdProto {
                    value: self.data_handle_id.as_raw(),
                }),
                committed_blocks: commit.commit_committed_blocks_snapshot.iter().map(Into::into).collect(),
                final_size: commit.commit_final_size,
            },
        ))
    }

    /// Freeze and return the abort cleanup plan for this write session.
    pub(crate) fn prepare_abort_cleanup(&mut self, client_id: ClientId) -> ClientResult<AbortCleanupPlan> {
        match self.state {
            WriteSessionState::Open => {
                if self.pending_blocks.iter().any(PendingBlock::has_worker_commit) {
                    self.state = WriteSessionState::AbortUnknown;
                    return Err(ClientError::UnknownOutcome(
                        "cannot safely abort after a worker block commit succeeded".to_string(),
                    ));
                }
                let session_identity = self.session_identity();
                let worker_cleanups = self
                    .pending_blocks
                    .iter()
                    .map(|pending| {
                        let worker_block = pending.worker_block().clone();
                        let detail = abort_worker_fingerprint_detail(&worker_block);
                        let identity = OperationIdentity::session(self.path.clone(), session_identity.clone())
                            .with_detail(detail.clone());
                        let abort_fingerprint = identity.fingerprint(OperationKind::WorkerWriteData, "AbortWrite");
                        AbortWorkerCleanupState {
                            abort_call_id: CallId::new(),
                            abort_fingerprint,
                            worker_block,
                            detail,
                        }
                    })
                    .collect::<Vec<_>>();
                let detail = abort_file_fingerprint_detail(
                    &self.write_handle,
                    self.inode_id,
                    self.data_handle_id,
                    &worker_cleanups,
                );
                let metadata_identity =
                    OperationIdentity::session(self.path.clone(), session_identity.clone()).with_detail(detail.clone());
                let metadata_fingerprint =
                    metadata_identity.fingerprint(OperationKind::CleanupBestEffort, "AbortFileWrite");
                self.abort = Some(AbortCleanupState {
                    metadata_call_id: CallId::new(),
                    metadata_fingerprint,
                    metadata_write_handle: self.write_handle,
                    session_identity,
                    detail,
                    worker_cleanups,
                });
                self.state = WriteSessionState::AbortUnknown;
            }
            WriteSessionState::AbortUnknown => {
                let abort = self.abort.as_ref().ok_or_else(|| {
                    ClientError::InvalidArgument("AbortUnknown state missing frozen cleanup plan".to_string())
                })?;
                if abort.metadata_write_handle != self.write_handle || abort.session_identity != self.session_identity()
                {
                    return Err(ClientError::InvalidArgument(
                        "Abort cleanup replay identity changed after cleanup started".to_string(),
                    ));
                }
            }
            _ => return Err(self.state_error_value()),
        }

        let abort = self
            .abort
            .as_ref()
            .ok_or_else(|| ClientError::InvalidArgument("abort cleanup state missing frozen plan".to_string()))?;
        let metadata_operation = OperationContext::with_call_id_and_fingerprint(
            client_id,
            abort.metadata_call_id,
            OperationKind::CleanupBestEffort,
            "AbortFileWrite",
            OperationIdentity::session(self.path.clone(), abort.session_identity.clone())
                .with_detail(abort.detail.clone()),
            abort.metadata_fingerprint,
        )?;
        let metadata_request = AbortFileWriteRequestProto {
            header: None,
            write_handle: Some(abort.metadata_write_handle),
        };
        let mut worker_cleanups = Vec::with_capacity(abort.worker_cleanups.len());
        for cleanup in &abort.worker_cleanups {
            let operation = OperationContext::with_call_id_and_fingerprint(
                client_id,
                cleanup.abort_call_id,
                OperationKind::WorkerWriteData,
                "AbortWrite",
                OperationIdentity::session(self.path.clone(), abort.session_identity.clone())
                    .with_detail(cleanup.detail.clone()),
                cleanup.abort_fingerprint,
            )?;
            worker_cleanups.push(AbortWorkerCleanupPlan {
                operation,
                worker_block: cleanup.worker_block.clone(),
            });
        }
        Ok(AbortCleanupPlan {
            metadata_operation,
            metadata_request,
            worker_cleanups,
        })
    }

    /// Mark CommitFile outcome as unknown and keep the session retryable.
    pub(crate) fn mark_commit_unknown(&mut self) {
        if matches!(self.state, WriteSessionState::CommitStarted) {
            self.state = WriteSessionState::CommitUnknown;
        }
    }

    /// Mark the session closed after metadata commit succeeds.
    pub(crate) fn mark_closed(&mut self, file_version: Option<u64>) {
        self.file_version = file_version;
        self.state = WriteSessionState::Closed;
    }

    /// Mark the session aborted after best-effort cleanup.
    pub(crate) fn mark_aborted(&mut self) {
        self.abort = None;
        self.state = WriteSessionState::Aborted;
    }

    /// Mark the session as blocked by an unknown write outcome.
    pub(crate) fn mark_unknown_outcome(&mut self) {
        self.state = WriteSessionState::UnknownOutcome;
    }

    /// Mark the session invalid after a fencing or lease failure.
    pub(crate) fn mark_session_invalid(&mut self) {
        self.state = WriteSessionState::SessionInvalid;
    }

    /// Mark the session expired after local or metadata lease expiration.
    pub(crate) fn mark_session_expired(&mut self) {
        self.state = WriteSessionState::SessionExpired;
    }

    /// Mark abort cleanup as uncertain while keeping retry metadata.
    pub(crate) fn mark_abort_unknown(&mut self) {
        self.state = WriteSessionState::AbortUnknown;
    }

    /// Record the latest metadata lease expiration returned by RenewLease.
    pub(crate) fn update_expires_at_ms(&mut self, expires_at_ms: u64) {
        self.expires_at_ms = Some(expires_at_ms);
    }

    /// Return the latest known lease expiration.
    #[cfg(test)]
    pub(crate) fn expires_at_ms(&self) -> Option<u64> {
        self.expires_at_ms
    }

    /// Return whether CommitFile outcome is unresolved and retryable.
    pub(crate) fn is_commit_unknown(&self) -> bool {
        matches!(self.state, WriteSessionState::CommitUnknown)
    }

    /// Reject close attempts on handles that already reached a terminal state.
    pub(crate) fn ensure_close_allowed(&mut self) -> ClientResult<()> {
        self.ensure_open_for_close()
    }

    /// Reject writes unless the session is open and the lease is locally valid.
    pub(crate) fn ensure_open_for_write(&mut self) -> ClientResult<()> {
        self.ensure_open_for_write_at_ms(unix_now_ms())
    }

    /// Reject close unless the session can start or continue a safe close.
    pub(crate) fn ensure_open_for_close(&mut self) -> ClientResult<()> {
        self.ensure_open_for_close_at_ms(unix_now_ms())
    }

    /// Reject abort unless cleanup is safe to attempt.
    pub(crate) fn ensure_open_for_abort(&mut self) -> ClientResult<()> {
        self.ensure_open_for_abort_at_ms(unix_now_ms())
    }

    /// Reject lease renew unless the handle still represents an open session.
    pub(crate) fn ensure_open_for_renew(&mut self) -> ClientResult<()> {
        self.ensure_open_for_renew_at_ms(unix_now_ms())
    }

    /// Reject side-effect-free barriers after validating session state.
    pub(crate) fn ensure_open_for_barrier(&mut self) -> ClientResult<()> {
        self.ensure_open_for_barrier_at_ms(unix_now_ms())
    }

    #[cfg(test)]
    pub(crate) fn ensure_open_for_write_at_ms(&mut self, now_ms: u64) -> ClientResult<()> {
        self.ensure_state_allows_write()?;
        self.ensure_lease_valid_at_ms(now_ms, LEASE_EXPIRY_SAFETY_WINDOW_MS)
    }

    #[cfg(not(test))]
    fn ensure_open_for_write_at_ms(&mut self, now_ms: u64) -> ClientResult<()> {
        self.ensure_state_allows_write()?;
        self.ensure_lease_valid_at_ms(now_ms, LEASE_EXPIRY_SAFETY_WINDOW_MS)
    }

    #[cfg(test)]
    pub(crate) fn ensure_open_for_close_at_ms(&mut self, now_ms: u64) -> ClientResult<()> {
        match self.state {
            WriteSessionState::Open => self.ensure_lease_valid_at_ms(now_ms, LEASE_EXPIRY_SAFETY_WINDOW_MS),
            WriteSessionState::CommitStarted | WriteSessionState::CommitUnknown => Ok(()),
            _ => self.state_error(),
        }
    }

    #[cfg(not(test))]
    fn ensure_open_for_close_at_ms(&mut self, now_ms: u64) -> ClientResult<()> {
        match self.state {
            WriteSessionState::Open => self.ensure_lease_valid_at_ms(now_ms, LEASE_EXPIRY_SAFETY_WINDOW_MS),
            WriteSessionState::CommitStarted | WriteSessionState::CommitUnknown => Ok(()),
            _ => self.state_error(),
        }
    }

    #[cfg(test)]
    pub(crate) fn ensure_open_for_abort_at_ms(&mut self, now_ms: u64) -> ClientResult<()> {
        match self.state {
            WriteSessionState::Open => self.ensure_lease_valid_at_ms(now_ms, LEASE_EXPIRY_SAFETY_WINDOW_MS),
            WriteSessionState::AbortUnknown => Ok(()),
            _ => self.state_error(),
        }
    }

    #[cfg(not(test))]
    fn ensure_open_for_abort_at_ms(&mut self, now_ms: u64) -> ClientResult<()> {
        match self.state {
            WriteSessionState::Open => self.ensure_lease_valid_at_ms(now_ms, LEASE_EXPIRY_SAFETY_WINDOW_MS),
            WriteSessionState::AbortUnknown => Ok(()),
            _ => self.state_error(),
        }
    }

    #[cfg(test)]
    pub(crate) fn ensure_open_for_renew_at_ms(&mut self, now_ms: u64) -> ClientResult<()> {
        self.ensure_state_allows_write()?;
        self.ensure_lease_valid_at_ms(now_ms, 0)
    }

    #[cfg(not(test))]
    fn ensure_open_for_renew_at_ms(&mut self, now_ms: u64) -> ClientResult<()> {
        self.ensure_state_allows_write()?;
        self.ensure_lease_valid_at_ms(now_ms, 0)
    }

    #[cfg(test)]
    pub(crate) fn ensure_open_for_barrier_at_ms(&mut self, now_ms: u64) -> ClientResult<()> {
        self.ensure_state_allows_write()?;
        self.ensure_lease_valid_at_ms(now_ms, LEASE_EXPIRY_SAFETY_WINDOW_MS)
    }

    #[cfg(not(test))]
    fn ensure_open_for_barrier_at_ms(&mut self, now_ms: u64) -> ClientResult<()> {
        self.ensure_state_allows_write()?;
        self.ensure_lease_valid_at_ms(now_ms, LEASE_EXPIRY_SAFETY_WINDOW_MS)
    }

    fn ensure_state_allows_write(&self) -> ClientResult<()> {
        match self.state {
            WriteSessionState::Open => Ok(()),
            _ => self.state_error(),
        }
    }

    fn ensure_lease_valid_at_ms(&mut self, now_ms: u64, safety_window_ms: u64) -> ClientResult<()> {
        let Some(expires_at_ms) = self.expires_at_ms else {
            return Ok(());
        };
        if expires_at_ms <= now_ms {
            self.mark_session_expired();
            return Err(ClientError::StaleHandle {
                reason: "write session lease expired".to_string(),
            });
        }
        if expires_at_ms.saturating_sub(now_ms) <= safety_window_ms {
            self.mark_session_expired();
            return Err(ClientError::StaleHandle {
                reason: "write session lease is near expiry".to_string(),
            });
        }
        Ok(())
    }

    fn state_error(&self) -> ClientResult<()> {
        if matches!(self.state, WriteSessionState::Open) {
            return Ok(());
        }
        Err(self.state_error_value())
    }

    fn state_error_value(&self) -> ClientError {
        match self.state {
            WriteSessionState::Open => ClientError::InvalidArgument("write session is open".to_string()),
            WriteSessionState::CommitStarted | WriteSessionState::CommitUnknown => ClientError::StaleHandle {
                reason: "write handle has an in-progress CommitFile".to_string(),
            },
            WriteSessionState::Closed => ClientError::StaleHandle {
                reason: "write handle is closed".to_string(),
            },
            WriteSessionState::Aborted => ClientError::StaleHandle {
                reason: "write handle is aborted".to_string(),
            },
            WriteSessionState::UnknownOutcome => ClientError::StaleHandle {
                reason: "write handle has an unknown outcome".to_string(),
            },
            WriteSessionState::SessionInvalid => ClientError::StaleHandle {
                reason: "write session is invalid".to_string(),
            },
            WriteSessionState::SessionExpired => ClientError::StaleHandle {
                reason: "write session lease expired".to_string(),
            },
            WriteSessionState::AbortUnknown => ClientError::StaleHandle {
                reason: "write handle abort outcome is unknown".to_string(),
            },
        }
    }
}

/// Pending worker block in a write session.
#[derive(Clone, Debug)]
pub(crate) struct PendingBlock {
    target: WriteTarget,
    worker_block: WorkerWriteBlock,
    written_len: u64,
    commit_seq: u64,
    worker_commit_level: WorkerCommitLevel,
}

impl PendingBlock {
    /// Metadata write target for this block.
    pub(crate) fn target(&self) -> &WriteTarget {
        &self.target
    }

    /// Worker stream state for this block.
    pub(crate) fn worker_block(&self) -> &WorkerWriteBlock {
        &self.worker_block
    }

    /// Length accepted by the worker write path.
    pub(crate) fn written_len(&self) -> u64 {
        self.written_len
    }

    /// Last worker-acknowledged write sequence for CommitWrite.
    pub(crate) fn commit_seq(&self) -> u64 {
        self.commit_seq
    }

    /// Whether any CommitWrite already succeeded for this block.
    pub(crate) fn has_worker_commit(&self) -> bool {
        self.worker_commit_level != WorkerCommitLevel::Uncommitted
    }

    /// Current worker-observed commit level for this block.
    pub(crate) fn worker_commit_level(&self) -> WorkerCommitLevel {
        self.worker_commit_level
    }

    /// Mark worker commit as successful at the visibility or durability level.
    pub(crate) fn mark_worker_committed(&mut self, require_sync: bool) {
        self.worker_commit_level = WorkerCommitLevel::from_require_sync(require_sync);
    }
}

/// Client-observed worker commit strength for a pending write block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WorkerCommitLevel {
    Uncommitted,
    Visible,
    Durable,
}

impl WorkerCommitLevel {
    /// Required level for the existing non-sync close path.
    pub(crate) const CLOSE_REQUIRED: Self = Self::Visible;

    /// Return the required worker commit level for a CommitWrite request.
    pub(crate) fn from_require_sync(require_sync: bool) -> Self {
        if require_sync {
            Self::Durable
        } else {
            Self::Visible
        }
    }

    pub(crate) fn satisfies(self, required: Self) -> bool {
        matches!(
            (self, required),
            (Self::Durable, Self::Durable | Self::Visible)
                | (Self::Visible, Self::Visible)
                | (Self::Uncommitted, Self::Uncommitted)
        )
    }

    /// Whether CommitWrite must request worker-side sync for this level.
    pub(crate) fn requires_sync(self) -> bool {
        matches!(self, Self::Durable)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WriteSessionState {
    Open,
    CommitStarted,
    CommitUnknown,
    Closed,
    Aborted,
    UnknownOutcome,
    SessionInvalid,
    SessionExpired,
    AbortUnknown,
}

fn unix_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

#[derive(Clone, Debug)]
struct CommitFileState {
    commit_call_id: CallId,
    commit_fingerprint: OperationFingerprint,
    commit_write_handle: WriteHandleProto,
    commit_final_size: u64,
    commit_committed_blocks_snapshot: Vec<CommittedBlock>,
    session_identity: String,
    detail: String,
}

#[derive(Clone)]
struct AbortCleanupState {
    metadata_call_id: CallId,
    metadata_fingerprint: OperationFingerprint,
    metadata_write_handle: WriteHandleProto,
    session_identity: String,
    detail: String,
    worker_cleanups: Vec<AbortWorkerCleanupState>,
}

impl std::fmt::Debug for AbortCleanupState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AbortCleanupState").finish_non_exhaustive()
    }
}

#[derive(Clone)]
struct AbortWorkerCleanupState {
    abort_call_id: CallId,
    abort_fingerprint: OperationFingerprint,
    worker_block: WorkerWriteBlock,
    detail: String,
}

impl std::fmt::Debug for AbortWorkerCleanupState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AbortWorkerCleanupState").finish_non_exhaustive()
    }
}

/// Frozen side-effecting abort cleanup plan.
#[derive(Clone)]
pub(crate) struct AbortCleanupPlan {
    metadata_operation: OperationContext,
    metadata_request: AbortFileWriteRequestProto,
    worker_cleanups: Vec<AbortWorkerCleanupPlan>,
}

impl AbortCleanupPlan {
    /// Metadata AbortFileWrite operation with stable call identity.
    pub(crate) fn metadata_operation(&self) -> OperationContext {
        self.metadata_operation.clone()
    }

    /// Metadata AbortFileWrite request frozen before cleanup starts.
    pub(crate) fn metadata_request(&self) -> AbortFileWriteRequestProto {
        self.metadata_request.clone()
    }

    /// Per-worker cleanup operations frozen before cleanup starts.
    pub(crate) fn worker_cleanups(&self) -> &[AbortWorkerCleanupPlan] {
        &self.worker_cleanups
    }
}

/// Frozen worker AbortWrite cleanup operation.
#[derive(Clone)]
pub(crate) struct AbortWorkerCleanupPlan {
    operation: OperationContext,
    worker_block: WorkerWriteBlock,
}

impl AbortWorkerCleanupPlan {
    /// Worker AbortWrite operation with stable call identity.
    pub(crate) fn operation(&self) -> OperationContext {
        self.operation.clone()
    }

    /// Worker write block snapshot to abort.
    pub(crate) fn worker_block(&self) -> &WorkerWriteBlock {
        &self.worker_block
    }
}

fn validate_write_handle(handle: &WriteHandleProto) -> ClientResult<()> {
    if handle.handle_id == 0 {
        return Err(ClientError::Metadata(
            "write handle handle_id must be non-zero".to_string(),
        ));
    }
    if handle.lease_id.is_none() {
        return Err(ClientError::Metadata("write handle missing lease_id".to_string()));
    }
    if handle.lease_epoch == 0 {
        return Err(ClientError::Metadata(
            "write handle lease_epoch must be non-zero".to_string(),
        ));
    }
    if handle.open_epoch == 0 {
        return Err(ClientError::Metadata(
            "write handle open_epoch must be non-zero".to_string(),
        ));
    }
    if handle.fencing_token.is_none() {
        return Err(ClientError::Metadata("write handle missing fencing_token".to_string()));
    }
    Ok(())
}

fn commit_fingerprint_detail(
    write_handle: &WriteHandleProto,
    inode_id: InodeId,
    data_handle_id: DataHandleId,
    final_size: u64,
    committed_blocks: &[CommittedBlock],
) -> String {
    let mut detail = String::new();
    let _ = write!(
        &mut detail,
        "inode={} write_handle={} data_handle={} lease={} lease_epoch={} open_epoch={} final_size={} fencing={}",
        inode_id.as_raw(),
        write_handle.handle_id,
        data_handle_id.as_raw(),
        lease_identity(write_handle),
        write_handle.lease_epoch,
        write_handle.open_epoch,
        final_size,
        fencing_identity(write_handle)
    );
    detail.push_str(" blocks=[");
    for block in committed_blocks {
        let _ = write!(
            &mut detail,
            "{}:{}@{}+{};",
            block.block_id.data_handle_id.as_raw(),
            block.block_id.index.as_raw(),
            block.file_offset,
            block.len
        );
    }
    detail.push(']');
    detail
}

fn abort_file_fingerprint_detail(
    write_handle: &WriteHandleProto,
    inode_id: InodeId,
    data_handle_id: DataHandleId,
    worker_cleanups: &[AbortWorkerCleanupState],
) -> String {
    let mut detail = String::new();
    let _ = write!(
        &mut detail,
        "inode={} write_handle={} data_handle={} lease={} lease_epoch={} open_epoch={} fencing={}",
        inode_id.as_raw(),
        write_handle.handle_id,
        data_handle_id.as_raw(),
        lease_identity(write_handle),
        write_handle.lease_epoch,
        write_handle.open_epoch,
        fencing_identity(write_handle)
    );
    detail.push_str(" worker_cleanups=[");
    for cleanup in worker_cleanups {
        append_worker_block_identity(&mut detail, &cleanup.worker_block);
        detail.push(';');
    }
    detail.push(']');
    detail
}

fn abort_worker_fingerprint_detail(worker_block: &WorkerWriteBlock) -> String {
    let mut detail = String::new();
    append_worker_block_identity(&mut detail, worker_block);
    detail
}

fn append_worker_block_identity(detail: &mut String, worker_block: &WorkerWriteBlock) {
    let block_id = worker_block.target.block_id;
    let _ = write!(
        detail,
        "group={} worker={} protocol={} worker_epoch={} block={} stamp={} offset={} len={} stream={}:{} next_seq={}",
        worker_block.group_id,
        worker_block.worker.worker_id,
        proto::common::WorkerNetProtocolProto::from(worker_block.worker.worker_net_protocol) as i32,
        worker_block.worker.worker_epoch,
        block_id,
        worker_block.target.block_stamp,
        worker_block.target.file_offset,
        worker_block.target.len,
        worker_block.stream_id.high,
        worker_block.stream_id.low,
        worker_block.next_seq
    );
}

fn lease_identity(write_handle: &WriteHandleProto) -> String {
    write_handle
        .lease_id
        .as_ref()
        .map(|lease| format!("{}:{}", lease.high, lease.low))
        .unwrap_or_else(|| "missing".to_string())
}

fn fencing_identity(write_handle: &WriteHandleProto) -> String {
    write_handle
        .fencing_token
        .as_ref()
        .map(|token| {
            let block = token
                .block_id
                .as_ref()
                .map(|block| format!("{}:{}", block.data_handle_id, block.block_index))
                .unwrap_or_else(|| "missing".to_string());
            format!("block={} owner={} epoch={}", block, token.owner, token.epoch)
        })
        .unwrap_or_else(|| "missing".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    use proto::common::{BlockIdProto, FencingTokenProto, LeaseIdProto, StreamIdProto};
    use types::lease::FencingToken;
    use types::{
        BlockId, BlockIndex, ClientId, CommittedBlock, DataHandleId, WorkerEndpointInfo, WorkerId, WorkerNetProtocol,
        WriteTarget,
    };

    use crate::data::WorkerWriteBlock;
    use crate::runtime::AttemptContext;

    #[test]
    fn commit_file_fingerprint_is_stable_for_same_payload() {
        let handle = write_handle_proto(1, 302);
        let blocks = vec![committed_block(302, 0, 0, 5)];

        let first = commit_fingerprint(&handle, 5, &blocks);
        let second = commit_fingerprint(&handle, 5, &blocks);

        assert_eq!(first, second);
    }

    #[test]
    fn commit_file_fingerprint_changes_when_final_size_changes() {
        let handle = write_handle_proto(1, 302);
        let blocks = vec![committed_block(302, 0, 0, 5)];

        assert_ne!(
            commit_fingerprint(&handle, 5, &blocks),
            commit_fingerprint(&handle, 6, &blocks)
        );
    }

    #[test]
    fn commit_file_fingerprint_changes_when_committed_blocks_change() {
        let handle = write_handle_proto(1, 302);
        let first_blocks = vec![committed_block(302, 0, 0, 5)];
        let changed_blocks = vec![committed_block(302, 0, 0, 4)];

        assert_ne!(
            commit_fingerprint(&handle, 5, &first_blocks),
            commit_fingerprint(&handle, 5, &changed_blocks)
        );
    }

    #[test]
    fn commit_file_fingerprint_changes_when_write_handle_changes() {
        let first_handle = write_handle_proto(1, 302);
        let changed_handle = write_handle_proto(2, 302);
        let blocks = vec![committed_block(302, 0, 0, 5)];

        assert_ne!(
            commit_fingerprint(&first_handle, 5, &blocks),
            commit_fingerprint(&changed_handle, 5, &blocks)
        );
    }

    #[test]
    fn prepare_commit_file_reuses_call_id_fingerprint_and_frozen_payload() {
        let mut session = WriteSession::new(
            "/alpha".to_string(),
            InodeId::new(301),
            DataHandleId::new(302),
            write_handle_proto(1, 302),
            0,
        )
        .expect("session");
        let blocks = vec![committed_block(302, 0, 0, 5)];

        let (first_operation, first_request) = session
            .prepare_commit_file(ClientId::new(7), blocks.clone(), 5)
            .expect("first commit plan");
        session.mark_commit_unknown();
        let (second_operation, second_request) = session
            .prepare_commit_file(ClientId::new(7), blocks, 5)
            .expect("retry commit plan");

        let first_ctx = AttemptContext::for_metadata(&first_operation, 9, 0).expect("first context");
        let second_ctx = AttemptContext::for_metadata(&second_operation, 9, 0).expect("second context");
        assert_eq!(first_ctx.call_id(), second_ctx.call_id());
        assert_eq!(
            first_operation.operation_fingerprint(),
            second_operation.operation_fingerprint()
        );
        assert_eq!(first_request.committed_blocks, second_request.committed_blocks);
        assert_eq!(first_request.final_size, second_request.final_size);
    }

    #[test]
    fn prepare_commit_file_rejects_changed_payload_after_commit_started() {
        let mut session = WriteSession::new(
            "/alpha".to_string(),
            InodeId::new(301),
            DataHandleId::new(302),
            write_handle_proto(1, 302),
            0,
        )
        .expect("session");

        session
            .prepare_commit_file(ClientId::new(7), vec![committed_block(302, 0, 0, 5)], 5)
            .expect("first commit plan");
        let err = session
            .prepare_commit_file(ClientId::new(7), vec![committed_block(302, 0, 0, 6)], 6)
            .expect_err("changed commit payload must fail");

        assert!(matches!(err, ClientError::InvalidArgument(msg) if msg.contains("payload changed")));
    }

    #[test]
    fn prepare_commit_file_rejects_session_identity_change_after_unknown_without_replacing_call_id() {
        let mut session = WriteSession::new(
            "/alpha".to_string(),
            InodeId::new(301),
            DataHandleId::new(302),
            write_handle_proto(1, 302),
            0,
        )
        .expect("session");
        let blocks = vec![committed_block(302, 0, 0, 5)];

        let (first_operation, _) = session
            .prepare_commit_file(ClientId::new(7), blocks.clone(), 5)
            .expect("first commit plan");
        let first_ctx = AttemptContext::for_metadata(&first_operation, 9, 0).expect("first context");
        session.mark_commit_unknown();

        session.write_handle.lease_epoch = 2;
        let err = session
            .prepare_commit_file(ClientId::new(7), blocks.clone(), 5)
            .expect_err("changed session identity must fail");
        assert!(matches!(err, ClientError::InvalidArgument(msg) if msg.contains("identity changed")));

        session.write_handle.lease_epoch = 1;
        let (retry_operation, retry_request) = session
            .prepare_commit_file(ClientId::new(7), blocks, 5)
            .expect("retry commit plan");
        let retry_ctx = AttemptContext::for_metadata(&retry_operation, 9, 0).expect("retry context");

        assert_eq!(first_ctx.call_id(), retry_ctx.call_id());
        assert_eq!(
            first_operation.operation_fingerprint(),
            retry_operation.operation_fingerprint()
        );
        assert_eq!(retry_request.final_size, 5);
        assert_eq!(
            retry_request.committed_blocks,
            vec![proto::metadata::CommittedBlockProto::from(committed_block(
                302, 0, 0, 5
            ))]
        );
    }

    #[test]
    fn prepare_abort_cleanup_reuses_call_id_fingerprint_and_frozen_payload() {
        let mut session = WriteSession::new(
            "/alpha".to_string(),
            InodeId::new(301),
            DataHandleId::new(302),
            write_handle_proto(1, 302),
            0,
        )
        .expect("session");
        session
            .push_pending_block(write_target(302, 0, 0, 5), worker_block(302, 0, 0, 5, 9), 5, 1)
            .expect("pending block");

        let first = session
            .prepare_abort_cleanup(ClientId::new(7))
            .expect("first abort plan");
        let first_metadata =
            AttemptContext::for_metadata(&first.metadata_operation(), 9, 0).expect("first metadata context");
        let first_worker = first.worker_cleanups()[0].operation();
        let first_worker_ctx = AttemptContext::for_data(&first_worker, 0);
        let first_worker_snapshot = worker_block_signature(first.worker_cleanups()[0].worker_block());
        session.pending_blocks.clear();

        let second = session
            .prepare_abort_cleanup(ClientId::new(7))
            .expect("retry abort plan");
        let second_metadata =
            AttemptContext::for_metadata(&second.metadata_operation(), 9, 0).expect("second metadata context");
        let second_worker = second.worker_cleanups()[0].operation();
        let second_worker_ctx = AttemptContext::for_data(&second_worker, 0);

        assert_eq!(first_metadata.call_id(), second_metadata.call_id());
        assert_eq!(
            first.metadata_operation().operation_fingerprint(),
            second.metadata_operation().operation_fingerprint()
        );
        assert_eq!(
            first.metadata_request().write_handle,
            second.metadata_request().write_handle
        );
        assert_eq!(second.worker_cleanups().len(), 1);
        assert_eq!(first_worker_ctx.call_id(), second_worker_ctx.call_id());
        assert_eq!(
            first_worker.operation_fingerprint(),
            second_worker.operation_fingerprint()
        );
        assert_eq!(
            first_worker_snapshot,
            worker_block_signature(second.worker_cleanups()[0].worker_block())
        );
    }

    #[test]
    fn prepare_abort_cleanup_rejects_session_identity_drift_after_unknown_without_replacing_call_id() {
        let mut session = WriteSession::new(
            "/alpha".to_string(),
            InodeId::new(301),
            DataHandleId::new(302),
            write_handle_proto(1, 302),
            0,
        )
        .expect("session");

        let first = session
            .prepare_abort_cleanup(ClientId::new(7))
            .expect("first abort plan");
        let first_ctx =
            AttemptContext::for_metadata(&first.metadata_operation(), 9, 0).expect("first metadata context");

        session.write_handle.lease_epoch = 2;
        let err = match session.prepare_abort_cleanup(ClientId::new(7)) {
            Ok(_) => panic!("identity drift must reject abort replay"),
            Err(err) => err,
        };
        assert!(matches!(err, ClientError::InvalidArgument(msg) if msg.contains("identity changed")));

        session.write_handle.lease_epoch = 1;
        let retry = session
            .prepare_abort_cleanup(ClientId::new(7))
            .expect("retry abort plan");
        let retry_ctx =
            AttemptContext::for_metadata(&retry.metadata_operation(), 9, 0).expect("retry metadata context");
        assert_eq!(first_ctx.call_id(), retry_ctx.call_id());
        assert_eq!(
            first.metadata_operation().operation_fingerprint(),
            retry.metadata_operation().operation_fingerprint()
        );
    }

    #[test]
    fn prepare_abort_cleanup_after_worker_committed_block_is_conservative() {
        let mut session = WriteSession::new(
            "/alpha".to_string(),
            InodeId::new(301),
            DataHandleId::new(302),
            write_handle_proto(1, 302),
            0,
        )
        .expect("session");
        session
            .push_pending_block(write_target(302, 0, 0, 5), worker_block(302, 0, 0, 5, 9), 5, 1)
            .expect("pending block");
        session.pending_blocks[0].mark_worker_committed(false);

        let err = match session.prepare_abort_cleanup(ClientId::new(7)) {
            Ok(_) => panic!("committed worker block cannot be safely aborted"),
            Err(err) => err,
        };
        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("worker block commit")));
        assert!(matches!(
            session.ensure_open_for_write_at_ms(0),
            Err(ClientError::StaleHandle { reason }) if reason.contains("abort outcome")
        ));
        assert!(matches!(
            session.ensure_open_for_close_at_ms(0),
            Err(ClientError::StaleHandle { reason }) if reason.contains("abort outcome")
        ));
    }

    #[test]
    fn lease_expiry_guard_marks_session_expired_and_blocks_side_effects() {
        let mut session = WriteSession::new(
            "/alpha".to_string(),
            InodeId::new(301),
            DataHandleId::new(302),
            write_handle_proto(1, 302),
            0,
        )
        .expect("session");

        session.update_expires_at_ms(1_000);
        let err = session
            .ensure_open_for_write_at_ms(1_001)
            .expect_err("expired lease must block write");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("expired")));

        for rejected in [
            session.ensure_open_for_write_at_ms(1_001),
            session.ensure_open_for_close_at_ms(1_001),
            session.ensure_open_for_renew_at_ms(1_001),
            session.ensure_open_for_abort_at_ms(1_001),
            session.ensure_open_for_barrier_at_ms(1_001),
        ] {
            assert!(matches!(rejected, Err(ClientError::StaleHandle { reason }) if reason.contains("expired")));
        }
    }

    #[test]
    fn state_transition_table_covers_all_write_session_states() {
        #[derive(Clone, Copy)]
        enum Operation {
            Write,
            Close,
            Abort,
            RenewLease,
            Barrier,
        }

        let cases = [
            (WriteSessionState::Open, Operation::Write, true),
            (WriteSessionState::Open, Operation::Close, true),
            (WriteSessionState::Open, Operation::Abort, true),
            (WriteSessionState::Open, Operation::RenewLease, true),
            (WriteSessionState::Open, Operation::Barrier, true),
            (WriteSessionState::CommitStarted, Operation::Write, false),
            (WriteSessionState::CommitStarted, Operation::Close, true),
            (WriteSessionState::CommitStarted, Operation::Abort, false),
            (WriteSessionState::CommitStarted, Operation::RenewLease, false),
            (WriteSessionState::CommitStarted, Operation::Barrier, false),
            (WriteSessionState::CommitUnknown, Operation::Write, false),
            (WriteSessionState::CommitUnknown, Operation::Close, true),
            (WriteSessionState::CommitUnknown, Operation::Abort, false),
            (WriteSessionState::CommitUnknown, Operation::RenewLease, false),
            (WriteSessionState::CommitUnknown, Operation::Barrier, false),
            (WriteSessionState::Closed, Operation::Write, false),
            (WriteSessionState::Closed, Operation::Close, false),
            (WriteSessionState::Closed, Operation::Abort, false),
            (WriteSessionState::Closed, Operation::RenewLease, false),
            (WriteSessionState::Closed, Operation::Barrier, false),
            (WriteSessionState::Aborted, Operation::Write, false),
            (WriteSessionState::Aborted, Operation::Close, false),
            (WriteSessionState::Aborted, Operation::Abort, false),
            (WriteSessionState::Aborted, Operation::RenewLease, false),
            (WriteSessionState::Aborted, Operation::Barrier, false),
            (WriteSessionState::UnknownOutcome, Operation::Write, false),
            (WriteSessionState::UnknownOutcome, Operation::Close, false),
            (WriteSessionState::UnknownOutcome, Operation::Abort, false),
            (WriteSessionState::UnknownOutcome, Operation::RenewLease, false),
            (WriteSessionState::UnknownOutcome, Operation::Barrier, false),
            (WriteSessionState::SessionInvalid, Operation::Write, false),
            (WriteSessionState::SessionInvalid, Operation::Close, false),
            (WriteSessionState::SessionInvalid, Operation::Abort, false),
            (WriteSessionState::SessionInvalid, Operation::RenewLease, false),
            (WriteSessionState::SessionInvalid, Operation::Barrier, false),
            (WriteSessionState::SessionExpired, Operation::Write, false),
            (WriteSessionState::SessionExpired, Operation::Close, false),
            (WriteSessionState::SessionExpired, Operation::Abort, false),
            (WriteSessionState::SessionExpired, Operation::RenewLease, false),
            (WriteSessionState::SessionExpired, Operation::Barrier, false),
            (WriteSessionState::AbortUnknown, Operation::Write, false),
            (WriteSessionState::AbortUnknown, Operation::Close, false),
            (WriteSessionState::AbortUnknown, Operation::Abort, true),
            (WriteSessionState::AbortUnknown, Operation::RenewLease, false),
            (WriteSessionState::AbortUnknown, Operation::Barrier, false),
        ];

        for (state, operation, allowed) in cases {
            let mut session = WriteSession::new(
                "/alpha".to_string(),
                InodeId::new(301),
                DataHandleId::new(302),
                write_handle_proto(1, 302),
                0,
            )
            .expect("session");
            session.state = state;

            let result = match operation {
                Operation::Write => session.ensure_open_for_write_at_ms(0),
                Operation::Close => session.ensure_open_for_close_at_ms(0),
                Operation::Abort => session.ensure_open_for_abort_at_ms(0),
                Operation::RenewLease => session.ensure_open_for_renew_at_ms(0),
                Operation::Barrier => session.ensure_open_for_barrier_at_ms(0),
            };
            assert_eq!(result.is_ok(), allowed, "unexpected transition for {state:?}");
        }
    }

    fn commit_fingerprint(
        write_handle: &WriteHandleProto,
        final_size: u64,
        committed_blocks: &[CommittedBlock],
    ) -> OperationFingerprint {
        let detail = commit_fingerprint_detail(
            write_handle,
            InodeId::new(301),
            DataHandleId::new(302),
            final_size,
            committed_blocks,
        );
        OperationIdentity::session("/alpha", "session-1")
            .with_detail(detail)
            .fingerprint(OperationKind::MetadataSessionBarrier, "CommitFile")
    }

    fn write_handle_proto(handle_id: u64, data_handle_id: u64) -> WriteHandleProto {
        WriteHandleProto {
            handle_id,
            lease_id: Some(LeaseIdProto {
                high: 0,
                low: handle_id,
            }),
            lease_epoch: 1,
            open_epoch: 1,
            fencing_token: Some(FencingTokenProto {
                block_id: Some(BlockIdProto {
                    data_handle_id,
                    block_index: 0,
                }),
                owner: 7,
                epoch: 1,
            }),
        }
    }

    fn committed_block(data_handle_id: u64, block_index: u32, file_offset: u64, len: u64) -> CommittedBlock {
        CommittedBlock {
            block_id: BlockId::new(DataHandleId::new(data_handle_id), BlockIndex::new(block_index)),
            file_offset,
            len,
            checksum: None,
        }
    }

    fn write_target(data_handle_id: u64, block_index: u32, file_offset: u64, len: u64) -> WriteTarget {
        WriteTarget {
            block_id: BlockId::new(DataHandleId::new(data_handle_id), BlockIndex::new(block_index)),
            file_offset,
            len,
            worker_endpoints: vec![worker_endpoint()],
            fencing_token: FencingToken {
                block_id: BlockId::new(DataHandleId::new(data_handle_id), BlockIndex::new(block_index)),
                owner: ClientId::new(7),
                epoch: 1,
            },
            block_stamp: 77,
            chunk_size: 1024,
        }
    }

    fn worker_endpoint() -> WorkerEndpointInfo {
        WorkerEndpointInfo {
            worker_id: WorkerId::new(11),
            endpoint: "127.0.0.1:19101".to_string(),
            worker_net_protocol: WorkerNetProtocol::Grpc,
            worker_epoch: 13,
        }
    }

    fn worker_block(
        data_handle_id: u64,
        block_index: u32,
        file_offset: u64,
        len: u64,
        stream_low: u64,
    ) -> WorkerWriteBlock {
        WorkerWriteBlock {
            group_id: 9,
            worker: worker_endpoint(),
            target: write_target(data_handle_id, block_index, file_offset, len),
            stream_id: StreamIdProto {
                high: 1,
                low: stream_low,
            },
            frame_size: 1024,
            next_seq: 1,
        }
    }

    fn worker_block_signature(block: &WorkerWriteBlock) -> (u64, u64, i32, u64, u64, u64, u64, u32, u64, u64, u64) {
        (
            block.group_id,
            block.worker.worker_id.as_raw(),
            proto::common::WorkerNetProtocolProto::from(block.worker.worker_net_protocol) as i32,
            block.worker.worker_epoch,
            block.target.file_offset,
            block.target.len,
            block.target.block_stamp,
            block.target.block_id.index.as_raw(),
            block.target.block_id.data_handle_id.as_raw(),
            block.stream_id.high,
            block.stream_id.low,
        )
    }
}
