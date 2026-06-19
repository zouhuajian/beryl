// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Client-side sequential write session state.

use std::fmt::Write as _;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use proto::metadata::WriteHandleProto;
use types::{CallId, ClientId, CommittedBlock, DataHandleId, FileLayout, WriteTarget};

use crate::data::WorkerBlockWriteHandle;
use crate::error::{ClientError, ClientResult};
use crate::runtime::context::{OperationContext, OperationFingerprint, OperationIdentity};
use crate::runtime::policy::OperationKind;

const LEASE_EXPIRY_SAFETY_WINDOW_MS: u64 = 1_000;

/// Open sequential write session tracked by an internal file handle field.
#[derive(Clone, Debug)]
pub(crate) struct WriteSession {
    path: String,
    data_handle_id: DataHandleId,
    layout: FileLayout,
    file_version: Option<u64>,
    write_handle: WriteHandleProto,
    cursor: u64,
    flush_cursor: u64,
    base_size: u64,
    buffered: BytesMut,
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
        data_handle_id: DataHandleId,
        layout: FileLayout,
        write_handle: WriteHandleProto,
        base_size: u64,
        expires_at_ms: u64,
    ) -> ClientResult<Self> {
        validate_write_handle(&write_handle)?;
        if expires_at_ms == 0 {
            return Err(ClientError::InvalidArgument(
                "write session expires_at_ms must be non-zero".to_string(),
            ));
        }
        layout
            .validate()
            .map_err(|err| ClientError::InvalidLayout(format!("write session layout invalid: {err}")))?;
        Ok(Self {
            path,
            data_handle_id,
            layout,
            file_version: None,
            write_handle,
            cursor: base_size,
            flush_cursor: base_size,
            base_size,
            buffered: BytesMut::new(),
            expires_at_ms: Some(expires_at_ms),
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

    /// Number of locally buffered bytes not yet assigned to a worker block.
    pub(crate) fn buffered_len(&self) -> usize {
        self.buffered.len()
    }

    /// Metadata-confirmed block size as a usize for local buffering decisions.
    pub(crate) fn block_size_usize(&self) -> usize {
        self.layout.block_size as usize
    }

    /// Accept bytes into the SDK-visible write cursor.
    pub(crate) fn advance_cursor(&mut self, len: usize) -> ClientResult<()> {
        self.cursor = self
            .cursor
            .checked_add(len as u64)
            .ok_or_else(|| ClientError::InvalidArgument("write cursor overflow".to_string()))?;
        Ok(())
    }

    /// Append bytes to the current local block buffer.
    pub(crate) fn buffer_bytes(&mut self, data: &[u8]) -> ClientResult<()> {
        self.advance_cursor(data.len())?;
        self.buffered.extend_from_slice(data);
        Ok(())
    }

    /// Take a full buffered block when the metadata-confirmed boundary is reached.
    pub(crate) fn take_full_buffered_block(&mut self) -> Option<Bytes> {
        let block_size = self.block_size_usize();
        if self.buffered.len() < block_size {
            return None;
        }
        Some(self.buffered.split_to(block_size).freeze())
    }

    /// Take any remaining buffered tail for a barrier or close.
    pub(crate) fn take_buffered_tail(&mut self) -> Option<Bytes> {
        if self.buffered.is_empty() {
            return None;
        }
        let len = self.buffered.len();
        Some(self.buffered.split_to(len).freeze())
    }

    /// Discard local bytes that never reached metadata or a worker.
    pub(crate) fn discard_buffered_bytes(&mut self) {
        self.buffered.clear();
    }

    /// Metadata write handle.
    pub(crate) fn write_handle(&self) -> WriteHandleProto {
        self.write_handle
    }

    /// Stable identity used by replay gates for session-scoped operations.
    pub(crate) fn session_identity(&self) -> String {
        format!(
            "handle={} data_handle={} base_size={} lease_epoch={} open_epoch={} fencing={}",
            self.write_handle.handle_id,
            self.data_handle_id.as_raw(),
            self.base_size,
            self.write_handle.lease_epoch,
            self.write_handle.open_epoch,
            fencing_identity(&self.write_handle)
        )
    }

    /// Validate a metadata write target before opening the worker stream.
    pub(crate) fn validate_target(&mut self, target: &WriteTarget, expected_len: u64) -> ClientResult<()> {
        self.ensure_open_for_write()?;
        if target.file_offset != self.flush_cursor {
            return Err(ClientError::InvalidLayout(format!(
                "write target file_offset mismatch: expected {}, got {}",
                self.flush_cursor, target.file_offset
            )));
        }
        if target.effective_len != expected_len {
            return Err(ClientError::InvalidLayout(format!(
                "write target effective_len mismatch: expected {}, got {}",
                expected_len, target.effective_len
            )));
        }
        if target.block_size == 0 {
            return Err(ClientError::InvalidLayout(
                "write target block_size must be non-zero".to_string(),
            ));
        }
        if target.effective_len > target.block_size {
            return Err(ClientError::InvalidLayout(
                "write target effective_len must not exceed block_size".to_string(),
            ));
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
        if u64::from(target.chunk_size) > target.block_size {
            return Err(ClientError::InvalidLayout(
                "write target chunk_size must not exceed block_size".to_string(),
            ));
        }
        if !target.block_size.is_multiple_of(u64::from(target.chunk_size)) {
            return Err(ClientError::InvalidLayout(
                "write target block_size must be a multiple of chunk_size".to_string(),
            ));
        }
        Ok(())
    }

    /// Record a worker-accepted block and advance the cursor.
    pub(crate) fn push_pending_block(
        &mut self,
        target: WriteTarget,
        block_write_handle: WorkerBlockWriteHandle,
        written_len: u64,
        commit_seq: u64,
    ) -> ClientResult<()> {
        if commit_seq == 0 {
            return Err(ClientError::Worker(
                "worker WriteStream acknowledged no non-empty frame".to_string(),
            ));
        }
        let final_offset = self
            .flush_cursor
            .checked_add(written_len)
            .ok_or_else(|| ClientError::InvalidArgument("write flush cursor overflow".to_string()))?;
        self.pending_blocks.push(PendingBlock {
            target,
            block_write_handle,
            written_len,
            commit_seq,
            worker_commit_level: WorkerCommitLevel::Uncommitted,
        });
        self.flush_cursor = final_offset;
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
        client_name: &str,
        committed_blocks: Vec<CommittedBlock>,
        final_size: u64,
    ) -> ClientResult<CommitFilePlan> {
        match self.state {
            WriteSessionState::Open => {
                let session_identity = self.session_identity();
                let detail =
                    commit_fingerprint_detail(&self.write_handle, self.data_handle_id, final_size, &committed_blocks);
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
                let detail =
                    commit_fingerprint_detail(&self.write_handle, self.data_handle_id, final_size, &committed_blocks);
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
        let operation = OperationContext::with_call_id_named_and_fingerprint(
            client_id,
            client_name,
            commit.commit_call_id,
            OperationKind::MetadataSessionBarrier,
            "CommitFile",
            OperationIdentity::session(self.path.clone(), commit.session_identity.clone())
                .with_detail(commit.detail.clone()),
            commit.commit_fingerprint,
        )?;
        Ok(CommitFilePlan {
            operation,
            write_handle: commit.commit_write_handle,
            data_handle_id: self.data_handle_id,
            committed_blocks: commit.commit_committed_blocks_snapshot.clone(),
            final_size: commit.commit_final_size,
        })
    }

    /// Freeze and return the abort cleanup plan for this write session.
    pub(crate) fn prepare_abort_cleanup(
        &mut self,
        client_id: ClientId,
        client_name: &str,
    ) -> ClientResult<AbortCleanupPlan> {
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
                        let block_write_handle = pending.block_write_handle().clone();
                        let detail = abort_block_write_handle_fingerprint_detail(&block_write_handle);
                        let identity = OperationIdentity::session(self.path.clone(), session_identity.clone())
                            .with_detail(detail.clone());
                        let abort_fingerprint = identity.fingerprint(OperationKind::WorkerWriteData, "AbortWrite");
                        AbortWorkerCleanupState {
                            abort_call_id: CallId::new(),
                            abort_fingerprint,
                            block_write_handle,
                            detail,
                        }
                    })
                    .collect::<Vec<_>>();
                let detail = abort_file_fingerprint_detail(&self.write_handle, self.data_handle_id, &worker_cleanups);
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
        let metadata_operation = OperationContext::with_call_id_named_and_fingerprint(
            client_id,
            client_name,
            abort.metadata_call_id,
            OperationKind::CleanupBestEffort,
            "AbortFileWrite",
            OperationIdentity::session(self.path.clone(), abort.session_identity.clone())
                .with_detail(abort.detail.clone()),
            abort.metadata_fingerprint,
        )?;
        let mut worker_cleanups = Vec::with_capacity(abort.worker_cleanups.len());
        for cleanup in &abort.worker_cleanups {
            let operation = OperationContext::with_call_id_named_and_fingerprint(
                client_id,
                client_name,
                cleanup.abort_call_id,
                OperationKind::WorkerWriteData,
                "AbortWrite",
                OperationIdentity::session(self.path.clone(), abort.session_identity.clone())
                    .with_detail(cleanup.detail.clone()),
                cleanup.abort_fingerprint,
            )?;
            worker_cleanups.push(AbortWorkerCleanupPlan {
                operation,
                block_write_handle: cleanup.block_write_handle.clone(),
            });
        }
        Ok(AbortCleanupPlan {
            metadata_operation,
            metadata_write_handle: abort.metadata_write_handle,
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
    block_write_handle: WorkerBlockWriteHandle,
    written_len: u64,
    commit_seq: u64,
    worker_commit_level: WorkerCommitLevel,
}

impl PendingBlock {
    /// Metadata write target for this block.
    pub(crate) fn target(&self) -> &WriteTarget {
        &self.target
    }

    /// Worker block write handle for this pending block.
    pub(crate) fn block_write_handle(&self) -> &WorkerBlockWriteHandle {
        &self.block_write_handle
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
    block_write_handle: WorkerBlockWriteHandle,
    detail: String,
}

impl std::fmt::Debug for AbortWorkerCleanupState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AbortWorkerCleanupState").finish_non_exhaustive()
    }
}

/// Frozen metadata CommitFile operation and request payload.
#[derive(Clone, Debug)]
pub(crate) struct CommitFilePlan {
    pub(crate) operation: OperationContext,
    pub(crate) write_handle: WriteHandleProto,
    pub(crate) data_handle_id: DataHandleId,
    pub(crate) committed_blocks: Vec<CommittedBlock>,
    pub(crate) final_size: u64,
}

/// Frozen side-effecting abort cleanup plan.
#[derive(Clone)]
pub(crate) struct AbortCleanupPlan {
    metadata_operation: OperationContext,
    metadata_write_handle: WriteHandleProto,
    worker_cleanups: Vec<AbortWorkerCleanupPlan>,
}

impl AbortCleanupPlan {
    /// Metadata AbortFileWrite operation with stable call identity.
    pub(crate) fn metadata_operation(&self) -> OperationContext {
        self.metadata_operation.clone()
    }

    /// Metadata write handle payload frozen before cleanup starts.
    pub(crate) fn metadata_write_handle(&self) -> WriteHandleProto {
        self.metadata_write_handle
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
    block_write_handle: WorkerBlockWriteHandle,
}

impl AbortWorkerCleanupPlan {
    /// Worker AbortWrite operation with stable call identity.
    pub(crate) fn operation(&self) -> OperationContext {
        self.operation.clone()
    }

    /// Worker block write handle snapshot to abort.
    pub(crate) fn block_write_handle(&self) -> &WorkerBlockWriteHandle {
        &self.block_write_handle
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
    data_handle_id: DataHandleId,
    final_size: u64,
    committed_blocks: &[CommittedBlock],
) -> String {
    let mut detail = String::new();
    let _ = write!(
        &mut detail,
        "write_handle={} data_handle={} lease={} lease_epoch={} open_epoch={} final_size={} fencing={}",
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
    data_handle_id: DataHandleId,
    worker_cleanups: &[AbortWorkerCleanupState],
) -> String {
    let mut detail = String::new();
    let _ = write!(
        &mut detail,
        "write_handle={} data_handle={} lease={} lease_epoch={} open_epoch={} fencing={}",
        write_handle.handle_id,
        data_handle_id.as_raw(),
        lease_identity(write_handle),
        write_handle.lease_epoch,
        write_handle.open_epoch,
        fencing_identity(write_handle)
    );
    detail.push_str(" worker_cleanups=[");
    for cleanup in worker_cleanups {
        append_block_write_handle_identity(&mut detail, &cleanup.block_write_handle);
        detail.push(';');
    }
    detail.push(']');
    detail
}

fn abort_block_write_handle_fingerprint_detail(block_write_handle: &WorkerBlockWriteHandle) -> String {
    let mut detail = String::new();
    append_block_write_handle_identity(&mut detail, block_write_handle);
    detail
}

fn append_block_write_handle_identity(detail: &mut String, block_write_handle: &WorkerBlockWriteHandle) {
    let block_id = block_write_handle.target.block_id;
    let _ = write!(
        detail,
        "group={} worker={} protocol={} worker_run_id={} block={} stamp={} offset={} effective_len={} stream={}:{} next_seq={}",
        block_write_handle.group_name,
        block_write_handle.worker.worker_id,
        proto::common::WorkerNetProtocolProto::from(block_write_handle.worker.worker_net_protocol) as i32,
        block_write_handle.worker.worker_run_id,
        block_id,
        block_write_handle.target.block_stamp,
        block_write_handle.target.file_offset,
        block_write_handle.target.effective_len,
        block_write_handle.stream_id.high,
        block_write_handle.stream_id.low,
        block_write_handle.next_seq
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
            let owner = token
                .owner
                .as_ref()
                .map(|owner| format!("{}:{}", owner.high, owner.low))
                .unwrap_or_else(|| "missing".to_string());
            format!("block={} owner={} epoch={}", block, owner, token.epoch)
        })
        .unwrap_or_else(|| "missing".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    use proto::common::{BlockIdProto, FencingTokenProto, LeaseIdProto, StreamIdProto};
    use types::lease::FencingToken;
    use types::{
        BlockId, BlockIndex, ClientId, CommittedBlock, DataHandleId, GroupName, WorkerEndpointInfo, WorkerId,
        WorkerNetProtocol, WriteTarget,
    };

    use crate::data::WorkerBlockWriteHandle;
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
            DataHandleId::new(302),
            test_layout(),
            write_handle_proto(1, 302),
            0,
            1_000,
        )
        .expect("session");
        let blocks = vec![committed_block(302, 0, 0, 5)];

        let first = session
            .prepare_commit_file(ClientId::new(7), "test-client", blocks.clone(), 5)
            .expect("first commit plan");
        session.mark_commit_unknown();
        let second = session
            .prepare_commit_file(ClientId::new(7), "test-client", blocks, 5)
            .expect("retry commit plan");

        let first_ctx = AttemptContext::for_metadata(&first.operation, test_group_name(), 0).expect("first context");
        let second_ctx = AttemptContext::for_metadata(&second.operation, test_group_name(), 0).expect("second context");
        assert_eq!(first_ctx.call_id(), second_ctx.call_id());
        assert_eq!(
            first.operation.operation_fingerprint(),
            second.operation.operation_fingerprint()
        );
        assert_eq!(first.committed_blocks, second.committed_blocks);
        assert_eq!(first.final_size, second.final_size);
    }

    #[test]
    fn prepare_commit_file_rejects_changed_payload_after_commit_started() {
        let mut session = WriteSession::new(
            "/alpha".to_string(),
            DataHandleId::new(302),
            test_layout(),
            write_handle_proto(1, 302),
            0,
            1_000,
        )
        .expect("session");

        session
            .prepare_commit_file(ClientId::new(7), "test-client", vec![committed_block(302, 0, 0, 5)], 5)
            .expect("first commit plan");
        let err = session
            .prepare_commit_file(ClientId::new(7), "test-client", vec![committed_block(302, 0, 0, 6)], 6)
            .expect_err("changed commit payload must fail");

        assert!(matches!(err, ClientError::InvalidArgument(msg) if msg.contains("payload changed")));
    }

    #[test]
    fn prepare_commit_file_rejects_session_identity_change_after_unknown_without_replacing_call_id() {
        let mut session = WriteSession::new(
            "/alpha".to_string(),
            DataHandleId::new(302),
            test_layout(),
            write_handle_proto(1, 302),
            0,
            1_000,
        )
        .expect("session");
        let blocks = vec![committed_block(302, 0, 0, 5)];

        let first = session
            .prepare_commit_file(ClientId::new(7), "test-client", blocks.clone(), 5)
            .expect("first commit plan");
        let first_ctx = AttemptContext::for_metadata(&first.operation, test_group_name(), 0).expect("first context");
        session.mark_commit_unknown();

        session.write_handle.lease_epoch = 2;
        let err = session
            .prepare_commit_file(ClientId::new(7), "test-client", blocks.clone(), 5)
            .expect_err("changed session identity must fail");
        assert!(matches!(err, ClientError::InvalidArgument(msg) if msg.contains("identity changed")));

        session.write_handle.lease_epoch = 1;
        let retry = session
            .prepare_commit_file(ClientId::new(7), "test-client", blocks, 5)
            .expect("retry commit plan");
        let retry_ctx = AttemptContext::for_metadata(&retry.operation, test_group_name(), 0).expect("retry context");

        assert_eq!(first_ctx.call_id(), retry_ctx.call_id());
        assert_eq!(
            first.operation.operation_fingerprint(),
            retry.operation.operation_fingerprint()
        );
        assert_eq!(retry.final_size, 5);
        assert_eq!(retry.committed_blocks, vec![committed_block(302, 0, 0, 5)]);
    }

    #[test]
    fn prepare_abort_cleanup_reuses_call_id_fingerprint_and_frozen_payload() {
        let mut session = WriteSession::new(
            "/alpha".to_string(),
            DataHandleId::new(302),
            test_layout(),
            write_handle_proto(1, 302),
            0,
            1_000,
        )
        .expect("session");
        session
            .push_pending_block(write_target(302, 0, 0, 5), block_write_handle(302, 0, 0, 5, 9), 5, 1)
            .expect("pending block");

        let first = session
            .prepare_abort_cleanup(ClientId::new(7), "test-client")
            .expect("first abort plan");
        let first_metadata = AttemptContext::for_metadata(&first.metadata_operation(), test_group_name(), 0)
            .expect("first metadata context");
        let first_worker = first.worker_cleanups()[0].operation();
        let first_worker_ctx = AttemptContext::for_data(&first_worker, 0);
        let first_worker_snapshot = block_write_handle_signature(first.worker_cleanups()[0].block_write_handle());
        session.pending_blocks.clear();

        let second = session
            .prepare_abort_cleanup(ClientId::new(7), "test-client")
            .expect("retry abort plan");
        let second_metadata = AttemptContext::for_metadata(&second.metadata_operation(), test_group_name(), 0)
            .expect("second metadata context");
        let second_worker = second.worker_cleanups()[0].operation();
        let second_worker_ctx = AttemptContext::for_data(&second_worker, 0);

        assert_eq!(first_metadata.call_id(), second_metadata.call_id());
        assert_eq!(
            first.metadata_operation().operation_fingerprint(),
            second.metadata_operation().operation_fingerprint()
        );
        assert_eq!(first.metadata_write_handle(), second.metadata_write_handle());
        assert_eq!(second.worker_cleanups().len(), 1);
        assert_eq!(first_worker_ctx.call_id(), second_worker_ctx.call_id());
        assert_eq!(
            first_worker.operation_fingerprint(),
            second_worker.operation_fingerprint()
        );
        assert_eq!(
            first_worker_snapshot,
            block_write_handle_signature(second.worker_cleanups()[0].block_write_handle())
        );
    }

    #[test]
    fn prepare_abort_cleanup_rejects_session_identity_drift_after_unknown_without_replacing_call_id() {
        let mut session = WriteSession::new(
            "/alpha".to_string(),
            DataHandleId::new(302),
            test_layout(),
            write_handle_proto(1, 302),
            0,
            1_000,
        )
        .expect("session");

        let first = session
            .prepare_abort_cleanup(ClientId::new(7), "test-client")
            .expect("first abort plan");
        let first_ctx = AttemptContext::for_metadata(&first.metadata_operation(), test_group_name(), 0)
            .expect("first metadata context");

        session.write_handle.lease_epoch = 2;
        let err = match session.prepare_abort_cleanup(ClientId::new(7), "test-client") {
            Ok(_) => panic!("identity drift must reject abort replay"),
            Err(err) => err,
        };
        assert!(matches!(err, ClientError::InvalidArgument(msg) if msg.contains("identity changed")));

        session.write_handle.lease_epoch = 1;
        let retry = session
            .prepare_abort_cleanup(ClientId::new(7), "test-client")
            .expect("retry abort plan");
        let retry_ctx = AttemptContext::for_metadata(&retry.metadata_operation(), test_group_name(), 0)
            .expect("retry metadata context");
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
            DataHandleId::new(302),
            test_layout(),
            write_handle_proto(1, 302),
            0,
            1_000,
        )
        .expect("session");
        session
            .push_pending_block(write_target(302, 0, 0, 5), block_write_handle(302, 0, 0, 5, 9), 5, 1)
            .expect("pending block");
        session.pending_blocks[0].mark_worker_committed(false);

        let err = match session.prepare_abort_cleanup(ClientId::new(7), "test-client") {
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
            DataHandleId::new(302),
            test_layout(),
            write_handle_proto(1, 302),
            0,
            1_000,
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
                DataHandleId::new(302),
                test_layout(),
                write_handle_proto(1, 302),
                0,
                10_000,
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
        let detail = commit_fingerprint_detail(write_handle, DataHandleId::new(302), final_size, committed_blocks);
        OperationIdentity::session("/alpha", "session-1")
            .with_detail(detail)
            .fingerprint(OperationKind::MetadataSessionBarrier, "CommitFile")
    }

    fn test_layout() -> FileLayout {
        FileLayout::new(1024, 1024, 1)
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
                owner: Some(ClientId::new(7).into()),
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
            block_size: 1024,
            effective_len: len,
            worker_endpoints: vec![worker_endpoint()],
            fencing_token: FencingToken {
                block_id: BlockId::new(DataHandleId::new(data_handle_id), BlockIndex::new(block_index)),
                owner: ClientId::new(7),
                epoch: 1,
            },
            block_stamp: 77,
            chunk_size: 1024,
            block_format_id: types::BlockFormatId::CURRENT_FOR_NEW_FILE,
            tier: types::Tier::Hdd,
        }
    }

    fn worker_endpoint() -> WorkerEndpointInfo {
        WorkerEndpointInfo {
            worker_id: WorkerId::new(11),
            endpoint: "127.0.0.1:19101".to_string(),
            worker_net_protocol: WorkerNetProtocol::Grpc,
            worker_run_id: "550e8400-e29b-41d4-a716-446655440000"
                .parse()
                .expect("valid test WorkerRunId"),
        }
    }

    fn block_write_handle(
        data_handle_id: u64,
        block_index: u32,
        file_offset: u64,
        len: u64,
        stream_low: u64,
    ) -> WorkerBlockWriteHandle {
        WorkerBlockWriteHandle {
            group_name: test_group_name(),
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

    fn block_write_handle_signature(
        block: &WorkerBlockWriteHandle,
    ) -> (String, u64, i32, String, u64, u64, u64, u32, u64, u64, u64) {
        (
            block.group_name.to_string(),
            block.worker.worker_id.as_raw(),
            proto::common::WorkerNetProtocolProto::from(block.worker.worker_net_protocol) as i32,
            block.worker.worker_run_id.to_string(),
            block.target.file_offset,
            block.target.effective_len,
            block.target.block_stamp,
            block.target.block_id.index.as_raw(),
            block.target.block_id.data_handle_id.as_raw(),
            block.stream_id.high,
            block.stream_id.low,
        )
    }

    fn test_group_name() -> GroupName {
        GroupName::parse("root").unwrap()
    }
}
