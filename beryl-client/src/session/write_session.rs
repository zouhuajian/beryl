// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Client-side sequential write session state.

use std::time::{SystemTime, UNIX_EPOCH};

use beryl_proto::metadata::WriteHandleProto;
use beryl_types::{BlockShape, CallId, ClientId, CommittedBlock, DataHandleId, FileLayout, WriteTarget};
use bytes::{Bytes, BytesMut};

use crate::data::WorkerBlockWriteHandle;
use crate::error::{ClientError, ClientResult};
use crate::runtime::context::{OperationContext, OperationDeadline};

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

    /// Predecessor that identifies the next logical AddBlock step.
    pub(crate) fn previous_block_id(&self) -> Option<beryl_types::BlockId> {
        self.pending_blocks.last().map(|pending| pending.target.block_id)
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
        BlockShape::new(
            target.block_format_id,
            target.block_size,
            target.chunk_size,
            target.effective_len,
        )
        .map_err(|err| ClientError::InvalidLayout(format!("write target has invalid shape: {err}")))?;
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
        deadline: OperationDeadline,
    ) -> ClientResult<CommitFilePlan> {
        match self.state {
            WriteSessionState::Open => {
                self.commit = Some(CommitFileState {
                    commit_call_id: CallId::new(),
                    commit_write_handle: self.write_handle,
                    commit_final_size: final_size,
                    commit_committed_blocks_snapshot: committed_blocks,
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
                if commit.commit_write_handle != self.write_handle {
                    return Err(ClientError::InvalidArgument(
                        "CommitFile write handle changed after commit started".to_string(),
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
        let operation = OperationContext::with_call_id_named(
            client_id,
            client_name,
            commit.commit_call_id,
            "CommitFile",
            Some(self.path.clone()),
            deadline,
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
        deadline: OperationDeadline,
    ) -> ClientResult<AbortCleanupPlan> {
        match self.state {
            WriteSessionState::Open => {
                if self.pending_blocks.iter().any(PendingBlock::has_worker_commit) {
                    self.state = WriteSessionState::AbortUnknown;
                    return Err(ClientError::UnknownOutcome(
                        "cannot safely abort after a worker block commit succeeded".to_string(),
                    ));
                }
                let worker_cleanups = self
                    .pending_blocks
                    .iter()
                    .map(|pending| {
                        let block_write_handle = pending.block_write_handle().clone();
                        AbortWorkerCleanupState {
                            abort_call_id: CallId::new(),
                            block_write_handle,
                        }
                    })
                    .collect::<Vec<_>>();
                self.abort = Some(AbortCleanupState {
                    metadata_call_id: CallId::new(),
                    metadata_write_handle: self.write_handle,
                    worker_cleanups,
                });
                self.state = WriteSessionState::AbortUnknown;
            }
            WriteSessionState::AbortUnknown => {
                let abort = self.abort.as_ref().ok_or_else(|| {
                    ClientError::InvalidArgument("AbortUnknown state missing frozen cleanup plan".to_string())
                })?;
                if abort.metadata_write_handle != self.write_handle {
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
        let metadata_operation = OperationContext::with_call_id_named(
            client_id,
            client_name,
            abort.metadata_call_id,
            "AbortFileWrite",
            Some(self.path.clone()),
            deadline.clone(),
        )?;
        let mut worker_cleanups = Vec::with_capacity(abort.worker_cleanups.len());
        for cleanup in &abort.worker_cleanups {
            let operation = OperationContext::with_call_id_named(
                client_id,
                client_name,
                cleanup.abort_call_id,
                "AbortWrite",
                Some(self.path.clone()),
                deadline.clone(),
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

    /// Return whether the open session should renew before another side-effecting operation.
    pub(crate) fn should_renew_lease(&mut self, renew_before_expiry_ms: u64) -> ClientResult<bool> {
        self.should_renew_lease_at_ms(unix_now_ms(), renew_before_expiry_ms)
    }

    /// Return whether CommitFile outcome is unresolved and retryable.
    pub(crate) fn is_commit_unknown(&self) -> bool {
        matches!(self.state, WriteSessionState::CommitUnknown)
    }

    /// Reject writes unless the session is open and the lease is locally valid.
    pub(crate) fn ensure_open_for_write(&mut self) -> ClientResult<()> {
        self.ensure_operation_allowed(WriteSessionOperation::Write)
    }

    /// Reject close unless the session can start or continue a safe close.
    pub(crate) fn ensure_open_for_close(&mut self) -> ClientResult<()> {
        self.ensure_operation_allowed(WriteSessionOperation::Close)
    }

    /// Reject abort unless cleanup is safe to attempt.
    pub(crate) fn ensure_open_for_abort(&mut self) -> ClientResult<()> {
        self.ensure_operation_allowed(WriteSessionOperation::Abort)
    }

    /// Reject lease renew unless the handle still represents an open session.
    pub(crate) fn ensure_open_for_renew(&mut self) -> ClientResult<()> {
        self.ensure_operation_allowed(WriteSessionOperation::Renew)
    }

    /// Reject side-effect-free barriers after validating session state.
    pub(crate) fn ensure_open_for_barrier(&mut self) -> ClientResult<()> {
        self.ensure_operation_allowed(WriteSessionOperation::Barrier)
    }

    fn ensure_operation_allowed(&mut self, operation: WriteSessionOperation) -> ClientResult<()> {
        self.ensure_operation_allowed_at_ms(operation, unix_now_ms())
    }

    fn ensure_operation_allowed_at_ms(&mut self, operation: WriteSessionOperation, now_ms: u64) -> ClientResult<()> {
        let safety_window_ms = match (self.state, operation) {
            (WriteSessionState::Open, WriteSessionOperation::Renew) => 0,
            (
                WriteSessionState::Open,
                WriteSessionOperation::Write
                | WriteSessionOperation::Close
                | WriteSessionOperation::Abort
                | WriteSessionOperation::Barrier,
            ) => LEASE_EXPIRY_SAFETY_WINDOW_MS,
            (WriteSessionState::CommitStarted | WriteSessionState::CommitUnknown, WriteSessionOperation::Close)
            | (WriteSessionState::AbortUnknown, WriteSessionOperation::Abort) => return Ok(()),
            _ => return Err(self.state_error_value()),
        };
        self.ensure_lease_valid_at_ms(now_ms, safety_window_ms)
    }

    fn should_renew_lease_at_ms(&mut self, now_ms: u64, renew_before_expiry_ms: u64) -> ClientResult<bool> {
        if !matches!(self.state, WriteSessionState::Open) {
            return Ok(false);
        }
        let Some(expires_at_ms) = self.expires_at_ms else {
            return Ok(false);
        };
        if expires_at_ms <= now_ms {
            self.mark_session_expired();
            return Err(ClientError::StaleHandle {
                reason: "write session lease expired".to_string(),
            });
        }
        Ok(expires_at_ms.saturating_sub(now_ms) <= renew_before_expiry_ms)
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WriteSessionOperation {
    Write,
    Close,
    Abort,
    Renew,
    Barrier,
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
    commit_write_handle: WriteHandleProto,
    commit_final_size: u64,
    commit_committed_blocks_snapshot: Vec<CommittedBlock>,
}

#[derive(Clone)]
struct AbortCleanupState {
    metadata_call_id: CallId,
    metadata_write_handle: WriteHandleProto,
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
    block_write_handle: WorkerBlockWriteHandle,
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

#[cfg(test)]
mod tests {
    use super::*;

    use beryl_proto::common::{BlockIdProto, FencingTokenProto, LeaseIdProto, StreamIdProto};
    use beryl_types::lease::FencingToken;
    use beryl_types::{
        BlockId, BlockIndex, ClientId, CommittedBlock, DataHandleId, GroupName, WorkerEndpointInfo, WorkerId,
        WorkerNetProtocol, WriteTarget,
    };

    use crate::data::WorkerBlockWriteHandle;
    use crate::runtime::AttemptContext;

    #[test]
    fn prepare_commit_file_reuses_call_id_and_frozen_typed_payload() {
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
            .prepare_commit_file(
                ClientId::new(7),
                "test-client",
                blocks.clone(),
                5,
                OperationDeadline::new(1_000),
            )
            .expect("first commit plan");
        session.mark_commit_unknown();
        let second = session
            .prepare_commit_file(
                ClientId::new(7),
                "test-client",
                blocks,
                5,
                OperationDeadline::new(1_000),
            )
            .expect("retry commit plan");

        let first_ctx = AttemptContext::for_metadata(&first.operation, test_group_name(), 0).expect("first context");
        let second_ctx = AttemptContext::for_metadata(&second.operation, test_group_name(), 0).expect("second context");
        assert_eq!(first_ctx.call_id(), second_ctx.call_id());
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
            .prepare_commit_file(
                ClientId::new(7),
                "test-client",
                vec![committed_block(302, 0, 0, 5)],
                5,
                OperationDeadline::new(1_000),
            )
            .expect("first commit plan");
        let err = session
            .prepare_commit_file(
                ClientId::new(7),
                "test-client",
                vec![committed_block(302, 0, 0, 6)],
                6,
                OperationDeadline::new(1_000),
            )
            .expect_err("changed commit payload must fail");

        assert!(matches!(err, ClientError::InvalidArgument(msg) if msg.contains("payload changed")));
    }

    #[test]
    fn prepare_commit_file_rejects_write_handle_change_after_unknown() {
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
            .prepare_commit_file(
                ClientId::new(7),
                "test-client",
                blocks.clone(),
                5,
                OperationDeadline::new(1_000),
            )
            .expect("first commit plan");
        let first_ctx = AttemptContext::for_metadata(&first.operation, test_group_name(), 0).expect("first context");
        session.mark_commit_unknown();

        session.write_handle.lease_epoch = 2;
        let err = session
            .prepare_commit_file(
                ClientId::new(7),
                "test-client",
                blocks.clone(),
                5,
                OperationDeadline::new(1_000),
            )
            .expect_err("changed session identity must fail");
        assert!(matches!(err, ClientError::InvalidArgument(msg) if msg.contains("write handle changed")));

        session.write_handle.lease_epoch = 1;
        let retry = session
            .prepare_commit_file(
                ClientId::new(7),
                "test-client",
                blocks,
                5,
                OperationDeadline::new(1_000),
            )
            .expect("retry commit plan");
        let retry_ctx = AttemptContext::for_metadata(&retry.operation, test_group_name(), 0).expect("retry context");

        assert_eq!(first_ctx.call_id(), retry_ctx.call_id());
        assert_eq!(retry.final_size, 5);
        assert_eq!(retry.committed_blocks, vec![committed_block(302, 0, 0, 5)]);
    }

    #[test]
    fn prepare_abort_cleanup_reuses_call_id_and_frozen_typed_payload() {
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
            .prepare_abort_cleanup(ClientId::new(7), "test-client", OperationDeadline::new(1_000))
            .expect("first abort plan");
        let first_metadata = AttemptContext::for_metadata(&first.metadata_operation(), test_group_name(), 0)
            .expect("first metadata context");
        let first_worker = first.worker_cleanups()[0].operation();
        let first_worker_ctx = AttemptContext::for_data(&first_worker, 0);
        let first_worker_snapshot = block_write_handle_signature(first.worker_cleanups()[0].block_write_handle());
        session.pending_blocks.clear();

        let second = session
            .prepare_abort_cleanup(ClientId::new(7), "test-client", OperationDeadline::new(1_000))
            .expect("retry abort plan");
        let second_metadata = AttemptContext::for_metadata(&second.metadata_operation(), test_group_name(), 0)
            .expect("second metadata context");
        let second_worker = second.worker_cleanups()[0].operation();
        let second_worker_ctx = AttemptContext::for_data(&second_worker, 0);

        assert_eq!(first_metadata.call_id(), second_metadata.call_id());
        assert_eq!(first.metadata_write_handle(), second.metadata_write_handle());
        assert_eq!(second.worker_cleanups().len(), 1);
        assert_eq!(first_worker_ctx.call_id(), second_worker_ctx.call_id());
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
            .prepare_abort_cleanup(ClientId::new(7), "test-client", OperationDeadline::new(1_000))
            .expect("first abort plan");
        let first_ctx = AttemptContext::for_metadata(&first.metadata_operation(), test_group_name(), 0)
            .expect("first metadata context");

        session.write_handle.lease_epoch = 2;
        let err = match session.prepare_abort_cleanup(ClientId::new(7), "test-client", OperationDeadline::new(1_000)) {
            Ok(_) => panic!("identity drift must reject abort replay"),
            Err(err) => err,
        };
        assert!(matches!(err, ClientError::InvalidArgument(msg) if msg.contains("identity changed")));

        session.write_handle.lease_epoch = 1;
        let retry = session
            .prepare_abort_cleanup(ClientId::new(7), "test-client", OperationDeadline::new(1_000))
            .expect("retry abort plan");
        let retry_ctx = AttemptContext::for_metadata(&retry.metadata_operation(), test_group_name(), 0)
            .expect("retry metadata context");
        assert_eq!(first_ctx.call_id(), retry_ctx.call_id());
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

        let err = match session.prepare_abort_cleanup(ClientId::new(7), "test-client", OperationDeadline::new(1_000)) {
            Ok(_) => panic!("committed worker block cannot be safely aborted"),
            Err(err) => err,
        };
        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("worker block commit")));
        assert!(matches!(
            session.ensure_operation_allowed_at_ms(WriteSessionOperation::Write, 0),
            Err(ClientError::StaleHandle { reason }) if reason.contains("abort outcome")
        ));
        assert!(matches!(
            session.ensure_operation_allowed_at_ms(WriteSessionOperation::Close, 0),
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
            .ensure_operation_allowed_at_ms(WriteSessionOperation::Write, 1_001)
            .expect_err("expired lease must block write");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("expired")));

        for operation in [
            WriteSessionOperation::Write,
            WriteSessionOperation::Close,
            WriteSessionOperation::Abort,
            WriteSessionOperation::Renew,
            WriteSessionOperation::Barrier,
        ] {
            let rejected = session.ensure_operation_allowed_at_ms(operation, 1_001);
            assert!(matches!(rejected, Err(ClientError::StaleHandle { reason }) if reason.contains("expired")));
        }
    }

    #[test]
    fn operation_gate_preserves_lease_and_retry_semantics() {
        let new_session = |expires_at_ms| {
            WriteSession::new(
                "/alpha".to_string(),
                DataHandleId::new(302),
                test_layout(),
                write_handle_proto(1, 302),
                0,
                expires_at_ms,
            )
            .expect("session")
        };

        let mut renew = new_session(1_000);
        renew
            .ensure_operation_allowed_at_ms(WriteSessionOperation::Renew, 1)
            .expect("renew may run inside the side-effect safety window");

        for operation in [
            WriteSessionOperation::Write,
            WriteSessionOperation::Close,
            WriteSessionOperation::Abort,
            WriteSessionOperation::Barrier,
        ] {
            let mut session = new_session(1_000);
            let error = session
                .ensure_operation_allowed_at_ms(operation, 1)
                .expect_err("new side effects must stop near lease expiry");
            assert!(matches!(error, ClientError::StaleHandle { reason } if reason.contains("near expiry")));
        }

        for (state, operation) in [
            (WriteSessionState::CommitStarted, WriteSessionOperation::Close),
            (WriteSessionState::CommitUnknown, WriteSessionOperation::Close),
            (WriteSessionState::AbortUnknown, WriteSessionOperation::Abort),
        ] {
            let mut session = new_session(1);
            session.state = state;
            session
                .ensure_operation_allowed_at_ms(operation, 2)
                .expect("frozen cleanup or commit retry must not be blocked by lease expiry");
            assert_eq!(session.state, state);
        }
    }

    #[test]
    fn state_transition_table_covers_all_write_session_states() {
        let operations = [
            WriteSessionOperation::Write,
            WriteSessionOperation::Close,
            WriteSessionOperation::Abort,
            WriteSessionOperation::Renew,
            WriteSessionOperation::Barrier,
        ];
        let cases = [
            (WriteSessionState::Open, [true, true, true, true, true]),
            (WriteSessionState::CommitStarted, [false, true, false, false, false]),
            (WriteSessionState::CommitUnknown, [false, true, false, false, false]),
            (WriteSessionState::Closed, [false, false, false, false, false]),
            (WriteSessionState::Aborted, [false, false, false, false, false]),
            (WriteSessionState::UnknownOutcome, [false, false, false, false, false]),
            (WriteSessionState::SessionInvalid, [false, false, false, false, false]),
            (WriteSessionState::SessionExpired, [false, false, false, false, false]),
            (WriteSessionState::AbortUnknown, [false, false, true, false, false]),
        ];

        for (state, expected) in cases {
            for (operation, allowed) in operations.into_iter().zip(expected) {
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

                let result = session.ensure_operation_allowed_at_ms(operation, 0);
                assert_eq!(
                    result.is_ok(),
                    allowed,
                    "unexpected transition for {state:?} and {operation:?}"
                );
            }
        }
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
            block_format_id: beryl_types::BlockFormatId::CURRENT_FOR_NEW_FILE,
            tier: beryl_types::Tier::Hdd,
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
            beryl_proto::common::WorkerNetProtocolProto::from(block.worker.worker_net_protocol) as i32,
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
