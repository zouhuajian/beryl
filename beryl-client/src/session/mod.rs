// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Client-side sequential write session state.

use crate::error::{ClientError, ClientResult};
use crate::runtime::context::unix_now_ms;
use crate::runtime::context::{Operation, OperationContext, OperationDeadline};
use beryl_types::{BlockId, CallId, ClientId, CommittedBlock, ContentGeneration, LocatedBlock, WriteHandle, WriteMode};

const LEASE_EXPIRY_SAFETY_WINDOW_MS: u64 = 1_000;

/// Sole mutable lifecycle state for one open sequential writer.
#[derive(Debug)]
pub(crate) struct WriteSession {
    path: String,
    block_size: u32,
    generation: ContentGeneration,
    mode: WriteMode,
    write_handle: WriteHandle,
    base_len: u64,
    position: u64,
    expires_at_ms: u64,
    ready_blocks: Vec<CommittedBlock>,
    tail: Option<ReadyBlock>,
    write_group: Option<beryl_types::GroupName>,
    state: WriteSessionState,
}

impl WriteSession {
    /// Create a session from Metadata state whose handle passed wire validation.
    /// Shape and expiry have already passed response-boundary validation.
    pub(crate) fn new(
        path: String,
        block_size: u32,
        write_handle: WriteHandle,
        base_len: u64,
        expires_at_ms: u64,
        generation: ContentGeneration,
        mode: WriteMode,
    ) -> Self {
        Self {
            path,
            block_size,
            generation,
            mode,
            write_handle,
            base_len,
            position: base_len,
            expires_at_ms,
            ready_blocks: Vec::new(),
            tail: None,
            write_group: None,
            state: WriteSessionState::Open,
        }
    }

    /// Path associated with the original open operation.
    pub(crate) fn path(&self) -> &str {
        &self.path
    }

    /// Current sequential write position.
    pub(crate) fn position(&self) -> u64 {
        self.position
    }

    /// Advances the SDK-visible position after the current Worker request stream
    /// accepts ownership of bytes.
    pub(crate) fn set_position(&mut self, position: u64) {
        self.position = position;
    }

    /// Metadata write handle.
    pub(crate) fn write_handle(&self) -> WriteHandle {
        self.write_handle
    }

    /// Predecessor that identifies the next logical AllocateBlock step.
    /// It advances only after Worker completion, so a definite rejection before
    /// Worker IO leaves subsequent allocation calls addressing the same block.
    pub(crate) fn previous_block_id(&self) -> Option<BlockId> {
        self.tail.as_ref().map(|block| block.target.block_id)
    }

    /// Validate a metadata write target before opening the worker stream.
    pub(crate) fn validate_target(&mut self, target: &LocatedBlock) -> ClientResult<()> {
        self.ensure_open_for_write()?;
        if target.file_offset.checked_add(target.write_offset) != Some(self.position) {
            return Err(ClientError::invalid_layout(format!(
                "write target file_offset mismatch: expected {}, got {}",
                self.position, target.file_offset
            )));
        }
        if target.block_size != u64::from(self.block_size) {
            return Err(ClientError::invalid_layout(format!(
                "write target capacity {} differs from session capacity {}",
                target.block_size, self.block_size
            )));
        }
        let block = target.block_id;
        if block.inode_id != self.write_handle.inode_id {
            return Err(ClientError::stale_handle(format!(
                "write target inode_id {} does not match session inode_id {}",
                block.inode_id.as_raw(),
                self.write_handle.inode_id.as_raw()
            )));
        }
        if target.fencing_token.epoch != self.write_handle.lease_epoch {
            return Err(ClientError::invalid_layout(
                "write target writer epoch differs from session epoch".to_string(),
            ));
        }
        Ok(())
    }

    /// Binds the returned tail to the opened file and its validated Metadata group.
    pub(crate) fn accept_open_tail(
        &mut self,
        group: beryl_types::GroupName,
        tail: Option<LocatedBlock>,
    ) -> ClientResult<()> {
        let needs_tail = !self.base_len.is_multiple_of(u64::from(self.block_size));
        if needs_tail != tail.is_some() {
            return Err(ClientError::invalid_layout("OpenWrite tail does not match file length"));
        }
        self.record_write_group(group);
        if let Some(target) = tail {
            self.validate_target(&target)?;
            self.tail = Some(ReadyBlock {
                written_len: target.write_offset,
                target,
            });
        }
        Ok(())
    }

    pub(crate) fn record_write_group(&mut self, group: beryl_types::GroupName) {
        self.write_group = Some(group);
    }

    /// Reuses the most recent partial checkpoint; it is not another allocation step.
    pub(crate) fn reusable_tail(&self) -> Option<(beryl_types::GroupName, LocatedBlock)> {
        let last = self.tail.as_ref()?;
        if last.written_len >= last.target.block_size {
            return None;
        }
        let mut target = last.target.clone();
        target.write_offset = last.written_len;
        Some((
            self.write_group.clone().expect("ready tail has a validated group"),
            target,
        ))
    }

    /// Records the client's accepted block prefix after Worker confirms Ready.
    pub(crate) fn push_ready_block(&mut self, target: LocatedBlock, written_len: u64) {
        if target.file_offset + written_len > self.base_len {
            let block = CommittedBlock {
                block_id: target.block_id,
                len: written_len,
            };
            match self.ready_blocks.last_mut() {
                Some(last) if last.block_id == block.block_id => *last = block,
                _ => self.ready_blocks.push(block),
            }
        }
        self.tail = Some(ReadyBlock { target, written_len });
    }

    fn publication_snapshot(&self) -> Publication {
        Publication {
            write_handle: self.write_handle,
            committed_blocks: self.ready_blocks.clone(),
            len: self.position,
            expected_generation: self.generation,
            expected_file_len: self.base_len,
            write_mode: self.mode,
        }
    }

    /// Freezes one SyncWrite identity and payload before Metadata can observe it.
    ///
    /// A later call may only replay this exact plan until a validated success
    /// returns the session to `Open`.
    /// The caller must first pass `ensure_open_for_sync`.
    pub(crate) fn prepare_sync_write(
        &mut self,
        client_id: ClientId,
        client_name: &str,
        deadline: OperationDeadline,
    ) -> SyncWritePlan {
        if matches!(self.state, WriteSessionState::Open) {
            self.state = WriteSessionState::SyncPending(PendingPublication {
                call_id: CallId::new(),
                publication: self.publication_snapshot(),
            });
        }
        let WriteSessionState::SyncPending(sync) = &self.state else {
            unreachable!("SyncWrite preparation follows its operation gate");
        };
        let operation =
            OperationContext::with_call_id_named(client_id, client_name, sync.call_id, Operation::SyncWrite, deadline);
        SyncWritePlan {
            operation,
            publication: sync.publication.clone(),
        }
    }

    /// Freeze and return the CommitFile operation for this write session.
    /// The caller must first pass `ensure_open_for_close`.
    pub(crate) fn prepare_commit_file(
        &mut self,
        client_id: ClientId,
        client_name: &str,
        deadline: OperationDeadline,
    ) -> CommitFilePlan {
        if matches!(self.state, WriteSessionState::Open) {
            self.state = WriteSessionState::CommitPending(PendingPublication {
                call_id: CallId::new(),
                publication: self.publication_snapshot(),
            });
        }
        let WriteSessionState::CommitPending(commit) = &self.state else {
            unreachable!("CommitFile preparation follows its operation gate");
        };
        let operation = OperationContext::with_call_id_named(
            client_id,
            client_name,
            commit.call_id,
            Operation::CommitFile,
            deadline,
        );
        CommitFilePlan {
            operation,
            publication: commit.publication.clone(),
        }
    }

    /// Freezes the AbortFileWrite identity; the session's write handle is immutable.
    /// The caller must first pass `ensure_open_for_abort`.
    pub(crate) fn prepare_abort(
        &mut self,
        client_id: ClientId,
        client_name: &str,
        deadline: OperationDeadline,
    ) -> OperationContext {
        if matches!(self.state, WriteSessionState::Open) {
            self.state = WriteSessionState::AbortPending(CallId::new());
        }
        let WriteSessionState::AbortPending(call_id) = &self.state else {
            unreachable!("AbortFileWrite preparation follows its operation gate");
        };
        OperationContext::with_call_id_named(client_id, client_name, *call_id, Operation::AbortFileWrite, deadline)
    }

    pub(crate) fn is_closed(&self) -> bool {
        matches!(self.state, WriteSessionState::Closed)
    }

    /// Mark the session closed after metadata commit succeeds.
    pub(crate) fn mark_closed(&mut self) {
        self.state = WriteSessionState::Closed;
    }

    /// Completes the frozen SyncWrite and restores normal writer operations.
    pub(crate) fn mark_sync_completed(&mut self, generation: ContentGeneration, file_len: u64) {
        self.generation = generation;
        self.base_len = file_len;
        self.mode = WriteMode::Append;
        // The tail remains available for reuse and as the allocation predecessor.
        self.ready_blocks.clear();
        self.state = WriteSessionState::Open;
    }

    /// Marks the session aborted after Metadata accepts `AbortFileWrite`.
    /// Metadata cleanup owns any durable Worker Ready blocks left unpublished.
    pub(crate) fn mark_aborted(&mut self) {
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

    /// Record the latest metadata lease expiration returned by RenewLease.
    pub(crate) fn update_expires_at_ms(&mut self, expires_at_ms: u64) {
        self.expires_at_ms = expires_at_ms;
    }

    /// Current Metadata lease expiry used to bound an open Worker block RPC.
    pub(crate) fn expires_at_ms(&self) -> u64 {
        self.expires_at_ms
    }

    /// Return whether the open session should renew before another side-effecting operation.
    pub(crate) fn should_renew_lease(&mut self, renew_before_expiry_ms: u64) -> ClientResult<bool> {
        self.should_renew_lease_at_ms(unix_now_ms(), renew_before_expiry_ms)
    }

    /// Return whether CommitFile outcome is unresolved and retryable.
    pub(crate) fn is_commit_pending(&self) -> bool {
        matches!(self.state, WriteSessionState::CommitPending(_))
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

    /// Reject sync unless it can start or safely replay a frozen plan.
    pub(crate) fn ensure_open_for_sync(&mut self) -> ClientResult<()> {
        self.ensure_operation_allowed(WriteSessionOperation::Sync)
    }

    fn ensure_operation_allowed(&mut self, operation: WriteSessionOperation) -> ClientResult<()> {
        self.ensure_operation_allowed_at_ms(operation, unix_now_ms())
    }

    fn ensure_operation_allowed_at_ms(&mut self, operation: WriteSessionOperation, now_ms: u64) -> ClientResult<()> {
        let safety_window_ms = match (&self.state, operation) {
            (WriteSessionState::Open, WriteSessionOperation::Renew) => 0,
            (
                WriteSessionState::Open,
                WriteSessionOperation::Write
                | WriteSessionOperation::Close
                | WriteSessionOperation::Abort
                | WriteSessionOperation::Sync,
            ) => LEASE_EXPIRY_SAFETY_WINDOW_MS,
            (WriteSessionState::SyncPending(_), WriteSessionOperation::Sync)
            | (WriteSessionState::CommitPending(_), WriteSessionOperation::Close)
            | (WriteSessionState::AbortPending(_), WriteSessionOperation::Abort) => return Ok(()),
            _ => return Err(self.state_error_value()),
        };
        self.ensure_lease_valid_at_ms(now_ms, safety_window_ms)
    }

    fn should_renew_lease_at_ms(&mut self, now_ms: u64, renew_before_expiry_ms: u64) -> ClientResult<bool> {
        if !matches!(self.state, WriteSessionState::Open) {
            return Ok(false);
        }
        let expires_at_ms = self.expires_at_ms;
        if expires_at_ms <= now_ms {
            self.mark_session_expired();
            return Err(ClientError::stale_handle("write session lease expired"));
        }
        Ok((expires_at_ms - now_ms) <= renew_before_expiry_ms)
    }

    fn ensure_lease_valid_at_ms(&mut self, now_ms: u64, safety_window_ms: u64) -> ClientResult<()> {
        let expires_at_ms = self.expires_at_ms;
        if expires_at_ms <= now_ms {
            self.mark_session_expired();
            return Err(ClientError::stale_handle("write session lease expired"));
        }
        if (expires_at_ms - now_ms) <= safety_window_ms {
            self.mark_session_expired();
            return Err(ClientError::stale_handle("write session lease is near expiry"));
        }
        Ok(())
    }

    fn state_error_value(&self) -> ClientError {
        match &self.state {
            WriteSessionState::Open => unreachable!("open session handled before reporting a state error"),
            WriteSessionState::SyncPending(_) => ClientError::stale_handle("write handle has an unresolved SyncWrite"),
            WriteSessionState::CommitPending(_) => {
                ClientError::stale_handle("write handle has an in-progress CommitFile")
            }
            WriteSessionState::Closed => ClientError::stale_handle("write handle is closed"),
            WriteSessionState::Aborted => ClientError::stale_handle("write handle is aborted"),
            WriteSessionState::UnknownOutcome => ClientError::stale_handle("write handle has an unknown outcome"),
            WriteSessionState::SessionInvalid => ClientError::stale_handle("write session is invalid"),
            WriteSessionState::SessionExpired => ClientError::stale_handle("write session lease expired"),
            WriteSessionState::AbortPending(_) => ClientError::stale_handle("write handle abort outcome is unknown"),
        }
    }
}

/// Last durable Worker checkpoint, retained for tail reuse and allocation ordering.
#[derive(Debug)]
struct ReadyBlock {
    target: LocatedBlock,
    written_len: u64,
}

#[derive(Debug)]
enum WriteSessionState {
    Open,
    SyncPending(PendingPublication),
    CommitPending(PendingPublication),
    Closed,
    Aborted,
    UnknownOutcome,
    SessionInvalid,
    SessionExpired,
    AbortPending(CallId),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WriteSessionOperation {
    Write,
    Close,
    Abort,
    Renew,
    Sync,
}

#[derive(Debug)]
struct PendingPublication {
    call_id: CallId,
    publication: Publication,
}

/// Immutable publication intent, preserved across unknown-outcome retries.
#[derive(Clone, Debug)]
pub(crate) struct Publication {
    pub(crate) write_handle: WriteHandle,
    pub(crate) committed_blocks: Vec<CommittedBlock>,
    pub(crate) len: u64,
    pub(crate) expected_generation: ContentGeneration,
    pub(crate) expected_file_len: u64,
    pub(crate) write_mode: WriteMode,
}

/// Frozen metadata SyncWrite operation and request payload.
#[derive(Debug)]
pub(crate) struct SyncWritePlan {
    pub(crate) operation: OperationContext,
    pub(crate) publication: Publication,
}

/// Frozen metadata CommitFile operation and request payload.
#[derive(Debug)]
pub(crate) struct CommitFilePlan {
    pub(crate) operation: OperationContext,
    pub(crate) publication: Publication,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ClientErrorKind;
    use beryl_types::{ClientId, InodeId, LeaseEpoch};

    fn assert_error(error: &ClientError, kind: ClientErrorKind, message: &str) {
        assert_eq!(error.kind(), kind);
        assert!(error.message().contains(message), "unexpected error: {error:?}");
    }

    #[test]
    fn frozen_publication_plans_reuse_identity_and_payload() {
        let mut session = ready_session();
        let prepare = |session: &mut WriteSession| {
            session.prepare_commit_file(ClientId::new(7), "test-client", OperationDeadline::new(1_000))
        };
        let first = prepare(&mut session);
        let retry = prepare(&mut session);
        assert_eq!(first.operation.call_id(), retry.operation.call_id());
        assert_eq!(first.publication.write_handle, retry.publication.write_handle);
        assert_eq!(
            first.publication.expected_generation,
            retry.publication.expected_generation
        );
        assert_eq!(first.publication.len, 5);
        assert_eq!(first.publication.len, retry.publication.len);
        assert_eq!(
            first.publication.committed_blocks,
            vec![CommittedBlock {
                block_id: BlockId::new(InodeId::new(302), beryl_types::BlockIndex::new(0)),
                len: 5
            }]
        );
        assert_eq!(first.publication.committed_blocks, retry.publication.committed_blocks);

        let mut session = ready_session();
        let prepare = |session: &mut WriteSession| {
            session.prepare_sync_write(ClientId::new(7), "test-client", OperationDeadline::new(1_000))
        };
        let first = prepare(&mut session);
        let retry = prepare(&mut session);
        assert_eq!(first.operation.call_id(), retry.operation.call_id());
        assert_eq!(first.publication.len, 5);
        assert_eq!(first.publication.len, retry.publication.len);
        assert_eq!(first.publication.committed_blocks, retry.publication.committed_blocks);

        let mut session = ready_session();
        let prepare = |session: &mut WriteSession| {
            session.prepare_abort(ClientId::new(7), "test-client", OperationDeadline::new(1_000))
        };
        let first = prepare(&mut session);
        let retry = prepare(&mut session);
        assert_eq!(first.call_id(), retry.call_id());
    }

    #[test]
    fn operation_gate_preserves_lease_and_retry_semantics() {
        let mut renew = new_session(1_000);
        renew
            .ensure_operation_allowed_at_ms(WriteSessionOperation::Renew, 1)
            .expect("renew may run inside the side-effect safety window");

        let mut expired = new_session(1_000);
        for operation in [
            WriteSessionOperation::Write,
            WriteSessionOperation::Close,
            WriteSessionOperation::Abort,
            WriteSessionOperation::Renew,
            WriteSessionOperation::Sync,
        ] {
            let error = expired
                .ensure_operation_allowed_at_ms(operation, 1_001)
                .expect_err("expired lease must block every operation");
            assert_error(&error, ClientErrorKind::StaleHandle, "expired");
        }

        for operation in [
            WriteSessionOperation::Write,
            WriteSessionOperation::Close,
            WriteSessionOperation::Abort,
            WriteSessionOperation::Sync,
        ] {
            let mut session = new_session(1_000);
            let error = session
                .ensure_operation_allowed_at_ms(operation, 1)
                .expect_err("new side effects must stop near lease expiry");
            assert_error(&error, ClientErrorKind::StaleHandle, "near expiry");
        }

        for operation in [
            WriteSessionOperation::Sync,
            WriteSessionOperation::Close,
            WriteSessionOperation::Abort,
        ] {
            let mut session = new_session(1);
            let deadline = OperationDeadline::new(1_000);
            match operation {
                WriteSessionOperation::Sync => {
                    session.prepare_sync_write(ClientId::new(7), "test-client", deadline);
                }
                WriteSessionOperation::Close => {
                    session.prepare_commit_file(ClientId::new(7), "test-client", deadline);
                }
                WriteSessionOperation::Abort => {
                    session.prepare_abort(ClientId::new(7), "test-client", deadline);
                }
                _ => unreachable!(),
            }
            session
                .ensure_operation_allowed_at_ms(operation, 2)
                .expect("frozen lifecycle retry must not be blocked by lease expiry");
            session
                .ensure_operation_allowed_at_ms(WriteSessionOperation::Write, 2)
                .expect_err("unresolved publication blocks new writes");
        }
    }

    fn ready_session() -> WriteSession {
        let mut session = new_session(1_000);
        let block_id = BlockId::new(InodeId::new(302), beryl_types::BlockIndex::new(0));
        session.record_write_group(beryl_types::GroupName::parse("root").unwrap());
        session.set_position(5);
        session.push_ready_block(
            LocatedBlock {
                block_id,
                block_size: 1024,
                file_offset: 0,
                write_offset: 0,
                workers: vec![beryl_types::WorkerEndpointInfo {
                    worker_id: beryl_types::WorkerId::new(1),
                    endpoint: "127.0.0.1:19101".into(),
                    worker_run_id: "550e8400-e29b-41d4-a716-446655440000".parse().unwrap(),
                }],
                fencing_token: beryl_types::FencingToken::new(ClientId::new(7), LeaseEpoch::new(1)),
                tier: beryl_types::Tier::Mem,
            },
            5,
        );
        session
    }

    fn new_session(expires_at_ms: u64) -> WriteSession {
        WriteSession::new(
            "/alpha".to_string(),
            1024,
            write_handle(302),
            0,
            expires_at_ms,
            ContentGeneration::new(0),
            WriteMode::Overwrite,
        )
    }

    fn write_handle(inode_id: u64) -> WriteHandle {
        WriteHandle {
            inode_id: InodeId::new(inode_id),
            lease_epoch: LeaseEpoch::new(1),
        }
    }
}
