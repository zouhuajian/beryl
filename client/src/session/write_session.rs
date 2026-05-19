// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Client-side sequential write session state.

use std::fmt::Write as _;

use proto::metadata::{CommitFileRequestProto, CommittedBlockProto, WriteHandleProto, WriteTargetProto};
use types::{CallId, ClientId, DataHandleId, InodeId};

use crate::data::WorkerWriteBlock;
use crate::error::{ClientError, ClientResult};
use crate::runtime::context::{OperationContext, OperationFingerprint, OperationIdentity};
use crate::runtime::policy::OperationKind;

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
    pub(crate) fn validate_write_offset(&self, offset: u64) -> ClientResult<()> {
        self.ensure_open()?;
        if offset != self.cursor {
            return Err(ClientError::InvalidArgument(format!(
                "sequential write cursor mismatch: expected {}, got {}",
                self.cursor, offset
            )));
        }
        Ok(())
    }

    /// Validate a metadata write target before opening the worker stream.
    pub(crate) fn validate_target(&self, target: &WriteTargetProto, expected_len: u64) -> ClientResult<()> {
        self.ensure_open()?;
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
        let block = target
            .block_id
            .as_ref()
            .ok_or_else(|| ClientError::InvalidLayout("write target missing block_id".to_string()))?;
        if block.data_handle_id != self.data_handle_id.as_raw() {
            return Err(ClientError::StaleHandle {
                reason: format!(
                    "write target data_handle_id {} does not match session data_handle_id {}",
                    block.data_handle_id,
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
        target: WriteTargetProto,
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
            worker_committed: false,
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
        committed_blocks: Vec<CommittedBlockProto>,
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
                committed_blocks: commit.commit_committed_blocks_snapshot.clone(),
                final_size: commit.commit_final_size,
            },
        ))
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

    /// Mark abort cleanup as uncertain while keeping retry metadata.
    pub(crate) fn mark_abort_unknown(&mut self) {
        self.state = WriteSessionState::AbortUnknown;
    }

    /// Record the latest metadata lease expiration returned by RenewLease.
    pub(crate) fn update_expires_at_ms(&mut self, expires_at_ms: u64) {
        self.expires_at_ms = Some(expires_at_ms);
    }

    /// Reject close attempts on handles that already reached a terminal state.
    pub(crate) fn ensure_close_allowed(&self) -> ClientResult<()> {
        match self.state {
            WriteSessionState::Open | WriteSessionState::CommitStarted | WriteSessionState::CommitUnknown => Ok(()),
            WriteSessionState::Closed => Err(ClientError::StaleHandle {
                reason: "write handle is closed".to_string(),
            }),
            WriteSessionState::Aborted => Err(ClientError::StaleHandle {
                reason: "write handle is aborted".to_string(),
            }),
            WriteSessionState::UnknownOutcome => Err(ClientError::StaleHandle {
                reason: "write handle has an unknown outcome".to_string(),
            }),
            WriteSessionState::SessionInvalid => Err(ClientError::StaleHandle {
                reason: "write session is invalid".to_string(),
            }),
            WriteSessionState::AbortUnknown => Err(ClientError::StaleHandle {
                reason: "write handle abort outcome is unknown".to_string(),
            }),
        }
    }

    /// Reject operations on closed or aborted handles.
    pub(crate) fn ensure_open(&self) -> ClientResult<()> {
        match self.state {
            WriteSessionState::Open => Ok(()),
            WriteSessionState::CommitStarted | WriteSessionState::CommitUnknown => Err(ClientError::StaleHandle {
                reason: "write handle has an in-progress CommitFile".to_string(),
            }),
            WriteSessionState::Closed => Err(ClientError::StaleHandle {
                reason: "write handle is closed".to_string(),
            }),
            WriteSessionState::Aborted => Err(ClientError::StaleHandle {
                reason: "write handle is aborted".to_string(),
            }),
            WriteSessionState::UnknownOutcome => Err(ClientError::StaleHandle {
                reason: "write handle has an unknown outcome".to_string(),
            }),
            WriteSessionState::SessionInvalid => Err(ClientError::StaleHandle {
                reason: "write session is invalid".to_string(),
            }),
            WriteSessionState::AbortUnknown => Err(ClientError::StaleHandle {
                reason: "write handle abort outcome is unknown".to_string(),
            }),
        }
    }

    /// Reject aborts that cannot safely run for the current session state.
    pub(crate) fn ensure_abort_allowed(&self) -> ClientResult<()> {
        match self.state {
            WriteSessionState::Open | WriteSessionState::AbortUnknown => Ok(()),
            WriteSessionState::CommitStarted | WriteSessionState::CommitUnknown => Err(ClientError::StaleHandle {
                reason: "write handle has an in-progress CommitFile".to_string(),
            }),
            WriteSessionState::Closed => Err(ClientError::StaleHandle {
                reason: "write handle is closed".to_string(),
            }),
            WriteSessionState::Aborted => Err(ClientError::StaleHandle {
                reason: "write handle is aborted".to_string(),
            }),
            WriteSessionState::UnknownOutcome => Err(ClientError::StaleHandle {
                reason: "write handle has an unknown outcome".to_string(),
            }),
            WriteSessionState::SessionInvalid => Err(ClientError::StaleHandle {
                reason: "write session is invalid".to_string(),
            }),
        }
    }
}

/// Pending worker block in a write session.
#[derive(Clone, Debug)]
pub(crate) struct PendingBlock {
    target: WriteTargetProto,
    worker_block: WorkerWriteBlock,
    written_len: u64,
    commit_seq: u64,
    worker_committed: bool,
}

impl PendingBlock {
    /// Metadata write target for this block.
    pub(crate) fn target(&self) -> &WriteTargetProto {
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

    /// Whether CommitWrite already succeeded for this block.
    pub(crate) fn worker_committed(&self) -> bool {
        self.worker_committed
    }

    /// Mark worker commit as successful.
    pub(crate) fn mark_worker_committed(&mut self) {
        self.worker_committed = true;
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
    AbortUnknown,
}

#[derive(Clone, Debug)]
struct CommitFileState {
    commit_call_id: CallId,
    commit_fingerprint: OperationFingerprint,
    commit_write_handle: WriteHandleProto,
    commit_final_size: u64,
    commit_committed_blocks_snapshot: Vec<CommittedBlockProto>,
    session_identity: String,
    detail: String,
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
    committed_blocks: &[CommittedBlockProto],
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
            block.block_id.as_ref().map(|id| id.data_handle_id).unwrap_or_default(),
            block.block_id.as_ref().map(|id| id.block_index).unwrap_or_default(),
            block.file_offset,
            block.len
        );
    }
    detail.push(']');
    detail
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

    use proto::common::{BlockIdProto, FencingTokenProto, LeaseIdProto};
    use proto::metadata::CommittedBlockProto;
    use types::ClientId;

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
        assert_eq!(retry_request.committed_blocks, vec![committed_block(302, 0, 0, 5)]);
    }

    fn commit_fingerprint(
        write_handle: &WriteHandleProto,
        final_size: u64,
        committed_blocks: &[CommittedBlockProto],
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

    fn committed_block(data_handle_id: u64, block_index: u32, file_offset: u64, len: u64) -> CommittedBlockProto {
        CommittedBlockProto {
            block_id: Some(BlockIdProto {
                data_handle_id,
                block_index,
            }),
            file_offset,
            len,
            checksum: None,
        }
    }
}
