// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Public filesystem-facing facade and shared client runtime helpers.

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use proto::metadata::{
    AddBlockRequestProto, AppendFileRequestProto, CreateDispositionProto, CreateFileRequestProto, DeleteRequestProto,
    GetStatusRequestProto, ListStatusRequestProto, OpenFileRequestProto, RenameRequestProto, RenewLeaseRequestProto,
    SyncWriteRequestProto, WriteSyncModeProto,
};

use super::handle::{ReadHandle, WriteHandle};
use super::{
    AppendOptions, CreateDisposition, CreateOptions, DirectoryListing, FileReader, FileStatus, FileWriter, ListOptions,
    OpenOptions,
};
use crate::canonical::{ClientAction, RefreshHint};
use crate::config::ClientConfig;
use crate::data::{WorkerBlockSyncResult, WorkerCommitResult, WorkerDataPlane};
use crate::error::{side_effect_response_body_mismatch, ClientError, ClientResult};
use crate::metadata::{MetadataGateway, TonicMetadataGateway};
use crate::metrics::{ClientMetric, ClientMetricEvent, ClientMetricLabels, ClientMetrics, NoopClientMetrics};
use crate::planner::read_planner::ReadPlanner;
use crate::runtime::{
    AttemptContext, BackoffPolicy, BackoffSleeper, ClientIdentity, ErrorClass, ErrorClassifier, OperationContext,
    OperationExecutor, OperationIdentity, OperationKind, OperationRuntime, RefreshManager, RefreshReason,
    RetryDecision, RetryDecisionInput, TokioBackoffSleeper,
};
use crate::session::write_session::{PendingBlock, WorkerCommitLevel, WriteSession};

pub(super) const DEFAULT_BLOCK_SIZE: u32 = 64 * 1024 * 1024;
pub(super) const DEFAULT_CHUNK_SIZE: u32 = 4 * 1024 * 1024;
pub(super) const DEFAULT_REPLICATION: u32 = 1;
pub(super) const MAX_PREALLOCATED_WRITE_BLOCKS: u64 = 10;

/// Public filesystem-facing client facade.
#[derive(Clone)]
pub struct FsClient {
    pub(super) config: ClientConfig,
    pub(super) executor: OperationExecutor,
    pub(super) data_plane: WorkerDataPlane,
    pub(super) backoff: BackoffPolicy,
    pub(super) sleeper: Arc<dyn BackoffSleeper>,
    pub(super) metrics: Arc<dyn ClientMetrics>,
}

impl FsClient {
    /// Create a new filesystem client facade.
    pub fn new(config: ClientConfig) -> Self {
        Self::try_new(config).expect("valid client metadata configuration")
    }

    /// Create a new filesystem client facade and return configuration errors.
    pub fn try_new(config: ClientConfig) -> ClientResult<Self> {
        let endpoint = config
            .metadata_endpoints
            .first()
            .cloned()
            .ok_or_else(|| ClientError::Config("client.metadata.endpoints must not be empty".to_string()))?;
        let metrics: Arc<dyn ClientMetrics> = Arc::new(NoopClientMetrics);
        let gateway = Arc::new(TonicMetadataGateway::new_lazy_with_config(
            endpoint,
            &config,
            Arc::clone(&metrics),
        )?);
        let data_plane = WorkerDataPlane::from_config(&config, Arc::clone(&metrics));
        Self::with_runtime_hooks(config, gateway, data_plane, Arc::new(TokioBackoffSleeper), metrics)
    }

    pub(crate) fn with_runtime_hooks(
        config: ClientConfig,
        gateway: Arc<dyn MetadataGateway>,
        data_plane: WorkerDataPlane,
        sleeper: Arc<dyn BackoffSleeper>,
        metrics: Arc<dyn ClientMetrics>,
    ) -> ClientResult<Self> {
        let identity = ClientIdentity::generate(config.client_name.clone())?;
        let refresh_manager = RefreshManager::from_config(&config.metadata_group_names, &config.metadata_endpoints)?;
        let executor = OperationExecutor::with_runtime(
            identity,
            gateway,
            refresh_manager,
            OperationRuntime {
                retry: config.retry.clone(),
                refresh: config.refresh.clone(),
                backoff: config.backoff.clone(),
                sleeper: Arc::clone(&sleeper),
                metrics: Arc::clone(&metrics),
            },
        )?;
        let backoff = BackoffPolicy::from_config(&config.backoff);
        Ok(Self {
            config,
            executor,
            data_plane,
            backoff,
            sleeper,
            metrics,
        })
    }

    /// Return the client configuration.
    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    /// Return file or directory status through the metadata runtime.
    pub async fn stat(&self, path: &str) -> ClientResult<FileStatus> {
        validate_path(path)?;
        let response = self
            .executor
            .get_status(
                path,
                GetStatusRequestProto {
                    header: None,
                    path: path.to_string(),
                },
            )
            .await?;
        FileStatus::from_proto(path, response)
    }

    /// Lists a directory using explicit pagination options.
    pub async fn list(&self, path: &str, options: ListOptions) -> ClientResult<DirectoryListing> {
        validate_path(path)?;
        let response = self
            .executor
            .list_status(
                path,
                ListStatusRequestProto {
                    header: None,
                    path: path.to_string(),
                    recursive: options.recursive,
                    cursor: options.cursor.unwrap_or_default(),
                    limit: options.limit.unwrap_or(0),
                },
            )
            .await?;
        Ok(DirectoryListing::from_proto(path, response))
    }

    /// Delete a file, symlink, or directory through the metadata runtime.
    pub async fn delete(&self, path: &str, recursive: bool) -> ClientResult<()> {
        validate_path(path)?;
        self.executor
            .delete(
                path,
                DeleteRequestProto {
                    header: None,
                    path: path.to_string(),
                    recursive,
                },
            )
            .await
            .map(|_| ())
    }

    /// Rename a namespace entry through the metadata runtime.
    pub async fn rename(&self, src: &str, dst: &str) -> ClientResult<()> {
        validate_path(src)?;
        validate_path(dst)?;
        self.executor
            .rename(
                src,
                dst,
                RenameRequestProto {
                    header: None,
                    src_path: src.to_string(),
                    dst_path: dst.to_string(),
                    flags: 0,
                },
            )
            .await
            .map(|_| ())
    }

    /// Opens an existing file for reads and returns a snapshot reader.
    ///
    /// Existing files use the metadata-stored `FileLayout`; open options do not
    /// override layout shape.
    pub async fn open(&self, path: &str, _options: OpenOptions) -> ClientResult<FileReader> {
        validate_path(path)?;
        let response = self
            .executor
            .open_file(
                path,
                OpenFileRequestProto {
                    header: None,
                    path: path.to_string(),
                    range: None,
                    include_locations: false,
                },
            )
            .await?;
        let handle = ReadHandle::from_open_response(path, response)?;
        Ok(FileReader::new(self.clone(), handle))
    }

    /// Creates a file write session according to the supplied creation options.
    ///
    /// `CreateOptions` layout fields are create-time intent for new file
    /// creation. Metadata validates and persists the accepted `FileLayout`.
    pub async fn create(&self, path: &str, options: CreateOptions) -> ClientResult<FileWriter> {
        validate_path(path)?;
        let disposition = match options.disposition {
            CreateDisposition::Create => CreateDispositionProto::CreateNew,
            CreateDisposition::Overwrite => CreateDispositionProto::Overwrite,
        };
        let response = match self
            .executor
            .create_file(
                path,
                CreateFileRequestProto {
                    header: None,
                    path: path.to_string(),
                    attrs: Some(default_file_attrs()),
                    layout: Some(layout_for_new_file(&options)),
                    disposition: disposition as i32,
                    desired_len: Some(default_write_preallocation_len()),
                },
            )
            .await
        {
            Ok(response) => response,
            Err(err) => {
                return Err(self.normalize_unknown_outcome("CreateFile", OperationKind::MetadataMutation, err));
            }
        };
        Ok(FileWriter::new(
            self.clone(),
            WriteHandle::from_create_response(path, response)?,
        ))
    }

    /// Opens an append write session for an existing file.
    ///
    /// Append uses the metadata-stored `FileLayout` and does not send a new
    /// layout override.
    pub async fn append(&self, path: &str, _options: AppendOptions) -> ClientResult<FileWriter> {
        validate_path(path)?;
        let response = match self
            .executor
            .append_file(
                path,
                AppendFileRequestProto {
                    header: None,
                    path: path.to_string(),
                    desired_len: Some(default_write_preallocation_len()),
                },
            )
            .await
        {
            Ok(response) => response,
            Err(err) => {
                return Err(self.normalize_unknown_outcome("AppendFile", OperationKind::MetadataMutation, err));
            }
        };
        Ok(FileWriter::new(
            self.clone(),
            WriteHandle::from_append_response(path, response)?,
        ))
    }

    pub(crate) async fn read_handle(&self, handle: &ReadHandle, offset: u64, len: u32) -> ClientResult<Bytes> {
        let Some(span) = ReadPlanner::plan_requested_range(offset, len, handle.size_hint())? else {
            return Ok(Bytes::new());
        };
        let file_version = handle.file_version();
        let inode_id = handle.inode_id();
        let data_handle_id = handle.data_handle_id();
        let operation = OperationContext::new_named(
            self.executor.client_id(),
            self.executor.client_name(),
            OperationKind::WorkerReadData,
            "Read",
            OperationIdentity::path(handle.path().to_string()),
        )?;
        let mut retry_used = 0usize;
        let mut refresh_used = 0usize;
        let retry_budget = self.config.retry.max_retry_attempts();
        let refresh_budget = self.config.refresh.max_refresh_attempts;
        let mut attempt = 0u32;
        loop {
            let layout = self
                .executor
                .read_layout_for_data_handle(handle.path(), data_handle_id, span.file_offset, span.len)
                .await?;
            let (group_name, segments) =
                ReadPlanner::resolve_response(inode_id, data_handle_id, Some(file_version), span, &layout)?;
            let ctx = self.data_attempt_context(&operation, attempt);
            match self
                .worker_rpc_with_timeout(
                    "Read",
                    OperationKind::WorkerReadData,
                    self.data_plane.read_all(ctx, group_name, &segments),
                )
                .await
            {
                Ok(bytes) => return Ok(bytes),
                Err(err) => {
                    let class = ErrorClassifier.classify_error(&err);
                    self.record_error_metric("Read", OperationKind::WorkerReadData, &class);
                    let refresh_reason = match class {
                        ErrorClass::NeedRefresh(reason) => Some(reason),
                        _ => None,
                    };
                    let decision = RetryDecision::from_input(RetryDecisionInput {
                        operation_kind: OperationKind::WorkerReadData,
                        operation_name: "Read",
                        attempt_number: attempt,
                        retry_budget_remaining: retry_budget.saturating_sub(retry_used),
                        refresh_budget_remaining: refresh_budget.saturating_sub(refresh_used),
                        error_class: class.clone(),
                        refresh_reason,
                        replay_safety: operation.replay_safety(),
                        side_effects_may_have_occurred: false,
                        has_stable_call_id_and_fingerprint: true,
                        has_stable_session_identity: true,
                        public_bytes_returned: false,
                        outcome_unknown: matches!(err, ClientError::UnknownOutcome(_)),
                    });
                    self.record_retry_decision("Read", OperationKind::WorkerReadData, &class, refresh_reason, decision);
                    match decision {
                        RetryDecision::RefreshThenRetry if should_replan_after_worker_error(&err) => {
                            let reason = refresh_reason.expect("refresh decision requires reason");
                            self.executor
                                .record_data_refresh(&operation, reason, &refresh_hint_from_error(&err))?;
                            self.record_refresh_metric("Read", OperationKind::WorkerReadData, reason, "refresh");
                            retry_used += 1;
                            refresh_used += 1;
                            attempt = attempt.saturating_add(1);
                        }
                        RetryDecision::Retry => {
                            let retry_index = retry_used;
                            retry_used += 1;
                            self.record_metric(
                                ClientMetric::RetryAttempt,
                                metric_labels("Read", OperationKind::WorkerReadData).with_error_class(class.label()),
                            );
                            self.sleep_before_retry(retry_index, "Read", OperationKind::WorkerReadData)
                                .await;
                            attempt = attempt.saturating_add(1);
                        }
                        RetryDecision::ReturnError => {
                            if matches!(class, ErrorClass::RetryableTransport)
                                && retry_budget.saturating_sub(retry_used) == 0
                            {
                                self.record_metric(
                                    ClientMetric::RetryExhausted,
                                    metric_labels("Read", OperationKind::WorkerReadData)
                                        .with_error_class(class.label()),
                                );
                            }
                            if let Some(reason) = refresh_reason {
                                if reason != RefreshReason::Unknown && refresh_budget.saturating_sub(refresh_used) == 0
                                {
                                    self.record_metric(
                                        ClientMetric::RefreshExhausted,
                                        metric_labels("Read", OperationKind::WorkerReadData)
                                            .with_refresh_reason(reason.label()),
                                    );
                                    return Err(ClientError::Worker(format!(
                                        "read refresh budget exhausted for {}",
                                        reason.label()
                                    )));
                                }
                            }
                            return Err(err);
                        }
                        RetryDecision::UnknownOutcome => {
                            self.record_metric(
                                ClientMetric::UnknownOutcome,
                                metric_labels("Read", OperationKind::WorkerReadData)
                                    .with_error_class(class.label())
                                    .with_outcome("unknown"),
                            );
                            return Err(err);
                        }
                        RetryDecision::DenyUnsafeReplay => {
                            self.record_metric(
                                ClientMetric::UnsafeReplayDenied,
                                metric_labels("Read", OperationKind::WorkerReadData).with_outcome("denied"),
                            );
                            return Err(ClientError::Unsupported(
                                "Read replay denied by retry policy".to_string(),
                            ));
                        }
                        RetryDecision::RefreshThenRetry => return Err(err),
                    }
                }
            }
        }
    }
    pub(crate) async fn write_handle_all(&self, handle: &WriteHandle, data: Bytes) -> ClientResult<u64> {
        let session_ref = handle.write_session();
        let mut session = session_ref.lock().await;
        session.ensure_open_for_write()?;
        if data.is_empty() {
            return Ok(session.cursor());
        }
        let mut submitted = 0usize;
        let block_size = session.block_size() as usize;
        while submitted < data.len() {
            let remaining = data.len() - submitted;
            let block_len = remaining.min(block_size);
            let chunk = data.slice(submitted..submitted + block_len);
            self.write_one_block(&mut session, chunk).await?;
            handle.store_write_cursor(session.cursor());
            submitted += block_len;
        }
        Ok(session.cursor())
    }

    pub(crate) async fn close_handle(&self, handle: &WriteHandle) -> ClientResult<()> {
        let session_ref = handle.write_session();
        let mut session = session_ref.lock().await;
        session.ensure_close_allowed()?;
        let path = session.path().to_string();
        let final_size = session.cursor();
        let committed_blocks = self
            .ensure_pending_worker_blocks_at_level(&mut session, WorkerCommitLevel::CLOSE_REQUIRED)
            .await?;

        let retrying_unknown_commit = session.is_commit_unknown();
        let (operation, request) = session.prepare_commit_file(
            self.executor.client_id(),
            self.executor.client_name(),
            committed_blocks,
            final_size,
        )?;
        if retrying_unknown_commit {
            self.record_metric(
                ClientMetric::CommitUnknownRetry,
                metric_labels("CommitFile", OperationKind::MetadataSessionBarrier).with_outcome("retry"),
            );
        }
        match self.executor.commit_file(operation, request).await {
            Ok(response) => {
                session.mark_closed(response.file_version);
                Ok(())
            }
            Err(err) if is_unknown_session_barrier_outcome(&err) => {
                session.mark_commit_unknown();
                self.record_metric(
                    ClientMetric::UnknownOutcome,
                    metric_labels("CommitFile", OperationKind::MetadataSessionBarrier).with_outcome("unknown"),
                );
                Err(ClientError::UnknownOutcome(format!(
                    "CommitFile outcome is unknown for path {}: {}",
                    path, err
                )))
            }
            Err(err) => {
                mark_session_after_session_error(&mut session, &err);
                let class = ErrorClassifier.classify_error(&err);
                self.record_error_metric("CommitFile", OperationKind::MetadataSessionBarrier, &class);
                Err(err)
            }
        }
    }

    pub(crate) async fn sync_write_visibility_handle(&self, handle: &WriteHandle) -> ClientResult<()> {
        self.sync_write_barrier(handle, WriteSyncModeProto::WriteSyncModeVisibility)
            .await
    }

    pub(crate) async fn sync_write_durability_handle(&self, handle: &WriteHandle) -> ClientResult<()> {
        self.sync_write_barrier(handle, WriteSyncModeProto::WriteSyncModeDurability)
            .await
    }

    pub(crate) async fn renew_lease_handle(&self, handle: &WriteHandle) -> ClientResult<()> {
        let session_ref = handle.write_session();
        let mut session = session_ref.lock().await;
        session.ensure_open_for_renew()?;
        let path = session.path().to_string();
        let session_identity = session.session_identity();
        let write_handle = session.write_handle();
        self.record_metric(
            ClientMetric::LeaseRenewAttempt,
            metric_labels("RenewLease", OperationKind::MetadataSessionBarrier).with_outcome("attempt"),
        );
        match self
            .executor
            .renew_lease(
                &path,
                session_identity,
                RenewLeaseRequestProto {
                    header: None,
                    write_handle: Some(write_handle),
                },
            )
            .await
        {
            Ok(response) => {
                session.update_expires_at_ms(response.expires_at_ms);
                self.record_metric(
                    ClientMetric::LeaseRenewSuccess,
                    metric_labels("RenewLease", OperationKind::MetadataSessionBarrier).with_outcome("success"),
                );
                Ok(())
            }
            Err(err) => {
                mark_session_after_session_error(&mut session, &err);
                let class = ErrorClassifier.classify_error(&err);
                self.record_error_metric("RenewLease", OperationKind::MetadataSessionBarrier, &class);
                self.record_metric(
                    ClientMetric::LeaseRenewFailure,
                    metric_labels("RenewLease", OperationKind::MetadataSessionBarrier)
                        .with_error_class(class.label())
                        .with_outcome("failure"),
                );
                Err(err)
            }
        }
    }

    pub(crate) async fn abort_handle(&self, handle: &WriteHandle) -> ClientResult<()> {
        let session_ref = handle.write_session();
        let mut session = session_ref.lock().await;
        session.ensure_open_for_abort()?;
        let plan = session.prepare_abort_cleanup(self.executor.client_id(), self.executor.client_name())?;
        let mut abort_error = None;
        self.record_metric(
            ClientMetric::AbortAttempt,
            metric_labels("AbortFileWrite", OperationKind::CleanupBestEffort).with_outcome("attempt"),
        );
        for cleanup in plan.worker_cleanups() {
            let operation = cleanup.operation();
            let ctx = self.data_attempt_context(&operation, 0);
            if let Err(err) = self
                .worker_rpc_with_timeout(
                    "AbortWrite",
                    OperationKind::CleanupBestEffort,
                    self.data_plane.abort_write(ctx, cleanup.worker_block()),
                )
                .await
            {
                abort_error.get_or_insert(err);
            }
        }
        if let Err(err) = self
            .executor
            .abort_file_write(plan.metadata_operation(), plan.metadata_request())
            .await
        {
            abort_error.get_or_insert(self.normalize_unknown_outcome(
                "AbortFileWrite",
                OperationKind::CleanupBestEffort,
                err,
            ));
        }
        match abort_error {
            Some(err) => {
                session.mark_abort_unknown();
                let normalized = self.normalize_unknown_outcome("AbortWrite", OperationKind::CleanupBestEffort, err);
                let metric = if matches!(normalized, ClientError::UnknownOutcome(_)) {
                    ClientMetric::AbortUnknown
                } else {
                    ClientMetric::AbortFailure
                };
                self.record_metric(
                    metric,
                    metric_labels("AbortWrite", OperationKind::CleanupBestEffort).with_outcome("unknown"),
                );
                Err(normalized)
            }
            None => {
                session.mark_aborted();
                self.record_metric(
                    ClientMetric::AbortSuccess,
                    metric_labels("AbortFileWrite", OperationKind::CleanupBestEffort).with_outcome("success"),
                );
                Ok(())
            }
        }
    }

    async fn write_one_block(&self, session: &mut WriteSession, data: Bytes) -> ClientResult<()> {
        let block_len = data.len() as u64;
        let add_block = match self
            .executor
            .add_block(
                session.path(),
                session.session_identity(),
                AddBlockRequestProto {
                    header: None,
                    write_handle: Some(session.write_handle()),
                    desired_len: Some(block_len),
                },
            )
            .await
        {
            Ok(add_block) => add_block,
            Err(err) => {
                mark_session_after_write_error(session, &err);
                return Err(self.normalize_unknown_outcome("AddBlock", OperationKind::MetadataMutation, err));
            }
        };
        if let Err(err) = session.validate_target(&add_block.target, block_len) {
            session.mark_unknown_outcome();
            self.record_metric(
                ClientMetric::WorkerResponseBodyMismatch,
                metric_labels("AddBlock", OperationKind::MetadataMutation).with_outcome("unknown"),
            );
            self.record_metric(
                ClientMetric::UnknownOutcome,
                metric_labels("AddBlock", OperationKind::MetadataMutation).with_outcome("unknown"),
            );
            return Err(side_effect_response_body_mismatch("AddBlock", err));
        }
        let operation = worker_write_operation(
            self.executor.client_id(),
            self.executor.client_name(),
            "OpenWriteStream",
            session.path(),
            &session.session_identity(),
        )?;
        let ctx = self.data_attempt_context(&operation, 0);
        let worker_block = match self
            .worker_rpc_with_timeout(
                "OpenWriteStream",
                OperationKind::WorkerWriteData,
                self.data_plane
                    .open_write(ctx, add_block.group_name.clone(), add_block.target.clone()),
            )
            .await
        {
            Ok(worker_block) => worker_block,
            Err(err) => {
                mark_session_after_write_error(session, &err);
                return Err(self.normalize_unknown_outcome("OpenWriteStream", OperationKind::WorkerWriteData, err));
            }
        };
        let response = match self
            .worker_rpc_with_timeout(
                "WriteStream",
                OperationKind::WorkerWriteData,
                self.data_plane.write_all(&worker_block, data),
            )
            .await
        {
            Ok(response) => response,
            Err(err) => {
                mark_session_after_write_error(session, &err);
                return Err(self.normalize_unknown_outcome("WriteStream", OperationKind::WorkerWriteData, err));
            }
        };
        if response.written_through != block_len {
            session.mark_unknown_outcome();
            self.record_metric(
                ClientMetric::WorkerResponseBodyMismatch,
                metric_labels("WriteStream", OperationKind::WorkerWriteData).with_outcome("unknown"),
            );
            self.record_metric(
                ClientMetric::UnknownOutcome,
                metric_labels("WriteStream", OperationKind::WorkerWriteData).with_outcome("unknown"),
            );
            return Err(ClientError::UnknownOutcome(format!(
                "worker WriteStream written_through mismatch: expected {}, got {}",
                block_len, response.written_through
            )));
        }
        if let Err(err) = session.push_pending_block(add_block.target, worker_block, block_len, response.last_acked_seq)
        {
            session.mark_session_invalid();
            return Err(err);
        }
        Ok(())
    }

    /// Ensure pending worker blocks reach the requested level, then publish the SyncWrite barrier in metadata.
    async fn sync_write_barrier(&self, handle: &WriteHandle, mode: WriteSyncModeProto) -> ClientResult<()> {
        let session_ref = handle.write_session();
        let mut session = session_ref.lock().await;
        session.ensure_open_for_barrier()?;
        let path = session.path().to_string();
        let session_identity = session.session_identity();
        let target_size = session.cursor();
        let required_level = sync_write_required_commit_level(mode)?;
        let committed_blocks = self
            .ensure_pending_worker_blocks_at_level(&mut session, required_level)
            .await?;
        let request = SyncWriteRequestProto {
            header: None,
            write_handle: Some(session.write_handle()),
            data_handle_id: Some(proto::common::DataHandleIdProto {
                value: handle.data_handle_id().as_raw(),
            }),
            committed_blocks: committed_blocks.iter().map(Into::into).collect(),
            target_size,
            mode: mode as i32,
            flags: 0,
        };
        match self.executor.sync_write(&path, session_identity, request).await {
            Ok(_) => Ok(()),
            Err(err) => {
                let class = ErrorClassifier.classify_error(&err);
                if is_unknown_session_barrier_outcome(&err) {
                    session.mark_unknown_outcome();
                    self.record_metric(
                        ClientMetric::UnknownOutcome,
                        metric_labels("SyncWrite", OperationKind::MetadataSessionBarrier).with_outcome("unknown"),
                    );
                    return Err(ClientError::UnknownOutcome(format!(
                        "SyncWrite outcome is unknown for path {}: {}",
                        path, err
                    )));
                }
                mark_session_after_session_error(&mut session, &err);
                self.record_error_metric("SyncWrite", OperationKind::MetadataSessionBarrier, &class);
                Err(err)
            }
        }
    }

    /// Move worker blocks to the required level and return the metadata block list for the open session.
    async fn ensure_pending_worker_blocks_at_level(
        &self,
        session: &mut WriteSession,
        required_level: WorkerCommitLevel,
    ) -> ClientResult<Vec<types::CommittedBlock>> {
        let worker_path = session.path().to_string();
        let worker_session_identity = session.session_identity();
        let mut committed_blocks = Vec::with_capacity(session.pending_blocks_mut().len());
        for pending in session.pending_blocks_mut() {
            if pending.worker_commit_level().satisfies(required_level) {
                committed_blocks.push(committed_block_from_pending(pending)?);
                continue;
            }

            match (pending.worker_commit_level(), required_level) {
                (WorkerCommitLevel::Uncommitted, WorkerCommitLevel::Visible | WorkerCommitLevel::Durable) => {
                    let require_sync = required_level.requires_sync();
                    let operation = worker_write_operation(
                        self.executor.client_id(),
                        self.executor.client_name(),
                        "CommitWrite",
                        &worker_path,
                        &worker_session_identity,
                    )?;
                    let ctx = self.data_attempt_context(&operation, 0);
                    let commit_result = match self
                        .worker_rpc_with_timeout(
                            "CommitWrite",
                            OperationKind::WorkerWriteData,
                            self.data_plane.commit_write(
                                ctx,
                                pending.worker_block(),
                                pending.written_len(),
                                pending.commit_seq(),
                                require_sync,
                            ),
                        )
                        .await
                    {
                        Ok(result) => result,
                        Err(err) => {
                            mark_session_after_write_error(session, &err);
                            return Err(self.normalize_unknown_outcome(
                                "CommitWrite",
                                OperationKind::WorkerWriteData,
                                err,
                            ));
                        }
                    };
                    if let Err(err) = validate_worker_commit_result(pending, commit_result) {
                        session.mark_unknown_outcome();
                        self.record_metric(
                            ClientMetric::WorkerResponseBodyMismatch,
                            metric_labels("CommitWrite", OperationKind::WorkerWriteData).with_outcome("unknown"),
                        );
                        self.record_metric(
                            ClientMetric::UnknownOutcome,
                            metric_labels("CommitWrite", OperationKind::WorkerWriteData).with_outcome("unknown"),
                        );
                        return Err(err);
                    }
                    pending.mark_worker_committed(require_sync);
                }
                (WorkerCommitLevel::Visible, WorkerCommitLevel::Durable) => {
                    let operation = worker_write_operation(
                        self.executor.client_id(),
                        self.executor.client_name(),
                        "SyncCommittedBlock",
                        &worker_path,
                        &worker_session_identity,
                    )?;
                    let ctx = self.data_attempt_context(&operation, 0);
                    let sync_result = match self
                        .worker_rpc_with_timeout(
                            "SyncCommittedBlock",
                            OperationKind::WorkerWriteData,
                            self.data_plane
                                .sync_committed_block(ctx, pending.worker_block(), pending.written_len()),
                        )
                        .await
                    {
                        Ok(result) => result,
                        Err(err) => {
                            mark_session_after_block_sync_error(session, &err);
                            return Err(self.normalize_unknown_outcome(
                                "SyncCommittedBlock",
                                OperationKind::WorkerWriteData,
                                err,
                            ));
                        }
                    };
                    if let Err(err) = validate_worker_block_sync_result(pending, sync_result) {
                        session.mark_unknown_outcome();
                        self.record_metric(
                            ClientMetric::WorkerResponseBodyMismatch,
                            metric_labels("SyncCommittedBlock", OperationKind::WorkerWriteData).with_outcome("unknown"),
                        );
                        self.record_metric(
                            ClientMetric::UnknownOutcome,
                            metric_labels("SyncCommittedBlock", OperationKind::WorkerWriteData).with_outcome("unknown"),
                        );
                        return Err(err);
                    }
                    pending.mark_worker_committed(true);
                }
                (WorkerCommitLevel::Visible, WorkerCommitLevel::Visible)
                | (WorkerCommitLevel::Durable, WorkerCommitLevel::Visible | WorkerCommitLevel::Durable)
                | (WorkerCommitLevel::Uncommitted, WorkerCommitLevel::Uncommitted)
                | (WorkerCommitLevel::Visible | WorkerCommitLevel::Durable, WorkerCommitLevel::Uncommitted) => {}
            }
            committed_blocks.push(committed_block_from_pending(pending)?);
        }
        Ok(committed_blocks)
    }

    pub(super) fn data_attempt_context(&self, operation: &OperationContext, attempt: u32) -> AttemptContext {
        AttemptContext::for_data(operation, attempt).with_operation_timeout_ms(self.config.retry.operation_timeout_ms)
    }

    pub(super) async fn worker_rpc_with_timeout<T, Fut>(
        &self,
        operation: &'static str,
        kind: OperationKind,
        future: Fut,
    ) -> ClientResult<T>
    where
        Fut: Future<Output = ClientResult<T>>,
    {
        let Some(timeout) = self.operation_timeout_duration() else {
            return future.await;
        };
        match tokio::time::timeout(timeout, future).await {
            Ok(result) => result,
            Err(_) => {
                self.record_metric(
                    ClientMetric::RpcTimeout,
                    metric_labels(operation, kind)
                        .with_error_class(ErrorClass::RetryableTransport.label())
                        .with_outcome("timeout"),
                );
                Err(timeout_error(kind.target_plane(), operation, timeout))
            }
        }
    }

    fn operation_timeout_duration(&self) -> Option<Duration> {
        self.config.retry.operation_timeout_ms.map(Duration::from_millis)
    }

    pub(super) async fn sleep_before_retry(&self, retry_index: usize, operation: &'static str, kind: OperationKind) {
        self.record_metric(
            ClientMetric::BackoffDelay,
            metric_labels(operation, kind).with_outcome("scheduled"),
        );
        self.sleeper.sleep(self.backoff.delay_for_retry(retry_index)).await;
    }

    pub(super) fn record_retry_decision(
        &self,
        operation: &'static str,
        kind: OperationKind,
        class: &ErrorClass,
        reason: Option<RefreshReason>,
        decision: RetryDecision,
    ) {
        let mut labels = metric_labels(operation, kind)
            .with_error_class(class.label())
            .with_retry_decision(decision.label());
        if let Some(reason) = reason {
            labels = labels.with_refresh_reason(reason.label());
        }
        self.record_metric(ClientMetric::RetryDecision, labels);
    }

    pub(super) fn record_refresh_metric(
        &self,
        operation: &'static str,
        kind: OperationKind,
        reason: RefreshReason,
        outcome: &'static str,
    ) {
        let labels = metric_labels(operation, kind)
            .with_refresh_reason(reason.label())
            .with_outcome(outcome);
        self.record_metric(ClientMetric::RefreshDecision, labels.clone());
        self.record_metric(ClientMetric::RefreshReason, labels);
    }

    pub(super) fn record_error_metric(&self, operation: &'static str, kind: OperationKind, class: &ErrorClass) {
        let metric = match class {
            ErrorClass::InvalidHeader => Some(ClientMetric::InvalidHeader),
            ErrorClass::UnknownOutcome => Some(ClientMetric::UnknownOutcome),
            ErrorClass::Fencing => Some(ClientMetric::FencingMismatch),
            ErrorClass::SessionInvalid => Some(ClientMetric::SessionInvalid),
            ErrorClass::SessionExpired => Some(ClientMetric::SessionExpired),
            ErrorClass::Unsupported => Some(ClientMetric::UnsupportedOperation),
            _ => None,
        };
        if let Some(metric) = metric {
            self.record_metric(metric, metric_labels(operation, kind).with_error_class(class.label()));
        }
    }

    pub(super) fn normalize_unknown_outcome(
        &self,
        operation: &'static str,
        kind: OperationKind,
        err: ClientError,
    ) -> ClientError {
        let class = ErrorClassifier.classify_error(&err);
        self.record_error_metric(operation, kind, &class);
        let normalized = normalize_unknown_outcome(operation, err);
        if matches!(normalized, ClientError::UnknownOutcome(_)) {
            self.record_metric(
                ClientMetric::UnknownOutcome,
                metric_labels(operation, kind)
                    .with_error_class(ErrorClassifier.classify_error(&normalized).label())
                    .with_outcome("unknown"),
            );
        }
        normalized
    }

    pub(super) fn record_metric(&self, metric: ClientMetric, labels: ClientMetricLabels) {
        self.metrics.record(ClientMetricEvent::new(metric, labels));
    }
}

impl fmt::Debug for FsClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FsClient")
            .field("config", &self.config)
            .field("executor", &self.executor)
            .field("data_plane", &self.data_plane)
            .finish_non_exhaustive()
    }
}

pub(super) fn validate_path(path: &str) -> ClientResult<()> {
    if path.is_empty() {
        Err(ClientError::InvalidArgument("path must not be empty".to_string()))
    } else {
        Ok(())
    }
}

pub(super) fn metric_labels(operation: &'static str, kind: OperationKind) -> ClientMetricLabels {
    ClientMetricLabels::default().with_operation(kind.label(), operation, kind.target_plane())
}

fn timeout_error(target_plane: &str, operation: &str, timeout: Duration) -> ClientError {
    ClientError::from(tonic::Status::deadline_exceeded(format!(
        "{target_plane} {operation} timed out after {}ms",
        timeout.as_millis()
    )))
}

pub(super) fn refresh_hint_from_error(err: &ClientError) -> RefreshHint {
    match err {
        ClientError::Action(action) => match action.as_ref() {
            ClientAction::Refresh { hint, .. } => hint.as_ref().clone(),
            _ => RefreshHint::default(),
        },
        _ => RefreshHint::default(),
    }
}

fn normalize_unknown_outcome(operation: &str, err: ClientError) -> ClientError {
    match ErrorClassifier.classify_error(&err) {
        ErrorClass::RetryableTransport => {
            ClientError::UnknownOutcome(format!("{operation} outcome is unknown after transport failure: {err}"))
        }
        ErrorClass::InvalidHeader => ClientError::UnknownOutcome(format!(
            "{operation} outcome is unknown after malformed OK response: {err}"
        )),
        _ => err,
    }
}

fn default_write_preallocation_len() -> u64 {
    u64::from(DEFAULT_BLOCK_SIZE) * MAX_PREALLOCATED_WRITE_BLOCKS
}

fn default_file_attrs() -> proto::fs::FileAttrsProto {
    proto::fs::FileAttrsProto {
        mode: 0o644,
        uid: 0,
        gid: 0,
        size: 0,
        atime_ms: 0,
        mtime_ms: 0,
        ctime_ms: 0,
        nlink: 1,
    }
}

fn layout_for_new_file(options: &CreateOptions) -> proto::common::FileLayoutProto {
    proto::common::FileLayoutProto {
        block_size: options.block_size,
        chunk_size: options.chunk_size,
        replication: DEFAULT_REPLICATION,
        block_format_id: options.block_format_id.as_raw(),
    }
}

fn should_replan_after_worker_error(err: &ClientError) -> bool {
    matches!(
        ErrorClassifier.classify_error(err),
        ErrorClass::NeedRefresh(
            RefreshReason::RouteEpochMismatch | RefreshReason::WorkerRunMismatch | RefreshReason::BlockStampMismatch
        )
    )
}

fn sync_write_required_commit_level(mode: WriteSyncModeProto) -> ClientResult<WorkerCommitLevel> {
    match mode {
        WriteSyncModeProto::WriteSyncModeDurability => Ok(WorkerCommitLevel::Durable),
        WriteSyncModeProto::WriteSyncModeVisibility => Ok(WorkerCommitLevel::Visible),
        WriteSyncModeProto::WriteSyncModeUnspecified => Err(ClientError::InvalidArgument(
            "SyncWrite mode must be visibility or durability".to_string(),
        )),
    }
}

fn worker_write_operation(
    client_id: types::ClientId,
    client_name: &str,
    operation_name: &str,
    path: &str,
    session_identity: &str,
) -> ClientResult<OperationContext> {
    OperationContext::new_named(
        client_id,
        client_name,
        OperationKind::WorkerWriteData,
        operation_name,
        OperationIdentity::session(path, session_identity),
    )
}

fn committed_block_from_pending(pending: &PendingBlock) -> ClientResult<types::CommittedBlock> {
    let target = pending.target();
    Ok(types::CommittedBlock {
        block_id: target.block_id,
        file_offset: target.file_offset,
        len: pending.written_len(),
        checksum: None,
    })
}

fn validate_worker_commit_result(pending: &PendingBlock, result: WorkerCommitResult) -> ClientResult<()> {
    let expected_len = pending.written_len();
    if result.effective_len != expected_len {
        return Err(side_effect_response_body_mismatch(
            "CommitWrite",
            format!("effective_len expected {}, got {}", expected_len, result.effective_len),
        ));
    }
    if result.written_through != expected_len {
        return Err(side_effect_response_body_mismatch(
            "CommitWrite",
            format!(
                "written_through expected {}, got {}",
                expected_len, result.written_through
            ),
        ));
    }
    let expected_stamp = pending.target().block_stamp;
    if result.block_stamp != expected_stamp {
        return Err(side_effect_response_body_mismatch(
            "CommitWrite",
            format!("block_stamp expected {}, got {}", expected_stamp, result.block_stamp),
        ));
    }
    Ok(())
}

fn validate_worker_block_sync_result(pending: &PendingBlock, result: WorkerBlockSyncResult) -> ClientResult<()> {
    let expected_len = pending.written_len();
    if result.effective_len != expected_len {
        return Err(side_effect_response_body_mismatch(
            "SyncCommittedBlock",
            format!("effective_len expected {}, got {}", expected_len, result.effective_len),
        ));
    }
    let expected_stamp = pending.target().block_stamp;
    if result.block_stamp != expected_stamp {
        return Err(side_effect_response_body_mismatch(
            "SyncCommittedBlock",
            format!("block_stamp expected {}, got {}", expected_stamp, result.block_stamp),
        ));
    }
    Ok(())
}

fn is_unknown_session_barrier_outcome(err: &ClientError) -> bool {
    matches!(err, ClientError::UnknownOutcome(_))
        || matches!(ErrorClassifier.classify_error(err), ErrorClass::RetryableTransport)
}

fn mark_session_after_write_error(session: &mut WriteSession, err: &ClientError) {
    if is_conservative_unknown_outcome_error(err) {
        session.mark_unknown_outcome();
    } else if is_session_or_fencing_error(err) || is_write_refresh_error(err) {
        mark_session_after_session_error(session, err);
    }
}

fn mark_session_after_block_sync_error(session: &mut WriteSession, err: &ClientError) {
    if is_session_or_fencing_error(err) || is_write_refresh_error(err) {
        mark_session_after_session_error(session, err);
    } else {
        session.mark_unknown_outcome();
    }
}

fn mark_session_after_session_error(session: &mut WriteSession, err: &ClientError) {
    match ErrorClassifier.classify_error(err) {
        ErrorClass::SessionExpired => session.mark_session_expired(),
        ErrorClass::Fencing | ErrorClass::SessionInvalid | ErrorClass::NeedRefresh(_) => session.mark_session_invalid(),
        _ => {}
    }
}

fn is_conservative_unknown_outcome_error(err: &ClientError) -> bool {
    matches!(err, ClientError::UnknownOutcome(_))
        || matches!(
            ErrorClassifier.classify_error(err),
            ErrorClass::RetryableTransport | ErrorClass::InvalidHeader
        )
}

fn is_session_or_fencing_error(err: &ClientError) -> bool {
    matches!(
        ErrorClassifier.classify_error(err),
        ErrorClass::Fencing | ErrorClass::SessionInvalid | ErrorClass::SessionExpired
    )
}

fn is_write_refresh_error(err: &ClientError) -> bool {
    matches!(
        ErrorClassifier.classify_error(err),
        ErrorClass::NeedRefresh(
            RefreshReason::RouteEpochMismatch
                | RefreshReason::WorkerRunMismatch
                | RefreshReason::BlockStampMismatch
                | RefreshReason::Unknown
        )
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fs_client_creates_runtime_identity_from_client_name_config() {
        let default_client = FsClient::try_new(ClientConfig::default()).expect("default client");
        assert!(!default_client.executor.client_id().is_zero());
        assert_eq!(
            default_client.executor.client_name(),
            crate::config::DEFAULT_CLIENT_NAME
        );

        let mut flat = common::FlatConfig::new();
        flat.set("client.name", "prod_ns01");
        let config = ClientConfig::from_flat(flat).expect("config");
        let named_client = FsClient::try_new(config).expect("named client");

        assert!(!named_client.executor.client_id().is_zero());
        assert_eq!(named_client.executor.client_name(), "prod_ns01");
    }
}
