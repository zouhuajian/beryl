// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Public filesystem-facing facade.

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use proto::metadata::{
    AddBlockRequestProto, AppendFileRequestProto, CommittedBlockProto, CreateDispositionProto, CreateFileRequestProto,
    DeleteRequestProto, GetBlockLocationsResponseProto, GetStatusRequestProto, ListStatusRequestProto,
    OpenFileRequestProto, RenameRequestProto, RenewLeaseRequestProto,
};
use types::{DataHandleId, InodeId};

use crate::api::{CreateMode, DirectoryListing, FileHandle, FileStatus, OpenOptions};
use crate::cache::{LayoutCache, LayoutCacheKey};
use crate::canonical::{ClientAction, RefreshHint};
use crate::config::ClientConfig;
use crate::data::DataPlaneBoundary;
use crate::error::{side_effect_response_body_mismatch, ClientError, ClientResult};
use crate::metadata::{MetadataGateway, TonicMetadataGateway, WriteSessionSeed};
use crate::metrics::{ClientMetric, ClientMetricEvent, ClientMetricLabels, ClientMetrics, NoopClientMetrics};
use crate::planner::read_planner::ReadPlanner;
use crate::runtime::singleflight::{Singleflight, SingleflightMode};
use crate::runtime::{
    AttemptContext, BackoffPolicy, BackoffSleeper, ErrorClass, ErrorClassifier, OperationContext, OperationExecutor,
    OperationIdentity, OperationKind, OperationRuntime, RefreshManager, RefreshReason, RetryDecision,
    RetryDecisionInput, TokioBackoffSleeper,
};
use crate::session::write_session::WriteSession;

const DEFAULT_BLOCK_SIZE: u32 = 64 * 1024 * 1024;
const DEFAULT_CHUNK_SIZE: u32 = 4 * 1024 * 1024;
const DEFAULT_REPLICATION: u32 = 1;
const MAX_PREALLOCATED_WRITE_BLOCKS: u64 = 10;

/// Public filesystem-facing client facade.
#[derive(Clone)]
pub struct FsClient {
    config: ClientConfig,
    executor: OperationExecutor,
    data_boundary: DataPlaneBoundary,
    layout_cache: LayoutCache,
    layout_singleflight: Singleflight<LayoutCacheKey, GetBlockLocationsResponseProto>,
    backoff: BackoffPolicy,
    sleeper: Arc<dyn BackoffSleeper>,
    metrics: Arc<dyn ClientMetrics>,
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
        Self::with_metadata_gateway(config, gateway)
    }

    /// Create a filesystem client with an injected metadata gateway.
    pub(crate) fn with_metadata_gateway(config: ClientConfig, gateway: Arc<dyn MetadataGateway>) -> ClientResult<Self> {
        let metrics: Arc<dyn ClientMetrics> = Arc::new(NoopClientMetrics);
        let data_boundary = DataPlaneBoundary::from_config(&config, metrics);
        Self::with_data_boundary(config, gateway, data_boundary)
    }

    fn with_data_boundary(
        config: ClientConfig,
        gateway: Arc<dyn MetadataGateway>,
        data_boundary: DataPlaneBoundary,
    ) -> ClientResult<Self> {
        Self::with_runtime_hooks(
            config,
            gateway,
            data_boundary,
            Arc::new(TokioBackoffSleeper),
            Arc::new(NoopClientMetrics),
        )
    }

    fn with_runtime_hooks(
        config: ClientConfig,
        gateway: Arc<dyn MetadataGateway>,
        data_boundary: DataPlaneBoundary,
        sleeper: Arc<dyn BackoffSleeper>,
        metrics: Arc<dyn ClientMetrics>,
    ) -> ClientResult<Self> {
        let client_id = config.client_id()?;
        let layout_cache = LayoutCache::from_config(&config.cache, Arc::clone(&metrics));
        let refresh_manager = RefreshManager::from_config(&config.metadata_group_ids, &config.metadata_endpoints)?
            .with_caches(Some(layout_cache.clone()), data_boundary.worker_endpoint_cache());
        let executor = OperationExecutor::with_runtime(
            client_id,
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
            data_boundary,
            layout_cache,
            layout_singleflight: Singleflight::default(),
            backoff,
            sleeper,
            metrics,
        })
    }

    /// Return the client configuration.
    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    /// Open a file using explicit options.
    pub async fn open(&self, path: &str, options: OpenOptions) -> ClientResult<FileHandle> {
        validate_path(path)?;
        if options.read && !options.write && options.create == CreateMode::None && !options.append && !options.truncate
        {
            let snapshot = self
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
            return handle_from_open_snapshot(path, snapshot);
        }

        if options.write && !options.read && options.append && options.create == CreateMode::None {
            let seed = match self
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
                Ok(seed) => seed,
                Err(err) => {
                    return Err(self.normalize_unknown_outcome("AppendFile", OperationKind::MetadataMutation, err));
                }
            };
            return handle_from_write_seed(path, seed);
        }

        if options.write
            && !options.read
            && !options.append
            && matches!(options.create, CreateMode::CreateNew | CreateMode::Overwrite)
        {
            let disposition = match options.create {
                CreateMode::CreateNew => CreateDispositionProto::CreateNew,
                CreateMode::Overwrite => CreateDispositionProto::Overwrite,
                CreateMode::None | CreateMode::CreateOrOpen => unreachable!("checked above"),
            };
            let seed = match self
                .executor
                .create_file(
                    path,
                    CreateFileRequestProto {
                        header: None,
                        path: path.to_string(),
                        attrs: Some(default_file_attrs()),
                        layout: Some(default_file_layout()),
                        disposition: disposition as i32,
                        desired_len: Some(default_write_preallocation_len()),
                    },
                )
                .await
            {
                Ok(seed) => seed,
                Err(err) => {
                    return Err(self.normalize_unknown_outcome("CreateFile", OperationKind::MetadataMutation, err));
                }
            };
            return handle_from_write_seed(path, seed);
        }

        Err(ClientError::Unsupported(format!(
            "unsupported FsClient open options: {options:?}"
        )))
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

    /// List a directory through the metadata runtime.
    pub async fn list(&self, path: &str) -> ClientResult<DirectoryListing> {
        validate_path(path)?;
        let response = self
            .executor
            .list_status(
                path,
                ListStatusRequestProto {
                    header: None,
                    path: path.to_string(),
                    recursive: false,
                    cursor: Vec::new(),
                    limit: 0,
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

    /// Read a full range into one buffer using all-or-error semantics.
    pub async fn read(&self, handle: &FileHandle, offset: u64, len: u32) -> ClientResult<Bytes> {
        let Some(span) = ReadPlanner::plan_requested_range(offset, len, handle.size_hint())? else {
            return Ok(Bytes::new());
        };
        let Some(file_version) = handle.file_version() else {
            return Err(ClientError::StaleHandle {
                reason: "read handle missing file_version".to_string(),
            });
        };
        let inode_id = handle.inode_id();
        let data_handle_id = handle.data_handle_id();
        let layout_key = LayoutCacheKey::new(inode_id, data_handle_id, file_version, span);
        let operation = OperationContext::new(
            self.executor.client_id(),
            OperationKind::WorkerReadData,
            "Read",
            OperationIdentity::path(handle.path().to_string()),
        )?;
        let mut retry_used = 0usize;
        let mut refresh_used = 0usize;
        let retry_budget = self.config.retry.worker_retry_budget();
        let refresh_budget = self.config.refresh.max_refresh_attempts;
        let mut attempt = 0u32;
        loop {
            let layout = self
                .load_layout(handle.path(), data_handle_id, span, layout_key)
                .await?;
            let (group_id, segments) =
                ReadPlanner::resolve_response(inode_id, data_handle_id, Some(file_version), span, &layout)?;
            let ctx = self.data_attempt_context(&operation, attempt);
            match self
                .worker_rpc_with_timeout(
                    "Read",
                    OperationKind::WorkerReadData,
                    self.data_boundary.read_all(ctx, group_id, &segments),
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

    /// Write sequential bytes at the current write cursor.
    pub async fn write(&self, handle: &FileHandle, offset: u64, data: Bytes) -> ClientResult<()> {
        let session_ref = handle
            .write_session()
            .ok_or_else(|| ClientError::InvalidArgument("handle is not a write handle".to_string()))?;
        let mut session = session_ref.lock().await;
        session.validate_write_offset(offset)?;
        if data.is_empty() {
            return Ok(());
        }
        let mut submitted = 0usize;
        while submitted < data.len() {
            let remaining = data.len() - submitted;
            let block_len = remaining.min(DEFAULT_BLOCK_SIZE as usize);
            let chunk = data.slice(submitted..submitted + block_len);
            self.write_one_block(&mut session, chunk).await?;
            submitted += block_len;
        }
        Ok(())
    }

    /// Close a write handle and publish committed file metadata.
    pub async fn close(&self, handle: &FileHandle) -> ClientResult<()> {
        let session_ref = handle
            .write_session()
            .ok_or_else(|| ClientError::InvalidArgument("handle is not a write handle".to_string()))?;
        let mut session = session_ref.lock().await;
        session.ensure_close_allowed()?;
        let path = session.path().to_string();
        let session_identity = session.session_identity();
        let final_size = session.cursor();
        let worker_path = path.clone();
        let worker_session_identity = session_identity.clone();

        let mut committed_blocks = Vec::with_capacity(session.pending_blocks_mut().len());
        for pending in session.pending_blocks_mut() {
            if !pending.worker_committed() {
                let operation = worker_write_operation(
                    self.executor.client_id(),
                    "CommitWrite",
                    &worker_path,
                    &worker_session_identity,
                )?;
                let ctx = self.data_attempt_context(&operation, 0);
                let commit_result = match self
                    .worker_rpc_with_timeout(
                        "CommitWrite",
                        OperationKind::WorkerWriteData,
                        self.data_boundary.commit_write(
                            ctx,
                            pending.worker_block(),
                            pending.written_len(),
                            pending.commit_seq(),
                            false,
                        ),
                    )
                    .await
                {
                    Ok(result) => result,
                    Err(err) => {
                        mark_session_after_write_error(&mut session, &err);
                        return Err(self.normalize_unknown_outcome("CommitWrite", OperationKind::WorkerWriteData, err));
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
                pending.mark_worker_committed();
            }
            committed_blocks.push(committed_block_from_pending(pending)?);
        }

        let retrying_unknown_commit = session.is_commit_unknown();
        let (operation, request) =
            session.prepare_commit_file(self.executor.client_id(), committed_blocks, final_size)?;
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
            Err(err) if is_unknown_commit_file_outcome(&err) => {
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

    /// Return Unsupported after validating the write handle.
    pub async fn hflush(&self, handle: &FileHandle) -> ClientResult<()> {
        self.unsupported_write_barrier(handle, "hflush", "visibility barrier not available")
            .await
    }

    /// Return Unsupported after validating the write handle.
    pub async fn hsync(&self, handle: &FileHandle) -> ClientResult<()> {
        self.unsupported_write_barrier(handle, "hsync", "durability barrier not available")
            .await
    }

    /// Renew the lease for an open write handle.
    pub async fn renew_lease(&self, handle: &FileHandle) -> ClientResult<()> {
        let session_ref = handle
            .write_session()
            .ok_or_else(|| ClientError::InvalidArgument("handle is not a write handle".to_string()))?;
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

    /// Abort a write handle best effort.
    pub async fn abort(&self, handle: &FileHandle) -> ClientResult<()> {
        let session_ref = handle
            .write_session()
            .ok_or_else(|| ClientError::InvalidArgument("handle is not a write handle".to_string()))?;
        let mut session = session_ref.lock().await;
        session.ensure_open_for_abort()?;
        let plan = session.prepare_abort_cleanup(self.executor.client_id())?;
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
                    self.data_boundary.abort_write(ctx, cleanup.worker_block()),
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
            "OpenWriteStream",
            session.path(),
            &session.session_identity(),
        )?;
        let ctx = self.data_attempt_context(&operation, 0);
        let worker_block = match self
            .worker_rpc_with_timeout(
                "OpenWriteStream",
                OperationKind::WorkerWriteData,
                self.data_boundary
                    .open_write(ctx, add_block.group_id, add_block.target.clone()),
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
                self.data_boundary.write_all(&worker_block, data),
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

    async fn load_layout(
        &self,
        path: &str,
        data_handle_id: DataHandleId,
        span: crate::planner::read_planner::PlannedReadRange,
        layout_key: LayoutCacheKey,
    ) -> ClientResult<GetBlockLocationsResponseProto> {
        if let Some(layout) = self.layout_cache.get(&layout_key) {
            return Ok(layout);
        }
        if !self.config.cache.layout_singleflight_enabled {
            let layout = self
                .executor
                .read_layout_for_data_handle(path, data_handle_id, span.file_offset, span.len)
                .await?;
            self.layout_cache.insert_validated(layout_key, layout.clone())?;
            return Ok(layout);
        }

        let executor = self.executor.clone();
        let layout_cache = self.layout_cache.clone();
        let layout_cache_for_flight = layout_cache.clone();
        let path = path.to_string();
        let (mode, result) = self
            .layout_singleflight
            .run(layout_key, move || async move {
                tokio::task::yield_now().await;
                if let Some(layout) = layout_cache_for_flight.get(&layout_key) {
                    return Ok(layout);
                }
                let layout = executor
                    .read_layout_for_data_handle(&path, data_handle_id, span.file_offset, span.len)
                    .await?;
                layout_cache.insert_validated(layout_key, layout.clone())?;
                Ok(layout)
            })
            .await;
        if mode == SingleflightMode::Joined {
            self.record_metric(
                ClientMetric::LayoutSingleflightJoin,
                cache_metric_labels("layout", "metadata", "read", "join"),
            );
            self.record_metric(
                ClientMetric::LayoutDuplicateRequestAvoided,
                cache_metric_labels("layout", "metadata", "read", "avoided"),
            );
        }
        if result.is_err() {
            self.record_metric(
                ClientMetric::LayoutSingleflightFailure,
                cache_metric_labels("layout", "metadata", "read", "failure"),
            );
        }
        result
    }

    async fn unsupported_write_barrier(
        &self,
        handle: &FileHandle,
        operation: &'static str,
        reason: &'static str,
    ) -> ClientResult<()> {
        let session_ref = handle
            .write_session()
            .ok_or_else(|| ClientError::InvalidArgument("handle is not a write handle".to_string()))?;
        let mut session = session_ref.lock().await;
        session.ensure_open_for_barrier()?;
        self.record_metric(
            ClientMetric::UnsupportedOperation,
            metric_labels(operation, OperationKind::MetadataSessionBarrier)
                .with_error_class(ErrorClass::Unsupported.label())
                .with_outcome("error"),
        );
        Err(ClientError::Unsupported(format!(
            "unsupported operation: {operation} ({reason})"
        )))
    }

    fn data_attempt_context(&self, operation: &OperationContext, attempt: u32) -> AttemptContext {
        AttemptContext::for_data(operation, attempt).with_operation_timeout_ms(self.config.retry.operation_timeout_ms)
    }

    async fn worker_rpc_with_timeout<T, Fut>(
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

    async fn sleep_before_retry(&self, retry_index: usize, operation: &'static str, kind: OperationKind) {
        self.record_metric(
            ClientMetric::BackoffDelay,
            metric_labels(operation, kind).with_outcome("scheduled"),
        );
        self.sleeper.sleep(self.backoff.delay_for_retry(retry_index)).await;
    }

    fn record_retry_decision(
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

    fn record_refresh_metric(
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

    fn record_error_metric(&self, operation: &'static str, kind: OperationKind, class: &ErrorClass) {
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

    fn normalize_unknown_outcome(&self, operation: &'static str, kind: OperationKind, err: ClientError) -> ClientError {
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

    fn record_metric(&self, metric: ClientMetric, labels: ClientMetricLabels) {
        self.metrics.record(ClientMetricEvent::new(metric, labels));
    }
}

impl fmt::Debug for FsClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FsClient")
            .field("config", &self.config)
            .field("executor", &self.executor)
            .field("data_boundary", &self.data_boundary)
            .field("layout_cache", &self.layout_cache)
            .finish_non_exhaustive()
    }
}

fn validate_path(path: &str) -> ClientResult<()> {
    if path.is_empty() {
        Err(ClientError::InvalidArgument("path must not be empty".to_string()))
    } else {
        Ok(())
    }
}

fn handle_from_open_snapshot(path: &str, snapshot: proto::metadata::OpenFileResponseProto) -> ClientResult<FileHandle> {
    Ok(FileHandle::read(
        path.to_string(),
        inode_id_from_proto(snapshot.inode_id, "OpenFileResponseProto.inode_id")?,
        data_handle_id_from_proto(snapshot.data_handle_id, "OpenFileResponseProto.data_handle_id")?,
        file_version_from_proto(snapshot.file_version, "OpenFileResponseProto.file_version")?,
        snapshot.file_size,
    ))
}

fn handle_from_write_seed(path: &str, seed: WriteSessionSeed) -> ClientResult<FileHandle> {
    match seed {
        WriteSessionSeed::Create(response) => write_handle_from_create(path, response),
        WriteSessionSeed::Append(response) => write_handle_from_append(path, response),
    }
}

fn write_handle_from_create(
    path: &str,
    response: proto::metadata::CreateFileResponseProto,
) -> ClientResult<FileHandle> {
    let inode_id = inode_id_from_proto(response.inode_id, "CreateFileResponseProto.inode_id")?;
    let data_handle_id = data_handle_id_from_proto(response.data_handle_id, "CreateFileResponseProto.data_handle_id")?;
    let write_handle = response
        .write_handle
        .ok_or_else(|| ClientError::Metadata("CreateFileResponseProto.write_handle missing".to_string()))?;
    let session = WriteSession::new(
        path.to_string(),
        inode_id,
        data_handle_id,
        write_handle,
        response.base_size,
    )?;
    Ok(FileHandle::write(
        path.to_string(),
        inode_id,
        data_handle_id,
        response.base_size,
        session,
    ))
}

fn write_handle_from_append(
    path: &str,
    response: proto::metadata::AppendFileResponseProto,
) -> ClientResult<FileHandle> {
    let inode_id = inode_id_from_proto(response.inode_id, "AppendFileResponseProto.inode_id")?;
    let data_handle_id = data_handle_id_from_proto(response.data_handle_id, "AppendFileResponseProto.data_handle_id")?;
    let write_handle = response
        .write_handle
        .ok_or_else(|| ClientError::Metadata("AppendFileResponseProto.write_handle missing".to_string()))?;
    let session = WriteSession::new(
        path.to_string(),
        inode_id,
        data_handle_id,
        write_handle,
        response.base_size,
    )?;
    Ok(FileHandle::write(
        path.to_string(),
        inode_id,
        data_handle_id,
        response.base_size,
        session,
    ))
}

fn default_write_preallocation_len() -> u64 {
    u64::from(DEFAULT_BLOCK_SIZE) * MAX_PREALLOCATED_WRITE_BLOCKS
}

fn worker_write_operation(
    client_id: types::ClientId,
    operation_name: &str,
    path: &str,
    session_identity: &str,
) -> ClientResult<OperationContext> {
    OperationContext::new(
        client_id,
        OperationKind::WorkerWriteData,
        operation_name,
        OperationIdentity::session(path, session_identity),
    )
}

fn committed_block_from_pending(
    pending: &crate::session::write_session::PendingBlock,
) -> ClientResult<CommittedBlockProto> {
    let target = pending.target();
    Ok(CommittedBlockProto {
        block_id: target.block_id,
        file_offset: target.file_offset,
        len: pending.written_len(),
        checksum: None,
    })
}

fn validate_worker_commit_result(
    pending: &crate::session::write_session::PendingBlock,
    result: crate::data::WorkerCommitResult,
) -> ClientResult<()> {
    let expected_len = pending.written_len();
    if result.effective_block_len != expected_len {
        return Err(side_effect_response_body_mismatch(
            "CommitWrite",
            format!(
                "effective_block_len expected {}, got {}",
                expected_len, result.effective_block_len
            ),
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

fn inode_id_from_proto(value: Option<proto::fs::InodeIdProto>, field: &str) -> ClientResult<InodeId> {
    value
        .map(|id| InodeId::new(id.value))
        .ok_or_else(|| ClientError::Metadata(format!("{field} missing")))
}

fn data_handle_id_from_proto(
    value: Option<proto::common::DataHandleIdProto>,
    field: &str,
) -> ClientResult<DataHandleId> {
    value
        .map(|id| DataHandleId::new(id.value))
        .ok_or_else(|| ClientError::Metadata(format!("{field} missing")))
}

fn file_version_from_proto(value: Option<u64>, field: &str) -> ClientResult<u64> {
    value.ok_or_else(|| ClientError::Metadata(format!("{field} missing")))
}

fn should_replan_after_worker_error(err: &ClientError) -> bool {
    matches!(
        ErrorClassifier.classify_error(err),
        ErrorClass::NeedRefresh(
            RefreshReason::RouteEpochMismatch | RefreshReason::WorkerEpochMismatch | RefreshReason::BlockStampMismatch
        )
    )
}

fn metric_labels(operation: &'static str, kind: OperationKind) -> ClientMetricLabels {
    ClientMetricLabels::default().with_operation(kind.label(), operation, kind.target_plane())
}

fn cache_metric_labels(
    cache: &'static str,
    plane: &'static str,
    operation: &'static str,
    outcome: &'static str,
) -> ClientMetricLabels {
    ClientMetricLabels::default()
        .with_cache(cache)
        .with_target_plane(plane)
        .with_operation_name(operation)
        .with_outcome(outcome)
}

fn timeout_error(target_plane: &str, operation: &str, timeout: Duration) -> ClientError {
    ClientError::from(tonic::Status::deadline_exceeded(format!(
        "{target_plane} {operation} timed out after {}ms",
        timeout.as_millis()
    )))
}

fn refresh_hint_from_error(err: &ClientError) -> RefreshHint {
    match err {
        ClientError::Action(action) => match action.as_ref() {
            ClientAction::Refresh { hint, .. } => hint.as_ref().clone(),
            _ => RefreshHint::default(),
        },
        _ => RefreshHint::default(),
    }
}

fn is_unknown_commit_file_outcome(err: &ClientError) -> bool {
    matches!(err, ClientError::UnknownOutcome(_))
        || matches!(ErrorClassifier.classify_error(err), ErrorClass::RetryableTransport)
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

fn mark_session_after_write_error(session: &mut WriteSession, err: &ClientError) {
    if is_conservative_unknown_outcome_error(err) {
        session.mark_unknown_outcome();
    } else if is_session_or_fencing_error(err) || is_write_refresh_error(err) {
        mark_session_after_session_error(session, err);
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
                | RefreshReason::WorkerEpochMismatch
                | RefreshReason::BlockStampMismatch
                | RefreshReason::Unknown
        )
    )
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

fn default_file_layout() -> proto::common::FileLayoutProto {
    proto::common::FileLayoutProto {
        block_size: DEFAULT_BLOCK_SIZE,
        chunk_size: DEFAULT_CHUNK_SIZE,
        replication: DEFAULT_REPLICATION,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, VecDeque};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;
    use common::error::canonical::{CanonicalError, RefreshHint as CanonicalRefreshHint, RefreshReason};
    use common::header::RpcErrorCode;
    use proto::common::{BlockIdProto, FencingTokenProto, WorkerEndpointInfoProto, WorkerNetProtocolProto};
    use proto::metadata::{
        AbortFileWriteResponseProto, AppendFileResponseProto, CommitFileResponseProto, CreateFileResponseProto,
        DeleteResponseProto, FileBlockLocationProto, GetBlockLocationsResponseProto, GetStatusResponseProto,
        ListStatusResponseProto, OpenFileResponseProto, RenameResponseProto, RenewLeaseResponseProto, WriteHandleProto,
        WriteTargetProto,
    };
    use tokio::sync::Notify;
    use types::{DataHandleId, InodeId};

    use crate::canonical::{ClientAction, RefreshHint};
    use crate::data::{WorkerCommitResult, WorkerDataClient, WorkerWriteBlock, WorkerWriteTarget};
    use crate::metadata::{
        AbortFileWriteOp, AbortFileWriteResult, AddBlockOp, AddBlockResult, AppendFileOp, CommitFileOp,
        CommitFileResult, CreateFileOp, DeleteOp, GetBlockLocationsOp, GetStatusOp, ListStatusOp, MsyncOp, OpenFileOp,
        RenameOp, RenewLeaseOp, RenewLeaseResult,
    };
    use crate::planner::read_planner::PlannedReadSegment;

    type EventLog = Arc<Mutex<Vec<&'static str>>>;

    #[derive(Debug)]
    struct NoSleep;

    #[async_trait]
    impl BackoffSleeper for NoSleep {
        async fn sleep(&self, _delay: Duration) {}
    }

    #[derive(Debug, Default)]
    struct RecordingMetrics {
        events: Mutex<Vec<ClientMetricEvent>>,
    }

    impl ClientMetrics for RecordingMetrics {
        fn record(&self, event: ClientMetricEvent) {
            self.events.lock().expect("events").push(event);
        }
    }

    impl RecordingMetrics {
        fn events(&self) -> Vec<ClientMetricEvent> {
            self.events.lock().expect("events").clone()
        }
    }

    fn client_with_metrics(
        config: ClientConfig,
        gateway: Arc<dyn MetadataGateway>,
        data_boundary: DataPlaneBoundary,
        metrics: Arc<RecordingMetrics>,
    ) -> FsClient {
        let metrics_hook: Arc<dyn ClientMetrics> = metrics;
        FsClient::with_runtime_hooks(config, gateway, data_boundary, Arc::new(NoSleep), metrics_hook).expect("client")
    }

    #[tokio::test]
    async fn open_read_replays_owner_redirect_with_same_call_id() {
        let gateway = Arc::new(MockGateway::owner_redirect_then_open(11));
        let client = FsClient::with_metadata_gateway(test_config(9), gateway.clone()).expect("client");

        let handle = client
            .open("/alpha", OpenOptions::read_only())
            .await
            .expect("open replay succeeds");

        assert_eq!(handle.inode_id(), InodeId::new(101));
        assert_eq!(handle.data_handle_id(), DataHandleId::new(202));
        let calls = gateway.calls();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].method, "open_file");
        assert_eq!(calls[0].group_id, 9);
        assert_eq!(calls[1].group_id, 11);
        assert_eq!(calls[0].call_id, calls[1].call_id);
    }

    #[tokio::test]
    async fn open_create_and_append_use_metadata_gateway() {
        let gateway = Arc::new(MockGateway::default());
        let client = FsClient::with_metadata_gateway(test_config(9), gateway.clone()).expect("client");

        let created = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("create open");
        let appended = client
            .open("/append", OpenOptions::append())
            .await
            .expect("append open");

        assert_eq!(created.path(), "/created");
        assert_eq!(appended.path(), "/append");
        let methods: Vec<_> = gateway.calls().into_iter().map(|call| call.method).collect();
        assert_eq!(methods, vec!["create_file", "append_file"]);
    }

    #[tokio::test]
    async fn file_handle_debug_redacts_write_session_identity_names() {
        let gateway = Arc::new(MockGateway::default());
        let client = FsClient::with_metadata_gateway(test_config(9), gateway).expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");
        let debug = format!("{handle:?}");

        assert!(debug.contains("is_write"));
        for needle in [
            concat!("inode", "_id"),
            concat!("data", "_handle_id"),
            concat!("file", "_version"),
            concat!("write", "_handle"),
            "fencing",
            concat!("route", "_epoch"),
            concat!("worker", "_epoch"),
            concat!("block", "_stamp"),
            concat!("call", "_id"),
            concat!("stream", "_id"),
        ] {
            assert!(
                !debug.contains(needle),
                "FileHandle Debug output must redact {needle}: {debug}"
            );
        }
    }

    #[tokio::test]
    async fn stat_list_delete_and_rename_use_metadata_gateway() {
        let gateway = Arc::new(MockGateway::default());
        let client = FsClient::with_metadata_gateway(test_config(9), gateway.clone()).expect("client");

        let status = client.stat("/alpha").await.expect("stat");
        let listing = client.list("/alpha").await.expect("list");
        client.delete("/alpha", false).await.expect("delete");
        client.rename("/alpha", "/beta").await.expect("rename");

        assert_eq!(status.path(), "/alpha");
        assert_eq!(status.attrs.size, 10);
        assert_eq!(listing.path(), "/alpha");
        assert!(listing.eof);
        assert_eq!(listing.entries.len(), 1);
        assert_eq!(listing.entries[0].name, "child");
        assert_eq!(listing.entries[0].kind, Some(crate::api::FileKind::File));
        assert_eq!(listing.entries[0].attrs.as_ref().expect("entry attrs").size, 4);
        let methods: Vec<_> = gateway.calls().into_iter().map(|call| call.method).collect();
        assert_eq!(methods, vec!["get_status", "list_status", "delete", "rename"]);
    }

    #[tokio::test]
    async fn public_read_handles_empty_ranges_without_worker_io() {
        let gateway = Arc::new(MockGateway::default());
        let client = FsClient::with_metadata_gateway(test_config(9), gateway).expect("client");
        let handle = read_handle(10);

        assert!(client.read(&handle, 0, 0).await.expect("zero read").is_empty());
        assert!(client.read(&handle, 10, 8).await.expect("past EOF read").is_empty());
    }

    #[tokio::test]
    async fn public_write_rejects_non_write_handle() {
        let gateway = Arc::new(MockGateway::default());
        let client = FsClient::with_metadata_gateway(test_config(9), gateway).expect("client");
        let handle = read_handle(10);

        let err = client
            .write(&handle, 0, Bytes::from_static(b"x"))
            .await
            .expect_err("read handle write must fail");

        assert!(matches!(err, ClientError::InvalidArgument(msg) if msg.contains("write handle")));
    }

    #[tokio::test]
    async fn empty_write_on_valid_write_handle_is_noop() {
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient::default());
        let client = FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker.clone()))
            .expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        client
            .write(&handle, 0, Bytes::new())
            .await
            .expect("empty write on valid write handle");
        client.close(&handle).await.expect("close empty file");

        assert!(worker.written_bytes().is_empty());
        let commit = gateway
            .calls()
            .into_iter()
            .find(|call| call.method == "commit_file")
            .expect("commit_file call");
        assert_eq!(commit.final_size, Some(0));
        assert!(commit.committed_block_lens.is_empty());
    }

    #[tokio::test]
    async fn empty_write_on_read_handle_fails() {
        let gateway = Arc::new(MockGateway::default());
        let client = FsClient::with_metadata_gateway(test_config(9), gateway).expect("client");
        let handle = read_handle(10);

        let err = client
            .write(&handle, 0, Bytes::new())
            .await
            .expect_err("empty write on read handle must fail");

        assert!(matches!(err, ClientError::InvalidArgument(msg) if msg.contains("write handle")));
    }

    #[tokio::test]
    async fn empty_write_on_closed_handle_fails() {
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient::default());
        let client = FsClient::with_data_boundary(test_config(9), gateway, data_boundary(worker)).expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        client.close(&handle).await.expect("empty close");
        let err = client
            .write(&handle, 0, Bytes::new())
            .await
            .expect_err("empty write on closed handle must fail");

        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("closed")));
    }

    #[tokio::test]
    async fn empty_write_on_invalid_handle_fails() {
        let gateway = Arc::new(MockGateway::default());
        let client = FsClient::with_metadata_gateway(test_config(9), gateway).expect("client");
        let handle = FileHandle::read("/invalid".to_string(), InodeId::new(0), DataHandleId::new(0), 0, 0);

        let err = client
            .write(&handle, 0, Bytes::new())
            .await
            .expect_err("empty write on invalid handle must fail");

        assert!(matches!(err, ClientError::InvalidArgument(msg) if msg.contains("write handle")));
    }

    #[tokio::test]
    async fn public_write_rejects_sparse_offset_mismatch_without_worker_io() {
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient::default());
        let client = FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker.clone()))
            .expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        let err = client
            .write(&handle, 1, Bytes::from_static(b"abc"))
            .await
            .expect_err("non-sequential write must fail");

        assert!(matches!(err, ClientError::InvalidArgument(msg) if msg.contains("sequential write cursor")));
        assert_eq!(worker.calls(), 0);
        assert!(worker.written_bytes().is_empty());

        client
            .write(&handle, 0, Bytes::from_static(b"abc"))
            .await
            .expect("local offset validation must not poison the session");
        client.close(&handle).await.expect("close after local validation error");
        assert_eq!(worker.written_bytes(), Bytes::from_static(b"abc"));
        assert_eq!(method_count(&gateway.calls(), "commit_file"), 1);
    }

    #[tokio::test]
    async fn public_write_create_write_close_commits_final_size() {
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient::default());
        let client = FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker.clone()))
            .expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect("sequential write");
        client.close(&handle).await.expect("close commit");

        assert_eq!(worker.written_bytes(), Bytes::from_static(b"hello"));
        let calls = gateway.calls();
        assert!(calls.iter().any(|call| call.method == "add_block"));
        let commit = calls
            .iter()
            .find(|call| call.method == "commit_file")
            .expect("commit_file call");
        assert_eq!(commit.final_size, Some(5));
        assert_eq!(commit.committed_block_lens, vec![5]);
    }

    #[tokio::test]
    async fn public_hflush_and_hsync_are_unsupported_without_side_effects() {
        let events = event_log();
        let gateway = Arc::new(MockGateway::with_events(events.clone()));
        let worker = Arc::new(MockDataClient::with_events(events.clone()));
        let client = FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker.clone()))
            .expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect("sequential write");

        let events_before = events.lock().expect("events").clone();
        let calls_before = gateway.calls();
        let worker_calls_before = worker.calls();
        let written_before = worker.written_bytes();

        let err = client.hflush(&handle).await.expect_err("hflush must be unsupported");
        assert!(matches!(err, ClientError::Unsupported(msg) if msg.contains("hflush")));
        let err = client.hsync(&handle).await.expect_err("hsync must be unsupported");
        assert!(matches!(err, ClientError::Unsupported(msg) if msg.contains("hsync")));

        assert_eq!(events.lock().expect("events").clone(), events_before);
        assert_eq!(gateway.calls(), calls_before);
        assert_eq!(worker.calls(), worker_calls_before);
        assert_eq!(worker.written_bytes(), written_before);
        assert!(worker.committed_lens().is_empty());
        assert!(worker.commit_sync_flags().is_empty());
        assert_eq!(method_count(&gateway.calls(), "hflush"), 0);
        assert_eq!(method_count(&gateway.calls(), "hsync"), 0);
        assert_eq!(method_count(&gateway.calls(), "commit_file"), 0);
    }

    #[tokio::test]
    async fn public_hflush_hsync_emit_unsupported_metrics_with_safe_labels() {
        let metrics = Arc::new(RecordingMetrics::default());
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient::default());
        let client = client_with_metrics(
            test_config(9),
            gateway.clone(),
            data_boundary(worker),
            Arc::clone(&metrics),
        );
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        let _ = client.hflush(&handle).await.expect_err("hflush unsupported");
        let _ = client.hsync(&handle).await.expect_err("hsync unsupported");

        let events = metrics.events();
        assert_eq!(metric_count(&events, ClientMetric::UnsupportedOperation), 2);
        assert!(events.iter().all(|event| event.labels.has_only_safe_values()));
        assert_eq!(method_count(&gateway.calls(), "hflush"), 0);
        assert_eq!(method_count(&gateway.calls(), "hsync"), 0);
    }

    #[tokio::test]
    async fn public_write_and_close_continue_after_unsupported_hflush_hsync() {
        let events = event_log();
        let gateway = Arc::new(MockGateway::with_events(events.clone()));
        let worker = Arc::new(MockDataClient::with_events(events.clone()));
        let client = FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker.clone()))
            .expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect("sequential write");
        let events_before = events.lock().expect("events").clone();
        let err = client.hflush(&handle).await.expect_err("hflush must be unsupported");
        assert!(matches!(err, ClientError::Unsupported(msg) if msg.contains("hflush")));
        let err = client.hsync(&handle).await.expect_err("hsync must be unsupported");
        assert!(matches!(err, ClientError::Unsupported(msg) if msg.contains("hsync")));

        assert_eq!(events.lock().expect("events").clone(), events_before);
        client
            .write(&handle, 5, Bytes::from_static(b"!"))
            .await
            .expect("write continues at cursor");
        client.close(&handle).await.expect("close after unsupported barriers");

        assert_eq!(worker.committed_lens(), vec![5, 1]);
        assert_eq!(worker.commit_sync_flags(), vec![false, false]);
        assert_eq!(method_count(&gateway.calls(), "hsync"), 0);
        assert_eq!(method_count(&gateway.calls(), "hflush"), 0);
        let commit = gateway
            .calls()
            .into_iter()
            .find(|call| call.method == "commit_file")
            .expect("commit_file call");
        assert_eq!(commit.final_size, Some(6));
        assert_eq!(commit.committed_block_offsets, vec![0, 5]);
        assert_eq!(commit.committed_block_lens, vec![5, 1]);
        assert_event_order(&events, "commit_write", "commit_file");
    }

    #[tokio::test]
    async fn public_hflush_hsync_validate_handles_before_unsupported() {
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient::default());
        let client =
            FsClient::with_data_boundary(test_config(9), gateway, data_boundary(worker.clone())).expect("client");
        let invalid = FileHandle::read("/invalid".to_string(), InodeId::new(0), DataHandleId::new(0), 0, 0);
        let read = read_handle(10);
        let write = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");
        let aborted = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        let err = client.hflush(&invalid).await.expect_err("invalid hflush must fail");
        assert!(matches!(err, ClientError::InvalidArgument(msg) if msg.contains("write handle")));
        let err = client.hsync(&invalid).await.expect_err("invalid hsync must fail");
        assert!(matches!(err, ClientError::InvalidArgument(msg) if msg.contains("write handle")));

        let err = client.hflush(&read).await.expect_err("read hflush must fail");
        assert!(matches!(err, ClientError::InvalidArgument(msg) if msg.contains("write handle")));
        let err = client.hsync(&read).await.expect_err("read hsync must fail");
        assert!(matches!(err, ClientError::InvalidArgument(msg) if msg.contains("write handle")));

        client.close(&write).await.expect("close empty handle");
        let err = client.hflush(&write).await.expect_err("closed hflush must fail");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("closed")));
        let err = client.hsync(&write).await.expect_err("closed hsync must fail");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("closed")));

        client.abort(&aborted).await.expect("abort empty handle");
        let err = client.hflush(&aborted).await.expect_err("aborted hflush must fail");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("aborted")));
        let err = client.hsync(&aborted).await.expect_err("aborted hsync must fail");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("aborted")));
    }

    #[tokio::test]
    async fn public_renew_lease_succeeds_without_exposing_lease_state() {
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient::default());
        let client = FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker.clone()))
            .expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        client.renew_lease(&handle).await.expect("renew write lease");
        let session_ref = handle.write_session().expect("write session");
        let session = session_ref.lock().await;
        assert_eq!(session.expires_at_ms(), Some(u64::MAX / 2));
        drop(session);
        client
            .write(&handle, 0, Bytes::from_static(b"ok"))
            .await
            .expect("write after renew");
        client.close(&handle).await.expect("close after renew");

        assert_eq!(method_count(&gateway.calls(), "renew_lease"), 1);
        assert_eq!(worker.written_bytes(), Bytes::from_static(b"ok"));
    }

    #[tokio::test]
    async fn public_renew_lease_transport_failure_preserves_session_state() {
        let gateway = Arc::new(MockGateway::with_renew_outcomes(vec![RenewOutcome::TransportFailure]));
        let worker = Arc::new(MockDataClient::default());
        let client =
            FsClient::with_data_boundary(test_config_with_retries(9, 0), gateway.clone(), data_boundary(worker))
                .expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        let err = client
            .renew_lease(&handle)
            .await
            .expect_err("renew transport failure must surface");
        assert_eq!(ErrorClassifier.classify_error(&err), ErrorClass::RetryableTransport);

        client
            .write(&handle, 0, Bytes::from_static(b"ok"))
            .await
            .expect("write still uses existing session state");
        client.close(&handle).await.expect("close after failed renew");
    }

    #[tokio::test]
    async fn public_renew_lease_session_expired_is_typed_and_blocks_session() {
        let gateway = Arc::new(MockGateway::with_renew_outcomes(vec![RenewOutcome::SessionExpired]));
        let worker = Arc::new(MockDataClient::default());
        let client =
            FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker)).expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        let err = client
            .renew_lease(&handle)
            .await
            .expect_err("expired session must fail");
        assert_eq!(ErrorClassifier.classify_error(&err), ErrorClass::SessionExpired);

        let err = client
            .write(&handle, 0, Bytes::from_static(b"x"))
            .await
            .expect_err("expired session blocks writes");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("session")));
    }

    #[tokio::test]
    async fn public_overwrite_sequential_write_close_commits_from_zero_without_append_base() {
        let events = event_log();
        let gateway = Arc::new(MockGateway::with_events(events.clone()));
        let worker = Arc::new(MockDataClient::with_events(events.clone()));
        let client = FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker.clone()))
            .expect("client");
        let handle = client
            .open("/overwrite", OpenOptions::overwrite())
            .await
            .expect("overwrite handle");

        client
            .write(&handle, 0, Bytes::from_static(b"fresh"))
            .await
            .expect("overwrite sequential write");
        client.close(&handle).await.expect("overwrite close");

        assert_eq!(handle.size_hint(), 0);
        assert_eq!(worker.written_bytes(), Bytes::from_static(b"fresh"));
        assert_eq!(worker.committed_lens(), vec![5]);

        let calls = gateway.calls();
        assert!(
            !calls.iter().any(|call| call.method == "append_file"),
            "overwrite must not use append"
        );
        let create = calls
            .iter()
            .find(|call| call.method == "create_file")
            .expect("create_file call");
        assert_eq!(
            create.create_disposition,
            Some(CreateDispositionProto::Overwrite as i32)
        );
        let commit = calls
            .iter()
            .find(|call| call.method == "commit_file")
            .expect("commit_file call");
        assert_eq!(commit.final_size, Some(5));
        assert_eq!(commit.committed_block_offsets, vec![0]);
        assert_eq!(commit.committed_block_lens, vec![5]);
        assert_event_order(&events, "commit_write", "commit_file");
    }

    #[tokio::test]
    async fn public_write_rejects_earlier_offset_after_cursor_advances_without_state_change() {
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient::default());
        let client = FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker.clone()))
            .expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        client
            .write(&handle, 0, Bytes::from_static(b"abc"))
            .await
            .expect("first sequential write");
        let add_block_count = method_count(&gateway.calls(), "add_block");

        let err = client
            .write(&handle, 0, Bytes::from_static(b"z"))
            .await
            .expect_err("earlier offset must fail");

        assert!(matches!(err, ClientError::InvalidArgument(msg) if msg.contains("sequential write cursor")));
        assert_eq!(worker.calls(), 1);
        assert_eq!(worker.written_bytes(), Bytes::from_static(b"abc"));
        assert!(worker.committed_lens().is_empty());
        assert_eq!(method_count(&gateway.calls(), "add_block"), add_block_count);

        client
            .write(&handle, 3, Bytes::from_static(b"de"))
            .await
            .expect("cursor was not changed by rejected write");
        client.close(&handle).await.expect("close commit");

        let commit = gateway
            .calls()
            .into_iter()
            .find(|call| call.method == "commit_file")
            .expect("commit_file call");
        assert_eq!(commit.final_size, Some(5));
        assert_eq!(commit.committed_block_offsets, vec![0, 3]);
        assert_eq!(commit.committed_block_lens, vec![3, 2]);
    }

    #[tokio::test]
    async fn empty_write_after_abort_validates_session_state_before_noop() {
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient::default());
        let client = FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker.clone()))
            .expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        client.abort(&handle).await.expect("abort empty write handle");
        let err = client
            .write(&handle, 0, Bytes::new())
            .await
            .expect_err("empty write after abort must fail");

        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("aborted")));
        assert_eq!(worker.calls(), 0);
        assert!(worker.written_bytes().is_empty());
        assert_eq!(method_count(&gateway.calls(), "abort_file_write"), 1);
    }

    #[tokio::test]
    async fn public_write_after_abort_is_rejected_without_worker_io() {
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient::default());
        let client = FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker.clone()))
            .expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        client.abort(&handle).await.expect("abort write handle");
        let err = client
            .write(&handle, 0, Bytes::from_static(b"x"))
            .await
            .expect_err("write after abort must fail");

        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("aborted")));
        assert_eq!(worker.calls(), 0);
        assert!(worker.written_bytes().is_empty());
        assert_eq!(method_count(&gateway.calls(), "abort_file_write"), 1);
    }

    #[tokio::test]
    async fn public_abort_after_commitfile_unknown_is_rejected_and_preserves_retry_state() {
        let gateway = Arc::new(MockGateway::with_commit_outcomes(vec![
            CommitOutcome::TransportUnknown,
            CommitOutcome::Ok,
        ]));
        let worker = Arc::new(MockDataClient::default());
        let client =
            FsClient::with_data_boundary(test_config_with_retries(9, 0), gateway.clone(), data_boundary(worker))
                .expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect("sequential write");
        let err = client.close(&handle).await.expect_err("close outcome unknown");
        assert!(matches!(err, ClientError::UnknownOutcome(_)));

        let err = client
            .abort(&handle)
            .await
            .expect_err("abort after unknown CommitFile is unsafe");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("CommitFile")));
        assert_eq!(method_count(&gateway.calls(), "abort_file_write"), 0);

        let err = client
            .write(&handle, 5, Bytes::from_static(b"!"))
            .await
            .expect_err("unknown CommitFile state must reject more writes");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("CommitFile")));

        client
            .close(&handle)
            .await
            .expect("retry close still uses frozen commit identity");

        let commits: Vec<_> = gateway
            .calls()
            .into_iter()
            .filter(|call| call.method == "commit_file")
            .collect();
        assert_eq!(commits.len(), 2);
        assert_eq!(commits[0].call_id, commits[1].call_id);
        assert_eq!(commits[0].operation_fingerprint, commits[1].operation_fingerprint);
        assert_eq!(commits[0].final_size, Some(5));
        assert_eq!(commits[1].final_size, Some(5));
        assert_eq!(commits[0].committed_block_offsets, vec![0]);
        assert_eq!(commits[1].committed_block_offsets, vec![0]);
        assert_eq!(commits[0].committed_block_lens, vec![5]);
        assert_eq!(commits[1].committed_block_lens, vec![5]);
    }

    #[tokio::test]
    async fn public_abort_rejects_read_closed_and_repeated_handles() {
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient::default());
        let client = FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker.clone()))
            .expect("client");
        let read = read_handle(10);
        let closed = client
            .open("/closed", OpenOptions::create_new())
            .await
            .expect("closed handle");
        let aborted = client
            .open("/aborted", OpenOptions::create_new())
            .await
            .expect("aborted handle");

        let err = client.abort(&read).await.expect_err("read handle abort must fail");
        assert!(matches!(err, ClientError::InvalidArgument(msg) if msg.contains("write handle")));

        client.close(&closed).await.expect("close empty handle");
        let err = client.abort(&closed).await.expect_err("closed handle abort must fail");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("closed")));

        client.abort(&aborted).await.expect("abort empty handle");
        let err = client
            .abort(&aborted)
            .await
            .expect_err("repeated abort returns a clear stale-handle error");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("aborted")));
        assert_eq!(worker.calls(), 0);
        assert_eq!(method_count(&gateway.calls(), "abort_file_write"), 1);
    }

    #[tokio::test]
    async fn public_abort_after_worker_write_calls_worker_then_metadata_and_blocks_session() {
        let events = event_log();
        let gateway = Arc::new(MockGateway::with_events(events.clone()));
        let worker = Arc::new(MockDataClient::with_events(events.clone()));
        let client = FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker.clone()))
            .expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect("sequential write");
        client.abort(&handle).await.expect("abort pending worker stream");

        assert_eq!(method_count(&gateway.calls(), "abort_file_write"), 1);
        assert_event_order(&events, "abort_write", "abort_file_write");
        let err = client
            .write(&handle, 5, Bytes::from_static(b"!"))
            .await
            .expect_err("write after abort must fail");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("aborted")));
        let err = client.close(&handle).await.expect_err("close after abort must fail");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("aborted")));
    }

    #[tokio::test]
    async fn public_abort_attempts_metadata_after_worker_abort_unknown_and_allows_abort_retry() {
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient::with_abort_outcomes(vec![
            WorkerAbortOutcome::Unknown,
            WorkerAbortOutcome::Ok,
        ]));
        let client = FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker.clone()))
            .expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect("sequential write");
        let err = client.abort(&handle).await.expect_err("worker abort outcome unknown");
        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("AbortWrite")));
        assert_eq!(method_count(&gateway.calls(), "abort_file_write"), 1);

        let err = client
            .write(&handle, 5, Bytes::from_static(b"!"))
            .await
            .expect_err("unknown abort outcome blocks writes");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("abort outcome")));
        let err = client
            .close(&handle)
            .await
            .expect_err("unknown abort outcome blocks close");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("abort outcome")));

        client.abort(&handle).await.expect("abort retry can finish cleanup");
        let err = client.abort(&handle).await.expect_err("finished abort is terminal");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("aborted")));
        assert_eq!(method_count(&gateway.calls(), "abort_file_write"), 2);
    }

    #[tokio::test]
    async fn abort_unknown_retry_reuses_worker_abort_identity_and_snapshot() {
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient::with_abort_outcomes(vec![
            WorkerAbortOutcome::Unknown,
            WorkerAbortOutcome::Ok,
        ]));
        let client = FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker.clone()))
            .expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect("sequential write");
        let err = client.abort(&handle).await.expect_err("worker abort outcome unknown");
        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("AbortWrite")));

        client.abort(&handle).await.expect("abort retry succeeds");

        let worker_aborts = worker.abort_records();
        assert_eq!(worker_aborts.len(), 2);
        assert_eq!(worker_aborts[0].call_id, worker_aborts[1].call_id);
        assert_eq!(
            worker_aborts[0].operation_fingerprint,
            worker_aborts[1].operation_fingerprint
        );
        assert_eq!(
            worker_block_signature(&worker_aborts[0].block),
            worker_block_signature(&worker_aborts[1].block)
        );

        let metadata_aborts = gateway.abort_file_records();
        assert_eq!(metadata_aborts.len(), 2);
        assert_eq!(metadata_aborts[0].call_id, metadata_aborts[1].call_id);
        assert_eq!(
            metadata_aborts[0].operation_fingerprint,
            metadata_aborts[1].operation_fingerprint
        );
        assert_eq!(metadata_aborts[0].write_handle, metadata_aborts[1].write_handle);
    }

    #[tokio::test]
    async fn abort_unknown_retry_reuses_metadata_abort_identity_and_payload() {
        let gateway = Arc::new(MockGateway::with_abort_outcomes(vec![
            AbortOutcome::TransportUnknown,
            AbortOutcome::Ok,
        ]));
        let worker = Arc::new(MockDataClient::default());
        let client =
            FsClient::with_data_boundary(test_config_with_retries(9, 0), gateway.clone(), data_boundary(worker))
                .expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        let err = client.abort(&handle).await.expect_err("metadata abort outcome unknown");
        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("AbortFileWrite")));
        client.abort(&handle).await.expect("abort retry succeeds");

        let metadata_aborts = gateway.abort_file_records();
        assert_eq!(metadata_aborts.len(), 2);
        assert_eq!(metadata_aborts[0].call_id, metadata_aborts[1].call_id);
        assert_eq!(
            metadata_aborts[0].operation_fingerprint,
            metadata_aborts[1].operation_fingerprint
        );
        assert_eq!(metadata_aborts[0].write_handle, metadata_aborts[1].write_handle);
    }

    #[tokio::test]
    async fn abort_cleanup_timeout_marks_abort_unknown_and_preserves_frozen_identity() {
        let gateway = Arc::new(MockGateway::with_abort_outcomes(vec![
            AbortOutcome::Pending,
            AbortOutcome::Ok,
        ]));
        let worker = Arc::new(MockDataClient::default());
        let client = FsClient::with_data_boundary(
            test_config_with_timeout(9, 0, 10),
            gateway.clone(),
            data_boundary(worker),
        )
        .expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        let err = client.abort(&handle).await.expect_err("metadata abort timeout unknown");
        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("AbortFileWrite")));
        let err = client
            .write(&handle, 0, Bytes::from_static(b"x"))
            .await
            .expect_err("AbortUnknown blocks writes");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("abort outcome")));
        let err = client.close(&handle).await.expect_err("AbortUnknown blocks close");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("abort outcome")));

        client.abort(&handle).await.expect("abort retry succeeds");

        let metadata_aborts = gateway.abort_file_records();
        assert_eq!(metadata_aborts.len(), 2);
        assert_eq!(metadata_aborts[0].call_id, metadata_aborts[1].call_id);
        assert_eq!(
            metadata_aborts[0].operation_fingerprint,
            metadata_aborts[1].operation_fingerprint
        );
        assert_eq!(metadata_aborts[0].write_handle, metadata_aborts[1].write_handle);
    }

    #[tokio::test]
    async fn abort_cleanup_metrics_cover_unknown_and_success() {
        let metrics = Arc::new(RecordingMetrics::default());
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient::with_abort_outcomes(vec![
            WorkerAbortOutcome::Unknown,
            WorkerAbortOutcome::Ok,
        ]));
        let client = client_with_metrics(test_config(9), gateway, data_boundary(worker), Arc::clone(&metrics));
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect("sequential write");
        let _ = client.abort(&handle).await.expect_err("abort unknown");
        client.abort(&handle).await.expect("abort retry succeeds");

        let events = metrics.events();
        assert_metric(&events, ClientMetric::AbortAttempt);
        assert_metric(&events, ClientMetric::AbortUnknown);
        assert_metric(&events, ClientMetric::AbortSuccess);
        assert_metric(&events, ClientMetric::UnknownOutcome);
    }

    #[tokio::test]
    async fn public_abort_file_unknown_outcome_blocks_write_and_close_but_can_retry_abort() {
        let gateway = Arc::new(MockGateway::with_abort_outcomes(vec![
            AbortOutcome::TransportUnknown,
            AbortOutcome::Ok,
        ]));
        let worker = Arc::new(MockDataClient::default());
        let client =
            FsClient::with_data_boundary(test_config_with_retries(9, 0), gateway.clone(), data_boundary(worker))
                .expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        let err = client.abort(&handle).await.expect_err("metadata abort outcome unknown");
        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("AbortFileWrite")));
        let err = client
            .write(&handle, 0, Bytes::from_static(b"x"))
            .await
            .expect_err("unknown abort outcome blocks writes");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("abort outcome")));
        let err = client
            .close(&handle)
            .await
            .expect_err("unknown abort outcome blocks close");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("abort outcome")));

        client.abort(&handle).await.expect("abort retry succeeds");
        assert_eq!(method_count(&gateway.calls(), "abort_file_write"), 2);
    }

    #[tokio::test]
    async fn add_block_unknown_outcome_blocks_fresh_write_attempt() {
        let gateway = Arc::new(MockGateway::with_add_block_outcomes(vec![
            AddBlockOutcome::TransportUnknown,
        ]));
        let worker = Arc::new(MockDataClient::default());
        let client =
            FsClient::with_data_boundary(test_config_with_retries(9, 0), gateway.clone(), data_boundary(worker))
                .expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        let err = client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect_err("AddBlock unknown outcome must fail");
        assert!(matches!(err, ClientError::UnknownOutcome(ref msg) if msg.contains("AddBlock")));

        let err = client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect_err("write session must stay conservative");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("unknown outcome")));
        let err = client.close(&handle).await.expect_err("close must stay conservative");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("unknown outcome")));
        assert_eq!(method_count(&gateway.calls(), "add_block"), 1);
    }

    #[tokio::test]
    async fn add_block_malformed_ok_header_blocks_write_and_close() {
        let gateway = Arc::new(MockGateway::with_add_block_outcomes(vec![
            AddBlockOutcome::InvalidHeader,
        ]));
        let worker = Arc::new(MockDataClient::default());
        let client =
            FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker)).expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        let err = client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect_err("malformed AddBlock OK header must fail");
        assert!(matches!(err, ClientError::UnknownOutcome(ref msg) if msg.contains("AddBlock")));
        assert_ne!(ErrorClassifier.classify_error(&err), ErrorClass::RetryableTransport);

        let err = client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect_err("malformed AddBlock response blocks writes");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("unknown outcome")));
        let err = client
            .close(&handle)
            .await
            .expect_err("malformed AddBlock response blocks close");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("unknown outcome")));
        assert_eq!(method_count(&gateway.calls(), "add_block"), 1);
    }

    #[tokio::test]
    async fn invalid_header_side_effecting_write_metrics_are_emitted() {
        let metrics = Arc::new(RecordingMetrics::default());
        let gateway = Arc::new(MockGateway::with_add_block_outcomes(vec![
            AddBlockOutcome::InvalidHeader,
        ]));
        let worker = Arc::new(MockDataClient::default());
        let client = client_with_metrics(test_config(9), gateway, data_boundary(worker), Arc::clone(&metrics));
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        let err = client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect_err("invalid header must fail");

        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("AddBlock")));
        let events = metrics.events();
        assert_metric(&events, ClientMetric::InvalidHeader);
        assert_metric(&events, ClientMetric::UnknownOutcome);
        assert!(events.iter().all(|event| event.labels.has_only_safe_values()));
    }

    #[tokio::test]
    async fn add_block_body_mismatch_blocks_without_worker_io() {
        let gateway = Arc::new(MockGateway::with_add_block_outcomes(vec![
            AddBlockOutcome::MismatchedTargetDataHandle,
        ]));
        let worker = Arc::new(MockDataClient::default());
        let client = FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker.clone()))
            .expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        let err = client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect_err("AddBlock body mismatch must fail conservatively");
        assert!(matches!(err, ClientError::UnknownOutcome(ref msg) if msg.contains("AddBlock")));
        assert_eq!(worker.calls(), 0);

        let err = client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect_err("AddBlock body mismatch blocks writes");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("unknown outcome")));
        let err = client
            .close(&handle)
            .await
            .expect_err("AddBlock body mismatch blocks close");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("unknown outcome")));
        assert_eq!(method_count(&gateway.calls(), "add_block"), 1);
    }

    #[tokio::test]
    async fn add_block_session_expired_is_typed_and_blocks_fresh_write_attempt() {
        let gateway = Arc::new(MockGateway::with_add_block_outcomes(vec![
            AddBlockOutcome::SessionExpired,
        ]));
        let worker = Arc::new(MockDataClient::default());
        let client =
            FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker)).expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        let err = client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect_err("expired session must fail");
        assert_eq!(ErrorClassifier.classify_error(&err), ErrorClass::SessionExpired);

        let err = client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect_err("expired session blocks further writes");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("session")));
        assert_eq!(method_count(&gateway.calls(), "add_block"), 1);
    }

    #[tokio::test]
    async fn open_write_fatal_fencing_mismatch_blocks_write_and_close() {
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient::with_open_outcomes(vec![
            WorkerOpenOutcome::FatalFencing,
        ]));
        let client =
            FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker)).expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        let err = client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect_err("worker fencing mismatch must fail");
        assert_eq!(ErrorClassifier.classify_error(&err), ErrorClass::Fencing);
        assert_ne!(ErrorClassifier.classify_error(&err), ErrorClass::RetryableTransport);

        let err = client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect_err("fenced session blocks further writes");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("session")));
        let err = client.close(&handle).await.expect_err("fenced session blocks close");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("session")));
        assert_eq!(method_count(&gateway.calls(), "add_block"), 1);
    }

    #[tokio::test]
    async fn fencing_mismatch_metric_is_emitted_for_worker_write() {
        let metrics = Arc::new(RecordingMetrics::default());
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient::with_open_outcomes(vec![
            WorkerOpenOutcome::FatalFencing,
        ]));
        let client = client_with_metrics(test_config(9), gateway, data_boundary(worker), Arc::clone(&metrics));
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        let _ = client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect_err("fencing mismatch");

        assert_metric(&metrics.events(), ClientMetric::FencingMismatch);
    }

    #[tokio::test]
    async fn open_write_unknown_outcome_blocks_fresh_write_attempt() {
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient::with_open_outcomes(vec![WorkerOpenOutcome::Unknown]));
        let client =
            FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker)).expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        let err = client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect_err("OpenWriteStream unknown outcome must fail");
        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("OpenWriteStream")));

        let err = client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect_err("write session must stay conservative");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("unknown outcome")));
        assert_eq!(method_count(&gateway.calls(), "add_block"), 1);
    }

    #[tokio::test]
    async fn pending_worker_open_write_times_out_without_advancing_cursor_or_committing_block() {
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient::with_open_outcomes(vec![WorkerOpenOutcome::Pending]));
        let metrics = Arc::new(RecordingMetrics::default());
        let client = client_with_metrics(
            test_config_with_timeout(9, 0, 10),
            gateway,
            data_boundary(worker.clone()),
            metrics.clone(),
        );
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        let result = tokio::time::timeout(
            Duration::from_millis(200),
            client.write(&handle, 0, Bytes::from_static(b"hello")),
        )
        .await
        .expect("write must return before outer test timeout");
        let err = result.expect_err("pending OpenWriteStream must time out");

        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("OpenWriteStream")));
        assert_eq!(worker.written_bytes(), Bytes::new());
        let session_ref = handle.write_session().expect("write session");
        let mut session = session_ref.lock().await;
        assert_eq!(session.cursor(), 0);
        assert!(session.pending_blocks_mut().is_empty());
        assert_metric(&metrics.events(), ClientMetric::RpcTimeout);
    }

    #[tokio::test]
    async fn open_write_malformed_ok_header_blocks_write_and_close() {
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient::with_open_outcomes(vec![
            WorkerOpenOutcome::InvalidHeader,
        ]));
        let client =
            FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker)).expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        let err = client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect_err("malformed OpenWriteStream OK header must fail");
        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("OpenWriteStream")));

        let err = client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect_err("malformed OpenWriteStream response blocks writes");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("unknown outcome")));
        let err = client
            .close(&handle)
            .await
            .expect_err("malformed OpenWriteStream response blocks close");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("unknown outcome")));
        assert_eq!(method_count(&gateway.calls(), "add_block"), 1);
    }

    #[tokio::test]
    async fn write_stream_partial_ack_blocks_fresh_write_attempt() {
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient::with_write_outcomes(vec![
            WorkerWriteOutcome::PartialAck,
        ]));
        let client =
            FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker)).expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        let err = client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect_err("partial WriteStream ack must fail");
        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("WriteStream")));

        let err = client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect_err("write session must stay conservative");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("unknown outcome")));
        assert_eq!(method_count(&gateway.calls(), "add_block"), 1);
    }

    #[tokio::test]
    async fn write_stream_malformed_ok_header_blocks_without_committing() {
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient::with_write_outcomes(vec![
            WorkerWriteOutcome::InvalidHeader,
        ]));
        let client = FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker.clone()))
            .expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        let err = client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect_err("malformed WriteStream OK header must fail");
        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("WriteStream")));

        let err = client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect_err("malformed WriteStream response blocks writes");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("unknown outcome")));
        let err = client
            .close(&handle)
            .await
            .expect_err("malformed WriteStream response blocks close");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("unknown outcome")));
        assert!(worker.committed_lens().is_empty());
        assert_eq!(method_count(&gateway.calls(), "commit_file"), 0);
    }

    #[tokio::test]
    async fn commit_write_unknown_outcome_blocks_abort_and_close_retry() {
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient::with_commit_write_outcomes(vec![
            WorkerCommitOutcome::Unknown,
            WorkerCommitOutcome::Ok,
        ]));
        let client =
            FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker)).expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect("sequential write");
        let err = client.close(&handle).await.expect_err("CommitWrite outcome unknown");
        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("CommitWrite")));

        let err = client
            .close(&handle)
            .await
            .expect_err("unsafe CommitWrite retry denied");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("unknown outcome")));
        let err = client
            .abort(&handle)
            .await
            .expect_err("abort after CommitWrite unknown denied");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("unknown outcome")));
        assert_eq!(method_count(&gateway.calls(), "commit_file"), 0);
    }

    #[tokio::test]
    async fn commit_write_malformed_ok_header_blocks_close_retry_and_commitfile() {
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient::with_commit_write_outcomes(vec![
            WorkerCommitOutcome::InvalidHeader,
            WorkerCommitOutcome::Ok,
        ]));
        let client =
            FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker)).expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect("sequential write");
        let err = client
            .close(&handle)
            .await
            .expect_err("malformed CommitWrite OK header must fail");
        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("CommitWrite")));

        let err = client
            .close(&handle)
            .await
            .expect_err("malformed CommitWrite response blocks close retry");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("unknown outcome")));
        assert_eq!(method_count(&gateway.calls(), "commit_file"), 0);
    }

    #[tokio::test]
    async fn commit_write_length_body_mismatch_blocks_close_retry_and_commitfile() {
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient::with_commit_write_outcomes(vec![
            WorkerCommitOutcome::LengthMismatch,
            WorkerCommitOutcome::Ok,
        ]));
        let client =
            FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker)).expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect("sequential write");
        let err = client
            .close(&handle)
            .await
            .expect_err("CommitWrite length mismatch must fail");
        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("CommitWrite")));

        let err = client
            .close(&handle)
            .await
            .expect_err("CommitWrite mismatch blocks close retry");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("unknown outcome")));
        assert_eq!(method_count(&gateway.calls(), "commit_file"), 0);
    }

    #[tokio::test]
    async fn commit_write_written_through_body_mismatch_blocks_commitfile() {
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient::with_commit_write_outcomes(vec![
            WorkerCommitOutcome::WrittenThroughMismatch,
        ]));
        let client =
            FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker)).expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect("sequential write");
        let err = client
            .close(&handle)
            .await
            .expect_err("CommitWrite written_through mismatch must fail");

        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("CommitWrite")));
        assert_eq!(method_count(&gateway.calls(), "commit_file"), 0);
    }

    #[tokio::test]
    async fn commit_write_block_stamp_body_mismatch_blocks_commitfile() {
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient::with_commit_write_outcomes(vec![
            WorkerCommitOutcome::BlockStampMismatch,
        ]));
        let client =
            FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker)).expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect("sequential write");
        let err = client
            .close(&handle)
            .await
            .expect_err("CommitWrite block_stamp mismatch must fail");

        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("CommitWrite")));
        assert_eq!(method_count(&gateway.calls(), "commit_file"), 0);
    }

    #[tokio::test]
    async fn commit_write_fencing_mismatch_is_typed_and_blocks_session() {
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient::with_commit_write_outcomes(vec![
            WorkerCommitOutcome::Fencing,
        ]));
        let client =
            FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker)).expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect("sequential write");
        let err = client.close(&handle).await.expect_err("fencing mismatch must fail");
        assert_eq!(ErrorClassifier.classify_error(&err), ErrorClass::Fencing);

        let err = client
            .close(&handle)
            .await
            .expect_err("fenced session blocks close retry");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("session")));
        assert_eq!(method_count(&gateway.calls(), "commit_file"), 0);
    }

    #[tokio::test]
    async fn commit_write_fatal_fencing_mismatch_is_typed_and_blocks_session() {
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient::with_commit_write_outcomes(vec![
            WorkerCommitOutcome::FatalFencing,
            WorkerCommitOutcome::Ok,
        ]));
        let client =
            FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker)).expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect("sequential write");
        let err = client.close(&handle).await.expect_err("fencing mismatch must fail");
        assert_eq!(ErrorClassifier.classify_error(&err), ErrorClass::Fencing);
        assert_ne!(ErrorClassifier.classify_error(&err), ErrorClass::RetryableTransport);

        let err = client
            .close(&handle)
            .await
            .expect_err("fenced session blocks close retry");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("session")));
        assert_eq!(method_count(&gateway.calls(), "commit_file"), 0);
    }

    #[tokio::test]
    async fn commit_write_worker_epoch_mismatch_is_typed_and_blocks_session() {
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient::with_commit_write_outcomes(vec![
            WorkerCommitOutcome::WorkerEpochMismatch,
        ]));
        let client =
            FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker)).expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect("sequential write");
        let err = client
            .close(&handle)
            .await
            .expect_err("worker epoch mismatch must fail");
        assert_eq!(
            ErrorClassifier.classify_error(&err),
            ErrorClass::NeedRefresh(crate::runtime::RefreshReason::WorkerEpochMismatch)
        );

        let err = client
            .close(&handle)
            .await
            .expect_err("stale worker session blocks close retry");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("session")));
        assert_eq!(method_count(&gateway.calls(), "commit_file"), 0);
    }

    #[tokio::test]
    async fn commit_file_session_expired_is_typed_and_blocks_close_retry() {
        let gateway = Arc::new(MockGateway::with_commit_outcomes(vec![CommitOutcome::SessionExpired]));
        let worker = Arc::new(MockDataClient::default());
        let client =
            FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker)).expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect("sequential write");
        let err = client.close(&handle).await.expect_err("expired session must fail");
        assert_eq!(ErrorClassifier.classify_error(&err), ErrorClass::SessionExpired);

        let err = client
            .close(&handle)
            .await
            .expect_err("expired session blocks close retry");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("session")));
        assert_eq!(method_count(&gateway.calls(), "commit_file"), 1);
    }

    #[tokio::test]
    async fn create_file_transport_exhausted_returns_unknown_outcome_without_handle() {
        let gateway = Arc::new(MockGateway::with_create_outcomes(vec![CreateOutcome::TransportUnknown]));
        let client = FsClient::with_metadata_gateway(test_config_with_retries(9, 0), gateway.clone()).expect("client");

        let err = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect_err("unresolved CreateFile transport outcome must fail");

        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("CreateFile")));
        assert_eq!(method_count(&gateway.calls(), "create_file"), 1);
    }

    #[tokio::test]
    async fn append_file_transport_exhausted_returns_unknown_outcome_without_handle() {
        let gateway = Arc::new(MockGateway::with_append_outcomes(vec![AppendOutcome::TransportUnknown]));
        let client = FsClient::with_metadata_gateway(test_config_with_retries(9, 0), gateway.clone()).expect("client");

        let err = client
            .open("/append", OpenOptions::append())
            .await
            .expect_err("unresolved AppendFile transport outcome must fail");

        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("AppendFile")));
        assert_eq!(method_count(&gateway.calls(), "append_file"), 1);
    }

    #[tokio::test]
    async fn create_file_transport_retry_reuses_call_id_and_can_return_persisted_result() {
        let gateway = Arc::new(MockGateway::with_create_outcomes(vec![
            CreateOutcome::TransportUnknown,
            CreateOutcome::Ok,
        ]));
        let client = FsClient::with_metadata_gateway(test_config_with_retries(9, 1), gateway.clone()).expect("client");

        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("CreateFile retry returns persisted result");

        assert_eq!(handle.path(), "/created");
        let calls: Vec<_> = gateway
            .calls()
            .into_iter()
            .filter(|call| call.method == "create_file")
            .collect();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].call_id, calls[1].call_id);
        assert_eq!(calls[0].operation_fingerprint, calls[1].operation_fingerprint);
    }

    #[tokio::test]
    async fn create_file_validation_error_is_not_unknown_outcome() {
        let gateway = Arc::new(MockGateway::with_create_outcomes(vec![CreateOutcome::InvalidArgument]));
        let client = FsClient::with_metadata_gateway(test_config(9), gateway).expect("client");

        let err = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect_err("metadata validation error must surface directly");

        assert!(matches!(err, ClientError::InvalidArgument(msg) if msg.contains("CreateFile")));
    }

    #[tokio::test]
    async fn abort_does_not_affect_another_write_session() {
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient::default());
        let client = FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker.clone()))
            .expect("client");
        let aborted = client
            .open("/aborted", OpenOptions::create_new())
            .await
            .expect("aborted handle");
        let other = client
            .open("/other", OpenOptions::create_new())
            .await
            .expect("other handle");

        client.abort(&aborted).await.expect("abort first handle");
        client
            .write(&other, 0, Bytes::from_static(b"ok"))
            .await
            .expect("other handle still writable");
        client.close(&other).await.expect("other handle closes");

        assert_eq!(worker.written_bytes(), Bytes::from_static(b"ok"));
        let commit = gateway
            .calls()
            .into_iter()
            .find(|call| call.method == "commit_file")
            .expect("commit_file call");
        assert_eq!(commit.final_size, Some(2));
    }

    #[tokio::test]
    async fn public_write_accepts_multiple_sequential_calls() {
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient::default());
        let client = FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker.clone()))
            .expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        client
            .write(&handle, 0, Bytes::from_static(b"hel"))
            .await
            .expect("first sequential write");
        client
            .write(&handle, 3, Bytes::from_static(b"lo"))
            .await
            .expect("second sequential write");
        client.close(&handle).await.expect("close commit");

        assert_eq!(worker.written_bytes(), Bytes::from_static(b"hello"));
        let commit = gateway
            .calls()
            .into_iter()
            .find(|call| call.method == "commit_file")
            .expect("commit_file call");
        assert_eq!(commit.final_size, Some(5));
        assert_eq!(commit.committed_block_offsets, vec![0, 3]);
        assert_eq!(commit.committed_block_lens, vec![3, 2]);
    }

    #[tokio::test]
    async fn public_close_uses_worker_ack_sequence_for_commit() {
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient::default());
        let client =
            FsClient::with_data_boundary(test_config(9), gateway, data_boundary(worker.clone())).expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");
        let data = Bytes::from(vec![7; 3 * 1024]);

        client.write(&handle, 0, data).await.expect("sequential write");
        client.close(&handle).await.expect("close commit");

        assert_eq!(worker.committed_seqs(), vec![3]);
    }

    #[tokio::test]
    async fn public_write_rejects_closed_handle() {
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient::default());
        let client = FsClient::with_data_boundary(test_config(9), gateway, data_boundary(worker)).expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        client.close(&handle).await.expect("empty close");
        let err = client
            .write(&handle, 0, Bytes::from_static(b"x"))
            .await
            .expect_err("closed handle must fail");

        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("closed")));
    }

    #[tokio::test]
    async fn public_close_after_unknown_reuses_commit_identity_and_frozen_payload_then_closes() {
        let gateway = Arc::new(MockGateway::with_commit_outcomes(vec![
            CommitOutcome::TransportUnknown,
            CommitOutcome::Ok,
        ]));
        let worker = Arc::new(MockDataClient::default());
        let client = FsClient::with_data_boundary(
            test_config_with_retries(9, 0),
            gateway.clone(),
            data_boundary(worker.clone()),
        )
        .expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect("sequential write");
        let err = client.close(&handle).await.expect_err("first close is unknown");
        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("CommitFile")));

        client
            .close(&handle)
            .await
            .expect("retry close succeeds through metadata dedup");
        assert_eq!(worker.committed_lens(), vec![5]);

        let commits: Vec<_> = gateway
            .calls()
            .into_iter()
            .filter(|call| call.method == "commit_file")
            .collect();
        assert_eq!(commits.len(), 2);
        assert_eq!(commits[0].call_id, commits[1].call_id);
        assert_eq!(commits[0].operation_fingerprint, commits[1].operation_fingerprint);
        assert_eq!(commits[0].final_size, Some(5));
        assert_eq!(commits[1].final_size, Some(5));
        assert_eq!(commits[0].committed_block_offsets, vec![0]);
        assert_eq!(commits[1].committed_block_offsets, vec![0]);
        assert_eq!(commits[0].committed_block_lens, vec![5]);
        assert_eq!(commits[1].committed_block_lens, vec![5]);

        let err = client
            .close(&handle)
            .await
            .expect_err("successful retry marks handle closed");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("closed")));
    }

    #[tokio::test]
    async fn public_write_after_commitfile_unknown_is_rejected() {
        let gateway = Arc::new(MockGateway::with_commit_outcomes(vec![CommitOutcome::TransportUnknown]));
        let worker = Arc::new(MockDataClient::default());
        let client = FsClient::with_data_boundary(test_config_with_retries(9, 0), gateway, data_boundary(worker))
            .expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect("sequential write");
        let err = client.close(&handle).await.expect_err("close outcome unknown");
        assert!(matches!(err, ClientError::UnknownOutcome(_)));

        let err = client
            .write(&handle, 5, Bytes::from_static(b"!"))
            .await
            .expect_err("writes after CommitFile starts must fail");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("CommitFile")));
    }

    #[tokio::test]
    async fn public_close_after_unknown_returns_clear_commit_replay_denial() {
        let gateway = Arc::new(MockGateway::with_commit_outcomes(vec![
            CommitOutcome::TransportUnknown,
            CommitOutcome::FingerprintMismatch,
        ]));
        let worker = Arc::new(MockDataClient::default());
        let client = FsClient::with_data_boundary(test_config_with_retries(9, 0), gateway, data_boundary(worker))
            .expect("client");
        let handle = client
            .open("/created", OpenOptions::create_new())
            .await
            .expect("write handle");

        client
            .write(&handle, 0, Bytes::from_static(b"hello"))
            .await
            .expect("sequential write");
        let err = client.close(&handle).await.expect_err("close outcome unknown");
        assert!(matches!(err, ClientError::UnknownOutcome(_)));

        let err = client
            .close(&handle)
            .await
            .expect_err("metadata rejects changed fingerprint replay");
        assert!(matches!(err, ClientError::Unsupported(msg) if msg.contains("fingerprint")));
    }

    #[tokio::test]
    async fn public_append_starts_at_existing_file_size() {
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient::default());
        let client = FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker.clone()))
            .expect("client");
        let handle = client
            .open("/append", OpenOptions::append())
            .await
            .expect("append handle");

        client
            .write(&handle, 10, Bytes::from_static(b"tail"))
            .await
            .expect("append write");
        client.close(&handle).await.expect("append close");

        assert_eq!(worker.written_bytes(), Bytes::from_static(b"tail"));
        let commit = gateway
            .calls()
            .into_iter()
            .find(|call| call.method == "commit_file")
            .expect("commit_file call");
        assert_eq!(commit.final_size, Some(14));
        assert_eq!(commit.committed_block_offsets, vec![10]);
    }

    #[tokio::test]
    async fn public_read_returns_exact_single_block_bytes() {
        let gateway = Arc::new(MockGateway::with_layout(layout_response(
            9,
            101,
            202,
            Some(3),
            16,
            vec![location(202, 0, 0, 16)],
        )));
        let worker = Arc::new(MockDataClient::from_file(b"abcdefghijklmnop"));
        let client = FsClient::with_data_boundary(test_config(9), gateway, data_boundary(worker)).expect("client");
        let handle = read_handle(16);

        let bytes = client.read(&handle, 2, 5).await.expect("read succeeds");

        assert_eq!(bytes, Bytes::from_static(b"cdefg"));
    }

    #[tokio::test]
    async fn public_read_requests_layout_by_data_handle_with_exact_range() {
        let gateway = Arc::new(MockGateway::with_layout(layout_response(
            9,
            101,
            202,
            Some(3),
            16,
            vec![location(202, 0, 0, 16)],
        )));
        let worker = Arc::new(MockDataClient::from_file(b"abcdefghijklmnop"));
        let client =
            FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker)).expect("client");
        let handle = read_handle(16);

        let bytes = client.read(&handle, 3, 6).await.expect("read succeeds");

        assert_eq!(bytes, Bytes::from_static(b"defghi"));
        let read_layout = gateway
            .calls()
            .into_iter()
            .find(|call| call.method == "read_layout")
            .expect("read layout call");
        assert_eq!(read_layout.group_id, 9);
        assert_eq!(read_layout.target_data_handle_id, Some(202));
        assert_eq!(read_layout.range, Some((3, 6)));
    }

    #[tokio::test]
    async fn disabled_layout_cache_fetches_layout_for_each_read() {
        let gateway = Arc::new(MockGateway::with_layout(layout_response(
            9,
            101,
            202,
            Some(3),
            16,
            vec![location(202, 0, 0, 16)],
        )));
        let worker = Arc::new(MockDataClient::from_file(b"abcdefghijklmnop"));
        let client =
            FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker)).expect("client");
        let handle = read_handle(16);

        client.read(&handle, 0, 4).await.expect("first read");
        client.read(&handle, 0, 4).await.expect("second read");

        assert_eq!(method_count(&gateway.calls(), "read_layout"), 2);
    }

    #[tokio::test]
    async fn enabled_layout_cache_reuses_validated_same_handle_range() {
        let gateway = Arc::new(MockGateway::with_layout(layout_response(
            9,
            101,
            202,
            Some(3),
            16,
            vec![location_with_stamp(202, 0, 0, 16, 77)],
        )));
        let worker = Arc::new(MockDataClient::from_file(b"abcdefghijklmnop"));
        let client = FsClient::with_data_boundary(
            test_config_with_layout_cache(9, Duration::from_secs(60), 16),
            gateway.clone(),
            data_boundary(worker.clone()),
        )
        .expect("client");
        let handle = read_handle(16);

        client.read(&handle, 0, 4).await.expect("first read");
        client.read(&handle, 0, 4).await.expect("second read");

        assert_eq!(method_count(&gateway.calls(), "read_layout"), 1);
        assert_eq!(worker.stamps(), vec![77, 77]);
    }

    #[tokio::test]
    async fn zero_layout_cache_ttl_forces_metadata_miss() {
        let gateway = Arc::new(MockGateway::with_layout(layout_response(
            9,
            101,
            202,
            Some(3),
            16,
            vec![location(202, 0, 0, 16)],
        )));
        let worker = Arc::new(MockDataClient::from_file(b"abcdefghijklmnop"));
        let client = FsClient::with_data_boundary(
            test_config_with_layout_cache(9, Duration::ZERO, 16),
            gateway.clone(),
            data_boundary(worker),
        )
        .expect("client");
        let handle = read_handle(16);

        client.read(&handle, 0, 4).await.expect("first read");
        client.read(&handle, 0, 4).await.expect("second read");

        assert_eq!(method_count(&gateway.calls(), "read_layout"), 2);
    }

    #[tokio::test]
    async fn layout_cache_key_includes_data_handle_and_file_version() {
        let gateway = Arc::new(MockGateway::with_layout(layout_response(
            9,
            101,
            202,
            Some(3),
            16,
            vec![location(202, 0, 0, 16)],
        )));
        let worker = Arc::new(MockDataClient::from_file(b"abcdefghijklmnop"));
        let client = FsClient::with_data_boundary(
            test_config_with_layout_cache(9, Duration::from_secs(60), 16),
            gateway.clone(),
            data_boundary(worker),
        )
        .expect("client");
        let handle = read_handle(16);
        let stale_data_handle =
            FileHandle::read("/alpha".to_string(), InodeId::new(101), DataHandleId::new(303), 3, 16);
        let stale_version = FileHandle::read("/alpha".to_string(), InodeId::new(101), DataHandleId::new(202), 4, 16);

        client.read(&handle, 0, 4).await.expect("cache seed");
        let data_err = client
            .read(&stale_data_handle, 0, 4)
            .await
            .expect_err("stale data handle must not reuse cached layout");
        let version_err = client
            .read(&stale_version, 0, 4)
            .await
            .expect_err("stale file version must not reuse cached layout");

        assert!(matches!(data_err, ClientError::StaleHandle { .. }));
        assert!(matches!(
            version_err,
            ClientError::VersionMismatch { expected: 4, actual: 3 }
        ));
        assert_eq!(method_count(&gateway.calls(), "read_layout"), 3);
    }

    #[tokio::test]
    async fn invalid_layout_is_not_inserted_into_cache() {
        let gateway = Arc::new(MockGateway::with_layouts(vec![
            layout_response(9, 101, 202, Some(3), 16, vec![location_with_stamp(202, 0, 0, 16, 0)]),
            layout_response(9, 101, 202, Some(3), 16, vec![location_with_stamp(202, 0, 0, 16, 88)]),
        ]));
        let worker = Arc::new(MockDataClient::from_file(b"abcdefghijklmnop"));
        let client = FsClient::with_data_boundary(
            test_config_with_layout_cache(9, Duration::from_secs(60), 16),
            gateway.clone(),
            data_boundary(worker.clone()),
        )
        .expect("client");
        let handle = read_handle(16);

        let err = client
            .read(&handle, 0, 4)
            .await
            .expect_err("zero block stamp layout must fail");
        assert!(matches!(err, ClientError::InvalidLayout(msg) if msg.contains("block_stamp")));
        client.read(&handle, 0, 4).await.expect("second layout succeeds");

        assert_eq!(method_count(&gateway.calls(), "read_layout"), 2);
        assert_eq!(worker.stamps(), vec![88]);
    }

    #[tokio::test]
    async fn block_stamp_mismatch_invalidates_cached_layout_before_retry() {
        let gateway = Arc::new(MockGateway::with_layouts(vec![
            layout_response(9, 101, 202, Some(3), 16, vec![location_with_stamp(202, 0, 0, 16, 11)]),
            layout_response(9, 101, 202, Some(3), 16, vec![location_with_stamp(202, 0, 0, 16, 22)]),
        ]));
        let worker = Arc::new(MockDataClient::with_refresh_once(
            b"abcdefghijklmnop",
            RefreshReason::BlockStampMismatch,
        ));
        let client = FsClient::with_data_boundary(
            test_config_with_layout_cache(9, Duration::from_secs(60), 16),
            gateway.clone(),
            data_boundary(worker.clone()),
        )
        .expect("client");
        let handle = read_handle(16);

        client
            .read(&handle, 0, 4)
            .await
            .expect("read succeeds after stamp refresh");

        assert_eq!(method_count(&gateway.calls(), "read_layout"), 2);
        assert_eq!(worker.stamps(), vec![11, 22]);
    }

    #[tokio::test]
    async fn concurrent_layout_cache_miss_same_key_coalesces_to_one_metadata_call() {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let gateway = Arc::new(MockGateway::with_layout_gate(
            layout_response(9, 101, 202, Some(3), 16, vec![location_with_stamp(202, 0, 0, 16, 77)]),
            Arc::clone(&started),
            Arc::clone(&release),
        ));
        let worker = Arc::new(MockDataClient::from_file(b"abcdefghijklmnop"));
        let client = FsClient::with_data_boundary(
            test_config_with_layout_cache(9, Duration::from_secs(60), 16),
            gateway.clone(),
            data_boundary(worker),
        )
        .expect("client");
        let handle = read_handle(16);

        let mut reads = Vec::with_capacity(8);
        for _ in 0..8 {
            let client = client.clone();
            let handle = handle.clone();
            reads.push(tokio::spawn(async move { client.read(&handle, 0, 4).await }));
        }
        started.notified().await;
        release.notify_waiters();

        for read in reads {
            let bytes = read.await.expect("read task").expect("coalesced read");
            assert_eq!(bytes, Bytes::from_static(b"abcd"));
        }
        assert_eq!(method_count(&gateway.calls(), "read_layout"), 1);
    }

    #[tokio::test]
    async fn concurrent_layout_cache_miss_different_keys_do_not_share_result() {
        let gateway = Arc::new(MockGateway::with_layouts(vec![
            layout_response(9, 101, 202, Some(3), 16, vec![location_with_stamp(202, 0, 0, 16, 77)]),
            layout_response(9, 101, 202, Some(3), 16, vec![location_with_stamp(202, 0, 0, 16, 88)]),
        ]));
        let worker = Arc::new(MockDataClient::from_file(b"abcdefghijklmnop"));
        let client = FsClient::with_data_boundary(
            test_config_with_layout_cache(9, Duration::from_secs(60), 16),
            gateway.clone(),
            data_boundary(worker),
        )
        .expect("client");
        let handle = read_handle(16);

        let first = {
            let client = client.clone();
            let handle = handle.clone();
            tokio::spawn(async move { client.read(&handle, 0, 4).await })
        };
        let second = {
            let client = client.clone();
            let handle = handle.clone();
            tokio::spawn(async move { client.read(&handle, 4, 4).await })
        };

        first.await.expect("first task").expect("first read");
        second.await.expect("second task").expect("second read");
        assert_eq!(method_count(&gateway.calls(), "read_layout"), 2);
    }

    #[tokio::test]
    async fn layout_singleflight_failure_wakes_all_waiters_without_cache_poisoning() {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let gateway = Arc::new(MockGateway::with_layout_failure_gate(
            Arc::clone(&started),
            Arc::clone(&release),
        ));
        let worker = Arc::new(MockDataClient::from_file(b"abcdefghijklmnop"));
        let client = FsClient::with_data_boundary(
            test_config_with_layout_cache(9, Duration::from_secs(60), 16),
            gateway.clone(),
            data_boundary(worker),
        )
        .expect("client");
        let handle = read_handle(16);

        let mut reads = Vec::with_capacity(4);
        for _ in 0..4 {
            let client = client.clone();
            let handle = handle.clone();
            reads.push(tokio::spawn(async move { client.read(&handle, 0, 4).await }));
        }
        started.notified().await;
        release.notify_waiters();

        for read in reads {
            let err = read.await.expect("read task").expect_err("layout failure");
            assert!(matches!(err, ClientError::Metadata(msg) if msg.contains("injected layout failure")));
        }
        assert_eq!(method_count(&gateway.calls(), "read_layout"), 1);
        assert_eq!(client.layout_cache.len(), 0);
    }

    #[tokio::test]
    async fn layout_cache_metrics_use_safe_low_cardinality_labels() {
        let metrics = Arc::new(RecordingMetrics::default());
        let gateway = Arc::new(MockGateway::with_layout(layout_response(
            9,
            101,
            202,
            Some(3),
            16,
            vec![location(202, 0, 0, 16)],
        )));
        let worker = Arc::new(MockDataClient::from_file(b"abcdefghijklmnop"));
        let client = client_with_metrics(
            test_config_with_layout_cache(9, Duration::from_secs(60), 16),
            gateway,
            data_boundary(worker),
            Arc::clone(&metrics),
        );
        let handle = read_handle(16);

        client.read(&handle, 0, 4).await.expect("first read");
        client.read(&handle, 0, 4).await.expect("second read");

        let events = metrics.events();
        assert_metric(&events, ClientMetric::LayoutCacheLookup);
        assert_metric(&events, ClientMetric::LayoutCacheMiss);
        assert_metric(&events, ClientMetric::LayoutCacheInsert);
        assert_metric(&events, ClientMetric::LayoutCacheHit);
        assert!(events.iter().all(|event| event.labels.has_only_safe_values()));
    }

    #[tokio::test]
    async fn public_read_returns_exact_multi_block_bytes() {
        let gateway = Arc::new(MockGateway::with_layout(layout_response(
            9,
            101,
            202,
            Some(3),
            16,
            vec![location(202, 0, 0, 8), location(202, 1, 8, 8)],
        )));
        let worker = Arc::new(MockDataClient::from_file(b"abcdefghijklmnop"));
        let client = FsClient::with_data_boundary(test_config(9), gateway, data_boundary(worker)).expect("client");
        let handle = read_handle(16);

        let bytes = client.read(&handle, 2, 12).await.expect("read succeeds");

        assert_eq!(bytes, Bytes::from_static(b"cdefghijklmn"));
    }

    #[tokio::test]
    async fn public_read_truncates_at_eof() {
        let gateway = Arc::new(MockGateway::with_layout(layout_response(
            9,
            101,
            202,
            Some(3),
            16,
            vec![location(202, 1, 8, 8)],
        )));
        let worker = Arc::new(MockDataClient::from_file(b"abcdefghijklmnop"));
        let client = FsClient::with_data_boundary(test_config(9), gateway, data_boundary(worker)).expect("client");
        let handle = read_handle(16);

        let bytes = client.read(&handle, 12, 16).await.expect("read succeeds");

        assert_eq!(bytes, Bytes::from_static(b"mnop"));
    }

    #[tokio::test]
    async fn public_read_returns_error_without_exposing_partial_bytes() {
        let gateway = Arc::new(MockGateway::with_layout(layout_response(
            9,
            101,
            202,
            Some(3),
            16,
            vec![location(202, 0, 0, 8), location(202, 1, 8, 8)],
        )));
        let worker = Arc::new(MockDataClient::with_failure_after_first(b"abcdefghijklmnop"));
        let client =
            FsClient::with_data_boundary(test_config(9), gateway, data_boundary(worker.clone())).expect("client");
        let handle = read_handle(16);

        let err = client
            .read(&handle, 0, 16)
            .await
            .expect_err("second worker segment fails");

        assert!(matches!(err, ClientError::Worker(msg) if msg.contains("injected read failure")));
        assert_eq!(worker.calls(), 2);
    }

    #[tokio::test]
    async fn pending_worker_read_times_out_without_returning_partial_bytes() {
        let gateway = Arc::new(MockGateway::with_layout(layout_response(
            9,
            101,
            202,
            Some(3),
            16,
            vec![location(202, 0, 0, 16)],
        )));
        let worker = Arc::new(MockDataClient::with_read_outcomes(vec![WorkerReadOutcome::Pending]));
        let metrics = Arc::new(RecordingMetrics::default());
        let client = client_with_metrics(
            test_config_with_timeout(9, 0, 10),
            gateway,
            data_boundary(worker.clone()),
            metrics.clone(),
        );
        let handle = read_handle(16);

        let result = tokio::time::timeout(Duration::from_millis(200), client.read(&handle, 0, 5))
            .await
            .expect("read must return before outer test timeout");
        let err = result.expect_err("pending worker read must time out");

        assert_eq!(ErrorClassifier.classify_error(&err), ErrorClass::RetryableTransport);
        assert_eq!(worker.calls(), 1);
        assert_metric(&metrics.events(), ClientMetric::RpcTimeout);
    }

    #[tokio::test]
    async fn public_read_rejects_data_handle_mismatch() {
        let gateway = Arc::new(MockGateway::with_layout(layout_response(
            9,
            101,
            303,
            Some(3),
            16,
            vec![location(303, 0, 0, 16)],
        )));
        let worker = Arc::new(MockDataClient::from_file(b"abcdefghijklmnop"));
        let client = FsClient::with_data_boundary(test_config(9), gateway, data_boundary(worker)).expect("client");
        let handle = read_handle(16);

        let err = client.read(&handle, 0, 1).await.expect_err("stale data handle");

        assert!(matches!(err, ClientError::StaleHandle { .. }));
    }

    #[tokio::test]
    async fn public_read_rejects_file_version_mismatch() {
        let gateway = Arc::new(MockGateway::with_layout(layout_response(
            9,
            101,
            202,
            Some(4),
            16,
            vec![location(202, 0, 0, 16)],
        )));
        let worker = Arc::new(MockDataClient::from_file(b"abcdefghijklmnop"));
        let client = FsClient::with_data_boundary(test_config(9), gateway, data_boundary(worker)).expect("client");
        let handle = read_handle(16);

        let err = client.read(&handle, 0, 1).await.expect_err("version mismatch");

        assert!(matches!(err, ClientError::VersionMismatch { expected: 3, actual: 4 }));
    }

    #[tokio::test]
    async fn public_read_replans_after_worker_route_refresh() {
        let gateway = Arc::new(MockGateway::with_layout(layout_response(
            9,
            101,
            202,
            Some(3),
            16,
            vec![location(202, 0, 0, 16)],
        )));
        let worker = Arc::new(MockDataClient::with_refresh_once(
            b"abcdefghijklmnop",
            RefreshReason::RouteEpochMismatch,
        ));
        let client =
            FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker)).expect("client");
        let handle = read_handle(16);

        let bytes = client.read(&handle, 0, 4).await.expect("read succeeds after refresh");

        assert_eq!(bytes, Bytes::from_static(b"abcd"));
        assert_eq!(
            gateway
                .calls()
                .iter()
                .filter(|call| call.method == "read_layout")
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn public_read_replans_after_worker_refresh() {
        let gateway = Arc::new(MockGateway::with_layout(layout_response(
            9,
            101,
            202,
            Some(3),
            16,
            vec![location(202, 0, 0, 16)],
        )));
        let worker = Arc::new(MockDataClient::with_refresh_once(
            b"abcdefghijklmnop",
            RefreshReason::WorkerEpochMismatch,
        ));
        let client =
            FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker)).expect("client");
        let handle = read_handle(16);

        let bytes = client.read(&handle, 1, 3).await.expect("read succeeds after refresh");

        assert_eq!(bytes, Bytes::from_static(b"bcd"));
        assert_eq!(
            gateway
                .calls()
                .iter()
                .filter(|call| call.method == "read_layout")
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn public_read_replans_after_stamp_mismatch() {
        let gateway = Arc::new(MockGateway::with_layouts(vec![
            layout_response(9, 101, 202, Some(3), 16, vec![location_with_stamp(202, 0, 0, 16, 11)]),
            layout_response(9, 101, 202, Some(3), 16, vec![location_with_stamp(202, 0, 0, 16, 22)]),
        ]));
        let worker = Arc::new(MockDataClient::with_refresh_once(
            b"abcdefghijklmnop",
            RefreshReason::BlockStampMismatch,
        ));
        let client = FsClient::with_data_boundary(test_config(9), gateway.clone(), data_boundary(worker.clone()))
            .expect("client");
        let handle = read_handle(16);

        let bytes = client
            .read(&handle, 0, 4)
            .await
            .expect("read succeeds after stamp refresh");

        assert_eq!(bytes, Bytes::from_static(b"abcd"));
        assert_eq!(
            gateway
                .calls()
                .iter()
                .filter(|call| call.method == "read_layout")
                .count(),
            2
        );
        assert_eq!(worker.stamps(), vec![11, 22]);
    }

    #[tokio::test]
    async fn read_refresh_reason_metrics_are_emitted() {
        let metrics = Arc::new(RecordingMetrics::default());
        let gateway = Arc::new(MockGateway::with_layouts(vec![
            layout_response(9, 101, 202, Some(3), 16, vec![location_with_stamp(202, 0, 0, 16, 11)]),
            layout_response(9, 101, 202, Some(3), 16, vec![location_with_stamp(202, 0, 0, 16, 22)]),
        ]));
        let worker = Arc::new(MockDataClient::with_refresh_once(
            b"abcdefghijklmnop",
            RefreshReason::BlockStampMismatch,
        ));
        let client = client_with_metrics(test_config(9), gateway, data_boundary(worker), Arc::clone(&metrics));
        let handle = read_handle(16);

        client.read(&handle, 0, 4).await.expect("read succeeds");

        let events = metrics.events();
        assert_metric(&events, ClientMetric::RefreshDecision);
        assert_metric(&events, ClientMetric::RefreshReason);
        assert!(
            events.iter().any(|event| {
                event.metric == ClientMetric::RefreshReason
                    && event.labels.refresh_reason == Some("block_stamp_mismatch")
            }),
            "missing block stamp refresh reason metric: {events:?}"
        );
    }

    fn event_log() -> EventLog {
        Arc::new(Mutex::new(Vec::new()))
    }

    fn method_count(calls: &[RecordedCall], method: &str) -> usize {
        calls.iter().filter(|call| call.method == method).count()
    }

    fn metric_count(events: &[ClientMetricEvent], metric: ClientMetric) -> usize {
        events.iter().filter(|event| event.metric == metric).count()
    }

    fn assert_metric(events: &[ClientMetricEvent], metric: ClientMetric) {
        assert!(
            events.iter().any(|event| event.metric == metric),
            "missing metric {metric:?}: {events:?}"
        );
    }

    fn assert_event_order(events: &EventLog, before: &'static str, after: &'static str) {
        let events = events.lock().expect("events");
        let before_index = events
            .iter()
            .position(|event| *event == before)
            .unwrap_or_else(|| panic!("missing event {before}: {events:?}"));
        let after_index = events
            .iter()
            .position(|event| *event == after)
            .unwrap_or_else(|| panic!("missing event {after}: {events:?}"));
        assert!(
            before_index < after_index,
            "{before} must happen before {after}: {events:?}"
        );
    }

    fn worker_block_signature(
        block: &WorkerWriteBlock,
    ) -> (u64, u64, String, i32, u64, u64, u64, u64, u32, u64, u64, u64) {
        (
            block.group_id,
            block.worker_id,
            block.endpoint.clone(),
            block.worker_net_protocol,
            block.worker_epoch,
            block.target.file_offset,
            block.target.len,
            block.target.block_stamp,
            block
                .target
                .block_id
                .as_ref()
                .map(|block_id| block_id.block_index)
                .unwrap_or_default(),
            block
                .target
                .block_id
                .as_ref()
                .map(|block_id| block_id.data_handle_id)
                .unwrap_or_default(),
            block.stream_id.high,
            block.stream_id.low,
        )
    }

    fn test_config(group_id: u64) -> ClientConfig {
        let mut config = ClientConfig {
            metadata_endpoints: vec!["http://127.0.0.1:18080".to_string()],
            metadata_group_ids: vec![group_id],
            ..ClientConfig::default()
        };
        config.inner.inner.set("client.id", 7i64);
        config
    }

    fn test_config_with_retries(group_id: u64, max_retries: usize) -> ClientConfig {
        let mut config = test_config(group_id);
        config.retry.max_retries = max_retries;
        config.retry.max_retry_attempts = max_retries;
        config.retry.metadata_retry_budget = max_retries;
        config.retry.worker_retry_budget = max_retries;
        config.refresh.max_refresh_attempts = max_retries;
        config
    }

    fn test_config_with_timeout(group_id: u64, max_retries: usize, timeout_ms: u64) -> ClientConfig {
        let mut config = test_config_with_retries(group_id, max_retries);
        config.retry.operation_timeout_ms = Some(timeout_ms);
        config
    }

    fn test_config_with_layout_cache(group_id: u64, ttl: Duration, max_entries: usize) -> ClientConfig {
        let mut config = test_config(group_id);
        config.cache.layout_cache_enabled = true;
        config.cache.layout_cache_ttl = ttl;
        config.cache.layout_cache_max_entries = max_entries;
        config
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct RecordedCall {
        method: &'static str,
        group_id: u64,
        call_id: String,
        operation_fingerprint: Option<crate::runtime::context::OperationFingerprint>,
        target_data_handle_id: Option<u64>,
        range: Option<(u64, u32)>,
        target_size: Option<u64>,
        final_size: Option<u64>,
        committed_block_offsets: Vec<u64>,
        committed_block_lens: Vec<u64>,
        create_disposition: Option<i32>,
    }

    #[derive(Clone, Debug)]
    struct RecordedMetadataAbort {
        call_id: String,
        operation_fingerprint: crate::runtime::context::OperationFingerprint,
        write_handle: Option<WriteHandleProto>,
    }

    #[derive(Clone, Debug)]
    struct RecordedWorkerAbort {
        call_id: String,
        operation_fingerprint: crate::runtime::context::OperationFingerprint,
        block: WorkerWriteBlock,
    }

    #[derive(Debug, Default)]
    struct MockGateway {
        calls: Mutex<Vec<RecordedCall>>,
        abort_file_records: Mutex<Vec<RecordedMetadataAbort>>,
        owner_redirect_group: Option<u64>,
        layouts: Mutex<VecDeque<GetBlockLocationsResponseProto>>,
        layout_gate: Option<(Arc<Notify>, Arc<Notify>)>,
        layout_failure_gate: Option<(Arc<Notify>, Arc<Notify>)>,
        next_offsets: Mutex<HashMap<u64, u64>>,
        next_block_indexes: Mutex<HashMap<u64, u32>>,
        create_outcomes: Mutex<VecDeque<CreateOutcome>>,
        append_outcomes: Mutex<VecDeque<AppendOutcome>>,
        add_block_outcomes: Mutex<VecDeque<AddBlockOutcome>>,
        commit_outcomes: Mutex<VecDeque<CommitOutcome>>,
        abort_outcomes: Mutex<VecDeque<AbortOutcome>>,
        renew_outcomes: Mutex<VecDeque<RenewOutcome>>,
        events: Option<EventLog>,
    }

    impl MockGateway {
        fn owner_redirect_then_open(group_id: u64) -> Self {
            Self {
                owner_redirect_group: Some(group_id),
                ..Self::default()
            }
        }

        fn calls(&self) -> Vec<RecordedCall> {
            self.calls.lock().expect("calls").clone()
        }

        fn abort_file_records(&self) -> Vec<RecordedMetadataAbort> {
            self.abort_file_records.lock().expect("abort file records").clone()
        }

        fn with_events(events: EventLog) -> Self {
            Self {
                events: Some(events),
                ..Self::default()
            }
        }

        fn with_layout(layout: GetBlockLocationsResponseProto) -> Self {
            let mut layouts = VecDeque::new();
            layouts.push_back(layout);
            Self {
                layouts: Mutex::new(layouts),
                ..Self::default()
            }
        }

        fn with_layouts(layouts: Vec<GetBlockLocationsResponseProto>) -> Self {
            Self {
                layouts: Mutex::new(layouts.into()),
                ..Self::default()
            }
        }

        fn with_layout_gate(
            layout: GetBlockLocationsResponseProto,
            started: Arc<Notify>,
            release: Arc<Notify>,
        ) -> Self {
            let mut layouts = VecDeque::new();
            layouts.push_back(layout);
            Self {
                layouts: Mutex::new(layouts),
                layout_gate: Some((started, release)),
                ..Self::default()
            }
        }

        fn with_layout_failure_gate(started: Arc<Notify>, release: Arc<Notify>) -> Self {
            Self {
                layout_failure_gate: Some((started, release)),
                ..Self::default()
            }
        }

        fn record(&self, method: &'static str, ctx: &AttemptContext) {
            let header = ctx.metadata_header().expect("metadata header");
            self.calls.lock().expect("calls").push(RecordedCall {
                method,
                group_id: header.group_id,
                call_id: header.client.as_ref().expect("client").call_id.clone(),
                target_data_handle_id: None,
                range: None,
                target_size: None,
                final_size: None,
                committed_block_offsets: Vec::new(),
                committed_block_lens: Vec::new(),
                operation_fingerprint: Some(ctx.operation_fingerprint()),
                create_disposition: None,
            });
        }

        fn record_read_layout(&self, ctx: &AttemptContext, req: &GetBlockLocationsOp) {
            let header = ctx.metadata_header().expect("metadata header");
            let target_data_handle_id = match req.target.as_ref() {
                Some(proto::metadata::get_block_locations_request_proto::Target::DataHandleId(id)) => Some(id.value),
                _ => None,
            };
            let range = req.range.as_ref().map(|range| (range.offset, range.len));
            self.calls.lock().expect("calls").push(RecordedCall {
                method: "read_layout",
                group_id: header.group_id,
                call_id: header.client.as_ref().expect("client").call_id.clone(),
                target_data_handle_id,
                range,
                target_size: None,
                final_size: None,
                committed_block_offsets: Vec::new(),
                committed_block_lens: Vec::new(),
                operation_fingerprint: Some(ctx.operation_fingerprint()),
                create_disposition: None,
            });
        }

        fn record_create_file(&self, ctx: &AttemptContext, req: &CreateFileOp) {
            let header = ctx.metadata_header().expect("metadata header");
            self.calls.lock().expect("calls").push(RecordedCall {
                method: "create_file",
                group_id: header.group_id,
                call_id: header.client.as_ref().expect("client").call_id.clone(),
                target_data_handle_id: None,
                range: None,
                target_size: None,
                final_size: None,
                committed_block_offsets: Vec::new(),
                committed_block_lens: Vec::new(),
                operation_fingerprint: Some(ctx.operation_fingerprint()),
                create_disposition: Some(req.disposition),
            });
        }

        fn record_commit_file(&self, ctx: &AttemptContext, req: &CommitFileOp) {
            self.record_event("commit_file");
            let header = ctx.metadata_header().expect("metadata header");
            self.calls.lock().expect("calls").push(RecordedCall {
                method: "commit_file",
                group_id: header.group_id,
                call_id: header.client.as_ref().expect("client").call_id.clone(),
                target_data_handle_id: req.data_handle_id.as_ref().map(|id| id.value),
                range: None,
                target_size: None,
                final_size: Some(req.final_size),
                committed_block_offsets: req.committed_blocks.iter().map(|block| block.file_offset).collect(),
                committed_block_lens: req.committed_blocks.iter().map(|block| block.len).collect(),
                operation_fingerprint: Some(ctx.operation_fingerprint()),
                create_disposition: None,
            });
        }

        fn with_create_outcomes(outcomes: Vec<CreateOutcome>) -> Self {
            Self {
                create_outcomes: Mutex::new(outcomes.into()),
                ..Self::default()
            }
        }

        fn with_append_outcomes(outcomes: Vec<AppendOutcome>) -> Self {
            Self {
                append_outcomes: Mutex::new(outcomes.into()),
                ..Self::default()
            }
        }

        fn with_commit_outcomes(outcomes: Vec<CommitOutcome>) -> Self {
            Self {
                commit_outcomes: Mutex::new(outcomes.into()),
                ..Self::default()
            }
        }

        fn with_add_block_outcomes(outcomes: Vec<AddBlockOutcome>) -> Self {
            Self {
                add_block_outcomes: Mutex::new(outcomes.into()),
                ..Self::default()
            }
        }

        fn with_abort_outcomes(outcomes: Vec<AbortOutcome>) -> Self {
            Self {
                abort_outcomes: Mutex::new(outcomes.into()),
                ..Self::default()
            }
        }

        fn with_renew_outcomes(outcomes: Vec<RenewOutcome>) -> Self {
            Self {
                renew_outcomes: Mutex::new(outcomes.into()),
                ..Self::default()
            }
        }

        fn next_add_block_outcome(&self) -> AddBlockOutcome {
            self.add_block_outcomes
                .lock()
                .expect("add block outcomes")
                .pop_front()
                .unwrap_or(AddBlockOutcome::Ok)
        }

        fn next_create_outcome(&self) -> CreateOutcome {
            self.create_outcomes
                .lock()
                .expect("create outcomes")
                .pop_front()
                .unwrap_or(CreateOutcome::Ok)
        }

        fn next_append_outcome(&self) -> AppendOutcome {
            self.append_outcomes
                .lock()
                .expect("append outcomes")
                .pop_front()
                .unwrap_or(AppendOutcome::Ok)
        }

        fn next_commit_outcome(&self) -> CommitOutcome {
            self.commit_outcomes
                .lock()
                .expect("commit outcomes")
                .pop_front()
                .unwrap_or(CommitOutcome::Ok)
        }

        fn next_abort_outcome(&self) -> AbortOutcome {
            self.abort_outcomes
                .lock()
                .expect("abort outcomes")
                .pop_front()
                .unwrap_or(AbortOutcome::Ok)
        }

        fn next_renew_outcome(&self) -> RenewOutcome {
            self.renew_outcomes
                .lock()
                .expect("renew outcomes")
                .pop_front()
                .unwrap_or(RenewOutcome::Ok)
        }

        fn record_event(&self, event: &'static str) {
            if let Some(events) = &self.events {
                events.lock().expect("events").push(event);
            }
        }

        fn maybe_owner_redirect(&self, ctx: &AttemptContext) -> ClientResult<()> {
            let Some(owner_group) = self.owner_redirect_group else {
                return Ok(());
            };
            if ctx.metadata_header().expect("metadata header").group_id == owner_group {
                return Ok(());
            }
            let canonical = CanonicalError::need_refresh_with_hint(
                RpcErrorCode::ShardMoved,
                RefreshReason::Moved,
                CanonicalRefreshHint {
                    group_id: Some(owner_group),
                    ..CanonicalRefreshHint::default()
                },
                "owner group moved",
            );
            Err(ClientError::from(ClientAction::Refresh {
                reason: RefreshReason::Moved,
                hint: Box::new(RefreshHint {
                    group_id: Some(owner_group),
                    ..RefreshHint::default()
                }),
                canonical: Box::new(canonical),
            }))
        }
    }

    #[async_trait]
    impl MetadataGateway for MockGateway {
        async fn get_status(&self, ctx: AttemptContext, _req: GetStatusOp) -> ClientResult<GetStatusResponseProto> {
            self.record("get_status", &ctx);
            Ok(GetStatusResponseProto {
                inode_id: Some(proto::fs::InodeIdProto { value: 101 }),
                attrs: Some(file_attrs_proto(10)),
                ..GetStatusResponseProto::default()
            })
        }

        async fn list_status(&self, ctx: AttemptContext, _req: ListStatusOp) -> ClientResult<ListStatusResponseProto> {
            self.record("list_status", &ctx);
            Ok(ListStatusResponseProto {
                entries: vec![proto::fs::DirEntryProto {
                    name: "child".to_string(),
                    inode_id: Some(proto::fs::InodeIdProto { value: 102 }),
                    kind: proto::fs::InodeKindProto::InodeKindFile as i32,
                    attrs: Some(file_attrs_proto(4)),
                }],
                eof: true,
                ..ListStatusResponseProto::default()
            })
        }

        async fn delete(&self, ctx: AttemptContext, _req: DeleteOp) -> ClientResult<DeleteResponseProto> {
            self.record("delete", &ctx);
            Ok(DeleteResponseProto::default())
        }

        async fn rename(&self, ctx: AttemptContext, _req: RenameOp) -> ClientResult<RenameResponseProto> {
            self.record("rename", &ctx);
            Ok(RenameResponseProto::default())
        }

        async fn open_file(&self, ctx: AttemptContext, _req: OpenFileOp) -> ClientResult<OpenFileResponseProto> {
            self.record("open_file", &ctx);
            self.maybe_owner_redirect(&ctx)?;
            Ok(OpenFileResponseProto {
                inode_id: Some(proto::fs::InodeIdProto { value: 101 }),
                data_handle_id: Some(proto::common::DataHandleIdProto { value: 202 }),
                file_size: 10,
                file_version: Some(3),
                ..OpenFileResponseProto::default()
            })
        }

        async fn read_layout(
            &self,
            ctx: AttemptContext,
            req: GetBlockLocationsOp,
        ) -> ClientResult<GetBlockLocationsResponseProto> {
            self.record_read_layout(&ctx, &req);
            self.maybe_owner_redirect(&ctx)?;
            if let Some((started, release)) = &self.layout_failure_gate {
                started.notify_waiters();
                release.notified().await;
                return Err(ClientError::Metadata("injected layout failure".to_string()));
            }
            if let Some((started, release)) = &self.layout_gate {
                started.notify_waiters();
                release.notified().await;
            }
            let mut layouts = self.layouts.lock().expect("layouts");
            Ok(if layouts.len() > 1 {
                layouts.pop_front().expect("layout queue reported a non-empty length")
            } else {
                layouts
                    .front()
                    .cloned()
                    .unwrap_or_else(|| layout_response(9, 101, 202, Some(3), 10, Vec::new()))
            })
        }

        async fn create_file(&self, ctx: AttemptContext, req: CreateFileOp) -> ClientResult<WriteSessionSeed> {
            self.record_create_file(&ctx, &req);
            match self.next_create_outcome() {
                CreateOutcome::Ok => {}
                CreateOutcome::TransportUnknown => {
                    return Err(ClientError::from(tonic::Status::unavailable(
                        "injected CreateFile transport uncertainty",
                    )));
                }
                CreateOutcome::InvalidArgument => {
                    return Err(ClientError::InvalidArgument(
                        "CreateFile rejected by metadata validation".to_string(),
                    ));
                }
            }
            self.next_offsets.lock().expect("offsets").insert(1, 0);
            Ok(WriteSessionSeed::Create(CreateFileResponseProto {
                write_handle: Some(write_handle_proto(1, 302)),
                inode_id: Some(proto::fs::InodeIdProto { value: 301 }),
                data_handle_id: Some(proto::common::DataHandleIdProto { value: 302 }),
                base_size: 0,
                ..CreateFileResponseProto::default()
            }))
        }

        async fn append_file(&self, ctx: AttemptContext, _req: AppendFileOp) -> ClientResult<WriteSessionSeed> {
            self.record("append_file", &ctx);
            match self.next_append_outcome() {
                AppendOutcome::Ok => {}
                AppendOutcome::TransportUnknown => {
                    return Err(ClientError::from(tonic::Status::unavailable(
                        "injected AppendFile transport uncertainty",
                    )));
                }
            }
            self.next_offsets.lock().expect("offsets").insert(2, 10);
            Ok(WriteSessionSeed::Append(AppendFileResponseProto {
                write_handle: Some(write_handle_proto(2, 402)),
                inode_id: Some(proto::fs::InodeIdProto { value: 401 }),
                data_handle_id: Some(proto::common::DataHandleIdProto { value: 402 }),
                base_size: 10,
                ..AppendFileResponseProto::default()
            }))
        }

        async fn add_block(&self, ctx: AttemptContext, req: AddBlockOp) -> ClientResult<AddBlockResult> {
            self.record("add_block", &ctx);
            let outcome = self.next_add_block_outcome();
            match outcome {
                AddBlockOutcome::Ok => {}
                AddBlockOutcome::TransportUnknown => {
                    return Err(ClientError::from(tonic::Status::unavailable(
                        "injected AddBlock transport uncertainty",
                    )));
                }
                AddBlockOutcome::InvalidHeader => return Err(invalid_header_error("AddBlock")),
                AddBlockOutcome::SessionExpired => return Err(session_error(RefreshReason::SessionExpired)),
                AddBlockOutcome::MismatchedTargetDataHandle => {}
            }
            let write_handle = req.write_handle.as_ref().expect("write handle");
            let len = req.desired_len.expect("desired len");
            let offset = {
                let mut offsets = self.next_offsets.lock().expect("offsets");
                let offset = *offsets.entry(write_handle.handle_id).or_insert(0);
                offsets.insert(write_handle.handle_id, offset + len);
                offset
            };
            let block_index = {
                let mut indexes = self.next_block_indexes.lock().expect("block indexes");
                let index = *indexes.entry(write_handle.handle_id).or_insert(0);
                indexes.insert(write_handle.handle_id, index + 1);
                index
            };
            let data_handle_id = write_handle
                .fencing_token
                .as_ref()
                .and_then(|token| token.block_id.as_ref())
                .map(|block| block.data_handle_id)
                .expect("fencing block");
            let target_data_handle_id = match outcome {
                AddBlockOutcome::MismatchedTargetDataHandle => data_handle_id + 1,
                AddBlockOutcome::Ok
                | AddBlockOutcome::TransportUnknown
                | AddBlockOutcome::InvalidHeader
                | AddBlockOutcome::SessionExpired => data_handle_id,
            };
            let header = ctx.metadata_header().expect("metadata header");
            Ok(AddBlockResult {
                group_id: header.group_id,
                target: write_target_proto(target_data_handle_id, block_index, offset, len),
            })
        }

        async fn commit_file(&self, ctx: AttemptContext, req: CommitFileOp) -> ClientResult<CommitFileResult> {
            self.record_commit_file(&ctx, &req);
            match self.next_commit_outcome() {
                CommitOutcome::Ok => Ok(CommitFileResponseProto {
                    committed_size: req.final_size,
                    file_version: Some(1),
                    ..CommitFileResponseProto::default()
                }),
                CommitOutcome::TransportUnknown => Err(ClientError::from(tonic::Status::unavailable(
                    "injected CommitFile transport uncertainty",
                ))),
                CommitOutcome::FingerprintMismatch => Err(ClientError::Unsupported(
                    "CommitFile replay denied: operation fingerprint changed".to_string(),
                )),
                CommitOutcome::SessionExpired => Err(session_error(RefreshReason::SessionExpired)),
            }
        }

        async fn abort_file_write(
            &self,
            ctx: AttemptContext,
            req: AbortFileWriteOp,
        ) -> ClientResult<AbortFileWriteResult> {
            self.record_event("abort_file_write");
            self.record("abort_file_write", &ctx);
            self.abort_file_records
                .lock()
                .expect("abort file records")
                .push(RecordedMetadataAbort {
                    call_id: ctx.call_id().to_string(),
                    operation_fingerprint: ctx.operation_fingerprint(),
                    write_handle: req.write_handle,
                });
            match self.next_abort_outcome() {
                AbortOutcome::Ok => Ok(AbortFileWriteResponseProto::default()),
                AbortOutcome::TransportUnknown => Err(ClientError::from(tonic::Status::unavailable(
                    "injected AbortFileWrite transport uncertainty",
                ))),
                AbortOutcome::Pending => std::future::pending::<ClientResult<AbortFileWriteResult>>().await,
            }
        }

        async fn renew_lease(&self, ctx: AttemptContext, _req: RenewLeaseOp) -> ClientResult<RenewLeaseResult> {
            self.record("renew_lease", &ctx);
            match self.next_renew_outcome() {
                RenewOutcome::Ok => Ok(RenewLeaseResponseProto {
                    expires_at_ms: u64::MAX / 2,
                    ..RenewLeaseResponseProto::default()
                }),
                RenewOutcome::TransportFailure => Err(ClientError::from(tonic::Status::unavailable(
                    "injected RenewLease transport failure",
                ))),
                RenewOutcome::SessionExpired => Err(session_error(RefreshReason::SessionExpired)),
            }
        }

        async fn msync(
            &self,
            ctx: AttemptContext,
            _req: MsyncOp,
        ) -> ClientResult<proto::common::GroupStateWatermarkProto> {
            self.record("msync", &ctx);
            Ok(proto::common::GroupStateWatermarkProto::default())
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum CreateOutcome {
        Ok,
        TransportUnknown,
        InvalidArgument,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum AppendOutcome {
        Ok,
        TransportUnknown,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum CommitOutcome {
        Ok,
        TransportUnknown,
        FingerprintMismatch,
        SessionExpired,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum AddBlockOutcome {
        Ok,
        TransportUnknown,
        InvalidHeader,
        SessionExpired,
        MismatchedTargetDataHandle,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum AbortOutcome {
        Ok,
        TransportUnknown,
        Pending,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum RenewOutcome {
        Ok,
        TransportFailure,
        SessionExpired,
    }

    #[derive(Debug)]
    struct MockDataClient {
        file: Bytes,
        read_outcomes: Mutex<VecDeque<WorkerReadOutcome>>,
        fail_after_first: bool,
        refresh_once: Option<RefreshReason>,
        calls: Mutex<usize>,
        stamps: Mutex<Vec<u64>>,
        written: Mutex<Vec<u8>>,
        committed: Mutex<Vec<u64>>,
        committed_seqs: Mutex<Vec<u64>>,
        commit_sync_flags: Mutex<Vec<bool>>,
        open_outcomes: Mutex<VecDeque<WorkerOpenOutcome>>,
        write_outcomes: Mutex<VecDeque<WorkerWriteOutcome>>,
        commit_write_outcomes: Mutex<VecDeque<WorkerCommitOutcome>>,
        abort_outcomes: Mutex<VecDeque<WorkerAbortOutcome>>,
        abort_records: Mutex<Vec<RecordedWorkerAbort>>,
        events: Option<EventLog>,
    }

    impl MockDataClient {
        fn from_file(file: &'static [u8]) -> Self {
            Self {
                file: Bytes::from_static(file),
                read_outcomes: Mutex::new(VecDeque::new()),
                fail_after_first: false,
                refresh_once: None,
                calls: Mutex::new(0),
                stamps: Mutex::new(Vec::new()),
                written: Mutex::new(Vec::new()),
                committed: Mutex::new(Vec::new()),
                committed_seqs: Mutex::new(Vec::new()),
                commit_sync_flags: Mutex::new(Vec::new()),
                open_outcomes: Mutex::new(VecDeque::new()),
                write_outcomes: Mutex::new(VecDeque::new()),
                commit_write_outcomes: Mutex::new(VecDeque::new()),
                abort_outcomes: Mutex::new(VecDeque::new()),
                abort_records: Mutex::new(Vec::new()),
                events: None,
            }
        }

        fn with_events(events: EventLog) -> Self {
            Self {
                events: Some(events),
                ..Self::default()
            }
        }

        fn with_failure_after_first(file: &'static [u8]) -> Self {
            Self {
                fail_after_first: true,
                ..Self::from_file(file)
            }
        }

        fn with_refresh_once(file: &'static [u8], reason: RefreshReason) -> Self {
            Self {
                refresh_once: Some(reason),
                ..Self::from_file(file)
            }
        }

        fn with_read_outcomes(outcomes: Vec<WorkerReadOutcome>) -> Self {
            Self {
                read_outcomes: Mutex::new(outcomes.into()),
                ..Self::default()
            }
        }

        fn with_open_outcomes(outcomes: Vec<WorkerOpenOutcome>) -> Self {
            Self {
                open_outcomes: Mutex::new(outcomes.into()),
                ..Self::default()
            }
        }

        fn with_write_outcomes(outcomes: Vec<WorkerWriteOutcome>) -> Self {
            Self {
                write_outcomes: Mutex::new(outcomes.into()),
                ..Self::default()
            }
        }

        fn with_commit_write_outcomes(outcomes: Vec<WorkerCommitOutcome>) -> Self {
            Self {
                commit_write_outcomes: Mutex::new(outcomes.into()),
                ..Self::default()
            }
        }

        fn with_abort_outcomes(outcomes: Vec<WorkerAbortOutcome>) -> Self {
            Self {
                abort_outcomes: Mutex::new(outcomes.into()),
                ..Self::default()
            }
        }

        fn next_open_outcome(&self) -> WorkerOpenOutcome {
            self.open_outcomes
                .lock()
                .expect("open outcomes")
                .pop_front()
                .unwrap_or(WorkerOpenOutcome::Ok)
        }

        fn next_read_outcome(&self) -> WorkerReadOutcome {
            self.read_outcomes
                .lock()
                .expect("read outcomes")
                .pop_front()
                .unwrap_or(WorkerReadOutcome::Ok)
        }

        fn next_write_outcome(&self) -> WorkerWriteOutcome {
            self.write_outcomes
                .lock()
                .expect("write outcomes")
                .pop_front()
                .unwrap_or(WorkerWriteOutcome::Ok)
        }

        fn next_commit_write_outcome(&self) -> WorkerCommitOutcome {
            self.commit_write_outcomes
                .lock()
                .expect("commit write outcomes")
                .pop_front()
                .unwrap_or(WorkerCommitOutcome::Ok)
        }

        fn next_abort_outcome(&self) -> WorkerAbortOutcome {
            self.abort_outcomes
                .lock()
                .expect("abort outcomes")
                .pop_front()
                .unwrap_or(WorkerAbortOutcome::Ok)
        }

        fn calls(&self) -> usize {
            *self.calls.lock().expect("calls")
        }

        fn stamps(&self) -> Vec<u64> {
            self.stamps.lock().expect("stamps").clone()
        }

        fn written_bytes(&self) -> Bytes {
            Bytes::from(self.written.lock().expect("written").clone())
        }

        fn committed_seqs(&self) -> Vec<u64> {
            self.committed_seqs.lock().expect("committed seqs").clone()
        }

        fn committed_lens(&self) -> Vec<u64> {
            self.committed.lock().expect("committed").clone()
        }

        fn commit_sync_flags(&self) -> Vec<bool> {
            self.commit_sync_flags.lock().expect("commit sync flags").clone()
        }

        fn abort_records(&self) -> Vec<RecordedWorkerAbort> {
            self.abort_records.lock().expect("abort records").clone()
        }

        fn record_event(&self, event: &'static str) {
            if let Some(events) = &self.events {
                events.lock().expect("events").push(event);
            }
        }
    }

    impl Default for MockDataClient {
        fn default() -> Self {
            Self::from_file(b"")
        }
    }

    #[async_trait]
    impl WorkerDataClient for MockDataClient {
        async fn read_segment(
            &self,
            _ctx: AttemptContext,
            _group_id: u64,
            segment: &PlannedReadSegment,
        ) -> ClientResult<Bytes> {
            let call_number = {
                let mut calls = self.calls.lock().expect("calls");
                *calls += 1;
                *calls
            };
            self.stamps.lock().expect("stamps").push(segment.block_stamp);
            if self.fail_after_first && call_number > 1 {
                return Err(ClientError::Worker("injected read failure".to_string()));
            }
            if call_number == 1 {
                if let Some(reason) = self.refresh_once {
                    return Err(refresh_action_error(reason));
                }
            }
            match self.next_read_outcome() {
                WorkerReadOutcome::Ok => {}
                WorkerReadOutcome::Pending => std::future::pending::<()>().await,
            }
            let start = segment.file_offset as usize;
            let end = start + segment.len as usize;
            Ok(self.file.slice(start..end))
        }

        async fn open_write(&self, _ctx: AttemptContext, target: WorkerWriteTarget) -> ClientResult<WorkerWriteBlock> {
            self.record_event("open_write");
            let call_number = {
                let mut calls = self.calls.lock().expect("calls");
                *calls += 1;
                *calls
            };
            match self.next_open_outcome() {
                WorkerOpenOutcome::Ok => {}
                WorkerOpenOutcome::Unknown => {
                    return Err(ClientError::UnknownOutcome(
                        "OpenWriteStream outcome is unknown".to_string(),
                    ));
                }
                WorkerOpenOutcome::InvalidHeader => return Err(invalid_header_error("OpenWriteStream")),
                WorkerOpenOutcome::FatalFencing => return Err(fatal_fencing_error()),
                WorkerOpenOutcome::Pending => std::future::pending::<()>().await,
            }
            Ok(WorkerWriteBlock {
                group_id: target.group_id,
                worker_id: 1,
                endpoint: "127.0.0.1:19101".to_string(),
                worker_net_protocol: WorkerNetProtocolProto::WorkerNetProtocolGrpc as i32,
                worker_epoch: 7,
                target: target.target,
                stream_id: proto::common::StreamIdProto {
                    high: 1,
                    low: call_number as u64,
                },
                frame_size: 1024,
                next_seq: 1,
            })
        }

        async fn write_stream(
            &self,
            block: &WorkerWriteBlock,
            data: Bytes,
        ) -> ClientResult<proto::worker::WriteStreamResponseProto> {
            self.record_event("write_stream");
            self.written.lock().expect("written").extend_from_slice(&data);
            let frame_size = block.frame_size.max(1) as usize;
            let frame_count = data.len().div_ceil(frame_size) as u64;
            let expected_last_seq = block.next_seq + frame_count.saturating_sub(1);
            match self.next_write_outcome() {
                WorkerWriteOutcome::Ok => Ok(proto::worker::WriteStreamResponseProto {
                    accepted: true,
                    last_acked_seq: expected_last_seq,
                    written_through: data.len() as u64,
                }),
                WorkerWriteOutcome::PartialAck => Ok(proto::worker::WriteStreamResponseProto {
                    accepted: true,
                    last_acked_seq: expected_last_seq.saturating_sub(1),
                    written_through: data.len().saturating_sub(1) as u64,
                }),
                WorkerWriteOutcome::InvalidHeader => Err(invalid_header_error("WriteStream")),
            }
        }

        async fn commit_write(
            &self,
            _ctx: AttemptContext,
            block: &WorkerWriteBlock,
            effective_len: u64,
            commit_seq: u64,
            require_sync: bool,
        ) -> ClientResult<WorkerCommitResult> {
            self.record_event("commit_write");
            let outcome = self.next_commit_write_outcome();
            match outcome {
                WorkerCommitOutcome::Ok => {}
                WorkerCommitOutcome::Unknown => {
                    return Err(ClientError::UnknownOutcome(
                        "CommitWrite outcome is unknown".to_string(),
                    ));
                }
                WorkerCommitOutcome::InvalidHeader => return Err(invalid_header_error("CommitWrite")),
                WorkerCommitOutcome::Fencing => return Err(refresh_action_error(RefreshReason::Fencing)),
                WorkerCommitOutcome::FatalFencing => return Err(fatal_fencing_error()),
                WorkerCommitOutcome::WorkerEpochMismatch => {
                    return Err(refresh_action_error(RefreshReason::WorkerEpochMismatch));
                }
                WorkerCommitOutcome::LengthMismatch
                | WorkerCommitOutcome::WrittenThroughMismatch
                | WorkerCommitOutcome::BlockStampMismatch => {}
            }
            self.committed.lock().expect("committed").push(effective_len);
            self.committed_seqs.lock().expect("committed seqs").push(commit_seq);
            self.commit_sync_flags
                .lock()
                .expect("commit sync flags")
                .push(require_sync);
            let response_effective_len = match outcome {
                WorkerCommitOutcome::LengthMismatch => effective_len.saturating_sub(1),
                _ => effective_len,
            };
            Ok(WorkerCommitResult {
                effective_block_len: response_effective_len,
                block_stamp: match outcome {
                    WorkerCommitOutcome::BlockStampMismatch => block.target.block_stamp + 1,
                    _ => block.target.block_stamp,
                },
                written_through: match outcome {
                    WorkerCommitOutcome::WrittenThroughMismatch => effective_len.saturating_sub(1),
                    _ => effective_len,
                },
            })
        }

        async fn abort_write(&self, ctx: AttemptContext, block: &WorkerWriteBlock) -> ClientResult<()> {
            self.record_event("abort_write");
            self.abort_records
                .lock()
                .expect("abort records")
                .push(RecordedWorkerAbort {
                    call_id: ctx.call_id().to_string(),
                    operation_fingerprint: ctx.operation_fingerprint(),
                    block: block.clone(),
                });
            match self.next_abort_outcome() {
                WorkerAbortOutcome::Ok => Ok(()),
                WorkerAbortOutcome::Unknown => {
                    Err(ClientError::UnknownOutcome("AbortWrite outcome is unknown".to_string()))
                }
            }
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum WorkerReadOutcome {
        Ok,
        Pending,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum WorkerOpenOutcome {
        Ok,
        Unknown,
        InvalidHeader,
        FatalFencing,
        Pending,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum WorkerWriteOutcome {
        Ok,
        PartialAck,
        InvalidHeader,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum WorkerCommitOutcome {
        Ok,
        Unknown,
        InvalidHeader,
        Fencing,
        FatalFencing,
        WorkerEpochMismatch,
        LengthMismatch,
        WrittenThroughMismatch,
        BlockStampMismatch,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum WorkerAbortOutcome {
        Ok,
        Unknown,
    }

    fn data_boundary(client: Arc<MockDataClient>) -> DataPlaneBoundary {
        DataPlaneBoundary::with_client(client)
    }

    fn read_handle(file_size: u64) -> FileHandle {
        FileHandle::read(
            "/alpha".to_string(),
            InodeId::new(101),
            DataHandleId::new(202),
            3,
            file_size,
        )
    }

    fn layout_response(
        group_id: u64,
        inode_id: u64,
        data_handle_id: u64,
        file_version: Option<u64>,
        file_size: u64,
        locations: Vec<FileBlockLocationProto>,
    ) -> GetBlockLocationsResponseProto {
        GetBlockLocationsResponseProto {
            header: Some(proto::common::ResponseHeaderProto {
                client: Some(proto::common::ClientInfoProto {
                    call_id: types::CallId::new().to_string(),
                    client_id: 7,
                    client_name: String::new(),
                }),
                group_id,
                ..proto::common::ResponseHeaderProto::default()
            }),
            inode_id: Some(proto::fs::InodeIdProto { value: inode_id }),
            data_handle_id: Some(proto::common::DataHandleIdProto { value: data_handle_id }),
            file_size,
            locations,
            file_version,
        }
    }

    fn file_attrs_proto(size: u64) -> proto::fs::FileAttrsProto {
        proto::fs::FileAttrsProto {
            mode: 0o100644,
            uid: 1000,
            gid: 1000,
            size,
            atime_ms: 11,
            mtime_ms: 12,
            ctime_ms: 13,
            nlink: 1,
        }
    }

    fn write_handle_proto(handle_id: u64, data_handle_id: u64) -> WriteHandleProto {
        WriteHandleProto {
            handle_id,
            lease_id: Some(proto::common::LeaseIdProto {
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

    fn write_target_proto(data_handle_id: u64, block_index: u32, file_offset: u64, len: u64) -> WriteTargetProto {
        WriteTargetProto {
            block_id: Some(BlockIdProto {
                data_handle_id,
                block_index,
            }),
            file_offset,
            len,
            worker_endpoints: vec![WorkerEndpointInfoProto {
                worker_id: 1,
                endpoint: "127.0.0.1:19101".to_string(),
                worker_net_protocol: WorkerNetProtocolProto::WorkerNetProtocolGrpc as i32,
                worker_epoch: 7,
            }],
            fencing_token: Some(FencingTokenProto {
                block_id: Some(BlockIdProto {
                    data_handle_id,
                    block_index,
                }),
                owner: 7,
                epoch: 1,
            }),
            block_stamp: 1,
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }

    fn location(data_handle_id: u64, block_index: u32, file_offset: u64, len: u64) -> FileBlockLocationProto {
        location_with_stamp(
            data_handle_id,
            block_index,
            file_offset,
            len,
            u64::from(block_index) + 1,
        )
    }

    fn location_with_stamp(
        data_handle_id: u64,
        block_index: u32,
        file_offset: u64,
        len: u64,
        stamp: u64,
    ) -> FileBlockLocationProto {
        FileBlockLocationProto {
            block_id: Some(BlockIdProto {
                data_handle_id,
                block_index,
            }),
            file_offset,
            len,
            workers: vec![WorkerEndpointInfoProto {
                worker_id: 1,
                endpoint: "127.0.0.1:19101".to_string(),
                worker_net_protocol: WorkerNetProtocolProto::WorkerNetProtocolGrpc as i32,
                worker_epoch: 7,
            }],
            worker_epoch: Some(7),
            block_stamp: Some(stamp),
        }
    }

    fn refresh_action_error(reason: RefreshReason) -> ClientError {
        let code = match reason {
            RefreshReason::RouteEpochMismatch => RpcErrorCode::RouteEpochMismatch,
            RefreshReason::WorkerEpochMismatch => RpcErrorCode::WorkerEpochMismatch,
            RefreshReason::BlockStampMismatch => RpcErrorCode::BlockStampMismatch,
            RefreshReason::Fencing => RpcErrorCode::Fencing,
            RefreshReason::SessionExpired => RpcErrorCode::Application,
            other => panic!("unsupported test refresh reason {other:?}"),
        };
        let canonical = CanonicalError::need_refresh_with_hint(
            code,
            reason,
            CanonicalRefreshHint {
                route_epoch: Some(55),
                worker_epoch: Some(66),
                worker_resolve_required: reason == RefreshReason::WorkerEpochMismatch,
                ..CanonicalRefreshHint::default()
            },
            "worker requested refresh",
        );
        ClientError::from(ClientAction::Refresh {
            reason,
            hint: Box::new(RefreshHint {
                route_epoch: Some(55),
                worker_epoch: Some(66),
                worker_resolve_required: reason == RefreshReason::WorkerEpochMismatch,
                ..RefreshHint::default()
            }),
            canonical: Box::new(canonical),
        })
    }

    fn invalid_header_error(operation: &'static str) -> ClientError {
        ClientError::from(ClientAction::Fail {
            canonical: Box::new(CanonicalError {
                class: common::error::canonical::ErrorClass::Fatal,
                code: Some(common::error::canonical::ErrorCode::RpcCode(
                    RpcErrorCode::InvalidHeader,
                )),
                reason: None,
                retry_after_ms: None,
                message: format!("{operation} OK response header is malformed"),
                refresh_hint: None,
            }),
        })
    }

    fn fatal_fencing_error() -> ClientError {
        ClientError::from(ClientAction::Fail {
            canonical: Box::new(CanonicalError {
                class: common::error::canonical::ErrorClass::Fatal,
                code: Some(common::error::canonical::ErrorCode::RpcCode(RpcErrorCode::Fencing)),
                reason: None,
                retry_after_ms: None,
                message: "fencing mismatch".to_string(),
                refresh_hint: None,
            }),
        })
    }

    fn session_error(reason: RefreshReason) -> ClientError {
        let canonical = CanonicalError::need_refresh(RpcErrorCode::Application, reason, "write session expired");
        ClientError::from(ClientAction::Refresh {
            reason,
            hint: Box::default(),
            canonical: Box::new(canonical),
        })
    }
}
