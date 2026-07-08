// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Central operation executor types.

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use crate::api::handle::{ReadHandle, WriteHandle};
use crate::api::options::{DEFAULT_BLOCK_SIZE, DEFAULT_REPLICATION, MAX_PREALLOCATED_WRITE_BLOCKS};
use crate::api::path::NamespacePathBuf;
use crate::api::{CreateMode, CreateOptions, DirectoryEntry, DirectoryListing, FileStatus, ListOptions};
use crate::config::{ClientConfig, RefreshConfig, RetryConfig};
use crate::error::{ClientError, ClientResult};
use crate::metadata::{AddBlockResult, MetadataGateway, ReadLayout};
use crate::metrics::{ClientMetric, ClientMetricEvent, ClientMetricLabels, ClientMetrics};
use crate::rpc_error::ClientAction;
use crate::runtime::classify::{ErrorClass, ErrorClassifier, MetadataRefreshCause};
pub(crate) use crate::runtime::context::{AttemptContext, OperationContext, OperationIdentity};
use crate::runtime::policy::{OperationKind, ReplayPolicyTable};
use crate::runtime::refresh::MetadataTargets;
use crate::runtime::ClientIdentity;
use crate::runtime::{BackoffPolicy, BackoffSleeper};
use crate::session::write_session::{CommitFilePlan, WriteSession};
use types::{CommittedBlock, DataHandleId, FileLayout};

/// Executes public operations through policy, classification, refresh, and replay gates.
#[derive(Clone)]
pub(crate) struct OperationExecutor {
    identity: ClientIdentity,
    gateway: Arc<dyn MetadataGateway>,
    metadata_targets: MetadataTargets,
    replay_policy: ReplayPolicyTable,
    classifier: ErrorClassifier,
    retry: RetryConfig,
    refresh: RefreshConfig,
    backoff: BackoffPolicy,
    sleeper: Arc<dyn BackoffSleeper>,
    metrics: Arc<dyn ClientMetrics>,
}

impl OperationExecutor {
    /// Create a metadata executor with explicit runtime policy and hooks.
    pub(crate) fn new(
        identity: ClientIdentity,
        gateway: Arc<dyn MetadataGateway>,
        metadata_targets: MetadataTargets,
        config: &ClientConfig,
        sleeper: Arc<dyn BackoffSleeper>,
        metrics: Arc<dyn ClientMetrics>,
    ) -> ClientResult<Self> {
        Ok(Self {
            identity,
            gateway,
            metadata_targets,
            replay_policy: ReplayPolicyTable::new(),
            classifier: ErrorClassifier,
            retry: config.retry.clone(),
            refresh: config.refresh.clone(),
            backoff: BackoffPolicy::from_config(&config.backoff),
            sleeper,
            metrics,
        })
    }

    /// Execute GetStatus and return a public status snapshot.
    pub(crate) async fn stat(&self, path: NamespacePathBuf) -> ClientResult<FileStatus> {
        let path = path.as_str().to_string();
        let response = self
            .get_status_request(
                &path,
                proto::metadata::GetStatusRequestProto {
                    header: None,
                    path: path.clone(),
                },
            )
            .await?;
        file_status_from_response(path, response)
    }

    /// Execute ListStatus and return a public directory listing.
    pub(crate) async fn list(&self, path: NamespacePathBuf, options: ListOptions) -> ClientResult<DirectoryListing> {
        let path = path.into_string();
        let response = self
            .list_status_request(
                &path,
                proto::metadata::ListStatusRequestProto {
                    header: None,
                    path: path.clone(),
                    recursive: options.recursive,
                    cursor: options.cursor.unwrap_or_default(),
                    limit: options.limit.unwrap_or(0),
                },
            )
            .await?;
        Ok(directory_listing_from_response(path, response))
    }

    /// Execute CreateDirectory and return a public status snapshot.
    pub(crate) async fn create_directory(&self, path: NamespacePathBuf, recursive: bool) -> ClientResult<FileStatus> {
        let path = path.into_string();
        let response = self
            .create_directory_request(
                &path,
                proto::metadata::CreateDirectoryRequestProto {
                    header: None,
                    path: path.clone(),
                    attrs: Some(default_dir_attrs()),
                    recursive,
                },
            )
            .await?;
        directory_status_from_response(path, response)
    }

    /// Execute Delete.
    pub(crate) async fn delete(&self, path: NamespacePathBuf, recursive: bool) -> ClientResult<()> {
        let path = path.into_string();
        self.delete_request(
            &path,
            proto::metadata::DeleteRequestProto {
                header: None,
                path: path.clone(),
                recursive,
            },
        )
        .await
        .map(|_| ())
    }

    /// Execute Rename.
    pub(crate) async fn rename(&self, src: NamespacePathBuf, dst: NamespacePathBuf) -> ClientResult<()> {
        let src_text = src.into_string();
        let dst_text = dst.into_string();
        self.rename_request(
            &src_text,
            &dst_text,
            proto::metadata::RenameRequestProto {
                header: None,
                src_path: src_text.clone(),
                dst_path: dst_text.clone(),
                flags: 0,
            },
        )
        .await
        .map(|_| ())
    }

    /// Execute OpenFile and return a read handle.
    pub(crate) async fn open_file(&self, path: NamespacePathBuf) -> ClientResult<ReadHandle> {
        let path = path.into_string();
        let operation = OperationContext::new_with_identity(
            &self.identity,
            OperationKind::MetadataRead,
            "OpenFile",
            OperationIdentity::path(path.clone()),
        )?;
        let request = proto::metadata::OpenFileRequestProto {
            header: None,
            path: path.clone(),
        };
        let response = self
            .execute_metadata(operation, request, |gateway, ctx, req| async move {
                gateway.open_file(ctx, req).await
            })
            .await?;

        let Some(data_handle_id) = response.data_handle_id else {
            return Err(ClientError::Metadata(
                "OpenFileResponseProto.data_handle_id missing".to_string(),
            ));
        };
        let Some(file_version) = response.file_version else {
            return Err(ClientError::Metadata(
                "OpenFileResponseProto.file_version missing".to_string(),
            ));
        };

        Ok(ReadHandle::new(
            path,
            DataHandleId::new(data_handle_id.value),
            file_version,
            response.file_size,
        ))
    }

    /// Execute a metadata layout read.
    pub(crate) async fn read_layout(
        &self,
        path: &str,
        req: proto::metadata::GetBlockLocationsRequestProto,
    ) -> ClientResult<ReadLayout> {
        let operation = OperationContext::new_with_identity(
            &self.identity,
            OperationKind::MetadataRead,
            "GetBlockLocations",
            OperationIdentity::path(path),
        )?;
        self.execute_metadata(operation, req, |gateway, ctx, req| async move {
            gateway.read_layout(ctx, req).await
        })
        .await
    }

    /// Execute a metadata layout read for a data handle.
    pub(crate) async fn read_layout_for_data_handle(
        &self,
        path: &str,
        data_handle_id: DataHandleId,
        offset: u64,
        len: u32,
    ) -> ClientResult<ReadLayout> {
        self.read_layout(
            path,
            proto::metadata::GetBlockLocationsRequestProto {
                header: None,
                target: Some(
                    proto::metadata::get_block_locations_request_proto::Target::DataHandleId(
                        proto::common::DataHandleIdProto {
                            value: data_handle_id.as_raw(),
                        },
                    ),
                ),
                range: Some(proto::common::ByteRangeProto { offset, len }),
            },
        )
        .await
    }

    /// Return the generated client id.
    pub(crate) fn client_id(&self) -> types::ClientId {
        self.identity.client_id()
    }

    pub(crate) fn client_name(&self) -> &str {
        self.identity.client_name()
    }

    /// Record a worker data-path refresh and run owned cache invalidation.
    pub(crate) fn record_data_refresh(
        &self,
        operation: &OperationContext,
        reason: MetadataRefreshCause,
        hint: &crate::rpc_error::RefreshHint,
    ) -> ClientResult<()> {
        self.metadata_targets.record_refresh(operation, reason, hint)
    }

    /// Execute GetStatus.
    async fn get_status_request(
        &self,
        path: &str,
        req: proto::metadata::GetStatusRequestProto,
    ) -> ClientResult<proto::metadata::GetStatusResponseProto> {
        let operation = OperationContext::new_with_identity(
            &self.identity,
            OperationKind::MetadataRead,
            "GetStatus",
            OperationIdentity::path(path),
        )?;
        self.execute_metadata(operation, req, |gateway, ctx, req| async move {
            gateway.get_status(ctx, req).await
        })
        .await
    }

    /// Execute ListStatus.
    async fn list_status_request(
        &self,
        path: &str,
        req: proto::metadata::ListStatusRequestProto,
    ) -> ClientResult<proto::metadata::ListStatusResponseProto> {
        let operation = OperationContext::new_with_identity(
            &self.identity,
            OperationKind::MetadataRead,
            "ListStatus",
            OperationIdentity::path(path),
        )?;
        self.execute_metadata(operation, req, |gateway, ctx, req| async move {
            gateway.list_status(ctx, req).await
        })
        .await
    }

    /// Execute CreateDirectory.
    async fn create_directory_request(
        &self,
        path: &str,
        req: proto::metadata::CreateDirectoryRequestProto,
    ) -> ClientResult<proto::metadata::CreateDirectoryResponseProto> {
        let operation = OperationContext::new_with_identity(
            &self.identity,
            OperationKind::MetadataMutation,
            "CreateDirectory",
            OperationIdentity::path(path).with_detail(format!("recursive={}", req.recursive)),
        )?;
        self.execute_metadata(operation, req, |gateway, ctx, req| async move {
            gateway.create_directory(ctx, req).await
        })
        .await
    }

    /// Execute CreateFile and return a write handle.
    pub(crate) async fn create_file(
        &self,
        path: NamespacePathBuf,
        options: CreateOptions,
    ) -> ClientResult<WriteHandle> {
        let path = path.into_string();
        let create_mode = match options.create_mode {
            CreateMode::CreateNew => proto::metadata::CreateModeProto::CreateNew,
            CreateMode::CreateOrOverwrite => proto::metadata::CreateModeProto::CreateOrOverwrite,
        };
        let response = self
            .create_file_request(
                &path,
                proto::metadata::CreateFileRequestProto {
                    header: None,
                    path: path.clone(),
                    attrs: Some(default_file_attrs()),
                    layout: Some(layout_for_new_file(&options)),
                    create_mode: create_mode as i32,
                    desired_len: Some(default_write_preallocation_len()),
                },
            )
            .await?;
        let Some(data_handle_id) = response.data_handle_id else {
            return Err(ClientError::Metadata(
                "CreateFileResponseProto.data_handle_id missing".to_string(),
            ));
        };
        let Some(layout) = response.layout else {
            return Err(ClientError::Metadata(
                "CreateFileResponseProto.layout missing".to_string(),
            ));
        };
        let layout = FileLayout::try_from(layout)
            .map_err(|err| ClientError::InvalidLayout(format!("CreateFileResponseProto.layout invalid: {err}")))?;
        let Some(write_handle) = response.write_handle else {
            return Err(ClientError::Metadata(
                "CreateFileResponseProto.write_handle missing".to_string(),
            ));
        };

        let expires_at_ms = crate::api::handle::valid_write_session_expiry("CreateFile", response.expires_at_ms)?;
        let data_handle_id = DataHandleId::new(data_handle_id.value);
        let session = WriteSession::new(
            path.clone(),
            data_handle_id,
            layout,
            write_handle,
            response.base_size,
            expires_at_ms,
        )?;
        Ok(WriteHandle::new(path, data_handle_id, response.base_size, session))
    }

    async fn create_file_request(
        &self,
        path: &str,
        req: proto::metadata::CreateFileRequestProto,
    ) -> ClientResult<proto::metadata::CreateFileResponseProto> {
        let detail = format!("create_mode={}", req.create_mode);
        let operation = OperationContext::new_with_identity(
            &self.identity,
            OperationKind::MetadataMutation,
            "CreateFile",
            OperationIdentity::path(path).with_detail(detail),
        )?;
        self.execute_metadata(operation, req, |gateway, ctx, req| async move {
            gateway.create_file(ctx, req).await
        })
        .await
    }

    /// Execute AppendFile and return a write handle.
    pub(crate) async fn append_file(&self, path: NamespacePathBuf) -> ClientResult<WriteHandle> {
        let path = path.into_string();
        let response = self
            .append_file_request(
                &path,
                proto::metadata::AppendFileRequestProto {
                    header: None,
                    path: path.clone(),
                    desired_len: Some(default_write_preallocation_len()),
                },
            )
            .await?;
        let Some(data_handle_id) = response.data_handle_id else {
            return Err(ClientError::Metadata(
                "AppendFileResponseProto.data_handle_id missing".to_string(),
            ));
        };
        let Some(layout) = response.layout else {
            return Err(ClientError::Metadata(
                "AppendFileResponseProto.layout missing".to_string(),
            ));
        };
        let layout = FileLayout::try_from(layout)
            .map_err(|err| ClientError::InvalidLayout(format!("AppendFileResponseProto.layout invalid: {err}")))?;
        let Some(write_handle) = response.write_handle else {
            return Err(ClientError::Metadata(
                "AppendFileResponseProto.write_handle missing".to_string(),
            ));
        };

        let expires_at_ms = crate::api::handle::valid_write_session_expiry("AppendFile", response.expires_at_ms)?;
        let data_handle_id = DataHandleId::new(data_handle_id.value);
        let session = WriteSession::new(
            path.clone(),
            data_handle_id,
            layout,
            write_handle,
            response.base_size,
            expires_at_ms,
        )?;
        Ok(WriteHandle::new(path, data_handle_id, response.base_size, session))
    }

    async fn append_file_request(
        &self,
        path: &str,
        req: proto::metadata::AppendFileRequestProto,
    ) -> ClientResult<proto::metadata::AppendFileResponseProto> {
        let operation = OperationContext::new_with_identity(
            &self.identity,
            OperationKind::MetadataMutation,
            "AppendFile",
            OperationIdentity::path(path),
        )?;
        self.execute_metadata(operation, req, |gateway, ctx, req| async move {
            gateway.append_file(ctx, req).await
        })
        .await
    }

    /// Execute AddBlock.
    pub(crate) async fn add_block(
        &self,
        path: &str,
        session_identity: String,
        write_handle: proto::metadata::WriteHandleProto,
        desired_len: u64,
    ) -> ClientResult<AddBlockResult> {
        let detail = format!("desired_len={desired_len}");
        let operation = OperationContext::new_with_identity(
            &self.identity,
            OperationKind::MetadataMutation,
            "AddBlock",
            OperationIdentity::session(path, session_identity).with_detail(detail),
        )?;
        let req = proto::metadata::AddBlockRequestProto {
            header: None,
            write_handle: Some(write_handle),
            desired_len: Some(desired_len),
        };
        self.execute_metadata(operation, req, |gateway, ctx, req| async move {
            gateway.add_block(ctx, req).await
        })
        .await
    }

    /// Execute CommitFile.
    pub(crate) async fn commit_file(
        &self,
        plan: CommitFilePlan,
    ) -> ClientResult<proto::metadata::CommitFileResponseProto> {
        let req = proto::metadata::CommitFileRequestProto {
            header: None,
            write_handle: Some(plan.write_handle),
            data_handle_id: Some(proto::common::DataHandleIdProto {
                value: plan.data_handle_id.as_raw(),
            }),
            committed_blocks: plan.committed_blocks.iter().map(Into::into).collect(),
            final_size: plan.final_size,
        };
        self.execute_metadata(plan.operation, req, |gateway, ctx, req| async move {
            gateway.commit_file(ctx, req).await
        })
        .await
    }

    /// Execute AbortFileWrite with a frozen cleanup operation identity.
    pub(crate) async fn abort_file_write(
        &self,
        operation: OperationContext,
        write_handle: proto::metadata::WriteHandleProto,
    ) -> ClientResult<proto::metadata::AbortFileWriteResponseProto> {
        let req = proto::metadata::AbortFileWriteRequestProto {
            header: None,
            write_handle: Some(write_handle),
        };
        self.execute_metadata(operation, req, |gateway, ctx, req| async move {
            gateway.abort_file_write(ctx, req).await
        })
        .await
    }

    /// Execute RenewLease.
    pub(crate) async fn renew_lease(
        &self,
        path: &str,
        session_identity: String,
        write_handle: proto::metadata::WriteHandleProto,
    ) -> ClientResult<proto::metadata::RenewLeaseResponseProto> {
        let operation = OperationContext::new_with_identity(
            &self.identity,
            OperationKind::MetadataSessionBarrier,
            "RenewLease",
            OperationIdentity::session(path, session_identity),
        )?;
        let req = proto::metadata::RenewLeaseRequestProto {
            header: None,
            write_handle: Some(write_handle),
        };
        self.execute_metadata(operation, req, |gateway, ctx, req| async move {
            gateway.renew_lease(ctx, req).await
        })
        .await
    }

    /// Execute SyncWrite.
    pub(crate) async fn sync_write(
        &self,
        session: &WriteSession,
        data_handle_id: DataHandleId,
        committed_blocks: Vec<CommittedBlock>,
        target_size: u64,
        mode: proto::metadata::WriteSyncModeProto,
    ) -> ClientResult<proto::metadata::SyncWriteResponseProto> {
        let detail = format!("mode={} target_size={}", mode as i32, target_size);
        let operation = OperationContext::new_with_identity(
            &self.identity,
            OperationKind::MetadataSessionBarrier,
            "SyncWrite",
            OperationIdentity::session(session.path(), session.session_identity()).with_detail(detail),
        )?;
        let req = proto::metadata::SyncWriteRequestProto {
            header: None,
            write_handle: Some(session.write_handle()),
            data_handle_id: Some(proto::common::DataHandleIdProto {
                value: data_handle_id.as_raw(),
            }),
            committed_blocks: committed_blocks.iter().map(Into::into).collect(),
            target_size,
            mode: mode as i32,
            flags: 0,
        };
        self.execute_metadata(operation, req, |gateway, ctx, req| async move {
            gateway.sync_write(ctx, req).await
        })
        .await
    }

    /// Execute Delete.
    async fn delete_request(
        &self,
        path: &str,
        req: proto::metadata::DeleteRequestProto,
    ) -> ClientResult<proto::metadata::DeleteResponseProto> {
        let operation = OperationContext::new_with_identity(
            &self.identity,
            OperationKind::MetadataMutation,
            "Delete",
            OperationIdentity::path(path).with_detail(format!("recursive={}", req.recursive)),
        )?;
        self.execute_metadata(operation, req, |gateway, ctx, req| async move {
            gateway.delete(ctx, req).await
        })
        .await
    }

    /// Execute Rename.
    async fn rename_request(
        &self,
        src: &str,
        dst: &str,
        req: proto::metadata::RenameRequestProto,
    ) -> ClientResult<proto::metadata::RenameResponseProto> {
        let operation = OperationContext::new_with_identity(
            &self.identity,
            OperationKind::MetadataMutation,
            "Rename",
            OperationIdentity::path_pair(src, dst).with_detail(format!("flags={}", req.flags)),
        )?;
        self.execute_metadata(operation, req, |gateway, ctx, req| async move {
            gateway.rename(ctx, req).await
        })
        .await
    }

    async fn execute_metadata<Req, T, F, Fut>(
        &self,
        operation: OperationContext,
        request: Req,
        mut call: F,
    ) -> ClientResult<T>
    where
        Req: Clone,
        F: FnMut(Arc<dyn MetadataGateway>, AttemptContext, Req) -> Fut,
        Fut: Future<Output = ClientResult<T>>,
    {
        let mut target_group = self.metadata_targets.group_for_operation(&operation)?;
        let mut retry_used = 0usize;
        let mut refresh_used = 0usize;
        let retry_budget = self.retry_budget_for_operation(operation.kind());
        let refresh_budget = self.refresh.max_refresh_attempts;
        let mut attempt = 0u32;

        loop {
            let endpoint = self.metadata_targets.endpoint_for_group(&target_group, attempt)?;
            let mut ctx = AttemptContext::for_metadata(&operation, target_group.clone(), attempt)?
                .with_operation_timeout_ms(self.retry.operation_timeout_ms);
            ctx = ctx.with_metadata_endpoint(&endpoint);
            ctx = self.metadata_targets.enrich_attempt_context(&operation, ctx);
            if let Some(watermark) = self.metadata_targets.state_watermark_proto(&target_group) {
                ctx = ctx.with_state(vec![watermark]);
            }
            let observed_fingerprint = ctx.operation_fingerprint();

            match self
                .metadata_rpc_with_timeout(&operation, call(Arc::clone(&self.gateway), ctx, request.clone()))
                .await
            {
                Ok(result) => return Ok(result),
                Err(err) => {
                    let class = self.classifier.classify_error(&err);
                    self.record_error_metric(&operation, &class);
                    match class.clone() {
                        ErrorClass::UnknownOutcome => {
                            self.record_metric(
                                ClientMetric::UnknownOutcome,
                                operation_labels(&operation)
                                    .with_error_class(class.label())
                                    .with_outcome("unknown"),
                            );
                            return Err(match err {
                                ClientError::UnknownOutcome(_) => err,
                                other => ClientError::UnknownOutcome(format!(
                                    "{} outcome is unknown after {}",
                                    operation.operation_name(),
                                    other
                                )),
                            });
                        }
                        ErrorClass::RefreshMetadata(MetadataRefreshCause::Unknown) => return Err(err),
                        ErrorClass::RefreshMetadata(reason) => {
                            if refresh_budget.saturating_sub(refresh_used) == 0 {
                                self.record_exhausted_if_needed(
                                    &operation,
                                    &class,
                                    retry_budget.saturating_sub(retry_used),
                                    refresh_budget.saturating_sub(refresh_used),
                                );
                                return Err(ClientError::Metadata(format!(
                                    "{} refresh budget exhausted for {}",
                                    operation.operation_name(),
                                    reason.label()
                                )));
                            }
                            if retry_budget.saturating_sub(retry_used) == 0 {
                                self.record_exhausted_if_needed(
                                    &operation,
                                    &class,
                                    retry_budget.saturating_sub(retry_used),
                                    refresh_budget.saturating_sub(refresh_used),
                                );
                                return Err(err);
                            }
                            if let Err(policy_err) = self
                                .replay_policy
                                .ensure_replay_allowed(&operation, Some(observed_fingerprint))
                            {
                                self.record_replay_denied(&operation);
                                return Err(policy_err);
                            }
                            let hint = refresh_hint_from_error(&err);
                            self.record_metric(
                                ClientMetric::RefreshDecision,
                                operation_labels(&operation)
                                    .with_error_class(class.label())
                                    .with_metadata_refresh_cause(reason.label())
                                    .with_outcome("refresh"),
                            );
                            self.record_metric(
                                ClientMetric::MetadataRefreshCause,
                                operation_labels(&operation).with_metadata_refresh_cause(reason.label()),
                            );
                            self.metadata_targets.record_refresh(&operation, reason, &hint)?;
                            if reason == MetadataRefreshCause::StaleState {
                                self.refresh_state(&operation, target_group.clone(), attempt.saturating_add(1))
                                    .await?;
                            }
                            target_group = self.metadata_targets.group_for_operation(&operation)?;
                            refresh_used += 1;
                            retry_used += 1;
                            attempt = attempt.saturating_add(1);
                        }
                        ErrorClass::RetryableTransport => {
                            if retry_budget.saturating_sub(retry_used) == 0 {
                                self.record_exhausted_if_needed(
                                    &operation,
                                    &class,
                                    retry_budget.saturating_sub(retry_used),
                                    refresh_budget.saturating_sub(refresh_used),
                                );
                                return Err(err);
                            }
                            if let Err(policy_err) = self
                                .replay_policy
                                .ensure_replay_allowed(&operation, Some(observed_fingerprint))
                            {
                                self.record_replay_denied(&operation);
                                return Err(policy_err);
                            }
                            self.metadata_targets.record_transport_failure(&target_group, &endpoint);
                            let retry_index = retry_used;
                            retry_used += 1;
                            self.record_metric(
                                ClientMetric::RetryAttempt,
                                operation_labels(&operation).with_error_class(class.label()),
                            );
                            self.sleep_before_retry(retry_index, &operation).await;
                            attempt = attempt.saturating_add(1);
                        }
                        _ => {
                            self.record_exhausted_if_needed(
                                &operation,
                                &class,
                                retry_budget.saturating_sub(retry_used),
                                refresh_budget.saturating_sub(refresh_used),
                            );
                            return Err(err);
                        }
                    }
                }
            }
        }
    }

    async fn refresh_state(
        &self,
        operation: &OperationContext,
        target_group: types::GroupName,
        attempt_number: u32,
    ) -> ClientResult<()> {
        let endpoint = self
            .metadata_targets
            .endpoint_for_group(&target_group, attempt_number)?;
        let ctx = AttemptContext::for_metadata(operation, target_group, attempt_number)?
            .with_operation_timeout_ms(self.retry.operation_timeout_ms)
            .with_metadata_endpoint(endpoint);
        let watermark = self
            .metadata_rpc_with_timeout(
                operation,
                self.gateway
                    .msync(ctx, proto::metadata::MsyncRequestProto { header: None }),
            )
            .await?;
        self.metadata_targets.record_state_watermark(watermark)
    }

    async fn metadata_rpc_with_timeout<T, Fut>(&self, operation: &OperationContext, future: Fut) -> ClientResult<T>
    where
        Fut: Future<Output = ClientResult<T>>,
    {
        let Some(timeout) = operation_timeout_duration(self.retry.operation_timeout_ms) else {
            return future.await;
        };
        match tokio::time::timeout(timeout, future).await {
            Ok(result) => result,
            Err(_) => {
                self.record_metric(
                    ClientMetric::RpcTimeout,
                    operation_labels(operation)
                        .with_error_class(ErrorClass::RetryableTransport.label())
                        .with_outcome("timeout"),
                );
                Err(timeout_error("metadata", operation.operation_name(), timeout))
            }
        }
    }

    fn retry_budget_for_operation(&self, kind: OperationKind) -> usize {
        match kind {
            OperationKind::MetadataSessionBarrier => 0,
            OperationKind::MetadataRead
            | OperationKind::MetadataMutation
            | OperationKind::CleanupBestEffort
            | OperationKind::WorkerReadData
            | OperationKind::WorkerWriteData => self.retry.max_retry_attempts(),
        }
    }

    async fn sleep_before_retry(&self, retry_index: usize, operation: &OperationContext) {
        let delay = self.backoff.delay_for_retry(retry_index);
        self.record_metric(
            ClientMetric::BackoffDelay,
            operation_labels(operation).with_outcome("scheduled"),
        );
        self.sleeper.sleep(delay).await;
    }

    fn record_exhausted_if_needed(
        &self,
        operation: &OperationContext,
        class: &ErrorClass,
        retry_remaining: usize,
        refresh_remaining: usize,
    ) {
        match class {
            ErrorClass::RetryableTransport if retry_remaining == 0 => self.record_metric(
                ClientMetric::RetryExhausted,
                operation_labels(operation).with_error_class(class.label()),
            ),
            ErrorClass::RefreshMetadata(reason)
                if *reason != MetadataRefreshCause::Unknown && refresh_remaining == 0 =>
            {
                self.record_metric(
                    ClientMetric::RefreshExhausted,
                    operation_labels(operation)
                        .with_error_class(class.label())
                        .with_metadata_refresh_cause(reason.label()),
                );
            }
            _ => {}
        }
    }

    fn record_error_metric(&self, operation: &OperationContext, class: &ErrorClass) {
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
            self.record_metric(metric, operation_labels(operation).with_error_class(class.label()));
        }
    }

    fn record_replay_denied(&self, operation: &OperationContext) {
        self.record_metric(
            ClientMetric::UnsafeReplayDenied,
            operation_labels(operation).with_outcome("denied"),
        );
        if operation.kind() == OperationKind::MetadataSessionBarrier {
            self.record_metric(
                ClientMetric::SessionBarrierReplayDenied,
                operation_labels(operation).with_outcome("denied"),
            );
        }
    }

    fn record_metric(&self, metric: ClientMetric, labels: ClientMetricLabels) {
        self.metrics.record(ClientMetricEvent::new(metric, labels));
    }
}

fn operation_timeout_duration(timeout_ms: Option<u64>) -> Option<Duration> {
    timeout_ms.map(Duration::from_millis)
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

fn default_dir_attrs() -> proto::fs::FileAttrsProto {
    proto::fs::FileAttrsProto {
        mode: 0o755,
        uid: 0,
        gid: 0,
        size: 0,
        atime_ms: 0,
        mtime_ms: 0,
        ctime_ms: 0,
        nlink: 2,
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

fn file_status_from_response(
    path: String,
    response: proto::metadata::GetStatusResponseProto,
) -> ClientResult<FileStatus> {
    let attrs = response
        .attrs
        .ok_or_else(|| ClientError::Metadata("GetStatusResponseProto.attrs missing".to_string()))?;
    Ok(FileStatus::new(path, attrs.into()))
}

fn directory_status_from_response(
    path: String,
    response: proto::metadata::CreateDirectoryResponseProto,
) -> ClientResult<FileStatus> {
    let attrs = response
        .attrs
        .ok_or_else(|| ClientError::Metadata("CreateDirectoryResponseProto.attrs missing".to_string()))?;
    Ok(FileStatus::new(path, attrs.into()))
}

fn directory_listing_from_response(
    path: String,
    response: proto::metadata::ListStatusResponseProto,
) -> DirectoryListing {
    let next_cursor = if response.next_cursor.is_empty() {
        None
    } else {
        Some(response.next_cursor)
    };
    let entries = response
        .entries
        .into_iter()
        .map(|entry| {
            let kind = proto::fs::InodeKindProto::try_from(entry.kind)
                .ok()
                .and_then(|kind| kind.try_into().ok());
            DirectoryEntry::new(entry.name, kind, entry.attrs.map(Into::into))
        })
        .collect();
    DirectoryListing::new(path, entries, next_cursor, response.eof)
}

fn timeout_error(target_plane: &str, operation: &str, timeout: Duration) -> ClientError {
    ClientError::from(tonic::Status::deadline_exceeded(format!(
        "{target_plane} {operation} timed out after {}ms",
        timeout.as_millis()
    )))
}

impl fmt::Debug for OperationExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OperationExecutor")
            .field("client_id", &self.identity.client_id())
            .field("client_name", &self.identity.client_name())
            .field("metadata_targets", &self.metadata_targets)
            .field("retry", &self.retry)
            .field("refresh", &self.refresh)
            .finish_non_exhaustive()
    }
}

fn operation_labels(operation: &OperationContext) -> ClientMetricLabels {
    ClientMetricLabels::default().with_operation(
        operation.kind().label(),
        operation.operation_name(),
        operation.kind().target_plane(),
    )
}

fn refresh_hint_from_error(err: &ClientError) -> crate::rpc_error::RefreshHint {
    match err {
        ClientError::Action(action) => match action.action() {
            ClientAction::Refresh { hint, .. } => hint.as_ref().clone(),
            _ => crate::rpc_error::RefreshHint::default(),
        },
        _ => crate::rpc_error::RefreshHint::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BackoffConfig;
    use crate::runtime::policy::OperationKind;
    use crate::runtime::refresh::MetadataGroupTargets;
    use async_trait::async_trait;
    use common::error::rpc::{
        ErrorKind, InternalErrorKind, MetadataErrorKind, ProtocolErrorKind, RecoveryAction,
        RefreshHint as RpcRefreshHint, RpcErrorDetail,
    };
    use proto::common::{GroupStateWatermarkProto, RaftLogIdProto};
    use proto::metadata::{
        AbortFileWriteResponseProto, AppendFileResponseProto, CommitFileResponseProto, CreateFileResponseProto,
        DeleteRequestProto, DeleteResponseProto, GetStatusRequestProto, GetStatusResponseProto,
        RenewLeaseResponseProto,
    };
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::time::Duration;
    use types::lease::FencingToken;
    use types::{
        BlockId, BlockIndex, ClientId, DataHandleId, GroupName, WorkerEndpointInfo, WorkerId, WorkerNetProtocol,
        WriteTarget,
    };

    #[derive(Debug, Default)]
    struct RecordingSleeper {
        delays: Mutex<Vec<Duration>>,
    }

    #[async_trait]
    impl BackoffSleeper for RecordingSleeper {
        async fn sleep(&self, delay: Duration) {
            self.delays.lock().expect("delays").push(delay);
        }
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

    #[tokio::test]
    async fn not_leader_refresh_replays_metadata_read_on_cached_leader_endpoint() {
        let gateway = Arc::new(ScriptedGateway::new(vec![
            refresh_outcome(
                ErrorKind::Metadata(MetadataErrorKind::NotLeader),
                MetadataRefreshCause::NotLeader,
                RpcRefreshHint {
                    group_name: Some("root".to_string()),
                    leader_endpoint: Some("http://127.0.0.1:18081".to_string()),
                    ..RpcRefreshHint::default()
                },
            ),
            GatewayOutcome::Ok,
        ]));
        let executor = test_executor(gateway.clone());

        executor
            .get_status_request(
                "/alpha",
                GetStatusRequestProto {
                    header: None,
                    path: "/alpha".to_string(),
                },
            )
            .await
            .expect("metadata read replay");

        let calls = gateway.calls();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].endpoint.as_deref(), Some("http://127.0.0.1:18080"));
        assert_eq!(calls[1].endpoint.as_deref(), Some("http://127.0.0.1:18081"));
        assert_eq!(calls[0].call_id, calls[1].call_id);
    }

    #[tokio::test]
    async fn owner_group_mismatch_with_leader_hint_replays_same_call_id_on_refreshed_endpoint() {
        let gateway = Arc::new(ScriptedGateway::new(vec![
            refresh_outcome(
                ErrorKind::Metadata(MetadataErrorKind::OwnerGroupMismatch),
                MetadataRefreshCause::OwnerGroupMismatch,
                RpcRefreshHint {
                    group_name: Some("analytics".to_string()),
                    leader_endpoint: Some("http://127.0.0.1:18082".to_string()),
                    ..RpcRefreshHint::default()
                },
            ),
            GatewayOutcome::Ok,
        ]));
        let executor = test_executor(gateway.clone());

        executor
            .get_status_request(
                "/alpha",
                GetStatusRequestProto {
                    header: None,
                    path: "/alpha".to_string(),
                },
            )
            .await
            .expect("metadata read replay");

        let calls = gateway.calls();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].group_name, group_name("root"));
        assert_eq!(calls[1].group_name, group_name("analytics"));
        assert_eq!(calls[0].call_id, calls[1].call_id);
        assert_eq!(calls[1].endpoint.as_deref(), Some("http://127.0.0.1:18082"));
    }

    #[tokio::test]
    async fn metadata_mutation_owner_redirect_replays_same_call_id() {
        let gateway = Arc::new(ScriptedGateway::new(vec![
            refresh_outcome(
                ErrorKind::Metadata(MetadataErrorKind::OwnerGroupMismatch),
                MetadataRefreshCause::OwnerGroupMismatch,
                RpcRefreshHint {
                    group_name: Some("analytics".to_string()),
                    ..RpcRefreshHint::default()
                },
            ),
            GatewayOutcome::Ok,
        ]));
        let executor = test_executor(gateway.clone());

        executor
            .delete_request(
                "/alpha",
                DeleteRequestProto {
                    header: None,
                    path: "/alpha".to_string(),
                    recursive: false,
                },
            )
            .await
            .expect("metadata mutation replay");

        let calls = gateway.calls();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].group_name, group_name("root"));
        assert_eq!(calls[1].group_name, group_name("analytics"));
        assert_eq!(calls[0].call_id, calls[1].call_id);
    }

    #[tokio::test]
    async fn stale_state_refresh_replays_with_state_watermark() {
        let watermark = watermark_proto("root", 44);
        let gateway = Arc::new(
            ScriptedGateway::new(vec![
                refresh_outcome(
                    ErrorKind::Metadata(MetadataErrorKind::StaleState),
                    MetadataRefreshCause::StaleState,
                    RpcRefreshHint {
                        group_name: Some("root".to_string()),
                        ..RpcRefreshHint::default()
                    },
                ),
                GatewayOutcome::Ok,
            ])
            .with_msync_watermark(watermark.clone()),
        );
        let executor = test_executor(gateway.clone());

        executor
            .get_status_request(
                "/alpha",
                GetStatusRequestProto {
                    header: None,
                    path: "/alpha".to_string(),
                },
            )
            .await
            .expect("stale state replay");

        let calls = gateway.calls();
        assert_eq!(calls.len(), 3);
        assert_eq!(calls[0].method, "get_status");
        assert_eq!(calls[1].method, "msync");
        assert_eq!(calls[2].method, "get_status");
        assert!(calls[0].state.is_empty());
        assert_eq!(calls[2].state, vec![watermark]);
        assert_eq!(calls[0].call_id, calls[2].call_id);
    }

    #[tokio::test]
    async fn mount_epoch_mismatch_replays_with_refreshed_mount_epoch_header() {
        let gateway = Arc::new(ScriptedGateway::new(vec![
            refresh_outcome(
                ErrorKind::Metadata(MetadataErrorKind::MountEpochMismatch),
                MetadataRefreshCause::MountEpochMismatch,
                RpcRefreshHint {
                    mount_epoch: Some(42),
                    ..RpcRefreshHint::default()
                },
            ),
            GatewayOutcome::Ok,
        ]));
        let executor = test_executor(gateway.clone());

        executor
            .get_status_request(
                "/alpha",
                GetStatusRequestProto {
                    header: None,
                    path: "/alpha".to_string(),
                },
            )
            .await
            .expect("mount epoch replay");

        let calls = gateway.calls();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].mount_epoch, None);
        assert_eq!(calls[1].mount_epoch, Some(42));
        assert_eq!(calls[0].call_id, calls[1].call_id);
    }

    #[tokio::test]
    async fn route_epoch_mismatch_replays_with_refreshed_route_epoch_header() {
        let gateway = Arc::new(ScriptedGateway::new(vec![
            refresh_outcome(
                ErrorKind::Metadata(MetadataErrorKind::RouteEpochMismatch),
                MetadataRefreshCause::RouteEpochMismatch,
                RpcRefreshHint {
                    route_epoch: Some(24),
                    ..RpcRefreshHint::default()
                },
            ),
            GatewayOutcome::Ok,
        ]));
        let executor = test_executor(gateway.clone());

        executor
            .get_status_request(
                "/alpha",
                GetStatusRequestProto {
                    header: None,
                    path: "/alpha".to_string(),
                },
            )
            .await
            .expect("route epoch replay");

        let calls = gateway.calls();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].route_epoch, None);
        assert_eq!(calls[1].route_epoch, Some(24));
        assert_eq!(calls[0].call_id, calls[1].call_id);
    }

    #[tokio::test]
    async fn refresh_without_epoch_hint_does_not_inject_fake_default_epoch() {
        let gateway = Arc::new(ScriptedGateway::new(vec![
            refresh_outcome(
                ErrorKind::Metadata(MetadataErrorKind::MountEpochMismatch),
                MetadataRefreshCause::MountEpochMismatch,
                RpcRefreshHint::default(),
            ),
            GatewayOutcome::Ok,
        ]));
        let executor = test_executor(gateway.clone());

        executor
            .get_status_request(
                "/alpha",
                GetStatusRequestProto {
                    header: None,
                    path: "/alpha".to_string(),
                },
            )
            .await
            .expect("refresh replay without epoch hint");

        let calls = gateway.calls();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[1].mount_epoch, None);
        assert_eq!(calls[1].route_epoch, None);
    }

    #[tokio::test]
    async fn session_barrier_refresh_metadata_honors_retry_budget_before_replay_gate() {
        let gateway = Arc::new(ScriptedGateway::new(vec![refresh_outcome(
            ErrorKind::Metadata(MetadataErrorKind::StaleState),
            MetadataRefreshCause::StaleState,
            RpcRefreshHint::default(),
        )]));
        let metrics = Arc::new(RecordingMetrics::default());
        let executor = test_executor_with_budgets(gateway.clone(), 1, 1, Arc::clone(&metrics));
        let operation = OperationContext::new(
            ClientId::new(7),
            OperationKind::MetadataSessionBarrier,
            "CommitFile",
            OperationIdentity::path("/alpha"),
        )
        .expect("operation context");

        let err = executor
            .execute_metadata(
                operation,
                GetStatusRequestProto {
                    header: None,
                    path: "/alpha".to_string(),
                },
                |gateway, ctx, req| async move { gateway.get_status(ctx, req).await },
            )
            .await
            .expect_err("session barrier must not replay after retry budget is exhausted");

        assert_eq!(
            ErrorClassifier.classify_error(&err),
            ErrorClass::RefreshMetadata(MetadataRefreshCause::StaleState)
        );
        assert_eq!(gateway.calls().len(), 1);
        assert!(metrics
            .events()
            .iter()
            .all(|event| event.metric != ClientMetric::UnsafeReplayDenied));
    }

    #[tokio::test]
    async fn metadata_read_missing_header_is_not_retried_as_transport() {
        let gateway = Arc::new(ScriptedGateway::new(vec![GatewayOutcome::MissingHeader]));
        let executor = test_executor(gateway.clone());

        let err = executor
            .get_status_request(
                "/alpha",
                GetStatusRequestProto {
                    header: None,
                    path: "/alpha".to_string(),
                },
            )
            .await
            .expect_err("missing header must fail");

        assert_invalid_header_not_retryable(&err);
        assert_eq!(gateway.calls().len(), 1);
    }

    #[tokio::test]
    async fn metadata_read_transport_exhaustion_remains_retryable_transport() {
        let gateway = Arc::new(ScriptedGateway::new(vec![
            GatewayOutcome::TransportUnavailable,
            GatewayOutcome::TransportUnavailable,
            GatewayOutcome::TransportUnavailable,
        ]));
        let executor = test_executor(gateway.clone());

        let err = executor
            .get_status_request(
                "/alpha",
                GetStatusRequestProto {
                    header: None,
                    path: "/alpha".to_string(),
                },
            )
            .await
            .expect_err("metadata read transport exhaustion must surface as transport");

        assert_eq!(ErrorClassifier.classify_error(&err), ErrorClass::RetryableTransport);
        assert_eq!(gateway.calls().len(), 3);
    }

    #[tokio::test]
    async fn metadata_read_zero_retry_budget_is_honored_and_observed() {
        let gateway = Arc::new(ScriptedGateway::new(vec![
            GatewayOutcome::TransportUnavailable,
            GatewayOutcome::Ok,
        ]));
        let metrics = Arc::new(RecordingMetrics::default());
        let executor = test_executor_with_budgets(gateway.clone(), 0, 1, Arc::clone(&metrics));

        let err = executor
            .get_status_request(
                "/alpha",
                GetStatusRequestProto {
                    header: None,
                    path: "/alpha".to_string(),
                },
            )
            .await
            .expect_err("zero retry budget must not retry");

        assert_eq!(ErrorClassifier.classify_error(&err), ErrorClass::RetryableTransport);
        assert_eq!(gateway.calls().len(), 1);
        assert_metric(&metrics.events(), ClientMetric::RetryExhausted);
    }

    #[tokio::test]
    async fn metadata_refresh_zero_budget_is_honored_and_observed() {
        let gateway = Arc::new(ScriptedGateway::new(vec![refresh_outcome(
            ErrorKind::Metadata(MetadataErrorKind::NotLeader),
            MetadataRefreshCause::NotLeader,
            RpcRefreshHint {
                group_name: Some("root".to_string()),
                leader_endpoint: Some("http://127.0.0.1:18081".to_string()),
                ..RpcRefreshHint::default()
            },
        )]));
        let metrics = Arc::new(RecordingMetrics::default());
        let executor = test_executor_with_budgets(gateway.clone(), 1, 0, Arc::clone(&metrics));

        let err = executor
            .get_status_request(
                "/alpha",
                GetStatusRequestProto {
                    header: None,
                    path: "/alpha".to_string(),
                },
            )
            .await
            .expect_err("zero refresh budget must not refresh");

        assert!(matches!(err, ClientError::Metadata(msg) if msg.contains("refresh budget exhausted")));
        assert_eq!(gateway.calls().len(), 1);
        assert_metric(&metrics.events(), ClientMetric::RefreshExhausted);
    }

    #[tokio::test]
    async fn metadata_retry_records_backoff_without_real_sleep() {
        let gateway = Arc::new(ScriptedGateway::new(vec![
            GatewayOutcome::TransportUnavailable,
            GatewayOutcome::Ok,
        ]));
        let metrics = Arc::new(RecordingMetrics::default());
        let sleeper = Arc::new(RecordingSleeper::default());
        let metrics_hook: Arc<dyn ClientMetrics> = metrics.clone();
        let sleeper_hook: Arc<dyn BackoffSleeper> = sleeper.clone();
        let executor = test_executor_with_hooks(
            gateway.clone(),
            retry_config(1),
            RefreshConfig {
                max_refresh_attempts: 1,
            },
            metrics_hook,
            sleeper_hook,
        );

        executor
            .get_status_request(
                "/alpha",
                GetStatusRequestProto {
                    header: None,
                    path: "/alpha".to_string(),
                },
            )
            .await
            .expect("retry succeeds");

        assert_eq!(gateway.calls().len(), 2);
        assert_eq!(
            sleeper.delays.lock().expect("delays").as_slice(),
            &[Duration::from_millis(100)]
        );
        let events = metrics.events();
        assert_metric(&events, ClientMetric::RetryAttempt);
        assert_metric(&events, ClientMetric::BackoffDelay);
        assert!(events.iter().all(|event| event.labels.has_only_safe_values()));
    }

    #[tokio::test]
    async fn metadata_retry_after_ms_is_hint_and_runtime_backoff_schedules_sleep() {
        let gateway = Arc::new(ScriptedGateway::new(vec![
            GatewayOutcome::Retryable {
                retry_after_ms: Some(250),
            },
            GatewayOutcome::Ok,
        ]));
        let metrics = Arc::new(RecordingMetrics::default());
        let sleeper = Arc::new(RecordingSleeper::default());
        let metrics_hook: Arc<dyn ClientMetrics> = metrics.clone();
        let sleeper_hook: Arc<dyn BackoffSleeper> = sleeper.clone();
        let executor = test_executor_with_hooks(
            gateway.clone(),
            retry_config(1),
            RefreshConfig {
                max_refresh_attempts: 1,
            },
            metrics_hook,
            sleeper_hook,
        );

        executor
            .get_status_request(
                "/alpha",
                GetStatusRequestProto {
                    header: None,
                    path: "/alpha".to_string(),
                },
            )
            .await
            .expect("retry succeeds");

        assert_eq!(gateway.calls().len(), 2);
        assert_eq!(
            sleeper.delays.lock().expect("delays").as_slice(),
            &[Duration::from_millis(100)]
        );
    }

    #[tokio::test]
    async fn add_block_session_expired_is_not_replayed_as_refresh() {
        let gateway = Arc::new(ScriptedGateway::new(vec![refresh_outcome(
            ErrorKind::Metadata(MetadataErrorKind::SessionExpired),
            MetadataRefreshCause::Unknown,
            RpcRefreshHint::default(),
        )]));
        let executor = test_executor(gateway.clone());

        let err = executor
            .add_block(
                "/alpha",
                "handle=1".to_string(),
                proto::metadata::WriteHandleProto::default(),
                5,
            )
            .await
            .expect_err("expired session must fail without replay");

        assert_eq!(ErrorClassifier.classify_error(&err), ErrorClass::SessionExpired);
        assert_eq!(gateway.calls().len(), 1);
    }

    #[tokio::test]
    async fn commit_file_fencing_is_not_replayed_as_refresh() {
        let gateway = Arc::new(ScriptedGateway::new(vec![refresh_outcome(
            ErrorKind::Metadata(MetadataErrorKind::Fencing),
            MetadataRefreshCause::Unknown,
            RpcRefreshHint::default(),
        )]));
        let executor = test_executor(gateway.clone());
        let operation = OperationContext::new(
            ClientId::new(7),
            OperationKind::MetadataSessionBarrier,
            "CommitFile",
            OperationIdentity::session("/alpha", "handle=1"),
        )
        .expect("operation context");
        let plan = CommitFilePlan {
            operation,
            write_handle: proto::metadata::WriteHandleProto::default(),
            data_handle_id: DataHandleId::new(302),
            committed_blocks: Vec::new(),
            final_size: 0,
        };

        let err = executor
            .commit_file(plan)
            .await
            .expect_err("fencing mismatch must fail without replay");

        assert_eq!(ErrorClassifier.classify_error(&err), ErrorClass::Fencing);
        assert_eq!(gateway.calls().len(), 1);
    }

    #[tokio::test]
    async fn unknown_metadata_refresh_cause_is_not_blindly_replayed() {
        let gateway = Arc::new(ScriptedGateway::new(vec![refresh_outcome(
            ErrorKind::Internal(InternalErrorKind::Internal),
            MetadataRefreshCause::Unknown,
            RpcRefreshHint::default(),
        )]));
        let executor = test_executor(gateway.clone());

        let err = executor
            .get_status_request(
                "/alpha",
                GetStatusRequestProto {
                    header: None,
                    path: "/alpha".to_string(),
                },
            )
            .await
            .expect_err("unknown metadata refresh cause must fail without replay");

        assert_eq!(
            ErrorClassifier.classify_error(&err),
            ErrorClass::RefreshMetadata(MetadataRefreshCause::Unknown)
        );
        assert_eq!(gateway.calls().len(), 1);
    }

    #[tokio::test]
    async fn pending_metadata_rpc_times_out_with_configured_operation_timeout() {
        let metrics = Arc::new(RecordingMetrics::default());
        let gateway = Arc::new(ScriptedGateway::new(vec![GatewayOutcome::Pending]));
        let mut retry = retry_config(0);
        retry.operation_timeout_ms = Some(10);
        let metrics_hook: Arc<dyn ClientMetrics> = metrics.clone();
        let executor = test_executor_with_hooks(
            gateway.clone(),
            retry,
            RefreshConfig::default(),
            metrics_hook,
            Arc::new(RecordingSleeper::default()),
        );

        let result = tokio::time::timeout(
            Duration::from_millis(200),
            executor.get_status_request(
                "/alpha",
                GetStatusRequestProto {
                    header: None,
                    path: "/alpha".to_string(),
                },
            ),
        )
        .await
        .expect("executor must return before outer test timeout");
        let err = result.expect_err("pending metadata call must fail with timeout");

        assert_eq!(ErrorClassifier.classify_error(&err), ErrorClass::RetryableTransport);
        assert_metric(&metrics.events(), ClientMetric::RpcTimeout);
        let calls = gateway.calls();
        assert_eq!(calls.len(), 1);
        assert!(calls[0].deadline_ms > 0);
    }

    #[tokio::test]
    async fn metadata_timeout_consumes_retry_budget_and_reuses_call_id() {
        let gateway = Arc::new(ScriptedGateway::new(vec![GatewayOutcome::Pending, GatewayOutcome::Ok]));
        let mut retry = retry_config(1);
        retry.operation_timeout_ms = Some(10);
        let executor = test_executor_with_hooks(
            gateway.clone(),
            retry,
            RefreshConfig::default(),
            Arc::new(RecordingMetrics::default()),
            Arc::new(RecordingSleeper::default()),
        );

        tokio::time::timeout(
            Duration::from_millis(200),
            executor.get_status_request(
                "/alpha",
                GetStatusRequestProto {
                    header: None,
                    path: "/alpha".to_string(),
                },
            ),
        )
        .await
        .expect("executor must return before outer test timeout")
        .expect("retry after timeout should succeed");

        let calls = gateway.calls();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].call_id, calls[1].call_id);
        assert!(calls.iter().all(|call| call.deadline_ms > 0));
    }

    #[derive(Clone, Debug)]
    enum GatewayOutcome {
        Ok,
        Refresh {
            kind: ErrorKind,
            reason: MetadataRefreshCause,
            hint: RpcRefreshHint,
        },
        Retryable {
            retry_after_ms: Option<u64>,
        },
        TransportUnavailable,
        MissingHeader,
        Pending,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct RecordedAttempt {
        method: &'static str,
        group_name: GroupName,
        endpoint: Option<String>,
        call_id: String,
        deadline_ms: i64,
        mount_epoch: Option<u64>,
        route_epoch: Option<u64>,
        state: Vec<GroupStateWatermarkProto>,
    }

    #[derive(Debug)]
    struct ScriptedGateway {
        outcomes: Mutex<VecDeque<GatewayOutcome>>,
        calls: Mutex<Vec<RecordedAttempt>>,
        msync_watermark: GroupStateWatermarkProto,
    }

    impl ScriptedGateway {
        fn new(outcomes: Vec<GatewayOutcome>) -> Self {
            Self {
                outcomes: Mutex::new(VecDeque::from(outcomes)),
                calls: Mutex::new(Vec::new()),
                msync_watermark: watermark_proto("root", 1),
            }
        }

        fn with_msync_watermark(mut self, watermark: GroupStateWatermarkProto) -> Self {
            self.msync_watermark = watermark;
            self
        }

        fn calls(&self) -> Vec<RecordedAttempt> {
            self.calls.lock().expect("calls").clone()
        }

        fn record(&self, method: &'static str, ctx: &AttemptContext) {
            let header = ctx.metadata_header().expect("metadata header");
            self.calls.lock().expect("calls").push(RecordedAttempt {
                method,
                group_name: GroupName::parse(&header.group_name).expect("recorded metadata header group_name"),
                endpoint: ctx.metadata_endpoint().map(ToOwned::to_owned),
                call_id: header.client.expect("client").call_id,
                deadline_ms: header.deadline_ms,
                mount_epoch: header.mount_epoch,
                route_epoch: header.route_epoch,
                state: header.state,
            });
        }

        async fn next_result(&self, method: &'static str, ctx: &AttemptContext) -> ClientResult<()> {
            self.record(method, ctx);
            let outcome = {
                let mut outcomes = self.outcomes.lock().expect("outcomes");
                outcomes.pop_front().unwrap_or(GatewayOutcome::Ok)
            };
            match outcome {
                GatewayOutcome::Ok => Ok(()),
                GatewayOutcome::Refresh { kind, reason, hint } => Err(refresh_error(kind, reason, hint)),
                GatewayOutcome::Retryable { retry_after_ms } => Err(retryable_error(retry_after_ms)),
                GatewayOutcome::TransportUnavailable => Err(ClientError::from(tonic::Status::unavailable(
                    "injected metadata transport failure",
                ))),
                GatewayOutcome::MissingHeader => {
                    Err(invalid_header_error("metadata OK response missing ResponseHeader"))
                }
                GatewayOutcome::Pending => {
                    std::future::pending::<()>().await;
                    Ok(())
                }
            }
        }
    }

    fn read_layout_response(group_name: GroupName) -> ReadLayout {
        ReadLayout {
            group_name,
            data_handle_id: DataHandleId::new(202),
            file_size: 0,
            file_version: Some(1),
            locations: Vec::new(),
        }
    }

    fn worker_endpoint() -> WorkerEndpointInfo {
        WorkerEndpointInfo {
            worker_id: WorkerId::new(1),
            endpoint: "127.0.0.1:19101".to_string(),
            worker_net_protocol: WorkerNetProtocol::Grpc,
            worker_run_id: "550e8400-e29b-41d4-a716-446655440000"
                .parse()
                .expect("valid test WorkerRunId"),
        }
    }

    fn write_target() -> WriteTarget {
        let block_id = BlockId::new(DataHandleId::new(202), BlockIndex::new(0));
        WriteTarget {
            block_id,
            file_offset: 0,
            block_size: 4096,
            effective_len: 1,
            worker_endpoints: vec![worker_endpoint()],
            fencing_token: FencingToken::new(block_id, ClientId::new(7), 1),
            block_stamp: 1,
            chunk_size: 4096,
            block_format_id: types::BlockFormatId::CURRENT_FOR_NEW_FILE,
            tier: types::Tier::Hdd,
        }
    }

    #[async_trait]
    impl MetadataGateway for ScriptedGateway {
        async fn get_status(
            &self,
            ctx: AttemptContext,
            _req: proto::metadata::GetStatusRequestProto,
        ) -> ClientResult<proto::metadata::GetStatusResponseProto> {
            self.next_result("get_status", &ctx).await?;
            Ok(GetStatusResponseProto::default())
        }

        async fn list_status(
            &self,
            ctx: AttemptContext,
            _req: proto::metadata::ListStatusRequestProto,
        ) -> ClientResult<proto::metadata::ListStatusResponseProto> {
            self.next_result("list_status", &ctx).await?;
            Ok(proto::metadata::ListStatusResponseProto::default())
        }

        async fn create_directory(
            &self,
            ctx: AttemptContext,
            _req: proto::metadata::CreateDirectoryRequestProto,
        ) -> ClientResult<proto::metadata::CreateDirectoryResponseProto> {
            self.next_result("create_directory", &ctx).await?;
            Ok(proto::metadata::CreateDirectoryResponseProto::default())
        }

        async fn delete(
            &self,
            ctx: AttemptContext,
            _req: proto::metadata::DeleteRequestProto,
        ) -> ClientResult<proto::metadata::DeleteResponseProto> {
            self.next_result("delete", &ctx).await?;
            Ok(DeleteResponseProto::default())
        }

        async fn rename(
            &self,
            ctx: AttemptContext,
            _req: proto::metadata::RenameRequestProto,
        ) -> ClientResult<proto::metadata::RenameResponseProto> {
            self.next_result("rename", &ctx).await?;
            Ok(proto::metadata::RenameResponseProto::default())
        }

        async fn open_file(
            &self,
            ctx: AttemptContext,
            _req: proto::metadata::OpenFileRequestProto,
        ) -> ClientResult<proto::metadata::OpenFileResponseProto> {
            self.next_result("open_file", &ctx).await?;
            Ok(proto::metadata::OpenFileResponseProto::default())
        }

        async fn read_layout(
            &self,
            ctx: AttemptContext,
            _req: proto::metadata::GetBlockLocationsRequestProto,
        ) -> ClientResult<ReadLayout> {
            self.next_result("read_layout", &ctx).await?;
            Ok(read_layout_response(
                GroupName::parse(&ctx.metadata_header()?.group_name).unwrap(),
            ))
        }

        async fn create_file(
            &self,
            ctx: AttemptContext,
            _req: proto::metadata::CreateFileRequestProto,
        ) -> ClientResult<proto::metadata::CreateFileResponseProto> {
            self.next_result("create_file", &ctx).await?;
            Ok(CreateFileResponseProto::default())
        }

        async fn append_file(
            &self,
            ctx: AttemptContext,
            _req: proto::metadata::AppendFileRequestProto,
        ) -> ClientResult<proto::metadata::AppendFileResponseProto> {
            self.next_result("append_file", &ctx).await?;
            Ok(AppendFileResponseProto::default())
        }

        async fn add_block(
            &self,
            ctx: AttemptContext,
            _req: proto::metadata::AddBlockRequestProto,
        ) -> ClientResult<AddBlockResult> {
            self.next_result("add_block", &ctx).await?;
            Ok(AddBlockResult {
                group_name: GroupName::parse(&ctx.metadata_header()?.group_name).unwrap(),
                target: write_target(),
            })
        }

        async fn commit_file(
            &self,
            ctx: AttemptContext,
            _req: proto::metadata::CommitFileRequestProto,
        ) -> ClientResult<proto::metadata::CommitFileResponseProto> {
            self.next_result("commit_file", &ctx).await?;
            Ok(CommitFileResponseProto::default())
        }

        async fn abort_file_write(
            &self,
            ctx: AttemptContext,
            _req: proto::metadata::AbortFileWriteRequestProto,
        ) -> ClientResult<proto::metadata::AbortFileWriteResponseProto> {
            self.next_result("abort_file_write", &ctx).await?;
            Ok(AbortFileWriteResponseProto::default())
        }

        async fn renew_lease(
            &self,
            ctx: AttemptContext,
            _req: proto::metadata::RenewLeaseRequestProto,
        ) -> ClientResult<proto::metadata::RenewLeaseResponseProto> {
            self.next_result("renew_lease", &ctx).await?;
            Ok(RenewLeaseResponseProto::default())
        }

        async fn sync_write(
            &self,
            ctx: AttemptContext,
            _req: proto::metadata::SyncWriteRequestProto,
        ) -> ClientResult<proto::metadata::SyncWriteResponseProto> {
            self.next_result("sync_write", &ctx).await?;
            Ok(proto::metadata::SyncWriteResponseProto::default())
        }

        async fn msync(
            &self,
            ctx: AttemptContext,
            _req: proto::metadata::MsyncRequestProto,
        ) -> ClientResult<proto::common::GroupStateWatermarkProto> {
            self.record("msync", &ctx);
            Ok(self.msync_watermark.clone())
        }
    }

    fn test_executor(gateway: Arc<ScriptedGateway>) -> OperationExecutor {
        test_executor_with_budgets(gateway, 2, 2, Arc::new(RecordingMetrics::default()))
    }

    fn test_executor_with_budgets(
        gateway: Arc<ScriptedGateway>,
        retry_budget: usize,
        refresh_budget: usize,
        metrics: Arc<RecordingMetrics>,
    ) -> OperationExecutor {
        let metrics_hook: Arc<dyn ClientMetrics> = metrics;
        let sleeper_hook: Arc<dyn BackoffSleeper> = Arc::new(RecordingSleeper::default());
        test_executor_with_hooks(
            gateway,
            retry_config(retry_budget),
            RefreshConfig {
                max_refresh_attempts: refresh_budget,
            },
            metrics_hook,
            sleeper_hook,
        )
    }

    fn test_executor_with_hooks(
        gateway: Arc<ScriptedGateway>,
        retry: RetryConfig,
        refresh: RefreshConfig,
        metrics: Arc<dyn ClientMetrics>,
        sleeper: Arc<dyn BackoffSleeper>,
    ) -> OperationExecutor {
        let gateway: Arc<dyn MetadataGateway> = gateway;
        let metadata_targets = MetadataTargets::new(vec![
            MetadataGroupTargets {
                group_name: group_name("root"),
                endpoints: vec!["http://127.0.0.1:18080".to_string()],
            },
            MetadataGroupTargets {
                group_name: group_name("analytics"),
                endpoints: vec!["http://127.0.0.1:18082".to_string()],
            },
        ])
        .expect("metadata targets");
        let config = ClientConfig {
            retry,
            refresh: RefreshConfig {
                max_refresh_attempts: refresh.max_refresh_attempts,
            },
            backoff: BackoffConfig::default(),
            ..ClientConfig::default()
        };
        OperationExecutor::new(
            ClientIdentity::from_parts(ClientId::new(7), "test-client").expect("client identity"),
            gateway,
            metadata_targets,
            &config,
            sleeper,
            metrics,
        )
        .expect("executor")
    }

    fn retry_config(max_retry_attempts: usize) -> RetryConfig {
        RetryConfig {
            max_retry_attempts,
            operation_timeout_ms: None,
        }
    }

    fn assert_metric(events: &[ClientMetricEvent], metric: ClientMetric) {
        assert!(
            events.iter().any(|event| event.metric == metric),
            "missing metric {metric:?}: {events:?}"
        );
    }

    fn refresh_outcome(kind: ErrorKind, reason: MetadataRefreshCause, hint: RpcRefreshHint) -> GatewayOutcome {
        GatewayOutcome::Refresh { kind, reason, hint }
    }

    fn refresh_error(kind: ErrorKind, reason: MetadataRefreshCause, hint: RpcRefreshHint) -> ClientError {
        let rpc_error = match kind {
            ErrorKind::Metadata(MetadataErrorKind::SessionExpired)
            | ErrorKind::Metadata(MetadataErrorKind::SessionInvalid)
            | ErrorKind::Metadata(MetadataErrorKind::Fencing)
            | ErrorKind::Metadata(MetadataErrorKind::EpochMismatch) => {
                RpcErrorDetail::reopen_write_session(kind, hint.clone(), "needs refresh")
            }
            _ => RpcErrorDetail::refresh_metadata(kind, hint.clone(), "needs refresh"),
        };
        ClientError::from(ClientAction::Refresh {
            reason,
            hint: Box::new(client_hint_from_rpc_error(&hint)),
            rpc_error: Box::new(rpc_error),
        })
    }

    fn retryable_error(retry_after_ms: Option<u64>) -> ClientError {
        let rpc_error = RpcErrorDetail::retry(
            ErrorKind::Internal(InternalErrorKind::NodeUnavailable),
            retry_after_ms,
            "retry later",
        );
        ClientError::from(ClientAction::Retry {
            retry_after_ms_hint: retry_after_ms,
            rpc_error: Box::new(rpc_error),
        })
    }

    fn client_hint_from_rpc_error(hint: &RpcRefreshHint) -> crate::rpc_error::RefreshHint {
        let worker_endpoints = hint
            .worker_endpoints
            .iter()
            .cloned()
            .map(crate::rpc_error::EndpointHint::from)
            .collect::<Vec<_>>();
        crate::rpc_error::RefreshHint {
            leader_endpoint: hint.leader_endpoint.clone(),
            group_name: hint.group_name.as_deref().and_then(|name| GroupName::parse(name).ok()),
            route_epoch: hint.route_epoch,
            mount_epoch: hint.mount_epoch,
            mount_prefix: hint.mount_prefix.clone(),
            endpoint_hint: worker_endpoints.first().cloned(),
            worker_endpoints,
            worker_resolve_required: hint.worker_resolve_required,
        }
    }

    fn invalid_header_error(message: impl Into<String>) -> ClientError {
        ClientError::from(ClientAction::Fail {
            rpc_error: Box::new(RpcErrorDetail::fail(
                ErrorKind::Protocol(ProtocolErrorKind::InvalidHeader),
                message,
            )),
        })
    }

    fn assert_invalid_header_not_retryable(err: &ClientError) {
        assert_ne!(
            ErrorClassifier.classify_error(err),
            ErrorClass::RetryableTransport,
            "invalid OK response headers must not enter transport retry handling"
        );
        match action(err) {
            ClientAction::Fail { rpc_error } => {
                assert_eq!(rpc_error.kind, ErrorKind::Protocol(ProtocolErrorKind::InvalidHeader));
                assert_eq!(rpc_error.recovery, RecoveryAction::Fail);
            }
            other => panic!("expected fatal invalid header action, got {other:?}"),
        }
    }

    fn action(err: &ClientError) -> &ClientAction {
        match err {
            ClientError::Action(action) => action.action(),
            other => panic!("expected action error, got {other:?}"),
        }
    }

    fn watermark_proto(group_name: &str, index: u64) -> GroupStateWatermarkProto {
        GroupStateWatermarkProto {
            group_name: group_name.to_string(),
            state_id: Some(RaftLogIdProto {
                term: 1,
                leader_node_id: 1,
                index,
            }),
        }
    }

    fn group_name(raw: &str) -> GroupName {
        GroupName::parse(raw).unwrap()
    }
}
