// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Metadata RPC execution with one call ID per RPC and one deadline per public operation.

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind};
use beryl_types::{BlockId, CommittedBlock, DataHandleId, FileLayout};

use crate::api::handle::{ReadHandle, WriteHandle};
use crate::api::options::{DEFAULT_BLOCK_SIZE, DEFAULT_REPLICATION, MAX_PREALLOCATED_WRITE_BLOCKS};
use crate::api::path::NamespacePathBuf;
use crate::api::{CreateMode, CreateOptions, DirectoryEntry, DirectoryListing, FileStatus, ListOptions};
use crate::config::{ClientConfig, RetryConfig};
use crate::error::{ClientError, ClientResult};
use crate::metadata::{AddBlockResult, MetadataGateway, ReadLayout};
use crate::metrics::{ClientMetric, ClientMetricEvent, ClientMetricLabels, ClientMetrics};
use crate::rpc_error::ClientAction;
use crate::runtime::classify::{classify_error, ErrorClass};
use crate::runtime::context::{AttemptContext, ClientIdentity, OperationContext, OperationDeadline};
use crate::runtime::refresh::MetadataTargets;
use crate::session::write_session::{CommitFilePlan, WriteSession};

const INITIAL_BACKOFF_MS: u64 = 100;
const MAX_BACKOFF_MS: u64 = 2_000;
const MAX_SERVER_RETRY_AFTER_MS: u64 = 5_000;

#[derive(Clone)]
pub(crate) struct MetadataExecutor {
    identity: ClientIdentity,
    gateway: Arc<dyn MetadataGateway>,
    metadata_targets: MetadataTargets,
    retry: RetryConfig,
    metrics: Arc<dyn ClientMetrics>,
}

impl MetadataExecutor {
    pub(crate) fn new(
        identity: ClientIdentity,
        gateway: Arc<dyn MetadataGateway>,
        metadata_targets: MetadataTargets,
        config: &ClientConfig,
        metrics: Arc<dyn ClientMetrics>,
    ) -> ClientResult<Self> {
        Ok(Self {
            identity,
            gateway,
            metadata_targets,
            retry: config.retry.clone(),
            metrics,
        })
    }

    pub(crate) fn operation_deadline(&self) -> OperationDeadline {
        OperationDeadline::new(self.retry.operation_timeout_ms)
    }

    fn operation(
        &self,
        name: &'static str,
        route_path: Option<String>,
        deadline: OperationDeadline,
    ) -> ClientResult<OperationContext> {
        OperationContext::new_with_identity(&self.identity, name, route_path, deadline)
    }

    pub(crate) async fn stat(&self, path: NamespacePathBuf) -> ClientResult<FileStatus> {
        let path = path.into_string();
        let deadline = self.operation_deadline();
        let operation = self.operation("GetStatus", Some(path.clone()), deadline)?;
        let response = self
            .execute_metadata(
                operation,
                beryl_proto::metadata::GetStatusRequestProto {
                    header: None,
                    path: path.clone(),
                },
                |gateway, ctx, req| async move { gateway.get_status(ctx, req).await },
            )
            .await?;
        file_status_from_response(path, response)
    }

    pub(crate) async fn list(&self, path: NamespacePathBuf, options: ListOptions) -> ClientResult<DirectoryListing> {
        let path = path.into_string();
        let operation = self.operation("ListStatus", Some(path.clone()), self.operation_deadline())?;
        let response = self
            .execute_metadata(
                operation,
                beryl_proto::metadata::ListStatusRequestProto {
                    header: None,
                    path: path.clone(),
                    recursive: options.recursive,
                    cursor: options.cursor.unwrap_or_default(),
                    limit: options.limit.unwrap_or(0),
                },
                |gateway, ctx, req| async move { gateway.list_status(ctx, req).await },
            )
            .await?;
        Ok(directory_listing_from_response(path, response))
    }

    pub(crate) async fn create_directory(&self, path: NamespacePathBuf, recursive: bool) -> ClientResult<FileStatus> {
        let path = path.into_string();
        let operation = self.operation("CreateDirectory", Some(path.clone()), self.operation_deadline())?;
        let response = self
            .execute_mutation_metadata(
                operation,
                beryl_proto::metadata::CreateDirectoryRequestProto {
                    header: None,
                    path: path.clone(),
                    attrs: Some(default_dir_attrs()),
                    recursive,
                },
                |gateway, ctx, req| async move { gateway.create_directory(ctx, req).await },
            )
            .await?;
        directory_status_from_response(path, response)
    }

    pub(crate) async fn delete(&self, path: NamespacePathBuf, recursive: bool) -> ClientResult<()> {
        let path = path.into_string();
        let operation = self.operation("Delete", Some(path.clone()), self.operation_deadline())?;
        self.execute_mutation_metadata(
            operation,
            beryl_proto::metadata::DeleteRequestProto {
                header: None,
                path,
                recursive,
            },
            |gateway, ctx, req| async move { gateway.delete(ctx, req).await },
        )
        .await
        .map(|_| ())
    }

    pub(crate) async fn rename(&self, src: NamespacePathBuf, dst: NamespacePathBuf) -> ClientResult<()> {
        let src = src.into_string();
        let dst = dst.into_string();
        let operation = self.operation("Rename", Some(src.clone()), self.operation_deadline())?;
        self.execute_mutation_metadata(
            operation,
            beryl_proto::metadata::RenameRequestProto {
                header: None,
                src_path: src,
                dst_path: dst,
                flags: 0,
            },
            |gateway, ctx, req| async move { gateway.rename(ctx, req).await },
        )
        .await
        .map(|_| ())
    }

    pub(crate) async fn open_file(&self, path: NamespacePathBuf) -> ClientResult<ReadHandle> {
        let path = path.into_string();
        let operation = self.operation("OpenFile", Some(path.clone()), self.operation_deadline())?;
        let response = self
            .execute_metadata(
                operation,
                beryl_proto::metadata::OpenFileRequestProto {
                    header: None,
                    path: path.clone(),
                },
                |gateway, ctx, req| async move { gateway.open_file(ctx, req).await },
            )
            .await?;
        let data_handle_id = response
            .data_handle_id
            .ok_or_else(|| ClientError::Metadata("OpenFileResponseProto.data_handle_id missing".to_string()))?;
        let file_version = response
            .file_version
            .ok_or_else(|| ClientError::Metadata("OpenFileResponseProto.file_version missing".to_string()))?;
        Ok(ReadHandle::new(
            path,
            DataHandleId::new(data_handle_id.value),
            file_version,
            response.file_size,
        ))
    }

    pub(crate) async fn read_layout(
        &self,
        path: &str,
        req: beryl_proto::metadata::GetBlockLocationsRequestProto,
        deadline: OperationDeadline,
    ) -> ClientResult<ReadLayout> {
        let operation = self.operation("GetBlockLocations", Some(path.to_string()), deadline)?;
        self.execute_metadata(operation, req, |gateway, ctx, req| async move {
            gateway.read_layout(ctx, req).await
        })
        .await
    }

    pub(crate) async fn read_layout_for_data_handle(
        &self,
        path: &str,
        data_handle_id: DataHandleId,
        offset: u64,
        len: u32,
        deadline: OperationDeadline,
    ) -> ClientResult<ReadLayout> {
        self.read_layout(
            path,
            beryl_proto::metadata::GetBlockLocationsRequestProto {
                header: None,
                target: Some(
                    beryl_proto::metadata::get_block_locations_request_proto::Target::DataHandleId(
                        beryl_proto::common::DataHandleIdProto {
                            value: data_handle_id.as_raw(),
                        },
                    ),
                ),
                range: Some(beryl_proto::common::ByteRangeProto { offset, len }),
            },
            deadline,
        )
        .await
    }

    pub(crate) async fn create_file(
        &self,
        path: NamespacePathBuf,
        options: CreateOptions,
    ) -> ClientResult<WriteHandle> {
        let path = path.into_string();
        let deadline = self.operation_deadline();
        let create_mode = match options.create_mode {
            CreateMode::CreateNew => beryl_proto::metadata::CreateModeProto::CreateNew,
            CreateMode::CreateOrOverwrite => beryl_proto::metadata::CreateModeProto::CreateOrOverwrite,
        };
        let create = self
            .execute_mutation_metadata(
                self.operation("CreateFile", Some(path.clone()), deadline.clone())?,
                beryl_proto::metadata::CreateFileRequestProto {
                    header: None,
                    path: path.clone(),
                    attrs: Some(default_file_attrs()),
                    layout: Some(layout_for_new_file(&options)?),
                    create_mode: create_mode as i32,
                },
                |gateway, ctx, req| async move { gateway.create_file(ctx, req).await },
            )
            .await?;
        let created_data_handle = create
            .data_handle_id
            .ok_or_else(|| ClientError::Metadata("CreateFileResponseProto.data_handle_id missing".to_string()))?;
        let created_layout = create
            .layout
            .ok_or_else(|| ClientError::Metadata("CreateFileResponseProto.layout missing".to_string()))?;
        let created_layout = FileLayout::try_from(created_layout)
            .map_err(|err| ClientError::InvalidLayout(format!("CreateFileResponseProto.layout invalid: {err}")))?;
        let open = self
            .open_write_request(
                &path,
                beryl_proto::metadata::OpenWriteModeProto::OpenWriteModeWrite,
                deadline,
            )
            .await?;
        if open.data_handle_id.as_ref().map(|id| id.value) != Some(created_data_handle.value) {
            return Err(ClientError::Metadata(
                "OpenWrite returned a different data_handle_id than CreateFile".to_string(),
            ));
        }
        let open_layout = open
            .layout
            .ok_or_else(|| ClientError::Metadata("OpenWriteResponseProto.layout missing".to_string()))?;
        let open_layout = FileLayout::try_from(open_layout)
            .map_err(|err| ClientError::InvalidLayout(format!("OpenWriteResponseProto.layout invalid: {err}")))?;
        if open_layout != created_layout {
            return Err(ClientError::Metadata(
                "OpenWrite returned a different layout than CreateFile".to_string(),
            ));
        }
        write_handle_from_open_response(path, open)
    }

    pub(crate) async fn open_append(&self, path: NamespacePathBuf) -> ClientResult<WriteHandle> {
        let path = path.into_string();
        let open = self
            .open_write_request(
                &path,
                beryl_proto::metadata::OpenWriteModeProto::OpenWriteModeAppend,
                self.operation_deadline(),
            )
            .await?;
        write_handle_from_open_response(path, open)
    }

    async fn open_write_request(
        &self,
        path: &str,
        mode: beryl_proto::metadata::OpenWriteModeProto,
        deadline: OperationDeadline,
    ) -> ClientResult<beryl_proto::metadata::OpenWriteResponseProto> {
        self.execute_mutation_metadata(
            self.operation("OpenWrite", Some(path.to_string()), deadline)?,
            beryl_proto::metadata::OpenWriteRequestProto {
                header: None,
                path: path.to_string(),
                mode: mode as i32,
                desired_len: Some(default_write_preallocation_len()),
            },
            |gateway, ctx, req| async move {
                match gateway.open_write(ctx, req).await {
                    Err(err) if matches!(classify_error(&err), ErrorClass::RetryableTransport) => {
                        Err(ClientError::UnknownOutcome(format!(
                            "OpenWrite outcome is unknown after transport ambiguity: {err}"
                        )))
                    }
                    result => result,
                }
            },
        )
        .await
    }

    pub(crate) async fn add_block(
        &self,
        path: &str,
        write_handle: beryl_proto::metadata::WriteHandleProto,
        desired_len: u64,
        previous_block_id: Option<BlockId>,
        deadline: OperationDeadline,
    ) -> ClientResult<AddBlockResult> {
        self.execute_mutation_metadata(
            self.operation("AddBlock", Some(path.to_string()), deadline)?,
            beryl_proto::metadata::AddBlockRequestProto {
                header: None,
                write_handle: Some(write_handle),
                desired_len: Some(desired_len),
                previous_block_id: previous_block_id.map(Into::into),
            },
            |gateway, ctx, req| async move { gateway.add_block(ctx, req).await },
        )
        .await
    }

    pub(crate) async fn commit_file(
        &self,
        plan: CommitFilePlan,
    ) -> ClientResult<beryl_proto::metadata::CommitFileResponseProto> {
        let req = beryl_proto::metadata::CommitFileRequestProto {
            header: None,
            write_handle: Some(plan.write_handle),
            data_handle_id: Some(beryl_proto::common::DataHandleIdProto {
                value: plan.data_handle_id.as_raw(),
            }),
            committed_blocks: plan.committed_blocks.iter().map(Into::into).collect(),
            final_size: plan.final_size,
        };
        self.execute_mutation_metadata(plan.operation, req, |gateway, ctx, req| async move {
            gateway.commit_file(ctx, req).await
        })
        .await
    }

    pub(crate) async fn abort_file_write(
        &self,
        operation: OperationContext,
        write_handle: beryl_proto::metadata::WriteHandleProto,
    ) -> ClientResult<beryl_proto::metadata::AbortFileWriteResponseProto> {
        self.execute_mutation_metadata(
            operation,
            beryl_proto::metadata::AbortFileWriteRequestProto {
                header: None,
                write_handle: Some(write_handle),
            },
            |gateway, ctx, req| async move { gateway.abort_file_write(ctx, req).await },
        )
        .await
    }

    pub(crate) async fn renew_lease(
        &self,
        path: &str,
        write_handle: beryl_proto::metadata::WriteHandleProto,
        deadline: OperationDeadline,
    ) -> ClientResult<beryl_proto::metadata::RenewLeaseResponseProto> {
        self.execute_mutation_metadata(
            self.operation("RenewLease", Some(path.to_string()), deadline)?,
            beryl_proto::metadata::RenewLeaseRequestProto {
                header: None,
                write_handle: Some(write_handle),
            },
            |gateway, ctx, req| async move { gateway.renew_lease(ctx, req).await },
        )
        .await
    }

    pub(crate) async fn sync_write(
        &self,
        session: &WriteSession,
        data_handle_id: DataHandleId,
        committed_blocks: Vec<CommittedBlock>,
        target_size: u64,
        mode: beryl_proto::metadata::WriteSyncModeProto,
        deadline: OperationDeadline,
    ) -> ClientResult<beryl_proto::metadata::SyncWriteResponseProto> {
        let req = beryl_proto::metadata::SyncWriteRequestProto {
            header: None,
            write_handle: Some(session.write_handle()),
            data_handle_id: Some(beryl_proto::common::DataHandleIdProto {
                value: data_handle_id.as_raw(),
            }),
            committed_blocks: committed_blocks.iter().map(Into::into).collect(),
            target_size,
            mode: mode as i32,
            flags: 0,
        };
        self.execute_mutation_metadata(
            self.operation("SyncWrite", Some(session.path().to_string()), deadline)?,
            req,
            |gateway, ctx, req| async move { gateway.sync_write(ctx, req).await },
        )
        .await
    }

    pub(crate) fn client_id(&self) -> beryl_types::ClientId {
        self.identity.client_id()
    }

    pub(crate) fn client_name(&self) -> &str {
        self.identity.client_name()
    }

    pub(crate) fn record_data_refresh(
        &self,
        operation: &OperationContext,
        kind: ErrorKind,
        hint: &crate::rpc_error::RefreshHint,
    ) -> ClientResult<()> {
        self.metadata_targets.record_refresh(operation, kind, hint)
    }

    async fn execute_mutation_metadata<Req, T, F, Fut>(
        &self,
        operation: OperationContext,
        request: Req,
        call: F,
    ) -> ClientResult<T>
    where
        Req: Clone,
        F: FnMut(Arc<dyn MetadataGateway>, AttemptContext, Req) -> Fut,
        Fut: Future<Output = ClientResult<T>>,
    {
        let operation_name = operation.operation_name();
        match self.execute_metadata(operation, request, call).await {
            Err(err) if matches!(classify_error(&err), ErrorClass::RetryableTransport) => {
                let unknown = ClientError::UnknownOutcome(format!(
                    "{operation_name} outcome is unknown after transport ambiguity: {err}"
                ));
                self.record_metric(
                    ClientMetric::UnknownOutcome,
                    ClientMetricLabels::default()
                        .with_operation(operation_name, "metadata")
                        .with_error_class(ErrorClass::UnknownOutcome.label())
                        .with_outcome("unknown"),
                );
                Err(unknown)
            }
            result => result,
        }
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
        for attempt_index in 0..self.retry.max_attempts() {
            let attempt = attempt_index as u32;
            let endpoint = self.metadata_targets.endpoint_for_group(&target_group, attempt)?;
            let mut ctx = AttemptContext::for_metadata(&operation, target_group.clone(), attempt)?
                .with_metadata_endpoint(&endpoint);
            ctx = self.metadata_targets.enrich_attempt_context(&operation, ctx);
            if let Some(watermark) = self.metadata_targets.state_watermark_proto(&target_group) {
                ctx = ctx.with_state(vec![watermark]);
            }

            let result = self
                .metadata_rpc_with_deadline(&operation, call(Arc::clone(&self.gateway), ctx, request.clone()))
                .await;
            let Err(err) = result else {
                return result;
            };
            let class = classify_error(&err);
            self.record_error_metric(&operation, &class);
            let has_next = attempt_index + 1 < self.retry.max_attempts();

            match class {
                ErrorClass::RetryableTransport if has_next => {
                    self.metadata_targets.record_transport_failure(&target_group, &endpoint);
                    self.record_retry(&operation, &class);
                    self.sleep_with_deadline(&operation, backoff_delay(attempt_index))
                        .await?;
                }
                ErrorClass::ServerRetry if has_next => {
                    self.record_retry(&operation, &class);
                    let delay = server_retry_delay(&err).unwrap_or_else(|| backoff_delay(attempt_index));
                    self.sleep_with_deadline(&operation, delay).await?;
                }
                ErrorClass::RefreshMetadata(kind) if has_next => {
                    let hint = refresh_hint_from_error(&err);
                    self.metadata_targets.record_refresh(&operation, kind, &hint)?;
                    if kind == ErrorKind::Metadata(MetadataErrorKind::StaleState) {
                        self.refresh_state(&operation, target_group.clone(), attempt.saturating_add(1))
                            .await?;
                    }
                    target_group = self.metadata_targets.group_for_operation(&operation)?;
                    self.record_retry(&operation, &ErrorClass::RefreshMetadata(kind));
                }
                ErrorClass::RetryableTransport | ErrorClass::ServerRetry | ErrorClass::RefreshMetadata(_) => {
                    self.record_metric(
                        ClientMetric::RetryExhausted,
                        metadata_labels(&operation).with_error_class(class.label()),
                    );
                    return Err(err);
                }
                ErrorClass::UnknownOutcome => {
                    self.record_metric(
                        ClientMetric::UnknownOutcome,
                        metadata_labels(&operation)
                            .with_error_class(class.label())
                            .with_outcome("unknown"),
                    );
                    return Err(err);
                }
                _ => return Err(err),
            }
        }
        Err(ClientError::Metadata(format!(
            "{} exhausted attempts",
            operation.operation_name()
        )))
    }

    async fn refresh_state(
        &self,
        parent: &OperationContext,
        target_group: beryl_types::GroupName,
        attempt: u32,
    ) -> ClientResult<()> {
        let endpoint = self.metadata_targets.endpoint_for_group(&target_group, attempt)?;
        let operation = self.operation(
            "Msync",
            parent.original_target_path().map(ToOwned::to_owned),
            parent.deadline().clone(),
        )?;
        let ctx = AttemptContext::for_metadata(&operation, target_group, 0)?.with_metadata_endpoint(endpoint);
        let watermark = self
            .metadata_rpc_with_deadline(
                &operation,
                self.gateway
                    .msync(ctx, beryl_proto::metadata::MsyncRequestProto { header: None }),
            )
            .await?;
        self.metadata_targets.record_state_watermark(watermark)
    }

    async fn metadata_rpc_with_deadline<T, Fut>(&self, operation: &OperationContext, future: Fut) -> ClientResult<T>
    where
        Fut: Future<Output = ClientResult<T>>,
    {
        let remaining = operation.deadline().remaining();
        if remaining.is_zero() {
            self.record_timeout(operation);
            return Err(timeout_error("metadata", operation.operation_name()));
        }
        match tokio::time::timeout(remaining, future).await {
            Ok(result) => result,
            Err(_) => {
                self.record_timeout(operation);
                Err(timeout_error("metadata", operation.operation_name()))
            }
        }
    }

    async fn sleep_with_deadline(&self, operation: &OperationContext, delay: Duration) -> ClientResult<()> {
        let remaining = operation.deadline().remaining();
        if remaining.is_zero() || delay >= remaining {
            self.record_timeout(operation);
            return Err(timeout_error("metadata", operation.operation_name()));
        }
        tokio::time::sleep(delay).await;
        Ok(())
    }

    fn record_retry(&self, operation: &OperationContext, class: &ErrorClass) {
        self.record_metric(
            ClientMetric::RetryAttempt,
            metadata_labels(operation).with_error_class(class.label()),
        );
    }

    fn record_timeout(&self, operation: &OperationContext) {
        self.record_metric(
            ClientMetric::RpcTimeout,
            metadata_labels(operation)
                .with_error_class(ErrorClass::RetryableTransport.label())
                .with_outcome("timeout"),
        );
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
            self.record_metric(metric, metadata_labels(operation).with_error_class(class.label()));
        }
    }

    fn record_metric(&self, metric: ClientMetric, labels: ClientMetricLabels) {
        self.metrics.record(ClientMetricEvent::new(metric, labels));
    }
}

impl fmt::Debug for MetadataExecutor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetadataExecutor")
            .field("client_id", &self.identity.client_id())
            .field("client_name", &self.identity.client_name())
            .field("metadata_targets", &self.metadata_targets)
            .field("retry", &self.retry)
            .finish_non_exhaustive()
    }
}

fn metadata_labels(operation: &OperationContext) -> ClientMetricLabels {
    ClientMetricLabels::default().with_operation(operation.operation_name(), "metadata")
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

fn server_retry_delay(err: &ClientError) -> Option<Duration> {
    let ClientError::Action(action) = err else {
        return None;
    };
    let ClientAction::Retry {
        retry_after_ms_hint, ..
    } = action.action()
    else {
        return None;
    };
    retry_after_ms_hint.map(|delay| Duration::from_millis(delay.min(MAX_SERVER_RETRY_AFTER_MS)))
}

fn backoff_delay(retry_index: usize) -> Duration {
    let shift = retry_index.min(20) as u32;
    Duration::from_millis(INITIAL_BACKOFF_MS.saturating_mul(1u64 << shift).min(MAX_BACKOFF_MS))
}

fn timeout_error(target_plane: &str, operation: &str) -> ClientError {
    ClientError::from(tonic::Status::deadline_exceeded(format!(
        "{target_plane} {operation} exceeded the public operation deadline"
    )))
}

fn default_write_preallocation_len() -> u64 {
    u64::from(DEFAULT_BLOCK_SIZE) * MAX_PREALLOCATED_WRITE_BLOCKS
}

fn default_file_attrs() -> beryl_proto::fs::FileAttrsProto {
    beryl_proto::fs::FileAttrsProto {
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

fn default_dir_attrs() -> beryl_proto::fs::FileAttrsProto {
    beryl_proto::fs::FileAttrsProto {
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

fn layout_for_new_file(options: &CreateOptions) -> ClientResult<beryl_proto::common::FileLayoutProto> {
    let layout = FileLayout::with_block_format(
        options.block_size,
        options.chunk_size,
        DEFAULT_REPLICATION,
        options.block_format_id,
    );
    layout
        .validate()
        .map_err(|err| ClientError::InvalidLayout(format!("CreateOptions layout invalid: {err}")))?;
    Ok((&layout).into())
}

fn write_handle_from_open_response(
    path: String,
    response: beryl_proto::metadata::OpenWriteResponseProto,
) -> ClientResult<WriteHandle> {
    let data_handle_id = response
        .data_handle_id
        .ok_or_else(|| ClientError::Metadata("OpenWriteResponseProto.data_handle_id missing".to_string()))?;
    let layout = response
        .layout
        .ok_or_else(|| ClientError::Metadata("OpenWriteResponseProto.layout missing".to_string()))?;
    let layout = FileLayout::try_from(layout)
        .map_err(|err| ClientError::InvalidLayout(format!("OpenWriteResponseProto.layout invalid: {err}")))?;
    let write_handle = response
        .write_handle
        .ok_or_else(|| ClientError::Metadata("OpenWriteResponseProto.write_handle missing".to_string()))?;
    let expires_at_ms = crate::api::handle::valid_write_session_expiry("OpenWrite", response.expires_at_ms)?;
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

fn file_status_from_response(
    path: String,
    response: beryl_proto::metadata::GetStatusResponseProto,
) -> ClientResult<FileStatus> {
    let attrs = response
        .attrs
        .ok_or_else(|| ClientError::Metadata("GetStatusResponseProto.attrs missing".to_string()))?;
    Ok(FileStatus::new(path, attrs.into()))
}

fn directory_status_from_response(
    path: String,
    response: beryl_proto::metadata::CreateDirectoryResponseProto,
) -> ClientResult<FileStatus> {
    let attrs = response
        .attrs
        .ok_or_else(|| ClientError::Metadata("CreateDirectoryResponseProto.attrs missing".to_string()))?;
    Ok(FileStatus::new(path, attrs.into()))
}

fn directory_listing_from_response(
    path: String,
    response: beryl_proto::metadata::ListStatusResponseProto,
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
            let kind = beryl_proto::fs::InodeKindProto::try_from(entry.kind)
                .ok()
                .and_then(|kind| kind.try_into().ok());
            DirectoryEntry::new(entry.name, kind, entry.attrs.map(Into::into))
        })
        .collect();
    DirectoryListing::new(path, entries, next_cursor, response.eof)
}
