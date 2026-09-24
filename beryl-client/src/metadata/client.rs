// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Metadata operation execution, retry, and authority-state ownership.

use crate::api::path::NamespacePathBuf;
use crate::api::{DeleteOptions, FileStatus};
use crate::config::ClientConfig;
use crate::error::{
    refresh_hint_from_error, side_effect_response_body_mismatch, timeout_error, ClientError, ClientResult,
};
use crate::metadata::{
    AllocateBlockResult, GrpcMetadataTransport, ListStatusPage, ReadLayout, ValidatedMetadataResponse,
};
use crate::metrics;
use crate::metrics::{ClientMetric, ClientMetricLabels};
use crate::runtime::context::{AttemptContext, ClientIdentity, Operation, OperationContext, OperationDeadline};
use crate::runtime::refresh::MetadataTargets;
use crate::runtime::retry::backoff_delay;
use crate::runtime::{retry_decision, transport_outcome_is_ambiguous, RetryDecision};
use crate::session::{CommitFilePlan, SyncWritePlan, WriteSession};
use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind};
use beryl_proto::common::ByteRangeProto;
use beryl_proto::metadata::get_block_locations_request_proto::Target;
use beryl_proto::metadata::{
    AbortFileWriteRequestProto, AllocateBlockRequestProto, CommitFileRequestProto, CreateDirectoryRequestProto,
    CreateDirectoryResponseProto, CreateFileRequestProto, DeleteOptionsProto, DeleteRequestProto,
    GetBlockLocationsRequestProto, GetStatusRequestProto, ListStatusRequestProto, ListStatusResponseProto,
    MsyncRequestProto, OpenFileRequestProto, OpenWriteModeProto, OpenWriteRequestProto, OpenWriteResponseProto,
    RenameRequestProto, RenewLeaseRequestProto, SyncWriteRequestProto,
};
use beryl_types::{BlockId, ClientId, ContentGeneration, FileType, GroupName, InodeId, WriteHandle, WriteMode};
use std::fmt::{Debug, Formatter, Result};
use std::future::Future;
use std::time::Duration;

const MAX_SERVER_RETRY_AFTER_MS: u64 = 5_000;

/// Owns Metadata operation identity, retry policy, authority state, and the
/// transport used for each selected-endpoint attempt.
pub(crate) struct MetadataClient {
    /// Stable process-local identity reused when creating logical operations.
    identity: ClientIdentity,
    /// Sole Metadata network and wire-validation seam.
    transport: GrpcMetadataTransport,
    /// Client-side route and monotonic authority state learned from Metadata.
    metadata_targets: MetadataTargets,
    /// Bounded retry and absolute operation-timeout configuration.
    max_attempts: usize,
    operation_timeout_ms: u64,
}

impl MetadataClient {
    /// Creates the Metadata owner from validated client-wide dependencies.
    pub(crate) fn new(
        identity: ClientIdentity,
        transport: GrpcMetadataTransport,
        metadata_targets: MetadataTargets,
        config: &ClientConfig,
    ) -> Self {
        Self {
            identity,
            transport,
            metadata_targets,
            max_attempts: config.max_attempts(),
            operation_timeout_ms: config.operation_timeout_ms(),
        }
    }

    /// Starts one absolute deadline shared by all work in a public operation.
    pub(crate) fn operation_deadline(&self) -> OperationDeadline {
        OperationDeadline::new(self.operation_timeout_ms)
    }

    fn operation(&self, operation: Operation, deadline: OperationDeadline) -> OperationContext {
        OperationContext::new_with_identity(&self.identity, operation, deadline)
    }

    /// Returns the current status for a normalized namespace path.
    pub(crate) async fn get_status(&self, path: NamespacePathBuf) -> ClientResult<FileStatus> {
        let path = path.into_string();
        let deadline = self.operation_deadline();
        let operation = self.operation(Operation::GetStatus, deadline);
        let response = self
            .execute_metadata(
                operation,
                GetStatusRequestProto {
                    header: None,
                    path: path.clone(),
                },
                |transport, ctx, req| async move { transport.get_status(ctx, req).await },
            )
            .await?;
        status_from_proto(path, response.status, "GetStatus")
    }

    /// Returns one bounded Metadata-owned directory page.
    pub(crate) async fn list_status_page(
        &self,
        path: NamespacePathBuf,
        cursor: Option<Vec<u8>>,
        page_size: Option<u32>,
    ) -> ClientResult<ListStatusPage> {
        let path = path.into_string();
        let operation = self.operation(Operation::ListStatus, self.operation_deadline());
        let response = self
            .execute_metadata(
                operation,
                ListStatusRequestProto {
                    header: None,
                    path: path.clone(),
                    cursor: cursor.unwrap_or_default(),
                    limit: page_size.unwrap_or(0),
                },
                |transport, ctx, req| async move { transport.list_status(ctx, req).await },
            )
            .await?;
        list_status_page_from_response(path, response)
    }

    /// Creates one directory or ensures the recursive directory chain according
    /// to the existing retry contract.
    pub(crate) async fn mkdirs(&self, path: NamespacePathBuf, create_parent: bool) -> ClientResult<FileStatus> {
        let path = path.into_string();
        let kind = if create_parent {
            Operation::CreateDirectoryRecursive
        } else {
            Operation::CreateDirectory
        };
        let operation = self.operation(kind, self.operation_deadline());
        let request = CreateDirectoryRequestProto {
            header: None,
            path: path.clone(),
            recursive: create_parent,
        };
        let response = self
            .execute_mutation_metadata(operation.clone(), request, |transport, ctx, req| async move {
                transport.create_directory(ctx, req).await
            })
            .await?;
        directory_status_from_response(path, response).map_err(|error| {
            side_effect_response_body_mismatch("CreateDirectory", error).with_operation_context(&operation)
        })
    }

    /// Encodes the explicit delete contract and submits it without transport replay.
    ///
    /// Namespace deletion is side-effecting, so an ambiguous transport outcome
    /// is surfaced to the caller instead of replaying the mutation. Physical
    /// block reclamation remains asynchronous after Metadata commits the
    /// namespace change.
    pub(crate) async fn delete(&self, path: NamespacePathBuf, options: DeleteOptions) -> ClientResult<()> {
        let path = path.into_string();
        let operation = self.operation(Operation::Delete, self.operation_deadline());
        self.execute_mutation_metadata(
            operation,
            DeleteRequestProto {
                header: None,
                path,
                options: Some(DeleteOptionsProto {
                    recursive: options.recursive,
                }),
            },
            |transport, ctx, req| async move { transport.delete(ctx, req).await },
        )
        .await
        .map(|_| ())
    }

    /// Renames a namespace entry without replaying an ambiguous transport result.
    pub(crate) async fn rename(&self, src: NamespacePathBuf, dst: NamespacePathBuf) -> ClientResult<()> {
        let src = src.into_string();
        let dst = dst.into_string();
        let operation = self.operation(Operation::Rename, self.operation_deadline());
        self.execute_mutation_metadata(
            operation,
            RenameRequestProto {
                header: None,
                src_path: src,
                dst_path: dst,
                flags: 0,
            },
            |transport, ctx, req| async move { transport.rename(ctx, req).await },
        )
        .await
        .map(|_| ())
    }

    /// Validates a file open and returns its authoritative inode status.
    pub(crate) async fn open_file(&self, path: NamespacePathBuf) -> ClientResult<FileStatus> {
        let path = path.into_string();
        let operation = self.operation(Operation::OpenFile, self.operation_deadline());
        let response = self
            .execute_metadata(
                operation,
                OpenFileRequestProto {
                    header: None,
                    path: path.clone(),
                },
                |transport, ctx, req| async move { transport.open_file(ctx, req).await },
            )
            .await?;
        let status = status_from_proto(path, response.status, "OpenFile")?;
        if status.kind() != FileType::File {
            return Err(ClientError::invalid_response("OpenFile", "status must describe a file"));
        }
        Ok(status)
    }

    /// Reads an authoritative layout using the bounded read step's identity.
    pub(crate) async fn read_layout_for_inode(
        &self,
        operation: OperationContext,
        inode_id: InodeId,
        offset: u64,
        len: u32,
    ) -> ClientResult<ReadLayout> {
        self.execute_metadata(
            operation,
            GetBlockLocationsRequestProto {
                header: None,
                target: Some(Target::InodeId(inode_id.as_raw())),
                range: Some(ByteRangeProto { offset, len }),
            },
            |transport, ctx, req| async move { transport.read_layout(ctx, req).await },
        )
        .await
    }

    /// Atomically creates a file and validates Metadata's initial write session.
    pub(crate) async fn create_file(&self, path: NamespacePathBuf) -> ClientResult<WriteSession> {
        let path = path.into_string();
        let create_operation = self.operation(Operation::CreateFile, self.operation_deadline());
        let create = self
            .execute_mutation_metadata(
                create_operation.clone(),
                CreateFileRequestProto {
                    header: None,
                    path: path.clone(),
                },
                |transport, ctx, req| async move { transport.create_file(ctx, req).await },
            )
            .await?;
        let block_size = create.block_size;
        beryl_types::validate_block_size(u64::from(block_size)).map_err(|err| {
            side_effect_response_body_mismatch(
                "CreateFile",
                format!("CreateFileResponseProto.block_size invalid: {err}"),
            )
            .with_operation_context(&create_operation)
        })?;
        let write_handle = create.write_handle.ok_or_else(|| {
            side_effect_response_body_mismatch("CreateFile", "CreateFileResponseProto.write_handle missing")
                .with_operation_context(&create_operation)
        })?;
        let write_handle = WriteHandle::try_from(write_handle).map_err(|error| {
            side_effect_response_body_mismatch("CreateFile", error).with_operation_context(&create_operation)
        })?;
        if create.expires_at_ms == 0 {
            return Err(side_effect_response_body_mismatch(
                "CreateFile",
                "CreateFileResponseProto.expires_at_ms must be non-zero",
            )
            .with_operation_context(&create_operation));
        }
        Ok(WriteSession::new(
            path,
            block_size,
            write_handle,
            0,
            create.expires_at_ms,
            ContentGeneration::new(create.generation),
            WriteMode::Overwrite,
        ))
    }

    /// Opens an append session while preserving Metadata's stored layout.
    pub(crate) async fn open_append(&self, path: NamespacePathBuf) -> ClientResult<WriteSession> {
        let path = path.into_string();
        let operation = self.operation(Operation::OpenWrite, self.operation_deadline());
        let response = self
            .execute_mutation_metadata(
                operation.clone(),
                OpenWriteRequestProto {
                    header: None,
                    path: path.clone(),
                    mode: OpenWriteModeProto::OpenWriteModeAppend as i32,
                },
                |transport, ctx, req| async move { transport.open_write(ctx, req).await },
            )
            .await?;
        write_session_from_open_response(&operation, path, response)
    }

    /// Allocates the next Metadata-authorized block and retains its operation
    /// identity for cross-plane target validation.
    /// Retries keep the same handle, predecessor, call identity, and deadline;
    /// Metadata decides whether that predecessor still has a replayable result.
    pub(crate) async fn allocate_block(
        &self,
        write_handle: WriteHandle,
        previous_block_id: Option<BlockId>,
        deadline: OperationDeadline,
    ) -> ClientResult<(OperationContext, AllocateBlockResult)> {
        let operation = self.operation(Operation::AllocateBlock, deadline);
        let result = self
            .execute_mutation_metadata(
                operation.clone(),
                AllocateBlockRequestProto {
                    header: None,
                    write_handle: Some(write_handle.into()),
                    previous_block_id: previous_block_id.map(Into::into),
                },
                |transport, ctx, req| async move { transport.allocate_block(ctx, req).await },
            )
            .await?;
        Ok((operation, result))
    }

    /// Replays only the frozen commit plan and validates its publication size
    /// under the same operation identity.
    pub(crate) async fn commit_file(&self, plan: CommitFilePlan) -> ClientResult<()> {
        let operation = plan.operation.clone();
        let final_len = plan.publication.len;
        let req = CommitFileRequestProto {
            header: None,
            write_handle: Some(plan.publication.write_handle.into()),
            committed_blocks: plan.publication.committed_blocks.iter().map(Into::into).collect(),
            final_len: plan.publication.len,
            expected_generation: plan.publication.expected_generation.as_raw(),
            write_mode: OpenWriteModeProto::from(plan.publication.write_mode) as i32,
            expected_file_len: plan.publication.expected_file_len,
        };
        let response = self
            .execute_mutation_metadata(operation.clone(), req, |transport, ctx, req| async move {
                transport.commit_file(ctx, req).await
            })
            .await?;
        if response.committed_len != final_len {
            return Err(side_effect_response_body_mismatch(
                "CommitFile",
                format!(
                    "committed_len {} does not equal final_len {final_len}",
                    response.committed_len
                ),
            )
            .with_operation_context(&operation));
        }
        Ok(())
    }

    /// Aborts one exact write handle under its frozen operation identity.
    pub(crate) async fn abort_file_write(
        &self,
        operation: OperationContext,
        write_handle: WriteHandle,
    ) -> ClientResult<()> {
        self.execute_mutation_metadata(
            operation,
            AbortFileWriteRequestProto {
                header: None,
                write_handle: Some(write_handle.into()),
            },
            |transport, ctx, req| async move { transport.abort_file_write(ctx, req).await },
        )
        .await
        .map(|_| ())
    }

    /// Renews one active write lease and returns its validated nonzero expiry.
    pub(crate) async fn renew_lease(
        &self,
        write_handle: WriteHandle,
        deadline: OperationDeadline,
    ) -> ClientResult<u64> {
        let operation = self.operation(Operation::RenewLease, deadline);
        let response = self
            .execute_mutation_metadata(
                operation.clone(),
                RenewLeaseRequestProto {
                    header: None,
                    write_handle: Some(write_handle.into()),
                },
                |transport, ctx, req| async move { transport.renew_lease(ctx, req).await },
            )
            .await?;
        if response.expires_at_ms == 0 {
            return Err(
                side_effect_response_body_mismatch("RenewLease", "expires_at_ms must be non-zero")
                    .with_operation_context(&operation),
            );
        }
        Ok(response.expires_at_ms)
    }

    /// Replays only the frozen sync plan and validates its publication size
    /// under the same operation identity.
    pub(crate) async fn sync_write(&self, plan: SyncWritePlan) -> ClientResult<ContentGeneration> {
        let operation = plan.operation.clone();
        let target_len = plan.publication.len;
        let req = SyncWriteRequestProto {
            header: None,
            write_handle: Some(plan.publication.write_handle.into()),
            committed_blocks: plan.publication.committed_blocks.iter().map(Into::into).collect(),
            target_len: plan.publication.len,
            expected_generation: plan.publication.expected_generation.as_raw(),
            write_mode: OpenWriteModeProto::from(plan.publication.write_mode) as i32,
            expected_file_len: plan.publication.expected_file_len,
        };
        let response = self
            .execute_mutation_metadata(operation.clone(), req, |transport, ctx, req| async move {
                transport.sync_write(ctx, req).await
            })
            .await?;
        if response.synced_len != target_len {
            return Err(side_effect_response_body_mismatch(
                "SyncWrite",
                format!(
                    "synced_len {} does not equal target_len {target_len}",
                    response.synced_len
                ),
            )
            .with_operation_context(&operation));
        }
        response.generation.map(ContentGeneration::new).ok_or_else(|| {
            side_effect_response_body_mismatch("SyncWrite", "generation missing").with_operation_context(&operation)
        })
    }

    /// Returns the stable client identity used by Worker operation contexts.
    pub(crate) fn client_id(&self) -> ClientId {
        self.identity.client_id()
    }

    /// Returns the configured client name carried by operation headers.
    pub(crate) fn client_name(&self) -> &str {
        self.identity.client_name()
    }

    /// Executes a mutation under its typed replay policy and keeps ambiguity
    /// sticky until a validated success proves the final outcome.
    async fn execute_mutation_metadata<'a, Req, T, F, Fut>(
        &'a self,
        operation: OperationContext,
        request: Req,
        call: F,
    ) -> ClientResult<T>
    where
        Req: Clone,
        F: FnMut(&'a GrpcMetadataTransport, AttemptContext, Req) -> Fut,
        Fut: Future<Output = ClientResult<ValidatedMetadataResponse<T>>>,
    {
        let operation_name = operation.operation_name();
        let operation_context = operation.clone();
        let (result, saw_transport_ambiguity) = self.execute_metadata_attempts(operation, request, call).await;
        match result {
            Err(err) if saw_transport_ambiguity || err.is_outcome_unknown() || err.is_invalid_success_response() => {
                let unknown = if err.is_outcome_unknown() {
                    err.with_operation_context(&operation_context)
                } else if saw_transport_ambiguity {
                    let message = format!("{operation_name} outcome is unknown after transport ambiguity: {err}");
                    err.with_unknown_outcome(&operation_context, message)
                } else {
                    let message = format!("{operation_name} outcome is unknown after invalid success response: {err}");
                    err.with_unknown_outcome(&operation_context, message)
                };
                Err(unknown)
            }
            Err(err) => Err(err.with_operation_context(&operation_context)),
            Ok(value) => Ok(value),
        }
    }

    /// Executes a read-only Metadata operation and attaches its stable identity
    /// to any terminal failure.
    async fn execute_metadata<'a, Req, T, F, Fut>(
        &'a self,
        operation: OperationContext,
        request: Req,
        call: F,
    ) -> ClientResult<T>
    where
        Req: Clone,
        F: FnMut(&'a GrpcMetadataTransport, AttemptContext, Req) -> Fut,
        Fut: Future<Output = ClientResult<ValidatedMetadataResponse<T>>>,
    {
        let operation_context = operation.clone();
        self.execute_metadata_attempts(operation, request, call)
            .await
            .0
            .map_err(|error| error.with_operation_context(&operation_context))
    }

    /// Runs bounded metadata attempts and applies every validated successful
    /// authority update before returning the corresponding body.
    async fn execute_metadata_attempts<'a, Req, T, F, Fut>(
        &'a self,
        operation: OperationContext,
        request: Req,
        mut call: F,
    ) -> (ClientResult<T>, bool)
    where
        Req: Clone,
        F: FnMut(&'a GrpcMetadataTransport, AttemptContext, Req) -> Fut,
        Fut: Future<Output = ClientResult<ValidatedMetadataResponse<T>>>,
    {
        let target_group = self.metadata_targets.group_name().clone();
        let mut saw_transport_ambiguity = false;
        for attempt_index in 0..self.max_attempts {
            let attempt = attempt_index as u32;
            let endpoint = self.metadata_targets.endpoint(attempt);
            let ctx = AttemptContext::for_metadata(&operation, target_group.clone(), &endpoint)
                .with_state(self.metadata_targets.state_watermark_proto());

            let result = self
                .metadata_rpc_with_deadline(&operation, call(&self.transport, ctx, request.clone()))
                .await;
            let err = match result {
                Ok(response) => {
                    let (authority, body) = response.into_parts();
                    self.metadata_targets.apply_authority_update(authority);
                    return (Ok(body), saw_transport_ambiguity);
                }
                Err(err) => err,
            };
            let decision = retry_decision(&err, operation.retry_safety());
            saw_transport_ambiguity |= transport_outcome_is_ambiguous(&err, operation.retry_safety());
            metrics::record_error(operation.operation_name(), "metadata", &err);
            let has_next = attempt_index + 1 < self.max_attempts;

            match (decision, has_next) {
                (RetryDecision::Retry, true) => {
                    if err.is_retryable_transport() && !err.is_definitely_before_side_effect() {
                        self.metadata_targets.record_transport_failure(&endpoint);
                    }
                    self.record_retry(&operation, &err);
                    let delay = server_retry_delay(&err).unwrap_or_else(|| backoff_delay(attempt_index));
                    if let Err(err) = self.sleep_with_deadline(&operation, delay).await {
                        return (Err(err), saw_transport_ambiguity);
                    }
                }
                (RetryDecision::RefreshMetadata(kind), true) => {
                    let hint = refresh_hint_from_error(&err);
                    if let Err(err) = self.metadata_targets.record_refresh(kind, &hint) {
                        return (Err(err), saw_transport_ambiguity);
                    }
                    if kind == ErrorKind::Metadata(MetadataErrorKind::StaleState) {
                        if let Err(err) = self
                            .refresh_state(&operation, target_group.clone(), attempt.saturating_add(1))
                            .await
                        {
                            return (Err(err), saw_transport_ambiguity);
                        }
                    }
                    self.record_retry(&operation, &err);
                }
                (RetryDecision::Retry | RetryDecision::RefreshMetadata(_), false) => {
                    metrics::record(
                        ClientMetric::RetryExhausted,
                        metadata_labels(&operation).with_error_class(err.classification_label()),
                    );
                    return (Err(err), saw_transport_ambiguity);
                }
                (RetryDecision::Return, _) => return (Err(err), saw_transport_ambiguity),
            }
        }
        unreachable!("validated retry limit permits at least one attempt; the final attempt returns")
    }

    async fn refresh_state(
        &self,
        parent: &OperationContext,
        target_group: GroupName,
        attempt: u32,
    ) -> ClientResult<()> {
        let endpoint = self.metadata_targets.endpoint(attempt);
        let operation = self.operation(Operation::Msync, parent.deadline().clone());
        let ctx = AttemptContext::for_metadata(&operation, target_group, endpoint);
        let response = self
            .metadata_rpc_with_deadline(
                &operation,
                self.transport.msync(ctx, MsyncRequestProto { header: None }),
            )
            .await?;
        let (authority, _) = response.into_parts();
        self.metadata_targets.apply_authority_update(authority);
        Ok(())
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

    fn record_retry(&self, operation: &OperationContext, error: &ClientError) {
        metrics::record(
            ClientMetric::RetryAttempt,
            metadata_labels(operation).with_error_class(error.classification_label()),
        );
    }

    fn record_timeout(&self, operation: &OperationContext) {
        metrics::record(
            ClientMetric::RpcTimeout,
            metadata_labels(operation)
                .with_error_class("retryable_transport")
                .with_outcome("timeout"),
        );
    }
}

impl Debug for MetadataClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.debug_struct("MetadataClient")
            .field("client_id", &self.identity.client_id())
            .field("client_name", &self.identity.client_name())
            .field("metadata_targets", &self.metadata_targets)
            .field("max_attempts", &self.max_attempts)
            .field("operation_timeout_ms", &self.operation_timeout_ms)
            .finish_non_exhaustive()
    }
}

fn metadata_labels(operation: &OperationContext) -> ClientMetricLabels {
    ClientMetricLabels::default().with_operation(operation.operation_name(), "metadata")
}

fn server_retry_delay(err: &ClientError) -> Option<Duration> {
    err.retry_after()
        .map(|delay| delay.min(Duration::from_millis(MAX_SERVER_RETRY_AFTER_MS)))
}

/// Converts a validated open-write response into the sole client-side session
/// state consumed by `FileWriter`.
fn write_session_from_open_response(
    operation: &OperationContext,
    path: String,
    (group, response): (GroupName, OpenWriteResponseProto),
) -> ClientResult<WriteSession> {
    let block_size = response.block_size;
    beryl_types::validate_block_size(u64::from(block_size)).map_err(|err| {
        side_effect_response_body_mismatch("OpenWrite", format!("OpenWriteResponseProto.block_size invalid: {err}"))
            .with_operation_context(operation)
    })?;
    let write_handle = response.write_handle.ok_or_else(|| {
        side_effect_response_body_mismatch("OpenWrite", "OpenWriteResponseProto.write_handle missing")
            .with_operation_context(operation)
    })?;
    let write_handle = WriteHandle::try_from(write_handle)
        .map_err(|error| side_effect_response_body_mismatch("OpenWrite", error).with_operation_context(operation))?;
    if response.expires_at_ms == 0 {
        return Err(
            side_effect_response_body_mismatch("OpenWrite", "expires_at_ms must be non-zero")
                .with_operation_context(operation),
        );
    }
    let mut session = WriteSession::new(
        path,
        block_size,
        write_handle,
        response.base_len,
        response.expires_at_ms,
        ContentGeneration::new(response.generation),
        WriteMode::Append,
    );
    let tail = response
        .tail_block
        .map(TryInto::try_into)
        .transpose()
        .map_err(|error: String| {
            side_effect_response_body_mismatch("OpenWrite", error).with_operation_context(operation)
        })?;
    session
        .accept_open_tail(group, tail)
        .map_err(|error| side_effect_response_body_mismatch("OpenWrite", error).with_operation_context(operation))?;
    Ok(session)
}

fn status_from_proto(
    path: String,
    status: Option<beryl_proto::metadata::FileStatusProto>,
    operation: &'static str,
) -> ClientResult<FileStatus> {
    let status = status.ok_or_else(|| ClientError::invalid_response(operation, "status missing"))?;
    let mut status = FileStatus::try_from(status).map_err(|err| ClientError::invalid_response(operation, err))?;
    status.path = Some(path);
    Ok(status)
}

fn directory_status_from_response(path: String, response: CreateDirectoryResponseProto) -> ClientResult<FileStatus> {
    let status = status_from_proto(path, response.status, "CreateDirectory")?;
    if status.kind() != FileType::Dir {
        return Err(ClientError::invalid_response(
            "CreateDirectory",
            "status must describe a directory",
        ));
    }
    Ok(status)
}

/// Converts a successful wire page while requiring progress before continuation.
fn list_status_page_from_response(path: String, response: ListStatusResponseProto) -> ClientResult<ListStatusPage> {
    if !response.next_cursor.is_empty() && response.entries.is_empty() {
        return Err(ClientError::invalid_response(
            "ListStatus",
            "non-EOF page must contain entries",
        ));
    }
    let next_cursor = if response.next_cursor.is_empty() {
        None
    } else {
        Some(response.next_cursor)
    };
    let entries = response
        .entries
        .into_iter()
        .map(|entry| {
            if entry.name.is_empty() || entry.name.contains('/') {
                return Err(ClientError::invalid_response(
                    "ListStatus",
                    format!("invalid direct-child name: {:?}", entry.name),
                ));
            }
            let parent = path.trim_end_matches('/');
            let child_path = format!("{parent}/{}", entry.name);
            status_from_proto(child_path, entry.status, "ListStatus")
        })
        .collect::<ClientResult<Vec<_>>>()?;
    Ok(ListStatusPage { entries, next_cursor })
}
