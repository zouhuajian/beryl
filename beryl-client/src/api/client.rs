// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Public filesystem-facing facade.

use std::fmt;
use std::sync::Arc;
use std::vec::IntoIter;

use futures::{stream, StreamExt};

use super::{
    CreateOptions, DeleteOptions, DirectoryEntry, DirectoryListing, FileReader, FileStatus, FileWriter, ListOptions,
};
use crate::api::path::NamespacePathBuf;
use crate::config::ClientConfig;
use crate::data::WorkerDataPlane;
use crate::error::{invalid_response, ClientResult};
use crate::metadata::{GrpcMetadataGateway, MetadataGateway};
use crate::metrics::{ClientMetrics, NoopClientMetrics};
use crate::runtime::{ClientRuntime, MetadataTargets};

/// Public filesystem-facing client facade.
#[derive(Clone)]
pub struct FsClient {
    /// Shared runtime state reused by this facade and the handles it opens.
    pub(crate) runtime: Arc<ClientRuntime>,
}

/// Owned state for a lazily paginated public directory-entry stream.
struct DirectoryListStreamState {
    client: FsClient,
    path: NamespacePathBuf,
    options: ListOptions,
    buffered_entries: IntoIter<DirectoryEntry>,
    eof: bool,
}

impl FsClient {
    /// Create a new filesystem client facade.
    pub fn new(config: ClientConfig) -> Self {
        Self::try_new(config).expect("valid client metadata configuration")
    }

    /// Create a new filesystem client facade and return configuration errors.
    pub fn try_new(config: ClientConfig) -> ClientResult<Self> {
        let metrics: Arc<dyn ClientMetrics> = Arc::new(NoopClientMetrics);
        Self::try_new_with_metrics(config, metrics)
    }

    /// Create a new filesystem client facade with an injected metrics recorder.
    pub fn try_new_with_metrics(config: ClientConfig, metrics: Arc<dyn ClientMetrics>) -> ClientResult<Self> {
        let metadata_targets = MetadataTargets::from_config(&config)?;
        let gateway = Arc::new(GrpcMetadataGateway::new_lazy_with_config(
            &config,
            Arc::clone(&metrics),
        )?);
        let data_plane = WorkerDataPlane::from_config(&config, Arc::clone(&metrics));

        Self::with_runtime_hooks(config, gateway, metadata_targets, data_plane, metrics)
    }

    /// Builds a client with injected runtime dependencies for tests and internal wiring.
    pub(crate) fn with_runtime_hooks(
        config: ClientConfig,
        gateway: Arc<dyn MetadataGateway>,
        metadata_targets: MetadataTargets,
        data_plane: WorkerDataPlane,
        metrics: Arc<dyn ClientMetrics>,
    ) -> ClientResult<Self> {
        Ok(Self {
            runtime: Arc::new(ClientRuntime::new(
                config,
                gateway,
                metadata_targets,
                data_plane,
                metrics,
            )?),
        })
    }

    /// Return the client configuration.
    pub fn config(&self) -> &ClientConfig {
        &self.runtime.config
    }

    /// Return file or directory status through the metadata runtime.
    pub async fn stat(&self, path: &str) -> ClientResult<FileStatus> {
        let path = NamespacePathBuf::parse(path)?;
        self.runtime.executor.stat(path).await
    }

    /// Lists one bounded directory page using explicit pagination options.
    ///
    /// Continue with [`DirectoryListing::next_cursor`] until
    /// [`DirectoryListing::eof`] is true. The server retains no iterator or
    /// snapshot between calls, so pages are weakly consistent: entries inserted
    /// at or before the cursor may be omitted, while later insertions may appear.
    /// A cursor is valid only for the same directory path.
    pub async fn list(&self, path: &str, options: ListOptions) -> ClientResult<DirectoryListing> {
        let path = NamespacePathBuf::parse(path)?;
        self.runtime.executor.list(path, options).await
    }

    /// Lazily lists directory entries across bounded unary RPC pages.
    ///
    /// The returned stream fetches the next page only after buffered entries
    /// from the current page have been consumed. Dropping it cancels further
    /// pagination. `options.cursor` may resume from an earlier page, and
    /// `options.limit` applies independently to every request.
    ///
    /// Pagination remains weakly consistent because Metadata retains no
    /// iterator or snapshot between requests. Use [`Self::list`] when callers
    /// need explicit page boundaries or access to continuation cursors.
    pub fn list_stream(
        &self,
        path: &str,
        options: ListOptions,
    ) -> ClientResult<impl futures::Stream<Item = ClientResult<DirectoryEntry>> + Send + Unpin + 'static> {
        let state = DirectoryListStreamState {
            client: self.clone(),
            path: NamespacePathBuf::parse(path)?,
            options,
            buffered_entries: Vec::new().into_iter(),
            eof: false,
        };
        Ok(stream::try_unfold(state, |mut state| async move {
            loop {
                if let Some(entry) = state.buffered_entries.next() {
                    return Ok(Some((entry, state)));
                }
                if state.eof {
                    return Ok(None);
                }

                let previous_cursor = state.options.cursor.clone();
                let page = state
                    .client
                    .runtime
                    .executor
                    .list(state.path.clone(), state.options.clone())
                    .await?;
                if !page.eof && page.next_cursor == previous_cursor {
                    return Err(invalid_response(
                        "ListStatus",
                        "non-EOF page did not advance next_cursor",
                    ));
                }

                state.eof = page.eof;
                state.options.cursor = page.next_cursor;
                state.buffered_entries = page.entries.into_iter();
            }
        })
        .boxed())
    }

    /// Create a directory through the metadata runtime.
    /// When `recursive` is true, missing parent directories are created.
    pub async fn mkdirs(&self, path: &str, recursive: bool) -> ClientResult<FileStatus> {
        let path = NamespacePathBuf::parse(path)?;
        self.runtime.executor.create_directory(path, recursive).await
    }

    /// Delete a file, symlink, or directory through the metadata runtime.
    ///
    /// Namespace visibility changes atomically at metadata. Physical block
    /// reclamation follows the configured metadata grace period asynchronously.
    pub async fn delete(&self, path: &str, options: DeleteOptions) -> ClientResult<()> {
        let path = NamespacePathBuf::parse(path)?;
        self.runtime.executor.delete(path, options).await
    }

    /// Rename a namespace entry through the metadata runtime.
    pub async fn rename(&self, src: &str, dst: &str) -> ClientResult<()> {
        let src = NamespacePathBuf::parse(src)?;
        let dst = NamespacePathBuf::parse(dst)?;
        self.runtime.executor.rename(src, dst).await
    }

    /// Opens an existing file for reads and returns a file reader.
    ///
    /// Existing files use the metadata-stored `FileLayout`; there are no
    /// public read-open options until they carry real behavior.
    pub async fn open(&self, path: &str) -> ClientResult<FileReader> {
        let path = NamespacePathBuf::parse(path)?;
        let handle = self.runtime.executor.open_file(path).await?;
        Ok(FileReader::new(Arc::clone(&self.runtime), handle))
    }

    /// Creates a file write session according to the supplied creation options.
    ///
    /// `CreateOptions` layout fields are create-time intent for new file
    /// creation. Metadata validates and persists the accepted `FileLayout`.
    pub async fn create(&self, path: &str, options: CreateOptions) -> ClientResult<FileWriter> {
        let path = NamespacePathBuf::parse(path)?;
        let response = self.runtime.executor.create_file(path, options).await?;
        Ok(FileWriter::new(Arc::clone(&self.runtime), response))
    }

    /// Opens an append write session for an existing file.
    ///
    /// Append uses the metadata-stored `FileLayout` and does not send a new
    /// layout override.
    pub async fn append(&self, path: &str) -> ClientResult<FileWriter> {
        let path = NamespacePathBuf::parse(path)?;
        let response = self.runtime.executor.open_append(path).await?;
        Ok(FileWriter::new(Arc::clone(&self.runtime), response))
    }
}

impl fmt::Debug for FsClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FsClient")
            .field("config", &self.runtime.config)
            .field("executor", &self.runtime.executor)
            .field("data_plane", &self.runtime.data_plane)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::super::handle::{ReadHandle, WriteHandle};
    use super::super::options::{DEFAULT_BLOCK_SIZE, DEFAULT_CHUNK_SIZE, DEFAULT_REPLICATION};
    use super::*;
    use crate::config::{ClientConfig, MetadataGroupConfig};
    use crate::data::{
        BlockWrite, BlockWriteInput, BlockWriteLease, WorkerDataClient, WorkerDataPlane, WorkerReadResult,
        WorkerWriteTarget,
    };
    use crate::error::{ClientError, ClientResult};
    use crate::metadata::{
        AddBlockResult, MetadataAuthorityUpdate, MetadataGateway, ReadLayout, ValidatedMetadataResponse,
    };
    use crate::planner::PlannedBlockRead;
    use crate::rpc_error::{ClientAction, RefreshHint};
    use crate::runtime::{classify_error, AttemptContext, ErrorClass, MetadataTargets};
    use crate::session::write_session::WriteSession;
    use async_trait::async_trait;
    use beryl_common::error::rpc::{
        ErrorKind, InternalErrorKind, MetadataErrorKind, RefreshHint as RpcRefreshHint, RpcErrorDetail, WorkerErrorKind,
    };
    use beryl_common::header::{HEADER_WORKER_DATA_REJECTION, WORKER_DATA_REJECTION_CAPACITY_BEFORE_SIDE_EFFECT};
    use beryl_proto::metadata::{
        AbortFileWriteResponseProto, CommitFileResponseProto, CreateDirectoryResponseProto, CreateFileResponseProto,
        DeleteResponseProto, GetStatusResponseProto, ListStatusResponseProto, OpenFileResponseProto,
        OpenWriteResponseProto, RenameResponseProto, RenewLeaseResponseProto, SyncWriteResponseProto, WriteHandleProto,
    };
    use beryl_proto::worker::write_block_request_proto::Payload;
    use beryl_types::lease::FencingToken;
    use beryl_types::{
        BlockId, BlockIndex, ClientId, FileBlockLocation, GroupName, GroupStateWatermark, InodeId, RaftLogId,
        WorkerEndpointInfo, WorkerId, WorkerNetProtocol, WriteTarget,
    };
    use bytes::Bytes;
    use std::collections::{HashMap, VecDeque};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use tokio::sync::{mpsc, watch};

    type EventLog = Arc<Mutex<Vec<&'static str>>>;

    #[tokio::test(start_paused = true)]
    async fn metadata_retry_reuses_call_id_across_transport_and_server_retry() {
        let gateway = Arc::new(MockGateway::with_get_status_outcomes(vec![
            MetadataOutcome::Transport,
            MetadataOutcome::ServerRetry,
            MetadataOutcome::Ok,
        ]));
        let client = fs_client_with_gateway(test_config_with_retries("root", 3), gateway.clone()).expect("client");

        client.stat("/alpha").await.expect("third primary attempt succeeds");

        let calls = gateway.calls();
        assert_eq!(methods(&calls), vec!["get_status", "get_status", "get_status"]);
        assert!(calls.iter().all(|call| call.call_id == calls[0].call_id));
        assert!(calls.iter().all(|call| call.deadline_ms == calls[0].deadline_ms));
    }

    #[tokio::test(start_paused = true)]
    async fn read_transport_exhaustion_remains_a_transport_error() {
        let gateway = Arc::new(MockGateway::with_get_status_outcomes(vec![
            MetadataOutcome::Transport,
            MetadataOutcome::Transport,
            MetadataOutcome::Transport,
        ]));
        let client = fs_client_with_gateway(test_config_with_retries("root", 3), gateway.clone()).expect("client");

        let err = client
            .stat("/alpha")
            .await
            .expect_err("read transport attempts exhausted");

        assert_eq!(classify_error(&err), ErrorClass::RetryableTransport);
        let calls = gateway.calls();
        assert_eq!(methods(&calls), vec!["get_status", "get_status", "get_status"]);
        assert!(calls.iter().all(|call| call.call_id == calls[0].call_id));
    }

    #[tokio::test(start_paused = true)]
    async fn open_write_transport_ambiguity_fails_closed_without_replay() {
        let gateway = Arc::new(MockGateway::with_open_write_outcomes(vec![
            MetadataOutcome::Transport,
            MetadataOutcome::Ok,
        ]));
        let client = fs_client_with_gateway(test_config_with_retries("root", 3), gateway.clone()).expect("client");

        let err = client
            .append("/alpha")
            .await
            .expect_err("OpenWrite transport ambiguity must fail closed");

        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("OpenWrite")));
        assert_eq!(methods(&gateway.calls()), vec!["open_write"]);
    }

    #[tokio::test(start_paused = true)]
    async fn unsafe_namespace_mutations_fail_closed_without_transport_replay() {
        for operation in ["create_file", "create_directory", "delete", "rename"] {
            let gateway = Arc::new(MockGateway::with_mutation_outcomes(vec![
                MetadataOutcome::Transport,
                MetadataOutcome::Ok,
            ]));
            let client = fs_client_with_gateway(test_config_with_retries("root", 3), gateway.clone()).expect("client");

            let err = match operation {
                "create_file" => client
                    .create("/alpha", CreateOptions::create())
                    .await
                    .expect_err("CreateFile ambiguity must fail closed"),
                "create_directory" => client
                    .mkdirs("/alpha", false)
                    .await
                    .expect_err("non-recursive CreateDirectory ambiguity must fail closed"),
                "delete" => client
                    .delete("/alpha", DeleteOptions::default())
                    .await
                    .expect_err("Delete ambiguity must fail closed"),
                "rename" => client
                    .rename("/alpha", "/beta")
                    .await
                    .expect_err("Rename ambiguity must fail closed"),
                other => panic!("unexpected operation {other}"),
            };

            assert!(
                matches!(err, ClientError::UnknownOutcome(_)),
                "unexpected error for {operation}: {err:?}"
            );
            assert_eq!(methods(&gateway.calls()), vec![operation]);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn recursive_create_directory_retries_as_an_ensure_operation() {
        let gateway = Arc::new(MockGateway::with_mutation_outcomes(vec![
            MetadataOutcome::Transport,
            MetadataOutcome::Ok,
        ]));
        let client = fs_client_with_gateway(test_config_with_retries("root", 3), gateway.clone()).expect("client");

        client
            .mkdirs("/alpha/beta", true)
            .await
            .expect("recursive CreateDirectory converges after retry");

        let calls = gateway.calls();
        assert_eq!(methods(&calls), vec!["create_directory", "create_directory"]);
        assert_eq!(calls[0].call_id, calls[1].call_id);
        assert_eq!(
            gateway.create_directory_requests(),
            vec![("/alpha/beta".to_string(), true), ("/alpha/beta".to_string(), true),]
        );
    }

    #[tokio::test]
    async fn stale_state_refresh_uses_distinct_msync_call_id_and_shared_deadline() {
        let gateway = Arc::new(MockGateway::with_get_status_outcomes(vec![
            MetadataOutcome::StaleState,
            MetadataOutcome::Ok,
        ]));
        let client = fs_client_with_gateway(test_config_with_retries("root", 2), gateway.clone()).expect("client");

        client.stat("/alpha").await.expect("retry after one Msync succeeds");

        let calls = gateway.calls();
        assert_eq!(methods(&calls), vec!["get_status", "msync", "get_status"]);
        assert_eq!(calls[0].call_id, calls[2].call_id);
        assert_ne!(calls[0].call_id, calls[1].call_id);
        assert!(calls.iter().all(|call| call.deadline_ms == calls[0].deadline_ms));
        let headers = gateway.get_status_headers();
        assert!(headers[0].state.is_empty());
        assert_eq!(headers[1].state[0].state_id.as_ref().map(|state| state.index), Some(1));
    }

    #[tokio::test]
    async fn successful_metadata_authority_enriches_the_next_request() {
        let gateway = Arc::new(MockGateway::with_get_status_authority(MetadataAuthorityUpdate {
            group_name: group_name_from("root"),
            state: vec![GroupStateWatermark::new(
                group_name_from("root"),
                RaftLogId::new(2, 1, 9),
            )],
            mount_epoch: Some(31),
            route_epoch: Some(41),
        }));
        let client = fs_client_with_gateway(test_config("root"), gateway.clone()).expect("client");

        client.stat("/alpha").await.expect("first stat");
        client.stat("/alpha").await.expect("second stat");

        let headers = gateway.get_status_headers();
        assert_eq!(headers.len(), 2);
        assert!(headers[0].state.is_empty());
        assert_eq!(headers[1].mount_epoch, Some(31));
        assert_eq!(headers[1].route_epoch, Some(41));
        assert_eq!(headers[1].state.len(), 1);
        assert_eq!(headers[1].state[0].group_name, "root");
        assert_eq!(headers[1].state[0].state_id.as_ref().map(|state| state.index), Some(9));
    }

    #[tokio::test]
    async fn reader_read_exact_at_rejects_short_eof_read() {
        let gateway = Arc::new(MockGateway::default());
        let client = fs_client_with_gateway(test_config("root"), gateway).expect("client");
        let reader = read_reader(&client, 10);

        let err = reader
            .read_exact_at(10, 4)
            .await
            .expect_err("short EOF read must fail exact read");

        assert!(matches!(err, ClientError::InvalidArgument(msg)
            if msg.contains("read_exact_at") && msg.contains("requested 4 bytes")));
    }

    #[tokio::test]
    async fn reader_replans_after_worker_refresh() {
        let gateway = Arc::new(MockGateway::with_layout(layout_response(
            "root",
            202,
            Some(3),
            16,
            vec![location(202, 0, 0, 16)],
        )));
        let worker = Arc::new(MockDataClient::with_refresh_once(
            b"abcdefghijklmnop",
            ErrorKind::Worker(WorkerErrorKind::RunMismatch),
        ));
        let client =
            fs_client_with_data_plane(test_config("root"), gateway.clone(), data_plane(worker)).expect("client");
        let reader = read_reader(&client, 16);

        let bytes = reader.read_at(1, 3).await.expect("read succeeds after refresh");

        assert_eq!(bytes, Bytes::from_static(b"bcd"));
        assert_eq!(method_count(&gateway.calls(), "read_layout"), 2);
    }

    #[tokio::test]
    async fn writer_barrier_flush_worker_error_blocks_later_write_and_close() {
        let layout = recorded_layout_values(8, 4);
        let gateway = Arc::new(MockGateway::with_create_response_layout(Some(layout)));
        let worker = Arc::new(MockDataClient {
            write_outcomes: Mutex::new(vec![WorkerWriteOutcome::WorkerError].into()),
            ..MockDataClient::default()
        });
        let client =
            fs_client_with_data_plane(test_config("root"), gateway.clone(), data_plane(worker)).expect("client");
        let mut writer = client
            .create("/created", CreateOptions::create())
            .await
            .expect("writer");

        writer.write_all(Bytes::from_static(b"hello")).await.expect("write");
        let err = writer
            .sync_write_visibility()
            .await
            .expect_err("flush failure must fail barrier");
        assert!(matches!(err, ClientError::Worker(msg) if msg.contains("injected WriteBlock failure")));

        let err = writer
            .write_all(Bytes::from_static(b"!"))
            .await
            .expect_err("unsafe flush failure blocks writes");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("invalid")));
        let err = writer
            .sync_write_durability()
            .await
            .expect_err("unsafe flush failure blocks durability sync");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("invalid")));
        let err = writer.close().await.expect_err("unsafe flush failure blocks close");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("invalid")));
        assert_eq!(method_count(&gateway.calls(), "commit_file"), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn writer_retries_only_marked_before_side_effect_capacity_without_invalidating_session() {
        let events = event_log();
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient {
            write_outcomes: Mutex::new(
                vec![
                    WorkerWriteOutcome::CapacityRejected,
                    WorkerWriteOutcome::CapacityRejected,
                    WorkerWriteOutcome::CapacityRejected,
                ]
                .into(),
            ),
            events: Some(events.clone()),
            ..MockDataClient::default()
        });
        let client =
            fs_client_with_data_plane(test_config_with_retries("root", 3), gateway.clone(), data_plane(worker))
                .expect("client");
        let mut writer = client
            .create("/created", CreateOptions::create())
            .await
            .expect("writer");

        let error = writer
            .write_all(Bytes::from_static(b"x"))
            .await
            .expect_err("marked capacity must stop after the configured attempts");

        assert_eq!(classify_error(&error), ErrorClass::ServerRetry);
        assert_eq!(add_block_count(&gateway.calls()), 1);
        assert_eq!(
            events
                .lock()
                .expect("events")
                .iter()
                .filter(|event| **event == "write_block")
                .count(),
            3
        );
        writer
            .write_all(Bytes::new())
            .await
            .expect("definite rejection before Worker side effects keeps the session open");
    }

    #[tokio::test]
    async fn writer_auto_renews_near_expiry_before_write() {
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient::default());
        let mut config = test_config("root");
        config.write_lease.auto_renew = true;
        config.write_lease.renew_before_expiry_ms = 120_000;
        let client = fs_client_with_data_plane(config, gateway.clone(), data_plane(worker)).expect("client");
        let handle = write_handle_for_tests("/created", 0, unix_now_ms() + 60_000).expect("write handle");
        let mut writer = FileWriter::new(Arc::clone(&client.runtime), handle);

        writer.write_all(Bytes::from_static(b"hello")).await.expect("write");

        assert_eq!(methods(&gateway.calls()), vec!["renew_lease", "add_block"]);
    }

    #[tokio::test]
    async fn writer_lease_expiry_while_send_is_blocked_cancels_and_invalidates_session() {
        let events = event_log();
        let gateway = Arc::new(MockGateway::default());
        let worker = Arc::new(MockDataClient {
            write_outcomes: Mutex::new(vec![WorkerWriteOutcome::HoldRequests].into()),
            events: Some(events.clone()),
            ..MockDataClient::default()
        });
        let mut config = test_config("root");
        config.write_lease.auto_renew = false;
        let client = fs_client_with_data_plane(config, gateway.clone(), data_plane(worker)).expect("client");
        let handle = write_handle_for_tests("/created", 0, unix_now_ms() + 1_200).expect("write handle");
        let mut writer = FileWriter::new(Arc::clone(&client.runtime), handle);

        let error = writer
            .write_all(Bytes::from(vec![b'x'; beryl_proto::DEFAULT_WORKER_DATA_FRAME_SIZE + 1]))
            .await
            .expect_err("lease expiry must interrupt a blocked frame send");

        assert!(
            matches!(&error, ClientError::UnknownOutcome(message) if message.contains("lease expired")),
            "unexpected error: {error:?}"
        );
        assert_event_order(&events, "write_block", "cancel_write_block");
        let error = writer
            .write_all(Bytes::from_static(b"!"))
            .await
            .expect_err("expired in-flight write blocks later writes");
        assert!(matches!(error, ClientError::StaleHandle { reason } if reason.contains("unknown outcome")));
        assert_eq!(add_block_count(&gateway.calls()), 1);
    }

    #[tokio::test]
    async fn writer_cancellation_wait_uses_the_current_write_and_abort_deadline() {
        let write_events = event_log();
        let write_gateway = Arc::new(MockGateway::default());
        let write_worker = Arc::new(MockDataClient {
            write_outcomes: Mutex::new(vec![WorkerWriteOutcome::HoldCancellation].into()),
            events: Some(write_events.clone()),
            ..MockDataClient::default()
        });
        let mut write_config = test_config("root");
        write_config.retry.operation_timeout_ms = 50;
        write_config.write_lease.auto_renew = false;
        let write_client = fs_client_with_data_plane(write_config, write_gateway.clone(), data_plane(write_worker))
            .expect("write client");
        let write_handle = write_handle_for_tests("/write", 0, unix_now_ms() + 5_000).expect("write handle");
        let mut writer = FileWriter::new(Arc::clone(&write_client.runtime), write_handle);

        let write_error = tokio::time::timeout(
            Duration::from_millis(250),
            writer.write_all(Bytes::from(vec![b'x'; beryl_proto::DEFAULT_WORKER_DATA_FRAME_SIZE + 1])),
        )
        .await
        .expect("write cancellation stays within the outer bound")
        .expect_err("blocked write reaches its public deadline");
        assert!(write_error.to_string().contains("deadline"));

        let abort_events = event_log();
        let abort_gateway = Arc::new(MockGateway::default());
        let abort_worker = Arc::new(MockDataClient {
            write_outcomes: Mutex::new(vec![WorkerWriteOutcome::HoldCancellation].into()),
            events: Some(abort_events.clone()),
            ..MockDataClient::default()
        });
        let mut abort_config = test_config("root");
        abort_config.retry.operation_timeout_ms = 50;
        abort_config.write_lease.auto_renew = false;
        let abort_client = fs_client_with_data_plane(abort_config, abort_gateway.clone(), data_plane(abort_worker))
            .expect("abort client");
        let abort_handle = write_handle_for_tests("/abort", 0, unix_now_ms() + 5_000).expect("abort handle");
        let mut abort_writer = FileWriter::new(Arc::clone(&abort_client.runtime), abort_handle);
        abort_writer
            .write_all(Bytes::from_static(b"x"))
            .await
            .expect("partial block write");

        let abort_error = tokio::time::timeout(Duration::from_millis(250), abort_writer.abort())
            .await
            .expect("abort cancellation stays within the outer bound")
            .expect_err("blackholed cancellation reaches the abort deadline");
        assert!(abort_error.to_string().contains("deadline"));
        assert_event_order(&abort_events, "write_block", "cancel_write_block");
        assert_eq!(method_count(&abort_gateway.calls(), "abort_file_write"), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn writer_unknown_add_block_blocks_followup_writes() {
        let layout = recorded_layout_values(5, 5);
        let gateway = Arc::new(MockGateway {
            create_response_layout: Mutex::new(Some(Some(layout))),
            add_block_outcomes: Mutex::new(
                vec![
                    AddBlockOutcome::TransportUnknown,
                    AddBlockOutcome::TransportUnknown,
                    AddBlockOutcome::TransportUnknown,
                ]
                .into(),
            ),
            ..MockGateway::default()
        });
        let worker = Arc::new(MockDataClient::default());
        let client =
            fs_client_with_data_plane(test_config_with_retries("root", 3), gateway.clone(), data_plane(worker))
                .expect("client");
        let mut writer = client
            .create("/created", CreateOptions::create())
            .await
            .expect("writer");

        let err = writer
            .write_all(Bytes::from_static(b"hello"))
            .await
            .expect_err("AddBlock unknown outcome");
        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("AddBlock")));

        let err = writer
            .write_all(Bytes::from_static(b"!"))
            .await
            .expect_err("unknown outcome blocks writes");
        assert!(matches!(err, ClientError::StaleHandle { reason } if reason.contains("unknown outcome")));
        let calls = gateway.calls();
        let add_calls: Vec<_> = calls.iter().filter(|call| call.method == "add_block").collect();
        assert_eq!(add_calls.len(), 3);
        assert!(add_calls.iter().all(|call| call.call_id == add_calls[0].call_id));
    }

    #[tokio::test(start_paused = true)]
    async fn mutation_terminal_error_after_transport_ambiguity_remains_unknown() {
        let layout = recorded_layout_values(5, 5);
        let gateway = Arc::new(MockGateway {
            create_response_layout: Mutex::new(Some(Some(layout))),
            add_block_outcomes: Mutex::new(
                vec![AddBlockOutcome::TransportUnknown, AddBlockOutcome::TerminalFailure].into(),
            ),
            ..MockGateway::default()
        });
        let worker = Arc::new(MockDataClient::default());
        let client =
            fs_client_with_data_plane(test_config_with_retries("root", 3), gateway.clone(), data_plane(worker))
                .expect("client");
        let mut writer = client
            .create("/created", CreateOptions::create())
            .await
            .expect("writer");

        let err = writer
            .write_all(Bytes::from_static(b"hello"))
            .await
            .expect_err("a later terminal response cannot resolve the earlier mutation attempt");

        assert!(matches!(err, ClientError::UnknownOutcome(msg) if msg.contains("AddBlock")));
        assert_eq!(method_count(&gateway.calls(), "add_block"), 2);
    }

    fn event_log() -> EventLog {
        Arc::new(Mutex::new(Vec::new()))
    }

    fn methods(calls: &[RecordedCall]) -> Vec<&'static str> {
        calls.iter().map(|call| call.method).collect()
    }

    fn method_count(calls: &[RecordedCall], method: &str) -> usize {
        calls.iter().filter(|call| call.method == method).count()
    }

    fn group_name_from(raw: &str) -> GroupName {
        GroupName::parse(raw).unwrap()
    }

    /// Builds the successful response envelope used by the executor-facing mock.
    fn metadata_response<T>(ctx: &AttemptContext, body: T) -> ValidatedMetadataResponse<T> {
        let group_name = ctx.group_name().cloned().expect("metadata attempt group");
        ValidatedMetadataResponse::new(
            MetadataAuthorityUpdate {
                group_name,
                state: Vec::new(),
                mount_epoch: None,
                route_epoch: None,
            },
            body,
        )
    }

    fn metadata_ok<T>(ctx: &AttemptContext, body: T) -> ClientResult<ValidatedMetadataResponse<T>> {
        Ok(metadata_response(ctx, body))
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

    fn test_config(group_name: &str) -> ClientConfig {
        ClientConfig {
            metadata_groups: vec![metadata_group_config(group_name)],
            ..ClientConfig::default()
        }
    }

    fn metadata_group_config(group_name: &str) -> MetadataGroupConfig {
        MetadataGroupConfig {
            group_name: group_name_from(group_name),
            endpoints: vec!["http://127.0.0.1:18080".to_string()],
        }
    }

    fn test_config_with_retries(group_name: &str, max_attempts: usize) -> ClientConfig {
        let mut config = test_config(group_name);
        config.retry.max_attempts = max_attempts.max(1);
        config
    }

    fn fs_client_with_gateway(config: ClientConfig, gateway: Arc<dyn MetadataGateway>) -> ClientResult<FsClient> {
        let metrics: Arc<dyn crate::metrics::ClientMetrics> = Arc::new(crate::metrics::NoopClientMetrics);
        let data_plane = WorkerDataPlane::from_config(&config, metrics);
        fs_client_with_data_plane(config, gateway, data_plane)
    }

    fn fs_client_with_data_plane(
        config: ClientConfig,
        gateway: Arc<dyn MetadataGateway>,
        data_plane: WorkerDataPlane,
    ) -> ClientResult<FsClient> {
        let metadata_targets = MetadataTargets::from_config(&config)?;
        FsClient::with_runtime_hooks(
            config,
            gateway,
            metadata_targets,
            data_plane,
            Arc::new(crate::metrics::NoopClientMetrics),
        )
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct RecordedCall {
        method: &'static str,
        group_name: GroupName,
        call_id: String,
        deadline_ms: i64,
        target_inode_id: Option<u64>,
        range: Option<(u64, u32)>,
        target_size: Option<u64>,
        final_size: Option<u64>,
        committed_block_offsets: Vec<u64>,
        committed_block_lens: Vec<u64>,
        create_layout: Option<RecordedLayout>,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct RecordedLayout {
        block_size: u32,
        chunk_size: u32,
        replication: u32,
        block_format_id: u32,
    }

    fn default_layout() -> RecordedLayout {
        recorded_layout_values(DEFAULT_BLOCK_SIZE, DEFAULT_CHUNK_SIZE)
    }

    fn recorded_layout_values(block_size: u32, chunk_size: u32) -> RecordedLayout {
        RecordedLayout {
            block_size,
            chunk_size,
            replication: u32::from(DEFAULT_REPLICATION),
            block_format_id: beryl_types::BlockFormatId::CURRENT_FOR_NEW_FILE.as_raw(),
        }
    }

    fn recorded_layout(layout: &beryl_proto::common::FileLayoutProto) -> RecordedLayout {
        RecordedLayout {
            block_size: layout.block_size,
            chunk_size: layout.chunk_size,
            replication: layout.replication,
            block_format_id: layout.block_format_id,
        }
    }

    fn layout_proto(layout: RecordedLayout) -> beryl_proto::common::FileLayoutProto {
        beryl_proto::common::FileLayoutProto {
            block_size: layout.block_size,
            chunk_size: layout.chunk_size,
            replication: layout.replication,
            block_format_id: layout.block_format_id,
        }
    }

    fn add_block_count(calls: &[RecordedCall]) -> usize {
        calls.iter().filter(|call| call.method == "add_block").count()
    }

    fn unix_now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock must be after Unix epoch")
            .as_millis() as u64
    }

    #[derive(Debug, Default)]
    struct MockGateway {
        calls: Mutex<Vec<RecordedCall>>,
        get_status_headers: Mutex<Vec<beryl_proto::common::RequestHeaderProto>>,
        get_status_authority: Mutex<VecDeque<MetadataAuthorityUpdate>>,
        layouts: Mutex<VecDeque<ReadLayout>>,
        create_directory_requests: Mutex<Vec<(String, bool)>>,
        next_offsets: Mutex<HashMap<u64, u64>>,
        next_block_indexes: Mutex<HashMap<u64, u32>>,
        write_layouts: Mutex<HashMap<u64, RecordedLayout>>,
        create_response_layout: Mutex<Option<Option<RecordedLayout>>>,
        add_block_outcomes: Mutex<VecDeque<AddBlockOutcome>>,
        get_status_outcomes: Mutex<VecDeque<MetadataOutcome>>,
        open_write_outcomes: Mutex<VecDeque<MetadataOutcome>>,
        mutation_outcomes: Mutex<VecDeque<MetadataOutcome>>,
        events: Option<EventLog>,
    }

    impl MockGateway {
        fn calls(&self) -> Vec<RecordedCall> {
            self.calls.lock().expect("calls").clone()
        }

        fn create_directory_requests(&self) -> Vec<(String, bool)> {
            self.create_directory_requests
                .lock()
                .expect("create directory requests")
                .clone()
        }

        fn get_status_headers(&self) -> Vec<beryl_proto::common::RequestHeaderProto> {
            self.get_status_headers.lock().expect("get status headers").clone()
        }

        fn with_layout(layout: ReadLayout) -> Self {
            let mut layouts = VecDeque::new();
            layouts.push_back(layout);
            Self {
                layouts: Mutex::new(layouts),
                ..Self::default()
            }
        }

        fn with_create_response_layout(layout: Option<RecordedLayout>) -> Self {
            Self {
                create_response_layout: Mutex::new(Some(layout)),
                ..Self::default()
            }
        }

        fn with_get_status_outcomes(outcomes: Vec<MetadataOutcome>) -> Self {
            Self {
                get_status_outcomes: Mutex::new(outcomes.into()),
                ..Self::default()
            }
        }

        fn with_get_status_authority(authority: MetadataAuthorityUpdate) -> Self {
            Self {
                get_status_authority: Mutex::new(vec![authority].into()),
                ..Self::default()
            }
        }

        fn with_open_write_outcomes(outcomes: Vec<MetadataOutcome>) -> Self {
            Self {
                open_write_outcomes: Mutex::new(outcomes.into()),
                ..Self::default()
            }
        }

        fn with_mutation_outcomes(outcomes: Vec<MetadataOutcome>) -> Self {
            Self {
                mutation_outcomes: Mutex::new(outcomes.into()),
                ..Self::default()
            }
        }

        fn next_get_status_outcome(&self) -> MetadataOutcome {
            self.get_status_outcomes
                .lock()
                .expect("get status outcomes")
                .pop_front()
                .unwrap_or(MetadataOutcome::Ok)
        }

        fn next_add_block_outcome(&self) -> AddBlockOutcome {
            self.add_block_outcomes
                .lock()
                .expect("add block outcomes")
                .pop_front()
                .unwrap_or(AddBlockOutcome::Ok)
        }

        fn next_open_write_outcome(&self) -> MetadataOutcome {
            self.open_write_outcomes
                .lock()
                .expect("open write outcomes")
                .pop_front()
                .unwrap_or(MetadataOutcome::Ok)
        }

        fn next_mutation_outcome(&self) -> MetadataOutcome {
            self.mutation_outcomes
                .lock()
                .expect("mutation outcomes")
                .pop_front()
                .unwrap_or(MetadataOutcome::Ok)
        }

        fn apply_metadata_outcome(outcome: MetadataOutcome, operation: &str) -> ClientResult<()> {
            match outcome {
                MetadataOutcome::Ok => Ok(()),
                MetadataOutcome::Transport => Err(ClientError::from(tonic::Status::unavailable(format!(
                    "injected {operation} transport ambiguity"
                )))),
                MetadataOutcome::ServerRetry => Err(server_retry_error()),
                MetadataOutcome::StaleState => {
                    Err(refresh_action_error(ErrorKind::Metadata(MetadataErrorKind::StaleState)))
                }
            }
        }

        fn record(&self, method: &'static str, ctx: &AttemptContext) {
            let header = ctx.metadata_header().expect("metadata header");
            self.calls.lock().expect("calls").push(RecordedCall {
                method,
                group_name: group_name_from(&header.group_name),
                call_id: header.client.as_ref().expect("client").call_id.clone(),
                deadline_ms: header.deadline_ms,
                target_inode_id: None,
                range: None,
                target_size: None,
                final_size: None,
                committed_block_offsets: Vec::new(),
                committed_block_lens: Vec::new(),
                create_layout: None,
            });
        }

        fn record_create_file(&self, ctx: &AttemptContext, req: &beryl_proto::metadata::CreateFileRequestProto) {
            let header = ctx.metadata_header().expect("metadata header");
            self.calls.lock().expect("calls").push(RecordedCall {
                method: "create_file",
                group_name: group_name_from(&header.group_name),
                call_id: header.client.as_ref().expect("client").call_id.clone(),
                deadline_ms: header.deadline_ms,
                target_inode_id: None,
                range: None,
                target_size: None,
                final_size: None,
                committed_block_offsets: Vec::new(),
                committed_block_lens: Vec::new(),
                create_layout: req.layout.as_ref().map(recorded_layout),
            });
        }

        fn record_read_layout(&self, ctx: &AttemptContext, req: &beryl_proto::metadata::GetBlockLocationsRequestProto) {
            let header = ctx.metadata_header().expect("metadata header");
            let target_inode_id = match req.target.as_ref() {
                Some(beryl_proto::metadata::get_block_locations_request_proto::Target::InodeId(id)) => Some(*id),
                _ => None,
            };
            let range = req.range.as_ref().map(|range| (range.offset, range.len));
            self.calls.lock().expect("calls").push(RecordedCall {
                method: "read_layout",
                group_name: group_name_from(&header.group_name),
                call_id: header.client.as_ref().expect("client").call_id.clone(),
                deadline_ms: header.deadline_ms,
                target_inode_id,
                range,
                target_size: None,
                final_size: None,
                committed_block_offsets: Vec::new(),
                committed_block_lens: Vec::new(),
                create_layout: None,
            });
        }

        fn record_commit_file(&self, ctx: &AttemptContext, req: &beryl_proto::metadata::CommitFileRequestProto) {
            self.record_event("commit_file");
            let header = ctx.metadata_header().expect("metadata header");
            self.calls.lock().expect("calls").push(RecordedCall {
                method: "commit_file",
                group_name: group_name_from(&header.group_name),
                call_id: header.client.as_ref().expect("client").call_id.clone(),
                deadline_ms: header.deadline_ms,
                target_inode_id: req.write_handle.as_ref().map(|handle| handle.inode_id),
                range: None,
                target_size: None,
                final_size: Some(req.final_size),
                committed_block_offsets: req.committed_blocks.iter().map(|block| block.file_offset).collect(),
                committed_block_lens: req.committed_blocks.iter().map(|block| block.len).collect(),
                create_layout: None,
            });
        }

        fn record_sync_write(&self, ctx: &AttemptContext, req: &beryl_proto::metadata::SyncWriteRequestProto) {
            let header = ctx.metadata_header().expect("metadata header");
            self.calls.lock().expect("calls").push(RecordedCall {
                method: "sync_write",
                group_name: group_name_from(&header.group_name),
                call_id: header.client.as_ref().expect("client").call_id.clone(),
                deadline_ms: header.deadline_ms,
                target_inode_id: req.write_handle.as_ref().map(|handle| handle.inode_id),
                range: None,
                target_size: Some(req.target_size),
                final_size: None,
                committed_block_offsets: req.committed_blocks.iter().map(|block| block.file_offset).collect(),
                committed_block_lens: req.committed_blocks.iter().map(|block| block.len).collect(),
                create_layout: None,
            });
        }

        fn record_add_block(&self, ctx: &AttemptContext, _req: &beryl_proto::metadata::AddBlockRequestProto) {
            let header = ctx.metadata_header().expect("metadata header");
            self.calls.lock().expect("calls").push(RecordedCall {
                method: "add_block",
                group_name: group_name_from(&header.group_name),
                call_id: header.client.as_ref().expect("client").call_id.clone(),
                deadline_ms: header.deadline_ms,
                target_inode_id: None,
                range: None,
                target_size: None,
                final_size: None,
                committed_block_offsets: Vec::new(),
                committed_block_lens: Vec::new(),
                create_layout: None,
            });
        }

        fn record_event(&self, event: &'static str) {
            if let Some(events) = &self.events {
                events.lock().expect("events").push(event);
            }
        }
    }

    #[async_trait]
    impl MetadataGateway for MockGateway {
        async fn get_status(
            &self,
            ctx: AttemptContext,
            _req: beryl_proto::metadata::GetStatusRequestProto,
        ) -> ClientResult<ValidatedMetadataResponse<GetStatusResponseProto>> {
            self.record("get_status", &ctx);
            self.get_status_headers
                .lock()
                .expect("get status headers")
                .push(ctx.metadata_header().expect("metadata header"));
            match self.next_get_status_outcome() {
                MetadataOutcome::Ok => {}
                MetadataOutcome::Transport => {
                    return Err(ClientError::from(tonic::Status::unavailable(
                        "injected metadata transport ambiguity",
                    )))
                }
                MetadataOutcome::ServerRetry => return Err(server_retry_error()),
                MetadataOutcome::StaleState => {
                    return Err(refresh_action_error(ErrorKind::Metadata(MetadataErrorKind::StaleState)))
                }
            }
            let body = GetStatusResponseProto {
                attrs: Some(file_attrs_proto(10)),
                ..GetStatusResponseProto::default()
            };
            let authority = self
                .get_status_authority
                .lock()
                .expect("get status authority")
                .pop_front();
            Ok(match authority {
                Some(authority) => ValidatedMetadataResponse::new(authority, body),
                None => metadata_response(&ctx, body),
            })
        }

        async fn list_status(
            &self,
            ctx: AttemptContext,
            _req: beryl_proto::metadata::ListStatusRequestProto,
        ) -> ClientResult<ValidatedMetadataResponse<ListStatusResponseProto>> {
            self.record("list_status", &ctx);
            metadata_ok(
                &ctx,
                ListStatusResponseProto {
                    entries: vec![beryl_proto::metadata::DirEntryProto {
                        name: "child".to_string(),
                        kind: beryl_proto::metadata::InodeKindProto::InodeKindFile as i32,
                        attrs: Some(file_attrs_proto(4)),
                    }],
                    eof: true,
                    ..ListStatusResponseProto::default()
                },
            )
        }

        async fn create_directory(
            &self,
            ctx: AttemptContext,
            req: beryl_proto::metadata::CreateDirectoryRequestProto,
        ) -> ClientResult<ValidatedMetadataResponse<CreateDirectoryResponseProto>> {
            self.record("create_directory", &ctx);
            self.create_directory_requests
                .lock()
                .expect("create directory requests")
                .push((req.path, req.recursive));
            Self::apply_metadata_outcome(self.next_mutation_outcome(), "CreateDirectory")?;
            metadata_ok(
                &ctx,
                CreateDirectoryResponseProto {
                    attrs: Some(file_attrs_proto(0)),
                    ..CreateDirectoryResponseProto::default()
                },
            )
        }

        async fn delete(
            &self,
            ctx: AttemptContext,
            _req: beryl_proto::metadata::DeleteRequestProto,
        ) -> ClientResult<ValidatedMetadataResponse<DeleteResponseProto>> {
            self.record("delete", &ctx);
            Self::apply_metadata_outcome(self.next_mutation_outcome(), "Delete")?;
            metadata_ok(&ctx, DeleteResponseProto::default())
        }

        async fn rename(
            &self,
            ctx: AttemptContext,
            _req: beryl_proto::metadata::RenameRequestProto,
        ) -> ClientResult<ValidatedMetadataResponse<RenameResponseProto>> {
            self.record("rename", &ctx);
            Self::apply_metadata_outcome(self.next_mutation_outcome(), "Rename")?;
            metadata_ok(&ctx, RenameResponseProto::default())
        }

        async fn open_file(
            &self,
            ctx: AttemptContext,
            _req: beryl_proto::metadata::OpenFileRequestProto,
        ) -> ClientResult<ValidatedMetadataResponse<OpenFileResponseProto>> {
            self.record("open_file", &ctx);
            metadata_ok(
                &ctx,
                OpenFileResponseProto {
                    inode_id: 202,
                    file_size: 10,
                    content_revision: Some(3),
                    ..OpenFileResponseProto::default()
                },
            )
        }

        async fn read_layout(
            &self,
            ctx: AttemptContext,
            req: beryl_proto::metadata::GetBlockLocationsRequestProto,
        ) -> ClientResult<ValidatedMetadataResponse<ReadLayout>> {
            self.record_read_layout(&ctx, &req);
            let layouts = self.layouts.lock().expect("layouts");
            let body = layouts
                .front()
                .cloned()
                .unwrap_or_else(|| layout_response("root", 202, Some(3), 10, Vec::new()));
            metadata_ok(&ctx, body)
        }

        async fn create_file(
            &self,
            ctx: AttemptContext,
            req: beryl_proto::metadata::CreateFileRequestProto,
        ) -> ClientResult<ValidatedMetadataResponse<CreateFileResponseProto>> {
            self.record_create_file(&ctx, &req);
            Self::apply_metadata_outcome(self.next_mutation_outcome(), "CreateFile")?;
            self.next_offsets.lock().expect("offsets").insert(1, 0);
            let requested_layout = req.layout.as_ref().map(recorded_layout).unwrap_or_else(default_layout);
            let response_layout = self
                .create_response_layout
                .lock()
                .expect("create response layout")
                .unwrap_or(Some(requested_layout));
            if let Some(layout) = response_layout {
                self.write_layouts.lock().expect("write layouts").insert(302, layout);
            }
            metadata_ok(
                &ctx,
                CreateFileResponseProto {
                    inode_id: 302,
                    layout: response_layout.map(layout_proto),
                    ..CreateFileResponseProto::default()
                },
            )
        }

        async fn open_write(
            &self,
            ctx: AttemptContext,
            req: beryl_proto::metadata::OpenWriteRequestProto,
        ) -> ClientResult<ValidatedMetadataResponse<OpenWriteResponseProto>> {
            self.record("open_write", &ctx);
            match self.next_open_write_outcome() {
                MetadataOutcome::Ok => {}
                MetadataOutcome::Transport => {
                    return Err(ClientError::from(tonic::Status::unavailable(
                        "injected OpenWrite transport ambiguity",
                    )))
                }
                MetadataOutcome::ServerRetry => return Err(server_retry_error()),
                MetadataOutcome::StaleState => {
                    return Err(refresh_action_error(ErrorKind::Metadata(MetadataErrorKind::StaleState)))
                }
            }
            let append = req.mode == beryl_proto::metadata::OpenWriteModeProto::OpenWriteModeAppend as i32;
            let (inode_id, base_size) = if append { (402, 10) } else { (302, 0) };
            self.next_offsets.lock().expect("offsets").insert(inode_id, base_size);
            let layout = if append {
                default_layout()
            } else {
                self.write_layouts
                    .lock()
                    .expect("write layouts")
                    .get(&inode_id)
                    .copied()
                    .unwrap_or_else(default_layout)
            };
            self.write_layouts
                .lock()
                .expect("write layouts")
                .insert(inode_id, layout);
            metadata_ok(
                &ctx,
                OpenWriteResponseProto {
                    write_handle: Some(write_handle_proto(inode_id)),
                    base_size,
                    expires_at_ms: u64::MAX / 2,
                    layout: Some(layout_proto(layout)),
                    content_revision: 0,
                    ..OpenWriteResponseProto::default()
                },
            )
        }

        async fn add_block(
            &self,
            ctx: AttemptContext,
            req: beryl_proto::metadata::AddBlockRequestProto,
        ) -> ClientResult<ValidatedMetadataResponse<AddBlockResult>> {
            self.record_add_block(&ctx, &req);
            match self.next_add_block_outcome() {
                AddBlockOutcome::Ok => {}
                AddBlockOutcome::TransportUnknown => {
                    return Err(ClientError::from(tonic::Status::unavailable(
                        "injected AddBlock transport uncertainty",
                    )))
                }
                AddBlockOutcome::TerminalFailure => {
                    return Err(ClientError::InvalidArgument(
                        "injected AddBlock terminal failure".to_string(),
                    ))
                }
            }
            let write_handle = req.write_handle.as_ref().expect("write handle");
            let layout = self
                .write_layouts
                .lock()
                .expect("write layouts")
                .get(&inode_id_from_write_handle(write_handle))
                .copied()
                .unwrap_or_else(default_layout);
            let offset = {
                let mut offsets = self.next_offsets.lock().expect("offsets");
                let session_id = inode_id_from_write_handle(write_handle);
                let offset = *offsets.entry(session_id).or_insert(0);
                offsets.insert(session_id, offset + u64::from(layout.block_size));
                offset
            };
            let block_index = {
                let mut indexes = self.next_block_indexes.lock().expect("block indexes");
                let session_id = inode_id_from_write_handle(write_handle);
                let index = *indexes.entry(session_id).or_insert(0);
                indexes.insert(session_id, index + 1);
                index
            };
            let inode_id = inode_id_from_write_handle(write_handle);
            let group_name = ctx.group_name().cloned().expect("metadata attempt group");
            metadata_ok(
                &ctx,
                AddBlockResult {
                    group_name,
                    target: write_target_with_layout(inode_id, block_index, offset, layout),
                },
            )
        }

        async fn commit_file(
            &self,
            ctx: AttemptContext,
            req: beryl_proto::metadata::CommitFileRequestProto,
        ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::CommitFileResponseProto>> {
            self.record_commit_file(&ctx, &req);
            metadata_ok(
                &ctx,
                CommitFileResponseProto {
                    committed_size: req.final_size,
                    ..CommitFileResponseProto::default()
                },
            )
        }

        async fn abort_file_write(
            &self,
            ctx: AttemptContext,
            _req: beryl_proto::metadata::AbortFileWriteRequestProto,
        ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::AbortFileWriteResponseProto>> {
            self.record_event("abort_file_write");
            self.record("abort_file_write", &ctx);
            metadata_ok(&ctx, AbortFileWriteResponseProto::default())
        }

        async fn renew_lease(
            &self,
            ctx: AttemptContext,
            _req: beryl_proto::metadata::RenewLeaseRequestProto,
        ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::metadata::RenewLeaseResponseProto>> {
            self.record("renew_lease", &ctx);
            metadata_ok(
                &ctx,
                RenewLeaseResponseProto {
                    expires_at_ms: u64::MAX / 2,
                    ..RenewLeaseResponseProto::default()
                },
            )
        }

        async fn sync_write(
            &self,
            ctx: AttemptContext,
            req: beryl_proto::metadata::SyncWriteRequestProto,
        ) -> ClientResult<ValidatedMetadataResponse<SyncWriteResponseProto>> {
            self.record_sync_write(&ctx, &req);
            let synced_size = req.target_size;
            if let Some(write_handle) = req.write_handle.as_ref() {
                self.next_offsets
                    .lock()
                    .expect("offsets")
                    .insert(write_handle.inode_id, synced_size);
            }
            metadata_ok(
                &ctx,
                SyncWriteResponseProto {
                    synced_size,
                    content_revision: Some(1),
                    ..SyncWriteResponseProto::default()
                },
            )
        }

        async fn msync(
            &self,
            ctx: AttemptContext,
            _req: beryl_proto::metadata::MsyncRequestProto,
        ) -> ClientResult<ValidatedMetadataResponse<beryl_proto::common::GroupStateWatermarkProto>> {
            let group_name = ctx.group_name().cloned().expect("metadata attempt group");
            self.record("msync", &ctx);
            let body = beryl_proto::common::GroupStateWatermarkProto {
                group_name: group_name.to_string(),
                state_id: Some(beryl_proto::common::RaftLogIdProto {
                    term: 1,
                    leader_node_id: 1,
                    index: 1,
                }),
            };
            Ok(ValidatedMetadataResponse::new(
                MetadataAuthorityUpdate {
                    group_name,
                    state: vec![GroupStateWatermark::try_from(body.clone()).expect("msync watermark")],
                    mount_epoch: None,
                    route_epoch: None,
                },
                body,
            ))
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum AddBlockOutcome {
        Ok,
        TransportUnknown,
        TerminalFailure,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum MetadataOutcome {
        Ok,
        Transport,
        ServerRetry,
        StaleState,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum WorkerWriteOutcome {
        Ok,
        CapacityRejected,
        WorkerError,
        HoldRequests,
        HoldCancellation,
    }

    #[derive(Debug)]
    struct MockDataClient {
        file: Bytes,
        refresh_once: Mutex<Option<ErrorKind>>,
        calls: Mutex<usize>,
        write_outcomes: Mutex<VecDeque<WorkerWriteOutcome>>,
        events: Option<EventLog>,
    }

    impl MockDataClient {
        fn from_file(file: &'static [u8]) -> Self {
            Self {
                file: Bytes::from_static(file),
                refresh_once: Mutex::new(None),
                calls: Mutex::new(0),
                write_outcomes: Mutex::new(VecDeque::new()),
                events: None,
            }
        }

        fn with_refresh_once(file: &'static [u8], kind: ErrorKind) -> Self {
            Self {
                refresh_once: Mutex::new(Some(kind)),
                ..Self::from_file(file)
            }
        }

        fn record_event(&self, event: &'static str) {
            if let Some(events) = &self.events {
                events.lock().expect("events").push(event);
            }
        }

        fn next_write_outcome(&self) -> WorkerWriteOutcome {
            self.write_outcomes
                .lock()
                .expect("write outcomes")
                .pop_front()
                .unwrap_or(WorkerWriteOutcome::Ok)
        }
    }

    impl Default for MockDataClient {
        fn default() -> Self {
            Self::from_file(b"")
        }
    }

    #[async_trait]
    impl WorkerDataClient for MockDataClient {
        async fn read_block_range(
            &self,
            _ctx: AttemptContext,
            _group_name: GroupName,
            block_read: &PlannedBlockRead,
        ) -> ClientResult<WorkerReadResult> {
            let call_number = {
                let mut calls = self.calls.lock().expect("calls");
                *calls += 1;
                *calls
            };
            if call_number == 1 {
                if let Some(reason) = self.refresh_once.lock().expect("refresh").take() {
                    return Err(refresh_action_error(reason));
                }
            }
            let start = block_read.file_offset as usize;
            let end = start + block_read.len as usize;
            Ok(WorkerReadResult {
                bytes: self.file.slice(start..end),
            })
        }

        async fn open_write_block(
            &self,
            _ctx: AttemptContext,
            target: WorkerWriteTarget,
            lease_expires_at_ms: u64,
        ) -> ClientResult<BlockWrite> {
            self.record_event("write_block");
            let outcome = self.next_write_outcome();
            if matches!(outcome, WorkerWriteOutcome::CapacityRejected) {
                let mut metadata = tonic::metadata::MetadataMap::new();
                metadata.insert(
                    HEADER_WORKER_DATA_REJECTION,
                    tonic::metadata::MetadataValue::from_static(WORKER_DATA_REJECTION_CAPACITY_BEFORE_SIDE_EFFECT),
                );
                return Err(ClientError::from(tonic::Status::with_metadata(
                    tonic::Code::ResourceExhausted,
                    "injected Worker RPC capacity exhaustion",
                    metadata,
                )));
            }
            let events = self.events.clone();
            let (requests, mut request_stream) = mpsc::channel::<BlockWriteInput>(1);
            let (transport_cancellation, mut cancellation_signal) = watch::channel(false);
            let lease = Arc::new(BlockWriteLease::new(lease_expires_at_ms));
            let completion = tokio::spawn(async move {
                if matches!(
                    outcome,
                    WorkerWriteOutcome::HoldRequests | WorkerWriteOutcome::HoldCancellation
                ) {
                    let _ = cancellation_signal.changed().await;
                    if let Some(events) = &events {
                        events.lock().expect("events").push("cancel_write_block");
                    }
                    if matches!(outcome, WorkerWriteOutcome::HoldCancellation) {
                        std::future::pending::<()>().await;
                    }
                    return Err(ClientError::Worker("mock WriteBlock cancelled".to_string()));
                }
                loop {
                    tokio::select! {
                        biased;
                        _ = cancellation_signal.changed() => {
                            if let Some(events) = &events {
                                events.lock().expect("events").push("cancel_write_block");
                            }
                            return Err(ClientError::Worker("mock WriteBlock cancelled".to_string()));
                        }
                        request = request_stream.recv() => match request {
                            Some(BlockWriteInput::Data(request)) => match request.payload {
                                Some(Payload::Data(_)) => {}
                                _ => {
                                    return Err(ClientError::Worker(
                                        "mock WriteBlock received a non-data frame after acknowledgement".to_string(),
                                    ));
                                }
                            }
                            Some(BlockWriteInput::Finish) => break,
                            None => std::future::pending::<()>().await,
                        }
                    }
                }
                if matches!(outcome, WorkerWriteOutcome::WorkerError) {
                    return Err(ClientError::Worker("injected WriteBlock failure".to_string()));
                }
                Ok(())
            });
            Ok(BlockWrite::new(
                target.target,
                requests,
                transport_cancellation,
                lease,
                completion,
            ))
        }
    }

    fn data_plane(client: Arc<MockDataClient>) -> WorkerDataPlane {
        WorkerDataPlane::with_client(client)
    }

    fn read_reader(client: &FsClient, file_size: u64) -> FileReader {
        FileReader::new(
            Arc::clone(&client.runtime),
            ReadHandle::new("/alpha".to_string(), InodeId::new(202), 3, file_size),
        )
    }

    fn layout_response(
        group_name: &str,
        inode_id: u64,
        content_revision: Option<u64>,
        file_size: u64,
        locations: Vec<FileBlockLocation>,
    ) -> ReadLayout {
        ReadLayout {
            group_name: group_name_from(group_name),
            inode_id: InodeId::new(inode_id),
            file_size,
            content_revision,
            locations,
        }
    }

    fn file_attrs_proto(size: u64) -> beryl_proto::metadata::FileAttrsProto {
        beryl_proto::metadata::FileAttrsProto {
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

    fn write_handle_proto(inode_id: u64) -> WriteHandleProto {
        WriteHandleProto {
            inode_id,
            write_lease_epoch: 1,
        }
    }

    fn inode_id_from_write_handle(handle: &WriteHandleProto) -> u64 {
        handle.inode_id
    }

    fn write_handle_for_tests(path: &str, base_size: u64, expires_at_ms: u64) -> ClientResult<WriteHandle> {
        let inode_id = InodeId::new(302);
        let layout = beryl_types::FileLayout::try_from(layout_proto(default_layout()))
            .map_err(|err| ClientError::InvalidLayout(err.to_string()))?;
        let session = WriteSession::new(
            path.to_string(),
            layout,
            write_handle_proto(inode_id.as_raw()),
            base_size,
            expires_at_ms,
            0,
            beryl_proto::metadata::OpenWriteModeProto::OpenWriteModeWrite,
        )?;
        Ok(WriteHandle::new(path.to_string(), base_size, session))
    }

    fn write_target_with_layout(
        inode_id: u64,
        block_index: u32,
        file_offset: u64,
        layout: RecordedLayout,
    ) -> WriteTarget {
        let block_id = BlockId::new(InodeId::new(inode_id), BlockIndex::new(block_index));
        WriteTarget {
            block_id,
            file_offset,
            block_size: u64::from(layout.block_size),
            worker_endpoints: vec![worker_endpoint()],
            fencing_token: FencingToken::new(block_id, ClientId::new(7), 1),
            block_stamp: 1,
            chunk_size: layout.chunk_size,
            block_format_id: beryl_types::BlockFormatId::from_raw(layout.block_format_id)
                .expect("known test block format"),
            tier: beryl_types::Tier::Hdd,
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

    fn location(inode_id: u64, block_index: u32, file_offset: u64, len: u64) -> FileBlockLocation {
        FileBlockLocation {
            block_id: BlockId::new(InodeId::new(inode_id), BlockIndex::new(block_index)),
            file_offset,
            len,
            workers: vec![worker_endpoint()],
            block_stamp: u64::from(block_index) + 1,
            block_format_id: beryl_types::BlockFormatId::CURRENT_FOR_NEW_FILE,
            block_size: DEFAULT_BLOCK_SIZE as u64,
            chunk_size: DEFAULT_CHUNK_SIZE,
            effective_len: len,
        }
    }

    fn refresh_action_error(kind: ErrorKind) -> ClientError {
        let rpc_error = RpcErrorDetail::refresh_metadata(
            kind,
            RpcRefreshHint {
                worker_resolve_required: true,
                ..RpcRefreshHint::default()
            },
            "worker requested refresh",
        );
        ClientError::from(ClientAction::Refresh {
            hint: Box::new(RefreshHint {
                worker_resolve_required: true,
                ..RefreshHint::default()
            }),
            rpc_error: Box::new(rpc_error),
        })
    }

    fn server_retry_error() -> ClientError {
        let rpc_error = RpcErrorDetail::retry(
            ErrorKind::Internal(InternalErrorKind::NodeUnavailable),
            Some(1),
            "server requested retry",
        );
        ClientError::from(ClientAction::Retry {
            retry_after_ms_hint: Some(1),
            rpc_error: Box::new(rpc_error),
        })
    }
}
