// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! WorkerDataService: gRPC service implementation for ReadChunk/ReadRange.

use anyhow::Result;
use bytes::Bytes;
use futures::StreamExt;
use std::sync::Arc;
use std::time::Instant;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info_span, warn};

use proto::worker::worker_data_service_server::WorkerDataService;
use proto::worker::{
    ChunkDataProto as ProtoChunkData, ReadChunkRequestProto, ReadChunkResponseProto, ReadRangeChunkProto,
    ReadRangeRequestProto, WriteChunkRequestProto, WriteChunkResponseProto,
};

use common::error::canonical::RefreshReason;
use common::header::{RequestHeader, RpcErrorCode};
use tracing::Instrument;
use types::chunk::ChunkSlice;
use types::ids::{ClientId, DataHandleId, ShardGroupId, WorkerId};
use types::layout::FileLayout;

use crate::block_manager::BlockManager;
use crate::block_store::BlockStore;
use crate::convert::{chunk_slice_to_proto, proto_to_byte_range, proto_to_chunk_ref, proto_to_fencing_token};
use crate::pipeline::ChunkMerger;
use crate::stream_manager::{StreamManager, StreamMode, StreamState};
use crate::ufs_fill::UfsFiller;
use common::audit::AuditLogger;

/// Worker data service implementation.
#[derive(Clone)]
pub struct WorkerDataServiceImpl {
    block_store: Arc<BlockStore>,
    block_manager: Option<Arc<BlockManager>>,
    audit_logger: Arc<AuditLogger>,
    layout: FileLayout,
    worker_id: WorkerId,
    worker_epoch: u64,
    // Default group_id until metadata routing provides the actual value
    default_group_id: ShardGroupId,
    // UFS filler for read-through cache misses
    ufs_filler: Option<Arc<UfsFiller>>,
    // Replication client (optional, for auto-replication)
    replication_client: Option<Arc<dyn crate::block_manager::ReplicationClient + Send + Sync>>,
    // Stream manager for tracking active streams
    stream_manager: Arc<StreamManager>,
}

impl WorkerDataServiceImpl {
    pub fn new(
        block_store: Arc<BlockStore>,
        audit_logger: Arc<AuditLogger>,
        layout: FileLayout,
        worker_id: WorkerId,
        worker_epoch: u64,
    ) -> Self {
        let block_manager = Arc::new(BlockManager::new(Arc::clone(&block_store), layout.clone()));
        let stream_manager = Arc::new(StreamManager::with_default_timeout());
        Self {
            block_store,
            block_manager: Some(block_manager),
            audit_logger,
            layout,
            worker_id,
            worker_epoch,
            default_group_id: ShardGroupId::new(0), // Placeholder: use default group until routing is wired
            ufs_filler: None,
            replication_client: None,
            stream_manager,
        }
    }

    /// Create with replication client for auto-replication.
    pub fn with_replication(
        block_store: Arc<BlockStore>,
        audit_logger: Arc<AuditLogger>,
        layout: FileLayout,
        worker_id: WorkerId,
        worker_epoch: u64,
        replication_client: Arc<dyn crate::block_manager::ReplicationClient + Send + Sync>,
    ) -> Self {
        let block_manager = Arc::new(BlockManager::new(Arc::clone(&block_store), layout.clone()));
        let stream_manager = Arc::new(StreamManager::with_default_timeout());
        Self {
            block_store,
            block_manager: Some(block_manager),
            audit_logger,
            layout,
            worker_id,
            worker_epoch,
            default_group_id: ShardGroupId::new(0),
            ufs_filler: None,
            replication_client: Some(replication_client),
            stream_manager,
        }
    }

    /// Create with UFS filler for read-through cache misses.
    pub fn with_ufs_filler(
        block_store: Arc<BlockStore>,
        audit_logger: Arc<AuditLogger>,
        layout: FileLayout,
        worker_id: WorkerId,
        worker_epoch: u64,
        ufs_filler: Arc<UfsFiller>,
    ) -> Self {
        let block_manager = Arc::new(BlockManager::new(Arc::clone(&block_store), layout.clone()));
        let stream_manager = Arc::new(StreamManager::with_default_timeout());
        Self {
            block_store,
            block_manager: Some(block_manager),
            audit_logger,
            layout,
            worker_id,
            worker_epoch,
            default_group_id: ShardGroupId::new(0),
            ufs_filler: Some(ufs_filler),
            replication_client: None,
            stream_manager,
        }
    }

    #[cfg(test)]
    pub(crate) fn stream_manager_for_test(&self) -> Arc<StreamManager> {
        Arc::clone(&self.stream_manager)
    }

    /// Extract RequestHeader from RequestHeaderProto (proto).
    fn extract_caller_context(req_header: &Option<proto::common::RequestHeaderProto>) -> RequestHeader {
        if let Some(header) = req_header {
            RequestHeader::try_from(header.clone()).unwrap_or_else(|e| {
                warn!(error = %e, "Failed to parse RequestHeaderProto, using default");
                RequestHeader::new(types::ClientId::new(0))
            })
        } else {
            // Generate default header
            RequestHeader::new(types::ClientId::new(0))
        }
    }

    /// Extract DataRequestHeader from DataRequestHeaderProto (proto).
    fn extract_data_request_header(
        req_header: &Option<proto::worker::DataRequestHeaderProto>,
    ) -> crate::data_header::DataRequestHeader {
        if let Some(header) = req_header {
            crate::data_header::DataRequestHeader::from_proto(header.clone()).unwrap_or_else(|e| {
                warn!(error = %e, "Failed to parse DataRequestHeaderProto, using default");
                crate::data_header::DataRequestHeader::new(common::header::ClientInfo::new(types::ClientId::new(0)))
            })
        } else {
            // Generate default header
            crate::data_header::DataRequestHeader::new(common::header::ClientInfo::new(types::ClientId::new(0)))
        }
    }

    /// Extract RequestHeader from gRPC Request metadata (for requests without header field).
    fn extract_caller_context_from_metadata<T>(request: &Request<T>) -> RequestHeader {
        use common::header::RequestHeader;
        let metadata = request.metadata();
        let mut headers: Vec<(String, String)> = Vec::new();
        // Extract common headers manually
        if let Some(call_id) = metadata.get("x-call-id") {
            if let Ok(s) = call_id.to_str() {
                headers.push(("x-call-id".to_string(), s.to_string()));
            }
        }
        if let Some(client_id) = metadata.get("x-client-id") {
            if let Ok(s) = client_id.to_str() {
                headers.push(("x-client-id".to_string(), s.to_string()));
            }
        }
        if let Some(state_id) = metadata.get("x-state-id") {
            if let Ok(s) = state_id.to_str() {
                headers.push(("x-state-id".to_string(), s.to_string()));
            }
        }
        if let Some(traceparent) = metadata.get("traceparent") {
            if let Ok(s) = traceparent.to_str() {
                headers.push(("traceparent".to_string(), s.to_string()));
            }
        }
        if let Some(deadline) = metadata.get("x-deadline-ms") {
            if let Ok(s) = deadline.to_str() {
                headers.push(("x-deadline-ms".to_string(), s.to_string()));
            }
        }
        if let Some(grpc_timeout) = metadata.get("grpc-timeout") {
            if let Ok(s) = grpc_timeout.to_str() {
                headers.push(("grpc-timeout".to_string(), s.to_string()));
            }
        }
        RequestHeader::from_grpc_metadata(headers.into_iter())
    }

    /// Determine request source (DirectWorkerRead vs MetadataRoutedRead).
    fn determine_source(_req_ctx: &RequestHeader) -> String {
        // Currently always DirectWorkerRead (metadata routing not yet hooked up)
        "DirectWorkerRead".to_string()
    }

    /// Generate file path from data_handle_id (for audit logging).
    /// Currently constructs a path from data_handle_id; metadata-provided paths can replace this later.
    fn file_path_from_id(data_handle_id: DataHandleId) -> String {
        format!("/file/{}", data_handle_id.as_raw())
    }

    /// Create a response header with call_id from request header.
    fn create_response_header(&self, req_header: &RequestHeader) -> common::header::ResponseHeader {
        common::header::ResponseHeader::ok(req_header.client.clone())
    }

    /// Create audit record with path and block information.
    fn create_audit_record(
        operation: String,
        caller_ctx: &RequestHeader,
        source: &str,
        result: String,
        bytes: u64,
        latency_ms: f64,
        block_id: Option<types::ids::BlockId>,
        chunk_ref: Option<String>,
    ) -> common::audit::AuditRecord {
        let path = block_id.map(|bid| Self::file_path_from_id(bid.data_handle_id));
        let block_id_str = block_id.map(|bid| format!("{}:{}", bid.data_handle_id.as_raw(), bid.index.as_raw()));

        common::audit::AuditRecord {
            timestamp: chrono::Utc::now().to_rfc3339(),
            request_id: caller_ctx.client.call_id.to_string(),
            client_id: caller_ctx.client.client_id.as_raw(),
            operation,
            path,
            block_id: block_id_str,
            chunk_ref,
            source: source.to_string(),
            result,
            bytes,
            latency_ms,
        }
    }

    /// Map UFS error to gRPC status with retry information.
    fn map_ufs_error_to_status(err: ufs::UfsError) -> Status {
        use ufs::UfsError;
        match &err {
            UfsError::NotFound(msg) => Status::not_found(msg.clone()),
            UfsError::PermissionDenied(msg) => Status::permission_denied(msg.clone()),
            UfsError::Overloaded(msg) => {
                // Retryable error with retry_after_ms hint
                let status = Status::unavailable(format!("UFS overloaded: {}", msg));
                // Extract retry_after_ms from message if present
                if let Some(retry_ms) = msg
                    .split("retry_after_ms: ")
                    .nth(1)
                    .and_then(|s| s.split_whitespace().next())
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    // Add retry hint to status details (if proto supports it)
                    // For now, just log it
                    tracing::debug!(retry_after_ms = retry_ms, "UFS overloaded, retry after");
                }
                status
            }
            UfsError::Backend(_opendal_err) => {
                // Backend errors are already mapped through UfsError
                // Check if it's a known error type by converting to string
                let err_msg = _opendal_err.to_string();
                if err_msg.contains("not found") || err_msg.contains("NotFound") {
                    Status::not_found(err_msg)
                } else if err_msg.contains("permission denied") || err_msg.contains("PermissionDenied") {
                    Status::permission_denied(err_msg)
                } else if err_msg.contains("rate limit") || err_msg.contains("RateLimited") {
                    Status::unavailable(format!("UFS rate limited: {}", err_msg))
                } else {
                    Status::internal(format!("UFS backend error: {}", err_msg))
                }
            }
            _ => Status::internal(format!("UFS error: {}", err)),
        }
    }
}

#[tonic::async_trait]
impl WorkerDataService for WorkerDataServiceImpl {
    async fn read_chunk(
        &self,
        request: Request<ReadChunkRequestProto>,
    ) -> Result<Response<ReadChunkResponseProto>, Status> {
        let start = Instant::now();

        // Extract header from metadata (high-frequency requests don't have header field)
        let caller_ctx = Self::extract_caller_context_from_metadata(&request);
        let source = Self::determine_source(&caller_ctx);

        let req = request.into_inner();

        // Convert proto to domain types
        let chunk_ref = proto_to_chunk_ref(
            req.chunk
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("missing chunk"))?,
        )?;

        let offset_in_chunk = req.offset_in_chunk;
        let len = req.len;
        let expected_version = req.expected_version;

        // Extract read_mode from request
        let read_mode = proto::common::ReadModeProto::from_i32(req.read_mode)
            .unwrap_or(proto::common::ReadModeProto::ReadModeUnspecified);
        let use_cache = match read_mode {
            proto::common::ReadModeProto::ReadModeDirect => false, // Skip cache, go directly to UFS
            proto::common::ReadModeProto::ReadModeCached => true,  // Use cache if available
            proto::common::ReadModeProto::ReadModeUnspecified => true, // Default to cached
        };

        let span = info_span!(
            "worker.read_chunk",
            request_id = %caller_ctx.client.call_id,
            chunk = %chunk_ref,
            offset = offset_in_chunk,
            len = len,
            expected_version = expected_version,
            read_mode = ?read_mode,
            use_cache = use_cache,
        );

        let block_store = Arc::clone(&self.block_store);
        let audit_logger = Arc::clone(&self.audit_logger);
        let group_id = self.default_group_id;
        let chunk_ref_clone = chunk_ref.clone();

        async move {
            // Get current block version (use layout_version if available, otherwise committed_length)
            let current_version = {
                let block_id = chunk_ref_clone.block_id;
                match block_store.block_meta(group_id, block_id) {
                    Ok(Some(block_meta)) => {
                        // Use layout_version if available, otherwise fall back to committed_length
                        block_meta.layout_version.unwrap_or(block_meta.committed_length)
                    }
                    _ => {
                        // Block doesn't exist, version is 0
                        0
                    }
                }
            };

            // Check version if expected_version is provided (non-zero)
            if expected_version > 0 && expected_version != current_version {
                warn!(
                    expected_version = expected_version,
                    current_version = current_version,
                    block_id = %chunk_ref_clone.block_id,
                    "Version mismatch in read_chunk"
                );
                return Err(Status::new(
                    tonic::Code::FailedPrecondition,
                    format!(
                        "Version mismatch: expected {}, got {}",
                        expected_version, current_version
                    ),
                ));
            }

            // Check if chunk exists locally (only if use_cache is true)
            let slice = ChunkSlice {
                chunk: chunk_ref_clone.clone(),
                offset_in_chunk,
                len,
            };

            // Handle ReadMode: use_cache determines if we should try cache first
            if use_cache {
                // ReadMode::Cached: try cache first, fallback to UFS on miss
                match block_store.read_chunk_stream(group_id, slice.clone()).await {
                    Ok(Some(bytes)) => {
                        // Cache hit: use cached data
                        let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
                        let bytes_transferred = bytes.len() as u64;

                        // Audit log with path and block info
                        audit_logger.log(Self::create_audit_record(
                            "ReadChunk".to_string(),
                            &caller_ctx,
                            &source,
                            "Success".to_string(),
                            bytes_transferred,
                            latency_ms,
                            Some(chunk_ref_clone.block_id),
                            Some(format!(
                                "{}:{}:{}",
                                chunk_ref_clone.block_id.data_handle_id.as_raw(),
                                chunk_ref_clone.block_id.index.as_raw(),
                                chunk_ref_clone.chunk_idx
                            )),
                        ));

                        // Build response
                        // Proto now supports Bytes, so we can use it directly (zero-copy)
                        let chunk_data = ProtoChunkData {
                            slice: Some(chunk_slice_to_proto(&slice)),
                            data: bytes,   // Zero-copy: Bytes type
                            checksum32: 0, // TODO: compute checksum
                        };

                        return Ok(Response::new(ReadChunkResponseProto {
                            data: Some(chunk_data),
                            current_version,
                        }));
                    }
                    Ok(None) => {
                        // Cache miss: try UFS read-through if available
                        if let Some(ref ufs_filler) = self.ufs_filler {
                            match ufs_filler
                                .read_chunk_slice_stream(group_id, slice.clone(), &caller_ctx)
                                .await
                            {
                                Ok(Some(ufs_data)) => {
                                    // UFS read successful, data already filled back
                                    let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
                                    let bytes_transferred: u64 = ufs_data.len() as u64;

                                    // Audit log with path and block info
                                    audit_logger.log(Self::create_audit_record(
                                        "ReadChunk".to_string(),
                                        &caller_ctx,
                                        "UfsReadThrough",
                                        "Success".to_string(),
                                        bytes_transferred,
                                        latency_ms,
                                        Some(chunk_ref_clone.block_id),
                                        Some(format!(
                                            "{}:{}:{}",
                                            chunk_ref_clone.block_id.data_handle_id.as_raw(),
                                            chunk_ref_clone.block_id.index.as_raw(),
                                            chunk_ref_clone.chunk_idx
                                        )),
                                    ));

                                    // Build response with UFS data
                                    // Proto now supports Bytes, so we can use it directly
                                    let chunk_data = ProtoChunkData {
                                        slice: Some(chunk_slice_to_proto(&slice)),
                                        data: ufs_data, // Zero-copy: Bytes type
                                        checksum32: 0,  // TODO: compute checksum
                                    };

                                    return Ok(Response::new(ReadChunkResponseProto {
                                        data: Some(chunk_data),
                                        current_version,
                                    }));
                                }
                                Ok(None) => {
                                    // UFS returned no data
                                    warn!("UFS returned no data for chunk: {}", chunk_ref_clone);
                                    return Err(Status::not_found("chunk not found in cache or UFS"));
                                }
                                Err(e) => {
                                    // Map UFS error to gRPC status
                                    error!(error = %e, "UFS read failed");
                                    let status = Self::map_ufs_error_to_status(e);
                                    return Err(status);
                                }
                            }
                        } else {
                            return Err(Status::not_found(
                                "chunk not found in cache and no UFS filler available",
                            ));
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to read chunk from cache");
                        return Err(Status::internal(format!("Failed to read chunk: {}", e)));
                    }
                }
            } else {
                // ReadMode::Direct: skip cache, go directly to UFS
                if let Some(ref ufs_filler) = self.ufs_filler {
                    match ufs_filler
                        .read_chunk_slice_stream(group_id, slice.clone(), &caller_ctx)
                        .await
                    {
                        Ok(Some(ufs_data)) => {
                            // UFS read successful (direct mode, no cache fill)
                            let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
                            let bytes_transferred: u64 = ufs_data.len() as u64;

                            // Audit log with path and block info
                            audit_logger.log(Self::create_audit_record(
                                "ReadChunk".to_string(),
                                &caller_ctx,
                                "UfsDirect",
                                "Success".to_string(),
                                bytes_transferred,
                                latency_ms,
                                Some(chunk_ref_clone.block_id),
                                Some(format!(
                                    "{}:{}:{}",
                                    chunk_ref_clone.block_id.data_handle_id.as_raw(),
                                    chunk_ref_clone.block_id.index.as_raw(),
                                    chunk_ref_clone.chunk_idx
                                )),
                            ));

                            // Build response with UFS data
                            // Proto now supports Bytes, so we can use it directly (zero-copy)
                            let chunk_data = ProtoChunkData {
                                slice: Some(chunk_slice_to_proto(&slice)),
                                data: ufs_data, // Zero-copy: Bytes type
                                checksum32: 0,  // TODO: compute checksum
                            };

                            return Ok(Response::new(ReadChunkResponseProto {
                                data: Some(chunk_data),
                                current_version,
                            }));
                        }
                        Ok(None) => {
                            return Err(Status::not_found("chunk not found in UFS"));
                        }
                        Err(e) => {
                            error!(error = %e, "UFS read failed");
                            let status = Self::map_ufs_error_to_status(e);
                            return Err(status);
                        }
                    }
                } else {
                    return Err(Status::failed_precondition(
                        "ReadMode::Direct requires UFS filler but none available",
                    ));
                }
            }
        }
        .instrument(span)
        .await
    }

    async fn write_chunk(
        &self,
        request: Request<WriteChunkRequestProto>,
    ) -> Result<Response<WriteChunkResponseProto>, Status> {
        let start = Instant::now();

        // Extract header from metadata (high-frequency requests don't have header field)
        let caller_ctx = Self::extract_caller_context_from_metadata(&request);
        let source = Self::determine_source(&caller_ctx);

        let req = request.into_inner();

        // Validate fencing token
        // For replication requests, check if it's from internal replication
        let token = proto_to_fencing_token(
            req.token
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("missing fencing token"))?,
        )?;

        // Epoch/fencing checks (reject before side effects)
        let mut header = crate::data_header::DataResponseHeader::ok(caller_ctx.client.clone());
        if req.worker_epoch != 0 && req.worker_epoch != self.worker_epoch {
            header = crate::data_header::DataResponseHeader::need_refresh(
                caller_ctx.client.clone(),
                RefreshReason::WorkerEpochMismatch,
                RpcErrorCode::WorkerEpochMismatch,
                "worker_epoch mismatch".to_string(),
            );
            header.worker_epoch = Some(self.worker_epoch);
            return Ok(Response::new(WriteChunkResponseProto {
                header: Some(header.to_proto()),
                stored: false,
            }));
        }
        if token.epoch == 0 {
            header = crate::data_header::DataResponseHeader::need_refresh(
                caller_ctx.client.clone(),
                RefreshReason::Fencing,
                RpcErrorCode::Fencing,
                "fencing token epoch missing".to_string(),
            );
            header.worker_epoch = Some(self.worker_epoch);
            return Ok(Response::new(WriteChunkResponseProto {
                header: Some(header.to_proto()),
                stored: false,
            }));
        }

        // Check if this is a replication request (by checking client_id in context)
        // Replication uses client_id=0, so we can identify it
        let is_replication = caller_ctx.client.client_id.as_raw() == 0;

        let ok_header_proto = header.to_proto();

        if is_replication {
            // Replication path: allow if epoch=0 (skip mode) or valid token
            if token.epoch == 0 {
                // Skip mode: allow but record metrics
                // TODO: metrics::counter!("worker_replication_fencing_bypass_total", "mode" => "skip", "reason" => "epoch_zero").increment(1);
            } else {
                // Special/strict mode: validate token (for now, just check it exists)
                // TODO: metrics::counter!("worker_replication_requests_total", "result" => "success").increment(1);
            }
        } else {
            // Client path: strict validation (future: implement full token validation)
            // For now, just check token exists
        }

        // Extract chunk data
        let chunk_data = req.data.ok_or_else(|| Status::invalid_argument("missing chunk data"))?;

        let chunk_ref = proto_to_chunk_ref(
            chunk_data
                .slice
                .as_ref()
                .and_then(|s| s.chunk.as_ref())
                .ok_or_else(|| Status::invalid_argument("missing chunk in data"))?,
        )?;

        // Extract write_mode from request
        let write_mode = proto::common::WriteModeProto::from_i32(req.write_mode)
            .unwrap_or(proto::common::WriteModeProto::WriteModeUnspecified);

        // Determine write strategy based on WriteModeProto
        let (write_to_blockstore, write_to_ufs, sync_ufs) = match write_mode {
            proto::common::WriteModeProto::WriteModeThrough => {
                // Write-through: write to BlockStore and sync write to UFS
                (true, true, true)
            }
            proto::common::WriteModeProto::WriteModeBack => {
                // Write-back: write to BlockStore, async write to UFS (or skip)
                (true, false, false)
            }
            proto::common::WriteModeProto::WriteModeDirect => {
                // Direct: write directly to UFS, bypass BlockStore
                (false, true, true)
            }
            proto::common::WriteModeProto::WriteModeUnspecified => {
                // Default to write-back
                (true, false, false)
            }
        };

        let span = info_span!(
            "worker.write_chunk",
            request_id = %caller_ctx.client.call_id,
            chunk = %chunk_ref,
            write_id = req.write_id,
            write_mode = ?write_mode,
            write_to_blockstore = write_to_blockstore,
            write_to_ufs = write_to_ufs,
            sync_ufs = sync_ufs,
        );

        let block_store = Arc::clone(&self.block_store);
        let audit_logger = Arc::clone(&self.audit_logger);
        let group_id = self.default_group_id;
        let chunk_ref_clone = chunk_ref.clone();
        let layout = self.layout.clone();
        let ufs_filler_opt = self.ufs_filler.clone();

        async move {
            // Create stream from bytes (convert to async reader)
            let data_bytes = Bytes::from(chunk_data.data);
            let data_bytes_len = data_bytes.len() as u64;
            let data_bytes_clone = data_bytes.clone();

            // Handle WriteMode::Direct: write directly to UFS, bypass BlockStore
            if !write_to_blockstore && write_to_ufs {
                // WriteMode::Direct: write directly to UFS
                if let Some(ref ufs_filler) = ufs_filler_opt {
                    let ufs_registry = ufs_filler.ufs_registry();
                    let ufs_id = ufs_filler
                        .default_ufs_id()
                        .cloned()
                        .or_else(|| ufs_registry.list_ids().first().cloned())
                        .ok_or_else(|| Status::failed_precondition("No UFS instance available"))?;

                    let ufs_access = ufs_registry
                        .get(&ufs_id)
                        .ok_or_else(|| Status::failed_precondition(format!("UFS instance {} not found", ufs_id)))?;

                    // Calculate UFS path (same format as read)
                    let data_handle_id = chunk_ref_clone.block_id.data_handle_id;
                    let path = format!("{}", data_handle_id.as_raw());

                    // Write to UFS
                    ufs_access
                        .write_all(&path, data_bytes_clone.clone(), &caller_ctx)
                        .await
                        .map_err(|e| {
                            error!(error = %e, "Failed to write to UFS");
                            Status::internal(format!("Failed to write to UFS: {}", e))
                        })?;

                    let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
                    let bytes_transferred = data_bytes_len;

                    // Audit log
                    audit_logger.log(Self::create_audit_record(
                        "WriteChunk".to_string(),
                        &caller_ctx,
                        "UfsDirect",
                        "Success".to_string(),
                        bytes_transferred,
                        latency_ms,
                        Some(chunk_ref_clone.block_id),
                        Some(format!(
                            "{}:{}:{}",
                            chunk_ref_clone.block_id.data_handle_id.as_raw(),
                            chunk_ref_clone.block_id.index.as_raw(),
                            chunk_ref_clone.chunk_idx
                        )),
                    ));

                    return Ok(Response::new(WriteChunkResponseProto {
                        header: Some(ok_header_proto.clone()),
                        stored: true,
                    }));
                } else {
                    return Err(Status::failed_precondition(
                        "WriteMode::Direct requires UFS filler but none available",
                    ));
                }
            }

            // Write to BlockStore (for WriteMode::Through and WriteMode::Back)
            if write_to_blockstore {
                let stream_data = data_bytes_clone.clone();
                let mut stream: tokio::io::BufReader<std::io::Cursor<Bytes>> =
                    tokio::io::BufReader::new(std::io::Cursor::new(stream_data));

                // Write chunk stream (zero-copy: temporary file + atomic rename)
                block_store
                    .write_chunk_stream(group_id, chunk_ref_clone.clone(), &mut stream)
                    .await
                    .map_err(|e| {
                        error!(error = %e, "Failed to write chunk");
                        Status::internal(format!("Failed to write chunk: {}", e))
                    })?;
            }

            // Handle UFS write (for WriteMode::Through: sync write to UFS)
            if write_to_ufs && sync_ufs {
                // WriteMode::Through: sync write to UFS after BlockStore write
                if let Some(ref ufs_filler) = ufs_filler_opt {
                    let ufs_registry = ufs_filler.ufs_registry();
                    let ufs_id = ufs_filler
                        .default_ufs_id()
                        .cloned()
                        .or_else(|| ufs_registry.list_ids().first().cloned());

                    if let Some(ufs_id) = ufs_id {
                        if let Some(ufs_access) = ufs_registry.get(&ufs_id) {
                            let data_handle_id = chunk_ref_clone.block_id.data_handle_id;
                            let path = format!("{}", data_handle_id.as_raw());

                            // Sync write to UFS
                            if let Err(e) = ufs_access.write_all(&path, data_bytes_clone.clone(), &caller_ctx).await {
                                error!(error = %e, "Failed to sync write to UFS (write-through)");
                                // Log error but don't fail the request (BlockStore write succeeded)
                            } else {
                                debug!(
                                    chunk = %chunk_ref_clone,
                                    "WriteMode::Through: successfully wrote to UFS"
                                );
                            }
                        } else {
                            warn!("UFS instance {} not found in registry", ufs_id);
                        }
                    } else {
                        warn!("No UFS instance available for write-through");
                    }
                } else {
                    warn!("WriteMode::Through requested but no UFS filler available");
                }
            } else if write_to_blockstore && !write_to_ufs {
                // WriteMode::Back: async write to UFS (optional, can be handled by background task)
                // For now, we skip UFS write in write-back mode
                // Background task can handle UFS sync later
                debug!(
                    chunk = %chunk_ref_clone,
                    "WriteMode::Back: UFS write will be handled asynchronously by background task"
                );
            }

            // Check if block is complete and log for monitoring
            // Actual replication is triggered by metadata commands (ReplicateCommand)
            // This allows metadata to control placement and replication factor
            let block_manager_opt = self.block_manager.clone();
            let block_id = chunk_ref_clone.block_id;
            let group_id_val = group_id;
            let layout_clone = self.layout.clone();

            tokio::spawn(async move {
                if let Some(block_manager) = block_manager_opt {
                    // Check if block is complete
                    if let Ok(true) = block_manager.is_block_complete(group_id_val, block_id) {
                        // Get expected chunks to determine if block is truly complete
                        let expected_chunks = layout_clone.chunks_per_block();
                        if let Ok(Some(block_meta)) = block_manager.block_meta(group_id_val, block_id) {
                            if block_meta.is_complete(expected_chunks) {
                                // Block is complete, log for monitoring
                                // Replication will be triggered by metadata service via ReplicateCommand
                                debug!(
                                    group_id = group_id_val.as_raw(),
                                    block_id = %block_id,
                                    "Block is complete, waiting for metadata replication command"
                                );
                            }
                        }
                    }
                }
            });

            let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
            let bytes_transferred = data_bytes_clone.len() as u64;

            // Audit log with path and block info
            audit_logger.log(Self::create_audit_record(
                "WriteChunk".to_string(),
                &caller_ctx,
                &source,
                "Success".to_string(),
                bytes_transferred,
                latency_ms,
                Some(chunk_ref_clone.block_id),
                Some(format!(
                    "{}:{}:{}",
                    chunk_ref_clone.block_id.data_handle_id.as_raw(),
                    chunk_ref_clone.block_id.index.as_raw(),
                    chunk_ref_clone.chunk_idx
                )),
            ));

            Ok(Response::new(WriteChunkResponseProto {
                header: Some(ok_header_proto),
                stored: true,
            }))
        }
        .instrument(span)
        .await
    }

    type ReadRangeStream = std::pin::Pin<Box<dyn futures::Stream<Item = Result<ReadRangeChunkProto, Status>> + Send>>;

    async fn read_range(
        &self,
        request: Request<ReadRangeRequestProto>,
    ) -> Result<Response<Self::ReadRangeStream>, Status> {
        let start = Instant::now();

        // Extract header from metadata (high-frequency requests don't have header field)
        let caller_ctx = Self::extract_caller_context_from_metadata(&request);
        let source = Self::determine_source(&caller_ctx);

        let req = request.into_inner();

        let data_handle_id = DataHandleId::new(req.data_handle_id);
        let byte_range = proto_to_byte_range(
            req.range
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("missing range"))?,
        );
        let expected_version = req.expected_version;

        let _span = info_span!(
            "worker.read_range",
            request_id = %caller_ctx.client.call_id,
            data_handle_id = data_handle_id.as_raw(),
            offset = byte_range.offset,
            len = byte_range.len,
            expected_version = expected_version,
        );

        // Use pipeline: convert range to chunks using unified function
        let block_index = self.layout.block_index_from_offset(byte_range.offset);
        let block_id = types::ids::BlockId::new(data_handle_id, block_index);
        let offset_in_block = (byte_range.offset - self.layout.block_start_offset(block_index)) as u32;

        // Get current file/block version for version check
        let current_version = {
            let group_id = self.default_group_id;
            match self.block_store.block_meta(group_id, block_id) {
                Ok(Some(block_meta)) => {
                    // Use layout_version if available, otherwise fall back to committed_length
                    block_meta.layout_version.unwrap_or(block_meta.committed_length)
                }
                _ => 0,
            }
        };

        // Check version if expected_version is provided (non-zero)
        if expected_version > 0 && expected_version != current_version {
            warn!(
                expected_version = expected_version,
                current_version = current_version,
                data_handle_id = data_handle_id.as_raw(),
                block_id = %block_id,
                "Version mismatch in read_range"
            );
            return Err(Status::new(
                tonic::Code::FailedPrecondition,
                format!(
                    "Version mismatch: expected {}, got {}",
                    expected_version, current_version
                ),
            ));
        }

        let chunk_ranges = crate::pipeline::range_to_chunks(&self.layout, block_id, offset_in_block, byte_range.len);

        // Create a stream that yields chunks (with chunk merging support)
        let block_store = Arc::clone(&self.block_store);
        let audit_logger = Arc::clone(&self.audit_logger);
        let ufs_filler = self.ufs_filler.clone();
        let group_id = self.default_group_id;
        let caller_ctx_clone = caller_ctx.clone();
        let source_clone = source.clone();
        let layout = self.layout;
        let chunk_ranges_clone = chunk_ranges.clone();
        let current_version_clone = current_version;

        let stream = async_stream::stream! {
            let mut total_bytes = 0u64;
            let mut miss_count = 0u64;
            let mut chunk_merger = ChunkMerger::new(layout.chunk_size); // Merge to chunk_size

            for (chunk_idx, offset_in_chunk, chunk_len) in chunk_ranges {
                // Read chunk using block-level API
                match block_store.read_chunk(group_id, block_id, chunk_idx).await {
                    Ok(Some(chunk_data)) => {
                        // Hit: extract the requested slice
                        let start = offset_in_chunk as usize;
                        let end = (start + chunk_len as usize).min(chunk_data.len());
                        let slice_data = chunk_data.slice(start..end);

                        // Add to merger (may trigger flush if buffer is full)
                        if let Some(merged) = chunk_merger.add_chunk(slice_data) {
                            total_bytes += merged.len() as u64;

                            // Create chunk slice for response
                            let chunk_ref = types::chunk::ChunkRef::new(block_id, chunk_idx.as_raw());
                            let chunk_slice = types::chunk::ChunkSlice {
                                chunk: chunk_ref,
                                offset_in_chunk: 0,
                                len: merged.len() as u32,
                            };

                            // Proto now supports Bytes, so we can use it directly (zero-copy)
                            let chunk_data_proto = ProtoChunkData {
                                slice: Some(chunk_slice_to_proto(&chunk_slice)),
                                data: merged, // Zero-copy: Bytes type
                                checksum32: 0,
                            };

                            yield Ok(ReadRangeChunkProto {
                                data: Some(chunk_data_proto),
                                current_version: current_version_clone,
                            });
                        }
                    }
                    Ok(None) => {
                        // Miss: try UFS read-through if available
                        if let Some(ref ufs_filler) = ufs_filler {
                            let chunk_ref = types::chunk::ChunkRef::new(block_id, chunk_idx.as_raw());
                            let chunk_slice = types::chunk::ChunkSlice {
                                chunk: chunk_ref,
                                offset_in_chunk,
                                len: chunk_len,
                            };

                            match ufs_filler.read_chunk_slice_stream(
                                group_id,
                                chunk_slice,
                                &caller_ctx_clone,
                            ).await {
                                Ok(Some(ufs_data)) => {
                                    // UFS read successful
                                    if let Some(merged) = chunk_merger.add_chunk(ufs_data) {
                                        total_bytes += merged.len() as u64;

                                        let chunk_ref = types::chunk::ChunkRef::new(block_id, chunk_idx.as_raw());
                                        let chunk_slice = types::chunk::ChunkSlice {
                                            chunk: chunk_ref,
                                            offset_in_chunk: 0,
                                            len: merged.len() as u32,
                                        };

                                        // Proto now supports Bytes, so we can use it directly (zero-copy)
                                        let chunk_data_proto = ProtoChunkData {
                                            slice: Some(chunk_slice_to_proto(&chunk_slice)),
                                            data: merged, // Zero-copy: Bytes type
                                            checksum32: 0,
                                        };

                                        yield Ok(ReadRangeChunkProto {
                                            data: Some(chunk_data_proto),
                                            current_version: current_version_clone,
                                        });
                                    }
                                    continue;
                                }
                                Ok(None) => {
                                    miss_count += 1;
                                    warn!("UFS returned no data for chunk: {}", chunk_idx.as_raw());
                                }
                                Err(e) => {
                                    miss_count += 1;
                                    error!(error = %e, "UFS read failed for chunk: {}", chunk_idx.as_raw());
                                }
                            }
                        } else {
                            miss_count += 1;
                            warn!("Chunk miss in ReadRange: {}", chunk_idx.as_raw());
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to read chunk in range");
                        miss_count += 1;
                    }
                }
            }

            // Flush remaining merged chunks
            if let Some(merged) = chunk_merger.flush() {
                total_bytes += merged.len() as u64;

                // Create final chunk slice (use last chunk index from ranges)
                let last_chunk_idx = chunk_ranges_clone.last().map(|(idx, _, _)| idx.as_raw()).unwrap_or(0);
                let chunk_ref = types::chunk::ChunkRef::new(block_id, last_chunk_idx);
                let chunk_slice = types::chunk::ChunkSlice {
                    chunk: chunk_ref,
                    offset_in_chunk: 0,
                    len: merged.len() as u32,
                };

                // Proto now supports Bytes, so we can use it directly (zero-copy)
                let chunk_data_proto = ProtoChunkData {
                    slice: Some(chunk_slice_to_proto(&chunk_slice)),
                    data: merged, // Zero-copy: Bytes type
                    checksum32: 0,
                };

                yield Ok(ReadRangeChunkProto {
                    data: Some(chunk_data_proto),
                    current_version: current_version_clone,
                });
            }

            // Audit log for entire range read (with path and block info)
            let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
            audit_logger.log(Self::create_audit_record(
                "ReadRange".to_string(),
                &caller_ctx_clone,
                &source_clone,
                if miss_count > 0 { "Partial" } else { "Success" }.to_string(),
                total_bytes,
                latency_ms,
                Some(block_id),
                None, // Range read spans multiple chunks
            ));
        };

        Ok(Response::new(Box::pin(stream)))
    }

    // Stream operations with block-level binding and block_stamp validation
    async fn open_read_stream(
        &self,
        request: Request<proto::worker::OpenReadStreamRequestProto>,
    ) -> Result<Response<proto::worker::OpenReadStreamResponseProto>, Status> {
        let req = request.into_inner();

        // Extract header
        let data_header = Self::extract_data_request_header(&req.header);
        let client = data_header.client.clone();

        // Extract block_id and range_in_block
        let block_id_proto = req
            .block_id
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing block_id"))?;
        let block_id = types::ids::BlockId::new(
            types::ids::DataHandleId::new(block_id_proto.data_handle_id),
            types::ids::BlockIndex::new(block_id_proto.block_index),
        );

        let range_in_block = req
            .range_in_block
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing range_in_block"))?;
        let range = proto_to_byte_range(range_in_block);

        let expected_block_stamp = req.expected_block_stamp;
        let preferred_chunk_size = req.preferred_chunk_size;

        // Get block metadata and validate
        let group_id = self.default_group_id;
        let block_meta = self
            .block_store
            .block_meta(group_id, block_id)
            .map_err(|e| Status::internal(format!("Failed to get block meta: {}", e)))?;

        let (current_block_stamp, committed_length) = if let Some(ref meta) = block_meta {
            // Use block_stamp if available, otherwise fall back to layout_version or committed_length
            let stamp = if meta.block_stamp > 0 {
                meta.block_stamp
            } else if let Some(lv) = meta.layout_version {
                lv
            } else {
                meta.committed_length
            };
            (stamp, meta.committed_length)
        } else {
            // Block doesn't exist on this worker
            let resp_header = crate::data_header::DataResponseHeader::need_refresh(
                client.clone(),
                common::error::canonical::RefreshReason::Moved,
                common::header::RpcErrorCode::ShardMoved,
                format!("Block {} not found on this worker", block_id),
            );
            return Ok(Response::new(proto::worker::OpenReadStreamResponseProto {
                header: Some(resp_header.to_proto()),
                stream_id: None,
                chunk_size: 0,
                flow_control_window: 0,
                current_block_stamp: 0,
                committed_length: 0,
            }));
        };

        // Validate block_stamp if provided
        if expected_block_stamp > 0 && expected_block_stamp != current_block_stamp {
            let resp_header = crate::data_header::DataResponseHeader::need_refresh(
                client.clone(),
                common::error::canonical::RefreshReason::BlockStampMismatch,
                common::header::RpcErrorCode::BlockStampMismatch,
                format!(
                    "Block stamp mismatch: expected {}, got {}",
                    expected_block_stamp, current_block_stamp
                ),
            );
            return Ok(Response::new(proto::worker::OpenReadStreamResponseProto {
                header: Some(resp_header.to_proto()),
                stream_id: None,
                chunk_size: 0,
                flow_control_window: 0,
                current_block_stamp,
                committed_length,
            }));
        }

        // Create stream state
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        block_id.hash(&mut hasher);
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            .hash(&mut hasher);
        std::process::id().hash(&mut hasher);
        let hash1 = hasher.finish();
        let mut hasher2 = DefaultHasher::new();
        hash1.hash(&mut hasher2);
        let hash2 = hasher2.finish();
        let stream_id = types::ids::StreamId::new((hash1 as u128) << 64 | (hash2 as u128));
        let chunk_size = preferred_chunk_size.max(4096).min(65536); // Negotiate chunk size
        let flow_control_window = chunk_size * 16; // 16 chunks window

        let stream_state = StreamState::new(
            stream_id,
            block_id,
            StreamMode::Read,
            Some(range),
            chunk_size,
            flow_control_window,
            current_block_stamp,
            committed_length,
        );

        // Register stream
        self.stream_manager.register(stream_state).await;

        // Create success response
        let resp_header = crate::data_header::DataResponseHeader::ok(client);
        Ok(Response::new(proto::worker::OpenReadStreamResponseProto {
            header: Some(resp_header.to_proto()),
            stream_id: Some(proto::common::StreamIdProto {
                high: (stream_id.as_raw() >> 64) as u64,
                low: stream_id.as_raw() as u64,
            }),
            chunk_size,
            flow_control_window,
            current_block_stamp,
            committed_length,
        }))
    }

    type ReadStreamStream =
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<proto::worker::ReadStreamResponseProto, Status>> + Send>>;

    async fn read_stream(
        &self,
        request: Request<proto::worker::ReadStreamRequestProto>,
    ) -> Result<Response<Self::ReadStreamStream>, Status> {
        let req = request.into_inner();

        // Extract stream_id
        let stream_id_proto = req
            .stream_id
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing stream_id"))?;
        let stream_id =
            types::ids::StreamId::new(((stream_id_proto.high as u128) << 64) | (stream_id_proto.low as u128));

        let max_bytes = req.max_bytes;

        // Get stream state
        let stream_state = self
            .stream_manager
            .get(stream_id)
            .await
            .ok_or_else(|| Status::not_found("Stream not found or expired"))?;

        // Validate stream is for reading
        if stream_state.mode != StreamMode::Read {
            return Err(Status::failed_precondition("Stream is not a read stream"));
        }

        let block_id = stream_state.block_id;
        let range = stream_state
            .range_in_block
            .ok_or_else(|| Status::internal("Read stream missing range_in_block"))?;
        let cursor = stream_state.cursor;
        let chunk_size = stream_state.chunk_size;
        let group_id = self.default_group_id;

        // Calculate read range
        let read_offset = range.offset + cursor;
        let remaining = range.len.saturating_sub(cursor as u32);
        let read_len = max_bytes.min(remaining);

        if read_len == 0 {
            // End of stream
            let stream =
                futures::stream::once(
                    async move { Ok(proto::worker::ReadStreamResponseProto { data: None, eos: true }) },
                );
            return Ok(Response::new(Box::pin(stream)));
        }

        // Read data from block_store
        let block_store = Arc::clone(&self.block_store);
        let stream_manager = Arc::clone(&self.stream_manager);

        let stream = async_stream::stream! {
            // Calculate which chunks to read
            let start_chunk = (read_offset / chunk_size as u64) as u32;
            let end_chunk = ((read_offset + read_len as u64 - 1) / chunk_size as u64) as u32;

            let mut total_read = 0u32;

            for chunk_idx in start_chunk..=end_chunk {
                let chunk_id = types::ids::ChunkId::new(block_id, types::ids::ChunkIndex::new(chunk_idx));

                // Read chunk
                match block_store.read_chunk(group_id, block_id, chunk_id.index).await {
                    Ok(Some(chunk_data)) => {
                        // Calculate slice within chunk
                        let chunk_start = chunk_idx as u64 * chunk_size as u64;
                        let offset_in_chunk = read_offset.saturating_sub(chunk_start) as u32;
                        let remaining_in_request = read_len - total_read;
                        let remaining_in_chunk = (chunk_data.len() as u32).saturating_sub(offset_in_chunk);
                        let slice_len = remaining_in_request.min(remaining_in_chunk);

                        if slice_len > 0 && offset_in_chunk < chunk_data.len() as u32 {
                            let end = (offset_in_chunk + slice_len).min(chunk_data.len() as u32);
                            let slice = chunk_data.slice(offset_in_chunk as usize..end as usize);

                            // Create ChunkDataProto
                            use crate::convert::chunk_ref_to_proto;
                            let chunk_ref = types::chunk::ChunkRef::new(block_id, chunk_idx);
                            let chunk_id_proto = chunk_ref_to_proto(&chunk_ref);

                            let chunk_slice = proto::worker::ChunkSliceProto {
                                chunk: Some(chunk_id_proto),
                                offset_in_chunk,
                                len: slice_len,
                            };

                            let chunk_data_proto = proto::worker::ChunkDataProto {
                                slice: Some(chunk_slice),
                                data: slice,
                                checksum32: 0, // TODO: compute checksum
                            };

                            // Update cursor
                            let new_cursor = cursor + slice_len as u64;
                            stream_manager.update_cursor(stream_id, new_cursor).await;

                            total_read += slice_len;

                            yield Ok(proto::worker::ReadStreamResponseProto {
                                data: Some(chunk_data_proto),
                                eos: false,
                            });
                        }
                    }
                    Ok(None) => {
                        // Chunk not found, skip
                        continue;
                    }
                    Err(e) => {
                        yield Err(Status::internal(format!("Failed to read chunk: {}", e)));
                        return;
                    }
                }
            }

            // Check if we've reached end of stream
            let new_cursor = cursor + total_read as u64;
            let eos = new_cursor >= range.len as u64;

            if eos {
                yield Ok(proto::worker::ReadStreamResponseProto {
                    data: None,
                    eos: true,
                });
            }
        };

        Ok(Response::new(Box::pin(stream)))
    }

    async fn open_write_stream(
        &self,
        request: Request<proto::worker::OpenWriteStreamRequestProto>,
    ) -> Result<Response<proto::worker::OpenWriteStreamResponseProto>, Status> {
        let req = request.into_inner();

        // Extract header
        let data_header = Self::extract_data_request_header(&req.header);
        let client = data_header.client.clone();

        // Extract block_id and fencing token
        let block_id_proto = req
            .block_id
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing block_id"))?;
        let block_id = types::ids::BlockId::new(
            types::ids::DataHandleId::new(block_id_proto.data_handle_id),
            types::ids::BlockIndex::new(block_id_proto.block_index),
        );

        let token = req
            .token
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing fencing token"))?;
        let _fencing_token = proto_to_fencing_token(token)?;

        let expected_block_stamp = req.expected_block_stamp;
        let preferred_chunk_size = req.preferred_chunk_size;

        // Validate fencing token (basic check - full validation should be done by block_manager)
        // TODO: Add proper fencing token validation

        // Get block metadata and validate
        let group_id = self.default_group_id;
        let block_meta = self
            .block_store
            .block_meta(group_id, block_id)
            .map_err(|e| Status::internal(format!("Failed to get block meta: {}", e)))?;

        let (current_block_stamp, committed_length) = if let Some(ref meta) = block_meta {
            let stamp = if meta.block_stamp > 0 {
                meta.block_stamp
            } else if let Some(lv) = meta.layout_version {
                lv
            } else {
                meta.committed_length
            };
            (stamp, meta.committed_length)
        } else {
            // Block doesn't exist on this worker
            let resp_header = crate::data_header::DataResponseHeader::need_refresh(
                client.clone(),
                common::error::canonical::RefreshReason::Moved,
                common::header::RpcErrorCode::ShardMoved,
                format!("Block {} not found on this worker", block_id),
            );
            return Ok(Response::new(proto::worker::OpenWriteStreamResponseProto {
                header: Some(resp_header.to_proto()),
                stream_id: None,
                chunk_size: 0,
                flow_control_window: 0,
                current_block_stamp: 0,
                committed_length: 0,
            }));
        };

        // Validate block_stamp if provided
        if expected_block_stamp > 0 && expected_block_stamp != current_block_stamp {
            let resp_header = crate::data_header::DataResponseHeader::need_refresh(
                client.clone(),
                common::error::canonical::RefreshReason::BlockStampMismatch,
                common::header::RpcErrorCode::BlockStampMismatch,
                format!(
                    "Block stamp mismatch: expected {}, got {}",
                    expected_block_stamp, current_block_stamp
                ),
            );
            return Ok(Response::new(proto::worker::OpenWriteStreamResponseProto {
                header: Some(resp_header.to_proto()),
                stream_id: None,
                chunk_size: 0,
                flow_control_window: 0,
                current_block_stamp,
                committed_length,
            }));
        }

        // Create stream state
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        block_id.hash(&mut hasher);
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            .hash(&mut hasher);
        std::process::id().hash(&mut hasher);
        let hash1 = hasher.finish();
        let mut hasher2 = DefaultHasher::new();
        hash1.hash(&mut hasher2);
        let hash2 = hasher2.finish();
        let stream_id = types::ids::StreamId::new((hash1 as u128) << 64 | (hash2 as u128));
        let chunk_size = preferred_chunk_size.max(4096).min(65536);
        let flow_control_window = chunk_size * 16;

        let stream_state = StreamState::new(
            stream_id,
            block_id,
            StreamMode::Write,
            None, // Write streams don't have range
            chunk_size,
            flow_control_window,
            current_block_stamp,
            committed_length,
        );
        let mut stream_state = stream_state;
        stream_state.lease_epoch = Some(token.epoch);
        stream_state.fencing_owner = Some(client.client_id);

        // Register stream
        self.stream_manager.register(stream_state).await;

        // Create success response
        let resp_header = crate::data_header::DataResponseHeader::ok(client);
        Ok(Response::new(proto::worker::OpenWriteStreamResponseProto {
            header: Some(resp_header.to_proto()),
            stream_id: Some(proto::common::StreamIdProto {
                high: (stream_id.as_raw() >> 64) as u64,
                low: stream_id.as_raw() as u64,
            }),
            chunk_size,
            flow_control_window,
            current_block_stamp,
            committed_length,
        }))
    }

    async fn write_stream(
        &self,
        request: Request<tonic::Streaming<proto::worker::WriteStreamRequestProto>>,
    ) -> Result<Response<proto::worker::WriteStreamResponseProto>, Status> {
        let mut stream = request.into_inner();
        let mut stream_id: Option<types::ids::StreamId> = None;
        let mut acknowledged_offset = 0u64;
        let mut stored = true;
        let mut resp_header =
            crate::data_header::DataResponseHeader::ok(common::header::ClientInfo::new(ClientId::new(0)));

        let block_store = Arc::clone(&self.block_store);
        let stream_manager = Arc::clone(&self.stream_manager);
        let group_id = self.default_group_id;

        while let Some(req_result) = stream.next().await {
            let req = req_result.map_err(|e| Status::internal(format!("Stream error: {}", e)))?;

            if req.worker_epoch != 0 && req.worker_epoch != self.worker_epoch {
                resp_header = crate::data_header::DataResponseHeader::need_refresh(
                    common::header::ClientInfo::new(ClientId::new(0)),
                    RefreshReason::WorkerEpochMismatch,
                    RpcErrorCode::WorkerEpochMismatch,
                    "worker_epoch mismatch".to_string(),
                );
                resp_header.worker_epoch = Some(self.worker_epoch);
                stored = false;
                break;
            }

            // Extract stream_id from first request
            let req_stream_id_proto = req
                .stream_id
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("missing stream_id"))?;
            let req_stream_id = types::ids::StreamId::new(
                ((req_stream_id_proto.high as u64 as u128) << 64) | (req_stream_id_proto.low as u64 as u128),
            );

            if stream_id.is_none() {
                stream_id = Some(req_stream_id);
            } else if stream_id.unwrap() != req_stream_id {
                return Err(Status::invalid_argument("Stream ID mismatch"));
            }

            // Get stream state
            let stream_state = stream_manager
                .get(req_stream_id)
                .await
                .ok_or_else(|| Status::not_found("Stream not found or expired"))?;

            // Validate stream is for writing
            if stream_state.mode != StreamMode::Write {
                return Err(Status::failed_precondition("Stream is not a write stream"));
            }

            let block_id = stream_state.block_id;
            let cursor = stream_state.cursor;

            // Extract chunk data
            let chunk_data = req.data.ok_or_else(|| Status::invalid_argument("missing data"))?;
            let chunk_slice = chunk_data
                .slice
                .ok_or_else(|| Status::invalid_argument("missing slice"))?;
            let data = chunk_data.data;

            // Write chunk to block_store
            let chunk_id_proto = chunk_slice
                .chunk
                .ok_or_else(|| Status::invalid_argument("missing chunk in slice"))?;
            let block_id_proto = chunk_id_proto
                .block
                .ok_or_else(|| Status::invalid_argument("missing block in chunk_id"))?;
            let chunk_idx = types::ids::ChunkIndex::new(chunk_id_proto.chunk_index);

            // Validate block_id matches
            let expected_block_id = types::ids::BlockId::new(
                types::ids::DataHandleId::new(block_id_proto.data_handle_id),
                types::ids::BlockIndex::new(block_id_proto.block_index),
            );
            if expected_block_id != block_id {
                return Err(Status::invalid_argument("Block ID mismatch"));
            }

            // Write chunk
            let chunk_ref = types::chunk::ChunkRef::new(block_id, chunk_idx.as_raw());
            let mut data_stream = tokio::io::BufReader::new(std::io::Cursor::new(data.clone()));

            if let Err(e) = block_store
                .write_chunk_stream(group_id, chunk_ref, &mut data_stream)
                .await
            {
                return Err(Status::internal(format!("Failed to write chunk: {}", e)));
            }

            // Update cursor and acknowledged offset
            let new_cursor = cursor + data.len() as u64;
            stream_manager.update_cursor(req_stream_id, new_cursor).await;
            stream_manager.update_persisted(req_stream_id, new_cursor).await;
            acknowledged_offset = new_cursor;
        }

        Ok(Response::new(proto::worker::WriteStreamResponseProto {
            header: Some(resp_header.to_proto()),
            stored,
            acknowledged_offset,
        }))
    }

    async fn commit_write(
        &self,
        request: Request<proto::worker::CommitWriteRequestProto>,
    ) -> Result<Response<proto::worker::CommitWriteResponseProto>, Status> {
        let req = request.into_inner();
        let data_header = Self::extract_data_request_header(&req.header);
        let client = data_header.client.clone();
        let mut resp_header = crate::data_header::DataResponseHeader::ok(client.clone());

        if req.worker_epoch != 0 && req.worker_epoch != self.worker_epoch {
            resp_header = crate::data_header::DataResponseHeader::need_refresh(
                client.clone(),
                RefreshReason::WorkerEpochMismatch,
                RpcErrorCode::WorkerEpochMismatch,
                "worker_epoch mismatch".to_string(),
            );
            resp_header.worker_epoch = Some(self.worker_epoch);
            return Ok(Response::new(proto::worker::CommitWriteResponseProto {
                header: Some(resp_header.to_proto()),
                committed_length: 0,
                current_block_stamp: 0,
            }));
        }

        let block_id_proto = req
            .block_id
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing block_id"))?;
        let block_id = types::ids::BlockId::new(
            types::ids::DataHandleId::new(block_id_proto.data_handle_id),
            types::ids::BlockIndex::new(block_id_proto.block_index),
        );

        // Find active write stream for this block
        let stream_state = self.stream_manager.find_by_block(block_id).await;
        if let Some(state) = stream_state {
            // Fencing validation
            if let Some(token) = req.token.as_ref() {
                let owner_ok = state.fencing_owner.map(|o| o.as_raw()) == Some(token.owner);
                let epoch_ok = state.lease_epoch == Some(token.epoch);
                if !owner_ok || !epoch_ok {
                    resp_header = crate::data_header::DataResponseHeader::need_refresh(
                        client.clone(),
                        RefreshReason::Fencing,
                        RpcErrorCode::Fencing,
                        "fencing mismatch".to_string(),
                    );
                    resp_header.worker_epoch = Some(self.worker_epoch);
                    return Ok(Response::new(proto::worker::CommitWriteResponseProto {
                        header: Some(resp_header.to_proto()),
                        committed_length: 0,
                        current_block_stamp: 0,
                    }));
                }
            }

            let target = req.committed_length;
            let ok = self.stream_manager.wait_persisted(state.stream_id, target, 1_000).await;
            if !ok {
                resp_header = crate::data_header::DataResponseHeader::retryable(
                    client.clone(),
                    RpcErrorCode::StaleState,
                    Some(100),
                    "commit_wait timeout".to_string(),
                );
                return Ok(Response::new(proto::worker::CommitWriteResponseProto {
                    header: Some(resp_header.to_proto()),
                    committed_length: state.last_persisted,
                    current_block_stamp: state.block_stamp,
                }));
            }

            let committed_length = state.last_persisted.max(target);
            let current_block_stamp = state.block_stamp;
            self.stream_manager
                .update_persisted(state.stream_id, committed_length)
                .await;

            Ok(Response::new(proto::worker::CommitWriteResponseProto {
                header: Some(resp_header.to_proto()),
                committed_length,
                current_block_stamp,
            }))
        } else {
            resp_header = crate::data_header::DataResponseHeader::need_refresh(
                client,
                RefreshReason::StaleState,
                RpcErrorCode::StaleState,
                "no active write stream".to_string(),
            );
            Ok(Response::new(proto::worker::CommitWriteResponseProto {
                header: Some(resp_header.to_proto()),
                committed_length: 0,
                current_block_stamp: 0,
            }))
        }
    }
}
