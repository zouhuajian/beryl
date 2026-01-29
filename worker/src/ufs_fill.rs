// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! UFS filler: Handles read-through cache misses with streaming read and fill-back.
//!
//! This module implements zero-copy streaming read-through from UFS:
//! 1. On miss, read from UFS in chunks (at least chunk_size segments)
//! 2. Stream data to client immediately (with backpressure control)
//! 3. Simultaneously fill back to BlockStore (sync or async, configurable)
//! 4. All operations are streamed, no large Vec aggregation

use anyhow::Result;
use bytes::Bytes;
use std::sync::Arc;
use std::time::Instant;
use tracing::{error, info_span, Instrument};

use crate::block_store::BlockStore;
use common::audit::AuditLogger;
use common::header::RequestHeader;
use common::limit::ConcurrencyLimiter;
use common::observe::metrics::ufs as ufs_metrics;
use common::retry::{retry_async, RetryPolicy};
use types::chunk::{ChunkRef, ChunkSlice};
use types::ids::{ChunkIndex, ShardGroupId};
use types::layout::FileLayout;
use ufs::{UfsError, UfsId, UfsRegistry};

/// UFS filler with concurrency control and backpressure.
pub struct UfsFiller {
    /// UFS registry for managing multiple UFS instances.
    ufs_registry: Arc<UfsRegistry>,
    /// Block store for fill-back.
    block_store: Arc<BlockStore>,
    /// Audit logger.
    audit_logger: Arc<AuditLogger>,
    /// File layout.
    layout: FileLayout,
    /// Concurrency limiter per UFS instance (max concurrent reads).
    /// Key: UfsId, Value: ConcurrencyLimiter
    ufs_limiters: Arc<parking_lot::RwLock<std::collections::HashMap<UfsId, Arc<ConcurrencyLimiter>>>>,
    /// Retry policy for UFS operations.
    retry_policy: RetryPolicy,
    /// Default UFS instance ID (from config).
    default_ufs_id: Option<UfsId>,
    /// UFS read timeout per chunk.
    ufs_timeout_ms: u64,
    /// Whether to use async fill-back (true) or sync fill-back (false).
    async_fill: bool,
}

impl UfsFiller {
    /// Create a new UFS filler.
    pub fn new(
        ufs_registry: Arc<UfsRegistry>,
        block_store: Arc<BlockStore>,
        audit_logger: Arc<AuditLogger>,
        layout: FileLayout,
        default_ufs_id: Option<UfsId>,
        _max_concurrent_per_ufs: usize, // Used in init_limiters()
        ufs_timeout_ms: u64,
        async_fill: bool,
    ) -> Self {
        Self {
            ufs_registry,
            block_store,
            audit_logger,
            layout,
            ufs_limiters: Arc::new(parking_lot::RwLock::new(std::collections::HashMap::new())),
            retry_policy: RetryPolicy::default_idempotent(),
            default_ufs_id,
            ufs_timeout_ms,
            async_fill,
        }
    }

    /// Initialize concurrency limiters for all UFS instances in registry.
    pub fn init_limiters(&self, max_concurrent_per_ufs: usize) {
        let mut limiters = self.ufs_limiters.write();
        let ufs_ids = self.ufs_registry.list_ids();
        for ufs_id in ufs_ids {
            if !limiters.contains_key(&ufs_id) {
                limiters.insert(
                    ufs_id.clone(),
                    Arc::new(ConcurrencyLimiter::new(max_concurrent_per_ufs)),
                );
            }
        }
    }

    /// Get the UFS registry (for write operations).
    pub fn ufs_registry(&self) -> &Arc<UfsRegistry> {
        &self.ufs_registry
    }

    /// Get the default UFS ID.
    pub fn default_ufs_id(&self) -> Option<&UfsId> {
        self.default_ufs_id.as_ref()
    }

    /// Select UFS instance for a read operation.
    /// For now, use default_ufs_id or first available.
    fn select_ufs(&self) -> Option<UfsId> {
        if let Some(ref default_id) = self.default_ufs_id {
            if self.ufs_registry.get(default_id).is_some() {
                return Some(default_id.clone());
            }
        }
        // Fallback to first available
        self.ufs_registry.list_ids().first().cloned()
    }

    /// Read a chunk slice from UFS with streaming and fill-back.
    ///
    /// Returns:
    /// - Ok(Some(Bytes)): Data read from UFS, also filled back to BlockStore
    /// - Ok(None): UFS returned no data (should not happen for valid chunks)
    /// - Err: UFS error (mapped to retryable/non-retryable)
    pub async fn read_chunk_slice_stream(
        &self,
        group_id: ShardGroupId,
        chunk_slice: ChunkSlice,
        caller_ctx: &RequestHeader,
    ) -> Result<Option<Bytes>, UfsError> {
        let start = Instant::now();
        let ufs_id = self
            .select_ufs()
            .ok_or_else(|| UfsError::Internal("No UFS instance available".to_string()))?;

        let span = info_span!(
            "ufs_fill.read_chunk_slice",
            group_id = group_id.as_raw(),
            chunk = %chunk_slice.chunk,
            ufs_id = %ufs_id,
        );

        // Clone values needed in async block
        let ufs_registry = Arc::clone(&self.ufs_registry);
        let block_store = Arc::clone(&self.block_store);
        let audit_logger = Arc::clone(&self.audit_logger);
        let layout = self.layout;
        let async_fill = self.async_fill;
        let ufs_limiters = Arc::clone(&self.ufs_limiters);
        let retry_policy = self.retry_policy.clone();
        let caller_ctx_clone = caller_ctx.clone();

        async move {
            // Acquire concurrency limiter permit for this UFS instance (backpressure control)
            let limiter = {
                let limiters = ufs_limiters.read();
                limiters.get(&ufs_id).cloned()
            };

            // Acquire permit (respects deadline from CallerContext)
            let _permit = if let Some(limiter_arc) = &limiter {
                match limiter_arc.acquire(&caller_ctx_clone).await {
                    Ok(permit) => Some(permit),
                    Err(e) => {
                        return Err(UfsError::from(e));
                    }
                }
            } else {
                None
            };

            // Get UFS access (UfsAccess = UfsMeta + UfsData)
            let ufs_access = ufs_registry
                .get(&ufs_id)
                .ok_or_else(|| UfsError::Internal(format!("UFS instance {} not found", ufs_id)))?;

            // Calculate path and offset for UFS read
            let data_handle_id = chunk_slice.chunk.block_id.data_handle_id;
            let block_id = chunk_slice.chunk.block_id;
            let chunk_idx = ChunkIndex::new(chunk_slice.chunk.chunk_idx);

            // Path format: use data_handle_id as path (UFS stores files by data_handle_id)
            let path = format!("{}", data_handle_id.as_raw());
            let block_start = layout.block_start_offset(block_id.index);
            let chunk_start_in_block = layout.chunk_start_offset_in_block(chunk_idx) as u64;
            let chunk_offset = block_start + chunk_start_in_block;
            let len = layout.chunk_size as usize;

            // Read from UFS with retry (idempotent operation)
            let path_clone = path.clone();
            let ufs_access_clone = ufs_access.clone();
            let data = match retry_async(&retry_policy, &caller_ctx_clone, "ufs.read_range", || {
                let path = path_clone.clone();
                let ufs_access = ufs_access_clone.clone();
                let caller_ctx = caller_ctx_clone.child();
                async move {
                    ufs_access
                        .read_range(&path, chunk_offset, len, &caller_ctx)
                        .await
                        .map_err(|e| e.into()) // Convert UfsError to CommonError
                }
            })
            .await
            {
                Ok(data) => data,
                Err(e) => {
                    // Record error metrics
                    let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
                    let error_kind = match &e.code {
                        common::CommonErrorCode::NotFound => "not_found",
                        common::CommonErrorCode::PermissionDenied => "permission_denied",
                        common::CommonErrorCode::Timeout => "timeout",
                        common::CommonErrorCode::Unavailable
                        | common::CommonErrorCode::Throttled
                        | common::CommonErrorCode::Overloaded => "retryable",
                        _ => "internal",
                    };

                    metrics::counter!(
                        ufs_metrics::OPS_TOTAL,
                        "op" => "read_range",
                        "status" => "error",
                        "error_kind" => error_kind
                    )
                    .increment(1);

                    metrics::histogram!(
                        ufs_metrics::OP_LATENCY_MS,
                        "op" => "read_range"
                    )
                    .record(latency_ms);

                    return Err(e.into()); // Convert CommonError back to UfsError
                }
            };

            // Record metrics
            let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
            let bytes_transferred = data.len() as u64;

            // Record UFS operation metrics
            metrics::counter!(
                ufs_metrics::OPS_TOTAL,
                "op" => "read_range",
                "status" => "ok"
            )
            .increment(1);

            metrics::histogram!(
                ufs_metrics::OP_LATENCY_MS,
                "op" => "read_range"
            )
            .record(latency_ms);

            metrics::counter!(
                ufs_metrics::BYTES_TOTAL,
                "op" => "read_range"
            )
            .increment(bytes_transferred);

            // Fill back to BlockStore (streaming write)
            let data_clone = data.clone();
            let chunk_ref = chunk_slice.chunk.clone();

            if async_fill {
                // Async fill: spawn task, don't wait
                let block_store_clone = Arc::clone(&block_store);
                tokio::spawn(async move {
                    if let Err(e) = Self::fill_back_internal(block_store_clone, group_id, chunk_ref, data_clone).await {
                        error!(error = %e, "Async fill-back failed");
                    }
                });
            } else {
                // Sync fill: wait for completion
                Self::fill_back_internal(block_store.clone(), group_id, chunk_ref, data_clone)
                    .await
                    .map_err(|e| UfsError::Internal(format!("Fill-back failed: {}", e)))?;
            }

            // Audit log with path and block info
            let block_id = chunk_slice.chunk.block_id;
            let path = format!("/file/{}", block_id.data_handle_id.as_raw());
            audit_logger.log(common::audit::AuditRecord {
                timestamp: chrono::Utc::now().to_rfc3339(),
                request_id: caller_ctx.client.call_id.to_string(),
                client_id: caller_ctx.client.client_id.as_raw(),
                operation: "UfsRead".to_string(),
                path: Some(path),
                block_id: Some(format!(
                    "{}:{}",
                    block_id.data_handle_id.as_raw(),
                    block_id.index.as_raw()
                )),
                chunk_ref: Some(format!(
                    "{}:{}:{}",
                    block_id.data_handle_id.as_raw(),
                    block_id.index.as_raw(),
                    chunk_slice.chunk.chunk_idx
                )),
                source: format!("UfsRead:{}", ufs_id),
                result: "Success".to_string(),
                bytes: data.len() as u64,
                latency_ms,
            });

            // Return only the requested slice to the caller.
            let start = chunk_slice.offset_in_chunk as usize;
            let end = (start + chunk_slice.len as usize).min(data.len());
            Ok(Some(data.slice(start..end)))
        }
        .instrument(span)
        .await
    }

    /// Fill back data to BlockStore (streaming write).
    async fn fill_back_stream(&self, group_id: ShardGroupId, chunk_ref: ChunkRef, data: Bytes) -> Result<(), UfsError> {
        Self::fill_back_internal(Arc::clone(&self.block_store), group_id, chunk_ref, data)
            .await
            .map_err(|e| UfsError::Internal(format!("Fill-back failed: {}", e)))
    }

    /// Internal fill-back implementation (streaming write to BlockStore).
    async fn fill_back_internal(
        block_store: Arc<BlockStore>,
        group_id: ShardGroupId,
        chunk_ref: ChunkRef,
        data: Bytes,
    ) -> Result<()> {
        // Convert Bytes to AsyncRead stream
        use std::io::Cursor;
        let cursor = Cursor::new(data);
        let mut reader = tokio::io::BufReader::new(cursor);

        // Write using BlockStore's streaming write
        block_store
            .write_chunk_stream(group_id, chunk_ref, &mut reader)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to write chunk to BlockStore: {}", e))?;

        Ok(())
    }

    /// Map UFS errors to retryable/non-retryable with retry_after_ms if applicable.
    fn map_ufs_error_static(err: UfsError) -> UfsError {
        match &err {
            UfsError::NotFound(_) => err,         // Non-retryable
            UfsError::PermissionDenied(_) => err, // Non-retryable
            UfsError::Overloaded(msg) => {
                // Retryable with backoff
                UfsError::Overloaded(format!("{} (retry_after_ms: 1000)", msg))
            }
            UfsError::Backend(_opendal_err) => {
                // Backend errors are already mapped through UfsError
                // The ufs crate handles the conversion from opendal::Error to UfsError
                // We just need to add retry hints for rate limiting
                let err_msg = _opendal_err.to_string();
                if err_msg.contains("rate limit") || err_msg.contains("RateLimited") {
                    UfsError::Overloaded(format!("Rate limited: {} (retry_after_ms: 2000)", err_msg))
                } else {
                    // Keep as Backend error, it will be handled by the caller
                    err
                }
            }
            _ => err,
        }
    }
}
