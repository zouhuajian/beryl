// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! OpenDAL-based UFS implementation.

use async_trait::async_trait;
use bytes::Bytes;
use opendal::{services, Operator};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tracing::{debug, info_span, warn, Instrument};

use crate::capability::Capability;
use crate::error::UfsError;
use crate::spec::{BackendConfig, BackendKind, UfsId, UfsSpec};
use crate::traits::{UfsData, UfsDirEntry, UfsFileStatus, UfsMeta};

// Import from common
use common::observe::error::ErrorKind;
use common::{header::RequestHeader, retry_async, timeout_at, ConcurrencyLimiter, RetryPolicy};

mod ufs_metrics {
    pub const OPS_TOTAL: &str = "ufs_ops_total";
    pub const OP_LATENCY_MS: &str = "ufs_op_latency_ms";
    pub const BYTES_TOTAL: &str = "ufs_bytes_total";
}

fn classify_ufs_error_from_message(error_msg: &str) -> ErrorKind {
    let msg_lower = error_msg.to_lowercase();
    if msg_lower.contains("not found") {
        ErrorKind::NotFound
    } else if msg_lower.contains("permission denied") {
        ErrorKind::PermissionDenied
    } else if msg_lower.contains("unsupported") || msg_lower.contains("not supported") {
        ErrorKind::Unsupported
    } else if msg_lower.contains("not implemented") {
        ErrorKind::NotImplemented
    } else if msg_lower.contains("invalid") && (msg_lower.contains("path") || msg_lower.contains("range")) {
        ErrorKind::InvalidArgument
    } else if msg_lower.contains("backend error") {
        ErrorKind::Io
    } else {
        ErrorKind::Unknown
    }
}

/// OpenDAL-based UFS implementation.
pub struct OpendalUfs {
    /// UFS instance identifier.
    pub id: UfsId,
    /// OpenDAL operator.
    op: Operator,
    /// Capability flags.
    caps: Capability,
    /// Backend kind for metrics.
    backend_kind: BackendKind,
    /// Concurrency limiter for backpressure control.
    limiter: Arc<ConcurrencyLimiter>,
    /// Retry policy for idempotent operations.
    retry_policy: RetryPolicy,
}

impl OpendalUfs {
    /// Creates a new OpendalUfs from a specification.
    pub fn from_spec(spec: &UfsSpec) -> Result<Self, UfsError> {
        Self::from_spec_with_limiter(spec, 100) // Default max_inflight = 100
    }

    /// Creates a new OpendalUfs from a specification with custom concurrency limit.
    pub fn from_spec_with_limiter(spec: &UfsSpec, max_inflight: usize) -> Result<Self, UfsError> {
        let op = Self::build_operator(spec)?;
        let caps = Self::determine_capability(spec);

        Ok(Self {
            id: spec.id.clone(),
            op,
            caps,
            backend_kind: spec.kind.clone(),
            limiter: Arc::new(ConcurrencyLimiter::new(max_inflight)),
            retry_policy: RetryPolicy::default_idempotent(),
        })
    }

    /// Builds an OpenDAL operator from a UFS specification.
    fn build_operator(spec: &UfsSpec) -> Result<Operator, UfsError> {
        match (&spec.kind, &spec.config) {
            (BackendKind::S3, BackendConfig::S3(config)) => {
                let builder = services::S3::default()
                    .endpoint(&config.endpoint)
                    .bucket(&config.bucket);
                let builder = if let Some(root) = &config.root {
                    builder.root(root)
                } else {
                    builder
                };
                let builder = if let Some(access_key_id) = &config.access_key_id {
                    builder.access_key_id(access_key_id)
                } else {
                    builder
                };
                let builder = if let Some(secret_access_key) = &config.secret_access_key {
                    builder.secret_access_key(secret_access_key)
                } else {
                    builder
                };
                let builder = if let Some(region) = &config.region {
                    builder.region(region)
                } else {
                    builder
                };
                Ok(Operator::new(builder)?
                    .layer(opendal::layers::LoggingLayer::default())
                    .finish())
            }
            (BackendKind::Oss, BackendConfig::Oss(config)) => {
                let builder = services::Oss::default()
                    .endpoint(&config.endpoint)
                    .bucket(&config.bucket);
                let builder = if let Some(root) = &config.root {
                    builder.root(root)
                } else {
                    builder
                };
                let builder = if let Some(access_key_id) = &config.access_key_id {
                    builder.access_key_id(access_key_id)
                } else {
                    builder
                };
                let builder = if let Some(access_key_secret) = &config.access_key_secret {
                    builder.access_key_secret(access_key_secret)
                } else {
                    builder
                };
                Ok(Operator::new(builder)?
                    .layer(opendal::layers::LoggingLayer::default())
                    .finish())
            }
            #[cfg(feature = "ufs-jvm")]
            (BackendKind::Hdfs, BackendConfig::Hdfs(config)) => {
                let builder = services::Hdfs::default().name_node(&config.namenode);
                let builder = if let Some(root) = &config.root {
                    builder.root(root)
                } else {
                    builder
                };
                // Note: webhdfs_url is not directly supported in opendal 0.54
                // If needed, it can be configured via name_node or other means
                Ok(Operator::new(builder)?
                    .layer(opendal::layers::LoggingLayer::default())
                    .finish())
            }
            #[cfg(not(feature = "ufs-jvm"))]
            (BackendKind::Hdfs, BackendConfig::Hdfs(_)) => Err(UfsError::InvalidSpec(
                "HDFS backend requires the `ufs-jvm` feature to be enabled.".to_string(),
            )),
            (BackendKind::Fs, BackendConfig::Fs(config)) => {
                let builder = services::Fs::default().root(&config.root);
                Ok(Operator::new(builder)?
                    .layer(opendal::layers::LoggingLayer::default())
                    .finish())
            }
            _ => Err(UfsError::InvalidSpec(format!(
                "backend kind {:?} does not match config type",
                spec.kind
            ))),
        }
    }

    /// Determines capability flags based on backend kind and overrides.
    fn determine_capability(spec: &UfsSpec) -> Capability {
        let base = match spec.kind {
            BackendKind::Fs => Capability::for_filesystem(),
            BackendKind::Hdfs => Capability::for_hdfs(),
            BackendKind::S3 | BackendKind::Oss => Capability::for_object_storage(),
        };

        if let Some(ref overrides) = spec.capability_overrides {
            base.with_overrides(overrides)
        } else {
            base
        }
    }

    /// Get backend name for metrics.
    fn backend_name(&self) -> &'static str {
        match self.backend_kind {
            BackendKind::S3 => "s3",
            BackendKind::Oss => "oss",
            BackendKind::Hdfs => "hdfs",
            BackendKind::Fs => "fs",
        }
    }

    /// Helper to execute an operation with timeout, retry, and concurrency limiting.
    async fn execute_op<F, Fut, T>(
        &self,
        op: &'static str,
        ctx: &RequestHeader,
        is_idempotent: bool,
        mut f: F,
    ) -> Result<T, UfsError>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, UfsError>>,
    {
        // Acquire permit from limiter
        let _permit = self
            .limiter
            .acquire(ctx)
            .await
            .map_err(|e| UfsError::Internal(format!("Failed to acquire permit: {}", e.message)))?;

        // Use retry for idempotent operations
        if is_idempotent {
            // Wrap f in Mutex to allow multiple calls from the retry closure
            let f_mutex = Arc::new(Mutex::new(f));
            let deadline = ctx.deadline;

            retry_async(&self.retry_policy, ctx, op, || {
                let f_mutex = Arc::clone(&f_mutex);
                async move {
                    let mut f_guard = f_mutex.lock().await;
                    match timeout_at(deadline, f_guard()).await {
                        Ok(Ok(result)) => Ok(result),
                        Ok(Err(ufs_err)) => Err(ufs_err.into()), // Convert UfsError to CommonError
                        Err(common_err) => Err(common_err),      // timeout_at already returns CommonError
                    }
                }
            })
            .await
            .map_err(|e| e.into()) // Convert CommonError back to UfsError
        } else {
            // Non-idempotent: just execute with timeout
            timeout_at(ctx.deadline, f())
                .await
                .map_err(|e| UfsError::Internal(format!("Operation timeout: {}", e.message)))?
        }
    }

    /// Helper to instrument an operation with metrics and tracing.
    async fn instrument_op<F, Fut, T>(&self, op: &'static str, f: F) -> Result<T, UfsError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, UfsError>>,
    {
        let backend = self.backend_name();
        let start = Instant::now();

        let span = info_span!(
            "ufs.op",
            backend = backend,
            op = op,
            ufs_id = %self.id,
        );

        {
            let span_inner = span.clone();
            async move {
                let result = f().await;
                let latency_ms = start.elapsed();

                let (status, error_kind) = match &result {
                    Ok(_) => ("ok", ErrorKind::Ok),
                    Err(e) => {
                        let kind = classify_ufs_error_from_message(&e.to_string());
                        (kind.as_str(), kind)
                    }
                };

                // Record metrics
                metrics::counter!(ufs_metrics::OPS_TOTAL, "backend" => backend, "op" => op, "status" => status, "error_kind" => error_kind.as_str()).increment(1);
                metrics::histogram!(ufs_metrics::OP_LATENCY_MS, "backend" => backend, "op" => op).record(latency_ms.as_secs_f64() * 1000.0);

                span_inner.record("status", status);
                span_inner.record("latency_ms", latency_ms.as_millis() as u64);

                result
            }
        }
        .instrument(span)
        .await
    }
}

#[async_trait]
impl UfsMeta for OpendalUfs {
    async fn stat(&self, path: &str, ctx: &RequestHeader) -> Result<UfsFileStatus, UfsError> {
        // Use execute_op for timeout/retry/limiting
        let meta = self
            .execute_op("stat", ctx, true, || async {
                debug!(ufs_id = %self.id, path = path, "stat");
                self.op.stat(path).await.map_err(|e| e.into())
            })
            .await?;
        Ok(UfsFileStatus {
            is_dir: meta.mode().is_dir(),
            size: if meta.mode().is_file() {
                Some(meta.content_length())
            } else {
                None
            },
            modified: meta.last_modified().and_then(|t| {
                let system_time: SystemTime = t.into();
                system_time
                    .duration_since(UNIX_EPOCH)
                    .ok()
                    .map(|d| d.as_millis() as i64)
            }),
            etag: meta.etag().map(|s| s.to_string()),
        })
    }

    async fn list(&self, prefix: &str, ctx: &RequestHeader) -> Result<Vec<UfsDirEntry>, UfsError> {
        // Use execute_op for timeout/retry/limiting
        let opendal_entries = self
            .execute_op("list", ctx, true, || async {
                debug!(ufs_id = %self.id, prefix = prefix, "list");
                self.op.list_with(prefix).await.map_err(|e| e.into())
            })
            .await?;
        let mut entries = Vec::new();

        for entry in opendal_entries {
            entries.push(UfsDirEntry {
                path: entry.path().to_string(),
                is_dir: entry.metadata().mode().is_dir(),
                size: if entry.metadata().mode().is_file() {
                    Some(entry.metadata().content_length())
                } else {
                    None
                },
            });
        }

        Ok(entries)
    }

    async fn rename(&self, from: &str, to: &str, ctx: &RequestHeader) -> Result<(), UfsError> {
        // Use execute_op for timeout/limiting (rename is NOT idempotent, so no retry)
        self.execute_op("rename", ctx, false, || async {
            debug!(ufs_id = %self.id, from = from, to = to, "rename");

            // Try native rename first if supported
            if self.caps.supports_rename {
                match self.op.rename(from, to).await {
                    Ok(()) => return Ok(()),
                    Err(e) if e.kind() == opendal::ErrorKind::Unsupported => {
                        warn!(ufs_id = %self.id, "native rename not supported, falling back");
                    }
                    Err(e) => return Err(e.into()),
                }
            }

            // Fallback: copy + delete (non-atomic)
            if self.caps.rename_fallback_enabled {
                warn!(
                    ufs_id = %self.id,
                    from = from,
                    to = to,
                    "using rename fallback (copy + delete), operation is NOT atomic"
                );

                // Copy the file
                let data = self.op.read(from).await?;
                self.op.write(to, data).await?;

                // Delete the original
                self.op.delete(from).await?;

                Ok(())
            } else {
                Err(UfsError::Unsupported(format!(
                    "rename not supported and fallback is disabled for ufs {}",
                    self.id
                )))
            }
        })
        .await?;

        Ok(())
    }

    async fn delete(&self, path: &str, recursive: bool, ctx: &RequestHeader) -> Result<(), UfsError> {
        self.instrument_op("delete", || async {
            debug!(ufs_id = %self.id, path = path, recursive = recursive, "delete");

            if recursive && !self.caps.supports_recursive_delete {
                // Manual recursive delete: list and delete each entry
                warn!(
                    ufs_id = %self.id,
                    path = path,
                    "recursive delete not supported, using manual traversal"
                );

                let entries = self.list(path, ctx).await?;
                for entry in entries {
                    self.delete(&entry.path, true, ctx).await?;
                }
                // Delete the directory itself
                self.op.delete(path).await?;
                Ok(())
            } else {
                self.op.delete(path).await?;
                Ok(())
            }
        })
        .await
    }

    async fn mkdirs(&self, path: &str, ctx: &RequestHeader) -> Result<(), UfsError> {
        // Use execute_op for timeout/retry/limiting (mkdirs is idempotent)
        self.execute_op("mkdirs", ctx, true, || async {
            debug!(ufs_id = %self.id, path = path, "mkdirs");

            if !self.caps.supports_dir {
                // For object storage, directories are implicit, so this is a no-op
                debug!(ufs_id = %self.id, "mkdirs is no-op for object storage backend");
                return Ok(());
            }

            self.op.create_dir(path).await?;
            Ok(())
        })
        .await
    }

    async fn exists(&self, path: &str, ctx: &RequestHeader) -> Result<bool, UfsError> {
        // Use execute_op for timeout/retry/limiting (exists is idempotent)
        self.execute_op("exists", ctx, true, || async {
            debug!(ufs_id = %self.id, path = path, "exists");

            match self.op.stat(path).await {
                Ok(_) => Ok(true),
                Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(false),
                Err(e) => Err(e.into()),
            }
        })
        .await
    }
}

#[async_trait]
impl UfsData for OpendalUfs {
    async fn read_range(&self, path: &str, offset: u64, len: usize, ctx: &RequestHeader) -> Result<Bytes, UfsError> {
        let backend = self.backend_name();
        let op = "read_range";

        let span = info_span!(
            "ufs.op",
            backend = backend,
            op = op,
            ufs_id = %self.id,
        );
        let span_clone = span.clone();

        // Use execute_op for timeout/retry/limiting (read is idempotent)
        // But we need custom metrics for bytes_read, so we wrap it
        let result = self
            .execute_op("read_range", ctx, true, || async {
                debug!(
                    ufs_id = %self.id,
                    path = path,
                    offset = offset,
                    len = len,
                    "read_range"
                );

                let range = offset..(offset + len as u64);
                self.op.read_with(path).range(range).await.map_err(UfsError::from)
            })
            .await?;

        let bytes_read = result.len();

        // Record additional metrics for bytes (execute_op already records latency via instrument_op)
        metrics::counter!(ufs_metrics::BYTES_TOTAL, "backend" => backend, "direction" => "read")
            .increment(bytes_read as u64);
        span_clone.record("bytes", bytes_read);

        // Convert opendal::Buffer to Bytes via Vec<u8>
        Ok(Bytes::from(result.to_vec()))
    }

    async fn read_all(&self, path: &str, ctx: &RequestHeader) -> Result<Bytes, UfsError> {
        // Use execute_op for timeout/retry/limiting (read is idempotent)
        let result = self
            .execute_op("read_all", ctx, true, || async {
                debug!(ufs_id = %self.id, path = path, "read_all");
                self.op.read(path).await.map_err(|e| e.into())
            })
            .await?;
        // Convert opendal::Buffer to Bytes via Vec<u8>
        Ok(Bytes::from(result.to_vec()))
    }

    async fn write_all(&self, path: &str, data: Bytes, ctx: &RequestHeader) -> Result<(), UfsError> {
        let bytes_to_write = data.len();

        // Use execute_op for timeout/limiting (write is NOT idempotent, so no retry)
        self.execute_op("write_all", ctx, false, || async {
            debug!(ufs_id = %self.id, path = path, len = bytes_to_write, "write_all");
            // Convert Bytes to Vec<u8> to satisfy 'static lifetime requirement
            self.op.write(path, data.to_vec()).await.map_err(|e| e.into())
        })
        .await?;

        Ok(())
    }
}

// OpendalUfs implements UfsAccess via blanket implementation in traits.rs
