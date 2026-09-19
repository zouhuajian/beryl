// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Standard asynchronous streams and bounded owned range reads.

use crate::api::FileStatus;
use crate::client_inner::{metric_labels, refresh_hint_from_error, ClientInner};
use crate::error::{ClientError, ClientResult};
use crate::metadata::ReadLayout;
use crate::metrics::ClientMetric;
use crate::planner;
use crate::runtime::{retry_decision, AttemptContext, Operation, OperationContext, OperationDeadline, RetryDecision};
use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, RecoveryAction, WorkerErrorKind};
use bytes::{Buf, Bytes};
use futures::future::BoxFuture;
use futures::io::{AsyncRead, AsyncSeek};
use parking_lot::Mutex;
use std::fmt;
use std::io::{self, SeekFrom};
use std::ops::{Bound, Range, RangeBounds};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

/// A seekable asynchronous reader bound to the inode, generation, and length at open.
///
/// Use futures IO's `AsyncReadExt` and `AsyncSeekExt` for stream operations. The
/// current transport still requires a Tokio runtime. Each underlying bounded
/// read has one deadline, including retries. Callers control the total deadline and destination size of operations such as `read_to_end`
/// and `copy`. Cancelling a stream wait retains pending IO and undelivered data;
/// seeking or dropping the reader cancels that IO.
///
/// Cached locations need not detect changes or deletion immediately. A fixed
/// generation does not retain historical file contents.
///
/// ```no_run
/// use beryl_client::FsClient;
/// use futures::io::{AsyncReadExt, AsyncSeekExt};
/// use std::io::SeekFrom;
/// use tokio_util::compat::FuturesAsyncReadCompatExt;
///
/// # async fn example(client: &FsClient) -> Result<(), Box<dyn std::error::Error>> {
/// let mut reader = client.open("/file").await?;
/// let tail = reader.read_range(reader.len().saturating_sub(16)..).await?;
/// let mut contents = Vec::new();
/// reader.read_to_end(&mut contents).await?;
/// reader.seek(SeekFrom::Start(0)).await?;
/// let mut compatible = reader.compat();
/// tokio::io::copy(&mut compatible, &mut tokio::io::sink()).await?;
/// # Ok(())
/// # }
/// ```
pub struct FileReader {
    source: Arc<ReadSource>,
    position: u64,
    buffered: Bytes,
    // Only polled through &mut self. The mutex keeps FileReader Sync even though
    // its owned future is Send, allowing concurrent read_range calls through &self.
    pending: Mutex<Option<BoxFuture<'static, ClientResult<Bytes>>>>,
}

/// Shared authority and layout cache, independent of the stream's pending future.
struct ReadSource {
    inner: Arc<ClientInner>,
    file: FileStatus,
    layout: Mutex<Option<Arc<ReadLayout>>>,
}

impl FileReader {
    pub(crate) fn new(inner: Arc<ClientInner>, file: FileStatus) -> Self {
        Self {
            source: Arc::new(ReadSource {
                inner,
                file,
                layout: Mutex::new(None),
            }),
            position: 0,
            buffered: Bytes::new(),
            pending: Mutex::new(None),
        }
    }

    /// Returns the immutable file status captured at open.
    pub fn status(&self) -> &FileStatus {
        &self.source.file
    }

    /// Returns the path used to open this inode; renames do not change it.
    pub fn path(&self) -> &str {
        self.status().path().expect("opened file path")
    }

    /// Returns the length captured at open.
    pub fn len(&self) -> u64 {
        self.status().len()
    }

    /// Returns whether the opened file has zero length.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the stream position, advanced only by delivered bytes or a seek.
    pub fn position(&self) -> u64 {
        self.position
    }

    /// Reads a complete file-relative range without changing the stream position.
    ///
    /// Unbounded endpoints use zero and the opened length. Reversed ranges and
    /// boundary overflow are invalid arguments; endpoints beyond EOF return
    /// UnexpectedEof. Valid empty ranges perform no IO. The configured range
    /// limit is checked before allocation or IO. All chunks and retries share
    /// one deadline. Shared references can read ranges concurrently.
    pub async fn read_range(&self, range: impl RangeBounds<u64>) -> ClientResult<Bytes> {
        let range = resolve_range(range, self.len())?;
        let len = range.end - range.start;
        if len == 0 {
            return Ok(Bytes::new());
        }
        if len > self.source.inner.config.read_range_limit() {
            return Err(ClientError::invalid_argument(format!(
                "range length {len} exceeds configured read_range maximum {}",
                self.source.inner.config.read_range_limit()
            )));
        }
        let capacity = usize::try_from(len)
            .map_err(|_| ClientError::invalid_argument("range length exceeds addressable memory"))?;
        let deadline = self.source.inner.metadata.operation_deadline();
        let mut output = Vec::new();
        let mut offset = range.start;
        while offset < range.end {
            let len = (range.end - offset).min(u64::from(self.source.inner.config.max_read_step_bytes())) as u32;
            let bytes = self.source.read(offset, len, deadline.clone()).await?;
            offset += bytes.len() as u64;
            if output.is_empty() {
                if offset == range.end {
                    return Ok(bytes);
                }
                output.try_reserve_exact(capacity).map_err(|error| {
                    ClientError::resource_exhausted(format!("failed to reserve {capacity} range bytes: {error}"))
                })?;
            }
            output.extend_from_slice(&bytes);
        }
        Ok(Bytes::from(output))
    }
}

impl AsyncRead for FileReader {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if buf.is_empty() || this.position >= this.len() {
            return Poll::Ready(Ok(0));
        }
        if this.buffered.is_empty() {
            if this.pending.get_mut().is_none() {
                let source = Arc::clone(&this.source);
                let offset = this.position;
                let len = buf.len().min(source.inner.config.max_read_step_bytes() as usize) as u32;
                let deadline = source.inner.metadata.operation_deadline();
                *this.pending.get_mut() = Some(Box::pin(async move { source.read(offset, len, deadline).await }));
            }
            let result = ready!(this.pending.get_mut().as_mut().expect("pending read").as_mut().poll(cx));
            *this.pending.get_mut() = None;
            this.buffered = result.map_err(io::Error::from)?;
        }
        let len = buf.len().min(this.buffered.len());
        buf[..len].copy_from_slice(&this.buffered[..len]);
        this.buffered.advance(len);
        if this.buffered.is_empty() {
            this.buffered = Bytes::new();
        }
        this.position += len as u64;
        Poll::Ready(Ok(len))
    }
}

impl AsyncSeek for FileReader {
    fn poll_seek(self: Pin<&mut Self>, _: &mut Context<'_>, from: SeekFrom) -> Poll<io::Result<u64>> {
        let this = self.get_mut();
        let position = match from {
            SeekFrom::Start(position) => Some(position),
            SeekFrom::Current(delta) => this.position.checked_add_signed(delta),
            SeekFrom::End(delta) => this.len().checked_add_signed(delta),
        }
        .ok_or_else(|| ClientError::invalid_argument("seek position is negative or overflows"))?;
        // Seeking completes locally. Dropping the future prevents any old result
        // from being delivered or invalidating a later stream position's layout.
        *this.pending.get_mut() = None;
        this.buffered = Bytes::new();
        this.position = position;
        Poll::Ready(Ok(position))
    }
}

impl ReadSource {
    /// Reads at most one block's visible prefix, with the same bounds on cache hits and misses.
    async fn read(&self, offset: u64, len: u32, deadline: OperationDeadline) -> ClientResult<Bytes> {
        let operation = self.read_operation(deadline)?;
        let mut layout = None;
        for attempt_index in 0..self.inner.config.max_attempts() {
            let mut fetched = false;
            if layout.is_none() {
                let cached = self.layout.lock().clone();
                layout = if let Some(cached) = cached.filter(|cached| cached.location_at(offset).is_some()) {
                    Some(cached)
                } else {
                    // The block boundary is only known after lookup. Asking for
                    // this byte avoids depending on unread subsequent blocks.
                    let fresh = self
                        .inner
                        .metadata
                        .read_layout_for_inode(operation.clone(), self.file.inode_id(), offset, 1)
                        .await?;
                    fresh.validate_file(&self.file)?;
                    fetched = true;
                    Some(Arc::new(fresh))
                };
            }
            let current = layout.as_ref().expect("read layout initialized");
            let plan = planner::plan_block_read(&self.file, offset, len, current)?;
            if fetched {
                *self.layout.lock() = Some(Arc::clone(current));
            }
            let ctx = AttemptContext::for_data(&operation);
            match self
                .inner
                .worker_rpc_with_timeout(
                    &operation,
                    self.inner
                        .worker
                        .read_block_range(ctx, current.group_name.clone(), &plan),
                )
                .await
            {
                Ok(bytes) => return Ok(bytes),
                Err(error) => {
                    let decision = self.handle_worker_failure(&operation, attempt_index, &error).await;
                    // Retain the layout only while retrying it. A terminal failure
                    // lets a later read discover a replacement endpoint.
                    if !matches!(decision, Ok(RetryDecision::Retry)) {
                        let mut cached = self.layout.lock();
                        // An old failed request must not evict a concurrent replacement.
                        if cached.as_ref().is_some_and(|cached| Arc::ptr_eq(cached, current)) {
                            *cached = None;
                        }
                    }
                    match decision? {
                        RetryDecision::RefreshMetadata(_) => layout = None,
                        RetryDecision::Retry => {}
                        _ => return Err(error),
                    }
                }
            }
        }
        unreachable!("read attempt loop returns on its final attempt")
    }

    /// Applies bounded read retry policy and returns only an authorized next action.
    async fn handle_worker_failure(
        &self,
        operation: &OperationContext,
        attempt_index: usize,
        error: &ClientError,
    ) -> ClientResult<RetryDecision> {
        let decision = retry_decision(error, operation.retry_safety());
        self.inner.record_error_metric("Read", "worker", error);
        let has_next = attempt_index + 1 < self.inner.config.max_attempts();
        match (decision, has_next) {
            (RetryDecision::RefreshMetadata(reason), true) if should_replan_after_worker_error(error) => {
                self.inner
                    .metadata
                    .record_data_refresh(operation, reason, &refresh_hint_from_error(error))?;
                self.inner.record_metric(
                    ClientMetric::RetryAttempt,
                    metric_labels("Read", "worker").with_error_class(error.classification_label()),
                );
                Ok(decision)
            }
            (RetryDecision::Retry, true) => {
                self.inner.record_metric(
                    ClientMetric::RetryAttempt,
                    metric_labels("Read", "worker").with_error_class(error.classification_label()),
                );
                self.inner.sleep_before_retry(attempt_index, operation).await?;
                Ok(decision)
            }
            (RetryDecision::Retry | RetryDecision::RefreshMetadata(_), false) => {
                self.inner.record_metric(
                    ClientMetric::RetryExhausted,
                    metric_labels("Read", "worker").with_error_class(error.classification_label()),
                );
                Err(error.clone())
            }
            _ => Err(error.clone()),
        }
    }

    /// Creates one stable operation identity for a bounded Worker read step.
    fn read_operation(&self, deadline: OperationDeadline) -> ClientResult<OperationContext> {
        OperationContext::new_named(
            self.inner.metadata.client_id(),
            self.inner.metadata.client_name(),
            Operation::Read,
            self.file.path.clone(),
            deadline,
        )
    }
}

impl fmt::Debug for FileReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileReader")
            .field("path", &self.path())
            .field("len", &self.len())
            .field("position", &self.position())
            .finish()
    }
}

fn resolve_range(range: impl RangeBounds<u64>, len: u64) -> ClientResult<Range<u64>> {
    let start = match range.start_bound() {
        Bound::Unbounded => 0,
        Bound::Included(&start) => start,
        Bound::Excluded(&start) => start
            .checked_add(1)
            .ok_or_else(|| ClientError::invalid_argument("excluded range start overflows"))?,
    };
    let end = match range.end_bound() {
        Bound::Unbounded => len,
        Bound::Excluded(&end) => end,
        Bound::Included(&end) => end
            .checked_add(1)
            .ok_or_else(|| ClientError::invalid_argument("inclusive range end overflows"))?,
    };
    if matches!(range.end_bound(), Bound::Unbounded) && start > len {
        return Err(ClientError::unexpected_eof("range exceeds opened file length"));
    }
    if start > end {
        return Err(ClientError::invalid_argument("range start exceeds end"));
    }
    if start > len || end > len {
        return Err(ClientError::unexpected_eof("range exceeds opened file length"));
    }
    Ok(start..end)
}

fn should_replan_after_worker_error(error: &ClientError) -> bool {
    error.remote_error().is_some_and(|detail| {
        matches!(detail.recovery, RecoveryAction::RefreshMetadata { .. })
            && matches!(
                detail.kind,
                ErrorKind::Metadata(MetadataErrorKind::StaleState | MetadataErrorKind::RouteEpochMismatch)
                    | ErrorKind::Worker(
                        WorkerErrorKind::BlockLocationUnavailable
                            | WorkerErrorKind::RunMismatch
                            | WorkerErrorKind::FullReportRequired
                            | WorkerErrorKind::NotRegistered
                    )
            )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ClientErrorKind;

    #[test]
    fn reader_supports_shared_ranges_and_standard_io() {
        fn assert_traits<T: Send + Sync + Unpin + AsyncRead + AsyncSeek>() {}
        assert_traits::<FileReader>();
    }

    #[test]
    fn range_boundaries_are_exact_and_checked() {
        assert_eq!(resolve_range(.., 10).unwrap(), 0..10);
        assert_eq!(resolve_range(3.., 10).unwrap(), 3..10);
        assert_eq!(resolve_range(..=9, 10).unwrap(), 0..10);
        assert_eq!(
            resolve_range((Bound::Excluded(2), Bound::Included(4)), 10).unwrap(),
            3..5
        );
        assert_eq!(resolve_range(10..10, 10).unwrap(), 10..10);
        assert_eq!(resolve_range(.., 0).unwrap(), 0..0);
        assert_eq!(resolve_range(.., u64::MAX).unwrap(), 0..u64::MAX);
        for range in [
            (Bound::Included(4), Bound::Excluded(3)),
            (Bound::Excluded(u64::MAX), Bound::Unbounded),
            (Bound::Unbounded, Bound::Included(u64::MAX)),
        ] {
            assert_eq!(
                resolve_range(range, 10).unwrap_err().kind(),
                ClientErrorKind::InvalidArgument
            );
        }
        for range in [
            (Bound::Included(11), Bound::Unbounded),
            (Bound::Included(11), Bound::Excluded(11)),
            (Bound::Included(0), Bound::Included(10)),
        ] {
            assert_eq!(
                resolve_range(range, 10).unwrap_err().kind(),
                ClientErrorKind::UnexpectedEof
            );
        }
    }
}
