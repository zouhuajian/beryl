// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Concurrency limiter for backpressure control.

use crate::error::{CommonError, CommonErrorCode};
use crate::header::RequestHeader;
use crate::time::timeout_at;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{debug, warn};

/// Permit from ConcurrencyLimiter (released on drop).
#[derive(Debug)]
pub struct Permit {
    _permit: tokio::sync::OwnedSemaphorePermit,
}

/// Concurrency limiter using tokio semaphore.
///
/// Provides backpressure control by limiting the number of concurrent operations.
pub struct ConcurrencyLimiter {
    semaphore: Arc<Semaphore>,
    max: usize,
}

impl ConcurrencyLimiter {
    /// Create a new limiter with the given maximum concurrency.
    pub fn new(max: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max)),
            max,
        }
    }

    /// Try to acquire a permit without waiting.
    ///
    /// Returns None if no permit is available immediately.
    pub fn try_acquire(&self) -> Option<Permit> {
        match self.semaphore.clone().try_acquire_owned() {
            Ok(permit) => Some(Permit { _permit: permit }),
            Err(_) => None,
        }
    }

    /// Acquire a permit, waiting if necessary.
    ///
    /// Respects the deadline from the CallerContext. If the deadline passes
    /// while waiting, returns a Timeout error.
    pub async fn acquire(&self, ctx: &RequestHeader) -> Result<Permit, CommonError> {
        let start = std::time::Instant::now();
        let remaining = ctx.deadline.remaining();

        if remaining.is_zero() {
            return Err(CommonError::new(
                CommonErrorCode::Timeout,
                "deadline has passed, cannot acquire permit",
            ));
        }

        // Use timeout_at to respect the deadline
        // Clone the semaphore Arc to get an owned permit
        match timeout_at(ctx.deadline, self.semaphore.clone().acquire_owned()).await {
            Ok(Ok(permit)) => {
                let wait_ms = start.elapsed().as_millis();
                if wait_ms > 0 {
                    debug!(
                        wait_ms,
                        available = self.semaphore.available_permits(),
                        max = self.max,
                        "acquired permit after waiting"
                    );
                }
                Ok(Permit { _permit: permit })
            }
            Ok(Err(_)) => {
                warn!("semaphore closed");
                Err(CommonError::new(CommonErrorCode::Internal, "semaphore closed"))
            }
            Err(_) => {
                let wait_ms = start.elapsed().as_millis();
                warn!(wait_ms, "timeout waiting for permit");
                Err(CommonError::new(
                    CommonErrorCode::Timeout,
                    format!("timeout waiting for permit after {}ms", wait_ms),
                ))
            }
        }
    }

    /// Get the current number of available permits.
    pub fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// Get the maximum number of permits.
    pub fn max_permits(&self) -> usize {
        self.max
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CommonErrorCode;
    use crate::header::RequestHeader;
    use crate::time::Deadline;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::sleep;
    use types::ClientId;

    #[tokio::test]
    async fn test_try_acquire_success() {
        let limiter = ConcurrencyLimiter::new(2);

        let permit1 = limiter.try_acquire();
        assert!(permit1.is_some());

        let permit2 = limiter.try_acquire();
        assert!(permit2.is_some());

        let permit3 = limiter.try_acquire();
        assert!(permit3.is_none()); // Should fail, max is 2
    }

    #[tokio::test]
    async fn test_acquire_with_deadline() {
        let limiter = ConcurrencyLimiter::new(1);
        let ctx = RequestHeader::new(ClientId::new(1));

        // Acquire the only permit
        let _permit1 = limiter.acquire(&ctx).await.unwrap();

        // Try to acquire another (should wait)
        let ctx2 = RequestHeader::with_deadline(ClientId::new(2), Deadline::from_now(Duration::from_millis(50)));

        let result = limiter.acquire(&ctx2).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code, CommonErrorCode::Timeout);
    }

    #[tokio::test]
    async fn test_acquire_release() {
        let limiter = ConcurrencyLimiter::new(1);
        let ctx = RequestHeader::new(ClientId::new(1));

        {
            let _permit = limiter.acquire(&ctx).await.unwrap();
            assert_eq!(limiter.available_permits(), 0);
        } // Permit dropped here

        // Should be able to acquire again
        let _permit2 = limiter.acquire(&ctx).await.unwrap();
        assert_eq!(limiter.available_permits(), 0);
    }

    #[tokio::test]
    async fn test_concurrent_acquire() {
        let limiter = Arc::new(ConcurrencyLimiter::new(2));
        let ctx = RequestHeader::new(ClientId::new(1));

        let mut handles = Vec::new();
        for _i in 0..5 {
            let limiter = limiter.clone();
            let ctx = ctx.child();
            handles.push(tokio::spawn(async move {
                let permit = limiter.acquire(&ctx).await;
                sleep(Duration::from_millis(10)).await;
                // Permit dropped here
                permit.is_ok()
            }));
        }

        let results: Vec<bool> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // All should succeed (even if they had to wait)
        assert!(results.iter().all(|&r| r));
    }
}
