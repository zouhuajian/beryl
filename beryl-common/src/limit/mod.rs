// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Concurrency limiter for backpressure control.

use crate::error::{CommonError, CommonErrorKind};
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
                CommonErrorKind::Timeout,
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
                Err(CommonError::new(CommonErrorKind::Internal, "semaphore closed"))
            }
            Err(_) => {
                let wait_ms = start.elapsed().as_millis();
                warn!(wait_ms, "timeout waiting for permit");
                Err(CommonError::new(
                    CommonErrorKind::Timeout,
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
