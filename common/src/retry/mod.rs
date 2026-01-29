// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Retry policy and async retry utilities.

use crate::error::{CommonError, CommonErrorCode};
use crate::header::RequestHeader;
use std::future::Future;
use std::time::Duration;
use tracing::{debug, info_span, warn};

/// Retry policy configuration.
#[derive(Clone, Debug)]
pub struct RetryPolicy {
    /// Maximum number of retries (0 = disabled).
    pub max_retries: u32,
    /// Maximum elapsed time for all retries (optional).
    pub max_elapsed: Option<Duration>,
    /// Only retry idempotent operations.
    pub idempotent_only: bool,
    /// Base backoff duration.
    pub base_backoff: Duration,
    /// Maximum backoff duration.
    pub max_backoff: Duration,
    /// Function to determine if an error is retryable.
    pub retry_on: fn(&CommonError) -> bool,
}

impl RetryPolicy {
    /// Create a disabled retry policy.
    pub fn disabled() -> Self {
        Self {
            max_retries: 0,
            max_elapsed: None,
            idempotent_only: false,
            base_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
            retry_on: |_| false,
        }
    }

    /// Create a default retry policy for idempotent operations.
    pub fn default_idempotent() -> Self {
        Self {
            max_retries: 3,
            max_elapsed: None,
            idempotent_only: true,
            base_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
            retry_on: |e| e.is_retryable(),
        }
    }

    /// Create a default retry policy for all operations.
    pub fn default() -> Self {
        Self {
            max_retries: 3,
            max_elapsed: None,
            idempotent_only: false,
            base_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
            retry_on: |e| e.is_retryable(),
        }
    }

    /// Check if retries are enabled.
    pub fn is_enabled(&self) -> bool {
        self.max_retries > 0
    }

    /// Calculate backoff duration for the given attempt (0-indexed).
    pub fn backoff_for_attempt(&self, attempt: u32) -> Duration {
        if attempt == 0 {
            return Duration::ZERO;
        }

        // Exponential backoff with jitter
        let base_ms = self.base_backoff.as_millis() as u64;
        let exp_ms = base_ms * (1u64 << attempt.min(10));
        let backoff_ms = exp_ms.min(self.max_backoff.as_millis() as u64);

        // Add jitter: ±20%
        let jitter_range = backoff_ms / 5;
        let jitter = fastrand::u64(0..=jitter_range * 2);
        let jittered_ms = backoff_ms.saturating_sub(jitter_range).saturating_add(jitter);

        Duration::from_millis(jittered_ms.min(self.max_backoff.as_millis() as u64))
    }
}

/// Retry an async operation according to the policy.
///
/// The operation is retried if:
/// - The policy is enabled
/// - The error is retryable according to `retry_on`
/// - The context deadline hasn't passed
/// - The maximum retries haven't been exceeded
pub async fn retry_async<F, Fut, T>(
    policy: &RetryPolicy,
    ctx: &RequestHeader,
    op_name: &str,
    mut f: F,
) -> Result<T, CommonError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, CommonError>>,
{
    if !policy.is_enabled() {
        return f().await;
    }

    let start = std::time::Instant::now();
    let mut last_error = None;
    let mut attempt = 0u32;

    let span = info_span!("retry", op = op_name);
    let _guard = span.enter();

    loop {
        // Check deadline before attempting
        let remaining = ctx.deadline.remaining();
        if remaining.is_zero() {
            let err =
                last_error.unwrap_or_else(|| CommonError::new(CommonErrorCode::Timeout, "deadline exceeded before retry"));
            warn!(
                attempt,
                elapsed_ms = start.elapsed().as_millis(),
                "retry stopped due to deadline"
            );
            return Err(err);
        }

        // Check max elapsed time
        if let Some(max_elapsed) = policy.max_elapsed {
            if start.elapsed() > max_elapsed {
                let err =
                    last_error.unwrap_or_else(|| CommonError::new(CommonErrorCode::Timeout, "max elapsed time exceeded"));
                warn!(
                    attempt,
                    elapsed_ms = start.elapsed().as_millis(),
                    "retry stopped due to max elapsed time"
                );
                return Err(err);
            }
        }

        // Execute the operation
        match f().await {
            Ok(result) => {
                if attempt > 0 {
                    debug!(
                        attempt,
                        elapsed_ms = start.elapsed().as_millis(),
                        "operation succeeded after retries"
                    );
                }
                return Ok(result);
            }
            Err(e) => {
                last_error = Some(e.clone());

                // Check if error is retryable
                if !(policy.retry_on)(&e) {
                    debug!(
                        attempt,
                        error_code = ?e.code,
                        "error is not retryable, stopping"
                    );
                    return Err(e);
                }

                attempt += 1;
                if attempt > policy.max_retries {
                    warn!(
                        attempt,
                        max_retries = policy.max_retries,
                        elapsed_ms = start.elapsed().as_millis(),
                        error_code = ?e.code,
                        "max retries exceeded"
                    );
                    return Err(e);
                }

                // Calculate backoff, but respect deadline
                let backoff = policy.backoff_for_attempt(attempt);
                let effective_backoff = backoff.min(remaining);

                if effective_backoff.is_zero() {
                    warn!(attempt, "no time remaining for backoff, stopping");
                    return Err(e);
                }

                debug!(
                    attempt,
                    backoff_ms = effective_backoff.as_millis(),
                    error_code = ?e.code,
                    "retrying after backoff"
                );

                tokio::time::sleep(effective_backoff).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::CommonErrorCode;
    use crate::header::RequestHeader;
    use crate::time::Deadline;
    use tokio::time::sleep;
    use types::ClientId;

    #[tokio::test]
    async fn test_retry_success_on_first_attempt() {
        let policy = RetryPolicy::default();
        let ctx = RequestHeader::new(ClientId::new(1));

        let mut attempts = 0;
        let result = retry_async(&policy, &ctx, "test", || {
            attempts += 1;
            async move { Ok::<i32, CommonError>(42) }
        })
        .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempts, 1);
    }

    #[tokio::test]
    async fn test_retry_succeeds_after_retries() {
        let policy = RetryPolicy {
            max_retries: 3,
            base_backoff: Duration::from_millis(10),
            ..RetryPolicy::default()
        };
        let ctx = RequestHeader::new(ClientId::new(1));

        let mut attempts = 0;
        let result = retry_async(&policy, &ctx, "test", || {
            attempts += 1;
            async move {
                if attempts < 3 {
                    Err(CommonError::new(CommonErrorCode::Unavailable, "temporary error"))
                } else {
                    Ok::<i32, CommonError>(42)
                }
            }
        })
        .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempts, 3);
    }

    #[tokio::test]
    async fn test_retry_stops_on_non_retryable_error() {
        let policy = RetryPolicy::default();
        let ctx = RequestHeader::new(ClientId::new(1));

        let mut attempts = 0;
        let result: Result<i32, CommonError> = retry_async(&policy, &ctx, "test", || {
            attempts += 1;
            async move { Err(CommonError::new(CommonErrorCode::InvalidArgument, "not retryable")) }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(attempts, 1); // Should not retry
    }

    #[tokio::test]
    async fn test_retry_respects_deadline() {
        let policy = RetryPolicy {
            max_retries: 10,
            base_backoff: Duration::from_millis(50),
            ..RetryPolicy::default()
        };
        // Short deadline
        let ctx = RequestHeader::with_deadline(ClientId::new(1), Deadline::from_now(Duration::from_millis(100)));

        let mut attempts = 0;
        let result: Result<i32, CommonError> = retry_async(&policy, &ctx, "test", || {
            attempts += 1;
            async move {
                sleep(Duration::from_millis(20)).await;
                Err(CommonError::new(CommonErrorCode::Unavailable, "temporary error"))
            }
        })
        .await;

        assert!(result.is_err());
        // Should stop before max_retries due to deadline
        assert!(attempts < 10);
    }

    #[tokio::test]
    async fn test_retry_disabled() {
        let policy = RetryPolicy::disabled();
        let ctx = RequestHeader::new(ClientId::new(1));

        let mut attempts = 0;
        let result: Result<i32, CommonError> = retry_async(&policy, &ctx, "test", || {
            attempts += 1;
            async move { Err(CommonError::new(CommonErrorCode::Unavailable, "error")) }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(attempts, 1); // Should not retry
    }
}
