// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Deadline and timeout utilities.

use crate::error::{CommonError, CommonErrorKind};
use std::future::Future;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::time::timeout as tokio_timeout;

/// Deadline represents an absolute time point for request expiration.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Deadline {
    /// Unix timestamp in milliseconds.
    unix_ms: i64,
}

impl Deadline {
    /// Create a deadline from now plus a duration.
    pub fn from_now(duration: Duration) -> Self {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
        let deadline_ms = (now + duration).as_millis() as i64;
        Self { unix_ms: deadline_ms }
    }

    /// Create a deadline from a Unix timestamp in milliseconds.
    pub fn from_unix_ms(unix_ms: i64) -> Self {
        Self { unix_ms }
    }

    /// Get the deadline as Unix timestamp in milliseconds.
    pub fn as_unix_ms(&self) -> i64 {
        self.unix_ms
    }

    /// Get the remaining duration until the deadline.
    ///
    /// Returns Duration::ZERO if the deadline has passed.
    pub fn remaining(&self) -> Duration {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
        let now_ms = now.as_millis() as i64;
        let remaining_ms = self.unix_ms.saturating_sub(now_ms);
        if remaining_ms > 0 {
            Duration::from_millis(remaining_ms as u64)
        } else {
            Duration::ZERO
        }
    }

    /// Check if the deadline has passed.
    pub fn has_passed(&self) -> bool {
        self.remaining().is_zero()
    }

    /// Convert to tokio Instant (for use with tokio::time::sleep_until).
    pub fn to_tokio_instant(&self) -> Option<Instant> {
        let now = Instant::now();
        let remaining = self.remaining();
        if remaining.is_zero() {
            None
        } else {
            Some(now + remaining)
        }
    }
}

/// Execute a future with a deadline, returning a timeout error if exceeded.
pub async fn timeout_at<F>(deadline: Deadline, future: F) -> Result<F::Output, CommonError>
where
    F: Future,
{
    let remaining = deadline.remaining();
    if remaining.is_zero() {
        return Err(CommonError::new(
            CommonErrorKind::Timeout,
            "deadline has already passed",
        ));
    }

    match tokio_timeout(remaining, future).await {
        Ok(result) => Ok(result),
        Err(_) => Err(CommonError::new(
            CommonErrorKind::Timeout,
            format!("operation timed out after {}ms", remaining.as_millis()),
        )),
    }
}

/// Execute a future with a duration timeout.
pub async fn timeout<F>(duration: Duration, future: F) -> Result<F::Output, CommonError>
where
    F: Future,
{
    let deadline = Deadline::from_now(duration);
    timeout_at(deadline, future).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_timeout_at_success() {
        let deadline = Deadline::from_now(Duration::from_millis(100));
        let result = timeout_at(deadline, async {
            sleep(Duration::from_millis(10)).await;
            42
        })
        .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_timeout_at_timeout() {
        let deadline = Deadline::from_now(Duration::from_millis(10));
        let result = timeout_at(deadline, async {
            sleep(Duration::from_millis(100)).await;
            42
        })
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind, CommonErrorKind::Timeout);
    }

    #[tokio::test]
    async fn test_timeout_at_expired() {
        let deadline = Deadline::from_unix_ms(0); // Past deadline
        let result = timeout_at(deadline, async { 42 }).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind, CommonErrorKind::Timeout);
    }

    #[test]
    fn test_deadline_remaining() {
        let deadline = Deadline::from_now(Duration::from_secs(10));
        let remaining = deadline.remaining();
        assert!(remaining.as_secs() >= 9 && remaining.as_secs() <= 10);
    }

    #[test]
    fn test_deadline_has_passed() {
        let past_deadline = Deadline::from_unix_ms(0);
        assert!(past_deadline.has_passed());

        let future_deadline = Deadline::from_now(Duration::from_secs(10));
        assert!(!future_deadline.has_passed());
    }
}
