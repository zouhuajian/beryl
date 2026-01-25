// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Retry policy configuration and implementation.

use crate::error::TransportError;
use std::time::Duration;

/// Retry policy configuration.
#[derive(Clone, Debug)]
pub struct RetryPolicy {
    /// Maximum number of retries (0 = disabled).
    pub max_retries: u32,
    /// Base backoff duration.
    pub base_backoff: Duration,
    /// Maximum backoff duration.
    pub max_backoff: Duration,
    /// Set of gRPC status codes that are retryable.
    pub retryable_codes: std::collections::HashSet<u32>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::disabled()
    }
}

impl RetryPolicy {
    /// Create a disabled retry policy (no retries).
    pub fn disabled() -> Self {
        Self {
            max_retries: 0,
            base_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
            retryable_codes: std::collections::HashSet::new(),
        }
    }

    /// Create a default retry policy with common retryable codes.
    pub fn default_enabled() -> Self {
        Self {
            max_retries: 3,
            base_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
            retryable_codes: [4, 8, 14].iter().copied().collect(), // DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED, UNAVAILABLE
        }
    }

    /// Check if retries are enabled.
    pub fn is_enabled(&self) -> bool {
        self.max_retries > 0
    }

    /// Check if an error is retryable according to this policy.
    pub fn is_retryable(&self, error: &TransportError) -> bool {
        if !self.is_enabled() {
            return false;
        }

        match error {
            TransportError::Unavailable(_)
            | TransportError::DeadlineExceeded(_)
            | TransportError::Timeout(_)
            | TransportError::Connection(_) => true,
            TransportError::RemoteStatus { code, .. } => self.retryable_codes.contains(code),
            _ => false,
        }
    }

    /// Calculate backoff duration for the given attempt (0-indexed).
    pub fn backoff_for_attempt(&self, attempt: u32) -> Duration {
        if attempt == 0 {
            return Duration::from_secs(0);
        }

        let backoff_ms = self.base_backoff.as_millis() as u64 * (1u64 << attempt.min(10));
        let backoff = Duration::from_millis(backoff_ms.min(self.max_backoff.as_millis() as u64));
        backoff.min(self.max_backoff)
    }
}
