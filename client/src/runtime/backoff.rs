// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Bounded retry backoff.

use std::fmt;
use std::time::Duration;

use async_trait::async_trait;

use crate::config::BackoffConfig;

/// Exponential backoff policy for retryable transient failures.
#[derive(Clone, Debug)]
pub(crate) struct BackoffPolicy {
    initial_backoff_ms: u64,
    max_backoff_ms: u64,
    backoff_multiplier: f64,
}

impl BackoffPolicy {
    /// Build a backoff policy from validated client configuration.
    pub(crate) fn from_config(config: &BackoffConfig) -> Self {
        Self {
            initial_backoff_ms: config.initial_backoff_ms,
            max_backoff_ms: config.max_backoff_ms,
            backoff_multiplier: config.backoff_multiplier,
        }
    }

    /// Delay for retry index `0` is the initial backoff, capped at max.
    pub(crate) fn delay_for_retry(&self, retry_index: usize) -> Duration {
        let mut delay = self.initial_backoff_ms as f64;
        for _ in 0..retry_index {
            delay = (delay * self.backoff_multiplier).min(self.max_backoff_ms as f64);
        }
        if !delay.is_finite() {
            return Duration::from_millis(self.max_backoff_ms);
        }
        Duration::from_millis((delay as u64).min(self.max_backoff_ms))
    }
}

/// Injectable sleeper used to avoid real time sleeps in tests.
#[async_trait]
pub(crate) trait BackoffSleeper: Send + Sync + fmt::Debug {
    /// Sleep for the supplied delay.
    async fn sleep(&self, delay: Duration);
}

/// Tokio-backed production sleeper.
#[derive(Debug)]
pub(crate) struct TokioBackoffSleeper;

#[async_trait]
impl BackoffSleeper for TokioBackoffSleeper {
    async fn sleep(&self, delay: Duration) {
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_first_retry_uses_initial_delay() {
        let policy = BackoffPolicy::from_config(&BackoffConfig {
            initial_backoff_ms: 25,
            max_backoff_ms: 1000,
            backoff_multiplier: 2.0,
        });

        assert_eq!(policy.delay_for_retry(0), Duration::from_millis(25));
    }

    #[test]
    fn backoff_multiplies_and_caps_at_max() {
        let policy = BackoffPolicy::from_config(&BackoffConfig {
            initial_backoff_ms: 100,
            max_backoff_ms: 250,
            backoff_multiplier: 2.0,
        });

        assert_eq!(policy.delay_for_retry(0), Duration::from_millis(100));
        assert_eq!(policy.delay_for_retry(1), Duration::from_millis(200));
        assert_eq!(policy.delay_for_retry(2), Duration::from_millis(250));
        assert_eq!(policy.delay_for_retry(10), Duration::from_millis(250));
    }
}
