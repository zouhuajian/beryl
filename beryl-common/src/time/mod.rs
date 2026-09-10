// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Absolute request deadlines.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
}
