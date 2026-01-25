// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Connection abstraction for transport layer.

use crate::error::TransportResult;
use async_trait::async_trait;
use std::time::Duration;

/// Connection configuration.
#[derive(Clone, Debug)]
pub struct ConnectionConfig {
    /// Connection timeout.
    pub connect_timeout: Duration,
    /// Request timeout.
    pub request_timeout: Duration,
    /// Maximum number of concurrent requests per connection.
    pub max_concurrent_requests: usize,
    /// Enable keep-alive.
    pub keep_alive: bool,
    /// Keep-alive interval.
    pub keep_alive_interval: Duration,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            max_concurrent_requests: 100,
            keep_alive: true,
            keep_alive_interval: Duration::from_secs(30),
        }
    }
}

/// A connection to a remote endpoint.
///
/// Connections are managed by the connection pool and can be reused
/// for multiple requests.
#[async_trait]
pub trait Connection: Send + Sync {
    /// Get the remote address.
    fn remote_addr(&self) -> &str;

    /// Check if the connection is healthy.
    async fn is_healthy(&self) -> bool;

    /// Close the connection.
    async fn close(&mut self) -> TransportResult<()>;
}

/// Connection metadata for pool management.
#[derive(Clone, Debug)]
pub struct ConnectionMetadata {
    pub remote_addr: String,
    pub created_at: std::time::Instant,
    pub last_used: std::time::Instant,
    pub request_count: u64,
}

impl ConnectionMetadata {
    pub fn new(remote_addr: String) -> Self {
        let now = std::time::Instant::now();
        Self {
            remote_addr,
            created_at: now,
            last_used: now,
            request_count: 0,
        }
    }

    pub fn mark_used(&mut self) {
        self.last_used = std::time::Instant::now();
        self.request_count += 1;
    }

    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    pub fn idle_time(&self) -> Duration {
        self.last_used.elapsed()
    }
}
