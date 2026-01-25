// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Connection pool for managing transport connections.

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing::{debug, warn};

use crate::connection::{Connection, ConnectionConfig, ConnectionMetadata};
use crate::error::{TransportError, TransportResult};

/// Connection pool that manages reusable connections.
///
/// Features:
/// - Connection reuse
/// - Health checking
/// - Automatic cleanup of idle connections
/// - Backpressure via semaphore
pub struct ConnectionPool<C: Connection> {
    /// Pooled connections by address
    connections: Arc<RwLock<HashMap<String, PooledConnection<C>>>>,
    /// Semaphore for backpressure control
    semaphore: Arc<Semaphore>,
    /// Maximum idle time before closing connection
    max_idle_time: Duration,
}

struct PooledConnection<C: Connection> {
    connection: Arc<C>,
    metadata: ConnectionMetadata,
}

impl<C: Connection> ConnectionPool<C> {
    pub fn new(config: ConnectionConfig) -> Self {
        let max_concurrent = config.max_concurrent_requests;
        // Use keepalive interval to derive idle timeout to avoid holding stale connections.
        let max_idle_time = if config.keep_alive {
            config.keep_alive_interval * 4
        } else {
            Duration::from_secs(300)
        };
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
            max_idle_time,
        }
    }

    /// Get or create a connection to the given address.
    pub async fn get_connection(
        &self,
        addr: &str,
        factory: impl FnOnce() -> std::pin::Pin<Box<dyn std::future::Future<Output = TransportResult<C>> + Send>>,
    ) -> TransportResult<Arc<C>> {
        // Check if connection exists and is healthy
        {
            let mut conns = self.connections.write();
            if let Some(pooled) = conns.get_mut(addr) {
                if pooled.connection.is_healthy().await {
                    pooled.metadata.mark_used();
                    return Ok(Arc::clone(&pooled.connection));
                } else {
                    // Remove unhealthy connection
                    debug!("Removing unhealthy connection to {}", addr);
                    conns.remove(addr);
                }
            }
        }

        // Acquire semaphore for backpressure
        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|_| TransportError::Backpressure("semaphore closed".to_string()))?;

        // Create new connection
        let connection = factory().await?;
        let metadata = ConnectionMetadata::new(addr.to_string());
        let pooled = PooledConnection {
            connection: Arc::new(connection),
            metadata,
        };

        let conn = Arc::clone(&pooled.connection);
        {
            let mut conns = self.connections.write();
            conns.insert(addr.to_string(), pooled);
        }

        Ok(conn)
    }

    /// Remove a connection from the pool.
    pub fn remove_connection(&self, addr: &str) {
        let mut conns = self.connections.write();
        conns.remove(addr);
    }

    /// Clean up idle connections.
    pub async fn cleanup_idle(&self) {
        let mut conns = self.connections.write();
        let mut to_remove = Vec::new();

        for (addr, pooled) in conns.iter() {
            if pooled.metadata.idle_time() > self.max_idle_time {
                to_remove.push(addr.clone());
            }
        }

        for addr in to_remove {
            debug!("Removing idle connection to {}", addr);
            if let Some(pooled) = conns.remove(&addr) {
                // Try to close gracefully
                if let Ok(conn) = Arc::try_unwrap(pooled.connection) {
                    let mut conn = conn;
                    if let Err(e) = conn.close().await {
                        warn!("Error closing idle connection: {}", e);
                    }
                }
            }
        }
    }

    /// Get pool statistics.
    pub fn stats(&self) -> PoolStats {
        let conns = self.connections.read();
        PoolStats {
            total_connections: conns.len(),
            available_permits: self.semaphore.available_permits(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PoolStats {
    pub total_connections: usize,
    pub available_permits: usize,
}
