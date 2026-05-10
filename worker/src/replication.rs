// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Replication client for sending chunks to remote workers.
//!
//! This module implements GrpcReplicationClient which uses GrpcTransport
//! to replicate chunks to remote workers via gRPC.

use anyhow::{Context, Result};
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::debug;

use common::observe::metrics::replication as replication_metrics;
use transport::{GrpcConnection, GrpcTransport, NetTransport};
use types::ids::{BlockId, ChunkIndex, ShardGroupId, WorkerId};
use types::ClientId;

use crate::block_manager::ReplicationClient;
use crate::config::ReplicationConfig;

/// Cache for worker endpoint resolution.
/// Maps worker_id -> endpoint with TTL.
struct EndpointCache {
    cache: HashMap<WorkerId, (String, Instant)>,
    ttl: Duration,
}

impl EndpointCache {
    fn new(ttl: Duration) -> Self {
        Self {
            cache: HashMap::new(),
            ttl,
        }
    }

    fn get(&self, worker_id: &WorkerId) -> Option<String> {
        self.cache.get(worker_id).and_then(|(endpoint, cached_at)| {
            if cached_at.elapsed() < self.ttl {
                Some(endpoint.clone())
            } else {
                None
            }
        })
    }

    fn insert(&mut self, worker_id: WorkerId, endpoint: String) {
        self.cache.insert(worker_id, (endpoint, Instant::now()));
    }

    fn clear(&mut self) {
        self.cache.clear();
    }
}

/// Resolve worker endpoint from configuration or metadata.
///
/// Priority:
/// 1. Configuration mapping (worker.replication.peer_endpoints)
/// 2. Metadata client query (if available, not implemented in PR2)
fn resolve_worker_endpoint(
    worker_id: WorkerId,
    config: &ReplicationConfig,
    cache: &mut EndpointCache,
) -> Result<String> {
    // Check cache first
    if let Some(endpoint) = cache.get(&worker_id) {
        return Ok(endpoint);
    }

    // Check configuration mapping
    if let Some(endpoint) = config.peer_endpoints.get(&worker_id.as_raw()) {
        cache.insert(worker_id, endpoint.clone());
        return Ok(endpoint.clone());
    }

    // TODO: Query metadata client if available
    // For PR2, we only support configuration-based endpoint resolution

    Err(anyhow::anyhow!(
        "Worker {} endpoint not found in configuration",
        worker_id.as_raw()
    ))
}

/// gRPC-based replication client.
pub struct GrpcReplicationClient {
    transport: Arc<GrpcTransport>,
    config: ReplicationConfig,
    endpoint_cache: RwLock<EndpointCache>,
    // Per-peer connection pool: worker_id -> Vec<Arc<GrpcConnection>>
    // Uses round-robin selection for load balancing
    connections: RwLock<HashMap<WorkerId, Vec<Arc<GrpcConnection>>>>,
    // Round-robin index per worker
    connection_indices: RwLock<HashMap<WorkerId, usize>>,
    // Client ID for replication requests
    replication_client_id: ClientId,
}

impl GrpcReplicationClient {
    /// Create a new GrpcReplicationClient.
    pub fn new(transport: Arc<GrpcTransport>, config: ReplicationConfig) -> Self {
        let cache_ttl = Duration::from_secs(300); // 5 minutes TTL
        Self {
            transport,
            config,
            endpoint_cache: RwLock::new(EndpointCache::new(cache_ttl)),
            connections: RwLock::new(HashMap::new()),
            connection_indices: RwLock::new(HashMap::new()),
            replication_client_id: ClientId::new(0), // Use 0 for internal replication
        }
    }

    /// Get or create a connection to a worker (round-robin selection from pool).
    pub(crate) async fn get_connection(&self, worker_id: WorkerId) -> Result<Arc<GrpcConnection>> {
        // Check existing connections
        {
            let connections = self.connections.read();
            if let Some(pool) = connections.get(&worker_id) {
                if !pool.is_empty() {
                    // Round-robin selection
                    let mut indices = self.connection_indices.write();
                    let index = indices.entry(worker_id).or_insert(0);
                    let selected = *index % pool.len();
                    *index = (*index + 1) % pool.len();
                    return Ok(Arc::clone(&pool[selected]));
                }
            }
        }

        // Need to create new connection(s)
        let endpoint = {
            let mut cache = self.endpoint_cache.write();
            resolve_worker_endpoint(worker_id, &self.config, &mut cache)?
        };

        // Create connections up to pool size
        let pool_size = self.config.peer_connection_pool_size;
        let mut new_connections = Vec::with_capacity(pool_size);

        for _ in 0..pool_size {
            let connection = self.transport.connect(&endpoint).await.context(format!(
                "Failed to connect to worker {} at {}",
                worker_id.as_raw(),
                endpoint
            ))?;
            new_connections.push(Arc::new(connection));
        }

        // Store in connection pool
        {
            let mut connections = self.connections.write();
            connections.insert(worker_id, new_connections.clone());
        }

        // Initialize round-robin index
        {
            let mut indices = self.connection_indices.write();
            indices.insert(worker_id, 0);
        }

        // Return first connection
        Ok(Arc::clone(&new_connections[0]))
    }
}

impl ReplicationClient for GrpcReplicationClient {
    fn send_chunk(
        &self,
        target_worker: WorkerId,
        group_id: ShardGroupId,
        block_id: BlockId,
        chunk_idx: ChunkIndex,
        data: Bytes,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + '_>> {
        Box::pin(async move {
            let start = std::time::Instant::now();
            let data_len = data.len();

            debug!(
                target_worker = target_worker.as_raw(),
                group_id = group_id.as_raw(),
                block_id = %block_id,
                chunk_idx = chunk_idx.as_raw(),
                data_len = data_len,
                "Sending chunk for replication"
            );

            metrics::counter!(
                replication_metrics::CHUNKS_TOTAL,
                "status" => "attempted"
            )
            .increment(1);

            metrics::gauge!(replication_metrics::INFLIGHT_CHUNKS).increment(1.0);

            let result = async {
                let _ = (group_id, block_id, chunk_idx, data);
                Err(anyhow::anyhow!(
                    "replication data transfer must be rewired to WorkerDataService stream v2"
                ))
            }
            .await;

            let latency_ms = start.elapsed().as_millis() as u64;

            match &result {
                Ok(()) => {
                    metrics::counter!(
                        replication_metrics::CHUNKS_TOTAL,
                        "status" => "success"
                    )
                    .increment(1);

                    metrics::counter!(replication_metrics::BYTES_TOTAL).increment(data_len as u64);

                    metrics::histogram!(replication_metrics::CHUNK_LATENCY_MS).record(latency_ms as f64);

                    metrics::gauge!(replication_metrics::INFLIGHT_CHUNKS).decrement(1.0);

                    debug!(
                        target_worker = target_worker.as_raw(),
                        block_id = %block_id,
                        chunk_idx = chunk_idx.as_raw(),
                        latency_ms = latency_ms,
                        "Chunk replicated successfully"
                    );
                }
                Err(e) => {
                    let error_kind = if e.to_string().contains("timeout") {
                        "timeout"
                    } else if e.to_string().contains("connect") {
                        "connection"
                    } else {
                        "error"
                    };

                    metrics::counter!(
                        replication_metrics::CHUNKS_TOTAL,
                        "status" => "failure",
                        "error_kind" => error_kind
                    )
                    .increment(1);

                    metrics::histogram!(replication_metrics::CHUNK_LATENCY_MS).record(latency_ms as f64);

                    metrics::gauge!(replication_metrics::INFLIGHT_CHUNKS).decrement(1.0);

                    debug!(
                        target_worker = target_worker.as_raw(),
                        block_id = %block_id,
                        chunk_idx = chunk_idx.as_raw(),
                        error = %e,
                        "Chunk replication failed"
                    );
                }
            }

            result
        })
    }
}

impl GrpcReplicationClient {
    /// Get the max concurrent chunks per block from config.
    pub fn max_concurrent_chunks_per_block(&self) -> usize {
        self.config.max_concurrent_chunks_per_block
    }
}
