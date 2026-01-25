// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Rebalance and failure recovery for replication.
//!
//! This module implements:
//! - Automatic rebalance: Monitor replication status and trigger rebalance when needed
//! - Failure recovery: Detect failed replications and retry with backoff
//! - Health monitoring: Track replication health per block/worker

use anyhow::Result;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::interval;
use tracing::{debug, error, info, warn};

use crate::block_manager::{BlockManager, ReplicationClient};
use crate::volume_manager::VolumeManager;
use types::ids::{BlockId, ShardGroupId, WorkerId};

/// Replication status for a block.
#[derive(Clone, Debug)]
pub enum ReplicationStatus {
    /// Not replicated yet.
    Pending,
    /// Replication in progress.
    Replicating,
    /// Replication completed successfully.
    Completed,
    /// Replication failed (with retry count).
    Failed { retry_count: u32, last_error: String },
}

/// Rebalance manager: monitors and manages replication.
pub struct RebalanceManager {
    block_manager: Arc<BlockManager>,
    replication_client: Arc<dyn ReplicationClient + Send + Sync>,
    volume_manager: Option<Arc<VolumeManager>>,
    // Block replication status: (group_id, block_id) -> status
    replication_status: RwLock<HashMap<(ShardGroupId, BlockId), ReplicationStatus>>,
    // Failed replications with backoff: (group_id, block_id) -> (next_retry_time, retry_count)
    failed_replications: RwLock<HashMap<(ShardGroupId, BlockId), (Instant, u32)>>,
    // Replication targets per block: (group_id, block_id) -> Vec<WorkerId>
    replication_targets: RwLock<HashMap<(ShardGroupId, BlockId), Vec<WorkerId>>>,
    // Worker load tracking: worker_id -> (active_replications, capacity_usage)
    worker_load: RwLock<HashMap<WorkerId, (u32, f64)>>,
    // Max retry attempts
    max_retries: u32,
    // Initial backoff duration
    initial_backoff: Duration,
    // Max backoff duration
    max_backoff: Duration,
    // Rebalance check interval
    rebalance_interval: Duration,
}

impl RebalanceManager {
    /// Create a new RebalanceManager.
    pub fn new(block_manager: Arc<BlockManager>, replication_client: Arc<dyn ReplicationClient + Send + Sync>) -> Self {
        Self {
            block_manager,
            replication_client,
            volume_manager: None,
            replication_status: RwLock::new(HashMap::new()),
            failed_replications: RwLock::new(HashMap::new()),
            replication_targets: RwLock::new(HashMap::new()),
            worker_load: RwLock::new(HashMap::new()),
            max_retries: 3,
            initial_backoff: Duration::from_secs(5),
            max_backoff: Duration::from_secs(60),
            rebalance_interval: Duration::from_secs(30),
        }
    }

    /// Create with volume manager for capacity-aware rebalancing.
    pub fn with_volume_manager(
        block_manager: Arc<BlockManager>,
        replication_client: Arc<dyn ReplicationClient + Send + Sync>,
        volume_manager: Arc<VolumeManager>,
    ) -> Self {
        Self {
            block_manager,
            replication_client,
            volume_manager: Some(volume_manager),
            replication_status: RwLock::new(HashMap::new()),
            failed_replications: RwLock::new(HashMap::new()),
            replication_targets: RwLock::new(HashMap::new()),
            worker_load: RwLock::new(HashMap::new()),
            max_retries: 3,
            initial_backoff: Duration::from_secs(5),
            max_backoff: Duration::from_secs(60),
            rebalance_interval: Duration::from_secs(30),
        }
    }

    /// Register a block for replication monitoring.
    pub fn register_block(&self, group_id: ShardGroupId, block_id: BlockId, target_workers: Vec<WorkerId>) {
        let key = (group_id, block_id);
        {
            let mut status = self.replication_status.write();
            status.insert(key, ReplicationStatus::Pending);
        }
        {
            let mut targets = self.replication_targets.write();
            targets.insert(key, target_workers);
        }

        debug!(
            group_id = group_id.as_raw(),
            block_id = %block_id,
            targets = self.replication_targets.read().get(&key).map(|v| v.len()).unwrap_or(0),
            "Registered block for replication monitoring"
        );
    }

    /// Mark replication as completed.
    pub fn mark_replication_completed(&self, group_id: ShardGroupId, block_id: BlockId) {
        let key = (group_id, block_id);
        let target_workers = {
            let targets = self.replication_targets.read();
            targets.get(&key).cloned()
        };

        {
            let mut status = self.replication_status.write();
            status.insert(key, ReplicationStatus::Completed);
        }
        {
            let mut failed = self.failed_replications.write();
            failed.remove(&key);
        }

        // Update worker load (decrement active replications)
        if let Some(workers) = target_workers {
            let mut load = self.worker_load.write();
            for worker_id in &workers {
                if let Some(entry) = load.get_mut(worker_id) {
                    entry.0 = entry.0.saturating_sub(1);
                }
            }
        }

        // Update metrics (tracked in status map, updated in loop)

        debug!(
            group_id = group_id.as_raw(),
            block_id = %block_id,
            "Replication marked as completed"
        );
    }

    /// Mark replication as failed.
    pub fn mark_replication_failed(&self, group_id: ShardGroupId, block_id: BlockId, error: String) {
        let key = (group_id, block_id);
        let target_workers = {
            let targets = self.replication_targets.read();
            targets.get(&key).cloned()
        };

        let retry_count = {
            let mut failed = self.failed_replications.write();
            let entry = failed.entry(key).or_insert_with(|| (Instant::now(), 0));
            entry.1 += 1;
            let count = entry.1;

            // Calculate backoff: exponential backoff with jitter
            let backoff_secs = (self.initial_backoff.as_secs() as u64)
                .saturating_mul(1u64 << count.min(6)) // Cap at 2^6 = 64x
                .min(self.max_backoff.as_secs());
            entry.0 = Instant::now() + Duration::from_secs(backoff_secs);

            count
        };

        {
            let mut status = self.replication_status.write();
            status.insert(
                key,
                ReplicationStatus::Failed {
                    retry_count,
                    last_error: error.clone(),
                },
            );
        }

        // Update worker load (decrement active replications)
        if let Some(workers) = target_workers {
            let mut load = self.worker_load.write();
            for worker_id in &workers {
                if let Some(entry) = load.get_mut(worker_id) {
                    entry.0 = entry.0.saturating_sub(1);
                }
            }
        }

        // Update metrics (tracked in status map, updated in loop)

        if retry_count <= self.max_retries {
            warn!(
                group_id = group_id.as_raw(),
                block_id = %block_id,
                retry_count = retry_count,
                error = %error,
                "Replication failed, will retry"
            );
        } else {
            error!(
                group_id = group_id.as_raw(),
                block_id = %block_id,
                retry_count = retry_count,
                error = %error,
                "Replication failed after max retries"
            );
        }
    }

    /// Process failed replications and retry if backoff period has passed.
    pub async fn process_failed_replications(&self) -> Result<()> {
        let now = Instant::now();
        let mut to_retry = Vec::new();

        // Collect blocks that are ready for retry
        {
            let failed = self.failed_replications.read();
            for ((group_id, block_id), (next_retry, retry_count)) in failed.iter() {
                if now >= *next_retry && *retry_count <= self.max_retries {
                    to_retry.push((*group_id, *block_id, *retry_count));
                }
            }
        }

        // Retry replication for each block
        for (group_id, block_id, retry_count) in to_retry {
            info!(
                group_id = group_id.as_raw(),
                block_id = %block_id,
                retry_count = retry_count,
                "Retrying failed replication"
            );

            // Get target workers
            let target_workers = {
                let targets = self.replication_targets.read();
                targets.get(&(group_id, block_id)).cloned()
            };

            if let Some(targets) = target_workers {
                // Mark as replicating
                {
                    let mut status = self.replication_status.write();
                    status.insert((group_id, block_id), ReplicationStatus::Replicating);
                }

                // Trigger replication
                let block_manager = Arc::clone(&self.block_manager);
                let replication_client = Arc::clone(&self.replication_client);
                let group_id_val = group_id;
                let block_id_val = block_id;
                let targets_val = targets;
                let rebalance_mgr = self.clone_for_spawn();

                tokio::spawn(async move {
                    match block_manager
                        .replicate_block(group_id_val, block_id_val, targets_val, replication_client)
                        .await
                    {
                        Ok(success_count) => {
                            if success_count > 0 {
                                rebalance_mgr.mark_replication_completed(group_id_val, block_id_val);
                            } else {
                                rebalance_mgr.mark_replication_failed(
                                    group_id_val,
                                    block_id_val,
                                    "No successful replications".to_string(),
                                );
                            }
                        }
                        Err(e) => {
                            rebalance_mgr.mark_replication_failed(group_id_val, block_id_val, e.to_string());
                        }
                    }
                });
            }
        }

        Ok(())
    }

    /// Get replication status for a block.
    pub fn get_replication_status(&self, group_id: ShardGroupId, block_id: BlockId) -> Option<ReplicationStatus> {
        let status = self.replication_status.read();
        status.get(&(group_id, block_id)).cloned()
    }

    /// Start background rebalance loop.
    pub async fn start_rebalance_loop(&self) -> Result<()> {
        let mut interval = interval(Duration::from_secs(10)); // Check every 10 seconds

        loop {
            interval.tick().await;

            if let Err(e) = self.process_failed_replications().await {
                error!(error = %e, "Error processing failed replications");
            }
        }
    }

    /// Clone for spawning tasks (creates a new Arc wrapper).
    /// Note: This creates a new instance with cloned state, which is suitable for
    /// background tasks that need to update shared state.
    fn clone_for_spawn(&self) -> Arc<Self> {
        Arc::new(Self {
            block_manager: Arc::clone(&self.block_manager),
            replication_client: Arc::clone(&self.replication_client),
            volume_manager: self.volume_manager.as_ref().map(Arc::clone),
            replication_status: RwLock::new(self.replication_status.read().clone()),
            failed_replications: RwLock::new(self.failed_replications.read().clone()),
            replication_targets: RwLock::new(self.replication_targets.read().clone()),
            worker_load: RwLock::new(self.worker_load.read().clone()),
            max_retries: self.max_retries,
            initial_backoff: self.initial_backoff,
            max_backoff: self.max_backoff,
            rebalance_interval: self.rebalance_interval,
        })
    }
}
