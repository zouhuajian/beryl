// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Group role cache (leader/follower tracking).

use dashmap::DashMap;
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::{Duration, Instant};
use types::ids::ShardGroupId;

/// Group role information.
#[derive(Clone, Debug)]
pub struct GroupRole {
    /// Leader node ID.
    pub leader_id: Option<u64>,
    /// Follower node IDs.
    pub follower_ids: Vec<u64>,
    /// Last successful access time.
    pub last_success: Option<Instant>,
    /// Last failure time.
    pub last_failure: Option<Instant>,
    /// Failure count (for health tracking).
    pub failure_count: u32,
}

impl GroupRole {
    /// Check if leader is healthy (recent success).
    pub fn is_leader_healthy(&self, timeout: Duration) -> bool {
        if let Some(last_success) = self.last_success {
            last_success.elapsed() < timeout
        } else {
            false
        }
    }

    /// Check if a follower is healthy.
    pub fn is_follower_healthy(&self, follower_id: u64, _timeout: Duration) -> bool {
        if !self.follower_ids.contains(&follower_id) {
            return false;
        }
        // TODO: Track per-follower health
        true
    }
}

/// Group role cache.
pub struct GroupRoleCache {
    /// Group role map: group_id -> GroupRole.
    roles: Arc<RwLock<DashMap<ShardGroupId, GroupRole>>>,
    /// Health check timeout.
    health_timeout: Duration,
}

impl GroupRoleCache {
    /// Create a new group role cache.
    pub fn new(health_timeout_secs: u64) -> Self {
        Self {
            roles: Arc::new(RwLock::new(DashMap::new())),
            health_timeout: Duration::from_secs(health_timeout_secs),
        }
    }

    /// Get group role.
    pub fn get(&self, group_id: &ShardGroupId) -> Option<GroupRole> {
        let roles = self.roles.read();
        roles.get(group_id).map(|r| r.clone())
    }

    /// Update group role.
    pub fn update(&self, group_id: ShardGroupId, leader_id: Option<u64>, follower_ids: Vec<u64>) {
        let roles = self.roles.write();
        let mut role = roles.entry(group_id).or_insert_with(|| GroupRole {
            leader_id: None,
            follower_ids: vec![],
            last_success: None,
            last_failure: None,
            failure_count: 0,
        });
        role.leader_id = leader_id;
        role.follower_ids = follower_ids;
    }

    /// Record successful access.
    pub fn record_success(&self, group_id: &ShardGroupId) {
        let roles = self.roles.write();
        roles.entry(*group_id).and_modify(|role| {
            role.last_success = Some(Instant::now());
            role.failure_count = 0;
        });
    }

    /// Record failed access.
    pub fn record_failure(&self, group_id: &ShardGroupId) {
        let roles = self.roles.write();
        roles.entry(*group_id).and_modify(|role| {
            role.last_failure = Some(Instant::now());
            role.failure_count += 1;
        });
    }

    /// Get healthy leader for a group.
    pub fn get_healthy_leader(&self, group_id: &ShardGroupId) -> Option<u64> {
        let role = {
            let roles = self.roles.read();
            roles.get(group_id).map(|r| r.clone())
        }?;
        if role.is_leader_healthy(self.health_timeout) {
            role.leader_id
        } else {
            None
        }
    }

    /// Get healthy followers for a group.
    pub fn get_healthy_followers(&self, group_id: &ShardGroupId) -> Vec<u64> {
        let role = {
            let roles = self.roles.read();
            roles.get(group_id).map(|r| r.clone())
        };
        let role = match role {
            Some(r) => r,
            None => return vec![],
        };
        role.follower_ids
            .iter()
            .filter(|&&fid| role.is_follower_healthy(fid, self.health_timeout))
            .copied()
            .collect()
    }
}
