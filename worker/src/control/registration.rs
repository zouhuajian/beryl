// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker metadata registration state.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use types::ids::{ShardGroupId, WorkerId};
use types::WorkerRunId;

/// Metadata-confirmed worker registration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Registration {
    pub group_id: ShardGroupId,
    pub worker_id: WorkerId,
    pub worker_run_id: WorkerRunId,
    pub advertised_endpoint: String,
}

/// Worker-local readiness set for metadata group registration.
#[derive(Debug, Default)]
pub struct RegistrationSet {
    registrations: RwLock<HashMap<ShardGroupId, RegistrationLease>>,
}

#[derive(Clone, Debug)]
struct RegistrationLease {
    registration: Registration,
    heartbeat_deadline: Option<Instant>,
}

impl RegistrationSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_registered(&self, registration: Registration) {
        self.registrations.write().expect("registration state poisoned").insert(
            registration.group_id,
            RegistrationLease {
                registration,
                heartbeat_deadline: None,
            },
        );
    }

    pub fn record_heartbeat_success(&self, group_id: ShardGroupId, lease_duration: Duration) {
        if let Some(entry) = self
            .registrations
            .write()
            .expect("registration state poisoned")
            .get_mut(&group_id)
        {
            entry.heartbeat_deadline = Some(Instant::now() + lease_duration);
        }
    }

    pub fn mark_not_ready(&self, group_id: ShardGroupId) {
        if let Some(entry) = self
            .registrations
            .write()
            .expect("registration state poisoned")
            .get_mut(&group_id)
        {
            entry.heartbeat_deadline = None;
        }
    }

    pub fn mark_needs_register(&self, group_id: ShardGroupId) {
        self.registrations
            .write()
            .expect("registration state poisoned")
            .remove(&group_id);
    }

    pub fn registration(&self, group_id: ShardGroupId) -> Option<Registration> {
        self.registrations
            .read()
            .expect("registration state poisoned")
            .get(&group_id)
            .map(|entry| entry.registration.clone())
    }

    pub fn is_registered(&self, group_id: ShardGroupId) -> bool {
        self.registration(group_id).is_some()
    }

    pub fn is_ready(&self, group_id: ShardGroupId) -> bool {
        self.registrations
            .read()
            .expect("registration state poisoned")
            .get(&group_id)
            .and_then(|entry| entry.heartbeat_deadline)
            .map(|deadline| deadline > Instant::now())
            .unwrap_or(false)
    }

    pub fn is_any_ready(&self) -> bool {
        self.registrations
            .read()
            .expect("registration state poisoned")
            .values()
            .any(|entry| {
                entry
                    .heartbeat_deadline
                    .is_some_and(|deadline| deadline > Instant::now())
            })
    }

    #[cfg(test)]
    pub(crate) fn expire_heartbeat_for_test(&self, group_id: ShardGroupId) {
        if let Some(entry) = self
            .registrations
            .write()
            .expect("registration state poisoned")
            .get_mut(&group_id)
        {
            entry.heartbeat_deadline = Some(Instant::now() - Duration::from_millis(1));
        }
    }
}
