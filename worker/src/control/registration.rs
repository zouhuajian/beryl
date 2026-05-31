// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker metadata registration state.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use types::{GroupName, WorkerId, WorkerRunId};

/// Metadata-confirmed worker registration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Registration {
    pub group_name: GroupName,
    pub worker_id: WorkerId,
    pub worker_run_id: WorkerRunId,
    pub advertised_endpoint: String,
}

/// Worker-local readiness set for metadata group registration.
#[derive(Debug, Default)]
pub struct RegistrationSet {
    registrations: RwLock<HashMap<GroupName, RegistrationLease>>,
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
            registration.group_name.clone(),
            RegistrationLease {
                registration,
                heartbeat_deadline: None,
            },
        );
    }

    pub fn record_heartbeat_success(&self, group_name: &GroupName, lease_duration: Duration) {
        if let Some(entry) = self
            .registrations
            .write()
            .expect("registration state poisoned")
            .get_mut(group_name)
        {
            entry.heartbeat_deadline = Some(Instant::now() + lease_duration);
        }
    }

    pub fn mark_not_ready(&self, group_name: &GroupName) {
        if let Some(entry) = self
            .registrations
            .write()
            .expect("registration state poisoned")
            .get_mut(group_name)
        {
            entry.heartbeat_deadline = None;
        }
    }

    pub fn mark_needs_register(&self, group_name: &GroupName) {
        self.registrations
            .write()
            .expect("registration state poisoned")
            .remove(group_name);
    }

    pub fn registration(&self, group_name: &GroupName) -> Option<Registration> {
        self.registrations
            .read()
            .expect("registration state poisoned")
            .get(group_name)
            .map(|entry| entry.registration.clone())
    }

    pub fn is_registered(&self, group_name: &GroupName) -> bool {
        self.registration(group_name).is_some()
    }

    pub fn is_ready(&self, group_name: &GroupName) -> bool {
        self.registrations
            .read()
            .expect("registration state poisoned")
            .get(group_name)
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

    pub fn registration_for_group(&self, group_name: &GroupName) -> Option<Registration> {
        self.registration(group_name)
    }
}
