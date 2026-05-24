// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker metadata registration state.

use std::collections::HashMap;
use std::sync::RwLock;

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
    registrations: RwLock<HashMap<ShardGroupId, Registration>>,
}

impl RegistrationSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_registered(&self, registration: Registration) {
        self.registrations
            .write()
            .expect("registration state poisoned")
            .insert(registration.group_id, registration);
    }

    pub fn registration(&self, group_id: ShardGroupId) -> Option<Registration> {
        self.registrations
            .read()
            .expect("registration state poisoned")
            .get(&group_id)
            .cloned()
    }

    pub fn is_registered(&self, group_id: ShardGroupId) -> bool {
        self.registration(group_id).is_some()
    }

    pub fn is_ready(&self, group_id: ShardGroupId) -> bool {
        self.is_registered(group_id)
    }

    pub fn is_any_ready(&self) -> bool {
        !self
            .registrations
            .read()
            .expect("registration state poisoned")
            .is_empty()
    }
}
