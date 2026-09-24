// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Worker metadata registration state.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::RwLock;
use std::time::{Duration, Instant};

use beryl_types::{GroupName, WorkerId, WorkerRunId};

use crate::observe;

/// Metadata-confirmed worker registration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Registration {
    pub group_name: GroupName,
    pub worker_id: WorkerId,
    pub worker_run_id: WorkerRunId,
}

/// Worker-local readiness for one metadata group registration.
///
/// Every accepted registration receives a new process-local epoch. Block report
/// baselines bind to that epoch so a re-registration cannot reuse an observation
/// established by an earlier registration lifecycle.
#[derive(Debug, Default)]
pub struct RegistrationState {
    inner: RwLock<RegistrationInner>,
    shutting_down: AtomicBool,
}

#[derive(Debug, Default)]
struct RegistrationInner {
    lease: Option<RegistrationLease>,
    epoch: u64,
}

/// Registration identity and current heartbeat lease.
#[derive(Clone, Debug)]
struct RegistrationLease {
    registration: Registration,
    heartbeat_deadline: Option<Instant>,
}

impl RegistrationState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a registration as a new lifecycle that still requires a heartbeat.
    ///
    /// The epoch changes even when worker identity and run are unchanged.
    pub fn record_registered(&self, registration: Registration) {
        let mut inner = self.inner.write().expect("registration state poisoned");
        if self.shutting_down.load(Ordering::Acquire) {
            return;
        }
        inner.epoch = inner.epoch.checked_add(1).expect("worker registration epoch exhausted");
        inner.lease = Some(RegistrationLease {
            registration,
            heartbeat_deadline: None,
        });
        observe::set_worker_registered(true);
    }

    pub fn record_heartbeat_success(&self, group_name: &GroupName, lease_duration: Duration) {
        let mut inner = self.inner.write().expect("registration state poisoned");
        if self.shutting_down.load(Ordering::Acquire) {
            return;
        }
        if let Some(entry) = inner
            .lease
            .as_mut()
            .filter(|entry| &entry.registration.group_name == group_name)
        {
            entry.heartbeat_deadline = Some(Instant::now() + lease_duration);
        }
    }

    /// Permanently closes process readiness before Worker RPC drain begins.
    ///
    /// Registrations remain available to already accepted work, but their
    /// heartbeat leases are removed and late control-plane responses cannot
    /// make the process ready again.
    pub fn begin_shutdown(&self) {
        self.shutting_down.store(true, Ordering::Release);
        let mut inner = self.inner.write().expect("registration state poisoned");
        if let Some(registration) = inner.lease.as_mut() {
            registration.heartbeat_deadline = None;
        }
        observe::set_worker_registered(false);
    }

    pub fn mark_needs_register(&self, group_name: &GroupName) {
        let mut inner = self.inner.write().expect("registration state poisoned");
        if inner
            .lease
            .as_ref()
            .is_some_and(|entry| &entry.registration.group_name == group_name)
        {
            inner.lease = None;
            observe::set_worker_registered(false);
        }
    }

    pub fn registration(&self, group_name: &GroupName) -> Option<Registration> {
        self.inner
            .read()
            .expect("registration state poisoned")
            .lease
            .as_ref()
            .filter(|entry| &entry.registration.group_name == group_name)
            .map(|entry| entry.registration.clone())
    }

    /// Returns one consistent snapshot of a live registration and its epoch.
    ///
    /// An expired or absent heartbeat lease is not report-ready.
    pub(crate) fn ready_registration(&self, group_name: &GroupName) -> Option<(Registration, u64)> {
        let inner = self.inner.read().expect("registration state poisoned");
        self.ready_lease(&inner, group_name)
            .map(|entry| (entry.registration.clone(), inner.epoch))
    }

    pub fn is_registered(&self, group_name: &GroupName) -> bool {
        self.registration(group_name).is_some()
    }

    pub fn is_ready(&self, group_name: &GroupName) -> bool {
        let inner = self.inner.read().expect("registration state poisoned");
        self.ready_lease(&inner, group_name).is_some()
    }

    fn ready_lease<'a>(&self, inner: &'a RegistrationInner, group_name: &GroupName) -> Option<&'a RegistrationLease> {
        if self.shutting_down.load(Ordering::Acquire) {
            return None;
        }
        inner
            .lease
            .as_ref()
            .filter(|entry| &entry.registration.group_name == group_name)
            .filter(|entry| {
                entry
                    .heartbeat_deadline
                    .is_some_and(|deadline| deadline > Instant::now())
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shutdown_readiness_is_sticky_against_late_control_responses() {
        let group_name = test_group_name();
        let state = RegistrationState::new();
        state.record_registered(test_registration(group_name.clone()));
        state.record_heartbeat_success(&group_name, Duration::from_secs(30));
        assert!(state.is_ready(&group_name));

        state.begin_shutdown();
        state.record_registered(test_registration(group_name.clone()));
        state.record_heartbeat_success(&group_name, Duration::from_secs(30));

        assert!(!state.is_ready(&group_name));
        assert!(state.registration(&group_name).is_some());
    }

    fn test_group_name() -> GroupName {
        GroupName::parse("root").expect("test group name is valid")
    }

    fn test_registration(group_name: GroupName) -> Registration {
        Registration {
            group_name,
            worker_id: WorkerId::new(42),
            worker_run_id: WorkerRunId::new(),
        }
    }
}
