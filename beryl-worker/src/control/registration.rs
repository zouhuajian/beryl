// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Worker metadata registration state.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
    pub advertised_endpoint: String,
}

/// Worker-local readiness set for metadata group registration.
///
/// Every accepted registration receives a new process-local epoch. Block report
/// baselines bind to that epoch so a re-registration cannot reuse an observation
/// established by an earlier registration lifecycle.
#[derive(Debug, Default)]
pub struct RegistrationSet {
    registrations: RwLock<HashMap<GroupName, RegistrationLease>>,
    next_registration_epoch: AtomicU64,
    shutting_down: AtomicBool,
}

/// Registration identity, lifecycle epoch, and current heartbeat lease.
#[derive(Clone, Debug)]
struct RegistrationLease {
    registration: Registration,
    registration_epoch: u64,
    heartbeat_deadline: Option<Instant>,
}

impl RegistrationSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a registration as a new lifecycle that still requires a heartbeat.
    ///
    /// The epoch changes even when worker identity and run are unchanged.
    pub fn record_registered(&self, registration: Registration) {
        let mut registrations = self.registrations.write().expect("registration state poisoned");
        if self.shutting_down.load(Ordering::Acquire) {
            return;
        }
        let previous_epoch = self
            .next_registration_epoch
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |epoch| epoch.checked_add(1))
            .expect("worker registration epoch exhausted");
        registrations.insert(
            registration.group_name.clone(),
            RegistrationLease {
                registration,
                registration_epoch: previous_epoch + 1,
                heartbeat_deadline: None,
            },
        );
        observe::set_worker_registered(true);
    }

    pub fn record_heartbeat_success(&self, group_name: &GroupName, lease_duration: Duration) {
        let mut registrations = self.registrations.write().expect("registration state poisoned");
        if self.shutting_down.load(Ordering::Acquire) {
            return;
        }
        if let Some(entry) = registrations.get_mut(group_name) {
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
        let mut registrations = self.registrations.write().expect("registration state poisoned");
        for registration in registrations.values_mut() {
            registration.heartbeat_deadline = None;
        }
        observe::set_worker_registered(false);
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
        observe::set_worker_registered(false);
    }

    pub fn registration(&self, group_name: &GroupName) -> Option<Registration> {
        self.registrations
            .read()
            .expect("registration state poisoned")
            .get(group_name)
            .map(|entry| entry.registration.clone())
    }

    /// Returns one consistent snapshot of a live registration and its epoch.
    ///
    /// An expired or absent heartbeat lease is not report-ready.
    pub(crate) fn ready_registration(&self, group_name: &GroupName) -> Option<(Registration, u64)> {
        if self.shutting_down.load(Ordering::Acquire) {
            return None;
        }
        self.registrations
            .read()
            .expect("registration state poisoned")
            .get(group_name)
            .filter(|entry| {
                entry
                    .heartbeat_deadline
                    .is_some_and(|deadline| deadline > Instant::now())
            })
            .map(|entry| (entry.registration.clone(), entry.registration_epoch))
    }

    pub fn is_registered(&self, group_name: &GroupName) -> bool {
        self.registration(group_name).is_some()
    }

    pub fn is_ready(&self, group_name: &GroupName) -> bool {
        if self.shutting_down.load(Ordering::Acquire) {
            return false;
        }
        self.registrations
            .read()
            .expect("registration state poisoned")
            .get(group_name)
            .and_then(|entry| entry.heartbeat_deadline)
            .map(|deadline| deadline > Instant::now())
            .unwrap_or(false)
    }

    pub fn is_any_ready(&self) -> bool {
        if self.shutting_down.load(Ordering::Acquire) {
            return false;
        }
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use metrics::{Counter, Gauge, GaugeFn, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};

    use super::*;

    #[test]
    fn new_registration_set_starts_unregistered() {
        let state = RegistrationSet::new();
        let group_name = test_group_name();

        assert!(!state.is_registered(&group_name));
        assert!(!state.is_ready(&group_name));
    }

    #[test]
    fn registration_state_updates_registered_gauge() {
        let recorder = GaugeRecorder::default();
        let group_name = test_group_name();
        let registration = test_registration(group_name.clone());

        metrics::with_local_recorder(&recorder, || {
            let state = RegistrationSet::new();

            state.record_registered(registration);
            state.mark_needs_register(&group_name);
        });

        assert_eq!(*recorder.values.lock().expect("gauge values poisoned"), vec![1.0, 0.0]);
    }

    #[test]
    fn shutdown_readiness_is_sticky_against_late_control_responses() {
        let group_name = test_group_name();
        let state = RegistrationSet::new();
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
            advertised_endpoint: "http://127.0.0.1:9090".to_string(),
        }
    }

    #[derive(Default)]
    struct GaugeRecorder {
        values: Arc<Mutex<Vec<f64>>>,
    }

    impl Recorder for GaugeRecorder {
        fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn register_counter(&self, _key: &Key, _metadata: &Metadata<'_>) -> Counter {
            Counter::noop()
        }

        fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
            assert_eq!(key.name(), observe::WORKER_REGISTERED);
            assert_eq!(key.labels().count(), 0);
            Gauge::from_arc(Arc::new(TestGauge(Arc::clone(&self.values))))
        }

        fn register_histogram(&self, _key: &Key, _metadata: &Metadata<'_>) -> Histogram {
            Histogram::noop()
        }
    }

    struct TestGauge(Arc<Mutex<Vec<f64>>>);

    impl GaugeFn for TestGauge {
        fn increment(&self, value: f64) {
            self.set(value);
        }

        fn decrement(&self, value: f64) {
            self.set(-value);
        }

        fn set(&self, value: f64) {
            self.0.lock().expect("gauge values poisoned").push(value);
        }
    }
}
