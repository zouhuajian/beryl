// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Worker metadata registration state.

use std::collections::HashMap;
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
        observe::set_worker_registered(true);
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
        observe::set_worker_registered(false);
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
