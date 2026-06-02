// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker-owned metrics emitted through the shared recorder.

pub(crate) const WORKER_UP: &str = "worker_up";
pub(crate) const WORKER_REGISTERED: &str = "worker_registered";
pub(crate) const WORKER_HEARTBEAT_SENT_TOTAL: &str = "worker_heartbeat_sent_total";
pub(crate) const WORKER_BLOCK_REPORT_SENT_TOTAL: &str = "worker_block_report_sent_total";

pub fn record_worker_started() {
    metrics::gauge!(WORKER_UP).set(1.0);
}

pub fn set_worker_registered(registered: bool) {
    metrics::gauge!(WORKER_REGISTERED).set(if registered { 1.0 } else { 0.0 });
}

pub(crate) fn record_heartbeat_sent() {
    metrics::counter!(WORKER_HEARTBEAT_SENT_TOTAL).increment(1);
}

pub(crate) fn record_block_report_sent() {
    metrics::counter!(WORKER_BLOCK_REPORT_SENT_TOTAL).increment(1);
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use metrics::{Counter, Gauge, GaugeFn, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit};

    use super::*;

    #[test]
    fn p0_metric_names_match_contract() {
        let names = [
            WORKER_UP,
            WORKER_REGISTERED,
            WORKER_HEARTBEAT_SENT_TOTAL,
            WORKER_BLOCK_REPORT_SENT_TOTAL,
        ];

        assert_eq!(WORKER_UP, "worker_up");
        assert_eq!(WORKER_REGISTERED, "worker_registered");
        assert_eq!(WORKER_HEARTBEAT_SENT_TOTAL, "worker_heartbeat_sent_total");
        assert_eq!(WORKER_BLOCK_REPORT_SENT_TOTAL, "worker_block_report_sent_total");
        assert!(names.iter().all(|name| !name.starts_with(concat!("vecton", "_"))));
    }

    #[test]
    fn observe_helpers_emit_without_installed_recorder() {
        record_worker_started();
        set_worker_registered(false);
        set_worker_registered(true);
        record_heartbeat_sent();
        record_block_report_sent();
    }

    #[test]
    fn worker_registered_helper_sets_gauge_values() {
        let recorder = MetricRecorder::default();

        metrics::with_local_recorder(&recorder, || {
            set_worker_registered(false);
            set_worker_registered(true);
        });

        assert_eq!(
            *recorder.gauges.lock().expect("gauge values poisoned"),
            vec![
                (WORKER_REGISTERED.to_string(), 0.0),
                (WORKER_REGISTERED.to_string(), 1.0)
            ]
        );
    }

    #[derive(Default)]
    struct MetricRecorder {
        gauges: Arc<Mutex<Vec<(String, f64)>>>,
    }

    impl Recorder for MetricRecorder {
        fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
            assert_eq!(key.labels().count(), 0);
            Counter::noop()
        }

        fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
            assert_eq!(key.labels().count(), 0);
            Gauge::from_arc(Arc::new(TestGauge {
                name: key.name().to_string(),
                values: Arc::clone(&self.gauges),
            }))
        }

        fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
            assert_eq!(key.labels().count(), 0);
            Histogram::noop()
        }
    }

    struct TestGauge {
        name: String,
        values: Arc<Mutex<Vec<(String, f64)>>>,
    }

    impl GaugeFn for TestGauge {
        fn increment(&self, value: f64) {
            self.set(value);
        }

        fn decrement(&self, value: f64) {
            self.set(-value);
        }

        fn set(&self, value: f64) {
            self.values
                .lock()
                .expect("gauge values poisoned")
                .push((self.name.clone(), value));
        }
    }
}
