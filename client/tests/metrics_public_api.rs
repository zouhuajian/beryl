// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use std::sync::{Arc, Mutex};

use client::{ClientConfig, ClientMetric, ClientMetricEvent, ClientMetrics, FsClient, WriteLeaseConfig};

#[derive(Debug, Default)]
struct RecordingMetrics {
    events: Mutex<Vec<ClientMetricEvent>>,
}

impl RecordingMetrics {
    fn events(&self) -> Vec<ClientMetricEvent> {
        self.events.lock().expect("events").clone()
    }
}

impl ClientMetrics for RecordingMetrics {
    fn record(&self, event: ClientMetricEvent) {
        self.events.lock().expect("events").push(event);
    }
}

#[tokio::test]
async fn fs_client_accepts_public_metrics_sink() {
    let mut flat = common::FlatConfig::new();
    flat.set("client.metadata.group.root.endpoints", "http://[invalid");
    let config = ClientConfig::from_flat(flat).expect("client config");
    let metrics = Arc::new(RecordingMetrics::default());
    let client = FsClient::try_new_with_metrics(config, metrics.clone()).expect("client");

    client.stat("/alpha").await.expect_err("invalid endpoint must fail");

    let events = metrics.events();
    assert!(events.iter().any(|event| {
        event.metric() == ClientMetric::ChannelBuildError && event.labels().target_plane() == Some("metadata")
    }));
}

#[test]
fn write_lease_config_is_public_api() {
    let config = WriteLeaseConfig {
        auto_renew: false,
        renew_before_expiry_ms: 42,
    };

    assert!(!config.auto_renew);
    assert_eq!(config.renew_before_expiry_ms, 42);
}
