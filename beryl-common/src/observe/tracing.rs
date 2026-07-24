// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Tracing setup and configuration.

use std::io::IsTerminal;

use crate::observe::config::ObservabilityConfig;
use tracing_subscriber::{
    EnvFilter, Layer, Registry,
    fmt::{self},
    layer::{Layered, SubscriberExt},
    util::SubscriberInitExt,
};

/// Initialize tracing subscriber once with the configured logging layer.
pub fn init_tracing_subscriber(config: &ObservabilityConfig) -> Result<(), Box<dyn std::error::Error>> {
    let log = &config.log;
    match (log.format.as_str(), log.output.as_str()) {
        ("json", "stdout") => init_with_log_layer(
            config,
            fmt::layer()
                .json()
                .flatten_event(true)
                .with_current_span(false)
                .with_span_list(false)
                .with_ansi(false)
                .with_target(true)
                .with_file(false)
                .with_line_number(false)
                .with_writer(std::io::stdout),
        ),
        ("json", "stderr") => init_with_log_layer(
            config,
            fmt::layer()
                .json()
                .flatten_event(true)
                .with_current_span(false)
                .with_span_list(false)
                .with_ansi(false)
                .with_target(true)
                .with_file(false)
                .with_line_number(false)
                .with_writer(std::io::stderr),
        ),
        ("compact", "stdout") => init_with_log_layer(
            config,
            fmt::layer()
                .compact()
                .with_ansi(ansi_enabled(log.output.as_str()))
                .with_target(true)
                .with_file(false)
                .with_line_number(false)
                .with_writer(std::io::stdout),
        ),
        ("compact", "stderr") => init_with_log_layer(
            config,
            fmt::layer()
                .compact()
                .with_ansi(ansi_enabled(log.output.as_str()))
                .with_target(true)
                .with_file(false)
                .with_line_number(false)
                .with_writer(std::io::stderr),
        ),
        _ => Err(format!(
            "unsupported log format/output: format={}, output={}",
            log.format, log.output
        )
        .into()),
    }
}

fn init_with_log_layer<L>(config: &ObservabilityConfig, log_layer: L) -> Result<(), Box<dyn std::error::Error>>
where
    L: Layer<Registry> + Send + Sync + 'static,
    EnvFilter: Layer<Layered<L, Registry>>,
{
    let filter = EnvFilter::try_new(&config.log.level)?;
    Registry::default().with(log_layer).with(filter).try_init()?;
    Ok(())
}

fn ansi_enabled(output: &str) -> bool {
    match output {
        "stdout" => std::io::stdout().is_terminal(),
        "stderr" => std::io::stderr().is_terminal(),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::{Arc, Mutex};

    use tracing_subscriber::{Registry, layer::SubscriberExt};

    use super::*;

    #[test]
    fn json_formatter_flattens_event_and_omits_span_wrappers() {
        let output = Arc::new(Mutex::new(Vec::new()));
        let writer = TestWriter::new(Arc::clone(&output));
        let subscriber = Registry::default().with(
            fmt::layer()
                .json()
                .flatten_event(true)
                .with_current_span(false)
                .with_span_list(false)
                .with_ansi(false)
                .with_target(true)
                .with_file(false)
                .with_line_number(false)
                .with_writer(move || writer.clone()),
        );

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(
                event = "worker_soft_state_reset",
                reset_reason = "startup",
                "worker soft state reset"
            );
        });

        let line = String::from_utf8(output.lock().expect("test log output poisoned").clone()).unwrap();
        let json: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(json["event"], "worker_soft_state_reset");
        assert_eq!(json["reset_reason"], "startup");
        assert!(json.get("fields").is_none(), "{line}");
        assert!(json.get("span").is_none(), "{line}");
        assert!(json.get("spans").is_none(), "{line}");
    }

    #[test]
    fn compact_formatter_is_not_json() {
        let output = Arc::new(Mutex::new(Vec::new()));
        let writer = TestWriter::new(Arc::clone(&output));
        let subscriber = Registry::default().with(
            fmt::layer()
                .compact()
                .with_ansi(false)
                .with_target(true)
                .with_file(false)
                .with_line_number(false)
                .with_writer(move || writer.clone()),
        );

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(event = "worker_registered", worker_id = 1_u64, "worker registered");
        });

        let line = String::from_utf8(output.lock().expect("test log output poisoned").clone()).unwrap();
        assert!(!line.trim_start().starts_with('{'), "{line}");
        assert!(line.contains("worker registered"), "{line}");
    }

    #[derive(Clone)]
    struct TestWriter {
        output: Arc<Mutex<Vec<u8>>>,
    }

    impl TestWriter {
        fn new(output: Arc<Mutex<Vec<u8>>>) -> Self {
            Self { output }
        }
    }

    impl io::Write for TestWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.output
                .lock()
                .expect("test log output poisoned")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
}
