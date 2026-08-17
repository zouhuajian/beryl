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
