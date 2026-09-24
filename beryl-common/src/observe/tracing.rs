// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Tracing setup and configuration.

use std::io::IsTerminal;

use crate::observe::config::ObservabilityConfig;
use tracing_subscriber::{
    EnvFilter, Layer, Registry,
    fmt::{self, writer::EitherWriter},
    layer::{Layered, SubscriberExt},
    util::SubscriberInitExt,
};

/// Initialize tracing subscriber once with the configured logging layer.
pub(super) fn init_tracing_subscriber(config: &ObservabilityConfig) -> Result<(), Box<dyn std::error::Error>> {
    let invalid_config = || {
        format!(
            "unsupported log format/output: format={}, output={}",
            config.format, config.output
        )
    };
    let (stdout, ansi) = match config.output.as_str() {
        "stdout" => (true, std::io::stdout().is_terminal()),
        "stderr" => (false, std::io::stderr().is_terminal()),
        _ => return Err(invalid_config().into()),
    };
    let writer = move || {
        if stdout {
            EitherWriter::A(std::io::stdout())
        } else {
            EitherWriter::B(std::io::stderr())
        }
    };
    let layer = fmt::layer()
        .with_writer(writer)
        .with_target(true)
        .with_file(false)
        .with_line_number(false);
    match config.format.as_str() {
        "json" => init_with_log_layer(
            config,
            layer
                .json()
                .flatten_event(true)
                .with_current_span(false)
                .with_span_list(false)
                .with_ansi(false),
        ),
        "compact" => init_with_log_layer(config, layer.compact().with_ansi(ansi)),
        _ => Err(invalid_config().into()),
    }
}

fn init_with_log_layer<L>(config: &ObservabilityConfig, log_layer: L) -> Result<(), Box<dyn std::error::Error>>
where
    L: Layer<Registry> + Send + Sync + 'static,
    EnvFilter: Layer<Layered<L, Registry>>,
{
    let filter = EnvFilter::try_new(&config.level)?;
    Registry::default().with(log_layer).with(filter).try_init()?;
    Ok(())
}
