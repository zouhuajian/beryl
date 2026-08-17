// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Metadata service process entry point.

use std::ffi::OsString;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;

use beryl_common::build_info::BUILD_INFO;
use beryl_common::termination::TerminationSignal;
use beryl_metadata::lifecycle::format_metadata_storage;
use beryl_metadata::runtime::{init_observability, DynError, MetadataServer};
use beryl_metadata::MetadataConfig;
use clap::{CommandFactory, FromArgMatches, Parser, Subcommand};

/// Internal Metadata process protocol used by the packaged `beryl` CLI.
#[derive(Debug, Parser)]
#[command(
    name = "beryl-metadata",
    about = "Run or maintain the Beryl Metadata process",
    subcommand_required = true,
    arg_required_else_help = true
)]
struct MetadataCli {
    #[command(subcommand)]
    command: MetadataCommand,
}

/// Explicit Metadata actions; none may infer a config path or default to start.
#[derive(Debug, Subcommand)]
enum MetadataCommand {
    /// Start the Metadata process in the foreground.
    Start {
        /// Metadata YAML configuration file.
        #[arg(long, value_name = "FILE")]
        config: PathBuf,
    },
    /// Initialize empty Metadata storage described by the configuration.
    Format {
        /// Metadata YAML configuration file.
        #[arg(long, value_name = "FILE")]
        config: PathBuf,
    },
    /// Validate configuration without opening storage, binding ports, or starting tasks.
    ValidateConf {
        /// Metadata YAML configuration file.
        #[arg(long, value_name = "FILE")]
        config: PathBuf,
    },
}

fn main() -> Result<(), DynError> {
    let cli = parse_cli(std::env::args_os());

    match cli.command {
        MetadataCommand::Start { config } => run_async(start_metadata(config)),
        MetadataCommand::Format { config } => run_async(format_metadata(config)),
        MetadataCommand::ValidateConf { config } => validate_metadata_config(config),
    }
}

/// Builds the asynchronous runtime only for actions that own process or storage lifecycle.
fn run_async(future: impl Future<Output = Result<(), DynError>>) -> Result<(), DynError> {
    tokio::runtime::Runtime::new()?.block_on(future)
}

/// Starts Metadata while preserving its bounded shutdown lifecycle.
async fn start_metadata(config_path: PathBuf) -> Result<(), DynError> {
    let mut termination = TerminationSignal::install()?.monitor();
    let config = match MetadataConfig::load(config_path) {
        Ok(config) => Arc::new(config),
        Err(error) => {
            termination.shutdown().await?;
            return Err(error.into());
        }
    };
    if termination.is_cancelled() {
        let signal = termination.recv().await?;
        tracing::info!(?signal, "Shutdown signal received during Metadata startup");
        return Ok(());
    }
    let observability = match init_observability(config.as_ref()) {
        Ok(observability) => observability,
        Err(error) => {
            termination.shutdown().await?;
            return Err(error);
        }
    };
    let server = match MetadataServer::build(config, termination.cancellation_token()).await {
        Ok(Some(server)) => server,
        Ok(None) => {
            let signal = termination.recv().await?;
            tracing::info!(?signal, "Shutdown signal received during Metadata startup");
            return Ok(());
        }
        Err(error) => {
            termination.shutdown().await?;
            return Err(error);
        }
    };
    let result = server.serve(observability, &mut termination).await;
    termination.shutdown().await?;
    result
}

/// Formats only Metadata storage and preserves its existing fail-closed checks.
async fn format_metadata(config_path: PathBuf) -> Result<(), DynError> {
    let config = MetadataConfig::load(config_path)?;
    let marker = format_metadata_storage(&config).await?;
    println!(
        "Metadata storage formatted: cluster={}, group={}, node={}",
        marker.cluster_id, marker.group_name, marker.node_id
    );
    Ok(())
}

/// Runs exactly the static loader used by startup and performs no runtime initialization.
fn validate_metadata_config(config_path: PathBuf) -> Result<(), DynError> {
    MetadataConfig::load(&config_path)?;
    println!("Metadata configuration is valid: {}", config_path.display());
    Ok(())
}

/// Parses process arguments and lets clap terminate for help, version, or usage errors.
fn parse_cli<I, T>(args: I) -> MetadataCli
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    try_parse_cli(args).unwrap_or_else(|error| error.exit())
}

fn try_parse_cli<I, T>(args: I) -> Result<MetadataCli, clap::Error>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let version = BUILD_INFO.version_details();
    let matches = MetadataCli::command()
        .version(version.clone())
        .long_version(version)
        .try_get_matches_from(args)?;
    MetadataCli::from_arg_matches(&matches)
}
