// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Metadata service main entry point.

use std::sync::Arc;

use beryl_metadata::lifecycle::format_metadata_storage;
use beryl_metadata::runtime::{init_observability, DynError, MetadataServer};
use beryl_metadata::MetadataConfig;

#[tokio::main]
async fn main() -> Result<(), DynError> {
    let command = MetadataCommand::parse(std::env::args().skip(1))?;
    let config = command.load_config()?;

    match command.action {
        MetadataAction::Format => {
            let marker = format_metadata_storage(config.as_ref()).await?;
            tracing::info!(
                cluster_id = %marker.cluster_id,
                group_name = %marker.group_name,
                node_id = marker.node_id,
                "Metadata storage formatted"
            );
            Ok(())
        }
        MetadataAction::Start => {
            let _observability = init_observability(config.as_ref())?;
            let server = MetadataServer::build(config).await?;
            server.serve().await
        }
    }
}

enum MetadataAction {
    Format,
    Start,
}

struct MetadataCommand {
    action: MetadataAction,
    config_path: Option<String>,
}

impl MetadataCommand {
    fn parse<I>(args: I) -> Result<Self, DynError>
    where
        I: IntoIterator<Item = String>,
    {
        let mut args = args.into_iter().peekable();
        let mut action = MetadataAction::Start;
        if let Some(first) = args.peek().cloned() {
            match first.as_str() {
                "format" => {
                    args.next();
                    action = MetadataAction::Format;
                }
                "start" => {
                    args.next();
                    action = MetadataAction::Start;
                }
                _ if first.starts_with('-') => {}
                _ if looks_like_path(&first) => {
                    return Err(format!("metadata config path must be passed with --config: {first}").into());
                }
                _ => return Err(format!("unsupported metadata command: {first}").into()),
            }
        }

        let mut config_path = None;
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--config" => {
                    let Some(path) = args.next() else {
                        return Err("--config requires a path".into());
                    };
                    config_path = Some(path);
                }
                "--force" => return Err("--force is not supported; clean the metadata directory manually".into()),
                _ => return Err(format!("unknown metadata argument: {arg}").into()),
            }
        }

        Ok(Self { action, config_path })
    }

    fn load_config(&self) -> Result<Arc<MetadataConfig>, DynError> {
        let config_path = self
            .config_path
            .clone()
            .or_else(|| std::env::var("BERYL_CONFIG").ok())
            .unwrap_or_else(|| "conf/metadata.yaml".to_string());
        Ok(Arc::new(MetadataConfig::load(config_path)?))
    }
}

fn looks_like_path(value: &str) -> bool {
    value.contains('/') || value.ends_with(".yaml") || value.ends_with(".yml") || value.ends_with(".toml")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn parse(args: &[&str]) -> Result<MetadataCommand, DynError> {
        MetadataCommand::parse(args.iter().map(|arg| arg.to_string()))
    }

    #[test]
    fn valid_metadata_commands_parse() {
        let format = parse(&["format", "--config", "conf/local/metadata.yaml"]).unwrap();
        assert!(matches!(format.action, MetadataAction::Format));
        assert_eq!(format.config_path.as_deref(), Some("conf/local/metadata.yaml"));

        let start = parse(&["start", "--config", "conf/local/metadata.yaml"]).unwrap();
        assert!(matches!(start.action, MetadataAction::Start));
        assert_eq!(start.config_path.as_deref(), Some("conf/local/metadata.yaml"));

        let default_start = parse(&[]).unwrap();
        assert!(matches!(default_start.action, MetadataAction::Start));
        assert!(default_start.config_path.is_none());
    }

    #[test]
    fn metadata_observe_cli_overrides_are_rejected() {
        for flag in [
            "--observe-profile",
            "--log-level",
            "--log-format",
            "--log-output",
            "--metrics-bind",
            "--metrics-path",
            "--trace-enabled",
        ] {
            let err = parse(&["start", flag, "value"])
                .err()
                .expect("observe CLI override must fail");
            assert!(err.to_string().contains("unknown metadata argument"), "{flag}: {err}");
        }
    }

    #[test]
    fn metadata_startup_load_uses_file_observe_values() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("metadata.yaml");
        fs::write(
            &config_path,
            r#"
observe.log.format: json
observe.log.output: stdout
observe.log.level: "warn"
observe.metrics.prometheus.bind: "127.0.0.1:19081"
observe.metrics.prometheus.path: "/metrics"
"#,
        )
        .unwrap();

        let command = parse(&["start", "--config", config_path.to_str().unwrap()]).unwrap();
        let config = command.load_config().unwrap();

        assert_eq!(config.observability.log.format, "json");
        assert_eq!(config.observability.log.output, "stdout");
        assert_eq!(config.observability.metrics.prometheus.bind, "127.0.0.1:19081");
    }

    #[test]
    fn metadata_config_path_requires_explicit_config_flag() {
        let err = parse(&["conf/local/metadata.yaml"])
            .err()
            .expect("positional metadata config path must fail");
        assert!(err.to_string().contains("--config"));
    }

    #[test]
    fn metadata_format_branch_does_not_initialize_observability() {
        // Static guard only: a runtime test would need to format storage or add
        // test-only init hooks to production dispatch. Keep this narrow and do
        // not expand source checks beyond this start-vs-format invariant.
        let source = include_str!("main.rs");
        let production_source = source.split("\n#[cfg(test)]").next().expect("production source");
        let init_call = concat!("init_", "observability", "(config.as_ref())?");
        let format_call = "format_metadata_storage(config.as_ref()).await?";

        assert_eq!(
            production_source.matches(init_call).count(),
            1,
            "observability initialization must stay start-only"
        );

        let format_call = production_source.find(format_call).expect("format call");
        let start_branch = production_source
            .find("MetadataAction::Start =>")
            .expect("start branch");
        let init_call = production_source.find(init_call).expect("init call");

        assert!(
            format_call < start_branch,
            "format dispatch should stay separate from start"
        );
        assert!(
            start_branch < init_call,
            "observability should initialize only in start dispatch"
        );
    }
}
