// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata service main entry point.

use std::sync::Arc;

use metadata::lifecycle::format_metadata_storage;
use metadata::runtime::{init_observability, load_config, DynError, MetadataServer};
use metadata::MetadataConfig;

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
        if let Some(path) = &self.config_path {
            return Ok(Arc::new(MetadataConfig::load(path)?));
        }
        load_config()
    }
}

fn looks_like_path(value: &str) -> bool {
    value.contains('/') || value.ends_with(".yaml") || value.ends_with(".yml") || value.ends_with(".toml")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<MetadataCommand, DynError> {
        MetadataCommand::parse(args.iter().map(|arg| arg.to_string()))
    }

    #[test]
    fn valid_metadata_commands_parse() {
        let format = parse(&["format", "--config", "conf/local/core-site.yaml"]).unwrap();
        assert!(matches!(format.action, MetadataAction::Format));
        assert_eq!(format.config_path.as_deref(), Some("conf/local/core-site.yaml"));

        let start = parse(&["start", "--config", "conf/local/core-site.yaml"]).unwrap();
        assert!(matches!(start.action, MetadataAction::Start));
        assert_eq!(start.config_path.as_deref(), Some("conf/local/core-site.yaml"));

        let default_start = parse(&[]).unwrap();
        assert!(matches!(default_start.action, MetadataAction::Start));
        assert!(default_start.config_path.is_none());
    }

    #[test]
    fn removed_metadata_command_words_fail() {
        for args in [
            &["bootstrap"][..],
            &["auto-format"][..],
            &["worker"][..],
            &["bootstrap", "--config", "conf/core-site.yaml"][..],
        ] {
            let err = parse(args).err().expect("removed metadata command must fail");
            assert!(err.to_string().contains("unsupported metadata command"));
        }
    }

    #[test]
    fn metadata_config_path_requires_explicit_config_flag() {
        let err = parse(&["conf/local/core-site.yaml"])
            .err()
            .expect("positional metadata config path must fail");
        assert!(err.to_string().contains("--config"));
    }
}
