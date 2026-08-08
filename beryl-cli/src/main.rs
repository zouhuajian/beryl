// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Public command-line entry point for packaged Beryl installations.

use std::ffi::OsString;
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::Command;

use anyhow::{bail, Context, Result};
use beryl_common::build_info::BUILD_INFO;
use clap::{CommandFactory, FromArgMatches, Parser, Subcommand, ValueEnum};

/// Public Beryl CLI. Process implementation binaries remain package-internal.
#[derive(Debug, Parser)]
#[command(
    name = "beryl",
    about = "Operate Beryl metadata and worker processes",
    subcommand_required = true,
    arg_required_else_help = true
)]
struct BerylCli {
    /// Directory containing metadata.yaml and worker.yaml.
    #[arg(long, global = true, value_name = "DIR")]
    conf_dir: Option<PathBuf>,

    #[command(subcommand)]
    command: BerylCommand,
}

/// Stable public operations supported by the current product boundary.
#[derive(Debug, Subcommand)]
enum BerylCommand {
    /// Run the Metadata process in the foreground.
    Metadata,
    /// Run the Worker process in the foreground.
    Worker,
    /// Format an explicitly selected persistent role.
    Format {
        /// Persistent role whose storage will be formatted.
        #[arg(value_enum)]
        target: FormatTarget,
    },
    /// Validate role configuration without starting services or touching storage.
    ValidateConf {
        /// Validate one role; omit to validate Metadata and Worker.
        #[arg(value_enum)]
        role: Option<Role>,
    },
    /// Print the complete build identity.
    Version,
}

/// Process roles routed by the public CLI.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Role {
    Metadata,
    Worker,
}

impl Role {
    /// Returns the fixed package-internal executable name for this role.
    const fn process_binary(self) -> &'static str {
        match self {
            Self::Metadata => "beryl-metadata",
            Self::Worker => "beryl-worker",
        }
    }

    /// Returns the fixed configuration file name for this role.
    const fn config_file(self) -> &'static str {
        match self {
            Self::Metadata => "metadata.yaml",
            Self::Worker => "worker.yaml",
        }
    }

    /// Returns the role name used in user-facing diagnostics.
    const fn label(self) -> &'static str {
        match self {
            Self::Metadata => "Metadata",
            Self::Worker => "Worker",
        }
    }
}

/// Roles with an explicit destructive format operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum FormatTarget {
    Metadata,
}

fn main() -> Result<()> {
    let cli = parse_cli(std::env::args_os());

    match cli.command {
        BerylCommand::Version => {
            println!("{}", BUILD_INFO.version_text("beryl"));
            Ok(())
        }
        command => {
            let layout = InstallLayout::resolve(std::env::current_exe()?, cli.conf_dir)?;
            match command {
                BerylCommand::Metadata => exec_role(&layout, Role::Metadata, "start"),
                BerylCommand::Worker => exec_role(&layout, Role::Worker, "start"),
                BerylCommand::Format {
                    target: FormatTarget::Metadata,
                } => exec_role(&layout, Role::Metadata, "format"),
                BerylCommand::ValidateConf { role } => validate_configs(&layout, role),
                BerylCommand::Version => unreachable!("version handled without resolving package layout"),
            }
        }
    }
}

/// Fixed paths derived from the installed `bin/beryl` executable.
struct InstallLayout {
    root: PathBuf,
    conf_dir: PathBuf,
}

impl InstallLayout {
    /// Resolves package paths without consulting the current directory or PATH.
    fn resolve(executable: PathBuf, conf_dir: Option<PathBuf>) -> Result<Self> {
        let bin_dir = executable
            .parent()
            .context("beryl executable path has no parent directory")?;
        if bin_dir.file_name().and_then(|name| name.to_str()) != Some("bin") {
            bail!(
                "beryl executable must be installed as <install-root>/bin/beryl: {}",
                executable.display()
            );
        }
        let root = bin_dir
            .parent()
            .context("beryl bin directory has no installation root")?
            .to_path_buf();
        let conf_dir = conf_dir.unwrap_or_else(|| root.join("conf"));
        Ok(Self { root, conf_dir })
    }

    /// Resolves the package-internal executable for a concrete role.
    fn process_binary(&self, role: Role) -> PathBuf {
        self.root.join("libexec").join(role.process_binary())
    }

    /// Resolves the role configuration without inspecting or canonicalizing it.
    fn config_file(&self, role: Role) -> PathBuf {
        self.conf_dir.join(role.config_file())
    }
}

/// Replaces the CLI process so systemd signals and PID ownership reach the role directly.
fn exec_role(layout: &InstallLayout, role: Role, action: &str) -> Result<()> {
    let binary = layout.process_binary(role);
    let config = layout.config_file(role);
    let error = Command::new(&binary).arg(action).arg("--config").arg(&config).exec();
    Err(error).with_context(|| format!("failed to execute {} process at {}", role.label(), binary.display()))
}

/// Runs every selected static validator and reports failure only after all have run.
fn validate_configs(layout: &InstallLayout, selected_role: Option<Role>) -> Result<()> {
    let roles: &[Role] = match selected_role {
        Some(Role::Metadata) => &[Role::Metadata],
        Some(Role::Worker) => &[Role::Worker],
        None => &[Role::Metadata, Role::Worker],
    };
    let mut failed = false;

    for role in roles {
        let binary = layout.process_binary(*role);
        let config = layout.config_file(*role);
        println!("Validating {} configuration: {}", role.label(), config.display());
        match Command::new(&binary)
            .arg("validate-conf")
            .arg("--config")
            .arg(&config)
            .status()
        {
            Ok(status) if status.success() => {}
            Ok(status) => {
                eprintln!("{} configuration validation failed with {status}", role.label());
                failed = true;
            }
            Err(error) => {
                eprintln!(
                    "Failed to execute {} configuration validator at {}: {error}",
                    role.label(),
                    binary.display()
                );
                failed = true;
            }
        }
    }

    if failed {
        bail!("one or more Beryl configurations are invalid");
    }
    Ok(())
}

/// Parses the CLI with the shared detailed build identity used by both version forms.
fn parse_cli<I, T>(args: I) -> BerylCli
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let version = BUILD_INFO.version_details();
    let matches = BerylCli::command()
        .version(version.clone())
        .long_version(version)
        .try_get_matches_from(args)
        .unwrap_or_else(|error| error.exit());
    BerylCli::from_arg_matches(&matches).unwrap_or_else(|error| error.exit())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[test]
    fn install_layout_uses_package_relative_defaults() {
        let layout = InstallLayout::resolve(PathBuf::from("/opt/beryl/bin/beryl"), None).unwrap();

        assert_eq!(
            layout.process_binary(Role::Metadata),
            Path::new("/opt/beryl/libexec/beryl-metadata")
        );
        assert_eq!(
            layout.config_file(Role::Worker),
            Path::new("/opt/beryl/conf/worker.yaml")
        );
    }

    #[test]
    fn explicit_config_directory_replaces_only_the_config_root() {
        let layout =
            InstallLayout::resolve(PathBuf::from("/opt/beryl/bin/beryl"), Some(PathBuf::from("/etc/beryl"))).unwrap();

        assert_eq!(
            layout.process_binary(Role::Worker),
            Path::new("/opt/beryl/libexec/beryl-worker")
        );
        assert_eq!(
            layout.config_file(Role::Metadata),
            Path::new("/etc/beryl/metadata.yaml")
        );
    }
}
