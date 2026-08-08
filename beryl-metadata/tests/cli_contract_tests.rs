// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use std::fs;
use std::process::Command;

use tempfile::TempDir;

#[test]
fn help_and_version_do_not_require_configuration() {
    for flag in ["--help", "--version"] {
        let output = Command::new(env!("CARGO_BIN_EXE_beryl-metadata"))
            .arg(flag)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{flag}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn validate_conf_reads_configuration_without_touching_metadata_storage() {
    let temp = TempDir::new().unwrap();
    let storage_dir = temp.path().join("must-not-exist");
    let config_path = temp.path().join("metadata.yaml");
    fs::write(
        &config_path,
        format!(
            r#"
beryl.metadata.storage.dir: "{}"
beryl.logging.format: compact
beryl.logging.output: stderr
beryl.logging.level: warn
"#,
            storage_dir.display()
        ),
    )
    .unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_beryl-metadata"))
        .arg("validate-conf")
        .arg("--config")
        .arg(&config_path)
        .output()
        .unwrap();

    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert!(
        !storage_dir.exists(),
        "configuration validation created Metadata storage"
    );
}
