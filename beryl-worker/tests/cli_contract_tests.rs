// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use std::fs;
use std::process::Command;

use tempfile::TempDir;

#[test]
fn validate_conf_does_not_create_identity_or_storage_or_contact_metadata() {
    let temp = TempDir::new().unwrap();
    let identity_file = temp.path().join("identity").join("worker.identity");
    let storage_dir = temp.path().join("storage").join("hdd0");
    let config_path = temp.path().join("worker.yaml");
    fs::write(
        &config_path,
        format!(
            r#"
beryl.worker.identity-file: "{}"
beryl.worker.storage.dirs:
  hdd0:
    path: "{}"
    tier: hdd
    capacity: 10GiB
beryl.worker.metadata.addresses:
  - "203.0.113.1:1"
beryl.logging.format: compact
beryl.logging.output: stderr
beryl.logging.level: warn
"#,
            identity_file.display(),
            storage_dir.display()
        ),
    )
    .unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_beryl-worker"))
        .arg("validate-conf")
        .arg("--config")
        .arg(&config_path)
        .output()
        .unwrap();

    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert!(
        !identity_file.exists(),
        "configuration validation created Worker identity"
    );
    assert!(!storage_dir.exists(), "configuration validation created Worker storage");
}
