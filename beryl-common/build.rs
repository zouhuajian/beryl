// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use std::env;
use std::process::Command;

const UNKNOWN_REVISION: &str = "unknown";

fn main() {
    println!("cargo:rerun-if-env-changed=BERYL_SOURCE_REVISION");

    let source_revision = match env::var("BERYL_SOURCE_REVISION") {
        Ok(value) if is_full_git_revision(&value) => value,
        Ok(_) => panic!("BERYL_SOURCE_REVISION must be a full hexadecimal Git revision"),
        Err(env::VarError::NotPresent) => UNKNOWN_REVISION.to_string(),
        Err(env::VarError::NotUnicode(_)) => panic!("BERYL_SOURCE_REVISION must be valid UTF-8"),
    };
    let profile = env::var("PROFILE").expect("Cargo must provide PROFILE to build scripts");
    if profile == "release" && source_revision == UNKNOWN_REVISION {
        panic!("BERYL_SOURCE_REVISION is required for release builds");
    }

    let rustc = env::var("RUSTC").expect("Cargo must provide RUSTC to build scripts");
    let rustc_version = Command::new(rustc)
        .arg("--version")
        .output()
        .expect("failed to execute rustc --version");
    assert!(rustc_version.status.success(), "rustc --version failed");
    let rustc_version = String::from_utf8(rustc_version.stdout)
        .expect("rustc --version returned non-UTF-8 output")
        .trim()
        .to_string();
    let target = env::var("TARGET").expect("Cargo must provide TARGET to build scripts");

    println!("cargo:rustc-env=BERYL_BUILD_SOURCE_REVISION={source_revision}");
    println!("cargo:rustc-env=BERYL_BUILD_RUSTC_VERSION={rustc_version}");
    println!("cargo:rustc-env=BERYL_BUILD_TARGET={target}");
}

/// Accepts full SHA-1 and SHA-256 object identifiers used by Git repositories.
fn is_full_git_revision(value: &str) -> bool {
    matches!(value.len(), 40 | 64) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}
