// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use std::fs;
use std::path::Path;
use std::process::Command;

#[test]
fn public_api_does_not_expose_internal_identity_or_headers() {
    assert_external_snippet_fails(
        "public_boundary",
        r#"
use client::FileHandle;
use client::FsClient;

pub fn probe(mut handle: FileHandle) {
    let value = handle.path.clone();
    handle.path = value;

    let value = handle.inode_id;
    handle.inode_id = value;

    let value = handle.data_handle_id;
    handle.data_handle_id = value;

    let value = handle.file_version;
    handle.file_version = value;

    let value = handle.file_size;
    handle.file_size = value;

    let value = handle.write_session.clone();
    handle.write_session = value;
}

pub async fn probe_stat(client: FsClient) {
    let result = client.stat("/alpha").await.unwrap();
    let _ = result.header;
}

pub async fn probe_list(client: FsClient) {
    let result = client.list("/alpha").await.unwrap();
    let _ = result.header;
}
"#,
        &[
            "field `path`",
            "field `inode_id`",
            "field `data_handle_id`",
            "field `file_version`",
            "field `file_size`",
            "field `write_session`",
            "no field `header`",
        ],
    );
}

fn assert_external_snippet_fails(name: &str, source: &str, expected_stderr: &[&str]) {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace_dir = manifest_dir.parent().expect("client crate lives under workspace root");
    let case_dir = std::env::temp_dir()
        .join("public-api-boundary")
        .join(std::process::id().to_string())
        .join(name);
    let target_dir = workspace_dir.join("target").join("public-api-boundary");
    let _ = fs::remove_dir_all(&case_dir);
    fs::create_dir_all(case_dir.join("src")).expect("create compile-fail case");
    fs::write(case_dir.join("src/lib.rs"), source).expect("write compile-fail source");
    fs::write(
        case_dir.join("Cargo.toml"),
        format!(
            r#"[package]
name = "client_public_api_boundary_{name}"
version = "0.0.0"
edition = "2021"

[workspace]

[dependencies]
client = {{ path = "{}" }}
"#,
            manifest_dir.display()
        ),
    )
    .expect("write compile-fail manifest");

    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let output = Command::new(cargo)
        .arg("check")
        .arg("--quiet")
        .arg("--offline")
        .env("CARGO_TARGET_DIR", &target_dir)
        .current_dir(&case_dir)
        .output()
        .expect("run compile-fail cargo check");
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        !output.status.success(),
        "external snippet {name} unexpectedly compiled; stdout={}; stderr={stderr}",
        String::from_utf8_lossy(&output.stdout),
    );
    for expected in expected_stderr {
        assert!(
            stderr.contains(expected),
            "external snippet {name} failed for the wrong reason; expected stderr to contain {expected:?}, got {stderr}"
        );
    }
}
