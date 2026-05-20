// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use std::fs;
use std::path::Path;
use std::process::Command;

#[test]
fn file_handle_identity_fields_are_not_public_api() {
    for (name, source, expected) in [
        (
            "path",
            r#"
use client::FileHandle;

pub fn probe(mut handle: FileHandle) {
    let value = handle.path.clone();
    handle.path = value;
}
"#,
            "field `path`",
        ),
        (
            "inode_id",
            r#"
use client::FileHandle;

pub fn probe(mut handle: FileHandle) {
    let value = handle.inode_id;
    handle.inode_id = value;
}
"#,
            "field `inode_id`",
        ),
        (
            "data_handle_id",
            r#"
use client::FileHandle;

pub fn probe(mut handle: FileHandle) {
    let value = handle.data_handle_id;
    handle.data_handle_id = value;
}
"#,
            "field `data_handle_id`",
        ),
        (
            "file_version",
            r#"
use client::FileHandle;

pub fn probe(mut handle: FileHandle) {
    let value = handle.file_version;
    handle.file_version = value;
}
"#,
            "field `file_version`",
        ),
        (
            "file_size",
            r#"
use client::FileHandle;

pub fn probe(mut handle: FileHandle) {
    let value = handle.file_size;
    handle.file_size = value;
}
"#,
            "field `file_size`",
        ),
        (
            "write_session",
            r#"
use client::FileHandle;

pub fn probe(mut handle: FileHandle) {
    let value = handle.write_session.clone();
    handle.write_session = value;
}
"#,
            "field `write_session`",
        ),
    ] {
        assert_external_snippet_fails(name, source, expected);
    }
}

#[test]
fn public_stat_and_list_results_do_not_expose_raw_headers() {
    assert_external_snippet_fails(
        "stat_header",
        r#"
use client::FsClient;

pub async fn probe(client: FsClient) {
    let result = client.stat("/alpha").await.unwrap();
    let _ = result.header;
}
"#,
        "no field `header`",
    );
    assert_external_snippet_fails(
        "list_header",
        r#"
use client::FsClient;

pub async fn probe(client: FsClient) {
    let result = client.list("/alpha").await.unwrap();
    let _ = result.header;
}
"#,
        "no field `header`",
    );
}

fn assert_external_snippet_fails(name: &str, source: &str, expected_stderr: &str) {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let case_dir = std::env::temp_dir()
        .join("public-api-boundary")
        .join(std::process::id().to_string())
        .join(name);
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
        .current_dir(&case_dir)
        .output()
        .expect("run compile-fail cargo check");
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        !output.status.success(),
        "external snippet {name} unexpectedly compiled; stdout={}; stderr={stderr}",
        String::from_utf8_lossy(&output.stdout),
    );
    assert!(
        stderr.contains(expected_stderr),
        "external snippet {name} failed for the wrong reason; expected stderr to contain {expected_stderr:?}, got {stderr}"
    );
}
