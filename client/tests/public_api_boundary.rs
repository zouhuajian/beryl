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
use client::{AppendOptions, CreateOptions, FileReader, FileWriter, FsClient, ListOptions, OpenOptions};

pub fn probe_reader(reader: FileReader) {
    let _ = reader.inner;
    reader.close();
}

pub fn probe_writer(writer: FileWriter) {
    let _ = writer.inner;
}

pub async fn probe_stat(client: FsClient) {
    let result = client.stat("/alpha").await.unwrap();
    let _ = result.header;
}

pub async fn probe_list(client: FsClient) {
    let result = client.list("/alpha", ListOptions::default()).await.unwrap();
    let _ = result.header;
}

pub async fn probe_removed_client_handle_methods(client: FsClient, reader: FileReader, writer: FileWriter) {
    let _ = client.read(&reader, 0, 1).await.unwrap();
    client.write(&writer, 0, Vec::new().into()).await.unwrap();
    client.close(&writer).await.unwrap();
    client.sync_write_visibility(&writer).await.unwrap();
    client.sync_write_durability(&writer).await.unwrap();
    client.renew_lease(&writer).await.unwrap();
    client.abort(&writer).await.unwrap();
}

pub async fn probe_split_entrypoints(client: FsClient) {
    let _ = client.open("/alpha", OpenOptions::default()).await.unwrap();
    let _ = client.create("/alpha", CreateOptions::create()).await.unwrap();
    let _ = client.append("/alpha", AppendOptions::default()).await.unwrap();
}
"#,
        &[
            "field `inner`",
            "no method named `close` found for struct `FileReader`",
            "no method named `read` found for struct `FsClient`",
            "no method named `write` found for struct `FsClient`",
            "no method named `close` found for struct `FsClient`",
            "no method named `sync_write_visibility` found for struct `FsClient`",
            "no method named `sync_write_durability` found for struct `FsClient`",
            "no method named `renew_lease` found for struct `FsClient`",
            "no method named `abort` found for struct `FsClient`",
            "no field `header`",
        ],
    );
}

#[test]
fn public_writer_operations_require_mutable_writer_binding() {
    assert_external_snippet_fails(
        "writer_operations_require_mutable_binding",
        r#"
use client::FileWriter;

pub async fn probe_writer_mutability(writer: FileWriter) {
    writer.write_all(Vec::new().into()).await.unwrap();
    writer.sync_write_visibility().await.unwrap();
    writer.sync_write_durability().await.unwrap();
    writer.renew_lease().await.unwrap();
    writer.close().await.unwrap();
    writer.abort().await.unwrap();
}
"#,
        &["cannot borrow `writer` as mutable"],
    );
}

#[test]
fn public_api_does_not_export_stale_file_handle_or_create_mode() {
    assert_external_snippet_fails(
        "stale_handle_exports_removed",
        r#"
use client::{CreateMode, FileHandle};

pub fn probe(_handle: FileHandle, _mode: CreateMode) {}
"#,
        &["unresolved imports `client::CreateMode`, `client::FileHandle`"],
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
