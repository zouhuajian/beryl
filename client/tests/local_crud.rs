// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use client::{AppendOptions, ClientConfig, CreateOptions, FsClient, ListOptions, OpenOptions};

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[ignore = "requires local metadata and worker"]
async fn local_client_crud_roundtrip() {
    let config_path =
        std::env::var("VECTON_CLIENT_CONFIG").unwrap_or_else(|_| "conf/local/client-site.yaml".to_string());
    let config = ClientConfig::load(&config_path)
        .unwrap_or_else(|err| panic!("failed to load client config {config_path}: {err}"));
    let client = FsClient::try_new(config).expect("client config must be valid");
    let suffix = format!("codex-local-crud-{}", unique_suffix());
    let path = format!("/{suffix}");
    let renamed_path = format!("/{suffix}-renamed");

    let _ = client.delete(&path, false).await;
    let _ = client.delete(&renamed_path, false).await;

    let first = Bytes::from_static(b"vecton-local-crud");
    let second = Bytes::from_static(b"-append");
    let mut writer = client
        .create(&path, CreateOptions::overwrite())
        .await
        .unwrap_or_else(|err| panic!("create requires running local metadata and worker: {err}"));
    writer
        .write_all(first.clone())
        .await
        .unwrap_or_else(|err| panic!("write requires running local metadata and worker: {err}"));
    writer
        .close()
        .await
        .unwrap_or_else(|err| panic!("close requires running local metadata and worker: {err}"));

    let reader = client
        .open(&path, OpenOptions::default())
        .await
        .unwrap_or_else(|err| panic!("open requires running local metadata and worker: {err}"));
    let read = reader
        .read_at(0, first.len() as u32)
        .await
        .unwrap_or_else(|err| panic!("read requires running local metadata and worker: {err}"));
    assert_eq!(read, first);

    let mut appender = client
        .append(&path, AppendOptions::default())
        .await
        .unwrap_or_else(|err| panic!("append requires running local metadata and worker: {err}"));
    appender
        .write_all(second.clone())
        .await
        .unwrap_or_else(|err| panic!("append write requires running local metadata and worker: {err}"));
    appender
        .close()
        .await
        .unwrap_or_else(|err| panic!("append close requires running local metadata and worker: {err}"));

    client
        .rename(&path, &renamed_path)
        .await
        .unwrap_or_else(|err| panic!("rename requires running local metadata and worker: {err}"));
    let listing = client
        .list("/", ListOptions::default())
        .await
        .unwrap_or_else(|err| panic!("list requires running local metadata and worker: {err}"));
    let renamed_name = renamed_path.trim_start_matches('/');
    assert!(
        listing.entries.iter().any(|entry| entry.name == renamed_name),
        "root listing should include {renamed_name}"
    );

    let expected = [first.as_ref(), second.as_ref()].concat();
    let reader = client
        .open(&renamed_path, OpenOptions::default())
        .await
        .unwrap_or_else(|err| panic!("open renamed file requires running local metadata and worker: {err}"));
    let read = reader
        .read_at(0, expected.len() as u32)
        .await
        .unwrap_or_else(|err| panic!("read renamed file requires running local metadata and worker: {err}"));
    assert_eq!(read.as_ref(), expected.as_slice());

    client
        .delete(&renamed_path, false)
        .await
        .unwrap_or_else(|err| panic!("delete requires running local metadata and worker: {err}"));
}

fn unique_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos()
}
