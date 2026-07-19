// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use beryl_client::{ClientResult, CreateOptions, InodeKind, ListOptions};
use beryl_e2e::{data::deterministic_bytes, TestCluster};
use bytes::Bytes;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_client_crud_roundtrip() {
    let mut cluster = TestCluster::start().await.expect("start hermetic local cluster");
    let client = cluster.client();
    let dir = "/e2e";
    let path = "/e2e/file";
    let renamed_path = "/e2e/file.renamed";

    let created_dir = client.mkdirs(dir, true).await.expect("mkdirs through metadata");
    assert_eq!(created_dir.path(), dir);

    let first = Bytes::from(deterministic_bytes(1_337));
    let suffix = Bytes::from_static(b"-beryl-append-suffix");
    let expected = [first.as_ref(), suffix.as_ref()].concat();
    let create_options = CreateOptions::create().with_block_size(1024).with_chunk_size(1024);

    let mut writer = client
        .create(path, create_options)
        .await
        .expect("create through metadata");
    writer.write_all(first.clone()).await.expect("write through worker");
    writer.close().await.expect("close through metadata");
    cluster
        .converge_block_reports()
        .await
        .expect("block report convergence after create");

    let status = client.stat(path).await.expect("status after close");
    assert_eq!(status.path(), path);
    assert_eq!(status.attrs.size, first.len() as u64);

    let read = client
        .open(path)
        .await
        .expect("open after close")
        .read_all()
        .await
        .expect("read first bytes");
    assert_eq!(read, first);

    let mut appender = client.append(path).await.expect("append through metadata");
    appender
        .write_all(suffix.clone())
        .await
        .expect("append write through worker");
    appender.close().await.expect("append close through metadata");
    cluster
        .converge_block_reports()
        .await
        .expect("block report convergence after append");

    let read = client
        .open(path)
        .await
        .expect("open after append")
        .read_all()
        .await
        .expect("read appended bytes");
    assert_eq!(read.as_ref(), expected.as_slice());

    let listing = client
        .list(dir, ListOptions::default())
        .await
        .expect("non-recursive list");
    let file_entry = listing
        .entries
        .iter()
        .find(|entry| entry.name == "file")
        .expect("list includes file");
    assert_eq!(file_entry.kind, Some(InodeKind::File));
    assert_eq!(
        file_entry.attrs.as_ref().map(|attrs| attrs.size),
        Some(expected.len() as u64)
    );

    client
        .rename(path, renamed_path)
        .await
        .expect("rename through metadata");
    assert_not_found(client.stat(path).await, "old path after rename");

    let renamed_status = client.stat(renamed_path).await.expect("status after rename");
    assert_eq!(renamed_status.path(), renamed_path);
    assert_eq!(renamed_status.attrs.size, expected.len() as u64);

    let renamed_read = client
        .open(renamed_path)
        .await
        .expect("open renamed file")
        .read_all()
        .await
        .expect("read renamed file");
    assert_eq!(renamed_read.as_ref(), expected.as_slice());

    client
        .delete(renamed_path, false)
        .await
        .expect("namespace delete renamed file");
    assert_not_found(client.stat(renamed_path).await, "deleted path status");
    assert_not_found(client.open(renamed_path).await, "deleted path open");

    let listing = client
        .list(dir, ListOptions::default())
        .await
        .expect("list after delete");
    assert!(
        !listing.entries.iter().any(|entry| entry.name == "file.renamed"),
        "non-recursive list must not include deleted namespace entry"
    );

    cluster.shutdown().await.expect("local cluster shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn visibility_sync_then_continue_write_roundtrip() {
    let mut cluster = TestCluster::start().await.expect("start hermetic local cluster");
    let client = cluster.client();
    let path = "/sync-continue";
    let first = Bytes::from(vec![b'a'; 1024]);
    let second = Bytes::from(vec![b'b'; 1024]);

    let mut writer = client
        .create(
            path,
            CreateOptions::create().with_block_size(1024).with_chunk_size(1024),
        )
        .await
        .expect("create through metadata");
    writer.write_all(first.clone()).await.expect("write first block");
    writer
        .sync_write_visibility()
        .await
        .expect("publish first block while keeping session open");
    writer
        .write_all(second.clone())
        .await
        .expect("write after visibility sync");
    writer.close().await.expect("close after second block");
    cluster
        .converge_block_reports()
        .await
        .expect("converge both published block reports");

    let actual = client
        .open(path)
        .await
        .expect("open after close")
        .read_all()
        .await
        .expect("read both publication revisions");
    let expected = [first.as_ref(), second.as_ref()].concat();
    assert_eq!(actual.as_ref(), expected.as_slice());

    cluster.shutdown().await.expect("local cluster shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_more_than_ten_blocks_roundtrip() {
    let mut cluster = TestCluster::start().await.expect("start hermetic local cluster");
    let client = cluster.client();
    let path = "/many-blocks";
    let payload = Bytes::from(deterministic_bytes(12 * 1024 + 17));
    let mut writer = client
        .create(
            path,
            CreateOptions::create().with_block_size(1024).with_chunk_size(1024),
        )
        .await
        .expect("create file");

    writer
        .write_all(payload.clone())
        .await
        .expect("write more than ten blocks");
    writer.close().await.expect("close file");
    cluster.converge_block_reports().await.expect("converge block reports");

    let actual = client
        .open(path)
        .await
        .expect("open file")
        .read_all()
        .await
        .expect("read file");
    assert_eq!(actual, payload);
    cluster.shutdown().await.expect("local cluster shutdown");
}

fn assert_not_found<T: std::fmt::Debug>(result: ClientResult<T>, context: &str) {
    let err = result.expect_err(context);
    let message = err.to_string().to_ascii_lowercase();
    assert!(
        message.contains("not found") || message.contains("enoent"),
        "{context} should fail with not-found style error, got {err}"
    );
}
