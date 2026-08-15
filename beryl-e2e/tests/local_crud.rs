// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use beryl_client::{ClientResult, CreateOptions, DeleteOptions, InodeKind, ListOptions};
use beryl_e2e::{data::deterministic_bytes, TestCluster};
use bytes::Bytes;
use futures::TryStreamExt;

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

    let read = client
        .open(path)
        .await
        .expect("open after append")
        .read_all()
        .await
        .expect("read appended bytes");
    assert_eq!(read.as_ref(), expected.as_slice());

    let subdir = "/e2e/subdir";
    client.mkdirs(subdir, false).await.expect("create second listing entry");

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

    let first_page = client
        .list(
            dir,
            ListOptions {
                limit: Some(1),
                ..Default::default()
            },
        )
        .await
        .expect("first bounded listing page");
    assert_eq!(first_page.entries.len(), 1);
    assert!(!first_page.eof);
    let second_page = client
        .list(
            dir,
            ListOptions {
                cursor: first_page.next_cursor.clone(),
                limit: Some(1),
                ..Default::default()
            },
        )
        .await
        .expect("second bounded listing page");
    assert_eq!(second_page.entries.len(), 1);
    assert!(second_page.eof);
    assert!(second_page.next_cursor.is_none());
    let mut paged_names = first_page
        .entries
        .into_iter()
        .chain(second_page.entries)
        .map(|entry| entry.name)
        .collect::<Vec<_>>();
    paged_names.sort();
    assert_eq!(paged_names, vec!["file", "subdir"]);

    let mut streamed_names = client
        .list_stream(
            dir,
            ListOptions {
                limit: Some(1),
                ..Default::default()
            },
        )
        .expect("create automatic listing stream")
        .map_ok(|entry| entry.name)
        .try_collect::<Vec<_>>()
        .await
        .expect("automatically paginate directory listing");
    streamed_names.sort();
    assert_eq!(streamed_names, vec!["file", "subdir"]);

    client
        .delete(subdir, DeleteOptions::default())
        .await
        .expect("delete empty listing subdirectory");

    let reader_opened_before_rename = client.open(path).await.expect("open reader before rename");
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
    let moved_reader_bytes = reader_opened_before_rename
        .read_all()
        .await
        .expect("reader opened before rename remains bound to the inode");
    assert_eq!(moved_reader_bytes.as_ref(), expected.as_slice());

    client
        .delete(renamed_path, DeleteOptions::default())
        .await
        .expect("namespace delete renamed file");
    assert_not_found(client.stat(renamed_path).await, "deleted path status");
    assert_not_found(client.open(renamed_path).await, "deleted path open");
    assert_not_found(reader_opened_before_rename.read_all().await, "reader for deleted inode");

    let replacement = Bytes::from_static(b"replacement-file");
    let mut replacement_writer = client
        .create(
            renamed_path,
            CreateOptions::create().with_block_size(1024).with_chunk_size(1024),
        )
        .await
        .expect("recreate deleted path");
    replacement_writer
        .write_all(replacement.clone())
        .await
        .expect("write replacement file");
    replacement_writer
        .close()
        .await
        .unwrap_or_else(|err| panic!("close replacement file: {err} ({err:?})"));
    assert_not_found(
        reader_opened_before_rename.read_all().await,
        "old reader must not bind to recreated path",
    );
    let replacement_read = client
        .open(renamed_path)
        .await
        .expect("open replacement file")
        .read_all()
        .await
        .expect("read replacement file");
    assert_eq!(replacement_read, replacement);
    client
        .delete(renamed_path, DeleteOptions::default())
        .await
        .expect("delete replacement file");

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
    let first = Bytes::from(vec![b'a'; 317]);
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
    let visible_prefix = client
        .open(path)
        .await
        .expect("open immediately after visibility sync")
        .read_all()
        .await
        .expect("read published prefix while writer remains open");
    assert_eq!(visible_prefix, first);

    writer
        .write_all(second.clone())
        .await
        .expect("write after visibility sync");
    writer.close().await.expect("close after second block");

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

    for offset in (0..payload.len()).step_by(127) {
        let end = (offset + 127).min(payload.len());
        writer
            .write_all(payload.slice(offset..end))
            .await
            .expect("write small frame across more than ten blocks");
    }
    writer.close().await.expect("close file");

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
