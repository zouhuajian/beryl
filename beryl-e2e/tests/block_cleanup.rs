// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use beryl_client::{CreateOptions, DeleteOptions};
use beryl_e2e::{data::deterministic_bytes, TestCluster};
use bytes::Bytes;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_cleanup_commands_remove_only_deleted_file_blocks() {
    let mut cluster = TestCluster::start_with_cleanup()
        .await
        .expect("start cleanup-enabled cluster");
    cluster
        .start_metadata_process(std::path::Path::new(env!("CARGO_BIN_EXE_metadata-e2e-server")))
        .await
        .expect("start full metadata runtime with maintenance");
    let client = cluster.client();
    client.mkdirs("/cleanup", true).await.expect("create cleanup directory");

    let first = Bytes::from(deterministic_bytes(513));
    let second = Bytes::from(deterministic_bytes(777));
    write_file(client, "/cleanup/first", first.clone()).await;
    write_file(client, "/cleanup/second", second.clone()).await;
    cluster
        .converge_block_reports()
        .await
        .expect("publish initial block reports");
    assert_eq!(cluster.ready_block_count().expect("ready block count"), 2);
    assert_eq!(cluster.physical_block_count().expect("physical block count"), 2);

    client
        .delete("/cleanup/first", DeleteOptions::default())
        .await
        .expect("delete first namespace entry");
    cluster
        .converge_cleanup(1)
        .await
        .expect("reclaim only the deleted file block");
    assert_eq!(cluster.physical_block_count().expect("physical block count"), 1);

    let remaining = client
        .open("/cleanup/second")
        .await
        .expect("open remaining file")
        .read_all()
        .await
        .expect("read remaining file");
    assert_eq!(remaining, second);

    client
        .delete("/cleanup", DeleteOptions { recursive: true })
        .await
        .expect("recursively delete remaining namespace");
    cluster
        .converge_cleanup(0)
        .await
        .expect("reclaim recursive delete blocks");
    assert_eq!(cluster.physical_block_count().expect("physical block count"), 0);
    cluster.shutdown().await.expect("shutdown cleanup-enabled cluster");
}

async fn write_file(client: &beryl_client::FsClient, path: &str, payload: Bytes) {
    let mut writer = client
        .create(
            path,
            CreateOptions::create().with_block_size(1024).with_chunk_size(1024),
        )
        .await
        .expect("create cleanup test file");
    writer.write_all(payload).await.expect("write cleanup test file");
    writer.close().await.expect("publish cleanup test file");
}
