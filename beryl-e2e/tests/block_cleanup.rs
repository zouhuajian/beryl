// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use beryl_client::{ClientResult, DeleteOptions};
use beryl_e2e::{data::deterministic_bytes, TestCluster};
use bytes::Bytes;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_cleanup_commands_remove_only_deleted_file_blocks() {
    let mut cluster = TestCluster::start_with_cleanup_page_size(1)
        .await
        .expect("start cleanup-enabled cluster with one replica per scan");
    cluster
        .start_metadata_process(std::path::Path::new(env!("CARGO_BIN_EXE_metadata-e2e-server")))
        .await
        .expect("start full metadata runtime with maintenance");
    let client: beryl_client::FsClient = cluster.client().clone();
    client.mkdirs("/cleanup").await.expect("create cleanup directory");

    let first = Bytes::from(deterministic_bytes(513));
    let second = Bytes::from(deterministic_bytes(777));
    write_file(&client, "/cleanup/first", first.clone()).await;
    write_file(&client, "/cleanup/second", second.clone()).await;
    cluster.stop_background_block_reports().await;
    cluster
        .converge_block_reports()
        .await
        .expect("publish initial block reports");
    assert_eq!(cluster.ready_block_count().expect("ready block count"), 2);
    assert_eq!(cluster.physical_block_count().expect("physical block count"), 2);

    client
        .delete("/cleanup/first")
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
        .read_to_end()
        .await
        .expect("read remaining file");
    assert_eq!(remaining, second);

    client
        .delete_with_options("/cleanup", DeleteOptions { recursive: true })
        .await
        .expect("recursively delete remaining namespace");
    cluster
        .converge_cleanup(0)
        .await
        .expect("reclaim recursive delete blocks");
    assert_eq!(cluster.physical_block_count().expect("physical block count"), 0);
    cluster.shutdown().await.expect("shutdown cleanup-enabled cluster");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recursive_delete_stays_hidden_and_reclaims_after_metadata_restart() {
    let mut cluster = TestCluster::start_with_cleanup()
        .await
        .expect("start cleanup-enabled cluster");
    let metadata_executable = std::path::Path::new(env!("CARGO_BIN_EXE_metadata-e2e-server"));
    cluster
        .start_metadata_process(metadata_executable)
        .await
        .expect("start full metadata runtime with maintenance");
    let client: beryl_client::FsClient = cluster.client().clone();
    client
        .mkdirs("/restart-delete")
        .await
        .expect("create recursive-delete root");
    write_file(&client, "/restart-delete/file", Bytes::from(deterministic_bytes(913))).await;
    cluster.stop_background_block_reports().await;
    cluster
        .converge_block_reports()
        .await
        .expect("publish block before recursive delete");
    assert_eq!(cluster.physical_block_count().expect("physical block count"), 1);

    client
        .delete_with_options("/restart-delete", DeleteOptions { recursive: true })
        .await
        .expect("atomically detach recursive-delete root");
    assert_not_found(
        client.get_status("/restart-delete").await,
        "detached path before metadata restart",
    );

    cluster
        .restart_metadata_process(metadata_executable)
        .await
        .expect("restart full metadata runtime after detach");
    assert_not_found(
        client.get_status("/restart-delete").await,
        "detached path after metadata restart",
    );
    cluster
        .converge_block_reports()
        .await
        .expect("rebuild full block-report baseline after metadata restart");
    cluster
        .converge_cleanup(0)
        .await
        .expect("resume namespace and physical reclamation after restart");
    cluster.shutdown().await.expect("shutdown cleanup-enabled cluster");
}

async fn write_file(client: &beryl_client::FsClient, path: &str, payload: Bytes) {
    let mut writer = client.create(path).await.expect("create cleanup test file");
    writer.write_all(payload).await.expect("write cleanup test file");
    writer.close().await.expect("publish cleanup test file");
}

fn assert_not_found<T: std::fmt::Debug>(result: ClientResult<T>, context: &str) {
    let error = result.expect_err(context);
    let message = error.to_string().to_ascii_lowercase();
    assert!(
        message.contains("not found") || message.contains("enoent"),
        "{context} should fail with not-found style error, got {error}"
    );
}
