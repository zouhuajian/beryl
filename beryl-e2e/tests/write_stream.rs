// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use beryl_e2e::TestCluster;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_flush_sync_and_close_preserve_publication_boundaries() {
    let mut cluster = TestCluster::start().await.unwrap();
    let client = cluster.client();
    let path = "/stream-write";
    let mut writer = client.create(path).await.unwrap();
    let prefix = vec![b'a'; 317];
    writer.write_all(&prefix).await.unwrap();
    writer.flush().await.unwrap();
    assert_eq!(writer.position(), prefix.len() as u64);
    assert_eq!(client.get_status(path).await.unwrap().len(), 0);
    assert!(client
        .open(path)
        .await
        .unwrap()
        .read_range(..)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(cluster.physical_block_count().unwrap(), 1);

    let suffix = vec![b'b'; 1024];
    writer.write_all(&suffix).await.unwrap();
    writer.flush().await.unwrap();
    writer.flush().await.unwrap();
    assert_eq!(client.get_status(path).await.unwrap().len(), 0);
    writer.sync().await.unwrap();
    let published = [prefix.as_slice(), suffix.as_slice()].concat();
    assert_eq!(
        client.open(path).await.unwrap().read_range(..).await.unwrap().as_ref(),
        published
    );

    writer.write_all(b"tail").await.unwrap();
    writer.flush().await.unwrap();
    assert_eq!(client.get_status(path).await.unwrap().len(), published.len() as u64);
    writer.close().await.unwrap();
    writer.close().await.unwrap();
    let expected = [published.as_slice(), b"tail"].concat();
    assert_eq!(
        client.open(path).await.unwrap().read_range(..).await.unwrap().as_ref(),
        expected
    );
    assert_eq!(cluster.physical_block_count().unwrap(), 2);
    assert!(writer.write(b"closed").await.is_err());
    client.append(path).await.unwrap().close().await.unwrap();
    cluster.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abort_preserves_published_prefix_after_worker_only_flush() {
    let mut cluster = TestCluster::start().await.unwrap();
    let client = cluster.client();
    let path = "/abort-flushed-tail";
    let mut writer = client.create(path).await.unwrap();
    writer.write_all(b"published").await.unwrap();
    writer.sync().await.unwrap();
    writer.write_all(b"discarded").await.unwrap();
    writer.flush().await.unwrap();
    assert_eq!(writer.position(), 18);
    writer.abort().await.unwrap();
    assert_eq!(
        client.open(path).await.unwrap().read_range(..).await.unwrap().as_ref(),
        b"published"
    );
    let mut appender = client.append(path).await.unwrap();
    assert_eq!(appender.position(), 9);
    appender.write_all(b"-new").await.unwrap();
    appender.close().await.unwrap();
    assert_eq!(
        client.open(path).await.unwrap().read_range(..).await.unwrap().as_ref(),
        b"published-new"
    );
    assert_eq!(cluster.physical_block_count().unwrap(), 1);
    cluster.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_flush_does_not_allocate_a_block_and_close_ends_the_lease() {
    let mut cluster = TestCluster::start().await.unwrap();
    let client = cluster.client();
    let mut writer = client.create("/empty-stream").await.unwrap();
    writer.write_all(&[]).await.unwrap();
    writer.flush().await.unwrap();
    assert_eq!(writer.position(), 0);
    assert_eq!(cluster.physical_block_count().unwrap(), 0);
    writer.close().await.unwrap();
    client.append("/empty-stream").await.unwrap().close().await.unwrap();
    assert_eq!(client.get_status("/empty-stream").await.unwrap().len(), 0);
    cluster.shutdown().await.unwrap();
}
