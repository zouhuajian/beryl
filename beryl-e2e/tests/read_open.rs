// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use beryl_client::ClientErrorKind;
use beryl_e2e::{data::deterministic_bytes, TestCluster};
use bytes::Bytes;
use futures::io::{AsyncReadExt, AsyncSeekExt};
use std::io::SeekFrom;
use tokio_util::compat::FuturesAsyncReadCompatExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn open_status_and_reader_layouts_preserve_inode_state_across_reads_and_restart() {
    let mut cluster = TestCluster::start().await.unwrap();
    let client = cluster.client().clone();
    let path = "/read-open";
    let payload = Bytes::from(deterministic_bytes(3500));
    let mut writer = client.create(path).await.unwrap();
    writer.write_all(payload.clone()).await.unwrap();
    writer.close().await.unwrap();

    let status = client.get_status(path).await.unwrap();
    let mut reader = client.open(path).await.unwrap();
    assert_eq!(reader.status(), &status);
    let (middle, tail) = tokio::join!(reader.read_range(1900..2700), reader.read_range(3000..));
    assert_eq!(middle.unwrap(), payload[1900..2700]);
    assert_eq!(tail.unwrap(), payload[3000..]);
    assert_eq!(reader.position(), 0);
    let mut sequential = Vec::new();
    reader.read_to_end(&mut sequential).await.unwrap();
    assert_eq!(sequential, payload);
    assert_eq!(
        reader.seek(SeekFrom::End(-16)).await.unwrap(),
        payload.len() as u64 - 16
    );
    let mut last = [0; 16];
    reader.read_exact(&mut last).await.unwrap();
    assert_eq!(&last, &payload[payload.len() - 16..]);
    reader.seek(SeekFrom::Start(0)).await.unwrap();
    let mut copied = Vec::new();
    assert_eq!(
        futures::io::copy(&mut reader, &mut copied).await.unwrap(),
        payload.len() as u64
    );
    assert_eq!(copied, payload);

    let mut compatible = client.open(path).await.unwrap().compat();
    tokio::io::AsyncSeekExt::seek(&mut compatible, SeekFrom::End(-16))
        .await
        .unwrap();
    let mut tail = [0; 16];
    tokio::io::AsyncReadExt::read_exact(&mut compatible, &mut tail)
        .await
        .unwrap();
    assert_eq!(&tail, &payload[payload.len() - 16..]);
    tokio::io::AsyncSeekExt::seek(&mut compatible, SeekFrom::Start(0))
        .await
        .unwrap();
    let mut copied = Vec::new();
    assert_eq!(
        tokio::io::copy(&mut compatible, &mut copied).await.unwrap(),
        payload.len() as u64
    );
    assert_eq!(copied, payload);

    let stale = client.open(path).await.unwrap();
    let mut appender = client.append(path).await.unwrap();
    appender.write_all(Bytes::from_static(b"suffix")).await.unwrap();
    appender.close().await.unwrap();
    let error = stale.read_range(0..1).await.unwrap_err();
    assert_eq!(error.kind(), ClientErrorKind::StaleHandle);
    assert_eq!(stale.status(), &status);

    let mut cached = client.open(path).await.unwrap();
    cached.read_range(0..1).await.unwrap();
    cluster.restart_worker_until_heartbeat().await.unwrap();
    // Status-only open remains available while Worker locations are rebuilding.
    let lazy = client.open(path).await.unwrap();
    assert_eq!(lazy.len(), payload.len() as u64 + 6);
    cluster.converge_block_reports().await.unwrap();
    let mut first = [0; 16];
    assert_eq!(cached.read(&mut first).await.unwrap(), first.len());
    assert_eq!(&first, &payload[..16]);

    let mut empty_writer = client.create("/empty").await.unwrap();
    empty_writer.close().await.unwrap();
    let mut empty = client.open("/empty").await.unwrap();
    assert!(empty.is_empty());
    assert_eq!(empty.read(&mut first).await.unwrap(), 0);
    cluster.shutdown().await.unwrap();
}
