// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Public reader and writer handles.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::Mutex;
use types::fs::InodeId;
use types::ids::DataHandleId;

use super::fs_client::FsClient;
use crate::error::ClientResult;
use crate::session::write_session::WriteSession;

/// A reader for an immutable file snapshot opened through [`FsClient::open`].
#[derive(Clone)]
pub struct FileReader {
    client: FsClient,
    inner: ReadHandle,
}

impl FileReader {
    pub(crate) fn new(client: FsClient, inner: ReadHandle) -> Self {
        Self { client, inner }
    }

    /// Returns the namespace path used to open this file snapshot.
    pub fn path(&self) -> &str {
        self.inner.path()
    }

    /// Returns the file size observed when this reader was opened.
    pub fn size_hint(&self) -> u64 {
        self.inner.size_hint()
    }

    /// Reads a range from the file snapshot opened by [`FsClient::open`].
    pub async fn read_at(&self, offset: u64, len: u32) -> ClientResult<Bytes> {
        self.client.read_handle(&self.inner, offset, len).await
    }

    #[cfg(test)]
    pub(crate) fn inode_id(&self) -> InodeId {
        self.inner.inode_id()
    }

    #[cfg(test)]
    pub(crate) fn data_handle_id(&self) -> DataHandleId {
        self.inner.data_handle_id()
    }
}

impl fmt::Debug for FileReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileReader")
            .field("path", &self.path())
            .field("size_hint", &self.size_hint())
            .finish()
    }
}

/// A writer for a sequential write session created through [`FsClient::create`] or [`FsClient::append`].
pub struct FileWriter {
    client: FsClient,
    inner: WriteHandle,
}

impl FileWriter {
    pub(crate) fn new(client: FsClient, inner: WriteHandle) -> Self {
        Self { client, inner }
    }

    /// Returns the namespace path associated with this write session.
    pub fn path(&self) -> &str {
        self.inner.path()
    }

    /// Returns the next sequential write offset for this writer.
    pub fn cursor(&self) -> u64 {
        self.inner.write_cursor()
    }

    /// Writes all supplied bytes at the current sequential cursor.
    pub async fn write_all(&mut self, data: Bytes) -> ClientResult<()> {
        self.client.write_handle_all(&self.inner, data).await.map(|_| ())
    }

    /// Publishes the written prefix for visibility while keeping the writer open.
    pub async fn sync_write_visibility(&mut self) -> ClientResult<()> {
        self.client.sync_write_visibility_handle(&self.inner).await
    }

    /// Publishes the written prefix for durability while keeping the writer open.
    pub async fn sync_write_durability(&mut self) -> ClientResult<()> {
        self.client.sync_write_durability_handle(&self.inner).await
    }

    /// Renews the writer lease while keeping the write session open.
    pub async fn renew_lease(&mut self) -> ClientResult<()> {
        self.client.renew_lease_handle(&self.inner).await
    }

    /// Closes the writer and commits the final file metadata.
    pub async fn close(&mut self) -> ClientResult<()> {
        self.client.close_handle(&self.inner).await
    }

    /// Aborts this writer's open write session and reports cleanup failures.
    pub async fn abort(&mut self) -> ClientResult<()> {
        self.client.abort_handle(&self.inner).await
    }

    #[cfg(test)]
    pub(crate) fn write_session(&self) -> Option<Arc<Mutex<WriteSession>>> {
        Some(self.inner.write_session())
    }
}

impl fmt::Debug for FileWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileWriter")
            .field("path", &self.path())
            .field("cursor", &self.cursor())
            .finish()
    }
}

#[derive(Clone)]
pub(crate) struct ReadHandle {
    path: String,
    inode_id: InodeId,
    data_handle_id: DataHandleId,
    file_version: u64,
    file_size: u64,
}

impl ReadHandle {
    pub(crate) fn path(&self) -> &str {
        &self.path
    }

    pub(crate) fn size_hint(&self) -> u64 {
        self.file_size
    }

    pub(crate) fn new(
        path: String,
        inode_id: InodeId,
        data_handle_id: DataHandleId,
        file_version: u64,
        file_size: u64,
    ) -> Self {
        Self {
            path,
            inode_id,
            data_handle_id,
            file_version,
            file_size,
        }
    }

    pub(crate) fn inode_id(&self) -> InodeId {
        self.inode_id
    }

    pub(crate) fn data_handle_id(&self) -> DataHandleId {
        self.data_handle_id
    }

    pub(crate) fn file_version(&self) -> u64 {
        self.file_version
    }
}

impl fmt::Debug for ReadHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadHandle")
            .field("path", &self.path())
            .field("size_hint", &self.size_hint())
            .finish()
    }
}

pub(crate) struct WriteHandle {
    path: String,
    _inode_id: InodeId,
    data_handle_id: DataHandleId,
    write_session: Arc<Mutex<WriteSession>>,
    write_cursor: Arc<AtomicU64>,
}

impl WriteHandle {
    pub(crate) fn new(
        path: String,
        inode_id: InodeId,
        data_handle_id: DataHandleId,
        base_size: u64,
        session: WriteSession,
    ) -> Self {
        Self {
            path,
            _inode_id: inode_id,
            data_handle_id,
            write_session: Arc::new(Mutex::new(session)),
            write_cursor: Arc::new(AtomicU64::new(base_size)),
        }
    }

    pub(crate) fn path(&self) -> &str {
        &self.path
    }

    pub(crate) fn data_handle_id(&self) -> DataHandleId {
        self.data_handle_id
    }

    pub(crate) fn write_session(&self) -> Arc<Mutex<WriteSession>> {
        Arc::clone(&self.write_session)
    }

    pub(crate) fn write_cursor(&self) -> u64 {
        self.write_cursor.load(Ordering::SeqCst)
    }

    pub(crate) fn store_write_cursor(&self, cursor: u64) {
        self.write_cursor.store(cursor, Ordering::SeqCst);
    }
}

impl fmt::Debug for WriteHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WriteHandle")
            .field("path", &self.path())
            .field("cursor", &self.write_cursor())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ClientConfig;

    #[tokio::test]
    async fn file_reader_debug_redacts_identity_names() {
        let mut config = ClientConfig {
            metadata_endpoints: vec!["http://127.0.0.1:18080".to_string()],
            ..ClientConfig::default()
        };
        config.inner.inner.set("client.id", 7i64);
        let client = FsClient::try_new(config).expect("client");
        let read = FileReader::new(
            client,
            ReadHandle::new("/alpha".to_string(), InodeId::new(101), DataHandleId::new(202), 3, 10),
        );
        let debug = format!("{read:?}");

        assert!(debug.contains("FileReader"));
        assert!(debug.contains("size_hint"));
        assert_debug_redacts_internal_identity_names(&debug);
    }

    fn assert_debug_redacts_internal_identity_names(debug: &str) {
        for needle in [
            concat!("inode", "_id"),
            concat!("data", "_handle_id"),
            concat!("file", "_version"),
            concat!("write", "_handle"),
            concat!("fen", "cing"),
            concat!("route", "_epoch"),
            concat!("worker", "_epoch"),
            concat!("block", "_stamp"),
            concat!("call", "_id"),
            concat!("stream", "_id"),
        ] {
            assert!(
                !debug.contains(needle),
                "reader or writer Debug output must redact {needle}: {debug}"
            );
        }
    }
}
