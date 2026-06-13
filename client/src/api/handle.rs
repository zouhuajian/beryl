// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Public reader and writer handles.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::Mutex;
use types::{DataHandleId, FileLayout, InodeId};

use super::fs_client::FsClient;
use crate::error::{ClientError, ClientResult};
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

    pub(crate) fn from_open_response(
        path: &str,
        response: proto::metadata::OpenFileResponseProto,
    ) -> ClientResult<Self> {
        let Some(inode_id) = response.inode_id else {
            return Err(ClientError::Metadata(
                "OpenFileResponseProto.inode_id missing".to_string(),
            ));
        };
        let Some(data_handle_id) = response.data_handle_id else {
            return Err(ClientError::Metadata(
                "OpenFileResponseProto.data_handle_id missing".to_string(),
            ));
        };
        let Some(file_version) = response.file_version else {
            return Err(ClientError::Metadata(
                "OpenFileResponseProto.file_version missing".to_string(),
            ));
        };

        Ok(Self::new(
            path.to_string(),
            InodeId::new(inode_id.value),
            DataHandleId::new(data_handle_id.value),
            file_version,
            response.file_size,
        ))
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

    pub(crate) fn from_create_response(
        path: &str,
        response: proto::metadata::CreateFileResponseProto,
    ) -> ClientResult<Self> {
        let Some(inode_id) = response.inode_id else {
            return Err(ClientError::Metadata(
                "CreateFileResponseProto.inode_id missing".to_string(),
            ));
        };
        let Some(data_handle_id) = response.data_handle_id else {
            return Err(ClientError::Metadata(
                "CreateFileResponseProto.data_handle_id missing".to_string(),
            ));
        };
        let Some(layout) = response.layout else {
            return Err(ClientError::Metadata(
                "CreateFileResponseProto.layout missing".to_string(),
            ));
        };
        let layout = FileLayout::try_from(layout)
            .map_err(|err| ClientError::InvalidLayout(format!("CreateFileResponseProto.layout invalid: {err}")))?;
        let Some(write_handle) = response.write_handle else {
            return Err(ClientError::Metadata(
                "CreateFileResponseProto.write_handle missing".to_string(),
            ));
        };

        let inode_id = InodeId::new(inode_id.value);
        let data_handle_id = DataHandleId::new(data_handle_id.value);
        let session = WriteSession::new(
            path.to_string(),
            inode_id,
            data_handle_id,
            layout,
            write_handle,
            response.base_size,
        )?;
        Ok(Self::new(
            path.to_string(),
            inode_id,
            data_handle_id,
            response.base_size,
            session,
        ))
    }

    pub(crate) fn from_append_response(
        path: &str,
        response: proto::metadata::AppendFileResponseProto,
    ) -> ClientResult<Self> {
        let Some(inode_id) = response.inode_id else {
            return Err(ClientError::Metadata(
                "AppendFileResponseProto.inode_id missing".to_string(),
            ));
        };
        let Some(data_handle_id) = response.data_handle_id else {
            return Err(ClientError::Metadata(
                "AppendFileResponseProto.data_handle_id missing".to_string(),
            ));
        };
        let Some(layout) = response.layout else {
            return Err(ClientError::Metadata(
                "AppendFileResponseProto.layout missing".to_string(),
            ));
        };
        let layout = FileLayout::try_from(layout)
            .map_err(|err| ClientError::InvalidLayout(format!("AppendFileResponseProto.layout invalid: {err}")))?;
        let Some(write_handle) = response.write_handle else {
            return Err(ClientError::Metadata(
                "AppendFileResponseProto.write_handle missing".to_string(),
            ));
        };

        let inode_id = InodeId::new(inode_id.value);
        let data_handle_id = DataHandleId::new(data_handle_id.value);
        let session = WriteSession::new(
            path.to_string(),
            inode_id,
            data_handle_id,
            layout,
            write_handle,
            response.base_size,
        )?;
        Ok(Self::new(
            path.to_string(),
            inode_id,
            data_handle_id,
            response.base_size,
            session,
        ))
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
    use crate::error::ClientError;
    use proto::common::{DataHandleIdProto, FileLayoutProto};
    use proto::fs::InodeIdProto;
    use proto::metadata::{CreateFileResponseProto, OpenFileResponseProto, WriteHandleProto};

    #[tokio::test]
    async fn file_reader_debug_redacts_identity_names() {
        let config = ClientConfig {
            metadata_endpoints: vec!["http://127.0.0.1:18080".to_string()],
            ..ClientConfig::default()
        };
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

    #[test]
    fn read_handle_from_open_response_requires_inode_id() {
        let err = ReadHandle::from_open_response(
            "/alpha",
            OpenFileResponseProto {
                inode_id: None,
                data_handle_id: Some(DataHandleIdProto { value: 202 }),
                file_version: Some(3),
                file_size: 10,
                ..OpenFileResponseProto::default()
            },
        )
        .expect_err("missing inode_id must fail");

        assert!(
            matches!(&err, ClientError::Metadata(msg) if msg.contains("OpenFileResponseProto.inode_id missing")),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn write_handle_from_create_response_builds_session() {
        let handle = WriteHandle::from_create_response(
            "/created",
            CreateFileResponseProto {
                inode_id: Some(InodeIdProto { value: 301 }),
                data_handle_id: Some(DataHandleIdProto { value: 302 }),
                write_handle: Some(write_handle_proto(1, 302)),
                base_size: 8,
                layout: Some(layout_proto()),
                ..CreateFileResponseProto::default()
            },
        )
        .expect("write handle");

        assert_eq!(handle.path(), "/created");
        assert_eq!(handle.data_handle_id(), DataHandleId::new(302));
        assert_eq!(handle.write_cursor(), 8);
    }

    fn assert_debug_redacts_internal_identity_names(debug: &str) {
        for needle in [
            concat!("inode", "_id"),
            concat!("data", "_handle_id"),
            concat!("file", "_version"),
            concat!("write", "_handle"),
            concat!("fen", "cing"),
            concat!("route", "_epoch"),
            concat!("worker", "_run_id"),
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

    fn write_handle_proto(handle_id: u64, data_handle_id: u64) -> WriteHandleProto {
        WriteHandleProto {
            handle_id,
            lease_id: Some(proto::common::LeaseIdProto {
                high: 0,
                low: handle_id,
            }),
            lease_epoch: 1,
            open_epoch: 1,
            fencing_token: Some(proto::common::FencingTokenProto {
                block_id: Some(proto::common::BlockIdProto {
                    data_handle_id,
                    block_index: 0,
                }),
                owner: Some(types::ClientId::new(7).into()),
                epoch: 1,
            }),
        }
    }

    fn layout_proto() -> FileLayoutProto {
        FileLayoutProto {
            block_size: 64 * 1024 * 1024,
            chunk_size: 4 * 1024 * 1024,
            replication: 1,
            block_format_id: types::BlockFormatId::CURRENT_FOR_NEW_FILE.as_raw(),
        }
    }
}
