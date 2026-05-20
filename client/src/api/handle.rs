// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Public file handle type.

use std::fmt;
use std::sync::Arc;

use tokio::sync::Mutex;
use types::fs::InodeId;
use types::ids::DataHandleId;

use crate::session::write_session::WriteSession;

/// Public filesystem file handle.
#[derive(Clone)]
pub struct FileHandle {
    path: String,
    inode_id: InodeId,
    data_handle_id: DataHandleId,
    file_version: Option<u64>,
    file_size: u64,
    write_session: Option<Arc<Mutex<WriteSession>>>,
}

impl FileHandle {
    /// Return the namespace path used to open this handle.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Return the file size observed when the handle was opened.
    pub fn size_hint(&self) -> u64 {
        self.file_size
    }

    /// Return whether this handle owns an open write session.
    pub fn is_write(&self) -> bool {
        self.write_session.is_some()
    }

    /// Create an internal read handle.
    pub(crate) fn read(
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
            file_version: Some(file_version),
            file_size,
            write_session: None,
        }
    }

    /// Create an internal write handle.
    pub(crate) fn write(
        path: String,
        inode_id: InodeId,
        data_handle_id: DataHandleId,
        base_size: u64,
        session: WriteSession,
    ) -> Self {
        Self {
            path,
            inode_id,
            data_handle_id,
            file_version: None,
            file_size: base_size,
            write_session: Some(Arc::new(Mutex::new(session))),
        }
    }

    /// Return the internal write session, when this is a write handle.
    pub(crate) fn write_session(&self) -> Option<Arc<Mutex<WriteSession>>> {
        self.write_session.clone()
    }

    /// Return the sealed inode identity for internal read planning.
    pub(crate) fn inode_id(&self) -> InodeId {
        self.inode_id
    }

    /// Return the sealed data identity for internal read planning.
    pub(crate) fn data_handle_id(&self) -> DataHandleId {
        self.data_handle_id
    }

    /// Return the sealed file version for internal read planning.
    pub(crate) fn file_version(&self) -> Option<u64> {
        self.file_version
    }
}

impl fmt::Debug for FileHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileHandle")
            .field("path", &self.path())
            .field("size_hint", &self.size_hint())
            .field("is_write", &self.is_write())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_handle_debug_redacts_internal_identity_names() {
        let read = FileHandle::read("/alpha".to_string(), InodeId::new(101), DataHandleId::new(202), 3, 10);

        assert_debug_redacts_internal_identity_names(&format!("{read:?}"));
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
                "FileHandle Debug output must redact {needle}: {debug}"
            );
        }
    }
}
