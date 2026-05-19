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
    /// Path used to open the handle.
    pub path: String,
    /// Authoritative inode identity.
    pub inode_id: InodeId,
    /// Data-plane data instance identity.
    pub data_handle_id: DataHandleId,
    /// File version observed when the handle was opened.
    pub file_version: Option<u64>,
    /// File size observed when the handle was opened.
    pub file_size: u64,
    write_session: Option<Arc<Mutex<WriteSession>>>,
}

impl FileHandle {
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
}

impl fmt::Debug for FileHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileHandle")
            .field("path", &self.path)
            .field("inode_id", &self.inode_id)
            .field("data_handle_id", &self.data_handle_id)
            .field("file_version", &self.file_version)
            .field("file_size", &self.file_size)
            .field("is_write", &self.write_session.is_some())
            .finish()
    }
}
