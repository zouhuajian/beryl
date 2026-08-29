// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Lazily paginated directory status iteration.

use std::sync::Arc;
use std::vec::IntoIter;

use super::{FileStatus, ListStatusOptions};
use crate::api::path::NamespacePathBuf;
use crate::client_inner::ClientInner;
use crate::error::{invalid_response, ClientResult};
use crate::metadata::ListStatusPage;

/// Asynchronous iterator over Metadata-authorized statuses in one directory.
///
/// Each call to [`Self::next`] returns a buffered status or fetches one bounded
/// Metadata page. Metadata retains no snapshot between pages, so concurrent
/// namespace changes can be reflected weakly consistently. Dropping the
/// iterator does not start or retain background work.
#[must_use = "directory entries are not fetched unless the iterator is consumed"]
pub struct ListStatusIterator {
    inner: Arc<ClientInner>,
    path: NamespacePathBuf,
    options: ListStatusOptions,
    cursor: Option<Vec<u8>>,
    buffered: IntoIter<FileStatus>,
    eof: bool,
}

impl ListStatusIterator {
    /// Creates an iterator from the first page already fetched by `FsClient`.
    pub(crate) fn new(
        inner: Arc<ClientInner>,
        path: NamespacePathBuf,
        options: ListStatusOptions,
        first_page: ListStatusPage,
    ) -> Self {
        Self {
            inner,
            path,
            options,
            cursor: first_page.next_cursor,
            buffered: first_page.entries.into_iter(),
            eof: first_page.eof,
        }
    }

    /// Returns the directory path being listed.
    pub fn path(&self) -> &str {
        self.path.as_str()
    }

    /// Returns the next child status, fetching another bounded page as needed.
    ///
    /// A failed page request leaves the continuation cursor and buffered state
    /// unchanged, so callers may decide whether to invoke `next` again.
    pub async fn next(&mut self) -> ClientResult<Option<FileStatus>> {
        loop {
            if let Some(status) = self.buffered.next() {
                return Ok(Some(status));
            }
            if self.eof {
                return Ok(None);
            }

            let page = self
                .inner
                .metadata
                .list_status_page(self.path.clone(), self.cursor.clone(), self.options.page_size)
                .await?;
            if page.next_cursor == self.cursor {
                return Err(invalid_response(
                    "ListStatus",
                    "non-EOF page did not advance next_cursor",
                ));
            }

            self.cursor = page.next_cursor;
            self.buffered = page.entries.into_iter();
            self.eof = page.eof;
        }
    }
}
