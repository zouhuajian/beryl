// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Internal namespace path value used after public API validation.

use crate::error::{ClientError, ClientResult};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct NamespacePathBuf(String);

impl NamespacePathBuf {
    pub(crate) fn parse(raw: impl Into<String>) -> ClientResult<Self> {
        let raw = raw.into();
        if raw.is_empty() {
            Err(ClientError::InvalidArgument("path must not be empty".to_string()))
        } else {
            Ok(Self(raw))
        }
    }

    pub(crate) fn into_string(self) -> String {
        self.0
    }
}
