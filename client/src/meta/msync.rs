// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Msync client wrapper.

use crate::error::{ClientError, ClientResult};
use crate::meta::MetadataClient;
use common::header::RequestHeader;
use types::fs::InodeId;

/// Msync client wrapper.
pub struct MsyncClient {
    /// Metadata client.
    client: MetadataClient,
}

impl MsyncClient {
    /// Create a new msync client.
    pub fn new(client: MetadataClient) -> Self {
        Self { client }
    }

    /// Sync metadata for a group (lightweight: advances group state watermark).
    /// group_id must be set in ctx.group_id.
    pub async fn sync(
        &self,
        ctx: &RequestHeader,
        _legacy_inode_id: Option<InodeId>,
        _legacy_path: Option<&str>,
        _legacy_min_token: Option<u64>,
        _timeout_ms: Option<u64>,
    ) -> ClientResult<proto::metadata::MsyncResponseProto> {
        if ctx.group_id.is_none() {
            return Err(ClientError::Metadata(
                "MsyncRequestProto requires ctx.group_id to be set".to_string(),
            ));
        }

        self.client.msync(ctx).await
    }
}
