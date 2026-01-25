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

    /// Sync metadata for a group (lightweight: only advances state_id).
    /// group_id must be set in ctx.group_id.
    /// min_state_id (if provided) should be set in ctx.state_id.
    pub async fn sync(
        &self,
        ctx: &RequestHeader,
        _inode_id: Option<InodeId>, // Deprecated: use ctx.group_id instead
        _path: Option<&str>,        // Deprecated: use ctx.group_id instead
        _min_token: Option<u64>,    // Deprecated: use ctx.state_id instead
        _timeout_ms: Option<u64>,   // Deprecated: use ctx.deadline_ms instead
    ) -> ClientResult<proto::metadata::MsyncResponseProto> {
        // Check that group_id is set in ctx
        if ctx.group_id.is_none() {
            return Err(ClientError::Metadata(
                "MsyncRequestProto requires ctx.group_id to be set".to_string(),
            ));
        }

        // Use MetadataClient's msync method
        self.client.msync(ctx, false).await
    }
}
