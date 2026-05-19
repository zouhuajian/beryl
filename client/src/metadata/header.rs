// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata request/response header helpers.

use crate::error::ClientResult;
use crate::runtime::AttemptContext;

/// Replace an optional request header with the current attempt header.
pub fn ensure_metadata_header(
    header: &mut Option<proto::common::RequestHeaderProto>,
    ctx: &AttemptContext,
) -> ClientResult<()> {
    *header = Some(ctx.metadata_header()?);
    Ok(())
}
