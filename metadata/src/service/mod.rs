// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata service implementation.
//!
//! Implements MetadataClientService RPC handlers with proper context propagation
//! and error handling.

mod error_helpers;
mod fs_service;
pub mod guard;
mod path_service;

// pub use client_service::MetadataClientServiceImpl;
pub use error_helpers::{
    fatal_fs_header, header_from_canonical_error, need_refresh_header, ok_header_from_request, retryable_header,
};
pub use fs_service::{FsWriteOp, MetadataFsServiceImpl, RoutedFsWriteCtx};
pub use guard::{AuthzContext, AuthzOp, GuardChain, GuardSpec, LeadershipChecker};
pub use path_service::MetadataPathServiceImpl;

use common::header::RequestHeader;
use tracing::Span;

/// Extract RequestHeader from proto RequestHeaderProto and inject into tracing span.
pub fn extract_and_inject_context(req_header: &Option<proto::common::RequestHeaderProto>) -> RequestHeader {
    let header = if let Some(proto_header) = req_header {
        RequestHeader::try_from(proto_header.clone()).unwrap_or_else(|e| {
            tracing::warn!(error = %e, "Failed to parse RequestHeaderProto, using default");
            RequestHeader::new(types::ClientId::new(0))
        })
    } else {
        RequestHeader::new(types::ClientId::new(0))
    };

    // Inject into tracing span
    Span::current().record("call_id", &header.client.call_id.to_string());
    Span::current().record("client_id", &header.client.client_id.as_raw());
    if let Some(ref client_name) = header.client.client_name {
        Span::current().record("client_name", client_name);
    }
    if let Some(traceparent) = &header.traceparent {
        Span::current().record("traceparent", traceparent);
    }
    if let Some(ref state_id) = header.state_id {
        Span::current().record("state_id", &format!("{:?}", state_id));
    }

    header
}
