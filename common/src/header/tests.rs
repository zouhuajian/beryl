// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use crate::error::canonical::{CanonicalError, ErrorClass, ErrorCode, RefreshReason};
use crate::header::ClientInfo;
use crate::time::Deadline;
use crate::{RequestHeader, ResponseHeader, RpcErrorCode, RpcStatus};
use std::time::Duration;
use types::{ClientId, GroupStateWatermark, RaftLogId, ShardGroupId};

#[test]
fn test_client_info() {
    let client_id = ClientId::new(123);
    let client_info = ClientInfo::new(client_id);

    assert_eq!(client_info.client_id.as_raw(), 123);
    assert!(client_info.client_name.is_none());

    let client_info_with_name = client_info.with_client_name("test-client".to_string());
    assert_eq!(client_info_with_name.client_name, Some("test-client".to_string()));
}

#[test]
fn test_request_header_with_client_info() {
    let client_id = ClientId::new(456);
    let deadline = Deadline::from_now(Duration::from_secs(60));
    let header = RequestHeader::with_deadline(client_id, deadline);

    assert_eq!(header.client.client_id.as_raw(), 456);
    assert!(!header.deadline.has_passed());

    let child = header.child();
    assert_ne!(child.client.call_id, header.client.call_id);
    assert_eq!(child.client.client_id, header.client.client_id);

    let child_same_id = header.child_with_same_call_id();
    assert_eq!(child_same_id.client.call_id, header.client.call_id);
    assert_eq!(child_same_id.retry_count, header.retry_count + 1);
}

#[test]
fn test_response_header_with_client_info() {
    let client = ClientInfo::new(ClientId::new(789));
    let resp = ResponseHeader::ok(client.clone());

    assert_eq!(resp.client.client_id.as_raw(), 789);
    assert_eq!(resp.status, RpcStatus::Ok);
    assert!(resp.canonical_error.is_none());

    let canonical = CanonicalError {
        class: ErrorClass::Retryable,
        code: Some(ErrorCode::RpcCode(RpcErrorCode::NodeUnavailable)),
        reason: None,
        retry_after_ms: Some(1000),
        message: "Test error".to_string(),
        refresh_hint: None,
    };
    let resp_error = ResponseHeader::error(client, canonical);
    assert_eq!(resp_error.status, RpcStatus::Error);
    assert_eq!(
        resp_error.canonical_error.as_ref().and_then(|c| c.retry_after_ms),
        Some(1000)
    );
    assert!(matches!(
        resp_error.canonical_error.as_ref().and_then(|c| c.reason),
        None | Some(RefreshReason::Unknown)
    ));
}

#[test]
fn request_and_response_headers_use_group_state_vectors() {
    let watermark = GroupStateWatermark::new(ShardGroupId::new(7), RaftLogId::new(1, 2, 3));

    let request = RequestHeader::new(ClientId::new(1)).with_state(vec![watermark]);
    assert_eq!(request.state, vec![watermark]);

    let response = ResponseHeader::ok(ClientInfo::new(ClientId::new(1))).with_state(vec![watermark]);
    assert_eq!(response.state, vec![watermark]);
}
