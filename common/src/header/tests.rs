// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use crate::error::canonical::{CanonicalError, ErrorClass, ErrorCode, RefreshReason};
use crate::header::ClientInfo;
use crate::time::Deadline;
use crate::{CallerContext, CallerContextFields, RequestHeader, ResponseHeader, RpcErrorCode, RpcStatus};
use std::time::Duration;
use types::{ClientId, GroupName, GroupStateWatermark, RaftLogId};

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
    let watermark = GroupStateWatermark::new(GroupName::parse("root").unwrap(), RaftLogId::new(1, 2, 3));

    let request = RequestHeader::new(ClientId::new(1)).with_state(vec![watermark.clone()]);
    assert_eq!(request.state, vec![watermark.clone()]);

    let response = ResponseHeader::ok(ClientInfo::new(ClientId::new(1))).with_state(vec![watermark.clone()]);
    assert_eq!(response.state, vec![watermark]);
}

#[test]
fn caller_context_fields_parse_locality_hints_and_ignore_invalid_entries() {
    let context = CallerContext {
        context: "ip=10.0.0.1,host=worker-a,az=az-a,rack=rack-1,region=us-west".to_string(),
        signature: None,
    };
    let from_context = CallerContextFields::from_caller_context(&context);
    assert_eq!(from_context.ip(), Some("10.0.0.1"));
    assert_eq!(from_context.host(), Some("worker-a"));
    assert_eq!(from_context.az(), Some("az-a"));
    assert_eq!(from_context.rack(), Some("rack-1"));
    assert_eq!(from_context.region(), Some("us-west"));

    let cases = [
        ("", [None, None, None, None, None]),
        (
            "host=first,unknown=value,malformed,host=second,rack = rack-a, =empty-key,az",
            [None, Some("first"), None, Some("rack-a"), None],
        ),
        (
            " ip = 10.0.0.2 , az = az-b , region = us-east ",
            [Some("10.0.0.2"), None, Some("az-b"), None, Some("us-east")],
        ),
    ];

    for (raw, [ip, host, az, rack, region]) in cases {
        let fields = CallerContextFields::parse(raw);
        assert_eq!(fields.ip(), ip, "ip mismatch for {raw}");
        assert_eq!(fields.host(), host, "host mismatch for {raw}");
        assert_eq!(fields.az(), az, "az mismatch for {raw}");
        assert_eq!(fields.rack(), rack, "rack mismatch for {raw}");
        assert_eq!(fields.region(), region, "region mismatch for {raw}");
    }
}
