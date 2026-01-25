// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

#[cfg(test)]
mod tests {
    use crate::error::canonical::{CanonicalError, ErrorClass, ErrorCode, RefreshReason};
    use crate::header::ClientInfo;
    use crate::time::Deadline;
    use crate::{RequestHeader, ResponseHeader, RpcErrorCode, RpcStatus};
    use std::time::Duration;
    use types::ClientId;

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
        assert!(resp.legacy_error().is_none());

        let canonical = CanonicalError {
            class: ErrorClass::Retryable,
            code: Some(ErrorCode::RpcCode(RpcErrorCode::NodeUnavailable)),
            reason: None,
            retry_after_ms: Some(1000),
            message: "Test error".to_string(),
        };
        let resp_error = ResponseHeader::error(client, canonical);
        assert_eq!(resp_error.status, RpcStatus::Error);
        assert_eq!(
            resp_error.canonical_error.as_ref().and_then(|c| c.retry_after_ms),
            Some(1000)
        );
        assert!(resp_error.legacy_error().is_some());
        assert!(matches!(
            resp_error.canonical_error.as_ref().and_then(|c| c.reason.clone()),
            None | Some(RefreshReason::Unknown)
        ));
    }
}
