// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Codec for encoding/decoding RequestHeader to/from headers and gRPC metadata.
//!
//! This module provides utilities for propagating RequestHeader across service boundaries
//! via HTTP headers, gRPC metadata, or other transport mechanisms.

use crate::header::{AuthnType, RequestHeader};
use crate::time::Deadline;
use std::str::FromStr;
use types::{CallId, ClientId, GroupName, GroupStateWatermark, RaftLogId};

/// Header keys for context propagation.
pub const HEADER_CALL_ID: &str = "x-call-id";
pub const HEADER_CLIENT_ID: &str = "x-client-id";
pub const HEADER_STATE_ID: &str = "x-state-id";
pub const HEADER_MOUNT_EPOCH: &str = "x-mount-epoch";
pub const HEADER_TRACEPARENT: &str = "traceparent";
pub const HEADER_DEADLINE_MS: &str = "x-deadline-ms";
pub const HEADER_GRPC_TIMEOUT: &str = "grpc-timeout";
pub const HEADER_PRINCIPAL: &str = "x-principal";
pub const HEADER_REAL_USER: &str = "x-real-user";
pub const HEADER_DOAS: &str = "x-doas";
pub const HEADER_AUTHN_TYPE: &str = "x-authn-type";

/// Codec for RequestHeader header encoding/decoding.
pub struct RequestHeaderCodec;

impl RequestHeaderCodec {
    /// Encode RequestHeader to headers.
    ///
    /// Returns a vector of (key, value) pairs where values are strings.
    pub fn encode_to_headers(header: &RequestHeader) -> Vec<(String, String)> {
        let mut headers = Vec::new();

        // x-call-id
        headers.push((HEADER_CALL_ID.to_string(), header.client.call_id.to_string()));

        // x-client-id
        headers.push((
            HEADER_CLIENT_ID.to_string(),
            header.client.client_id.as_raw().to_string(),
        ));

        // x-state-id (if present): comma-separated group_name:term:leader:index entries.
        if !header.state.is_empty() {
            let state_id_str = header
                .state
                .iter()
                .map(|watermark| {
                    format!(
                        "{}:{}:{}:{}",
                        watermark.group_name,
                        watermark.state_id.term,
                        watermark.state_id.leader_node_id,
                        watermark.state_id.index
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            headers.push((HEADER_STATE_ID.to_string(), state_id_str));
        }

        // mount_epoch (if present)
        if let Some(mount_epoch) = header.mount_epoch {
            headers.push((HEADER_MOUNT_EPOCH.to_string(), mount_epoch.to_string()));
        }

        // traceparent (if present)
        if let Some(ref tp) = header.traceparent {
            headers.push((HEADER_TRACEPARENT.to_string(), tp.clone()));
        }

        if let Some(ref principal) = header.principal {
            headers.push((HEADER_PRINCIPAL.to_string(), principal.clone()));
        }
        if let Some(ref real_user) = header.real_user {
            headers.push((HEADER_REAL_USER.to_string(), real_user.clone()));
        }
        if let Some(ref doas) = header.doas {
            headers.push((HEADER_DOAS.to_string(), doas.clone()));
        }
        headers.push((
            HEADER_AUTHN_TYPE.to_string(),
            match header.authn_type {
                AuthnType::Unspecified => "unspecified",
                AuthnType::Simple => "simple",
                AuthnType::Kerberos => "kerberos",
                AuthnType::Token => "token",
            }
            .to_string(),
        ));

        // x-deadline-ms: absolute deadline in unix ms for lossless roundtrip
        headers.push((HEADER_DEADLINE_MS.to_string(), header.deadline.as_unix_ms().to_string()));

        // grpc-timeout (convert deadline_ms to gRPC timeout format) for gRPC-native peers
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let remaining_ms = (header.deadline.as_unix_ms() - now_ms).max(0);
        if remaining_ms > 0 {
            // Convert to gRPC timeout format (e.g., "30S", "500M" for milliseconds)
            let timeout_str = if remaining_ms >= 1000 {
                format!("{}S", remaining_ms / 1000)
            } else {
                format!("{}M", remaining_ms)
            };
            headers.push((HEADER_GRPC_TIMEOUT.to_string(), timeout_str));
        }

        headers
    }

    /// Decode RequestHeader from headers.
    ///
    /// Takes an iterator of (key, value) pairs where values are strings.
    /// Missing fields are filled with defaults (generates new call_id, uses unknown client_id).
    pub fn decode_from_headers<I>(iter: I) -> RequestHeader
    where
        I: Iterator<Item = (String, String)>,
    {
        let mut call_id = None;
        let mut client_id = None;
        let mut state = Vec::new();
        let mut traceparent = None;
        let mut deadline_ms = None;
        let mut mount_epoch = None;
        let mut principal = None;
        let mut real_user = None;
        let mut doas = None;
        let mut authn_type = AuthnType::Unspecified;

        for (key, value) in iter {
            match key.as_str() {
                k if k.eq_ignore_ascii_case(HEADER_CALL_ID) => {
                    call_id = CallId::from_str(&value).ok();
                }
                k if k.eq_ignore_ascii_case(HEADER_CLIENT_ID) => {
                    client_id = value.parse::<u64>().ok().map(ClientId::new);
                }
                k if k.eq_ignore_ascii_case(HEADER_STATE_ID) => {
                    // Parse format: "group_name:term:leader_node_id:index[,group_name:term:leader_node_id:index]"
                    for entry in value.split(',') {
                        let parts: Vec<&str> = entry.split(':').collect();
                        if parts.len() == 4
                            && let (Ok(group_name), Ok(term), Ok(leader_node_id), Ok(index)) = (
                                GroupName::parse(parts[0]),
                                parts[1].parse::<u64>(),
                                parts[2].parse::<u64>(),
                                parts[3].parse::<u64>(),
                            )
                        {
                            state.push(GroupStateWatermark::new(
                                group_name,
                                RaftLogId::new(term, leader_node_id, index),
                            ));
                        }
                    }
                }
                k if k.eq_ignore_ascii_case(HEADER_MOUNT_EPOCH) => {
                    mount_epoch = value.parse::<u64>().ok();
                }
                k if k.eq_ignore_ascii_case(HEADER_TRACEPARENT) => {
                    traceparent = Some(value);
                }
                k if k.eq_ignore_ascii_case(HEADER_PRINCIPAL) && !value.is_empty() => {
                    principal = Some(value);
                }
                k if k.eq_ignore_ascii_case(HEADER_REAL_USER) && !value.is_empty() => {
                    real_user = Some(value);
                }
                k if k.eq_ignore_ascii_case(HEADER_DOAS) && !value.is_empty() => {
                    doas = Some(value);
                }
                k if k.eq_ignore_ascii_case(HEADER_AUTHN_TYPE) => {
                    authn_type = match value.to_ascii_lowercase().as_str() {
                        "simple" => AuthnType::Simple,
                        "kerberos" => AuthnType::Kerberos,
                        "token" => AuthnType::Token,
                        _ => AuthnType::Unspecified,
                    };
                }
                k if k.eq_ignore_ascii_case(HEADER_GRPC_TIMEOUT) => {
                    // Parse gRPC timeout format (e.g., "30S", "500M")
                    // Precedence: grpc-timeout is lossy, x-deadline-ms is authoritative.
                    // If x-deadline-ms is present, keep it and ignore grpc-timeout.
                    let timeout_ms = if value.ends_with('S') {
                        value[..value.len() - 1].parse::<u64>().ok().map(|s| s * 1000)
                    } else if value.ends_with('M') {
                        value[..value.len() - 1].parse::<u64>().ok()
                    } else {
                        None
                    };
                    if deadline_ms.is_none()
                        && let Some(ms) = timeout_ms
                    {
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as i64;
                        deadline_ms = Some(now_ms + ms as i64);
                    }
                }
                k if k.eq_ignore_ascii_case(HEADER_DEADLINE_MS) => {
                    // Absolute deadline overrides any derived grpc-timeout.
                    deadline_ms = value.parse::<i64>().ok();
                }
                _ => {}
            }
        }
        // TODO: The default deadline_ms value needs from config
        RequestHeader {
            client: crate::header::ClientInfo {
                call_id: call_id.unwrap_or_else(CallId::new),
                client_id: client_id.unwrap_or_else(|| ClientId::new(0)),
                client_name: None,
            },
            deadline: deadline_ms
                .map(Deadline::from_unix_ms)
                .unwrap_or_else(|| Deadline::from_now(std::time::Duration::from_secs(30))),
            traceparent,
            caller_context: None,
            state,
            retry_count: 0,
            group_name: None,
            mount_epoch,
            route_epoch: None,
            principal,
            real_user,
            doas,
            authn_type,
        }
    }
}

/// Helper functions for writing error information to gRPC trailers.
///
/// For unrecoverable errors (UNAUTHENTICATED, PERMISSION_DENIED, INTERNAL, etc.),
/// the server should return a non-OK gRPC status and include minimal correlation
/// information in trailers metadata for debugging and correlation.
pub mod grpc_trailers {
    use super::super::types::RpcErrorCode;

    /// Write minimal error information to gRPC trailers.
    ///
    /// This function creates trailer entries for:
    /// - x-call-id: Call ID for correlation
    /// - x-error-code: Error code as string
    ///
    /// Returns a vector of (key, value) pairs suitable for use with tonic::Response.
    #[allow(dead_code)] // May be used in future error handling
    pub fn write_error_trailers(
        client_info: &crate::header::ClientInfo,
        error_code: &RpcErrorCode,
    ) -> Vec<(String, String)> {
        vec![
            ("x-call-id".to_string(), client_info.call_id.to_string()),
            ("x-error-code".to_string(), format!("{:?}", error_code)),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use types::CallId;

    #[test]
    fn test_encode_decode_roundtrip() {
        let client_id = ClientId::new(12345);
        let deadline = Deadline::from_now(std::time::Duration::from_secs(60));
        let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string();
        let state = vec![GroupStateWatermark::new(
            GroupName::parse("root").unwrap(),
            RaftLogId::new(1, 2, 100),
        )];

        let header = RequestHeader {
            client: crate::header::ClientInfo {
                call_id: CallId::new(),
                client_id,
                client_name: None,
            },
            deadline,
            traceparent: Some(traceparent.clone()),
            caller_context: None,
            state: state.clone(),
            retry_count: 0,
            group_name: None,
            mount_epoch: None,
            route_epoch: None,
            principal: None,
            real_user: None,
            doas: None,
            authn_type: AuthnType::Unspecified,
        };

        // Encode
        let headers = RequestHeaderCodec::encode_to_headers(&header);

        // Decode
        let decoded = RequestHeaderCodec::decode_from_headers(headers.into_iter());

        // Verify
        assert_eq!(decoded.client.call_id, header.client.call_id);
        assert_eq!(decoded.client.client_id, header.client.client_id);
        assert_eq!(decoded.deadline.as_unix_ms(), header.deadline.as_unix_ms());
        assert_eq!(decoded.traceparent, header.traceparent);
        assert_eq!(decoded.state, header.state);
    }

    #[test]
    fn test_decode_missing_fields() {
        // Empty headers should create a header with defaults
        let decoded = RequestHeaderCodec::decode_from_headers(std::iter::empty());

        // Should have generated a new call_id
        assert_ne!(decoded.client.call_id, CallId::new()); // Different call_ids
        // Should have default client_id (0)
        assert_eq!(decoded.client.client_id, ClientId::new(0));
        // Should have default deadline
        assert!(!decoded.deadline.has_passed());
    }

    #[test]
    fn test_decode_only_grpc_timeout() {
        let before_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let decoded = RequestHeaderCodec::decode_from_headers(
            vec![(HEADER_GRPC_TIMEOUT.to_string(), "5S".to_string())].into_iter(),
        );
        let after_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let deadline_ms = decoded.deadline.as_unix_ms();
        assert!(deadline_ms >= before_ms + 5_000);
        assert!(deadline_ms <= after_ms + 5_000);
    }

    #[test]
    fn test_decode_only_deadline_ms() {
        let deadline_ms = 123_456_789i64;
        let decoded = RequestHeaderCodec::decode_from_headers(
            vec![(HEADER_DEADLINE_MS.to_string(), deadline_ms.to_string())].into_iter(),
        );
        assert_eq!(decoded.deadline.as_unix_ms(), deadline_ms);
    }

    #[test]
    fn test_identity_fields_roundtrip() {
        let mut header = RequestHeader::new(ClientId::new(7));
        header.principal = Some("1000".to_string());
        header.real_user = Some("alice".to_string());
        header.doas = Some("bob".to_string());
        header.authn_type = AuthnType::Simple;

        let encoded = RequestHeaderCodec::encode_to_headers(&header);
        let decoded = RequestHeaderCodec::decode_from_headers(encoded.into_iter());
        assert_eq!(decoded.principal, Some("1000".to_string()));
        assert_eq!(decoded.real_user, Some("alice".to_string()));
        assert_eq!(decoded.doas, Some("bob".to_string()));
        assert_eq!(decoded.authn_type, AuthnType::Simple);
    }

    #[test]
    fn test_decode_deadline_precedence() {
        let deadline_ms = 987_654_321i64;
        let headers = vec![
            (HEADER_GRPC_TIMEOUT.to_string(), "1S".to_string()),
            (HEADER_DEADLINE_MS.to_string(), deadline_ms.to_string()),
        ];
        let decoded = RequestHeaderCodec::decode_from_headers(headers.into_iter());
        assert_eq!(decoded.deadline.as_unix_ms(), deadline_ms);
    }
}
