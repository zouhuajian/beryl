// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Codec for encoding/decoding RequestHeader to/from headers and gRPC metadata.
//!
//! This module provides utilities for propagating RequestHeader across service boundaries
//! via HTTP headers, gRPC metadata, or other transport mechanisms.

use crate::header::{AuthnType, RequestHeader};
use crate::time::Deadline;
use types::{CallId, ClientId, GroupName, GroupStateWatermark, RaftLogId};

/// Header keys for context propagation.
pub const HEADER_CALL_ID: &str = "x-call-id";
pub const HEADER_CLIENT_ID: &str = "x-client-id";
pub const HEADER_STATE_ID: &str = "x-state-id";
pub const HEADER_MOUNT_EPOCH: &str = "x-mount-epoch";
pub const HEADER_TRACEPARENT: &str = "traceparent";
pub const HEADER_TRACESTATE: &str = "tracestate";
pub const HEADER_BAGGAGE: &str = "baggage";
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
        if let Some(ref tp) = header.trace_context.traceparent {
            headers.push((HEADER_TRACEPARENT.to_string(), tp.clone()));
        }
        if let Some(ref tracestate) = header.trace_context.tracestate {
            headers.push((HEADER_TRACESTATE.to_string(), tracestate.clone()));
        }
        if let Some(ref baggage) = header.trace_context.baggage {
            headers.push((HEADER_BAGGAGE.to_string(), baggage.clone()));
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
    pub fn decode_from_headers<I>(iter: I) -> Result<RequestHeader, String>
    where
        I: Iterator<Item = (String, String)>,
    {
        let mut call_id = None;
        let mut client_id = None;
        let mut saw_header = false;
        let mut saw_call_id = false;
        let mut saw_client_id = false;
        let mut state = Vec::new();
        let mut traceparent = None;
        let mut tracestate = None;
        let mut baggage = None;
        let mut deadline_ms = None;
        let mut mount_epoch = None;
        let mut principal = None;
        let mut real_user = None;
        let mut doas = None;
        let mut authn_type = AuthnType::Unspecified;

        for (key, value) in iter {
            saw_header = true;
            match key.as_str() {
                k if k.eq_ignore_ascii_case(HEADER_CALL_ID) => {
                    saw_call_id = true;
                    call_id = Some(require_call_id(&value, "call_id")?);
                }
                k if k.eq_ignore_ascii_case(HEADER_CLIENT_ID) => {
                    saw_client_id = true;
                    client_id = Some(require_client_id(&value, "client_id")?);
                }
                k if k.eq_ignore_ascii_case(HEADER_STATE_ID) => {
                    state.extend(parse_state_header(&value)?);
                }
                k if k.eq_ignore_ascii_case(HEADER_MOUNT_EPOCH) => {
                    mount_epoch = Some(
                        value
                            .parse::<u64>()
                            .map_err(|err| format!("invalid mount_epoch: {err}"))?,
                    );
                }
                k if k.eq_ignore_ascii_case(HEADER_TRACEPARENT) => {
                    traceparent = Some(value);
                }
                k if k.eq_ignore_ascii_case(HEADER_TRACESTATE) => {
                    tracestate = Some(value);
                }
                k if k.eq_ignore_ascii_case(HEADER_BAGGAGE) => {
                    baggage = Some(value);
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
                        "unspecified" => AuthnType::Unspecified,
                        "simple" => AuthnType::Simple,
                        "kerberos" => AuthnType::Kerberos,
                        "token" => AuthnType::Token,
                        _ => return Err(format!("invalid authn_type: {value}")),
                    };
                }
                k if k.eq_ignore_ascii_case(HEADER_GRPC_TIMEOUT) => {
                    // Parse gRPC timeout format (e.g., "30S", "500M")
                    // Precedence: grpc-timeout is lossy, x-deadline-ms is authoritative.
                    // If x-deadline-ms is present, keep it and ignore grpc-timeout.
                    let timeout_ms = parse_grpc_timeout_ms(&value)?;
                    if deadline_ms.is_none() {
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as i64;
                        let timeout_ms = i64::try_from(timeout_ms)
                            .map_err(|_| "grpc-timeout exceeds supported deadline range".to_string())?;
                        deadline_ms = Some(
                            now_ms
                                .checked_add(timeout_ms)
                                .ok_or_else(|| "grpc-timeout deadline overflow".to_string())?,
                        );
                    }
                }
                k if k.eq_ignore_ascii_case(HEADER_DEADLINE_MS) => {
                    // Absolute deadline overrides any derived grpc-timeout.
                    deadline_ms = Some(
                        value
                            .parse::<i64>()
                            .map_err(|err| format!("invalid deadline_ms: {err}"))?,
                    );
                }
                _ => {}
            }
        }
        if !saw_header {
            return Err("missing RequestHeader".to_string());
        }
        if !saw_call_id && !saw_client_id {
            return Err("missing client info".to_string());
        }
        let call_id = call_id.ok_or_else(|| "missing call_id".to_string())?;
        let client_id = client_id.ok_or_else(|| "missing client_id".to_string())?;

        // TODO: The default deadline_ms value needs from config
        Ok(RequestHeader {
            client: crate::header::ClientInfo {
                call_id,
                client_id,
                client_name: None,
            },
            trace_context: crate::header::TraceContext {
                traceparent,
                tracestate,
                baggage,
            },
            group_name: None,
            mount_epoch,
            state,
            route_epoch: None,
            principal,
            real_user,
            doas,
            authn_type,
            deadline: deadline_ms
                .map(Deadline::from_unix_ms)
                .unwrap_or_else(|| Deadline::from_now(std::time::Duration::from_secs(30))),
            caller_context: None,
            retry_count: 0,
        })
    }
}

fn require_client_id(value: &str, field_name: &str) -> Result<ClientId, String> {
    if value.is_empty() {
        return Err(format!("{field_name} must not be empty"));
    }
    ClientId::parse(value).map_err(|err| format!("{field_name} {err}"))
}

fn require_call_id(value: &str, field_name: &str) -> Result<CallId, String> {
    if value.is_empty() {
        return Err(format!("{field_name} must not be empty"));
    }
    CallId::parse(value).map_err(|err| format!("{field_name} {err}"))
}

fn parse_state_header(value: &str) -> Result<Vec<GroupStateWatermark>, String> {
    if value.is_empty() {
        return Err("x-state-id must not be empty".to_string());
    }
    let mut state = Vec::new();
    for entry in value.split(',') {
        let parts: Vec<&str> = entry.split(':').collect();
        if parts.len() != 4 {
            return Err(format!("invalid x-state-id entry: {entry}"));
        }
        let group_name = GroupName::parse(parts[0]).map_err(|err| format!("invalid x-state-id group_name: {err}"))?;
        let term = parts[1]
            .parse::<u64>()
            .map_err(|err| format!("invalid x-state-id term: {err}"))?;
        let leader_node_id = parts[2]
            .parse::<u64>()
            .map_err(|err| format!("invalid x-state-id leader_node_id: {err}"))?;
        let index = parts[3]
            .parse::<u64>()
            .map_err(|err| format!("invalid x-state-id index: {err}"))?;
        state.push(GroupStateWatermark::new(
            group_name,
            RaftLogId::new(term, leader_node_id, index),
        ));
    }
    Ok(state)
}

fn parse_grpc_timeout_ms(value: &str) -> Result<u64, String> {
    if let Some(seconds) = value.strip_suffix('S') {
        return seconds
            .parse::<u64>()
            .map_err(|err| format!("invalid grpc-timeout: {err}"))?
            .checked_mul(1_000)
            .ok_or_else(|| "grpc-timeout overflow".to_string());
    }
    if let Some(milliseconds) = value.strip_suffix('M') {
        return milliseconds
            .parse::<u64>()
            .map_err(|err| format!("invalid grpc-timeout: {err}"));
    }
    Err(format!("invalid grpc-timeout: {value}"))
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
        let tracestate = "vendor=state".to_string();
        let baggage = "tenant=local".to_string();
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
            trace_context: crate::header::TraceContext {
                traceparent: Some(traceparent.clone()),
                tracestate: Some(tracestate.clone()),
                baggage: Some(baggage.clone()),
            },
            group_name: None,
            mount_epoch: None,
            state: state.clone(),
            route_epoch: None,
            principal: None,
            real_user: None,
            doas: None,
            authn_type: AuthnType::Unspecified,
            deadline,
            caller_context: None,
            retry_count: 0,
        };

        // Encode
        let headers = RequestHeaderCodec::encode_to_headers(&header);
        let legacy_request_key = concat!("request", "_id");
        assert!(
            headers.iter().all(|(key, _)| {
                !key.eq_ignore_ascii_case(legacy_request_key) && !key.eq_ignore_ascii_case("x-request-id")
            }),
            "legacy request id must not be serialized"
        );
        assert!(
            headers
                .iter()
                .filter(|(key, _)| key.eq_ignore_ascii_case(HEADER_TRACEPARENT))
                .all(|(_, value)| value != &header.client.call_id.to_string()),
            "call_id must not be serialized as traceparent"
        );

        // Decode
        let decoded = RequestHeaderCodec::decode_from_headers(headers.into_iter()).expect("decode header");

        // Verify
        assert_eq!(decoded.client.call_id, header.client.call_id);
        assert_eq!(decoded.client.client_id, header.client.client_id);
        assert_eq!(decoded.deadline.as_unix_ms(), header.deadline.as_unix_ms());
        assert_eq!(decoded.trace_context, header.trace_context);
        assert_eq!(decoded.state, header.state);
    }

    #[test]
    fn test_decode_missing_header_rejects() {
        let error = RequestHeaderCodec::decode_from_headers(std::iter::empty()).expect_err("missing header must fail");

        assert!(error.contains("RequestHeader"));
    }

    #[test]
    fn test_decode_missing_client_info_rejects() {
        let error = RequestHeaderCodec::decode_from_headers(
            vec![(HEADER_DEADLINE_MS.to_string(), "123".to_string())].into_iter(),
        )
        .expect_err("missing client info must fail");

        assert!(error.contains("client info"));
    }

    #[test]
    fn test_decode_missing_client_id_rejects() {
        let error = RequestHeaderCodec::decode_from_headers(
            vec![(HEADER_CALL_ID.to_string(), CallId::new().to_string())].into_iter(),
        )
        .expect_err("missing client_id must fail");

        assert!(error.contains("client_id"));
    }

    #[test]
    fn test_decode_zero_client_id_rejects() {
        let error = RequestHeaderCodec::decode_from_headers(
            vec![
                (HEADER_CALL_ID.to_string(), CallId::new().to_string()),
                (HEADER_CLIENT_ID.to_string(), "0".to_string()),
            ]
            .into_iter(),
        )
        .expect_err("zero client_id must fail");

        assert!(error.contains("client_id"));
    }

    #[test]
    fn test_decode_malformed_client_id_rejects_without_generation() {
        let error = RequestHeaderCodec::decode_from_headers(
            vec![
                (HEADER_CALL_ID.to_string(), CallId::new().to_string()),
                (HEADER_CLIENT_ID.to_string(), "not-a-client-id".to_string()),
            ]
            .into_iter(),
        )
        .expect_err("malformed client_id must fail");

        assert!(error.contains("client_id"));
    }

    #[test]
    fn test_decode_missing_call_id_rejects() {
        let error = RequestHeaderCodec::decode_from_headers(
            vec![(HEADER_CLIENT_ID.to_string(), ClientId::new(99).as_raw().to_string())].into_iter(),
        )
        .expect_err("missing call_id must fail");

        assert!(error.contains("call_id"));
    }

    #[test]
    fn test_decode_invalid_call_id_rejects() {
        let error = RequestHeaderCodec::decode_from_headers(
            vec![
                (HEADER_CALL_ID.to_string(), "not-a-call-id".to_string()),
                (HEADER_CLIENT_ID.to_string(), ClientId::new(99).as_raw().to_string()),
            ]
            .into_iter(),
        )
        .expect_err("invalid call_id must fail");

        assert!(error.contains("call_id"));
    }

    #[test]
    fn test_decode_zero_call_id_rejects() {
        let error = RequestHeaderCodec::decode_from_headers(
            vec![
                (
                    HEADER_CALL_ID.to_string(),
                    "00000000-0000-0000-0000-000000000000".to_string(),
                ),
                (HEADER_CLIENT_ID.to_string(), ClientId::new(99).as_raw().to_string()),
            ]
            .into_iter(),
        )
        .expect_err("zero call_id must fail");

        assert!(error.contains("call_id"));
    }

    #[test]
    fn test_decode_only_grpc_timeout() {
        let before_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let decoded = RequestHeaderCodec::decode_from_headers(
            valid_identity_headers()
                .into_iter()
                .chain(vec![(HEADER_GRPC_TIMEOUT.to_string(), "5S".to_string())]),
        )
        .expect("decode grpc timeout");
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
            valid_identity_headers()
                .into_iter()
                .chain(vec![(HEADER_DEADLINE_MS.to_string(), deadline_ms.to_string())]),
        )
        .expect("decode deadline");
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
        let decoded = RequestHeaderCodec::decode_from_headers(encoded.into_iter()).expect("decode identity fields");
        assert_eq!(decoded.principal, Some("1000".to_string()));
        assert_eq!(decoded.real_user, Some("alice".to_string()));
        assert_eq!(decoded.doas, Some("bob".to_string()));
        assert_eq!(decoded.authn_type, AuthnType::Simple);
    }

    #[test]
    fn test_decode_deadline_precedence() {
        let deadline_ms = 987_654_321i64;
        let headers = valid_identity_headers().into_iter().chain(vec![
            (HEADER_GRPC_TIMEOUT.to_string(), "1S".to_string()),
            (HEADER_DEADLINE_MS.to_string(), deadline_ms.to_string()),
        ]);
        let decoded = RequestHeaderCodec::decode_from_headers(headers.into_iter()).expect("decode deadline precedence");
        assert_eq!(decoded.deadline.as_unix_ms(), deadline_ms);
    }

    fn valid_identity_headers() -> Vec<(String, String)> {
        vec![
            (HEADER_CALL_ID.to_string(), CallId::new().to_string()),
            (
                HEADER_CLIENT_ID.to_string(),
                ClientId::new(0x0102_0304_0506_0708_1112_1314_1516_1718)
                    .as_raw()
                    .to_string(),
            ),
        ]
    }
}
