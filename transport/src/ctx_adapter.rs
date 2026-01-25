// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Adapter for injecting RequestHeader into gRPC metadata.
//!
//! Note: Conversion between RequestHeader and proto CallerContext is handled
//! by the `From` and `TryFrom` implementations in `proto/src/convert.rs`.

use common::header::RequestHeader;
use tonic::metadata::{MetadataMap, MetadataValue};

/// Inject RequestHeader into tonic metadata map.
///
/// This adds custom headers for observability and tracing:
/// - `x-request-id`: Call ID (UUID string)
/// - `x-client-id`: Client ID (u64 as string)
/// - `x-traceparent`: W3C Trace Context (if present)
/// - `x-deadline-ms`: Deadline timestamp in milliseconds
pub fn inject_context_to_metadata(ctx: &RequestHeader, metadata: &mut MetadataMap) -> Result<(), String> {
    // Inject call_id as x-request-id
    let request_id = ctx.client.call_id.to_string();
    metadata.insert(
        "x-request-id",
        MetadataValue::try_from(request_id.as_str())
            .map_err(|e| format!("Failed to create metadata value for request-id: {}", e))?,
    );

    // Inject client_id as x-client-id
    let client_id_str = ctx.client.client_id.as_raw().to_string();
    metadata.insert(
        "x-client-id",
        MetadataValue::try_from(client_id_str.as_str())
            .map_err(|e| format!("Failed to create metadata value for client-id: {}", e))?,
    );

    // Inject traceparent if present
    if let Some(ref tp) = ctx.traceparent {
        metadata.insert(
            "x-traceparent",
            MetadataValue::try_from(tp.as_str())
                .map_err(|e| format!("Failed to create metadata value for traceparent: {}", e))?,
        );
    }

    // Inject deadline as x-deadline-ms
    let deadline_str = ctx.deadline.as_unix_ms().to_string();
    metadata.insert(
        "x-deadline-ms",
        MetadataValue::try_from(deadline_str.as_str())
            .map_err(|e| format!("Failed to create metadata value for deadline: {}", e))?,
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use common::header::RequestHeader;
    use common::Deadline;
    use std::time::Duration;
    use types::ClientId;

    #[test]
    fn test_to_request_header() {
        let client_id = ClientId::new(123);
        let ctx = RequestHeader::with_deadline(client_id, Deadline::from_now(Duration::from_secs(10)));
        let header: proto::common::RequestHeaderProto = (&ctx).into();

        // Verify the conversion
        assert_eq!(header.client.as_ref().unwrap().call_id, ctx.client.call_id.to_string());
        assert_eq!(header.client.as_ref().unwrap().client_id, ctx.client.client_id.as_raw());
        assert_eq!(header.deadline_ms, ctx.deadline.as_unix_ms());
        if let Some(ref tp) = ctx.traceparent {
            assert_eq!(header.traceparent, *tp);
        } else {
            assert_eq!(header.traceparent, "");
        }
    }
}
