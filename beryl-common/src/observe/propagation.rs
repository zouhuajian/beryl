// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Context propagation through gRPC metadata.

use tonic::metadata::{MetadataMap, MetadataValue};

use crate::header::TraceContext;

/// Injects present trace fields, ignoring values that are invalid gRPC metadata.
pub fn inject_trace_context(carrier: &mut MetadataMap, context: &TraceContext) {
    for (key, value) in [
        ("traceparent", &context.traceparent),
        ("tracestate", &context.tracestate),
        ("baggage", &context.baggage),
    ] {
        if let Some(value) = value
            && let Ok(value) = value.parse::<MetadataValue<_>>()
        {
            carrier.insert(key, value);
        }
    }
}

/// Extracts trace fields that can be represented as strings.
pub fn extract_trace_context(carrier: &MetadataMap) -> TraceContext {
    let get = |key| {
        carrier
            .get(key)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
    };
    TraceContext {
        traceparent: get("traceparent"),
        tracestate: get("tracestate"),
        baggage: get("baggage"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_fields_round_trip_without_changing_unrelated_metadata() {
        let mut carrier = MetadataMap::new();
        carrier.insert("other", MetadataValue::from_static("keep"));
        let context = TraceContext {
            traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into()),
            tracestate: Some("vendor=value".into()),
            baggage: Some("region=east".into()),
        };

        inject_trace_context(&mut carrier, &context);

        assert_eq!(extract_trace_context(&carrier), context);
        assert_eq!(carrier.get("other").unwrap(), "keep");
    }

    #[test]
    fn absent_or_invalid_fields_do_not_overwrite_existing_metadata() {
        let mut carrier = MetadataMap::new();
        carrier.insert("traceparent", MetadataValue::from_static("existing"));
        let context = TraceContext {
            traceparent: Some("invalid\nvalue".into()),
            baggage: Some("region=east".into()),
            ..Default::default()
        };

        inject_trace_context(&mut carrier, &context);

        assert_eq!(
            extract_trace_context(&carrier),
            TraceContext {
                traceparent: Some("existing".into()),
                baggage: context.baggage,
                ..Default::default()
            }
        );
        assert!(extract_trace_context(&MetadataMap::new()).is_empty());
    }
}
