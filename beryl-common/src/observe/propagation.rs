// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Context propagation through gRPC metadata.

use tonic::metadata::MetadataMap;

use crate::header::TraceContext;

/// Extracts the incoming traceparent when it can be represented as a string.
pub fn extract_trace_context(carrier: &MetadataMap) -> TraceContext {
    TraceContext {
        traceparent: carrier
            .get("traceparent")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::MetadataValue;

    #[test]
    fn extracts_incoming_traceparent() {
        let mut carrier = MetadataMap::new();
        carrier.insert("other", MetadataValue::from_static("keep"));
        assert!(extract_trace_context(&carrier).is_empty());
        let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        carrier.insert("traceparent", MetadataValue::from_static(traceparent));
        assert_eq!(
            extract_trace_context(&carrier).traceparent.as_deref(),
            Some(traceparent)
        );
    }
}
