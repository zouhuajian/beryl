// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Context propagation for distributed tracing.

use std::collections::HashMap;
use std::str::FromStr;

/// Trait for setting trace context in a carrier (e.g., gRPC metadata).
pub trait CarrierSet {
    /// Set a key-value pair in the carrier.
    fn set(&mut self, key: &str, value: &str);
}

/// Trait for getting trace context from a carrier.
pub trait CarrierGet {
    /// Get a value by key from the carrier.
    fn get(&self, key: &str) -> Option<&str>;
}

/// Extracted trace context.
#[derive(Clone, Debug, Default)]
pub struct ExtractedContext {
    /// Trace parent (W3C traceparent header).
    pub traceparent: Option<String>,
    /// Trace state (W3C tracestate header).
    pub tracestate: Option<String>,
    /// Baggage (W3C baggage header).
    pub baggage: Option<String>,
}

impl ExtractedContext {
    pub fn is_empty(&self) -> bool {
        self.traceparent.is_none() && self.tracestate.is_none() && self.baggage.is_none()
    }
}

/// Inject trace context into a carrier.
pub fn inject_trace_context(carrier: &mut dyn CarrierSet, context: &ExtractedContext) {
    if let Some(ref traceparent) = context.traceparent {
        carrier.set("traceparent", traceparent);
    }
    if let Some(ref tracestate) = context.tracestate {
        carrier.set("tracestate", tracestate);
    }
    if let Some(ref baggage) = context.baggage {
        carrier.set("baggage", baggage);
    }
}

/// Extract trace context from a carrier.
pub fn extract_trace_context(carrier: &dyn CarrierGet) -> ExtractedContext {
    ExtractedContext {
        traceparent: carrier.get("traceparent").map(|s| s.to_string()),
        tracestate: carrier.get("tracestate").map(|s| s.to_string()),
        baggage: carrier.get("baggage").map(|s| s.to_string()),
    }
}

impl CarrierSet for tonic::metadata::MetadataMap {
    fn set(&mut self, key: &str, value: &str) {
        let Ok(key) = tonic::metadata::MetadataKey::from_bytes(key.as_bytes()) else {
            return;
        };
        let Ok(value) = tonic::metadata::MetadataValue::from_str(value) else {
            return;
        };
        self.insert(key, value);
    }
}

impl CarrierGet for tonic::metadata::MetadataMap {
    fn get(&self, key: &str) -> Option<&str> {
        let key = tonic::metadata::MetadataKey::from_bytes(key.as_bytes()).ok()?;
        self.get(key)?.to_str().ok()
    }
}

/// gRPC metadata adapter for context propagation.
///
/// Note: This module requires tonic to be available in the crate using it.
pub mod grpc {
    use super::{CarrierGet, CarrierSet};

    /// gRPC metadata carrier for setting context.
    ///
    /// Usage:
    /// ```ignore
    /// use tonic::metadata::MetadataMap;
    /// let mut metadata = MetadataMap::new();
    /// let mut carrier = GrpcMetadataSet::new(&mut metadata);
    /// carrier.set("traceparent", "...");
    /// ```
    pub struct GrpcMetadataSet<'a> {
        metadata: &'a mut dyn GrpcMetadataMapMut,
    }

    pub trait GrpcMetadataMapMut {
        fn insert_str(&mut self, key: &str, value: &str);
    }

    impl<'a> GrpcMetadataSet<'a> {
        pub fn new(metadata: &'a mut dyn GrpcMetadataMapMut) -> Self {
            Self { metadata }
        }
    }

    impl<'a> CarrierSet for GrpcMetadataSet<'a> {
        fn set(&mut self, key: &str, value: &str) {
            self.metadata.insert_str(key, value);
        }
    }

    /// gRPC metadata carrier for getting context.
    pub struct GrpcMetadataGet<'a> {
        metadata: &'a dyn GrpcMetadataMap,
    }

    pub trait GrpcMetadataMap {
        fn get_str(&self, key: &str) -> Option<&str>;
    }

    impl<'a> GrpcMetadataGet<'a> {
        pub fn new(metadata: &'a dyn GrpcMetadataMap) -> Self {
            Self { metadata }
        }
    }

    impl<'a> CarrierGet for GrpcMetadataGet<'a> {
        fn get(&self, key: &str) -> Option<&str> {
            self.metadata.get_str(key)
        }
    }
}

/// Simple HashMap-based carrier for testing.
pub struct HashMapCarrier(HashMap<String, String>);

impl HashMapCarrier {
    pub fn new() -> Self {
        Self(HashMap::new())
    }
}

impl CarrierSet for HashMapCarrier {
    fn set(&mut self, key: &str, value: &str) {
        self.0.insert(key.to_string(), value.to_string());
    }
}

impl CarrierGet for HashMapCarrier {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(|s| s.as_str())
    }
}

impl Default for HashMapCarrier {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::MetadataMap;

    #[test]
    fn metadata_map_inject_extracts_trace_context() {
        let mut metadata = MetadataMap::new();
        let context = ExtractedContext {
            traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string()),
            tracestate: Some("vendor=state".to_string()),
            baggage: Some("tenant=local".to_string()),
        };

        inject_trace_context(&mut metadata, &context);
        let extracted = extract_trace_context(&metadata);

        assert_eq!(extracted.traceparent, context.traceparent);
        assert_eq!(extracted.tracestate, context.tracestate);
        assert_eq!(extracted.baggage, context.baggage);
    }

    #[test]
    fn metadata_map_injection_ignores_invalid_metadata_value() {
        let mut metadata = MetadataMap::new();
        let context = ExtractedContext {
            traceparent: Some("bad\ntraceparent".to_string()),
            tracestate: None,
            baggage: None,
        };

        inject_trace_context(&mut metadata, &context);

        assert!(metadata.get("traceparent").is_none());
    }
}
