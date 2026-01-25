// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Context propagation for distributed tracing.

use std::collections::HashMap;

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
#[derive(Clone, Debug)]
pub struct ExtractedContext {
    /// Trace parent (W3C traceparent header).
    pub traceparent: Option<String>,
    /// Baggage (W3C baggage header).
    pub baggage: Option<String>,
    /// Request ID.
    pub request_id: Option<String>,
}

/// Inject trace context into a carrier.
pub fn inject_trace_context(carrier: &mut dyn CarrierSet, context: &ExtractedContext) {
    if let Some(ref traceparent) = context.traceparent {
        carrier.set("traceparent", traceparent);
    }
    if let Some(ref baggage) = context.baggage {
        carrier.set("baggage", baggage);
    }
    if let Some(ref request_id) = context.request_id {
        carrier.set("request-id", request_id);
    }
}

/// Extract trace context from a carrier.
pub fn extract_trace_context(carrier: &dyn CarrierGet) -> ExtractedContext {
    ExtractedContext {
        traceparent: carrier.get("traceparent").map(|s| s.to_string()),
        baggage: carrier.get("baggage").map(|s| s.to_string()),
        request_id: carrier
            .get("request-id")
            .or_else(|| carrier.get("x-request-id"))
            .map(|s| s.to_string()),
    }
}

/// gRPC metadata adapter for context propagation.
///
/// Note: This module requires tonic to be available. For use in transport crate,
/// import these types directly.
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
