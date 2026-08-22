// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Context propagation for distributed tracing.

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
