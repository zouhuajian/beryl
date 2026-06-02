// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Unified observability infrastructure for Vecton.
//!
//! This module provides:
//! - Logging (stdout JSON, production default)
//! - Tracing subscriber setup, with optional OTLP tracing when enabled
//! - Metrics recorder setup and Prometheus export
//! - Context propagation (W3C traceparent + request_id)
//! - Generic error classification for low-cardinality labels

pub mod config;
pub mod error;
pub mod init;
pub mod propagation;
pub mod tracing;

pub use config::{ObservabilityConfig, ServiceInfo};
pub use error::ErrorKind;
pub use init::{ObservabilityGuard, init_observability};
