// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Unified observability infrastructure for Beryl.
//!
//! This module provides:
//! - Logging with compact or flat JSON output
//! - Tracing subscriber setup
//! - Metrics recorder setup and Prometheus export
//! - Context propagation (W3C traceparent/tracestate/baggage)

pub mod config;
pub mod init;
pub mod propagation;
pub mod tracing;

pub use config::{ObservabilityConfig, ServiceInfo};
pub use init::{ObservabilityGuard, init_observability};
