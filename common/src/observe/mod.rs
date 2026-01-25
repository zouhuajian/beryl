// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Unified observability infrastructure for Vecton.
//!
//! This module provides:
//! - Logging (stdout JSON, production default)
//! - Tracing (console + OTLP)
//! - Metrics (Prometheus + OTLP)
//! - Context propagation (W3C traceparent + request_id)
//! - Error classification and naming conventions

pub mod config;
pub mod error;
pub mod init;
pub mod metrics;
pub mod propagation;
pub mod tracing;

pub use config::{ObservabilityConfig, ServiceInfo};
pub use error::ErrorKind;
pub use init::{ObservabilityGuard, init_observability};
