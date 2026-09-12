// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Process-local metadata readiness state.
//!
//! The common observability layer owns exported metrics. This module only holds
//! the root readiness value shared with the readiness gate.

use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

/// Process-local metadata readiness state.
#[derive(Clone)]
pub struct MetadataMetrics {
    pub(crate) root_ready: Arc<AtomicUsize>,
}

impl MetadataMetrics {
    pub fn new() -> Self {
        Self {
            root_ready: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl Default for MetadataMetrics {
    fn default() -> Self {
        Self::new()
    }
}
