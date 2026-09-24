// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Request admission after startup validation and until shutdown begins.

use crate::observe;
use std::sync::atomic::{AtomicBool, Ordering};

pub(crate) struct RootReadinessGate {
    ready: AtomicBool,
}

impl RootReadinessGate {
    /// Publish readiness after the authority has passed startup validation.
    pub(crate) fn new() -> Self {
        observe::record_root_ready(true);
        Self {
            ready: AtomicBool::new(true),
        }
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    /// Permanently closes admission before the process begins draining RPCs.
    pub(crate) fn begin_shutdown(&self) {
        self.ready.store(false, Ordering::Release);
        observe::record_root_ready(false);
    }
}
