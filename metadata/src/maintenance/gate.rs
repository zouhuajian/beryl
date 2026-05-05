// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Gate for maintenance tasks: fail-closed control for destructive operations.
//!
//! This module implements TaskGate for maintenance task-level gating.
//! For destructive action gating, use DestructiveGate directly.

use tracing::info;

/// Gate state for maintenance tasks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateState {
    /// Gate is ready, destructive actions allowed.
    Ready,
    /// Gate is degraded, only statistics allowed.
    Degraded,
    /// Gate is blocked, no actions allowed.
    Blocked,
}

impl GateState {
    pub fn as_str(&self) -> &'static str {
        match self {
            GateState::Ready => "ready",
            GateState::Degraded => "degraded",
            GateState::Blocked => "blocked",
        }
    }

    /// Check if destructive actions are allowed.
    pub fn allows_destructive(&self) -> bool {
        matches!(self, GateState::Ready)
    }
}

/// Gate for a maintenance task.
pub struct TaskGate {
    enabled: bool,
    state: GateState,
    last_error: String,
    last_error_ts: u64,
    last_success_ts: u64,
}

impl Default for TaskGate {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskGate {
    pub fn new() -> Self {
        Self {
            enabled: true,
            state: GateState::Ready,
            last_error: String::new(),
            last_error_ts: 0,
            last_success_ts: 0,
        }
    }

    pub fn state(&self) -> GateState {
        self.state.clone()
    }

    pub fn check(&self, _task_name: &str, _now_ms: u64) -> GateCheckResult {
        if !self.enabled {
            return GateCheckResult::Blocked {
                reason: "task_disabled".to_string(),
            };
        }

        match self.state {
            GateState::Ready => GateCheckResult::Ready,
            GateState::Degraded => GateCheckResult::Degraded {
                reason: self.last_error.clone(),
                last_error_ts: self.last_error_ts,
            },
            GateState::Blocked => GateCheckResult::Blocked {
                reason: self.last_error.clone(),
            },
        }
    }

    pub fn set_degraded(&mut self, reason: String, error: String, now_ms: u64) {
        if self.state != GateState::Degraded {
            info!(
                task = "maintenance",
                old_state = ?self.state,
                new_state = "degraded",
                reason = %reason,
                error = %error,
                "Maintenance gate state transition"
            );
        }
        self.state = GateState::Degraded;
        self.last_error = error;
        self.last_error_ts = now_ms;
    }

    pub fn set_ready(&mut self, now_ms: u64) {
        if self.state != GateState::Ready {
            info!(
                task = "maintenance",
                old_state = ?self.state,
                new_state = "ready",
                "Maintenance gate recovered: ready"
            );
        }
        self.state = GateState::Ready;
        self.last_error.clear();
        self.last_success_ts = now_ms;
    }

    /// Maybe set gate to ready (if currently degraded, allow recovery).
    pub fn maybe_set_ready(&mut self, now_ms: u64) {
        if self.state == GateState::Degraded {
            info!(
                task = "maintenance",
                old_state = "degraded",
                new_state = "ready",
                "Maintenance gate recovered from degraded to ready"
            );
            self.state = GateState::Ready;
            self.last_error.clear();
            self.last_success_ts = now_ms;
        }
    }

    pub fn set_blocked(&mut self, reason: String, error: String, now_ms: u64) {
        if self.state != GateState::Blocked {
            info!(
                task = "maintenance",
                old_state = ?self.state,
                new_state = "blocked",
                reason = %reason,
                error = %error,
                "Maintenance gate state transition"
            );
        }
        self.state = GateState::Blocked;
        self.last_error = error;
        self.last_error_ts = now_ms;
    }
}

/// Result of gate check.
#[derive(Debug)]
pub enum GateCheckResult {
    Ready,
    Degraded { reason: String, last_error_ts: u64 },
    Blocked { reason: String },
}

// Note: Unified check_destructive_allowed() is provided by DestructiveGate directly.
// This module focuses on TaskGate for maintenance task-level gating.
