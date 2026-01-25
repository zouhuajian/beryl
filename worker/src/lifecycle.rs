// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker lifecycle state machine.

use parking_lot::RwLock;
use std::sync::Arc;
use tracing::info;

/// Worker lifecycle states.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerState {
    /// Initial state before any initialization.
    Init,
    /// Bootstrapping: loading config, initializing observability.
    Bootstrapping,
    /// Volumes are ready (opened and validated).
    VolumesReady,
    /// RPC server is starting.
    RpcServing,
    /// Registering with metadata service.
    Registering,
    /// Fully serving requests.
    Serving,
    /// Degraded: some volumes failed, but still serving.
    Degraded,
    /// Draining: stopping gracefully, not accepting new requests.
    Draining,
    /// Stopped: shutdown complete.
    Stopped,
}

impl WorkerState {
    /// Check if the worker can accept requests.
    pub fn can_serve(&self) -> bool {
        matches!(self, WorkerState::Serving | WorkerState::Degraded)
    }

    /// Check if the worker is shutting down.
    pub fn is_shutting_down(&self) -> bool {
        matches!(self, WorkerState::Draining | WorkerState::Stopped)
    }
}

/// Worker lifecycle manager.
pub struct Lifecycle {
    state: Arc<RwLock<WorkerState>>,
}

impl Lifecycle {
    /// Create a new lifecycle manager.
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(WorkerState::Init)),
        }
    }

    /// Get current state.
    pub fn state(&self) -> WorkerState {
        *self.state.read()
    }

    /// Transition to a new state.
    pub fn transition(&self, new_state: WorkerState) -> Result<(), String> {
        let current = *self.state.read();

        // Validate transition
        if !Self::is_valid_transition(current, new_state) {
            return Err(format!("Invalid state transition: {:?} -> {:?}", current, new_state));
        }

        info!(
            from = ?current,
            to = ?new_state,
            "Worker state transition"
        );

        *self.state.write() = new_state;
        Ok(())
    }

    /// Check if a transition is valid.
    fn is_valid_transition(from: WorkerState, to: WorkerState) -> bool {
        match (from, to) {
            // Init -> Bootstrapping
            (WorkerState::Init, WorkerState::Bootstrapping) => true,
            // Bootstrapping -> VolumesReady
            (WorkerState::Bootstrapping, WorkerState::VolumesReady) => true,
            // VolumesReady -> RpcServing
            (WorkerState::VolumesReady, WorkerState::RpcServing) => true,
            // RpcServing -> Registering or Serving (depending on registration mode)
            (WorkerState::RpcServing, WorkerState::Registering) => true,
            (WorkerState::RpcServing, WorkerState::Serving) => true,
            // Registering -> Serving
            (WorkerState::Registering, WorkerState::Serving) => true,
            // Serving -> Degraded (on volume failure)
            (WorkerState::Serving, WorkerState::Degraded) => true,
            // Degraded -> Serving (on recovery)
            (WorkerState::Degraded, WorkerState::Serving) => true,
            // Any -> Draining (graceful shutdown)
            (_, WorkerState::Draining) if from != WorkerState::Stopped => true,
            // Draining -> Stopped
            (WorkerState::Draining, WorkerState::Stopped) => true,
            // Same state (idempotent)
            (a, b) if a == b => true,
            // Invalid transitions
            _ => false,
        }
    }

    /// Check if worker can serve requests (for gating).
    pub fn check_can_serve(&self) -> Result<(), String> {
        let state = self.state();
        if state.can_serve() {
            Ok(())
        } else {
            Err(format!("Worker is not ready to serve requests: {:?}", state))
        }
    }
}

impl Default for Lifecycle {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_transitions() {
        let lifecycle = Lifecycle::new();
        assert_eq!(lifecycle.state(), WorkerState::Init);

        // Valid transitions
        lifecycle.transition(WorkerState::Bootstrapping).unwrap();
        assert_eq!(lifecycle.state(), WorkerState::Bootstrapping);

        lifecycle.transition(WorkerState::VolumesReady).unwrap();
        assert_eq!(lifecycle.state(), WorkerState::VolumesReady);

        lifecycle.transition(WorkerState::RpcServing).unwrap();
        assert_eq!(lifecycle.state(), WorkerState::RpcServing);

        lifecycle.transition(WorkerState::Serving).unwrap();
        assert_eq!(lifecycle.state(), WorkerState::Serving);

        // Invalid transition
        let result = lifecycle.transition(WorkerState::Init);
        assert!(result.is_err());
    }

    #[test]
    fn test_can_serve() {
        let lifecycle = Lifecycle::new();
        assert!(!lifecycle.check_can_serve().is_ok());

        lifecycle.transition(WorkerState::Bootstrapping).unwrap();
        lifecycle.transition(WorkerState::VolumesReady).unwrap();
        lifecycle.transition(WorkerState::RpcServing).unwrap();
        lifecycle.transition(WorkerState::Serving).unwrap();
        assert!(lifecycle.check_can_serve().is_ok());

        lifecycle.transition(WorkerState::Degraded).unwrap();
        assert!(lifecycle.check_can_serve().is_ok());

        lifecycle.transition(WorkerState::Draining).unwrap();
        assert!(!lifecycle.check_can_serve().is_ok());
    }
}
