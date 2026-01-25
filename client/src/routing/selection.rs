// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Worker selection strategies.

use types::ids::WorkerId;

/// Worker selection strategy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SelectionStrategy {
    /// Always select the first worker in the list.
    First,
    // TODO: Add more strategies like LeastLatency, LeastLoad, Closest, etc.
}

/// Selects a worker from a list of candidates based on a strategy.
pub struct WorkerSelector {
    strategy: SelectionStrategy,
}

impl WorkerSelector {
    /// Create a new worker selector.
    pub fn new(strategy: SelectionStrategy) -> Self {
        Self { strategy }
    }

    /// Select a worker from a list of candidates.
    pub fn select_worker(&self, candidates: &[WorkerId]) -> Option<WorkerId> {
        if candidates.is_empty() {
            return None;
        }

        match self.strategy {
            SelectionStrategy::First => candidates.first().copied(),
            // TODO: Implement other strategies
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_selector_first() {
        let selector = WorkerSelector::new(SelectionStrategy::First);
        let candidates = vec![WorkerId::new(1), WorkerId::new(2), WorkerId::new(3)];

        let selected = selector.select_worker(&candidates);
        assert_eq!(selected, Some(WorkerId::new(1)));
    }

    #[test]
    fn test_worker_selector_empty() {
        let selector = WorkerSelector::new(SelectionStrategy::First);
        let candidates = vec![];

        let selected = selector.select_worker(&candidates);
        assert_eq!(selected, None);
    }
}
