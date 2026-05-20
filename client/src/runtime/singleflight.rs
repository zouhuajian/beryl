// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Private request coalescing helper for client cache and channel misses.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;

use futures::future::{AbortHandle, Abortable};
use parking_lot::Mutex;
use tokio::sync::watch;

use crate::error::{ClientError, ClientResult};

type FlightResult<T> = Option<ClientResult<T>>;

struct InFlight<T> {
    generation: u64,
    receiver: watch::Receiver<FlightResult<T>>,
    active_callers: usize,
    abort_handle: AbortHandle,
}

#[derive(Default)]
struct SingleflightState<K, T> {
    next_generation: u64,
    in_flight: HashMap<K, InFlight<T>>,
}

/// Result of entering a singleflight group.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SingleflightMode {
    /// This caller created and drives the shared future.
    Leader,
    /// This caller joined an already in-flight future.
    Joined,
}

/// Small async singleflight map keyed by correctness-safe private keys.
#[derive(Clone)]
pub(crate) struct Singleflight<K, T> {
    state: Arc<Mutex<SingleflightState<K, T>>>,
}

impl<K, T> fmt::Debug for Singleflight<K, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Singleflight").finish_non_exhaustive()
    }
}

impl<K, T> Default for Singleflight<K, T> {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(SingleflightState {
                next_generation: 0,
                in_flight: HashMap::new(),
            })),
        }
    }
}

impl<K, T> Singleflight<K, T>
where
    K: Clone + Eq + Hash + Send + 'static,
    T: Clone + Send + Sync + 'static,
{
    /// Run or join a shared future for `key`.
    pub(crate) async fn run<F, Fut>(&self, key: K, future: F) -> (SingleflightMode, ClientResult<T>)
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ClientResult<T>> + Send + 'static,
    {
        let (generation, mut receiver, mode) = {
            let mut state = self.state.lock();
            if let Some(entry) = state.in_flight.get_mut(&key) {
                let generation = entry.generation;
                let receiver = entry.receiver.clone();
                entry.active_callers = entry.active_callers.saturating_add(1);
                (generation, receiver, SingleflightMode::Joined)
            } else {
                let generation = state.next_generation;
                state.next_generation = state.next_generation.wrapping_add(1);
                let (sender, receiver) = watch::channel(None);
                let (abort_handle, abort_registration) = AbortHandle::new_pair();
                let task_state = Arc::clone(&self.state);
                let task_key = key.clone();
                state.in_flight.insert(
                    key.clone(),
                    InFlight {
                        generation,
                        receiver: receiver.clone(),
                        active_callers: 1,
                        abort_handle,
                    },
                );
                tokio::spawn(async move {
                    let task = async move {
                        let result = future().await;
                        let _ = sender.send(Some(result));
                        let mut state = task_state.lock();
                        if state
                            .in_flight
                            .get(&task_key)
                            .is_some_and(|entry| entry.generation == generation)
                        {
                            state.in_flight.remove(&task_key);
                        }
                    };
                    let _ = Abortable::new(task, abort_registration).await;
                });
                (generation, receiver, SingleflightMode::Leader)
            }
        };
        let guard = FlightGuard::new(Arc::clone(&self.state), key, generation);
        let result = wait_for_result(&mut receiver).await;
        guard.complete();
        (mode, result)
    }
}

async fn wait_for_result<T: Clone>(receiver: &mut watch::Receiver<FlightResult<T>>) -> ClientResult<T> {
    loop {
        if let Some(result) = receiver.borrow_and_update().clone() {
            return result;
        }
        if receiver.changed().await.is_err() {
            return Err(ClientError::Cache(
                "singleflight leader ended before publishing a result".to_string(),
            ));
        }
    }
}

struct FlightGuard<K, T>
where
    K: Eq + Hash,
{
    state: Arc<Mutex<SingleflightState<K, T>>>,
    key: Option<K>,
    generation: u64,
}

impl<K, T> FlightGuard<K, T>
where
    K: Eq + Hash,
{
    fn new(state: Arc<Mutex<SingleflightState<K, T>>>, key: K, generation: u64) -> Self {
        Self {
            state,
            key: Some(key),
            generation,
        }
    }

    fn complete(mut self) {
        self.key = None;
    }
}

impl<K, T> Drop for FlightGuard<K, T>
where
    K: Eq + Hash,
{
    fn drop(&mut self) {
        let Some(key) = self.key.as_ref() else {
            return;
        };
        let mut state = self.state.lock();
        let Some(entry) = state.in_flight.get_mut(key) else {
            return;
        };
        if entry.generation != self.generation {
            return;
        }
        entry.active_callers = entry.active_callers.saturating_sub(1);
        if entry.active_callers == 0 {
            entry.abort_handle.abort();
            state.in_flight.remove(key);
        }
    }
}
#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::Notify;

    use super::*;
    use crate::error::ClientError;

    #[tokio::test]
    async fn leader_cancellation_removes_in_flight_entry() {
        let singleflight = Singleflight::<String, u64>::default();
        let started = Arc::new(Notify::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let leader = {
            let singleflight = singleflight.clone();
            let started = Arc::clone(&started);
            let calls = Arc::clone(&calls);
            tokio::spawn(async move {
                singleflight
                    .run("alpha".to_string(), move || async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        started.notify_waiters();
                        std::future::pending::<ClientResult<u64>>().await
                    })
                    .await
            })
        };
        started.notified().await;
        leader.abort();
        let err = leader.await.expect_err("leader task must be cancelled");
        assert!(err.is_cancelled());

        let calls_for_retry = Arc::clone(&calls);
        let retry = tokio::time::timeout(
            Duration::from_millis(100),
            singleflight.run("alpha".to_string(), move || async move {
                calls_for_retry.fetch_add(1, Ordering::SeqCst);
                Ok(7)
            }),
        )
        .await
        .expect("cancelled leader must not leave a stale flight");

        assert_eq!(retry.0, SingleflightMode::Leader);
        assert_eq!(retry.1.expect("retry result"), 7);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn waiter_cancellation_does_not_cancel_leader_or_other_waiters() {
        let singleflight = Singleflight::<String, u64>::default();
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let leader = {
            let singleflight = singleflight.clone();
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            let calls = Arc::clone(&calls);
            tokio::spawn(async move {
                singleflight
                    .run("alpha".to_string(), move || async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        started.notify_waiters();
                        release.notified().await;
                        Ok(11)
                    })
                    .await
            })
        };
        started.notified().await;

        let cancelled_waiter = {
            let singleflight = singleflight.clone();
            tokio::spawn(async move { singleflight.run("alpha".to_string(), || async { Ok(99) }).await })
        };
        tokio::task::yield_now().await;
        cancelled_waiter.abort();
        let err = cancelled_waiter.await.expect_err("waiter task must be cancelled");
        assert!(err.is_cancelled());

        let other_waiter = {
            let singleflight = singleflight.clone();
            tokio::spawn(async move { singleflight.run("alpha".to_string(), || async { Ok(99) }).await })
        };
        tokio::task::yield_now().await;
        release.notify_waiters();

        let leader = leader.await.expect("leader join");
        let other_waiter = other_waiter.await.expect("waiter join");
        assert_eq!(leader.0, SingleflightMode::Leader);
        assert_eq!(leader.1.expect("leader result"), 11);
        assert_eq!(other_waiter.0, SingleflightMode::Joined);
        assert_eq!(other_waiter.1.expect("waiter result"), 11);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn all_waiters_cancelled_allows_next_caller_to_start_fresh() {
        let singleflight = Singleflight::<String, u64>::default();
        let started = Arc::new(Notify::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let leader = {
            let singleflight = singleflight.clone();
            let started = Arc::clone(&started);
            let calls = Arc::clone(&calls);
            tokio::spawn(async move {
                singleflight
                    .run("alpha".to_string(), move || async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        started.notify_waiters();
                        std::future::pending::<ClientResult<u64>>().await
                    })
                    .await
            })
        };
        started.notified().await;
        let waiter = {
            let singleflight = singleflight.clone();
            tokio::spawn(async move { singleflight.run("alpha".to_string(), || async { Ok(99) }).await })
        };
        tokio::task::yield_now().await;
        waiter.abort();
        leader.abort();
        let waiter_err = waiter.await.expect_err("waiter task must be cancelled");
        let leader_err = leader.await.expect_err("leader task must be cancelled");
        assert!(waiter_err.is_cancelled());
        assert!(leader_err.is_cancelled());

        let calls_for_retry = Arc::clone(&calls);
        let retry = tokio::time::timeout(
            Duration::from_millis(100),
            singleflight.run("alpha".to_string(), move || async move {
                calls_for_retry.fetch_add(1, Ordering::SeqCst);
                Ok(13)
            }),
        )
        .await
        .expect("all-cancelled flight must not poison the key");

        assert_eq!(retry.0, SingleflightMode::Leader);
        assert_eq!(retry.1.expect("retry result"), 13);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn leader_failure_wakes_waiters_and_removes_entry() {
        let singleflight = Singleflight::<String, u64>::default();
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let leader = {
            let singleflight = singleflight.clone();
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            let calls = Arc::clone(&calls);
            tokio::spawn(async move {
                singleflight
                    .run("alpha".to_string(), move || async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        started.notify_waiters();
                        release.notified().await;
                        Err(ClientError::Cache("injected failure".to_string()))
                    })
                    .await
            })
        };
        started.notified().await;
        let waiter = {
            let singleflight = singleflight.clone();
            tokio::spawn(async move { singleflight.run("alpha".to_string(), || async { Ok(99) }).await })
        };
        tokio::task::yield_now().await;
        release.notify_waiters();

        let leader = leader.await.expect("leader join");
        let waiter = waiter.await.expect("waiter join");
        assert_eq!(leader.0, SingleflightMode::Leader);
        assert_eq!(waiter.0, SingleflightMode::Joined);
        assert!(matches!(leader.1, Err(ClientError::Cache(_))));
        assert!(matches!(waiter.1, Err(ClientError::Cache(_))));

        let calls_for_retry = Arc::clone(&calls);
        let retry = singleflight
            .run("alpha".to_string(), move || async move {
                calls_for_retry.fetch_add(1, Ordering::SeqCst);
                Ok(17)
            })
            .await;
        assert_eq!(retry.0, SingleflightMode::Leader);
        assert_eq!(retry.1.expect("retry result"), 17);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn successful_leader_removes_entry_without_caching_result() {
        let singleflight = Singleflight::<String, u64>::default();
        let calls = Arc::new(AtomicUsize::new(0));
        let first_calls = Arc::clone(&calls);
        let first = singleflight
            .run("alpha".to_string(), move || async move {
                first_calls.fetch_add(1, Ordering::SeqCst);
                Ok(19)
            })
            .await;
        let second_calls = Arc::clone(&calls);
        let second = singleflight
            .run("alpha".to_string(), move || async move {
                second_calls.fetch_add(1, Ordering::SeqCst);
                Ok(23)
            })
            .await;

        assert_eq!(first.0, SingleflightMode::Leader);
        assert_eq!(first.1.expect("first result"), 19);
        assert_eq!(second.0, SingleflightMode::Leader);
        assert_eq!(second.1.expect("second result"), 23);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn same_key_callers_share_one_in_flight_result() {
        let singleflight = Singleflight::<String, u64>::default();
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::with_capacity(4);
        for _ in 0..4 {
            let singleflight = singleflight.clone();
            let started = Arc::clone(&started);
            let release = Arc::clone(&release);
            let calls = Arc::clone(&calls);
            tasks.push(tokio::spawn(async move {
                singleflight
                    .run("alpha".to_string(), move || async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        started.notify_waiters();
                        release.notified().await;
                        Ok(29)
                    })
                    .await
            }));
        }
        started.notified().await;
        tokio::task::yield_now().await;
        release.notify_waiters();

        let mut leaders = 0usize;
        for task in tasks {
            let (mode, result) = task.await.expect("task");
            if mode == SingleflightMode::Leader {
                leaders += 1;
            }
            assert_eq!(result.expect("result"), 29);
        }
        assert_eq!(leaders, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn different_keys_do_not_share_results() {
        let singleflight = Singleflight::<String, u64>::default();
        let first = {
            let singleflight = singleflight.clone();
            tokio::spawn(async move { singleflight.run("alpha".to_string(), || async { Ok(31) }).await })
        };
        let second = tokio::spawn(async move { singleflight.run("beta".to_string(), || async { Ok(37) }).await });

        let first = first.await.expect("first");
        let second = second.await.expect("second");
        assert_eq!(first.0, SingleflightMode::Leader);
        assert_eq!(first.1.expect("first result"), 31);
        assert_eq!(second.0, SingleflightMode::Leader);
        assert_eq!(second.1.expect("second result"), 37);
    }
}
