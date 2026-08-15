// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Process-local ownership for active block writes.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use beryl_types::ids::BlockId;
use beryl_types::GroupName;
use parking_lot::Mutex;

use crate::observe;

/// Exact worker-local identity of staging state owned by one write RPC.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct BlockWriteKey {
    pub(crate) group_name: GroupName,
    pub(crate) block_id: BlockId,
}

struct BlockWriteEntry {
    io: Mutex<BlockWriteIoState>,
    _inflight: BlockWriteInflightGuard,
}

impl BlockWriteEntry {
    fn new() -> Self {
        Self {
            io: Mutex::new(BlockWriteIoState {
                retiring: false,
                inflight: 0,
                cleanup_running: false,
            }),
            _inflight: BlockWriteInflightGuard::new(),
        }
    }

    fn begin_io(self: &Arc<Self>) -> Option<BlockWriteIoGuard> {
        let mut io = self.io.lock();
        if io.retiring {
            return None;
        }
        io.inflight += 1;
        Some(BlockWriteIoGuard {
            entry: Arc::clone(self),
        })
    }

    fn retire(&self) {
        self.io.lock().retiring = true;
    }

    fn retire_and_claim_cleanup(&self, drain: bool) -> bool {
        let mut io = self.io.lock();
        if drain {
            io.retiring = true;
        }
        if !io.retiring || io.inflight != 0 || io.cleanup_running {
            return false;
        }
        io.cleanup_running = true;
        true
    }

    fn release_cleanup(&self) {
        let mut io = self.io.lock();
        debug_assert!(io.cleanup_running);
        io.cleanup_running = false;
    }
}

struct BlockWriteIoState {
    retiring: bool,
    inflight: usize,
    cleanup_running: bool,
}

struct BlockWriteRegistryState {
    writes: HashMap<BlockWriteKey, Arc<BlockWriteEntry>>,
    cleanup_order: VecDeque<BlockWriteKey>,
}

/// Prevents concurrent RPCs from owning the same staging block and retains
/// cancelled writes until process-owned cleanup releases their local files.
pub(crate) struct BlockWriteRegistry {
    inner: Mutex<BlockWriteRegistryState>,
}

/// RPC-owned registration. Dropping it before completion schedules cleanup
/// without performing filesystem IO from a cancellation path.
pub(crate) struct BlockWriteRegistration {
    registry: Arc<BlockWriteRegistry>,
    key: BlockWriteKey,
    entry: Arc<BlockWriteEntry>,
    completed: bool,
}

impl BlockWriteRegistration {
    /// Acquires an IO lease that keeps cleanup behind a detached blocking task.
    pub(crate) fn begin_io(&self) -> Option<BlockWriteIoGuard> {
        self.entry.begin_io()
    }

    /// Removes exactly this RPC's registry entry after terminal local work.
    pub(crate) fn complete(mut self) {
        self.registry.complete_registration(&self.key, &self.entry);
        self.completed = true;
    }
}

impl Drop for BlockWriteRegistration {
    fn drop(&mut self) {
        if !self.completed {
            self.entry.retire();
        }
    }
}

/// Lease moved into one blocking store operation. Cleanup cannot select the
/// owning write until the operation has actually exited, even if its async
/// caller was cancelled and dropped its `JoinHandle`.
pub(crate) struct BlockWriteIoGuard {
    entry: Arc<BlockWriteEntry>,
}

impl Drop for BlockWriteIoGuard {
    fn drop(&mut self) {
        let mut io = self.entry.io.lock();
        io.inflight = io.inflight.checked_sub(1).expect("block write IO guard is balanced");
    }
}

/// Exclusive cleanup claim for one retiring write. Dropping the claim after an
/// error or unwind makes the exact registry entry eligible for a later retry.
pub(crate) struct RetiringBlockWrite {
    pub(crate) key: BlockWriteKey,
    registry: Arc<BlockWriteRegistry>,
    entry: Arc<BlockWriteEntry>,
    claimed: bool,
}

impl RetiringBlockWrite {
    /// Removes the exact registry entry after its terminal store operation.
    pub(crate) fn complete(mut self) -> bool {
        let removed = self.registry.remove_exact(&self.key, &self.entry);
        if removed {
            self.claimed = false;
        }
        removed
    }
}

impl Drop for RetiringBlockWrite {
    fn drop(&mut self) {
        if self.claimed {
            self.registry.release_cleanup_claim(&self.key, &self.entry);
        }
    }
}

impl BlockWriteRegistry {
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(BlockWriteRegistryState {
                writes: HashMap::new(),
                cleanup_order: VecDeque::new(),
            }),
        }
    }

    /// Acquires exclusive process-local ownership without replacing an active
    /// write for the same group and block.
    pub(crate) fn register(self: &Arc<Self>, key: BlockWriteKey) -> Option<BlockWriteRegistration> {
        let mut inner = self.inner.lock();
        if inner.writes.contains_key(&key) {
            return None;
        }
        let entry = Arc::new(BlockWriteEntry::new());
        inner.writes.insert(key.clone(), Arc::clone(&entry));
        inner.cleanup_order.push_back(key.clone());
        Some(BlockWriteRegistration {
            registry: Arc::clone(self),
            key,
            entry,
            completed: false,
        })
    }

    /// Selects and atomically claims at most one bounded batch. Normal passes
    /// only claim cancelled writes; shutdown drain first retires examined writes.
    pub(crate) fn take_cleanup_batch(self: &Arc<Self>, limit: usize, drain: bool) -> Vec<RetiringBlockWrite> {
        let mut inner = self.inner.lock();
        let examined = limit.min(inner.cleanup_order.len());
        let mut selected = Vec::with_capacity(examined);
        for _ in 0..examined {
            let Some(key) = inner.cleanup_order.pop_front() else {
                break;
            };
            let Some(entry) = inner.writes.get(&key).cloned() else {
                continue;
            };
            if entry.retire_and_claim_cleanup(drain) {
                selected.push(RetiringBlockWrite {
                    key: key.clone(),
                    registry: Arc::clone(self),
                    entry,
                    claimed: true,
                });
            }
            inner.cleanup_order.push_back(key);
        }
        selected
    }

    fn release_cleanup_claim(&self, key: &BlockWriteKey, entry: &Arc<BlockWriteEntry>) {
        let inner = self.inner.lock();
        if inner
            .writes
            .get(key)
            .is_some_and(|registered| Arc::ptr_eq(registered, entry))
        {
            entry.release_cleanup();
        }
    }

    pub(crate) fn active_count(&self) -> usize {
        self.inner.lock().writes.len()
    }

    fn complete_registration(&self, key: &BlockWriteKey, entry: &Arc<BlockWriteEntry>) -> bool {
        let mut inner = self.inner.lock();
        if !inner
            .writes
            .get(key)
            .is_some_and(|registered| Arc::ptr_eq(registered, entry))
        {
            return false;
        }
        if entry.io.lock().cleanup_running {
            return false;
        }
        inner.writes.remove(key);
        if let Some(position) = inner.cleanup_order.iter().position(|queued| queued == key) {
            inner.cleanup_order.remove(position);
        }
        true
    }

    fn remove_exact(&self, key: &BlockWriteKey, entry: &Arc<BlockWriteEntry>) -> bool {
        let mut inner = self.inner.lock();
        if !inner
            .writes
            .get(key)
            .is_some_and(|registered| Arc::ptr_eq(registered, entry))
        {
            return false;
        }
        inner.writes.remove(key);
        if let Some(position) = inner.cleanup_order.iter().position(|queued| queued == key) {
            inner.cleanup_order.remove(position);
        }
        true
    }
}

struct BlockWriteInflightGuard;

impl BlockWriteInflightGuard {
    fn new() -> Self {
        observe::increment_stream_inflight("write");
        Self
    }
}

impl Drop for BlockWriteInflightGuard {
    fn drop(&mut self) {
        observe::decrement_stream_inflight("write");
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use beryl_types::ids::{BlockId, BlockIndex, InodeId};
    use beryl_types::GroupName;

    use super::{BlockWriteKey, BlockWriteRegistry};

    fn key() -> BlockWriteKey {
        BlockWriteKey {
            group_name: GroupName::parse("root").expect("group name"),
            block_id: BlockId::new(InodeId::new(7), BlockIndex::new(3)),
        }
    }

    #[test]
    fn cancellation_retires_exact_owner_and_allows_reuse_after_cleanup() {
        let registry = Arc::new(BlockWriteRegistry::new());
        let registration = registry.register(key()).expect("first owner");
        assert!(registry.register(key()).is_none());

        drop(registration);
        let candidates = registry.take_cleanup_batch(1, false);
        assert_eq!(candidates.len(), 1);
        assert!(candidates.into_iter().next().expect("cleanup claim").complete());
        assert!(registry.register(key()).is_some());
    }
}
