// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Pins and reclamation fences for local block access.

use crate::error::{WorkerError, WorkerResult};
use crate::report::BlockReportChangeTracker;
use crate::store::block::BlockIdentity;
use beryl_common::error::rpc::{ErrorKind, WorkerErrorKind};
use beryl_types::ids::BlockId;
use beryl_types::GroupName;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BlockAccessState {
    Available { pins: usize },
    Reclaiming { pins: usize, operation_active: bool },
}

/// Coordinates access pins and destructive block lifecycle transitions.
///
/// `changed` wakes lifecycle waiters. `block_report_changes` retains the exact
/// identities whose reportable state changed before any notification is lost.
#[derive(Debug)]
pub(crate) struct BlockAccessRegistry {
    states: Mutex<HashMap<BlockIdentity, BlockAccessState>>,
    changed: Notify,
    block_report_changes: BlockReportChangeTracker,
}

/// RAII guard that keeps a Ready block available for one complete read RPC.
///
/// The guard is acquired before local metadata validation so cleanup cannot pass
/// between validation and response-stream ownership. A blocking read clones the
/// guard so cancellation cannot release reclamation before filesystem IO exits.
#[derive(Clone, Debug)]
pub(crate) struct BlockPin {
    _inner: Arc<BlockPinInner>,
}

impl BlockPin {
    /// Checked after write registration to close the pending-authorization/reclaim race.
    pub(crate) fn is_reclaiming(&self) -> bool {
        matches!(
            self._inner
                .registry
                .states
                .lock()
                .expect("block access state poisoned")
                .get(&self._inner.key),
            Some(BlockAccessState::Reclaiming { .. })
        )
    }
}

#[derive(Debug)]
struct BlockPinInner {
    registry: Arc<BlockAccessRegistry>,
    key: BlockIdentity,
}

impl Drop for BlockPinInner {
    fn drop(&mut self) {
        self.registry.release_pin(&self.key);
    }
}

/// Exclusive permission to reclaim one local block after all prior readers exit.
///
/// A failed or cancelled operation leaves the block in `Reclaiming` so new
/// readers remain rejected and a later cleanup retry can safely resume.
#[derive(Debug)]
pub(crate) struct ReclaimPermit {
    registry: Arc<BlockAccessRegistry>,
    key: BlockIdentity,
    completed: bool,
}

impl ReclaimPermit {
    /// Waits after new admission is closed and existing writers have been retired.
    pub(crate) async fn wait_for_pins(&self) {
        loop {
            let notified = self.registry.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let pins = match self
                .registry
                .states
                .lock()
                .expect("block access state poisoned")
                .get(&self.key)
            {
                Some(BlockAccessState::Reclaiming { pins, .. }) => *pins,
                _ => unreachable!("reclaim permit retains its reclaiming state"),
            };
            if pins == 0 {
                return;
            }
            notified.await;
        }
    }

    /// Completes reclamation and removes the transient lifecycle entry.
    ///
    /// Removing the entry also wakes block reporting after `Reclaiming` can no
    /// longer override a missing filesystem block as `Deleting`.
    pub(crate) fn complete(mut self) {
        self.registry.complete_reclaim(&self.key);
        self.completed = true;
    }
}

impl Drop for ReclaimPermit {
    fn drop(&mut self) {
        if !self.completed {
            self.registry.release_reclaim_operation(&self.key);
        }
    }
}

impl BlockAccessRegistry {
    pub(crate) fn new(group_name: GroupName) -> Self {
        Self {
            states: Mutex::new(HashMap::new()),
            changed: Notify::new(),
            block_report_changes: BlockReportChangeTracker::new(group_name),
        }
    }

    /// Atomically pins an available block or rejects a read after reclaim starts.
    pub(crate) fn pin_block(self: &Arc<Self>, group_name: &GroupName, block_id: BlockId) -> WorkerResult<BlockPin> {
        let key = BlockIdentity {
            group_name: group_name.clone(),
            block_id,
        };
        let mut states = self.states.lock().expect("block access state poisoned");
        match states.get_mut(&key) {
            Some(BlockAccessState::Available { pins }) => {
                *pins = pins.checked_add(1).expect("block read pin count overflow");
            }
            Some(BlockAccessState::Reclaiming { .. }) => {
                return Err(WorkerError::RefreshMetadata {
                    kind: ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
                    message: format!(
                        "local block reclamation has started: group_name={}, block_id={}",
                        key.group_name, key.block_id
                    ),
                });
            }
            None => {
                states.insert(key.clone(), BlockAccessState::Available { pins: 1 });
            }
        }
        drop(states);
        Ok(BlockPin {
            _inner: Arc::new(BlockPinInner {
                registry: Arc::clone(self),
                key,
            }),
        })
    }

    /// Starts or resumes reclamation and waits for all previously pinned readers.
    pub(crate) fn begin_reclaim(
        self: &Arc<Self>,
        group_name: &GroupName,
        block_id: BlockId,
    ) -> WorkerResult<ReclaimPermit> {
        let key = BlockIdentity {
            group_name: group_name.clone(),
            block_id,
        };
        {
            let mut states = self.states.lock().expect("block access state poisoned");
            match states.get_mut(&key) {
                Some(BlockAccessState::Available { pins }) => {
                    let pins = *pins;
                    states.insert(
                        key.clone(),
                        BlockAccessState::Reclaiming {
                            pins,
                            operation_active: true,
                        },
                    );
                }
                Some(BlockAccessState::Reclaiming {
                    operation_active: true, ..
                }) => {
                    return Err(WorkerError::Unavailable(
                        "local block reclamation is already running".into(),
                    ));
                }
                Some(BlockAccessState::Reclaiming { operation_active, .. }) => {
                    *operation_active = true;
                }
                None => {
                    states.insert(
                        key.clone(),
                        BlockAccessState::Reclaiming {
                            pins: 0,
                            operation_active: true,
                        },
                    );
                }
            }
        }
        self.block_report_changes.record(&key.group_name, key.block_id);

        let permit = ReclaimPermit {
            registry: Arc::clone(self),
            key,
            completed: false,
        };
        Ok(permit)
    }

    fn release_pin(&self, key: &BlockIdentity) {
        let mut states = self.states.lock().expect("block access state poisoned");
        let mut remove = false;
        {
            let state = states.get_mut(key).expect("live pin retains its block state");
            match state {
                BlockAccessState::Available { pins } => {
                    *pins = pins.checked_sub(1).expect("available block read pin underflow");
                    remove = *pins == 0;
                }
                BlockAccessState::Reclaiming { pins, .. } => {
                    *pins = pins.checked_sub(1).expect("reclaiming block read pin underflow");
                }
            }
        }
        if remove {
            states.remove(key);
        }
        drop(states);
        self.changed.notify_waiters();
    }

    fn release_reclaim_operation(&self, key: &BlockIdentity) {
        let mut states = self.states.lock().expect("block access state poisoned");
        let Some(BlockAccessState::Reclaiming { operation_active, .. }) = states.get_mut(key) else {
            unreachable!("reclaim permit retains its reclaiming state");
        };
        *operation_active = false;
        drop(states);
        self.changed.notify_waiters();
    }

    /// Clears a completed reclaim fence before advertising the lifecycle change.
    fn complete_reclaim(&self, key: &BlockIdentity) {
        let mut states = self.states.lock().expect("block access state poisoned");
        match states.get(key) {
            Some(BlockAccessState::Reclaiming { pins: 0, .. }) => {
                states.remove(key);
            }
            Some(BlockAccessState::Reclaiming { pins, .. }) => {
                panic!("completed block reclamation with {pins} active access pins");
            }
            _ => unreachable!("reclaim permit retains its reclaiming state"),
        }
        drop(states);
        self.changed.notify_waiters();
        self.block_report_changes.record(&key.group_name, key.block_id);
    }

    /// Snapshots exact identities currently fenced from new access for reporting.
    pub(crate) fn reclaiming_blocks(&self, group_name: &GroupName) -> Vec<BlockId> {
        let states = self.states.lock().expect("block access state poisoned");
        states
            .iter()
            .filter_map(|(key, state)| match state {
                BlockAccessState::Reclaiming { .. } if &key.group_name == group_name => Some(key.block_id),
                _ => None,
            })
            .collect()
    }

    /// Whether one block is fenced for reclamation.
    pub(crate) fn is_reclaiming(&self, group_name: &GroupName, block_id: BlockId) -> bool {
        let states = self.states.lock().expect("block access state poisoned");
        matches!(
            states.get(&BlockIdentity {
                group_name: group_name.clone(),
                block_id,
            }),
            Some(BlockAccessState::Reclaiming { .. })
        )
    }

    /// Returns the retained reclaim-lifecycle change source.
    pub(crate) fn block_report_changes(&self) -> &BlockReportChangeTracker {
        &self.block_report_changes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use beryl_types::{BlockIndex, InodeId};
    use std::time::Duration;

    #[tokio::test]
    async fn reclaim_drains_shared_io_pins_and_failure_keeps_admission_closed() {
        let manager = Arc::new(BlockAccessRegistry::new(GroupName::parse("root").unwrap()));
        let group = GroupName::parse("root").unwrap();
        let id = BlockId::new(InodeId::new(7), BlockIndex::new(3));
        let pin = manager.pin_block(&group, id).unwrap();
        let io_pin = pin.clone();
        let permit = manager.begin_reclaim(&group, id).unwrap();
        assert!(pin.is_reclaiming());
        assert!(manager.pin_block(&group, id).is_err());
        assert!(manager.begin_reclaim(&group, id).is_err());
        drop(pin);
        assert!(tokio::time::timeout(Duration::from_millis(20), permit.wait_for_pins())
            .await
            .is_err());
        drop(io_pin);
        permit.wait_for_pins().await;
        drop(permit); // A cancelled destructive operation retains exclusion.
        assert!(manager.pin_block(&group, id).is_err());
        let retry = manager.begin_reclaim(&group, id).unwrap();
        retry.wait_for_pins().await;
        retry.complete();
        assert!(manager.pin_block(&group, id).is_ok());
    }
}
