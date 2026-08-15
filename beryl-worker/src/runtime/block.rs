// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Block runtime metadata, validation, and local access lifecycle boundary.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, WorkerErrorKind};
use beryl_types::ids::BlockId;
use beryl_types::layout::BlockShape;
use beryl_types::GroupName;
use tokio::sync::Notify;

use crate::data::core::{ReadBlockRequest, WorkerCoreResult};
use crate::error::WorkerError;
use crate::store::block::{BlockState, LocalBlockStore};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct BlockAccessKey {
    group_name: GroupName,
    block_id: BlockId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BlockAccessState {
    Available {
        pins: usize,
    },
    Reclaiming {
        pins: usize,
        operation_active: bool,
        block_stamp: u64,
    },
}

/// Coordinates reader pins and destructive block lifecycle transitions.
///
/// `changed` wakes lifecycle waiters. `block_report_changed` is emitted only
/// after final reclaim state is cleared so reporting can observe removal.
#[derive(Debug, Default)]
struct BlockAccessRegistry {
    states: Mutex<HashMap<BlockAccessKey, BlockAccessState>>,
    changed: Notify,
    block_report_changed: Notify,
}

/// Exact block version currently excluded from new readers for reclamation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReclaimingBlock {
    pub block_id: BlockId,
    pub block_stamp: u64,
}

/// RAII guard that keeps a Ready block available for one complete read RPC.
///
/// The guard is acquired before local metadata validation so cleanup cannot pass
/// between validation and response-stream ownership. A blocking read clones the
/// guard so cancellation cannot release reclamation before filesystem IO exits.
#[derive(Clone, Debug)]
pub(crate) struct ReadPin {
    _inner: Arc<ReadPinInner>,
}

#[derive(Debug)]
struct ReadPinInner {
    registry: Arc<BlockAccessRegistry>,
    key: BlockAccessKey,
}

impl Drop for ReadPinInner {
    fn drop(&mut self) {
        self.registry.release_read(&self.key);
    }
}

/// Exclusive permission to reclaim one local block after all prior readers exit.
///
/// A failed or cancelled operation leaves the block in `Reclaiming` so new
/// readers remain rejected and a later cleanup retry can safely resume.
#[derive(Debug)]
pub(crate) struct ReclaimPermit {
    registry: Arc<BlockAccessRegistry>,
    key: BlockAccessKey,
    completed: bool,
}

impl ReclaimPermit {
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
    /// Atomically pins an available block or rejects a read after reclaim starts.
    fn pin_read(self: &Arc<Self>, key: BlockAccessKey) -> WorkerCoreResult<ReadPin> {
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
        Ok(ReadPin {
            _inner: Arc::new(ReadPinInner {
                registry: Arc::clone(self),
                key,
            }),
        })
    }

    /// Starts or resumes reclamation and waits for all previously pinned readers.
    async fn begin_reclaim(
        self: &Arc<Self>,
        key: BlockAccessKey,
        expected_block_stamp: u64,
    ) -> WorkerCoreResult<ReclaimPermit> {
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
                            block_stamp: expected_block_stamp,
                        },
                    );
                }
                Some(BlockAccessState::Reclaiming {
                    operation_active: true,
                    block_stamp,
                    ..
                }) => {
                    if *block_stamp != expected_block_stamp {
                        return Err(WorkerError::RefreshMetadata {
                            kind: ErrorKind::Worker(WorkerErrorKind::BlockStampMismatch),
                            message: format!(
                                "local block reclamation already targets a different stamp: group_name={}, block_id={}, requested={}, active={}",
                                key.group_name, key.block_id, expected_block_stamp, block_stamp
                            ),
                        });
                    }
                    return Err(WorkerError::Unavailable(format!(
                        "local block reclamation is already running: group_name={}, block_id={}",
                        key.group_name, key.block_id
                    )));
                }
                Some(BlockAccessState::Reclaiming {
                    operation_active,
                    block_stamp,
                    ..
                }) => {
                    if *block_stamp != expected_block_stamp {
                        return Err(WorkerError::RefreshMetadata {
                            kind: ErrorKind::Worker(WorkerErrorKind::BlockStampMismatch),
                            message: format!(
                                "local block reclamation is fenced by a different stamp: group_name={}, block_id={}, requested={}, reclaiming={}",
                                key.group_name, key.block_id, expected_block_stamp, block_stamp
                            ),
                        });
                    }
                    *operation_active = true;
                }
                None => {
                    states.insert(
                        key.clone(),
                        BlockAccessState::Reclaiming {
                            pins: 0,
                            operation_active: true,
                            block_stamp: expected_block_stamp,
                        },
                    );
                }
            }
        }

        let permit = ReclaimPermit {
            registry: Arc::clone(self),
            key,
            completed: false,
        };
        loop {
            let notified = self.changed.notified();
            let pins = {
                let states = self.states.lock().expect("block access state poisoned");
                match states.get(&permit.key) {
                    Some(BlockAccessState::Reclaiming { pins, .. }) => *pins,
                    _ => 0,
                }
            };
            if pins == 0 {
                return Ok(permit);
            }
            notified.await;
        }
    }

    fn release_read(&self, key: &BlockAccessKey) {
        let mut states = self.states.lock().expect("block access state poisoned");
        let mut remove = false;
        if let Some(state) = states.get_mut(key) {
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

    fn release_reclaim_operation(&self, key: &BlockAccessKey) {
        let mut states = self.states.lock().expect("block access state poisoned");
        if let Some(BlockAccessState::Reclaiming { operation_active, .. }) = states.get_mut(key) {
            *operation_active = false;
        }
        drop(states);
        self.changed.notify_waiters();
    }

    /// Clears a completed reclaim fence before advertising the lifecycle change.
    fn complete_reclaim(&self, key: &BlockAccessKey) {
        let mut states = self.states.lock().expect("block access state poisoned");
        match states.get(key) {
            Some(BlockAccessState::Reclaiming { pins: 0, .. }) => {
                states.remove(key);
            }
            Some(BlockAccessState::Reclaiming { pins, .. }) => {
                panic!("completed block reclamation with {pins} active read pins");
            }
            _ => {}
        }
        drop(states);
        self.changed.notify_waiters();
        self.block_report_changed.notify_one();
    }

    /// Snapshots exact versions currently fenced from new readers for reporting.
    fn reclaiming_blocks(&self, group_name: &GroupName) -> Vec<ReclaimingBlock> {
        let states = self.states.lock().expect("block access state poisoned");
        let mut blocks = states
            .iter()
            .filter_map(|(key, state)| match state {
                BlockAccessState::Reclaiming { block_stamp, .. } if &key.group_name == group_name => {
                    Some(ReclaimingBlock {
                        block_id: key.block_id,
                        block_stamp: *block_stamp,
                    })
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        blocks.sort_by_key(|block| (block.block_id.inode_id.as_raw(), block.block_id.index.as_raw()));
        blocks
    }

    /// Waits until completed reclamation may change the reportable block view.
    async fn wait_for_block_report_change(&self) {
        self.block_report_changed.notified().await;
    }
}

/// Block-level facade for open and commit decisions.
///
/// The manager owns block metadata checks, stamp validation, range validation,
/// fencing decisions, and reader-versus-reclaimer lifecycle coordination. It
/// does not perform block data reads or writes.
#[derive(Clone, Debug)]
pub struct BlockManager {
    /// Transport frame payload size used when a caller does not request one.
    /// This controls network batching and does not define StorageChunk size.
    default_frame_size: u32,
    /// Upper bound for Worker-selected read response payload size.
    max_frame_size: u32,
    access: Arc<BlockAccessRegistry>,
}

impl BlockManager {
    pub const DEFAULT_FRAME_SIZE: u32 = 1024 * 1024;
    pub const MAX_FRAME_SIZE: u32 = beryl_proto::MAX_WORKER_DATA_FRAME_SIZE;
    pub fn new(default_frame_size: u32, max_frame_size: u32) -> Self {
        Self {
            default_frame_size,
            max_frame_size,
            access: Arc::new(BlockAccessRegistry {
                states: Mutex::new(HashMap::new()),
                changed: Notify::new(),
                block_report_changed: Notify::new(),
            }),
        }
    }

    pub const fn default_frame_size(&self) -> u32 {
        self.default_frame_size
    }

    pub const fn max_frame_size(&self) -> u32 {
        self.max_frame_size
    }

    /// Pins a block before read validation and holds it through the `ReadBlock` response lifetime.
    pub(crate) fn pin_read(&self, group_name: &GroupName, block_id: BlockId) -> WorkerCoreResult<ReadPin> {
        self.access.pin_read(BlockAccessKey {
            group_name: group_name.clone(),
            block_id,
        })
    }

    /// Prevents new readers and waits for existing `ReadBlock` pins before cleanup.
    pub(crate) async fn begin_reclaim(
        &self,
        group_name: &GroupName,
        block_id: BlockId,
        expected_block_stamp: u64,
    ) -> WorkerCoreResult<ReclaimPermit> {
        self.access
            .begin_reclaim(
                BlockAccessKey {
                    group_name: group_name.clone(),
                    block_id,
                },
                expected_block_stamp,
            )
            .await
    }

    /// Lists exact block versions currently fenced from new readers.
    pub(crate) fn reclaiming_blocks(&self, group_name: &GroupName) -> Vec<ReclaimingBlock> {
        self.access.reclaiming_blocks(group_name)
    }

    /// Waits for a completed reclaim lifecycle transition.
    pub(crate) async fn wait_for_block_report_change(&self) {
        self.access.wait_for_block_report_change().await;
    }

    /// Validates local Ready state against metadata facts while the caller holds a read pin.
    pub(crate) fn validate_read(
        &self,
        store: &(dyn LocalBlockStore + Send + Sync),
        req: &ReadBlockRequest,
    ) -> WorkerCoreResult<()> {
        self.validate_read_request(req)?;
        let meta = match store.load_meta(&req.group_name, req.block_id) {
            Ok(meta) => meta,
            Err(WorkerError::NotFound(message)) => {
                return Err(Self::refresh_metadata(
                    ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
                    format!("local block is not available for read: {message}"),
                ));
            }
            Err(error) => return Err(error),
        };
        if meta.visibility.block_state != BlockState::Ready {
            return Err(Self::refresh_metadata(
                ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
                format!(
                    "local block is not Ready: group_name={}, block_id={}, state={:?}",
                    req.group_name, req.block_id, meta.visibility.block_state
                ),
            ));
        }
        if req.block_stamp != meta.visibility.block_stamp {
            return Err(Self::refresh_metadata(
                ErrorKind::Worker(WorkerErrorKind::BlockStampMismatch),
                format!(
                    "block stamp mismatch: group_name={}, block_id={}, requested={}, local={}",
                    req.group_name, req.block_id, req.block_stamp, meta.visibility.block_stamp
                ),
            ));
        }
        if req.block_format_id != meta.format.format_id
            || req.block_size != meta.format.block_size
            || u64::from(req.chunk_size) != meta.format.chunk_size
            || req.effective_len != meta.source.effective_len
        {
            return Err(Self::refresh_metadata(
                ErrorKind::Metadata(MetadataErrorKind::StaleState),
                format!(
                    "block layout mismatch: group_name={}, block_id={}, requested_format={}, local_format={}, requested_block_size={}, local_block_size={}, requested_chunk_size={}, local_chunk_size={}, requested_effective_len={}, local_effective_len={}",
                    req.group_name,
                    req.block_id,
                    req.block_format_id.as_raw(),
                    meta.format.format_id.as_raw(),
                    req.block_size,
                    meta.format.block_size,
                    req.chunk_size,
                    meta.format.chunk_size,
                    req.effective_len,
                    meta.source.effective_len
                ),
            ));
        }

        let range_end = req
            .byte_range
            .offset
            .checked_add(u64::from(req.byte_range.len))
            .ok_or_else(|| WorkerError::InvalidArgument("byte range offset overflow".to_string()))?;
        if req.byte_range.offset > meta.source.effective_len || range_end > meta.source.effective_len {
            return Err(WorkerError::InvalidArgument(format!(
                "byte range exceeds effective block length: group_name={}, block_id={}, offset={}, len={}, effective_len={}",
                req.group_name, req.block_id, req.byte_range.offset, req.byte_range.len, meta.source.effective_len
            )));
        }

        Ok(())
    }

    /// Rejects malformed or internally inconsistent read authority before pinning.
    pub(crate) fn validate_read_request(&self, req: &ReadBlockRequest) -> WorkerCoreResult<()> {
        if req.block_stamp == 0 {
            return Err(WorkerError::InvalidArgument(
                "block_stamp must be metadata-assigned and non-zero".to_string(),
            ));
        }
        BlockShape::new(req.block_format_id, req.block_size, req.chunk_size, req.effective_len)
            .map_err(|err| WorkerError::InvalidArgument(err.to_string()))?;

        let range_end = req
            .byte_range
            .offset
            .checked_add(u64::from(req.byte_range.len))
            .ok_or_else(|| WorkerError::InvalidArgument("byte range offset overflow".to_string()))?;
        if req.byte_range.offset > req.effective_len || range_end > req.effective_len {
            return Err(WorkerError::InvalidArgument(format!(
                "byte range exceeds expected block length: offset={}, len={}, effective_len={}",
                req.byte_range.offset, req.byte_range.len, req.effective_len
            )));
        }
        Ok(())
    }

    fn refresh_metadata(kind: ErrorKind, message: String) -> WorkerError {
        WorkerError::RefreshMetadata { kind, message }
    }
}

impl Default for BlockManager {
    fn default() -> Self {
        Self::new(Self::DEFAULT_FRAME_SIZE, Self::MAX_FRAME_SIZE)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use beryl_types::ids::{BlockId, BlockIndex, InodeId};

    use super::*;

    fn group_name() -> GroupName {
        GroupName::parse("root").expect("valid test group")
    }

    fn block_id() -> BlockId {
        BlockId::new(InodeId::new(7), BlockIndex::new(3))
    }

    #[tokio::test]
    async fn reclaim_waits_for_existing_pin_and_rejects_new_readers() {
        let manager = BlockManager::default();
        let pin = manager.pin_read(&group_name(), block_id()).expect("initial pin");
        let in_progress_read = pin.clone();
        drop(pin);
        let reclaim_manager = manager.clone();
        let reclaim = tokio::spawn(async move {
            reclaim_manager
                .begin_reclaim(&group_name(), block_id(), 41)
                .await
                .expect("reclaim permit")
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match manager.pin_read(&group_name(), block_id()) {
                    Err(WorkerError::RefreshMetadata { .. }) => break,
                    Ok(extra_pin) => drop(extra_pin),
                    Err(other) => panic!("unexpected read pin error: {other:?}"),
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("reclaim should become visible");
        assert!(!reclaim.is_finished(), "reclaim must wait for the first read pin");

        drop(in_progress_read);
        let permit = tokio::time::timeout(Duration::from_secs(1), reclaim)
            .await
            .expect("reclaim should drain")
            .expect("reclaim task");
        assert!(matches!(
            manager.pin_read(&group_name(), block_id()),
            Err(WorkerError::RefreshMetadata { .. })
        ));

        permit.complete();
        drop(manager.pin_read(&group_name(), block_id()).expect("pin after reclaim"));
    }

    #[tokio::test]
    async fn failed_reclaim_remains_fenced_and_can_resume() {
        let manager = BlockManager::default();
        let permit = manager
            .begin_reclaim(&group_name(), block_id(), 41)
            .await
            .expect("first reclaim permit");
        drop(permit);

        assert!(matches!(
            manager.pin_read(&group_name(), block_id()),
            Err(WorkerError::RefreshMetadata { .. })
        ));
        manager
            .begin_reclaim(&group_name(), block_id(), 41)
            .await
            .expect("resumed reclaim permit")
            .complete();
        drop(manager.pin_read(&group_name(), block_id()).expect("pin after retry"));
    }
}
