// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! In-process continuity tracking for incremental block reports.

use beryl_types::{BlockId, GroupName, MAX_REPORT_ENTRIES};
use std::collections::HashMap;
use std::sync::Mutex;
use tokio::sync::Notify;

/// One dirty block identity paired with the revision that selected it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DirtyBlock {
    pub(crate) block_id: BlockId,
    pub(crate) revision: u64,
}

#[derive(Debug, Default)]
struct BlockChanges {
    revision: u64,
    dirty: HashMap<BlockId, u64>,
    continuity_lost: bool,
}

/// Retains reportable block identities until Metadata acknowledges their state.
///
/// Notifications are deliberately separate from the retained dirty map: a
/// coalesced or missed wake-up cannot discard a local lifecycle transition.
/// Overflow fails closed by marking incremental continuity as lost, which
/// forces the reporter to establish a new full baseline.
#[derive(Debug)]
pub(crate) struct BlockReportChangeTracker {
    pub(crate) group_name: GroupName,
    changes: Mutex<BlockChanges>,
    changed: Notify,
}

impl BlockReportChangeTracker {
    pub(crate) fn new(group_name: GroupName) -> Self {
        Self {
            group_name,
            changes: Mutex::new(BlockChanges::default()),
            changed: Notify::new(),
        }
    }

    /// Records committed changes for the configured reporting group.
    /// Other groups can remain on disk but do not belong to this report stream.
    pub(crate) fn record(&self, group_name: &GroupName, block_id: BlockId) {
        if group_name != &self.group_name {
            return;
        }
        let mut changes = self.changes.lock().expect("block report change state poisoned");
        changes.revision = changes
            .revision
            .checked_add(1)
            .expect("block report change revision overflow");
        if !changes.continuity_lost {
            if changes.dirty.contains_key(&block_id) || changes.dirty.len() < MAX_REPORT_ENTRIES {
                let revision = changes.revision;
                changes.dirty.insert(block_id, revision);
            } else {
                changes.dirty.clear();
                changes.continuity_lost = true;
            }
        }
        drop(changes);
        self.changed.notify_one();
    }

    /// Starts a full snapshot and returns the revision it must cover.
    ///
    /// Earlier dirty identities can be discarded because the subsequent full
    /// scan observes every local mutation committed before this cut. Changes
    /// racing with the scan receive a newer revision and remain for Delta.
    pub(crate) fn begin_full_snapshot(&self) -> u64 {
        let mut changes = self.changes.lock().expect("block report change state poisoned");
        let revision = changes.revision;
        changes.dirty.clear();
        changes.continuity_lost = false;
        revision
    }

    /// Returns the bounded dirty view without removing unacknowledged entries.
    pub(crate) fn snapshot(&self) -> Result<Vec<DirtyBlock>, ()> {
        let changes = self.changes.lock().expect("block report change state poisoned");
        if changes.continuity_lost {
            return Err(());
        }
        let dirty = changes
            .dirty
            .iter()
            .map(|(&block_id, &revision)| DirtyBlock { block_id, revision })
            .collect::<Vec<_>>();
        Ok(dirty)
    }

    /// Removes only revisions covered by an acknowledged immutable batch.
    pub(crate) fn acknowledge(&self, acknowledged: &[DirtyBlock]) {
        let mut changes = self.changes.lock().expect("block report change state poisoned");
        for entry in acknowledged {
            if changes
                .dirty
                .get(&entry.block_id)
                .is_some_and(|revision| *revision <= entry.revision)
            {
                changes.dirty.remove(&entry.block_id);
            }
        }
    }

    /// Completes a full snapshot while retaining changes newer than its cut.
    pub(crate) fn acknowledge_full(&self, snapshot_revision: u64) -> bool {
        let mut changes = self.changes.lock().expect("block report change state poisoned");
        changes.dirty.retain(|_, revision| *revision > snapshot_revision);
        !changes.continuity_lost
    }

    /// Waits for a coalesced wake-up; retained dirty identities remain authoritative.
    pub(crate) async fn wait(&self) {
        self.changed.notified().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use beryl_types::ids::{BlockIndex, InodeId};

    fn block_id(index: usize) -> BlockId {
        BlockId::new(InodeId::new(1), BlockIndex::new(u32::try_from(index).unwrap()))
    }

    #[test]
    fn acknowledgement_preserves_newer_changes_and_full_recovers_overflow() {
        let group = GroupName::parse("root").unwrap();
        let tracker = BlockReportChangeTracker::new(group.clone());
        tracker.record(&GroupName::parse("other").unwrap(), block_id(0));
        assert!(tracker.snapshot().unwrap().is_empty());
        tracker.record(&group, block_id(0));
        let first = tracker.snapshot().unwrap();
        tracker.record(&group, block_id(0));

        tracker.acknowledge(&first);
        assert_eq!(tracker.snapshot().unwrap().len(), 1);

        for index in 1..=MAX_REPORT_ENTRIES {
            tracker.record(&group, block_id(index));
        }
        assert!(tracker.snapshot().is_err());

        let full_cut = tracker.begin_full_snapshot();
        tracker.record(&group, block_id(MAX_REPORT_ENTRIES + 1));
        assert!(tracker.acknowledge_full(full_cut));
        assert_eq!(
            tracker.snapshot().unwrap(),
            vec![DirtyBlock {
                block_id: block_id(MAX_REPORT_ENTRIES + 1),
                revision: full_cut + 1,
            }]
        );
    }
}
