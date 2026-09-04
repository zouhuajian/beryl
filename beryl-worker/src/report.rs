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
struct GroupChanges {
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
#[derive(Debug, Default)]
pub(crate) struct BlockReportChangeTracker {
    groups: Mutex<HashMap<GroupName, GroupChanges>>,
    changed: Notify,
}

impl BlockReportChangeTracker {
    /// Records a change only after its reportable local state becomes visible.
    pub(crate) fn record(&self, group_name: &GroupName, block_id: BlockId) {
        let mut groups = self.groups.lock().expect("block report change state poisoned");
        let changes = groups.entry(group_name.clone()).or_default();
        changes.revision = changes
            .revision
            .checked_add(1)
            .expect("block report change revision overflow");
        if !changes.continuity_lost {
            if changes.dirty.contains_key(&block_id) || changes.dirty.len() < MAX_REPORT_ENTRIES {
                changes.dirty.insert(block_id, changes.revision);
            } else {
                changes.dirty.clear();
                changes.continuity_lost = true;
            }
        }
        drop(groups);
        self.changed.notify_one();
    }

    /// Starts a full snapshot and returns the revision it must cover.
    ///
    /// Earlier dirty identities can be discarded because the subsequent full
    /// scan observes every local mutation committed before this cut. Changes
    /// racing with the scan receive a newer revision and remain for Delta.
    pub(crate) fn begin_full_snapshot(&self, group_name: &GroupName) -> u64 {
        let mut groups = self.groups.lock().expect("block report change state poisoned");
        let changes = groups.entry(group_name.clone()).or_default();
        let revision = changes.revision;
        changes.dirty.clear();
        changes.continuity_lost = false;
        revision
    }

    /// Returns the bounded dirty view without removing unacknowledged entries.
    pub(crate) fn snapshot(&self, group_name: &GroupName) -> Result<Vec<DirtyBlock>, ()> {
        let groups = self.groups.lock().expect("block report change state poisoned");
        let Some(changes) = groups.get(group_name) else {
            return Ok(Vec::new());
        };
        if changes.continuity_lost {
            return Err(());
        }
        let mut dirty = changes
            .dirty
            .iter()
            .map(|(&block_id, &revision)| DirtyBlock { block_id, revision })
            .collect::<Vec<_>>();
        dirty.sort_by_key(|entry| (entry.block_id.inode_id.as_raw(), entry.block_id.index.as_raw()));
        Ok(dirty)
    }

    /// Removes only revisions covered by an acknowledged immutable batch.
    pub(crate) fn acknowledge(&self, group_name: &GroupName, acknowledged: &[DirtyBlock]) {
        let mut groups = self.groups.lock().expect("block report change state poisoned");
        let Some(changes) = groups.get_mut(group_name) else {
            return;
        };
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
    pub(crate) fn acknowledge_full(&self, group_name: &GroupName, snapshot_revision: u64) -> bool {
        let mut groups = self.groups.lock().expect("block report change state poisoned");
        let Some(changes) = groups.get_mut(group_name) else {
            return true;
        };
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
        let tracker = BlockReportChangeTracker::default();
        let group = GroupName::parse("root").unwrap();
        tracker.record(&group, block_id(0));
        let first = tracker.snapshot(&group).unwrap();
        tracker.record(&group, block_id(0));

        tracker.acknowledge(&group, &first);
        assert_eq!(tracker.snapshot(&group).unwrap().len(), 1);

        for index in 1..=MAX_REPORT_ENTRIES {
            tracker.record(&group, block_id(index));
        }
        assert!(tracker.snapshot(&group).is_err());

        let full_cut = tracker.begin_full_snapshot(&group);
        tracker.record(&group, block_id(MAX_REPORT_ENTRIES + 1));
        assert!(tracker.acknowledge_full(&group, full_cut));
        assert_eq!(
            tracker.snapshot(&group).unwrap(),
            vec![DirtyBlock {
                block_id: block_id(MAX_REPORT_ENTRIES + 1),
                revision: full_cut + 1,
            }]
        );
    }
}
