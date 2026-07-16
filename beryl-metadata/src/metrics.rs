// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Process-local metadata counters shared by metadata subsystems.
//!
//! The common observability layer owns exported metrics. This module only holds
//! in-process state used by metadata readiness, maintenance, and MetadataFileSystem paths.

use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::Arc;

// Dedup metrics are updated by Raft storage/state-machine paths. They remain
// process-wide because dedup is an authority-wide apply concern.
pub(crate) static DEDUP_LOOKUP_HIT_TOTAL: AtomicU64 = AtomicU64::new(0);
pub(crate) static DEDUP_LOOKUP_MISS_TOTAL: AtomicU64 = AtomicU64::new(0);
pub(crate) static DEDUP_LOOKUP_MISMATCH_TOTAL: AtomicU64 = AtomicU64::new(0);
pub(crate) static DEDUP_STORE_ENTRIES_GAUGE: AtomicU64 = AtomicU64::new(0);

/// Process-local metadata counters.
#[derive(Clone)]
pub struct MetadataMetrics {
    // Root readiness.
    pub(crate) root_ready: Arc<AtomicUsize>,
    pub(crate) root_wait_attempts: Arc<AtomicU64>,
    pub(crate) root_wait_elapsed_ms: Arc<AtomicU64>,

    // Filesystem routing and Raft append guardrails.
    pub(crate) fs_write_routed_total: Arc<AtomicU64>,
    pub(crate) fs_write_cross_mount_rename_exdev_total: Arc<AtomicU64>,
    pub(crate) fs_write_mount_epoch_mismatch_total: Arc<AtomicU64>,
    pub(crate) fs_raft_appends_total: Arc<AtomicU64>,
    pub(crate) fs_raft_appends_create: Arc<AtomicU64>,
    pub(crate) fs_raft_appends_mkdir: Arc<AtomicU64>,
    pub(crate) fs_raft_appends_unlink: Arc<AtomicU64>,
    pub(crate) fs_raft_appends_directory_delete: Arc<AtomicU64>,
    pub(crate) fs_raft_appends_rename: Arc<AtomicU64>,
    pub(crate) fs_raft_appends_setattr: Arc<AtomicU64>,
}

impl MetadataMetrics {
    pub fn new() -> Self {
        Self {
            root_ready: Arc::new(AtomicUsize::new(0)),
            root_wait_attempts: Arc::new(AtomicU64::new(0)),
            root_wait_elapsed_ms: Arc::new(AtomicU64::new(0)),
            fs_write_routed_total: Arc::new(AtomicU64::new(0)),
            fs_write_cross_mount_rename_exdev_total: Arc::new(AtomicU64::new(0)),
            fs_write_mount_epoch_mismatch_total: Arc::new(AtomicU64::new(0)),
            fs_raft_appends_total: Arc::new(AtomicU64::new(0)),
            fs_raft_appends_create: Arc::new(AtomicU64::new(0)),
            fs_raft_appends_mkdir: Arc::new(AtomicU64::new(0)),
            fs_raft_appends_unlink: Arc::new(AtomicU64::new(0)),
            fs_raft_appends_directory_delete: Arc::new(AtomicU64::new(0)),
            fs_raft_appends_rename: Arc::new(AtomicU64::new(0)),
            fs_raft_appends_setattr: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl Default for MetadataMetrics {
    fn default() -> Self {
        Self::new()
    }
}
