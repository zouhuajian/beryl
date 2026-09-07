// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Leader-only proposal of bounded detached-root authority mutations.

use crate::config::NamespaceDeleteConfig;
use crate::error::{MetadataError, MetadataResult};
use crate::observe;
use crate::raft::{AppRaftNode, ApplySuccess, Command, DetachedRootReclaimResult, RocksDBStorage};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio_util::sync::CancellationToken;
use tracing::error;

/// Outcome of one maintenance pass before the service chooses its next delay.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DetachedRootReclaimPass {
    NotLeader,
    Idle,
    Applied(DetachedRootReclaimResult),
}

/// Selects durable markers for the serial reclaim proposal loop.
///
/// The selector is leader-local scheduling only. Marker authority, budgets,
/// idempotence, and all namespace changes remain in the Raft state machine.
pub(crate) struct DetachedRootReclaimer {
    raft_node: Arc<AppRaftNode>,
    storage: Arc<RocksDBStorage>,
    config: NamespaceDeleteConfig,
}

impl DetachedRootReclaimer {
    pub(crate) fn new(
        raft_node: Arc<AppRaftNode>,
        storage: Arc<RocksDBStorage>,
        config: NamespaceDeleteConfig,
    ) -> Self {
        Self {
            raft_node,
            storage,
            config,
        }
    }

    /// Run the single proposal loop with bounded exponential error backoff.
    pub(crate) async fn run(self: Arc<Self>, shutdown: CancellationToken) {
        let scan_interval = Duration::from_millis(self.config.scan_interval_ms);
        let retry_initial = Duration::from_millis(self.config.retry_initial_backoff_ms);
        let retry_max = Duration::from_millis(self.config.retry_max_backoff_ms);
        let mut retry_delay = retry_initial;
        loop {
            let delay = match tokio::select! {
                biased;
                _ = shutdown.cancelled() => return,
                result = self.reclaim_once() => result,
            } {
                Ok(_) => {
                    retry_delay = retry_initial;
                    scan_interval
                }
                Err(error) => {
                    error!(task = "detached_root_reclamation", %error, "Detached-root reclamation failed");
                    let current = retry_delay;
                    retry_delay = retry_delay.saturating_mul(2).min(retry_max);
                    current
                }
            };
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => return,
                _ = tokio::time::sleep(delay) => {}
            }
        }
    }

    /// Select candidates and await one bounded Raft proposal on the leader.
    async fn reclaim_once(&self) -> MetadataResult<DetachedRootReclaimPass> {
        let started = Instant::now();
        let result = self.reclaim_once_inner().await;
        match &result {
            Ok(DetachedRootReclaimPass::NotLeader) => {
                observe::record_detached_root_reclaim_pass("not_leader", 0, 0, started.elapsed().as_secs_f64());
            }
            Ok(DetachedRootReclaimPass::Idle) => {
                observe::record_detached_root_reclaim_pass("idle", 0, 0, started.elapsed().as_secs_f64());
            }
            Ok(DetachedRootReclaimPass::Applied(applied)) => {
                observe::record_detached_root_reclaim_pass(
                    "applied",
                    applied.processed_entries,
                    applied.logical_batch_bytes,
                    started.elapsed().as_secs_f64(),
                );
            }
            Err(_) => {
                observe::record_detached_root_reclaim_pass("error", 0, 0, started.elapsed().as_secs_f64());
            }
        }
        result
    }

    async fn reclaim_once_inner(&self) -> MetadataResult<DetachedRootReclaimPass> {
        if !self.raft_node.is_leader() {
            observe::set_detached_root_reclaim_candidates(0, false, 0.0);
            return Ok(DetachedRootReclaimPass::NotLeader);
        }

        let (roots, has_more) = self.storage.list_detached_roots(self.config.max_candidates as usize)?;
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let oldest_age_seconds = roots
            .iter()
            .map(|(_, marker)| now_ms.saturating_sub(marker.detached_at_ms))
            .max()
            .unwrap_or(0) as f64
            / 1_000.0;
        observe::set_detached_root_reclaim_candidates(roots.len(), has_more, oldest_age_seconds);
        if roots.is_empty() {
            return Ok(DetachedRootReclaimPass::Idle);
        }

        let response = self
            .raft_node
            .propose(Command::ReclaimDetachedRoots {
                candidate_root_inode_ids: roots.into_iter().map(|(inode_id, _)| inode_id).collect(),
                max_entries: self.config.max_entries,
                max_batch_bytes: self.config.max_batch_bytes,
            })
            .await?;
        match response {
            ApplySuccess::DetachedRootsReclaimed(result) => Ok(DetachedRootReclaimPass::Applied(result)),
            unexpected => Err(MetadataError::Internal(format!(
                "detached-root Raft command returned unexpected result: {unexpected:?}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RaftConfig;
    use crate::inode::InodeAttrs;
    use crate::mount::{MountTable, ROOT_INODE_ID};
    use crate::raft::AppRaftStateMachine;

    use beryl_types::ids::MountId;
    use beryl_types::GroupName;
    use metrics::{Counter, Gauge, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder, SharedString, Unit};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::runtime::Builder;

    async fn reclaimer(
        dir: &TempDir,
        initialize_leader: bool,
    ) -> (Arc<RocksDBStorage>, Arc<AppRaftNode>, DetachedRootReclaimer) {
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage)));
        let raft_node = Arc::new(
            AppRaftNode::new(
                1,
                Arc::clone(&storage),
                state_machine,
                Arc::new(MountTable::new()),
                &RaftConfig::default(),
            )
            .await
            .unwrap(),
        );
        if initialize_leader {
            raft_node
                .initialize_single_node("127.0.0.1:0".to_string())
                .await
                .unwrap();
            for _ in 0..100 {
                if raft_node.is_leader() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert!(raft_node.is_leader());
            let bootstrap = raft_node
                .propose(Command::BootstrapNamespace {
                    proposed_at_ms: 1,
                    group_name: GroupName::parse("root").unwrap(),
                })
                .await
                .unwrap();
            assert!(matches!(bootstrap, ApplySuccess::MountUpserted(_)));
        }
        let reclaimer = DetachedRootReclaimer::new(
            Arc::clone(&raft_node),
            Arc::clone(&storage),
            NamespaceDeleteConfig::default(),
        );
        (storage, raft_node, reclaimer)
    }

    async fn detach_empty_root(raft_node: &AppRaftNode, name: &str) -> beryl_types::ids::InodeId {
        let created = raft_node
            .propose(Command::CreateDirectory {
                proposed_at_ms: 2,
                root_inode_id: ROOT_INODE_ID,
                components: vec![name.to_string()],
                attrs: InodeAttrs::new(),
                recursive: false,
            })
            .await
            .unwrap();
        let ApplySuccess::DirectoryEnsured { inode_id: root_id, .. } = created else {
            panic!("create directory returned unexpected result: {created:?}");
        };
        let deleted = raft_node
            .propose(Command::Delete {
                proposed_at_ms: 3,
                mount_id: MountId::new(1),
                expected_mount_epoch: 1,
                mount_root_inode_id: ROOT_INODE_ID,
                relative_components: vec![name.to_string()],
                expected_inode_id: root_id,
                expected_file_lease_epoch: None,
                recursive: true,
            })
            .await
            .unwrap();
        assert!(matches!(deleted, ApplySuccess::DeleteApplied));
        root_id
    }

    #[tokio::test]
    async fn leader_reclaims_selected_empty_root_and_follower_does_not_propose() {
        let leader_dir = TempDir::new().unwrap();
        let (storage, raft_node, leader) = reclaimer(&leader_dir, true).await;
        let root_id = detach_empty_root(&raft_node, "detached").await;
        assert!(storage.get_detached_root(root_id).unwrap().is_some());

        let pass = leader.reclaim_once().await.unwrap();
        assert!(matches!(
            pass,
            DetachedRootReclaimPass::Applied(DetachedRootReclaimResult { completed_roots: 1, .. })
        ));
        assert!(storage.get_detached_root(root_id).unwrap().is_none());
        raft_node.shutdown().await.unwrap();

        let follower_dir = TempDir::new().unwrap();
        let (storage, _raft_node, follower) = reclaimer(&follower_dir, false).await;
        let last_applied = storage.load_raft_state().unwrap().last_applied_log_id;
        assert_eq!(
            follower.reclaim_once().await.unwrap(),
            DetachedRootReclaimPass::NotLeader
        );
        assert_eq!(storage.load_raft_state().unwrap().last_applied_log_id, last_applied);
        _raft_node.shutdown().await.unwrap();
    }

    #[test]
    fn run_waits_for_each_proposal_and_stops_on_cancellation() {
        let raft_runtime = Builder::new_current_thread().enable_all().build().unwrap();
        let loop_runtime = Builder::new_current_thread()
            .enable_all()
            .start_paused(true)
            .build()
            .unwrap();
        let dir = TempDir::new().unwrap();
        let (storage, raft_node, mut reclaimer) = raft_runtime.block_on(reclaimer(&dir, true));
        let first_root = raft_runtime.block_on(detach_empty_root(&raft_node, "first"));
        let second_root = raft_runtime.block_on(detach_empty_root(&raft_node, "second"));
        reclaimer.config.max_candidates = 1;
        let interval = Duration::from_millis(reclaimer.config.scan_interval_ms);
        let shutdown = CancellationToken::new();
        let mut run = Box::pin(Arc::new(reclaimer).run(shutdown.clone()));
        let recorder = ReclaimRecorder::default();
        let _recorder = metrics::set_default_local_recorder(&recorder);

        // Raft's current-thread runtime cannot process the queued command until
        // explicitly driven, while the real maintenance loop keeps being polled.
        loop_runtime.block_on(async {
            assert!(futures::poll!(run.as_mut()).is_pending());
            assert_eq!(recorder.proposals.0.load(Ordering::Relaxed), 1);
            for _ in 0..3 {
                tokio::time::advance(interval).await;
                assert!(futures::poll!(run.as_mut()).is_pending());
            }
            assert_eq!(recorder.proposals.0.load(Ordering::Relaxed), 1);
            assert_eq!(recorder.passes.load(Ordering::Relaxed), 0);
        });
        assert!(storage.get_detached_root(first_root).unwrap().is_some());
        assert!(storage.get_detached_root(second_root).unwrap().is_some());

        let deadline = Instant::now() + Duration::from_secs(5);
        while recorder.passes.load(Ordering::Relaxed) == 0 {
            assert!(Instant::now() < deadline, "first reclaim proposal did not complete");
            raft_runtime.block_on(tokio::task::yield_now());
            loop_runtime.block_on(async {
                assert!(futures::poll!(run.as_mut()).is_pending());
            });
        }
        assert!(storage.get_detached_root(first_root).unwrap().is_none());
        assert!(storage.get_detached_root(second_root).unwrap().is_some());

        loop_runtime.block_on(async {
            assert!(futures::poll!(run.as_mut()).is_pending());
            assert_eq!(recorder.passes.load(Ordering::Relaxed), 1);
            tokio::time::advance(interval / 2).await;
            assert!(futures::poll!(run.as_mut()).is_pending());
            assert_eq!(recorder.proposals.0.load(Ordering::Relaxed), 1);
            tokio::time::advance(interval / 2).await;
            assert!(futures::poll!(run.as_mut()).is_pending());
            assert_eq!(recorder.proposals.0.load(Ordering::Relaxed), 2);
            assert_eq!(recorder.passes.load(Ordering::Relaxed), 1);

            shutdown.cancel();
            assert!(futures::poll!(run.as_mut()).is_ready());
            tokio::time::advance(interval * 3).await;
            assert_eq!(recorder.proposals.0.load(Ordering::Relaxed), 2);
        });
        // Cancellation stops the selector, not a command already queued in Raft.
        raft_runtime.block_on(raft_node.shutdown()).unwrap();
    }

    #[derive(Default)]
    struct ReclaimRecorder {
        proposals: Arc<ProposalCount>,
        passes: Arc<AtomicU64>,
    }

    impl Recorder for ReclaimRecorder {
        fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}
        fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}
        fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
            if key.name() == observe::METADATA_DETACHED_ROOT_RECLAIM_PASSES_TOTAL {
                Counter::from_arc(Arc::clone(&self.passes))
            } else {
                Counter::noop()
            }
        }

        fn register_gauge(&self, _key: &Key, _metadata: &Metadata<'_>) -> Gauge {
            Gauge::noop()
        }

        fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
            if key.name() == observe::METADATA_RAFT_COMMAND_BYTES
                && key
                    .labels()
                    .any(|label| label.key() == "operation" && label.value() == "reclaim_detached_roots")
            {
                Histogram::from_arc(Arc::clone(&self.proposals))
            } else {
                Histogram::noop()
            }
        }
    }

    #[derive(Default)]
    struct ProposalCount(AtomicU64);

    impl HistogramFn for ProposalCount {
        fn record(&self, _value: f64) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
}
