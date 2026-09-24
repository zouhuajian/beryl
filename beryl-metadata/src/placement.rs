// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Metadata-side block placement planning.
//!
//! The planner consumes metadata-owned worker membership and report views. It
//! does not execute UFS loads, repair copies, worker commands, or report-state
//! mutations, and it does not define client-side placement policy.

use beryl_common::header::CallerContextFields;
use beryl_types::ids::{BlockId, WorkerId};
use beryl_types::{GroupName, Tier, TierFree, WorkerRunId};

const WRITE_TIER_ORDER: [Tier; 3] = [Tier::Nvme, Tier::Ssd, Tier::Hdd];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlacementOp {
    Read,
    Write,
}

/// Logical capacity and placement policy over Metadata-owned live Worker evidence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlacementRequest<'a> {
    pub group_name: GroupName,
    pub op: PlacementOp,
    pub block_id: BlockId,
    pub visible_len: u64,
    pub block_size: u32,
    pub caller: Option<CallerContextFields>,
    pub existing: &'a [ReportedBlockLocation],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReportedBlockLocation {
    pub tier: Tier,
    pub durable_len: u64,
    pub worker_id: WorkerId,
    pub worker_run_id: WorkerRunId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerPlacementView {
    pub worker_id: WorkerId,
    pub worker_run_id: Option<WorkerRunId>,
    pub endpoint: String,
    pub lease_valid: bool,
    pub host: Option<String>,
    pub tier_free: Vec<TierFree>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlacementWorker {
    pub worker_id: WorkerId,
    pub worker_run_id: WorkerRunId,
    pub endpoint: String,
    pub tier: Tier,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlacementStatus {
    Ok,
    NoLiveWorker,
    NoWritableTier,
    InsufficientCapacity,
    NoLiveReplica,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PlacementStats {
    pub live_count: usize,
    pub tier_count: usize,
    pub capacity_count: usize,
    pub max_free_bytes: u64,
    pub max_free_worker_id: Option<WorkerId>,
    pub max_free_tier: Option<Tier>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlacementPlan {
    pub workers: Vec<PlacementWorker>,
    pub status: PlacementStatus,
    pub stats: PlacementStats,
}

impl PlacementPlan {
    pub fn failure_message(&self, req: &PlacementRequest) -> String {
        let max_worker = self
            .stats
            .max_free_worker_id
            .map(|worker_id| worker_id.to_string())
            .unwrap_or_else(|| "-".to_string());
        let max_tier = self
            .stats
            .max_free_tier
            .map(|tier| tier.to_string())
            .unwrap_or_else(|| "-".to_string());
        format!(
            "placement failed: status={:?} group={} required={} policy=[{}] live={} tier_ok={} capacity_ok={} max_free={} max_worker={} max_tier={}",
            self.status,
            req.group_name,
            req.block_size,
            write_tier_policy_label(),
            self.stats.live_count,
            self.stats.tier_count,
            self.stats.capacity_count,
            self.stats.max_free_bytes,
            max_worker,
            max_tier
        )
    }
}

/// Select live replicas for reads or one writable target; local disk versions are Worker-owned.
pub fn plan_placement(req: &PlacementRequest, workers: &[WorkerPlacementView]) -> PlacementPlan {
    match req.op {
        PlacementOp::Read => choose_read(req, workers),
        PlacementOp::Write => choose_write_target(req, workers),
    }
}

fn choose_read(req: &PlacementRequest, workers: &[WorkerPlacementView]) -> PlacementPlan {
    let mut candidates = Vec::new();
    for location in req.existing {
        if location.durable_len < req.visible_len {
            continue;
        }
        let Some(worker) = workers.iter().find(|worker| worker.worker_id == location.worker_id) else {
            continue;
        };
        if worker.lease_valid
            && worker
                .worker_run_id
                .is_some_and(|worker_run_id| worker_run_id == location.worker_run_id)
        {
            candidates.push((worker, location.tier));
        }
    }
    sort_workers(req, &mut candidates);
    let selected = workers_from_views(candidates);
    let status = if selected.is_empty() {
        PlacementStatus::NoLiveReplica
    } else {
        PlacementStatus::Ok
    };
    PlacementPlan {
        workers: selected,
        status,
        stats: PlacementStats::default(),
    }
}

fn choose_write_target(req: &PlacementRequest, workers: &[WorkerPlacementView]) -> PlacementPlan {
    let mut stats = PlacementStats::default();
    let live_candidates: Vec<_> = workers.iter().filter(|worker| worker.lease_valid).collect();
    stats.live_count = live_candidates.len();
    if live_candidates.is_empty() {
        return PlacementPlan {
            workers: Vec::new(),
            status: PlacementStatus::NoLiveWorker,
            stats,
        };
    }

    let required_len = u64::from(req.block_size);
    for worker in &live_candidates {
        for tier in WRITE_TIER_ORDER {
            if let Some(free_bytes) = tier_free_bytes(worker, tier) {
                record_max_free(&mut stats, worker.worker_id, Some(tier), free_bytes);
            }
        }
    }

    let mut candidates = Vec::new();
    for worker in live_candidates {
        let mut has_persistent_tier = false;
        for tier in WRITE_TIER_ORDER {
            let Some(free_bytes) = tier_free_bytes(worker, tier) else {
                continue;
            };
            has_persistent_tier = true;
            if free_bytes >= required_len {
                candidates.push((worker, tier));
                break;
            }
        }
        if has_persistent_tier {
            stats.tier_count += 1;
        }
    }

    if stats.tier_count == 0 {
        return PlacementPlan {
            workers: Vec::new(),
            status: PlacementStatus::NoWritableTier,
            stats,
        };
    }

    stats.capacity_count = candidates.len();
    sort_write_candidates(req, &mut candidates);
    if candidates.is_empty() {
        return PlacementPlan {
            workers: Vec::new(),
            status: PlacementStatus::InsufficientCapacity,
            stats,
        };
    }

    candidates.truncate(1);
    PlacementPlan {
        workers: workers_from_views(candidates),
        status: PlacementStatus::Ok,
        stats,
    }
}

fn tier_free_bytes(worker: &WorkerPlacementView, tier: Tier) -> Option<u64> {
    worker
        .tier_free
        .iter()
        .filter(|entry| entry.tier == tier)
        .map(|entry| entry.free_bytes)
        .max()
}

fn record_max_free(stats: &mut PlacementStats, worker_id: WorkerId, tier: Option<Tier>, free_bytes: u64) {
    if free_bytes > stats.max_free_bytes || stats.max_free_worker_id.is_none() {
        stats.max_free_bytes = free_bytes;
        stats.max_free_worker_id = Some(worker_id);
        stats.max_free_tier = tier;
    }
}

fn sort_workers(req: &PlacementRequest, workers: &mut [(&WorkerPlacementView, Tier)]) {
    workers.sort_by_key(|(worker, _)| {
        let locality = req
            .caller
            .as_ref()
            .map(|caller| locality_rank(caller, worker))
            .unwrap_or(0);
        (
            locality,
            stable_order(&req.group_name, req.block_id, worker.worker_id),
            worker.worker_id.as_raw(),
        )
    });
}

fn sort_write_candidates(req: &PlacementRequest, candidates: &mut [(&WorkerPlacementView, Tier)]) {
    candidates.sort_by_key(|(worker, tier)| {
        let locality = req
            .caller
            .as_ref()
            .map(|caller| locality_rank(caller, worker))
            .unwrap_or(0);
        (
            write_tier_rank(*tier),
            locality,
            stable_order(&req.group_name, req.block_id, worker.worker_id),
            worker.worker_id.as_raw(),
        )
    });
}

fn workers_from_views(workers: Vec<(&WorkerPlacementView, Tier)>) -> Vec<PlacementWorker> {
    workers
        .into_iter()
        .map(|(worker, tier)| PlacementWorker {
            worker_id: worker.worker_id,
            worker_run_id: worker
                .worker_run_id
                .expect("live placement candidates have a registered run"),
            endpoint: worker.endpoint.clone(),
            tier,
        })
        .collect()
}

fn locality_rank(caller: &CallerContextFields, worker: &WorkerPlacementView) -> u8 {
    if matches_pair(caller.host(), &worker.host) || matches_pair(caller.ip(), &worker.host) {
        0
    } else {
        1
    }
}

fn matches_pair(left: Option<&str>, right: &Option<String>) -> bool {
    left.zip(right.as_deref())
        .map(|(left, right)| left == right)
        .unwrap_or(false)
}

fn stable_order(group_name: &GroupName, block_id: BlockId, worker_id: WorkerId) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325;
    for value in group_name.as_str().as_bytes() {
        hash ^= u64::from(*value);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    for value in [
        block_id.inode_id.as_raw(),
        u64::from(block_id.index.as_raw()),
        worker_id.as_raw(),
    ] {
        hash ^= value;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        hash ^= value.rotate_left(32);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn write_tier_rank(tier: Tier) -> u8 {
    match tier {
        Tier::Nvme => 0,
        Tier::Ssd => 1,
        Tier::Hdd => 2,
        Tier::Mem => 3,
    }
}

fn write_tier_policy_label() -> &'static str {
    "NVME,SSD,HDD"
}

#[cfg(test)]
mod tests {
    use super::*;
    use beryl_common::header::CallerContextFields;
    use beryl_types::ids::{BlockId, BlockIndex, InodeId, WorkerId};
    use beryl_types::{GroupName, Tier, TierFree, WorkerRunId};

    fn run_id(suffix: u32) -> WorkerRunId {
        format!("550e8400-e29b-41d4-a716-{suffix:012}")
            .parse()
            .expect("valid worker run id")
    }

    fn block(inode_id: u64, index: u32) -> BlockId {
        BlockId::new(InodeId::new(inode_id), BlockIndex::new(index))
    }

    fn group_name(raw: &str) -> GroupName {
        GroupName::parse(raw).unwrap()
    }

    fn worker(worker_id: u64, worker_run_id: WorkerRunId, host: &str) -> WorkerPlacementView {
        WorkerPlacementView {
            worker_id: WorkerId::new(worker_id),
            worker_run_id: Some(worker_run_id),
            endpoint: format!("{host}:19101"),
            lease_valid: true,
            host: Some(host.to_string()),
            tier_free: vec![TierFree {
                tier: Tier::Hdd,
                free_bytes: 4096,
            }],
        }
    }

    fn request(group_name: &GroupName, op: PlacementOp, block_id: BlockId) -> PlacementRequest<'_> {
        let block_size = 4096;
        PlacementRequest {
            group_name: group_name.clone(),
            op,
            block_id,
            visible_len: 64,
            block_size,
            caller: None,
            existing: &[],
        }
    }

    #[test]
    fn write_uses_single_replica_and_prefers_caller_locality() {
        let group = group_name("g10");
        let block_id = block(66, 0);
        let mut req = request(&group, PlacementOp::Write, block_id);
        req.caller = Some(CallerContextFields::parse("host=host-b"));
        let workers = vec![worker(1, run_id(21), "host-a"), worker(2, run_id(22), "host-b")];

        let plan = plan_placement(&req, &workers);

        assert_eq!(plan.status, PlacementStatus::Ok);
        assert_eq!(
            plan.workers.iter().map(|w| w.worker_id).collect::<Vec<_>>(),
            vec![WorkerId::new(2)]
        );
        assert_eq!(plan.workers[0].tier, Tier::Hdd);
    }

    #[test]
    fn read_retains_reported_tier_when_another_tier_has_free_space() {
        let group = group_name("g10");
        let block_id = block(66, 0);
        let mut req = request(&group, PlacementOp::Read, block_id);
        let mut live_worker = worker(1, run_id(21), "host-a");
        live_worker.tier_free.push(TierFree {
            tier: Tier::Ssd,
            free_bytes: 0,
        });
        let workers = vec![live_worker];
        let reported = [ReportedBlockLocation {
            durable_len: req.visible_len,
            worker_id: WorkerId::new(1),
            worker_run_id: run_id(21),
            tier: Tier::Ssd,
        }];
        req.existing = &reported;

        let plan = plan_placement(&req, &workers);

        assert_eq!(plan.status, PlacementStatus::Ok);
        assert_eq!(
            plan.workers
                .iter()
                .map(|worker| (worker.worker_id, worker.tier))
                .collect::<Vec<_>>(),
            vec![(WorkerId::new(1), Tier::Ssd)]
        );
    }
}
