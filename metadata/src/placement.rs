// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata-side block placement planning.
//!
//! The planner consumes metadata-owned worker membership and report views. It
//! does not execute UFS loads, repair copies, worker commands, or report-state
//! mutations, and it does not define client-side placement policy.

use std::collections::HashSet;

use common::header::CallerContextFields;
use types::ids::{BlockId, WorkerId};
use types::layout::{BlockFormatId, FileLayout};
use types::{GroupName, Tier, TierFree, WorkerRunId};

const WRITE_TIER_ORDER: [Tier; 3] = [Tier::Nvme, Tier::Ssd, Tier::Hdd];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlacementOp {
    Read,
    Load,
    Write,
    Repair,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlacementRequest {
    pub group_name: GroupName,
    pub op: PlacementOp,
    pub block_id: BlockId,
    pub block_stamp: Option<u64>,
    pub layout: FileLayout,
    pub caller: Option<CallerContextFields>,
    pub existing: Vec<ReportedBlockLocation>,
    pub exclude_workers: Vec<WorkerId>,
    pub target_replicas: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReportedBlockLocation {
    pub group_name: GroupName,
    pub block_id: BlockId,
    pub block_stamp: u64,
    pub worker_id: WorkerId,
    pub worker_run_id: WorkerRunId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerPlacementView {
    pub group_name: GroupName,
    pub worker_id: WorkerId,
    pub worker_run_id: Option<WorkerRunId>,
    pub endpoint: String,
    pub worker_net_protocol: i32,
    pub registered: bool,
    pub lease_valid: bool,
    pub ip: Option<String>,
    pub host: Option<String>,
    pub az: Option<String>,
    pub rack: Option<String>,
    pub region: Option<String>,
    pub free_bytes: Option<u64>,
    pub tier_free: Vec<TierFree>,
    /// Metadata-visible block format capabilities. StoreBackend / IoEngine
    /// details remain worker-local and are not part of placement input.
    pub supported_block_formats: Vec<BlockFormatId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlacementWorker {
    pub worker_id: WorkerId,
    pub worker_run_id: WorkerRunId,
    pub endpoint: String,
    pub worker_net_protocol: i32,
    pub tier: Option<Tier>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlacementStatus {
    Ok,
    NoLiveWorker,
    NoEligibleWorker,
    UnsupportedBlockFormat,
    NoWritableTier,
    InsufficientCapacity,
    NoLiveReplica,
    NotEnoughReplicas,
    Unsupported,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PlacementStats {
    pub live_count: usize,
    pub group_count: usize,
    pub format_count: usize,
    pub tier_count: usize,
    pub capacity_count: usize,
    pub max_free_bytes: u64,
    pub max_free_worker_id: Option<WorkerId>,
    pub max_free_tier: Option<Tier>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlacementPlan {
    pub group_name: GroupName,
    pub op: PlacementOp,
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
            "placement failed: status={:?} group={} format={} required={} policy=[{}] live={} group_ok={} format_ok={} tier_ok={} capacity_ok={} max_free={} max_worker={} max_tier={}",
            self.status,
            req.group_name,
            req.layout.block_format_id.as_raw(),
            req.layout.block_size,
            write_tier_policy_label(),
            self.stats.live_count,
            self.stats.group_count,
            self.stats.format_count,
            self.stats.tier_count,
            self.stats.capacity_count,
            self.stats.max_free_bytes,
            max_worker,
            max_tier
        )
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PlacementPlanner;

impl PlacementPlanner {
    pub fn plan(&self, req: &PlacementRequest, workers: &[WorkerPlacementView]) -> PlacementPlan {
        match req.op {
            PlacementOp::Read => choose_read(req, workers),
            PlacementOp::Load => choose_live_targets(req, workers, 1, false),
            PlacementOp::Write => choose_live_targets(req, workers, req.target_replicas.max(1), true),
            PlacementOp::Repair => plan(req, Vec::new(), PlacementStatus::Unsupported),
        }
    }
}

fn choose_read(req: &PlacementRequest, workers: &[WorkerPlacementView]) -> PlacementPlan {
    let mut seen = HashSet::new();
    let mut candidates = Vec::new();
    for location in &req.existing {
        if location.group_name != req.group_name
            || location.block_id != req.block_id
            || req
                .block_stamp
                .map(|block_stamp| block_stamp != location.block_stamp)
                .unwrap_or(false)
            || !seen.insert(location.worker_id)
        {
            continue;
        }
        let Some(worker) = workers
            .iter()
            .find(|worker| worker.group_name == req.group_name && worker.worker_id == location.worker_id)
        else {
            continue;
        };
        if is_live(worker)
            && worker
                .worker_run_id
                .is_some_and(|worker_run_id| worker_run_id.matches(location.worker_run_id))
        {
            candidates.push(worker);
        }
    }
    sort_workers(req, &mut candidates, true);
    let selected = workers_from_views(candidates);
    let status = if selected.is_empty() {
        PlacementStatus::NoLiveReplica
    } else {
        PlacementStatus::Ok
    };
    plan(req, selected, status)
}

fn choose_live_targets(
    req: &PlacementRequest,
    workers: &[WorkerPlacementView],
    target_replicas: u8,
    use_locality: bool,
) -> PlacementPlan {
    let exclude: HashSet<WorkerId> = req.exclude_workers.iter().copied().collect();
    let required_len = u64::from(req.layout.block_size);
    let mut stats = PlacementStats::default();
    let live_candidates: Vec<_> = workers
        .iter()
        .filter(|worker| worker.group_name == req.group_name && is_live(worker))
        .collect();
    stats.live_count = live_candidates.len();
    if live_candidates.is_empty() {
        return plan_with_stats(req, Vec::new(), PlacementStatus::NoLiveWorker, stats);
    }

    let group_candidates: Vec<_> = live_candidates
        .into_iter()
        .filter(|worker| !exclude.contains(&worker.worker_id))
        .collect();
    stats.group_count = group_candidates.len();
    if group_candidates.is_empty() {
        return plan_with_stats(req, Vec::new(), PlacementStatus::NoEligibleWorker, stats);
    }

    let format_candidates: Vec<_> = group_candidates
        .into_iter()
        .filter(|worker| supports_block_format(worker, req.layout.block_format_id))
        .collect();
    stats.format_count = format_candidates.len();
    if format_candidates.is_empty() {
        return plan_with_stats(req, Vec::new(), PlacementStatus::UnsupportedBlockFormat, stats);
    }

    if req.op == PlacementOp::Write {
        return choose_write_targets(req, format_candidates, target_replicas, use_locality, stats);
    }

    stats.tier_count = format_candidates.len();
    for worker in &format_candidates {
        if let Some(free_bytes) = worker.free_bytes {
            record_max_free(&mut stats, worker.worker_id, None, free_bytes);
        }
    }
    let mut candidates: Vec<_> = format_candidates
        .into_iter()
        .filter(|worker| has_capacity(worker, required_len))
        .collect();
    stats.capacity_count = candidates.len();
    sort_workers(req, &mut candidates, use_locality);
    if candidates.is_empty() {
        return plan_with_stats(req, Vec::new(), PlacementStatus::InsufficientCapacity, stats);
    }

    let target = usize::from(target_replicas.max(1));
    let selected = workers_from_views(candidates.into_iter().take(target).collect());
    let status = if selected.len() < target {
        PlacementStatus::NotEnoughReplicas
    } else {
        PlacementStatus::Ok
    };
    plan_with_stats(req, selected, status, stats)
}

fn choose_write_targets(
    req: &PlacementRequest,
    workers: Vec<&WorkerPlacementView>,
    target_replicas: u8,
    use_locality: bool,
    mut stats: PlacementStats,
) -> PlacementPlan {
    let required_len = u64::from(req.layout.block_size);
    for worker in &workers {
        for tier in WRITE_TIER_ORDER {
            if let Some(free_bytes) = tier_free_bytes(worker, tier) {
                record_max_free(&mut stats, worker.worker_id, Some(tier), free_bytes);
            }
        }
    }

    let mut candidates = Vec::new();
    for worker in workers {
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
        return plan_with_stats(req, Vec::new(), PlacementStatus::NoWritableTier, stats);
    }

    stats.capacity_count = candidates.len();
    sort_write_candidates(req, &mut candidates, use_locality);
    if candidates.is_empty() {
        return plan_with_stats(req, Vec::new(), PlacementStatus::InsufficientCapacity, stats);
    }

    let target = usize::from(target_replicas.max(1));
    let mut seen = HashSet::new();
    let mut selected = Vec::with_capacity(target);
    for (worker, tier) in candidates {
        if !seen.insert(worker.worker_id) {
            continue;
        }
        if let Some(worker_run_id) = worker.worker_run_id {
            selected.push(PlacementWorker {
                worker_id: worker.worker_id,
                worker_run_id,
                endpoint: worker.endpoint.clone(),
                worker_net_protocol: worker.worker_net_protocol,
                tier: Some(tier),
            });
        }
        if selected.len() == target {
            break;
        }
    }

    let status = if selected.len() < target {
        PlacementStatus::NotEnoughReplicas
    } else {
        PlacementStatus::Ok
    };
    plan_with_stats(req, selected, status, stats)
}

fn is_live(worker: &WorkerPlacementView) -> bool {
    worker.registered && worker.lease_valid && worker.worker_run_id.is_some()
}

fn has_capacity(worker: &WorkerPlacementView, required_len: u64) -> bool {
    worker
        .free_bytes
        .map(|free_bytes| free_bytes >= required_len)
        .unwrap_or(true)
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

fn supports_block_format(worker: &WorkerPlacementView, block_format_id: BlockFormatId) -> bool {
    worker.supported_block_formats.contains(&block_format_id)
}

fn sort_workers(req: &PlacementRequest, workers: &mut Vec<&WorkerPlacementView>, use_locality: bool) {
    workers.sort_by_key(|worker| {
        let locality = if use_locality {
            req.caller
                .as_ref()
                .map(|caller| locality_rank(caller, worker))
                .unwrap_or(0)
        } else {
            0
        };
        (
            locality,
            stable_order(&req.group_name, req.block_id, worker.worker_id),
            worker.worker_id.as_raw(),
        )
    });
}

fn sort_write_candidates(
    req: &PlacementRequest,
    candidates: &mut Vec<(&WorkerPlacementView, Tier)>,
    use_locality: bool,
) {
    candidates.sort_by_key(|(worker, tier)| {
        let locality = if use_locality {
            req.caller
                .as_ref()
                .map(|caller| locality_rank(caller, worker))
                .unwrap_or(0)
        } else {
            0
        };
        (
            write_tier_rank(*tier),
            locality,
            stable_order(&req.group_name, req.block_id, worker.worker_id),
            worker.worker_id.as_raw(),
        )
    });
}

fn workers_from_views(workers: Vec<&WorkerPlacementView>) -> Vec<PlacementWorker> {
    workers
        .into_iter()
        .filter_map(|worker| {
            worker.worker_run_id.map(|worker_run_id| PlacementWorker {
                worker_id: worker.worker_id,
                worker_run_id,
                endpoint: worker.endpoint.clone(),
                worker_net_protocol: worker.worker_net_protocol,
                tier: None,
            })
        })
        .collect()
}

fn plan(req: &PlacementRequest, workers: Vec<PlacementWorker>, status: PlacementStatus) -> PlacementPlan {
    plan_with_stats(req, workers, status, PlacementStats::default())
}

fn plan_with_stats(
    req: &PlacementRequest,
    workers: Vec<PlacementWorker>,
    status: PlacementStatus,
    stats: PlacementStats,
) -> PlacementPlan {
    PlacementPlan {
        group_name: req.group_name.clone(),
        op: req.op,
        workers,
        status,
        stats,
    }
}

fn locality_rank(caller: &CallerContextFields, worker: &WorkerPlacementView) -> u8 {
    if matches_pair(caller.host(), &worker.host) || matches_pair(caller.ip(), &worker.ip) {
        0
    } else if matches_pair(caller.az(), &worker.az) {
        1
    } else if matches_pair(caller.rack(), &worker.rack) {
        2
    } else if matches_pair(caller.region(), &worker.region) {
        3
    } else {
        4
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
        block_id.data_handle_id.as_raw(),
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
    use types::ids::{BlockIndex, DataHandleId};

    fn req(block_size: u32) -> PlacementRequest {
        PlacementRequest {
            group_name: GroupName::parse("root").unwrap(),
            op: PlacementOp::Write,
            block_id: BlockId::new(DataHandleId::new(1), BlockIndex::new(0)),
            block_stamp: Some(7),
            layout: FileLayout::new(block_size, 1024, 1),
            caller: None,
            existing: Vec::new(),
            exclude_workers: Vec::new(),
            target_replicas: 1,
        }
    }

    fn worker(free_bytes: Option<u64>) -> WorkerPlacementView {
        WorkerPlacementView {
            group_name: GroupName::parse("root").unwrap(),
            worker_id: WorkerId::new(11),
            worker_run_id: Some(WorkerRunId::new()),
            endpoint: "http://127.0.0.1:19090".to_string(),
            worker_net_protocol: 1,
            registered: true,
            lease_valid: true,
            ip: None,
            host: None,
            az: None,
            rack: None,
            region: None,
            free_bytes,
            tier_free: free_bytes
                .map(|free_bytes| {
                    vec![TierFree {
                        tier: Tier::Hdd,
                        free_bytes,
                    }]
                })
                .unwrap_or_default(),
            supported_block_formats: vec![BlockFormatId::CURRENT_FOR_NEW_FILE],
        }
    }

    #[test]
    fn live_worker_with_enough_free_bytes_is_eligible() {
        let plan = PlacementPlanner.plan(&req(4096), &[worker(Some(4096))]);

        assert_eq!(plan.status, PlacementStatus::Ok);
        assert_eq!(plan.workers.len(), 1);
    }

    #[test]
    fn live_worker_with_insufficient_free_bytes_is_capacity_failure() {
        let plan = PlacementPlanner.plan(&req(4096), &[worker(Some(4095))]);

        assert_eq!(plan.status, PlacementStatus::InsufficientCapacity);
        assert!(plan.workers.is_empty());
    }

    #[test]
    fn no_live_worker_remains_no_live_worker() {
        let mut stale = worker(Some(4096));
        stale.worker_run_id = None;

        let plan = PlacementPlanner.plan(&req(4096), &[stale]);

        assert_eq!(plan.status, PlacementStatus::NoLiveWorker);
        assert!(plan.workers.is_empty());
    }
}
