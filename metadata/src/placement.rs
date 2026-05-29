// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Metadata-side block placement planning.
//!
//! The planner consumes metadata-owned worker membership and report views. It
//! does not execute UFS loads, repair copies, worker commands, or report-state
//! mutations, and it does not define client-side placement policy.

use std::collections::HashSet;

use common::header::CallerContextFields;
use types::ids::{BlockId, ShardGroupId, WorkerId};
use types::layout::{BlockFormatId, FileLayout};
use types::WorkerRunId;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlacementOp {
    Read,
    Load,
    Write,
    Repair,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlacementRequest {
    pub group_id: ShardGroupId,
    pub op: PlacementOp,
    pub block_id: BlockId,
    pub block_stamp: Option<u64>,
    pub layout: FileLayout,
    pub caller: Option<CallerContextFields>,
    pub existing: Vec<ReportedBlockLocation>,
    pub exclude_workers: Vec<WorkerId>,
    pub target_replicas: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReportedBlockLocation {
    pub group_id: ShardGroupId,
    pub block_id: BlockId,
    pub block_stamp: u64,
    pub worker_id: WorkerId,
    pub worker_run_id: WorkerRunId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerPlacementView {
    pub group_id: ShardGroupId,
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
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlacementStatus {
    Ok,
    NoLiveWorker,
    NoLiveReplica,
    NotEnoughReplicas,
    Unsupported,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlacementPlan {
    pub group_id: ShardGroupId,
    pub op: PlacementOp,
    pub workers: Vec<PlacementWorker>,
    pub status: PlacementStatus,
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
        if location.group_id != req.group_id
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
            .find(|worker| worker.group_id == req.group_id && worker.worker_id == location.worker_id)
        else {
            continue;
        };
        if is_live(worker) && worker.worker_run_id == Some(location.worker_run_id) {
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
    let mut candidates: Vec<_> = workers
        .iter()
        .filter(|worker| {
            worker.group_id == req.group_id
                && is_live(worker)
                && !exclude.contains(&worker.worker_id)
                && supports_block_format(worker, req.layout.block_format_id)
                && has_capacity(worker, required_len)
        })
        .collect();
    sort_workers(req, &mut candidates, use_locality);
    if candidates.is_empty() {
        return plan(req, Vec::new(), PlacementStatus::NoLiveWorker);
    }

    let target = usize::from(target_replicas.max(1));
    let selected = workers_from_views(candidates.into_iter().take(target).collect());
    let status = if selected.len() < target {
        PlacementStatus::NotEnoughReplicas
    } else {
        PlacementStatus::Ok
    };
    plan(req, selected, status)
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
            stable_order(req.group_id, req.block_id, worker.worker_id),
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
            })
        })
        .collect()
}

fn plan(req: &PlacementRequest, workers: Vec<PlacementWorker>, status: PlacementStatus) -> PlacementPlan {
    PlacementPlan {
        group_id: req.group_id,
        op: req.op,
        workers,
        status,
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

fn stable_order(group_id: ShardGroupId, block_id: BlockId, worker_id: WorkerId) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325;
    for value in [
        group_id.as_raw(),
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
