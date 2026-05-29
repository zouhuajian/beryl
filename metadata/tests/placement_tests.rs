use common::header::CallerContextFields;
use metadata::placement::{
    PlacementOp, PlacementPlanner, PlacementRequest, PlacementStatus, ReportedBlockLocation, WorkerPlacementView,
};
use types::ids::{BlockId, BlockIndex, DataHandleId, ShardGroupId, WorkerId};
use types::layout::{BlockFormatId, FileLayout};
use types::WorkerRunId;

fn run_id(suffix: u32) -> WorkerRunId {
    format!("550e8400-e29b-41d4-a716-{suffix:012}")
        .parse()
        .expect("valid worker run id")
}

fn block(data_handle_id: u64, index: u32) -> BlockId {
    BlockId::new(DataHandleId::new(data_handle_id), BlockIndex::new(index))
}

fn worker(group_id: ShardGroupId, worker_id: u64, worker_run_id: WorkerRunId, host: &str) -> WorkerPlacementView {
    WorkerPlacementView {
        group_id,
        worker_id: WorkerId::new(worker_id),
        worker_run_id: Some(worker_run_id),
        endpoint: format!("{host}:19101"),
        worker_net_protocol: 1,
        registered: true,
        lease_valid: true,
        ip: None,
        host: Some(host.to_string()),
        az: None,
        rack: None,
        region: None,
        free_bytes: Some(4096),
        supported_block_formats: vec![BlockFormatId::FULL_EFFECTIVE],
    }
}

fn request(group_id: ShardGroupId, op: PlacementOp, block_id: BlockId) -> PlacementRequest {
    let layout = FileLayout::new(4096, 1024, 1);
    PlacementRequest {
        group_id,
        op,
        block_id,
        block_stamp: Some(9),
        layout,
        caller: None,
        existing: Vec::new(),
        exclude_workers: Vec::new(),
        target_replicas: layout.replication,
    }
}

fn location(
    group_id: ShardGroupId,
    block_id: BlockId,
    block_stamp: u64,
    worker_id: u64,
    worker_run_id: WorkerRunId,
) -> ReportedBlockLocation {
    ReportedBlockLocation {
        group_id,
        block_id,
        block_stamp,
        worker_id: WorkerId::new(worker_id),
        worker_run_id,
    }
}

#[test]
fn read_uses_live_matching_replicas_in_deterministic_order() {
    let group = ShardGroupId::new(7);
    let other_group = ShardGroupId::new(8);
    let block_id = block(44, 0);
    let worker_a_run = run_id(1);
    let worker_b_run = run_id(2);
    let stale_run = run_id(3);
    let mut req = request(group, PlacementOp::Read, block_id);
    req.caller = Some(CallerContextFields::parse("host=host-b"));
    req.existing = vec![
        location(group, block_id, 9, 1, worker_a_run),
        location(group, block_id, 9, 2, worker_b_run),
        location(group, block_id, 9, 3, stale_run),
        location(other_group, block_id, 9, 4, run_id(4)),
        location(group, block(44, 1), 9, 5, run_id(5)),
        location(group, block_id, 8, 6, run_id(6)),
        location(group, block_id, 9, 7, run_id(7)),
    ];
    let workers = vec![
        worker(group, 1, worker_a_run, "host-a"),
        worker(group, 2, worker_b_run, "host-b"),
        worker(group, 3, run_id(30), "host-c"),
        worker(other_group, 4, run_id(4), "host-d"),
        worker(group, 5, run_id(5), "host-e"),
        worker(group, 6, run_id(6), "host-f"),
        WorkerPlacementView {
            lease_valid: false,
            ..worker(group, 7, run_id(7), "host-g")
        },
    ];

    let planner = PlacementPlanner;
    let first = planner.plan(&req, &workers);
    let mut reversed_workers = workers.clone();
    reversed_workers.reverse();
    let second = planner.plan(&req, &reversed_workers);

    assert_eq!(first.status, PlacementStatus::Ok);
    assert_eq!(first, second);
    assert_eq!(
        first.workers.iter().map(|w| w.worker_id).collect::<Vec<_>>(),
        vec![WorkerId::new(2), WorkerId::new(1)]
    );
}

#[test]
fn load_and_write_filter_workers_without_required_block_format() {
    let group = ShardGroupId::new(9);
    let unsupported = WorkerPlacementView {
        supported_block_formats: Vec::new(),
        ..worker(group, 1, run_id(11), "host-a")
    };
    let supported = worker(group, 2, run_id(12), "host-b");

    for op in [PlacementOp::Load, PlacementOp::Write] {
        let req = request(group, op, block(55, 0));
        let plan = PlacementPlanner.plan(&req, &[unsupported.clone(), supported.clone()]);

        assert_eq!(
            plan.status,
            PlacementStatus::Ok,
            "{op:?} should select supported worker"
        );
        assert_eq!(
            plan.workers.iter().map(|w| w.worker_id).collect::<Vec<_>>(),
            vec![WorkerId::new(2)]
        );

        let plan = PlacementPlanner.plan(&req, std::slice::from_ref(&unsupported));
        assert_eq!(
            plan.status,
            PlacementStatus::NoLiveWorker,
            "{op:?} should reject unsupported-only set"
        );
        assert!(plan.workers.is_empty());
    }
}

#[test]
fn write_uses_layout_replication_and_prefers_caller_locality() {
    let group = ShardGroupId::new(10);
    let block_id = block(66, 0);
    let mut req = request(group, PlacementOp::Write, block_id);
    req.caller = Some(CallerContextFields::parse("host=host-b"));
    let workers = vec![
        worker(group, 1, run_id(21), "host-a"),
        worker(group, 2, run_id(22), "host-b"),
    ];

    let plan = PlacementPlanner.plan(&req, &workers);

    assert_eq!(req.target_replicas, req.layout.replication);
    assert_eq!(req.target_replicas, 1);
    assert_eq!(plan.status, PlacementStatus::Ok);
    assert_eq!(
        plan.workers.iter().map(|w| w.worker_id).collect::<Vec<_>>(),
        vec![WorkerId::new(2)]
    );
}

#[test]
fn repair_is_inert() {
    let group = ShardGroupId::new(11);
    let req = request(group, PlacementOp::Repair, block(77, 0));

    let plan = PlacementPlanner.plan(&req, &[]);

    assert_eq!(plan.status, PlacementStatus::Unsupported);
    assert!(plan.workers.is_empty());
}
