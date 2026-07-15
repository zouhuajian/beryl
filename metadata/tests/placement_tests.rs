use common::header::CallerContextFields;
use metadata::placement::{
    PlacementOp, PlacementPlanner, PlacementRequest, PlacementStatus, ReportedBlockLocation, WorkerPlacementView,
};
use types::ids::{BlockId, BlockIndex, DataHandleId, WorkerId};
use types::layout::{BlockFormatId, FileLayout};
use types::{GroupName, Tier, TierFree, WorkerRunId};

fn run_id(suffix: u32) -> WorkerRunId {
    format!("550e8400-e29b-41d4-a716-{suffix:012}")
        .parse()
        .expect("valid worker run id")
}

fn block(data_handle_id: u64, index: u32) -> BlockId {
    BlockId::new(DataHandleId::new(data_handle_id), BlockIndex::new(index))
}

fn group_name(raw: &str) -> GroupName {
    GroupName::parse(raw).unwrap()
}

fn worker(group_name: &GroupName, worker_id: u64, worker_run_id: WorkerRunId, host: &str) -> WorkerPlacementView {
    WorkerPlacementView {
        group_name: group_name.clone(),
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
        tier_free: vec![TierFree {
            tier: Tier::Hdd,
            free_bytes: 4096,
        }],
        supported_block_formats: vec![BlockFormatId::FULL_EFFECTIVE],
    }
}

fn request(group_name: &GroupName, op: PlacementOp, block_id: BlockId) -> PlacementRequest {
    let layout = FileLayout::try_new(4096, 1024, 1).unwrap();
    PlacementRequest {
        group_name: group_name.clone(),
        op,
        block_id,
        block_stamp: Some(9),
        layout,
        caller: None,
        existing: Vec::new(),
        exclude_workers: Vec::new(),
        target_replicas: layout.replication(),
    }
}

fn location(
    group_name: &GroupName,
    block_id: BlockId,
    block_stamp: u64,
    worker_id: u64,
    worker_run_id: WorkerRunId,
) -> ReportedBlockLocation {
    ReportedBlockLocation {
        group_name: group_name.clone(),
        block_id,
        block_stamp,
        worker_id: WorkerId::new(worker_id),
        worker_run_id,
    }
}

#[test]
fn read_uses_live_matching_replicas_in_deterministic_order() {
    let group = group_name("g7");
    let other_group = group_name("g8");
    let block_id = block(44, 0);
    let worker_a_run = run_id(1);
    let worker_b_run = run_id(2);
    let stale_run = run_id(3);
    let mut req = request(&group, PlacementOp::Read, block_id);
    req.caller = Some(CallerContextFields::parse("host=host-b"));
    req.existing = vec![
        location(&group, block_id, 9, 1, worker_a_run),
        location(&group, block_id, 9, 2, worker_b_run),
        location(&group, block_id, 9, 3, stale_run),
        location(&other_group, block_id, 9, 4, run_id(4)),
        location(&group, block(44, 1), 9, 5, run_id(5)),
        location(&group, block_id, 8, 6, run_id(6)),
        location(&group, block_id, 9, 7, run_id(7)),
    ];
    let workers = vec![
        worker(&group, 1, worker_a_run, "host-a"),
        worker(&group, 2, worker_b_run, "host-b"),
        worker(&group, 3, run_id(30), "host-c"),
        worker(&other_group, 4, run_id(4), "host-d"),
        worker(&group, 5, run_id(5), "host-e"),
        worker(&group, 6, run_id(6), "host-f"),
        WorkerPlacementView {
            lease_valid: false,
            ..worker(&group, 7, run_id(7), "host-g")
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
    let group = group_name("g9");
    let unsupported = WorkerPlacementView {
        supported_block_formats: Vec::new(),
        ..worker(&group, 1, run_id(11), "host-a")
    };
    let supported = worker(&group, 2, run_id(12), "host-b");

    for op in [PlacementOp::Load, PlacementOp::Write] {
        let req = request(&group, op, block(55, 0));
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
        assert_eq!(plan.status, PlacementStatus::UnsupportedBlockFormat);
        assert!(plan.workers.is_empty());
    }
}

#[test]
fn write_reports_distinct_eligibility_statuses() {
    let group = group_name("g12");
    let req = request(&group, PlacementOp::Write, block(88, 0));
    let no_live = WorkerPlacementView {
        worker_run_id: None,
        ..worker(&group, 1, run_id(31), "host-a")
    };
    assert_eq!(
        PlacementPlanner.plan(&req, &[no_live]).status,
        PlacementStatus::NoLiveWorker
    );

    let unsupported = WorkerPlacementView {
        supported_block_formats: Vec::new(),
        ..worker(&group, 2, run_id(32), "host-b")
    };
    assert_eq!(
        PlacementPlanner.plan(&req, &[unsupported]).status,
        PlacementStatus::UnsupportedBlockFormat
    );

    let mem_only = WorkerPlacementView {
        tier_free: vec![TierFree {
            tier: Tier::Mem,
            free_bytes: 4096,
        }],
        ..worker(&group, 3, run_id(33), "host-c")
    };
    assert_eq!(
        PlacementPlanner.plan(&req, &[mem_only]).status,
        PlacementStatus::NoWritableTier
    );

    let small_hdd = WorkerPlacementView {
        tier_free: vec![TierFree {
            tier: Tier::Hdd,
            free_bytes: 4095,
        }],
        ..worker(&group, 4, run_id(34), "host-d")
    };
    assert_eq!(
        PlacementPlanner.plan(&req, &[small_hdd]).status,
        PlacementStatus::InsufficientCapacity
    );

    let ok = PlacementPlanner.plan(&req, &[worker(&group, 5, run_id(35), "host-e")]);
    assert_eq!(ok.status, PlacementStatus::Ok);
}

#[test]
fn write_capacity_failure_message_includes_max_worker_and_tier() {
    let group = group_name("g15");
    let req = request(&group, PlacementOp::Write, block(91, 0));
    let workers = vec![
        WorkerPlacementView {
            tier_free: vec![TierFree {
                tier: Tier::Ssd,
                free_bytes: 16,
            }],
            ..worker(&group, 1, run_id(41), "host-a")
        },
        WorkerPlacementView {
            tier_free: vec![TierFree {
                tier: Tier::Hdd,
                free_bytes: 8,
            }],
            ..worker(&group, 2, run_id(42), "host-b")
        },
    ];

    let plan = PlacementPlanner.plan(&req, &workers);
    let message = plan.failure_message(&req);

    assert_eq!(plan.status, PlacementStatus::InsufficientCapacity);
    assert!(
        message.contains("placement failed: status=InsufficientCapacity"),
        "{message}"
    );
    assert!(message.contains("live=2"), "{message}");
    assert!(message.contains("format_ok=2"), "{message}");
    assert!(message.contains("tier_ok=2"), "{message}");
    assert!(message.contains("capacity_ok=0"), "{message}");
    assert!(message.contains("max_free=16"), "{message}");
    assert!(message.contains("max_worker=1"), "{message}");
    assert!(message.contains("max_tier=SSD"), "{message}");
}

#[test]
fn write_selects_first_persistent_tier_with_capacity() {
    let group = group_name("g13");
    let req = request(&group, PlacementOp::Write, block(89, 0));
    let worker = WorkerPlacementView {
        tier_free: vec![
            TierFree {
                tier: Tier::Nvme,
                free_bytes: 4095,
            },
            TierFree {
                tier: Tier::Ssd,
                free_bytes: 4096,
            },
            TierFree {
                tier: Tier::Hdd,
                free_bytes: 4096,
            },
        ],
        ..worker(&group, 1, run_id(36), "host-a")
    };

    let plan = PlacementPlanner.plan(&req, &[worker]);

    assert_eq!(plan.status, PlacementStatus::Ok);
    assert_eq!(plan.workers[0].tier, Some(Tier::Ssd));
}

#[test]
fn write_selects_nvme_ssd_and_hdd_only_workers() {
    let group = group_name("g14");
    for (tier, worker_id) in [(Tier::Nvme, 1), (Tier::Ssd, 2), (Tier::Hdd, 3)] {
        let req = request(&group, PlacementOp::Write, block(90 + worker_id, 0));
        let worker = WorkerPlacementView {
            tier_free: vec![TierFree { tier, free_bytes: 4096 }],
            ..worker(&group, worker_id, run_id(40 + worker_id as u32), "host-a")
        };

        let plan = PlacementPlanner.plan(&req, &[worker]);

        assert_eq!(plan.status, PlacementStatus::Ok);
        assert_eq!(plan.workers[0].tier, Some(tier));
    }
}

#[test]
fn write_uses_layout_replication_and_prefers_caller_locality() {
    let group = group_name("g10");
    let block_id = block(66, 0);
    let mut req = request(&group, PlacementOp::Write, block_id);
    req.caller = Some(CallerContextFields::parse("host=host-b"));
    let workers = vec![
        worker(&group, 1, run_id(21), "host-a"),
        worker(&group, 2, run_id(22), "host-b"),
    ];

    let plan = PlacementPlanner.plan(&req, &workers);

    assert_eq!(req.target_replicas, req.layout.replication());
    assert_eq!(req.target_replicas, 1);
    assert_eq!(plan.status, PlacementStatus::Ok);
    assert_eq!(
        plan.workers.iter().map(|w| w.worker_id).collect::<Vec<_>>(),
        vec![WorkerId::new(2)]
    );
}

#[test]
fn repair_is_inert() {
    let group = group_name("g11");
    let req = request(&group, PlacementOp::Repair, block(77, 0));

    let plan = PlacementPlanner.plan(&req, &[]);

    assert_eq!(plan.status, PlacementStatus::Unsupported);
    assert!(plan.workers.is_empty());
}
