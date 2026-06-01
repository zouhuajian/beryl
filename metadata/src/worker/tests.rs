// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Tests for worker manager and registration.

use super::manager::{
    worker_net_protocol_label, BlockLocationKey, BlockReportBlock, BlockReportBlockState, BlockReportDeltaEntry,
    BlockReportDeltaOp, HealthStatus, WorkerInfo, WorkerManager, WorkerRegistrationKey,
};
use crate::error::MetadataError;
use std::time::Duration;
use types::ids::{BlockId, BlockIndex, DataHandleId, WorkerId};
use types::{GroupName, WorkerRunId};

fn group_name(raw: &str) -> GroupName {
    GroupName::parse(raw).unwrap()
}

#[test]
fn worker_net_protocol_label_uses_readable_names() {
    assert_eq!(
        worker_net_protocol_label(proto::common::WorkerNetProtocolProto::WorkerNetProtocolUnspecified as i32),
        "unspecified"
    );
    assert_eq!(
        worker_net_protocol_label(proto::common::WorkerNetProtocolProto::WorkerNetProtocolGrpc as i32),
        "grpc"
    );
    assert_eq!(
        worker_net_protocol_label(proto::common::WorkerNetProtocolProto::WorkerNetProtocolQuic as i32),
        "quic"
    );
    assert_eq!(
        worker_net_protocol_label(proto::common::WorkerNetProtocolProto::WorkerNetProtocolRdma as i32),
        "rdma"
    );
    assert_eq!(worker_net_protocol_label(99), "unknown");
}

#[test]
fn test_worker_registration_with_worker_net_protocol() {
    let manager = WorkerManager::new(60);
    let worker_id = WorkerId::new(1);
    let address = "127.0.0.1:9090".to_string();
    let worker_net_protocol = 1; // GRPC

    // Register worker
    manager
        .register_worker(&group_name("g1"), worker_id, address.clone(), worker_net_protocol, None)
        .unwrap();

    // Get descriptor and verify fields
    let descriptor = manager.get_descriptor(&group_name("g1"), worker_id).unwrap();
    assert_eq!(descriptor.worker_id, worker_id);
    assert_eq!(descriptor.address, address);
    assert_eq!(descriptor.worker_net_protocol, worker_net_protocol);
}

fn report_run_id() -> WorkerRunId {
    "550e8400-e29b-41d4-a716-446655440100".parse().unwrap()
}

fn report_block(index: u32) -> BlockReportBlock {
    let block_id = BlockId::new(DataHandleId::new(9), BlockIndex::new(index));
    report_block_with_id(block_id)
}

fn report_block_with_id(block_id: BlockId) -> BlockReportBlock {
    BlockReportBlock {
        block_id,
        data_handle_id: block_id.data_handle_id.as_raw(),
        block_index: block_id.index.as_raw(),
        block_stamp: u64::from(block_id.index.as_raw()) + 100,
        effective_len: 4096,
        committed_length: 4096,
        block_state: BlockReportBlockState::Ready,
    }
}

fn register_live_report_worker(
    manager: &WorkerManager,
    group_name: &GroupName,
    worker_id: WorkerId,
    run_id: WorkerRunId,
) {
    manager
        .register_worker_run(group_name, worker_id, "127.0.0.1:9090".to_string(), 1, run_id, None)
        .unwrap();
    manager
        .record_heartbeat(
            group_name,
            worker_id,
            run_id,
            1,
            "127.0.0.1:9090",
            1,
            1_000,
            100,
            900,
            0,
            0,
            HealthStatus::Healthy,
        )
        .unwrap();
}

#[test]
fn full_report_batches_publish_only_after_final_batch() {
    let manager = WorkerManager::new(60);
    let group_name_value = group_name("g21");
    let worker_id = WorkerId::new(5);
    let run_id = report_run_id();
    register_live_report_worker(&manager, &group_name_value, worker_id, run_id);

    manager
        .receive_full_block_report(&group_name_value, worker_id, run_id, 1, 0, false, vec![report_block(0)])
        .unwrap();

    assert!(manager
        .get_block_locations(&group_name_value, report_block(0).block_id)
        .is_empty());
    assert!(manager.get_worker_blocks(&group_name_value, worker_id).is_empty());

    manager
        .receive_full_block_report(&group_name_value, worker_id, run_id, 1, 1, true, vec![report_block(1)])
        .unwrap();

    assert_eq!(manager.get_worker_blocks(&group_name_value, worker_id).len(), 2);
    assert_eq!(
        manager.get_block_locations(&group_name_value, report_block(0).block_id),
        vec![worker_id]
    );
    assert_eq!(
        manager.get_block_locations(&group_name_value, report_block(1).block_id),
        vec![worker_id]
    );
}

#[test]
fn final_full_report_marks_active_worker_converged() {
    let manager = WorkerManager::new(60);
    let group_name_value = group_name("g22");
    let worker_id = WorkerId::new(6);
    let run_id = report_run_id();
    register_live_report_worker(&manager, &group_name_value, worker_id, run_id);

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let before = manager.blockreport_convergence_snapshot(now_ms, 60_000, manager.get_metadata_epoch(), 0.80);
    assert_eq!(before.active_workers, 1);
    assert_eq!(before.full_reported_workers, 0);
    assert!(!before.converged);

    manager
        .receive_full_block_report(&group_name_value, worker_id, run_id, 1, 0, true, vec![report_block(0)])
        .unwrap();

    let after = manager.blockreport_convergence_snapshot(now_ms, 60_000, manager.get_metadata_epoch(), 0.80);
    assert_eq!(after.active_workers, 1);
    assert_eq!(after.full_reported_workers, 1);
    assert!(after.converged);
}

#[test]
fn stale_full_report_seq_cannot_roll_back_published_view() {
    let manager = WorkerManager::new(60);
    let group_name_value = group_name("g25");
    let worker_id = WorkerId::new(9);
    let run_id = report_run_id();
    register_live_report_worker(&manager, &group_name_value, worker_id, run_id);

    manager
        .receive_full_block_report(&group_name_value, worker_id, run_id, 7, 0, true, vec![report_block(0)])
        .unwrap();
    assert_eq!(
        manager.get_block_locations(&group_name_value, report_block(0).block_id),
        vec![worker_id]
    );

    let stale = manager
        .receive_full_block_report(&group_name_value, worker_id, run_id, 6, 0, true, vec![report_block(1)])
        .expect_err("stale report_seq must not reset the published baseline");
    assert!(stale.to_string().contains("full report required"));
    assert_eq!(
        manager.get_block_locations(&group_name_value, report_block(0).block_id),
        vec![worker_id]
    );
    assert!(manager
        .get_block_locations(&group_name_value, report_block(1).block_id)
        .is_empty());
}

#[test]
fn full_report_rejects_sequence_run_and_registration_errors() {
    let manager = WorkerManager::new(60);
    let group_name_value = group_name("g22");
    let worker_id = WorkerId::new(6);
    let run_id = report_run_id();

    let missing = manager
        .receive_full_block_report(&group_name_value, worker_id, run_id, 1, 0, true, vec![report_block(0)])
        .expect_err("missing registration must fail");
    assert!(missing.to_string().contains("not registered"));

    register_live_report_worker(&manager, &group_name_value, worker_id, run_id);
    let stale_run: WorkerRunId = "550e8400-e29b-41d4-a716-446655440101".parse().unwrap();
    let stale = manager
        .receive_full_block_report(
            &group_name_value,
            worker_id,
            stale_run,
            1,
            0,
            true,
            vec![report_block(0)],
        )
        .expect_err("stale worker_run_id must fail");
    assert!(stale.to_string().contains("worker_run_id mismatch"));

    manager
        .receive_full_block_report(&group_name_value, worker_id, run_id, 2, 0, false, vec![report_block(0)])
        .unwrap();
    let mismatch = manager
        .receive_full_block_report(&group_name_value, worker_id, run_id, 2, 2, true, vec![report_block(1)])
        .expect_err("batch_seq gap must fail");
    assert!(mismatch.to_string().contains("full report required"));
    assert!(manager.get_worker_blocks(&group_name_value, worker_id).is_empty());
}

#[test]
fn delta_report_requires_ready_baseline_and_ordered_sequence() {
    let manager = WorkerManager::new(60);
    let group_name_value = group_name("g23");
    let worker_id = WorkerId::new(7);
    let run_id = report_run_id();
    register_live_report_worker(&manager, &group_name_value, worker_id, run_id);

    let before_full = manager
        .apply_delta_block_report(
            &group_name_value,
            worker_id,
            run_id,
            1,
            0,
            vec![BlockReportDeltaEntry {
                op: BlockReportDeltaOp::AddUpdate,
                block: report_block(0),
            }],
        )
        .expect_err("delta before full report must fail");
    assert!(before_full.to_string().contains("full report required"));

    manager
        .receive_full_block_report(&group_name_value, worker_id, run_id, 7, 0, true, vec![report_block(0)])
        .unwrap();

    manager
        .apply_delta_block_report(
            &group_name_value,
            worker_id,
            run_id,
            7,
            0,
            vec![BlockReportDeltaEntry {
                op: BlockReportDeltaOp::AddUpdate,
                block: report_block(1),
            }],
        )
        .unwrap();
    assert_eq!(
        manager.get_block_locations(&group_name_value, report_block(1).block_id),
        vec![worker_id]
    );

    manager
        .apply_delta_block_report(
            &group_name_value,
            worker_id,
            run_id,
            7,
            0,
            vec![BlockReportDeltaEntry {
                op: BlockReportDeltaOp::AddUpdate,
                block: report_block(1),
            }],
        )
        .unwrap();

    let gap = manager
        .apply_delta_block_report(
            &group_name_value,
            worker_id,
            run_id,
            7,
            3,
            vec![BlockReportDeltaEntry {
                op: BlockReportDeltaOp::Remove,
                block: report_block(1),
            }],
        )
        .expect_err("delta gap must require full report");
    assert!(gap.to_string().contains("full report required"));

    let epoch_mismatch = manager
        .apply_delta_block_report(
            &group_name_value,
            worker_id,
            run_id,
            8,
            1,
            vec![BlockReportDeltaEntry {
                op: BlockReportDeltaOp::Remove,
                block: report_block(1),
            }],
        )
        .expect_err("report_seq mismatch must require full report");
    assert!(epoch_mismatch.to_string().contains("full report required"));
}

#[test]
fn recreated_report_runtime_requires_full_report_again() {
    let manager = WorkerManager::new(60);
    let group_name_value = group_name("g24");
    let worker_id = WorkerId::new(8);
    let run_id = report_run_id();
    register_live_report_worker(&manager, &group_name_value, worker_id, run_id);
    manager
        .receive_full_block_report(&group_name_value, worker_id, run_id, 1, 0, true, vec![report_block(0)])
        .unwrap();
    assert_eq!(
        manager.get_block_locations(&group_name_value, report_block(0).block_id),
        vec![worker_id]
    );

    manager
        .load_registered_workers(vec![WorkerInfo {
            group_name: group_name_value.clone(),
            worker_id,
            address: "127.0.0.1:9090".to_string(),
            worker_net_protocol: 1,
            capacity_total: 0,
            capacity_used: 0,
            capacity_available: 0,
            active_reads: 0,
            active_writes: 0,
            health: HealthStatus::Healthy,
            last_heartbeat: 0,
            fault_domain: None,
        }])
        .unwrap();

    assert!(manager
        .get_block_locations(&group_name_value, report_block(0).block_id)
        .is_empty());
    let delta = manager
        .apply_delta_block_report(
            &group_name_value,
            worker_id,
            run_id,
            1,
            0,
            vec![BlockReportDeltaEntry {
                op: BlockReportDeltaOp::AddUpdate,
                block: report_block(1),
            }],
        )
        .expect_err("metadata restart must require a new full report");
    assert!(delta.to_string().contains("not registered"));
}

#[test]
fn reported_block_locations_are_group_qualified() {
    let manager = WorkerManager::new(60);
    let first_group = group_name("g31");
    let second_group = group_name("g32");
    let first_worker = WorkerId::new(10);
    let second_worker = WorkerId::new(11);
    let first_run = report_run_id();
    let second_run: WorkerRunId = "550e8400-e29b-41d4-a716-446655440102".parse().unwrap();
    let block_id = BlockId::new(DataHandleId::new(41), BlockIndex::new(0));

    register_live_report_worker(&manager, &first_group, first_worker, first_run);
    register_live_report_worker(&manager, &second_group, second_worker, second_run);
    manager
        .receive_full_block_report(
            &first_group,
            first_worker,
            first_run,
            1,
            0,
            true,
            vec![report_block_with_id(block_id)],
        )
        .unwrap();
    manager
        .receive_full_block_report(
            &second_group,
            second_worker,
            second_run,
            1,
            0,
            true,
            vec![report_block_with_id(block_id)],
        )
        .unwrap();

    assert_eq!(manager.get_block_locations(&first_group, block_id), vec![first_worker]);
    assert_eq!(
        manager.get_block_locations(&second_group, block_id),
        vec![second_worker]
    );

    let mut reported = manager.list_reported_blocks();
    reported.sort_by_key(|key| (key.group_name.to_string(), key.block_id.to_string()));
    assert_eq!(
        reported,
        vec![
            BlockLocationKey::new(&first_group, block_id),
            BlockLocationKey::new(&second_group, block_id),
        ]
    );

    manager
        .receive_full_block_report(&first_group, first_worker, first_run, 2, 0, true, Vec::new())
        .unwrap();

    assert!(manager.get_block_locations(&first_group, block_id).is_empty());
    assert_eq!(
        manager.get_block_locations(&second_group, block_id),
        vec![second_worker]
    );
    assert_eq!(
        manager.list_reported_blocks(),
        vec![BlockLocationKey::new(&second_group, block_id)]
    );
}

#[test]
fn block_report_runtime_sources_stay_memory_only() {
    let manager = include_str!("manager.rs");
    let service = include_str!("service.rs");
    let report_storage = ["report", "_storage"].concat();
    let put_report = ["put_block", "_report"].concat();
    let a = "block_";
    let b = "report_";
    let c = "storage";
    let block_storage_key = [a, b, c].concat();
    let d = "Command::";
    let e = "Report";
    let report_command = [d, e].concat();
    let f = "propose";
    let g = "_report";
    let propose_key = [f, g].concat();

    assert!(!manager.contains(&report_storage));
    assert!(!manager.contains(&put_report));
    assert!(!manager.contains(&block_storage_key));
    assert!(!manager.contains(&report_command));
    assert!(!service.contains(&report_command));
    assert!(!service.contains(&propose_key));
    assert!(!service.contains(&report_storage));
}

#[test]
fn worker_run_registration_is_group_scoped() {
    let manager = WorkerManager::new(60);
    let worker_id = WorkerId::new(1);
    let first_group = group_name("g1");
    let second_group = group_name("g2");
    let first_run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440010".parse().unwrap();
    let second_run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440011".parse().unwrap();

    manager
        .register_worker_run(
            &first_group,
            worker_id,
            "127.0.0.1:9090".to_string(),
            1,
            first_run_id,
            None,
        )
        .unwrap();
    manager
        .register_worker_run(
            &second_group,
            worker_id,
            "127.0.0.1:9091".to_string(),
            1,
            second_run_id,
            None,
        )
        .unwrap();

    let first = manager.get_descriptor(&first_group, worker_id).unwrap();
    let second = manager.get_descriptor(&second_group, worker_id).unwrap();
    let first_registration = manager.get_registration(&first_group, worker_id).unwrap();
    let second_registration = manager.get_registration(&second_group, worker_id).unwrap();
    assert_eq!(first.group_name, first_group);
    assert_eq!(first.address, "127.0.0.1:9090");
    assert_eq!(first_registration.worker_run_id, first_run_id);
    assert_eq!(second.group_name, second_group);
    assert_eq!(second.address, "127.0.0.1:9091");
    assert_eq!(second_registration.worker_run_id, second_run_id);
}

#[test]
fn worker_descriptor_runtime_and_liveness_are_group_scoped() {
    let manager = WorkerManager::new(60);
    let worker_id = WorkerId::new(7);
    let first_group = group_name("g11");
    let second_group = group_name("g12");
    let first_run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440050".parse().unwrap();
    let second_run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440051".parse().unwrap();

    manager
        .register_worker_run(
            &first_group,
            worker_id,
            "127.0.0.1:9107".to_string(),
            1,
            first_run_id,
            Some("rack-a".to_string()),
        )
        .unwrap();
    manager
        .register_worker_run(
            &second_group,
            worker_id,
            "127.0.0.1:9207".to_string(),
            1,
            second_run_id,
            Some("rack-b".to_string()),
        )
        .unwrap();
    manager
        .record_heartbeat(
            &first_group,
            worker_id,
            first_run_id,
            1,
            "127.0.0.1:9107",
            1,
            1_000,
            100,
            900,
            1,
            0,
            HealthStatus::Healthy,
        )
        .unwrap();
    manager
        .record_heartbeat(
            &second_group,
            worker_id,
            second_run_id,
            1,
            "127.0.0.1:9207",
            1,
            2_000,
            300,
            1_700,
            3,
            1,
            HealthStatus::Degraded,
        )
        .unwrap();

    let first_descriptor = manager.get_descriptor(&first_group, worker_id).unwrap();
    let second_descriptor = manager.get_descriptor(&second_group, worker_id).unwrap();
    let first_registration = manager.get_registration(&first_group, worker_id).unwrap();
    let second_registration = manager.get_registration(&second_group, worker_id).unwrap();
    let first_runtime = manager.get_worker(&first_group, worker_id).unwrap();
    let second_runtime = manager.get_worker(&second_group, worker_id).unwrap();

    assert_eq!(first_descriptor.address, "127.0.0.1:9107");
    assert_eq!(second_descriptor.address, "127.0.0.1:9207");
    assert_eq!(first_registration.worker_run_id, first_run_id);
    assert_eq!(second_registration.worker_run_id, second_run_id);
    assert_eq!(first_runtime.capacity_total, 1_000);
    assert_eq!(second_runtime.capacity_total, 2_000);
    assert!(manager.is_worker_live(&first_group, worker_id));
    assert!(manager.is_worker_live(&second_group, worker_id));
    let mut live_workers = manager.list_live_workers();
    live_workers.sort_by_key(|key| (key.group_name.to_string(), key.worker_id.as_raw()));
    assert_eq!(
        live_workers,
        vec![
            WorkerRegistrationKey::new(&first_group, worker_id),
            WorkerRegistrationKey::new(&second_group, worker_id),
        ]
    );
}

#[test]
fn worker_manager_api_does_not_expose_production_any_group_lookup() {
    let source = include_str!("manager.rs");
    assert!(
        !source.contains(concat!("pub fn get_worker", "_any_group")),
        "WorkerManager must not expose a production WorkerId-only lookup"
    );
    assert!(
        !source.contains("pub fn update_locations"),
        "WorkerManager must not expose direct block-location mutation outside report handling"
    );
}

#[test]
fn production_worker_lookup_sources_reject_implicit_group_patterns() {
    let sources = [
        ("metadata/src/worker/manager.rs", include_str!("manager.rs")),
        ("metadata/src/raft/storage.rs", include_str!("../raft/storage.rs")),
        (
            "metadata/src/service/fs_core/read.rs",
            include_str!("../service/fs_core/read.rs"),
        ),
        (
            "metadata/src/service/fs_core/write_session.rs",
            include_str!("../service/fs_core/write_session.rs"),
        ),
        (
            "metadata/src/maintenance/delete/executor.rs",
            include_str!("../maintenance/delete/executor.rs"),
        ),
        (
            "metadata/src/maintenance/overrep.rs",
            include_str!("../maintenance/overrep.rs"),
        ),
        (
            "metadata/src/maintenance/repair/planner.rs",
            include_str!("../maintenance/repair/planner.rs"),
        ),
        (
            "metadata/src/maintenance/repair/actions.rs",
            include_str!("../maintenance/repair/actions.rs"),
        ),
        (
            "metadata/src/maintenance/repair/types.rs",
            include_str!("../maintenance/repair/types.rs"),
        ),
    ];
    let forbidden = [
        concat!("unwrap_or_else(|| group_name(", "\"root\"))"),
        "pub fn get_worker(&self, worker_id: WorkerId)",
        concat!("get_worker", "_any_group"),
        concat!("get_block_locations_for", "_single", "_report_group"),
        concat!("get_report_groups", "_for_block"),
        concat!("reported_locations_for", "_single_group"),
        concat!("single", "_report", "_group"),
        "descriptors.iter().find_map",
        concat!("Move", "Copy"),
    ];

    for (path, source) in sources {
        for pattern in forbidden {
            assert!(
                !source.contains(pattern),
                "{path} contains forbidden implicit worker group pattern: {pattern}"
            );
        }
    }
}

#[test]
fn worker_run_registration_same_run_same_descriptor_is_idempotent() {
    let manager = WorkerManager::new(60);
    let worker_id = WorkerId::new(1);
    let group_name_value = group_name("g1");
    let first_run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440020".parse().unwrap();

    register_live_report_worker(&manager, &group_name_value, worker_id, first_run_id);
    manager
        .receive_full_block_report(
            &group_name_value,
            worker_id,
            first_run_id,
            1,
            0,
            true,
            vec![report_block(0)],
        )
        .unwrap();

    manager
        .validate_worker_registration_preflight(&group_name_value, worker_id, first_run_id, "127.0.0.1:9090", 1)
        .unwrap();
    manager
        .register_worker_run(
            &group_name_value,
            worker_id,
            "127.0.0.1:9090".to_string(),
            1,
            first_run_id,
            None,
        )
        .unwrap();

    let descriptor = manager.get_descriptor(&group_name_value, worker_id).unwrap();
    assert_eq!(descriptor.address, "127.0.0.1:9090");
    assert_eq!(descriptor.worker_net_protocol, 1);
    assert_eq!(
        manager
            .get_registration(&group_name_value, worker_id)
            .unwrap()
            .worker_run_id,
        first_run_id
    );
    assert!(manager.is_worker_live(&group_name_value, worker_id));
    assert!(!manager.needs_full_block_report(&group_name_value, worker_id));
    assert_eq!(
        manager.get_block_locations(&group_name_value, report_block(0).block_id),
        vec![worker_id]
    );
}

#[test]
fn worker_run_registration_rejects_same_run_endpoint_mismatch_without_clearing_state() {
    let manager = WorkerManager::new(60);
    let worker_id = WorkerId::new(2);
    let group_name_value = group_name("g1");
    let run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440021".parse().unwrap();

    register_live_report_worker(&manager, &group_name_value, worker_id, run_id);
    manager
        .receive_full_block_report(&group_name_value, worker_id, run_id, 1, 0, true, vec![report_block(0)])
        .unwrap();

    let error = manager
        .validate_worker_registration_preflight(&group_name_value, worker_id, run_id, "127.0.0.1:9091", 1)
        .expect_err("same worker_run_id must not change endpoint");
    assert!(matches!(error, MetadataError::InvalidArgument(_)));
    assert!(error.to_string().contains("worker descriptor mismatch"));

    let apply_error = manager
        .register_worker_run(
            &group_name_value,
            worker_id,
            "127.0.0.1:9091".to_string(),
            1,
            run_id,
            None,
        )
        .expect_err("same worker_run_id endpoint mismatch must fail at apply");
    assert!(matches!(apply_error, MetadataError::InvalidArgument(_)));
    assert!(apply_error.to_string().contains("worker descriptor mismatch"));

    let descriptor = manager.get_descriptor(&group_name_value, worker_id).unwrap();
    assert_eq!(descriptor.address, "127.0.0.1:9090");
    assert_eq!(descriptor.worker_net_protocol, 1);
    assert_eq!(
        manager
            .get_registration(&group_name_value, worker_id)
            .unwrap()
            .worker_run_id,
        run_id
    );
    assert!(manager.is_worker_live(&group_name_value, worker_id));
    assert!(!manager.needs_full_block_report(&group_name_value, worker_id));
    assert_eq!(
        manager.get_block_locations(&group_name_value, report_block(0).block_id),
        vec![worker_id]
    );
}

#[test]
fn worker_run_registration_rejects_same_run_protocol_mismatch() {
    let manager = WorkerManager::new(60);
    let worker_id = WorkerId::new(3);
    let group_name_value = group_name("g1");
    let run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440022".parse().unwrap();

    manager
        .register_worker_run(
            &group_name_value,
            worker_id,
            "127.0.0.1:9090".to_string(),
            1,
            run_id,
            None,
        )
        .unwrap();

    let error = manager
        .validate_worker_registration_preflight(&group_name_value, worker_id, run_id, "127.0.0.1:9090", 2)
        .expect_err("same worker_run_id must not change protocol");
    assert!(matches!(error, MetadataError::InvalidArgument(_)));
    assert!(error.to_string().contains("worker descriptor mismatch"));

    let apply_error = manager
        .register_worker_run(
            &group_name_value,
            worker_id,
            "127.0.0.1:9090".to_string(),
            2,
            run_id,
            None,
        )
        .expect_err("same worker_run_id protocol mismatch must fail at apply");
    assert!(matches!(apply_error, MetadataError::InvalidArgument(_)));
    assert!(apply_error.to_string().contains("worker descriptor mismatch"));
}

#[test]
fn worker_run_registration_replaces_restart_and_resets_run_state() {
    let manager = WorkerManager::new(60);
    let worker_id = WorkerId::new(4);
    let group_name_value = group_name("g1");
    let first_run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440023".parse().unwrap();
    let second_run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440024".parse().unwrap();

    register_live_report_worker(&manager, &group_name_value, worker_id, first_run_id);
    manager
        .receive_full_block_report(
            &group_name_value,
            worker_id,
            first_run_id,
            1,
            0,
            true,
            vec![report_block(0)],
        )
        .unwrap();
    assert_eq!(
        manager.get_block_locations(&group_name_value, report_block(0).block_id),
        vec![worker_id]
    );

    manager
        .register_worker_run(
            &group_name_value,
            worker_id,
            "127.0.0.1:9090".to_string(),
            1,
            second_run_id,
            None,
        )
        .unwrap();

    assert_eq!(
        manager
            .get_registration(&group_name_value, worker_id)
            .unwrap()
            .worker_run_id,
        second_run_id
    );
    assert!(!manager.is_worker_live(&group_name_value, worker_id));
    assert!(manager.needs_full_block_report(&group_name_value, worker_id));
    assert!(manager.get_worker_blocks(&group_name_value, worker_id).is_empty());
    assert!(manager
        .get_block_locations(&group_name_value, report_block(0).block_id)
        .is_empty());
    let after_restart_snapshot = manager.blockreport_convergence_snapshot(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
        60_000,
        manager.get_metadata_epoch(),
        0.80,
    );
    assert_eq!(after_restart_snapshot.active_workers, 0);
    assert_eq!(after_restart_snapshot.full_reported_workers, 0);

    let old_heartbeat = manager
        .record_heartbeat(
            &group_name_value,
            worker_id,
            first_run_id,
            2,
            "127.0.0.1:9090",
            1,
            1_000,
            100,
            900,
            0,
            0,
            HealthStatus::Healthy,
        )
        .expect_err("old worker_run_id must be fenced after replacement");
    assert!(matches!(old_heartbeat, MetadataError::StaleState(_)));
    assert!(old_heartbeat.to_string().contains("worker_run_id mismatch"));

    let old_report = manager
        .receive_full_block_report(
            &group_name_value,
            worker_id,
            first_run_id,
            2,
            0,
            true,
            vec![report_block(1)],
        )
        .expect_err("old worker_run_id block report must be fenced after replacement");
    assert!(matches!(old_report, MetadataError::StaleState(_)));
    assert!(old_report.to_string().contains("worker_run_id mismatch"));

    manager
        .record_heartbeat(
            &group_name_value,
            worker_id,
            second_run_id,
            1,
            "127.0.0.1:9090",
            1,
            1_000,
            100,
            900,
            0,
            0,
            HealthStatus::Healthy,
        )
        .unwrap();
    let before_new_full_report = manager.blockreport_convergence_snapshot(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64,
        60_000,
        manager.get_metadata_epoch(),
        0.80,
    );
    assert_eq!(before_new_full_report.active_workers, 1);
    assert_eq!(before_new_full_report.full_reported_workers, 0);
    assert!(!before_new_full_report.converged);

    let delta = manager
        .apply_delta_block_report(
            &group_name_value,
            worker_id,
            second_run_id,
            1,
            0,
            vec![BlockReportDeltaEntry {
                op: BlockReportDeltaOp::AddUpdate,
                block: report_block(1),
            }],
        )
        .expect_err("replacement must require a new full report baseline");
    assert!(matches!(delta, MetadataError::FullReportRequired(_)));
}

#[test]
fn worker_run_registration_updates_endpoint_when_previous_run_is_not_live() {
    let manager = WorkerManager::new(60);
    let worker_id = WorkerId::new(5);
    let group_name_value = group_name("g1");
    let first_run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440025".parse().unwrap();
    let second_run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440026".parse().unwrap();

    manager
        .register_worker_run(
            &group_name_value,
            worker_id,
            "127.0.0.1:9090".to_string(),
            1,
            first_run_id,
            None,
        )
        .unwrap();
    manager
        .validate_worker_registration_preflight(&group_name_value, worker_id, second_run_id, "127.0.0.1:9091", 2)
        .unwrap();
    manager
        .register_worker_run(
            &group_name_value,
            worker_id,
            "127.0.0.1:9091".to_string(),
            2,
            second_run_id,
            None,
        )
        .unwrap();

    let descriptor = manager.get_descriptor(&group_name_value, worker_id).unwrap();
    let registration = manager.get_registration(&group_name_value, worker_id).unwrap();
    assert_eq!(descriptor.address, "127.0.0.1:9091");
    assert_eq!(descriptor.worker_net_protocol, 2);
    assert_eq!(registration.worker_run_id, second_run_id);
}

#[test]
fn worker_run_registration_rejects_live_endpoint_conflict() {
    let manager = WorkerManager::new(60);
    let worker_id = WorkerId::new(6);
    let group_name_value = group_name("g1");
    let first_run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440027".parse().unwrap();
    let second_run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440028".parse().unwrap();

    register_live_report_worker(&manager, &group_name_value, worker_id, first_run_id);
    let error = manager
        .validate_worker_registration_preflight(&group_name_value, worker_id, second_run_id, "127.0.0.1:9091", 2)
        .expect_err("different endpoint for a live WorkerId must conflict");
    assert!(matches!(error, MetadataError::ActiveWorkerConflict(_)));
    assert!(error.to_string().contains("active worker conflict"));
    assert_eq!(
        manager
            .get_registration(&group_name_value, worker_id)
            .unwrap()
            .worker_run_id,
        first_run_id
    );
}

#[test]
fn loading_persisted_workers_drops_live_run_registration() {
    let manager = WorkerManager::new(60);
    let worker_id = WorkerId::new(1);
    let group_name_value = group_name("g1");
    let run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440030".parse().unwrap();

    manager
        .register_worker_run(
            &group_name_value,
            worker_id,
            "127.0.0.1:9090".to_string(),
            1,
            run_id,
            Some("rack-a".to_string()),
        )
        .unwrap();
    manager
        .record_heartbeat(
            &group_name_value,
            worker_id,
            run_id,
            1,
            "127.0.0.1:9090",
            1,
            1000,
            10,
            990,
            0,
            0,
            HealthStatus::Healthy,
        )
        .unwrap();
    manager
        .receive_full_block_report(&group_name_value, worker_id, run_id, 1, 0, true, vec![report_block(0)])
        .unwrap();

    manager
        .load_registered_workers(vec![WorkerInfo {
            group_name: group_name_value.clone(),
            worker_id,
            address: "127.0.0.1:9090".to_string(),
            worker_net_protocol: 1,
            capacity_total: 0,
            capacity_used: 0,
            capacity_available: 0,
            active_reads: 0,
            active_writes: 0,
            health: HealthStatus::Healthy,
            last_heartbeat: 0,
            fault_domain: Some("rack-a".to_string()),
        }])
        .unwrap();

    assert!(manager.get_registration(&group_name_value, worker_id).is_none());
    assert!(manager.get_descriptor(&group_name_value, worker_id).is_some());
    assert!(manager.get_worker(&group_name_value, worker_id).is_none());
    assert!(manager.needs_full_block_report(&group_name_value, worker_id));
}

#[test]
fn worker_heartbeat_updates_live_state_without_moving_stale_seq_backward() {
    let manager = WorkerManager::new(60);
    let group_name_value = group_name("g1");
    let worker_id = WorkerId::new(1);
    let run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440040".parse().unwrap();

    manager
        .register_worker_run(
            &group_name_value,
            worker_id,
            "127.0.0.1:9090".to_string(),
            1,
            run_id,
            Some("rack-a".to_string()),
        )
        .unwrap();

    let first = manager
        .record_heartbeat(
            &group_name_value,
            worker_id,
            run_id,
            10,
            "127.0.0.1:9090",
            1,
            1_000,
            100,
            900,
            2,
            1,
            HealthStatus::Healthy,
        )
        .unwrap();
    assert_eq!(first.heartbeat_seq, 10);
    assert_eq!(
        manager.get_worker(&group_name_value, worker_id).unwrap().capacity_total,
        1_000
    );

    let stale = manager
        .record_heartbeat(
            &group_name_value,
            worker_id,
            run_id,
            9,
            "127.0.0.1:9090",
            1,
            2_000,
            1_000,
            1_000,
            9,
            9,
            HealthStatus::Unhealthy,
        )
        .unwrap();
    assert_eq!(stale.heartbeat_seq, 10);

    let worker = manager.get_worker(&group_name_value, worker_id).unwrap();
    assert_eq!(worker.capacity_total, 1_000);
    assert_eq!(worker.active_reads, 2);
    assert_eq!(worker.health, HealthStatus::Healthy);
}

#[test]
fn heartbeat_liveness_expiry_removes_runtime_but_keeps_registration() {
    let manager = WorkerManager::new(1);
    let group_name_value = group_name("g1");
    let worker_id = WorkerId::new(1);
    let run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440041".parse().unwrap();

    manager
        .register_worker_run(
            &group_name_value,
            worker_id,
            "127.0.0.1:9090".to_string(),
            1,
            run_id,
            None,
        )
        .unwrap();
    manager
        .record_heartbeat(
            &group_name_value,
            worker_id,
            run_id,
            1,
            "127.0.0.1:9090",
            1,
            1_000,
            100,
            900,
            0,
            0,
            HealthStatus::Healthy,
        )
        .unwrap();

    std::thread::sleep(Duration::from_millis(1100));
    let expired = manager.expire_liveness();

    assert_eq!(expired, vec![(group_name_value.clone(), worker_id)]);
    assert!(!manager.is_worker_live(&group_name_value, worker_id));
    assert_eq!(
        manager
            .get_registration(&group_name_value, worker_id)
            .expect("current run registration")
            .worker_run_id,
        run_id
    );
    assert!(manager.get_descriptor(&group_name_value, worker_id).is_some());
}
