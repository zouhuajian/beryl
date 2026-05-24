// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Tests for worker manager and registration.

use super::manager::{HealthStatus, WorkerInfo, WorkerManager};
use types::ids::{ShardGroupId, WorkerId};
use types::WorkerRunId;

#[test]
fn test_worker_registration_with_worker_net_protocol_and_epoch() {
    let manager = WorkerManager::new(60);
    let worker_id = WorkerId::new(1);
    let address = "127.0.0.1:9090".to_string();
    let worker_net_protocol = 1; // GRPC
    let worker_epoch = 100;

    // Register worker
    manager
        .register_worker(
            ShardGroupId::new(1),
            worker_id,
            address.clone(),
            worker_net_protocol,
            worker_epoch,
            None,
        )
        .unwrap();

    // Get descriptor and verify fields
    let descriptor = manager.get_descriptor(worker_id).unwrap();
    assert_eq!(descriptor.worker_id, worker_id);
    assert_eq!(descriptor.address, address);
    assert_eq!(descriptor.worker_net_protocol, worker_net_protocol);
    assert_eq!(descriptor.worker_epoch, worker_epoch);

    // get_worker requires both descriptor and runtime, so send a heartbeat first
    manager
        .update_runtime(
            ShardGroupId::new(1),
            worker_id,
            worker_net_protocol,
            worker_epoch,
            0,
            0,
            0,
            0,
            0,
            HealthStatus::Healthy,
        )
        .unwrap();

    // Now get_worker should work
    let worker = manager.get_worker(ShardGroupId::new(1), worker_id).unwrap();
    assert_eq!(worker.worker_id, worker_id);
    assert_eq!(worker.address, address);
    assert_eq!(worker.worker_net_protocol, worker_net_protocol);
    assert_eq!(worker.worker_epoch, worker_epoch);
}

#[test]
fn test_worker_heartbeat_updates_worker_net_protocol_and_epoch() {
    let manager = WorkerManager::new(60);
    let worker_id = WorkerId::new(1);
    let address = "127.0.0.1:9090".to_string();
    let worker_net_protocol = 1; // GRPC
    let worker_epoch = 100;

    // Register worker
    manager
        .register_worker(
            ShardGroupId::new(1),
            worker_id,
            address.clone(),
            worker_net_protocol,
            worker_epoch,
            None,
        )
        .unwrap();

    // Update runtime (note: descriptor fields don't change via update_runtime)
    // Runtime fields are updated, but descriptor fields require re-register
    manager
        .update_runtime(
            ShardGroupId::new(1),
            worker_id,
            worker_net_protocol, // Same as before (descriptor doesn't change)
            worker_epoch,        // Same as before (descriptor doesn't change)
            1000,
            500,
            500,
            0,
            0,
            HealthStatus::Healthy,
        )
        .unwrap();

    // Verify runtime fields are updated
    let worker = manager.get_worker(ShardGroupId::new(1), worker_id).unwrap();
    assert_eq!(worker.capacity_total, 1000);
    assert_eq!(worker.capacity_used, 500);
    assert_eq!(worker.capacity_available, 500);
    // Descriptor fields remain unchanged (require re-register via Raft)
    assert_eq!(worker.worker_net_protocol, worker_net_protocol);
    assert_eq!(worker.worker_epoch, worker_epoch);
}

#[test]
fn test_worker_heartbeat_detects_descriptor_change() {
    let manager = WorkerManager::new(60);
    let worker_id = WorkerId::new(1);

    manager
        .register_worker(
            ShardGroupId::new(1),
            worker_id,
            "127.0.0.1:9090".to_string(),
            1,
            100,
            None,
        )
        .unwrap();

    let unchanged = manager
        .update_runtime(
            ShardGroupId::new(1),
            worker_id,
            1,
            100,
            1000,
            500,
            500,
            0,
            0,
            HealthStatus::Healthy,
        )
        .unwrap();
    assert!(!unchanged);

    let epoch_changed = manager
        .update_runtime(
            ShardGroupId::new(1),
            worker_id,
            1,
            101,
            1000,
            500,
            500,
            0,
            0,
            HealthStatus::Healthy,
        )
        .unwrap();
    assert!(epoch_changed);

    let transport_changed = manager
        .update_runtime(
            ShardGroupId::new(1),
            worker_id,
            2,
            100,
            1000,
            500,
            500,
            0,
            0,
            HealthStatus::Healthy,
        )
        .unwrap();
    assert!(transport_changed);
}

#[test]
fn test_incremental_before_full_rejected() {
    let manager = WorkerManager::new(60);
    let worker_id = WorkerId::new(1);

    // Register worker
    manager
        .register_worker(
            ShardGroupId::new(1),
            worker_id,
            "127.0.0.1:9090".to_string(),
            1,
            100,
            None,
        )
        .unwrap();

    // Try to apply incremental report before full sync
    let added = vec![types::ids::BlockId::new(
        types::ids::DataHandleId::new(1),
        types::ids::BlockIndex::new(0),
    )];
    let removed = vec![];

    let result = manager.apply_delta_report(ShardGroupId::new(1), worker_id, added, removed);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Full sync required"));
}

#[test]
fn worker_run_registration_is_group_scoped() {
    let manager = WorkerManager::new(60);
    let worker_id = WorkerId::new(1);
    let first_group = ShardGroupId::new(1);
    let second_group = ShardGroupId::new(2);
    let first_run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440010".parse().unwrap();
    let second_run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440011".parse().unwrap();

    manager
        .register_worker_run(
            first_group,
            worker_id,
            "127.0.0.1:9090".to_string(),
            1,
            first_run_id,
            None,
        )
        .unwrap();
    manager
        .register_worker_run(
            second_group,
            worker_id,
            "127.0.0.1:9091".to_string(),
            1,
            second_run_id,
            None,
        )
        .unwrap();

    let first = manager.get_descriptor_in_group(first_group, worker_id).unwrap();
    let second = manager.get_descriptor_in_group(second_group, worker_id).unwrap();
    let first_registration = manager.get_registration_in_group(first_group, worker_id).unwrap();
    let second_registration = manager.get_registration_in_group(second_group, worker_id).unwrap();
    assert_eq!(first.group_id, first_group);
    assert_eq!(first.address, "127.0.0.1:9090");
    assert_eq!(first_registration.worker_run_id, first_run_id);
    assert_eq!(second.group_id, second_group);
    assert_eq!(second.address, "127.0.0.1:9091");
    assert_eq!(second_registration.worker_run_id, second_run_id);
}

#[test]
fn worker_run_registration_conflict_is_live_group_local() {
    let manager = WorkerManager::new(60);
    let worker_id = WorkerId::new(1);
    let group_id = ShardGroupId::new(1);
    let first_run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440020".parse().unwrap();
    let second_run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440021".parse().unwrap();

    manager
        .register_worker_run(group_id, worker_id, "127.0.0.1:9090".to_string(), 1, first_run_id, None)
        .unwrap();
    manager
        .register_worker_run(group_id, worker_id, "127.0.0.1:9090".to_string(), 1, first_run_id, None)
        .unwrap();

    let error = manager
        .register_worker_run(
            group_id,
            worker_id,
            "127.0.0.1:9091".to_string(),
            1,
            second_run_id,
            None,
        )
        .expect_err("different live WorkerRunId must conflict");
    assert!(error.to_string().contains("already registered"));
    assert_eq!(
        manager
            .get_registration_in_group(group_id, worker_id)
            .unwrap()
            .worker_run_id,
        first_run_id
    );
}

#[test]
fn loading_persisted_workers_drops_live_run_registration() {
    let manager = WorkerManager::new(60);
    let worker_id = WorkerId::new(1);
    let group_id = ShardGroupId::new(1);
    let run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440030".parse().unwrap();

    manager
        .register_worker_run(
            group_id,
            worker_id,
            "127.0.0.1:9090".to_string(),
            1,
            run_id,
            Some("rack-a".to_string()),
        )
        .unwrap();
    manager
        .update_runtime(group_id, worker_id, 1, 0, 1000, 10, 990, 0, 0, HealthStatus::Healthy)
        .unwrap();
    manager.mark_full_sync_complete(group_id, worker_id);

    manager
        .load_registered_workers(vec![WorkerInfo {
            group_id,
            worker_id,
            address: "127.0.0.1:9090".to_string(),
            worker_net_protocol: 1,
            worker_epoch: 0,
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

    assert!(manager.get_registration_in_group(group_id, worker_id).is_none());
    assert!(manager.get_descriptor_in_group(group_id, worker_id).is_some());
    assert!(manager.get_worker(group_id, worker_id).is_none());
    assert!(manager.needs_full_sync(group_id, worker_id));
}
