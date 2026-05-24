// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Tests for worker manager and registration.

use super::manager::{HealthStatus, WorkerManager};
use types::ids::WorkerId;

#[test]
fn test_worker_registration_with_worker_net_protocol_and_epoch() {
    let manager = WorkerManager::new(60);
    let worker_id = WorkerId::new(1);
    let address = "127.0.0.1:9090".to_string();
    let worker_net_protocol = 1; // GRPC
    let worker_epoch = 100;

    // Register worker
    manager
        .register_worker(worker_id, address.clone(), worker_net_protocol, worker_epoch, None)
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
    let worker = manager.get_worker(worker_id).unwrap();
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
        .register_worker(worker_id, address.clone(), worker_net_protocol, worker_epoch, None)
        .unwrap();

    // Update runtime (note: descriptor fields don't change via update_runtime)
    // Runtime fields are updated, but descriptor fields require re-register
    manager
        .update_runtime(
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
    let worker = manager.get_worker(worker_id).unwrap();
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
        .register_worker(worker_id, "127.0.0.1:9090".to_string(), 1, 100, None)
        .unwrap();

    let unchanged = manager
        .update_runtime(worker_id, 1, 100, 1000, 500, 500, 0, 0, HealthStatus::Healthy)
        .unwrap();
    assert!(!unchanged);

    let epoch_changed = manager
        .update_runtime(worker_id, 1, 101, 1000, 500, 500, 0, 0, HealthStatus::Healthy)
        .unwrap();
    assert!(epoch_changed);

    let transport_changed = manager
        .update_runtime(worker_id, 2, 100, 1000, 500, 500, 0, 0, HealthStatus::Healthy)
        .unwrap();
    assert!(transport_changed);
}

#[test]
fn test_incremental_before_full_rejected() {
    let manager = WorkerManager::new(60);
    let worker_id = WorkerId::new(1);

    // Register worker
    manager
        .register_worker(worker_id, "127.0.0.1:9090".to_string(), 1, 100, None)
        .unwrap();

    // Try to apply incremental report before full sync
    let added = vec![types::ids::BlockId::new(
        types::ids::DataHandleId::new(1),
        types::ids::BlockIndex::new(0),
    )];
    let removed = vec![];

    let result = manager.apply_delta_report(worker_id, added, removed);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Full sync required"));
}
