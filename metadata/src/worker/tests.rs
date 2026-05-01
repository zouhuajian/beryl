// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! Tests for worker manager and registration.

#[cfg(test)]
mod tests {
    use super::super::manager::{HealthStatus, WorkerManager};
    use types::ids::WorkerId;

    #[test]
    fn test_worker_registration_with_transport_kind_and_epoch() {
        let manager = WorkerManager::new(60);
        let worker_id = WorkerId::new(1);
        let address = "127.0.0.1:9090".to_string();
        let net_transport_kind = 1; // GRPC
        let worker_epoch = 100;

        // Register worker
        manager
            .register_worker(worker_id, address.clone(), net_transport_kind, worker_epoch, None)
            .unwrap();

        // Get descriptor and verify fields
        let descriptor = manager.get_descriptor(worker_id).unwrap();
        assert_eq!(descriptor.worker_id, worker_id);
        assert_eq!(descriptor.address, address);
        assert_eq!(descriptor.net_transport_kind, net_transport_kind);
        assert_eq!(descriptor.worker_epoch, worker_epoch);

        // get_worker requires both descriptor and runtime, so send a heartbeat first
        manager
            .update_runtime(
                worker_id,
                net_transport_kind,
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
        assert_eq!(worker.net_transport_kind, net_transport_kind);
        assert_eq!(worker.worker_epoch, worker_epoch);
    }

    #[test]
    fn test_worker_heartbeat_updates_transport_kind_and_epoch() {
        let manager = WorkerManager::new(60);
        let worker_id = WorkerId::new(1);
        let address = "127.0.0.1:9090".to_string();
        let net_transport_kind = 1; // GRPC
        let worker_epoch = 100;

        // Register worker
        manager
            .register_worker(worker_id, address.clone(), net_transport_kind, worker_epoch, None)
            .unwrap();

        // Update runtime (note: descriptor fields don't change via update_runtime)
        // Runtime fields are updated, but descriptor fields require re-register
        manager
            .update_runtime(
                worker_id,
                net_transport_kind, // Same as before (descriptor doesn't change)
                worker_epoch,       // Same as before (descriptor doesn't change)
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
        assert_eq!(worker.net_transport_kind, net_transport_kind);
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

    #[test]
    fn test_concurrent_full_sync_rate_limit() {
        // Test legacy try_start_full_sync (still works for backward compatibility)
        let manager = WorkerManager::new(60);

        // Register multiple workers
        let worker_ids: Vec<_> = (1..=15).map(WorkerId::new).collect();
        for &worker_id in &worker_ids {
            manager
                .register_worker(worker_id, "127.0.0.1:9090".to_string(), 1, 100, None)
                .unwrap();
        }

        // Try to start full sync for all workers
        let mut started = 0;
        let mut rate_limited = 0;
        for &worker_id in &worker_ids {
            if manager.try_start_full_sync(worker_id) {
                started += 1;
            } else {
                rate_limited += 1;
            }
        }

        // Should have started up to max_concurrent_full_syncs (default 10)
        assert_eq!(started, 10);
        assert_eq!(rate_limited, 5); // 15 - 10 = 5 rate limited

        // Complete some full syncs
        for &worker_id in &worker_ids[0..5] {
            manager.mark_full_sync_complete(worker_id);
        }

        // Now should be able to start more
        let mut additional_started = 0;
        for &worker_id in &worker_ids[10..15] {
            if manager.try_start_full_sync(worker_id) {
                additional_started += 1;
            }
        }

        // Should have started 5 more (freed up slots)
        assert_eq!(additional_started, 5);
    }
}
