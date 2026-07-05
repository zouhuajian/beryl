// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use super::*;
use crate::config::RaftConfig;
use crate::mount::{DataIoPolicy, MountEntry, MountKind, ROOT_INODE_ID};
use crate::placement::{ReportedBlockLocation, WorkerPlacementView};
use crate::raft::{AppRaftNode, AppRaftStateMachine, Command, DedupKey, RocksDBStorage};
use crate::service::domain::{
    AbortWriteInput, AddBlockInput, CloseWriteInput, CloseWriteIntent, CoreResult, CreateInput, Freshness,
    GetAttrInput, GetFileLayoutInput, OpenWriteInput, PresentedFencingToken, ReadDirInput, RenameInput,
    RenewLeaseInput, RequestContext, SessionKey, SyncWriteInput, SyncWriteMode, UnlinkInput,
};
use crate::state::{MemoryStateStore, RouteEpoch};
use crate::worker::{BlockReportBlock, BlockReportBlockState, HealthStatus, WorkerInfo, WorkerManager};
use async_trait::async_trait;
use common::error::canonical::{ErrorClass, ErrorCode as CanonicalErrorCode, RefreshReason};
use common::header::{AuthnType, CallerContext, RequestHeader, RpcErrorCode};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;
use types::fs::{FileAttrs, Inode};
use types::ids::{BlockId, BlockIndex, ClientId, DataHandleId, LeaseId, MountId, WorkerId};
use types::layout::FileLayout;
use types::lease::FencingToken;
use types::worker::WorkerNetProtocol;
use types::{CommittedBlock, GroupName, Tier, TierFree, WorkerEndpointInfo, WorkerRunId, WriteTarget};

use super::freshness::{FreshnessValidator, StaleStateStatus};

struct StorageBackedRouteEpochStore {
    storage: Arc<RocksDBStorage>,
}

impl FsCore {
    fn new_default(state_store: Arc<dyn StateStore>, mount_table: Arc<MountTable>) -> Self {
        let write_session_manager = Arc::new(crate::write_session::WriteSessionManager::default());
        let inode_lease_manager = Arc::new(crate::inode_lease::InodeLeaseManager::default());
        let worker_commit_hook: SharedWorkerCommitHook = Arc::new(Mutex::new(None));
        Self::new(
            state_store,
            mount_table,
            write_session_manager,
            inode_lease_manager,
            worker_commit_hook,
        )
    }

    fn write_session_manager_for_test(&self) -> Arc<crate::write_session::WriteSessionManager> {
        Arc::clone(&self.write_session_manager)
    }

    fn inode_lease_manager_for_test(&self) -> Arc<crate::inode_lease::InodeLeaseManager> {
        Arc::clone(&self.inode_lease_manager)
    }
}

#[async_trait]
impl StateStore for StorageBackedRouteEpochStore {
    async fn get_route_epoch(&self) -> MetadataResult<RouteEpoch> {
        self.storage.get_route_epoch()
    }
}

fn request_context() -> RequestContext {
    RequestContext {
        caller: RequestHeader::new(ClientId::new(7)),
        traceparent: None,
        route_epoch: None,
        principal: None,
        real_user: None,
        doas: None,
        authn_type: AuthnType::Unspecified,
    }
}

fn group_name(raw: &str) -> GroupName {
    GroupName::parse(raw).unwrap()
}

fn fs_core_with_mount(mount_id: MountId, mount_epoch: u64, group_name: &GroupName) -> FsCore {
    let mount_table = Arc::new(MountTable::new());
    mount_table
        .upsert(MountEntry {
            mount_id,
            mount_prefix: "/".to_string(),
            mount_kind: MountKind::Internal,
            ufs_uri: None,
            data_io_policy: DataIoPolicy::Allow,
            mount_epoch,
            namespace_owner_group_name: group_name.clone(),
            root_inode_id: ROOT_INODE_ID,
        })
        .unwrap();
    FsCore::new_default(Arc::new(MemoryStateStore::new()), mount_table)
}

fn worker_run_id(group_name: &GroupName, worker_id: WorkerId) -> WorkerRunId {
    let group_component = group_name
        .as_str()
        .bytes()
        .fold(0u64, |acc, byte| acc.saturating_add(u64::from(byte)));
    let suffix = group_component
        .saturating_mul(1_000_000)
        .saturating_add(worker_id.as_raw());
    format!("550e8400-e29b-41d4-a716-{suffix:012x}")
        .parse()
        .expect("valid test WorkerRunId")
}

#[allow(clippy::too_many_arguments)]
fn record_worker_heartbeat(
    manager: &WorkerManager,
    group_name: &GroupName,
    worker_id: WorkerId,
    capacity_total: u64,
    capacity_used: u64,
    capacity_available: u64,
    active_reads: u32,
    active_writes: u32,
    health: HealthStatus,
) {
    record_worker_heartbeat_with_tiers(
        manager,
        group_name,
        worker_id,
        capacity_total,
        capacity_used,
        capacity_available,
        vec![TierFree {
            tier: Tier::Hdd,
            free_bytes: capacity_available,
        }],
        active_reads,
        active_writes,
        health,
    );
}

#[allow(clippy::too_many_arguments)]
fn record_worker_heartbeat_with_tiers(
    manager: &WorkerManager,
    group_name: &GroupName,
    worker_id: WorkerId,
    capacity_total: u64,
    capacity_used: u64,
    capacity_available: u64,
    tier_free: Vec<TierFree>,
    active_reads: u32,
    active_writes: u32,
    health: HealthStatus,
) {
    let descriptor = manager
        .get_descriptor(group_name, worker_id)
        .expect("worker descriptor should be registered");
    let run_id = manager
        .get_registration(group_name, worker_id)
        .map(|registration| registration.worker_run_id)
        .unwrap_or_else(|| {
            let run_id = worker_run_id(group_name, worker_id);
            manager
                .register_worker_run(
                    group_name,
                    worker_id,
                    descriptor.address.clone(),
                    descriptor.worker_net_protocol,
                    run_id,
                    descriptor.fault_domain.clone(),
                )
                .expect("worker run should register");
            run_id
        });
    manager
        .record_heartbeat_with_tier_free(
            group_name,
            worker_id,
            run_id,
            1,
            &descriptor.address,
            descriptor.worker_net_protocol,
            capacity_total,
            capacity_used,
            capacity_available,
            tier_free,
            active_reads,
            active_writes,
            health,
        )
        .expect("heartbeat should be accepted");
    manager
        .upsert_descriptor(descriptor)
        .expect("descriptor should be restored");
}

fn worker_manager_for_tier(group_name: &GroupName, tier: Tier, free_bytes: u64) -> Arc<WorkerManager> {
    let manager = Arc::new(WorkerManager::new(60));
    let worker_id = WorkerId::new(11);
    manager
        .register_worker(group_name, worker_id, "127.0.0.1:9111".to_string(), 1, None)
        .unwrap();
    record_worker_heartbeat_with_tiers(
        &manager,
        group_name,
        worker_id,
        free_bytes,
        0,
        free_bytes,
        vec![TierFree { tier, free_bytes }],
        0,
        0,
        HealthStatus::Healthy,
    );
    manager
}

fn report_block(block_id: BlockId) -> BlockReportBlock {
    report_block_with_stamp(block_id, u64::from(block_id.index.as_raw()) + 1)
}

fn report_block_with_stamp(block_id: BlockId, block_stamp: u64) -> BlockReportBlock {
    report_block_with_stamp_and_state(block_id, block_stamp, BlockReportBlockState::Ready)
}

fn report_block_with_stamp_and_state(
    block_id: BlockId,
    block_stamp: u64,
    block_state: BlockReportBlockState,
) -> BlockReportBlock {
    BlockReportBlock {
        block_id,
        data_handle_id: block_id.data_handle_id.as_raw(),
        block_index: block_id.index.as_raw(),
        block_stamp,
        effective_len: 4096,
        committed_length: 4096,
        block_state,
    }
}

fn publish_report_locations(
    manager: &WorkerManager,
    group_name: &GroupName,
    worker_id: WorkerId,
    report_seq: u64,
    blocks: Vec<BlockId>,
) {
    publish_report_locations_with_stamp(manager, group_name, worker_id, report_seq, None, blocks);
}

fn publish_report_locations_with_stamp(
    manager: &WorkerManager,
    group_name: &GroupName,
    worker_id: WorkerId,
    report_seq: u64,
    block_stamp: Option<u64>,
    blocks: Vec<BlockId>,
) {
    let run_id = manager
        .get_registration(group_name, worker_id)
        .expect("worker registration")
        .worker_run_id;
    manager
        .receive_full_block_report(
            group_name,
            worker_id,
            run_id,
            report_seq,
            0,
            true,
            blocks
                .into_iter()
                .map(|block_id| {
                    block_stamp
                        .map(|stamp| report_block_with_stamp(block_id, stamp))
                        .unwrap_or_else(|| report_block(block_id))
                })
                .collect(),
        )
        .expect("full block report should publish locations");
}

fn publish_report_block(
    manager: &WorkerManager,
    group_name: &GroupName,
    worker_id: WorkerId,
    report_seq: u64,
    block: BlockReportBlock,
) {
    let run_id = manager
        .get_registration(group_name, worker_id)
        .expect("worker registration")
        .worker_run_id;
    manager
        .receive_full_block_report(group_name, worker_id, run_id, report_seq, 0, true, vec![block])
        .expect("full block report should publish locations");
}

fn worker_manager_for_write_targets(group_name: &GroupName) -> Arc<WorkerManager> {
    let manager = Arc::new(WorkerManager::new(60));
    for raw in 1..=3 {
        let worker_id = types::ids::WorkerId::new(raw);
        manager
            .register_worker(group_name, worker_id, format!("127.0.0.1:{}", 9000 + raw), 1, None)
            .unwrap();
        record_worker_heartbeat(
            &manager,
            group_name,
            worker_id,
            1024 * 1024,
            0,
            1024 * 1024,
            0,
            0,
            HealthStatus::Healthy,
        );
    }
    manager
}

fn fs_core_without_mount() -> FsCore {
    FsCore::new_default(Arc::new(MemoryStateStore::new()), Arc::new(MountTable::new()))
}

fn assert_block_location_unavailable(failure: &CoreFailure, block_id: BlockId) {
    assert_eq!(
        failure.error.code,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::BlockLocationUnavailable))
    );
    assert_eq!(failure.error.reason, Some(RefreshReason::BlockLocationUnavailable));
    assert!(
        failure.error.message.contains(&block_id.to_string()),
        "error should include block id context: {}",
        failure.error.message
    );
}

#[tokio::test]
async fn get_file_layout_returns_reported_locations_using_read_placement() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_id = MountId::new(48);
    let group_name_value = group_name("g8");
    let inode_id = InodeId::new(480);
    let data_handle_id = DataHandleId::new(9480);
    let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
    let mut fs_core = fs_core_with_mount(mount_id, 9, &group_name_value);
    let worker_manager = Arc::new(WorkerManager::new(60));
    for (raw, endpoint) in [(2, "127.0.0.2:9102"), (1, "127.0.0.1:9101")] {
        let worker_id = WorkerId::new(raw);
        worker_manager
            .register_worker(&group_name_value, worker_id, endpoint.to_string(), 1, None)
            .unwrap();
        record_worker_heartbeat(
            &worker_manager,
            &group_name_value,
            worker_id,
            1024,
            0,
            1024,
            0,
            0,
            HealthStatus::Healthy,
        );
        publish_report_locations_with_stamp(
            &worker_manager,
            &group_name_value,
            worker_id,
            raw,
            Some(41),
            vec![block_id],
        );
    }
    fs_core.set_storage(Arc::clone(&storage));
    fs_core.set_worker_manager(worker_manager);

    let mut attrs = FileAttrs::new();
    attrs.size = 512;
    let mut inode = Inode::new_file(inode_id, attrs, mount_id, data_handle_id);
    inode.data = types::fs::InodeData::File {
        extents: vec![types::fs::Extent {
            file_offset: 0,
            block_id,
            block_offset: 0,
            len: 512,
            file_version: None,
            block_stamp: Some(41),
        }],
        file_version: Some(1),
        lease_epoch: None,
    };
    storage.put_inode(&inode).unwrap();
    storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
    storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

    let mut ctx = request_context();
    ctx.caller = ctx.caller.with_caller_context(CallerContext {
        context: "host=127.0.0.2".to_string(),
        signature: None,
    });

    let success = fs_core
        .execute_get_file_layout(GetFileLayoutInput {
            ctx,
            inode_id,
            range: None,
            requested_data_handle_id: None,
            freshness: Freshness::default(),
        })
        .await
        .expect("layout read succeeds");

    assert_eq!(success.payload.locations.len(), 1);
    let location = &success.payload.locations[0];
    assert_eq!(location.block_id, block_id);
    assert_eq!(
        location
            .workers
            .iter()
            .map(|worker| worker.worker_id)
            .collect::<Vec<_>>(),
        vec![WorkerId::new(2), WorkerId::new(1)]
    );
    assert_eq!(location.block_stamp, 41);
    assert_eq!(location.block_format_id, types::BlockFormatId::CURRENT_FOR_NEW_FILE);
    assert_eq!(location.block_size, 4096);
    assert_eq!(location.chunk_size, 4096);
    assert_eq!(location.effective_len, 512);
    assert_eq!(location.workers[0].endpoint, "127.0.0.2:9102");
    assert_eq!(
        location.workers[0].worker_run_id,
        worker_run_id(&group_name_value, WorkerId::new(2))
    );
}

#[tokio::test]
async fn get_file_layout_rejects_visible_block_when_report_is_missing() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_id = MountId::new(53);
    let group_name_value = group_name("g8");
    let inode_id = InodeId::new(530);
    let data_handle_id = DataHandleId::new(9530);
    let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
    let mut fs_core = fs_core_with_mount(mount_id, 9, &group_name_value);
    fs_core.set_storage(Arc::clone(&storage));
    fs_core.set_worker_manager(Arc::new(WorkerManager::new(60)));

    let mut attrs = FileAttrs::new();
    attrs.size = 512;
    let mut inode = Inode::new_file(inode_id, attrs, mount_id, data_handle_id);
    inode.data = types::fs::InodeData::File {
        extents: vec![types::fs::Extent {
            file_offset: 0,
            block_id,
            block_offset: 0,
            len: 512,
            file_version: Some(1),
            block_stamp: Some(41),
        }],
        file_version: Some(1),
        lease_epoch: None,
    };
    storage.put_inode(&inode).unwrap();
    storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
    storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

    let failure = fs_core
        .execute_get_file_layout(GetFileLayoutInput {
            ctx: request_context(),
            inode_id,
            range: None,
            requested_data_handle_id: None,
            freshness: Freshness::default(),
        })
        .await
        .expect_err("visible block without reported location must fail precisely");

    assert_block_location_unavailable(&failure, block_id);
}

#[tokio::test]
async fn get_file_layout_filters_non_ready_reported_locations() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_id = MountId::new(54);
    let group_name_value = group_name("g8");
    let inode_id = InodeId::new(540);
    let data_handle_id = DataHandleId::new(9540);
    let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
    let worker_id = WorkerId::new(1);
    let mut fs_core = fs_core_with_mount(mount_id, 9, &group_name_value);
    let worker_manager = Arc::new(WorkerManager::new(60));
    worker_manager
        .register_worker(&group_name_value, worker_id, "127.0.0.1:9101".to_string(), 1, None)
        .unwrap();
    record_worker_heartbeat(
        &worker_manager,
        &group_name_value,
        worker_id,
        1024,
        0,
        1024,
        0,
        0,
        HealthStatus::Healthy,
    );
    publish_report_block(
        &worker_manager,
        &group_name_value,
        worker_id,
        1,
        report_block_with_stamp_and_state(block_id, 41, BlockReportBlockState::Partial),
    );
    fs_core.set_storage(Arc::clone(&storage));
    fs_core.set_worker_manager(worker_manager);

    let mut attrs = FileAttrs::new();
    attrs.size = 512;
    let mut inode = Inode::new_file(inode_id, attrs, mount_id, data_handle_id);
    inode.data = types::fs::InodeData::File {
        extents: vec![types::fs::Extent {
            file_offset: 0,
            block_id,
            block_offset: 0,
            len: 512,
            file_version: Some(1),
            block_stamp: Some(41),
        }],
        file_version: Some(1),
        lease_epoch: None,
    };
    storage.put_inode(&inode).unwrap();
    storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
    storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

    let failure = fs_core
        .execute_get_file_layout(GetFileLayoutInput {
            ctx: request_context(),
            inode_id,
            range: None,
            requested_data_handle_id: None,
            freshness: Freshness::default(),
        })
        .await
        .expect_err("non-ready report must not produce an empty worker location");

    assert_block_location_unavailable(&failure, block_id);
}

#[tokio::test]
async fn get_file_layout_filters_reported_locations_with_mismatched_block_stamp() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_id = MountId::new(55);
    let group_name_value = group_name("g8");
    let inode_id = InodeId::new(550);
    let data_handle_id = DataHandleId::new(9550);
    let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
    let worker_id = WorkerId::new(1);
    let mut fs_core = fs_core_with_mount(mount_id, 9, &group_name_value);
    let worker_manager = Arc::new(WorkerManager::new(60));
    worker_manager
        .register_worker(&group_name_value, worker_id, "127.0.0.1:9101".to_string(), 1, None)
        .unwrap();
    record_worker_heartbeat(
        &worker_manager,
        &group_name_value,
        worker_id,
        1024,
        0,
        1024,
        0,
        0,
        HealthStatus::Healthy,
    );
    publish_report_locations_with_stamp(
        &worker_manager,
        &group_name_value,
        worker_id,
        1,
        Some(40),
        vec![block_id],
    );
    fs_core.set_storage(Arc::clone(&storage));
    fs_core.set_worker_manager(worker_manager);

    let mut attrs = FileAttrs::new();
    attrs.size = 512;
    let mut inode = Inode::new_file(inode_id, attrs, mount_id, data_handle_id);
    inode.data = types::fs::InodeData::File {
        extents: vec![types::fs::Extent {
            file_offset: 0,
            block_id,
            block_offset: 0,
            len: 512,
            file_version: Some(1),
            block_stamp: Some(41),
        }],
        file_version: Some(1),
        lease_epoch: None,
    };
    storage.put_inode(&inode).unwrap();
    storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
    storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

    let failure = fs_core
        .execute_get_file_layout(GetFileLayoutInput {
            ctx: request_context(),
            inode_id,
            range: None,
            requested_data_handle_id: None,
            freshness: Freshness::default(),
        })
        .await
        .expect_err("mismatched reported block stamp must fail precisely");

    assert_eq!(
        failure.error.code,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::BlockStampMismatch))
    );
    assert_eq!(failure.error.reason, Some(RefreshReason::BlockStampMismatch));
    assert!(failure.error.message.contains(&block_id.to_string()));
}

#[tokio::test]
async fn get_file_layout_rejects_visible_block_when_reported_worker_is_not_live() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_id = MountId::new(56);
    let group_name_value = group_name("g8");
    let inode_id = InodeId::new(560);
    let data_handle_id = DataHandleId::new(9560);
    let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
    let worker_id = WorkerId::new(1);
    let mut fs_core = fs_core_with_mount(mount_id, 9, &group_name_value);
    let worker_manager = Arc::new(WorkerManager::new(1));
    worker_manager
        .register_worker(&group_name_value, worker_id, "127.0.0.1:9101".to_string(), 1, None)
        .unwrap();
    record_worker_heartbeat(
        &worker_manager,
        &group_name_value,
        worker_id,
        1024,
        0,
        1024,
        0,
        0,
        HealthStatus::Healthy,
    );
    publish_report_locations_with_stamp(
        &worker_manager,
        &group_name_value,
        worker_id,
        1,
        Some(41),
        vec![block_id],
    );
    std::thread::sleep(Duration::from_millis(1100));
    assert_eq!(
        worker_manager.expire_liveness(),
        vec![(group_name_value.clone(), worker_id)]
    );
    fs_core.set_storage(Arc::clone(&storage));
    fs_core.set_worker_manager(worker_manager);

    let mut attrs = FileAttrs::new();
    attrs.size = 512;
    let mut inode = Inode::new_file(inode_id, attrs, mount_id, data_handle_id);
    inode.data = types::fs::InodeData::File {
        extents: vec![types::fs::Extent {
            file_offset: 0,
            block_id,
            block_offset: 0,
            len: 512,
            file_version: Some(1),
            block_stamp: Some(41),
        }],
        file_version: Some(1),
        lease_epoch: None,
    };
    storage.put_inode(&inode).unwrap();
    storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
    storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

    let failure = fs_core
        .execute_get_file_layout(GetFileLayoutInput {
            ctx: request_context(),
            inode_id,
            range: None,
            requested_data_handle_id: None,
            freshness: Freshness::default(),
        })
        .await
        .expect_err("reported block on an expired worker must fail precisely");

    assert_block_location_unavailable(&failure, block_id);
}

#[test]
fn unavailable_read_location_classifier_detects_stale_worker_run() {
    let group_name_value = group_name("g8b");
    let data_handle_id = DataHandleId::new(9561);
    let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
    let worker_id = WorkerId::new(1);
    let reported_run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440061".parse().unwrap();
    let current_run_id: WorkerRunId = "550e8400-e29b-41d4-a716-446655440062".parse().unwrap();
    let extent = types::fs::Extent {
        file_offset: 0,
        block_id,
        block_offset: 0,
        len: 512,
        file_version: Some(1),
        block_stamp: Some(41),
    };
    let reported = vec![ReportedBlockLocation {
        group_name: group_name_value.clone(),
        block_id,
        block_stamp: 41,
        worker_id,
        worker_run_id: reported_run_id,
    }];
    let views = vec![WorkerPlacementView {
        group_name: group_name_value.clone(),
        worker_id,
        worker_run_id: Some(current_run_id),
        endpoint: "127.0.0.1:9101".to_string(),
        worker_net_protocol: 1,
        registered: true,
        lease_valid: true,
        ip: Some("127.0.0.1".to_string()),
        host: Some("127.0.0.1".to_string()),
        az: None,
        rack: None,
        region: None,
        free_bytes: Some(1024),
        tier_free: vec![TierFree {
            tier: Tier::Hdd,
            free_bytes: 1024,
        }],
        supported_block_formats: vec![types::BlockFormatId::CURRENT_FOR_NEW_FILE],
    }];

    let (rpc_code, reason, message) = FsCore::classify_unavailable_read_location(
        &request_context(),
        &group_name_value,
        &extent,
        41,
        &reported,
        &views,
    );

    assert_eq!(rpc_code, RpcErrorCode::WorkerRunMismatch);
    assert_eq!(reason, RefreshReason::WorkerRunMismatch);
    assert!(message.contains("stale worker run"));
    assert!(message.contains(&block_id.to_string()));
}

#[tokio::test]
async fn get_file_layout_rejects_worker_lookup_without_authoritative_group() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_id = MountId::new(52);
    let inode_id = InodeId::new(520);
    let data_handle_id = DataHandleId::new(9520);
    let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
    let worker_id = WorkerId::new(1);
    let mut fs_core = fs_core_without_mount();
    let worker_manager = Arc::new(WorkerManager::new(60));
    let fallback_group = group_name("root");

    worker_manager
        .register_worker(&fallback_group, worker_id, "127.0.0.1:9101".to_string(), 1, None)
        .unwrap();
    record_worker_heartbeat(
        &worker_manager,
        &fallback_group,
        worker_id,
        1024,
        0,
        1024,
        0,
        0,
        HealthStatus::Healthy,
    );
    publish_report_locations(&worker_manager, &fallback_group, worker_id, 1, vec![block_id]);
    fs_core.set_storage(Arc::clone(&storage));
    fs_core.set_worker_manager(worker_manager);

    let mut attrs = FileAttrs::new();
    attrs.size = 512;
    let mut inode = Inode::new_file(inode_id, attrs, mount_id, data_handle_id);
    inode.data = types::fs::InodeData::File {
        extents: vec![types::fs::Extent {
            file_offset: 0,
            block_id,
            block_offset: 0,
            len: 512,
            file_version: Some(1),
            block_stamp: Some(41),
        }],
        file_version: Some(1),
        lease_epoch: None,
    };
    storage.put_inode(&inode).unwrap();
    storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
    storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

    let failure = fs_core
        .execute_get_file_layout(GetFileLayoutInput {
            ctx: request_context(),
            inode_id,
            range: None,
            requested_data_handle_id: None,
            freshness: Freshness::default(),
        })
        .await
        .expect_err("missing mount owner group must reject worker lookup");

    assert!(
        failure
            .error
            .message
            .contains("GetFileLayout worker lookup requires authoritative metadata group"),
        "unexpected error: {}",
        failure.error.message
    );
    assert_eq!(failure.group_name, None);
}

#[tokio::test]
async fn get_file_layout_does_not_cross_read_worker_descriptor_from_other_group() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_id = MountId::new(51);
    let served_group = group_name("g9");
    let other_group = group_name("g10");
    let inode_id = InodeId::new(510);
    let data_handle_id = DataHandleId::new(9510);
    let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
    let worker_id = WorkerId::new(1);
    let worker_run_id: types::WorkerRunId = "550e8400-e29b-41d4-a716-446655440052".parse().unwrap();
    let mut fs_core = fs_core_with_mount(mount_id, 9, &served_group);
    let worker_manager = Arc::new(WorkerManager::new(60));

    worker_manager
        .register_worker_run(
            &other_group,
            worker_id,
            "127.0.0.1:9999".to_string(),
            1,
            worker_run_id,
            None,
        )
        .unwrap();
    worker_manager
        .record_heartbeat(
            &other_group,
            worker_id,
            worker_run_id,
            1,
            "127.0.0.1:9999",
            1,
            1024,
            0,
            1024,
            0,
            0,
            HealthStatus::Healthy,
        )
        .unwrap();
    publish_report_locations(&worker_manager, &other_group, worker_id, 1, vec![block_id]);
    fs_core.set_storage(Arc::clone(&storage));
    fs_core.set_worker_manager(worker_manager);

    let mut attrs = FileAttrs::new();
    attrs.size = 512;
    let mut inode = Inode::new_file(inode_id, attrs, mount_id, data_handle_id);
    inode.data = types::fs::InodeData::File {
        extents: vec![types::fs::Extent {
            file_offset: 0,
            block_id,
            block_offset: 0,
            len: 512,
            file_version: Some(1),
            block_stamp: Some(41),
        }],
        file_version: Some(1),
        lease_epoch: None,
    };
    storage.put_inode(&inode).unwrap();
    storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
    storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

    let failure = fs_core
        .execute_get_file_layout(GetFileLayoutInput {
            ctx: request_context(),
            inode_id,
            range: None,
            requested_data_handle_id: None,
            freshness: Freshness::default(),
        })
        .await
        .expect_err("served group must not return empty locations from another group");

    assert_block_location_unavailable(&failure, block_id);
}

#[tokio::test]
async fn get_file_layout_rejects_returned_extent_without_block_stamp() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_id = MountId::new(49);
    let inode_id = InodeId::new(490);
    let data_handle_id = DataHandleId::new(9490);
    let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
    let mut fs_core = fs_core_with_mount(mount_id, 9, &group_name("g8"));
    fs_core.set_storage(Arc::clone(&storage));

    let mut attrs = FileAttrs::new();
    attrs.size = 512;
    let mut inode = Inode::new_file(inode_id, attrs, mount_id, data_handle_id);
    inode.data = types::fs::InodeData::File {
        extents: vec![types::fs::Extent {
            file_offset: 0,
            block_id,
            block_offset: 0,
            len: 512,
            file_version: Some(1),
            block_stamp: None,
        }],
        file_version: Some(1),
        lease_epoch: None,
    };
    storage.put_inode(&inode).unwrap();
    storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
    storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

    let failure = fs_core
        .execute_get_file_layout(GetFileLayoutInput {
            ctx: request_context(),
            inode_id,
            range: None,
            requested_data_handle_id: None,
            freshness: Freshness::default(),
        })
        .await
        .expect_err("missing block_stamp must reject returned layout");

    assert_eq!(
        failure.error.code,
        Some(CanonicalErrorCode::FsErrno(FsErrorCode::EInval))
    );
    assert!(failure.error.message.contains("block_stamp"));
}

#[tokio::test]
async fn get_file_layout_rejects_returned_extent_with_zero_block_stamp() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_id = MountId::new(50);
    let inode_id = InodeId::new(500);
    let data_handle_id = DataHandleId::new(9500);
    let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
    let mut fs_core = fs_core_with_mount(mount_id, 9, &group_name("g8"));
    fs_core.set_storage(Arc::clone(&storage));

    let mut attrs = FileAttrs::new();
    attrs.size = 512;
    let mut inode = Inode::new_file(inode_id, attrs, mount_id, data_handle_id);
    inode.data = types::fs::InodeData::File {
        extents: vec![types::fs::Extent {
            file_offset: 0,
            block_id,
            block_offset: 0,
            len: 512,
            file_version: Some(1),
            block_stamp: Some(0),
        }],
        file_version: Some(1),
        lease_epoch: Some(1),
    };
    storage.put_inode(&inode).unwrap();
    storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
    storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

    let failure = fs_core
        .execute_get_file_layout(GetFileLayoutInput {
            ctx: request_context(),
            inode_id,
            range: None,
            requested_data_handle_id: None,
            freshness: Freshness::default(),
        })
        .await
        .expect_err("zero block_stamp must reject returned layout");

    assert_eq!(
        failure.error.code,
        Some(CanonicalErrorCode::FsErrno(FsErrorCode::EInval))
    );
    assert!(failure.error.message.contains("zero block_stamp"));
}

#[tokio::test]
async fn get_status_rejects_stale_mount_epoch() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_id = MountId::new(70);
    let inode_id = InodeId::new(700);
    let mut fs_core = fs_core_with_mount(mount_id, 9, &group_name("g17"));
    fs_core.set_storage(Arc::clone(&storage));
    storage
        .put_inode(&Inode::new_file(
            inode_id,
            FileAttrs::new(),
            mount_id,
            DataHandleId::new(9700),
        ))
        .unwrap();

    let failure = fs_core
        .execute_get_attr(GetAttrInput {
            ctx: request_context(),
            inode_id,
            freshness: Freshness {
                mount_epoch: Some(8),
                route_epoch: None,
            },
        })
        .await
        .expect_err("stale mount_epoch must reject GetStatus");

    assert_eq!(
        failure.error.code,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::MountEpochMismatch))
    );
    assert_eq!(failure.error.reason, Some(RefreshReason::MountEpochMismatch));
    assert_eq!(failure.group_name, Some(group_name("g17")));
    assert_eq!(failure.mount_epoch, Some(9));
}

#[tokio::test]
async fn list_status_rejects_stale_mount_epoch() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_id = MountId::new(71);
    let parent_inode_id = InodeId::new(710);
    let mut fs_core = fs_core_with_mount(mount_id, 9, &group_name("g18"));
    fs_core.set_storage(Arc::clone(&storage));
    storage
        .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), mount_id))
        .unwrap();

    let failure = fs_core
        .execute_read_dir(ReadDirInput {
            ctx: request_context(),
            parent_inode_id,
            cursor_key: None,
            max_entries: None,
            freshness: Freshness {
                mount_epoch: Some(8),
                route_epoch: None,
            },
        })
        .await
        .expect_err("stale mount_epoch must reject ListStatus");

    assert_eq!(
        failure.error.code,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::MountEpochMismatch))
    );
    assert_eq!(failure.error.reason, Some(RefreshReason::MountEpochMismatch));
    assert_eq!(failure.group_name, Some(group_name("g18")));
    assert_eq!(failure.mount_epoch, Some(9));
}

#[tokio::test]
async fn open_file_rejects_stale_route_epoch() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_id = MountId::new(72);
    let inode_id = InodeId::new(720);
    let data_handle_id = DataHandleId::new(9720);
    let mut fs_core = fs_core_with_mount(mount_id, 9, &group_name("g19"));
    fs_core.set_storage(Arc::clone(&storage));
    storage
        .put_inode(&Inode::new_file(inode_id, FileAttrs::new(), mount_id, data_handle_id))
        .unwrap();
    storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
    storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

    let failure = fs_core
        .execute_get_file_layout(GetFileLayoutInput {
            ctx: request_context(),
            inode_id,
            range: None,
            requested_data_handle_id: None,
            freshness: Freshness {
                mount_epoch: None,
                route_epoch: Some(0),
            },
        })
        .await
        .expect_err("stale route_epoch must reject OpenFile");

    assert_eq!(
        failure.error.code,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::RouteEpochMismatch))
    );
    assert_eq!(failure.error.reason, Some(RefreshReason::RouteEpochMismatch));
    assert_eq!(failure.route_epoch, Some(1));
}

#[tokio::test]
async fn get_locations_rejects_stale_route_epoch() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_id = MountId::new(73);
    let inode_id = InodeId::new(730);
    let data_handle_id = DataHandleId::new(9730);
    let mut fs_core = fs_core_with_mount(mount_id, 9, &group_name("g20"));
    fs_core.set_storage(Arc::clone(&storage));
    storage
        .put_inode(&Inode::new_file(inode_id, FileAttrs::new(), mount_id, data_handle_id))
        .unwrap();
    storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
    storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

    let failure = fs_core
        .execute_get_file_layout(GetFileLayoutInput {
            ctx: request_context(),
            inode_id,
            range: None,
            requested_data_handle_id: Some(data_handle_id),
            freshness: Freshness {
                mount_epoch: None,
                route_epoch: Some(0),
            },
        })
        .await
        .expect_err("stale route_epoch must reject GetBlockLocations");

    assert_eq!(
        failure.error.code,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::RouteEpochMismatch))
    );
    assert_eq!(failure.error.reason, Some(RefreshReason::RouteEpochMismatch));
    assert_eq!(failure.route_epoch, Some(1));
}

#[tokio::test]
async fn read_success_returns_freshness_hints() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_id = MountId::new(74);
    let inode_id = InodeId::new(740);
    let mut fs_core = fs_core_with_mount(mount_id, 9, &group_name("g21"));
    fs_core.set_storage(Arc::clone(&storage));
    storage
        .put_inode(&Inode::new_file(
            inode_id,
            FileAttrs::new(),
            mount_id,
            DataHandleId::new(9740),
        ))
        .unwrap();

    let success = fs_core
        .execute_get_attr(GetAttrInput {
            ctx: request_context(),
            inode_id,
            freshness: Freshness::default(),
        })
        .await
        .expect("read should succeed");

    assert_eq!(success.group_name, Some(group_name("g21")));
    assert_eq!(success.mount_epoch, Some(9));
    assert_eq!(success.route_epoch, Some(1));
}

#[tokio::test]
async fn get_locations_rejects_range_overflow() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_id = MountId::new(75);
    let inode_id = InodeId::new(750);
    let data_handle_id = DataHandleId::new(9750);
    let mut fs_core = fs_core_with_mount(mount_id, 9, &group_name("g22"));
    fs_core.set_storage(Arc::clone(&storage));
    storage
        .put_inode(&Inode::new_file(inode_id, FileAttrs::new(), mount_id, data_handle_id))
        .unwrap();
    storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
    storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

    let failure = fs_core
        .execute_get_file_layout(GetFileLayoutInput {
            ctx: request_context(),
            inode_id,
            range: Some(crate::service::domain::FileRange {
                offset: u64::MAX,
                len: 1,
            }),
            requested_data_handle_id: None,
            freshness: Freshness::default(),
        })
        .await
        .expect_err("overflowing range must be rejected");

    assert_eq!(
        failure.error.code,
        Some(CanonicalErrorCode::FsErrno(FsErrorCode::EInval))
    );
    assert!(failure.error.message.contains("range end overflows"));
}

#[tokio::test]
async fn get_locations_handles_empty_range() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_id = MountId::new(76);
    let inode_id = InodeId::new(760);
    let data_handle_id = DataHandleId::new(9760);
    let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
    let mut attrs = FileAttrs::new();
    attrs.size = 512;
    let mut inode = Inode::new_file(inode_id, attrs, mount_id, data_handle_id);
    inode.data = types::fs::InodeData::File {
        extents: vec![types::fs::Extent {
            file_offset: 0,
            block_id,
            block_offset: 0,
            len: 512,
            file_version: Some(4),
            block_stamp: Some(4),
        }],
        file_version: Some(4),
        lease_epoch: Some(4),
    };
    let mut fs_core = fs_core_with_mount(mount_id, 9, &group_name("g23"));
    fs_core.set_storage(Arc::clone(&storage));
    storage.put_inode(&inode).unwrap();
    storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
    storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

    let success = fs_core
        .execute_get_file_layout(GetFileLayoutInput {
            ctx: request_context(),
            inode_id,
            range: Some(crate::service::domain::FileRange { offset: 0, len: 0 }),
            requested_data_handle_id: None,
            freshness: Freshness::default(),
        })
        .await
        .expect("empty range should be stable");

    assert!(success.payload.extents.is_empty());
    assert!(success.payload.locations.is_empty());
    assert_eq!(success.payload.file_size, 512);
    assert_eq!(success.payload.file_version, Some(4));
}

#[tokio::test]
async fn get_locations_filters_range() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_id = MountId::new(77);
    let inode_id = InodeId::new(770);
    let data_handle_id = DataHandleId::new(9770);
    let mut attrs = FileAttrs::new();
    attrs.size = 300;
    let mut inode = Inode::new_file(inode_id, attrs, mount_id, data_handle_id);
    inode.data = types::fs::InodeData::File {
        extents: (0_u32..3)
            .map(|idx| types::fs::Extent {
                file_offset: u64::from(idx) * 100,
                block_id: BlockId::new(data_handle_id, BlockIndex::new(idx)),
                block_offset: 0,
                len: 100,
                file_version: Some(5),
                block_stamp: Some(u64::from(idx) + 50),
            })
            .collect(),
        file_version: Some(5),
        lease_epoch: Some(5),
    };
    let mut fs_core = fs_core_with_mount(mount_id, 9, &group_name("g24"));
    fs_core.set_storage(Arc::clone(&storage));
    storage.put_inode(&inode).unwrap();
    storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
    storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

    let success = fs_core
        .execute_get_file_layout(GetFileLayoutInput {
            ctx: request_context(),
            inode_id,
            range: Some(crate::service::domain::FileRange { offset: 50, len: 150 }),
            requested_data_handle_id: None,
            freshness: Freshness::default(),
        })
        .await
        .expect("range filter should succeed");

    assert_eq!(
        success
            .payload
            .locations
            .iter()
            .map(|location| location.block_id.index.as_raw())
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert_eq!(
        success
            .payload
            .locations
            .iter()
            .map(|location| location.block_stamp)
            .collect::<Vec<_>>(),
        vec![50, 51]
    );
    assert_eq!(success.payload.file_version, Some(5));
}

fn install_write_session(fs_core: &FsCore, inode_id: InodeId, mount_id: MountId) -> u64 {
    let writer = ClientId::new(7);
    let data_handle_id = DataHandleId::new(424_242);
    let (lease_id, lease_epoch, _) = fs_core
        .inode_lease_manager_for_test()
        .try_acquire(
            inode_id,
            writer,
            Some(types::CallId::new()),
            crate::inode_lease::WriteMode::Write,
            None,
        )
        .expect("lease acquired");
    fs_core
        .write_session_manager_for_test()
        .create_session(crate::write_session::CreateSessionInput {
            inode_id,
            mount_id,
            data_handle_id,
            lease_id,
            lease_epoch,
            fencing_token: FencingToken {
                block_id: BlockId::new(data_handle_id, BlockIndex::new(0)),
                owner: writer,
                epoch: lease_epoch,
            },
            open_epoch: 1234,
            base_size: 0,
            mode: crate::inode_lease::WriteMode::Write,
            write_targets: vec![WriteTarget {
                block_id: BlockId::new(data_handle_id, BlockIndex::new(0)),
                file_offset: 0,
                block_size: 64,
                effective_len: 64,
                worker_endpoints: Vec::new(),
                fencing_token: FencingToken {
                    block_id: BlockId::new(data_handle_id, BlockIndex::new(0)),
                    owner: writer,
                    epoch: lease_epoch,
                },
                block_stamp: 1,
                chunk_size: 64,
                block_format_id: types::BlockFormatId::CURRENT_FOR_NEW_FILE,
                tier: types::Tier::Hdd,
            }],
            writer_identity: crate::write_session::WriterIdentity {
                client_id: writer,
                call_id: types::CallId::new(),
            },
        })
}

fn presented_session_token(session: &crate::write_session::WriteSession) -> PresentedFencingToken {
    PresentedFencingToken {
        block_id: Some(session.fencing_token.block_id),
        owner: session.fencing_token.owner,
        epoch: session.fencing_token.epoch,
    }
}

fn abort_input_for_session(
    session: &crate::write_session::WriteSession,
    file_handle: u64,
    ctx: RequestContext,
) -> AbortWriteInput {
    AbortWriteInput {
        ctx,
        file_handle,
        lease_id: Some(session.lease_id),
        lease_epoch: session.lease_epoch,
        open_epoch: session.open_epoch,
        fencing_token: Some(presented_session_token(session)),
        freshness: Freshness::default(),
    }
}

fn renew_input_for_session(
    session: &crate::write_session::WriteSession,
    file_handle: u64,
    ctx: RequestContext,
) -> RenewLeaseInput {
    RenewLeaseInput {
        ctx,
        file_handle,
        lease_id: Some(session.lease_id),
        lease_epoch: session.lease_epoch,
        open_epoch: session.open_epoch,
        fencing_token: Some(presented_session_token(session)),
        freshness: Freshness::default(),
    }
}

fn committed_block(block_id: BlockId, file_offset: u64, len: u64) -> CommittedBlock {
    CommittedBlock {
        block_id,
        file_offset,
        len,
        checksum: None,
    }
}

fn presented_key_token(key: &SessionKey) -> PresentedFencingToken {
    PresentedFencingToken {
        block_id: Some(key.fencing_token.block_id),
        owner: key.fencing_token.owner,
        epoch: key.fencing_token.epoch,
    }
}

async fn add_block_for_key(fs_core: &FsCore, key: &SessionKey, desired_len: u64) -> WriteTarget {
    fs_core
        .execute_add_block(AddBlockInput {
            ctx: request_context(),
            file_handle: key.file_handle,
            lease_id: Some(key.lease_id),
            lease_epoch: key.lease_epoch,
            open_epoch: key.open_epoch,
            fencing_token: Some(presented_key_token(key)),
            desired_len: Some(desired_len),
            freshness: Freshness::default(),
        })
        .await
        .expect("AddBlock should succeed")
        .payload
        .target
}

async fn commit_for_key(
    fs_core: &FsCore,
    key: &SessionKey,
    committed_blocks: Vec<CommittedBlock>,
    final_size: u64,
) -> CoreResult<crate::service::domain::CloseWriteOutput> {
    fs_core
        .execute_close_write(CloseWriteInput {
            ctx: request_context(),
            file_handle: key.file_handle,
            lease_id: Some(key.lease_id),
            lease_epoch: key.lease_epoch,
            open_epoch: key.open_epoch,
            fencing_token: Some(presented_key_token(key)),
            intent: CloseWriteIntent {
                committed_blocks,
                final_size,
            },
            freshness: Freshness::default(),
        })
        .await
}

async fn sync_for_key(
    fs_core: &FsCore,
    key: &SessionKey,
    committed_blocks: Vec<CommittedBlock>,
    target_size: u64,
    mode: SyncWriteMode,
) -> CoreResult<crate::service::domain::SyncWriteOutput> {
    fs_core
        .execute_sync_write(SyncWriteInput {
            ctx: request_context(),
            file_handle: key.file_handle,
            lease_id: Some(key.lease_id),
            lease_epoch: key.lease_epoch,
            open_epoch: key.open_epoch,
            fencing_token: Some(presented_key_token(key)),
            data_handle_id: key.fencing_token.block_id.data_handle_id,
            committed_blocks,
            target_size,
            flags: 0,
            mode,
            freshness: Freshness::default(),
        })
        .await
}

struct WriteFlowEnv {
    _dir: TempDir,
    storage: Arc<RocksDBStorage>,
    fs_core: FsCore,
    inode_id: InodeId,
    data_handle_id: DataHandleId,
    group_name: GroupName,
}

async fn write_flow_env(base_size: u64) -> WriteFlowEnv {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_id = MountId::new(57 + base_size);
    let group_name = group_name(&format!("g{}", 15 + base_size));
    let inode_id = InodeId::new(570 + base_size);
    let data_handle_id = DataHandleId::new(9570 + base_size);
    let mut fs_core = fs_core_with_mount(mount_id, 9, &group_name);
    let mount_table = Arc::clone(&fs_core.mount_table);
    let (raft_node, _state_machine) = single_node_raft(Arc::clone(&storage), mount_table).await;
    fs_core.set_storage(Arc::clone(&storage));
    fs_core.set_raft_node(raft_node);
    fs_core.set_worker_manager(worker_manager_for_write_targets(&group_name));

    let mut attrs = FileAttrs::new();
    attrs.size = base_size;
    storage
        .put_inode(&Inode::new_file(inode_id, attrs, mount_id, data_handle_id))
        .unwrap();
    storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
    storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

    WriteFlowEnv {
        _dir: dir,
        storage,
        fs_core,
        inode_id,
        data_handle_id,
        group_name,
    }
}

fn seed_committed_file_version(env: &WriteFlowEnv, file_version: u64, lease_epoch: u64) {
    let block_id = BlockId::new(env.data_handle_id, BlockIndex::new(0));
    let mut inode = env
        .storage
        .get_inode(env.inode_id)
        .unwrap()
        .expect("test inode should exist");
    inode.attrs.size = 64;
    match &mut inode.data {
        types::fs::InodeData::File {
            extents,
            file_version: stored_file_version,
            lease_epoch: stored_lease_epoch,
        } => {
            *extents = vec![types::fs::Extent {
                file_offset: 0,
                block_id,
                block_offset: 0,
                len: 64,
                file_version: Some(file_version),
                block_stamp: Some(file_version),
            }];
            *stored_file_version = Some(file_version);
            *stored_lease_epoch = Some(lease_epoch);
        }
        other => panic!("unexpected inode data: {:?}", other),
    }
    env.storage.put_inode(&inode).unwrap();
    env.storage.put_block_ref_count(block_id, 1).unwrap();
}

fn publish_env_block_location(env: &WriteFlowEnv, block_id: BlockId, block_stamp: u64, report_seq: u64) {
    let worker_manager = env.fs_core.worker_manager.as_ref().expect("worker manager");
    publish_report_locations_with_stamp(
        worker_manager,
        &env.group_name,
        WorkerId::new(1),
        report_seq,
        Some(block_stamp),
        vec![block_id],
    );
}

fn stored_file_version(storage: &RocksDBStorage, inode_id: InodeId) -> Option<u64> {
    let inode = storage.get_inode(inode_id).unwrap().expect("test inode should exist");
    match inode.data {
        types::fs::InodeData::File { file_version, .. } => file_version,
        other => panic!("unexpected inode data: {:?}", other),
    }
}

async fn single_node_raft(
    storage: Arc<RocksDBStorage>,
    mount_table: Arc<MountTable>,
) -> (Arc<AppRaftNode>, Arc<AppRaftStateMachine>) {
    let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage), mount_table));
    let raft_config = RaftConfig::default();
    let raft_node = Arc::new(
        AppRaftNode::new(1, storage, Arc::clone(&state_machine), &raft_config)
            .await
            .unwrap(),
    );
    raft_node
        .initialize_single_node("127.0.0.1:0".to_string())
        .await
        .unwrap();
    (raft_node, state_machine)
}

#[test]
fn freshness_validator_rejects_routed_write_mount_epoch_with_replay_hint() {
    let mount_id = MountId::new(12);
    let group_name_value = group_name("g4");
    let mount_table = Arc::new(MountTable::new());
    mount_table
        .upsert(MountEntry {
            mount_id,
            mount_prefix: "/data".to_string(),
            mount_kind: MountKind::Internal,
            ufs_uri: None,
            data_io_policy: DataIoPolicy::Allow,
            mount_epoch: 9,
            namespace_owner_group_name: group_name_value.clone(),
            root_inode_id: ROOT_INODE_ID,
        })
        .unwrap();
    let validator = FreshnessValidator::new(Arc::new(MemoryStateStore::new()), mount_table);
    let ctx = request_context();

    let failure = validator
        .validate_routed_write_mount_epoch(
            &ctx,
            Freshness {
                mount_epoch: Some(4),
                route_epoch: None,
            },
            mount_id,
        )
        .unwrap_err();

    assert_eq!(
        failure.error.code,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::MountEpochMismatch))
    );
    assert_eq!(failure.error.reason, Some(RefreshReason::MountEpochMismatch));
    assert_eq!(
        failure.error.message,
        "mount_epoch mismatch: client=4, server=9; refresh metadata and reopen write handle, then replay request"
    );
    let hint = failure.error.refresh_hint.expect("refresh hint");
    assert_eq!(hint.group_name, Some(group_name_value.to_string()));
    assert_eq!(hint.mount_epoch, Some(9));
    assert_eq!(failure.group_name, Some(group_name_value.clone()));
    assert_eq!(failure.mount_epoch, Some(9));
}

#[test]
fn routed_write_mount_epoch_mismatch_preserves_metrics_and_wire_shape() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_id = MountId::new(13);
    let parent_inode_id = InodeId::new(130);
    storage
        .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), mount_id))
        .unwrap();
    let mut fs_core = fs_core_with_mount(mount_id, 9, &group_name("g5"));
    fs_core.set_storage(storage);
    let metrics = Arc::new(crate::metrics::MetadataMetrics::new());
    fs_core.set_metrics(Arc::clone(&metrics));

    let failure = fs_core
        .route_ctx_for_write(
            &request_context(),
            CoreWriteOp::Create,
            &[parent_inode_id],
            Freshness {
                mount_epoch: Some(4),
                route_epoch: None,
            },
        )
        .unwrap_err();

    assert_eq!(
        failure.error.code,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::MountEpochMismatch))
    );
    assert_eq!(failure.error.reason, Some(RefreshReason::MountEpochMismatch));
    assert_eq!(
        failure.error.message,
        "mount_epoch mismatch: client=4, server=9; refresh metadata and reopen write handle, then replay request"
    );
    let hint = failure.error.refresh_hint.expect("refresh hint");
    assert_eq!(hint.group_name, Some("g5".to_string()));
    assert_eq!(hint.mount_epoch, Some(9));
    assert_eq!(failure.group_name, Some(group_name("g5")));
    assert_eq!(failure.mount_epoch, Some(9));
    assert_eq!(metrics.fs_write_mount_epoch_mismatch_total.load(Ordering::Relaxed), 1);
}

#[test]
fn freshness_validator_rejects_stale_state_watermark() {
    let group_name_value = group_name("g4");
    let validator = FreshnessValidator::new(Arc::new(MemoryStateStore::new()), Arc::new(MountTable::new()));
    let mut ctx = request_context();
    ctx.caller.state = vec![types::GroupStateWatermark::new(
        group_name_value.clone(),
        types::RaftLogId::new(1, 7, 12),
    )];

    let failure = validator
        .validate_stale_state(
            &ctx,
            Some(types::RaftLogId::new(1, 7, 10)),
            Some(group_name_value.clone()),
            Some(9),
        )
        .unwrap_err();

    assert_eq!(
        failure.error.code,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::StaleState))
    );
    assert_eq!(failure.error.reason, Some(RefreshReason::StaleState));
    assert_eq!(
        failure.error.message,
        "Stale state: last_applied=RaftLogId { term: 1, leader_node_id: 7, index: 10 } < required=RaftLogId { term: 1, leader_node_id: 7, index: 12 }"
    );
    assert_eq!(failure.group_name, Some(group_name_value.clone()));
    assert_eq!(failure.mount_epoch, Some(9));
    assert!(failure.state.is_empty());

    let unknown = validator
        .validate_stale_state(&ctx, None, Some(group_name_value.clone()), Some(9))
        .expect("missing last_applied should preserve existing precheck fallback");
    assert_eq!(unknown, StaleStateStatus::UnknownLastApplied);
}

#[tokio::test]
async fn abort_releases_lease() {
    let mount_id = MountId::new(41);
    let group_name_value = group_name("g4");
    let inode_id = InodeId::new(410);
    let fs_core = fs_core_with_mount(mount_id, 9, &group_name_value);
    let file_handle = install_write_session(&fs_core, inode_id, mount_id);
    let session = fs_core
        .write_session_for_handle(file_handle)
        .expect("session should be installed");

    let success = fs_core
        .execute_abort_write(abort_input_for_session(&session, file_handle, request_context()))
        .await
        .expect("abort succeeds");

    assert!(fs_core.write_session_for_handle(file_handle).is_none());
    assert!(fs_core
        .inode_lease_manager_for_test()
        .get_active_lease(inode_id)
        .is_none());
    assert_eq!(success.mount_epoch, Some(9));
    assert_eq!(success.group_name, Some(group_name_value));
}

#[tokio::test]
async fn abort_checks_handle() {
    let mount_id = MountId::new(43);
    let inode_id = InodeId::new(430);
    let fs_core = fs_core_with_mount(mount_id, 9, &group_name("g6"));

    let failure = fs_core
        .execute_abort_write(AbortWriteInput {
            ctx: request_context(),
            file_handle: 999,
            lease_id: Some(LeaseId::new(1)),
            lease_epoch: 1,
            open_epoch: 1,
            fencing_token: Some(PresentedFencingToken {
                block_id: Some(BlockId::new(DataHandleId::new(1), BlockIndex::new(0))),
                owner: ClientId::new(7),
                epoch: 1,
            }),
            freshness: Freshness::default(),
        })
        .await
        .expect_err("missing write handle must be rejected");

    assert_eq!(
        failure.error.code,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Fencing))
    );
    assert_eq!(failure.error.class, ErrorClass::NeedRefresh);
    assert_eq!(failure.error.reason, Some(RefreshReason::SessionInvalid));
    let roundtrip = crate::service::core_util::header_from_core_failure(&request_context(), &failure);
    let roundtrip_error = proto::convert::error_detail_to_canonical(
        roundtrip.error.as_ref().expect("session failure must carry wire error"),
    );
    assert_eq!(roundtrip_error.class, ErrorClass::NeedRefresh);
    assert_eq!(roundtrip_error.reason, Some(RefreshReason::SessionInvalid));

    let file_handle = install_write_session(&fs_core, inode_id, mount_id);
    let session = fs_core
        .write_session_for_handle(file_handle)
        .expect("session should be installed");
    let mut stale = abort_input_for_session(&session, file_handle, request_context());
    stale.lease_epoch += 1;

    let stale_failure = fs_core
        .execute_abort_write(stale)
        .await
        .expect_err("stale abort handle must be rejected");

    assert_eq!(
        stale_failure.error.code,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Fencing))
    );
    assert_eq!(stale_failure.error.class, ErrorClass::NeedRefresh);
    assert_eq!(stale_failure.error.reason, Some(RefreshReason::SessionInvalid));
    assert!(fs_core.write_session_for_handle(file_handle).is_some());
    assert!(fs_core
        .inode_lease_manager_for_test()
        .get_active_lease(inode_id)
        .is_some());
}

#[tokio::test]
async fn renew_lease_checks_open_epoch() {
    let mount_id = MountId::new(44);
    let inode_id = InodeId::new(440);
    let fs_core = fs_core_with_mount(mount_id, 9, &group_name("g6"));
    let file_handle = install_write_session(&fs_core, inode_id, mount_id);
    let session = fs_core
        .write_session_for_handle(file_handle)
        .expect("session should be installed");

    fs_core
        .execute_renew_inode_lease(renew_input_for_session(&session, file_handle, request_context()))
        .await
        .expect("valid full write handle should renew lease");

    let mut stale_open = renew_input_for_session(&session, file_handle, request_context());
    stale_open.open_epoch += 1;
    let failure = fs_core
        .execute_renew_inode_lease(stale_open)
        .await
        .expect_err("open_epoch mismatch must be rejected");

    assert_eq!(
        failure.error.code,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::EpochMismatch))
    );
    assert_eq!(failure.error.reason, Some(RefreshReason::SessionInvalid));
}

#[tokio::test]
async fn renew_lease_checks_fencing() {
    let mount_id = MountId::new(45);
    let inode_id = InodeId::new(450);
    let fs_core = fs_core_with_mount(mount_id, 9, &group_name("g6"));
    let file_handle = install_write_session(&fs_core, inode_id, mount_id);
    let session = fs_core
        .write_session_for_handle(file_handle)
        .expect("session should be installed");

    let mut stale_fencing = renew_input_for_session(&session, file_handle, request_context());
    stale_fencing.fencing_token = Some(PresentedFencingToken {
        block_id: Some(BlockId::new(DataHandleId::new(999_999), BlockIndex::new(0))),
        owner: session.fencing_token.owner,
        epoch: session.fencing_token.epoch,
    });
    let failure = fs_core
        .execute_renew_inode_lease(stale_fencing)
        .await
        .expect_err("fencing token mismatch must be rejected");

    assert_eq!(
        failure.error.code,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Fencing))
    );
    assert_eq!(failure.error.reason, Some(RefreshReason::SessionInvalid));

    let mut missing_fencing = renew_input_for_session(&session, file_handle, request_context());
    missing_fencing.fencing_token = None;
    let missing = fs_core
        .execute_renew_inode_lease(missing_fencing)
        .await
        .expect_err("missing fencing token must be rejected");

    assert_eq!(
        missing.error.code,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Fencing))
    );
    assert_eq!(missing.error.reason, Some(RefreshReason::SessionInvalid));
}

#[tokio::test]
async fn renew_lease_rejects_lease_epoch_mismatch() {
    let mount_id = MountId::new(46);
    let inode_id = InodeId::new(460);
    let fs_core = fs_core_with_mount(mount_id, 9, &group_name("g6"));
    let file_handle = install_write_session(&fs_core, inode_id, mount_id);
    let session = fs_core
        .write_session_for_handle(file_handle)
        .expect("session should be installed");

    let mut stale = renew_input_for_session(&session, file_handle, request_context());
    stale.lease_epoch += 1;
    let failure = fs_core
        .execute_renew_inode_lease(stale)
        .await
        .expect_err("lease_epoch mismatch must be rejected");

    assert_eq!(
        failure.error.code,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Fencing))
    );
    assert_eq!(failure.error.reason, Some(RefreshReason::SessionInvalid));
}

#[tokio::test]
async fn renew_lease_rejects_missing_or_stale_handle() {
    let fs_core = fs_core_with_mount(MountId::new(47), 9, &group_name("g6"));

    let failure = fs_core
        .execute_renew_inode_lease(RenewLeaseInput {
            ctx: request_context(),
            file_handle: 404,
            lease_id: Some(LeaseId::new(1)),
            lease_epoch: 1,
            open_epoch: 1,
            fencing_token: Some(PresentedFencingToken {
                block_id: Some(BlockId::new(DataHandleId::new(1), BlockIndex::new(0))),
                owner: ClientId::new(7),
                epoch: 1,
            }),
            freshness: Freshness::default(),
        })
        .await
        .expect_err("missing write handle must be rejected");

    assert_eq!(
        failure.error.code,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Fencing))
    );
    assert_eq!(failure.error.reason, Some(RefreshReason::SessionInvalid));
}

#[tokio::test]
async fn open_write_cleans_lease_on_error() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_id = MountId::new(50);
    let inode_id = InodeId::new(500);
    let data_handle_id = DataHandleId::new(9500);
    storage
        .put_inode(&Inode::new_file(inode_id, FileAttrs::new(), mount_id, data_handle_id))
        .unwrap();
    storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
    storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

    let mut fs_core = fs_core_with_mount(mount_id, 9, &group_name("g7"));
    fs_core.set_storage(storage);

    let failure = fs_core
        .execute_open_write(OpenWriteInput {
            ctx: request_context(),
            inode_id,
            desired_len: Some(4096),
            mode: crate::inode_lease::WriteMode::Write,
            freshness: Freshness::default(),
        })
        .await
        .expect_err("missing worker manager should fail open_write");

    assert!(failure.error.message.contains("Worker manager not available"));
    assert!(fs_core
        .inode_lease_manager_for_test()
        .get_active_lease(inode_id)
        .is_none());
}

#[tokio::test]
async fn open_write_targets_use_inode_current_data_handle() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_id = MountId::new(51);
    let group_name_value = group_name("g9");
    let inode_id = InodeId::new(510);
    let data_handle_id = DataHandleId::new(9510);
    storage
        .put_inode(&Inode::new_file(inode_id, FileAttrs::new(), mount_id, data_handle_id))
        .unwrap();
    storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
    storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

    let mut fs_core = fs_core_with_mount(mount_id, 9, &group_name_value);
    fs_core.set_storage(storage);
    fs_core.set_worker_manager(worker_manager_for_write_targets(&group_name_value));

    let success = fs_core
        .execute_open_write(OpenWriteInput {
            ctx: request_context(),
            inode_id,
            desired_len: Some(4096),
            mode: crate::inode_lease::WriteMode::Write,
            freshness: Freshness::default(),
        })
        .await
        .expect("open_write should succeed");

    assert_ne!(inode_id.as_raw(), data_handle_id.as_raw());
    assert!(!success.payload.write_targets.is_empty());
    for target in &success.payload.write_targets {
        assert_eq!(target.block_id.data_handle_id, data_handle_id);
        assert_eq!(target.block_size, 4096);
        assert_eq!(target.effective_len, 4096);
        assert_eq!(target.chunk_size, 4096);
        assert_eq!(target.block_format_id, types::BlockFormatId::CURRENT_FOR_NEW_FILE);
    }
    assert_eq!(
        success.payload.session_key.fencing_token.block_id.data_handle_id,
        data_handle_id
    );
    let session = fs_core
        .write_session_for_handle(success.payload.session_key.file_handle)
        .expect("session should be stored");
    assert_eq!(session.data_handle_id, data_handle_id);
}

#[tokio::test]
async fn open_write_rejects_missing_file_layout_without_default_fallback() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_id = MountId::new(52);
    let group_name_value = group_name("g9");
    let inode_id = InodeId::new(520);
    let data_handle_id = DataHandleId::new(9520);
    storage
        .put_inode(&Inode::new_file(inode_id, FileAttrs::new(), mount_id, data_handle_id))
        .unwrap();
    storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

    let mut fs_core = fs_core_with_mount(mount_id, 9, &group_name_value);
    fs_core.set_storage(storage);
    fs_core.set_worker_manager(worker_manager_for_write_targets(&group_name_value));

    let failure = fs_core
        .execute_open_write(OpenWriteInput {
            ctx: request_context(),
            inode_id,
            desired_len: Some(4096),
            mode: crate::inode_lease::WriteMode::Write,
            freshness: Freshness::default(),
        })
        .await
        .expect_err("missing persisted layout must fail open_write");

    assert!(failure.error.message.contains("Layout not found"));
}

#[tokio::test]
async fn create_file_persists_valid_client_layout_shape() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_id = MountId::new(59);
    let group_name_value = group_name("g9");
    let parent_inode_id = InodeId::new(590);
    storage
        .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), mount_id))
        .unwrap();
    let mut fs_core = fs_core_with_mount(mount_id, 9, &group_name_value);
    let mount_table = Arc::clone(&fs_core.mount_table);
    let (raft_node, _state_machine) = single_node_raft(Arc::clone(&storage), mount_table).await;
    fs_core.set_storage(Arc::clone(&storage));
    fs_core.set_raft_node(raft_node);
    fs_core.set_worker_manager(worker_manager_for_write_targets(&group_name_value));
    let layout = FileLayout::with_block_format(8192, 1024, 1, types::BlockFormatId::FULL_EFFECTIVE);

    let success = fs_core
        .execute_create(CreateInput {
            ctx: request_context(),
            parent_inode_id,
            name: "file".to_string(),
            attrs: FileAttrs::new(),
            layout,
            freshness: Freshness::default(),
        })
        .await
        .expect("valid create layout should succeed");
    let inode_id = success.payload.inode_id.expect("created inode id");

    assert_eq!(storage.get_layout(inode_id).unwrap(), layout);
}

#[tokio::test]
async fn create_file_rejects_invalid_block_chunk_shape() {
    let fs_core = fs_core_without_mount();

    let failure = fs_core
        .execute_create(CreateInput {
            ctx: request_context(),
            parent_inode_id: InodeId::new(1),
            name: "file".to_string(),
            attrs: FileAttrs::new(),
            layout: FileLayout::new(4097, 1024, 1),
            freshness: Freshness::default(),
        })
        .await
        .expect_err("invalid create layout must fail before storage mutation");

    assert!(failure.error.message.contains("multiple of chunk_size"));
}

#[tokio::test]
async fn open_write_rejects_multi_replica_layout_until_durable_replication_exists() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_id = MountId::new(54);
    let group_name_value = group_name("g9");
    let inode_id = InodeId::new(540);
    let data_handle_id = DataHandleId::new(9540);
    storage
        .put_inode(&Inode::new_file(inode_id, FileAttrs::new(), mount_id, data_handle_id))
        .unwrap();
    storage.put_layout(inode_id, FileLayout::new(4096, 4096, 2)).unwrap();
    storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

    let mut fs_core = fs_core_with_mount(mount_id, 9, &group_name_value);
    fs_core.set_storage(storage);
    fs_core.set_worker_manager(worker_manager_for_write_targets(&group_name_value));

    let failure = fs_core
        .execute_open_write(OpenWriteInput {
            ctx: request_context(),
            inode_id,
            desired_len: Some(4096),
            mode: crate::inode_lease::WriteMode::Write,
            freshness: Freshness::default(),
        })
        .await
        .expect_err("multi-replica layout must fail active write");

    assert!(
        failure
            .error
            .message
            .contains("multi-replica write is not supported yet; replication must be 1"),
        "unexpected error: {}",
        failure.error.message
    );
}

#[tokio::test]
async fn open_write_rejects_layout_shape_worker_would_reject() {
    for (layout, expected) in [
        (FileLayout::new(4097, 1024, 1), "multiple of chunk_size"),
        (FileLayout::new(1024, 4096, 1), "chunk_size must not exceed block_size"),
    ] {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(55);
        let group_name_value = group_name("g9");
        let inode_id = InodeId::new(550);
        let data_handle_id = DataHandleId::new(9550);
        storage
            .put_inode(&Inode::new_file(inode_id, FileAttrs::new(), mount_id, data_handle_id))
            .unwrap();
        storage.put_layout(inode_id, layout).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        let mut fs_core = fs_core_with_mount(mount_id, 9, &group_name_value);
        fs_core.set_storage(storage);
        fs_core.set_worker_manager(worker_manager_for_write_targets(&group_name_value));

        let failure = fs_core
            .execute_open_write(OpenWriteInput {
                ctx: request_context(),
                inode_id,
                desired_len: Some(4096),
                mode: crate::inode_lease::WriteMode::Write,
                freshness: Freshness::default(),
            })
            .await
            .expect_err("invalid layout shape must fail active write");

        assert!(
            failure.error.message.contains(expected),
            "expected {expected:?} in {}",
            failure.error.message
        );
    }
}

#[test]
fn open_write_preflight_rejects_placement_without_authoritative_group() {
    let mut fs_core = fs_core_without_mount();
    fs_core.set_worker_manager(worker_manager_for_write_targets(&group_name("root")));

    let failure = fs_core
        .preflight_open_write_runtime(
            &request_context(),
            Some(4096),
            FileLayout::new(4096, 4096, 1),
            None,
            None,
        )
        .expect("missing authoritative group must reject placement preflight");

    assert!(
        failure
            .error
            .message
            .contains("OpenWrite preflight worker lookup requires authoritative metadata group"),
        "unexpected error: {}",
        failure.error.message
    );
}

#[tokio::test]
async fn commit_worker_run_check_rejects_missing_authoritative_group() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_id = MountId::new(53);
    let inode_id = InodeId::new(530);
    let data_handle_id = DataHandleId::new(9530);
    let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
    let worker_id = WorkerId::new(1);
    let mut fs_core = fs_core_without_mount();
    fs_core.set_storage(Arc::clone(&storage));
    fs_core.set_worker_manager(worker_manager_for_write_targets(&group_name("root")));

    storage
        .put_inode(&Inode::new_file(inode_id, FileAttrs::new(), mount_id, data_handle_id))
        .unwrap();
    let writer = ClientId::new(7);
    let (lease_id, lease_epoch, _) = fs_core
        .inode_lease_manager_for_test()
        .try_acquire(
            inode_id,
            writer,
            Some(types::CallId::new()),
            crate::inode_lease::WriteMode::Write,
            None,
        )
        .expect("lease acquired");
    let file_handle =
        fs_core
            .write_session_manager_for_test()
            .create_session(crate::write_session::CreateSessionInput {
                inode_id,
                mount_id,
                data_handle_id,
                lease_id,
                lease_epoch,
                fencing_token: FencingToken {
                    block_id,
                    owner: writer,
                    epoch: lease_epoch,
                },
                open_epoch: 777,
                base_size: 0,
                mode: crate::inode_lease::WriteMode::Write,
                write_targets: vec![WriteTarget {
                    block_id,
                    file_offset: 0,
                    block_size: 64,
                    effective_len: 64,
                    worker_endpoints: vec![WorkerEndpointInfo {
                        worker_id,
                        endpoint: "127.0.0.1:9001".to_string(),
                        worker_net_protocol: WorkerNetProtocol::Grpc,
                        worker_run_id: worker_run_id(&group_name("root"), worker_id),
                    }],
                    fencing_token: FencingToken {
                        block_id,
                        owner: writer,
                        epoch: lease_epoch,
                    },
                    block_stamp: 1,
                    chunk_size: 64,
                    block_format_id: types::BlockFormatId::CURRENT_FOR_NEW_FILE,
                    tier: types::Tier::Hdd,
                }],
                writer_identity: crate::write_session::WriterIdentity {
                    client_id: writer,
                    call_id: types::CallId::new(),
                },
            });
    let session = fs_core
        .write_session_for_handle(file_handle)
        .expect("session should be installed");
    fs_core
        .execute_add_block(AddBlockInput {
            ctx: request_context(),
            file_handle,
            lease_id: Some(session.lease_id),
            lease_epoch: session.lease_epoch,
            open_epoch: session.open_epoch,
            fencing_token: Some(presented_session_token(&session)),
            desired_len: Some(64),
            freshness: Freshness::default(),
        })
        .await
        .expect("preallocated target should be issued");
    let session = fs_core
        .write_session_for_handle(file_handle)
        .expect("session should remain installed");

    let failure = fs_core
        .execute_close_write(CloseWriteInput {
            ctx: request_context(),
            file_handle,
            lease_id: Some(session.lease_id),
            lease_epoch: session.lease_epoch,
            open_epoch: session.open_epoch,
            fencing_token: Some(presented_session_token(&session)),
            intent: CloseWriteIntent {
                committed_blocks: vec![committed_block(block_id, 0, 64)],
                final_size: 64,
            },
            freshness: Freshness::default(),
        })
        .await
        .expect_err("missing authoritative group must reject commit worker lookup");

    assert!(
        failure
            .error
            .message
            .contains("CommitFile worker lookup requires authoritative metadata group"),
        "unexpected error: {}",
        failure.error.message
    );
}

#[tokio::test]
async fn open_write_rejects_file_missing_current_data_handle() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_id = MountId::new(52);
    let inode_id = InodeId::new(520);
    storage
        .put_inode(&Inode::new_file(
            inode_id,
            FileAttrs::new(),
            mount_id,
            DataHandleId::new(0),
        ))
        .unwrap();

    let mut fs_core = fs_core_with_mount(mount_id, 9, &group_name("g10"));
    fs_core.set_storage(storage);
    fs_core.set_worker_manager(worker_manager_for_write_targets(&group_name("g10")));

    let failure = fs_core
        .execute_open_write(OpenWriteInput {
            ctx: request_context(),
            inode_id,
            desired_len: Some(4096),
            mode: crate::inode_lease::WriteMode::Write,
            freshness: Freshness::default(),
        })
        .await
        .unwrap_err();

    assert!(failure.error.message.contains("missing current_data_handle_id"));
}

#[tokio::test]
async fn commit_advances_file_version() {
    let env = write_flow_env(64).await;
    seed_committed_file_version(&env, 41, 900);

    let open = env
        .fs_core
        .execute_open_write(OpenWriteInput {
            ctx: request_context(),
            inode_id: env.inode_id,
            desired_len: Some(64),
            mode: crate::inode_lease::WriteMode::Write,
            freshness: Freshness::default(),
        })
        .await
        .expect("replace open should succeed");
    let key = open.payload.session_key;
    assert!(key.lease_epoch > 41);
    let target = add_block_for_key(&env.fs_core, &key, 64).await;

    let close = commit_for_key(
        &env.fs_core,
        &key,
        vec![committed_block(
            target.block_id,
            target.file_offset,
            target.effective_len,
        )],
        64,
    )
    .await
    .expect("replace commit should succeed");

    assert_eq!(close.payload.file_version, Some(42));
    assert_eq!(stored_file_version(&env.storage, env.inode_id), Some(42));
}

#[tokio::test]
async fn create_then_add_block() {
    let env = write_flow_env(0).await;
    let open = env
        .fs_core
        .execute_open_write(OpenWriteInput {
            ctx: request_context(),
            inode_id: env.inode_id,
            desired_len: Some(512),
            mode: crate::inode_lease::WriteMode::Write,
            freshness: Freshness::default(),
        })
        .await
        .expect("open write should succeed");
    let key = open.payload.session_key;
    let session_before = env
        .fs_core
        .write_session_for_handle(key.file_handle)
        .expect("session should be stored");
    assert_eq!(session_before.next_target_index, 0);
    assert!(session_before.issued_targets.is_empty());

    let target = add_block_for_key(&env.fs_core, &key, 512).await;
    assert_eq!(target.block_id.data_handle_id, env.data_handle_id);
    assert_eq!(target.file_offset, 0);
    assert_eq!(target.block_size, 4096);
    assert_eq!(target.effective_len, 512);
    assert!(target.effective_len < target.block_size);
    assert_eq!(target.chunk_size, 4096);
    assert_eq!(target.block_format_id, types::BlockFormatId::CURRENT_FOR_NEW_FILE);
    assert!(target.worker_endpoints[0].endpoint.starts_with("127.0.0.1:900"));
    assert!(!target.worker_endpoints[0].endpoint.ends_with(":0"));
    assert_eq!(
        target.worker_endpoints[0].worker_run_id,
        worker_run_id(&env.group_name, target.worker_endpoints[0].worker_id)
    );
    let session_after_add = env
        .fs_core
        .write_session_for_handle(key.file_handle)
        .expect("session should remain open");
    assert_eq!(session_after_add.next_target_index, 1);
    assert_eq!(session_after_add.issued_targets.len(), 1);

    let committed = committed_block(target.block_id, target.file_offset, target.effective_len);
    let success = commit_for_key(&env.fs_core, &key, vec![committed], 512)
        .await
        .expect("commit should succeed");
    assert_eq!(success.payload.committed_size, 512);
    assert_eq!(success.payload.file_version, Some(1));
    assert!(env.fs_core.write_session_for_handle(key.file_handle).is_none());
    assert_eq!(env.storage.get_inode(env.inode_id).unwrap().unwrap().attrs.size, 512);
}

#[tokio::test]
async fn open_write_target_uses_stored_file_layout_shape() {
    let env = write_flow_env(0).await;
    let layout = FileLayout::with_block_format(8192, 1024, 1, types::BlockFormatId::FULL_EFFECTIVE);
    env.storage.put_layout(env.inode_id, layout).unwrap();
    let open = env
        .fs_core
        .execute_open_write(OpenWriteInput {
            ctx: request_context(),
            inode_id: env.inode_id,
            desired_len: Some(2048),
            mode: crate::inode_lease::WriteMode::Write,
            freshness: Freshness::default(),
        })
        .await
        .expect("open write should use stored layout");
    let key = open.payload.session_key;

    let target = add_block_for_key(&env.fs_core, &key, 2048).await;

    assert_eq!(target.block_format_id, layout.block_format_id);
    assert_eq!(target.block_size, u64::from(layout.block_size));
    assert_eq!(target.chunk_size, layout.chunk_size);
    assert_eq!(target.effective_len, 2048);
}

#[tokio::test]
async fn open_write_target_uses_metadata_selected_storage_tier() {
    let mut env = write_flow_env(0).await;
    env.fs_core
        .set_worker_manager(worker_manager_for_tier(&env.group_name, Tier::Ssd, 4096));
    let open = env
        .fs_core
        .execute_open_write(OpenWriteInput {
            ctx: request_context(),
            inode_id: env.inode_id,
            desired_len: Some(2048),
            mode: crate::inode_lease::WriteMode::Write,
            freshness: Freshness::default(),
        })
        .await
        .expect("open write should select SSD worker");
    let key = open.payload.session_key;

    let target = add_block_for_key(&env.fs_core, &key, 2048).await;

    assert_eq!(target.tier, Tier::Ssd);
}

#[tokio::test]
async fn commit_worker_run_check_uses_session_group() {
    let env = write_flow_env(0).await;
    let open = env
        .fs_core
        .execute_open_write(OpenWriteInput {
            ctx: request_context(),
            inode_id: env.inode_id,
            desired_len: Some(64),
            mode: crate::inode_lease::WriteMode::Write,
            freshness: Freshness::default(),
        })
        .await
        .expect("open write should succeed");
    let key = open.payload.session_key;
    let target = add_block_for_key(&env.fs_core, &key, 64).await;
    let endpoint = target.worker_endpoints.first().expect("worker endpoint").clone();
    let worker_manager = env.fs_core.worker_manager.as_ref().expect("worker manager");
    let other_group = group_name("other");

    worker_manager
        .load_registered_workers(vec![WorkerInfo {
            group_name: other_group,
            worker_id: endpoint.worker_id,
            address: "127.0.0.1:9999".to_string(),
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
        .expect("replace manager descriptors with another group");

    let failure = commit_for_key(
        &env.fs_core,
        &key,
        vec![committed_block(
            target.block_id,
            target.file_offset,
            target.effective_len,
        )],
        64,
    )
    .await
    .expect_err("commit must not validate worker_run_id against another group's registration");

    assert_eq!(
        failure.error.code,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::WorkerRunMismatch))
    );
    assert_eq!(failure.error.reason, Some(RefreshReason::WorkerRunMismatch));
    assert!(failure.error.message.contains("worker_run_id mismatch"));
}

#[tokio::test]
async fn commit_worker_run_check_rejects_stale_live_registration() {
    let env = write_flow_env(0).await;
    let open = env
        .fs_core
        .execute_open_write(OpenWriteInput {
            ctx: request_context(),
            inode_id: env.inode_id,
            desired_len: Some(64),
            mode: crate::inode_lease::WriteMode::Write,
            freshness: Freshness::default(),
        })
        .await
        .expect("open write should succeed");
    let key = open.payload.session_key;
    let target = add_block_for_key(&env.fs_core, &key, 64).await;
    let endpoint = target.worker_endpoints.first().expect("worker endpoint").clone();
    let worker_manager = env.fs_core.worker_manager.as_ref().expect("worker manager");
    let replacement_run_id: WorkerRunId = "550e8400-e29b-41d4-a716-44665544f00d"
        .parse()
        .expect("valid replacement WorkerRunId");

    worker_manager
        .load_registered_workers(vec![WorkerInfo {
            group_name: env.group_name.clone(),
            worker_id: endpoint.worker_id,
            address: endpoint.endpoint.clone(),
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
        .expect("replace manager descriptors after metadata reload");
    worker_manager
        .register_worker_run(
            &env.group_name,
            endpoint.worker_id,
            endpoint.endpoint.clone(),
            1,
            replacement_run_id,
            None,
        )
        .expect("replacement worker run should register after reload");

    let failure = commit_for_key(
        &env.fs_core,
        &key,
        vec![committed_block(
            target.block_id,
            target.file_offset,
            target.effective_len,
        )],
        64,
    )
    .await
    .expect_err("commit must reject stale worker process-run identity");

    assert_eq!(
        failure.error.code,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::WorkerRunMismatch))
    );
    assert_eq!(failure.error.reason, Some(RefreshReason::WorkerRunMismatch));
    assert!(failure.error.message.contains("worker_run_id mismatch"));
}

#[tokio::test]
async fn create_new_commit_returns_initial_file_version() {
    let env = write_flow_env(0).await;
    let open = env
        .fs_core
        .execute_open_write(OpenWriteInput {
            ctx: request_context(),
            inode_id: env.inode_id,
            desired_len: Some(64),
            mode: crate::inode_lease::WriteMode::Write,
            freshness: Freshness::default(),
        })
        .await
        .expect("open write should succeed");
    let key = open.payload.session_key;
    let target = add_block_for_key(&env.fs_core, &key, 64).await;

    let close = commit_for_key(
        &env.fs_core,
        &key,
        vec![committed_block(
            target.block_id,
            target.file_offset,
            target.effective_len,
        )],
        64,
    )
    .await
    .expect("commit should succeed");

    assert_eq!(close.payload.file_version, Some(1));
}

#[tokio::test]
async fn open_returns_file_version() {
    let env = write_flow_env(64).await;
    seed_committed_file_version(&env, 41, 900);
    publish_env_block_location(&env, BlockId::new(env.data_handle_id, BlockIndex::new(0)), 41, 1);

    let read = env
        .fs_core
        .execute_get_file_layout(GetFileLayoutInput {
            ctx: request_context(),
            inode_id: env.inode_id,
            range: None,
            requested_data_handle_id: None,
            freshness: Freshness::default(),
        })
        .await
        .expect("open/read layout should succeed");

    assert_eq!(read.payload.file_version, Some(41));
}

#[tokio::test]
async fn append_advances_file_version() {
    let env = write_flow_env(0).await;
    let first_open = env
        .fs_core
        .execute_open_write(OpenWriteInput {
            ctx: request_context(),
            inode_id: env.inode_id,
            desired_len: Some(64),
            mode: crate::inode_lease::WriteMode::Write,
            freshness: Freshness::default(),
        })
        .await
        .expect("first open should succeed");
    let first_key = first_open.payload.session_key;
    let first_target = add_block_for_key(&env.fs_core, &first_key, 64).await;
    let first_close = commit_for_key(
        &env.fs_core,
        &first_key,
        vec![committed_block(
            first_target.block_id,
            first_target.file_offset,
            first_target.effective_len,
        )],
        64,
    )
    .await
    .expect("first commit should succeed");

    let second_open = env
        .fs_core
        .execute_open_write(OpenWriteInput {
            ctx: request_context(),
            inode_id: env.inode_id,
            desired_len: Some(64),
            mode: crate::inode_lease::WriteMode::Append,
            freshness: Freshness::default(),
        })
        .await
        .expect("append open should succeed");
    let second_key = second_open.payload.session_key;
    let second_target = add_block_for_key(&env.fs_core, &second_key, 64).await;
    let second_close = commit_for_key(
        &env.fs_core,
        &second_key,
        vec![committed_block(
            second_target.block_id,
            second_target.file_offset,
            second_target.effective_len,
        )],
        128,
    )
    .await
    .expect("second commit should succeed");

    assert_eq!(first_close.payload.file_version, Some(1));
    assert_eq!(second_close.payload.file_version, Some(2));
    assert_eq!(stored_file_version(&env.storage, env.inode_id), Some(2));
}

#[tokio::test]
async fn locations_return_file_version() {
    let env = write_flow_env(64).await;
    seed_committed_file_version(&env, 41, 900);
    publish_env_block_location(&env, BlockId::new(env.data_handle_id, BlockIndex::new(0)), 41, 1);

    let locations = env
        .fs_core
        .execute_get_file_layout(GetFileLayoutInput {
            ctx: request_context(),
            inode_id: env.inode_id,
            range: None,
            requested_data_handle_id: Some(env.data_handle_id),
            freshness: Freshness::default(),
        })
        .await
        .expect("locations should succeed");

    assert_eq!(locations.payload.file_version, Some(41));
}

#[tokio::test]
async fn worker_report_does_not_change_file_version() {
    let env = write_flow_env(0).await;
    let open = env
        .fs_core
        .execute_open_write(OpenWriteInput {
            ctx: request_context(),
            inode_id: env.inode_id,
            desired_len: Some(64),
            mode: crate::inode_lease::WriteMode::Write,
            freshness: Freshness::default(),
        })
        .await
        .expect("open write should succeed");
    let key = open.payload.session_key;
    let target = add_block_for_key(&env.fs_core, &key, 64).await;
    let close = commit_for_key(
        &env.fs_core,
        &key,
        vec![committed_block(
            target.block_id,
            target.file_offset,
            target.effective_len,
        )],
        64,
    )
    .await
    .expect("commit should succeed");

    let worker_manager = env.fs_core.worker_manager.as_ref().expect("worker manager");
    record_worker_heartbeat(
        worker_manager,
        &env.group_name,
        WorkerId::new(1),
        1024,
        1,
        2048,
        2,
        3,
        HealthStatus::Healthy,
    );
    publish_report_locations(
        worker_manager,
        &env.group_name,
        WorkerId::new(1),
        1,
        vec![target.block_id],
    );

    let locations = env
        .fs_core
        .execute_get_file_layout(GetFileLayoutInput {
            ctx: request_context(),
            inode_id: env.inode_id,
            range: None,
            requested_data_handle_id: Some(env.data_handle_id),
            freshness: Freshness::default(),
        })
        .await
        .expect("locations should succeed");

    assert_eq!(close.payload.file_version, Some(1));
    assert_eq!(locations.payload.file_version, Some(1));
    assert_eq!(stored_file_version(&env.storage, env.inode_id), Some(1));
}

#[tokio::test]
async fn get_locations_rejects_stale_state_watermark() {
    let env = write_flow_env(0).await;
    let open = env
        .fs_core
        .execute_open_write(OpenWriteInput {
            ctx: request_context(),
            inode_id: env.inode_id,
            desired_len: Some(64),
            mode: crate::inode_lease::WriteMode::Write,
            freshness: Freshness::default(),
        })
        .await
        .expect("open write should succeed");
    let key = open.payload.session_key;
    let target = add_block_for_key(&env.fs_core, &key, 64).await;
    commit_for_key(
        &env.fs_core,
        &key,
        vec![committed_block(
            target.block_id,
            target.file_offset,
            target.effective_len,
        )],
        64,
    )
    .await
    .expect("commit should succeed");

    let current_state = env
        .fs_core
        .raft_node
        .as_ref()
        .and_then(|raft_node| raft_node.get_last_applied_state_id())
        .expect("commit should advance applied state");
    let mut ctx = request_context();
    ctx.caller.state.push(types::GroupStateWatermark::new(
        group_name("g15"),
        types::RaftLogId {
            term: current_state.term,
            leader_node_id: current_state.leader_node_id,
            index: current_state.index + 1,
        },
    ));

    let failure = env
        .fs_core
        .execute_get_file_layout(GetFileLayoutInput {
            ctx,
            inode_id: env.inode_id,
            range: None,
            requested_data_handle_id: Some(env.data_handle_id),
            freshness: Freshness::default(),
        })
        .await
        .expect_err("read should reject state watermark beyond local applied state");

    assert_eq!(
        failure.error.code,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::StaleState))
    );
    assert_eq!(failure.error.reason, Some(RefreshReason::StaleState));
}

#[tokio::test]
async fn close_write_invalid_lease_or_fencing_does_not_clear_runtime_session() {
    let mount_id = MountId::new(53);
    let group_name_value = group_name("g11");
    let inode_id = InodeId::new(530);
    let fs_core = fs_core_with_mount(mount_id, 9, &group_name_value);
    let file_handle = install_write_session(&fs_core, inode_id, mount_id);
    let session = fs_core
        .write_session_for_handle(file_handle)
        .expect("session should be installed");

    let wrong_lease = fs_core
        .execute_close_write(CloseWriteInput {
            ctx: request_context(),
            file_handle,
            lease_id: Some(types::ids::LeaseId::new(session.lease_id.as_raw() + 1)),
            lease_epoch: session.lease_epoch,
            open_epoch: session.open_epoch,
            fencing_token: Some(PresentedFencingToken {
                block_id: Some(session.fencing_token.block_id),
                owner: session.fencing_token.owner,
                epoch: session.fencing_token.epoch,
            }),
            intent: CloseWriteIntent {
                committed_blocks: Vec::new(),
                final_size: 0,
            },
            freshness: Freshness::default(),
        })
        .await
        .unwrap_err();

    assert_eq!(
        wrong_lease.error.code,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Fencing))
    );
    assert_eq!(wrong_lease.error.reason, Some(RefreshReason::SessionInvalid));
    assert!(fs_core.write_session_for_handle(file_handle).is_some());
    assert!(fs_core
        .inode_lease_manager_for_test()
        .get_active_lease(inode_id)
        .is_some());

    let wrong_fencing = fs_core
        .execute_close_write(CloseWriteInput {
            ctx: request_context(),
            file_handle,
            lease_id: Some(session.lease_id),
            lease_epoch: session.lease_epoch,
            open_epoch: session.open_epoch,
            fencing_token: Some(PresentedFencingToken {
                block_id: Some(BlockId::new(DataHandleId::new(999_999), BlockIndex::new(0))),
                owner: session.fencing_token.owner,
                epoch: session.fencing_token.epoch,
            }),
            intent: CloseWriteIntent {
                committed_blocks: Vec::new(),
                final_size: 0,
            },
            freshness: Freshness::default(),
        })
        .await
        .unwrap_err();

    assert_eq!(
        wrong_fencing.error.code,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Fencing))
    );
    assert_eq!(wrong_fencing.error.reason, Some(RefreshReason::SessionInvalid));
    assert!(fs_core.write_session_for_handle(file_handle).is_some());
    assert!(fs_core
        .inode_lease_manager_for_test()
        .get_active_lease(inode_id)
        .is_some());
}

#[tokio::test]
async fn commit_rejects_unissued_block() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_id = MountId::new(56);
    let group_name_value = group_name("g14");
    let inode_id = InodeId::new(560);
    let data_handle_id = DataHandleId::new(424_242);
    let mut fs_core = fs_core_with_mount(mount_id, 9, &group_name_value);
    let mount_table = Arc::clone(&fs_core.mount_table);
    let (raft_node, _state_machine) = single_node_raft(Arc::clone(&storage), mount_table).await;
    fs_core.set_storage(Arc::clone(&storage));
    fs_core.set_raft_node(raft_node);
    storage
        .put_inode(&Inode::new_file(inode_id, FileAttrs::new(), mount_id, data_handle_id))
        .unwrap();
    storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
    storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

    let file_handle = install_write_session(&fs_core, inode_id, mount_id);
    let session = fs_core
        .write_session_for_handle(file_handle)
        .expect("session should be installed");
    let failure = fs_core
        .execute_close_write(CloseWriteInput {
            ctx: request_context(),
            file_handle,
            lease_id: Some(session.lease_id),
            lease_epoch: session.lease_epoch,
            open_epoch: session.open_epoch,
            fencing_token: Some(PresentedFencingToken {
                block_id: Some(session.fencing_token.block_id),
                owner: session.fencing_token.owner,
                epoch: session.fencing_token.epoch,
            }),
            intent: CloseWriteIntent {
                committed_blocks: vec![committed_block(BlockId::new(data_handle_id, BlockIndex::new(0)), 0, 64)],
                final_size: 64,
            },
            freshness: Freshness::default(),
        })
        .await
        .expect_err("unissued block must be rejected");

    assert_eq!(
        failure.error.code,
        Some(CanonicalErrorCode::FsErrno(FsErrorCode::EInval))
    );
    assert!(fs_core.write_session_for_handle(file_handle).is_some());
}

#[tokio::test]
async fn commit_rejects_duplicate_block() {
    let env = write_flow_env(0).await;
    let open = env
        .fs_core
        .execute_open_write(OpenWriteInput {
            ctx: request_context(),
            inode_id: env.inode_id,
            desired_len: Some(256),
            mode: crate::inode_lease::WriteMode::Write,
            freshness: Freshness::default(),
        })
        .await
        .expect("open write should succeed");
    let key = open.payload.session_key;
    let target = add_block_for_key(&env.fs_core, &key, 256).await;
    let block = committed_block(target.block_id, target.file_offset, target.effective_len);

    let failure = commit_for_key(&env.fs_core, &key, vec![block.clone(), block], 256)
        .await
        .expect_err("duplicate committed block must be rejected");

    assert_eq!(
        failure.error.code,
        Some(CanonicalErrorCode::FsErrno(FsErrorCode::EInval))
    );
    assert!(env.fs_core.write_session_for_handle(key.file_handle).is_some());
}

#[tokio::test]
async fn commit_rejects_offset_mismatch() {
    let env = write_flow_env(0).await;
    let open = env
        .fs_core
        .execute_open_write(OpenWriteInput {
            ctx: request_context(),
            inode_id: env.inode_id,
            desired_len: Some(256),
            mode: crate::inode_lease::WriteMode::Write,
            freshness: Freshness::default(),
        })
        .await
        .expect("open write should succeed");
    let key = open.payload.session_key;
    let target = add_block_for_key(&env.fs_core, &key, 256).await;
    let committed = committed_block(target.block_id, target.file_offset + 1, target.effective_len);

    let failure = commit_for_key(&env.fs_core, &key, vec![committed], 257)
        .await
        .expect_err("offset mismatch must be rejected");

    assert_eq!(
        failure.error.code,
        Some(CanonicalErrorCode::FsErrno(FsErrorCode::EInval))
    );
    assert!(env.fs_core.write_session_for_handle(key.file_handle).is_some());
}

#[tokio::test]
async fn sync_write_visibility_publishes_prefix_and_keeps_session_open() {
    let env = write_flow_env(0).await;
    let open = env
        .fs_core
        .execute_open_write(OpenWriteInput {
            ctx: request_context(),
            inode_id: env.inode_id,
            desired_len: Some(8192),
            mode: crate::inode_lease::WriteMode::Write,
            freshness: Freshness::default(),
        })
        .await
        .expect("open write should succeed");
    let key = open.payload.session_key;
    let first = add_block_for_key(&env.fs_core, &key, 64).await;
    let second = add_block_for_key(&env.fs_core, &key, 64).await;

    let synced = sync_for_key(
        &env.fs_core,
        &key,
        vec![committed_block(first.block_id, first.file_offset, first.effective_len)],
        64,
        SyncWriteMode::Visibility,
    )
    .await
    .expect("visibility sync should succeed");

    assert_eq!(synced.payload.synced_size, 64);
    assert_eq!(env.storage.get_inode(env.inode_id).unwrap().unwrap().attrs.size, 64);
    assert!(synced.payload.file_version.is_some());
    assert!(env.fs_core.write_session_for_handle(key.file_handle).is_some());
    publish_env_block_location(&env, first.block_id, first.block_stamp, 1);

    let layout = env
        .fs_core
        .execute_get_file_layout(GetFileLayoutInput {
            ctx: request_context(),
            inode_id: env.inode_id,
            range: None,
            requested_data_handle_id: Some(env.data_handle_id),
            freshness: Freshness::default(),
        })
        .await
        .expect("synced prefix should be readable");
    assert_eq!(layout.payload.file_size, 64);
    assert_eq!(layout.payload.extents.len(), 1);
    assert_eq!(layout.payload.extents[0].block_id, first.block_id);

    commit_for_key(
        &env.fs_core,
        &key,
        vec![
            committed_block(first.block_id, first.file_offset, first.effective_len),
            committed_block(second.block_id, second.file_offset, second.effective_len),
        ],
        128,
    )
    .await
    .expect("CommitFile should still close after SyncWrite");
    assert!(env.fs_core.write_session_for_handle(key.file_handle).is_none());
    assert_eq!(env.storage.get_inode(env.inode_id).unwrap().unwrap().attrs.size, 128);
}

#[tokio::test]
async fn sync_write_durability_uses_same_metadata_publish_path() {
    let env = write_flow_env(0).await;
    let open = env
        .fs_core
        .execute_open_write(OpenWriteInput {
            ctx: request_context(),
            inode_id: env.inode_id,
            desired_len: Some(64),
            mode: crate::inode_lease::WriteMode::Write,
            freshness: Freshness::default(),
        })
        .await
        .expect("open write should succeed");
    let key = open.payload.session_key;
    let target = add_block_for_key(&env.fs_core, &key, 64).await;

    let synced = sync_for_key(
        &env.fs_core,
        &key,
        vec![committed_block(
            target.block_id,
            target.file_offset,
            target.effective_len,
        )],
        64,
        SyncWriteMode::Durability,
    )
    .await
    .expect("durability sync should publish through metadata");

    assert_eq!(synced.payload.synced_size, 64);
    assert_eq!(env.storage.get_inode(env.inode_id).unwrap().unwrap().attrs.size, 64);
    assert!(env.fs_core.write_session_for_handle(key.file_handle).is_some());
}

#[tokio::test]
async fn sync_write_rejects_target_beyond_committed_block_coverage() {
    let env = write_flow_env(0).await;
    let open = env
        .fs_core
        .execute_open_write(OpenWriteInput {
            ctx: request_context(),
            inode_id: env.inode_id,
            desired_len: Some(64),
            mode: crate::inode_lease::WriteMode::Write,
            freshness: Freshness::default(),
        })
        .await
        .expect("open write should succeed");
    let key = open.payload.session_key;
    let target = add_block_for_key(&env.fs_core, &key, 64).await;

    let failure = sync_for_key(
        &env.fs_core,
        &key,
        vec![committed_block(
            target.block_id,
            target.file_offset,
            target.effective_len,
        )],
        128,
        SyncWriteMode::Visibility,
    )
    .await
    .expect_err("target beyond committed coverage must be rejected");

    assert_eq!(
        failure.error.code,
        Some(CanonicalErrorCode::FsErrno(FsErrorCode::EInval))
    );
    assert!(env.fs_core.write_session_for_handle(key.file_handle).is_some());
    assert_eq!(env.storage.get_inode(env.inode_id).unwrap().unwrap().attrs.size, 0);
}

#[tokio::test]
async fn repeated_identical_sync_write_is_idempotent_without_file_version_advance() {
    let env = write_flow_env(0).await;
    let open = env
        .fs_core
        .execute_open_write(OpenWriteInput {
            ctx: request_context(),
            inode_id: env.inode_id,
            desired_len: Some(64),
            mode: crate::inode_lease::WriteMode::Write,
            freshness: Freshness::default(),
        })
        .await
        .expect("open write should succeed");
    let key = open.payload.session_key;
    let target = add_block_for_key(&env.fs_core, &key, 64).await;
    let blocks = vec![committed_block(
        target.block_id,
        target.file_offset,
        target.effective_len,
    )];

    let first = sync_for_key(&env.fs_core, &key, blocks.clone(), 64, SyncWriteMode::Visibility)
        .await
        .expect("first SyncWrite should publish");
    let first_version = stored_file_version(&env.storage, env.inode_id).expect("file version");
    let second = sync_for_key(&env.fs_core, &key, blocks, 64, SyncWriteMode::Visibility)
        .await
        .expect("repeated SyncWrite should be a no-op");

    assert_eq!(second.payload.file_version, first.payload.file_version);
    assert_eq!(stored_file_version(&env.storage, env.inode_id), Some(first_version));
}

#[tokio::test]
async fn append_uses_base_size() {
    let env = write_flow_env(128).await;
    let open = env
        .fs_core
        .execute_open_write(OpenWriteInput {
            ctx: request_context(),
            inode_id: env.inode_id,
            desired_len: Some(64),
            mode: crate::inode_lease::WriteMode::Append,
            freshness: Freshness::default(),
        })
        .await
        .expect("append open should succeed");
    let key = open.payload.session_key;
    assert_eq!(open.payload.base_size, 128);
    let target = add_block_for_key(&env.fs_core, &key, 64).await;
    assert_eq!(target.file_offset, 128);

    let wrong_offset = committed_block(target.block_id, 0, target.effective_len);
    let failure = commit_for_key(&env.fs_core, &key, vec![wrong_offset], 64)
        .await
        .expect_err("append commit must start at base_size");

    assert_eq!(
        failure.error.code,
        Some(CanonicalErrorCode::FsErrno(FsErrorCode::EInval))
    );
    assert!(env.fs_core.write_session_for_handle(key.file_handle).is_some());
}

#[tokio::test]
async fn replay_keeps_file_version() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_id = MountId::new(54);
    let group_name_value = group_name("g12");
    let inode_id = InodeId::new(540);
    let data_handle_id = DataHandleId::new(424_242);
    let mut fs_core = fs_core_with_mount(mount_id, 9, &group_name_value);
    let mount_table = Arc::clone(&fs_core.mount_table);
    let (raft_node, _state_machine) = single_node_raft(Arc::clone(&storage), mount_table).await;
    fs_core.set_storage(Arc::clone(&storage));
    fs_core.set_raft_node(raft_node);
    storage
        .put_inode(&Inode::new_file(inode_id, FileAttrs::new(), mount_id, data_handle_id))
        .unwrap();
    storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
    storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

    let file_handle = install_write_session(&fs_core, inode_id, mount_id);
    let session = fs_core
        .write_session_for_handle(file_handle)
        .expect("session should be installed");
    fs_core
        .write_session_manager_for_test()
        .allocate_target(file_handle, None)
        .expect("target should be issued");
    let ctx = request_context();
    let request = CloseWriteInput {
        ctx,
        file_handle,
        lease_id: Some(session.lease_id),
        lease_epoch: session.lease_epoch,
        open_epoch: session.open_epoch,
        fencing_token: Some(PresentedFencingToken {
            block_id: Some(session.fencing_token.block_id),
            owner: session.fencing_token.owner,
            epoch: session.fencing_token.epoch,
        }),
        intent: CloseWriteIntent {
            committed_blocks: vec![committed_block(BlockId::new(data_handle_id, BlockIndex::new(0)), 0, 64)],
            final_size: 64,
        },
        freshness: Freshness::default(),
    };

    let first = fs_core
        .execute_close_write(request.clone())
        .await
        .expect("first close should succeed");
    assert_eq!(first.payload.committed_size, 64);
    assert!(fs_core.write_session_for_handle(file_handle).is_none());

    let inode_after_first = storage.get_inode(inode_id).unwrap().unwrap();
    let block_ref_after_first = storage
        .get_block_ref_count(BlockId::new(data_handle_id, BlockIndex::new(0)))
        .unwrap();

    let replay = fs_core
        .execute_close_write(request.clone())
        .await
        .expect("same close replay should return persisted result");

    assert_eq!(replay.payload.committed_size, first.payload.committed_size);
    assert_eq!(replay.payload.file_version, first.payload.file_version);
    assert!(fs_core.write_session_for_handle(file_handle).is_none());
    assert_eq!(storage.get_inode(inode_id).unwrap().unwrap(), inode_after_first);
    assert_eq!(
        storage
            .get_block_ref_count(BlockId::new(data_handle_id, BlockIndex::new(0)))
            .unwrap(),
        block_ref_after_first
    );

    let mut mismatch = request;
    mismatch.intent.final_size = 65;
    let mismatch_failure = fs_core
        .execute_close_write(mismatch)
        .await
        .expect_err("same call_id with different close payload should fail");
    assert_eq!(
        mismatch_failure.error.code,
        Some(CanonicalErrorCode::FsErrno(FsErrorCode::EInval))
    );
    assert!(fs_core.write_session_for_handle(file_handle).is_none());
    assert_eq!(storage.get_inode(inode_id).unwrap().unwrap(), inode_after_first);
    assert_eq!(
        storage
            .get_block_ref_count(BlockId::new(data_handle_id, BlockIndex::new(0)))
            .unwrap(),
        block_ref_after_first
    );
}

#[tokio::test]
async fn replay_keeps_append_commit_mode() {
    let env = write_flow_env(64).await;
    let open = env
        .fs_core
        .execute_open_write(OpenWriteInput {
            ctx: request_context(),
            inode_id: env.inode_id,
            desired_len: Some(64),
            mode: crate::inode_lease::WriteMode::Append,
            freshness: Freshness::default(),
        })
        .await
        .expect("append open should succeed");
    let key = open.payload.session_key;
    let target = add_block_for_key(&env.fs_core, &key, 64).await;
    assert_eq!(target.file_offset, 64);
    let request = CloseWriteInput {
        ctx: request_context(),
        file_handle: key.file_handle,
        lease_id: Some(key.lease_id),
        lease_epoch: key.lease_epoch,
        open_epoch: key.open_epoch,
        fencing_token: Some(presented_key_token(&key)),
        intent: CloseWriteIntent {
            committed_blocks: vec![committed_block(
                target.block_id,
                target.file_offset,
                target.effective_len,
            )],
            final_size: 128,
        },
        freshness: Freshness::default(),
    };

    let first = env
        .fs_core
        .execute_close_write(request.clone())
        .await
        .expect("append close should succeed");
    assert_eq!(first.payload.committed_size, 128);
    assert!(env.fs_core.write_session_for_handle(key.file_handle).is_none());

    let replay = env
        .fs_core
        .execute_close_write(request)
        .await
        .expect("append close replay should recover original commit mode");
    assert_eq!(replay.payload.committed_size, first.payload.committed_size);
    assert!(env.fs_core.write_session_for_handle(key.file_handle).is_none());
}

#[tokio::test]
async fn delete_file_with_active_write_session_returns_busy_without_namespace_mutation() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_id = MountId::new(55);
    let group_name_value = group_name("g13");
    let parent_inode_id = InodeId::new(550);
    let inode_id = InodeId::new(551);
    let data_handle_id = DataHandleId::new(552);
    let mut fs_core = fs_core_with_mount(mount_id, 9, &group_name_value);
    let mount_table = Arc::clone(&fs_core.mount_table);
    let (raft_node, _state_machine) = single_node_raft(Arc::clone(&storage), mount_table).await;
    fs_core.set_storage(Arc::clone(&storage));
    fs_core.set_raft_node(raft_node);

    storage
        .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), mount_id))
        .unwrap();
    storage
        .put_inode(&Inode::new_file(inode_id, FileAttrs::new(), mount_id, data_handle_id))
        .unwrap();
    storage.put_dentry(parent_inode_id, "busy", inode_id).unwrap();
    storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
    storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();
    let file_handle = install_write_session(&fs_core, inode_id, mount_id);

    let failure = fs_core
        .execute_unlink(UnlinkInput {
            ctx: request_context(),
            parent_inode_id,
            name: "busy".to_string(),
            freshness: Freshness::default(),
        })
        .await
        .unwrap_err();

    assert_eq!(
        failure.error.code,
        Some(CanonicalErrorCode::FsErrno(FsErrorCode::EBusy))
    );
    assert!(fs_core.write_session_for_handle(file_handle).is_some());
    assert_eq!(storage.get_dentry(parent_inode_id, "busy").unwrap(), Some(inode_id));
    assert!(storage.get_inode(inode_id).unwrap().is_some());
}

#[tokio::test]
async fn rename_rejects_active_write_target() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_id = MountId::new(56);
    let group_name_value = group_name("g14");
    let parent_inode_id = InodeId::new(560);
    let source_inode_id = InodeId::new(561);
    let target_inode_id = InodeId::new(562);
    let source_handle = DataHandleId::new(563);
    let target_handle = DataHandleId::new(564);
    let mut fs_core = fs_core_with_mount(mount_id, 9, &group_name_value);
    let mount_table = Arc::clone(&fs_core.mount_table);
    let (raft_node, _state_machine) = single_node_raft(Arc::clone(&storage), mount_table).await;
    fs_core.set_storage(Arc::clone(&storage));
    fs_core.set_raft_node(raft_node);

    storage
        .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), mount_id))
        .unwrap();
    storage
        .put_inode(&Inode::new_file(
            source_inode_id,
            FileAttrs::new(),
            mount_id,
            source_handle,
        ))
        .unwrap();
    storage
        .put_inode(&Inode::new_file(
            target_inode_id,
            FileAttrs::new(),
            mount_id,
            target_handle,
        ))
        .unwrap();
    storage.put_dentry(parent_inode_id, "source", source_inode_id).unwrap();
    storage.put_dentry(parent_inode_id, "target", target_inode_id).unwrap();
    storage
        .put_layout(source_inode_id, FileLayout::new(4096, 4096, 1))
        .unwrap();
    storage
        .put_layout(target_inode_id, FileLayout::new(4096, 4096, 1))
        .unwrap();
    storage.put_data_handle_owner(source_handle, source_inode_id).unwrap();
    storage.put_data_handle_owner(target_handle, target_inode_id).unwrap();
    let file_handle = install_write_session(&fs_core, target_inode_id, mount_id);

    let failure = fs_core
        .execute_rename(RenameInput {
            ctx: request_context(),
            src_parent_inode_id: parent_inode_id,
            src_name: "source".to_string(),
            dst_parent_inode_id: parent_inode_id,
            dst_name: "target".to_string(),
            flags: 0,
            freshness: Freshness::default(),
        })
        .await
        .unwrap_err();

    assert_eq!(
        failure.error.code,
        Some(CanonicalErrorCode::FsErrno(FsErrorCode::EBusy))
    );
    assert!(fs_core.write_session_for_handle(file_handle).is_some());
    assert_eq!(
        storage.get_dentry(parent_inode_id, "source").unwrap(),
        Some(source_inode_id)
    );
    assert_eq!(
        storage.get_dentry(parent_inode_id, "target").unwrap(),
        Some(target_inode_id)
    );
    assert!(storage.get_inode(target_inode_id).unwrap().is_some());
}

#[tokio::test]
async fn rename_keeps_file_version() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_id = MountId::new(59);
    let group_name_value = group_name("g16");
    let parent_inode_id = InodeId::new(590);
    let source_inode_id = InodeId::new(591);
    let target_inode_id = InodeId::new(592);
    let source_handle = DataHandleId::new(593);
    let target_handle = DataHandleId::new(594);
    let mut fs_core = fs_core_with_mount(mount_id, 9, &group_name_value);
    let mount_table = Arc::clone(&fs_core.mount_table);
    let (raft_node, _state_machine) = single_node_raft(Arc::clone(&storage), mount_table).await;
    fs_core.set_storage(Arc::clone(&storage));
    fs_core.set_raft_node(raft_node);

    storage
        .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), mount_id))
        .unwrap();
    let mut source = Inode::new_file(source_inode_id, FileAttrs::new(), mount_id, source_handle);
    if let types::fs::InodeData::File {
        file_version,
        lease_epoch,
        ..
    } = &mut source.data
    {
        *file_version = Some(77);
        *lease_epoch = Some(900);
    }
    let mut target = Inode::new_file(target_inode_id, FileAttrs::new(), mount_id, target_handle);
    if let types::fs::InodeData::File {
        file_version,
        lease_epoch,
        ..
    } = &mut target.data
    {
        *file_version = Some(12);
        *lease_epoch = Some(12);
    }
    storage.put_inode(&source).unwrap();
    storage.put_inode(&target).unwrap();
    storage.put_dentry(parent_inode_id, "source", source_inode_id).unwrap();
    storage.put_dentry(parent_inode_id, "target", target_inode_id).unwrap();
    storage
        .put_layout(source_inode_id, FileLayout::new(4096, 4096, 1))
        .unwrap();
    storage
        .put_layout(target_inode_id, FileLayout::new(4096, 4096, 1))
        .unwrap();
    storage.put_data_handle_owner(source_handle, source_inode_id).unwrap();
    storage.put_data_handle_owner(target_handle, target_inode_id).unwrap();

    fs_core
        .execute_rename(RenameInput {
            ctx: request_context(),
            src_parent_inode_id: parent_inode_id,
            src_name: "source".to_string(),
            dst_parent_inode_id: parent_inode_id,
            dst_name: "target".to_string(),
            flags: 0,
            freshness: Freshness::default(),
        })
        .await
        .expect("same-mount overwrite rename should succeed");

    assert_eq!(storage.get_dentry(parent_inode_id, "source").unwrap(), None);
    assert_eq!(
        storage.get_dentry(parent_inode_id, "target").unwrap(),
        Some(source_inode_id)
    );
    assert_eq!(stored_file_version(&storage, source_inode_id), Some(77));
    assert!(storage.get_inode(target_inode_id).unwrap().is_none());
    assert_eq!(storage.get_inode_by_data_handle(target_handle).unwrap(), None);
}

#[tokio::test]
async fn rename_rejects_cross_mount() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let src_mount_id = MountId::new(57);
    let dst_mount_id = MountId::new(58);
    let src_parent_inode_id = InodeId::new(570);
    let dst_parent_inode_id = InodeId::new(580);
    let source_inode_id = InodeId::new(571);
    let mut fs_core = fs_core_with_mount(src_mount_id, 9, &group_name("g15"));
    fs_core.set_storage(Arc::clone(&storage));

    storage
        .put_inode(&Inode::new_dir(src_parent_inode_id, FileAttrs::new(), src_mount_id))
        .unwrap();
    storage
        .put_inode(&Inode::new_dir(dst_parent_inode_id, FileAttrs::new(), dst_mount_id))
        .unwrap();
    storage
        .put_inode(&Inode::new_file(
            source_inode_id,
            FileAttrs::new(),
            src_mount_id,
            DataHandleId::new(571),
        ))
        .unwrap();
    storage
        .put_dentry(src_parent_inode_id, "source", source_inode_id)
        .unwrap();

    let failure = fs_core
        .execute_rename(RenameInput {
            ctx: request_context(),
            src_parent_inode_id,
            src_name: "source".to_string(),
            dst_parent_inode_id,
            dst_name: "target".to_string(),
            flags: 0,
            freshness: Freshness::default(),
        })
        .await
        .unwrap_err();

    assert_eq!(
        failure.error.code,
        Some(CanonicalErrorCode::FsErrno(FsErrorCode::EXDev))
    );
    assert_eq!(
        storage.get_dentry(src_parent_inode_id, "source").unwrap(),
        Some(source_inode_id)
    );
    assert_eq!(storage.get_dentry(dst_parent_inode_id, "target").unwrap(), None);
}

#[tokio::test]
async fn close_write_session_missing_without_applied_result_stays_session_invalid() {
    let group_name_value = group_name("g13");
    let fs_core = fs_core_with_mount(MountId::new(55), 9, &group_name_value);
    let mut ctx = request_context();
    ctx.caller = ctx.caller.with_group_name(group_name_value.clone());

    let failure = fs_core
        .execute_close_write(CloseWriteInput {
            ctx,
            file_handle: 999_999,
            lease_id: Some(types::ids::LeaseId::new(1)),
            lease_epoch: 1,
            open_epoch: 1,
            fencing_token: Some(PresentedFencingToken {
                block_id: Some(BlockId::new(DataHandleId::new(1), BlockIndex::new(0))),
                owner: ClientId::new(7),
                epoch: 1,
            }),
            intent: CloseWriteIntent {
                committed_blocks: Vec::new(),
                final_size: 0,
            },
            freshness: Freshness::default(),
        })
        .await
        .unwrap_err();

    assert_eq!(
        failure.error.code,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::Fencing))
    );
    assert_eq!(failure.error.reason, Some(RefreshReason::SessionInvalid));
    assert_eq!(failure.group_name, Some(group_name_value));
}

#[tokio::test]
async fn create_mount_route_epoch_progression_rejects_stale_client_route_epoch() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_table = Arc::new(MountTable::new());
    let state_machine = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

    let mount_id = MountId::new(21);
    let root_inode_id = InodeId::new(210);
    storage
        .put_inode(&Inode::new_dir(root_inode_id, FileAttrs::new(), mount_id))
        .unwrap();

    let stale_route_epoch = storage.get_route_epoch().unwrap().as_u64();
    state_machine
        .apply(Command::CreateMount {
            dedup: DedupKey::system(),
            mount_id,
            mount_prefix: "/mnt/route".to_string(),
            mount_kind: MountKind::External,
            ufs_uri: Some("ufs://route".to_string()),
            data_io_policy: DataIoPolicy::Allow,
            namespace_owner_group_name: group_name("g6"),
            root_inode_id,
        })
        .unwrap();

    let advanced_route_epoch = storage.get_route_epoch().unwrap().as_u64();
    assert_eq!(advanced_route_epoch, stale_route_epoch + 1);

    let fs_core = FsCore::new_default(
        Arc::new(StorageBackedRouteEpochStore {
            storage: Arc::clone(&storage),
        }),
        mount_table,
    );
    let failure = fs_core
        .validate_route_epoch(
            &request_context(),
            Freshness {
                mount_epoch: Some(1),
                route_epoch: Some(stale_route_epoch),
            },
            Some(group_name("g6")),
            Some(1),
            "OpenWrite",
        )
        .await
        .unwrap_err();

    assert_eq!(
        failure.error.code,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::RouteEpochMismatch))
    );
    assert_eq!(failure.error.reason, Some(RefreshReason::RouteEpochMismatch));
    let hint = failure.error.refresh_hint.expect("refresh hint");
    assert_eq!(hint.route_epoch, Some(advanced_route_epoch));
    assert_eq!(hint.mount_epoch, Some(1));
    assert_eq!(failure.route_epoch, Some(advanced_route_epoch));
}

#[tokio::test]
async fn delete_mount_route_epoch_progression_rejects_stale_client_route_epoch() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
    let mount_table = Arc::new(MountTable::new());
    let state_machine = AppRaftStateMachine::new(Arc::clone(&storage), Arc::clone(&mount_table));

    let mount_id = MountId::new(31);
    let root_inode_id = InodeId::new(310);
    storage
        .put_inode(&Inode::new_dir(root_inode_id, FileAttrs::new(), mount_id))
        .unwrap();

    state_machine
        .apply(Command::CreateMount {
            dedup: DedupKey::system(),
            mount_id,
            mount_prefix: "/mnt/delete-route".to_string(),
            mount_kind: MountKind::External,
            ufs_uri: Some("ufs://delete-route".to_string()),
            data_io_policy: DataIoPolicy::Allow,
            namespace_owner_group_name: group_name("g8"),
            root_inode_id,
        })
        .unwrap();

    let stale_route_epoch = storage.get_route_epoch().unwrap().as_u64();
    state_machine
        .apply(Command::DeleteMount {
            dedup: DedupKey::system(),
            mount_id,
        })
        .unwrap();

    let advanced_route_epoch = storage.get_route_epoch().unwrap().as_u64();
    assert_eq!(advanced_route_epoch, stale_route_epoch + 1);

    let fs_core = FsCore::new_default(
        Arc::new(StorageBackedRouteEpochStore {
            storage: Arc::clone(&storage),
        }),
        mount_table,
    );
    let failure = fs_core
        .validate_route_epoch(
            &request_context(),
            Freshness {
                mount_epoch: None,
                route_epoch: Some(stale_route_epoch),
            },
            None,
            None,
            "GetFileLayout",
        )
        .await
        .unwrap_err();

    assert_eq!(
        failure.error.code,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::RouteEpochMismatch))
    );
    assert_eq!(failure.error.reason, Some(RefreshReason::RouteEpochMismatch));
    let hint = failure.error.refresh_hint.expect("refresh hint");
    assert_eq!(hint.route_epoch, Some(advanced_route_epoch));
    assert_eq!(failure.route_epoch, Some(advanced_route_epoch));
}
