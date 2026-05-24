// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use super::*;
use crate::config::RaftConfig;
use crate::mount::{DataIoPolicy, MountEntry, MountKind, ROOT_INODE_ID};
use crate::raft::{AppRaftNode, AppRaftStateMachine, Command, DedupKey, RocksDBStorage};
use crate::service::domain::{
    AbortWriteInput, AddBlockInput, CloseWriteInput, CloseWriteIntent, CoreResult, Freshness, GetAttrInput,
    GetFileLayoutInput, OpenWriteInput, PresentedFencingToken, ReadDirInput, RenameInput, RenewLeaseInput,
    RequestContext, SessionKey, SyncWriteInput, SyncWriteMode, UnlinkInput,
};
use crate::state::{MemoryStateStore, RouteEpoch};
use crate::worker::{HealthStatus, WorkerManager};
use async_trait::async_trait;
use common::error::canonical::{ErrorCode as CanonicalErrorCode, RefreshReason};
use common::header::{AuthnType, RequestHeader, RpcErrorCode};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use types::fs::{FileAttrs, Inode};
use types::ids::{BlockId, BlockIndex, ClientId, DataHandleId, LeaseId, MountId, ShardGroupId, WorkerId};
use types::layout::FileLayout;
use types::lease::FencingToken;
use types::{CommittedBlock, WriteTarget};

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

fn fs_core_with_mount(mount_id: MountId, mount_epoch: u64, group_id: ShardGroupId) -> FsCore {
    let mount_table = Arc::new(MountTable::new());
    mount_table
        .upsert(MountEntry {
            mount_id,
            mount_prefix: "/".to_string(),
            mount_kind: MountKind::Internal,
            ufs_uri: None,
            data_io_policy: DataIoPolicy::Allow,
            mount_version: mount_epoch,
            namespace_owner_group_id: group_id,
            root_inode_id: ROOT_INODE_ID,
        })
        .unwrap();
    FsCore::new_default(Arc::new(MemoryStateStore::new()), mount_table)
}

fn worker_manager_for_write_targets(group_id: ShardGroupId) -> Arc<WorkerManager> {
    let manager = Arc::new(WorkerManager::new(60));
    for raw in 1..=3 {
        let worker_id = types::ids::WorkerId::new(raw);
        manager
            .register_worker(
                group_id,
                worker_id,
                format!("127.0.0.1:{}", 9000 + raw),
                1,
                10 + raw,
                None,
            )
            .unwrap();
        manager
            .update_runtime(
                group_id,
                worker_id,
                1,
                10 + raw,
                1024 * 1024,
                0,
                1024 * 1024,
                0,
                0,
                HealthStatus::Healthy,
            )
            .unwrap();
    }
    manager
}

#[tokio::test]
async fn get_file_layout_returns_worker_locations_from_worker_manager() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
    let mount_id = MountId::new(48);
    let group_id = ShardGroupId::new(8);
    let inode_id = InodeId::new(480);
    let data_handle_id = DataHandleId::new(9480);
    let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
    let mut fs_core = fs_core_with_mount(mount_id, 9, group_id);
    let worker_manager = Arc::new(WorkerManager::new(60));
    for (raw, port) in [(2, 9102), (1, 9101)] {
        let worker_id = WorkerId::new(raw);
        worker_manager
            .register_worker(group_id, worker_id, format!("127.0.0.1:{port}"), 1, 20 + raw, None)
            .unwrap();
        worker_manager
            .update_runtime(
                group_id,
                worker_id,
                1,
                20 + raw,
                1024,
                0,
                1024,
                0,
                0,
                HealthStatus::Healthy,
            )
            .unwrap();
        worker_manager.update_locations(worker_id, vec![block_id]).unwrap();
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

    let success = fs_core
        .execute_get_file_layout(GetFileLayoutInput {
            ctx: request_context(),
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
        vec![WorkerId::new(1), WorkerId::new(2)]
    );
    assert_eq!(location.worker_epoch, Some(22));
    assert_eq!(location.block_stamp, 41);
    assert_eq!(location.workers[0].endpoint, "127.0.0.1:9101");
}

#[tokio::test]
async fn get_file_layout_rejects_returned_extent_without_block_stamp() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
    let mount_id = MountId::new(49);
    let inode_id = InodeId::new(490);
    let data_handle_id = DataHandleId::new(9490);
    let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
    let mut fs_core = fs_core_with_mount(mount_id, 9, ShardGroupId::new(8));
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
    let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
    let mount_id = MountId::new(50);
    let inode_id = InodeId::new(500);
    let data_handle_id = DataHandleId::new(9500);
    let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
    let mut fs_core = fs_core_with_mount(mount_id, 9, ShardGroupId::new(8));
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
    let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
    let mount_id = MountId::new(70);
    let inode_id = InodeId::new(700);
    let mut fs_core = fs_core_with_mount(mount_id, 9, ShardGroupId::new(17));
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
                worker_epoch: None,
            },
        })
        .await
        .expect_err("stale mount_epoch must reject GetStatus");

    assert_eq!(
        failure.error.code,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::MountEpochMismatch))
    );
    assert_eq!(failure.error.reason, Some(RefreshReason::MountEpochMismatch));
    assert_eq!(failure.group_id, Some(17));
    assert_eq!(failure.mount_epoch, Some(9));
}

#[tokio::test]
async fn list_status_rejects_stale_mount_epoch() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
    let mount_id = MountId::new(71);
    let parent_inode_id = InodeId::new(710);
    let mut fs_core = fs_core_with_mount(mount_id, 9, ShardGroupId::new(18));
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
                worker_epoch: None,
            },
        })
        .await
        .expect_err("stale mount_epoch must reject ListStatus");

    assert_eq!(
        failure.error.code,
        Some(CanonicalErrorCode::RpcCode(RpcErrorCode::MountEpochMismatch))
    );
    assert_eq!(failure.error.reason, Some(RefreshReason::MountEpochMismatch));
    assert_eq!(failure.group_id, Some(18));
    assert_eq!(failure.mount_epoch, Some(9));
}

#[tokio::test]
async fn open_file_rejects_stale_route_epoch() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
    let mount_id = MountId::new(72);
    let inode_id = InodeId::new(720);
    let data_handle_id = DataHandleId::new(9720);
    let mut fs_core = fs_core_with_mount(mount_id, 9, ShardGroupId::new(19));
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
                worker_epoch: None,
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
    let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
    let mount_id = MountId::new(73);
    let inode_id = InodeId::new(730);
    let data_handle_id = DataHandleId::new(9730);
    let mut fs_core = fs_core_with_mount(mount_id, 9, ShardGroupId::new(20));
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
                worker_epoch: None,
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
    let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
    let mount_id = MountId::new(74);
    let inode_id = InodeId::new(740);
    let mut fs_core = fs_core_with_mount(mount_id, 9, ShardGroupId::new(21));
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

    assert_eq!(success.group_id, Some(21));
    assert_eq!(success.mount_epoch, Some(9));
    assert_eq!(success.route_epoch, Some(1));
}

#[tokio::test]
async fn get_locations_rejects_range_overflow() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
    let mount_id = MountId::new(75);
    let inode_id = InodeId::new(750);
    let data_handle_id = DataHandleId::new(9750);
    let mut fs_core = fs_core_with_mount(mount_id, 9, ShardGroupId::new(22));
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
    let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
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
    let mut fs_core = fs_core_with_mount(mount_id, 9, ShardGroupId::new(23));
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
    let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
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
    let mut fs_core = fs_core_with_mount(mount_id, 9, ShardGroupId::new(24));
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
                len: 64,
                worker_endpoints: Vec::new(),
                fencing_token: FencingToken {
                    block_id: BlockId::new(data_handle_id, BlockIndex::new(0)),
                    owner: writer,
                    epoch: lease_epoch,
                },
                block_stamp: 1,
                chunk_size: 64,
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
        owner: session.fencing_token.owner.as_raw(),
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
        owner: key.fencing_token.owner.as_raw(),
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
    group_id: ShardGroupId,
}

async fn write_flow_env(base_size: u64) -> WriteFlowEnv {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
    let mount_id = MountId::new(57 + base_size);
    let group_id = ShardGroupId::new(15 + base_size);
    let inode_id = InodeId::new(570 + base_size);
    let data_handle_id = DataHandleId::new(9570 + base_size);
    let mut fs_core = fs_core_with_mount(mount_id, 9, group_id);
    let mount_table = Arc::clone(&fs_core.mount_table);
    let (raft_node, _state_machine) = single_node_raft(Arc::clone(&storage), mount_table).await;
    fs_core.set_storage(Arc::clone(&storage));
    fs_core.set_raft_node(raft_node);
    fs_core.set_worker_manager(worker_manager_for_write_targets(group_id));

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
        group_id,
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
    let raft_config = RaftConfig {
        node_id: 1,
        peers: vec!["127.0.0.1:0".to_string()],
    };
    let raft_node = Arc::new(
        AppRaftNode::new(1, storage, Arc::clone(&state_machine), &raft_config)
            .await
            .unwrap(),
    );
    (raft_node, state_machine)
}

#[test]
fn freshness_validator_rejects_routed_write_mount_epoch_with_replay_hint() {
    let mount_id = MountId::new(12);
    let group_id = ShardGroupId::new(4);
    let mount_table = Arc::new(MountTable::new());
    mount_table
        .upsert(MountEntry {
            mount_id,
            mount_prefix: "/data".to_string(),
            mount_kind: MountKind::Internal,
            ufs_uri: None,
            data_io_policy: DataIoPolicy::Allow,
            mount_version: 9,
            namespace_owner_group_id: group_id,
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
                worker_epoch: None,
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
    assert_eq!(hint.group_id, Some(group_id.as_raw()));
    assert_eq!(hint.mount_epoch, Some(9));
    assert_eq!(failure.group_id, Some(group_id.as_raw()));
    assert_eq!(failure.mount_epoch, Some(9));
}

#[test]
fn routed_write_mount_epoch_mismatch_preserves_metrics_and_wire_shape() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
    let mount_id = MountId::new(13);
    let parent_inode_id = InodeId::new(130);
    storage
        .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), mount_id))
        .unwrap();
    let mut fs_core = fs_core_with_mount(mount_id, 9, ShardGroupId::new(5));
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
                worker_epoch: None,
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
    assert_eq!(hint.group_id, Some(5));
    assert_eq!(hint.mount_epoch, Some(9));
    assert_eq!(failure.group_id, Some(5));
    assert_eq!(failure.mount_epoch, Some(9));
    assert_eq!(metrics.fs_write_mount_epoch_mismatch_total.load(Ordering::Relaxed), 1);
}

#[test]
fn freshness_validator_rejects_stale_state_watermark() {
    let group_id = ShardGroupId::new(4);
    let validator = FreshnessValidator::new(Arc::new(MemoryStateStore::new()), Arc::new(MountTable::new()));
    let mut ctx = request_context();
    ctx.caller.state = vec![types::GroupStateWatermark::new(
        group_id,
        types::RaftLogId::new(1, 7, 12),
    )];

    let failure = validator
        .validate_stale_state(
            &ctx,
            Some(types::RaftLogId::new(1, 7, 10)),
            Some(group_id.as_raw()),
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
    assert_eq!(failure.group_id, Some(group_id.as_raw()));
    assert_eq!(failure.mount_epoch, Some(9));
    assert!(failure.state.is_empty());

    let unknown = validator
        .validate_stale_state(&ctx, None, Some(group_id.as_raw()), Some(9))
        .expect("missing last_applied should preserve existing precheck fallback");
    assert_eq!(unknown, StaleStateStatus::UnknownLastApplied);
}

#[tokio::test]
async fn abort_releases_lease() {
    let mount_id = MountId::new(41);
    let group_id = ShardGroupId::new(4);
    let inode_id = InodeId::new(410);
    let fs_core = fs_core_with_mount(mount_id, 9, group_id);
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
    assert_eq!(success.group_id, Some(group_id.as_raw()));
}

#[tokio::test]
async fn abort_checks_handle() {
    let mount_id = MountId::new(43);
    let inode_id = InodeId::new(430);
    let fs_core = fs_core_with_mount(mount_id, 9, ShardGroupId::new(6));

    let failure = fs_core
        .execute_abort_write(AbortWriteInput {
            ctx: request_context(),
            file_handle: 999,
            lease_id: Some(LeaseId::new(1)),
            lease_epoch: 1,
            open_epoch: 1,
            fencing_token: Some(PresentedFencingToken {
                block_id: Some(BlockId::new(DataHandleId::new(1), BlockIndex::new(0))),
                owner: 7,
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
    let fs_core = fs_core_with_mount(mount_id, 9, ShardGroupId::new(6));
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
    let fs_core = fs_core_with_mount(mount_id, 9, ShardGroupId::new(6));
    let file_handle = install_write_session(&fs_core, inode_id, mount_id);
    let session = fs_core
        .write_session_for_handle(file_handle)
        .expect("session should be installed");

    let mut stale_fencing = renew_input_for_session(&session, file_handle, request_context());
    stale_fencing.fencing_token = Some(PresentedFencingToken {
        block_id: Some(BlockId::new(DataHandleId::new(999_999), BlockIndex::new(0))),
        owner: session.fencing_token.owner.as_raw(),
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
    let fs_core = fs_core_with_mount(mount_id, 9, ShardGroupId::new(6));
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
    let fs_core = fs_core_with_mount(MountId::new(47), 9, ShardGroupId::new(6));

    let failure = fs_core
        .execute_renew_inode_lease(RenewLeaseInput {
            ctx: request_context(),
            file_handle: 404,
            lease_id: Some(LeaseId::new(1)),
            lease_epoch: 1,
            open_epoch: 1,
            fencing_token: Some(PresentedFencingToken {
                block_id: Some(BlockId::new(DataHandleId::new(1), BlockIndex::new(0))),
                owner: 7,
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
    let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
    let mount_id = MountId::new(50);
    let inode_id = InodeId::new(500);
    let data_handle_id = DataHandleId::new(9500);
    storage
        .put_inode(&Inode::new_file(inode_id, FileAttrs::new(), mount_id, data_handle_id))
        .unwrap();
    storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

    let mut fs_core = fs_core_with_mount(mount_id, 9, ShardGroupId::new(7));
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
    let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
    let mount_id = MountId::new(51);
    let group_id = ShardGroupId::new(9);
    let inode_id = InodeId::new(510);
    let data_handle_id = DataHandleId::new(9510);
    storage
        .put_inode(&Inode::new_file(inode_id, FileAttrs::new(), mount_id, data_handle_id))
        .unwrap();
    storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

    let mut fs_core = fs_core_with_mount(mount_id, 9, group_id);
    fs_core.set_storage(storage);
    fs_core.set_worker_manager(worker_manager_for_write_targets(group_id));

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
async fn open_write_rejects_file_missing_current_data_handle() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
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

    let mut fs_core = fs_core_with_mount(mount_id, 9, ShardGroupId::new(10));
    fs_core.set_storage(storage);
    fs_core.set_worker_manager(worker_manager_for_write_targets(ShardGroupId::new(10)));

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
        vec![committed_block(target.block_id, target.file_offset, target.len)],
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
    assert_eq!(target.len, 512);
    assert!(target.worker_endpoints[0].endpoint.starts_with("127.0.0.1:900"));
    assert!(!target.worker_endpoints[0].endpoint.ends_with(":0"));
    let session_after_add = env
        .fs_core
        .write_session_for_handle(key.file_handle)
        .expect("session should remain open");
    assert_eq!(session_after_add.next_target_index, 1);
    assert_eq!(session_after_add.issued_targets.len(), 1);

    let committed = committed_block(target.block_id, target.file_offset, target.len);
    let success = commit_for_key(&env.fs_core, &key, vec![committed], 512)
        .await
        .expect("commit should succeed");
    assert_eq!(success.payload.committed_size, 512);
    assert_eq!(success.payload.file_version, Some(1));
    assert!(env.fs_core.write_session_for_handle(key.file_handle).is_none());
    assert_eq!(env.storage.get_inode(env.inode_id).unwrap().unwrap().attrs.size, 512);
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
        vec![committed_block(target.block_id, target.file_offset, target.len)],
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
            first_target.len,
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
            second_target.len,
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
        vec![committed_block(target.block_id, target.file_offset, target.len)],
        64,
    )
    .await
    .expect("commit should succeed");

    let worker_manager = env.fs_core.worker_manager.as_ref().expect("worker manager");
    worker_manager
        .update_locations(WorkerId::new(1), vec![target.block_id])
        .expect("worker report should update soft locations");
    worker_manager
        .update_runtime(
            env.group_id,
            WorkerId::new(1),
            1,
            99,
            1024,
            1,
            2048,
            2,
            3,
            HealthStatus::Healthy,
        )
        .expect("worker runtime should update soft state");

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
        vec![committed_block(target.block_id, target.file_offset, target.len)],
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
        ShardGroupId::new(15),
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
    let group_id = ShardGroupId::new(11);
    let inode_id = InodeId::new(530);
    let fs_core = fs_core_with_mount(mount_id, 9, group_id);
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
                owner: session.fencing_token.owner.as_raw(),
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
                owner: session.fencing_token.owner.as_raw(),
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
    let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
    let mount_id = MountId::new(56);
    let group_id = ShardGroupId::new(14);
    let inode_id = InodeId::new(560);
    let data_handle_id = DataHandleId::new(424_242);
    let mut fs_core = fs_core_with_mount(mount_id, 9, group_id);
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
                owner: session.fencing_token.owner.as_raw(),
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
    let block = committed_block(target.block_id, target.file_offset, target.len);

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
    let committed = committed_block(target.block_id, target.file_offset + 1, target.len);

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
        vec![committed_block(first.block_id, first.file_offset, first.len)],
        64,
        SyncWriteMode::Visibility,
    )
    .await
    .expect("visibility sync should succeed");

    assert_eq!(synced.payload.synced_size, 64);
    assert_eq!(env.storage.get_inode(env.inode_id).unwrap().unwrap().attrs.size, 64);
    assert!(synced.payload.file_version.is_some());
    assert!(env.fs_core.write_session_for_handle(key.file_handle).is_some());

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
            committed_block(first.block_id, first.file_offset, first.len),
            committed_block(second.block_id, second.file_offset, second.len),
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
        vec![committed_block(target.block_id, target.file_offset, target.len)],
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
        vec![committed_block(target.block_id, target.file_offset, target.len)],
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
    let blocks = vec![committed_block(target.block_id, target.file_offset, target.len)];

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

    let wrong_offset = committed_block(target.block_id, 0, target.len);
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
    let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
    let mount_id = MountId::new(54);
    let group_id = ShardGroupId::new(12);
    let inode_id = InodeId::new(540);
    let data_handle_id = DataHandleId::new(424_242);
    let mut fs_core = fs_core_with_mount(mount_id, 9, group_id);
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
            owner: session.fencing_token.owner.as_raw(),
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
            committed_blocks: vec![committed_block(target.block_id, target.file_offset, target.len)],
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
    let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
    let mount_id = MountId::new(55);
    let group_id = ShardGroupId::new(13);
    let parent_inode_id = InodeId::new(550);
    let inode_id = InodeId::new(551);
    let data_handle_id = DataHandleId::new(552);
    let mut fs_core = fs_core_with_mount(mount_id, 9, group_id);
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
    let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
    let mount_id = MountId::new(56);
    let group_id = ShardGroupId::new(14);
    let parent_inode_id = InodeId::new(560);
    let source_inode_id = InodeId::new(561);
    let target_inode_id = InodeId::new(562);
    let source_handle = DataHandleId::new(563);
    let target_handle = DataHandleId::new(564);
    let mut fs_core = fs_core_with_mount(mount_id, 9, group_id);
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
    let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
    let mount_id = MountId::new(59);
    let group_id = ShardGroupId::new(16);
    let parent_inode_id = InodeId::new(590);
    let source_inode_id = InodeId::new(591);
    let target_inode_id = InodeId::new(592);
    let source_handle = DataHandleId::new(593);
    let target_handle = DataHandleId::new(594);
    let mut fs_core = fs_core_with_mount(mount_id, 9, group_id);
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
    let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
    let src_mount_id = MountId::new(57);
    let dst_mount_id = MountId::new(58);
    let src_parent_inode_id = InodeId::new(570);
    let dst_parent_inode_id = InodeId::new(580);
    let source_inode_id = InodeId::new(571);
    let mut fs_core = fs_core_with_mount(src_mount_id, 9, ShardGroupId::new(15));
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
    let fs_core = fs_core_with_mount(MountId::new(55), 9, ShardGroupId::new(13));

    let failure = fs_core
        .execute_close_write(CloseWriteInput {
            ctx: request_context(),
            file_handle: 999_999,
            lease_id: Some(types::ids::LeaseId::new(1)),
            lease_epoch: 1,
            open_epoch: 1,
            fencing_token: Some(PresentedFencingToken {
                block_id: Some(BlockId::new(DataHandleId::new(1), BlockIndex::new(0))),
                owner: 7,
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
}

#[tokio::test]
async fn create_mount_route_epoch_progression_rejects_stale_client_route_epoch() {
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
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
            namespace_owner_group_id: ShardGroupId::new(6),
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
                worker_epoch: None,
            },
            Some(6),
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
    let storage = Arc::new(RocksDBStorage::open(dir.path()).unwrap());
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
            namespace_owner_group_id: ShardGroupId::new(8),
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
                worker_epoch: None,
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
