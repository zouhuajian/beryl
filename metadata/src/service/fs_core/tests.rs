// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use super::*;
use crate::mount::{DataIoPolicy, MountEntry, MountKind, ROOT_INODE_ID};
use crate::raft::{AppRaftStateMachine, Command, DedupKey, RocksDBStorage};
use crate::service::domain::{Freshness, ReleaseSessionInput, RequestContext};
use crate::state::{BlockMetaState, LeaseState, MemoryStateStore, RouteEpoch};
use async_trait::async_trait;
use common::error::canonical::{ErrorCode as CanonicalErrorCode, RefreshReason};
use common::header::{AuthnType, RequestHeader, RpcErrorCode};
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use types::block::{BlockPlacement, BlockState};
use types::fs::{FileAttrs, Inode};
use types::ids::{BlockId, BlockIndex, ClientId, DataHandleId, MountId, ShardGroupId};
use types::layout::FileLayout;
use types::lease::FencingToken;

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

fn unsupported_test_store_op<T>() -> MetadataResult<T> {
    Err(MetadataError::Internal(
        "storage-backed route_epoch test store only supports route_epoch".to_string(),
    ))
}

#[async_trait]
impl StateStore for StorageBackedRouteEpochStore {
    async fn get_block(&self, _block_id: BlockId) -> MetadataResult<Option<BlockMetaState>> {
        unsupported_test_store_op()
    }

    async fn create_block(
        &self,
        _inode_id: InodeId,
        _block_id: BlockId,
        _placement: BlockPlacement,
    ) -> MetadataResult<BlockMetaState> {
        unsupported_test_store_op()
    }

    async fn update_block_state(&self, _block_id: BlockId, _state: BlockState) -> MetadataResult<()> {
        unsupported_test_store_op()
    }

    async fn get_lease(&self, _block_id: BlockId) -> MetadataResult<Option<LeaseState>> {
        unsupported_test_store_op()
    }

    async fn acquire_lease(
        &self,
        _block_id: BlockId,
        _client_id: ClientId,
        _epoch: u64,
        _expires_at_ms: u64,
    ) -> MetadataResult<LeaseState> {
        unsupported_test_store_op()
    }

    async fn release_lease(&self, _block_id: BlockId) -> MetadataResult<()> {
        unsupported_test_store_op()
    }

    async fn get_inode(&self, _inode_id: InodeId) -> MetadataResult<Option<Inode>> {
        unsupported_test_store_op()
    }

    async fn get_layout(&self, _inode_id: InodeId) -> MetadataResult<FileLayout> {
        unsupported_test_store_op()
    }

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
            config_version: mount_epoch,
            namespace_owner_group_id: group_id,
            root_inode_id: ROOT_INODE_ID,
        })
        .unwrap();
    FsCore::new_default(Arc::new(MemoryStateStore::new()), mount_table)
}

fn install_write_session(fs_core: &FsCore, inode_id: InodeId, mount_id: MountId) -> u64 {
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
    fs_core.write_session_manager_for_test().create_session(
        inode_id,
        mount_id,
        lease_id,
        lease_epoch,
        FencingToken {
            block_id: BlockId::new(DataHandleId::new(inode_id.as_raw()), BlockIndex::new(0)),
            owner: writer,
            epoch: lease_epoch,
        },
        1234,
        0,
        crate::inode_lease::WriteMode::Write,
        Vec::new(),
        crate::write_session::WriterIdentity {
            client_id: writer,
            call_id: types::CallId::new(),
        },
    )
}

#[test]
fn validate_mount_freshness_returns_mount_epoch_need_refresh_without_replay_suffix() {
    let mount_id = MountId::new(11);
    let group_id = ShardGroupId::new(3);
    let fs_core = fs_core_with_mount(mount_id, 9, group_id);
    let ctx = request_context();

    let failure = fs_core
        .validate_mount_freshness(
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
    assert_eq!(failure.error.message, "mount_epoch mismatch: client=4, server=9");
    let hint = failure.error.refresh_hint.expect("refresh hint");
    assert_eq!(hint.group_id, Some(group_id.as_raw()));
    assert_eq!(hint.mount_epoch, Some(9));
    assert_eq!(failure.group_id, Some(group_id.as_raw()));
    assert_eq!(failure.mount_epoch, Some(9));
    assert_eq!(failure.route_epoch, None);
}

#[tokio::test]
async fn write_session_coordinator_release_cleans_up_runtime_state() {
    let mount_id = MountId::new(41);
    let group_id = ShardGroupId::new(4);
    let inode_id = InodeId::new(410);
    let fs_core = fs_core_with_mount(mount_id, 9, group_id);
    let file_handle = install_write_session(&fs_core, inode_id, mount_id);

    let success = fs_core
        .execute_release(ReleaseSessionInput {
            ctx: request_context(),
            file_handle,
        })
        .await
        .expect("release succeeds");

    assert!(fs_core.write_session_for_handle(file_handle).is_none());
    assert!(fs_core
        .inode_lease_manager_for_test()
        .get_active_lease(inode_id)
        .is_none());
    assert_eq!(success.mount_epoch, Some(9));
    assert_eq!(success.group_id, Some(group_id.as_raw()));
}

#[tokio::test]
async fn fs_core_release_facade_remains_idempotent_for_missing_session() {
    let mount_id = MountId::new(42);
    let group_id = ShardGroupId::new(5);
    let fs_core = fs_core_with_mount(mount_id, 9, group_id);

    let success = fs_core
        .execute_release(ReleaseSessionInput {
            ctx: request_context(),
            file_handle: 999,
        })
        .await
        .expect("release succeeds");

    assert_eq!(success.group_id, None);
    assert_eq!(success.mount_epoch, None);
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
        .apply(
            Command::CreateMount {
                dedup: DedupKey::system(),
                mount_id,
                mount_prefix: "/mnt/route".to_string(),
                mount_kind: MountKind::External,
                ufs_uri: Some("ufs://route".to_string()),
                data_io_policy: DataIoPolicy::Allow,
                namespace_owner_group_id: ShardGroupId::new(6),
                root_inode_id,
            },
            1,
        )
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
            "OpenWriteByPath",
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
        .apply(
            Command::CreateMount {
                dedup: DedupKey::system(),
                mount_id,
                mount_prefix: "/mnt/delete-route".to_string(),
                mount_kind: MountKind::External,
                ufs_uri: Some("ufs://delete-route".to_string()),
                data_io_policy: DataIoPolicy::Allow,
                namespace_owner_group_id: ShardGroupId::new(8),
                root_inode_id,
            },
            1,
        )
        .unwrap();

    let stale_route_epoch = storage.get_route_epoch().unwrap().as_u64();
    state_machine
        .apply(
            Command::DeleteMount {
                dedup: DedupKey::system(),
                mount_id,
            },
            2,
        )
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
