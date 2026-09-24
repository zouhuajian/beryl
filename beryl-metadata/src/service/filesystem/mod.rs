// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Filesystem semantics shared by metadata RPC handlers.

mod command;
mod guard;
mod namespace;
mod publish;
mod read;
mod write;

use crate::error::{to_rpc_error, MetadataError};
use crate::mount::MountEntry;
use crate::mount::MountTable;
use crate::path_resolver::{PathResolver, ResolvedPath};
use crate::raft::{AppRaftNode, RocksDBStorage};
use crate::readiness::RootReadinessGate;
use crate::session_registry::SessionRegistry;
use crate::worker::WorkerManager;
use beryl_common::error::rpc::{ErrorKind, RefreshHint, RpcErrorDetail};
use beryl_common::header::RequestHeader;
use beryl_types::{GroupName, GroupStateWatermark, WriteHandle};
use std::sync::Arc;
use tokio::sync::RwLock;

pub(super) use namespace::{CreateDirectoryArgs, DeleteArgs, RenameArgs};
pub(super) use read::{BlockLocationsTarget, GetBlockLocationsArgs, ListStatusArgs};
pub(super) use write::{AllocateBlockArgs, AuthorizeBlockWriteArgs, OpenWriteArgs};

#[derive(Clone, Copy, Debug)]
pub(crate) struct FileRange {
    pub(crate) offset: u64,
    pub(crate) len: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct FsSuccess<T> {
    pub(crate) payload: T,
    pub(crate) group_name: Option<GroupName>,
    pub(crate) state: Option<GroupStateWatermark>,
}

#[derive(Clone, Debug)]
pub(crate) struct FsFailure {
    pub(crate) error: Box<RpcErrorDetail>,
    pub(crate) group_name: Option<GroupName>,
}

impl FsFailure {
    fn new(error: RpcErrorDetail, group_name: Option<GroupName>) -> Self {
        Self {
            error: Box::new(error),
            group_name,
        }
    }
}

pub(crate) type FsResult<T> = Result<FsSuccess<T>, FsFailure>;

fn fs_failure_from_metadata_error(ctx: &RequestHeader, err: MetadataError, group_name: Option<GroupName>) -> FsFailure {
    fs_failure_from_rpc_error(ctx, to_rpc_error(err), group_name)
}

fn fs_failure_from_rpc_error(ctx: &RequestHeader, err: RpcErrorDetail, group_name: Option<GroupName>) -> FsFailure {
    let group_name = group_name.or_else(|| ctx.group_name.clone());
    FsFailure::new(err, group_name)
}

fn refresh_metadata_fs_failure(
    ctx: &RequestHeader,
    kind: ErrorKind,
    message: impl Into<String>,
    group_name: Option<GroupName>,
    hint: Option<RefreshHint>,
) -> FsFailure {
    let err = RpcErrorDetail::refresh_metadata(kind, hint.unwrap_or_default(), message);
    fs_failure_from_rpc_error(ctx, err, group_name)
}

fn missing_resolved_target_error(resolved: &ResolvedPath) -> MetadataError {
    let message = match (resolved.parent_inode_id, resolved.name.as_deref()) {
        (Some(parent_inode_id), Some(name)) => {
            format!("Entry not found: {} (parent inode: {})", name, parent_inode_id)
        }
        _ => "resolved path has no target".to_string(),
    };
    MetadataError::NotFound(message)
}

pub(crate) struct MetadataFileSystemDeps {
    pub(crate) mount_table: Arc<MountTable>,
    pub(crate) storage: Arc<RocksDBStorage>,
    pub(crate) raft_node: Arc<AppRaftNode>,
    pub(crate) session_registry: Arc<SessionRegistry>,
    pub(crate) worker_manager: Arc<WorkerManager>,
    pub(crate) readiness_gate: Arc<RootReadinessGate>,
    /// Validated server-owned block capacity used by atomic CreateFile.
    pub(crate) file_block_size: u32,
}

/// Metadata service state combining durable Raft authority with leader-local admission state.
pub(crate) struct MetadataFileSystem {
    path_resolver: PathResolver,
    /// Serializes path-bound write admission with topology-changing operations.
    ///
    /// Create/OpenWrite/RenewLease take a shared guard; Rename/Delete take an
    /// exclusive guard. This lock is leader-local admission only: Raft apply
    /// preconditions and persisted fencing epochs provide replay-safe durable
    /// authority, while OpenWrite revalidates its path before replying.
    namespace_topology: RwLock<()>,
    readiness_gate: Arc<RootReadinessGate>,
    mount_table: Arc<MountTable>,
    storage: Arc<RocksDBStorage>,
    raft_node: Arc<AppRaftNode>,
    session_registry: Arc<SessionRegistry>,
    worker_manager: Arc<WorkerManager>,
    file_block_size: u32,
}

impl MetadataFileSystem {
    pub(crate) fn new(deps: MetadataFileSystemDeps) -> Self {
        let path_resolver = PathResolver::new(Arc::clone(&deps.mount_table), Arc::clone(&deps.storage));

        Self {
            path_resolver,
            namespace_topology: RwLock::new(()),
            readiness_gate: deps.readiness_gate,
            mount_table: deps.mount_table,
            storage: deps.storage,
            raft_node: deps.raft_node,
            session_registry: deps.session_registry,
            worker_manager: deps.worker_manager,
            file_block_size: deps.file_block_size,
        }
    }

    fn response_state_for_success(&self, group_name: Option<&GroupName>) -> Option<GroupStateWatermark> {
        let Some(group_name) = group_name else {
            // A response without a known owner group cannot authorize a state cache advance.
            return None;
        };
        if !self.raft_node.is_leader() {
            return None;
        }
        self.raft_node
            .get_last_applied_state_id()
            .map(|state_id| GroupStateWatermark::new(group_name.clone(), state_id))
    }

    fn success<T>(&self, payload: T, group_name: Option<GroupName>) -> FsResult<T> {
        Ok(FsSuccess {
            payload,
            group_name: group_name.clone(),
            state: self.response_state_for_success(group_name.as_ref()),
        })
    }

    fn failure_from_error(&self, ctx: &RequestHeader, err: MetadataError, group_name: Option<GroupName>) -> FsFailure {
        fs_failure_from_metadata_error(ctx, err, group_name)
    }

    fn failure_from_path_error(&self, ctx: &RequestHeader, path: &str, err: MetadataError) -> FsFailure {
        let mount_ctx = PathResolver::normalize(path)
            .and_then(|normalized| self.path_resolver.resolve_mount_components(&normalized))
            .ok()
            .map(|(mount_ctx, _)| mount_ctx);
        self.failure_from_resolved_path_error(ctx, err, mount_ctx.as_ref())
    }

    fn failure_from_resolved_path_error(
        &self,
        ctx: &RequestHeader,
        err: MetadataError,
        mount_ctx: Option<&MountEntry>,
    ) -> FsFailure {
        let group_name = mount_ctx.map(|mount| mount.namespace_owner_group_name.clone());
        self.failure_from_error(ctx, err, group_name)
    }

    fn require_worker_lookup_group(
        &self,
        ctx: &RequestHeader,
        group_name: Option<GroupName>,
        intent: &str,
    ) -> Result<GroupName, FsFailure> {
        group_name.clone().ok_or_else(|| {
            fs_failure_from_metadata_error(
                ctx,
                MetadataError::Internal(format!("{intent} worker lookup requires authoritative metadata group")),
                group_name,
            )
        })
    }

    fn refresh_metadata_failure(
        &self,
        ctx: &RequestHeader,
        kind: ErrorKind,
        message: impl Into<String>,
        group_name: Option<GroupName>,
    ) -> FsFailure {
        let hint = RefreshHint {
            group_name: group_name.as_ref().map(ToString::to_string),
            leader_endpoint: None,
        };
        refresh_metadata_fs_failure(ctx, kind, message, group_name, Some(hint))
    }

    fn session_terminal_failure(
        &self,
        ctx: &RequestHeader,
        kind: ErrorKind,
        message: impl Into<String>,
        group_name: Option<GroupName>,
    ) -> FsFailure {
        let group_name = group_name.or_else(|| ctx.group_name.clone());
        FsFailure::new(
            RpcErrorDetail::reopen_write_session(kind, RefreshHint::default(), message),
            group_name,
        )
    }
}

#[cfg(test)]
mod tests {
    pub(super) use super::*;

    pub(super) use crate::inode::Inode;
    pub(super) use crate::inode::InodeAttrs;
    use crate::inode::InodeKind;
    pub(super) use crate::mount::{MountEntry, ROOT_INODE_ID};
    use crate::raft::PublishMode;
    pub(super) use crate::raft::{AppRaftNode, AppRaftStateMachine, RocksDBStorage};
    pub(super) use crate::service::filesystem::write::OpenWriteOutput;
    use crate::session_registry::{BeginAllocateBlock, BeginSessionInput, WriteSession};
    pub(super) use crate::worker::{BlockReportBlock, BlockReportBlockState, WorkerManager};
    pub(super) use beryl_common::error::rpc::{
        ErrorKind, MetadataErrorKind, RecoveryAction, RpcErrorDetail, WorkerErrorKind,
    };
    pub(super) use beryl_common::header::RequestHeader;
    pub(super) use beryl_types::ids::{BlockId, BlockIndex, ClientId, InodeId, MountId, WorkerId};

    pub(super) use beryl_types::lease::FencingToken;
    pub(super) use beryl_types::{CommittedBlock, GroupName, LocatedBlock, Tier, TierFree, WorkerRunId};
    use beryl_types::{ContentGeneration, LeaseEpoch, WriteMode};
    use std::ops::Deref;
    pub(super) use std::sync::Arc;
    pub(super) use std::time::Duration;
    pub(super) use tempfile::TempDir;

    pub(super) struct TestFilesystem {
        filesystem: MetadataFileSystem,
        session_registry: Arc<SessionRegistry>,
        _storage_dir: Option<TempDir>,
    }

    impl Deref for TestFilesystem {
        type Target = MetadataFileSystem;

        fn deref(&self) -> &Self::Target {
            &self.filesystem
        }
    }

    impl TestFilesystem {
        pub(super) fn write_session_for_inode(&self, inode_id: InodeId) -> Option<WriteSession> {
            self.session_registry.get_session(inode_id)
        }

        pub(super) fn session_registry(&self) -> Arc<SessionRegistry> {
            Arc::clone(&self.session_registry)
        }

        pub(super) fn raft_node(&self) -> Arc<AppRaftNode> {
            Arc::clone(&self.filesystem.raft_node)
        }
    }

    pub(super) struct TestFilesystemBuilder {
        mount_table: Arc<MountTable>,
        storage: Option<Arc<RocksDBStorage>>,
        raft_node: Option<Arc<AppRaftNode>>,
        session_registry: Option<Arc<SessionRegistry>>,
        worker_manager: Option<Arc<WorkerManager>>,
    }

    impl TestFilesystemBuilder {
        fn new(mount_table: Arc<MountTable>) -> Self {
            Self {
                mount_table,
                storage: None,
                raft_node: None,
                session_registry: None,
                worker_manager: None,
            }
        }

        pub(super) fn with_storage(mut self, storage: Arc<RocksDBStorage>) -> Self {
            self.storage = Some(storage);
            self
        }

        pub(super) fn mount_table(&self) -> Arc<MountTable> {
            Arc::clone(&self.mount_table)
        }

        pub(super) fn with_raft_node(mut self, raft_node: Arc<AppRaftNode>) -> Self {
            self.raft_node = Some(raft_node);
            self
        }

        pub(super) fn with_worker_manager(mut self, worker_manager: Arc<WorkerManager>) -> Self {
            self.worker_manager = Some(worker_manager);
            self
        }

        pub(super) fn with_session_registry(mut self, session_registry: Arc<SessionRegistry>) -> Self {
            self.session_registry = Some(session_registry);
            self
        }

        pub(super) async fn build(self) -> TestFilesystem {
            let (storage, storage_dir) = match self.storage {
                Some(storage) => (storage, None),
                None => {
                    let storage_dir = TempDir::new().unwrap();
                    let storage = Arc::new(RocksDBStorage::create_for_format(storage_dir.path()).unwrap());
                    (storage, Some(storage_dir))
                }
            };
            let raft_node = match self.raft_node {
                Some(raft_node) => raft_node,
                None => {
                    single_node_raft(Arc::clone(&storage), Arc::clone(&self.mount_table))
                        .await
                        .0
                }
            };
            let session_registry = self
                .session_registry
                .unwrap_or_else(|| Arc::new(SessionRegistry::default()));
            let filesystem = MetadataFileSystem::new(MetadataFileSystemDeps {
                mount_table: self.mount_table,
                storage,
                raft_node,
                session_registry: Arc::clone(&session_registry),
                worker_manager: self
                    .worker_manager
                    .unwrap_or_else(|| Arc::new(WorkerManager::new(60_000))),
                readiness_gate: Arc::new(RootReadinessGate::new()),
                file_block_size: crate::config::MetadataConfig::default().file_block_size,
            });

            TestFilesystem {
                filesystem,
                session_registry,
                _storage_dir: storage_dir,
            }
        }
    }

    pub(super) fn request_context() -> RequestHeader {
        RequestHeader::new(ClientId::new(7))
    }

    #[test]
    fn failure_without_resolved_authority_preserves_requested_group_identity() {
        let group_name = group_name("requested");
        let ctx = RequestHeader::new(ClientId::new(7)).with_group_name(group_name.clone());

        let failure = fs_failure_from_metadata_error(&ctx, MetadataError::NotFound("missing inode".to_string()), None);

        assert_eq!(failure.group_name, Some(group_name));
    }

    pub(super) fn group_name(raw: &str) -> GroupName {
        GroupName::parse(raw).unwrap()
    }

    pub(super) fn filesystem_builder_with_mount(mount_id: MountId, group_name: &GroupName) -> TestFilesystemBuilder {
        let mount_table = Arc::new(MountTable::default());
        mount_table.upsert(MountEntry {
            mount_id,
            namespace_owner_group_name: group_name.clone(),
            root_inode_id: ROOT_INODE_ID,
        });
        TestFilesystemBuilder::new(mount_table)
    }

    pub(super) fn worker_run_id(group_name: &GroupName, worker_id: WorkerId) -> WorkerRunId {
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

    pub(super) fn register_worker_descriptor(
        manager: &WorkerManager,
        group_name: &GroupName,
        worker_id: WorkerId,
        address: String,
    ) {
        manager.register_worker_run(group_name, worker_id, address, worker_run_id(group_name, worker_id));
    }

    pub(super) fn record_worker_heartbeat(
        manager: &WorkerManager,
        group_name: &GroupName,
        worker_id: WorkerId,
        free_bytes: u64,
    ) {
        let descriptor = manager
            .collect_worker_placement_views(group_name)
            .into_iter()
            .find(|view| view.worker_id == worker_id)
            .expect("worker descriptor should be registered");
        let run_id = manager.get_registered_run(group_name, worker_id).unwrap_or_else(|| {
            let run_id = worker_run_id(group_name, worker_id);
            manager.register_worker_run(group_name, worker_id, descriptor.endpoint.clone(), run_id);
            run_id
        });
        manager
            .record_heartbeat_with_tier_free(
                group_name,
                worker_id,
                run_id,
                1,
                &descriptor.endpoint,
                vec![TierFree {
                    tier: Tier::Hdd,
                    free_bytes,
                }],
            )
            .expect("heartbeat should be accepted");
    }

    pub(super) fn report_block(block_id: BlockId) -> BlockReportBlock {
        report_block_with_epoch(block_id, 1)
    }

    pub(super) fn report_block_with_epoch(block_id: BlockId, lease_epoch: u64) -> BlockReportBlock {
        report_block_with_epoch_and_len(block_id, lease_epoch, 64)
    }

    pub(super) fn report_block_with_epoch_and_len(
        block_id: BlockId,
        lease_epoch: u64,
        effective_len: u64,
    ) -> BlockReportBlock {
        BlockReportBlock {
            tier: Some(beryl_types::Tier::Hdd),
            block_id,
            lease_epoch,
            block_state: BlockReportBlockState::Ready,
            effective_len,
        }
    }

    pub(super) fn report_block_with_epoch_and_state(
        block_id: BlockId,
        lease_epoch: u64,
        block_state: BlockReportBlockState,
    ) -> BlockReportBlock {
        BlockReportBlock {
            tier: Some(beryl_types::Tier::Hdd),
            block_id,
            lease_epoch,
            block_state,
            effective_len: if block_state == BlockReportBlockState::Ready {
                64
            } else {
                0
            },
        }
    }

    pub(super) fn publish_report_locations_with_epoch(
        manager: &WorkerManager,
        group_name: &GroupName,
        worker_id: WorkerId,
        report_seq: u64,
        lease_epoch: Option<u64>,
        blocks: Vec<BlockId>,
    ) {
        let run_id = manager
            .get_registered_run(group_name, worker_id)
            .expect("worker registration");
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
                        lease_epoch
                            .map(|epoch| report_block_with_epoch(block_id, epoch))
                            .unwrap_or_else(|| report_block(block_id))
                    })
                    .collect(),
            )
            .expect("full block report should publish locations");
    }

    pub(super) fn publish_report_block(
        manager: &WorkerManager,
        group_name: &GroupName,
        worker_id: WorkerId,
        report_seq: u64,
        block: BlockReportBlock,
    ) {
        let run_id = manager
            .get_registered_run(group_name, worker_id)
            .expect("worker registration");
        manager
            .receive_full_block_report(group_name, worker_id, run_id, report_seq, 0, true, vec![block])
            .expect("full block report should publish locations");
    }

    pub(super) fn worker_manager_for_write_targets(group_name: &GroupName) -> Arc<WorkerManager> {
        let manager = Arc::new(WorkerManager::new(60_000));
        for raw in 1..=3 {
            let worker_id = WorkerId::new(raw);
            register_worker_descriptor(&manager, group_name, worker_id, format!("127.0.0.1:{}", 9000 + raw));
            record_worker_heartbeat(&manager, group_name, worker_id, 1024 * 1024);
        }
        manager
    }

    pub(super) fn assert_block_location_unavailable(failure: &FsFailure, block_id: BlockId) {
        assert_refresh_metadata(
            &failure.error,
            ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
        );
        assert!(
            failure.error.message.contains(&block_id.to_string()),
            "error should include block id context: {}",
            failure.error.message
        );
    }

    pub(super) fn assert_fail(error: &RpcErrorDetail, kind: ErrorKind) {
        assert_eq!(error.kind, kind);
        assert_eq!(error.recovery, RecoveryAction::Fail);
    }

    pub(super) fn assert_retry(error: &RpcErrorDetail, kind: ErrorKind) {
        assert_eq!(error.kind, kind);
        assert!(matches!(error.recovery, RecoveryAction::Retry { .. }));
    }

    pub(super) fn assert_refresh_metadata(error: &RpcErrorDetail, kind: ErrorKind) {
        assert_eq!(error.kind, kind);
        assert!(matches!(error.recovery, RecoveryAction::RefreshMetadata { .. }));
    }

    pub(super) fn install_write_session_with_ancestors(
        filesystem: &TestFilesystem,
        inode_id: InodeId,
        mount_id: MountId,
        ancestor_inode_ids: Vec<InodeId>,
    ) {
        let writer = ClientId::new(7);
        let lease_epoch = LeaseEpoch::new(1);
        let block_id = BlockId::new(inode_id, BlockIndex::new(0));
        let target = LocatedBlock {
            write_offset: 0,
            block_id,
            file_offset: 0,
            block_size: 64,
            workers: Vec::new(),
            fencing_token: FencingToken {
                owner: writer,
                epoch: lease_epoch,
            },

            tier: Tier::Hdd,
        };
        let session_registry = filesystem.session_registry();
        let opening = session_registry
            .begin_session(BeginSessionInput {
                normalized_path: "/file".to_string(),
                inode_id,
                mount_id,
                current_lease_epoch: LeaseEpoch::new(0),
                mode: WriteMode::Overwrite,
                open_client_id: writer,
                block_size: 64,
                ancestor_inode_ids,
            })
            .expect("session capacity");
        let file = crate::inode::FileData {
            block_size: 64,
            len: 0,
            generation: ContentGeneration::default(),
            blocks: Vec::new(),
            next_index: 0,
            lease_epoch,
            last_commit: None,
        };
        opening.activate(&file, None).expect("session created");
        let target_reservation = match session_registry
            .begin_allocate_block(inode_id, lease_epoch, None)
            .expect("target capacity")
        {
            BeginAllocateBlock::Reserved(reservation) => reservation,
            BeginAllocateBlock::Replay(_) => panic!("new target must reserve capacity"),
        };
        target_reservation.complete(target).expect("target installed");
    }

    pub(super) fn committed_block(block_id: BlockId, len: u64) -> CommittedBlock {
        CommittedBlock { block_id, len }
    }

    pub(super) async fn allocate_block_for_key(filesystem: &MetadataFileSystem, key: &OpenWriteOutput) -> LocatedBlock {
        let previous_block_id = filesystem
            .session_registry
            .get_session(key.inode_id)
            .and_then(|session| session.issued_targets.last().map(|target| target.block_id));
        filesystem
            .allocate_block_session(&request_context(), key.inode_id, key.lease_epoch, previous_block_id)
            .await
            .expect("AllocateBlock should succeed")
            .payload
    }

    pub(super) async fn commit_for_key(
        filesystem: &MetadataFileSystem,
        key: &OpenWriteOutput,
        committed_blocks: Vec<CommittedBlock>,
        final_len: u64,
    ) -> FsResult<u64> {
        filesystem
            .close_write_session(
                &request_context(),
                key.inode_id,
                crate::inode::FilePublication {
                    blocks: committed_blocks,
                    target_len: final_len,
                    expected_file_len: key.base_len,
                    expected_generation: key.generation,
                    lease_epoch: key.lease_epoch,
                    mode: match filesystem
                        .session_registry
                        .get_session(key.inode_id)
                        .expect("active write session")
                        .mode
                    {
                        WriteMode::Overwrite => PublishMode::ReplaceIfUnchanged,
                        WriteMode::Append => PublishMode::AppendIfUnchanged,
                    },
                },
            )
            .await
    }

    pub(super) struct WriteFlowEnv {
        pub(super) _dir: TempDir,
        pub(super) storage: Arc<RocksDBStorage>,
        pub(super) filesystem: TestFilesystem,
        pub(super) inode_id: InodeId,
        pub(super) group_name: GroupName,
    }

    pub(super) async fn write_flow_env(base_len: u64) -> WriteFlowEnv {
        build_write_flow_env(base_len, worker_manager_for_write_targets).await
    }

    async fn build_write_flow_env(
        base_len: u64,
        worker_manager: impl FnOnce(&GroupName) -> Arc<WorkerManager>,
    ) -> WriteFlowEnv {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(1);
        let group_name = group_name(&format!("g{}", 15 + base_len));
        let inode_id = InodeId::new(9570 + base_len);
        let builder = filesystem_builder_with_mount(mount_id, &group_name);
        let mount_table = builder.mount_table();
        let (raft_node, _state_machine) = single_node_raft(Arc::clone(&storage), mount_table).await;
        let filesystem = builder
            .with_storage(Arc::clone(&storage))
            .with_raft_node(raft_node)
            .with_worker_manager(worker_manager(&group_name))
            .build()
            .await;

        let attrs = InodeAttrs::new();
        let mut inode = Inode::new_file(inode_id, attrs, mount_id, 64);
        let file = inode.file_mut().unwrap();
        file.len = base_len;
        let count = crate::inode::FileData::block_count(base_len, 64).unwrap();
        file.blocks = (0..count)
            .map(|index| BlockId::new(inode_id, beryl_types::BlockIndex::new(index as u32)))
            .collect();
        file.next_index = count as u64;
        storage.put_inode(&inode).unwrap();

        WriteFlowEnv {
            _dir: dir,
            storage,
            filesystem,
            inode_id,
            group_name,
        }
    }

    pub(super) fn publish_env_write_target(env: &WriteFlowEnv, target: &LocatedBlock, report_seq: u64) {
        publish_env_write_target_with_len(env, target, report_seq, 64);
    }

    pub(super) fn publish_env_write_target_with_len(
        env: &WriteFlowEnv,
        target: &LocatedBlock,
        report_seq: u64,
        effective_len: u64,
    ) {
        let worker = target.workers.first().expect("write target worker");
        let worker_manager = &env.filesystem.worker_manager;
        publish_report_block(
            worker_manager,
            &env.group_name,
            worker.worker_id,
            report_seq,
            report_block_with_epoch_and_len(target.block_id, target.fencing_token.epoch.as_raw(), effective_len),
        );
    }

    pub(super) fn stored_generation(storage: &RocksDBStorage, inode_id: InodeId) -> ContentGeneration {
        let inode = storage.get_inode(inode_id).unwrap().expect("test inode should exist");
        match inode.kind {
            InodeKind::File(crate::inode::FileData { generation, .. }) => generation,
            other => panic!("unexpected inode data: {:?}", other),
        }
    }

    pub(super) async fn single_node_raft(
        storage: Arc<RocksDBStorage>,
        mount_table: Arc<MountTable>,
    ) -> (Arc<AppRaftNode>, AppRaftStateMachine) {
        if let Some(mount) = mount_table.root() {
            storage.put_mount(&mount).unwrap();
        }
        let state_machine = AppRaftStateMachine::new(Arc::clone(&storage));
        let raft_node = Arc::new(
            AppRaftNode::new(1, Arc::clone(&storage), AppRaftStateMachine::new(storage), mount_table)
                .await
                .unwrap(),
        );
        raft_node
            .initialize_single_node("127.0.0.1:0".to_string())
            .await
            .unwrap();
        (raft_node, state_machine)
    }
}
