// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Filesystem semantics shared by metadata RPC handlers.

mod command;
mod guard;
mod namespace;
mod publish;
mod read;
mod write;

use crate::error::{to_fs_error_detail, MetadataError, MetadataResult};
use crate::inode_lease::LeaseManager;
use crate::metrics::MetadataMetrics;
use crate::mount::MountTable;
use crate::path_resolver::{MountContext, PathResolver, ResolvedPath};
use crate::raft::{AppRaftNode, RocksDBStorage};
use crate::readiness::RootReadinessGate;
use crate::session_registry::SessionRegistry;
use crate::state::StateStore;
use crate::worker::WorkerManager;
use beryl_common::error::rpc::{ErrorKind, RefreshHint, RpcErrorDetail};
use beryl_common::header::RequestHeader;
use beryl_types::fs::{FsErrorCode, InodeId};
use beryl_types::ids::{DataHandleId, WorkerId};
use beryl_types::{GroupName, GroupStateWatermark, WorkerEndpointInfo};
use std::sync::Arc;

use command::RoutedFsWriteCtx;
use guard::{AdmissionFailure, AdmissionGuard, FreshnessValidator, StaleStateStatus};
pub(super) use namespace::{CreateDirectoryArgs, CreateFileArgs, DeleteArgs, RenameArgs};
pub(super) use publish::{CommitFileArgs, SyncWriteArgs};
pub(super) use read::{BlockLocationsTarget, GetBlockLocationsArgs, GetStatusArgs, ListStatusArgs, OpenFileArgs};
pub(super) use write::{AbortFileWriteArgs, AddBlockArgs, OpenWriteArgs, RenewLeaseArgs};

#[derive(Clone, Debug)]
pub(crate) struct RequestContext {
    pub(crate) caller: RequestHeader,
    pub(crate) route_epoch: Option<u64>,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Freshness {
    pub(crate) mount_epoch: Option<u64>,
    pub(crate) route_epoch: Option<u64>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct FileRange {
    pub(crate) offset: u64,
    pub(crate) len: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct FsSuccess<T> {
    pub(crate) payload: T,
    pub(crate) group_name: Option<GroupName>,
    pub(crate) mount_epoch: Option<u64>,
    pub(crate) route_epoch: Option<u64>,
    pub(crate) state: Vec<GroupStateWatermark>,
}

#[derive(Clone, Debug)]
pub(crate) struct FsFailure {
    pub(crate) error: Box<RpcErrorDetail>,
    pub(crate) group_name: Option<GroupName>,
    pub(crate) mount_epoch: Option<u64>,
    pub(crate) route_epoch: Option<u64>,
    pub(crate) state: Vec<GroupStateWatermark>,
}

impl FsFailure {
    fn new(
        error: RpcErrorDetail,
        group_name: Option<GroupName>,
        mount_epoch: Option<u64>,
        route_epoch: Option<u64>,
        state: Vec<GroupStateWatermark>,
    ) -> Self {
        Self {
            error: Box::new(error),
            group_name,
            mount_epoch,
            route_epoch,
            state,
        }
    }
}

pub(crate) type FsResult<T> = Result<FsSuccess<T>, FsFailure>;

fn fs_failure_from_metadata_error(
    ctx: &RequestContext,
    err: MetadataError,
    group_name: Option<GroupName>,
    mount_epoch: Option<u64>,
    route_epoch: Option<u64>,
) -> FsFailure {
    fs_failure_from_rpc_error(ctx, to_fs_error_detail(err), group_name, mount_epoch, route_epoch)
}

fn fs_failure_from_rpc_error(
    _ctx: &RequestContext,
    err: RpcErrorDetail,
    group_name: Option<GroupName>,
    mount_epoch: Option<u64>,
    route_epoch: Option<u64>,
) -> FsFailure {
    FsFailure::new(err, group_name, mount_epoch, route_epoch, Vec::new())
}

#[allow(clippy::too_many_arguments)]
fn refresh_metadata_fs_failure(
    ctx: &RequestContext,
    kind: ErrorKind,
    message: impl Into<String>,
    group_name: Option<GroupName>,
    mount_epoch: Option<u64>,
    route_epoch: Option<u64>,
    hint: Option<RefreshHint>,
) -> FsFailure {
    let err = RpcErrorDetail::refresh_metadata(kind, hint.unwrap_or_default(), message);
    fs_failure_from_rpc_error(ctx, err, group_name, mount_epoch, route_epoch)
}

fn fatal_fs_failure(
    ctx: &RequestContext,
    errno: FsErrorCode,
    message: impl Into<String>,
    group_name: Option<GroupName>,
    mount_epoch: Option<u64>,
) -> FsFailure {
    fs_failure_from_rpc_error(ctx, RpcErrorDetail::fs(errno, message), group_name, mount_epoch, None)
}

fn worker_endpoint_from_parts(
    worker_id: WorkerId,
    endpoint: String,
    worker_net_protocol: i32,
    worker_run_id: beryl_types::WorkerRunId,
) -> Result<WorkerEndpointInfo, MetadataError> {
    if worker_net_protocol != 1 {
        return Err(MetadataError::InvalidArgument(format!(
            "unsupported persisted worker network protocol {worker_net_protocol}"
        )));
    }
    beryl_proto::convert::worker_endpoint_info_from_parts(worker_id, endpoint, worker_run_id.to_string())
        .map_err(MetadataError::InvalidArgument)
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

impl MetadataFileSystem {
    fn has_active_write(&self, inode_id: InodeId) -> bool {
        self.session_registry
            .remove_inactive_for_inode(inode_id, self.lease_manager.as_ref());
        self.lease_manager.has_active_lease(inode_id)
    }
}

pub(crate) struct MetadataFileSystemDeps {
    pub(crate) state_store: Arc<dyn StateStore>,
    pub(crate) mount_table: Arc<MountTable>,
    pub(crate) storage: Arc<RocksDBStorage>,
    pub(crate) raft_node: Option<Arc<AppRaftNode>>,
    pub(crate) session_registry: Arc<SessionRegistry>,
    pub(crate) lease_manager: Arc<LeaseManager>,
    pub(crate) worker_manager: Option<Arc<WorkerManager>>,
    pub(crate) metrics: Option<Arc<MetadataMetrics>>,
    pub(crate) readiness_gate: Option<Arc<RootReadinessGate>>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct PresentedWriteHandle {
    pub(crate) data_handle_id: DataHandleId,
    pub(crate) lease_epoch: u64,
}

pub(crate) struct MetadataFileSystem {
    path_resolver: PathResolver,
    admission: AdmissionGuard,
    mount_table: Arc<MountTable>,
    freshness_validator: FreshnessValidator,
    storage: Arc<RocksDBStorage>,
    raft_node: Option<Arc<AppRaftNode>>,
    metrics: Option<Arc<MetadataMetrics>>,
    session_registry: Arc<SessionRegistry>,
    worker_manager: Option<Arc<WorkerManager>>,
    lease_manager: Arc<LeaseManager>,
}

impl MetadataFileSystem {
    pub(crate) fn new(deps: MetadataFileSystemDeps) -> Self {
        let path_resolver = PathResolver::new(Arc::clone(&deps.mount_table), Arc::clone(&deps.storage));
        let admission = AdmissionGuard::new(Arc::clone(&deps.mount_table))
            .with_readiness_gate(deps.readiness_gate)
            .with_raft_node(deps.raft_node.clone());
        let freshness_validator = FreshnessValidator::new(Arc::clone(&deps.state_store), Arc::clone(&deps.mount_table));

        Self {
            path_resolver,
            admission,
            mount_table: deps.mount_table,
            freshness_validator,
            storage: deps.storage,
            raft_node: deps.raft_node,
            metrics: deps.metrics,
            session_registry: deps.session_registry,
            worker_manager: deps.worker_manager,
            lease_manager: deps.lease_manager,
        }
    }

    async fn authoritative_route_epoch(&self) -> MetadataResult<u64> {
        self.freshness_validator.authoritative_route_epoch().await
    }

    fn response_state_for_success(&self, group_name: Option<&GroupName>) -> Vec<GroupStateWatermark> {
        let (Some(group_name), Some(raft_node)) = (group_name, self.raft_node.as_ref()) else {
            // A response without a known owner group cannot authorize a state cache advance.
            return Vec::new();
        };
        if !raft_node.is_leader() {
            return Vec::new();
        }
        raft_node
            .get_last_applied_state_id()
            .map(|state_id| GroupStateWatermark::new(group_name.clone(), state_id))
            .into_iter()
            .collect()
    }

    fn success<T>(
        &self,
        ctx: &RequestContext,
        payload: T,
        group_name: Option<GroupName>,
        mount_epoch: Option<u64>,
    ) -> FsResult<T> {
        self.success_with_route_epoch(ctx, payload, group_name, mount_epoch, None)
    }

    fn success_with_route_epoch<T>(
        &self,
        _ctx: &RequestContext,
        payload: T,
        group_name: Option<GroupName>,
        mount_epoch: Option<u64>,
        route_epoch: Option<u64>,
    ) -> FsResult<T> {
        Ok(FsSuccess {
            payload,
            group_name: group_name.clone(),
            mount_epoch,
            route_epoch,
            state: self.response_state_for_success(group_name.as_ref()),
        })
    }

    fn failure_from_error<T>(
        &self,
        ctx: &RequestContext,
        err: MetadataError,
        group_name: Option<GroupName>,
        mount_epoch: Option<u64>,
    ) -> FsResult<T> {
        self.failure_from_error_with_route_epoch(ctx, err, group_name, mount_epoch, None)
    }

    fn failure_from_error_with_route_epoch<T>(
        &self,
        ctx: &RequestContext,
        err: MetadataError,
        group_name: Option<GroupName>,
        mount_epoch: Option<u64>,
        route_epoch: Option<u64>,
    ) -> FsResult<T> {
        Err(fs_failure_from_metadata_error(
            ctx,
            err,
            group_name,
            mount_epoch,
            route_epoch,
        ))
    }

    fn failure_from_admission<T>(&self, failure: AdmissionFailure) -> FsResult<T> {
        Err(FsFailure {
            error: failure.err,
            group_name: failure.group_name,
            mount_epoch: failure.mount_epoch,
            route_epoch: None,
            state: Vec::new(),
        })
    }

    fn failure_from_path_error<T>(&self, ctx: &RequestContext, path: &str, err: MetadataError) -> FsResult<T> {
        let mount_ctx = self
            .path_resolver
            .resolve_mount_components(path)
            .ok()
            .map(|(mount_ctx, _)| mount_ctx);
        self.failure_from_resolved_path_error(ctx, err, mount_ctx.as_ref())
    }

    fn failure_from_resolved_path_error<T>(
        &self,
        ctx: &RequestContext,
        err: MetadataError,
        mount_ctx: Option<&MountContext>,
    ) -> FsResult<T> {
        let (group_name, mount_epoch) = mount_ctx
            .map(|mount| (Some(mount.owner_group_name.clone()), Some(mount.mount_epoch)))
            .unwrap_or((None, None));
        self.failure_from_error(ctx, err, group_name, mount_epoch)
    }

    fn require_worker_lookup_group(
        &self,
        ctx: &RequestContext,
        group_name: Option<GroupName>,
        mount_epoch: Option<u64>,
        route_epoch: Option<u64>,
        intent: &str,
    ) -> Result<GroupName, FsFailure> {
        group_name.clone().ok_or_else(|| {
            fs_failure_from_metadata_error(
                ctx,
                MetadataError::Internal(format!("{intent} worker lookup requires authoritative metadata group")),
                group_name,
                mount_epoch,
                route_epoch,
            )
        })
    }

    // Refresh failures must keep caller and server hint fields explicit.
    #[allow(clippy::too_many_arguments)]
    fn refresh_metadata_failure_with_hint<T>(
        &self,
        ctx: &RequestContext,
        kind: ErrorKind,
        message: impl Into<String>,
        group_name: Option<GroupName>,
        mount_epoch: Option<u64>,
        route_epoch: Option<u64>,
        mut hint: Option<RefreshHint>,
    ) -> FsResult<T> {
        if let Some(group_name_value) = &group_name {
            hint.get_or_insert_with(RefreshHint::default).group_name = Some(group_name_value.to_string());
        }
        if let Some(mount_epoch_value) = mount_epoch {
            hint.get_or_insert_with(RefreshHint::default).mount_epoch = Some(mount_epoch_value);
        }
        if let Some(route_epoch_value) = route_epoch {
            hint.get_or_insert_with(RefreshHint::default).route_epoch = Some(route_epoch_value);
        }

        Err(refresh_metadata_fs_failure(
            ctx,
            kind,
            message,
            group_name.clone(),
            mount_epoch,
            route_epoch,
            hint,
        ))
    }

    fn fatal_fs_failure<T>(
        &self,
        ctx: &RequestContext,
        errno: FsErrorCode,
        message: impl Into<String>,
        group_name: Option<GroupName>,
        mount_epoch: Option<u64>,
    ) -> FsResult<T> {
        Err(fatal_fs_failure(ctx, errno, message, group_name, mount_epoch))
    }

    fn session_terminal_failure<T>(
        &self,
        ctx: &RequestContext,
        kind: ErrorKind,
        message: impl Into<String>,
        group_name: Option<GroupName>,
        mount_epoch: Option<u64>,
    ) -> FsResult<T> {
        let group_name = group_name.or_else(|| ctx.caller.group_name.clone());
        Err(FsFailure::new(
            RpcErrorDetail::reopen_write_session(kind, RefreshHint::default(), message),
            group_name,
            mount_epoch,
            None,
            Vec::new(),
        ))
    }

    fn replay_hint(intent: &str) -> String {
        format!("refresh metadata and reopen write handle, then replay {}", intent)
    }

    fn read_inode(&self, inode_id: InodeId) -> MetadataResult<Option<beryl_types::fs::Inode>> {
        self.storage.get_inode(inode_id)
    }

    fn read_dentry(&self, parent_inode_id: InodeId, name: &str) -> MetadataResult<Option<InodeId>> {
        self.storage.get_dentry(parent_inode_id, name)
    }

    fn read_layout(&self, inode_id: InodeId) -> MetadataResult<beryl_types::layout::FileLayout> {
        self.storage.get_layout(inode_id)
    }
}

fn validate_active_write_layout(layout: &beryl_types::layout::FileLayout) -> Result<(), MetadataError> {
    if layout.replication != 1 {
        return Err(MetadataError::InvalidArgument(
            "multi-replica write is not supported yet; replication must be 1".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod test_support {
    pub(super) use super::*;
    pub(super) use crate::config::RaftConfig;
    pub(super) use crate::mount::{DataIoPolicy, MountEntry, MountKind, ROOT_INODE_ID};
    pub(super) use crate::raft::{AppRaftNode, AppRaftStateMachine, RocksDBStorage};
    pub(super) use crate::service::filesystem::publish::{CloseWriteIntent, CloseWriteOutput};
    pub(super) use crate::service::filesystem::write::OpenWriteOutput;
    pub(super) use crate::state::MemoryStateStore;
    pub(super) use crate::worker::{BlockReportBlock, BlockReportBlockState, HealthStatus, WorkerManager};
    pub(super) use beryl_common::error::rpc::{
        ErrorKind, RecoveryAction, RefreshHint, RpcErrorDetail, WorkerErrorKind,
    };
    pub(super) use beryl_common::header::{CallerContext, RequestHeader};
    pub(super) use beryl_types::fs::{FileAttrs, FsErrorCode, Inode};
    pub(super) use beryl_types::ids::{BlockId, BlockIndex, ClientId, DataHandleId, MountId, WorkerId};
    pub(super) use beryl_types::layout::FileLayout;
    pub(super) use beryl_types::lease::FencingToken;
    pub(super) use beryl_types::{CommittedBlock, GroupName, Tier, TierFree, WorkerRunId, WriteTarget};
    pub(super) use std::sync::Arc;
    pub(super) use std::time::Duration;
    pub(super) use tempfile::TempDir;

    pub(super) struct TestFilesystem {
        filesystem: MetadataFileSystem,
        session_registry: Arc<SessionRegistry>,
        lease_manager: Arc<LeaseManager>,
        _storage_dir: Option<TempDir>,
    }

    impl std::ops::Deref for TestFilesystem {
        type Target = MetadataFileSystem;

        fn deref(&self) -> &Self::Target {
            &self.filesystem
        }
    }

    impl TestFilesystem {
        pub(super) fn write_session_for_handle(
            &self,
            data_handle_id: DataHandleId,
        ) -> Option<crate::session_registry::WriteSession> {
            self.session_registry.get_session(data_handle_id)
        }

        pub(super) fn session_registry(&self) -> Arc<SessionRegistry> {
            Arc::clone(&self.session_registry)
        }

        pub(super) fn lease_manager(&self) -> Arc<LeaseManager> {
            Arc::clone(&self.lease_manager)
        }
    }

    pub(super) struct TestFilesystemBuilder {
        mount_table: Arc<MountTable>,
        storage: Option<Arc<RocksDBStorage>>,
        raft_node: Option<Arc<AppRaftNode>>,
        lease_manager: Option<Arc<LeaseManager>>,
        worker_manager: Option<Arc<WorkerManager>>,
    }

    impl TestFilesystemBuilder {
        fn new(mount_table: Arc<MountTable>) -> Self {
            Self {
                mount_table,
                storage: None,
                raft_node: None,
                lease_manager: None,
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

        pub(super) fn with_lease_manager(mut self, lease_manager: Arc<LeaseManager>) -> Self {
            self.lease_manager = Some(lease_manager);
            self
        }

        pub(super) fn build(self) -> TestFilesystem {
            let (storage, storage_dir) = match self.storage {
                Some(storage) => (storage, None),
                None => {
                    let storage_dir = TempDir::new().unwrap();
                    let storage = Arc::new(RocksDBStorage::create_for_format(storage_dir.path()).unwrap());
                    (storage, Some(storage_dir))
                }
            };
            let session_registry = Arc::new(SessionRegistry::default());
            let lease_manager = self
                .lease_manager
                .unwrap_or_else(|| Arc::new(LeaseManager::default()));
            let filesystem = MetadataFileSystem::new(MetadataFileSystemDeps {
                state_store: Arc::new(MemoryStateStore::new()),
                mount_table: self.mount_table,
                storage,
                raft_node: self.raft_node,
                session_registry: Arc::clone(&session_registry),
                lease_manager: Arc::clone(&lease_manager),
                worker_manager: self.worker_manager,
                metrics: None,
                readiness_gate: None,
            });

            TestFilesystem {
                filesystem,
                session_registry,
                lease_manager,
                _storage_dir: storage_dir,
            }
        }
    }

    pub(super) fn request_context() -> RequestContext {
        RequestContext {
            caller: RequestHeader::new(ClientId::new(7)),
            route_epoch: None,
        }
    }

    pub(super) fn group_name(raw: &str) -> GroupName {
        GroupName::parse(raw).unwrap()
    }

    pub(super) fn filesystem_builder_with_mount(
        mount_id: MountId,
        mount_epoch: u64,
        group_name: &GroupName,
    ) -> TestFilesystemBuilder {
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

    #[allow(clippy::too_many_arguments)]
    pub(super) fn record_worker_heartbeat(
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
    pub(super) fn record_worker_heartbeat_with_tiers(
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

    pub(super) fn report_block(block_id: BlockId) -> BlockReportBlock {
        report_block_with_stamp(block_id, u64::from(block_id.index.as_raw()) + 1)
    }

    pub(super) fn report_block_with_stamp(block_id: BlockId, block_stamp: u64) -> BlockReportBlock {
        report_block_with_stamp_and_state(block_id, block_stamp, BlockReportBlockState::Ready)
    }

    pub(super) fn report_block_with_stamp_and_state(
        block_id: BlockId,
        block_stamp: u64,
        block_state: BlockReportBlockState,
    ) -> BlockReportBlock {
        BlockReportBlock {
            block_id,
            block_stamp,
            block_state,
        }
    }

    pub(super) fn publish_report_locations(
        manager: &WorkerManager,
        group_name: &GroupName,
        worker_id: WorkerId,
        report_seq: u64,
        blocks: Vec<BlockId>,
    ) {
        publish_report_locations_with_stamp(manager, group_name, worker_id, report_seq, None, blocks);
    }

    pub(super) fn publish_report_locations_with_stamp(
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

    pub(super) fn publish_report_block(
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

    pub(super) fn worker_manager_for_write_targets(group_name: &GroupName) -> Arc<WorkerManager> {
        let manager = Arc::new(WorkerManager::new(60));
        for raw in 1..=3 {
            let worker_id = beryl_types::ids::WorkerId::new(raw);
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

    pub(super) fn filesystem_builder_without_mount() -> TestFilesystemBuilder {
        TestFilesystemBuilder::new(Arc::new(MountTable::new()))
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

    pub(super) fn assert_refresh_metadata(error: &RpcErrorDetail, kind: ErrorKind) {
        assert_eq!(error.kind, kind);
        assert!(matches!(error.recovery, RecoveryAction::RefreshMetadata { .. }));
    }

    pub(super) fn refresh_hint(error: &RpcErrorDetail) -> &RefreshHint {
        match &error.recovery {
            RecoveryAction::RefreshMetadata { hint } | RecoveryAction::ReopenWriteSession { hint } => hint,
            other => panic!("expected refresh-like recovery, got {other:?}"),
        }
    }

    pub(super) fn install_write_session(
        filesystem: &TestFilesystem,
        inode_id: InodeId,
        mount_id: MountId,
    ) -> DataHandleId {
        let writer = ClientId::new(7);
        let data_handle_id = DataHandleId::new(424_242);
        let (lease_epoch, expires_at_ms) = filesystem
            .lease_manager()
            .try_acquire(inode_id, writer, crate::inode_lease::WriteMode::Write, None)
            .expect("lease acquired");
        filesystem
            .session_registry()
            .create_session(crate::session_registry::CreateSessionInput {
                inode_id,
                mount_id,
                data_handle_id,
                lease_epoch,
                base_size: 0,
                content_revision: 0,
                mode: crate::inode_lease::WriteMode::Write,
                open_client_id: writer,
                layout: FileLayout::new(64, 64, 1),
                expires_at_ms,
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
                    block_format_id: beryl_types::BlockFormatId::CURRENT_FOR_NEW_FILE,
                    tier: beryl_types::Tier::Hdd,
                }],
            })
            .expect("session created");
        data_handle_id
    }

    pub(super) fn committed_block(block_id: BlockId, file_offset: u64, len: u64) -> CommittedBlock {
        CommittedBlock {
            block_id,
            file_offset,
            len,
        }
    }

    pub(super) async fn add_block_for_key(
        filesystem: &MetadataFileSystem,
        key: &OpenWriteOutput,
        desired_len: u64,
    ) -> WriteTarget {
        let previous_block_id = filesystem
            .session_registry
            .get_session(key.data_handle_id)
            .and_then(|session| session.issued_targets.last().map(|target| target.block_id));
        filesystem
            .add_block_session(
                &request_context(),
                key.data_handle_id,
                key.lease_epoch,
                Some(desired_len),
                previous_block_id,
                Freshness::default(),
            )
            .await
            .expect("AddBlock should succeed")
            .payload
            .target
    }

    pub(super) async fn commit_for_key(
        filesystem: &MetadataFileSystem,
        key: &OpenWriteOutput,
        committed_blocks: Vec<CommittedBlock>,
        final_size: u64,
    ) -> FsResult<CloseWriteOutput> {
        filesystem
            .close_write_session(
                &request_context(),
                PresentedWriteHandle {
                    data_handle_id: key.data_handle_id,
                    lease_epoch: key.lease_epoch,
                },
                CloseWriteIntent {
                    committed_blocks,
                    final_size,
                    expected_file_size: key.base_size,
                },
                Freshness::default(),
                key.content_revision,
                match filesystem
                    .session_registry
                    .get_session(key.data_handle_id)
                    .expect("active write session")
                    .mode
                {
                    crate::inode_lease::WriteMode::Write => crate::raft::PublishMode::ReplaceIfUnchanged,
                    crate::inode_lease::WriteMode::Append => crate::raft::PublishMode::AppendIfUnchanged,
                },
            )
            .await
    }

    pub(super) struct WriteFlowEnv {
        pub(super) _dir: TempDir,
        pub(super) storage: Arc<RocksDBStorage>,
        pub(super) filesystem: TestFilesystem,
        pub(super) inode_id: InodeId,
        pub(super) data_handle_id: DataHandleId,
        pub(super) group_name: GroupName,
    }

    pub(super) async fn write_flow_env(base_size: u64) -> WriteFlowEnv {
        build_write_flow_env(base_size, worker_manager_for_write_targets).await
    }

    async fn build_write_flow_env(
        base_size: u64,
        worker_manager: impl FnOnce(&GroupName) -> Arc<WorkerManager>,
    ) -> WriteFlowEnv {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(57 + base_size);
        let group_name = group_name(&format!("g{}", 15 + base_size));
        let inode_id = InodeId::new(570 + base_size);
        let data_handle_id = DataHandleId::new(9570 + base_size);
        let builder = filesystem_builder_with_mount(mount_id, 9, &group_name);
        let mount_table = builder.mount_table();
        let (raft_node, _state_machine) = single_node_raft(Arc::clone(&storage), mount_table).await;
        let filesystem = builder
            .with_storage(Arc::clone(&storage))
            .with_raft_node(raft_node)
            .with_worker_manager(worker_manager(&group_name))
            .build();

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
            filesystem,
            inode_id,
            data_handle_id,
            group_name,
        }
    }

    pub(super) fn seed_committed_content_revision(env: &WriteFlowEnv, content_revision: u64, lease_epoch: u64) {
        let block_id = BlockId::new(env.data_handle_id, BlockIndex::new(0));
        let mut inode = env
            .storage
            .get_inode(env.inode_id)
            .unwrap()
            .expect("test inode should exist");
        inode.attrs.size = 64;
        match &mut inode.data {
            beryl_types::fs::InodeData::File {
                extents,
                content_revision: stored_content_revision,
                lease_epoch: stored_lease_epoch,
            } => {
                *extents = vec![beryl_types::fs::Extent {
                    file_offset: 0,
                    block_id,
                    block_offset: 0,
                    len: 64,
                    content_revision: Some(content_revision),
                    block_stamp: Some(content_revision),
                }];
                *stored_content_revision = Some(content_revision);
                *stored_lease_epoch = Some(lease_epoch);
            }
            other => panic!("unexpected inode data: {:?}", other),
        }
        env.storage.put_inode(&inode).unwrap();
    }

    pub(super) fn publish_env_block_location(env: &WriteFlowEnv, block_id: BlockId, block_stamp: u64, report_seq: u64) {
        let worker_manager = env.filesystem.worker_manager.as_ref().expect("worker manager");
        publish_report_locations_with_stamp(
            worker_manager,
            &env.group_name,
            WorkerId::new(1),
            report_seq,
            Some(block_stamp),
            vec![block_id],
        );
    }

    pub(super) fn stored_content_revision(storage: &RocksDBStorage, inode_id: InodeId) -> Option<u64> {
        let inode = storage.get_inode(inode_id).unwrap().expect("test inode should exist");
        match inode.data {
            beryl_types::fs::InodeData::File { content_revision, .. } => content_revision,
            other => panic!("unexpected inode data: {:?}", other),
        }
    }

    pub(super) async fn single_node_raft(
        storage: Arc<RocksDBStorage>,
        mount_table: Arc<MountTable>,
    ) -> (Arc<AppRaftNode>, Arc<AppRaftStateMachine>) {
        let state_machine = Arc::new(AppRaftStateMachine::new(Arc::clone(&storage)));
        let raft_config = RaftConfig::default();
        let raft_node = Arc::new(
            AppRaftNode::new(1, storage, Arc::clone(&state_machine), mount_table, &raft_config)
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
