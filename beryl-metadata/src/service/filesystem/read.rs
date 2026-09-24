// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

//! Namespace and file-location read operations.

use super::{
    missing_resolved_target_error, FileRange, FsFailure, FsResult, FsSuccess, MetadataFileSystem, RequestHeader,
};
use crate::error::MetadataError;
use crate::inode::Inode;
use crate::observe;
use crate::placement::{
    plan_placement, PlacementOp, PlacementRequest, PlacementStatus, ReportedBlockLocation, WorkerPlacementView,
};
use beryl_common::error::rpc::{ErrorKind, WorkerErrorKind};
use beryl_common::header::CallerContextFields;
use beryl_types::ids::{InodeId, MountId};
use beryl_types::FileStatus;
use beryl_types::{FileBlockLocation, GroupName, WorkerEndpointInfo};
use std::time::Instant;

/// One direct child joined with its authoritative inode status.
#[derive(Clone, Debug)]
pub(crate) struct ReadDirEntry {
    pub(crate) name: String,
    pub(crate) status: FileStatus,
}

#[derive(Clone, Debug)]
pub(crate) struct GetFileLayoutOutput {
    pub(crate) status: FileStatus,
    pub(crate) locations: Vec<FileBlockLocation>,
}

/// Validated filesystem arguments for one public directory-listing page.
pub(crate) struct ListStatusArgs {
    pub(crate) path: String,
    pub(crate) cursor_key: Option<Vec<u8>>,
    /// Positive server-resolved page size; wire defaults and caps are already applied.
    pub(crate) max_entries: usize,
}

#[derive(Debug)]
pub(crate) struct ListStatusOutput {
    pub(crate) entries: Vec<ReadDirEntry>,
    pub(crate) next_cursor_key: Option<Vec<u8>>,
}

pub(crate) enum BlockLocationsTarget {
    Path(String),
    InodeId(InodeId),
}

pub(crate) struct GetBlockLocationsArgs {
    pub(crate) target: BlockLocationsTarget,
    pub(crate) range: Option<FileRange>,
}

impl MetadataFileSystem {
    pub(crate) async fn get_status(&self, ctx: &RequestHeader, path: &str) -> FsResult<FileStatus> {
        self.check_readiness()?;
        let resolved = match self.path_resolver.resolve_path(path) {
            Ok(resolved) => resolved,
            Err(err) => return Err(self.failure_from_path_error(ctx, path, err)),
        };
        let Some(inode_id) = resolved.inode_id else {
            return Err(self.failure_from_resolved_path_error(
                ctx,
                missing_resolved_target_error(&resolved),
                Some(&resolved.mount_ctx),
            ));
        };

        self.get_attr_resolved(ctx, inode_id).await
    }

    /// Resolves a directory path and returns one bounded weakly consistent page.
    pub(crate) async fn list_status(&self, ctx: &RequestHeader, args: ListStatusArgs) -> FsResult<ListStatusOutput> {
        self.check_readiness()?;
        let resolved = match self.path_resolver.resolve_path(&args.path) {
            Ok(resolved) => resolved,
            Err(err) => return Err(self.failure_from_path_error(ctx, &args.path, err)),
        };
        let Some(inode_id) = resolved.inode_id else {
            return Err(self.failure_from_resolved_path_error(
                ctx,
                missing_resolved_target_error(&resolved),
                Some(&resolved.mount_ctx),
            ));
        };
        self.read_dir_resolved(ctx, inode_id, args.cursor_key.as_deref(), args.max_entries)
            .await
    }

    pub(crate) async fn open_file(&self, ctx: &RequestHeader, path: &str) -> FsResult<FileStatus> {
        self.check_readiness()?;
        let resolved = match self.path_resolver.resolve_path(path) {
            Ok(resolved) => resolved,
            Err(err) => return Err(self.failure_from_path_error(ctx, path, err)),
        };
        let Some(inode_id) = resolved.inode_id else {
            return Err(self.failure_from_resolved_path_error(
                ctx,
                missing_resolved_target_error(&resolved),
                Some(&resolved.mount_ctx),
            ));
        };

        self.read_file_inode(ctx, inode_id).await.map(|success| FsSuccess {
            payload: success.payload.status(),
            group_name: success.group_name,
            state: success.state,
        })
    }

    pub(crate) async fn get_block_locations(
        &self,
        ctx: &RequestHeader,
        args: GetBlockLocationsArgs,
    ) -> FsResult<GetFileLayoutOutput> {
        self.check_readiness()?;

        let inode_id = match args.target {
            BlockLocationsTarget::Path(path) => {
                let resolved = match self.path_resolver.resolve_path(&path) {
                    Ok(resolved) => resolved,
                    Err(err) => return Err(self.failure_from_path_error(ctx, &path, err)),
                };
                let Some(inode_id) = resolved.inode_id else {
                    return Err(self.failure_from_resolved_path_error(
                        ctx,
                        missing_resolved_target_error(&resolved),
                        Some(&resolved.mount_ctx),
                    ));
                };
                inode_id
            }
            BlockLocationsTarget::InodeId(inode_id) => inode_id,
        };

        self.get_file_layout_resolved(ctx, inode_id, args.range).await
    }

    async fn validate_read_freshness_for_mount(
        &self,
        req_ctx: &RequestHeader,
        mount_id: MountId,
    ) -> Result<Option<GroupName>, FsFailure> {
        let group_name = self.mount_owner(mount_id);
        self.check_leader(req_ctx, group_name.clone()).await?;
        self.validate_stale_state(req_ctx, self.raft_node.get_last_applied_state_id(), group_name.clone())?;
        Ok(group_name)
    }

    fn caller_context_fields(req_ctx: &RequestHeader) -> Option<CallerContextFields> {
        req_ctx
            .caller_context
            .as_ref()
            .map(CallerContextFields::from_caller_context)
    }

    fn classify_unavailable_read_location(
        reported: &[ReportedBlockLocation],
        views: &[WorkerPlacementView],
    ) -> ErrorKind {
        if reported.iter().any(|location| {
            views.iter().any(|worker| {
                worker.worker_id == location.worker_id
                    && worker.worker_run_id.is_some_and(|run| run != location.worker_run_id)
            })
        }) {
            ErrorKind::Worker(WorkerErrorKind::RunMismatch)
        } else {
            ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable)
        }
    }

    async fn get_attr_resolved(&self, ctx: &RequestHeader, inode_id: InodeId) -> FsResult<FileStatus> {
        let started = Instant::now();
        let result = async {
            let inode = match self.storage.get_inode(inode_id) {
                Ok(Some(inode)) => inode,
                Ok(None) => {
                    return Err(self.failure_from_error(
                        ctx,
                        MetadataError::NotFound(format!("Inode not found: {}", inode_id)),
                        None,
                    ));
                }
                Err(err) => return Err(self.failure_from_error(ctx, err, None)),
            };

            let group_name = self.validate_read_freshness_for_mount(ctx, inode.mount_id).await?;
            self.success(inode.status(), group_name)
        }
        .await;
        record_fs_read_result("get_status", started, &result);
        result
    }

    /// Reads a bounded dentry page and joins each dentry with its inode authority.
    async fn read_dir_resolved(
        &self,
        ctx: &RequestHeader,
        parent_inode_id: InodeId,
        cursor_key: Option<&[u8]>,
        max_entries: usize,
    ) -> FsResult<ListStatusOutput> {
        let started = Instant::now();
        let result = async {
            let parent_inode = match self.storage.get_inode(parent_inode_id) {
                Ok(Some(parent_inode)) => parent_inode,
                Ok(None) => {
                    return Err(self.failure_from_error(
                        ctx,
                        MetadataError::NotFound(format!("Parent inode not found: {}", parent_inode_id)),
                        None,
                    ));
                }
                Err(err) => return Err(self.failure_from_error(ctx, err, None)),
            };
            if !parent_inode.file_type().is_dir() {
                return Err(self.failure_from_error(
                    ctx,
                    MetadataError::InvalidArgument(format!("Parent is not a directory: {}", parent_inode_id)),
                    None,
                ));
            }

            let group_name = self
                .validate_read_freshness_for_mount(ctx, parent_inode.mount_id)
                .await?;

            let (entries, next_cursor_key) =
                match self
                    .storage
                    .list_dentries_with_cursor(parent_inode_id, cursor_key, max_entries)
                {
                    Ok(result) => result,
                    Err(err) => return Err(self.failure_from_error(ctx, err, group_name)),
                };

            let mut dir_entries = Vec::with_capacity(entries.len());
            for (name, child_inode_id) in entries {
                let child_inode = match self.storage.get_inode(child_inode_id) {
                    Ok(Some(child_inode)) => child_inode,
                    Ok(None) => {
                        return Err(self.failure_from_error(
                            ctx,
                            MetadataError::NotFound(format!(
                                "Directory dentry '{}' under parent inode {} points to missing inode {}",
                                name, parent_inode_id, child_inode_id
                            )),
                            group_name,
                        ));
                    }
                    Err(err) => {
                        return Err(self.failure_from_error(ctx, err, group_name));
                    }
                };
                dir_entries.push(ReadDirEntry {
                    name,
                    status: child_inode.status(),
                });
            }

            self.success(
                ListStatusOutput {
                    entries: dir_entries,
                    next_cursor_key,
                },
                group_name,
            )
        }
        .await;
        record_fs_read_result("list_status", started, &result);
        result
    }

    async fn read_file_inode(&self, ctx: &RequestHeader, inode_id: InodeId) -> FsResult<Inode> {
        let inode = match self.storage.get_inode(inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                return Err(self.failure_from_error(
                    ctx,
                    MetadataError::NotFound(format!("Inode not found: {}", inode_id)),
                    None,
                ));
            }
            Err(err) => {
                return Err(self.failure_from_error(ctx, err, None));
            }
        };

        if !inode.file_type().is_file() {
            return Err(self.failure_from_error(
                ctx,
                MetadataError::IsDir(format!("Inode is not a file: {}", inode_id)),
                None,
            ));
        }

        self.check_data_read(inode.mount_id)?;

        let group_name = self.validate_read_freshness_for_mount(ctx, inode.mount_id).await?;

        self.success(inode, group_name)
    }

    async fn get_file_layout_resolved(
        &self,
        ctx: &RequestHeader,
        inode_id: InodeId,
        range: Option<FileRange>,
    ) -> FsResult<GetFileLayoutOutput> {
        let started = Instant::now();
        let result = async {
            let success = self.read_file_inode(ctx, inode_id).await?;
            let inode = success.payload;
            let group_name = success.group_name;
            let file = inode.file().expect("file kind checked above");
            if let Err(error) = file.validate(inode_id) {
                return Err(self.failure_from_error(ctx, error, group_name));
            }
            let block_size = file.block_size;

            let (range_start, range_end) = match range {
                Some(range) => match range.offset.checked_add(range.len) {
                    Some(end) => (range.offset.min(file.len), end.min(file.len)),
                    None => {
                        return Err(self.failure_from_error(
                            ctx,
                            MetadataError::InvalidArgument("range end overflows".into()),
                            group_name,
                        ))
                    }
                },
                None => (0, file.len),
            };
            let manager = &self.worker_manager;
            let caller = Self::caller_context_fields(ctx);
            let mut locations = Vec::new();
            let first = (range_start / u64::from(block_size)) as usize;
            let end = range_end.div_ceil(u64::from(block_size)) as usize;
            let end = if range_start == range_end { first } else { end };
            if first < end {
                let worker_group = self.require_worker_lookup_group(ctx, group_name.clone(), "GetFileLayout")?;
                for (index, block_id) in file.blocks[first..end].iter().copied().enumerate() {
                    let ordinal = first + index;
                    let file_offset = ordinal as u64 * u64::from(block_size);
                    let effective_len = file.block_len(ordinal);
                    let reported = manager.reported_block_locations(&worker_group, block_id);
                    let views = manager.collect_worker_placement_views(&worker_group);
                    let plan = plan_placement(
                        &PlacementRequest {
                            group_name: worker_group.clone(),
                            op: PlacementOp::Read,
                            block_id,
                            visible_len: effective_len,
                            block_size,
                            caller: caller.clone(),
                            existing: &reported,
                        },
                        &views,
                    );
                    if plan.status == PlacementStatus::NoLiveReplica {
                        return Err(self.refresh_metadata_failure(
                            ctx,
                            Self::classify_unavailable_read_location(&reported, &views),
                            format!("no live replica holds the visible prefix of block {block_id}"),
                            group_name,
                        ));
                    }
                    let workers = plan
                        .workers
                        .into_iter()
                        .map(|worker| WorkerEndpointInfo {
                            worker_id: worker.worker_id,
                            worker_run_id: worker.worker_run_id,
                            endpoint: worker.endpoint,
                        })
                        .collect();
                    locations.push(FileBlockLocation {
                        block_id,
                        file_offset,
                        len: effective_len,
                        workers,

                        block_size: u64::from(block_size),
                    });
                }
            }

            self.success(
                GetFileLayoutOutput {
                    status: inode.status(),
                    locations,
                },
                group_name,
            )
        }
        .await;
        record_fs_read_result("get_file_layout", started, &result);
        result
    }
}

fn record_fs_read_result<T>(operation: &str, started: Instant, result: &FsResult<T>) {
    match result {
        Ok(_) => observe::record_fs_op(operation, "ok", "none", started.elapsed().as_secs_f64()),
        Err(failure) => observe::record_fs_op(
            operation,
            "error",
            observe::rpc_error_kind(&failure.error),
            started.elapsed().as_secs_f64(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inode::{Inode, InodeKind};
    use crate::service::filesystem::tests::*;
    use beryl_types::{ContentGeneration, GroupStateWatermark, RaftLogId, WriteMode};

    fn seed_visible_block(storage: &RocksDBStorage, mount_id: MountId, inode_id: InodeId, block_id: BlockId) {
        let attrs = InodeAttrs::new();
        let mut inode = Inode::new_file(inode_id, attrs, mount_id, 4096);
        inode.kind = InodeKind::File(crate::inode::FileData {
            len: 512,
            block_size: 4096,
            blocks: vec![block_id],
            generation: ContentGeneration::new(1),
            lease_epoch: beryl_types::LeaseEpoch::default(),
            next_index: 1,
            last_commit: None,
        });
        storage.put_inode(&inode).unwrap();
    }

    #[tokio::test]
    async fn open_status_is_independent_of_locations_and_range_selects_only_intersecting_blocks() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(1);
        let group = group_name("g8");
        let inode_id = InodeId::new(481);
        let first = BlockId::new(inode_id, BlockIndex::new(0));
        let second = BlockId::new(inode_id, BlockIndex::new(1));
        let manager = Arc::new(WorkerManager::new(60_000));
        let filesystem = filesystem_builder_with_mount(mount_id, &group)
            .with_storage(Arc::clone(&storage))
            .with_worker_manager(Arc::clone(&manager))
            .build()
            .await;
        storage
            .put_inode(&Inode::new_dir(ROOT_INODE_ID, InodeAttrs::new(), mount_id))
            .unwrap();
        seed_visible_block(&storage, mount_id, inode_id, first);
        let mut inode = storage.get_inode(inode_id).unwrap().unwrap();
        let file = inode.file_mut().unwrap();
        file.len = 4096 + 512;
        file.blocks.push(second);
        file.next_index = 2;
        storage.put_inode(&inode).unwrap();
        storage.put_dentry(ROOT_INODE_ID, "file", inode_id).unwrap();
        let status = filesystem.open_file(&request_context(), "/file").await.unwrap().payload;
        assert_eq!(status, inode.status());
        let locations = |range| GetBlockLocationsArgs {
            target: BlockLocationsTarget::InodeId(inode_id),
            range: Some(range),
        };
        assert!(filesystem
            .get_block_locations(&request_context(), locations(FileRange { offset: 0, len: 1 }))
            .await
            .is_err());
        let worker_id = WorkerId::new(1);
        register_worker_descriptor(&manager, &group, worker_id, "127.0.0.1:9101".into());
        record_worker_heartbeat(&manager, &group, worker_id, 8192);
        publish_report_block(
            &manager,
            &group,
            worker_id,
            1,
            report_block_with_epoch_and_len(second, 1, 512),
        );
        let tail = filesystem
            .get_block_locations(
                &request_context(),
                locations(FileRange {
                    offset: 4200,
                    len: 1000,
                }),
            )
            .await
            .unwrap()
            .payload;
        assert_eq!(tail.status, status);
        assert_eq!(tail.locations.len(), 1);
        assert_eq!(tail.locations[0].block_id, second);
        assert_eq!(tail.locations[0].len, 512);
        // The unavailable first block must not prevent an explicitly requested tail layout.
        let eof = filesystem
            .get_block_locations(&request_context(), locations(FileRange { offset: 9000, len: 1 }))
            .await
            .unwrap()
            .payload;
        assert!(eof.locations.is_empty());
    }

    #[tokio::test]
    async fn get_file_layout_rejects_unavailable_or_stale_reported_locations() {
        #[derive(Clone, Copy, Debug)]
        enum Case {
            MissingReport,
            NonReady,
            ShortPrefix,
            ExpiredWorker,
        }

        for (offset, case) in [
            (0, Case::MissingReport),
            (1, Case::NonReady),
            (2, Case::ShortPrefix),
            (3, Case::ExpiredWorker),
        ] {
            let dir = TempDir::new().unwrap();
            let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
            let mount_id = MountId::new(1);
            let group_name_value = group_name("g8");
            let inode_id = InodeId::new(530 + offset);
            let block_id = BlockId::new(inode_id, BlockIndex::new(0));
            let worker_id = WorkerId::new(1);
            let worker_manager = Arc::new(WorkerManager::new(if matches!(case, Case::ExpiredWorker) {
                1_000
            } else {
                60_000
            }));

            if !matches!(case, Case::MissingReport) {
                register_worker_descriptor(
                    &worker_manager,
                    &group_name_value,
                    worker_id,
                    "127.0.0.1:9101".to_string(),
                );
                record_worker_heartbeat(&worker_manager, &group_name_value, worker_id, 1024);
            }
            match case {
                Case::MissingReport => {}
                Case::NonReady => publish_report_block(
                    &worker_manager,
                    &group_name_value,
                    worker_id,
                    1,
                    report_block_with_epoch_and_state(block_id, 41, BlockReportBlockState::Corrupt),
                ),
                Case::ShortPrefix => publish_report_locations_with_epoch(
                    &worker_manager,
                    &group_name_value,
                    worker_id,
                    1,
                    Some(40),
                    vec![block_id],
                ),
                Case::ExpiredWorker => {
                    publish_report_locations_with_epoch(
                        &worker_manager,
                        &group_name_value,
                        worker_id,
                        1,
                        Some(41),
                        vec![block_id],
                    );
                    std::thread::sleep(Duration::from_millis(1100));
                    worker_manager.expire_liveness();
                }
            }

            let filesystem = filesystem_builder_with_mount(mount_id, &group_name_value)
                .with_storage(Arc::clone(&storage))
                .with_worker_manager(worker_manager)
                .build()
                .await;
            seed_visible_block(&storage, mount_id, inode_id, block_id);

            let failure = match filesystem
                .get_file_layout_resolved(&request_context(), inode_id, None)
                .await
            {
                Ok(success) => panic!("case {case:?} unexpectedly returned {success:?}"),
                Err(failure) => failure,
            };

            assert_block_location_unavailable(&failure, block_id);
        }
    }

    #[tokio::test]
    async fn get_locations_rejects_stale_state_watermark() {
        let env = write_flow_env(0).await;
        let open = env
            .filesystem
            .open_write_inode(
                &request_context(),
                "/file".to_string(),
                env.inode_id,
                vec![env.inode_id],
                WriteMode::Overwrite,
            )
            .await
            .expect("open write should succeed");
        let key = open.payload;
        let target = allocate_block_for_key(&env.filesystem, &key).await;
        publish_env_write_target(&env, &target, 1);
        commit_for_key(&env.filesystem, &key, vec![committed_block(target.block_id, 64)], 64)
            .await
            .expect("commit should succeed");

        let current_state = env
            .filesystem
            .raft_node
            .get_last_applied_state_id()
            .expect("commit should advance applied state");
        let mut ctx = request_context();
        ctx.state = Some(GroupStateWatermark::new(
            group_name("g15"),
            RaftLogId {
                term: current_state.term,
                leader_node_id: current_state.leader_node_id,
                index: current_state.index + 1,
            },
        ));

        let failure = env
            .filesystem
            .get_file_layout_resolved(&ctx, env.inode_id, None)
            .await
            .expect_err("read should reject state watermark beyond local applied state");

        assert_refresh_metadata(&failure.error, ErrorKind::Metadata(MetadataErrorKind::StaleState));
    }
}
