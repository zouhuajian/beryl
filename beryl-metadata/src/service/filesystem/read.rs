// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Beryl Contributors

use super::refresh_metadata_fs_failure;
use super::{
    missing_resolved_target_error, worker_endpoint_from_parts, FileRange, Freshness, FsFailure, FsResult, FsSuccess,
    MetadataFileSystem, RequestContext, StaleStateStatus,
};
use crate::error::MetadataError;
use crate::observe;
use crate::placement::{
    PlacementOp, PlacementPlanner, PlacementRequest, PlacementStatus, ReportedBlockLocation, WorkerPlacementView,
};
use beryl_common::error::rpc::{ErrorKind, MetadataErrorKind, RefreshHint, WorkerErrorKind};
use beryl_common::header::CallerContextFields;
use beryl_types::fs::{Extent, FileAttrs, InodeId, InodeKind};
use beryl_types::ids::{DataHandleId, MountId};
use beryl_types::{FileBlockLocation, GroupName};
use std::time::Instant;

#[derive(Clone, Debug)]
pub(super) struct GetAttrInput {
    pub(super) ctx: RequestContext,
    pub(super) inode_id: InodeId,
    pub(super) freshness: Freshness,
}

#[derive(Clone, Debug)]
pub(super) struct GetAttrOutput {
    pub(super) attrs: FileAttrs,
}

#[derive(Clone, Debug)]
struct ReadDirInput {
    ctx: RequestContext,
    parent_inode_id: InodeId,
    cursor_key: Option<Vec<u8>>,
    max_entries: Option<usize>,
    freshness: Freshness,
}

#[derive(Clone, Debug)]
pub(crate) struct ReadDirEntry {
    pub(crate) name: String,
    pub(crate) kind: Option<InodeKind>,
    pub(crate) attrs: Option<FileAttrs>,
}

#[derive(Clone, Debug, Default)]
struct ReadDirOutput {
    entries: Vec<ReadDirEntry>,
    next_cursor_key: Vec<u8>,
    eof: bool,
}

#[derive(Clone, Copy, Debug)]
struct InodeMountGuardInputs {
    mount_id: MountId,
}

#[derive(Clone, Debug)]
pub(super) struct GetFileLayoutInput {
    pub(super) ctx: RequestContext,
    pub(super) inode_id: InodeId,
    pub(super) range: Option<FileRange>,
    pub(super) requested_data_handle_id: Option<DataHandleId>,
    pub(super) freshness: Freshness,
}

#[derive(Clone, Debug, Default)]
pub(super) struct GetFileLayoutOutput {
    pub(super) file_size: u64,
    pub(super) content_revision: Option<u64>,
    pub(super) locations: Vec<FileBlockLocation>,
}

pub(crate) struct GetStatusArgs {
    pub(crate) path: String,
    pub(crate) freshness: Freshness,
}

pub(crate) struct GetStatusOutput {
    pub(crate) attrs: FileAttrs,
}

pub(crate) struct ListStatusArgs {
    pub(crate) path: String,
    pub(crate) recursive: bool,
    pub(crate) cursor_key: Option<Vec<u8>>,
    pub(crate) max_entries: Option<usize>,
    pub(crate) freshness: Freshness,
}

pub(crate) struct ListStatusOutput {
    pub(crate) entries: Vec<ReadDirEntry>,
    pub(crate) next_cursor_key: Vec<u8>,
    pub(crate) eof: bool,
}

pub(crate) struct OpenFileArgs {
    pub(crate) path: String,
    pub(crate) freshness: Freshness,
}

pub(crate) struct OpenFileOutput {
    pub(crate) data_handle_id: DataHandleId,
    pub(crate) file_size: u64,
    pub(crate) content_revision: Option<u64>,
}

pub(crate) enum BlockLocationsTarget {
    Path(String),
    DataHandle(DataHandleId),
}

pub(crate) struct GetBlockLocationsArgs {
    pub(crate) target: BlockLocationsTarget,
    pub(crate) range: Option<FileRange>,
    pub(crate) freshness: Freshness,
}

pub(crate) struct GetBlockLocationsOutput {
    pub(crate) data_handle_id: DataHandleId,
    pub(crate) file_size: u64,
    pub(crate) content_revision: Option<u64>,
    pub(crate) locations: Vec<FileBlockLocation>,
}

impl MetadataFileSystem {
    pub(crate) async fn get_status(&self, ctx: &RequestContext, args: GetStatusArgs) -> FsResult<GetStatusOutput> {
        if let Err(failure) = self.admission.check_meta_read(ctx).await {
            return self.failure_from_admission(failure);
        }
        let resolved = match self.path_resolver.resolve_path(&args.path) {
            Ok(resolved) => resolved,
            Err(err) => return self.failure_from_path_error(ctx, &args.path, err),
        };
        let Some(inode_id) = resolved.inode_id else {
            return self.failure_from_resolved_path_error(
                ctx,
                missing_resolved_target_error(&resolved),
                Some(&resolved.mount_ctx),
            );
        };

        self.get_attr_resolved(GetAttrInput {
            ctx: ctx.clone(),
            inode_id,
            freshness: args.freshness,
        })
        .await
        .map(|success| FsSuccess {
            payload: GetStatusOutput {
                attrs: success.payload.attrs,
            },
            group_name: success.group_name,
            mount_epoch: success.mount_epoch,
            route_epoch: success.route_epoch,
            state: success.state,
        })
    }

    pub(crate) async fn list_status(&self, ctx: &RequestContext, args: ListStatusArgs) -> FsResult<ListStatusOutput> {
        if let Err(failure) = self.admission.check_meta_read(ctx).await {
            return self.failure_from_admission(failure);
        }
        let resolved = match self.path_resolver.resolve_path(&args.path) {
            Ok(resolved) => resolved,
            Err(err) => return self.failure_from_path_error(ctx, &args.path, err),
        };
        let Some(inode_id) = resolved.inode_id else {
            return self.failure_from_resolved_path_error(
                ctx,
                missing_resolved_target_error(&resolved),
                Some(&resolved.mount_ctx),
            );
        };
        if args.recursive {
            return self.failure_from_resolved_path_error(
                ctx,
                MetadataError::NotSupported("Recursive listing not yet implemented".to_string()),
                Some(&resolved.mount_ctx),
            );
        }

        self.read_dir_resolved(ReadDirInput {
            ctx: ctx.clone(),
            parent_inode_id: inode_id,
            cursor_key: args.cursor_key,
            max_entries: args.max_entries,
            freshness: args.freshness,
        })
        .await
        .map(|success| FsSuccess {
            payload: ListStatusOutput {
                entries: success.payload.entries,
                next_cursor_key: success.payload.next_cursor_key,
                eof: success.payload.eof,
            },
            group_name: success.group_name,
            mount_epoch: success.mount_epoch,
            route_epoch: success.route_epoch,
            state: success.state,
        })
    }

    pub(crate) async fn open_file(&self, ctx: &RequestContext, args: OpenFileArgs) -> FsResult<OpenFileOutput> {
        if let Err(failure) = self.admission.check_meta_read(ctx).await {
            return self.failure_from_admission(failure);
        }
        let resolved = match self.path_resolver.resolve_path(&args.path) {
            Ok(resolved) => resolved,
            Err(err) => return self.failure_from_path_error(ctx, &args.path, err),
        };
        let Some(inode_id) = resolved.inode_id else {
            return self.failure_from_resolved_path_error(
                ctx,
                missing_resolved_target_error(&resolved),
                Some(&resolved.mount_ctx),
            );
        };
        if let Err(failure) = self.admission.check_data_read(ctx, resolved.mount_ctx.mount_id).await {
            return self.failure_from_admission(failure);
        }
        let inode = match self.read_inode(inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                return self.failure_from_resolved_path_error(
                    ctx,
                    MetadataError::NotFound(format!("Inode not found: {}", inode_id)),
                    Some(&resolved.mount_ctx),
                )
            }
            Err(err) => return self.failure_from_resolved_path_error(ctx, err, Some(&resolved.mount_ctx)),
        };
        if !inode.kind.is_file() {
            return self.failure_from_resolved_path_error(
                ctx,
                MetadataError::IsDir(format!("Inode is not a file: {}", inode_id)),
                Some(&resolved.mount_ctx),
            );
        }
        let data_handle_id = inode.current_data_handle_id;

        self.get_file_layout_resolved(GetFileLayoutInput {
            ctx: ctx.clone(),
            inode_id,
            range: None,
            requested_data_handle_id: None,
            freshness: args.freshness,
        })
        .await
        .map(|success| FsSuccess {
            payload: OpenFileOutput {
                data_handle_id,
                file_size: success.payload.file_size,
                content_revision: success.payload.content_revision,
            },
            group_name: success.group_name,
            mount_epoch: success.mount_epoch,
            route_epoch: success.route_epoch,
            state: success.state,
        })
    }

    pub(crate) async fn get_block_locations(
        &self,
        ctx: &RequestContext,
        args: GetBlockLocationsArgs,
    ) -> FsResult<GetBlockLocationsOutput> {
        if let Err(failure) = self.admission.check_meta_read(ctx).await {
            return self.failure_from_admission(failure);
        }

        let (inode_id, requested_data_handle_id) = match args.target {
            BlockLocationsTarget::Path(path) => {
                let resolved = match self.path_resolver.resolve_path(&path) {
                    Ok(resolved) => resolved,
                    Err(err) => return self.failure_from_path_error(ctx, &path, err),
                };
                let Some(inode_id) = resolved.inode_id else {
                    return self.failure_from_resolved_path_error(
                        ctx,
                        missing_resolved_target_error(&resolved),
                        Some(&resolved.mount_ctx),
                    );
                };
                if let Err(failure) = self.admission.check_data_read(ctx, resolved.mount_ctx.mount_id).await {
                    return self.failure_from_admission(failure);
                }
                (inode_id, None)
            }
            BlockLocationsTarget::DataHandle(data_handle_id) => {
                let inode_id = self.inode_for_data_handle(ctx, data_handle_id).await?.payload;
                let mount_id = self.plan_inode_mount(ctx, inode_id).await?.payload.mount_id;
                if let Err(failure) = self.admission.check_data_read(ctx, mount_id).await {
                    return self.failure_from_admission(failure);
                }
                (inode_id, Some(data_handle_id))
            }
        };

        let inode = match self.read_inode(inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                return self.failure_from_error(
                    ctx,
                    MetadataError::NotFound(format!("Inode not found: {}", inode_id)),
                    None,
                    None,
                )
            }
            Err(err) => return self.failure_from_error(ctx, err, None, None),
        };
        let data_handle_id = inode.current_data_handle_id;

        self.get_file_layout_resolved(GetFileLayoutInput {
            ctx: ctx.clone(),
            inode_id,
            range: args.range,
            requested_data_handle_id,
            freshness: args.freshness,
        })
        .await
        .map(|success| FsSuccess {
            payload: GetBlockLocationsOutput {
                data_handle_id,
                file_size: success.payload.file_size,
                content_revision: success.payload.content_revision,
                locations: success.payload.locations,
            },
            group_name: success.group_name,
            mount_epoch: success.mount_epoch,
            route_epoch: success.route_epoch,
            state: success.state,
        })
    }

    async fn validate_read_freshness_for_mount(
        &self,
        req_ctx: &RequestContext,
        freshness: Freshness,
        mount_id: beryl_types::ids::MountId,
        intent: &str,
    ) -> Result<(Option<GroupName>, Option<u64>, Option<u64>), FsFailure> {
        let (group_name, mount_epoch) = self
            .freshness_validator
            .validate_mount_epoch(req_ctx, freshness, mount_id)?;
        let route_epoch = self
            .freshness_validator
            .validate_route_epoch(req_ctx, freshness, group_name.clone(), mount_epoch, intent)
            .await?;
        match self.freshness_validator.validate_stale_state(
            req_ctx,
            self.raft_node
                .as_ref()
                .and_then(|raft_node| raft_node.get_last_applied_state_id()),
            group_name.clone(),
            mount_epoch,
        )? {
            StaleStateStatus::Ready => Ok((group_name, mount_epoch, route_epoch)),
            StaleStateStatus::UnknownLastApplied => Err(refresh_metadata_fs_failure(
                req_ctx,
                ErrorKind::Metadata(MetadataErrorKind::StaleState),
                "local applied state is unavailable for read freshness validation",
                group_name,
                mount_epoch,
                route_epoch,
                None,
            )),
        }
    }

    fn content_revision_for_inode(inode: &beryl_types::fs::Inode) -> Option<u64> {
        match &inode.data {
            beryl_types::fs::InodeData::File { content_revision, .. } => *content_revision,
            _ => None,
        }
    }

    fn caller_context_fields(req_ctx: &RequestContext) -> Option<CallerContextFields> {
        req_ctx
            .caller
            .caller_context
            .as_ref()
            .map(CallerContextFields::from_caller_context)
    }

    fn has_usable_read_endpoint(worker: &WorkerPlacementView) -> bool {
        let Some(worker_run_id) = worker.worker_run_id else {
            return false;
        };
        worker_endpoint_from_parts(
            worker.worker_id,
            worker.endpoint.clone(),
            worker.worker_net_protocol,
            worker_run_id,
        )
        .is_ok()
    }

    fn classify_unavailable_read_location(
        req: &RequestContext,
        worker_lookup_group_name: &GroupName,
        extent: &Extent,
        block_stamp: u64,
        reported: &[ReportedBlockLocation],
        views: &[WorkerPlacementView],
    ) -> (ErrorKind, String) {
        let matching_stamp = reported
            .iter()
            .filter(|location| location.block_stamp == block_stamp)
            .collect::<Vec<_>>();
        if !reported.is_empty() && matching_stamp.is_empty() {
            return (
                ErrorKind::Worker(WorkerErrorKind::BlockStampMismatch),
                format!(
                    "block location unavailable: block stamp mismatch for group={} block={} file_offset={} expected_stamp={} reported_stamps={:?}",
                    worker_lookup_group_name,
                    extent.block_id,
                    extent.file_offset,
                    block_stamp,
                    reported.iter().map(|location| location.block_stamp).collect::<Vec<_>>()
                ),
            );
        }

        for location in &matching_stamp {
            if let Some(view) = views
                .iter()
                .find(|view| view.group_name == *worker_lookup_group_name && view.worker_id == location.worker_id)
            {
                if view
                    .worker_run_id
                    .is_some_and(|worker_run_id| !worker_run_id.matches(location.worker_run_id))
                {
                    return (
                        ErrorKind::Worker(WorkerErrorKind::RunMismatch),
                        format!(
                            "block location unavailable: stale worker run for group={} block={} file_offset={} worker={} reported_run={} current_run={:?}",
                            worker_lookup_group_name,
                            extent.block_id,
                            extent.file_offset,
                            location.worker_id,
                            location.worker_run_id,
                            view.worker_run_id
                        ),
                    );
                }
            }
        }

        let reason_detail = if reported.is_empty() {
            "no ready block report"
        } else if matching_stamp.is_empty() {
            "no matching block stamp"
        } else {
            "no usable live replica"
        };
        let caller_context = req
            .caller
            .caller_context
            .as_ref()
            .map(|ctx| ctx.context.as_str())
            .unwrap_or("-");
        (
            ErrorKind::Worker(WorkerErrorKind::BlockLocationUnavailable),
            format!(
                "block location unavailable: reason={} group={} block={} file_offset={} block_stamp={} caller_context={} reported_locations={} worker_views={}",
                reason_detail,
                worker_lookup_group_name,
                extent.block_id,
                extent.file_offset,
                block_stamp,
                caller_context,
                reported.len(),
                views.len()
            ),
        )
    }

    async fn plan_inode_mount(&self, req_ctx: &RequestContext, inode_id: InodeId) -> FsResult<InodeMountGuardInputs> {
        let inode = match self.read_inode(inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                return self.failure_from_error(
                    req_ctx,
                    MetadataError::NotFound(format!("Inode not found: {}", inode_id)),
                    None,
                    None,
                );
            }
            Err(err) => return self.failure_from_error(req_ctx, err, None, None),
        };
        self.success(
            req_ctx,
            InodeMountGuardInputs {
                mount_id: inode.mount_id,
            },
            None,
            None,
        )
    }

    pub(super) async fn get_attr_resolved(&self, req: GetAttrInput) -> FsResult<GetAttrOutput> {
        let started = Instant::now();
        let result = async {
            let inode = match self.read_inode(req.inode_id) {
                Ok(Some(inode)) => inode,
                Ok(None) => {
                    return self.failure_from_error(
                        &req.ctx,
                        MetadataError::NotFound(format!("Inode not found: {}", req.inode_id)),
                        None,
                        None,
                    );
                }
                Err(err) => return self.failure_from_error(&req.ctx, err, None, None),
            };

            let (group_name, mount_epoch, route_epoch) = self
                .validate_read_freshness_for_mount(&req.ctx, req.freshness, inode.mount_id, "GetStatus")
                .await?;
            self.success_with_route_epoch(
                &req.ctx,
                GetAttrOutput {
                    attrs: inode.attrs.clone(),
                },
                group_name,
                mount_epoch,
                route_epoch,
            )
        }
        .await;
        record_fs_read_result("get_status", started, &result);
        result
    }

    async fn inode_for_data_handle(&self, req_ctx: &RequestContext, data_handle_id: DataHandleId) -> FsResult<InodeId> {
        let inode_id = match self.storage.get_inode_by_data_handle(data_handle_id) {
            Ok(Some(inode_id)) => inode_id,
            Ok(None) => {
                return self.failure_from_error(
                    req_ctx,
                    MetadataError::NotFound(format!("data_handle_id not found: {}", data_handle_id)),
                    None,
                    None,
                );
            }
            Err(err) => return self.failure_from_error(req_ctx, err, None, None),
        };
        self.success(req_ctx, inode_id, None, None)
    }

    async fn read_dir_resolved(&self, req: ReadDirInput) -> FsResult<ReadDirOutput> {
        let started = Instant::now();
        let result = async {
            let parent_inode = match self.read_inode(req.parent_inode_id) {
                Ok(Some(parent_inode)) => parent_inode,
                Ok(None) => {
                    return self.failure_from_error(
                        &req.ctx,
                        MetadataError::NotFound(format!("Parent inode not found: {}", req.parent_inode_id)),
                        None,
                        None,
                    );
                }
                Err(err) => return self.failure_from_error(&req.ctx, err, None, None),
            };
            if !parent_inode.kind.is_dir() {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::InvalidArgument(format!("Parent is not a directory: {}", req.parent_inode_id)),
                    None,
                    None,
                );
            }

            let (group_name, mount_epoch, route_epoch) = self
                .validate_read_freshness_for_mount(&req.ctx, req.freshness, parent_inode.mount_id, "ListStatus")
                .await?;

            let cursor_key = req.cursor_key.as_deref();
            let (entries, next_cursor_key, eof) =
                match self
                    .storage
                    .list_dentries_with_cursor(req.parent_inode_id, cursor_key, req.max_entries)
                {
                    Ok(result) => result,
                    Err(err) => return self.failure_from_error(&req.ctx, err, group_name, mount_epoch),
                };

            let mut dir_entries = Vec::with_capacity(entries.len());
            for (name, child_inode_id) in entries {
                let child_inode = match self.read_inode(child_inode_id) {
                    Ok(Some(child_inode)) => child_inode,
                    Ok(None) => {
                        return self.failure_from_error_with_route_epoch(
                            &req.ctx,
                            MetadataError::NotFound(format!(
                                "Directory dentry '{}' under parent inode {} points to missing inode {}",
                                name, req.parent_inode_id, child_inode_id
                            )),
                            group_name,
                            mount_epoch,
                            route_epoch,
                        );
                    }
                    Err(err) => {
                        return self.failure_from_error_with_route_epoch(
                            &req.ctx,
                            err,
                            group_name,
                            mount_epoch,
                            route_epoch,
                        );
                    }
                };
                dir_entries.push(ReadDirEntry {
                    name,
                    kind: Some(child_inode.kind),
                    attrs: Some(child_inode.attrs.clone()),
                });
            }

            self.success_with_route_epoch(
                &req.ctx,
                ReadDirOutput {
                    entries: dir_entries,
                    next_cursor_key: next_cursor_key.unwrap_or_default(),
                    eof,
                },
                group_name,
                mount_epoch,
                route_epoch,
            )
        }
        .await;
        record_fs_read_result("list_status", started, &result);
        result
    }

    pub(super) async fn get_file_layout_resolved(&self, req: GetFileLayoutInput) -> FsResult<GetFileLayoutOutput> {
        let started = Instant::now();
        let result = async {
        let inode = match self.read_inode(req.inode_id) {
            Ok(Some(inode)) => inode,
            Ok(None) => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::NotFound(format!("Inode not found: {}", req.inode_id)),
                    None,
                    None,
                );
            }
            Err(err) => {
                return self.failure_from_error(&req.ctx, err, None, None);
            }
        };

        if !inode.kind.is_file() {
            return self.failure_from_error(
                &req.ctx,
                MetadataError::IsDir(format!("Inode is not a file: {}", req.inode_id)),
                None,
                None,
            );
        }

        let (group_name, mount_epoch, route_epoch) = self
            .validate_read_freshness_for_mount(&req.ctx, req.freshness, inode.mount_id, "GetFileLayout")
            .await?;

        let content_revision = Self::content_revision_for_inode(&inode);
        let extents = match &inode.data {
            beryl_types::fs::InodeData::File { extents, .. } => extents.clone(),
            _ => Vec::new(),
        };
        let data_handle_id = inode.current_data_handle_id;
        if data_handle_id.as_raw() == 0 {
            return self.failure_from_error_with_route_epoch(
                &req.ctx,
                MetadataError::Internal(format!("File inode {} is missing current_data_handle_id", req.inode_id)),
                group_name,
                mount_epoch,
                route_epoch,
            );
        }
        if let Some(requested_data_handle_id) = req.requested_data_handle_id {
            if requested_data_handle_id != data_handle_id {
                return self.failure_from_error_with_route_epoch(
                    &req.ctx,
                    MetadataError::StaleState(format!(
                        "requested data_handle_id {} is not current data_handle_id {} for inode {}; refresh metadata state",
                        requested_data_handle_id, data_handle_id, req.inode_id
                    )),
                    group_name,
                    mount_epoch,
                    route_epoch,
                );
            }
        }
        let layout = match self.read_layout(req.inode_id) {
            Ok(layout) => layout,
            Err(err) => {
                return self.failure_from_error_with_route_epoch(&req.ctx, err, group_name, mount_epoch, route_epoch)
            }
        };
        for extent in &extents {
            if extent.block_id.data_handle_id != data_handle_id {
                return self.failure_from_error_with_route_epoch(
                    &req.ctx,
                    MetadataError::Internal(format!(
                        "Extent block data_handle_id {} does not match inode {} current_data_handle_id {}",
                        extent.block_id.data_handle_id, req.inode_id, data_handle_id
                    )),
                    group_name,
                    mount_epoch,
                    route_epoch,
                );
            }
        }
        let filtered_extents: Vec<Extent> = if let Some(range) = req.range {
            let range_end = match range.offset.checked_add(range.len) {
                Some(range_end) => range_end,
                None => {
                    return self.failure_from_error_with_route_epoch(
                        &req.ctx,
                        MetadataError::InvalidArgument(format!(
                            "range end overflows: offset={}, len={}",
                            range.offset, range.len
                        )),
                        group_name,
                        mount_epoch,
                        route_epoch,
                    );
                }
            };
            if range.len == 0 {
                Vec::new()
            } else {
                let mut filtered = Vec::with_capacity(extents.len());
                for extent in extents {
                    let extent_end = match extent.file_offset.checked_add(extent.len) {
                        Some(extent_end) => extent_end,
                        None => {
                            return self.failure_from_error_with_route_epoch(
                                &req.ctx,
                                MetadataError::Internal(format!(
                                    "extent range overflows: file_offset={}, len={}",
                                    extent.file_offset, extent.len
                                )),
                                group_name,
                                mount_epoch,
                                route_epoch,
                            );
                        }
                    };
                    if extent.file_offset < range_end && extent_end > range.offset {
                        filtered.push(extent);
                    }
                }
                filtered
            }
        } else {
            extents
        };

        let worker_manager = self.worker_manager.as_ref();
        let worker_lookup_group_name = if worker_manager.is_some() && !filtered_extents.is_empty() {
            Some(self.require_worker_lookup_group(
                &req.ctx,
                group_name.clone(),
                mount_epoch,
                route_epoch,
                "GetFileLayout",
            )?)
        } else {
            None
        };
        let caller = Self::caller_context_fields(&req.ctx);
        let mut locations = Vec::with_capacity(filtered_extents.len());
        for extent in &filtered_extents {
            let block_stamp = match extent.block_stamp {
                Some(stamp) => {
                    if stamp == 0 {
                        return self.failure_from_error_with_route_epoch(
                            &req.ctx,
                            MetadataError::InvalidArgument(format!(
                                "extent {} at file_offset {} has zero block_stamp",
                                extent.block_id, extent.file_offset
                            )),
                            group_name,
                            mount_epoch,
                            route_epoch,
                        );
                    }
                    stamp
                }
                None => {
                    return self.failure_from_error_with_route_epoch(
                        &req.ctx,
                        MetadataError::InvalidArgument(format!(
                            "extent {} at file_offset {} missing block_stamp",
                            extent.block_id, extent.file_offset
                        )),
                        group_name,
                        mount_epoch,
                        route_epoch,
                    );
                }
            };
            let effective_len = match extent.block_offset.checked_add(extent.len) {
                Some(len) => len,
                None => {
                    return self.failure_from_error_with_route_epoch(
                        &req.ctx,
                        MetadataError::Internal(format!(
                            "extent block range overflows: block_offset={}, len={}",
                            extent.block_offset, extent.len
                        )),
                        group_name,
                        mount_epoch,
                        route_epoch,
                    );
                }
            };
            let mut workers = Vec::new();
            if let (Some(worker_manager), Some(worker_lookup_group_name)) =
                (worker_manager, worker_lookup_group_name.as_ref())
            {
                let reported = worker_manager.reported_block_locations(worker_lookup_group_name, extent.block_id);
                let views = worker_manager.collect_worker_placement_views(worker_lookup_group_name);
                let usable_views: Vec<_> = views.into_iter().filter(Self::has_usable_read_endpoint).collect();
                let plan = PlacementPlanner.plan(
                    &PlacementRequest {
                        group_name: worker_lookup_group_name.clone(),
                        op: PlacementOp::Read,
                        block_id: extent.block_id,
                        block_stamp: Some(block_stamp),
                        layout,
                        caller: caller.clone(),
                        existing: reported.clone(),
                        exclude_workers: Vec::new(),
                        target_replicas: layout.replication,
                    },
                    &usable_views,
                );
                if plan.status == PlacementStatus::NoLiveReplica {
                    let (kind, message) = Self::classify_unavailable_read_location(
                        &req.ctx,
                        worker_lookup_group_name,
                        extent,
                        block_stamp,
                        &reported,
                        &usable_views,
                    );
                    return self.refresh_metadata_failure_with_hint(
                        &req.ctx,
                        kind,
                        message,
                        group_name,
                        mount_epoch,
                        route_epoch,
                        Some(RefreshHint {
                            worker_resolve_required: true,
                            ..RefreshHint::default()
                        }),
                    );
                }
                workers.reserve(plan.workers.len());
                for worker in plan.workers {
                    if let Ok(endpoint) = worker_endpoint_from_parts(
                        worker.worker_id,
                        worker.endpoint,
                        worker.worker_net_protocol,
                        worker.worker_run_id,
                    ) {
                        workers.push(endpoint);
                    }
                }
            }
            locations.push(FileBlockLocation {
                block_id: extent.block_id,
                file_offset: extent.file_offset,
                len: extent.len,
                block_stamp,
                workers,
                block_format_id: layout.block_format_id,
                block_size: u64::from(layout.block_size),
                chunk_size: layout.chunk_size,
                effective_len,
            });
        }

        self.success_with_route_epoch(
            &req.ctx,
            GetFileLayoutOutput {
                file_size: inode.attrs.size,
                content_revision,
                locations,
            },
            group_name,
            mount_epoch,
            route_epoch,
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
    use crate::service::filesystem::test_support::*;

    #[tokio::test]
    async fn get_file_layout_uses_inode_authority_without_reverse_owner_read() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(48);
        let group_name_value = group_name("g8");
        let inode_id = InodeId::new(480);
        let data_handle_id = DataHandleId::new(9480);
        let block_id = BlockId::new(data_handle_id, BlockIndex::new(0));
        let builder = filesystem_builder_with_mount(mount_id, 9, &group_name_value);
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
        let filesystem = builder
            .with_storage(Arc::clone(&storage))
            .with_worker_manager(worker_manager)
            .build();

        let mut attrs = FileAttrs::new();
        attrs.size = 512;
        let mut inode = Inode::new_file(inode_id, attrs, mount_id, data_handle_id);
        inode.data = beryl_types::fs::InodeData::File {
            extents: vec![beryl_types::fs::Extent {
                file_offset: 0,
                block_id,
                block_offset: 0,
                len: 512,
                content_revision: None,
                block_stamp: Some(41),
            }],
            content_revision: Some(1),
            lease_epoch: None,
        };
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();

        let mut ctx = request_context();
        ctx.caller = ctx.caller.with_caller_context(CallerContext {
            context: "host=127.0.0.2".to_string(),
            signature: None,
        });

        let success = filesystem
            .get_file_layout_resolved(GetFileLayoutInput {
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
        assert_eq!(
            location.block_format_id,
            beryl_types::BlockFormatId::CURRENT_FOR_NEW_FILE
        );
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
        let filesystem = filesystem_builder_with_mount(mount_id, 9, &group_name_value)
            .with_storage(Arc::clone(&storage))
            .with_worker_manager(Arc::new(WorkerManager::new(60)))
            .build();

        let mut attrs = FileAttrs::new();
        attrs.size = 512;
        let mut inode = Inode::new_file(inode_id, attrs, mount_id, data_handle_id);
        inode.data = beryl_types::fs::InodeData::File {
            extents: vec![beryl_types::fs::Extent {
                file_offset: 0,
                block_id,
                block_offset: 0,
                len: 512,
                content_revision: Some(1),
                block_stamp: Some(41),
            }],
            content_revision: Some(1),
            lease_epoch: None,
        };
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        let failure = filesystem
            .get_file_layout_resolved(GetFileLayoutInput {
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
        let builder = filesystem_builder_with_mount(mount_id, 9, &group_name_value);
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
        let filesystem = builder
            .with_storage(Arc::clone(&storage))
            .with_worker_manager(worker_manager)
            .build();

        let mut attrs = FileAttrs::new();
        attrs.size = 512;
        let mut inode = Inode::new_file(inode_id, attrs, mount_id, data_handle_id);
        inode.data = beryl_types::fs::InodeData::File {
            extents: vec![beryl_types::fs::Extent {
                file_offset: 0,
                block_id,
                block_offset: 0,
                len: 512,
                content_revision: Some(1),
                block_stamp: Some(41),
            }],
            content_revision: Some(1),
            lease_epoch: None,
        };
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        let failure = filesystem
            .get_file_layout_resolved(GetFileLayoutInput {
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
        let builder = filesystem_builder_with_mount(mount_id, 9, &group_name_value);
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
        let filesystem = builder
            .with_storage(Arc::clone(&storage))
            .with_worker_manager(worker_manager)
            .build();

        let mut attrs = FileAttrs::new();
        attrs.size = 512;
        let mut inode = Inode::new_file(inode_id, attrs, mount_id, data_handle_id);
        inode.data = beryl_types::fs::InodeData::File {
            extents: vec![beryl_types::fs::Extent {
                file_offset: 0,
                block_id,
                block_offset: 0,
                len: 512,
                content_revision: Some(1),
                block_stamp: Some(41),
            }],
            content_revision: Some(1),
            lease_epoch: None,
        };
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        let failure = filesystem
            .get_file_layout_resolved(GetFileLayoutInput {
                ctx: request_context(),
                inode_id,
                range: None,
                requested_data_handle_id: None,
                freshness: Freshness::default(),
            })
            .await
            .expect_err("mismatched reported block stamp must fail precisely");

        assert_refresh_metadata(&failure.error, ErrorKind::Worker(WorkerErrorKind::BlockStampMismatch));
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
        let builder = filesystem_builder_with_mount(mount_id, 9, &group_name_value);
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
        let filesystem = builder
            .with_storage(Arc::clone(&storage))
            .with_worker_manager(worker_manager)
            .build();

        let mut attrs = FileAttrs::new();
        attrs.size = 512;
        let mut inode = Inode::new_file(inode_id, attrs, mount_id, data_handle_id);
        inode.data = beryl_types::fs::InodeData::File {
            extents: vec![beryl_types::fs::Extent {
                file_offset: 0,
                block_id,
                block_offset: 0,
                len: 512,
                content_revision: Some(1),
                block_stamp: Some(41),
            }],
            content_revision: Some(1),
            lease_epoch: None,
        };
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        let failure = filesystem
            .get_file_layout_resolved(GetFileLayoutInput {
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
        let extent = beryl_types::fs::Extent {
            file_offset: 0,
            block_id,
            block_offset: 0,
            len: 512,
            content_revision: Some(1),
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
            supported_block_formats: vec![beryl_types::BlockFormatId::CURRENT_FOR_NEW_FILE],
        }];

        let (kind, message) = MetadataFileSystem::classify_unavailable_read_location(
            &request_context(),
            &group_name_value,
            &extent,
            41,
            &reported,
            &views,
        );

        assert_eq!(kind, ErrorKind::Worker(WorkerErrorKind::RunMismatch));
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
        let builder = filesystem_builder_without_mount();
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
        let filesystem = builder
            .with_storage(Arc::clone(&storage))
            .with_worker_manager(worker_manager)
            .build();

        let mut attrs = FileAttrs::new();
        attrs.size = 512;
        let mut inode = Inode::new_file(inode_id, attrs, mount_id, data_handle_id);
        inode.data = beryl_types::fs::InodeData::File {
            extents: vec![beryl_types::fs::Extent {
                file_offset: 0,
                block_id,
                block_offset: 0,
                len: 512,
                content_revision: Some(1),
                block_stamp: Some(41),
            }],
            content_revision: Some(1),
            lease_epoch: None,
        };
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        let failure = filesystem
            .get_file_layout_resolved(GetFileLayoutInput {
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
        let worker_run_id: beryl_types::WorkerRunId = "550e8400-e29b-41d4-a716-446655440052".parse().unwrap();
        let builder = filesystem_builder_with_mount(mount_id, 9, &served_group);
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
        let filesystem = builder
            .with_storage(Arc::clone(&storage))
            .with_worker_manager(worker_manager)
            .build();

        let mut attrs = FileAttrs::new();
        attrs.size = 512;
        let mut inode = Inode::new_file(inode_id, attrs, mount_id, data_handle_id);
        inode.data = beryl_types::fs::InodeData::File {
            extents: vec![beryl_types::fs::Extent {
                file_offset: 0,
                block_id,
                block_offset: 0,
                len: 512,
                content_revision: Some(1),
                block_stamp: Some(41),
            }],
            content_revision: Some(1),
            lease_epoch: None,
        };
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        let failure = filesystem
            .get_file_layout_resolved(GetFileLayoutInput {
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
        let filesystem = filesystem_builder_with_mount(mount_id, 9, &group_name("g8"))
            .with_storage(Arc::clone(&storage))
            .build();

        let mut attrs = FileAttrs::new();
        attrs.size = 512;
        let mut inode = Inode::new_file(inode_id, attrs, mount_id, data_handle_id);
        inode.data = beryl_types::fs::InodeData::File {
            extents: vec![beryl_types::fs::Extent {
                file_offset: 0,
                block_id,
                block_offset: 0,
                len: 512,
                content_revision: Some(1),
                block_stamp: None,
            }],
            content_revision: Some(1),
            lease_epoch: None,
        };
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        let failure = filesystem
            .get_file_layout_resolved(GetFileLayoutInput {
                ctx: request_context(),
                inode_id,
                range: None,
                requested_data_handle_id: None,
                freshness: Freshness::default(),
            })
            .await
            .expect_err("missing block_stamp must reject returned layout");

        assert_fail(&failure.error, ErrorKind::Fs(FsErrorCode::EInval));
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
        let filesystem = filesystem_builder_with_mount(mount_id, 9, &group_name("g8"))
            .with_storage(Arc::clone(&storage))
            .build();

        let mut attrs = FileAttrs::new();
        attrs.size = 512;
        let mut inode = Inode::new_file(inode_id, attrs, mount_id, data_handle_id);
        inode.data = beryl_types::fs::InodeData::File {
            extents: vec![beryl_types::fs::Extent {
                file_offset: 0,
                block_id,
                block_offset: 0,
                len: 512,
                content_revision: Some(1),
                block_stamp: Some(0),
            }],
            content_revision: Some(1),
            lease_epoch: Some(1),
        };
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        let failure = filesystem
            .get_file_layout_resolved(GetFileLayoutInput {
                ctx: request_context(),
                inode_id,
                range: None,
                requested_data_handle_id: None,
                freshness: Freshness::default(),
            })
            .await
            .expect_err("zero block_stamp must reject returned layout");

        assert_fail(&failure.error, ErrorKind::Fs(FsErrorCode::EInval));
        assert!(failure.error.message.contains("zero block_stamp"));
    }

    #[tokio::test]
    async fn list_status_rejects_stale_mount_epoch() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(71);
        let parent_inode_id = InodeId::new(710);
        let filesystem = filesystem_builder_with_mount(mount_id, 9, &group_name("g18"))
            .with_storage(Arc::clone(&storage))
            .build();
        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), mount_id))
            .unwrap();

        let failure = filesystem
            .read_dir_resolved(ReadDirInput {
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

        assert_refresh_metadata(
            &failure.error,
            ErrorKind::Metadata(MetadataErrorKind::MountEpochMismatch),
        );
        assert_eq!(failure.group_name, Some(group_name("g18")));
        assert_eq!(failure.mount_epoch, Some(9));
    }

    #[tokio::test]
    async fn list_status_returns_complete_child_metadata() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(711);
        let parent_inode_id = InodeId::new(7110);
        let child_inode_id = InodeId::new(7111);
        let filesystem = filesystem_builder_with_mount(mount_id, 9, &group_name("g18"))
            .with_storage(Arc::clone(&storage))
            .build();

        let parent = Inode::new_dir(parent_inode_id, FileAttrs::new(), mount_id);
        let mut child_attrs = FileAttrs::new();
        child_attrs.size = 128;
        child_attrs.mode = 0o600;
        let child = Inode::new_file(child_inode_id, child_attrs.clone(), mount_id, DataHandleId::new(7111));
        storage.put_inode(&parent).unwrap();
        storage.put_inode(&child).unwrap();
        storage.put_dentry(parent_inode_id, "child", child_inode_id).unwrap();

        let output = filesystem
            .read_dir_resolved(ReadDirInput {
                ctx: request_context(),
                parent_inode_id,
                cursor_key: None,
                max_entries: None,
                freshness: Freshness::default(),
            })
            .await
            .expect("list should succeed")
            .payload;

        assert_eq!(output.entries.len(), 1);
        let entry = &output.entries[0];
        assert_eq!(entry.name, "child");
        assert_eq!(entry.kind, Some(beryl_types::fs::InodeKind::File));
        assert_eq!(entry.attrs, Some(child_attrs));
        assert!(output.eof);
        assert!(output.next_cursor_key.is_empty());
    }

    #[tokio::test]
    async fn list_status_propagates_child_inode_load_error() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(712);
        let parent_inode_id = InodeId::new(7120);
        let child_inode_id = InodeId::new(7121);
        let filesystem = filesystem_builder_with_mount(mount_id, 9, &group_name("g18"))
            .with_storage(Arc::clone(&storage))
            .build();

        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), mount_id))
            .unwrap();
        storage
            .put_dentry(parent_inode_id, "bad-child", child_inode_id)
            .unwrap();
        let mut key = b"inode/".to_vec();
        key.extend_from_slice(&child_inode_id.to_be_bytes());
        storage
            .with_pinned_db(|db| {
                let inodes_cf = db.cf_handle("inodes").expect("inodes column family");
                db.put_cf(inodes_cf, key, b"not-json-inode")
                    .expect("write malformed child inode value");
                Ok(())
            })
            .unwrap();

        let failure = filesystem
            .read_dir_resolved(ReadDirInput {
                ctx: request_context(),
                parent_inode_id,
                cursor_key: None,
                max_entries: None,
                freshness: Freshness::default(),
            })
            .await
            .expect_err("child inode storage failure must reject ListStatus");

        assert_fail(&failure.error, ErrorKind::Fs(FsErrorCode::EInval));
        assert!(
            failure.error.message.contains("Failed to deserialize Inode"),
            "{}",
            failure.error.message
        );
    }

    #[tokio::test]
    async fn list_status_rejects_dangling_child_inode() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(713);
        let parent_inode_id = InodeId::new(7130);
        let child_inode_id = InodeId::new(7131);
        let filesystem = filesystem_builder_with_mount(mount_id, 9, &group_name("g18"))
            .with_storage(Arc::clone(&storage))
            .build();

        storage
            .put_inode(&Inode::new_dir(parent_inode_id, FileAttrs::new(), mount_id))
            .unwrap();
        storage
            .put_dentry(parent_inode_id, "missing-child", child_inode_id)
            .unwrap();

        let failure = filesystem
            .read_dir_resolved(ReadDirInput {
                ctx: request_context(),
                parent_inode_id,
                cursor_key: None,
                max_entries: None,
                freshness: Freshness::default(),
            })
            .await
            .expect_err("dangling child inode must reject ListStatus");

        assert_fail(&failure.error, ErrorKind::Fs(FsErrorCode::ENoEnt));
        assert!(
            failure.error.message.contains("missing-child")
                && failure.error.message.contains(&child_inode_id.to_string()),
            "{}",
            failure.error.message
        );
    }

    #[tokio::test]
    async fn get_locations_rejects_stale_route_epoch() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(73);
        let inode_id = InodeId::new(730);
        let data_handle_id = DataHandleId::new(9730);
        let filesystem = filesystem_builder_with_mount(mount_id, 9, &group_name("g20"))
            .with_storage(Arc::clone(&storage))
            .build();
        storage
            .put_inode(&Inode::new_file(inode_id, FileAttrs::new(), mount_id, data_handle_id))
            .unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        let failure = filesystem
            .get_file_layout_resolved(GetFileLayoutInput {
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

        assert_refresh_metadata(
            &failure.error,
            ErrorKind::Metadata(MetadataErrorKind::RouteEpochMismatch),
        );
        assert_eq!(failure.route_epoch, Some(1));
    }

    #[tokio::test]
    async fn read_success_returns_freshness_hints() {
        let dir = TempDir::new().unwrap();
        let storage = Arc::new(RocksDBStorage::create_for_format(dir.path()).unwrap());
        let mount_id = MountId::new(74);
        let inode_id = InodeId::new(740);
        let filesystem = filesystem_builder_with_mount(mount_id, 9, &group_name("g21"))
            .with_storage(Arc::clone(&storage))
            .build();
        storage
            .put_inode(&Inode::new_file(
                inode_id,
                FileAttrs::new(),
                mount_id,
                DataHandleId::new(9740),
            ))
            .unwrap();

        let success = filesystem
            .get_attr_resolved(GetAttrInput {
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
        let filesystem = filesystem_builder_with_mount(mount_id, 9, &group_name("g22"))
            .with_storage(Arc::clone(&storage))
            .build();
        storage
            .put_inode(&Inode::new_file(inode_id, FileAttrs::new(), mount_id, data_handle_id))
            .unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        let failure = filesystem
            .get_file_layout_resolved(GetFileLayoutInput {
                ctx: request_context(),
                inode_id,
                range: Some(FileRange {
                    offset: u64::MAX,
                    len: 1,
                }),
                requested_data_handle_id: None,
                freshness: Freshness::default(),
            })
            .await
            .expect_err("overflowing range must be rejected");

        assert_fail(&failure.error, ErrorKind::Fs(FsErrorCode::EInval));
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
        inode.data = beryl_types::fs::InodeData::File {
            extents: vec![beryl_types::fs::Extent {
                file_offset: 0,
                block_id,
                block_offset: 0,
                len: 512,
                content_revision: Some(4),
                block_stamp: Some(4),
            }],
            content_revision: Some(4),
            lease_epoch: Some(4),
        };
        let filesystem = filesystem_builder_with_mount(mount_id, 9, &group_name("g23"))
            .with_storage(Arc::clone(&storage))
            .build();
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        let success = filesystem
            .get_file_layout_resolved(GetFileLayoutInput {
                ctx: request_context(),
                inode_id,
                range: Some(FileRange { offset: 0, len: 0 }),
                requested_data_handle_id: None,
                freshness: Freshness::default(),
            })
            .await
            .expect("empty range should be stable");

        assert!(success.payload.locations.is_empty());
        assert_eq!(success.payload.file_size, 512);
        assert_eq!(success.payload.content_revision, Some(4));
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
        inode.data = beryl_types::fs::InodeData::File {
            extents: (0_u32..3)
                .map(|idx| beryl_types::fs::Extent {
                    file_offset: u64::from(idx) * 100,
                    block_id: BlockId::new(data_handle_id, BlockIndex::new(idx)),
                    block_offset: 0,
                    len: 100,
                    content_revision: Some(5),
                    block_stamp: Some(u64::from(idx) + 50),
                })
                .collect(),
            content_revision: Some(5),
            lease_epoch: Some(5),
        };
        let filesystem = filesystem_builder_with_mount(mount_id, 9, &group_name("g24"))
            .with_storage(Arc::clone(&storage))
            .build();
        storage.put_inode(&inode).unwrap();
        storage.put_layout(inode_id, FileLayout::new(4096, 4096, 1)).unwrap();
        storage.put_data_handle_owner(data_handle_id, inode_id).unwrap();

        let success = filesystem
            .get_file_layout_resolved(GetFileLayoutInput {
                ctx: request_context(),
                inode_id,
                range: Some(FileRange { offset: 50, len: 150 }),
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
        assert_eq!(success.payload.content_revision, Some(5));
    }
    #[tokio::test]
    async fn locations_return_content_revision() {
        let env = write_flow_env(64).await;
        seed_committed_content_revision(&env, 41, 900);
        publish_env_block_location(&env, BlockId::new(env.data_handle_id, BlockIndex::new(0)), 41, 1);

        let locations = env
            .filesystem
            .get_file_layout_resolved(GetFileLayoutInput {
                ctx: request_context(),
                inode_id: env.inode_id,
                range: None,
                requested_data_handle_id: Some(env.data_handle_id),
                freshness: Freshness::default(),
            })
            .await
            .expect("locations should succeed");

        assert_eq!(locations.payload.content_revision, Some(41));
    }

    #[tokio::test]
    async fn worker_report_does_not_change_content_revision() {
        let env = write_flow_env(0).await;
        let open = env
            .filesystem
            .open_write_inode(
                &request_context(),
                env.inode_id,
                Some(64),
                crate::inode_lease::WriteMode::Write,
                Freshness::default(),
            )
            .await
            .expect("open write should succeed");
        let key = open.payload;
        let target = add_block_for_key(&env.filesystem, &key, 64).await;
        let close = commit_for_key(
            &env.filesystem,
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

        let worker_manager = env.filesystem.worker_manager.as_ref().expect("worker manager");
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
            .filesystem
            .get_file_layout_resolved(GetFileLayoutInput {
                ctx: request_context(),
                inode_id: env.inode_id,
                range: None,
                requested_data_handle_id: Some(env.data_handle_id),
                freshness: Freshness::default(),
            })
            .await
            .expect("locations should succeed");

        assert_eq!(close.payload.content_revision, Some(1));
        assert_eq!(locations.payload.content_revision, Some(1));
        assert_eq!(stored_content_revision(&env.storage, env.inode_id), Some(1));
    }

    #[tokio::test]
    async fn get_locations_rejects_stale_state_watermark() {
        let env = write_flow_env(0).await;
        let open = env
            .filesystem
            .open_write_inode(
                &request_context(),
                env.inode_id,
                Some(64),
                crate::inode_lease::WriteMode::Write,
                Freshness::default(),
            )
            .await
            .expect("open write should succeed");
        let key = open.payload;
        let target = add_block_for_key(&env.filesystem, &key, 64).await;
        commit_for_key(
            &env.filesystem,
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
            .filesystem
            .raft_node
            .as_ref()
            .and_then(|raft_node| raft_node.get_last_applied_state_id())
            .expect("commit should advance applied state");
        let mut ctx = request_context();
        ctx.caller.state.push(beryl_types::GroupStateWatermark::new(
            group_name("g15"),
            beryl_types::RaftLogId {
                term: current_state.term,
                leader_node_id: current_state.leader_node_id,
                index: current_state.index + 1,
            },
        ));

        let failure = env
            .filesystem
            .get_file_layout_resolved(GetFileLayoutInput {
                ctx,
                inode_id: env.inode_id,
                range: None,
                requested_data_handle_id: Some(env.data_handle_id),
                freshness: Freshness::default(),
            })
            .await
            .expect_err("read should reject state watermark beyond local applied state");

        assert_refresh_metadata(&failure.error, ErrorKind::Metadata(MetadataErrorKind::StaleState));
    }
}
