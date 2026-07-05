// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use super::{FsCore, StaleStateStatus};
use crate::error::MetadataError;
use crate::observe;
use crate::placement::{
    PlacementOp, PlacementPlanner, PlacementRequest, PlacementStatus, ReportedBlockLocation, WorkerPlacementView,
};
use crate::service::core_util::{need_refresh_core_failure, worker_endpoint_from_parts};
use crate::service::domain::{
    CoreFailure, CoreResult, Freshness, GetAttrInput, GetAttrOutput, GetFileLayoutInput, GetFileLayoutOutput,
    InodeMountGuardInputs, ReadDirEntry, ReadDirInput, ReadDirOutput, RequestContext,
};
use common::error::canonical::{RefreshHint, RefreshReason};
use common::header::CallerContextFields;
use common::header::RpcErrorCode;
use std::time::Instant;
use types::fs::{Extent, InodeId};
use types::ids::DataHandleId;
use types::{FileBlockLocation, GroupName};

impl FsCore {
    async fn validate_read_freshness_for_mount(
        &self,
        req_ctx: &RequestContext,
        freshness: Freshness,
        mount_id: types::ids::MountId,
        intent: &str,
    ) -> Result<(Option<GroupName>, Option<u64>, Option<u64>), CoreFailure> {
        let (group_name, mount_epoch) = self.validate_mount_epoch_for_mount(req_ctx, freshness, mount_id)?;
        let route_epoch = self
            .validate_route_epoch(req_ctx, freshness, group_name.clone(), mount_epoch, intent)
            .await?;
        match self.validate_stale_state(
            req_ctx,
            self.raft_node
                .as_ref()
                .and_then(|raft_node| raft_node.get_last_applied_state_id()),
            group_name.clone(),
            mount_epoch,
        )? {
            StaleStateStatus::Ready => Ok((group_name, mount_epoch, route_epoch)),
            StaleStateStatus::UnknownLastApplied => Err(need_refresh_core_failure(
                req_ctx,
                RpcErrorCode::StaleState,
                RefreshReason::StaleState,
                "local applied state is unavailable for read freshness validation",
                group_name,
                mount_epoch,
                route_epoch,
                None,
            )),
        }
    }

    fn file_version_for_inode(inode: &types::fs::Inode) -> Option<u64> {
        match &inode.data {
            types::fs::InodeData::File { file_version, .. } => *file_version,
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

    pub(super) fn classify_unavailable_read_location(
        req: &RequestContext,
        worker_lookup_group_name: &GroupName,
        extent: &Extent,
        block_stamp: u64,
        reported: &[ReportedBlockLocation],
        views: &[WorkerPlacementView],
    ) -> (RpcErrorCode, RefreshReason, String) {
        let matching_stamp = reported
            .iter()
            .filter(|location| location.block_stamp == block_stamp)
            .collect::<Vec<_>>();
        if !reported.is_empty() && matching_stamp.is_empty() {
            return (
                RpcErrorCode::BlockStampMismatch,
                RefreshReason::BlockStampMismatch,
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
                        RpcErrorCode::WorkerRunMismatch,
                        RefreshReason::WorkerRunMismatch,
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
            RpcErrorCode::BlockLocationUnavailable,
            RefreshReason::BlockLocationUnavailable,
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

    pub(crate) async fn plan_inode_mount(
        &self,
        req_ctx: &RequestContext,
        inode_id: InodeId,
    ) -> CoreResult<InodeMountGuardInputs> {
        let storage = match self.storage_for_ctx(req_ctx) {
            Ok(storage) => storage,
            Err(failure) => return Err(failure),
        };
        let inode = match storage.get_inode(inode_id) {
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
                inode_id,
                mount_id: inode.mount_id,
            },
            None,
            None,
        )
    }

    pub(crate) async fn execute_get_attr(&self, req: GetAttrInput) -> CoreResult<GetAttrOutput> {
        let started = Instant::now();
        let result = async {
            let storage = match self.storage.as_ref() {
                Some(storage) => storage,
                None => {
                    return self.failure_from_error(
                        &req.ctx,
                        MetadataError::Internal("Storage not available".to_string()),
                        None,
                        None,
                    );
                }
            };

            let inode = match storage.get_inode(req.inode_id) {
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

    pub(crate) async fn inode_for_data_handle(
        &self,
        req_ctx: &RequestContext,
        data_handle_id: DataHandleId,
    ) -> CoreResult<InodeId> {
        let storage = match self.storage.as_ref() {
            Some(storage) => storage,
            None => {
                return self.failure_from_error(
                    req_ctx,
                    MetadataError::Internal("Storage not available".to_string()),
                    None,
                    None,
                );
            }
        };
        let inode_id = match storage.get_inode_by_data_handle(data_handle_id) {
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

    pub(crate) async fn execute_read_dir(&self, req: ReadDirInput) -> CoreResult<ReadDirOutput> {
        let started = Instant::now();
        let result = async {
            let storage = match self.storage.as_ref() {
                Some(storage) => storage,
                None => {
                    return self.failure_from_error(
                        &req.ctx,
                        MetadataError::Internal("Storage not available".to_string()),
                        None,
                        None,
                    );
                }
            };

            let parent_inode = match storage.get_inode(req.parent_inode_id) {
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
                match storage.list_dentries_with_cursor(req.parent_inode_id, cursor_key, req.max_entries) {
                    Ok(result) => result,
                    Err(err) => return self.failure_from_error(&req.ctx, err, group_name, mount_epoch),
                };

            let mut dir_entries = Vec::with_capacity(entries.len());
            for (name, child_inode_id) in entries {
                let child_inode = match storage.get_inode(child_inode_id) {
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
                    inode_id: child_inode_id,
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

    pub(crate) async fn execute_get_file_layout(&self, req: GetFileLayoutInput) -> CoreResult<GetFileLayoutOutput> {
        let started = Instant::now();
        let result = async {
        let storage = match self.storage.as_ref() {
            Some(storage) => storage,
            None => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::Internal("Storage not available".to_string()),
                    None,
                    None,
                );
            }
        };

        let inode = match storage.get_inode(req.inode_id) {
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

        let file_version = Self::file_version_for_inode(&inode);
        let extents = match &inode.data {
            types::fs::InodeData::File { extents, .. } => extents.clone(),
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
        if let Err(err) = storage.validate_data_handle_owner(data_handle_id, Some(req.inode_id)) {
            return self.failure_from_error_with_route_epoch(&req.ctx, err, group_name, mount_epoch, route_epoch);
        }
        let layout = match storage.get_layout(req.inode_id) {
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
                    let (rpc_code, reason, message) = Self::classify_unavailable_read_location(
                        &req.ctx,
                        worker_lookup_group_name,
                        extent,
                        block_stamp,
                        &reported,
                        &usable_views,
                    );
                    return self.need_refresh_failure_with_hint(
                        &req.ctx,
                        rpc_code,
                        reason,
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
                extents: filtered_extents,
                file_size: inode.attrs.size,
                file_version,
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

fn record_fs_read_result<T>(operation: &str, started: Instant, result: &CoreResult<T>) {
    match result {
        Ok(_) => observe::record_fs_op(operation, "ok", "none", started.elapsed().as_secs_f64()),
        Err(failure) => observe::record_fs_op(
            operation,
            "error",
            observe::canonical_error_kind(&failure.error),
            started.elapsed().as_secs_f64(),
        ),
    }
}
