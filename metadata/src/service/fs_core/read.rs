// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

use super::FsCore;
use crate::error::MetadataError;
use crate::service::domain::{
    AccessInput, AccessOutput, CoreResult, FileBlockLocation, GetAttrInput, GetAttrOutput, GetFileLayoutInput,
    GetFileLayoutOutput, GetXattrInput, GetXattrOutput, InodeMountGuardInputs, LinkInput, LinkOutput, ListXattrInput,
    ListXattrOutput, LookupInput, LookupOutput, OpenInput, OpenOutput, ReadDirEntry, ReadDirInput, ReadDirOutput,
    ReadlinkInput, ReadlinkOutput, RequestContext, StatFsInput, StatFsOutput, SymlinkInput, SymlinkOutput, WorkerHint,
};
use types::fs::{Extent, InodeId};

impl FsCore {
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

    pub(crate) async fn execute_lookup(&self, req: LookupInput) -> CoreResult<LookupOutput> {
        let storage = match self.storage_for_ctx(&req.ctx) {
            Ok(storage) => storage,
            Err(failure) => return Err(failure),
        };

        let child_inode_id = match storage.get_dentry(req.parent_inode_id, &req.name) {
            Ok(Some(child_inode_id)) => child_inode_id,
            Ok(None) => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::NotFound(format!(
                        "Entry not found: parent={}, name={}",
                        req.parent_inode_id, req.name
                    )),
                    None,
                    None,
                );
            }
            Err(err) => return self.failure_from_error(&req.ctx, err, None, None),
        };

        let child_inode = match storage.get_inode(child_inode_id) {
            Ok(Some(child_inode)) => child_inode,
            Ok(None) => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::NotFound(format!("Inode not found: {}", child_inode_id)),
                    None,
                    None,
                );
            }
            Err(err) => return self.failure_from_error(&req.ctx, err, None, None),
        };

        let (group_id, mount_epoch) = self.mount_hints_for_mount(child_inode.mount_id);
        let route_epoch = self.authoritative_route_epoch().await;
        self.success_with_route_epoch(
            &req.ctx,
            LookupOutput { inode: child_inode },
            group_id,
            mount_epoch,
            route_epoch,
        )
    }

    pub(crate) async fn execute_get_attr(&self, req: GetAttrInput) -> CoreResult<GetAttrOutput> {
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

        let (group_id, mount_epoch) = self.mount_hints_for_mount(inode.mount_id);
        let route_epoch = self.authoritative_route_epoch().await;
        self.success_with_route_epoch(
            &req.ctx,
            GetAttrOutput {
                attrs: inode.attrs.clone(),
            },
            group_id,
            mount_epoch,
            route_epoch,
        )
    }

    pub(crate) async fn execute_read_dir(&self, req: ReadDirInput) -> CoreResult<ReadDirOutput> {
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

        let cursor_key = req.cursor_key.as_deref();
        let (entries, next_cursor_key, eof) =
            match storage.list_dentries_with_cursor(req.parent_inode_id, cursor_key, req.max_entries) {
                Ok(result) => result,
                Err(err) => return self.failure_from_error(&req.ctx, err, None, None),
            };

        let mut dir_entries = Vec::with_capacity(entries.len());
        for (name, child_inode_id) in entries {
            let child_inode = storage.get_inode(child_inode_id).ok().flatten();
            dir_entries.push(ReadDirEntry {
                name,
                inode_id: child_inode_id,
                kind: child_inode.as_ref().map(|i| i.kind),
                attrs: child_inode.as_ref().map(|i| i.attrs.clone()),
            });
        }

        let (group_id, mount_epoch) = self.mount_hints_for_mount(parent_inode.mount_id);
        let route_epoch = self.authoritative_route_epoch().await;
        self.success_with_route_epoch(
            &req.ctx,
            ReadDirOutput {
                entries: dir_entries,
                next_cursor_key: next_cursor_key.unwrap_or_default(),
                eof,
            },
            group_id,
            mount_epoch,
            route_epoch,
        )
    }

    pub(crate) async fn execute_open(&self, req: OpenInput) -> CoreResult<OpenOutput> {
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

        let _flags = req.flags;
        let (group_id, mount_epoch) = self.mount_hints_for_mount(inode.mount_id);
        let route_epoch = self.authoritative_route_epoch().await;
        self.success_with_route_epoch(
            &req.ctx,
            OpenOutput { file_handle: 0 },
            group_id,
            mount_epoch,
            route_epoch,
        )
    }

    pub(crate) async fn execute_get_xattr(&self, req: GetXattrInput) -> CoreResult<GetXattrOutput> {
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

        let value = match inode.xattrs.get(&req.name) {
            Some(value) => value.clone(),
            None => {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::NotFound(format!("xattr not found: {}", req.name)),
                    None,
                    None,
                );
            }
        };

        let (group_id, mount_epoch) = self.mount_hints_for_mount(inode.mount_id);
        self.success(&req.ctx, GetXattrOutput { value }, group_id, mount_epoch)
    }

    pub(crate) async fn execute_list_xattr(&self, req: ListXattrInput) -> CoreResult<ListXattrOutput> {
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

        let names = inode.xattrs.keys().cloned().collect::<Vec<_>>();
        let (group_id, mount_epoch) = self.mount_hints_for_mount(inode.mount_id);
        self.success(&req.ctx, ListXattrOutput { names }, group_id, mount_epoch)
    }

    pub(crate) async fn execute_get_file_layout(&self, req: GetFileLayoutInput) -> CoreResult<GetFileLayoutOutput> {
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

        let (group_id, mount_epoch) = match self.validate_mount_epoch_for_mount(&req.ctx, req.freshness, inode.mount_id)
        {
            Ok(hints) => hints,
            Err(err) => return Err(err),
        };

        let route_epoch = match self
            .validate_route_epoch(&req.ctx, req.freshness, group_id, mount_epoch, "GetFileLayout")
            .await
        {
            Ok(route_epoch) => route_epoch,
            Err(err) => return Err(err),
        };

        let extents = match &inode.data {
            types::fs::InodeData::File { extents, .. } => extents.clone(),
            _ => Vec::new(),
        };
        let data_handle_id = inode.current_data_handle_id;
        if data_handle_id.as_raw() == 0 {
            return self.failure_from_error(
                &req.ctx,
                MetadataError::Internal(format!("File inode {} is missing current_data_handle_id", req.inode_id)),
                group_id,
                mount_epoch,
            );
        }
        for extent in &extents {
            if extent.block_id.data_handle_id != data_handle_id {
                return self.failure_from_error(
                    &req.ctx,
                    MetadataError::Internal(format!(
                        "Extent block data_handle_id {} does not match inode {} current_data_handle_id {}",
                        extent.block_id.data_handle_id, req.inode_id, data_handle_id
                    )),
                    group_id,
                    mount_epoch,
                );
            }
        }

        let filtered_extents: Vec<Extent> = if let Some(range) = req.range {
            extents
                .into_iter()
                .filter(|e| {
                    let extent_end = e.file_offset + e.len;
                    let range_end = range.offset + range.len;
                    e.file_offset < range_end && extent_end > range.offset
                })
                .collect()
        } else {
            extents
        };

        let worker_manager = self.worker_manager.as_ref();
        let locations: Vec<FileBlockLocation> = filtered_extents
            .iter()
            .map(|extent| {
                let mut workers = Vec::new();
                if let Some(worker_manager) = worker_manager {
                    let mut worker_ids = worker_manager.get_block_locations(extent.block_id);
                    worker_ids.sort_by_key(|worker_id| worker_id.as_raw());
                    workers.reserve(worker_ids.len());
                    for worker_id in worker_ids {
                        if let Some(descriptor) = worker_manager.get_descriptor(worker_id) {
                            workers.push(WorkerHint {
                                worker_id,
                                endpoint: descriptor.address,
                                net_transport_kind: descriptor.net_transport_kind,
                                worker_epoch: descriptor.worker_epoch,
                            });
                        }
                    }
                }
                let worker_epoch = workers.iter().map(|worker| worker.worker_epoch).max();
                FileBlockLocation {
                    block_id: extent.block_id,
                    file_offset: extent.file_offset,
                    len: extent.len,
                    workers,
                    worker_epoch,
                }
            })
            .collect();

        self.success_with_route_epoch(
            &req.ctx,
            GetFileLayoutOutput {
                extents: filtered_extents,
                file_size: inode.attrs.size,
                locations,
            },
            group_id,
            mount_epoch,
            route_epoch,
        )
    }

    pub(crate) async fn execute_stat_fs(&self, req: StatFsInput) -> CoreResult<StatFsOutput> {
        self.failure_from_error(
            &req.ctx,
            MetadataError::NotSupported("StatFs not yet implemented".to_string()),
            None,
            None,
        )
    }

    pub(crate) async fn execute_access(&self, req: AccessInput) -> CoreResult<AccessOutput> {
        let _ = req.mode;
        self.failure_from_error(
            &req.ctx,
            MetadataError::NotSupported("Access not yet implemented".to_string()),
            None,
            None,
        )
    }

    pub(crate) async fn execute_symlink(&self, req: SymlinkInput) -> CoreResult<SymlinkOutput> {
        self.failure_from_error(
            &req.ctx,
            MetadataError::NotSupported("Symlink not yet implemented".to_string()),
            None,
            None,
        )
    }

    pub(crate) async fn execute_readlink(&self, req: ReadlinkInput) -> CoreResult<ReadlinkOutput> {
        self.failure_from_error(
            &req.ctx,
            MetadataError::NotSupported("Readlink not yet implemented".to_string()),
            None,
            None,
        )
    }

    pub(crate) async fn execute_link(&self, req: LinkInput) -> CoreResult<LinkOutput> {
        self.failure_from_error(
            &req.ctx,
            MetadataError::NotSupported("Link not yet implemented".to_string()),
            None,
            None,
        )
    }
}
