// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! MetadataPathServiceProto implementation.
//!
//! This is a thin adapter layer that converts path-based requests to inode-based operations.
//! All FS semantics are delegated to MetadataFsServiceProto (fs.proto).

use super::extract_and_inject_context;
use super::{header_from_canonical_error, ok_header_from_request};
use crate::error::{to_canonical_fs, MetadataError, MetadataResult};
use crate::mount::MountTable;
use crate::path_resolver::{MountContext, PathResolver};
use crate::raft::RocksDBStorage;
use proto::metadata::metadata_fs_service_proto_server::MetadataFsServiceProto as FsServiceTrait;
use proto::metadata::metadata_path_service_proto_server::MetadataPathServiceProto;
use proto::metadata::*;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{instrument, warn};
use types::fs::FileAttrs;
use types::layout::FileLayout;

/// MetadataPathServiceProto implementation.
pub struct MetadataPathServiceImpl {
    path_resolver: PathResolver,
    fs_service: Arc<super::fs_service::MetadataFsServiceImpl>,
    metrics: Option<Arc<crate::metrics::MetadataMetrics>>,
}

macro_rules! response_with_header {
    ($resp:expr, $header:expr) => {{
        let mut resp = $resp;
        resp.header = Some($header);
        Ok(Response::new(resp))
    }};
}

macro_rules! error_response {
    ($resp_ty:ty, $header:expr) => {{
        response_with_header!(<$resp_ty>::default(), $header)
    }};
}

impl MetadataPathServiceImpl {
    pub fn new(
        mount_table: Arc<MountTable>,
        storage: Arc<RocksDBStorage>,
        fs_service: Arc<super::fs_service::MetadataFsServiceImpl>,
    ) -> Self {
        let path_resolver = PathResolver::new(mount_table.clone(), storage.clone());
        Self {
            path_resolver,
            fs_service,
            metrics: None,
        }
    }

    /// Set metrics for tracking (optional).
    pub fn with_metrics(mut self, metrics: Arc<crate::metrics::MetadataMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Validate mount_epoch for write operations.
    /// Returns error if mount_epoch mismatch (should be converted to NEED_REFRESH).
    fn validate_mount_epoch(
        &self,
        req_header: &Option<proto::common::RequestHeaderProto>,
        mount_ctx: &crate::path_resolver::MountContext,
    ) -> MetadataResult<()> {
        if let Some(header) = req_header {
            if let Some(client_mount_epoch) = header.mount_epoch {
                if client_mount_epoch != mount_ctx.mount_epoch {
                    return Err(MetadataError::MountEpochMismatch {
                        expected: mount_ctx.mount_epoch,
                        got: client_mount_epoch,
                        mount_id: Some(mount_ctx.mount_id),
                    });
                }
            }
        }
        Ok(())
    }

    fn header_or_ok(
        &self,
        req_header: &Option<proto::common::RequestHeaderProto>,
        fs_header: Option<proto::common::ResponseHeaderProto>,
        group_id: Option<u64>,
        mount_epoch: Option<u64>,
    ) -> proto::common::ResponseHeaderProto {
        fs_header.unwrap_or_else(|| ok_header_from_request(req_header, group_id, mount_epoch))
    }

    fn header_from_path_error(
        &self,
        req_header: &Option<proto::common::RequestHeaderProto>,
        err: MetadataError,
        mount_ctx: Option<&MountContext>,
    ) -> proto::common::ResponseHeaderProto {
        let (group_id, mount_epoch) = mount_ctx
            .map(|ctx| (Some(ctx.owner_group_id.as_raw()), Some(ctx.mount_epoch)))
            .unwrap_or((None, None));

        let canonical = to_canonical_fs(err);
        header_from_canonical_error(req_header, group_id, mount_epoch, &canonical)
    }

    /// Convert types FileAttrs to proto FileAttrsProto.
    fn file_attrs_to_proto(attrs: &FileAttrs) -> proto::fs::FileAttrsProto {
        proto::fs::FileAttrsProto {
            mode: attrs.mode,
            uid: attrs.uid,
            gid: attrs.gid,
            size: attrs.size,
            atime_ms: attrs.atime_ms,
            mtime_ms: attrs.mtime_ms,
            ctime_ms: attrs.ctime_ms,
            nlink: attrs.nlink,
        }
    }

    /// Convert proto FileAttrsProto to types FileAttrs.
    fn proto_to_file_attrs(attrs: Option<proto::fs::FileAttrsProto>) -> MetadataResult<FileAttrs> {
        let attrs = attrs.ok_or_else(|| MetadataError::InvalidArgument("Missing FileAttrs".to_string()))?;
        Ok(FileAttrs {
            mode: attrs.mode,
            uid: attrs.uid,
            gid: attrs.gid,
            size: attrs.size,
            atime_ms: attrs.atime_ms,
            mtime_ms: attrs.mtime_ms,
            ctime_ms: attrs.ctime_ms,
            nlink: attrs.nlink,
        })
    }

    /// Convert proto FileLayoutProto to types FileLayout.
    fn proto_to_file_layout(layout: Option<proto::common::FileLayoutProto>) -> MetadataResult<FileLayout> {
        let layout = layout.ok_or_else(|| MetadataError::InvalidArgument("Missing FileLayout".to_string()))?;
        Ok(FileLayout::new(
            layout.block_size,
            layout.chunk_size,
            layout.replication as u8,
        ))
    }
}

#[tonic::async_trait]
impl MetadataPathServiceProto for MetadataPathServiceImpl {
    #[instrument(skip(self), fields(call_id, client_id))]
    async fn lookup_path(
        &self,
        request: Request<LookupPathRequestProto>,
    ) -> Result<Response<LookupPathResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        // Resolve path to parent + name
        let resolved = match self.path_resolver.resolve_path(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(LookupPathResponseProto, resp_header);
            }
        };

        if resolved.inode_id.is_none() {
            let resp_header = self.header_from_path_error(
                &req.header,
                MetadataError::NotFound(format!("Path not found: {}", req.path)),
                Some(&resolved.mount_ctx),
            );
            return error_response!(LookupPathResponseProto, resp_header);
        }

        let fs_req = LookupRequestProto {
            header: req.header.clone(),
            parent_inode_id: Some(proto::fs::InodeIdProto {
                value: resolved.parent_inode_id.as_raw(),
            }),
            name: resolved.name,
        };

        let fs_resp = FsServiceTrait::lookup(self.fs_service.as_ref(), Request::new(fs_req)).await?;
        let LookupResponseProto { header, inode, attrs } = fs_resp.into_inner();

        let resp_header = self.header_or_ok(
            &req.header,
            header,
            Some(resolved.mount_ctx.owner_group_id.as_raw()),
            Some(resolved.mount_ctx.mount_epoch),
        );

        response_with_header!(
            LookupPathResponseProto {
                inode,
                attrs,
                ..Default::default()
            },
            resp_header
        )
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn get_attr_path(
        &self,
        request: Request<GetAttrPathRequestProto>,
    ) -> Result<Response<GetAttrPathResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        // Resolve path to inode
        let resolved = match self.path_resolver.resolve_inode(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(GetAttrPathResponseProto, resp_header);
            }
        };

        let fs_req = GetAttrRequestProto {
            header: req.header.clone(),
            inode_id: Some(proto::fs::InodeIdProto {
                value: resolved.inode_id.as_raw(),
            }),
        };

        let fs_resp = FsServiceTrait::get_attr(self.fs_service.as_ref(), Request::new(fs_req)).await?;
        let GetAttrResponseProto { header, attrs } = fs_resp.into_inner();

        let resp_header = self.header_or_ok(
            &req.header,
            header,
            Some(resolved.mount_ctx.owner_group_id.as_raw()),
            Some(resolved.mount_ctx.mount_epoch),
        );

        response_with_header!(
            GetAttrPathResponseProto {
                attrs,
                inode: None,
                ..Default::default()
            },
            resp_header
        )
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn mkdir_path(
        &self,
        request: Request<MkdirPathRequestProto>,
    ) -> Result<Response<MkdirPathResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        // Resolve path
        let resolved = match self.path_resolver.resolve_path(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(MkdirPathResponseProto, resp_header);
            }
        };

        // Validate mount_epoch
        if let Err(err) = self.validate_mount_epoch(&req.header, &resolved.mount_ctx) {
            warn!(
                path = %req.path,
                err = %err,
                "MkdirPath rejected: mount epoch mismatch (NEED_REFRESH)"
            );
            let resp_header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
            return error_response!(MkdirPathResponseProto, resp_header);
        }

        // Convert attrs
        let attrs = match Self::proto_to_file_attrs(req.attrs) {
            Ok(attrs) => attrs,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
                return error_response!(MkdirPathResponseProto, resp_header);
            }
        };

        // Call FS service Mkdir
        let fs_req = MkdirRequestProto {
            header: req.header.clone(),
            parent_inode_id: Some(proto::fs::InodeIdProto {
                value: resolved.parent_inode_id.as_raw(),
            }),
            name: resolved.name,
            attrs: Some(Self::file_attrs_to_proto(&attrs)),
        };

        let fs_resp = FsServiceTrait::mkdir(self.fs_service.as_ref(), Request::new(fs_req)).await?;
        let MkdirResponseProto { header, inode, attrs } = fs_resp.into_inner();

        let resp_header = self.header_or_ok(
            &req.header,
            header,
            Some(resolved.mount_ctx.owner_group_id.as_raw()),
            Some(resolved.mount_ctx.mount_epoch),
        );

        response_with_header!(
            MkdirPathResponseProto {
                inode,
                attrs,
                ..Default::default()
            },
            resp_header
        )
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn create_path(
        &self,
        request: Request<CreatePathRequestProto>,
    ) -> Result<Response<CreatePathResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        // Resolve path
        let resolved = match self.path_resolver.resolve_path(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(CreatePathResponseProto, resp_header);
            }
        };

        // Validate mount_epoch
        if let Err(err) = self.validate_mount_epoch(&req.header, &resolved.mount_ctx) {
            warn!(
                path = %req.path,
                err = %err,
                "CreatePath rejected: mount epoch mismatch (NEED_REFRESH)"
            );
            let resp_header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
            return error_response!(CreatePathResponseProto, resp_header);
        }

        // Convert attrs and layout
        let attrs = match Self::proto_to_file_attrs(req.attrs) {
            Ok(attrs) => attrs,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
                return error_response!(CreatePathResponseProto, resp_header);
            }
        };
        let layout = match Self::proto_to_file_layout(req.layout) {
            Ok(layout) => layout,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
                return error_response!(CreatePathResponseProto, resp_header);
            }
        };

        // Call FS service Create
        let fs_req = CreateRequestProto {
            header: req.header.clone(),
            parent_inode_id: Some(proto::fs::InodeIdProto {
                value: resolved.parent_inode_id.as_raw(),
            }),
            name: resolved.name,
            attrs: Some(Self::file_attrs_to_proto(&attrs)),
            layout: Some(proto::common::FileLayoutProto {
                block_size: layout.block_size,
                chunk_size: layout.chunk_size,
                replication: layout.replication as u32,
            }),
        };

        let fs_resp = FsServiceTrait::create(self.fs_service.as_ref(), Request::new(fs_req)).await?;
        let CreateResponseProto {
            header,
            inode,
            attrs,
            data_handle_id: _,
        } = fs_resp.into_inner();
        let inode_id = inode.as_ref().and_then(|i| i.inode_id.clone());

        let resp_header = self.header_or_ok(
            &req.header,
            header,
            Some(resolved.mount_ctx.owner_group_id.as_raw()),
            Some(resolved.mount_ctx.mount_epoch),
        );

        response_with_header!(
            CreatePathResponseProto {
                inode_id,
                inode,
                attrs,
                ..Default::default()
            },
            resp_header
        )
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn unlink_path(
        &self,
        request: Request<UnlinkPathRequestProto>,
    ) -> Result<Response<UnlinkPathResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        // Resolve path
        let resolved = match self.path_resolver.resolve_path(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(UnlinkPathResponseProto, resp_header);
            }
        };

        // Validate mount_epoch
        if let Err(err) = self.validate_mount_epoch(&req.header, &resolved.mount_ctx) {
            warn!(
                path = %req.path,
                err = %err,
                "UnlinkPath rejected: mount epoch mismatch (NEED_REFRESH)"
            );
            let resp_header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
            return error_response!(UnlinkPathResponseProto, resp_header);
        }

        // Call FS service Unlink
        let fs_req = UnlinkRequestProto {
            header: req.header.clone(),
            parent_inode_id: Some(proto::fs::InodeIdProto {
                value: resolved.parent_inode_id.as_raw(),
            }),
            name: resolved.name,
        };

        let fs_resp = FsServiceTrait::unlink(self.fs_service.as_ref(), Request::new(fs_req)).await?;
        let UnlinkResponseProto { header } = fs_resp.into_inner();

        let resp_header = self.header_or_ok(
            &req.header,
            header,
            Some(resolved.mount_ctx.owner_group_id.as_raw()),
            Some(resolved.mount_ctx.mount_epoch),
        );

        response_with_header!(UnlinkPathResponseProto::default(), resp_header)
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn rmdir_path(
        &self,
        request: Request<RmdirPathRequestProto>,
    ) -> Result<Response<RmdirPathResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        // Resolve path
        let resolved = match self.path_resolver.resolve_path(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(RmdirPathResponseProto, resp_header);
            }
        };

        // Validate mount_epoch
        if let Err(err) = self.validate_mount_epoch(&req.header, &resolved.mount_ctx) {
            warn!(
                path = %req.path,
                err = %err,
                "RmdirPath rejected: mount epoch mismatch (NEED_REFRESH)"
            );
            let resp_header = self.header_from_path_error(&req.header, err, Some(&resolved.mount_ctx));
            return error_response!(RmdirPathResponseProto, resp_header);
        }

        // Call FS service Rmdir
        let fs_req = RmdirRequestProto {
            header: req.header.clone(),
            parent_inode_id: Some(proto::fs::InodeIdProto {
                value: resolved.parent_inode_id.as_raw(),
            }),
            name: resolved.name,
        };

        let fs_resp = FsServiceTrait::rmdir(self.fs_service.as_ref(), Request::new(fs_req)).await?;
        let RmdirResponseProto { header } = fs_resp.into_inner();

        let resp_header = self.header_or_ok(
            &req.header,
            header,
            Some(resolved.mount_ctx.owner_group_id.as_raw()),
            Some(resolved.mount_ctx.mount_epoch),
        );

        response_with_header!(RmdirPathResponseProto::default(), resp_header)
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn rename_path(
        &self,
        request: Request<RenamePathRequestProto>,
    ) -> Result<Response<RenamePathResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        // Resolve both paths
        let (src_resolved, dst_resolved) = match self.path_resolver.resolve_rename(&req.src_path, &req.dst_path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(RenamePathResponseProto, resp_header);
            }
        };

        // Validate mount_epoch (both should be same mount at this point)
        if let Err(err) = self.validate_mount_epoch(&req.header, &src_resolved.mount_ctx) {
            warn!(
                src_path = %req.src_path,
                dst_path = %req.dst_path,
                err = %err,
                "RenamePath rejected: mount epoch mismatch (NEED_REFRESH)"
            );
            let resp_header = self.header_from_path_error(&req.header, err, Some(&src_resolved.mount_ctx));
            return error_response!(RenamePathResponseProto, resp_header);
        }

        // Call FS service Rename
        let fs_req = FsRenameRequestProto {
            header: req.header.clone(),
            src_parent_inode_id: Some(proto::fs::InodeIdProto {
                value: src_resolved.parent_inode_id.as_raw(),
            }),
            src_name: src_resolved.name,
            dst_parent_inode_id: Some(proto::fs::InodeIdProto {
                value: dst_resolved.parent_inode_id.as_raw(),
            }),
            dst_name: dst_resolved.name,
            flags: req.flags,
        };

        let fs_resp = FsServiceTrait::rename(self.fs_service.as_ref(), Request::new(fs_req)).await?;
        let FsRenameResponseProto { header } = fs_resp.into_inner();

        let resp_header = self.header_or_ok(
            &req.header,
            header,
            Some(src_resolved.mount_ctx.owner_group_id.as_raw()),
            Some(src_resolved.mount_ctx.mount_epoch),
        );

        response_with_header!(RenamePathResponseProto::default(), resp_header)
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn list_status_path(
        &self,
        request: Request<ListStatusPathRequestProto>,
    ) -> Result<Response<ListStatusPathResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        // Resolve path to inode
        let resolved = match self.path_resolver.resolve_inode(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(ListStatusPathResponseProto, resp_header);
            }
        };

        // For non-recursive listing, just call FS service ReadDir
        if !req.recursive {
            // Parse cursor (if provided)
            let cursor_key = if req.cursor.is_empty() {
                None
            } else {
                Some(req.cursor.as_slice())
            };

            let _max_entries = if req.limit == 0 { None } else { Some(req.limit as usize) };

            // Call FS service ReadDir
            let fs_req = ReadDirRequestProto {
                header: req.header.clone(),
                parent_inode_id: Some(proto::fs::InodeIdProto {
                    value: resolved.inode_id.as_raw(),
                }),
                cursor_key: cursor_key.map(|c| c.to_vec()).unwrap_or_default(),
                max_entries: req.limit,
            };

            let fs_resp = FsServiceTrait::read_dir(self.fs_service.as_ref(), Request::new(fs_req)).await?;

            let ReadDirResponseProto {
                header,
                entries,
                next_cursor_key,
                eof,
            } = fs_resp.into_inner();

            let resp_header = self.header_or_ok(
                &req.header,
                header,
                Some(resolved.mount_ctx.owner_group_id.as_raw()),
                Some(resolved.mount_ctx.mount_epoch),
            );

            response_with_header!(
                ListStatusPathResponseProto {
                    entries,
                    next_cursor: next_cursor_key,
                    eof,
                    ..Default::default()
                },
                resp_header
            )
        } else {
            // Recursive listing: TODO implement BFS/DFS with hard limits
            let resp_header = self.header_from_path_error(
                &req.header,
                MetadataError::NotSupported("Recursive listing not yet implemented".to_string()),
                None,
            );
            error_response!(ListStatusPathResponseProto, resp_header)
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn open_path(
        &self,
        request: Request<OpenPathRequestProto>,
    ) -> Result<Response<OpenPathResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        // Resolve path to inode
        let resolved = match self.path_resolver.resolve_inode(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(OpenPathResponseProto, resp_header);
            }
        };

        // Call FS service Open
        let fs_req = OpenRequestProto {
            header: req.header.clone(),
            inode_id: Some(proto::fs::InodeIdProto {
                value: resolved.inode_id.as_raw(),
            }),
            flags: req.flags,
        };

        let fs_resp = FsServiceTrait::open(self.fs_service.as_ref(), Request::new(fs_req)).await?;

        let OpenResponseProto { header, file_handle } = fs_resp.into_inner();

        let resp_header = self.header_or_ok(
            &req.header,
            header,
            Some(resolved.mount_ctx.owner_group_id.as_raw()),
            Some(resolved.mount_ctx.mount_epoch),
        );

        response_with_header!(
            OpenPathResponseProto {
                file_handle,
                ..Default::default()
            },
            resp_header
        )
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn release_path(
        &self,
        request: Request<ReleasePathRequestProto>,
    ) -> Result<Response<ReleasePathResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        // Call FS service Release
        let fs_req = ReleaseRequestProto {
            header: req.header.clone(),
            file_handle: req.file_handle,
        };

        let fs_resp = FsServiceTrait::release(self.fs_service.as_ref(), Request::new(fs_req)).await?;
        let ReleaseResponseProto { header } = fs_resp.into_inner();

        let resp_header = self.header_or_ok(&req.header, header, None, None);

        response_with_header!(ReleasePathResponseProto::default(), resp_header)
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn fsync_path(
        &self,
        request: Request<FsyncPathRequestProto>,
    ) -> Result<Response<FsyncPathResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        // Handle path-based or handle-based fsync
        match req.target {
            Some(proto::metadata::fsync_path_request_proto::Target::Path(path)) => {
                // Resolve path to inode
                let resolved = match self.path_resolver.resolve_inode(&path) {
                    Ok(resolved) => resolved,
                    Err(err) => {
                        let resp_header = self.header_from_path_error(&req.header, err, None);
                        return error_response!(FsyncPathResponseProto, resp_header);
                    }
                };

                // Call FS service Fsync
                let fs_req = FsyncRequestProto {
                    header: req.header.clone(),
                    inode_id: Some(proto::fs::InodeIdProto {
                        value: resolved.inode_id.as_raw(),
                    }),
                    flags: req.flags,
                    file_handle: None,
                    lease_id: None,
                    lease_epoch: None,
                    fencing_token: None,
                    route_epoch: req.header.as_ref().and_then(|h| h.mount_epoch),
                    worker_epoch: None,
                    target_size: None,
                };

                let fs_resp = FsServiceTrait::fsync(self.fs_service.as_ref(), Request::new(fs_req)).await?;
                let fs_resp_inner = fs_resp.into_inner();
                let resp_header = self.header_or_ok(
                    &req.header,
                    fs_resp_inner.header,
                    Some(resolved.mount_ctx.owner_group_id.as_raw()),
                    Some(resolved.mount_ctx.mount_epoch),
                );
                return response_with_header!(FsyncPathResponseProto::default(), resp_header);
            }
            Some(proto::metadata::fsync_path_request_proto::Target::FileHandle(_handle)) => {
                // Handle-based fsync: TODO implement when FS service supports it
                let resp_header = self.header_from_path_error(
                    &req.header,
                    MetadataError::NotSupported("Handle-based fsync not yet implemented".to_string()),
                    None,
                );
                return error_response!(FsyncPathResponseProto, resp_header);
            }
            None => {
                let resp_header = self.header_from_path_error(
                    &req.header,
                    MetadataError::InvalidArgument("Either path or file_handle must be provided".to_string()),
                    None,
                );
                return error_response!(FsyncPathResponseProto, resp_header);
            }
        }
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn hsync_path(
        &self,
        request: Request<HsyncPathRequestProto>,
    ) -> Result<Response<HsyncPathResponseProto>, Status> {
        let req = request.into_inner();
        let inner = match req.fsync {
            Some(inner) => inner,
            None => {
                let resp_header = self.header_from_path_error(
                    &None,
                    MetadataError::InvalidArgument("missing fsync".to_string()),
                    None,
                );
                return error_response!(HsyncPathResponseProto, resp_header);
            }
        };
        let fallback_header = inner.header.clone();
        let resp = self.fsync_path(Request::new(inner)).await?;
        response_with_header!(
            HsyncPathResponseProto::default(),
            resp.into_inner()
                .header
                .unwrap_or_else(|| ok_header_from_request(&fallback_header, None, None))
        )
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn hflush_path(
        &self,
        request: Request<HflushPathRequestProto>,
    ) -> Result<Response<HflushPathResponseProto>, Status> {
        let req = request.into_inner();
        let inner = match req.fsync {
            Some(inner) => inner,
            None => {
                let resp_header = self.header_from_path_error(
                    &None,
                    MetadataError::InvalidArgument("missing fsync".to_string()),
                    None,
                );
                return error_response!(HflushPathResponseProto, resp_header);
            }
        };
        let fallback_header = inner.header.clone();
        let resp = self.fsync_path(Request::new(inner)).await?;
        response_with_header!(
            HflushPathResponseProto::default(),
            resp.into_inner()
                .header
                .unwrap_or_else(|| ok_header_from_request(&fallback_header, None, None))
        )
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn truncate_path(
        &self,
        request: Request<TruncatePathRequestProto>,
    ) -> Result<Response<TruncatePathResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        // Resolve path to inode
        let resolved = match self.path_resolver.resolve_inode(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(TruncatePathResponseProto, resp_header);
            }
        };

        // Call FS service Truncate
        let fs_req = TruncateRequestProto {
            header: req.header.clone(),
            inode_id: Some(proto::fs::InodeIdProto {
                value: resolved.inode_id.as_raw(),
            }),
            new_size: req.new_size,
            lease_id: req.lease_id,
            lease_epoch: req.lease_epoch,
        };

        let fs_resp = FsServiceTrait::truncate(self.fs_service.as_ref(), Request::new(fs_req)).await?;
        let TruncateResponseProto { header, new_size } = fs_resp.into_inner();

        let resp_header = self.header_or_ok(
            &req.header,
            header,
            Some(resolved.mount_ctx.owner_group_id.as_raw()),
            Some(resolved.mount_ctx.mount_epoch),
        );

        response_with_header!(
            TruncatePathResponseProto {
                new_size,
                ..Default::default()
            },
            resp_header
        )
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn set_xattr_path(
        &self,
        request: Request<SetXattrPathRequestProto>,
    ) -> Result<Response<SetXattrPathResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);
        let resolved = match self.path_resolver.resolve_inode(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(SetXattrPathResponseProto, resp_header);
            }
        };
        let fs_req = SetXattrRequestProto {
            header: req.header.clone(),
            inode_id: Some(proto::fs::InodeIdProto {
                value: resolved.inode_id.as_raw(),
            }),
            name: req.name,
            value: req.value,
            create: req.create,
            replace: req.replace,
        };
        let resp = FsServiceTrait::set_xattr(self.fs_service.as_ref(), Request::new(fs_req)).await?;
        let resp_inner = resp.into_inner();
        let resp_header = self.header_or_ok(
            &req.header,
            resp_inner.header,
            Some(resolved.mount_ctx.owner_group_id.as_raw()),
            Some(resolved.mount_ctx.mount_epoch),
        );
        response_with_header!(SetXattrPathResponseProto::default(), resp_header)
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn get_xattr_path(
        &self,
        request: Request<GetXattrPathRequestProto>,
    ) -> Result<Response<GetXattrPathResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);
        let resolved = match self.path_resolver.resolve_inode(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(GetXattrPathResponseProto, resp_header);
            }
        };
        let fs_req = GetXattrRequestProto {
            header: req.header.clone(),
            inode_id: Some(proto::fs::InodeIdProto {
                value: resolved.inode_id.as_raw(),
            }),
            name: req.name,
        };
        let resp = FsServiceTrait::get_xattr(self.fs_service.as_ref(), Request::new(fs_req)).await?;
        let GetXattrResponseProto { header, value } = resp.into_inner();
        let resp_header = self.header_or_ok(
            &req.header,
            header,
            Some(resolved.mount_ctx.owner_group_id.as_raw()),
            Some(resolved.mount_ctx.mount_epoch),
        );
        response_with_header!(
            GetXattrPathResponseProto {
                value,
                ..Default::default()
            },
            resp_header
        )
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn list_xattr_path(
        &self,
        request: Request<ListXattrPathRequestProto>,
    ) -> Result<Response<ListXattrPathResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);
        let resolved = match self.path_resolver.resolve_inode(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(ListXattrPathResponseProto, resp_header);
            }
        };
        let fs_req = ListXattrRequestProto {
            header: req.header.clone(),
            inode_id: Some(proto::fs::InodeIdProto {
                value: resolved.inode_id.as_raw(),
            }),
        };
        let resp = FsServiceTrait::list_xattr(self.fs_service.as_ref(), Request::new(fs_req)).await?;
        let ListXattrResponseProto { header, names } = resp.into_inner();
        let resp_header = self.header_or_ok(
            &req.header,
            header,
            Some(resolved.mount_ctx.owner_group_id.as_raw()),
            Some(resolved.mount_ctx.mount_epoch),
        );
        response_with_header!(
            ListXattrPathResponseProto {
                names,
                ..Default::default()
            },
            resp_header
        )
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn remove_xattr_path(
        &self,
        request: Request<RemoveXattrPathRequestProto>,
    ) -> Result<Response<RemoveXattrPathResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);
        let resolved = match self.path_resolver.resolve_inode(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(RemoveXattrPathResponseProto, resp_header);
            }
        };
        let fs_req = RemoveXattrRequestProto {
            header: req.header.clone(),
            inode_id: Some(proto::fs::InodeIdProto {
                value: resolved.inode_id.as_raw(),
            }),
            name: req.name,
        };
        let resp = FsServiceTrait::remove_xattr(self.fs_service.as_ref(), Request::new(fs_req)).await?;
        let resp_inner = resp.into_inner();
        let resp_header = self.header_or_ok(
            &req.header,
            resp_inner.header,
            Some(resolved.mount_ctx.owner_group_id.as_raw()),
            Some(resolved.mount_ctx.mount_epoch),
        );
        response_with_header!(RemoveXattrPathResponseProto::default(), resp_header)
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn get_file_block_locations_path(
        &self,
        request: Request<GetFileBlockLocationsPathRequestProto>,
    ) -> Result<Response<GetFileBlockLocationsPathResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);
        let resolved = match self.path_resolver.resolve_inode(&req.path) {
            Ok(resolved) => resolved,
            Err(err) => {
                let resp_header = self.header_from_path_error(&req.header, err, None);
                return error_response!(GetFileBlockLocationsPathResponseProto, resp_header);
            }
        };
        let fs_req = GetFileLayoutRequestProto {
            header: req.header.clone(),
            inode_id: Some(proto::fs::InodeIdProto {
                value: resolved.inode_id.as_raw(),
            }),
            range: req.range,
        };
        let resp = FsServiceTrait::get_file_layout(self.fs_service.as_ref(), Request::new(fs_req)).await?;
        let GetFileLayoutResponseProto {
            header,
            extents,
            file_size,
            locations,
        } = resp.into_inner();
        let resp_header = self.header_or_ok(
            &req.header,
            header,
            Some(resolved.mount_ctx.owner_group_id.as_raw()),
            Some(resolved.mount_ctx.mount_epoch),
        );
        response_with_header!(
            GetFileBlockLocationsPathResponseProto {
                extents,
                file_size,
                locations,
                ..Default::default()
            },
            resp_header
        )
    }
}
