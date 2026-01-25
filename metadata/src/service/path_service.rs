// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: 2026 Vecton Contributors

//! MetadataPathServiceProto implementation.
//!
//! This is a thin adapter layer that converts path-based requests to inode-based operations.
//! All FS semantics are delegated to MetadataFsServiceProto (fs.proto).

use super::extract_and_inject_context;
use super::{fatal_fs_header, need_refresh_header, ok_header_from_request};
use crate::error::{MetadataError, MetadataResult};
use crate::mount::MountTable;
use crate::path_resolver::PathResolver;
use crate::raft::{AppRaftNode, RocksDBStorage};
use common::error::canonical::RefreshReason;
use common::header::RpcErrorCode;
use proto::metadata::metadata_fs_service_proto_server::MetadataFsServiceProto as FsServiceTrait;
use proto::metadata::metadata_path_service_proto_server::MetadataPathServiceProto;
use proto::metadata::*;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{instrument, warn};
use types::fs::{FileAttrs, FsErrorCode, Inode, InodeKind};
use types::layout::FileLayout;
use types::RaftLogId;

/// MetadataPathServiceProto implementation.
pub struct MetadataPathServiceImpl {
    path_resolver: PathResolver,
    fs_service: Arc<super::fs_service::MetadataFsServiceImpl>,
    mount_table: Arc<MountTable>,
    storage: Arc<RocksDBStorage>,
    raft_node: Option<Arc<AppRaftNode>>,
    metrics: Option<Arc<crate::metrics::MetadataMetrics>>,
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
            mount_table,
            storage,
            raft_node: None,
            metrics: None,
        }
    }

    /// Set Raft node for leader/follower information (optional).
    pub fn with_raft_node(mut self, raft_node: Arc<AppRaftNode>) -> Self {
        self.raft_node = Some(raft_node);
        self
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
                    return Err(MetadataError::StaleState(format!(
                        "Mount epoch mismatch: client={}, server={} (mount_id={:?}). Client must refresh mount table.",
                        client_mount_epoch, mount_ctx.mount_epoch, mount_ctx.mount_id
                    )));
                }
            }
        }
        Ok(())
    }

    /// Get latest state_id if available.
    fn get_latest_state_id(&self) -> Option<RaftLogId> {
        self.raft_node
            .as_ref()
            .and_then(|node| node.get_last_applied_state_id())
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

    /// Convert types Inode to proto InodeProto.
    fn inode_to_proto(inode: &Inode) -> proto::fs::InodeProto {
        let (file_data, dir_data, symlink_data) = match &inode.data {
            types::fs::InodeData::File { extents, lease_epoch } => (
                Some(proto::fs::InodeFileProto {
                    extents: extents
                        .iter()
                        .map(|e| proto::fs::ExtentProto {
                            file_offset: e.file_offset,
                            block_id: Some(proto::common::BlockIdProto {
                                data_handle_id: e.block_id.data_handle_id.as_raw(),
                                block_index: e.block_id.index.as_raw(),
                            }),
                            block_offset: e.block_offset,
                            len: e.len,
                            file_version: e.file_version,
                            block_stamp: e.block_stamp,
                        })
                        .collect(),
                    lease_epoch: *lease_epoch,
                    lease_id: None, // Optional, not stored in inode currently
                }),
                None,
                None,
            ),
            types::fs::InodeData::Dir => (None, Some(proto::fs::InodeDirectoryProto {}), None),
            types::fs::InodeData::Symlink { target } => (
                None,
                None,
                target.as_ref().map(|t| proto::fs::InodeSymlinkProto {
                    target: Some(t.clone()),
                }),
            ),
        };

        use proto::fs::inode_proto::Data;
        let data = if let Some(file) = file_data {
            Some(Data::File(file))
        } else if let Some(dir) = dir_data {
            Some(Data::Dir(dir))
        } else if let Some(symlink) = symlink_data {
            Some(Data::Symlink(symlink))
        } else {
            None
        };

        proto::fs::InodeProto {
            inode_id: Some(proto::fs::InodeIdProto {
                value: inode.inode_id.as_raw(),
            }),
            kind: match inode.kind {
                InodeKind::File => proto::fs::InodeKindProto::InodeKindFile as i32,
                InodeKind::Dir => proto::fs::InodeKindProto::InodeKindDir as i32,
                InodeKind::Symlink => proto::fs::InodeKindProto::InodeKindSymlink as i32,
            },
            attrs: Some(Self::file_attrs_to_proto(&inode.attrs)),
            data,
            mount_id: Some(proto::common::MountIdProto {
                value: inode.mount_id.as_raw(),
            }),
            xattrs: inode
                .xattrs
                .iter()
                .map(|(k, v)| proto::fs::XattrProto {
                    name: k.clone(),
                    value: v.clone(),
                })
                .collect(),
        }
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

        // Resolve path to inode
        let resolved = self
            .path_resolver
            .resolve_path(&req.path)
            .map_err(|e| Status::from_error(Box::new(e)))?;

        // If entry doesn't exist, return ENOENT
        let inode_id = resolved.inode_id.ok_or_else(|| {
            let err = MetadataError::NotFound(format!("Entry not found: {}", req.path));
            Status::from_error(Box::new(err))
        })?;

        // Get inode and attrs
        let inode = self
            .storage
            .get_inode(inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {}", inode_id)))?;

        let resp_header = ok_header_from_request(&req.header, None, None);

        Ok(Response::new(LookupPathResponseProto {
            header: Some(resp_header),
            inode: Some(Self::inode_to_proto(&inode)),
            attrs: Some(Self::file_attrs_to_proto(&inode.attrs)),
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn get_attr_path(
        &self,
        request: Request<GetAttrPathRequestProto>,
    ) -> Result<Response<GetAttrPathResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        // Resolve path to inode
        let resolved = self
            .path_resolver
            .resolve_inode(&req.path)
            .map_err(|e| Status::from_error(Box::new(e)))?;

        // Get inode and attrs
        let inode = self
            .storage
            .get_inode(resolved.inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {}", resolved.inode_id)))?;

        let resp_header = ok_header_from_request(&req.header, None, None);

        Ok(Response::new(GetAttrPathResponseProto {
            header: Some(resp_header),
            attrs: Some(Self::file_attrs_to_proto(&inode.attrs)),
            inode: Some(Self::inode_to_proto(&inode)),
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn mkdir_path(
        &self,
        request: Request<MkdirPathRequestProto>,
    ) -> Result<Response<MkdirPathResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        // Resolve path
        let resolved = self
            .path_resolver
            .resolve_path(&req.path)
            .map_err(|e| Status::from_error(Box::new(e)))?;

        // Validate mount_epoch
        if let Err(MetadataError::StaleState(msg)) = self.validate_mount_epoch(&req.header, &resolved.mount_ctx) {
            warn!(
                path = %req.path,
                msg = %msg,
                "MkdirPath rejected: mount epoch mismatch (NEED_REFRESH)"
            );
            let resp_header = need_refresh_header(
                &req.header,
                RpcErrorCode::MountEpochMismatch,
                RefreshReason::MountEpochMismatch,
                msg,
                Some(resolved.mount_ctx.owner_group_id.as_raw()),
                Some(resolved.mount_ctx.mount_epoch),
            );
            return Ok(Response::new(MkdirPathResponseProto {
                header: Some(resp_header),
                inode: None,
                attrs: None,
            }));
        }

        // Convert attrs
        let attrs = Self::proto_to_file_attrs(req.attrs)?;

        // Call FS service Mkdir
        let fs_req = MkdirRequestProto {
            header: req.header.clone(),
            parent_inode_id: Some(proto::fs::InodeIdProto {
                value: resolved.parent_inode_id.as_raw(),
            }),
            name: resolved.name,
            attrs: Some(Self::file_attrs_to_proto(&attrs)),
        };

        let fs_resp = FsServiceTrait::mkdir(self.fs_service.as_ref(), Request::new(fs_req))
            .await
            .map_err(|e| {
                // Convert gRPC Status to MetadataError
                MetadataError::Internal(format!("FS service error: {}", e))
            })?;

        let fs_resp_inner = fs_resp.into_inner();

        // Build response with mount_epoch and state_id
        let mut resp_header = ok_header_from_request(
            &req.header,
            Some(resolved.mount_ctx.owner_group_id.as_raw()),
            Some(resolved.mount_ctx.mount_epoch),
        );
        if let Some(state_id) = self.get_latest_state_id() {
            resp_header.state_id = Some(proto::common::RaftLogIdProto {
                term: state_id.term,
                leader_node_id: state_id.leader_node_id,
                index: state_id.index,
            });
        }

        Ok(Response::new(MkdirPathResponseProto {
            header: Some(resp_header),
            inode: fs_resp_inner.inode,
            attrs: fs_resp_inner.attrs,
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn create_path(
        &self,
        request: Request<CreatePathRequestProto>,
    ) -> Result<Response<CreatePathResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        // Resolve path
        let resolved = self
            .path_resolver
            .resolve_path(&req.path)
            .map_err(|e| Status::from_error(Box::new(e)))?;

        // Validate mount_epoch
        if let Err(MetadataError::StaleState(msg)) = self.validate_mount_epoch(&req.header, &resolved.mount_ctx) {
            warn!(
                path = %req.path,
                msg = %msg,
                "CreatePath rejected: mount epoch mismatch (NEED_REFRESH)"
            );
            let resp_header = need_refresh_header(
                &req.header,
                RpcErrorCode::MountEpochMismatch,
                RefreshReason::MountEpochMismatch,
                msg,
                Some(resolved.mount_ctx.owner_group_id.as_raw()),
                Some(resolved.mount_ctx.mount_epoch),
            );
            return Ok(Response::new(CreatePathResponseProto {
                header: Some(resp_header),
                inode_id: None,
                inode: None,
                attrs: None,
            }));
        }

        // Convert attrs and layout
        let attrs = Self::proto_to_file_attrs(req.attrs)?;
        let layout = Self::proto_to_file_layout(req.layout)?;

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

        let fs_resp = FsServiceTrait::create(self.fs_service.as_ref(), Request::new(fs_req))
            .await
            .map_err(|e| MetadataError::Internal(format!("FS service error: {}", e)))?;

        let fs_resp_inner = fs_resp.into_inner();

        // Build response
        let mut resp_header = ok_header_from_request(
            &req.header,
            Some(resolved.mount_ctx.owner_group_id.as_raw()),
            Some(resolved.mount_ctx.mount_epoch),
        );
        if let Some(state_id) = self.get_latest_state_id() {
            resp_header.state_id = Some(proto::common::RaftLogIdProto {
                term: state_id.term,
                leader_node_id: state_id.leader_node_id,
                index: state_id.index,
            });
        }

        Ok(Response::new(CreatePathResponseProto {
            header: Some(resp_header),
            inode_id: fs_resp_inner.inode.as_ref().and_then(|i| i.inode_id.clone()),
            inode: fs_resp_inner.inode,
            attrs: fs_resp_inner.attrs,
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn unlink_path(
        &self,
        request: Request<UnlinkPathRequestProto>,
    ) -> Result<Response<UnlinkPathResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        // Resolve path
        let resolved = self
            .path_resolver
            .resolve_path(&req.path)
            .map_err(|e| Status::from_error(Box::new(e)))?;

        // Validate mount_epoch
        if let Err(MetadataError::StaleState(msg)) = self.validate_mount_epoch(&req.header, &resolved.mount_ctx) {
            warn!(
                path = %req.path,
                msg = %msg,
                "UnlinkPath rejected: mount epoch mismatch (NEED_REFRESH)"
            );
            let resp_header = need_refresh_header(
                &req.header,
                RpcErrorCode::MountEpochMismatch,
                RefreshReason::MountEpochMismatch,
                msg,
                Some(resolved.mount_ctx.owner_group_id.as_raw()),
                Some(resolved.mount_ctx.mount_epoch),
            );
            return Ok(Response::new(UnlinkPathResponseProto {
                header: Some(resp_header),
            }));
        }

        // Call FS service Unlink
        let fs_req = UnlinkRequestProto {
            header: req.header.clone(),
            parent_inode_id: Some(proto::fs::InodeIdProto {
                value: resolved.parent_inode_id.as_raw(),
            }),
            name: resolved.name,
        };

        FsServiceTrait::unlink(self.fs_service.as_ref(), Request::new(fs_req))
            .await
            .map_err(|e| MetadataError::Internal(format!("FS service error: {}", e)))?;

        // Build response
        let mut resp_header = ok_header_from_request(
            &req.header,
            Some(resolved.mount_ctx.owner_group_id.as_raw()),
            Some(resolved.mount_ctx.mount_epoch),
        );
        if let Some(state_id) = self.get_latest_state_id() {
            resp_header.state_id = Some(proto::common::RaftLogIdProto {
                term: state_id.term,
                leader_node_id: state_id.leader_node_id,
                index: state_id.index,
            });
        }

        Ok(Response::new(UnlinkPathResponseProto {
            header: Some(resp_header),
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn rmdir_path(
        &self,
        request: Request<RmdirPathRequestProto>,
    ) -> Result<Response<RmdirPathResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        // Resolve path
        let resolved = self
            .path_resolver
            .resolve_path(&req.path)
            .map_err(|e| Status::from_error(Box::new(e)))?;

        // Validate mount_epoch
        if let Err(MetadataError::StaleState(msg)) = self.validate_mount_epoch(&req.header, &resolved.mount_ctx) {
            warn!(
                path = %req.path,
                msg = %msg,
                "RmdirPath rejected: mount epoch mismatch (NEED_REFRESH)"
            );
            let resp_header = need_refresh_header(
                &req.header,
                RpcErrorCode::MountEpochMismatch,
                RefreshReason::MountEpochMismatch,
                msg,
                Some(resolved.mount_ctx.owner_group_id.as_raw()),
                Some(resolved.mount_ctx.mount_epoch),
            );
            return Ok(Response::new(RmdirPathResponseProto {
                header: Some(resp_header),
            }));
        }

        // Call FS service Rmdir
        let fs_req = RmdirRequestProto {
            header: req.header.clone(),
            parent_inode_id: Some(proto::fs::InodeIdProto {
                value: resolved.parent_inode_id.as_raw(),
            }),
            name: resolved.name,
        };

        FsServiceTrait::rmdir(self.fs_service.as_ref(), Request::new(fs_req))
            .await
            .map_err(|e| MetadataError::Internal(format!("FS service error: {}", e)))?;

        // Build response
        let mut resp_header = ok_header_from_request(
            &req.header,
            Some(resolved.mount_ctx.owner_group_id.as_raw()),
            Some(resolved.mount_ctx.mount_epoch),
        );
        if let Some(state_id) = self.get_latest_state_id() {
            resp_header.state_id = Some(proto::common::RaftLogIdProto {
                term: state_id.term,
                leader_node_id: state_id.leader_node_id,
                index: state_id.index,
            });
        }

        Ok(Response::new(RmdirPathResponseProto {
            header: Some(resp_header),
        }))
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
            Err(e) => {
                // Check if it's a cross-mount error
                if let MetadataError::InvalidArgument(msg) = &e {
                    if msg.contains("Cross-mount") {
                        // Return EXDEV error
                        let resp_header = fatal_fs_header(&req.header, FsErrorCode::EXDev, msg.clone(), None, None);
                        return Ok(Response::new(RenamePathResponseProto {
                            header: Some(resp_header),
                        }));
                    }
                }
                return Err(Status::from_error(Box::new(e)));
            }
        };

        // Validate mount_epoch (both should be same mount at this point)
        if let Err(MetadataError::StaleState(msg)) = self.validate_mount_epoch(&req.header, &src_resolved.mount_ctx) {
            warn!(
                src_path = %req.src_path,
                dst_path = %req.dst_path,
                msg = %msg,
                "RenamePath rejected: mount epoch mismatch (NEED_REFRESH)"
            );
            let resp_header = need_refresh_header(
                &req.header,
                RpcErrorCode::MountEpochMismatch,
                RefreshReason::MountEpochMismatch,
                msg,
                Some(src_resolved.mount_ctx.owner_group_id.as_raw()),
                Some(src_resolved.mount_ctx.mount_epoch),
            );
            return Ok(Response::new(RenamePathResponseProto {
                header: Some(resp_header),
            }));
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

        FsServiceTrait::rename(self.fs_service.as_ref(), Request::new(fs_req))
            .await
            .map_err(|e| MetadataError::Internal(format!("FS service error: {}", e)))?;

        // Build response
        let mut resp_header = ok_header_from_request(
            &req.header,
            Some(src_resolved.mount_ctx.owner_group_id.as_raw()),
            Some(src_resolved.mount_ctx.mount_epoch),
        );
        if let Some(state_id) = self.get_latest_state_id() {
            resp_header.state_id = Some(proto::common::RaftLogIdProto {
                term: state_id.term,
                leader_node_id: state_id.leader_node_id,
                index: state_id.index,
            });
        }

        Ok(Response::new(RenamePathResponseProto {
            header: Some(resp_header),
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn list_status_path(
        &self,
        request: Request<ListStatusPathRequestProto>,
    ) -> Result<Response<ListStatusPathResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        // Resolve path to inode
        let resolved = self
            .path_resolver
            .resolve_inode(&req.path)
            .map_err(|e| Status::from_error(Box::new(e)))?;

        // Verify it's a directory
        let inode = self
            .storage
            .get_inode(resolved.inode_id)?
            .ok_or_else(|| MetadataError::NotFound(format!("Inode not found: {}", resolved.inode_id)))?;

        if !inode.kind.is_dir() {
            let resp_header = fatal_fs_header(
                &req.header,
                FsErrorCode::ENotDir,
                format!("Not a directory: {}", req.path),
                None,
                None,
            );
            return Ok(Response::new(ListStatusPathResponseProto {
                header: Some(resp_header),
                entries: vec![],
                next_cursor: vec![],
                eof: true,
            }));
        }

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

            let fs_resp = FsServiceTrait::read_dir(self.fs_service.as_ref(), Request::new(fs_req))
                .await
                .map_err(|e| MetadataError::Internal(format!("FS service error: {}", e)))?;

            let fs_resp_inner = fs_resp.into_inner();

            let resp_header = ok_header_from_request(&req.header, None, None);

            Ok(Response::new(ListStatusPathResponseProto {
                header: Some(resp_header),
                entries: fs_resp_inner.entries,
                next_cursor: fs_resp_inner.next_cursor_key,
                eof: fs_resp_inner.eof,
            }))
        } else {
            // Recursive listing: TODO implement BFS/DFS with hard limits
            // For now, return unimplemented
            let resp_header = fatal_fs_header(
                &req.header,
                FsErrorCode::ENotImpl,
                "Recursive listing not yet implemented".to_string(),
                None,
                None,
            );
            Ok(Response::new(ListStatusPathResponseProto {
                header: Some(resp_header),
                entries: vec![],
                next_cursor: vec![],
                eof: true,
            }))
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
        let resolved = self
            .path_resolver
            .resolve_inode(&req.path)
            .map_err(|e| Status::from_error(Box::new(e)))?;

        // Call FS service Open
        let fs_req = OpenRequestProto {
            header: req.header.clone(),
            inode_id: Some(proto::fs::InodeIdProto {
                value: resolved.inode_id.as_raw(),
            }),
            flags: req.flags,
        };

        let fs_resp = FsServiceTrait::open(self.fs_service.as_ref(), Request::new(fs_req))
            .await
            .map_err(|e| MetadataError::Internal(format!("FS service error: {}", e)))?;

        let fs_resp_inner = fs_resp.into_inner();

        let resp_header = ok_header_from_request(&req.header, None, None);

        Ok(Response::new(OpenPathResponseProto {
            header: Some(resp_header),
            file_handle: fs_resp_inner.file_handle,
        }))
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

        FsServiceTrait::release(self.fs_service.as_ref(), Request::new(fs_req))
            .await
            .map_err(|e| MetadataError::Internal(format!("FS service error: {}", e)))?;

        let resp_header = ok_header_from_request(&req.header, None, None);

        Ok(Response::new(ReleasePathResponseProto {
            header: Some(resp_header),
        }))
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
                let resolved = self
                    .path_resolver
                    .resolve_inode(&path)
                    .map_err(|e| Status::from_error(Box::new(e)))?;

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

                FsServiceTrait::fsync(self.fs_service.as_ref(), Request::new(fs_req))
                    .await
                    .map_err(|e| MetadataError::Internal(format!("FS service error: {}", e)))?;
            }
            Some(proto::metadata::fsync_path_request_proto::Target::FileHandle(_handle)) => {
                // Handle-based fsync: TODO implement when FS service supports it
                return Err(Status::unimplemented("Handle-based fsync not yet implemented"));
            }
            None => {
                return Err(Status::invalid_argument("Either path or file_handle must be provided"));
            }
        }

        let resp_header = ok_header_from_request(&req.header, None, None);

        Ok(Response::new(FsyncPathResponseProto {
            header: Some(resp_header),
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn hsync_path(
        &self,
        request: Request<HsyncPathRequestProto>,
    ) -> Result<Response<HsyncPathResponseProto>, Status> {
        let req = request.into_inner();
        let inner = req.fsync.ok_or_else(|| Status::invalid_argument("missing fsync"))?;
        let resp = self.fsync_path(Request::new(inner)).await?;
        Ok(Response::new(HsyncPathResponseProto {
            header: resp.into_inner().header,
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn hflush_path(
        &self,
        request: Request<HflushPathRequestProto>,
    ) -> Result<Response<HflushPathResponseProto>, Status> {
        let req = request.into_inner();
        let inner = req.fsync.ok_or_else(|| Status::invalid_argument("missing fsync"))?;
        let resp = self.fsync_path(Request::new(inner)).await?;
        Ok(Response::new(HflushPathResponseProto {
            header: resp.into_inner().header,
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn truncate_path(
        &self,
        request: Request<TruncatePathRequestProto>,
    ) -> Result<Response<TruncatePathResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);

        // Resolve path to inode
        let resolved = self
            .path_resolver
            .resolve_inode(&req.path)
            .map_err(|e| Status::from_error(Box::new(e)))?;

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

        let fs_resp = FsServiceTrait::truncate(self.fs_service.as_ref(), Request::new(fs_req))
            .await
            .map_err(|e| MetadataError::Internal(format!("FS service error: {}", e)))?;

        let fs_resp_inner = fs_resp.into_inner();

        Ok(Response::new(TruncatePathResponseProto {
            header: fs_resp_inner.header,
            new_size: fs_resp_inner.new_size,
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn set_xattr_path(
        &self,
        request: Request<SetXattrPathRequestProto>,
    ) -> Result<Response<SetXattrPathResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);
        let resolved = self
            .path_resolver
            .resolve_inode(&req.path)
            .map_err(|e| Status::from_error(Box::new(e)))?;
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
        let resp = FsServiceTrait::set_xattr(self.fs_service.as_ref(), Request::new(fs_req))
            .await
            .map_err(|e| MetadataError::Internal(format!("FS service error: {}", e)))?;
        Ok(Response::new(SetXattrPathResponseProto {
            header: resp.into_inner().header,
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn get_xattr_path(
        &self,
        request: Request<GetXattrPathRequestProto>,
    ) -> Result<Response<GetXattrPathResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);
        let resolved = self
            .path_resolver
            .resolve_inode(&req.path)
            .map_err(|e| Status::from_error(Box::new(e)))?;
        let fs_req = GetXattrRequestProto {
            header: req.header.clone(),
            inode_id: Some(proto::fs::InodeIdProto {
                value: resolved.inode_id.as_raw(),
            }),
            name: req.name,
        };
        let resp = FsServiceTrait::get_xattr(self.fs_service.as_ref(), Request::new(fs_req))
            .await
            .map_err(|e| MetadataError::Internal(format!("FS service error: {}", e)))?;
        let inner = resp.into_inner();
        Ok(Response::new(GetXattrPathResponseProto {
            header: inner.header,
            value: inner.value,
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn list_xattr_path(
        &self,
        request: Request<ListXattrPathRequestProto>,
    ) -> Result<Response<ListXattrPathResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);
        let resolved = self
            .path_resolver
            .resolve_inode(&req.path)
            .map_err(|e| Status::from_error(Box::new(e)))?;
        let fs_req = ListXattrRequestProto {
            header: req.header.clone(),
            inode_id: Some(proto::fs::InodeIdProto {
                value: resolved.inode_id.as_raw(),
            }),
        };
        let resp = FsServiceTrait::list_xattr(self.fs_service.as_ref(), Request::new(fs_req))
            .await
            .map_err(|e| MetadataError::Internal(format!("FS service error: {}", e)))?;
        let inner = resp.into_inner();
        Ok(Response::new(ListXattrPathResponseProto {
            header: inner.header,
            names: inner.names,
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn remove_xattr_path(
        &self,
        request: Request<RemoveXattrPathRequestProto>,
    ) -> Result<Response<RemoveXattrPathResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);
        let resolved = self
            .path_resolver
            .resolve_inode(&req.path)
            .map_err(|e| Status::from_error(Box::new(e)))?;
        let fs_req = RemoveXattrRequestProto {
            header: req.header.clone(),
            inode_id: Some(proto::fs::InodeIdProto {
                value: resolved.inode_id.as_raw(),
            }),
            name: req.name,
        };
        let resp = FsServiceTrait::remove_xattr(self.fs_service.as_ref(), Request::new(fs_req))
            .await
            .map_err(|e| MetadataError::Internal(format!("FS service error: {}", e)))?;
        Ok(Response::new(RemoveXattrPathResponseProto {
            header: resp.into_inner().header,
        }))
    }

    #[instrument(skip(self), fields(call_id, client_id))]
    async fn get_file_block_locations_path(
        &self,
        request: Request<GetFileBlockLocationsPathRequestProto>,
    ) -> Result<Response<GetFileBlockLocationsPathResponseProto>, Status> {
        let req = request.into_inner();
        let _caller_ctx = extract_and_inject_context(&req.header);
        let resolved = self
            .path_resolver
            .resolve_inode(&req.path)
            .map_err(|e| Status::from_error(Box::new(e)))?;
        let fs_req = GetFileLayoutRequestProto {
            header: req.header.clone(),
            inode_id: Some(proto::fs::InodeIdProto {
                value: resolved.inode_id.as_raw(),
            }),
            range: req.range,
        };
        let resp = FsServiceTrait::get_file_layout(self.fs_service.as_ref(), Request::new(fs_req))
            .await
            .map_err(|e| MetadataError::Internal(format!("FS service error: {}", e)))?;
        let inner = resp.into_inner();
        Ok(Response::new(GetFileBlockLocationsPathResponseProto {
            header: inner.header,
            extents: inner.extents,
            file_size: inner.file_size,
            locations: inner.locations,
        }))
    }
}
